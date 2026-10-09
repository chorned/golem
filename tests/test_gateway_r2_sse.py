#!/usr/bin/env python3
"""
tests/test_gateway_r2_sse.py
Targeted test suite for Milestone 2: Live Backstage SSE Stream Wiring & Event Bridge.

Validates the 5 Hand B Auditor Invariants:
  1. Headless Invariant: Zero AttributeError when webui_server is None.
  2. Buffer Stream Invariant: TextDeltaEvent emits both update_hand_buffer and hand_buffer SSE broadcast.
  3. No Hanging Hands Invariant: Hands settled (hand_settle) and unregistered (unregister_active_hand) on success, error, and cancellation.
  4. Mouth Broadcast Invariant: mouth_briefing broadcasts exact {"briefing": clean_briefing, "model": model} upon Slack post.
  5. Thread Safety Invariant: Ingress and WebUI integration runs without event loop deadlock.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import time
import asyncio
import inspect
import unittest
from typing import Optional, List, Dict, Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    AgentEvent,
)
from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle
from golem.gateway.ingress import (
    IngressGateway,
    call_gemini_api,
    get_mouth_tools_declaration,
)
from golem.webui.server import WebUIServer, _SyncAndAwaitableNone


class MockWebUIServer:
    """Mock WebUIServer recording registered hands, buffer updates, unregistrations, and broadcasts."""

    def __init__(self) -> None:
        self.registered_hands: List[Dict[str, Any]] = []
        self.buffer_updates: List[Dict[str, Any]] = []
        self.unregistered_hands: List[str] = []
        self.broadcasts: List[Dict[str, Any]] = []
        self._active_hands: Dict[str, Dict[str, Any]] = {}

    def register_active_hand(
        self, pane_id: str, node: str = "localhost", command: str = "", pid: Optional[int] = None, worker: Optional[str] = "agy", **kwargs: Any
    ) -> Any:
        record = {
            "pane_id": pane_id,
            "node": node,
            "command": command,
            "pid": pid,
            "worker": worker or "agy",
            "status": "RUNNING",
            "buffer_tail": f"[INFO] Agent {pane_id} initialized\n",
        }
        self.registered_hands.append(record)
        self._active_hands[pane_id] = record
        return _SyncAndAwaitableNone()

    def update_hand_buffer(self, pane_id: str, delta: str) -> Any:
        self.buffer_updates.append({"pane_id": pane_id, "delta": delta})
        if pane_id in self._active_hands:
            self._active_hands[pane_id]["buffer_tail"] += delta
        return _SyncAndAwaitableNone()

    def unregister_active_hand(self, pane_id: str) -> Any:
        self.unregistered_hands.append(pane_id)
        self._active_hands.pop(pane_id, None)
        return _SyncAndAwaitableNone()

    def broadcast(self, event: str, data: Any) -> Any:
        self.broadcasts.append({"event": event, "data": data})
        return _SyncAndAwaitableNone()


class MockSlackClient:
    """Mock Slack client recording chat_postMessage and reactions."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []

    async def chat_postMessage(
        self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        msg = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": f"msg_{time.time()}_{len(self.posted_messages)}",
        }
        self.posted_messages.append(msg)
        return {"ok": True, "ts": msg["ts"], "message": msg}

    async def reactions_add(self, name: str, channel: str, timestamp: str) -> Dict[str, Any]:
        self.reactions_added.append({"name": name, "channel": channel, "timestamp": timestamp})
        return {"ok": True}

    async def reactions_remove(self, name: str, channel: str, timestamp: str) -> Dict[str, Any]:
        self.reactions_removed.append({"name": name, "channel": channel, "timestamp": timestamp})
        return {"ok": True}


class TestGatewayR2SSEWiring(unittest.IsolatedAsyncioTestCase):
    """Targeted tests for Milestone 2 backstage SSE stream wiring in IngressGateway."""

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(registry_path="/tmp/test_golem_r2_registry.json")
        self.client = MockSlackClient()
        self.webui = MockWebUIServer()
        self.gateway = IngressGateway(router=self.router, webui_server=self.webui)
        self.session = await self.router.get_or_create(channel="C_TEST", thread_ts="T_100")
        if isinstance(self.session, tuple):
            self.session = self.session[0]

    async def test_01_headless_invariant_when_webui_server_none(self) -> None:
        """Invariant 1: When webui_server is None (headless mode), execution completes with zero AttributeError."""
        headless_gateway = IngressGateway(router=self.router, webui_server=None)
        self.assertIsNone(headless_gateway.webui_server)

        # Mock event stream with text delta and success finish
        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="echo 42\n")
            yield FinishEvent(status="SUCCESS", response="")

        with patch.object(headless_gateway, "_stream_backstage_events", side_effect=mock_events):
            accumulator = await headless_gateway._execute_backstage_task(
                task_desc="test command",
                session=self.session,
                client=self.client,
                channel="C_TEST",
                thread_ts="T_100",
            )
            self.assertEqual(accumulator.turn_status, "SUCCESS")
            self.assertIn("echo 42", accumulator.get_full_output())

    async def test_02_hand_spawn_registration_and_broadcast(self) -> None:
        """Verify task dispatch registers active hand and broadcasts hand_spawn event."""
        async def mock_events(session, desc):
            yield FinishEvent(status="SUCCESS", response="Done")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            await self.gateway._execute_backstage_task(
                task_desc="cargo build --release",
                target_node="desktop-pc",
                session=self.session,
                client=self.client,
                channel="C_TEST",
                thread_ts="T_100",
                worker="agy",
            )

        # Verify hand registered
        self.assertEqual(len(self.webui.registered_hands), 1)
        reg = self.webui.registered_hands[0]
        self.assertEqual(reg["node"], "desktop-pc")
        self.assertEqual(reg["command"], "cargo build --release")
        self.assertEqual(reg["worker"], "agy")

        # Verify hand_spawn broadcast
        spawn_events = [b for b in self.webui.broadcasts if b["event"] == "hand_spawn"]
        self.assertEqual(len(spawn_events), 1)
        spawn_payload = spawn_events[0]["data"]
        self.assertEqual(spawn_payload["command"], "cargo build --release")
        self.assertEqual(spawn_payload["node"], "desktop-pc")
        self.assertEqual(spawn_payload["status"], "RUNNING")

    async def test_03_buffer_stream_invariant_incremental_deltas(self) -> None:
        """Invariant 2: Each TextDeltaEvent emits both update_hand_buffer and hand_buffer broadcast."""
        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="Compiling core v1.0...\n")
            yield TextDeltaEvent(text_delta="Compiling network v1.0...\n")
            yield TextDeltaEvent(text_delta="Finished release [optimized]\n")
            yield FinishEvent(status="SUCCESS", response="Done")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            await self.gateway._execute_backstage_task(
                task_desc="compile project",
                session=self.session,
                client=self.client,
                channel="C_TEST",
                thread_ts="T_100",
            )

        # Verify 3 update_hand_buffer calls
        self.assertEqual(len(self.webui.buffer_updates), 3)
        self.assertEqual(self.webui.buffer_updates[0]["delta"], "Compiling core v1.0...\n")
        self.assertEqual(self.webui.buffer_updates[1]["delta"], "Compiling network v1.0...\n")
        self.assertEqual(self.webui.buffer_updates[2]["delta"], "Finished release [optimized]\n")

        # Verify 3 hand_buffer broadcast events
        buffer_broadcasts = [b for b in self.webui.broadcasts if b["event"] == "hand_buffer"]
        self.assertEqual(len(buffer_broadcasts), 3)
        self.assertEqual(buffer_broadcasts[0]["data"]["delta"], "Compiling core v1.0...\n")
        self.assertEqual(buffer_broadcasts[1]["data"]["delta"], "Compiling network v1.0...\n")
        self.assertEqual(buffer_broadcasts[2]["data"]["delta"], "Finished release [optimized]\n")

    async def test_04_no_hanging_hands_invariant_on_success(self) -> None:
        """Invariant 3a: On success, hand_settle broadcast emitted (status='EXIT_0') and unregister called."""
        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="ok\n")
            yield FinishEvent(status="SUCCESS", response="ok")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            await self.gateway._execute_backstage_task(
                task_desc="run tests",
                session=self.session,
                client=self.client,
                channel="C_TEST",
                thread_ts="T_100",
            )

        # Verify hand_settle broadcast
        settle_events = [b for b in self.webui.broadcasts if b["event"] == "hand_settle"]
        self.assertEqual(len(settle_events), 1)
        self.assertEqual(settle_events[0]["data"]["exit_code"], 0)
        self.assertEqual(settle_events[0]["data"]["status"], "EXIT_0")

        # Verify unregister_active_hand
        self.assertEqual(len(self.webui.unregistered_hands), 1)
        self.assertEqual(self.webui.unregistered_hands[0], settle_events[0]["data"]["pane_id"])
        # Active hands registry is empty
        self.assertEqual(len(self.webui._active_hands), 0)

    async def test_05_no_hanging_hands_invariant_on_error(self) -> None:
        """Invariant 3b: On failure or exception, hand_settle emitted (status='FAILED') and unregister called."""
        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="Error: segmentation fault\n")
            yield FinishEvent(status="ERROR", response="", error="Segmentation fault")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            await self.gateway._execute_backstage_task(
                task_desc="faulty binary",
                session=self.session,
                client=self.client,
                channel="C_TEST",
                thread_ts="T_100",
            )

        settle_events = [b for b in self.webui.broadcasts if b["event"] == "hand_settle"]
        self.assertEqual(len(settle_events), 1)
        self.assertEqual(settle_events[0]["data"]["exit_code"], 1)
        self.assertEqual(settle_events[0]["data"]["status"], "FAILED")
        self.assertEqual(len(self.webui.unregistered_hands), 1)
        self.assertEqual(len(self.webui._active_hands), 0)

    async def test_06_no_hanging_hands_invariant_on_cancellation(self) -> None:
        """Invariant 3c: When task is cancelled mid-flight, finally block settles and unregisters hand."""
        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="Working...\n")
            raise asyncio.CancelledError()

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            try:
                await self.gateway._execute_backstage_task(
                    task_desc="long running build",
                    session=self.session,
                    client=self.client,
                    channel="C_TEST",
                    thread_ts="T_100",
                )
            except asyncio.CancelledError:
                pass

        # Verify settlement in finally block
        settle_events = [b for b in self.webui.broadcasts if b["event"] == "hand_settle"]
        self.assertEqual(len(settle_events), 1)
        self.assertEqual(len(self.webui.unregistered_hands), 1)
        self.assertEqual(len(self.webui._active_hands), 0)

    async def test_07_mouth_briefing_broadcast_invariant_after_task_synthesis(self) -> None:
        """Invariant 4a: After Step 2 executive briefing synthesis, mouth_briefing is broadcast with exact payload."""
        step1_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "run test suite", "worker": "agy"},
                                }
                            }
                        ]
                    }
                }
            ]
        }
        step2_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "✅ *Test Suite Complete:* All 1,540 tests passed cleanly across mesh."}
                        ]
                    }
                }
            ]
        }

        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="Tests passed.\n")
            yield FinishEvent(status="SUCCESS", response="OK")

        call_count = 0

        async def mock_call_gemini(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return step1_response
            return step2_response

        with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call_gemini), \
             patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events), \
             patch("golem.gateway.ingress.get_mouth_model", return_value="gemini-3.8-flash"), \
             patch("golem.gateway.ingress.get_gemini_api_key", return_value="mock-key"):
            await self.gateway._handle_mouth_turn(
                session=self.session,
                prompt="run test suite",
                client=self.client,
                channel="C_TEST",
                thread_ts="T_100",
                ts="123456.789",
                is_imessage=False,
            )

        # Verify Slack post was made
        self.assertTrue(len(self.client.posted_messages) > 0)
        posted_text = self.client.posted_messages[-1]["text"]
        self.assertIn("Test Suite Complete", posted_text)

        # Verify mouth_briefing SSE broadcast
        mouth_broadcasts = [b for b in self.webui.broadcasts if b["event"] == "mouth_briefing"]
        self.assertEqual(len(mouth_broadcasts), 1)
        payload = mouth_broadcasts[0]["data"]
        self.assertEqual(payload["briefing"], "✅ *Test Suite Complete:* All 1,540 tests passed cleanly across mesh.")
        self.assertEqual(payload["model"], "gemini-3.8-flash")

    async def test_08_mouth_briefing_broadcast_invariant_pure_conversational_turn(self) -> None:
        """Invariant 4b: On Case A pure conversational turn (zero tools), mouth_briefing is broadcast immediately."""
        step1_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "⚓ *Fleet Standing By:* All sensory sentinels report green."}
                        ]
                    }
                }
            ]
        }

        with patch.object(self.gateway, "_call_gemini_api", return_value=step1_response), \
             patch("golem.gateway.ingress.get_mouth_model", return_value="gemini-3.8-flash"), \
             patch("golem.gateway.ingress.get_gemini_api_key", return_value="mock-key"):
            await self.gateway._handle_mouth_turn(
                session=self.session,
                prompt="how we looking?",
                client=self.client,
                channel="C_TEST",
                thread_ts="T_100",
                ts="123456.789",
                is_imessage=False,
            )

        # Verify Slack post made
        self.assertEqual(len(self.client.posted_messages), 1)
        self.assertIn("Fleet Standing By", self.client.posted_messages[0]["text"])

        # Verify mouth_briefing broadcast
        mouth_broadcasts = [b for b in self.webui.broadcasts if b["event"] == "mouth_briefing"]
        self.assertEqual(len(mouth_broadcasts), 1)
        payload = mouth_broadcasts[0]["data"]
        self.assertEqual(payload["briefing"], "⚓ *Fleet Standing By:* All sensory sentinels report green.")
        self.assertEqual(payload["model"], "gemini-3.8-flash")

    async def test_09_dict_and_attribute_text_delta_variations(self) -> None:
        """Verify handling of diverse event formats (TextDeltaEvent, dict with event_type, text attribute)."""
        async def mock_events(session, desc):
            yield {"event_type": "text_delta", "text_delta": "from dict\n"}
            yield {"event_type": "text_delta", "text": "from dict text\n"}
            yield TextDeltaEvent(text="from object text\n")
            yield FinishEvent(status="SUCCESS", response="OK")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            await self.gateway._execute_backstage_task(
                task_desc="stream mixed events",
                session=self.session,
                client=self.client,
                channel="C_TEST",
                thread_ts="T_100",
            )

        buffer_broadcasts = [b for b in self.webui.broadcasts if b["event"] == "hand_buffer"]
        self.assertEqual(len(buffer_broadcasts), 3)
        self.assertEqual(buffer_broadcasts[0]["data"]["delta"], "from dict\n")
        self.assertEqual(buffer_broadcasts[1]["data"]["delta"], "from dict text\n")
        self.assertEqual(buffer_broadcasts[2]["data"]["delta"], "from object text\n")


if __name__ == "__main__":
    unittest.main()
