#!/usr/bin/env python3
"""
tests/test_challenger_r2_sse.py
Adversarial Empirical Challenge Test Suite for Milestone 2:
Live Backstage SSE Stream Wiring & Event Bridge.

Author: teamwork_preview_challenger_m2_1 (EMPIRICAL CHALLENGER)
Focus: Finding edge-case bugs, stress-testing assumptions, and rigorously
verifying the 5 Hand B Auditor Invariants + Specs under hostile conditions:
  1. Headless Invariant: Zero exceptions / AttributeError with None, missing, or malformed webui_server.
  2. Buffer Stream Invariant: Streaming integrity, exact 4096-char capping boundary, zero token drops, large/unicode deltas.
  3. No Hanging Hands Invariant: Guaranteed settlement and unregistration on normal finish, errors, generator aborts, and cancellation.
  4. Mouth Broadcast Invariant: Exact payload schema, Slack mrkdwn fidelity, model fallback across all ingress paths.
  5. Thread Safety Invariant: High-concurrency multi-threaded publishing into SSE streams without deadlocks or race conditions.
  6. Dual Sync/Async & Immediate Handshake: Polymorphic protocol, multi-client concurrent connection state synchronization.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import time
import json
import socket
import asyncio
import inspect
import threading
import unittest
from typing import Dict, Any, Optional, List, AsyncGenerator
from unittest.mock import patch, MagicMock, AsyncMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import aiohttp
from aiohttp import web, ClientSession

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
from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
)
from golem.webui.server import (
    WebUIServer,
    _SyncAndAwaitableNone,
    format_sse_event,
    get_system_status,
    start_webui_server,
    stop_webui_server,
    get_active_webui_server,
)


def _get_free_port() -> int:
    """Acquire a temporary available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class MockRecordingWebUIServer:
    """Rigorous recording WebUI server recording all method invocations and timing."""

    def __init__(self) -> None:
        self.registered_hands: List[Dict[str, Any]] = []
        self.buffer_updates: List[Dict[str, Any]] = []
        self.unregistered_hands: List[str] = []
        self.broadcasts: List[Dict[str, Any]] = []
        self._active_hands: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()

    def register_active_hand(
        self, pane_id: str, node: str = "localhost", command: str = "", pid: Optional[int] = None, worker: Optional[str] = "agy", **kwargs: Any
    ) -> Any:
        with self.lock:
            record = {
                "pane_id": pane_id,
                "node": node,
                "command": command,
                "pid": pid,
                "worker": worker or "agy",
                "status": "RUNNING",
                "buffer_tail": f"[PID: {pid if pid is not None else 'N/A'}]",
            }
            self.registered_hands.append(record)
            self._active_hands[pane_id] = record
        return _SyncAndAwaitableNone()

    def update_hand_buffer(self, pane_id: str, delta: str) -> Any:
        with self.lock:
            self.buffer_updates.append({"pane_id": pane_id, "delta": delta})
            if pane_id not in self._active_hands:
                self.register_active_hand(pane_id=pane_id, command="Auto")
            curr = self._active_hands[pane_id].get("buffer_tail", "")
            self._active_hands[pane_id]["buffer_tail"] = (curr + delta)[-4096:]
        return _SyncAndAwaitableNone()

    def unregister_active_hand(self, pane_id: str) -> Any:
        with self.lock:
            self.unregistered_hands.append(pane_id)
            self._active_hands.pop(pane_id, None)
        return _SyncAndAwaitableNone()

    def broadcast(self, event: str, data: Any) -> Any:
        with self.lock:
            self.broadcasts.append({"event": event, "data": data})
        return _SyncAndAwaitableNone()


class MockRecordingSlackClient:
    """Mock Slack client tracking all message posts and reactions."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions: List[Dict[str, Any]] = []

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any) -> Dict[str, Any]:
        msg = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": f"msg_{time.time()}_{len(self.posted_messages)}",
        }
        self.posted_messages.append(msg)
        return {"ok": True, "ts": msg["ts"], "message": msg}

    async def reactions_add(self, name: str, channel: str, timestamp: str) -> Dict[str, Any]:
        self.reactions.append({"action": "add", "name": name, "channel": channel, "ts": timestamp})
        return {"ok": True}

    async def reactions_remove(self, name: str, channel: str, timestamp: str) -> Dict[str, Any]:
        self.reactions.append({"action": "remove", "name": name, "channel": channel, "ts": timestamp})
        return {"ok": True}


class TestChallengerHeadlessInvariant(unittest.IsolatedAsyncioTestCase):
    """
    INVARIANT 1 CHALLENGE:
    Ingress execution when webui_server is None must raise ZERO AttributeError or exceptions
    under adversarial and abnormal stream conditions.
    """

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(registry_path="/tmp/test_challenger_headless_reg.json")
        self.client = MockRecordingSlackClient()
        self.gateway = IngressGateway(router=self.router, webui_server=None)
        self.session = await self.router.get_or_create(channel="C_HEADLESS", thread_ts="T_1")
        if isinstance(self.session, tuple):
            self.session = self.session[0]

    async def test_headless_normal_task_zero_attribute_error(self) -> None:
        """Verify normal task execution in headless mode executes without webui_server."""
        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="headless delta 1\n")
            yield TextDeltaEvent(text_delta="headless delta 2\n")
            yield FinishEvent(status="SUCCESS", response="")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            accumulator = await self.gateway._execute_backstage_task(
                task_desc="echo 'headless'",
                session=self.session,
                client=self.client,
                channel="C_HEADLESS",
                thread_ts="T_1",
            )
            self.assertEqual(accumulator.turn_status, "SUCCESS")
            self.assertIn("headless delta 1", accumulator.get_full_output())

    async def test_headless_empty_stream_zero_attribute_error(self) -> None:
        """Verify empty event stream (zero events emitted) completes cleanly."""
        async def mock_events(session, desc):
            if False:
                yield None

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            accumulator = await self.gateway._execute_backstage_task(
                task_desc="empty stream task",
                session=self.session,
                client=self.client,
                channel="C_HEADLESS",
                thread_ts="T_1",
            )
            self.assertIsNotNone(accumulator)

    async def test_headless_exception_in_stream_zero_attribute_error(self) -> None:
        """Verify stream raising exception terminates cleanly without AttributeError on settlement."""
        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="Starting...\n")
            raise RuntimeError("Underlying PTY crashed!")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            accumulator = await self.gateway._execute_backstage_task(
                task_desc="crash task",
                session=self.session,
                client=self.client,
                channel="C_HEADLESS",
                thread_ts="T_1",
            )
            self.assertEqual(accumulator.turn_status, "ERROR")

    async def test_headless_cancellation_zero_attribute_error(self) -> None:
        """Verify task cancellation mid-flight completes finally block cleanly without webui_server."""
        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="before cancel\n")
            raise asyncio.CancelledError()

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            try:
                await self.gateway._execute_backstage_task(
                    task_desc="cancel task",
                    session=self.session,
                    client=self.client,
                    channel="C_HEADLESS",
                    thread_ts="T_1",
                )
            except asyncio.CancelledError:
                pass

    async def test_headless_deleted_or_missing_webui_server_attr(self) -> None:
        """Verify gateway handles complete absence of webui_server attribute without AttributeError."""
        delattr(self.gateway, "webui_server")
        self.assertFalse(hasattr(self.gateway, "webui_server"))

        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="delta without attr\n")
            yield FinishEvent(status="SUCCESS", response="")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            accumulator = await self.gateway._execute_backstage_task(
                task_desc="no attr task",
                session=self.session,
                client=self.client,
                channel="C_HEADLESS",
                thread_ts="T_1",
            )
            self.assertEqual(accumulator.turn_status, "SUCCESS")

        # Also test _broadcast_mouth_briefing without attr
        await self.gateway._broadcast_mouth_briefing("Test briefing", "flash")

    async def test_headless_mouth_broadcast_call_is_noop(self) -> None:
        """Verify _broadcast_mouth_briefing is a safe no-op when webui_server is None."""
        self.gateway.webui_server = None
        # Should complete without error
        await self.gateway._broadcast_mouth_briefing("Executive summary", "flash")


class TestChallengerBufferStreamInvariant(unittest.IsolatedAsyncioTestCase):
    """
    INVARIANT 2 CHALLENGE:
    TextDeltaEvent streams deltas to WebUI buffer without dropping tokens or truncating under 4096 chars.
    Stress-tests boundary conditions, massive token counts, and exotic encodings.
    """

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(registry_path="/tmp/test_challenger_buffer_reg.json")
        self.client = MockRecordingSlackClient()
        self.webui = MockRecordingWebUIServer()
        self.gateway = IngressGateway(router=self.router, webui_server=self.webui)
        self.session = await self.router.get_or_create(channel="C_BUFFER", thread_ts="T_2")
        if isinstance(self.session, tuple):
            self.session = self.session[0]

    async def test_stress_1000_rapid_micro_tokens_no_drops(self) -> None:
        """Verify 1,000 rapid sequential micro-deltas are all captured without dropping a single token."""
        token_count = 1000
        tokens = [f"t{i}_" for i in range(token_count)]
        expected_full_text = "".join(tokens)

        async def mock_events(session, desc):
            for t in tokens:
                yield TextDeltaEvent(text_delta=t)
            yield FinishEvent(status="SUCCESS", response="")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            accumulator = await self.gateway._execute_backstage_task(
                task_desc="stream 1000 tokens",
                session=self.session,
                client=self.client,
                channel="C_BUFFER",
                thread_ts="T_2",
            )

        # 1. Verify accumulator received every token
        self.assertEqual(accumulator.get_full_output(), expected_full_text)

        # 2. Verify webui received exactly 1000 buffer updates
        self.assertEqual(len(self.webui.buffer_updates), token_count)
        reconstructed = "".join(u["delta"] for u in self.webui.buffer_updates)
        self.assertEqual(reconstructed, expected_full_text)

        # 3. Verify webui broadcast exactly 1000 hand_buffer events
        hand_buffer_broadcasts = [b for b in self.webui.broadcasts if b["event"] == "hand_buffer"]
        self.assertEqual(len(hand_buffer_broadcasts), token_count)

    def test_buffer_exact_4096_boundary_capping(self) -> None:
        """Test exact boundary: 4,095 chars, exactly 4,096 chars (no truncation), and 4,097 chars (+1 truncated)."""
        server = WebUIServer(host="127.0.0.1", port=_get_free_port())
        server.register_active_hand(pane_id="test_pane", command="boundary_test")

        # Reset initial buffer_tail to empty for clean measurement
        server._active_hands["test_pane"]["buffer_tail"] = ""

        # Step 1: 4,095 characters
        chunk_4095 = "A" * 4095
        server.update_hand_buffer("test_pane", chunk_4095)
        self.assertEqual(len(server.active_hands["test_pane"]["buffer_tail"]), 4095)
        self.assertEqual(server.active_hands["test_pane"]["buffer_tail"], chunk_4095)

        # Step 2: Add 1 character -> exactly 4,096 characters (MUST NOT truncate)
        server.update_hand_buffer("test_pane", "B")
        tail = server.active_hands["test_pane"]["buffer_tail"]
        self.assertEqual(len(tail), 4096)
        self.assertTrue(tail.startswith("AAAA"))
        self.assertTrue(tail.endswith("B"))

        # Step 3: Add 1 character -> 4,097 chars total, must truncate oldest character
        server.update_hand_buffer("test_pane", "C")
        tail = server.active_hands["test_pane"]["buffer_tail"]
        self.assertEqual(len(tail), 4096)
        # Should now start with 4094 A's, then B, then C
        self.assertEqual(tail[-2:], "BC")
        self.assertEqual(tail[0], "A")

    def test_huge_delta_and_unicode_surrogate_safety(self) -> None:
        """Verify huge delta (100,000 characters) and exotic Unicode (emojis, CJK, ANSI) are handled cleanly."""
        server = WebUIServer(host="127.0.0.1", port=_get_free_port())
        server.register_active_hand(pane_id="unicode_pane", command="unicode_test")
        server._active_hands["unicode_pane"]["buffer_tail"] = ""

        # Exotic Unicode with complex graphemes and ANSI colors
        exotic = "\033[32m[SUCCESS]\033[0m 🚀 Multi-agent mesh: 👨‍👩‍👧‍👦 漢語/日本語 test\n"
        server.update_hand_buffer("unicode_pane", exotic)
        tail = server.active_hands["unicode_pane"]["buffer_tail"]
        self.assertEqual(tail, exotic)

        # Massive 100k chunk
        huge = "Z" * 100000 + "END"
        server.update_hand_buffer("unicode_pane", huge)
        tail = server.active_hands["unicode_pane"]["buffer_tail"]
        self.assertEqual(len(tail), 4096)
        self.assertTrue(tail.endswith("END"))

    async def test_empty_string_and_none_text_deltas(self) -> None:
        """Verify empty strings or None deltas do not cause crashes or broadcast blank updates."""
        async def mock_events(session, desc):
            yield TextDeltaEvent(text_delta="")
            yield TextDeltaEvent(text_delta=None)
            yield TextDeltaEvent(text_delta="valid\n")
            yield FinishEvent(status="SUCCESS", response="")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=mock_events):
            accumulator = await self.gateway._execute_backstage_task(
                task_desc="empty deltas",
                session=self.session,
                client=self.client,
                channel="C_BUFFER",
                thread_ts="T_2",
            )

        # Only the valid delta should trigger buffer update
        self.assertEqual(len(self.webui.buffer_updates), 1)
        self.assertEqual(self.webui.buffer_updates[0]["delta"], "valid\n")


class TestChallengerNoHangingHandsInvariant(unittest.IsolatedAsyncioTestCase):
    """
    INVARIANT 3 CHALLENGE:
    Settles (hand_settle) and unregisters hand on success, error, and task cancellation.
    No hanging hands allowed under any failure mode.
    """

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(registry_path="/tmp/test_challenger_nohang_reg.json")
        self.client = MockRecordingSlackClient()
        self.webui = MockRecordingWebUIServer()
        self.gateway = IngressGateway(router=self.router, webui_server=self.webui)
        self.session = await self.router.get_or_create(channel="C_NOHANG", thread_ts="T_3")
        if isinstance(self.session, tuple):
            self.session = self.session[0]

    async def test_settle_on_unhandled_generator_abort(self) -> None:
        """Generator blows up immediately on first iteration: must still settle and unregister."""
        async def exploding_generator(session, desc):
            raise ConnectionResetError("Fatal network loss to remote PTY worker!")
            yield TextDeltaEvent(text_delta="unreachable")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=exploding_generator):
            accumulator = await self.gateway._execute_backstage_task(
                task_desc="exploding generator",
                session=self.session,
                client=self.client,
                channel="C_NOHANG",
                thread_ts="T_3",
            )

        # Must record error turn
        self.assertEqual(accumulator.turn_status, "ERROR")

        # Hand MUST be registered and then settled + unregistered
        self.assertEqual(len(self.webui.registered_hands), 1)
        self.assertEqual(len(self.webui.unregistered_hands), 1)
        settles = [b for b in self.webui.broadcasts if b["event"] == "hand_settle"]
        self.assertEqual(len(settles), 1)
        self.assertEqual(settles[0]["data"]["exit_code"], 1)
        self.assertEqual(settles[0]["data"]["status"], "FAILED")
        self.assertEqual(len(self.webui._active_hands), 0)

    async def test_settle_on_external_task_cancellation(self) -> None:
        """Cancel task externally while blocked in event queue: must settle and unregister."""
        task_started = asyncio.Event()

        async def slow_generator(session, desc):
            task_started.set()
            while True:
                await asyncio.sleep(0.1)
                yield TextDeltaEvent(text_delta="waiting...")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=slow_generator):
            coro = self.gateway._execute_backstage_task(
                task_desc="slow build",
                session=self.session,
                client=self.client,
                channel="C_NOHANG",
                thread_ts="T_3",
            )
            task = asyncio.create_task(coro)
            await task_started.wait()
            # Let it consume at least one delta
            await asyncio.sleep(0.05)
            # Cancel task mid-stream
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Must settle and unregister
        self.assertEqual(len(self.webui.registered_hands), 1)
        self.assertEqual(len(self.webui.unregistered_hands), 1)
        settles = [b for b in self.webui.broadcasts if b["event"] == "hand_settle"]
        self.assertEqual(len(settles), 1)
        self.assertEqual(len(self.webui._active_hands), 0)

    async def test_concurrent_sessions_distinct_panes_no_cross_contamination(self) -> None:
        """Two concurrent backstage tasks on distinct panes: settling Pane A does NOT unregister Pane B."""
        session_b = await self.router.get_or_create(channel="C_NOHANG", thread_ts="T_4")
        if isinstance(session_b, tuple):
            session_b = session_b[0]

        pane_a = "runner_pane_a"
        pane_b = "runner_pane_b"
        self.session.runner_pane = pane_a
        session_b.runner_pane = pane_b

        gate_a = asyncio.Event()
        gate_b = asyncio.Event()

        async def stream_a(s, d):
            yield TextDeltaEvent(text_delta="A output\n")
            await gate_a.wait()
            yield FinishEvent(status="SUCCESS", response="Done A")

        async def stream_b(s, d):
            yield TextDeltaEvent(text_delta="B output\n")
            await gate_b.wait()
            yield FinishEvent(status="SUCCESS", response="Done B")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=[stream_a(None, None), stream_b(None, None)]):
            task_a = asyncio.create_task(self.gateway._execute_backstage_task(
                task_desc="Task A", session=self.session, client=self.client, channel="C_NOHANG", thread_ts="T_3",
            ))
            task_b = asyncio.create_task(self.gateway._execute_backstage_task(
                task_desc="Task B", session=session_b, client=self.client, channel="C_NOHANG", thread_ts="T_4",
            ))

            # Wait briefly until both are registered
            await asyncio.sleep(0.05)
            self.assertIn(pane_a, self.webui._active_hands)
            self.assertIn(pane_b, self.webui._active_hands)

            # Let Task A complete
            gate_a.set()
            await task_a

            # Verify Pane A is settled and unregistered, but Pane B is STILL ACTIVE!
            self.assertNotIn(pane_a, self.webui._active_hands)
            self.assertIn(pane_b, self.webui._active_hands)

            # Let Task B complete
            gate_b.set()
            await task_b
            self.assertNotIn(pane_b, self.webui._active_hands)
            self.assertEqual(len(self.webui._active_hands), 0)


class TestChallengerMouthBroadcastInvariant(unittest.IsolatedAsyncioTestCase):
    """
    INVARIANT 4 CHALLENGE:
    mouth_briefing payload matches Slack post format and is broadcast immediately.
    Verify across all ingress execution pathways.
    """

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(registry_path="/tmp/test_challenger_mouth_reg.json")
        self.client = MockRecordingSlackClient()
        self.webui = MockRecordingWebUIServer()
        self.gateway = IngressGateway(router=self.router, webui_server=self.webui)
        self.session = await self.router.get_or_create(channel="C_MOUTH", thread_ts="T_5")
        if isinstance(self.session, tuple):
            self.session = self.session[0]

    async def test_mouth_briefing_exact_payload_and_mrkdwn_preservation(self) -> None:
        """Verify mouth_briefing retains exact Slack mrkdwn bold/links and model label."""
        slack_msg = "🎯 *Mesh Deployment Complete:*\n• Pod `<https://k8s.local/pod/1|api-server>` is active\n• Exit code: `0`"
        await self.gateway._broadcast_mouth_briefing(slack_msg, model="gemini-3.8-flash")

        mouth_broadcasts = [b for b in self.webui.broadcasts if b["event"] == "mouth_briefing"]
        self.assertEqual(len(mouth_broadcasts), 1)
        data = mouth_broadcasts[0]["data"]
        self.assertEqual(data["briefing"], slack_msg)
        self.assertEqual(data["model"], "gemini-3.8-flash")

    async def test_mouth_briefing_fallback_model_resolution(self) -> None:
        """Verify that when model is None, it resolves to mouth_model or get_mouth_model()."""
        with patch("golem.gateway.ingress.get_mouth_model", return_value="gemini-flash-fallback"):
            await self.gateway._broadcast_mouth_briefing("Test brief", model=None)

        mouth_broadcasts = [b for b in self.webui.broadcasts if b["event"] == "mouth_briefing"]
        self.assertEqual(len(mouth_broadcasts), 1)
        data = mouth_broadcasts[0]["data"]
        self.assertEqual(data["model"], "gemini-flash-fallback")

    async def test_mouth_briefing_rapid_successive_burst(self) -> None:
        """Verify 50 rapid briefings are all broadcast in FIFO order without drops."""
        for i in range(50):
            await self.gateway._broadcast_mouth_briefing(f"Briefing #{i}", model="test-model")

        mouth_broadcasts = [b for b in self.webui.broadcasts if b["event"] == "mouth_briefing"]
        self.assertEqual(len(mouth_broadcasts), 50)
        for i in range(50):
            self.assertEqual(mouth_broadcasts[i]["data"]["briefing"], f"Briefing #{i}")


class TestChallengerThreadSafetyInvariant(unittest.IsolatedAsyncioTestCase):
    """
    INVARIANT 5 CHALLENGE:
    Event bus alerts bridge to SSE queues strictly via loop.call_soon_threadsafe.
    Adversarial stress-test: 50 concurrent OS threads publishing sensory alerts
    while connected SSE clients continuously read stream chunks.
    """

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port, event_bus=self.bus)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()
        self.bus.clear()

    async def test_multi_threaded_storm_50_threads_zero_deadlocks(self) -> None:
        """50 separate OS threads publishing concurrently into bus while client reads SSE stream."""
        url = f"http://127.0.0.1:{self.port}/api/stream"
        thread_count = 50
        errors: List[Exception] = []

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                self.assertEqual(resp.status, 200)

                # Drain connected handshake
                await resp.content.readline()  # event: connected
                await resp.content.readline()  # data: {...}
                await resp.content.readline()  # \n

                def worker_thread(idx: int) -> None:
                    try:
                        alert = ShoulderTapAlert(
                            alert_type="eye_log",
                            source="eyes",
                            summary=f"Storm thread #{idx}",
                            data={"state": "RUNNING", "thread_idx": idx},
                        )
                        self.bus.publish(alert)
                    except Exception as e:
                        errors.append(e)

                threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(thread_count)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=3.0)

                self.assertEqual(len(errors), 0, f"Thread errors encountered: {errors}")

                # Read events from SSE stream
                received_indices = set()
                # Give a brief moment for all threadsafe callbacks to process
                deadline = time.time() + 5.0
                while len(received_indices) < thread_count and time.time() < deadline:
                    try:
                        line = await asyncio.wait_for(resp.content.readline(), timeout=1.0)
                    except asyncio.TimeoutError:
                        break
                    line_str = line.decode().strip()
                    if line_str.startswith("event: eye_log"):
                        data_line = await resp.content.readline()
                        data = json.loads(data_line.decode().replace("data: ", "").strip())
                        idx = data.get("data", {}).get("thread_idx")
                        if idx is not None:
                            received_indices.add(idx)

                # Verify all 50 threads delivered their events cleanly into SSE
                self.assertEqual(len(received_indices), thread_count)


class TestChallengerSyncAndAwaitableNoneAndHandshake(unittest.IsolatedAsyncioTestCase):
    """
    INVARIANTS 6 & 7 CHALLENGE:
    _SyncAndAwaitableNone protocol ergonomics + Immediate State Handshake for SSE clients.
    """

    def test_sync_and_awaitable_none_deep_ergonomics(self) -> None:
        """Verify _SyncAndAwaitableNone edge cases: slots, noop await, repeated awaits, gather."""
        obj = _SyncAndAwaitableNone()
        self.assertTrue(inspect.isawaitable(obj))

        async def runner():
            # Awaiting once
            r1 = await obj
            self.assertIsNone(r1)

            # Awaiting again (must not exhaust generator)
            r2 = await obj
            self.assertIsNone(r2)

            # Usable inside asyncio.gather
            results = await asyncio.gather(obj, obj, obj)
            self.assertEqual(results, [None, None, None])

        asyncio.run(runner())

    async def test_immediate_hands_handshake_multi_client_race(self) -> None:
        """5 clients connect concurrently while 3 active hands are running: ALL receive immediate hands_status."""
        port = _get_free_port()
        server = WebUIServer(host="127.0.0.1", port=port)
        await server.start()

        try:
            # Register 3 active hands
            for i in range(3):
                server.register_active_hand(
                    pane_id=f"pane_{i}",
                    node=f"node_{i}",
                    command=f"cmd_{i}",
                    pid=1000 + i,
                )

            url = f"http://127.0.0.1:{port}/events"
            client_count = 5

            async def client_reader(cid: int) -> int:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        # Drain connected
                        await resp.content.readline()
                        await resp.content.readline()
                        await resp.content.readline()

                        # Read hands_status
                        ev_line = await asyncio.wait_for(resp.content.readline(), timeout=2.0)
                        self.assertEqual(ev_line.decode().strip(), "event: hands_status")
                        data_line = await resp.content.readline()
                        hands = json.loads(data_line.decode().replace("data: ", "").strip())
                        return len(hands)

            tasks = [asyncio.create_task(client_reader(i)) for i in range(client_count)]
            counts = await asyncio.gather(*tasks)
            self.assertEqual(counts, [3] * client_count)

        finally:
            await server.stop()


if __name__ == "__main__":
    unittest.main()
