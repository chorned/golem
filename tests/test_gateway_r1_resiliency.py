#!/usr/bin/env python3
"""
tests/test_gateway_r1_resiliency.py
Targeted test suite for Milestone 1: Slack Gateway Resiliency & Protocol Integrity (R1).

Validates:
  1. Removal of hardcoded fallback briefings ('Runner is currently IDLE...', 'Air is clean...')
  2. Step 2 schema invariant: briefing_payload includes complete 'tools' schema declaration
  3. Fail-loud error reporting on Step 2 synthesis failure (raw error, HTTP code, tool JSON, :boom: reaction)
  4. Thread context rehydration on all active thread replies without mutating clean_text
  5. Direct sensory command routing (/status, !status, /eyes, !eyes, /health, !health, /nose, !nose)
  6. Mouth system prompt calibration forbidding unprompted sensory tool calls during conversational chat
"""

import os
import sys
import json
import time
import asyncio
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


class MockSlackClient:
    """Mock Slack client recording chat_postMessage, reactions, and thread history."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.uploaded_files: List[Dict[str, Any]] = []
        self.thread_history: Dict[str, List[Dict[str, Any]]] = {}

    async def chat_postMessage(
        self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        msg = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": f"msg_{time.time()}_{len(self.posted_messages)}",
            **kwargs,
        }
        self.posted_messages.append(msg)
        return msg

    async def reactions_add(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> None:
        self.reactions_added.append(
            {"channel": channel, "timestamp": timestamp, "name": name}
        )

    async def reactions_remove(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> None:
        self.reactions_removed.append(
            {"channel": channel, "timestamp": timestamp, "name": name}
        )

    async def conversations_replies(
        self, channel: str, ts: str, limit: int = 20, **kwargs: Any
    ) -> Dict[str, Any]:
        key = f"{channel}:{ts}"
        messages = self.thread_history.get(key, [])
        return {"ok": True, "messages": messages}


class MockHeadlessAdapter:
    """Mock adapter recording received turns and yielding events."""

    def __init__(self, cwd: str = "/tmp") -> None:
        self.cwd = cwd
        self.turns_received: List[str] = []
        self.started = True
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_received.append(prompt)
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text_delta=f"*Response to:* {prompt}")
        yield FinishEvent(response=f"*Response to:* {prompt}", status="SUCCESS")


class TestGatewayR1Resiliency(unittest.IsolatedAsyncioTestCase):
    """Test suite validating R1 resiliency and protocol integrity invariants."""

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR,
            adapter_factory=lambda cwd: MockHeadlessAdapter(cwd=cwd),
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = MockSlackClient()
        os.environ["GEMINI_API_KEY"] = "fake-test-key-for-unit-testing"

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()
        os.environ.pop("GEMINI_API_KEY", None)

    # --------------------------------------------------------------------------
    # 1. Step 2 Schema Invariant
    # --------------------------------------------------------------------------
    async def test_step2_synthesis_payload_includes_tools_schema(self) -> None:
        """Verify that Step 2 synthesis payload strictly includes 'tools' schema declaration."""
        captured_payloads: List[Dict[str, Any]] = []

        async def mock_call_gemini(payload: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
            captured_payloads.append(payload)
            return {
                "candidates": [
                    {
                        "content": {
                            "parts": [{"text": "Executive briefing synthesized successfully."}]
                        }
                    }
                ]
            }

        self.gateway._call_gemini_api = mock_call_gemini

        candidate = {
            "content": {
                "parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]
            }
        }
        tool_result = {"state": "IDLE", "buffer_tail": "all quiet"}

        briefing_text, error_info = await self.gateway._execute_step2_synthesis(
            sys_prompt="Chief of Staff prompt",
            prompt="what is running?",
            candidate=candidate,
            fn_call={"name": "check_eyes"},
            fn_name="check_eyes",
            tool_result=tool_result,
            model="gemini-2.5-flash",
            api_key="fake-key",
        )

        self.assertEqual(briefing_text, "Executive briefing synthesized successfully.")
        self.assertIsNone(error_info)
        self.assertEqual(len(captured_payloads), 1)

        payload = captured_payloads[0]
        # Invariant: tools schema MUST be present in Step 2 payload
        self.assertIn("tools", payload)
        expected_tools = get_mouth_tools_declaration()
        self.assertEqual(payload["tools"], expected_tools)

        # Invariant: functionResponse part must match function name and tool result
        contents = payload["contents"]
        self.assertEqual(len(contents), 3)  # user prompt, model call, user functionResponse
        func_resp = contents[2]["parts"][0]["functionResponse"]
        self.assertEqual(func_resp["name"], "check_eyes")
        self.assertEqual(func_resp["response"]["content"], tool_result)

    # --------------------------------------------------------------------------
    # 2. Fail Loudly on Step 2 Synthesis Failure
    # --------------------------------------------------------------------------
    async def test_step2_synthesis_failure_posts_raw_exception_and_boom(self) -> None:
        """Verify that Step 2 synthesis failure posts HTTP error details, tool payload, and swaps to :boom:."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_nose", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        # Simulate Step 2 API error (e.g. HTTP 400 Bad Request)
        turn2_error = {
            "_error": True,
            "status": 400,
            "code": 400,
            "error_type": "HTTP_400",
            "raw_error": "Invalid functionResponse schema or quota exceeded",
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, turn2_error]

            with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_exec_nose:
                mock_exec_nose.return_value = {
                    "smells_off": False,
                    "minutes_ago": 1.5,
                    "verdict_summary": "All systems nominal; 0 errors.",
                    "disk": {"free_gb": 125.0},
                    "docker": {"container_count": 5},
                    "tailscale": {"online": True},
                }

                event = {
                    "channel": "C_RESIL",
                    "ts": "100.901",
                    "thread_ts": "100.901",
                    "text": "<@U_GOLEM> check fleet status",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                msg = self.client.posted_messages[0]["text"]

                # Invariant: Must fail loudly with raw exception, HTTP code, and tool payload
                self.assertIn("⚠️ *Executive Synthesis Failed* (HTTP 400 / HTTP_400):", msg)
                self.assertIn("Invalid functionResponse schema or quota exceeded", msg)
                self.assertIn("All systems nominal; 0 errors.", msg)
                self.assertIn("125.0", msg)
                self.assertIn("_Raw sensory data returned above without executive synthesis._", msg)

                # Invariant: Reaction must be swapped to boom
                added_reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("boom", added_reactions)

    # --------------------------------------------------------------------------
    # 3. Removal of Hardcoded Fallback Briefings
    # --------------------------------------------------------------------------
    async def test_fallback_briefings_removed_for_idle_eyes(self) -> None:
        """Verify that hardcoded 'Runner is currently IDLE' briefing is removed and fails loudly."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_eyes", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            # Turn 2 returns None (synthesis failure)
            mock_gemini.side_effect = [turn1_resp, None]

            with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_exec_eyes:
                mock_exec_eyes.return_value = {
                    "state": "IDLE",
                    "active_command": "",
                    "pid": None,
                    "blocker": None,
                    "buffer_tail": "",
                }

                event = {
                    "channel": "C_RESIL",
                    "ts": "100.902",
                    "thread_ts": "100.902",
                    "text": "<@U_GOLEM> eyes status",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                msg = self.client.posted_messages[0]["text"]

                # Invariant: Never simulate normal operational telemetry on failure
                self.assertNotIn("Runner is currently IDLE", msg)
                self.assertNotIn("Runner is currently *IDLE*", msg)
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)

                added_reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("boom", added_reactions)

    async def test_fallback_briefings_removed_for_clean_nose(self) -> None:
        """Verify that hardcoded 'Air is clean; all systems nominal' briefing is removed and fails loudly."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_nose", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, None]

            with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_exec_nose:
                mock_exec_nose.return_value = {
                    "smells_off": False,
                    "minutes_ago": 1.0,
                    "verdict_summary": "Clean fleet.",
                    "disk": {"free_gb": 80.0},
                    "docker": {"container_count": 3},
                    "tailscale": {"online": True},
                }

                event = {
                    "channel": "C_RESIL",
                    "ts": "100.903",
                    "thread_ts": "100.903",
                    "text": "<@U_GOLEM> nose status",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                msg = self.client.posted_messages[0]["text"]

                # Invariant: Never simulate normal operational telemetry on failure
                self.assertNotIn("Air is clean; all systems nominal", msg)
                self.assertNotIn("Air is clean", msg)
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)

                added_reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("boom", added_reactions)

    # --------------------------------------------------------------------------
    # 4. Thread Context Rehydration without Mutating clean_text
    # --------------------------------------------------------------------------
    async def test_thread_context_rehydrates_without_mutating_clean_text(self) -> None:
        """Verify thread context is rehydrated in prompt_to_send while preserving untouched clean_text."""
        self.client.thread_history["C_TH:100.1"] = [
            {"ts": "100.1", "user": "U_CARL", "text": "Deploying the microservices"},
            {"ts": "100.2", "user": "U_GOLEM", "text": "Deployment initiated"},
        ]

        # Warm reply in active thread
        event = {
            "channel": "C_TH",
            "thread_ts": "100.1",
            "ts": "100.3",
            "text": "<@U_GOLEM> /wrapup",  # User explicitly sends /wrapup in warm thread
        }

        # The message contains /wrapup. If clean_text were mutated with thread history,
        # clean_text would start with "[Previous Slack Thread Context:" and command parsing would fail!
        await self.gateway.handle_message(event, self.client)

        # /wrapup must have executed cleanly as a command interception
        self.assertEqual(len(self.client.posted_messages), 1)
        self.assertIn("🧹 *Wrapup:* Housekeeping complete", self.client.posted_messages[0]["text"])

    async def test_thread_context_rehydration_warm_followup_received_by_adapter(self) -> None:
        """Verify conversational replies in active thread receive rehydrated thread context."""
        adapter = MockHeadlessAdapter()
        self.router.adapter_factory = lambda cwd: adapter

        self.client.thread_history["C_TH:200.1"] = [
            {"ts": "200.1", "user": "U_CARL", "text": "Where is the build log located?"},
        ]

        event = {
            "channel": "C_TH",
            "thread_ts": "200.1",
            "ts": "200.2",
            "text": "and what was the returncode?",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(adapter.turns_received), 1)
        received = adapter.turns_received[0]
        # Invariant: prompt_to_send must contain rehydrated context followed by user's message
        self.assertIn("Previous Slack Thread Context", received)
        self.assertIn("Where is the build log located?", received)
        self.assertIn("and what was the returncode?", received)

    # --------------------------------------------------------------------------
    # 5. Direct Sensory Command Routing
    # --------------------------------------------------------------------------
    async def test_direct_sensory_command_routing_eyes(self) -> None:
        """Verify /status, !status, /eyes, !eyes route directly to _execute_check_eyes."""
        commands = ("/status", "!status", "/eyes", "!eyes")
        for i, cmd in enumerate(commands):
            with self.subTest(cmd=cmd):
                self.client.posted_messages.clear()
                self.client.reactions_added.clear()

                with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
                    mock_eyes.return_value = {
                        "state": "RUNNING",
                        "active_command": "cargo test --workspace",
                        "pid": 4242,
                        "buffer_tail": "test result: ok. 14 passed",
                        "blocker": None,
                    }

                    event = {
                        "channel": "C_CMD_EYES",
                        "ts": f"100.{100 + i}",
                        "thread_ts": f"100.{100 + i}",
                        "text": cmd,
                    }
                    await self.gateway.handle_message(event, self.client)

                    mock_eyes.assert_awaited_once()
                    self.assertEqual(len(self.client.posted_messages), 1)
                    msg = self.client.posted_messages[0]["text"]
                    self.assertIn("👀 *Runner Observer:* Status is *RUNNING* (PID 4242)", msg)
                    self.assertIn("cargo test --workspace", msg)
                    self.assertIn("test result: ok", msg)

                    added_reactions = [r["name"] for r in self.client.reactions_added]
                    self.assertIn("white_check_mark", added_reactions)

    async def test_direct_sensory_command_routing_eyes_blocker(self) -> None:
        """Verify /status with active blocker surfaces auth URL and sets warning reaction."""
        with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
            mock_eyes.return_value = {
                "state": "BLOCKER",
                "active_command": "rclone config",
                "pid": 9999,
                "blocker": {"url": "http://127.0.0.1:53682/auth"},
                "buffer_tail": "",
            }

            event = {
                "channel": "C_CMD_BLK",
                "ts": "100.999",
                "thread_ts": "100.999",
                "text": "/status",
            }
            await self.gateway.handle_message(event, self.client)

            msg = self.client.posted_messages[0]["text"]
            self.assertIn("Waiting for authorization (BLOCKER)", msg)
            self.assertIn("<http://127.0.0.1:53682/auth|Click here to authenticate>", msg)

            added_reactions = [r["name"] for r in self.client.reactions_added]
            self.assertIn("warning", added_reactions)

    async def test_direct_sensory_command_routing_nose(self) -> None:
        """Verify /health, !health, /nose, !nose route directly to _execute_check_nose."""
        commands = ("/health", "!health", "/nose", "!nose")
        for i, cmd in enumerate(commands):
            with self.subTest(cmd=cmd):
                self.client.posted_messages.clear()
                self.client.reactions_added.clear()

                with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
                    mock_nose.return_value = {
                        "smells_off": False,
                        "minutes_ago": 0.8,
                        "verdict_summary": "All daemon logs nominal.",
                        "disk": {"free_gb": 320.5},
                        "docker": {"container_count": 8},
                        "tailscale": {"online": True},
                    }

                    event = {
                        "channel": "C_CMD_NOSE",
                        "ts": f"100.{200 + i}",
                        "thread_ts": f"100.{200 + i}",
                        "text": cmd,
                    }
                    await self.gateway.handle_message(event, self.client)

                    mock_nose.assert_awaited_once_with(trigger_fresh_sniff=True)
                    self.assertEqual(len(self.client.posted_messages), 1)
                    msg = self.client.posted_messages[0]["text"]
                    self.assertIn("👃 *Fleet Health:* Air is clean; all systems nominal", msg)
                    self.assertIn("320.5 GB free", msg)
                    self.assertIn("8 containers running", msg)

                    added_reactions = [r["name"] for r in self.client.reactions_added]
                    self.assertIn("white_check_mark", added_reactions)

    async def test_direct_sensory_command_routing_nose_anomaly(self) -> None:
        """Verify /health with anomaly surfaces alert and sets warning reaction."""
        with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
            mock_nose.return_value = {
                "smells_off": True,
                "minutes_ago": 0.2,
                "verdict_summary": "Memory exhaustion on node desktop-pc.",
                "disk": {"free_gb": 4.1},
                "docker": {"container_count": 2},
                "tailscale": {"online": True},
            }

            event = {
                "channel": "C_CMD_ANOM",
                "ts": "100.888",
                "thread_ts": "100.888",
                "text": "!health",
            }
            await self.gateway.handle_message(event, self.client)

            msg = self.client.posted_messages[0]["text"]
            self.assertIn("👃 *Fleet Health Anomaly:* Something smells off", msg)
            self.assertIn("Memory exhaustion on node desktop-pc", msg)

            added_reactions = [r["name"] for r in self.client.reactions_added]
            self.assertIn("warning", added_reactions)

    # --------------------------------------------------------------------------
    # 6. Mouth System Prompt Calibration
    # --------------------------------------------------------------------------
    def test_mouth_prompt_forbids_unprompted_sensory_calls(self) -> None:
        """Verify Mouth system prompt explicitly forbids unprompted sensory tool calls."""
        ingress_path = os.path.join(PROJECT_DIR, "golem", "gateway", "ingress.py")
        with open(ingress_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Invariant: Must include fleet topology and machine roles
        self.assertIn("Fleet Topology & Machine Roles:", content)
        self.assertIn("desktop-pc", content)
        self.assertIn("Windows", content)

    # --------------------------------------------------------------------------
    # 7. call_gemini_api Error Payload Integrity
    # --------------------------------------------------------------------------
    async def test_call_gemini_api_error_payload_on_network_failure(self) -> None:
        """Verify call_gemini_api returns structured _error dictionary on exception."""
        with patch("aiohttp.ClientSession.post", side_effect=RuntimeError("Connection refused")):
            res = await call_gemini_api(payload={"dummy": True}, api_key="test-key")
            self.assertIsInstance(res, dict)
            self.assertTrue(res.get("_error"))
            self.assertEqual(res.get("status"), 500)
            self.assertEqual(res.get("error_type"), "RuntimeError")
            self.assertIn("Connection refused", res.get("raw_error", ""))

    async def test_conversational_machine_query_does_not_trigger_check_nose(self) -> None:
        """Verify that conversational machine inquiry ('which machine is the PC?') does not call check_nose."""
        event = {
            "type": "message",
            "text": "<@U12345> do you know which machine is the PC?",
            "channel": "C1",
            "ts": "1720000001.0001",
        }
        mock_client = MagicMock()
        mock_client.reactions_add = AsyncMock(return_value={"ok": True})
        mock_client.reactions_remove = AsyncMock(return_value={"ok": True})
        mock_client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "1720000001.0002"})

        # Mock gemini initial response as pure conversational reply using Fleet Topology
        mock_turn_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "Yes, that's `desktop-pc`—our high-VRAM Windows node for CUDA workloads."}
                        ]
                    }
                }
            ]
        }

        with patch.object(self.gateway, "_call_gemini_api", new_callable=AsyncMock) as mock_gemini, \
             patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
            mock_gemini.return_value = mock_turn_resp
            await self.gateway.handle_message(event, mock_client)

            # Verification: check_nose was NEVER called
            mock_nose.assert_not_called()

            # Verification: Message posted contains the answer, not sensory fallback
            mock_client.chat_postMessage.assert_called_once()
            call_kwargs = mock_client.chat_postMessage.call_args[1]
            posted_text = call_kwargs.get("text", "")
            self.assertIn("desktop-pc", posted_text)
            self.assertNotIn("Air is clean", posted_text)
            self.assertNotIn("Fleet Health", posted_text)


if __name__ == "__main__":
    unittest.main()
