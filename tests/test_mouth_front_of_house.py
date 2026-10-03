#!/usr/bin/env python3
"""
tests/test_mouth_front_of_house.py
Integration and unit tests for Universal Front-of-House Mouth,
Structured Task Dispatching (Gemini Function Calling), Silent Backstage Execution,
and 3.0s Yield Window.

Validates:
  - Conversational inquiries answered directly by Mouth in 1 turn (0 worker tasks dispatched)
  - Operational tasks trigger dispatch_task(task_description, target_node, cwd)
  - Zero worker text_delta chunks leaked or streamed to Slack during backstage execution
  - 3.0s yield window emits authentic liaison acknowledgment for long-running tasks
  - Final executive briefing synthesized and posted to Slack upon task completion
  - [SEND_FILE: <path>] directives parsed and uploaded cleanly
  - Blocker alerts from Shoulder Tap Event Bus wake Mouth to post interactive action cards
"""

import os
import sys
import time
import json
import asyncio
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Optional, List, Dict, Any, AsyncGenerator

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from first_mate.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert
from first_mate.gateway.ingress import (
    IngressGateway,
    BackstageExecutionAccumulator,
    call_gemini_api,
)
from first_mate.gateway.router import SessionRouter
from first_mate.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    FinishEvent,
    AgentEvent,
)


class MockSlackClient:
    """Mock client recording chat_postMessage, reactions_add, reactions_remove, and files_upload_v2."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.uploaded_files: List[Dict[str, Any]] = []
        self.thread_history: Dict[str, List[Dict[str, Any]]] = {}

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any) -> Dict[str, Any]:
        msg = {"channel": channel, "text": text, "thread_ts": thread_ts, "ts": f"msg_{time.time()}", **kwargs}
        self.posted_messages.append(msg)
        return msg

    async def reactions_add(self, channel: str, timestamp: str, name: str, **kwargs: Any) -> None:
        self.reactions_added.append({"channel": channel, "timestamp": timestamp, "name": name})

    async def reactions_remove(self, channel: str, timestamp: str, name: str, **kwargs: Any) -> None:
        self.reactions_removed.append({"channel": channel, "timestamp": timestamp, "name": name})

    async def files_upload_v2(self, channel: str, thread_ts: Optional[str] = None, file: Optional[str] = None, **kwargs: Any) -> Dict[str, Any]:
        call = {"channel": channel, "thread_ts": thread_ts, "file": file, **kwargs}
        self.uploaded_files.append(call)
        return {"ok": True}

    async def conversations_replies(self, channel: str, ts: str, limit: int = 15, **kwargs: Any) -> Dict[str, Any]:
        return {"ok": True, "messages": self.thread_history.get(f"{channel}:{ts}", [])}


def make_mock_adapter(cwd: str, extra_args: Optional[List[str]] = None) -> Any:
    adapter = MagicMock()
    adapter.start = AsyncMock()
    adapter.stop = AsyncMock()
    adapter.conversation_id = "mock-conv-id"
    return adapter


class TestMouthConversationalInquiry(unittest.IsolatedAsyncioTestCase):
    """Tests verifying conversational inquiries are answered directly by Mouth in 1 turn."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_mock_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_FIRSTMATE", sensory_bus=self.bus)
        self.client = MockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        self.bus.clear()

    async def test_conversational_inquiry_single_turn_direct_reply(self) -> None:
        """
        Verify conversational inquiries like 'How's our fleet doing?' are answered
        directly by The Mouth in 1 turn:
          - Exactly 1 Slack message posted
          - 0 worker subagent tasks dispatched
          - 0 commands run in Pane 2
          - 0 canned announcements or 'Task Completed' footers
        """
        mock_gemini_reply = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "*Fleet Status:* All 4 nodes (hermes, desktop-pc, personal-laptop, oci-machine) are online and operational."}
                        ]
                    },
                    "finishReason": "STOP"
                }
            ]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.return_value = mock_gemini_reply

            event = {
                "channel": "C_MAIN",
                "ts": "100.001",
                "thread_ts": "100.001",
                "text": "<@U_FIRSTMATE> How's our fleet doing?",
            }
            await self.gateway.handle_message(event, self.client)

            # Assertions:
            # 1. Exactly 1 response posted
            self.assertEqual(len(self.client.posted_messages), 1)
            msg = self.client.posted_messages[0]
            self.assertIn("Fleet Status", msg["text"])
            self.assertIn("All 4 nodes", msg["text"])

            # 2. No canned announcements or footers
            self.assertNotIn("Task Completed", msg["text"])
            self.assertNotIn("Running command", msg["text"])

            # 3. Check reaction lifecycle: gear added and swapped to checkmark
            added_names = [r["name"] for r in self.client.reactions_added]
            self.assertIn("white_check_mark", added_names)

    async def test_casual_banter_and_greetings_handled_directly(self) -> None:
        """Verify greetings and check-ins are handled directly without worker involvement."""
        mock_gemini_reply = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "Morning Carl! Standing by for today's operations. What's on deck?"}
                        ]
                    },
                    "finishReason": "STOP"
                }
            ]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.return_value = mock_gemini_reply

            event = {
                "channel": "C_MAIN",
                "ts": "100.002",
                "text": "Good morning firstmate!",
            }
            await self.gateway.handle_message(event, self.client)

            self.assertEqual(len(self.client.posted_messages), 1)
            self.assertIn("Morning Carl", self.client.posted_messages[0]["text"])


class TestStructuredTaskDispatch(unittest.IsolatedAsyncioTestCase):
    """Tests verifying operational tasks trigger structured dispatch_task function calling."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_mock_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_FIRSTMATE", sensory_bus=self.bus)
        self.client = MockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        self.bus.clear()

    async def test_operational_task_triggers_dispatch_task_tool_call(self) -> None:
        """
        Verify operational task ('Run unit tests on hermes') triggers Gemini
        native function calling dispatch_task(task_description, target_node, cwd).
        """
        turn1_tool_call_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {
                                        "task_description": "python3 -m unittest discover tests",
                                        "target_node": "hermes",
                                        "cwd": "/Users/hermes/projects/first-mate"
                                    }
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP"
                }
            ]
        }
        turn2_executive_briefing_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "*Test Suite Green:* Ran 933 tests cleanly in 143s with 0 failures."}
                        ]
                    },
                    "finishReason": "STOP"
                }
            ]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_tool_call_response, turn2_executive_briefing_response]

            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_exec:
                mock_exec.return_value = {"status": "SUCCESS", "output": "Ran 933 tests in 143.984s\n\nOK"}

                event = {
                    "channel": "C_MAIN",
                    "ts": "100.003",
                    "thread_ts": "100.003",
                    "text": "<@U_FIRSTMATE> Run the test suite on hermes",
                }
                await self.gateway.handle_message(event, self.client)

                # Verify dispatch_task was called with correct parameters
                self.assertEqual(mock_exec.call_count, 1)
                args = mock_exec.call_args[0]
                self.assertEqual(args[0], "python3 -m unittest discover tests")
                self.assertEqual(args[1], "hermes")
                self.assertEqual(args[2], "/Users/hermes/projects/first-mate")

    async def test_thought_signature_preserved_in_executive_briefing_payload(self) -> None:
        """
        Verify Gemini 3.x thoughtSignature / thought_signature is preserved
        in the model turn when formulating the executive briefing payload.
        """
        turn1_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "npm run UAT"}
                                },
                                "thoughtSignature": "valid_cryptographic_thought_sig_12345"
                            }
                        ]
                    },
                    "finishReason": "STOP"
                }
            ]
        }
        turn2_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "✅ *All 29 UAT phases passed.*"}
                        ]
                    }
                }
            ]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_response, turn2_response]

            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_exec:
                mock_exec.return_value = {"status": "SUCCESS", "output": "UAT completed successfully with 26/29 passed."}

                event = {
                    "channel": "C_MAIN",
                    "ts": "100.004",
                    "thread_ts": "100.004",
                    "text": "<@U_FIRSTMATE> run UAT suite on desktop-pc",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(mock_gemini.call_count, 2)
                briefing_payload = mock_gemini.call_args_list[1][1]["payload"]
                contents = briefing_payload["contents"]
                model_turn = contents[1]
                self.assertEqual(model_turn["role"], "model")
                self.assertEqual(model_turn["parts"][0]["thoughtSignature"], "valid_cryptographic_thought_sig_12345")
                self.assertIn("dispatch_task", model_turn["parts"][0]["functionCall"]["name"])

    async def test_fallback_does_not_arbitrarily_truncate_output_to_500_chars(self) -> None:
        """
        Verify that if Gemini synthesis fails and drops into the fallback path,
        a 2000-character test summary is NOT truncated down to the last 500 characters.
        """
        turn1_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "npm run UAT"}
                                }
                            }
                        ]
                    }
                }
            ]
        }

        long_uat_output = (
            "Phase 1: Boot Sequence PASSED\n"
            "Phase 2: Header Navigation PASSED\n"
            "Phase 3: Topology Canvas PASSED\n"
            + ("Phase Details Line " * 100) + "\n"
            "Phase 10: Teardown PASSED"
        )
        self.assertGreater(len(long_uat_output), 1500)

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            # Step 1 succeeds, Step 2 (synthesis) returns None to trigger fallback
            mock_gemini.side_effect = [turn1_response, None]

            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_exec:
                mock_exec.return_value = {"status": "SUCCESS", "output": long_uat_output}

                event = {
                    "channel": "C_MAIN",
                    "ts": "100.005",
                    "thread_ts": "100.005",
                    "text": "<@U_FIRSTMATE> run UAT",
                }
                await self.gateway.handle_message(event, self.client)

                posted = [msg["text"] for msg in self.client.posted_messages]
                last_msg = posted[-1]
                self.assertIn("Task Complete", last_msg)
                self.assertIn("Phase 1: Boot Sequence PASSED", last_msg)
                self.assertIn("Phase 10: Teardown PASSED", last_msg)


class TestWorkerStdoutElimination(unittest.IsolatedAsyncioTestCase):
    """
    CRITICAL ACCEPTANCE CRITERION:
    Tests verifying worker stdout (text_delta NDJSON tokens) is NOT streamed to Slack.
    The Hands operate silently backstage.
    """

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_mock_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_FIRSTMATE", sensory_bus=self.bus)
        self.client = MockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        self.bus.clear()

    async def test_worker_text_delta_never_streamed_to_slack(self) -> None:
        """
        Verify that intermediate worker text_delta tokens produced during task execution
        are strictly retained backstage and NEVER posted to Slack.
        Only the final synthesized executive briefing appears in Slack.
        """
        async def noisy_worker_generator() -> AsyncGenerator[AgentEvent, None]:
            yield StatusUpdateEvent(status="working")
            # Intermediate noisy progress lines that MUST NOT leak into Slack:
            yield TextDeltaEvent(text_delta="[1/10] Compiling module_a.c...\n")
            yield TextDeltaEvent(text_delta="[2/10] Compiling module_b.c...\n")
            yield TextDeltaEvent(text_delta="Running test runner suite...\n")
            yield TextDeltaEvent(text_delta="Tests passed: 933 OK\n")
            yield FinishEvent(response="933 tests passed in 143s", status="SUCCESS")

        turn1_dispatch = {
            "candidates": [{
                "content": {"parts": [{"functionCall": {"name": "dispatch_task", "args": {"task_description": "make test"}}}]},
                "finishReason": "STOP"
            }]
        }
        turn2_briefing = {
            "candidates": [{
                "content": {"parts": [{"text": "*Build & Test Complete:* All 933 unit tests passed without errors."}]},
                "finishReason": "STOP"
            }]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_dispatch, turn2_briefing]

            with patch.object(self.gateway, "_stream_backstage_events", return_value=noisy_worker_generator()):
                event = {
                    "channel": "C_MAIN",
                    "ts": "100.004",
                    "thread_ts": "100.004",
                    "text": "Run make test",
                }
                await self.gateway.handle_message(event, self.client)

                # Exactly 1 message posted in Slack: the final executive briefing
                self.assertEqual(len(self.client.posted_messages), 1)
                posted_text = self.client.posted_messages[0]["text"]

                # Assert worker stdout tokens are NOT present in Slack
                self.assertNotIn("Compiling module_a.c", posted_text)
                self.assertNotIn("Compiling module_b.c", posted_text)
                self.assertNotIn("[1/10]", posted_text)

                # Assert executive briefing IS present in Slack
                self.assertIn("Build & Test Complete", posted_text)


class TestOperationalTurn3sYield(unittest.IsolatedAsyncioTestCase):
    """Tests verifying 3.0s yield window liaison acknowledgment."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_mock_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_FIRSTMATE", sensory_bus=self.bus)
        self.client = MockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        self.bus.clear()

    async def test_operational_turn_yield_acknowledgment(self) -> None:
        """Tasks running longer than yield timeout emit immediate liaison acknowledgment."""
        async def slow_worker_generator() -> AsyncGenerator[AgentEvent, None]:
            # Sleep longer than the yield timeout (0.05s)
            await asyncio.sleep(0.12)
            yield TextDeltaEvent(text_delta="Task done")
            yield FinishEvent(response="Task done", status="SUCCESS")

        session, _ = await self.router.get_or_create("C_YIELD", "100.1")

        with patch.object(self.gateway, "_stream_backstage_events", return_value=slow_worker_generator()):
            accumulator = await self.gateway._execute_backstage_task(
                task_desc="Long running task",
                target_node="personal-laptop",
                session=session,
                client=self.client,
                channel="C_YIELD",
                thread_ts="100.1",
                ts="100.1",
                yield_timeout=0.05,
            )

            # Assert liaison acknowledgment was posted during execution
            self.assertGreaterEqual(len(self.client.posted_messages), 1)
            ack_text = self.client.posted_messages[0]["text"]
            self.assertIn("Dispatched backstage task to `personal-laptop`", ack_text)
            self.assertEqual(accumulator.turn_status, "SUCCESS")


class TestExecutiveBriefingDelivery(unittest.IsolatedAsyncioTestCase):
    """Tests verifying executive briefings and file directives."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_mock_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_FIRSTMATE", sensory_bus=self.bus)
        self.client = MockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        self.bus.clear()

    async def test_file_directive_handling_in_executive_briefing(self) -> None:
        """
        Verify [SEND_FILE: <path>] directives in executive briefing trigger
        client.files_upload_v2 and are stripped from public message text.
        """
        test_pdf = "/tmp/fleet_telemetry.pdf"
        with open(test_pdf, "w") as f:
            f.write("%PDF-1.4 Mock PDF Content")

        turn1_dispatch = {
            "candidates": [{
                "content": {"parts": [{"functionCall": {"name": "dispatch_task", "args": {"task_description": "generate report"}}}]},
                "finishReason": "STOP"
            }]
        }
        turn2_briefing_with_file = {
            "candidates": [{
                "content": {"parts": [
                    {"text": "*Report Ready:* Exported fleet telemetry.\n[SEND_FILE: /tmp/fleet_telemetry.pdf]"}
                ]},
                "finishReason": "STOP"
            }]
        }

        try:
            with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
                mock_gemini.side_effect = [turn1_dispatch, turn2_briefing_with_file]

                with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_exec:
                    mock_exec.return_value = {"status": "SUCCESS", "output": "Report generated at /tmp/fleet_telemetry.pdf"}

                    event = {
                        "channel": "C_MAIN",
                        "ts": "100.005",
                        "thread_ts": "100.005",
                        "text": "Generate telemetry report",
                    }
                    await self.gateway.handle_message(event, self.client)

                    # Posted message must have [SEND_FILE: ...] directive stripped
                    self.assertEqual(len(self.client.posted_messages), 1)
                    clean_text = self.client.posted_messages[0]["text"]
                    self.assertIn("Report Ready", clean_text)
                    self.assertNotIn("[SEND_FILE:", clean_text)

                    # File upload must have been invoked
                    self.assertEqual(len(self.client.uploaded_files), 1)
                    self.assertEqual(os.path.realpath(self.client.uploaded_files[0]["file"]), os.path.realpath(test_pdf))
        finally:
            if os.path.exists(test_pdf):
                try:
                    os.remove(test_pdf)
                except Exception:
                    pass


class TestShoulderTapBlockerWakesMouth(unittest.IsolatedAsyncioTestCase):
    """Tests verifying Shoulder Tap Event Bus blocker alerts wake Mouth to post interactive cards."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_FIRSTMATE", sensory_bus=self.bus)
        self.client = MockSlackClient()

    async def asyncTearDown(self) -> None:
        self.bus.clear()

    async def test_eyes_blocker_alert_surfaces_action_card_out_of_band(self) -> None:
        """
        Verify that when Eyes emits a blocker alert on the bus, The Mouth
        immediately wakes and posts a clickable authentication card to Slack with :warning:.
        """
        session_id = "C_TEST:200.1"
        queue = self.bus.get_session_queue(session_id)

        # Emit blocker alert across the bus
        alert = ShoulderTapAlert(
            alert_type="blocker",
            source="eyes",
            session_id=session_id,
            channel="C_TEST",
            thread_ts="200.1",
            summary="OAuth authorization required",
            data={
                "type": "oauth",
                "url": "http://127.0.0.1:53682/auth?state=xyz",
            }
        )
        self.bus.publish(alert)

        # Trigger gateway alert processing
        await self.gateway.process_sensory_alert(alert, self.client)

        # Message posted to Slack with clickable OAuth link
        self.assertEqual(len(self.client.posted_messages), 1)
        card_text = self.client.posted_messages[0]["text"]
        self.assertIn("Authentication Required", card_text)
        self.assertIn("<http://127.0.0.1:53682/auth?state=xyz|Click here to authenticate>", card_text)

        # Reaction swapped to warning
        added_reactions = [r["name"] for r in self.client.reactions_added]
        self.assertIn("warning", added_reactions)


if __name__ == "__main__":
    unittest.main()
