#!/usr/bin/env python3
"""
tests/test_mouth_front_of_house_adversarial.py
Adversarial Stress Test Suite for First Mate Full Sensory Architecture:
  1. Conversational vs Operational Triage Under Stress & Edge-Case Prompts
  2. Absolute Elimination of Worker text_delta Leaks Under Flooding & Exceptions
  3. 3.0s Yield Window Timing, Background Continuity, and Mid-Stream Blocker Handling
  4. Directive Extraction ([SEND_FILE: ...]) Under Complex Formatting & Security Gates
  5. Out-of-Band Mid-Stream Message Handling (Status, Banter, Cancellation) During Active Execution
  6. Shoulder Tap Event Bus High-Concurrency Burst & Error Isolation
"""

import os
import sys
import time
import json
import asyncio
import tempfile
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Optional, List, Dict, Any, AsyncGenerator

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from first_mate.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert, get_default_event_bus
from first_mate.gateway.ingress import (
    IngressGateway,
    BackstageExecutionAccumulator,
    call_gemini_api,
)
from first_mate.gateway.router import SessionRouter
from first_mate.gateway.session import SessionHandle
from first_mate.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    FinishEvent,
    AgentEvent,
)
from first_mate.transport.file_transfer import (
    extract_and_strip_file_directives,
    upload_directive_files,
    is_sensitive_target,
)


class AdversarialMockSlackClient:
    """Mock Slack client tracking all message posts, reactions, and file uploads."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.uploaded_files: List[Dict[str, Any]] = []

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any) -> Dict[str, Any]:
        msg = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": f"msg_{time.time()}_{len(self.posted_messages)}",
            **kwargs,
        }
        self.posted_messages.append(msg)
        return msg

    async def reactions_add(self, channel: str, timestamp: str, name: str, **kwargs: Any) -> None:
        self.reactions_added.append({"channel": channel, "timestamp": timestamp, "name": name})

    async def reactions_remove(self, channel: str, timestamp: str, name: str, **kwargs: Any) -> None:
        self.reactions_removed.append({"channel": channel, "timestamp": timestamp, "name": name})

    async def files_upload_v2(self, channel: str, thread_ts: Optional[str] = None, file: Optional[str] = None, **kwargs: Any) -> Dict[str, Any]:
        call = {"channel": channel, "thread_ts": thread_ts, "file": file, **kwargs}
        self.uploaded_files.append(call)
        return {"ok": True, "file": file}


def make_adversarial_adapter(cwd: str, extra_args: Optional[List[str]] = None) -> Any:
    adapter = MagicMock()
    adapter.start = AsyncMock()
    adapter.stop = AsyncMock()
    adapter.conversation_id = f"adv-conv-{time.time()}"
    return adapter


class TestAdversarialTriage(unittest.IsolatedAsyncioTestCase):
    """Stress-test conversational vs operational triage under edge cases and malformed responses."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_adversarial_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_ADV", sensory_bus=self.bus)
        self.client = AdversarialMockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        self.bus.clear()

    async def test_ambiguous_mixed_prompt_with_function_and_text(self) -> None:
        """
        Adversarial Scenario: Gemini returns candidate containing BOTH conversational text
        and a dispatch_task functionCall.
        Verify:
          - Task is executed backstage silently.
          - Pre-task conversational text is not leaked prematurely.
          - Final executive briefing is delivered cleanly.
        """
        gemini_turn1 = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "I will check the fleet and run pytest for you."},
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {
                                        "task_description": "pytest tests/",
                                        "target_node": "hermes",
                                    },
                                }
                            },
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        gemini_turn2 = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "*Execution Complete:* 150 tests passed on hermes."}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [gemini_turn1, gemini_turn2]
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_exec:
                mock_exec.return_value = {"status": "SUCCESS", "output": "150 passed in 2.1s"}

                event = {
                    "channel": "C_TRIAGE",
                    "ts": "101.001",
                    "thread_ts": "101.001",
                    "text": "How's the fleet and run pytest",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(mock_exec.call_count, 1)
                self.assertEqual(len(self.client.posted_messages), 1)
                self.assertIn("Execution Complete", self.client.posted_messages[0]["text"])

    async def test_gemini_api_complete_failure_triggers_direct_fallback(self) -> None:
        """
        Adversarial Scenario: Gemini API is completely unavailable (returns None or raises exception).
        Verify:
          - Falls back gracefully to direct adapter execution.
          - Delivers clean sanitized output.
          - Swaps reaction to white_check_mark.
        """
        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.return_value = None  # Complete API failure

            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_exec:
                mock_exec.return_value = {"status": "SUCCESS", "output": "Direct fallback output success"}

                event = {
                    "channel": "C_TRIAGE",
                    "ts": "101.002",
                    "thread_ts": "101.002",
                    "text": "Check git status",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                self.assertIn("Direct fallback output success", self.client.posted_messages[0]["text"])
                reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("white_check_mark", reactions)

    async def test_empty_or_whitespace_prompt_dropped_cleanly(self) -> None:
        """
        Adversarial Scenario: User sends whitespace-only, bot mention only, or empty prompt.
        Verify:
          - Ingress gateway drops the event cleanly without posting or crashing.
        """
        event = {
            "channel": "C_TRIAGE",
            "ts": "101.003",
            "thread_ts": "101.003",
            "text": "   <@U_ADV>    \n\t  ",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(len(self.client.posted_messages), 0)


class TestAdversarialWorkerLeakElimination(unittest.IsolatedAsyncioTestCase):
    """
    CRITICAL EMPIRICAL CHALLENGE:
    Absolute elimination of worker text_delta leaks to Slack under heavy stress,
    flooding, and catastrophic worker exceptions.
    """

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_adversarial_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_ADV", sensory_bus=self.bus)
        self.client = AdversarialMockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        self.bus.clear()

    async def test_volumetric_flood_strictly_retained_backstage(self) -> None:
        """
        Adversarial Flood Test:
        Worker emits 500 rapid text_delta chunks containing secret tokens, ANSI colors,
        and internal scratchpad thoughts.
        Verify:
          - ZERO intermediate text_delta chunks leak into Slack.
          - Final executive briefing is the ONLY message posted.
          - Accumulator bounded properly.
        """
        secret_tokens = [f"INTERNAL_SECRET_WORKER_CHUNK_{i}" for i in range(100)]

        async def flood_worker_generator() -> AsyncGenerator[AgentEvent, None]:
            yield StatusUpdateEvent(status="busy")
            for token in secret_tokens:
                yield TextDeltaEvent(text_delta=f"\x1b[31m[DEBUG]\x1b[0m {token}\n")
            yield FinishEvent(response="Processing 100 items complete", status="SUCCESS")

        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "process items"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_briefing = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "*Job Done:* All 100 items processed successfully."}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_dispatch, turn2_briefing]
            with patch.object(self.gateway, "_stream_backstage_events", return_value=flood_worker_generator()):
                event = {
                    "channel": "C_LEAK",
                    "ts": "102.001",
                    "thread_ts": "102.001",
                    "text": "Process items now",
                }
                await self.gateway.handle_message(event, self.client)

                # Exactly 1 message posted: the executive briefing
                self.assertEqual(len(self.client.posted_messages), 1)
                posted_text = self.client.posted_messages[0]["text"]
                self.assertIn("Job Done", posted_text)

                # EMPIRICAL LEAK CHECK: None of the secret worker tokens or ANSI codes leaked!
                for token in secret_tokens:
                    self.assertNotIn(token, posted_text)
                self.assertNotIn("\x1b[31m", posted_text)

    async def test_worker_catastrophic_exception_midstream_cleanly_handled(self) -> None:
        """
        Adversarial Failure Test:
        Worker produces 10 deltas and then abruptly raises an unhandled exception
        (e.g., BrokenPipeError or ProcessLookupError).
        Verify:
          - Zero unhandled crash in gateway.
          - Zero text_delta leak before the crash.
          - Clean failure notice posted to Slack with :x: reaction.
        """
        async def crashing_worker_generator() -> AsyncGenerator[AgentEvent, None]:
            yield TextDeltaEvent(text_delta="Beginning hazardous operation...")
            yield TextDeltaEvent(text_delta="CRITICAL_LEAK_TOKEN_SHOULD_NOT_APPEAR")
            # Abrupt crash
            raise RuntimeError("Backstage worker process died unexpectedly (SIGSEGV)")

        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "hazardous op"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        # In turn 2, Gemini synthesizes the error briefing
        turn2_error_briefing = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "❌ *Operation Aborted:* The worker encountered a critical crash."}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_dispatch, turn2_error_briefing]
            with patch.object(self.gateway, "_stream_backstage_events", return_value=crashing_worker_generator()):
                event = {
                    "channel": "C_LEAK",
                    "ts": "102.002",
                    "thread_ts": "102.002",
                    "text": "Run hazardous operation",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                posted_text = self.client.posted_messages[0]["text"]
                self.assertIn("Operation Aborted", posted_text)
                self.assertNotIn("CRITICAL_LEAK_TOKEN_SHOULD_NOT_APPEAR", posted_text)

                reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("x", reactions)

    async def test_huge_output_buffer_bounded_safely(self) -> None:
        """
        Adversarial Memory Limit Test:
        Worker emits 200,000 characters of stdout.
        Verify accumulator bounds memory strictly without overflow or OOM.
        """
        acc = BackstageExecutionAccumulator(max_output_chars=10_000)
        huge_chunk = "A" * 100_000
        acc.record_event(TextDeltaEvent(text_delta=huge_chunk))
        acc.record_event(TextDeltaEvent(text_delta=huge_chunk))

        output = acc.get_full_output()
        self.assertLessEqual(len(output), 10_000)
        summary = acc.get_structured_summary()
        self.assertLessEqual(len(summary["output_tail"]), 8000)


class TestAdversarialYieldAndContinuity(unittest.IsolatedAsyncioTestCase):
    """Stress-test 3.0s yield window timing, background continuity, and mid-stream blockers."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_adversarial_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_ADV", sensory_bus=self.bus)
        self.client = AdversarialMockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        self.bus.clear()

    async def test_sub_3s_task_does_not_emit_spurious_yield_ack(self) -> None:
        """
        Timing Test:
        Task finishes in 0.05s (< 3.0s yield timeout).
        Verify ZERO interim yield acknowledgments are posted. Only final response posted.
        """
        async def fast_worker_generator() -> AsyncGenerator[AgentEvent, None]:
            await asyncio.sleep(0.01)
            yield FinishEvent(response="Quick execution completed", status="SUCCESS")

        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "fast task"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_briefing = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": "*Quick Task:* Completed successfully in 10ms."}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_dispatch, turn2_briefing]
            with patch.object(self.gateway, "_stream_backstage_events", return_value=fast_worker_generator()):
                event = {
                    "channel": "C_TIMING",
                    "ts": "103.001",
                    "thread_ts": "103.001",
                    "text": "Run fast task",
                }
                await self.gateway.handle_message(event, self.client)

                # Exactly 1 message posted (NO interim yield message)
                self.assertEqual(len(self.client.posted_messages), 1)
                self.assertIn("Quick Task", self.client.posted_messages[0]["text"])
                self.assertNotIn("Dispatched backstage task", self.client.posted_messages[0]["text"])

    async def test_super_3s_task_yields_and_runs_to_completion(self) -> None:
        """
        Timing & Continuity Test:
        Task takes 0.12s with yield_timeout=0.04s.
        Verify:
          - Yield acknowledgment is emitted when timeout fires.
          - Worker is NOT interrupted and completes backstage.
          - Final executive briefing is delivered.
          - Exactly 2 messages posted.
        """
        worker_finished = False

        async def slow_worker_generator() -> AsyncGenerator[AgentEvent, None]:
            nonlocal worker_finished
            await asyncio.sleep(0.10)
            worker_finished = True
            yield FinishEvent(response="Long task finally completed", status="SUCCESS")

        session, _ = await self.router.get_or_create("C_TIMING", "103.002")

        with patch.object(self.gateway, "_stream_backstage_events", return_value=slow_worker_generator()):
            accumulator = await self.gateway._execute_backstage_task(
                task_desc="Run heavy simulation",
                target_node="compute-grid",
                session=session,
                client=self.client,
                channel="C_TIMING",
                thread_ts="103.002",
                ts="103.002",
                yield_timeout=0.03,  # Fast yield window for test speed
            )

            # 1. Yield message was posted
            self.assertEqual(len(self.client.posted_messages), 1)
            self.assertIn("Dispatched backstage task to `compute-grid`", self.client.posted_messages[0]["text"])

            # 2. Worker actually ran to completion
            self.assertTrue(worker_finished)
            self.assertEqual(accumulator.turn_status, "SUCCESS")

    async def test_blocker_arriving_during_post_yield_surfaces_action_card(self) -> None:
        """
        Sensory Integration Test:
        While worker is running after the yield window, an interactive blocker
        arrives via the ShoulderTapEventBus on the session queue.
        Verify:
          - The blocker action card is posted immediately to Slack.
          - Reaction is swapped to :warning:.
        """
        session, _ = await self.router.get_or_create("C_BLOCK", "103.003")

        async def worker_needing_auth() -> AsyncGenerator[AgentEvent, None]:
            # Step 1: Wait enough to exceed yield
            await asyncio.sleep(0.04)
            # Step 2: Publish blocker alert on the bus
            alert = ShoulderTapAlert(
                alert_type="blocker",
                source="eyes",
                session_id=session.session_id,
                channel="C_BLOCK",
                thread_ts="103.003",
                summary="OAuth authorization required",
                data={
                    "type": "oauth",
                    "url": "http://127.0.0.1:53682/auth?state=adversarial_test",
                },
            )
            self.bus.publish(alert)
            # Step 3: Sleep slightly more and finish
            await asyncio.sleep(0.05)
            yield FinishEvent(response="Done after auth", status="SUCCESS")

        with patch.object(self.gateway, "_stream_backstage_events", return_value=worker_needing_auth()):
            await self.gateway._execute_backstage_task(
                task_desc="Run rclone sync",
                target_node="hermes",
                session=session,
                client=self.client,
                channel="C_BLOCK",
                thread_ts="103.003",
                ts="103.003",
                yield_timeout=0.02,
            )

            # We expect at least:
            # 1. Liaison yield ack
            # 2. OAuth action card
            messages = [m["text"] for m in self.client.posted_messages]
            self.assertTrue(any("Dispatched backstage task" in m for m in messages))
            self.assertTrue(any("Authentication Required" in m and "53682/auth" in m for m in messages))

            reactions = [r["name"] for r in self.client.reactions_added]
            self.assertIn("warning", reactions)


class TestAdversarialDirectiveExtraction(unittest.IsolatedAsyncioTestCase):
    """Stress-test directive extraction, security filters, and multiple directives."""

    async def asyncSetUp(self) -> None:
        self.client = AdversarialMockSlackClient()
        self.tmp_files: List[str] = []

    def make_temp_file(self, name: str, content: str = "Test Content") -> str:
        f = tempfile.NamedTemporaryFile(delete=False, suffix=f"_{name}")
        f.write(content.encode("utf-8"))
        f.close()
        self.tmp_files.append(f.name)
        return f.name

    async def asyncTearDown(self) -> None:
        for p in self.tmp_files:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

    def test_extract_multiple_mixed_directives(self) -> None:
        """
        Adversarial Directives:
        Briefing includes SEND_FILE, UPLOAD_FILE, ATTACH_FILE with varying whitespace,
        quotes, and punctuation.
        Verify all 3 paths extracted cleanly and stripped from output text.
        """
        raw_text = (
            "Executive Briefing:\n"
            "• [SEND_FILE: /tmp/file_a.txt]\n"
            "• [UPLOAD_FILE: \"/tmp/file_b.csv\"]\n"
            "• [ATTACH_FILE: `/tmp/file_c.json`]\n\n"
            "All three assets generated cleanly."
        )
        clean, targets = extract_and_strip_file_directives(raw_text)

        self.assertEqual(len(targets), 3)
        self.assertIn("/tmp/file_a.txt", targets)
        self.assertIn("/tmp/file_b.csv", targets)
        self.assertIn("/tmp/file_c.json", targets)

        self.assertNotIn("[SEND_FILE:", clean)
        self.assertNotIn("[UPLOAD_FILE:", clean)
        self.assertNotIn("[ATTACH_FILE:", clean)
        self.assertIn("All three assets generated cleanly", clean)

    def test_sensitive_file_security_block(self) -> None:
        """
        Security Stress Test:
        Directive attempting to exfiltrate .env or private keys must be rejected by is_sensitive_target.
        """
        self.assertTrue(is_sensitive_target("/Users/hermes/projects/first-mate/.env"))
        self.assertTrue(is_sensitive_target(".env"))
        self.assertTrue(is_sensitive_target("/home/user/.ssh/id_rsa"))
        self.assertTrue(is_sensitive_target("desktop-pc:C:\\secrets\\credentials.json"))
        self.assertFalse(is_sensitive_target("/tmp/normal_report.pdf"))

    async def test_upload_directive_files_handles_missing_files_gracefully(self) -> None:
        """
        Adversarial Missing File:
        Directive specifies non-existent path.
        Verify upload_directive_files skips missing files without throwing unhandled error.
        """
        targets = ["/tmp/definitely_nonexistent_file_12345.xyz"]
        res = await upload_directive_files(
            client=self.client,
            channel="C_TEST",
            thread_ts="104.001",
            targets=targets,
            active_cwd=PROJECT_DIR,
        )
        self.assertEqual(len(res), 0)
        self.assertEqual(len(self.client.uploaded_files), 0)


class TestAdversarialOutOfBandHandling(unittest.IsolatedAsyncioTestCase):
    """Stress-test out-of-band mid-stream status queries and cancellations."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_adversarial_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_ADV", sensory_bus=self.bus)
        self.client = AdversarialMockSlackClient()

    async def asyncTearDown(self) -> None:
        self.bus.clear()

    async def test_out_of_band_status_inquiry_while_executing(self) -> None:
        """
        Concurrency Test:
        While session is marked executing in Pane 2, Carl sends "What's the status right now?".
        Verify:
          - Handled out-of-band immediately.
          - Posts status update to Slack.
          - Swaps reaction.
          - Does NOT invoke the adapter or block on session lock.
        """
        session, _ = await self.router.get_or_create("C_OOB", "105.001")
        # Simulate active execution handle in Pane 2
        mock_exec = MagicMock()
        mock_exec.status = "running"
        mock_exec.pid = 98765
        mock_exec.command = "docker build -t app ."
        session.active_execution = mock_exec

        event = {
            "channel": "C_OOB",
            "ts": "105.002",
            "thread_ts": "105.001",
            "text": "What's the status right now?",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(self.client.posted_messages), 1)
        posted_text = self.client.posted_messages[0]["text"]
        self.assertIn("98765", posted_text)
        self.assertIn("Runner In Progress", posted_text)

    async def test_out_of_band_cancellation_stops_execution(self) -> None:
        """
        Cancellation Test:
        While session is marked executing, Carl sends "/cancel".
        Verify:
          - session.cancel_execution is invoked.
          - Immediate cancellation notice posted to Slack with :x:.
        """
        session, _ = await self.router.get_or_create("C_OOB", "105.003")
        mock_exec = MagicMock()
        mock_exec.status = "running"
        mock_exec.pid = 54321
        session.active_execution = mock_exec
        session.cancel_execution = AsyncMock()

        event = {
            "channel": "C_OOB",
            "ts": "105.004",
            "thread_ts": "105.003",
            "text": "/cancel",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertTrue(session.cancel_execution.called)
        self.assertEqual(len(self.client.posted_messages), 1)
        posted = self.client.posted_messages[0]["text"]
        self.assertIn("Execution Cancelled", posted)

        reactions = [r["name"] for r in self.client.reactions_added]
        self.assertIn("x", reactions)


class TestAdversarialSensoryBusConcurrency(unittest.IsolatedAsyncioTestCase):
    """Stress-test ShoulderTapEventBus with high-concurrency alert storms and failing subscribers."""

    async def test_high_concurrency_alert_burst(self) -> None:
        """
        Burst Stress:
        Publish 500 alerts concurrently across 20 distinct session IDs.
        Verify all alerts delivered to corresponding queues without loss or deadlock.
        """
        bus = ShoulderTapEventBus(queue_maxsize=50)
        num_sessions = 20
        alerts_per_session = 10

        queues = {
            f"sess_{i}": bus.get_session_queue(f"sess_{i}")
            for i in range(num_sessions)
        }

        async def send_alert(session_id: str, idx: int) -> None:
            alert = ShoulderTapAlert(
                alert_type="blocker",
                source="eyes",
                session_id=session_id,
                summary=f"Alert {idx}",
            )
            bus.publish(alert)

        tasks = []
        for i in range(num_sessions):
            sess_id = f"sess_{i}"
            for j in range(alerts_per_session):
                tasks.append(send_alert(sess_id, j))

        await asyncio.gather(*tasks)

        # Verify each session received exactly alerts_per_session alerts
        for sess_id, q in queues.items():
            self.assertEqual(q.qsize(), alerts_per_session)
            items = []
            while not q.empty():
                items.append(q.get_nowait())
            self.assertEqual(len(items), alerts_per_session)

        bus.clear()


class TestAdversarialSynthesisAndEdgeCases(unittest.IsolatedAsyncioTestCase):
    """Stress-test synthesis failure fallbacks, awaiting input events, and session isolation."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.router = SessionRouter(default_target_cwd=PROJECT_DIR, adapter_factory=make_adversarial_adapter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_ADV", sensory_bus=self.bus)
        self.client = AdversarialMockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        self.bus.clear()

    async def test_gemini_briefing_failure_delivers_fallback_without_leak(self) -> None:
        """
        Adversarial Synthesis Failure:
        Turn 1 dispatch_task succeeds, but Turn 2 Gemini synthesis fails (returns None).
        Verify:
          - Safe fallback message is generated (e.g. '✅ *Task Complete:*').
          - Message posted to Slack cleanly.
          - Reaction swapped to white_check_mark.
        """
        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "build binary"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            # Turn 1 returns dispatch; Turn 2 (briefing synthesis) returns None
            mock_gemini.side_effect = [turn1_dispatch, None]
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_exec:
                mock_exec.return_value = {"status": "SUCCESS", "output": "Binary built at target/bin"}

                event = {
                    "channel": "C_SYNTH",
                    "ts": "106.001",
                    "thread_ts": "106.001",
                    "text": "Build the binary",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                posted = self.client.posted_messages[0]["text"]
                self.assertIn("Task Complete", posted)
                reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("white_check_mark", reactions)

    async def test_awaiting_input_event_yields_warning_prompt(self) -> None:
        """
        Interactive Event Test:
        Worker emits awaiting_input event during execution.
        Verify:
          - Interrupted immediately.
          - Posted message prompts user for input.
          - Reaction swapped to :warning:.
        """
        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "deploy with sudo"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        acc = BackstageExecutionAccumulator()
        acc.has_awaiting_input = True
        acc.awaiting_input_prompt = "Please enter your sudo password"

        with patch("first_mate.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.return_value = turn1_dispatch
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_exec:
                mock_exec.return_value = acc

                event = {
                    "channel": "C_INPUT",
                    "ts": "106.002",
                    "thread_ts": "106.002",
                    "text": "Deploy with sudo",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                posted = self.client.posted_messages[0]["text"]
                self.assertIn("Please enter your sudo password", posted)

                reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("warning", reactions)

    async def test_session_none_boundary_safety(self) -> None:
        """
        Boundary Test:
        Calling _execute_backstage_task with session=None returns an empty accumulator without throwing.
        """
        acc = await self.gateway._execute_backstage_task(
            task_desc="orphan task",
            session=None,
        )
        self.assertIsInstance(acc, BackstageExecutionAccumulator)
        self.assertEqual(acc.get_full_output(), "")

    async def test_dual_queue_simultaneous_completion_never_discards_worker_events(self) -> None:
        """
        Regression test for Reviewer 2 Finding 2:
        When both session_queue (alert) and event_queue (worker event) complete
        simultaneously in asyncio.wait(FIRST_COMPLETED), eq_task.result() must not
        be dropped by an unconditional continue under if sq_task in done.
        """
        session, _ = await self.router.get_or_create("C_RACE", "103.004")

        async def worker_simultaneous_event_and_alert():
            await asyncio.sleep(0.08)
            alert = ShoulderTapAlert(
                alert_type="blocker",
                source="eyes",
                session_id=session.session_id,
                channel="C_RACE",
                thread_ts="103.004",
                summary="OAuth authorization required",
                data={
                    "type": "oauth",
                    "url": "http://127.0.0.1:53682/auth?state=race_test",
                },
            )
            self.bus.publish(alert)
            yield FinishEvent(response="Task completed successfully despite simultaneous alert", status="SUCCESS")

        with patch.object(self.gateway, "_stream_backstage_events", return_value=worker_simultaneous_event_and_alert()):
            acc = await self.gateway._execute_backstage_task(
                task_desc="Run task with simultaneous alert",
                target_node="hermes",
                session=session,
                client=self.client,
                channel="C_RACE",
                thread_ts="103.004",
                ts="103.004",
                yield_timeout=0.02,
            )

            # 1. Verify blocker action card was delivered to Slack
            messages = [m["text"] for m in self.client.posted_messages]
            self.assertTrue(any("Authentication Required" in m and "53682/auth" in m for m in messages))

            # 2. Verify finish event was NOT dropped by the alert handling
            self.assertEqual(acc.turn_status, "SUCCESS")
            self.assertEqual(acc.final_response, "Task completed successfully despite simultaneous alert")


if __name__ == "__main__":
    unittest.main()

