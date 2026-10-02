#!/usr/bin/env python3
"""
tests/test_conversational_concurrency.py
Comprehensive test suite for Milestone 3: Conversational Concurrency & Gateway Lock Decoupling.
Validates:
  - TC-03: Incoming user messages sent while Pane 2 is executing are processed immediately
    by the liaison without blocking behind session.lock.
  - Out-of-band status queries return runner buffer and extracted OAuth/prompt blockers immediately.
  - Out-of-band cancellation cancels background command execution immediately.
  - Thread context and normal turn serialization for sequential turns is preserved when not executing in background.
  - Edge cases, rapid concurrent status queries, and clean teardown.
"""

import os
import sys
import time
import asyncio
import unittest
from typing import Optional, List, Dict, Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from first_mate.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    AgentEvent,
)
from first_mate.execution.runner import ExecutionHandle
from first_mate.execution.buffer import RunnerBufferObserver
from first_mate.gateway.session import (
    SessionHandle,
    SessionState,
    is_cancellation_message,
    is_status_inquiry,
    is_conversational_checkin,
    is_out_of_band_message,
)
from first_mate.gateway.router import SessionRouter
from first_mate.gateway.ingress import IngressGateway


class MockSlowAdapter:
    """Mock adapter simulating a turn that holds the session lock for a designated duration."""

    def __init__(self, delay: float = 0.5) -> None:
        self.delay = delay
        self.turns_received: List[str] = []
        self.is_running = False

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_received.append(prompt)
        self.is_running = True
        try:
            yield StatusUpdateEvent(status="working")
            if self.delay > 0:
                await asyncio.sleep(self.delay)
            yield TextDeltaEvent(text_delta=f"Finished: {prompt}")
            yield FinishEvent(response=f"Finished: {prompt}", status="SUCCESS")
        finally:
            self.is_running = False

    async def stop(self) -> None:
        pass


class MockSlackClient:
    """Mock client recording chat_postMessage, reactions_add, and reactions_remove."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None) -> Dict[str, Any]:
        msg = {"channel": channel, "text": text, "thread_ts": thread_ts, "ts": f"msg_{time.time()}"}
        self.posted_messages.append(msg)
        return msg

    async def reactions_add(self, channel: str, timestamp: str, name: str) -> None:
        self.reactions_added.append({"channel": channel, "timestamp": timestamp, "name": name})

    async def reactions_remove(self, channel: str, timestamp: str, name: str) -> None:
        self.reactions_removed.append({"channel": channel, "timestamp": timestamp, "name": name})


class TestConversationalConcurrency(unittest.IsolatedAsyncioTestCase):
    """Test suite for lock decoupling and conversational concurrency."""

    async def asyncSetUp(self) -> None:
        self.mock_adapter = MockSlowAdapter(delay=0.4)
        self.buffer_observer = MagicMock(spec=RunnerBufferObserver)
        self.buffer_observer.read_buffer = AsyncMock(return_value="Default idle buffer")
        self.buffer_observer.extract_all_blockers = MagicMock(return_value=[])
        self.buffer_observer.extract_interactive_blockers = MagicMock(return_value=None)

        self.session = SessionHandle(
            session_id="test_channel:100.1",
            channel="test_channel",
            thread_ts="100.1",
            adapter=self.mock_adapter,
            buffer_observer=self.buffer_observer,
        )
        self.router = SessionRouter(adapter_factory=lambda cwd: self.mock_adapter)
        # Pre-seed session in router
        self.router.sessions[self.session.session_id] = self.session
        self.gateway = IngressGateway(router=self.router)
        self.client = MockSlackClient()

    async def asyncTearDown(self) -> None:
        await self.session.close()

    # =========================================================================
    # 1. Intent Detection Tests
    # =========================================================================

    def test_intent_detection_cancellation(self) -> None:
        """Verify cancellation phrases are correctly identified."""
        self.assertTrue(is_cancellation_message("cancel"))
        self.assertTrue(is_cancellation_message("stop"))
        self.assertTrue(is_cancellation_message("abort"))
        self.assertTrue(is_cancellation_message("/cancel"))
        self.assertTrue(is_cancellation_message("!stop"))
        self.assertTrue(is_cancellation_message("Please cancel that execution"))
        self.assertTrue(is_cancellation_message("can you cancel that?"))
        self.assertTrue(is_cancellation_message("could you stop this?"))
        self.assertTrue(is_cancellation_message("please cancel"))
        self.assertTrue(is_cancellation_message("could you please cancel that?"))
        self.assertTrue(is_cancellation_message("can you stop this?"))
        self.assertTrue(is_out_of_band_message("can you cancel that?"))
        self.assertTrue(is_out_of_band_message("cancel"))
        self.assertFalse(is_cancellation_message("Write a script to cancel reservations"))
        self.assertFalse(is_cancellation_message("Explain why the task was cancelled"))
        self.assertFalse(is_cancellation_message("What causes an aborted connection in MySQL?"))

    def test_intent_detection_status(self) -> None:
        """Verify status inquiry phrases are correctly identified."""
        self.assertTrue(is_status_inquiry("status"))
        self.assertTrue(is_status_inquiry("what's the url?"))
        self.assertTrue(is_status_inquiry("what is the url"))
        self.assertTrue(is_status_inquiry("how is it going?"))
        self.assertTrue(is_status_inquiry("how's it going"))
        self.assertTrue(is_status_inquiry("progress"))
        self.assertTrue(is_status_inquiry("check status"))
        self.assertTrue(is_out_of_band_message("status"))
        self.assertFalse(is_status_inquiry("Build a web server"))

    def test_intent_detection_checkin(self) -> None:
        """Verify conversational check-in phrases are correctly identified."""
        self.assertTrue(is_conversational_checkin("are you still there?"))
        self.assertTrue(is_conversational_checkin("ping"))
        self.assertTrue(is_conversational_checkin("any updates?"))
        self.assertTrue(is_conversational_checkin("still running?"))
        self.assertTrue(is_out_of_band_message("ping"))
        self.assertTrue(is_out_of_band_message("Can I ask you anything mid stream like this?"))
        self.assertTrue(is_out_of_band_message("Can I ask a quick question?"))
        self.assertFalse(is_conversational_checkin("Deploy the container"))

    # =========================================================================
    # 2. TC-03: Concurrency & Lock Decoupling
    # =========================================================================

    async def test_tc03_incoming_status_while_pane2_executing_processed_immediately(self) -> None:
        """
        TC-03: When Pane 2 is executing a background command, an incoming user status message
        is processed immediately out-of-band by the liaison without blocking behind session.lock.
        """
        # Set session as actively executing in Pane 2
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="make test-heavy",
            pid=7788,
            message="Running make test-heavy in Pane 2",
        )
        self.assertTrue(self.session.is_executing)
        self.assertTrue(self.session.is_busy)

        self.buffer_observer.read_buffer.return_value = "Compiling modules... 60%\nTest suite running"

        t_start = time.time()
        event = {
            "channel": "test_channel",
            "thread_ts": "100.1",
            "ts": "100.2",
            "text": "How is it going?",
        }
        await self.gateway.handle_message(event, self.client)
        elapsed = time.time() - t_start

        # Must execute within < 0.2s without waiting for any delay
        self.assertLess(elapsed, 0.25)
        self.assertGreaterEqual(len(self.client.posted_messages), 1)
        last_msg = self.client.posted_messages[-1]["text"]
        self.assertIn("Runner In Progress", last_msg)
        self.assertIn("PID 7788", last_msg)

    async def test_tc03_send_user_message_bypasses_lock_when_busy(self) -> None:
        """
        Verify session.send_user_message() processes out-of-band check-ins immediately
        even when session.lock is currently locked by an ongoing turn.
        """
        # Artificially acquire session.lock as if Turn 1 is executing
        await self.session.lock.acquire()
        self.assertTrue(self.session.lock.locked())

        # Set active execution
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="sleep 100",
            pid=9911,
        )

        try:
            t_start = time.time()
            events: List[AgentEvent] = []
            async for ev in self.session.send_user_message("status"):
                events.append(ev)
            elapsed = time.time() - t_start

            # Must return immediately (< 0.15s) despite lock being held
            self.assertLess(elapsed, 0.15)
            self.assertGreaterEqual(len(events), 2)
            # Find finish event
            finish_ev = next(e for e in events if getattr(e, "event_type", None) == "finish")
            self.assertIn("Runner In Progress", finish_ev.response)
            self.assertIn("PID 9911", finish_ev.response)

            # Lock must STILL be locked by the original holder!
            self.assertTrue(self.session.lock.locked())
        finally:
            if self.session.lock.locked():
                self.session.lock.release()

    async def test_natural_midstream_inquiry_while_pane2_executing_processed_immediately(self) -> None:
        """
        Verify natural mid-stream inquiry ('Can I ask you anything mid stream like this?')
        is processed immediately out-of-band by the liaison without blocking behind session.lock.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="python3 generate_dossiers.py",
            pid=8844,
            message="Running Playwright automation in Pane 2",
        )
        self.assertTrue(self.session.is_executing)
        self.assertTrue(self.session.is_busy)

        t_start = time.time()
        event = {
            "channel": "test_channel",
            "thread_ts": "100.1",
            "ts": "100.2",
            "text": "Can I ask you anything mid stream like this?",
        }
        await self.gateway.handle_message(event, self.client)
        elapsed = time.time() - t_start

        # Must execute immediately (< 0.25s) without waiting behind lock
        self.assertLess(elapsed, 0.25)
        self.assertGreaterEqual(len(self.client.posted_messages), 1)
        last_msg = self.client.posted_messages[-1]["text"]
        self.assertIn("Liaison Update", last_msg)
        self.assertIn("PID 8844", last_msg)

    async def test_natural_midstream_inquiry_bypasses_lock_when_busy(self) -> None:
        """
        Verify session.send_user_message('Can I ask you anything mid stream like this?')
        bypasses session.lock immediately when lock is held by an ongoing turn.
        """
        await self.session.lock.acquire()
        self.assertTrue(self.session.lock.locked())

        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="playwright test",
            pid=7766,
        )

        try:
            t_start = time.time()
            events: List[AgentEvent] = []
            async for ev in self.session.send_user_message("Can I ask you anything mid stream like this?"):
                events.append(ev)
            elapsed = time.time() - t_start

            self.assertLess(elapsed, 0.15)
            self.assertGreaterEqual(len(events), 2)
            finish_ev = next(e for e in events if getattr(e, "event_type", None) == "finish")
            self.assertIn("Liaison Update", finish_ev.response)
            self.assertIn("PID 7766", finish_ev.response)
        finally:
            if self.session.lock.locked():
                self.session.lock.release()

    async def test_natural_conversational_statement_without_question_mark_or_starter_processed_immediately(self) -> None:
        """
        Verify natural conversational statements that do not contain '?' or standard question starters
        (e.g. 'I was wondering if the podcast scraper is almost done', 'Just checking in on things')
        are processed immediately out-of-band by the liaison without blocking behind session.lock.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="python3 generate_dossiers.py",
            pid=8844,
            message="Running Playwright automation in Pane 2",
        )
        self.assertTrue(self.session.is_executing)
        self.assertTrue(self.session.is_busy)

        test_phrases = [
            "I was wondering if the podcast scraper is almost done",
            "Let me know if you run into any issues",
            "Just checking in on things",
            "FYI I need this in 10 minutes",
            "Looks like it is taking a while",
        ]
        for phrase in test_phrases:
            with self.subTest(phrase=phrase):
                t_start = time.time()
                event = {
                    "channel": "test_channel",
                    "thread_ts": "100.1",
                    "ts": f"100.{time.time()}",
                    "text": phrase,
                }
                await self.gateway.handle_message(event, self.client)
                elapsed = time.time() - t_start

                self.assertLess(elapsed, 0.25)
                self.assertGreaterEqual(len(self.client.posted_messages), 1)
                last_msg = self.client.posted_messages[-1]["text"]
                self.assertIn("Liaison Update", last_msg)
                self.assertIn("PID 8844", last_msg)

    # =========================================================================
    # 3. Out-of-Band Blocker Extraction (OAuth & Prompts)
    # =========================================================================

    async def test_out_of_band_status_extracts_oauth_blocker(self) -> None:
        """
        Verify status query extracts OAuth callback URL from runner buffer immediately,
        swaps reaction to :warning:, and formats Slack message with clickable link.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="rclone config reconnect",
            pid=4455,
        )
        oauth_buf = (
            "2026/10/01 02:00:00 NOTICE: Log in to your browser\n"
            "Redirecting to: http://127.0.0.1:53682/auth?state=xyz987\n"
            "Waiting for code..."
        )
        self.buffer_observer.read_buffer.return_value = oauth_buf
        self.buffer_observer.extract_all_blockers.return_value = [{
            "type": "oauth",
            "content": "Redirecting to: http://127.0.0.1:53682/auth?state=xyz987",
            "url": "http://127.0.0.1:53682/auth?state=xyz987"
        }]

        event = {
            "channel": "test_channel",
            "thread_ts": "100.1",
            "ts": "100.3",
            "text": "what's the url?",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertGreaterEqual(len(self.client.posted_messages), 1)
        reply = self.client.posted_messages[-1]["text"]
        self.assertIn("Authentication Required", reply)
        self.assertIn("<http://127.0.0.1:53682/auth?state=xyz987|Click here to authenticate>", reply)

        # Verify warning reaction swapped
        added_reactions = [r["name"] for r in self.client.reactions_added]
        self.assertIn("warning", added_reactions)

    async def test_out_of_band_status_extracts_confirmation_prompt(self) -> None:
        """
        Verify status query extracts interactive confirmation prompt from runner buffer,
        swaps reaction to :warning:, and notifies the user.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="rm -rf /tmp/data",
            pid=5566,
        )
        prompt_buf = "Warning: 10 files will be permanently deleted.\nDo you want to proceed? [y/N]"
        self.buffer_observer.read_buffer.return_value = prompt_buf
        self.buffer_observer.extract_all_blockers.return_value = [{
            "type": "prompt",
            "content": "Do you want to proceed? [y/N]",
            "url": None
        }]

        event = {
            "channel": "test_channel",
            "thread_ts": "100.1",
            "ts": "100.4",
            "text": "status",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertGreaterEqual(len(self.client.posted_messages), 1)
        reply = self.client.posted_messages[-1]["text"]
        self.assertIn("Confirmation Required", reply)
        self.assertIn("[y/N]", reply)

        added_reactions = [r["name"] for r in self.client.reactions_added]
        self.assertIn("warning", added_reactions)

    async def test_out_of_band_status_when_idle(self) -> None:
        """Verify status inquiry when runner is idle returns clean status digest."""
        self.session.active_execution = None
        self.buffer_observer.read_buffer.return_value = "zsh$ "

        # Call get_runner_status directly
        status_info = await self.session.get_runner_status()
        self.assertEqual(status_info["status"], "idle")
        self.assertEqual(status_info["blockers"], [])

    # =========================================================================
    # 4. Out-of-Band Cancellation
    # =========================================================================

    async def test_out_of_band_cancellation_cancels_runner_and_resets_state(self) -> None:
        """
        Verify out-of-band cancellation message interrupts Pane 2, resets active_execution,
        swaps reaction to :x:, and delivers cancellation confirmation immediately.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="python3 long_train.py",
            pid=6677,
        )
        self.assertTrue(self.session.is_executing)

        with patch("first_mate.gateway.session.os.kill") as mock_kill:
            event = {
                "channel": "test_channel",
                "thread_ts": "100.1",
                "ts": "100.5",
                "text": "cancel",
            }
            t_start = time.time()
            await self.gateway.handle_message(event, self.client)
            elapsed = time.time() - t_start

            self.assertLess(elapsed, 0.2)
            # Active execution must be reset
            self.assertIsNone(self.session.active_execution)
            self.assertFalse(self.session.is_executing)

            # Signal sent to PID
            mock_kill.assert_called_once()

            # Confirmation message posted
            self.assertGreaterEqual(len(self.client.posted_messages), 1)
            reply = self.client.posted_messages[-1]["text"]
            self.assertIn("Execution Cancelled", reply)

            # Reaction swapped to :x:
            added_reactions = [r["name"] for r in self.client.reactions_added]
            self.assertIn("x", added_reactions)

    async def test_cancellation_via_send_user_message_while_lock_held(self) -> None:
        """Verify session.send_user_message('stop') cancels execution out-of-band when lock is held."""
        await self.session.lock.acquire()
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="docker build .",
            pid=8899,
        )

        try:
            with patch("first_mate.gateway.session.os.kill") as mock_kill:
                events: List[AgentEvent] = []
                async for ev in self.session.send_user_message("stop"):
                    events.append(ev)

                self.assertIsNone(self.session.active_execution)
                mock_kill.assert_called_once()
                finish_ev = next(e for e in events if getattr(e, "event_type", None) == "finish")
                self.assertIn("Execution Cancelled", finish_ev.response)
        finally:
            if self.session.lock.locked():
                self.session.lock.release()

    # =========================================================================
    # 5. Thread Context & Sequential Turn Serialization
    # =========================================================================

    async def test_sequential_turns_preserve_lock_serialization(self) -> None:
        """
        Verify that sequential turns (when not executing in background and not out-of-band)
        are strictly serialized under session.lock, incrementing turn count and preserving context.
        """
        self.session.active_execution = None

        # Turn 1
        gen1 = self.session.send_user_message("First turn: check repo")
        events1 = [ev async for ev in gen1]
        self.assertEqual(self.session.current_turn, 1)
        self.assertEqual(self.session.last_prompt, "First turn: check repo")
        self.assertEqual(self.session.state, SessionState.IDLE)
        self.assertIn("Finished: First turn: check repo", events1[-1].response)

        # Turn 2
        gen2 = self.session.send_user_message("Second turn: summarize changes")
        events2 = [ev async for ev in gen2]
        self.assertEqual(self.session.current_turn, 2)
        self.assertEqual(self.session.last_prompt, "Second turn: summarize changes")
        self.assertEqual(self.session.state, SessionState.IDLE)
        self.assertIn("Finished: Second turn: summarize changes", events2[-1].response)

    async def test_concurrent_normal_turns_serialize_in_order(self) -> None:
        """
        Verify that two concurrent normal (non-out-of-band) turns do not corrupt state;
        Turn 2 waits for Turn 1 to release session.lock before executing.
        """
        self.session.active_execution = None
        self.mock_adapter.delay = 0.15

        start_time = time.time()
        task1 = asyncio.create_task(
            self.gateway.handle_message(
                {"channel": "test_channel", "thread_ts": "100.1", "ts": "100.10", "text": "Turn A"},
                self.client,
            )
        )
        # Give task1 a tiny slice to acquire lock
        await asyncio.sleep(0.02)
        task2 = asyncio.create_task(
            self.gateway.handle_message(
                {"channel": "test_channel", "thread_ts": "100.1", "ts": "100.11", "text": "Turn B"},
                self.client,
            )
        )

        await asyncio.gather(task1, task2)
        total_time = time.time() - start_time

        # Both turns took ~0.15s each serialized -> total >= 0.25s
        self.assertGreaterEqual(total_time, 0.25)
        self.assertEqual(self.session.current_turn, 2)
        self.assertEqual(len(self.mock_adapter.turns_received), 2)
        self.assertEqual(self.mock_adapter.turns_received, ["Turn A", "Turn B"])

    # =========================================================================
    # 6. Rapid Concurrency & Stress
    # =========================================================================

    async def test_rapid_concurrent_status_inquiries_do_not_deadlock(self) -> None:
        """
        Verify that 10 rapid concurrent status inquiries while Pane 2 is executing
        all resolve in parallel without deadlocks or latency spikes.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="sleep 10",
            pid=3344,
        )

        tasks = []
        t_start = time.time()
        for i in range(10):
            event = {
                "channel": "test_channel",
                "thread_ts": "100.1",
                "ts": f"100.{20 + i}",
                "text": f"status {i}",
            }
            client_i = MockSlackClient()
            tasks.append(self.gateway.handle_message(event, client_i))

        await asyncio.gather(*tasks)
        elapsed = time.time() - t_start

        # 10 parallel queries must complete very quickly (< 0.3s)
        self.assertLess(elapsed, 0.4)
        self.assertTrue(self.session.is_executing)

    async def test_session_close_cleans_up_active_execution(self) -> None:
        """Verify closing a session automatically resets and cancels active execution."""
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="python -m http.server",
            pid=112233,
        )
        with patch("first_mate.gateway.session.os.kill") as mock_kill:
            await self.session.close()
            self.assertIsNone(self.session.active_execution)
            self.assertEqual(self.session.state, SessionState.CLOSED)
            mock_kill.assert_called_once()

    # =========================================================================
    # 7. Additional Edge Cases & Defensive Hardening
    # =========================================================================

    def test_runner_buffer_observer_extract_all_blockers(self) -> None:
        """Verify RunnerBufferObserver extracts all blockers (OAuth and prompt)."""
        observer = RunnerBufferObserver()
        text = (
            "Authenticate at http://127.0.0.1:53682/auth?state=123\n"
            "Do you want to proceed? [y/N]\n"
        )
        blockers = observer.extract_all_blockers(text)
        self.assertEqual(len(blockers), 2)
        self.assertEqual(blockers[0]["type"], "oauth")
        self.assertEqual(blockers[0]["url"], "http://127.0.0.1:53682/auth?state=123")
        self.assertEqual(blockers[1]["type"], "prompt")
        self.assertIn("[y/N]", blockers[1]["content"])

    def test_runner_buffer_observer_with_ansi_codes(self) -> None:
        """Verify ANSI escapes are stripped when extracting blockers."""
        observer = RunnerBufferObserver()
        ansi_text = "\x1b[32mhttp://127.0.0.1:8080/auth\x1b[0m\n\x1b[1mPassword:\x1b[0m "
        single = observer.extract_interactive_blockers(ansi_text)
        self.assertIsNotNone(single)
        self.assertEqual(single["type"], "oauth")
        self.assertEqual(single["url"], "http://127.0.0.1:8080/auth")

    async def test_out_of_band_checkin_returns_liaison_update(self) -> None:
        """Verify conversational check-in message returns Liaison update card."""
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="python3 -m unittest",
            pid=4242,
        )
        event = {
            "channel": "test_channel",
            "thread_ts": "100.1",
            "ts": "100.30",
            "text": "are you still there?",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertGreaterEqual(len(self.client.posted_messages), 1)
        reply = self.client.posted_messages[-1]["text"]
        self.assertIn("Liaison Update", reply)
        self.assertIn("PID 4242", reply)

    async def test_error_handling_in_get_runner_status(self) -> None:
        """Verify get_runner_status handles observer exceptions gracefully."""
        self.buffer_observer.read_buffer.side_effect = RuntimeError("Broken pipe to Herdr")
        status_info = await self.session.get_runner_status()
        self.assertIn("Error reading runner buffer", status_info["raw_buffer"])
        self.assertEqual(status_info["status"], "idle")

    async def test_error_handling_in_cancel_execution(self) -> None:
        """Verify cancel_execution handles exceptions gracefully and still resets active_execution."""
        self.session.active_execution = ExecutionHandle(status="running", pid=-999, pane="runner", command="test")
        with patch("first_mate.gateway.session.os.kill", side_effect=Exception("Failed to kill")):
            res = await self.session.cancel_execution()
            self.assertTrue(res)
            self.assertIsNone(self.session.active_execution)

    async def test_runner_pid_query_timeout_retains_last_pid(self) -> None:
        """
        Verify that _get_pane_foreground_pid retains last_pid on query timeout
        instead of returning None (fixing the false-positive completion defect).
        """
        from first_mate.execution.runner import RunnerExecutor
        executor = RunnerExecutor()

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            p = AsyncMock()
            p.returncode = 0
            # Simulate a slow query that times out
            async def slow_communicate():
                await asyncio.sleep(0.5)
                return (b"", b"")
            p.communicate.side_effect = slow_communicate
            p.kill = MagicMock()
            p.wait = AsyncMock()
            mock_exec.return_value = p

            # Query with 0.05s timeout and last_pid=8888
            pid = await executor._get_pane_foreground_pid("runner", timeout=0.05, last_pid=8888)
            # Must retain 8888!
            self.assertEqual(pid, 8888)

            # Query without last_pid must return -1 sentinel, NOT None
            pid_no_last = await executor._get_pane_foreground_pid("runner", timeout=0.05, last_pid=None)
            self.assertEqual(pid_no_last, -1)

    async def test_cancellation_with_queued_turn_cancels_executing_turn_not_queued(self) -> None:
        """
        Verify that when Turn 1 is executing and Turn 2 is queued waiting for session.lock,
        an out-of-band cancellation cancels Turn 1 (the active turn) and NOT Turn 2.
        """
        turn1_started = asyncio.Event()
        turn1_cancelled = False

        class QueuedHangingAdapter:
            async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
                nonlocal turn1_cancelled
                if prompt == "Turn 1":
                    turn1_started.set()
                    try:
                        while True:
                            await asyncio.sleep(0.05)
                    except asyncio.CancelledError:
                        turn1_cancelled = True
                        raise
                else:
                    yield StatusUpdateEvent(status="working")
                    yield TextDeltaEvent(text_delta=f"Completed {prompt}")
                    yield FinishEvent(response=f"Completed {prompt}", status="SUCCESS")

            async def stop(self) -> None:
                pass

        hanging_adapter = QueuedHangingAdapter()
        test_session = SessionHandle(
            session_id="test_channel:100.queued",
            channel="test_channel",
            thread_ts="100.queued",
            adapter=hanging_adapter,
            buffer_observer=self.buffer_observer,
        )
        self.router.sessions[test_session.session_id] = test_session

        async def run_turn1():
            async for _ in test_session.send_user_message("Turn 1"):
                pass

        turn1_task = asyncio.create_task(run_turn1())
        await turn1_started.wait()
        await asyncio.sleep(0.02)
        self.assertTrue(test_session.lock.locked())

        turn2_completed = False
        async def run_turn2():
            nonlocal turn2_completed
            async for ev in test_session.send_user_message("Turn 2"):
                if getattr(ev, "event_type", None) == "finish":
                    turn2_completed = True

        turn2_task = asyncio.create_task(run_turn2())
        await asyncio.sleep(0.02)

        # Confirm _owner_task is Turn 1's task, NOT Turn 2's task!
        self.assertIsNotNone(test_session._owner_task)
        self.assertEqual(test_session._owner_task, turn1_task)
        self.assertNotEqual(test_session._owner_task, turn2_task)

        # Trigger out-of-band cancellation
        cancel_event = {
            "channel": "test_channel",
            "thread_ts": "100.queued",
            "ts": "100.cancel",
            "text": "can you cancel that?",
        }
        await self.gateway.handle_message(cancel_event, self.client)

        try:
            await asyncio.wait_for(turn1_task, timeout=0.5)
        except (asyncio.CancelledError, Exception):
            pass

        self.assertTrue(turn1_cancelled)
        self.assertTrue(turn1_task.cancelled() or turn1_task.done())

        # Turn 2 was queued, NOT cancelled
        self.assertFalse(turn2_task.cancelled())

        # Turn 2 should now acquire the lock and finish
        await asyncio.wait_for(turn2_task, timeout=1.0)
        self.assertTrue(turn2_completed)
        await test_session.close()

    async def test_run_command_populates_session_active_execution_and_persists(self) -> None:
        """
        Verify that background commands executed via herdr_tools.run_command()
        automatically populate session.active_execution and leave session.is_executing == True
        after the initial turn yields.
        """
        from herdr_tools import run_command

        class BackgroundCommandAdapter:
            async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
                with patch("first_mate.execution.runner.RunnerExecutor.run_command") as mock_run:
                    mock_run.return_value = ExecutionHandle(
                        status="running",
                        pane="runner",
                        command="long_build.sh",
                        pid=65432,
                        message="Command running in background.",
                    )
                    res = await run_command("long_build.sh", pane="runner")
                    yield TextDeltaEvent(text_delta=res)
                    yield FinishEvent(response=res, status="SUCCESS")

            async def stop(self) -> None:
                pass

        bg_adapter = BackgroundCommandAdapter()
        bg_session = SessionHandle(
            session_id="test_channel:100.bg",
            channel="test_channel",
            thread_ts="100.bg",
            adapter=bg_adapter,
            runner_pane="runner",
            buffer_observer=self.buffer_observer,
        )
        self.router.sessions[bg_session.session_id] = bg_session

        # Run Turn 1 through send_user_message
        events = []
        async for ev in bg_session.send_user_message("Run the build"):
            events.append(ev)

        # After Turn 1 completes and releases lock:
        self.assertFalse(bg_session.lock.locked())
        # session.active_execution MUST be set by run_command!
        self.assertIsNotNone(bg_session.active_execution)
        self.assertEqual(bg_session.active_execution.pid, 65432)
        self.assertEqual(bg_session.active_execution.status, "running")
        self.assertTrue(bg_session.is_executing)
        self.assertTrue(bg_session.is_busy)

        # Subsequent status inquiry handled out-of-band by ingress!
        status_client = MockSlackClient()
        status_event = {
            "channel": "test_channel",
            "thread_ts": "100.bg",
            "ts": "100.bg_status",
            "text": "what's the status?",
        }
        await self.gateway.handle_message(status_event, status_client)

        self.assertGreaterEqual(len(status_client.posted_messages), 1)
        status_reply = status_client.posted_messages[-1]["text"]
        self.assertIn("Runner In Progress", status_reply)
        self.assertIn("PID 65432", status_reply)

        await bg_session.close()

    async def test_get_runner_status_detects_running_process_when_active_execution_none(self) -> None:
        """
        Verify that get_runner_status() detects a running process in runner pane
        and synthesizes active_execution when active_execution is initially None.
        """
        detect_session = SessionHandle(
            session_id="test_channel:100.detect",
            channel="test_channel",
            thread_ts="100.detect",
            adapter=self.mock_adapter,
            runner_pane="runner",
            buffer_observer=self.buffer_observer,
        )
        self.assertIsNone(detect_session.active_execution)
        self.assertFalse(detect_session.is_executing)

        with patch("first_mate.execution.runner.RunnerExecutor._get_pane_foreground_pid", return_value=9876):
            status_info = await detect_session.get_runner_status()

            self.assertEqual(status_info["status"], "running")
            self.assertEqual(status_info["pid"], 9876)
            self.assertIsNotNone(detect_session.active_execution)
            self.assertEqual(detect_session.active_execution.pid, 9876)
            self.assertEqual(detect_session.active_execution.status, "running")
            self.assertTrue(detect_session.is_executing)

        await detect_session.close()


if __name__ == "__main__":
    unittest.main()

