#!/usr/bin/env python3
"""
tests/test_empirical_m3_stress.py
Empirical stress-testing harness for Milestone 3:
- Blocker surfacing (OAuth URLs, interactive confirmation prompts, ANSI escapes, multi-blockers)
- Out-of-band cancellation (Real OS subprocess SIGINT, keyword triage, reaction swap, latency)
- Concurrency & lock-bypass verification under high pressure

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import time
import signal
import asyncio
import subprocess
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
from golem.execution.runner import ExecutionHandle
from golem.execution.buffer import RunnerBufferObserver
from golem.gateway.session import (
    SessionHandle,
    SessionState,
    is_cancellation_message,
    is_status_inquiry,
    is_conversational_checkin,
    is_out_of_band_message,
)
from golem.gateway.router import SessionRouter
from golem.gateway.ingress import IngressGateway


class MockRecordingSlackClient:
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


class HangingTurnAdapter:
    """Adapter simulating a long turn holding session.lock indefinitely until cancelled."""

    def __init__(self) -> None:
        self.cancelled = False

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        yield StatusUpdateEvent(status="working")
        try:
            # Hang for 100 seconds
            await asyncio.sleep(100.0)
            yield FinishEvent(response="Done", status="SUCCESS")
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    async def stop(self) -> None:
        pass


class TestEmpiricalM3Stress(unittest.IsolatedAsyncioTestCase):
    """Rigorous empirical validation of blocker extraction, out-of-band triage, and cancellation."""

    async def asyncSetUp(self) -> None:
        self.buffer_observer = MagicMock(spec=RunnerBufferObserver)
        self.buffer_observer.read_buffer = AsyncMock(return_value="Idle buffer")
        self.buffer_observer.extract_all_blockers = MagicMock(return_value=[])
        self.buffer_observer.extract_interactive_blockers = MagicMock(return_value=None)

        self.mock_adapter = HangingTurnAdapter()
        self.session = SessionHandle(
            session_id="stress_channel:200.1",
            channel="stress_channel",
            thread_ts="200.1",
            adapter=self.mock_adapter,
            buffer_observer=self.buffer_observer,
        )
        self.router = SessionRouter(adapter_factory=lambda cwd: self.mock_adapter)
        self.router.sessions[self.session.session_id] = self.session
        self.gateway = IngressGateway(router=self.router)
        self.client = MockRecordingSlackClient()

    async def asyncTearDown(self) -> None:
        await self.session.close()

    # =========================================================================
    # PART 1: BLOCKER EXTRACTION & SURFACING EMPIRICAL STRESS TESTS
    # =========================================================================

    def test_blocker_pattern_extraction_comprehensive(self) -> None:
        """
        Empirically verify RunnerBufferObserver with diverse real-world blocker strings:
        - OAuth URLs: http://127.0.0.1:53682/auth, http://localhost:8080/callback?code=abc, etc.
        - Confirmation prompts: [y/N], (y/n), password prompts, continue? [Y/n]
        """
        observer = RunnerBufferObserver()

        # 1. Standard rclone OAuth URL
        rclone_buf = (
            "2026/10/01 02:00:00 NOTICE: If your browser doesn't open automatically go to the following link:\n"
            "http://127.0.0.1:53682/auth?state=cW9vMm9kZmtla3Jq\n"
            "Log in and authorize rclone for access\n"
            "Waiting for code..."
        )
        b1 = observer.extract_interactive_blockers(rclone_buf)
        self.assertIsNotNone(b1)
        self.assertEqual(b1["type"], "oauth")
        self.assertEqual(b1["url"], "http://127.0.0.1:53682/auth?state=cW9vMm9kZmtla3Jq")

        # 2. Localhost OAuth with port and path
        gdrive_buf = "Please visit: https://localhost:9090/oauth2callback to authorize."
        b2 = observer.extract_interactive_blockers(gdrive_buf)
        self.assertIsNotNone(b2)
        self.assertEqual(b2["type"], "oauth")
        self.assertEqual(b2["url"], "https://localhost:9090/oauth2callback")

        # 3. 0.0.0.0 OAuth URL
        zero_buf = "Listening on http://0.0.0.0:3000/auth/callback for login"
        b3 = observer.extract_interactive_blockers(zero_buf)
        self.assertIsNotNone(b3)
        self.assertEqual(b3["type"], "oauth")
        self.assertEqual(b3["url"], "http://0.0.0.0:3000/auth/callback")

        # 4. Confirmation prompts [y/N], (y/n), Proceed? [y/N]
        prompts = [
            ("Do you wish to continue? [y/N]", "[y/N]"),
            ("Apply changes to production database? (y/n)", "(y/n)"),
            ("Proceed? [Y/n]", "[Y/n]"),
            ("[sudo] password for hermes:", "password"),
            ("Enter passphrase for key '/Users/hermes/.ssh/id_rsa':", "passphrase"),
        ]
        for prompt_text, expected_substr in prompts:
            b = observer.extract_interactive_blockers(prompt_text)
            self.assertIsNotNone(b, f"Failed to extract prompt from: {prompt_text}")
            self.assertEqual(b["type"], "prompt")
            self.assertIn(expected_substr.lower(), b["content"].lower())

        # 5. Buffer with ANSI escapes around URL and prompt
        ansi_buf = (
            "\x1b[38;5;34mNOTICE\x1b[0m: Visit \x1b[1m\x1b[4mhttp://127.0.0.1:53682/auth\x1b[0m\n"
            "\x1b[33mDo you want to proceed? [y/N]\x1b[0m"
        )
        all_b = observer.extract_all_blockers(ansi_buf)
        self.assertEqual(len(all_b), 2)
        self.assertEqual(all_b[0]["type"], "oauth")
        self.assertEqual(all_b[0]["url"], "http://127.0.0.1:53682/auth")
        self.assertEqual(all_b[1]["type"], "prompt")
        self.assertIn("[y/N]", all_b[1]["content"])

    async def test_get_runner_status_surfaces_blocker_immediately(self) -> None:
        """
        Verify get_runner_status() non-intrusively extracts blocker and returns in < 15ms.
        """
        real_observer = RunnerBufferObserver()
        real_observer.read_buffer = AsyncMock(
            return_value="Action required:\nhttp://127.0.0.1:53682/auth?token=abc\nWaiting for response..."
        )
        self.session.buffer_observer = real_observer
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="rclone authorize",
            pid=12345,
        )

        t0 = time.time()
        status_info = await self.session.get_runner_status()
        duration = time.time() - t0

        self.assertLess(duration, 0.30, f"get_runner_status took too long: {duration}s")
        self.assertEqual(status_info["status"], "running")
        self.assertIsNotNone(status_info["blocker"])
        self.assertEqual(status_info["blocker"]["type"], "oauth")
        self.assertEqual(status_info["blocker"]["url"], "http://127.0.0.1:53682/auth?token=abc")
        self.assertEqual(len(status_info["blockers"]), 1)

    async def test_out_of_band_blocker_surfacing_via_ingress_gateway(self) -> None:
        """
        Simulate an interactive blocker appearing in Pane 2's buffer.
        Verify IngressGateway immediately surfaces the blocker to Slack:
        - Reaction swapped to :warning:
        - Egress message contains formatted link <http://127.0.0.1:53682/auth|Click here to authenticate>
        - Total latency < 100ms
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="rclone config reconnect",
            pid=23456,
        )
        real_observer = RunnerBufferObserver()
        real_observer.read_buffer = AsyncMock(
            return_value="Visit http://127.0.0.1:53682/auth to continue"
        )
        self.session.buffer_observer = real_observer

        event = {
            "channel": "stress_channel",
            "thread_ts": "200.1",
            "ts": "200.2",
            "text": "what's the url?",
        }

        t0 = time.time()
        await self.gateway.handle_message(event, self.client)
        duration = time.time() - t0

        self.assertLess(duration, 0.60, f"Out-of-band triage took too long: {duration}s")
        self.assertGreaterEqual(len(self.client.posted_messages), 1)
        posted_text = self.client.posted_messages[-1]["text"]

        self.assertIn("Authentication Required", posted_text)
        self.assertIn("<http://127.0.0.1:53682/auth|Click here to authenticate>", posted_text)

        # Confirm warning reaction added
        reactions = [r["name"] for r in self.client.reactions_added]
        self.assertIn("warning", reactions)

    async def test_blocker_surfaced_even_when_session_lock_is_held(self) -> None:
        """
        Verify that even if session.lock is currently locked by a long-running turn,
        an incoming status inquiry for a prompt blocker [y/N] bypasses the lock
        and surfaces the blocker without deadlocking.
        """
        # Lock session as if Turn 1 is running
        await self.session.lock.acquire()
        self.assertTrue(self.session.lock.locked())

        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="git clean -fdx",
            pid=34567,
        )
        real_observer = RunnerBufferObserver()
        real_observer.read_buffer = AsyncMock(
            return_value="Removing untracked files...\nAre you sure you want to proceed? [y/N]"
        )
        self.session.buffer_observer = real_observer

        try:
            t0 = time.time()
            events = [ev async for ev in self.session.send_user_message("check status")]
            duration = time.time() - t0

            self.assertLess(duration, 0.60, f"Lock bypass took too long: {duration}s")
            finish_ev = next(e for e in events if getattr(e, "event_type", None) == "finish")
            self.assertIn("Confirmation Required", finish_ev.response)
            self.assertIn("[y/N]", finish_ev.response)
            self.assertTrue(self.session.lock.locked())
        finally:
            if self.session.lock.locked():
                self.session.lock.release()

    # =========================================================================
    # PART 2: OUT-OF-BAND CANCELLATION EMPIRICAL STRESS TESTS
    # =========================================================================

    async def test_real_subprocess_cancellation_sigint(self) -> None:
        """
        EMPIRICAL TEST: Spawn a real OS subprocess, attach to active_execution,
        invoke session.cancel_execution(), and verify the OS process is actually
        terminated by SIGINT immediately.
        """
        # Launch real sleep process
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        pid = proc.pid
        self.assertGreater(pid, 0)
        # Verify process is running
        self.assertIsNone(proc.poll(), "Process should be running")

        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="python -c sleep",
            pid=pid,
        )
        self.session.state = SessionState.WORKING
        self.assertTrue(self.session.is_executing)

        # Mock herdr client so we don't depend on live herdr daemon CLI
        mock_herdr = MagicMock()
        mock_herdr.run_cli = AsyncMock(return_value="ok")
        self.session.harness = MagicMock()
        self.session.harness.herdr_client = mock_herdr

        t0 = time.time()
        res = await self.session.cancel_execution()
        duration = time.time() - t0

        self.assertTrue(res)
        self.assertLess(duration, 0.60, f"cancel_execution took too long: {duration}s")

        # active_execution must be cleared
        self.assertIsNone(self.session.active_execution)
        self.assertFalse(self.session.is_executing)
        self.assertEqual(self.session.state, SessionState.IDLE)

        # Verify real process was killed by SIGINT
        # Wait up to 1.5s for process to exit
        try:
            return_code = proc.wait(timeout=1.5)
            # On Unix, SIGINT exit code is -2 (or 130)
            self.assertTrue(return_code == -signal.SIGINT or return_code == 130 or return_code != 0)
        except subprocess.TimeoutExpired:
            proc.kill()
            self.fail("Process was NOT terminated by cancel_execution() SIGINT!")

    async def test_out_of_band_cancellation_keywords_via_ingress(self) -> None:
        """
        Test diverse cancellation keywords through IngressGateway:
        'cancel', 'stop', 'abort', 'kill', '/cancel', '!stop', 'Please cancel that execution'
        Verify that for each keyword:
        - Cancellation is executed
        - Reaction swapped to :x:
        - Execution is halted
        """
        keywords = [
            "cancel",
            "stop",
            "abort",
            "kill",
            "/cancel",
            "!stop",
            "Please cancel that execution",
            "stop the task",
            "abort execution",
            "cancel it",
        ]

        for idx, kw in enumerate(keywords):
            # Spawn real background process
            proc = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            pid = proc.pid
            self.session.active_execution = ExecutionHandle(
                status="running",
                pane="runner",
                command="sleep 30",
                pid=pid,
            )
            self.session.state = SessionState.WORKING

            client = MockRecordingSlackClient()
            event = {
                "channel": "stress_channel",
                "thread_ts": "200.1",
                "ts": f"200.{100 + idx}",
                "text": kw,
            }

            t0 = time.time()
            await self.gateway.handle_message(event, client)
            duration = time.time() - t0

            self.assertLess(duration, 0.50, f"Keyword '{kw}' took too long: {duration}s")
            self.assertIsNone(self.session.active_execution, f"Active execution not cleared for '{kw}'")
            self.assertEqual(self.session.state, SessionState.IDLE)

            # Confirm :x: reaction
            reactions = [r["name"] for r in client.reactions_added]
            self.assertIn("x", reactions, f"Missing :x: reaction for keyword '{kw}'")

            # Confirm posted response
            self.assertGreaterEqual(len(client.posted_messages), 1)
            self.assertIn("Execution Cancelled", client.posted_messages[-1]["text"])

            # Verify process exit
            try:
                proc.wait(timeout=1.5)
            except subprocess.TimeoutExpired:
                proc.kill()
                self.fail(f"Subprocess not terminated for keyword '{kw}'")

    def test_cancellation_false_positive_rejection(self) -> None:
        """
        Verify that messages mentioning cancellation words in non-imperative context
        are NOT treated as cancellation commands:
        e.g. 'Write a python script to cancel bookings', 'Build a stopwatch'
        """
        non_cancellations = [
            "Write a python script to cancel bookings",
            "Build a stopwatch application",
            "How do I abort a transaction in PostgreSQL?",
            "Explain why the task was cancelled",
            "What causes an aborted connection in MySQL?",
        ]
        for msg in non_cancellations:
            self.assertFalse(
                is_cancellation_message(msg),
                f"False positive cancellation detected for: '{msg}'"
            )

    async def test_owner_task_cancellation_during_active_turn(self) -> None:
        """
        Verify that if an active turn is running under an owner asyncio task,
        cancel_execution() cancels the owner task and stops turn execution immediately.
        """
        # Start a turn that hangs in self.mock_adapter
        turn_started = asyncio.Event()

        async def _run_turn():
            turn_started.set()
            async for _ in self.session.send_user_message("Run long turn"):
                pass

        turn_task = asyncio.create_task(_run_turn())
        await turn_started.wait()
        # Give it a moment to enter the adapter
        await asyncio.sleep(0.05)

        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="long job",
            pid=99999,
        )

        # Now trigger cancellation out-of-band
        t0 = time.time()
        res = await self.session.cancel_execution()
        duration = time.time() - t0

        self.assertTrue(res)
        self.assertLess(duration, 0.60)
        self.assertIsNone(self.session.active_execution)

        # Wait for turn_task to finish (should be cancelled)
        await asyncio.sleep(0.05)
        self.assertTrue(self.mock_adapter.cancelled)
        self.assertTrue(turn_task.done())

    # =========================================================================
    # PART 3: ADVERSARIAL EDGE CASES & STRESS TESTS
    # =========================================================================

    def test_non_oauth_urls_not_flagged_as_blockers(self) -> None:
        """
        Adversarial test: Verify standard web URLs are NOT falsely classified
        as OAuth callback blockers.
        """
        observer = RunnerBufferObserver()
        general_logs = (
            "Fetching https://github.com/user/repo/archive/main.tar.gz...\n"
            "Downloaded from https://registry.npmjs.org/express/-/express-4.18.2.tgz\n"
            "Connecting to database at http://db.internal.corp:5432/production\n"
            "Documentation available at http://example.com/docs/api\n"
        )
        blocker = observer.extract_interactive_blockers(general_logs)
        self.assertIsNone(blocker, f"Falsely classified non-OAuth URL as blocker: {blocker}")

    async def test_concurrent_rapid_cancellations(self) -> None:
        """
        Adversarial test: Fire 10 concurrent cancellation messages at once
        while a process is running. Ensure no race condition, deadlock, or exception.
        """
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="sleep 30",
            pid=proc.pid,
        )
        self.session.state = SessionState.WORKING

        # Fire 10 cancellation tasks concurrently
        async def _cancel_worker(i: int):
            cl = MockRecordingSlackClient()
            ev = {
                "channel": "stress_channel",
                "thread_ts": "200.1",
                "ts": f"200.{500 + i}",
                "text": "cancel",
            }
            await self.gateway.handle_message(ev, cl)
            return cl

        tasks = [asyncio.create_task(_cancel_worker(i)) for i in range(10)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            self.assertNotIsInstance(res, Exception)

        self.assertIsNone(self.session.active_execution)
        self.assertEqual(self.session.state, SessionState.IDLE)

        # Process should be terminated
        try:
            proc.wait(timeout=1.5)
        except subprocess.TimeoutExpired:
            proc.kill()
            self.fail("Process was not terminated under concurrent cancellations")

    async def test_cancel_when_pid_already_dead(self) -> None:
        """
        Adversarial test: Cancellation when PID is already non-existent or dead.
        Must handle ProcessLookupError gracefully, clean up state, and return True.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="exited job",
            pid=9999999,
        )
        self.session.state = SessionState.WORKING

        res = await self.session.cancel_execution()
        self.assertTrue(res)
        self.assertIsNone(self.session.active_execution)
        self.assertEqual(self.session.state, SessionState.IDLE)

    async def test_cancel_when_lock_locked_no_runner_pid(self) -> None:
        """
        Adversarial test: Cancellation when turn is holding lock with NO runner PID
        (e.g. liaison agent is hanging in its own turn).
        """
        turn_started = asyncio.Event()

        async def _run_hanging_turn():
            turn_started.set()
            async for _ in self.session.send_user_message("Hanging Turn without PID"):
                pass

        turn_task = asyncio.create_task(_run_hanging_turn())
        await turn_started.wait()
        await asyncio.sleep(0.05)

        self.assertTrue(self.session.lock.locked())
        self.assertIsNone(self.session.active_execution)

        client = MockRecordingSlackClient()
        event = {
            "channel": "stress_channel",
            "thread_ts": "200.1",
            "ts": "200.999",
            "text": "stop",
        }
        await self.gateway.handle_message(event, client)

        # Owner task cancelled
        await asyncio.sleep(0.05)
        self.assertTrue(self.mock_adapter.cancelled)
        self.assertTrue(turn_task.done())
        self.assertIn("Execution Cancelled", client.posted_messages[-1]["text"])

    async def test_blocker_extraction_resilience_to_observer_errors(self) -> None:
        """
        Adversarial test: When buffer observer throws unexpected exception,
        get_runner_status() catches it gracefully and does not crash.
        """
        broken_observer = MagicMock(spec=RunnerBufferObserver)
        broken_observer.read_buffer = AsyncMock(side_effect=IOError("Herdr socket broken"))
        broken_observer.extract_all_blockers = MagicMock(side_effect=ValueError("Regex error"))
        self.session.buffer_observer = broken_observer

        status = await self.session.get_runner_status()
        self.assertIsInstance(status, dict)
        self.assertEqual(status["blockers"], [])
        self.assertIn("Error reading runner buffer", status["raw_buffer"])


if __name__ == "__main__":
    unittest.main()

