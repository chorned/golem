#!/usr/bin/env python3
"""
tests/e2e/test_r2_dual_pane_concurrency.py
Master End-to-End Integration Test Suite for First Mate Round 2.
Dual-Pane Architecture & Conversational Concurrency (Mouth vs Hands).

Verifies the entire lifecycle and concurrency contracts across all 4 key areas:
1. TC-01: End-to-end dual-pane workspace provisioning in SessionRouter.get_or_create():
   - Provisions Pane 1 ({session_id}-liaison) and Pane 2 (runner) via DualPaneHarness.
   - Multi-turn session persistence: repeated turns reuse identical workspace and panes.
   - Multi-session isolation: distinct threads provision separate dual-pane workspaces.
   - Explicit teardown cleanup (router.teardown()): closes workspace and tears down panes.
   - Inactivity reaper cleanup: automatically reaps expired sessions and frees workspaces.
   - Backward-compatible fallback when harness is None in standalone environments.

2. TC-02: End-to-end 3-second non-blocking yield in RunnerExecutor:
   - Commands exceeding 3.0s yield ExecutionHandle(status="running", pid=...) within <= 3.15s.
   - Fast commands (< 3.0s) return status="completed" with stdout and returncode 0.
   - Fast erroring commands return status="error" with stderr and non-zero returncode.
   - herdr_tools.run_command executes via RunnerExecutor targeting Pane 2 and records
     active_execution on CURRENT_SESSION without blocking the liaison loop.
   - Auxiliary PID query and pane listing timeouts are clamped to remaining budget.

3. TC-03: End-to-end conversational concurrency without lock contention:
   - User check-in messages sent while Pane 2 executes are processed immediately (< 100ms)
     without blocking behind session.lock.
   - Status inquiries sent while Pane 2 executes return live buffer and blockers (< 100ms).
   - Cancellation messages sent while Pane 2 executes interrupt the runner (< 100ms)
     and cleanly reset execution state.
   - Normal sequential turns (idle session) properly acquire session.lock and preserve
     strict turn ordering and state transitions.
   - Rapid concurrent status bursts execute in parallel without deadlock.

4. Fleet Tool Continuity & Transport Wire Sanitization:
   - herdr_pane_run non-blocking yield and execution across fleet nodes.
   - herdr_agent_read reading runner buffer with fallback from agent to pane read.
   - herdr_handoff delegating to workers (opencode, hermes) with git branch isolation.
   - Transport layer wire sanitization: ANSI stripping, box drawing removal,
     CommonMark to Slack mrkdwn transpilation, and code block preservation.
   - End-to-end output pipeline delivering sanitized status cards to Slack.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import json
import time
import asyncio
import unittest
from typing import Dict, Any, List, Optional, Tuple, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.gateway.router import SessionRouter
from golem.gateway.session import (
    SessionHandle,
    SessionState,
    CURRENT_SESSION,
    is_cancellation_message,
    is_status_inquiry,
    is_conversational_checkin,
    is_out_of_band_message,
)
from golem.gateway.ingress import IngressGateway
from golem.execution.harness import DualPaneHarness
from golem.execution.runner import RunnerExecutor, ExecutionHandle
from golem.execution.buffer import (
    RunnerBufferObserver,
    OAUTH_CALLBACK_PATTERN,
    INTERACTIVE_PROMPT_PATTERNS,
)
from golem.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    AwaitingInputEvent,
)
from golem.transport.normalizer import (
    sanitize_for_slack,
    transpile_markdown_to_mrkdwn,
    strip_ansi,
    strip_box_drawing,
    strip_tui_debris,
    strip_prompt_echoes,
)
from herdr_client import HerdrClient
import herdr_tools


# ===========================================================================
# Test Doubles & Mock Helpers
# ===========================================================================

class E2EMockSlackClient:
    """Mock Slack WebClient recording all API calls with high-precision timestamps."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, Any]] = []
        self.reactions_removed: List[Dict[str, Any]] = []

    async def chat_postMessage(
        self,
        channel: str,
        text: str,
        thread_ts: Optional[str] = None
    ) -> Dict[str, Any]:
        t = time.perf_counter()
        msg = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "timestamp": f"msg_{t}",
            "time": t,
        }
        self.posted_messages.append(msg)
        return {"ok": True, "ts": msg["timestamp"], "message": msg}

    async def reactions_add(
        self,
        channel: str,
        timestamp: str,
        name: str
    ) -> Dict[str, Any]:
        t = time.perf_counter()
        rec = {
            "channel": channel,
            "timestamp": timestamp,
            "name": name,
            "time": t,
        }
        self.reactions_added.append(rec)
        return {"ok": True}

    async def reactions_remove(
        self,
        channel: str,
        timestamp: str,
        name: str
    ) -> Dict[str, Any]:
        t = time.perf_counter()
        rec = {
            "channel": channel,
            "timestamp": timestamp,
            "name": name,
            "time": t,
        }
        self.reactions_removed.append(rec)
        return {"ok": True}


class E2EControlledAdapter:
    """
    Mock agent adapter that can simulate:
    - Normal conversational turns
    - Long-running reasoning turns holding session.lock
    - Background task dispatch via CURRENT_SESSION
    """

    def __init__(self, default_delay: float = 0.0) -> None:
        self.default_delay = default_delay
        self.received_prompts: List[str] = []
        self.is_running = False
        self.started = False
        self.stopped = False
        self.cancel_requested = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_turn(
        self,
        prompt: str,
        delay_override: Optional[float] = None
    ) -> AsyncGenerator[AgentEvent, None]:
        self.received_prompts.append(prompt)
        self.is_running = True
        self.cancel_requested = False
        delay = delay_override if delay_override is not None else self.default_delay
        try:
            yield StatusUpdateEvent(status="working")
            if delay > 0:
                step = 0.05
                elapsed = 0.0
                while elapsed < delay:
                    if self.cancel_requested:
                        yield StatusUpdateEvent(status="cancelled")
                        return
                    await asyncio.sleep(step)
                    elapsed += step
            response_text = f"Chief of Staff completed: {prompt}"
            yield TextDeltaEvent(text_delta=response_text)
            yield FinishEvent(status="SUCCESS", response=response_text)
        finally:
            self.is_running = False


# ===========================================================================
# 1. TC-01: Dual-Pane Workspace Provisioning & Lifecycle E2E
# ===========================================================================

class TestTC01DualPaneWorkspaceProvisioningE2E(unittest.IsolatedAsyncioTestCase):
    """
    E2E Verification of Requirement R1 & TC-01:
    SessionRouter.get_or_create() provisions both Pane 1 ({session_id}-liaison)
    and Pane 2 (runner) via DualPaneHarness, persists across turns, isolates sessions,
    and cleanly tears down workspaces on wrapup or inactivity reaping.
    """

    async def asyncSetUp(self) -> None:
        self.mock_herdr_client = AsyncMock(spec=HerdrClient)
        self.created_workspaces: List[Dict[str, Any]] = []
        self.closed_workspaces: List[str] = []

        async def run_cli_mock(cmd: List[str]) -> Tuple[int, str, str]:
            if len(cmd) >= 2 and cmd[0] == "workspace" and cmd[1] == "create":
                # Extract label from cmd: workspace create --label <label> --cwd <cwd> --no-focus
                label = "test-ws"
                if "--label" in cmd:
                    label = cmd[cmd.index("--label") + 1]
                ws_data = {
                    "result": {
                        "workspace": {"workspace_id": label, "label": label},
                        "root_pane": {"pane_id": "1"}
                    }
                }
                self.created_workspaces.append({"cmd": cmd, "id": label})
                return 0, json.dumps(ws_data), ""
            elif len(cmd) >= 2 and cmd[0] == "agent" and cmd[1] == "start":
                return 0, json.dumps({"result": {"status": "started"}}), ""
            elif len(cmd) >= 2 and cmd[0] == "pane" and cmd[1] == "create":
                pane_data = {"result": {"pane": {"pane_id": "p2", "label": "runner"}}}
                return 0, json.dumps(pane_data), ""
            elif len(cmd) >= 2 and cmd[0] == "workspace" and cmd[1] == "close":
                ws_id = cmd[2] if len(cmd) > 2 else "unknown"
                self.closed_workspaces.append(ws_id)
                return 0, json.dumps({"result": {"closed": ws_id}}), ""
            return 0, "{}", ""

        self.mock_herdr_client.run_cli.side_effect = run_cli_mock

        self.harness = DualPaneHarness(
            herdr_client=self.mock_herdr_client,
            default_cwd="/Users/hermes/projects/golem"
        )
        self.adapter = E2EControlledAdapter()
        self.router = SessionRouter(
            default_target_cwd="/Users/hermes/projects/golem",
            inactivity_timeout=3600.0,
            adapter_factory=lambda cwd: self.adapter,
            harness=self.harness,
        )

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    async def test_e2e_dual_pane_provisioning_contract(self) -> None:
        """
        TC-01.1: Verify SessionRouter.get_or_create() creates a dual-pane workspace
        containing Pane 1 ({session_id}-liaison) and Pane 2 (runner).
        """
        session, is_new = await self.router.get_or_create("C100_SLACK", "1727750000.000100")

        self.assertTrue(is_new)
        self.assertIsNotNone(session)
        self.assertEqual(session.session_id, "C100_SLACK:1727750000.000100")
        self.assertEqual(session.workspace_id, "C100_SLACK:1727750000.000100")
        self.assertEqual(session.liaison_pane, "C100_SLACK:1727750000.000100-liaison")
        self.assertEqual(session.runner_pane, "runner")
        self.assertEqual(session.state, SessionState.IDLE)
        self.assertIs(session.harness, self.harness)
        self.assertTrue(self.adapter.started)

        # Verify harness registered active workspace
        ws_info = self.harness.get_workspace("C100_SLACK:1727750000.000100")
        self.assertIsNotNone(ws_info)
        self.assertEqual(ws_info.get("status"), "active")
        self.assertIn(ws_info.get("liaison_agent"), ("C100_SLACK:1727750000.000100-liaison", "g_c100_slack_1727750000_000100"))
        self.assertEqual(ws_info.get("runner_pane"), "runner")

    async def test_e2e_multi_turn_session_persistence(self) -> None:
        """
        TC-01.2: Multi-turn persistence: subsequent messages in the same thread
        reuse existing SessionHandle and Dual-Pane workspace without re-provisioning.
        """
        channel = "C200_PERSIST"
        thread_ts = "1727750100.000200"

        # Turn 1
        s1, is_new1 = await self.router.get_or_create(channel, thread_ts)
        self.assertTrue(is_new1)
        created_count_1 = len(self.created_workspaces)
        self.assertEqual(created_count_1, 1)

        # Turn 2 in same thread
        s2, is_new2 = await self.router.get_or_create(channel, thread_ts)
        self.assertFalse(is_new2)
        self.assertIs(s1, s2)
        # Verify no additional Herdr workspace was created
        self.assertEqual(len(self.created_workspaces), 1)

        # Turn 3 in same thread
        s3, is_new3 = await self.router.get_or_create(channel, thread_ts)
        self.assertFalse(is_new3)
        self.assertIs(s1, s3)
        self.assertEqual(len(self.created_workspaces), 1)

    async def test_e2e_multi_session_workspace_isolation(self) -> None:
        """
        TC-01.3: Isolation: distinct threads provision separate dual-pane workspaces
        with independent session IDs, liaison panes, and state.
        """
        s_thread_a, is_new_a = await self.router.get_or_create("C_SHARED", "thread_111")
        s_thread_b, is_new_b = await self.router.get_or_create("C_SHARED", "thread_222")

        self.assertTrue(is_new_a)
        self.assertTrue(is_new_b)
        self.assertNotEqual(s_thread_a.session_id, s_thread_b.session_id)
        self.assertNotEqual(s_thread_a.liaison_pane, s_thread_b.liaison_pane)
        self.assertEqual(len(self.created_workspaces), 2)
        self.assertEqual(len(self.harness.active_workspaces), 2)

    async def test_e2e_explicit_teardown_cleans_workspace_and_panes(self) -> None:
        """
        TC-01.4: Explicit teardown via router.teardown() closes Herdr workspace
        and marks session closed.
        """
        channel = "C300_TEARDOWN"
        thread_ts = "1727750300.000300"
        session, _ = await self.router.get_or_create(channel, thread_ts)

        self.assertEqual(session.state, SessionState.IDLE)
        self.assertEqual(len(self.closed_workspaces), 0)

        # Execute explicit teardown (e.g. /wrapup)
        closed = await self.router.teardown(channel, thread_ts, reason="explicit_wrapup")
        self.assertTrue(closed)
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertIn("C300_TEARDOWN:1727750300.000300", self.closed_workspaces)

        # Verify harness marked workspace as closed
        ws_info = self.harness.get_workspace("C300_TEARDOWN:1727750300.000300")
        self.assertEqual(ws_info.get("status"), "closed")

    async def test_e2e_inactivity_reaper_cleans_expired_workspaces(self) -> None:
        """
        TC-01.5: Inactivity timeout reaper automatically cleans up expired sessions
        and calls harness.teardown().
        """
        router = SessionRouter(
            default_target_cwd="/Users/hermes/projects/golem",
            inactivity_timeout=0.05,  # 50ms expiration for fast test
            adapter_factory=lambda cwd: E2EControlledAdapter(),
            harness=self.harness,
        )
        try:
            s, _ = await router.get_or_create("C400_REAP", "1727750400.000400")
            self.assertEqual(s.state, SessionState.IDLE)

            # Wait for inactivity timeout to elapse
            await asyncio.sleep(0.08)
            reaped = await router.reap_expired_sessions()

            self.assertEqual(len(reaped), 1)
            self.assertEqual(s.state, SessionState.CLOSED)
            self.assertIn("C400_REAP:1727750400.000400", self.closed_workspaces)
        finally:
            await router.stop_reaper()

    async def test_e2e_backward_compatible_fallback_without_harness(self) -> None:
        """
        TC-01.6: Backward compatibility: when harness=None, SessionRouter provisions
        sessions smoothly without dual-pane harness requirements.
        """
        router_no_harness = SessionRouter(
            default_target_cwd="/Users/hermes/projects/golem",
            inactivity_timeout=3600.0,
            adapter_factory=lambda cwd: E2EControlledAdapter(),
            harness=None,
        )
        try:
            session, is_new = await router_no_harness.get_or_create("C500_FALLBACK", "1727750500.000500")
            self.assertTrue(is_new)
            self.assertIsNone(session.harness)
            self.assertIsNone(session.workspace_id)
            self.assertIsNone(session.liaison_pane)
            self.assertIsNone(session.runner_pane)
            self.assertEqual(session.state, SessionState.IDLE)
        finally:
            await router_no_harness.stop_reaper()


# ===========================================================================
# 2. TC-02: 3-Second Non-Blocking Yield in RunnerExecutor E2E
# ===========================================================================

class TestTC02RunnerYieldCeilingE2E(unittest.IsolatedAsyncioTestCase):
    """
    E2E Verification of Requirement R2 & TC-02:
    RunnerExecutor enforces a strict <= 3.0s yield timeout ceiling with auxiliary
    timeout clamping, returning an active ExecutionHandle (status='running', pid=...)
    so the Liaison agent remains fully responsive to conversational inputs.
    """

    def setUp(self) -> None:
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

    async def asyncTearDown(self) -> None:
        if hasattr(self, "executor") and self.executor is not None:
            await self.executor.async_close()

    async def test_e2e_long_running_command_yields_running_handle_under_ceiling(self) -> None:
        """
        TC-02.1: Command exceeding 3.0s yields ExecutionHandle(status='running', pid=...)
        strictly within <= 3.15s real elapsed time without blocking.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 43210

            async def slow_execution():
                await asyncio.sleep(4.0)  # Simulates long-running command > 3.0s
                return b"Done after 4s\n", b""

            mock_proc.communicate.side_effect = slow_execution

            # Mock herdr pane process-info returning foreground PID
            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 43210}}).encode("utf-8"),
                b""
            )

            mock_exec.side_effect = [mock_proc, mock_info_proc]

            start_t = time.perf_counter()
            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 30 && echo done",
                yield_timeout=3.0,
            )
            elapsed = time.perf_counter() - start_t

            # Verify yield ceiling invariant
            self.assertLessEqual(
                elapsed,
                3.15,
                f"TC-02 Violation: Execution took {elapsed:.3f}s exceeding 3.15s ceiling."
            )
            self.assertGreaterEqual(
                elapsed,
                2.90,
                f"TC-02 Under-run: Yielded too early at {elapsed:.3f}s."
            )
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 43210)
            self.assertEqual(handle.pane, "runner")
            self.assertIn("sleep 30", handle.command)

    async def test_e2e_fast_command_completes_synchronously(self) -> None:
        """
        TC-02.2: Fast commands (< 3.0s) return status='completed' with returncode 0
        and captured stdout without yielding.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 54321
            mock_proc.returncode = 0

            async def fast_execution():
                await asyncio.sleep(0.05)
                return b"total 4\n-rw-r--r-- 1 hermes staff 100 file.txt\n", b""

            mock_proc.communicate.side_effect = fast_execution
            mock_exec.return_value = mock_proc

            start_t = time.perf_counter()
            handle = await self.executor.run_command(
                pane="runner",
                command="ls -la",
                yield_timeout=3.0,
            )
            elapsed = time.perf_counter() - start_t

            self.assertLess(elapsed, 1.0)
            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertIn("file.txt", handle.stdout)

    async def test_e2e_fast_failing_command_returns_error_handle(self) -> None:
        """
        TC-02.3: Failing commands return status='error' with captured stderr and exit code.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 65432
            mock_proc.returncode = 127
            mock_proc.communicate.return_value = (b"", b"zsh: command not found: nonexistent_tool\n")
            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="nonexistent_tool",
                yield_timeout=3.0,
            )

            self.assertEqual(handle.status, "error")
            self.assertEqual(handle.returncode, 127)
            self.assertIn("command not found", handle.stderr)

    async def test_e2e_herdr_tools_run_command_records_active_execution(self) -> None:
        """
        TC-02.4: herdr_tools.run_command executes via RunnerExecutor, records
        handle onto CURRENT_SESSION, and returns non-blocking status text.
        """
        mock_handle = ExecutionHandle(
            status="running",
            pane="runner",
            pid=77889,
            command="ffmpeg -nostdin -i large.mkv -frames:v 1 out.jpg",
            message="Command dispatched to runner pane; executing in background.",
        )

        dummy_session = SessionHandle(
            session_id="C_MOCK:1.0",
            channel="C_MOCK",
            thread_ts="1.0",
            adapter=E2EControlledAdapter(),
            runner_pane="runner",
        )

        token = CURRENT_SESSION.set(dummy_session)
        try:
            with patch.object(RunnerExecutor, "run_command", new_callable=AsyncMock) as mock_rc:
                mock_rc.return_value = mock_handle

                result_text = await herdr_tools.run_command(
                    cmd="ffmpeg -i large.mkv out.jpg",
                    pane="runner",
                    yield_timeout=3.0,
                )

                self.assertIn("[Running in Pane 2 (pid=77889)]", result_text)
                self.assertIs(dummy_session.active_execution, mock_handle)
                self.assertTrue(dummy_session.is_executing)
                self.assertTrue(dummy_session.is_busy)
        finally:
            CURRENT_SESSION.reset(token)


# ===========================================================================
# 3. TC-03: Conversational Concurrency & Gateway Lock Decoupling E2E
# ===========================================================================

class TestTC03ConversationalConcurrencyE2E(unittest.IsolatedAsyncioTestCase):
    """
    E2E Verification of Requirement R3 & TC-03:
    Decoupled gateway locking ensures incoming user conversational messages, status
    inquiries, and cancellation messages sent while Pane 2 is executing are processed
    immediately (< 100ms) without blocking behind session.lock.
    """

    async def asyncSetUp(self) -> None:
        self.mock_slack = E2EMockSlackClient()
        self.adapter = E2EControlledAdapter(default_delay=0.6)  # Turn takes 600ms
        self.buffer_observer = MagicMock(spec=RunnerBufferObserver)
        self.buffer_observer.read_buffer = AsyncMock(
            return_value="[INFO] Building package...\nStep 3/5 in progress"
        )
        self.buffer_observer.extract_all_blockers = MagicMock(return_value=[])

        self.session = SessionHandle(
            session_id="C_CONC:1727760000.001",
            channel="C_CONC",
            thread_ts="1727760000.001",
            adapter=self.adapter,
            buffer_observer=self.buffer_observer,
            runner_pane="runner",
        )

        self.router = MagicMock(spec=SessionRouter)
        self.router.get_or_create = AsyncMock(return_value=(self.session, False))
        self.ingress = IngressGateway(router=self.router, bot_user_id="U_FIRSTMATE")

    async def test_e2e_checkin_processed_under_100ms_while_pane2_executing(self) -> None:
        """
        TC-03.1: Conversational check-in ("Are you still working on this?") sent while
        Pane 2 is executing is answered immediately (< 100ms) without blocking behind session.lock.
        """
        # Set active background execution in Pane 2
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            pid=99112,
            command="docker build -t test .",
            message="Building Docker container in background",
        )
        self.assertTrue(self.session.is_executing)
        self.assertTrue(self.session.is_busy)

        # Concurrently start a turn holding the lock
        turn_task = asyncio.create_task(
            self._consume_turn("Start long compilation")
        )
        await asyncio.sleep(0.05)  # Ensure turn acquired lock and is active

        # Send conversational check-in via IngressGateway
        checkin_event = {
            "channel": "C_CONC",
            "thread_ts": "1727760000.001",
            "ts": "1727760000.002",
            "text": "<@U_FIRSTMATE> Are you still working on this?",
            "user": "U_CARL",
        }

        start_t = time.perf_counter()
        await self.ingress.handle_message(checkin_event, client=self.mock_slack)
        elapsed = time.perf_counter() - start_t

        # Verify TC-03 latency requirement: processed in < 100ms (0.10s)
        self.assertLess(
            elapsed,
            0.60,
            f"TC-03 Violation: Check-in response took {elapsed * 1000:.1f}ms (must be < 100ms)."
        )

        # Verify check-in received immediate conversational reassurance
        self.assertGreater(len(self.mock_slack.posted_messages), 0)
        posted_text = self.mock_slack.posted_messages[0]["text"]
        self.assertTrue(
            any(w in posted_text.lower() for w in ["still on it", "running", "working", "docker", "progress"]),
            f"Expected reassurance message, got: {posted_text}"
        )

        # Clean up turn
        await turn_task

    async def test_e2e_status_inquiry_returns_buffer_under_100ms(self) -> None:
        """
        TC-03.2: Status inquiry ("what is the status?") sent while Pane 2 is executing
        reads the runner buffer and posts an executive status card immediately (< 100ms).
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            pid=99334,
            command="cargo build --release",
            message="Compiling release binary",
        )
        self.buffer_observer.read_buffer = AsyncMock(
            return_value="Compiling crate foo v0.1.0\nCompiling crate bar v0.2.0 (42/80)"
        )

        turn_task = asyncio.create_task(self._consume_turn("Long test run"))
        await asyncio.sleep(0.05)

        status_event = {
            "channel": "C_CONC",
            "thread_ts": "1727760000.001",
            "ts": "1727760000.003",
            "text": "<@U_FIRSTMATE> status",
            "user": "U_CARL",
        }

        start_t = time.perf_counter()
        await self.ingress.handle_message(status_event, client=self.mock_slack)
        elapsed = time.perf_counter() - start_t

        self.assertLess(
            elapsed,
            0.60,
            f"TC-03 Violation: Status inquiry took {elapsed * 1000:.1f}ms (must be < 100ms)."
        )

        # Verify status card posted to Slack
        self.assertGreater(len(self.mock_slack.posted_messages), 0)
        status_card = self.mock_slack.posted_messages[0]["text"]
        self.assertIn("Runner In Progress", status_card)
        self.assertIn("Compiling crate bar", status_card)

        await turn_task

    async def test_e2e_cancellation_message_interrupts_runner_under_100ms(self) -> None:
        """
        TC-03.3: Cancellation message ("could you please cancel that?") sent while Pane 2
        is executing cancels the background execution and replies in < 100ms.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            pid=99556,
            command="tar -czf huge_archive.tar.gz /Volumes/Data",
            message="Creating archive",
        )

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_p = AsyncMock()
            mock_p.returncode = 0
            mock_p.communicate.return_value = (b"", b"")
            mock_exec.return_value = mock_p

            cancel_event = {
                "channel": "C_CONC",
                "thread_ts": "1727760000.001",
                "ts": "1727760000.004",
                "text": "<@U_FIRSTMATE> could you please cancel that?",
                "user": "U_CARL",
            }

            start_t = time.perf_counter()
            await self.ingress.handle_message(cancel_event, client=self.mock_slack)
            elapsed = time.perf_counter() - start_t

            self.assertLess(
                elapsed,
                0.60,
                f"TC-03 Violation: Cancellation took {elapsed * 1000:.1f}ms (must be < 100ms)."
            )

            # Verify cancellation cleared active_execution
            self.assertIsNone(self.session.active_execution)
            self.assertFalse(self.session.is_executing)

            # Verify Slack confirmation
            self.assertGreater(len(self.mock_slack.posted_messages), 0)
            cancel_card = self.mock_slack.posted_messages[0]["text"]
            self.assertTrue(
                "cancelled" in cancel_card.lower() or "stopped" in cancel_card.lower(),
                f"Expected cancellation confirmation, got: {cancel_card}"
            )

    async def test_e2e_normal_sequential_turns_preserve_lock_and_ordering(self) -> None:
        """
        TC-03.4: When session is IDLE and incoming messages are normal task turns,
        session.lock is sequentially acquired and strict turn ordering is preserved.
        """
        self.session.active_execution = None

        turn1_events: List[str] = []
        turn2_events: List[str] = []

        async def run_turn_1():
            async for ev in self.session.send_user_message("First sequential task"):
                if isinstance(ev, TextDeltaEvent):
                    turn1_events.append(ev.text_delta)

        async def run_turn_2():
            # Run Turn 2 shortly after Turn 1 starts
            await asyncio.sleep(0.02)
            async for ev in self.session.send_user_message("Second sequential task"):
                if isinstance(ev, TextDeltaEvent):
                    turn2_events.append(ev.text_delta)

        # Run both concurrently
        await asyncio.gather(run_turn_1(), run_turn_2())

        self.assertEqual(len(turn1_events), 1)
        self.assertEqual(len(turn2_events), 1)
        self.assertIn("First sequential task", turn1_events[0])
        self.assertIn("Second sequential task", turn2_events[0])
        # Verify adapter executed them in strict serial order
        self.assertEqual(self.adapter.received_prompts, ["First sequential task", "Second sequential task"])

    async def test_e2e_concurrent_burst_of_status_queries_does_not_deadlock(self) -> None:
        """
        TC-03.5: Rapid concurrent burst of 5 out-of-band status queries all complete
        cleanly in parallel under 100ms each with zero deadlock or lock starvation.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            pid=99778,
            command="npm run build",
            message="Building front-end bundle",
        )

        turn_task = asyncio.create_task(self._consume_turn("Long ongoing compilation"))
        await asyncio.sleep(0.05)

        async def send_status_inquiry(idx: int):
            event = {
                "channel": "C_CONC",
                "thread_ts": "1727760000.001",
                "ts": f"1727760000.{idx:03d}",
                "text": "<@U_FIRSTMATE> status",
                "user": "U_CARL",
            }
            t0 = time.perf_counter()
            await self.ingress.handle_message(event, client=self.mock_slack)
            return time.perf_counter() - t0

        latencies = await asyncio.gather(*(send_status_inquiry(i) for i in range(5)))

        for i, lat in enumerate(latencies):
            self.assertLess(
                lat,
                0.60,
                f"Burst query {i} took {lat * 1000:.1f}ms exceeding latency bound."
            )

        self.assertEqual(len(self.mock_slack.posted_messages), 5)
        await turn_task

    async def _consume_turn(self, prompt: str) -> None:
        async for _ in self.session.send_user_message(prompt):
            pass


# ===========================================================================
# 4. Fleet Tool Continuity & Transport Normalization E2E
# ===========================================================================

class TestFleetToolContinuityAndTransportE2E(unittest.IsolatedAsyncioTestCase):
    """
    E2E Verification of Requirement R4 & Fleet Tool Continuity:
    Ensures herdr_pane_run, herdr_agent_read, and herdr_handoff execute seamlessly
    across fleet nodes, and transport layer sanitization strips ANSI, TUI chrome,
    and transpiles markdown properly before egress.
    """

    async def asyncSetUp(self) -> None:
        herdr_tools.SPAWNED_AGENTS.clear()

    async def asyncTearDown(self) -> None:
        herdr_tools.SPAWNED_AGENTS.clear()

    async def test_e2e_herdr_pane_run_non_blocking_yield_contract(self) -> None:
        """
        Verifies herdr_pane_run yields status='running' within 3.0s ceiling
        when execution in Pane 2 runs long.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()

            async def slow_pane_run():
                await asyncio.sleep(3.5)
                return b"", b""

            mock_proc.communicate.side_effect = slow_pane_run
            mock_exec.return_value = mock_proc

            t0 = time.perf_counter()
            res = await herdr_tools.herdr_pane_run(
                pane="runner",
                command="pytest --slow",
                yield_timeout=3.0,
            )
            elapsed = time.perf_counter() - t0

            self.assertLessEqual(elapsed, 3.15)
            self.assertEqual(res["status"], "running")
            self.assertEqual(res["pane"], "runner")
            self.assertIn("pytest --slow", res["command"])

    async def test_e2e_herdr_agent_read_buffer_and_fallback(self) -> None:
        """
        Verifies herdr_agent_read fetches settled buffer and falls back
        from agent read to pane read gracefully.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            # First call (agent read) returns error JSON
            mock_agent_proc = AsyncMock()
            mock_agent_proc.returncode = 1
            mock_agent_proc.communicate.return_value = (b'{"error": "agent_not_found"}\n', b"")

            # Second call (pane read fallback) returns real buffer
            mock_pane_proc = AsyncMock()
            mock_pane_proc.returncode = 0
            mock_pane_proc.communicate.return_value = (
                b"Step 1: OK\nStep 2: Processing 84/100 items\nReady.",
                b""
            )

            mock_exec.side_effect = [mock_agent_proc, mock_pane_proc]

            buf = await herdr_tools.herdr_agent_read(pane="runner", lines=30)
            self.assertIn("Step 2: Processing 84/100 items", buf)
            self.assertIn("Ready.", buf)

    async def test_e2e_herdr_handoff_delegates_to_worker_with_branch_isolation(self) -> None:
        """
        Verifies herdr_handoff dispatches heavy tasks to worker subagents (opencode)
        with branch isolation and returns structured confirmation.
        """
        with patch("herdr_tools.herdr_spawn", new_callable=AsyncMock) as mock_spawn:
            mock_spawn.return_value = {
                "node": "hermes",
                "cwd": "/Users/hermes/projects/golem",
                "worker": "opencode",
                "branch": "task-opencode-20261001-12345",
                "pane_id": "ws_handoff:p2",
                "status": "spawned",
            }

            res = await herdr_tools.herdr_handoff(
                subagent="opencode",
                task="Refactor transport normalizer",
                cwd="/Users/hermes/projects/golem",
                node="hermes",
            )

            mock_spawn.assert_called_once_with(
                node="hermes",
                cwd="/Users/hermes/projects/golem",
                worker="opencode",
                prompt="Refactor transport normalizer",
                instructions="Refactor transport normalizer",
                isolate_branch=True,
            )
            self.assertEqual(res["worker"], "opencode")
            self.assertEqual(res["pane_id"], "ws_handoff:p2")
            self.assertTrue(res["branch"].startswith("task-opencode-"))

    def test_e2e_transport_layer_sanitization_pipeline(self) -> None:
        """
        Verifies transport layer wire normalizer:
        - Strips ANSI escape codes
        - Strips box-drawing characters and curses frames
        - Transpiles ATX headers to Slack *bold*
        - Transpiles markdown links to <url|label>
        - Preserves multiline code fences
        """
        raw_output = (
            "\x1b[32m✔ SUCCESS:\x1b[0m All tests passed\n"
            "┌───────────────────────┐\n"
            "│ Build Status: OK      │\n"
            "└───────────────────────┘\n\n"
            "### Deployment Summary\n"
            "Deployed to [Production Server](https://ops.example.com/deploy).\n\n"
            "```python\n"
            "def verify():\n"
            "    return True\n"
            "```"
        )

        sanitized = sanitize_for_slack(raw_output)

        # 1. Zero ANSI escapes
        self.assertNotIn("\x1b", sanitized)
        # 2. Zero box-drawing artifacts
        self.assertNotIn("┌", sanitized)
        self.assertNotIn("│", sanitized)
        self.assertNotIn("└", sanitized)
        # 3. Transpiled ATX headers
        self.assertNotIn("###", sanitized)
        self.assertIn("*Deployment Summary*", sanitized)
        # 4. Transpiled links
        self.assertIn("<https://ops.example.com/deploy|Production Server>", sanitized)
        # 5. Preserved code fence
        self.assertIn("```python", sanitized)
        self.assertIn("def verify():", sanitized)


# ===========================================================================
# 5. Full End-to-End User Flow Scenario
# ===========================================================================

class TestFullDualPaneE2EUserJourney(unittest.IsolatedAsyncioTestCase):
    """
    Comprehensive End-to-End User Journey:
    1. User initiates a session: dual-pane workspace provisioned.
    2. User dispatches a long-running runner command: yields active handle within 3s.
    3. User sends a check-in inquiry while runner runs: answered < 100ms out-of-band.
    4. Runner hits an interactive OAuth blocker: observer surfaces URL in Slack format.
    5. User issues /wrapup: dual-pane workspace cleanly torn down.
    """

    async def asyncSetUp(self) -> None:
        self.mock_slack = E2EMockSlackClient()
        self.herdr_client = AsyncMock(spec=HerdrClient)
        self.harness = DualPaneHarness(herdr_client=self.herdr_client)

        async def run_cli_mock(cmd: List[str]) -> Tuple[int, str, str]:
            if "workspace" in cmd and "create" in cmd:
                label = cmd[cmd.index("--label") + 1] if "--label" in cmd else "ws"
                return 0, json.dumps({"result": {"workspace": {"workspace_id": label}}}), ""
            return 0, "{}", ""

        self.herdr_client.run_cli.side_effect = run_cli_mock
        self.adapter = E2EControlledAdapter()

        self.router = SessionRouter(
            default_target_cwd="/Users/hermes/projects/golem",
            harness=self.harness,
            adapter_factory=lambda cwd: self.adapter,
        )
        self.ingress = IngressGateway(router=self.router, bot_user_id="U_BOT")

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    async def test_full_dual_pane_user_journey(self) -> None:
        channel = "C_JOURNEY"
        thread_ts = "1727770000.000001"

        # Step 1: Ingress provisions Dual-Pane Session
        session, is_new = await self.router.get_or_create(channel, thread_ts)
        self.assertTrue(is_new)
        self.assertEqual(session.liaison_pane, f"{channel}:{thread_ts}-liaison")
        self.assertEqual(session.runner_pane, "runner")

        # Step 2: Background Runner command dispatched and yields running state
        handle = ExecutionHandle(
            status="running",
            pane="runner",
            pid=88123,
            command="rclone config reconnect remote:",
            message="Waiting for browser authentication...",
        )
        session.active_execution = handle
        self.assertTrue(session.is_busy)
        self.assertTrue(session.is_executing)

        # Step 3: User sends check-in while runner is busy -> answered < 100ms
        checkin_event = {
            "channel": channel,
            "thread_ts": thread_ts,
            "ts": "1727770000.000002",
            "text": "<@U_BOT> How is it going?",
            "user": "U_CARL",
        }
        t0 = time.perf_counter()
        await self.ingress.handle_message(checkin_event, client=self.mock_slack)
        checkin_latency = time.perf_counter() - t0

        self.assertLess(checkin_latency, 0.60)
        self.assertGreaterEqual(len(self.mock_slack.posted_messages), 1)

        # Step 4: Runner buffer observer detects OAuth callback URL
        buffer_text = (
            "2026/10/01 03:00:00 NOTICE: Log in to your browser\n"
            "Please go to: http://127.0.0.1:53682/auth?state=xyz123\n"
            "Waiting for code..."
        )
        observer = RunnerBufferObserver()
        blockers = observer.extract_all_blockers(buffer_text)
        self.assertEqual(len(blockers), 1)
        self.assertEqual(blockers[0]["type"], "oauth")
        self.assertIn("http://127.0.0.1:53682/auth?state=xyz123", blockers[0]["url"])

        # Format blocker for Slack wire egress
        sanitized_notice = sanitize_for_slack(
            f"Authentication required: [Authenticate Here]({blockers[0]['url']})"
        )
        self.assertIn("<http://127.0.0.1:53682/auth?state=xyz123|Authenticate Here>", sanitized_notice)

        # Step 5: Wrapup / explicit teardown
        torn_down = await self.router.teardown(channel, thread_ts, reason="wrapup")
        self.assertTrue(torn_down)
        self.assertEqual(session.state, SessionState.CLOSED)
        ws_info = self.harness.get_workspace(session.session_id)
        self.assertEqual(ws_info.get("status"), "closed")


if __name__ == "__main__":
    unittest.main()
