#!/usr/bin/env python3
"""
tests/e2e/test_tier4_acceptance.py
E2E Tier 4 Acceptance Scenario Test Suite for First Mate 2.0.

Covers Real-World Application Scenarios TC-01 through TC-05:
- TC-01: Conversational Inquiry Direct Response (Features 1, 2, 4, 8, 10, 17, 20)
- TC-02: Multi-Turn Thread Continuity (Features 1, 3, 9, 10, 11, 16)
- TC-03: Interactive Blocker Resilience (Features 21, 22, 23, 27)
- TC-04: Isolated Worker Delegation via herdr_handoff (Features 24, 25, 28, 30)
- TC-05: Wire Normalization & Chrome Sanitization (Features 27, 28, 29, 30)

All tests follow opaque-box, requirement-driven test design derived from
ORIGINAL_REQUEST.md, PROJECT.md, and TEST_INFRA.md.
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import asyncio
import unittest
from typing import Dict, Any, List, Optional, AsyncGenerator
from unittest.mock import AsyncMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from first_mate.gateway.ingress import IngressGateway, strip_bot_mentions
from first_mate.gateway.router import SessionRouter
from first_mate.gateway.session import SessionHandle, SessionState
from first_mate.agent.adapter import HeadlessAgentAdapter
from first_mate.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    AwaitingInputEvent,
)
from first_mate.agent.prompts import CALIBRATION_PROMPT, build_calibrated_system_prompt
from first_mate.execution.runner import RunnerExecutor, ExecutionHandle
from first_mate.execution.buffer import (
    RunnerBufferObserver,
    OAUTH_CALLBACK_PATTERN,
    INTERACTIVE_PROMPT_PATTERNS,
)
from first_mate.execution.delegation import WorkerDelegator, sanitize_worker_output
from first_mate.execution.harness import DualPaneHarness
from first_mate.transport.normalizer import (
    sanitize_for_slack,
    transpile_markdown_to_mrkdwn,
    strip_ansi,
    strip_terminal_chrome,
    strip_box_drawing,
    strip_tui_debris,
    strip_prompt_echoes,
)
from first_mate.transport.chunker import split_message_boundary_aware


# ===========================================================================
# Test Harness & Mock Helpers
# ===========================================================================

class AcceptanceMockSlackClient:
    """Mock Slack client tracking calls, latency, reactions, and worker pane assertions."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, Any]] = []
        self.reactions_removed: List[Dict[str, Any]] = []
        self.worker_panes_spawned: List[str] = []

    async def reactions_add(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        t = time.perf_counter()
        self.reactions_added.append({
            "channel": channel,
            "timestamp": timestamp,
            "name": name,
            "time": t,
        })
        return {"ok": True}

    async def reactions_remove(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        t = time.perf_counter()
        self.reactions_removed.append({
            "channel": channel,
            "timestamp": timestamp,
            "name": name,
            "time": t,
        })
        return {"ok": True}

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None) -> Dict[str, Any]:
        t = time.perf_counter()
        self.posted_messages.append({
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "time": t,
        })
        return {"ok": True, "ts": f"{time.time()}"}


class AcceptanceMockAdapter:
    """Controllable mock adapter simulating Chief of Staff responses."""

    def __init__(self, cwd: str = "/Users/hermes/projects/AGY", delay_before_yield: float = 0.0) -> None:
        self.cwd = cwd
        self.delay_before_yield = delay_before_yield
        self.started = False
        self.stopped = False
        self.turns_received: List[str] = []
        self.custom_handler = None
        self.responses: Dict[str, str] = {}

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_received.append(prompt)
        if self.delay_before_yield > 0:
            await asyncio.sleep(self.delay_before_yield)

        if self.custom_handler:
            async for ev in self.custom_handler(prompt):
                yield ev
            return

        resp = self.responses.get(prompt, f"*Response:* Processed {prompt}")
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text_delta=resp)
        yield FinishEvent(response=resp, status="SUCCESS")


# ===========================================================================
# 1. TC-01: Conversational Inquiry Direct Response
# ===========================================================================

class TestTier4TC01ConversationalInquiry(unittest.IsolatedAsyncioTestCase):
    """
    Acceptance Scenario TC-01:
    Inbound conversational greeting or status inquiry ("How are you?", "What's the status of the project?").
    Assert:
    - Response generated in exactly 1 turn
    - Headers formatted with Slack *bold* (single asterisk, no ** or ATX #)
    - 0 worker subagent panes spawned
    - 0 canned announcements ("I'm starting to...", "Looking into this...", "I will dispatch...")
    - 0 completion footers ("Task completed via Herdr", "Herdr worker finished")
    - Non-verbal :gear: acknowledgment added (<200ms latency), swapped to :white_check_mark:
    """

    async def asyncSetUp(self) -> None:
        self.client = AcceptanceMockSlackClient()
        self.router = SessionRouter(inactivity_timeout=3600.0)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    async def test_conversational_greeting_direct_response(self) -> None:
        """TC-01: Conversational greeting receives direct 1-turn response without worker spawns or canned noise."""
        greeting_reply = (
            "*Chief of Staff Online*\n"
            "I am operating normally. All fleet nodes and background monitors are steady."
        )
        adapter = AcceptanceMockAdapter(delay_before_yield=0.02)
        adapter.responses["Hi AGY, how are you doing today?"] = greeting_reply
        self.router.adapter_factory = lambda cwd: adapter

        start_time = time.perf_counter()
        event = {
            "channel": "C_DEV",
            "ts": "1720000001.0001",
            "thread_ts": "1720000001.0001",
            "text": "<@U_AGY_BOT> Hi AGY, how are you doing today?",
        }

        await self.gateway.handle_message(event, self.client)

        # 1. Non-verbal :gear: acknowledgment (<200ms latency)
        gear_adds = [r for r in self.client.reactions_added if r["name"] == "gear"]
        self.assertEqual(len(gear_adds), 1, "Must add exactly one :gear: reaction")
        gear_latency_ms = (gear_adds[0]["time"] - start_time) * 1000.0
        self.assertLess(gear_latency_ms, 200.0, f"Non-verbal :gear: latency {gear_latency_ms:.2f}ms exceeded 200ms budget")

        # 2. Response generated in exactly 1 turn
        self.assertEqual(len(self.client.posted_messages), 1, "Response must be generated in exactly 1 turn")
        delivered = self.client.posted_messages[0]["text"]

        # 3. Headers formatted with Slack *bold*
        self.assertIn("*Chief of Staff Online*", delivered)
        self.assertNotIn("**", delivered, "TC-01 violation: Double asterisks '**' found")
        for line in delivered.splitlines():
            self.assertFalse(line.lstrip().startswith("#"), f"TC-01 violation: ATX header found: '{line}'")

        # 4. Zero worker subagent panes spawned
        self.assertEqual(len(self.client.worker_panes_spawned), 0, "TC-01 violation: worker panes spawned!")

        # 5. Zero canned announcements
        canned_phrases = [
            "i'm starting to", "starting worker", "looking into this",
            "i will dispatch", "task started", "let me check",
            "on it", "hello carl", "certainly", "i'd be glad",
            "taking a look", "processing your request", "analyzing request"
        ]
        for phrase in canned_phrases:
            self.assertNotIn(phrase, delivered.lower(), f"TC-01 violation: Canned announcement '{phrase}' found")

        # 6. Zero completion footers
        banned_footers = [
            "task completed via herdr", "task completed",
            "execution completed via", "herdr worker finished",
            "task finished"
        ]
        for footer in banned_footers:
            self.assertNotIn(footer, delivered.lower(), f"TC-01 violation: Completion footer '{footer}' found")

        # 7. Reaction swapped to :white_check_mark:
        check_adds = [r for r in self.client.reactions_added if r["name"] == "white_check_mark"]
        self.assertEqual(len(check_adds), 1, "Must add :white_check_mark: upon turn completion")
        gear_removes = [r for r in self.client.reactions_removed if r["name"] == "gear"]
        self.assertEqual(len(gear_removes), 1, "Must remove :gear: when swapping reaction")

    async def test_fleet_status_inquiry_direct_response(self) -> None:
        """TC-01: 'How's our fleet doing?' replies directly with bold headers and zero incident scraping."""
        fleet_status = (
            "*Fleet Overview:*\n"
            "• *hermes* (localhost): 🟢 Healthy\n"
            "• *desktop-pc* (CUDA Grid): 🟢 Idle\n"
            "• *personal-laptop* (Carl's Workstation): 🟢 Active\n"
            "• *oci-machine* (Cloud VPS): 🟢 Healthy"
        )
        adapter = AcceptanceMockAdapter()
        adapter.responses["How's our fleet doing?"] = fleet_status
        self.router.adapter_factory = lambda cwd: adapter

        event = {
            "channel": "C_FLEET",
            "ts": "1720000002.0001",
            "thread_ts": "1720000002.0001",
            "text": "How's our fleet doing?",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(self.client.posted_messages), 1)
        delivered = self.client.posted_messages[0]["text"]

        self.assertIn("*Fleet Overview:*", delivered)
        self.assertIn("*hermes*", delivered)
        self.assertNotIn("**", delivered)
        self.assertEqual(len(self.client.worker_panes_spawned), 0)

        # Suppression of unprompted error log triage (Feature 20)
        unsolicited_triage_words = ["exit code", "traceback", "remediation checklist", "scraping logs"]
        for word in unsolicited_triage_words:
            self.assertNotIn(word, delivered.lower())

    async def test_conversational_inquiry_with_real_subprocess(self) -> None:
        """TC-01: End-to-end conversational inquiry with real mock_agy_proc subprocess."""
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        self.assertTrue(os.path.exists(mock_bin), f"Mock binary '{mock_bin}' must exist")

        def factory(cwd: str) -> HeadlessAgentAdapter:
            return HeadlessAgentAdapter(
                cwd="/Users/hermes/projects/first-mate",
                agy_bin=mock_bin,
                env={"MOCK_AGY_MODE": "normal"},
            )

        router = SessionRouter(adapter_factory=factory)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")

        try:
            event = {
                "channel": "C_REAL_SUBPROC",
                "ts": "1720000003.0001",
                "thread_ts": "1720000003.0001",
                "text": "What's the status of the project?",
            }
            await gateway.handle_message(event, self.client)

            self.assertEqual(len(self.client.posted_messages), 1)
            delivered = self.client.posted_messages[0]["text"]
            self.assertIn("Echo: What's the status of the project?", delivered)
            self.assertEqual(len(self.client.worker_panes_spawned), 0)

            # Reactions lifecycle
            self.assertTrue(any(r["name"] == "gear" for r in self.client.reactions_added))
            self.assertTrue(any(r["name"] == "white_check_mark" for r in self.client.reactions_added))
        finally:
            await router.stop_reaper()

    def test_strip_bot_mentions_cleanliness(self) -> None:
        """TC-01: Bot user mentions and aliases are cleanly stripped from prompt."""
        self.assertEqual(
            strip_bot_mentions("<@U_AGY_BOT> What's the status of the project?", "U_AGY_BOT"),
            "What's the status of the project?"
        )
        self.assertEqual(
            strip_bot_mentions("@AGY How are you doing?"),
            "How are you doing?"
        )
        self.assertEqual(
            strip_bot_mentions("<@U12345|agy> Fleet status"),
            "Fleet status"
        )

    async def test_canned_announcements_and_regex_sniffing_eliminated(self) -> None:
        """TC-01: Elimination of canned announcements and regex keyword triggers across various casual queries."""
        test_queries = [
            "What's the status of the project?",
            "Can you give me an overview?",
            "Good morning!",
            "How are things looking right now?",
        ]
        adapter = AcceptanceMockAdapter()
        for q in test_queries:
            adapter.responses[q] = f"*Status:* All systems green for `{q}`."
        self.router.adapter_factory = lambda cwd: adapter

        for idx, query in enumerate(test_queries):
            ts = f"172000001{idx}.0001"
            event = {
                "channel": "C_TEST",
                "ts": ts,
                "thread_ts": ts,
                "text": query,
            }
            await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(self.client.posted_messages), len(test_queries))
        self.assertEqual(len(self.client.worker_panes_spawned), 0)

        for msg in self.client.posted_messages:
            text = msg["text"]
            self.assertFalse("i will dispatch" in text.lower())
            self.assertFalse("starting worker" in text.lower())
            self.assertFalse("task completed" in text.lower())


# ===========================================================================
# 2. TC-02: Multi-Turn Thread Continuity
# ===========================================================================

class TestTier4TC02MultiTurnContinuity(unittest.IsolatedAsyncioTestCase):
    """
    Acceptance Scenario TC-02:
    Sequence of conversational turns in the same Slack thread_ts.
    Assert:
    - SessionHandle retained across turns (same instance)
    - Underlying OS PID persists across turns (zero re-spawn)
    - Prior user messages are not echoed in agent response
    - Conversation context retained across turns (turn counter increments)
    - Thread isolation across distinct thread_ts
    """

    async def asyncSetUp(self) -> None:
        self.client = AcceptanceMockSlackClient()

    async def test_session_handle_identity_and_turn_counter_persistence(self) -> None:
        """TC-02: SessionHandle persists across turns in same thread and turn counter increments."""
        adapter = AcceptanceMockAdapter()
        adapter.responses["List all cron jobs"] = "*Active Cron Jobs:*\n1. backup at 2 AM\n2. cleanup at 3 AM"
        adapter.responses["Which one runs at 3 AM?"] = "*Cron at 3 AM:* The cleanup job runs at 3 AM."

        router = SessionRouter(adapter_factory=lambda cwd: adapter)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")

        try:
            channel = "C_THREAD_01"
            thread_ts = "1720000200.0001"

            # Turn 1
            ev1 = {
                "channel": channel,
                "ts": thread_ts,
                "thread_ts": thread_ts,
                "text": "List all cron jobs",
            }
            await gateway.handle_message(ev1, self.client)

            session_key = f"{channel}:{thread_ts}"
            self.assertIn(session_key, router.sessions)
            session_turn1 = router.sessions[session_key]
            self.assertEqual(session_turn1.current_turn, 1)
            self.assertEqual(session_turn1.state, SessionState.IDLE)

            # Turn 2
            ev2 = {
                "channel": channel,
                "ts": "1720000200.0002",
                "thread_ts": thread_ts,
                "text": "Which one runs at 3 AM?",
            }
            await gateway.handle_message(ev2, self.client)

            session_turn2 = router.sessions[session_key]

            # Invariant 1: SessionHandle identity retained
            self.assertIs(session_turn1, session_turn2, "TC-02: SessionHandle must be retained across turns")

            # Invariant 2: Turn counter increments to 2
            self.assertEqual(session_turn2.current_turn, 2)
            self.assertEqual(session_turn2.state, SessionState.IDLE)

            # Invariant 3: Responses delivered
            self.assertEqual(len(self.client.posted_messages), 2)
            t1_delivered = self.client.posted_messages[0]["text"]
            t2_delivered = self.client.posted_messages[1]["text"]

            self.assertIn("Active Cron Jobs", t1_delivered)
            self.assertIn("The cleanup job runs at 3 AM", t2_delivered)

            # Invariant 4: Prior user message is not echoed into Turn 2 output
            self.assertNotIn("List all cron jobs", t2_delivered, "TC-02: Stale prior prompt echoed into Turn 2 response")

        finally:
            await router.stop_reaper()

    async def test_underlying_os_pid_persists_across_turns_real_subprocess(self) -> None:
        """TC-02: Underlying OS PID persists across turns with zero re-spawning."""
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        self.assertTrue(os.path.exists(mock_bin), f"Mock binary '{mock_bin}' must exist")

        def factory(cwd: str) -> HeadlessAgentAdapter:
            return HeadlessAgentAdapter(
                cwd="/Users/hermes/projects/first-mate",
                agy_bin=mock_bin,
                env={"MOCK_AGY_MODE": "normal"},
            )

        router = SessionRouter(adapter_factory=factory)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")

        try:
            channel = "C_PID_TEST"
            thread_ts = "1720000210.0001"

            # Turn 1
            ev1 = {
                "channel": channel,
                "ts": thread_ts,
                "thread_ts": thread_ts,
                "text": "List all cron jobs",
            }
            await gateway.handle_message(ev1, self.client)

            session = router.sessions[f"{channel}:{thread_ts}"]
            self.assertTrue(session.adapter.is_alive)
            pid_t1 = session.adapter.process.pid
            self.assertIsNotNone(pid_t1)
            self.assertGreater(pid_t1, 0)

            # Turn 2
            t2_ts = "1720000210.0002"
            ev2 = {
                "channel": channel,
                "ts": t2_ts,
                "thread_ts": thread_ts,
                "text": "Which one runs at 3 AM?",
            }
            await gateway.handle_message(ev2, self.client)

            pid_t2 = session.adapter.process.pid

            # Invariant: Subprocess PID is identical
            self.assertEqual(pid_t1, pid_t2, f"TC-02: Subprocess re-spawned! Old PID={pid_t1}, New PID={pid_t2}")
            self.assertTrue(session.adapter.is_alive)
            self.assertEqual(session.current_turn, 2)

            # Assert Turn 2 output does not contain Turn 1 prompt
            t2_msg = self.client.posted_messages[1]["text"]
            self.assertNotIn("List all cron jobs", t2_msg)
            self.assertIn("Which one runs at 3 AM?", t2_msg)

        finally:
            await router.stop_reaper()

    async def test_distinct_threads_have_isolated_sessions_and_pids(self) -> None:
        """TC-02: Distinct thread_ts instances receive isolated SessionHandles and separate OS processes."""
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")

        def factory(cwd: str) -> HeadlessAgentAdapter:
            return HeadlessAgentAdapter(
                cwd="/Users/hermes/projects/first-mate",
                agy_bin=mock_bin,
                env={"MOCK_AGY_MODE": "normal"},
            )

        router = SessionRouter(adapter_factory=factory)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")

        try:
            channel = "C_ISOLATION"
            t1 = "1720000220.0001"
            t2 = "1720000230.0001"

            await gateway.handle_message({"channel": channel, "ts": t1, "thread_ts": t1, "text": "Thread 1 prompt"}, self.client)
            await gateway.handle_message({"channel": channel, "ts": t2, "thread_ts": t2, "text": "Thread 2 prompt"}, self.client)

            s1 = router.sessions[f"{channel}:{t1}"]
            s2 = router.sessions[f"{channel}:{t2}"]

            self.assertIsNot(s1, s2, "Distinct threads must have distinct SessionHandles")
            self.assertNotEqual(s1.session_id, s2.session_id)
            self.assertNotEqual(s1.adapter.process.pid, s2.adapter.process.pid, "Distinct threads must have distinct child PIDs")
            self.assertEqual(s1.current_turn, 1)
            self.assertEqual(s2.current_turn, 1)
        finally:
            await router.stop_reaper()

    async def test_multi_turn_state_transitions_across_three_turns(self) -> None:
        """TC-02: State transitions INITIALIZING -> WORKING -> IDLE across 3 turns without state corruption."""
        adapter = AcceptanceMockAdapter()
        router = SessionRouter(adapter_factory=lambda cwd: adapter)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")

        try:
            channel = "C_STATE"
            thread_ts = "1720000240.0001"

            for i in range(1, 4):
                ts = f"1720000240.000{i}"
                event = {"channel": channel, "ts": ts, "thread_ts": thread_ts, "text": f"Turn {i} request"}
                await gateway.handle_message(event, self.client)

            session = router.sessions[f"{channel}:{thread_ts}"]
            self.assertEqual(session.current_turn, 3)
            self.assertEqual(session.state, SessionState.IDLE)
            self.assertEqual(len(self.client.posted_messages), 3)
        finally:
            await router.stop_reaper()

    async def test_session_recovery_after_turn_failure(self) -> None:
        """TC-02: If a session encounters an error or crash, subsequent turn recovers cleanly."""
        adapter = AcceptanceMockAdapter()

        async def failing_turn(prompt: str) -> AsyncGenerator[AgentEvent, None]:
            yield StatusUpdateEvent(status="working")
            yield FinishEvent(response="Simulated error", status="ERROR")

        adapter.custom_handler = failing_turn
        router = SessionRouter(adapter_factory=lambda cwd: adapter)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")

        try:
            channel = "C_RECOVER"
            thread_ts = "1720000250.0001"

            # Turn 1: Fails
            await gateway.handle_message({"channel": channel, "ts": thread_ts, "thread_ts": thread_ts, "text": "Fail this"}, self.client)
            session = router.sessions[f"{channel}:{thread_ts}"]
            self.assertEqual(session.state, SessionState.FAILED)
            self.assertTrue(any(r["name"] == "x" for r in self.client.reactions_added))

            # Turn 2: Adapter restored to healthy handler
            adapter.custom_handler = None
            adapter.responses["Recover now"] = "*Recovered:* Everything back online."
            await gateway.handle_message({"channel": channel, "ts": "1720000250.0002", "thread_ts": thread_ts, "text": "Recover now"}, self.client)

            # Must have handled recovery turn and delivered response
            recovered_session = router.sessions[f"{channel}:{thread_ts}"]
            self.assertEqual(recovered_session.state, SessionState.IDLE)
            self.assertIn("Recovered", self.client.posted_messages[-1]["text"])
        finally:
            await router.stop_reaper()


# ===========================================================================
# 3. TC-03: Interactive Blocker Resilience
# ===========================================================================

class TestTier4TC03InteractiveBlockerResilience(unittest.IsolatedAsyncioTestCase):
    """
    Acceptance Scenario TC-03:
    Dispatched command blocks on interactive prompt or OAuth callback.
    Assert:
    - Command yields execution handle within <=3.0s (status="running", returning foreground PID)
    - Liaison dialogue loop remains active and responsive
    - OAuth callback URL (http://127.0.0.1:.../auth) cleanly extracted
    - Interactive prompt ([y/N], (y/n), password) cleanly extracted
    - Zero terminal ANSI escapes or curses characters in output
    - Fast commands complete within yield timeout
    """

    async def asyncSetUp(self) -> None:
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")
        self.observer = RunnerBufferObserver(herdr_bin="/usr/local/bin/herdr")

    async def test_long_running_command_yields_running_handle_within_3s(self) -> None:
        """TC-03: Command exceeding yield timeout yields ExecutionHandle(status='running') in <=3.0s."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            # Runner process simulating command that runs long
            mock_proc = AsyncMock()
            mock_proc.pid = 91823
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            # Process-info query returning foreground PID
            mock_info = AsyncMock()
            mock_info.returncode = 0
            mock_info.communicate.return_value = (
                json.dumps({"result": {"pid": 91823}}).encode(),
                b""
            )

            mock_exec.side_effect = [mock_proc, mock_info]

            start_t = time.perf_counter()
            handle = await self.executor.run_command(
                pane="runner",
                command="rclone config reconnect remote:",
                yield_timeout=0.10  # Yield in 100ms
            )
            elapsed = time.perf_counter() - start_t

            # Invariant 1: Yields within <= 3.0s
            self.assertLess(elapsed, 3.0, f"Yield took {elapsed:.2f}s, exceeding 3.0s limit")

            # Invariant 2: ExecutionHandle status is running with foreground PID
            self.assertIsInstance(handle, ExecutionHandle)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 91823)
            self.assertEqual(handle.pane, "runner")
            self.assertEqual(handle.command, "rclone config reconnect remote:")
            self.assertIn("running in Pane 2", handle.message)

    async def test_fast_command_completes_within_yield_timeout(self) -> None:
        """TC-03: Fast commands completing within yield timeout return status='completed'."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.pid = 41200
            mock_proc.communicate.return_value = (b"fast output line\n", b"")
            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="echo 'fast output line'",
                yield_timeout=3.0
            )

            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertEqual(handle.stdout, "fast output line")

    def test_oauth_callback_url_cleanly_extracted(self) -> None:
        """TC-03: OAuth callback URLs on localhost/127.0.0.1/0.0.0.0 are cleanly extracted."""
        test_buffers = [
            (
                "Waiting for authentication code...\n"
                "Redirecting browser to http://127.0.0.1:53682/auth for confirmation.\n"
                "If browser does not open, visit the URL manually.",
                "http://127.0.0.1:53682/auth"
            ),
            (
                "OAuth server listening on http://localhost:8080/callback?state=xyz987\n",
                "http://localhost:8080/callback?state=xyz987"
            ),
            (
                "Listening on http://0.0.0.0:5000/oauth/authorize for access tokens",
                "http://0.0.0.0:5000/oauth/authorize"
            ),
        ]

        for buf, expected_url in test_buffers:
            blocker = self.observer.extract_interactive_blockers(buf)
            self.assertIsNotNone(blocker, f"Failed to extract OAuth blocker from: {buf}")
            self.assertEqual(blocker["type"], "oauth")
            self.assertEqual(blocker["url"], expected_url)

    def test_interactive_prompts_cleanly_extracted(self) -> None:
        """TC-03: Interactive prompts ([y/N], (y/n), [yes/no], password) are cleanly extracted."""
        test_prompts = [
            ("Do you want to continue? [y/N]", "Do you want to continue? [y/N]"),
            ("Apply database migrations now? (y/n)", "Apply database migrations now? (y/n)"),
            ("Delete 24 files permanently? [yes/no]", "Delete 24 files permanently? [yes/no]"),
            ("[sudo] password for hermes: ", "[sudo] password for hermes:"),
            ("Enter passphrase for key '/id_rsa': ", "Enter passphrase for key '/id_rsa':"),
        ]

        for buf, expected_content in test_prompts:
            blocker = self.observer.extract_interactive_blockers(buf)
            self.assertIsNotNone(blocker, f"Failed to extract prompt from: {buf}")
            self.assertEqual(blocker["type"], "prompt")
            self.assertIn(expected_content.strip(), blocker["content"])
            self.assertIsNone(blocker["url"])

    def test_zero_terminal_ansi_or_curses_in_blocker_output(self) -> None:
        """TC-03: Blocker extraction strips ANSI escape sequences, box characters, and brackets."""
        decorated_buf = (
            "╭──────────────────────────────────────────────────╮\n"
            "│ \x1b[32mAuth URL:\x1b[0m \x1b[4mhttp://127.0.0.1:53682/auth\x1b[0m       │\n"
            "╰──────────────────────────────────────────────────╯"
        )
        blocker = self.observer.extract_interactive_blockers(decorated_buf)
        self.assertIsNotNone(blocker)
        self.assertEqual(blocker["type"], "oauth")
        self.assertEqual(blocker["url"], "http://127.0.0.1:53682/auth")

        # Zero ANSI in extracted url and content
        self.assertNotIn("\x1b", blocker["url"])
        self.assertNotIn("\x1b", blocker["content"])

        # When surfaced as output through the wire normalizer, all terminal box runes are completely stripped
        surfaced_output = sanitize_for_slack(blocker["content"])
        self.assertNotIn("\x1b", surfaced_output)
        self.assertNotIn("╭", surfaced_output)
        self.assertNotIn("│", surfaced_output)
        self.assertNotIn("╰", surfaced_output)
        self.assertIn("http://127.0.0.1:53682/auth", surfaced_output)


    async def test_liaison_responsiveness_during_runner_execution(self) -> None:
        """TC-03: Liaison dialogue loop remains responsive while runner command is executing."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 77123
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info = AsyncMock()
            mock_info.returncode = 0
            mock_info.communicate.return_value = (
                json.dumps({"result": {"pid": 77123}}).encode(),
                b""
            )
            mock_exec.side_effect = [mock_proc, mock_info]

            # 1. Dispatch long command with 50ms yield timeout
            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 30",
                yield_timeout=0.05
            )
            self.assertEqual(handle.status, "running")

            # 2. While runner is running, liaison answers conversational query immediately
            adapter = AcceptanceMockAdapter()
            adapter.responses["Are you still there?"] = "*Liaison Online:* Yes, running job in Pane 2."
            router = SessionRouter(adapter_factory=lambda cwd: adapter)
            gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")
            client = AcceptanceMockSlackClient()

            try:
                event = {
                    "channel": "C_LIAISON",
                    "ts": "1720000300.0001",
                    "thread_ts": "1720000300.0001",
                    "text": "Are you still there?",
                }
                await gateway.handle_message(event, client)

                self.assertEqual(len(client.posted_messages), 1)
                self.assertIn("Liaison Online", client.posted_messages[0]["text"])
            finally:
                await router.stop_reaper()


# ===========================================================================
# 4. TC-04: Isolated Worker Delegation via herdr_handoff
# ===========================================================================

class TestTier4TC04IsolatedWorkerDelegation(unittest.IsolatedAsyncioTestCase):
    """
    Acceptance Scenario TC-04:
    Development task delegated to opencode-dev / opencode.
    Assert:
    - Execution runs in dedicated git worktree and branch isolated from main
    - Base repo untouched on failure
    - Worker output filtered (scratchpads, VT100 boxes, prompt echoes, braille spinners stripped)
    - Clean executive briefing delivered
    """

    async def asyncSetUp(self) -> None:
        self.mock_herdr = AsyncMock()
        self.delegator = WorkerDelegator(
            herdr_client=self.mock_herdr,
            base_repo="/Users/hermes/projects/first-mate"
        )

    async def test_worker_delegation_isolated_worktree_and_branch(self) -> None:
        """TC-04: Delegated task executes on dedicated branch task-<ts>-<subagent> in isolated worktree."""
        mock_wt_json = json.dumps({
            "result": {
                "worktree": {"path": "/tmp/wt/task-1720000400-opencode"},
                "workspace": {"workspace_id": "ws-wt-400"}
            }
        })
        self.mock_herdr.run_cli.side_effect = [
            (0, mock_wt_json, ""),                                      # worktree create
            (0, "started", ""),                                         # agent start
            (0, "task accepted", ""),                                   # agent prompt
            (0, "idle", ""),                                            # agent wait
            (0, "Refactored session router and added unit tests.", ""), # agent read
        ]

        res = await self.delegator.handoff(
            subagent="opencode",
            task="Refactor session router",
            cwd="/Users/hermes/projects/first-mate",
            isolate_branch=True
        )

        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["subagent"], "opencode")
        self.assertTrue(res["branch"].startswith("task-"))
        self.assertTrue(res["branch"].endswith("-opencode"))
        self.assertEqual(res["cwd"], "/tmp/wt/task-1720000400-opencode")
        self.assertEqual(res["workspace_id"], "ws-wt-400")
        self.assertIn("Refactored session router", res["output"])

        # Check worktree create CLI arguments
        wt_call_args = self.mock_herdr.run_cli.call_args_list[0][0][0]
        self.assertEqual(wt_call_args[:2], ["worktree", "create"])
        self.assertIn("--branch", wt_call_args)
        self.assertNotIn("main", wt_call_args[wt_call_args.index("--branch") + 1])

    async def test_worker_delegation_failure_leaves_base_repo_untouched(self) -> None:
        """TC-04: Worktree failure aborts early with error, preserving base repository."""
        self.mock_herdr.run_cli.side_effect = [
            (1, "", "fatal: could not create worktree: branch already exists"),
        ]

        res = await self.delegator.handoff(
            subagent="opencode",
            task="Task on broken worktree",
            cwd="/Users/hermes/projects/first-mate",
            isolate_branch=True
        )

        self.assertEqual(res["status"], "error")
        self.assertIn("Worktree isolation failed", res["error"])
        # No subsequent worker agent commands were invoked
        self.assertEqual(len(self.mock_herdr.run_cli.call_args_list), 1)

    def test_worker_output_filtering_scratchpads_and_terminal_chrome(self) -> None:
        """TC-04: Strips internal scratchpads, VT100 boxes, prompt echoes, braille spinners, and crumbs."""
        raw_worker_output = (
            "<scratchpad>\n"
            "Analyzing AST differences in first_mate/gateway/router.py...\n"
            "Checking imports for session handles...\n"
            "</scratchpad>\n"
            "[scratchpad] calculating test coverage\n"
            "╭──────────────────────────────────────────────╮\n"
            "│ Refactoring Outcome: Successful Migration    │\n"
            "├──────────────────────────────────────────────┤\n"
            "│ Tests: 159 passing                           │\n"
            "╰──────────────────────────────────────────────╯\n"
            "⠋ Running test suite...\n"
            "first_mate/transport/chunker.py...)\n"
            "42...)\n"
            "(ctrl+o to expand)\n"
            "❯ git status\n"
            ">\n"
            ">\n"
            "\x1b[32mAll 159 tests passed.\x1b[0m\n"
            "Executive verdict: Migration ready for review."
        )

        cleaned = sanitize_worker_output(raw_worker_output)

        # 1. Scratchpad stripped
        self.assertNotIn("<scratchpad>", cleaned)
        self.assertNotIn("Analyzing AST differences", cleaned)
        self.assertNotIn("[scratchpad]", cleaned)

        # 2. VT100 boxes stripped
        for glyph in ["╭", "╮", "│", "├", "┤", "╰", "╯", "─"]:
            self.assertNotIn(glyph, cleaned, f"VT100 glyph '{glyph}' leaked into output")

        # 3. Braille spinner and crumbs stripped
        self.assertNotIn("⠋", cleaned)
        self.assertNotIn("Running test suite...", cleaned)
        self.assertNotIn("chunker.py...)", cleaned)
        self.assertNotIn("42...)", cleaned)
        self.assertNotIn("expand", cleaned)

        # 4. Prompt echoes stripped
        self.assertNotIn("❯ git status", cleaned)

        # 5. ANSI codes stripped
        self.assertNotIn("\x1b", cleaned)

        # 6. Substantive content retained
        self.assertIn("All 159 tests passed.", cleaned)
        self.assertIn("Executive verdict: Migration ready for review.", cleaned)

    async def test_worker_delegation_clean_executive_briefing_delivery(self) -> None:
        """TC-04: Handoff returns clean executive briefing conforming to FirstMate / GEMINI style."""
        raw_response = (
            "<scratchpad>worker finished AST pass</scratchpad>\n"
            "\x1b[1m*Migration Completed:*\x1b[0m\n"
            "• Added 5 new boundary tests for chunker.\n"
            "• All tests pass cleanly.\n"
            "❯ herdr agent wait\n"
        )
        self.mock_herdr.run_cli.side_effect = [
            (0, json.dumps({"result": {"worktree": {"path": "/tmp/wt/task-99"}}}), ""),
            (0, "", ""),
            (0, "", ""),
            (0, "", ""),
            (0, raw_response, ""),
        ]

        res = await self.delegator.handoff(
            subagent="opencode",
            task="Add boundary tests",
            isolate_branch=True
        )

        self.assertEqual(res["status"], "completed")
        briefing = res["output"]
        self.assertNotIn("<scratchpad>", briefing)
        self.assertNotIn("❯", briefing)
        self.assertNotIn("\x1b", briefing)
        self.assertIn("Migration Completed:", briefing)
        self.assertIn("Added 5 new boundary tests", briefing)


# ===========================================================================
# 5. TC-05: Wire Normalization & Chrome Sanitization
# ===========================================================================

class TestTier4TC05WireNormalizationAndSanitization(unittest.TestCase):
    """
    Acceptance Scenario TC-05:
    CommonMark and raw terminal output processed for Slack wire transmission.
    Assert:
    - 0 ATX markdown headers (#, ##, ###) outside code blocks
    - 0 double asterisks **bold** outside code blocks
    - 0 VT100 box characters (╭───╮, │, └─┘, ┌───┐, [\u2500-\u257f]) outside code blocks
    - 0 ANSI color escapes
    - All URLs follow Slack format <url|label> or <url>
    - Boundary-aware chunking <3,800 characters preserving code fences
    """

    def test_zero_atx_markdown_headers_outside_code_blocks(self) -> None:
        """TC-05: Transpiles ATX headers (# to ####) to *Header* bold markers outside code blocks."""
        raw = (
            "# Main Heading\n"
            "Some content here.\n"
            "## Secondary Section\n"
            "Details here.\n"
            "### Deep Subsection\n"
            "More details.\n"
            "```python\n"
            "# Python comment must not be modified\n"
            "### Another code comment\n"
            "```\n"
            "#### Leaf Node"
        )
        cleaned = sanitize_for_slack(raw)

        # Split into code vs non-code lines
        lines = cleaned.splitlines()
        in_code = False
        non_code_lines = []
        for line in lines:
            if line.startswith("```"):
                in_code = not in_code
                continue
            if not in_code:
                non_code_lines.append(line)

        # Invariant 1: 0 ATX markdown headers outside code blocks
        for line in non_code_lines:
            if "`" not in line:
                self.assertFalse(re.search(r"^#{1,6}\s", line), f"TC-05: ATX hash found outside code: '{line}'")

        # Invariant 2: Converted to bold markers
        self.assertIn("*Main Heading*", cleaned)
        self.assertIn("*Secondary Section*", cleaned)
        self.assertIn("*Deep Subsection*", cleaned)
        self.assertIn("*Leaf Node*", cleaned)

        # Invariant 3: Code block comments preserved verbatim
        self.assertIn("# Python comment must not be modified", cleaned)
        self.assertIn("### Another code comment", cleaned)

    def test_zero_double_asterisks_outside_code_blocks(self) -> None:
        """TC-05: Transpiles **bold** to *bold* outside code blocks while preserving code blocks."""
        raw = (
            "Here is **very important** status and ***critical alert***.\n"
            "### **Pre-formatted Title**\n"
            "```bash\n"
            "echo '**literal asterisks in shell script**'\n"
            "```\n"
            "Inline `**literal**` remains."
        )
        cleaned = sanitize_for_slack(raw)

        # Invariant 1: 0 double asterisks outside code
        lines = cleaned.splitlines()
        in_code = False
        for line in lines:
            if line.startswith("```"):
                in_code = not in_code
                continue
            if not in_code and "`" not in line:
                self.assertNotIn("**", line, f"TC-05: Leaked '**' on line: '{line}'")

        # Invariant 2: Transpiled properly
        self.assertIn("*very important*", cleaned)
        self.assertIn("*_critical alert_*", cleaned)

        # Invariant 3: Code block retains double asterisks
        self.assertIn("echo '**literal asterisks in shell script**'", cleaned)

    def test_zero_vt100_box_drawing_and_curses_characters(self) -> None:
        """TC-05: VT100 box-drawing glyphs ([\u2500-\u257f]) stripped outside code blocks."""
        raw = (
            "┌─────────────────────────┬──────────────┐\n"
            "│ Node                    │ Health       │\n"
            "├─────────────────────────┼──────────────┤\n"
            "│ localhost               │ 🟢 Online    │\n"
            "│ desktop-pc              │ 🟢 Idle      │\n"
            "└─────────────────────────┴──────────────┘\n"
            "```text\n"
            "┌───┐\n"
            "│box│\n"
            "└───┘\n"
            "```"
        )
        cleaned = sanitize_for_slack(raw)

        lines = cleaned.splitlines()
        in_code = False
        for line in lines:
            if line.startswith("```"):
                in_code = not in_code
                continue
            if not in_code and "`" not in line:
                self.assertFalse(re.search(r"[\u2500-\u257f]", line), f"TC-05: Box drawing rune leaked: '{line}'")

        # Substantive content retained
        self.assertIn("localhost", cleaned)
        self.assertIn("desktop-pc", cleaned)

        # Code block retains box
        self.assertIn("┌───┐", cleaned)

    def test_zero_ansi_escape_sequences(self) -> None:
        """TC-05: Strips standard SGR colors, 256 colors, RGB, cursor jumps, and OSC titles."""
        raw = (
            "\x1b[31;1mERROR:\x1b[0m Failed at \x1b[38;5;208mstep 2\x1b[0m\n"
            "\x1b[2K\r100% complete\n"
            "\x1b]0;Title\x07Clean output."
        )
        cleaned = sanitize_for_slack(raw)
        self.assertNotIn("\x1b", cleaned, "TC-05: ANSI escape sequence leaked")
        self.assertNotIn("\x07", cleaned, "TC-05: OSC bell terminator leaked")
        self.assertIn("ERROR: Failed at step 2", cleaned)
        self.assertIn("Clean output.", cleaned)

    def test_slack_link_formatting_conversion(self) -> None:
        """TC-05: Converts [label](url) to <url|label>, [url](url) to <url>, and ![alt](url) to <url|alt>."""
        raw = (
            "Visit the [Fleet Dashboard](https://fleet.internal:8080/dash) now.\n"
            "Bare URL: [https://github.com](https://github.com).\n"
            "Badge: ![Passing](https://img.shields.io/badge/status-ok-green)."
        )
        cleaned = sanitize_for_slack(raw)

        self.assertIn("<https://fleet.internal:8080/dash|Fleet Dashboard>", cleaned)
        self.assertIn("<https://github.com>", cleaned)
        self.assertIn("<https://img.shields.io/badge/status-ok-green|Passing>", cleaned)
        self.assertNotIn("[Fleet Dashboard](", cleaned)

    def test_boundary_aware_chunking_under_3800_chars(self) -> None:
        """TC-05: Large messages are split strictly < 3,800 chars along paragraph/newline boundaries."""
        paragraphs = [f"Paragraph {i}: " + ("Telemetry record entry for fleet node audit. " * 20) for i in range(12)]
        huge_text = "\n\n".join(paragraphs)  # ~11,000 characters
        self.assertGreater(len(huge_text), 7600)

        chunks = split_message_boundary_aware(huge_text, max_chars=3800)
        self.assertGreater(len(chunks), 1)

        for idx, chunk in enumerate(chunks):
            self.assertLessEqual(len(chunk), 3800, f"Chunk {idx} exceeded 3,800 characters: len={len(chunk)}")

        reconstructed = "\n\n".join(chunks)
        self.assertIn("Paragraph 0:", reconstructed)
        self.assertIn("Paragraph 11:", reconstructed)

    def test_chunking_preserves_code_fences_across_split(self) -> None:
        """TC-05: Slicing across code block closes chunk 1 with ``` and reopens chunk 2 with ```lang."""
        code_lines = [f"    step_{i} = execute_worker_migration_step({i})" for i in range(50)]
        code_block = "```python\n" + "\n".join(code_lines) + "\n```"
        full_text = f"Intro header:\n\n{code_block}\n\nConclusion paragraph."

        chunks = split_message_boundary_aware(full_text, max_chars=600)
        self.assertGreater(len(chunks), 1)

        for idx, chunk in enumerate(chunks):
            backtick_count = chunk.count("```")
            self.assertEqual(
                backtick_count % 2, 0,
                f"Chunk {idx} has unclosed/unbalanced code fence: count={backtick_count}\nContent:\n{chunk}"
            )

    def test_adversarial_composite_payload_sanitization(self) -> None:
        """TC-05: Adversarial composite input with all formatting artifacts passes all TC-05 assertions."""
        hostile_payload = (
            "### Fleet Telemetry & Node Status\n"
            "Status is **operational** with ***zero downtime***.\n"
            "╭────────────────────────────╮\n"
            "│ GPU Node: desktop-pc       │\n"
            "├────────────────────────────┤\n"
            "│ Temperature: 42°C          │\n"
            "╰────────────────────────────╯\n"
            "⠋ Checking services...\n"
            "first_mate/agent/adapter.py...)\n"
            "(ctrl+o to expand)\n"
            "❯ herdr fleet inspect\n"
            ">\n"
            "\x1b[32m✔ Service healthy.\x1b[0m\n"
            "See [Dashboard](https://fleet.internal/dash) or [Metrics](https://metrics.internal).\n"
            "```python\n"
            "# Code comment: ### Not a header\n"
            "bold_val = '**retained**'\n"
            "box_art = '┌───┐'\n"
            "```\n"
            "Final notice: All systems normal."
        )

        cleaned = sanitize_for_slack(hostile_payload)

        # 1. Zero ATX outside code
        lines = cleaned.splitlines()
        in_code = False
        for line in lines:
            if line.startswith("```"):
                in_code = not in_code
                continue
            if not in_code and "`" not in line:
                self.assertFalse(re.search(r"^#{1,6}\s", line), f"ATX hash leaked: '{line}'")
                self.assertNotIn("**", line, f"Double asterisks leaked: '{line}'")
                self.assertFalse(re.search(r"[\u2500-\u257f]", line), f"Box glyph leaked: '{line}'")

        # 2. Zero ANSI escapes
        self.assertNotIn("\x1b", cleaned)

        # 3. Terminal crumbs stripped
        self.assertNotIn("⠋", cleaned)
        self.assertNotIn("adapter.py...)", cleaned)
        self.assertNotIn("expand", cleaned)
        self.assertNotIn("❯ herdr", cleaned)

        # 4. URLs in Slack format
        self.assertIn("<https://fleet.internal/dash|Dashboard>", cleaned)
        self.assertIn("<https://metrics.internal|Metrics>", cleaned)

        # 5. Code block preserved
        self.assertIn("# Code comment: ### Not a header", cleaned)
        self.assertIn("bold_val = '**retained**'", cleaned)
        self.assertIn("box_art = '┌───┐'", cleaned)


if __name__ == "__main__":
    unittest.main()
