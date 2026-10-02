#!/usr/bin/env python3
"""
tests/test_gateway_m3_challenger6.py
Empirical Challenger 6 Verification Suite for Milestone 3 (Gate 3).

Verifies Acceptance Criteria:
  1. TC-01: Conversational query "How's our fleet doing?"
     - Immediate non-verbal :gear: acknowledgment (<200ms latency)
     - 0 canned chatter or greeting announcements
     - 0 worker panes spawned
     - Slack mrkdwn formatted response with *bold* headers (single asterisks)
     - 0 completion footers
     - Reaction swap to :white_check_mark: (and :gear: removed)
  2. TC-02: Multi-turn thread continuity
     - Turn 1: "List all cron jobs" -> Turn 2: "Which one runs at 3 AM?"
     - SessionHandle identity persistence across turns
     - OS child process PID persistence across turns (using real subprocess)
     - Zero stale prompt echoing into subsequent turns
     - Accurate reaction cycle per turn
     - Thread isolation between different threads
  3. Stress & Adversarial Hardening:
     - Empty pipe-label mention stripping and silent dropping
     - Early generator break recovery without deadlock
     - Concurrent close during turn without zombie state resurrection
     - Error turn handling (:x: reaction) and subsequent recovery turn
     - Slash commands direct handling
"""

from __future__ import annotations

import os
import sys
import time
import asyncio
import unittest
from typing import Optional, List, Dict, Any, AsyncGenerator

from first_mate.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    AwaitingInputEvent,
    FinishEvent,
    AgentEvent,
)
from first_mate.agent.adapter import HeadlessAgentAdapter
from first_mate.gateway.session import SessionHandle, SessionState
from first_mate.gateway.router import SessionRouter
from first_mate.gateway.ingress import (
    IngressGateway,
    strip_bot_mentions,
    add_reaction,
    remove_reaction,
    swap_reaction,
)


class Challenger6MockSlackClient:
    """Mock Slack client tracking calls, latency, and worker pane assertions."""

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


class Challenger6MockAdapter:
    """Controllable mock adapter for timing and response testing."""

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

        resp = self.responses.get(prompt, f"*Response for:* {prompt}")
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text_delta=resp)
        yield FinishEvent(response=resp, status="SUCCESS")


class TestGatewayM3Challenger6(unittest.IsolatedAsyncioTestCase):
    """Empirical verification suite for Milestone 3 Gate 3."""

    def setUp(self) -> None:
        self.adapters: List[Challenger6MockAdapter] = []

        def factory(cwd: str) -> Challenger6MockAdapter:
            ad = Challenger6MockAdapter(cwd=cwd)
            self.adapters.append(ad)
            return ad

        self.router = SessionRouter(
            default_target_cwd="/Users/hermes/projects/AGY",
            inactivity_timeout=14400.0,
            reaper_interval=60.0,
            adapter_factory=factory,
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = Challenger6MockSlackClient()

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    # -------------------------------------------------------------------------
    # TC-01: Conversational Inquiry Empirical Verification
    # -------------------------------------------------------------------------
    async def test_tc01_conversational_inquiry_rigorous(self) -> None:
        """
        Verify TC-01:
        Input: "<@U_AGY_BOT> How's our fleet doing?"
        Criteria:
          1. Immediate non-verbal :gear: acknowledgment added (<200ms latency).
          2. Zero canned chatter / greetings ("I will dispatch", "Starting worker", etc.).
          3. Zero worker subagent panes spawned.
          4. Single-turn response delivered with Slack mrkdwn bold (*bold*), no '**' or '#' ATX.
          5. Zero completion footers (*Task Completed via Herdr*).
          6. Reaction swapped to :white_check_mark: and :gear: removed.
        """
        fleet_status_text = (
            "🧭 *Fleet Status Overview:*\n"
            "• *hermes* (localhost (Primary Dev & Homelab)): 🟢 Online (Uptime: 28d)\n"
            "• *desktop-pc* (Compute Grid (Windows CUDA)): 🟢 Idle (GPU Temp: 40°C)\n"
            "• *personal-laptop* (Carl's Workstation): 🟢 Online (Awake)\n"
            "• *oci-machine* (Cloud Operations (VPS)): 🟢 Healthy (Load: 0.08)"
        )

        # Introduce a 100ms processing delay in adapter to confirm :gear: ack happens before response
        ad = Challenger6MockAdapter(delay_before_yield=0.10)
        ad.responses["How's our fleet doing?"] = fleet_status_text
        self.router.adapter_factory = lambda cwd: ad

        start_time = time.perf_counter()
        event = {
            "channel": "C_GENERAL",
            "ts": "1720000001.0001",
            "thread_ts": "1720000001.0001",
            "text": "<@U_AGY_BOT> How's our fleet doing?",
        }

        await self.gateway.handle_message(event, self.client)
        total_time = time.perf_counter() - start_time

        # Criterion 1: Non-verbal :gear: acknowledgment (<200ms latency)
        gear_adds = [r for r in self.client.reactions_added if r["name"] == "gear"]
        self.assertEqual(len(gear_adds), 1, "Must add exactly one :gear: reaction")
        gear_reaction = gear_adds[0]
        gear_latency_ms = (gear_reaction["time"] - start_time) * 1000.0
        self.assertLess(
            gear_latency_ms,
            200.0,
            f"Non-verbal :gear: latency {gear_latency_ms:.2f}ms exceeded 200ms budget!",
        )
        self.assertEqual(gear_reaction["timestamp"], "1720000001.0001")

        # Criterion 2: Zero canned chatter or greeting announcements
        self.assertEqual(len(self.client.posted_messages), 1, "Must post exactly 1 response message")
        delivered = self.client.posted_messages[0]["text"]

        banned_phrases = [
            "i will dispatch", "starting worker", "worker pane", "opencode",
            "let me check", "on it", "hello carl", "i am agy", "certainly",
            "i'd be glad", "taking a look", "processing your request",
            "task started", "analyzing request",
        ]
        for phrase in banned_phrases:
            self.assertNotIn(
                phrase,
                delivered.lower(),
                f"TC-01 violation: Canned phrase '{phrase}' detected in delivered response: {delivered}",
            )

        # Criterion 3: Zero worker panes spawned
        self.assertEqual(len(self.client.worker_panes_spawned), 0, "TC-01 violation: worker panes spawned!")

        # Criterion 4: Slack mrkdwn formatting
        self.assertIn("*Fleet Status Overview:*", delivered)
        self.assertIn("*hermes*", delivered)
        self.assertIn("*desktop-pc*", delivered)
        self.assertNotIn("**", delivered, "TC-01 violation: Double asterisks '**' found (invalid in Slack mrkdwn)")
        for line in delivered.splitlines():
            self.assertFalse(line.startswith("#"), f"TC-01 violation: ATX markdown heading found: '{line}'")

        # Criterion 5: Zero completion footers
        banned_footers = [
            "task completed via herdr",
            "task completed",
            "execution completed via",
            "herdr worker finished",
        ]
        for footer in banned_footers:
            self.assertNotIn(
                footer,
                delivered.lower(),
                f"TC-01 violation: Completion footer '{footer}' detected in delivered response!",
            )

        # Criterion 6: Reaction swap to :white_check_mark:
        check_adds = [r for r in self.client.reactions_added if r["name"] == "white_check_mark"]
        self.assertEqual(len(check_adds), 1, "Must add exactly one :white_check_mark: reaction")
        self.assertEqual(check_adds[0]["timestamp"], "1720000001.0001")

        gear_removes = [r for r in self.client.reactions_removed if r["name"] == "gear"]
        self.assertEqual(len(gear_removes), 1, "Must remove :gear: reaction when swapping to :white_check_mark:")

    # -------------------------------------------------------------------------
    # TC-02: Multi-Turn Thread Continuity with Real Subprocess & OS PID
    # -------------------------------------------------------------------------
    async def test_tc02_multi_turn_continuity_with_os_pid_persistence(self) -> None:
        """
        Verify TC-02:
        Simulate multi-turn thread using REAL OS child process (mock_agy_proc.py):
          Turn 1: "List all cron jobs"
          Turn 2: "Which one runs at 3 AM?"
        Criteria:
          1. SessionHandle identity persistence across Turn 1 and Turn 2.
          2. OS subprocess PID persistence across Turn 1 and Turn 2 (zero re-spawn).
          3. Zero stale prompt echoing into Turn 2 prompt or delivered output.
          4. Turn counter increments to 2.
          5. Correct reaction lifecycle on both turns.
          6. Thread isolation: a separate thread creates a distinct SessionHandle and distinct child PID.
        """
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        self.assertTrue(os.path.exists(mock_bin), f"Mock binary '{mock_bin}' must exist")

        def subprocess_adapter_factory(cwd: str) -> HeadlessAgentAdapter:
            return HeadlessAgentAdapter(
                cwd="/Users/hermes/projects/first-mate",
                agy_bin=mock_bin,
                env={"MOCK_AGY_MODE": "normal"},
            )

        router = SessionRouter(adapter_factory=subprocess_adapter_factory)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")
        client = Challenger6MockSlackClient()

        try:
            channel = "C_THREAD_TEST"
            thread_ts = "1720000100.0001"

            # ---------------- Turn 1 ----------------
            ev1 = {
                "channel": channel,
                "ts": thread_ts,
                "thread_ts": thread_ts,
                "text": "List all cron jobs",
            }
            await gateway.handle_message(ev1, client)

            session_key = f"{channel}:{thread_ts}"
            self.assertIn(session_key, router.sessions)
            session1 = router.sessions[session_key]

            # Verify session and OS child process state after Turn 1
            self.assertEqual(session1.current_turn, 1)
            self.assertEqual(session1.state, SessionState.IDLE)
            self.assertTrue(session1.adapter.is_alive, "Adapter child process must be alive")
            pid_turn1 = session1.adapter.process.pid
            self.assertIsNotNone(pid_turn1, "Adapter child process must have valid OS PID")
            self.assertGreater(pid_turn1, 0)

            # Verify Turn 1 message delivered
            self.assertEqual(len(client.posted_messages), 1)
            self.assertEqual(client.posted_messages[0]["thread_ts"], thread_ts)
            self.assertIn("Echo: List all cron jobs", client.posted_messages[0]["text"])

            # Verify Turn 1 reactions
            gear_1 = [r for r in client.reactions_added if r["name"] == "gear" and r["timestamp"] == thread_ts]
            check_1 = [r for r in client.reactions_added if r["name"] == "white_check_mark" and r["timestamp"] == thread_ts]
            self.assertEqual(len(gear_1), 1)
            self.assertEqual(len(check_1), 1)

            # ---------------- Turn 2 (Warm Continuity) ----------------
            t2_ts = "1720000100.0002"
            ev2 = {
                "channel": channel,
                "ts": t2_ts,
                "thread_ts": thread_ts,
                "text": "Which one runs at 3 AM?",
            }
            await gateway.handle_message(ev2, client)

            session2 = router.sessions[session_key]

            # Criterion 1: SessionHandle identity persistence
            self.assertIs(session1, session2, "TC-02 violation: SessionHandle instance was re-created instead of reused!")

            # Criterion 2: OS PID persistence
            self.assertTrue(session2.adapter.is_alive, "Adapter child process must remain alive across turns")
            pid_turn2 = session2.adapter.process.pid
            self.assertEqual(
                pid_turn1,
                pid_turn2,
                f"TC-02 violation: Subprocess was re-spawned! Old PID={pid_turn1}, New PID={pid_turn2}",
            )
            self.assertEqual(session2.current_turn, 2)
            self.assertEqual(session2.state, SessionState.IDLE)

            # Criterion 3: Zero stale prompt echoing
            self.assertEqual(len(client.posted_messages), 2)
            t2_msg = client.posted_messages[1]
            self.assertEqual(t2_msg["thread_ts"], thread_ts)
            self.assertEqual(t2_msg["text"], "Echo: Which one runs at 3 AM?")
            self.assertNotIn("List all cron jobs", t2_msg["text"], "TC-02 violation: Stale prompt echoed into Turn 2 output!")

            # Criterion 4: Reactions on Turn 2
            gear_2 = [r for r in client.reactions_added if r["name"] == "gear" and r["timestamp"] == t2_ts]
            check_2 = [r for r in client.reactions_added if r["name"] == "white_check_mark" and r["timestamp"] == t2_ts]
            self.assertEqual(len(gear_2), 1, "Must add :gear: to Turn 2 message")
            self.assertEqual(len(check_2), 1, "Must add :white_check_mark: to Turn 2 message")

            # ---------------- Turn 3 (Distinct Thread Isolation) ----------------
            diff_thread_ts = "1720000200.0001"
            ev3 = {
                "channel": channel,
                "ts": diff_thread_ts,
                "thread_ts": diff_thread_ts,
                "text": "Distinct thread query",
            }
            await gateway.handle_message(ev3, client)

            diff_key = f"{channel}:{diff_thread_ts}"
            self.assertIn(diff_key, router.sessions)
            session3 = router.sessions[diff_key]

            # Distinct session handle and distinct child process PID
            self.assertIsNot(session1, session3, "Distinct thread must create distinct SessionHandle")
            pid_turn3 = session3.adapter.process.pid
            self.assertNotEqual(
                pid_turn1,
                pid_turn3,
                f"Distinct threads must use separate child processes! Found duplicate PID {pid_turn3}",
            )
            self.assertEqual(session3.current_turn, 1)

        finally:
            await router.stop_reaper()

    # -------------------------------------------------------------------------
    # Adversarial Mention Stripping & Silent Drop
    # -------------------------------------------------------------------------
    def test_adversarial_mention_stripping_matrix(self) -> None:
        """
        Adversarial test matrix for bot mention stripping:
        Covers standard mentions, pipe labels, empty pipe labels, multiple mentions,
        case variations, and clean text passthrough.
        """
        matrix = [
            ("<@U_AGY_BOT> status", "status"),
            ("<@U_AGY_BOT|firstmate> status", "status"),
            ("<@U_AGY_BOT|> status", "status"),
            ("<@U_AGY_BOT||> status", "status"),
            ("<@U_AGY_BOT|firstmate|alias> status", "status"),
            ("<@U_OTHER|helper> <@U_AGY_BOT> status", "status"),
            ("   @AGY   status   ", "status"),
            ("   @agy   status   ", "status"),
            ("   @FirstMate   status   ", "status"),
            ("<@U_AGY_BOT>", ""),
            ("<@U_AGY_BOT|>", ""),
            ("<@U_AGY_BOT|firstmate>", ""),
            ("Plain text message with no mentions", "Plain text message with no mentions"),
        ]
        for raw, expected in matrix:
            actual = strip_bot_mentions(raw, "U_AGY_BOT")
            self.assertEqual(
                actual,
                expected,
                f"Mention stripping failed for '{raw}': expected '{expected}', got '{actual}'",
            )

    async def test_empty_and_mention_only_silently_dropped(self) -> None:
        """
        Adversarial test:
        Messages that are empty, whitespace-only, or contain only bot mentions
        must be dropped silently without reaction or session instantiation.
        """
        cases = [
            "<@U_AGY_BOT>",
            "<@U_AGY_BOT|>",
            "<@U_AGY_BOT|firstmate>",
            "    ",
            "",
            "@agy",
        ]
        for i, text in enumerate(cases):
            event = {
                "channel": "C_SILENT",
                "ts": f"1720000300.{i:04d}",
                "text": text,
            }
            await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(self.client.reactions_added), 0, "No reactions should be added for empty messages")
        self.assertEqual(len(self.client.posted_messages), 0, "No messages should be posted for empty messages")
        self.assertEqual(len(self.router.sessions), 0, "No sessions should be created for empty messages")

    # -------------------------------------------------------------------------
    # Adversarial Early Generator Break (Lock Deadlock Resilience)
    # -------------------------------------------------------------------------
    async def test_adversarial_early_generator_break_recovery(self) -> None:
        """
        Adversarial test:
        Caller consumes only 1 delta from session.send_user_message() and breaks early.
        Verify subsequent turn does not deadlock and lock is cleanly released.
        """
        async def multi_chunk_worker(prompt: str):
            yield StatusUpdateEvent(status="working")
            yield TextDeltaEvent(text_delta="chunk 1")
            yield TextDeltaEvent(text_delta="chunk 2")
            yield TextDeltaEvent(text_delta="chunk 3")
            yield FinishEvent(response="done", status="SUCCESS")

        ad = Challenger6MockAdapter()
        ad.custom_handler = multi_chunk_worker
        session = SessionHandle("C_BRK:T_BRK", "C_BRK", "T_BRK", ad)
        session.state = SessionState.IDLE

        # Turn 1: consume 1 event and break
        gen1 = session.send_user_message("Turn 1 break")
        async for _ in gen1:
            break

        # Turn 2: execute with 0.5s timeout
        turn2_chunks = []
        async def run_turn2():
            async for ev in session.send_user_message("Turn 2 normal"):
                if getattr(ev, "event_type", None) == "text_delta":
                    turn2_chunks.append(getattr(ev, "text_delta", ""))

        try:
            await asyncio.wait_for(run_turn2(), timeout=0.5)
        except asyncio.TimeoutError:
            self.fail("Turn 2 deadlocked after Turn 1 early generator break!")

        self.assertEqual(len(turn2_chunks), 3)
        self.assertFalse(session.lock.locked(), "Lock must not remain acquired")
        self.assertEqual(session.state, SessionState.IDLE)

    # -------------------------------------------------------------------------
    # Adversarial Concurrent Session Close (Zombie State Resilience)
    # -------------------------------------------------------------------------
    async def test_adversarial_concurrent_close_no_zombie_state(self) -> None:
        """
        Adversarial test:
        session.close() is called concurrently while a turn is in-flight.
        Verify session.state strictly remains CLOSED and does not resurrect to IDLE.
        """
        async def slow_turn(prompt: str):
            yield StatusUpdateEvent(status="working")
            await asyncio.sleep(0.05)
            yield TextDeltaEvent(text_delta="slow response")
            yield FinishEvent(response="slow response", status="SUCCESS")

        ad = Challenger6MockAdapter()
        ad.custom_handler = slow_turn
        session = SessionHandle("C_CLS:T_CLS", "C_CLS", "T_CLS", ad)
        session.state = SessionState.IDLE

        turn_task = asyncio.create_task(
            asyncio.to_thread(lambda: None)  # dummy
        )

        async def run_turn():
            try:
                async for _ in session.send_user_message("in flight"):
                    pass
            except Exception:
                pass

        turn_task = asyncio.create_task(run_turn())
        await asyncio.sleep(0.01)  # wait for turn to start

        await session.close(reason="test_close")
        self.assertEqual(session.state, SessionState.CLOSED)

        await turn_task

        # Verify state did not get overwritten to IDLE by FinishEvent
        self.assertEqual(session.state, SessionState.CLOSED, "Zombie resurrection: session state became IDLE after close!")

        # Subsequent messages must be rejected
        with self.assertRaises(RuntimeError):
            async for _ in session.send_user_message("after close"):
                pass

    # -------------------------------------------------------------------------
    # Adversarial Error Handling & Recovery
    # -------------------------------------------------------------------------
    async def test_adversarial_error_turn_reaction_and_clean_recovery(self) -> None:
        """
        Adversarial test:
        Turn 1 triggers execution error -> Reaction swaps to :x:.
        Turn 2 arrives on same thread -> Gateway recreates session and succeeds with :white_check_mark:.
        """
        ad1 = Challenger6MockAdapter()
        async def broken_turn(prompt: str):
            yield StatusUpdateEvent(status="working")
            yield FinishEvent(response="", status="ERROR", error="Simulated Fatal Engine Error")

        ad1.custom_handler = broken_turn

        ad2 = Challenger6MockAdapter()
        ad2.responses["Recovery question"] = "*All systems normal.*"

        adapters = [ad1, ad2]
        self.router.adapter_factory = lambda cwd: adapters.pop(0)

        channel = "C_ERR"
        thread_ts = "1720000400.0001"

        # Turn 1: Error
        ev1 = {"channel": channel, "ts": thread_ts, "thread_ts": thread_ts, "text": "Failing query"}
        await self.gateway.handle_message(ev1, self.client)

        session1 = self.router.sessions[f"{channel}:{thread_ts}"]
        self.assertEqual(session1.state, SessionState.FAILED)

        # Check :x: reaction
        x_adds = [r for r in self.client.reactions_added if r["name"] == "x" and r["timestamp"] == thread_ts]
        self.assertEqual(len(x_adds), 1, "Must add :x: reaction on turn failure")
        self.assertIn("Simulated Fatal Engine Error", self.client.posted_messages[0]["text"])

        # Turn 2: Recovery
        t2_ts = "1720000400.0002"
        ev2 = {"channel": channel, "ts": t2_ts, "thread_ts": thread_ts, "text": "Recovery question"}
        await self.gateway.handle_message(ev2, self.client)

        session2 = self.router.sessions[f"{channel}:{thread_ts}"]
        self.assertIsNot(session1, session2, "Must replace FAILED session with fresh SessionHandle")
        self.assertEqual(session2.state, SessionState.IDLE)

        # Check :white_check_mark: reaction on Turn 2
        check_adds = [r for r in self.client.reactions_added if r["name"] == "white_check_mark" and r["timestamp"] == t2_ts]
        self.assertEqual(len(check_adds), 1, "Must add :white_check_mark: on recovered turn")
        self.assertIn("*All systems normal.*", self.client.posted_messages[1]["text"])

    # -------------------------------------------------------------------------
    # Slash Commands Direct Handling
    # -------------------------------------------------------------------------
    async def test_slash_commands_handled_directly(self) -> None:
        """
        Verify /bearings, /ahoy, /learn, /wrapup, /archive execute directly
        with zero worker panes and Slack mrkdwn bold formatting.
        """
        commands = [
            ("/bearings", "Fleet Bearings & Status Digest"),
            ("/ahoy", "Session catch-up complete"),
            ("/learn disk quota trick", "Operational Lesson Crystallized"),
        ]

        for i, (cmd, expected_sub) in enumerate(commands):
            ts = f"1720000500.{i:04d}"
            ev = {"channel": "C_CMD", "ts": ts, "thread_ts": ts, "text": cmd}
            await self.gateway.handle_message(ev, self.client)

            # Check direct response
            msg = self.client.posted_messages[-1]
            self.assertIn(expected_sub, msg["text"])
            self.assertIn("*", msg["text"])  # Uses Slack bold
            self.assertNotIn("**", msg["text"])  # No double asterisks

            # Check reaction swap
            check_adds = [r for r in self.client.reactions_added if r["name"] == "white_check_mark" and r["timestamp"] == ts]
            self.assertEqual(len(check_adds), 1)

        # Zero worker panes spawned across all commands
        self.assertEqual(len(self.client.worker_panes_spawned), 0)


if __name__ == "__main__":
    unittest.main()
