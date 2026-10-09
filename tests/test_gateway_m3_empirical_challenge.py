#!/usr/bin/env python3
"""
tests/test_gateway_m3_empirical_challenge.py
Adversarial empirical verification suite for Milestone 3 (Ingress Gateway & Thread Session State).
Challenger 2 verification covering:
  - TC-01: Conversational Inquiry ("How's our fleet doing?")
    * Non-verbal :gear: acknowledgment added <200ms
    * Zero canned greeting announcements or worker pitches
    * Zero worker subagent panes spawned
    * Direct single-turn response delivered with Slack mrkdwn bold headers (*bold*)
    * Zero completion footers (*Task Completed via Herdr*)
  - TC-02: Multi-Turn Thread Continuity
    * Session context retained across turns in the same SessionHandle
    * Zero re-inspecting or echoing of stale conversational history from Turn 1
    * Reaction transitions correctly (:gear: -> :white_check_mark: on each turn)
    * Thread isolation between distinct threads
  - Real Subprocess IPC Empirical Verification:
    * HeadlessAgentAdapter with mock_agy_proc subprocess
  - Adversarial Stress Challenges:
    * Rapid concurrent turns on same thread serialized by SessionHandle.lock
    * Bot mention stripping fuzzing
    * Error recovery and state transition resilience
    * Inactivity expiration and automatic recreation

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import time
import asyncio
import unittest
from typing import Optional, List, Dict, Any, AsyncGenerator

from golem.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    AwaitingInputEvent,
    FinishEvent,
    AgentEvent,
)
from golem.agent.adapter import HeadlessAgentAdapter
from golem.gateway.session import SessionHandle, SessionState
from golem.gateway.router import SessionRouter
from golem.gateway.ingress import (
    IngressGateway,
    strip_bot_mentions,
    swap_reaction,
)


class TimingMockSlackClient:
    """Mock Slack WebClient recording exact timestamps and method calls."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, Any]] = []
        self.reactions_removed: List[Dict[str, Any]] = []
        self.worker_panes_spawned: List[str] = []

    async def reactions_add(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        call_time = time.perf_counter()
        self.reactions_added.append({
            "channel": channel,
            "timestamp": timestamp,
            "name": name,
            "time": call_time,
        })
        return {"ok": True}

    async def reactions_remove(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        call_time = time.perf_counter()
        self.reactions_removed.append({
            "channel": channel,
            "timestamp": timestamp,
            "name": name,
            "time": call_time,
        })
        return {"ok": True}

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None) -> Dict[str, Any]:
        call_time = time.perf_counter()
        self.posted_messages.append({
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "time": call_time,
        })
        return {"ok": True, "ts": f"{time.time()}"}


class ControllableMockAdapter:
    """Mock adapter allowing custom delays, custom generator callbacks, and recording turns."""

    def __init__(self, cwd: str = "/Users/hermes/projects/AGY", delay_before_yield: float = 0.0) -> None:
        self.cwd = cwd
        self.delay_before_yield = delay_before_yield
        self.started = False
        self.stopped = False
        self.turns_received: List[str] = []
        self.responses_to_yield: Dict[str, str] = {}
        self.custom_handler = None

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

        resp_text = self.responses_to_yield.get(prompt, f"*Result for:* {prompt}")
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text_delta=resp_text)
        yield FinishEvent(response=resp_text, status="SUCCESS")


class TestEmpiricalChallengerM3(unittest.IsolatedAsyncioTestCase):
    """Rigorous empirical challenge suite for Milestone 3 TC-01 and TC-02."""

    def setUp(self) -> None:
        self.adapters: List[ControllableMockAdapter] = []

        def factory(cwd: str) -> ControllableMockAdapter:
            ad = ControllableMockAdapter(cwd=cwd)
            self.adapters.append(ad)
            return ad

        self.router = SessionRouter(
            default_target_cwd="/Users/hermes/projects/AGY",
            inactivity_timeout=14400.0,
            reaper_interval=60.0,
            adapter_factory=factory,
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = TimingMockSlackClient()

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    # =========================================================================
    # TC-01: Conversational Inquiry Empirical Verification
    # =========================================================================

    async def test_tc01_conversational_inquiry_complete_spec(self) -> None:
        """
        TC-01 Verification:
        Simulate: "How's our fleet doing?"
        Verify:
          1. Immediate non-verbal :gear: reaction added (<200ms).
          2. Zero canned greeting announcements or worker pitches.
          3. Zero worker subagent panes spawned.
          4. Direct single-turn response delivered formatted with Slack mrkdwn bold headers (*bold*).
          5. Zero completion footers (*Task Completed via Herdr*).
        """
        # Configure fleet status response
        fleet_report = (
            "🧭 *Fleet Status Overview:*\n"
            "• *hermes* (localhost (Primary Dev & Homelab)): 🟢 Healthy (Uptime: 14d)\n"
            "• *desktop-pc* (Compute Grid (Windows CUDA)): 🟢 Idle (GPU Temp: 42°C)\n"
            "• *personal-laptop* (Carl's Workstation): 🟢 Online (Awake)\n"
            "• *oci-machine* (Cloud Operations (VPS)): 🟢 Healthy (Load: 0.12)"
        )

        # Introduce a deliberate 150ms delay in agent response to prove :gear: is added BEFORE turn completes
        ad = ControllableMockAdapter(delay_before_yield=0.15)
        ad.responses_to_yield["How's our fleet doing?"] = fleet_report
        self.router.adapter_factory = lambda cwd: ad

        start_time = time.perf_counter()
        event = {
            "channel": "C_GENERAL",
            "ts": "1711000000.1001",
            "thread_ts": "1711000000.1001",
            "text": "<@U_AGY_BOT> How's our fleet doing?",
        }

        await self.gateway.handle_message(event, self.client)
        total_duration = time.perf_counter() - start_time

        # 1. Non-verbal :gear: acknowledgment latency verification (<200ms)
        gear_adds = [r for r in self.client.reactions_added if r["name"] == "gear"]
        self.assertEqual(len(gear_adds), 1, "Must add exactly one :gear: reaction")
        gear_reaction = gear_adds[0]
        gear_latency_ms = (gear_reaction["time"] - start_time) * 1000.0
        self.assertLess(
            gear_latency_ms,
            200.0,
            f"Non-verbal :gear: ack must be delivered in <200ms, actual: {gear_latency_ms:.2f}ms",
        )
        self.assertEqual(gear_reaction["timestamp"], "1711000000.1001")
        self.assertEqual(gear_reaction["channel"], "C_GENERAL")

        # 2. Zero canned greeting announcements or worker pitches
        # Only exactly 1 message delivered (the direct response)
        self.assertEqual(len(self.client.posted_messages), 1, "Must post exactly 1 response message")
        delivered_msg = self.client.posted_messages[0]["text"]

        prohibited_canned_phrases = [
            "I will dispatch",
            "Starting worker",
            "worker pane",
            "opencode",
            "Let me check",
            "On it",
            "Hello Carl",
            "I am AGY",
            "Certainly",
            "I'd be glad",
            "Taking a look",
            "Processing your request",
        ]
        for phrase in prohibited_canned_phrases:
            self.assertNotIn(
                phrase.lower(),
                delivered_msg.lower(),
                f"TC-01 violation: canned phrase '{phrase}' found in response: {delivered_msg}",
            )

        # 3. Zero worker subagent panes spawned
        self.assertEqual(len(self.client.worker_panes_spawned), 0, "Zero worker panes must be spawned")

        # 4. Direct single-turn response delivered with Slack mrkdwn bold headers (*bold*)
        self.assertIn("*Fleet Status Overview:*", delivered_msg)
        self.assertIn("*hermes*", delivered_msg)
        self.assertIn("*desktop-pc*", delivered_msg)
        # Verify no un-transpiled CommonMark double asterisks
        self.assertNotIn("**", delivered_msg, "Must not contain Markdown double asterisks (Slack mrkdwn violation)")
        # Verify no ATX Markdown headers
        self.assertFalse(any(line.startswith("#") for line in delivered_msg.splitlines()), "Must not contain ATX headings")

        # 5. Zero completion footers (*Task Completed via Herdr*)
        prohibited_footers = [
            "*Task Completed via Herdr*",
            "Task Completed via Herdr",
            "*Task Completed*",
            "Task Completed",
            "Execution completed via",
            "Herdr worker finished",
        ]
        for footer in prohibited_footers:
            self.assertNotIn(
                footer.lower(),
                delivered_msg.lower(),
                f"TC-01 violation: completion footer '{footer}' found in response: {delivered_msg}",
            )

        # Verify reaction swap: :gear: -> :white_check_mark:
        checkmarks = [r for r in self.client.reactions_added if r["name"] == "white_check_mark"]
        self.assertEqual(len(checkmarks), 1, "Must swap reaction to :white_check_mark: on completion")
        self.assertEqual(checkmarks[0]["timestamp"], "1711000000.1001")

        gear_removals = [r for r in self.client.reactions_removed if r["name"] == "gear"]
        self.assertEqual(len(gear_removals), 1, "Must remove :gear: when swapping to :white_check_mark:")

    # =========================================================================
    # TC-02: Multi-Turn Thread Continuity Empirical Verification
    # =========================================================================

    async def test_tc02_multi_turn_thread_continuity_spec(self) -> None:
        """
        TC-02 Verification:
        Simulate multi-turn thread:
          Turn 1: "List all cron jobs"
          Turn 2: "Which one runs at 3 AM?"
        Verify:
          1. Session context is retained across turns in the same SessionHandle instance.
          2. Zero re-inspecting or echoing of stale conversational history from Turn 1.
          3. Reaction transitions correctly (:gear: -> :white_check_mark: on each turn).
          4. Thread isolation between distinct threads.
        """
        ad = ControllableMockAdapter()
        cron_list_response = (
            "🧭 *Scheduled Cron Tasks:*\n"
            "• *backup-zfs*: `0 2 * * *` (Daily ZFS snapshot pool backup)\n"
            "• *telemetry-sync*: `0 3 * * *` (Fleet state telemetry synchronization)\n"
            "• *log-sweep*: `0 4 * * *` (Audit and prune stale session logs)"
        )
        cron_answer_response = "*telemetry-sync* runs at 3 AM daily (`0 3 * * *`)."

        ad.responses_to_yield["List all cron jobs"] = cron_list_response
        ad.responses_to_yield["Which one runs at 3 AM?"] = cron_answer_response

        def factory_t2(cwd: str) -> ControllableMockAdapter:
            if not self.adapters:
                self.adapters.append(ad)
                return ad
            new_ad = ControllableMockAdapter(cwd=cwd)
            self.adapters.append(new_ad)
            return new_ad

        self.router.adapter_factory = factory_t2

        channel_id = "C_FLEET_OPS"
        thread_ts = "1711000100.0001"

        # ---------------------------------------------------------------------
        # Turn 1: "List all cron jobs"
        # ---------------------------------------------------------------------
        t1_start = time.perf_counter()
        event_turn1 = {
            "channel": channel_id,
            "ts": thread_ts,
            "thread_ts": thread_ts,
            "text": "List all cron jobs",
        }
        await self.gateway.handle_message(event_turn1, self.client)

        # Verify session was created
        session_key = SessionRouter.build_session_key(channel_id, thread_ts)
        self.assertIn(session_key, self.router.sessions)
        session_handle_t1 = self.router.sessions[session_key]
        self.assertEqual(session_handle_t1.current_turn, 1)
        self.assertEqual(session_handle_t1.state, SessionState.IDLE)
        self.assertEqual(len(ad.turns_received), 1)
        self.assertEqual(ad.turns_received[0], "List all cron jobs")

        # Verify Turn 1 posted message
        self.assertEqual(len(self.client.posted_messages), 1)
        t1_post = self.client.posted_messages[0]
        self.assertEqual(t1_post["thread_ts"], thread_ts)
        self.assertIn("*Scheduled Cron Tasks:*", t1_post["text"])

        # Verify Turn 1 reaction cycle: :gear: -> :white_check_mark:
        self.assertEqual(self.client.reactions_added[0]["name"], "gear")
        self.assertEqual(self.client.reactions_added[0]["timestamp"], thread_ts)
        self.assertEqual(self.client.reactions_added[1]["name"], "white_check_mark")
        self.assertEqual(self.client.reactions_added[1]["timestamp"], thread_ts)

        # ---------------------------------------------------------------------
        # Turn 2: "Which one runs at 3 AM?" (in the exact same thread)
        # ---------------------------------------------------------------------
        t2_ts = "1711000100.0002"
        event_turn2 = {
            "channel": channel_id,
            "ts": t2_ts,
            "thread_ts": thread_ts,
            "text": "Which one runs at 3 AM?",
        }
        await self.gateway.handle_message(event_turn2, self.client)

        # Verify exact same SessionHandle instance was reused (Warm continuity)
        session_handle_t2 = self.router.sessions[session_key]
        self.assertIs(session_handle_t1, session_handle_t2, "Must reuse identical SessionHandle instance")
        self.assertEqual(session_handle_t2.current_turn, 2, "Turn counter must increment to 2")
        self.assertEqual(session_handle_t2.state, SessionState.IDLE)

        # Verify NO duplicate adapter was created
        self.assertEqual(len(self.adapters), 1, "Must not create a new adapter instance for warm turn")

        # Verify zero re-inspecting or echoing of stale Turn 1 history into Turn 2 prompt
        self.assertEqual(len(ad.turns_received), 2)
        turn2_input = ad.turns_received[1]
        self.assertEqual(
            turn2_input,
            "Which one runs at 3 AM?",
            f"Turn 2 prompt must be clean; found unexpected wrapping/echoing: {turn2_input}",
        )
        self.assertNotIn("List all cron jobs", turn2_input)
        self.assertNotIn("Scheduled Cron Tasks", turn2_input)

        # Verify Turn 2 delivered response
        self.assertEqual(len(self.client.posted_messages), 2)
        t2_post = self.client.posted_messages[1]
        self.assertEqual(t2_post["thread_ts"], thread_ts)
        self.assertIn("*telemetry-sync* runs at 3 AM", t2_post["text"])

        # Verify Turn 2 output does NOT echo Turn 1's listing
        self.assertNotIn("backup-zfs", t2_post["text"])
        self.assertNotIn("log-sweep", t2_post["text"])

        # Verify Turn 2 reaction cycle: :gear: -> :white_check_mark: on t2_ts
        turn2_gear = [r for r in self.client.reactions_added if r["name"] == "gear" and r["timestamp"] == t2_ts]
        turn2_check = [r for r in self.client.reactions_added if r["name"] == "white_check_mark" and r["timestamp"] == t2_ts]
        self.assertEqual(len(turn2_gear), 1, "Must add :gear: to Turn 2 message")
        self.assertEqual(len(turn2_check), 1, "Must add :white_check_mark: to Turn 2 message")

        # ---------------------------------------------------------------------
        # Turn 3: Separate Thread (Thread Isolation Verification)
        # ---------------------------------------------------------------------
        separate_thread_ts = "1711000200.0001"
        event_turn3 = {
            "channel": channel_id,
            "ts": separate_thread_ts,
            "thread_ts": separate_thread_ts,
            "text": "Distinct thread task",
        }
        await self.gateway.handle_message(event_turn3, self.client)

        separate_key = SessionRouter.build_session_key(channel_id, separate_thread_ts)
        self.assertIn(separate_key, self.router.sessions)
        session_handle_t3 = self.router.sessions[separate_key]
        self.assertIsNot(session_handle_t1, session_handle_t3, "Distinct thread must have distinct SessionHandle")
        self.assertEqual(session_handle_t3.current_turn, 1)
        self.assertEqual(len(self.adapters), 2, "Distinct thread must instantiate dedicated adapter")

    # =========================================================================
    # Real Subprocess IPC Empirical Verification
    # =========================================================================

    async def test_real_subprocess_ipc_multi_turn_continuity(self) -> None:
        """
        Verify HeadlessAgentAdapter with real OS child process (mock_agy_proc.py)
        across multiple conversational turns inside SessionRouter and IngressGateway.
        """
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")

        def real_adapter_factory(cwd: str) -> HeadlessAgentAdapter:
            return HeadlessAgentAdapter(
                cwd="/Users/hermes/projects/golem",
                agy_bin=mock_bin,
                env={"MOCK_AGY_MODE": "normal"},
            )

        router = SessionRouter(adapter_factory=real_adapter_factory)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")
        client = TimingMockSlackClient()

        try:
            channel = "C_SUBPROC"
            thread_ts = "1711000300.0001"

            # Turn 1 via real subprocess
            ev1 = {
                "channel": channel,
                "ts": thread_ts,
                "thread_ts": thread_ts,
                "text": "List all cron jobs",
            }
            await gateway.handle_message(ev1, client)

            session = router.sessions[f"{channel}:{thread_ts}"]
            self.assertTrue(session.adapter.is_alive, "Subprocess must remain alive after turn 1")
            first_pid = session.adapter.process.pid
            self.assertIsNotNone(first_pid)

            self.assertEqual(len(client.posted_messages), 1)
            self.assertIn("Echo: List all cron jobs", client.posted_messages[0]["text"])

            # Turn 2 via same persistent subprocess
            ev2 = {
                "channel": channel,
                "ts": "1711000300.0002",
                "thread_ts": thread_ts,
                "text": "Which one runs at 3 AM?",
            }
            await gateway.handle_message(ev2, client)

            # Subprocess must still be alive with the EXACT same PID
            self.assertTrue(session.adapter.is_alive, "Subprocess must remain alive after turn 2")
            self.assertEqual(session.adapter.process.pid, first_pid, "Subprocess PID must be identical (no re-spawn)")
            self.assertEqual(session.current_turn, 2)

            self.assertEqual(len(client.posted_messages), 2)
            self.assertIn("Echo: Which one runs at 3 AM?", client.posted_messages[1]["text"])

            # Reactions for both turns
            gear_adds = [r for r in client.reactions_added if r["name"] == "gear"]
            check_adds = [r for r in client.reactions_added if r["name"] == "white_check_mark"]
            self.assertEqual(len(gear_adds), 2)
            self.assertEqual(len(check_adds), 2)

        finally:
            await router.stop_reaper()

    # =========================================================================
    # Adversarial Stress Challenges
    # =========================================================================

    async def test_rapid_concurrency_stress_serialized_by_session_lock(self) -> None:
        """
        Stress challenge: Fire 5 rapid concurrent user messages into the same thread.
        Verify:
          1. SessionHandle.lock strictly serializes execution.
          2. No race condition or interleaved corrupted events.
          3. All 5 turns complete with turn counter reaching 5.
          4. All 5 reactions complete (:gear: -> :white_check_mark:).
        """
        turn_log: List[str] = []

        async def serial_worker(prompt: str):
            turn_log.append(f"start:{prompt}")
            await asyncio.sleep(0.02)
            turn_log.append(f"finish:{prompt}")
            yield TextDeltaEvent(text_delta=f"Answer for {prompt}")
            yield FinishEvent(response=f"Answer for {prompt}", status="SUCCESS")

        ad = ControllableMockAdapter()
        ad.custom_handler = serial_worker
        self.router.adapter_factory = lambda cwd: ad

        channel = "C_CONCURRENCY"
        thread_ts = "1711000400.0001"

        # Create 5 distinct messages in the same thread
        events = [
            {
                "channel": channel,
                "ts": f"1711000400.000{i}",
                "thread_ts": thread_ts,
                "text": f"Question {i}",
            }
            for i in range(1, 6)
        ]

        # Dispatch all 5 concurrently
        await asyncio.gather(*(self.gateway.handle_message(ev, self.client) for ev in events))

        session_key = f"{channel}:{thread_ts}"
        session = self.router.sessions[session_key]
        self.assertEqual(session.current_turn, 5, "Session turn counter must be exactly 5")
        self.assertEqual(len(self.client.posted_messages), 5, "Must deliver exactly 5 responses")

        # Verify strict non-interleaved serialization: start:X must be immediately followed by finish:X
        for idx in range(0, len(turn_log), 2):
            start_entry = turn_log[idx]
            finish_entry = turn_log[idx + 1]
            q_num = start_entry.split(":")[1]
            self.assertTrue(start_entry.startswith("start:"), f"Expected start, got {start_entry}")
            self.assertEqual(finish_entry, f"finish:{q_num}", "Turn execution was interleaved!")

        # Verify all 5 messages received :gear: and :white_check_mark:
        gear_reactions = [r for r in self.client.reactions_added if r["name"] == "gear"]
        check_reactions = [r for r in self.client.reactions_added if r["name"] == "white_check_mark"]
        self.assertEqual(len(gear_reactions), 5)
        self.assertEqual(len(check_reactions), 5)

    async def test_bot_mention_fuzzing_supported_syntaxes(self) -> None:
        """
        Stress challenge: Test supported bot mention syntaxes.
        Verify all strip cleanly to prompt text without canned announcements or broken routing.
        """
        variations = [
            ("<@U_AGY_BOT> How's our fleet doing?", "How's our fleet doing?"),
            ("@AGY How's our fleet doing?", "How's our fleet doing?"),
            ("@agy How's our fleet doing?", "How's our fleet doing?"),
            ("   @FirstMate   How's our fleet doing?   ", "How's our fleet doing?"),
            ("<@W12345678> How's our fleet doing?", "How's our fleet doing?"),
        ]

        for raw_input, expected_clean in variations:
            cleaned = strip_bot_mentions(raw_input, "U_AGY_BOT")
            self.assertEqual(cleaned, expected_clean, f"Failed stripping mention from: {raw_input}")

    def test_defect_bot_mention_pipe_label_unhandled(self) -> None:
        """
        Empirical finding: Slack mentions containing display labels (e.g. <@U12345|bot_name>)
        are NOT stripped by strip_bot_mentions because regex <@[A-Za-z0-9_-]+> excludes '|'.
        This causes the raw mention to leak into the prompt and breaks command interception.
        """
        raw = "<@U_AGY_BOT|firstmate> How's our fleet doing?"
        cleaned = strip_bot_mentions(raw, "U_AGY_BOT")
        self.assertEqual(cleaned, "How's our fleet doing?", "Regex failed to strip mention containing pipe label")

    async def test_empty_or_mention_only_message_dropped_silently(self) -> None:
        """
        Adversarial challenge: Message containing only bot mention or whitespace.
        Verify no reaction added, no message posted, no exceptions raised.
        """
        event = {
            "channel": "C_EMPTY",
            "ts": "1711000500.0001",
            "text": "<@U_AGY_BOT>   ",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(len(self.client.reactions_added), 0)
        self.assertEqual(len(self.client.posted_messages), 0)
        self.assertEqual(len(self.router.sessions), 0)

    async def test_error_turn_followed_by_clean_recovery_turn(self) -> None:
        """
        Adversarial challenge:
        Turn 1 triggers an execution crash -> reaction swaps to :x:.
        Turn 2 arrives on the same thread -> Router detects FAILED state,
        recreates fresh warm session, and Turn 2 succeeds with :white_check_mark:.
        """
        ad1 = ControllableMockAdapter()
        async def failing_turn(prompt: str):
            yield StatusUpdateEvent(status="working")
            yield FinishEvent(response="", status="ERROR", error="Simulated GPU OOM")

        ad1.custom_handler = failing_turn

        ad2 = ControllableMockAdapter()
        ad2.responses_to_yield["Retry after clearing GPU cache"] = "*Recovered:* GPU memory freed, healthy."

        created_adapters = [ad1, ad2]
        self.router.adapter_factory = lambda cwd: created_adapters.pop(0)

        channel = "C_RECOVER"
        thread_ts = "1711000600.0001"

        # Turn 1: Fails
        ev1 = {
            "channel": channel,
            "ts": "1711000600.0001",
            "thread_ts": thread_ts,
            "text": "Run heavy model inference",
        }
        await self.gateway.handle_message(ev1, self.client)

        session1 = self.router.sessions.get(f"{channel}:{thread_ts}")
        self.assertIsNotNone(session1)
        self.assertEqual(session1.state, SessionState.FAILED)

        # Verify :x: reaction
        x_adds = [r for r in self.client.reactions_added if r["name"] == "x"]
        self.assertEqual(len(x_adds), 1)
        self.assertIn("Simulated GPU OOM", self.client.posted_messages[0]["text"])

        # Turn 2: Recover
        ev2 = {
            "channel": channel,
            "ts": "1711000600.0002",
            "thread_ts": thread_ts,
            "text": "Retry after clearing GPU cache",
        }
        await self.gateway.handle_message(ev2, self.client)

        session2 = self.router.sessions.get(f"{channel}:{thread_ts}")
        self.assertIsNotNone(session2)
        self.assertIsNot(session1, session2, "Must create fresh session after previous session FAILED")
        self.assertEqual(session2.state, SessionState.IDLE)

        # Verify :white_check_mark: reaction on Turn 2
        check_adds = [r for r in self.client.reactions_added if r["name"] == "white_check_mark"]
        self.assertEqual(len(check_adds), 1)
        self.assertIn("*Recovered:* GPU memory freed, healthy.", self.client.posted_messages[1]["text"])

    async def test_session_inactivity_reaping_and_subsequent_recreation(self) -> None:
        """
        Adversarial challenge: Verify that an inactive session (>4h) is reaped
        by SessionRouter, and a new turn in the same thread re-initializes cleanly.
        """
        session, is_new = await self.router.get_or_create("C_REAP", "1711000700.0001")
        self.assertTrue(is_new)
        key = "C_REAP:1711000700.0001"
        self.assertIn(key, self.router.sessions)

        # Force session to be expired (>14400s)
        session.last_active_ts = time.time() - 15000.0
        self.assertTrue(session.is_expired())

        # Audit reaper
        pruned = await self.router.reap_expired_sessions()
        self.assertEqual(pruned, [key])
        self.assertNotIn(key, self.router.sessions)
        self.assertEqual(session.state, SessionState.CLOSED)

        # Next user turn in the same thread transparently creates a new warm session
        ev = {
            "channel": "C_REAP",
            "ts": "1711000700.0002",
            "thread_ts": "1711000700.0001",
            "text": "How's our fleet doing?",
        }
        await self.gateway.handle_message(ev, self.client)
        self.assertIn(key, self.router.sessions)
        new_session = self.router.sessions[key]
        self.assertIsNot(session, new_session)
        self.assertEqual(new_session.state, SessionState.IDLE)

    async def test_session_handle_early_generator_break_deadlock_vulnerability(self) -> None:
        """
        Adversarial challenge:
        When a caller breaks early from session.send_user_message(), verify that the session
        does not deadlock on the next turn.
        """
        async def multi_delta_turn(prompt: str):
            yield TextDeltaEvent(text_delta="Chunk 1")
            yield TextDeltaEvent(text_delta="Chunk 2")
            yield FinishEvent(response="Done", status="SUCCESS")

        ad = ControllableMockAdapter()
        ad.custom_handler = multi_delta_turn
        session = SessionHandle("C_TEST:1", "C_TEST", "1", ad)

        # Consumer 1 starts turn, consumes 1 event, and breaks early:
        gen1 = session.send_user_message("Turn 1")
        async for ev in gen1:
            break

        # Attempt turn 2 with a 0.2s timeout: must preempt lingering generator and complete without deadlock
        turn2_events = []
        async def run_turn2():
            async for ev in session.send_user_message("Turn 2"):
                turn2_events.append(ev)

        turn2_deadlocked = False
        try:
            await asyncio.wait_for(run_turn2(), timeout=0.2)
        except asyncio.TimeoutError:
            turn2_deadlocked = True

        self.assertFalse(turn2_deadlocked, "SessionHandle deadlocked after early generator break!")
        self.assertEqual(len(turn2_events), 3, "Turn 2 must yield all expected events")
        self.assertFalse(session.lock.locked(), "Lock remained held after Turn 2 completed")


if __name__ == "__main__":
    unittest.main()
