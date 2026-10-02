#!/usr/bin/env python3
"""
tests/test_tc03_concurrency_stress_challenge.py
Adversarial Empirical Stress Challenge Test Suite for TC-03:
Conversational Concurrency & Gateway Lock Decoupling under Long-Running Pane 2 Execution.

Validates:
1. Long-running Pane 2 execution (5s - 10s) does NOT block concurrent incoming user check-ins.
2. Check-in queries ("how's it going?", "what are you doing?", etc.) resolve immediately (< 100ms).
3. Session lock contention is decoupled: check-ins bypass session.lock while it is held.
4. High-volume concurrent bursts (20+ parallel requests) maintain < 100ms latency with zero deadlocks.
5. Mixed concurrency: simultaneous check-ins, status inquiries, and out-of-band cancellations.
6. Normal task turns continue to serialize under session.lock, preserving turn context and ordering.
7. Resilience against buffer observer failures under stress.
"""

from __future__ import annotations

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


class StressMockAdapter:
    """Mock agent adapter simulating long-running turns under lock."""

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


class ThreadSafeMockSlackClient:
    """Thread-safe mock Slack WebClient tracking calls and exact dispatch/reply timestamps."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self._lock = asyncio.Lock()

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None) -> Dict[str, Any]:
        async with self._lock:
            msg = {
                "channel": channel,
                "text": text,
                "thread_ts": thread_ts,
                "ts": f"msg_{time.perf_counter()}",
                "timestamp": time.perf_counter(),
            }
            self.posted_messages.append(msg)
            return msg

    async def reactions_add(self, channel: str, timestamp: str, name: str) -> None:
        async with self._lock:
            self.reactions_added.append({"channel": channel, "timestamp": timestamp, "name": name})

    async def reactions_remove(self, channel: str, timestamp: str, name: str) -> None:
        async with self._lock:
            self.reactions_removed.append({"channel": channel, "timestamp": timestamp, "name": name})


class TestTC03ConcurrencyStressChallenge(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress-test suite for TC-03 conversational concurrency."""

    async def asyncSetUp(self) -> None:
        self.mock_adapter = StressMockAdapter(delay=5.0)
        self.buffer_observer = MagicMock(spec=RunnerBufferObserver)
        self.buffer_observer.read_buffer = AsyncMock(return_value="Building artifact... [45%]\nCompiling src/core.rs")
        self.buffer_observer.extract_all_blockers = MagicMock(return_value=[])
        self.buffer_observer.extract_interactive_blockers = MagicMock(return_value=None)

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
        self.client = ThreadSafeMockSlackClient()

    async def asyncTearDown(self) -> None:
        await self.session.close()

    # =========================================================================
    # Test 1: 5-Second Background Command in Pane 2 + Concurrent Check-Ins
    # =========================================================================

    async def test_empirical_5s_pane2_background_command_with_concurrent_checkins(self) -> None:
        """
        Simulate a 5-second long-running background command in Pane 2.
        While Pane 2 is running, dispatch multiple concurrent conversational check-ins:
        - "how's it going?"
        - "what are you doing?"
        Verify that EVERY check-in responds immediately (< 100ms) without waiting
        for Pane 2 to finish.
        """
        # Background task simulating Pane 2 command running for 5.0 seconds
        async def simulate_pane2_command():
            self.session.active_execution = ExecutionHandle(
                status="running",
                pane="runner",
                command="cargo build --release",
                pid=54321,
                message="Building rust binaries in Pane 2",
            )
            try:
                await asyncio.sleep(5.0)
                self.session.active_execution = ExecutionHandle(
                    status="completed",
                    pane="runner",
                    command="cargo build --release",
                    pid=54321,
                    returncode=0,
                    message="Build completed successfully",
                )
            except asyncio.CancelledError:
                self.session.active_execution = None

        pane2_task = asyncio.create_task(simulate_pane2_command())
        await asyncio.sleep(0.05)  # Let Pane 2 command start

        self.assertTrue(self.session.is_executing)
        self.assertTrue(self.session.is_busy)

        check_in_prompts = [
            "how's it going?",
            "what are you doing?",
            "how is it going?",
            "are you still there?",
            "any updates?",
        ]

        latencies_ms: List[float] = []

        try:
            for idx, prompt in enumerate(check_in_prompts):
                t_start = time.perf_counter()
                event = {
                    "channel": "stress_channel",
                    "thread_ts": "200.1",
                    "ts": f"200.{10 + idx}",
                    "text": prompt,
                }
                await self.gateway.handle_message(event, self.client)
                elapsed_ms = (time.perf_counter() - t_start) * 1000.0
                latencies_ms.append(elapsed_ms)

                # Assertion: EVERY check-in must respond in < 100ms
                self.assertLess(
                    elapsed_ms,
                    100.0,
                    f"Check-in prompt '{prompt}' took {elapsed_ms:.2f}ms (exceeds 100ms threshold!)",
                )

            # Check that Pane 2 task is STILL running during these check-ins
            self.assertFalse(pane2_task.done(), "Pane 2 task finished prematurely during test!")
            self.assertTrue(self.session.is_executing)

            # Verify responses received
            self.assertEqual(len(self.client.posted_messages), len(check_in_prompts))

            # Verify specific response formatting
            msg_texts = [m["text"] for m in self.client.posted_messages]

            # "how's it going?" -> Status / progress card
            self.assertIn("Runner In Progress", msg_texts[0])
            self.assertIn("PID 54321", msg_texts[0])

            # "what are you doing?" -> Liaison update card
            self.assertIn("Liaison Update", msg_texts[1])
            self.assertIn("PID 54321", msg_texts[1])

            # Ensure all latencies were fast (< 50ms average)
            avg_latency = sum(latencies_ms) / len(latencies_ms)
            self.assertLess(avg_latency, 50.0, f"Average latency was {avg_latency:.2f}ms")

        finally:
            pane2_task.cancel()
            try:
                await pane2_task
            except asyncio.CancelledError:
                pass

    # =========================================================================
    # Test 2: 10-Second Pane 2 Command with Parallel Burst (20 Concurrent Check-Ins)
    # =========================================================================

    async def test_empirical_10s_pane2_command_with_high_volume_burst(self) -> None:
        """
        Simulate a 10-second background execution in Pane 2.
        Dispatch a burst of 20 concurrent conversational check-ins simultaneously via asyncio.gather.
        Verify:
        - All 20 complete without deadlock.
        - Every single check-in takes < 100ms.
        - Total batch duration for all 20 is < 500ms.
        """
        async def simulate_pane2_command():
            self.session.active_execution = ExecutionHandle(
                status="running",
                pane="runner",
                command="pytest --large-suite",
                pid=9876,
            )
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                pass

        pane2_task = asyncio.create_task(simulate_pane2_command())
        await asyncio.sleep(0.05)

        prompts = [
            "how's it going?",
            "what are you doing?",
            "status",
            "ping",
            "are you still there?",
        ] * 4  # 20 items

        clients = [ThreadSafeMockSlackClient() for _ in range(20)]
        individual_durations_ms: List[float] = [0.0] * 20

        async def send_checkin(i: int, prompt: str):
            c = clients[i]
            t_start = time.perf_counter()
            event = {
                "channel": "stress_channel",
                "thread_ts": "200.1",
                "ts": f"200.{100 + i}",
                "text": prompt,
            }
            await self.gateway.handle_message(event, c)
            dur = (time.perf_counter() - t_start) * 1000.0
            individual_durations_ms[i] = dur

        try:
            batch_start = time.perf_counter()
            tasks = [asyncio.create_task(send_checkin(i, prompts[i])) for i in range(20)]
            await asyncio.gather(*tasks)
            batch_total_ms = (time.perf_counter() - batch_start) * 1000.0

            # Verify every check-in took < 100ms
            for i, dur in enumerate(individual_durations_ms):
                self.assertLess(
                    dur,
                    100.0,
                    f"Burst check-in {i} ('{prompts[i]}') took {dur:.2f}ms (threshold 100ms)",
                )

            # Entire batch of 20 concurrent messages completes < 500ms
            self.assertLess(batch_total_ms, 500.0, f"Batch took {batch_total_ms:.2f}ms")

            # Verify all 20 received responses
            for i, c in enumerate(clients):
                self.assertEqual(
                    len(c.posted_messages),
                    1,
                    f"Client {i} did not receive exactly 1 message",
                )

            self.assertTrue(self.session.is_executing)
        finally:
            pane2_task.cancel()
            try:
                await pane2_task
            except asyncio.CancelledError:
                pass

    # =========================================================================
    # Test 3: Lock-Decoupling Under Active Turn (session.lock explicitly held)
    # =========================================================================

    async def test_checkins_bypass_session_lock_while_turn1_holds_lock(self) -> None:
        """
        Verify that while Turn 1 is actively running and holding session.lock for 5 seconds,
        concurrent conversational check-ins via gateway.handle_message() bypass the lock
        and reply in < 100ms.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="python3 -m train_model",
            pid=33221,
        )

        turn1_finished = False

        async def run_turn1():
            nonlocal turn1_finished
            async for _ in self.session.send_user_message("Run heavy training pipeline"):
                pass
            turn1_finished = True

        turn1_task = asyncio.create_task(run_turn1())
        await asyncio.sleep(0.05)  # Allow turn 1 to start and acquire lock

        self.assertTrue(self.session.lock.locked(), "Session lock MUST be held by Turn 1")
        self.assertFalse(turn1_finished)

        try:
            # Check-in 1: "how's it going?"
            t0 = time.perf_counter()
            await self.gateway.handle_message(
                {"channel": "stress_channel", "thread_ts": "200.1", "ts": "200.201", "text": "how's it going?"},
                self.client,
            )
            elapsed1_ms = (time.perf_counter() - t0) * 1000.0

            # Check-in 2: "what are you doing?"
            t0 = time.perf_counter()
            await self.gateway.handle_message(
                {"channel": "stress_channel", "thread_ts": "200.1", "ts": "200.202", "text": "what are you doing?"},
                self.client,
            )
            elapsed2_ms = (time.perf_counter() - t0) * 1000.0

            # Both check-ins must complete < 100ms despite lock being held
            self.assertLess(elapsed1_ms, 100.0, f"Check-in 1 took {elapsed1_ms:.2f}ms while lock held")
            self.assertLess(elapsed2_ms, 100.0, f"Check-in 2 took {elapsed2_ms:.2f}ms while lock held")

            # Turn 1 must STILL be running and lock must STILL be held
            self.assertTrue(self.session.lock.locked())
            self.assertFalse(turn1_finished)

            # Verify responses were posted
            self.assertEqual(len(self.client.posted_messages), 2)
            self.assertIn("PID 33221", self.client.posted_messages[0]["text"])
            self.assertIn("PID 33221", self.client.posted_messages[1]["text"])

        finally:
            turn1_task.cancel()
            try:
                await turn1_task
            except asyncio.CancelledError:
                pass

    # =========================================================================
    # Test 4: Direct session.send_user_message() Concurrency Under Lock
    # =========================================================================

    async def test_direct_send_user_message_bypasses_lock_under_100ms(self) -> None:
        """
        Verify that direct programmatic callers using session.send_user_message()
        also bypass session.lock and stream out-of-band events in < 100ms.
        """
        await self.session.lock.acquire()
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="rclone sync /data remote:data",
            pid=77112,
        )

        try:
            # Check-in: "what are you doing?"
            t_start = time.perf_counter()
            events: List[AgentEvent] = []
            async for ev in self.session.send_user_message("what are you doing?"):
                events.append(ev)
            dur_ms = (time.perf_counter() - t_start) * 1000.0

            self.assertLess(dur_ms, 100.0, f"send_user_message took {dur_ms:.2f}ms")
            self.assertTrue(self.session.lock.locked())

            finish_ev = next(e for e in events if getattr(e, "event_type", None) == "finish")
            self.assertIn("Liaison Update", finish_ev.response)
            self.assertIn("PID 77112", finish_ev.response)

        finally:
            if self.session.lock.locked():
                self.session.lock.release()

    # =========================================================================
    # Test 5: Mixed Inquiries & Immediate Cancellation During 8s Execution
    # =========================================================================

    async def test_mixed_inquiries_and_cancellation_during_execution(self) -> None:
        """
        While an 8-second background task is running:
        1. Dispatch 'how's it going?' -> responds < 100ms with progress card.
        2. Dispatch 'what are you doing?' -> responds < 100ms with liaison update card.
        3. Dispatch 'status' -> responds < 100ms with progress card.
        4. Dispatch 'cancel' -> cancels execution < 100ms, sends SIGINT, and resets state.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="python3 -u long_worker.py",
            pid=44332,
        )

        with patch("first_mate.gateway.session.os.kill") as mock_kill:
            # Step 1: Check-in 1 while running
            t0 = time.perf_counter()
            await self.gateway.handle_message(
                {"channel": "stress_channel", "thread_ts": "200.1", "ts": "200.301", "text": "how's it going?"},
                self.client,
            )
            dur1 = (time.perf_counter() - t0) * 1000.0
            self.assertLess(dur1, 100.0)
            self.assertIn("Runner In Progress", self.client.posted_messages[-1]["text"])

            # Step 2: Check-in 2 while running
            t0 = time.perf_counter()
            await self.gateway.handle_message(
                {"channel": "stress_channel", "thread_ts": "200.1", "ts": "200.302", "text": "what are you doing?"},
                self.client,
            )
            dur2 = (time.perf_counter() - t0) * 1000.0
            self.assertLess(dur2, 100.0)
            self.assertIn("Liaison Update", self.client.posted_messages[-1]["text"])

            # Step 3: Status inquiry while running
            t0 = time.perf_counter()
            await self.gateway.handle_message(
                {"channel": "stress_channel", "thread_ts": "200.1", "ts": "200.303", "text": "status"},
                self.client,
            )
            dur3 = (time.perf_counter() - t0) * 1000.0
            self.assertLess(dur3, 100.0)
            self.assertIn("Runner In Progress", self.client.posted_messages[-1]["text"])

            # Step 4: Cancellation
            t0 = time.perf_counter()
            await self.gateway.handle_message(
                {"channel": "stress_channel", "thread_ts": "200.1", "ts": "200.304", "text": "cancel"},
                self.client,
            )
            dur4 = (time.perf_counter() - t0) * 1000.0
            self.assertLess(dur4, 100.0)
            mock_kill.assert_called_once()
            self.assertIsNone(self.session.active_execution)
            self.assertFalse(self.session.is_executing)
            self.assertIn("Execution Cancelled", self.client.posted_messages[-1]["text"])

    # =========================================================================
    # Test 6: Normal Task Ordering is Preserved (Does Not Bypass Lock)
    # =========================================================================

    async def test_normal_task_turns_queue_behind_lock_without_corruption(self) -> None:
        """
        Verify that non-checkin, substantive user requests (e.g. 'Deploy staging')
        do NOT bypass session.lock and instead safely serialize turn execution.
        """
        self.mock_adapter.delay = 0.2
        self.session.active_execution = None

        t_start = time.perf_counter()
        task1 = asyncio.create_task(
            self.gateway.handle_message(
                {"channel": "stress_channel", "thread_ts": "200.1", "ts": "200.401", "text": "Task Alpha"},
                self.client,
            )
        )
        await asyncio.sleep(0.02)
        task2 = asyncio.create_task(
            self.gateway.handle_message(
                {"channel": "stress_channel", "thread_ts": "200.1", "ts": "200.402", "text": "Task Beta"},
                self.client,
            )
        )

        await asyncio.gather(task1, task2)
        total_time_ms = (time.perf_counter() - t_start) * 1000.0

        # Since each turn takes ~200ms and they serialize, total time must be >= 350ms
        self.assertGreaterEqual(total_time_ms, 350.0)
        self.assertEqual(self.mock_adapter.turns_received, ["Task Alpha", "Task Beta"])
        self.assertEqual(self.session.current_turn, 2)

    # =========================================================================
    # Test 7: Resilience Against Buffer Observer Exceptions
    # =========================================================================

    async def test_observer_failure_does_not_block_checkins(self) -> None:
        """
        If buffer observer throws an unexpected exception during status check,
        check-in must still respond < 100ms with graceful fallback card.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="make test",
            pid=6655,
        )
        self.buffer_observer.read_buffer.side_effect = IOError("Herdr PTY read timeout")

        t0 = time.perf_counter()
        await self.gateway.handle_message(
            {"channel": "stress_channel", "thread_ts": "200.1", "ts": "200.501", "text": "how's it going?"},
            self.client,
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        self.assertLess(elapsed_ms, 100.0)
        self.assertEqual(len(self.client.posted_messages), 1)
        self.assertIn("Runner In Progress", self.client.posted_messages[-1]["text"])
        self.assertIn("PID 6655", self.client.posted_messages[-1]["text"])

    # =========================================================================
    # Test 8: Adversarial Phrasing & Punctuation Variations Under Execution
    # =========================================================================

    async def test_adversarial_phrasing_variations_under_pane2_execution(self) -> None:
        """
        Verify that diverse real-world conversational check-in and status phrases
        (with irregular casing, punctuation, emojis, and whitespace) are all recognized
        as out-of-band and respond in < 100ms without blocking.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="python3 -m train",
            pid=77889,
        )

        variations = [
            "  HOW'S IT GOING???  ",
            "what are you doing???",
            "What are you doing right now?",
            "Are you still working?",
            "Is it still running?",
            "Still running?",
            "any updates?",
            "ping",
            "updates?",
            "what's the status?",
            "what is the progress?",
            "check status",
            "where are we?",
            "what is happening?",
            "what's the url?",
            "what is the link?",
        ]

        for idx, phrase in enumerate(variations):
            client = ThreadSafeMockSlackClient()
            t0 = time.perf_counter()
            event = {
                "channel": "stress_channel",
                "thread_ts": "200.1",
                "ts": f"200.{600 + idx}",
                "text": phrase,
            }
            await self.gateway.handle_message(event, client)
            dur_ms = (time.perf_counter() - t0) * 1000.0

            self.assertLess(
                dur_ms,
                100.0,
                f"Phrase '{phrase}' took {dur_ms:.2f}ms (exceeds 100ms)",
            )
            self.assertEqual(
                len(client.posted_messages),
                1,
                f"Phrase '{phrase}' failed to receive immediate response card",
            )
            msg = client.posted_messages[0]["text"]
            self.assertTrue(
                "PID 77889" in msg or "Runner In Progress" in msg or "Liaison Update" in msg,
                f"Phrase '{phrase}' produced unexpected card: {msg}",
            )

    # =========================================================================
    # Test 9: 50 Concurrent Check-Ins Latency Distribution (p50, p95, p99 < 100ms)
    # =========================================================================

    async def test_50_concurrent_checkins_latency_distribution(self) -> None:
        """
        Stress-test gateway with 50 simultaneous concurrent check-ins.
        Measure latency distribution to prove p50, p95, and p99 are strictly < 100ms.
        """
        self.session.active_execution = ExecutionHandle(
            status="running",
            pane="runner",
            command="docker build -t app:latest .",
            pid=11224,
        )

        clients = [ThreadSafeMockSlackClient() for _ in range(50)]
        latencies_ms: List[float] = [0.0] * 50

        prompts = [
            "how's it going?",
            "what are you doing?",
            "status",
            "are you still there?",
            "ping",
        ] * 10  # 50 items

        async def worker(i: int):
            t0 = time.perf_counter()
            event = {
                "channel": "stress_channel",
                "thread_ts": "200.1",
                "ts": f"200.{700 + i}",
                "text": prompts[i],
            }
            await self.gateway.handle_message(event, clients[i])
            latencies_ms[i] = (time.perf_counter() - t0) * 1000.0

        tasks = [asyncio.create_task(worker(i)) for i in range(50)]
        await asyncio.gather(*tasks)

        sorted_lats = sorted(latencies_ms)
        p50 = sorted_lats[int(len(sorted_lats) * 0.50)]
        p95 = sorted_lats[int(len(sorted_lats) * 0.95)]
        p99 = sorted_lats[int(len(sorted_lats) * 0.99)]
        p_max = sorted_lats[-1]

        # Verify all percentiles are < 100ms
        self.assertLess(p50, 50.0, f"p50 latency {p50:.2f}ms exceeds 50ms")
        self.assertLess(p95, 80.0, f"p95 latency {p95:.2f}ms exceeds 80ms")
        self.assertLess(p99, 100.0, f"p99 latency {p99:.2f}ms exceeds 100ms")
        self.assertLess(p_max, 100.0, f"max latency {p_max:.2f}ms exceeds 100ms")


if __name__ == "__main__":
    unittest.main()

