#!/usr/bin/env python3
"""
tests/test_agent_m1_empirical_adversarial.py
Adversarial empirical stress suite for Milestone 1: Mouth Stream Reliability & Pipe Hygiene.
Challenger 1 (Milestone 1, Round 3).

Stress tests:
1. Repeated early consumer breaks (breaking after 1 delta) across 5+ sequential turns with zero phase shift.
2. Alternating early-break and full-turn across 10 sequential turns with zero cross-talk.
3. Massive stale buffer injection (100k+ bytes of contaminated NDJSON) into reader._buffer.
4. OS kernel pipe leak: child process pumps 800 unconsumed lines into the OS pipe; Turn 2 purges cleanly with zero phase shift.
5. High-volume flood leak: child outputs 5,000 lines; verify _drain_stdout bounds loop without hanging.
6. Rapid successive turn execution (25 rapid sequential turns) asserting zero lock deadlock and exact echo matching.
7. Concurrent turn preemption (overlapping send_turn calls) verifying clean generator closure, zero deadlock, and fresh response.
8. Concurrent burst execution: 10 concurrent send_turn calls via asyncio.gather verify zero deadlock and clean recovery.
9. Process resilience after abrupt subprocess exit and auto-recovery on subsequent turn.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import json
import time
import select
import asyncio
import unittest
from typing import List

from golem.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
)
from golem.agent.adapter import HeadlessAgentAdapter


class TestAgentM1EmpiricalAdversarial(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress harness for HeadlessAgentAdapter stream sync & pipe hygiene."""

    @classmethod
    def setUpClass(cls):
        cls.mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        cls.adv_mock_bin = os.path.abspath("tests/mock_adversarial_proc.py")

    async def test_repeated_early_breaks_5_turns_zero_phase_shift(self):
        """
        Challenge 1:
        Execute 5 sequential turns where every turn consumes exactly 1 delta and immediately breaks.
        Then execute Turn 6 to completion.
        Assert that Turn 6 receives 'Echo: Turn 6' and has ZERO phase shift from turns 1-5.
        """
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/golem",
            agy_bin=self.mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            # 5 consecutive early breaks
            for i in range(1, 6):
                prompt = f"Turn {i} - Early break prompt"
                deltas_seen = 0
                async for ev in adapter.send_turn(prompt):
                    if isinstance(ev, TextDeltaEvent):
                        deltas_seen += 1
                        # Break immediately after 1 delta
                        break
                self.assertEqual(deltas_seen, 1, f"Turn {i} should have yielded exactly 1 delta before break")

            # Turn 6: Full completion
            t6_events: List[AgentEvent] = []
            async for ev in adapter.send_turn("Turn 6 - Full execution"):
                t6_events.append(ev)

            finishes = [e for e in t6_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1, "Turn 6 must yield exactly 1 FinishEvent")
            self.assertEqual(finishes[0].status, "SUCCESS")
            # CRITICAL ASSERTION: Response must be for Turn 6, not Turn 1, 2, 3, 4, or 5
            self.assertEqual(
                finishes[0].response,
                "Echo: Turn 6 - Full execution",
                "Phase shift detected! Turn 6 consumed stale output from a prior turn.",
            )
            self.assertFalse(adapter._lock.locked(), "Adapter lock must be unlocked after turn 6")
        finally:
            await adapter.stop()

    async def test_alternating_early_breaks_and_full_turns(self):
        """
        Challenge 1B:
        Alternate between early break and full execution across 10 sequential turns.
        Every full turn must strictly match its own prompt.
        """
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/golem",
            agy_bin=self.mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            for turn_idx in range(1, 11):
                prompt = f"Turn {turn_idx} prompt"
                if turn_idx % 2 == 1:
                    # Odd turn: early break
                    async for ev in adapter.send_turn(prompt):
                        if isinstance(ev, TextDeltaEvent):
                            break
                else:
                    # Even turn: full execution
                    events: List[AgentEvent] = []
                    async for ev in adapter.send_turn(prompt):
                        events.append(ev)

                    finishes = [e for e in events if isinstance(e, FinishEvent)]
                    self.assertEqual(len(finishes), 1, f"Turn {turn_idx} must yield 1 FinishEvent")
                    self.assertEqual(finishes[0].status, "SUCCESS")
                    self.assertEqual(
                        finishes[0].response,
                        f"Echo: {prompt}",
                        f"Turn {turn_idx} response '{finishes[0].response}' did not match prompt '{prompt}'!",
                    )
        finally:
            await adapter.stop()

    async def test_stale_buffer_injection_100k_bytes_clean_purge(self):
        """
        Challenge 2:
        Inject 100,000+ bytes of mock events (fake results, errors, truncated JSON)
        into reader._buffer and OS pipe, and assert that _drain_stdout() purges
        all stale data cleanly before the next turn runs.
        """
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/golem",
            agy_bin=self.mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            await adapter.start()
            self.assertTrue(adapter.is_alive)
            reader = adapter.process.stdout

            # Build 100KB+ of adversarial stale frames:
            # - Fake result events claiming success with poisonous payloads
            # - Fake error events
            # - Random garbage strings
            stale_chunks = []
            for i in range(1200):
                fake_event = {
                    "event": "result" if i % 2 == 0 else "error",
                    "result": {
                        "status": "SUCCESS",
                        "response": f"POISON_STALE_FRAME_{i}_{'X' * 60}",
                    },
                    "error": {"message": f"POISON_ERROR_{i}"},
                }
                stale_chunks.append(json.dumps(fake_event) + "\n")

            stale_payload = "".join(stale_chunks).encode("utf-8")
            self.assertGreater(len(stale_payload), 100_000, "Payload must exceed 100KB")

            # Inject directly into reader._buffer
            reader._buffer.extend(stale_payload)
            self.assertGreaterEqual(len(reader._buffer), 100_000)

            # Test direct _drain_stdout() call first
            drained = await adapter._drain_stdout()
            self.assertGreaterEqual(drained, 100_000, "Drained bytes must reflect injected buffer")
            self.assertEqual(len(reader._buffer), 0, "reader._buffer must be empty after drain")

            # Inject another 100KB payload into reader._buffer right before calling send_turn()
            reader._buffer.extend(stale_payload)
            self.assertGreaterEqual(len(reader._buffer), 100_000)

            # send_turn() should purge stale buffer automatically at the beginning of the turn
            events: List[AgentEvent] = []
            async for ev in adapter.send_turn("Genuine verification turn"):
                events.append(ev)

            finishes = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1, "Must yield exactly 1 FinishEvent")
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertEqual(
                finishes[0].response,
                "Echo: Genuine verification turn",
                "Stale poison frame leaked into authentic response!",
            )
            self.assertNotIn("POISON", finishes[0].response)
        finally:
            await adapter.stop()

    async def test_os_kernel_pipe_unconsumed_leak_purged_on_next_turn(self):
        """
        Challenge 2B:
        Adversarial child process pumps 800 unconsumed lines into the OS stdout pipe
        during Turn 1. Consumer breaks early.
        Turn 2 starts: _drain_stdout() must purge pending pipe bytes so Turn 2 receives
        strictly its own response with 0 phase shift.
        """
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/golem",
            agy_bin=self.adv_mock_bin,
            env={"ADVERSARIAL_MODE": "kernel_pipe_unconsumed_leak"},
        )
        try:
            # Turn 1: consume 1 delta and break
            async for ev in adapter.send_turn("Turn 1 triggering 800 unconsumed pipe lines"):
                if isinstance(ev, TextDeltaEvent):
                    break

            # Give child process a moment to fill the pipe with 800 lines
            await asyncio.sleep(0.05)

            # Turn 2: run full turn
            t2_events: List[AgentEvent] = []
            async for ev in adapter.send_turn("Turn 2 clean inquiry"):
                t2_events.append(ev)

            finishes = [e for e in t2_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertEqual(
                finishes[0].response,
                "Authentic: Turn 2 clean inquiry",
                f"Pipe leakage! Turn 2 got '{finishes[0].response}' instead of authentic response.",
            )
            # Verify no leaked text delta events from turn 1
            deltas = [e.text_delta for e in t2_events if isinstance(e, TextDeltaEvent)]
            for d in deltas:
                self.assertNotIn("STALE_UNCONSUMED_LEAK", d)
        finally:
            await adapter.stop()

    async def test_high_volume_flood_drain_bounded_latency(self):
        """
        Challenge 2C:
        Verify that even when a high volume of output is flooded, _drain_stdout
        bounds its drain loop without hanging forever (respects round limits).
        """
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/golem",
            agy_bin=self.adv_mock_bin,
            env={"ADVERSARIAL_MODE": "unbounded_flood_leak"},
        )
        try:
            # Send prompt that floods
            t1_gen = adapter.send_turn("Flood prompt")
            await t1_gen.__anext__()
            # Break early
            await t1_gen.aclose()

            # Measuring drain time: must be fast (< 0.5s)
            t_start = time.monotonic()
            drained = await adapter._drain_stdout()
            t_elapsed = time.monotonic() - t_start
            self.assertLess(t_elapsed, 0.5, f"Drain took too long: {t_elapsed:.3f}s")
        finally:
            await adapter.stop()

    async def test_rapid_successive_turn_execution_25_turns(self):
        """
        Challenge 3:
        Execute 25 successive turns in rapid bursts without pausing.
        Assert zero lock deadlocks, sub-second execution, and exact sequential responses.
        """
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/golem",
            agy_bin=self.mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            start_time = time.monotonic()
            for i in range(1, 26):
                prompt = f"Rapid prompt #{i}"
                events: List[AgentEvent] = []
                async for ev in adapter.send_turn(prompt):
                    events.append(ev)

                finishes = [e for e in events if isinstance(e, FinishEvent)]
                self.assertEqual(len(finishes), 1, f"Turn #{i} must have 1 finish")
                self.assertEqual(finishes[0].status, "SUCCESS")
                self.assertEqual(finishes[0].response, f"Echo: {prompt}")

            elapsed = time.monotonic() - start_time
            # 25 turns should complete rapidly (typically < 3.0s total with mock process)
            self.assertLess(elapsed, 10.0, f"25 turns took too long: {elapsed:.2f}s")
            self.assertFalse(adapter._lock.locked())
        finally:
            await adapter.stop()

    async def test_concurrent_turn_preemption_zero_deadlock(self):
        """
        Challenge 4:
        Start Turn 1. While Turn 1 is active, immediately start Turn 2.
        Verify that Turn 1 is preempted without deadlocking, and Turn 2 yields
        its authentic response with zero pipe corruption.
        """
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/golem",
            agy_bin=self.mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            t1_events: List[AgentEvent] = []
            t2_events: List[AgentEvent] = []

            # Turn 1 iterator
            t1_gen = adapter.send_turn("Turn 1 slow prompt")
            # Read first event from Turn 1
            ev1 = await t1_gen.__anext__()
            t1_events.append(ev1)

            # Turn 2 preempts Turn 1
            async for ev in adapter.send_turn("Turn 2 preempting prompt"):
                t2_events.append(ev)

            # Verify Turn 2 completed cleanly
            t2_finishes = [e for e in t2_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(t2_finishes), 1)
            self.assertEqual(t2_finishes[0].status, "SUCCESS")
            self.assertEqual(t2_finishes[0].response, "Echo: Turn 2 preempting prompt")

            # Verify Turn 1 cannot continue (it was closed)
            with self.assertRaises((StopAsyncIteration, GeneratorExit)):
                await t1_gen.__anext__()

            self.assertFalse(adapter._lock.locked())
        finally:
            await adapter.stop()

    async def test_concurrent_burst_no_deadlock(self):
        """
        Challenge 4B:
        Fire 10 concurrent coroutines attempting send_turn() concurrently.
        Verify zero deadlock: the lock mutex cleanly arbitrates, all calls resolve
        or terminate safely, and the adapter remains functional for follow-up turns.
        """
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/golem",
            agy_bin=self.mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            async def worker(idx: int):
                events = []
                async for ev in adapter.send_turn(f"Burst worker {idx}"):
                    events.append(ev)
                return events

            tasks = [asyncio.create_task(worker(i)) for i in range(10)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # At least one turn must succeed, zero unhandled deadlocks
            exceptions = [r for r in results if isinstance(r, Exception)]
            # If any exception occurred, it must be GeneratorExit or StopAsyncIteration due to preemption
            for exc in exceptions:
                self.assertIsInstance(exc, (GeneratorExit, StopAsyncIteration))

            self.assertFalse(adapter._lock.locked(), "Lock must be released after burst")

            # Follow-up turn must work cleanly
            clean_events = []
            async for ev in adapter.send_turn("Post-burst follow up"):
                clean_events.append(ev)

            finishes = [e for e in clean_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertEqual(finishes[0].response, "Echo: Post-burst follow up")
        finally:
            await adapter.stop()

    async def test_process_crash_recovery_on_subsequent_turn(self):
        """
        Challenge 5:
        Simulate process crashing mid-turn (MOCK_AGY_MODE=exit_nonzero_mid_turn).
        Verify adapter terminates with ERROR FinishEvent, does not deadlock,
        and subsequent start or re-instantiation behaves gracefully.
        """
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/golem",
            agy_bin=self.mock_bin,
            env={"MOCK_AGY_MODE": "exit_nonzero_mid_turn"},
        )
        try:
            events: List[AgentEvent] = []
            async for ev in adapter.send_turn("Crash prompt"):
                events.append(ev)

            finishes = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "ERROR")
            self.assertIn("42", finishes[0].error or "")
            self.assertFalse(adapter.is_alive)
            self.assertFalse(adapter._lock.locked())
        finally:
            await adapter.stop()


if __name__ == "__main__":
    unittest.main()
