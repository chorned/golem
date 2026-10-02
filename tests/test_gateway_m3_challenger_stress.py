#!/usr/bin/env python3
"""
tests/test_gateway_m3_challenger_stress.py
Empirical Challenger 3 Stress Harness for Milestone 3 Gate 2.

Mandates:
1. Stress test early generator break recovery across 500+ iterations: verify 0 deadlocks on subsequent turns.
2. Stress test concurrent session.close() during in-flight turn execution: verify 0 zombie state resurrects (session.state remains CLOSED).
3. Stress test router.get_or_create() replacing FAILED sessions: verify 0 leaked child adapters under single-threaded and concurrent burst load.
"""

from __future__ import annotations

import asyncio
import time
import unittest
from typing import List, AsyncGenerator, Any

from first_mate.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    AgentEvent,
)
from first_mate.gateway.session import SessionHandle, SessionState
from first_mate.gateway.router import SessionRouter


class ChallengerMockAdapter:
    """Mock adapter simulating streaming events with lifecycle tracking."""

    def __init__(self, cwd: str = "/mock/cwd") -> None:
        self.cwd = cwd
        self.started = False
        self.stopped = False
        self.start_count = 0
        self.stop_count = 0
        self.turns_count = 0

    async def start(self) -> None:
        self.start_count += 1
        self.started = True

    async def stop(self) -> None:
        self.stop_count += 1
        self.stopped = True

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_count += 1
        yield StatusUpdateEvent(status="working")
        for i in range(5):
            await asyncio.sleep(0.001)
            yield TextDeltaEvent(text_delta=f"chunk-{i}-{prompt}")
        yield FinishEvent(response=f"Done: {prompt}", status="SUCCESS")


class TestGatewayM3ChallengerStress(unittest.IsolatedAsyncioTestCase):
    """Deep empirical challenge suite for M3 Gate 2 verification."""

    async def test_stress_500_early_generator_breaks_no_deadlock(self) -> None:
        """
        MANDATE 2:
        Stress test early generator break recovery across 500+ iterations:
        verify 0 deadlocks on subsequent turns even when generator references
        are deliberately retained in memory across turns and tasks.
        """
        adapter = ChallengerMockAdapter()
        session = SessionHandle("C_500:T_500", "C_500", "T_500", adapter)
        session.state = SessionState.IDLE

        retained_generators: List[Any] = []
        deadlock_count = 0
        completed_turns = 0

        start_time = time.monotonic()

        # Part A: Same-task sequential early breaks (300 iterations)
        for i in range(300):
            prompt1 = f"Same-task early break turn {i}"
            gen1 = session.send_user_message(prompt1)
            retained_generators.append(gen1)

            # Consume 1 chunk and break early
            async for _ in gen1:
                break

            # Subsequent turn in same task: must preempt lingering generator without deadlock
            prompt2 = f"Same-task recovery turn {i}"
            turn2_events = []
            try:
                async def execute_same_task():
                    async for ev in session.send_user_message(prompt2):
                        turn2_events.append(ev)

                await asyncio.wait_for(execute_same_task(), timeout=0.5)
                completed_turns += 1
            except (asyncio.TimeoutError, TimeoutError):
                deadlock_count += 1

            self.assertFalse(session.lock.locked(), f"Lock remained acquired at same-task iteration {i}")
            self.assertTrue(len(turn2_events) > 0, f"Same-task turn did not produce events at iteration {i}")

        # Part B: Distinct completed task early breaks (300 iterations)
        # Simulates separate task dispatch (e.g. Slack event handlers) where task 1 finishes
        for i in range(300):
            prompt1 = f"Cross-task early break turn {i}"

            async def run_broken_task():
                g = session.send_user_message(prompt1)
                retained_generators.append(g)
                async for _ in g:
                    break

            t1 = asyncio.create_task(run_broken_task())
            await t1  # Task 1 completes

            # Task 2 handles the subsequent recovery turn
            prompt2 = f"Cross-task recovery turn {i}"
            turn2_events = []

            async def run_recovery_task():
                async for ev in session.send_user_message(prompt2):
                    turn2_events.append(ev)

            try:
                t2 = asyncio.create_task(run_recovery_task())
                await asyncio.wait_for(t2, timeout=0.5)
                completed_turns += 1
            except (asyncio.TimeoutError, TimeoutError):
                deadlock_count += 1

            self.assertFalse(session.lock.locked(), f"Lock remained acquired at cross-task iteration {i}")
            self.assertTrue(len(turn2_events) > 0, f"Cross-task turn did not produce events at iteration {i}")

        elapsed = time.monotonic() - start_time
        total_iterations = 600

        # Empirical Assertions
        self.assertEqual(
            deadlock_count,
            0,
            f"Observed {deadlock_count} deadlocks across {total_iterations} early break iterations!",
        )
        self.assertEqual(
            completed_turns,
            total_iterations,
            f"Expected {total_iterations} completed recovery turns, got {completed_turns}",
        )
        self.assertEqual(len(retained_generators), total_iterations)
        self.assertFalse(session.lock.locked(), "Final session lock must be unlocked")
        self.assertEqual(session.state, SessionState.IDLE)
        self.assertLess(
            elapsed,
            20.0,
            f"600 iterations took {elapsed:.2f}s, exceeding 20.0s budget",
        )

    async def test_stress_concurrent_close_in_flight_no_zombie_resurrection(self) -> None:
        """
        MANDATE 3:
        Stress test concurrent session.close() during in-flight turn execution:
        across 100 iterations with jittered delays, verify 0 zombie state resurrects.
        session.state must strictly remain CLOSED.
        """
        iterations = 100
        zombie_resurrections = 0
        unstopped_adapters = 0

        for i in range(iterations):
            adapter = ChallengerMockAdapter()
            session = SessionHandle(f"C_Z:{i}", "C_Z", f"T_{i}", adapter)
            session.state = SessionState.IDLE

            async def run_turn():
                try:
                    async for _ in session.send_user_message(f"in-flight turn {i}"):
                        pass
                except Exception:
                    pass

            turn_task = asyncio.create_task(run_turn())

            # Vary the timing of close(): 0ms, 1ms, 2ms, 3ms, 4ms
            delay = (i % 5) * 0.001
            if delay > 0:
                await asyncio.sleep(delay)

            # Concurrent close triggered mid-flight
            await session.close(reason="concurrent_stress_close")
            self.assertEqual(session.state, SessionState.CLOSED)

            # Wait for turn task to finish
            await turn_task

            # Check for zombie resurrection
            if session.state != SessionState.CLOSED:
                zombie_resurrections += 1

            if not adapter.stopped:
                unstopped_adapters += 1

            # Verify that subsequent messages are rejected
            with self.assertRaises(RuntimeError):
                async for _ in session.send_user_message("post-close message"):
                    pass

        self.assertEqual(
            zombie_resurrections,
            0,
            f"Observed {zombie_resurrections} zombie state resurrects out of {iterations} concurrent close runs!",
        )
        self.assertEqual(
            unstopped_adapters,
            0,
            f"Observed {unstopped_adapters} unstopped adapters after close!",
        )

    async def test_stress_router_failed_session_replacement_zero_adapter_leaks(self) -> None:
        """
        MANDATE 4:
        Stress test router.get_or_create() replacing FAILED sessions:
        verify 0 leaked child adapters under sequential and concurrent burst load.
        """
        created_adapters: List[ChallengerMockAdapter] = []

        def factory(cwd: str) -> ChallengerMockAdapter:
            a = ChallengerMockAdapter(cwd=cwd)
            created_adapters.append(a)
            return a

        router = SessionRouter(adapter_factory=factory)

        # Part A: Sequential replacement across 100 cycles
        sequential_cycles = 100
        for i in range(sequential_cycles):
            s, is_new = await router.get_or_create("C_SEQ", "T_SEQ")
            self.assertEqual(s.state, SessionState.IDLE)
            idx = len(created_adapters) - 1

            # Mark session as FAILED (simulating runtime crash)
            s.state = SessionState.FAILED

            # Next call must detect FAILED and replace it
            s_next, is_new_next = await router.get_or_create("C_SEQ", "T_SEQ")
            self.assertTrue(is_new_next)
            self.assertIsNot(s, s_next)

            # Old adapter MUST be stopped
            self.assertTrue(
                created_adapters[idx].stopped,
                f"Adapter {idx} was leaked without stop() during failed session replacement!",
            )

        # Part B: High concurrency race replacement (50 concurrent tasks hitting a FAILED session)
        s_base, _ = await router.get_or_create("C_RACE", "T_RACE")
        s_base.state = SessionState.FAILED
        base_adapter = created_adapters[-1]
        self.assertFalse(base_adapter.stopped)

        adapters_before = len(created_adapters)

        async def concurrent_worker(task_id: int):
            return await router.get_or_create("C_RACE", "T_RACE")

        tasks = [asyncio.create_task(concurrent_worker(j)) for j in range(50)]
        results = await asyncio.gather(*tasks)

        # The prior failed session's adapter MUST be stopped
        self.assertTrue(
            base_adapter.stopped,
            "Base failed session adapter was not stopped under concurrent replacement!",
        )

        # Exactly 1 new session and 1 new adapter spawned among all 50 concurrent callers
        adapters_after = len(created_adapters)
        self.assertEqual(
            adapters_after - adapters_before,
            1,
            f"Expected exactly 1 new adapter spawned during concurrent replacement, got {adapters_after - adapters_before}",
        )

        # Exactly 1 caller got is_new=True, 49 got is_new=False
        new_flags = [is_new for _, is_new in results]
        self.assertEqual(new_flags.count(True), 1)
        self.assertEqual(new_flags.count(False), 49)

        # All 50 callers received the identical replacement handle
        rep_session = results[0][0]
        for session_h, _ in results:
            self.assertIs(session_h, rep_session)
        self.assertEqual(rep_session.state, SessionState.IDLE)

    def test_adversarial_mention_stripping_edge_cases(self) -> None:
        """
        Adversarial test on mention stripping:
        Tests exotic Slack mention patterns with display labels, pipes, whitespace,
        multiple mentions, and empty labels.
        """
        from first_mate.gateway.ingress import strip_bot_mentions

        cases = [
            ("<@U_AGY_BOT|firstmate> status", "status"),
            ("<@U_AGY_BOT|firstmate|alias> status", "status"),
            ("<@U_AGY_BOT> status", "status"),
            ("<@U_AGY_BOT|> status", "status"),
            ("<@U_AGY_BOT|firstmate>   <@U_OTHER|helper>   status", "status"),
            ("   @agy   status", "status"),
            ("   @FirstMate   status", "status"),
            ("<@U_AGY_BOT|firstmate>", ""),
            ("<@U_AGY_BOT|>", ""),
            ("No mention in text", "No mention in text"),
        ]

        for raw_in, expected_out in cases:
            actual = strip_bot_mentions(raw_in, "U_AGY_BOT")
            self.assertEqual(
                actual,
                expected_out,
                f"Failed stripping mention: '{raw_in}' -> expected '{expected_out}', got '{actual}'",
            )

    async def test_close_with_faulty_adapter_stop(self) -> None:
        """
        Adversarial test: Adapter.stop() raises an unexpected exception during session.close().
        Verifies:
        - close() does NOT raise or crash the caller.
        - session.state transitions to CLOSED and remains CLOSED.
        - Lock and generator cleanup still execute cleanly.
        """
        class FaultyAdapter(ChallengerMockAdapter):
            async def stop(self) -> None:
                raise RuntimeError("Hardware disconnect during adapter.stop()!")

        adapter = FaultyAdapter()
        session = SessionHandle("C_ERR:T_ERR", "C_ERR", "T_ERR", adapter)
        session.state = SessionState.IDLE

        # Start a turn and break early
        g = session.send_user_message("turn before close")
        async for _ in g:
            break

        # Close must handle adapter exception gracefully
        await session.close(reason="fault_test")
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertFalse(session.lock.locked())

        # Further turns must be rejected with RuntimeError
        with self.assertRaises(RuntimeError):
            async for _ in session.send_user_message("turn after close"):
                pass

    async def test_reaper_spares_active_working_sessions(self) -> None:
        """
        Adversarial test: Inactivity reaper runs while a session is actively WORKING.
        Even if created a long time ago, a session actively working must not be prematurely reaped.
        """
        adapter = ChallengerMockAdapter()
        router = SessionRouter(inactivity_timeout=10.0, adapter_factory=lambda cwd: adapter)
        session, _ = await router.get_or_create("C_ACTIVE", "T_ACTIVE")

        # Fake old creation time but recent touch
        session.created_at = time.time() - 1000.0
        session.last_active_ts = time.time()  # active now!
        session.state = SessionState.WORKING

        pruned = await router.reap_expired_sessions()
        self.assertEqual(len(pruned), 0, "Reaper improperly pruned an active WORKING session!")
        self.assertIn("C_ACTIVE:T_ACTIVE", router.sessions)
        self.assertFalse(adapter.stopped)


if __name__ == "__main__":
    unittest.main()
