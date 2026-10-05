#!/usr/bin/env python3
"""
tests/test_gateway_m3_stress.py
Empirical stress and adversarial verification suite for Milestone 3:
Ingress Gateway & Thread Session State.

Tests:
1. High concurrency burst (100 concurrent async tasks calling get_or_create()
   on identical and distinct keys, verifying 0 duplicate adapters and 0 deadlocks).
2. Inactivity reaper stress (50 active sessions, mock time advanced by 14,401s,
   verifying all 50 underlying adapters stopped and session registry cleanly pruned).
3. Multi-turn turn locking (5 rapid concurrent user messages into the same
   SessionHandle, verifying strict turn serialization through handle.lock without interleaving).
4. Adversarial stress & edge cases:
   - Early generator break (ensuring lock is released and no deadlock on next turn)
   - Adapter error during turn (verifying state=FAILED and lock release)
   - Closed session rejection (RuntimeError raised before/after lock acquisition)
   - Mixed high-concurrency race (get_or_create, send_user_message, teardown, reaper)
"""

import os
import sys
import time
import asyncio
import unittest
from unittest.mock import patch
from typing import List, Dict, Any, AsyncGenerator, Optional

from golem.agent.adapter import HeadlessAgentAdapter

from golem.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    AgentEvent,
)
from golem.gateway.session import SessionHandle, SessionState
from golem.gateway.router import SessionRouter


class StressMockAdapter:
    """Controllable mock adapter simulating async latency, state, and lifecycle."""

    def __init__(self, cwd: str = "/mock/cwd", start_delay: float = 0.005) -> None:
        self.cwd = cwd
        self.start_delay = start_delay
        self.started = False
        self.stopped = False
        self.start_call_count = 0
        self.stop_call_count = 0
        self.turns_handled: List[str] = []
        self.fail_on_turn = False

    async def start(self) -> None:
        self.start_call_count += 1
        if self.start_delay > 0:
            await asyncio.sleep(self.start_delay)
        self.started = True

    async def stop(self) -> None:
        self.stop_call_count += 1
        self.stopped = True

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_handled.append(prompt)
        yield StatusUpdateEvent(status="working")

        if self.fail_on_turn:
            raise RuntimeError(f"Simulated adapter failure during turn '{prompt}'")

        # Yield multiple text deltas with small yield to test interleaving
        for i in range(3):
            await asyncio.sleep(0.01)
            yield TextDeltaEvent(text_delta=f"delta-{i}-for-{prompt}")

        yield FinishEvent(response=f"Complete: {prompt}", status="SUCCESS")


class TestGatewayM3Stress(unittest.IsolatedAsyncioTestCase):
    """Empirical stress test suite for SessionRouter and SessionHandle."""

    # -------------------------------------------------------------------------
    # 1. High Concurrency Burst (100 concurrent tasks)
    # -------------------------------------------------------------------------
    async def test_high_concurrency_burst_identical_key(self) -> None:
        """
        Simulate 100 concurrent async tasks calling get_or_create() on the EXACT SAME session key.
        Verifies:
        - Exactly 1 underlying adapter spawned.
        - Exactly 1 task receives is_new=True; 99 receive is_new=False.
        - All 100 tasks receive the identical SessionHandle instance.
        - 0 deadlocks under high contention.
        """
        created_adapters: List[StressMockAdapter] = []

        def adapter_factory(cwd: str) -> StressMockAdapter:
            adapter = StressMockAdapter(cwd=cwd, start_delay=0.01)
            created_adapters.append(adapter)
            return adapter

        router = SessionRouter(adapter_factory=adapter_factory)

        async def worker(task_id: int):
            # Concurrent burst targeting the same thread
            return await router.get_or_create(
                channel="C_SHARED",
                thread_ts="1700000000.0001",
                target_cwd="/test/path",
            )

        # Launch 100 concurrent tasks simultaneously
        tasks = [asyncio.create_task(worker(i)) for i in range(100)]
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10.0)

        # Verification 1: Exactly 1 adapter created and started
        self.assertEqual(len(created_adapters), 1, "Duplicate adapters spawned for identical key!")
        self.assertEqual(created_adapters[0].start_call_count, 1)
        self.assertTrue(created_adapters[0].started)

        # Verification 2: All 100 tasks returned the exact same SessionHandle
        first_session = results[0][0]
        for session, is_new in results:
            self.assertIs(session, first_session)

        # Verification 3: Exactly one is_new is True, 99 are False
        new_flags = [is_new for _, is_new in results]
        self.assertEqual(new_flags.count(True), 1)
        self.assertEqual(new_flags.count(False), 99)

        # Verification 4: Registry state
        self.assertEqual(len(router.sessions), 1)
        self.assertIn("C_SHARED:1700000000.0001", router.sessions)

    async def test_high_concurrency_burst_distinct_and_shared_keys(self) -> None:
        """
        Simulate 100 concurrent async tasks across 10 distinct session keys (10 tasks per key).
        Verifies:
        - Exactly 10 adapters spawned across the 10 keys.
        - Zero duplicate adapters per key.
        - Zero deadlocks under heavy parallel contention.
        """
        created_adapters: List[StressMockAdapter] = []

        def adapter_factory(cwd: str) -> StressMockAdapter:
            adapter = StressMockAdapter(cwd=cwd, start_delay=0.005)
            created_adapters.append(adapter)
            return adapter

        router = SessionRouter(adapter_factory=adapter_factory)

        async def worker(task_id: int):
            key_id = task_id % 10  # 10 keys, 10 tasks per key
            return await router.get_or_create(
                channel=f"C_{key_id}",
                thread_ts=f"T_{key_id}",
            )

        tasks = [asyncio.create_task(worker(i)) for i in range(100)]
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10.0)

        # Exactly 10 adapters spawned
        self.assertEqual(len(created_adapters), 10)
        self.assertEqual(len(router.sessions), 10)

        # Verify each key has exactly 1 True and 9 False for is_new
        key_new_counts: Dict[str, int] = {}
        for session, is_new in results:
            key = session.session_id
            if is_new:
                key_new_counts[key] = key_new_counts.get(key, 0) + 1

        self.assertEqual(len(key_new_counts), 10)
        for key, count in key_new_counts.items():
            self.assertEqual(count, 1, f"Key {key} had {count} is_new=True instances!")

    async def test_high_concurrency_burst_100_distinct_keys(self) -> None:
        """
        Simulate 100 concurrent async tasks across 100 completely distinct keys.
        Verifies:
        - Exactly 100 adapters spawned.
        - Exactly 100 sessions created.
        - All 100 tasks receive is_new=True.
        - Zero deadlocks under heavy parallel distinct creation.
        """
        created_adapters: List[StressMockAdapter] = []

        def adapter_factory(cwd: str) -> StressMockAdapter:
            adapter = StressMockAdapter(cwd=cwd, start_delay=0.002)
            created_adapters.append(adapter)
            return adapter

        router = SessionRouter(adapter_factory=adapter_factory)

        async def worker(task_id: int):
            return await router.get_or_create(
                channel=f"C_DISTINCT_{task_id}",
                thread_ts=f"T_DISTINCT_{task_id}",
            )

        tasks = [asyncio.create_task(worker(i)) for i in range(100)]
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10.0)

        self.assertEqual(len(created_adapters), 100)
        self.assertEqual(len(router.sessions), 100)
        self.assertTrue(all(is_new for _, is_new in results))
        self.assertTrue(all(a.started for a in created_adapters))

    # -------------------------------------------------------------------------
    # 2. Inactivity Reaper Stress (50 active sessions, 14,401s time advance)
    # -------------------------------------------------------------------------
    async def test_inactivity_reaper_stress_50_sessions(self) -> None:
        """
        Populate 50 active sessions. Advance mock time by 14,401 seconds (4h 1s).
        Run reap_expired_sessions().
        Verifies:
        - All 50 underlying adapters are stopped (adapter.stop() called).
        - All 50 sessions transition to SessionState.CLOSED.
        - Session registry is cleanly pruned (0 sessions remaining).
        - Correct list of 50 reaped keys returned.
        """
        adapters: List[StressMockAdapter] = []

        def adapter_factory(cwd: str) -> StressMockAdapter:
            adapter = StressMockAdapter(cwd=cwd, start_delay=0.0)
            adapters.append(adapter)
            return adapter

        router = SessionRouter(inactivity_timeout=14400.0, adapter_factory=adapter_factory)

        # Populate 50 active sessions
        for i in range(50):
            session, is_new = await router.get_or_create(
                channel=f"C_{i}",
                thread_ts=f"T_{i}",
            )
            self.assertTrue(is_new)
            self.assertEqual(session.state, SessionState.IDLE)

        self.assertEqual(len(router.sessions), 50)
        self.assertEqual(len(adapters), 50)
        self.assertTrue(all(a.started for a in adapters))
        self.assertTrue(all(not a.stopped for a in adapters))

        # Advance mock time by 14,401 seconds (4 hours + 1 second)
        current_real_time = time.time()
        future_time = current_real_time + 14401.0

        with patch("time.time", return_value=future_time):
            # Verify is_expired() evaluates True for all sessions
            for s in router.sessions.values():
                self.assertTrue(s.is_expired())

            # Run reaper sweep
            pruned_keys = await router.reap_expired_sessions()

        # Verifications
        self.assertEqual(len(pruned_keys), 50, f"Expected 50 reaped keys, got {len(pruned_keys)}")
        self.assertEqual(len(router.sessions), 0, "Session registry not completely pruned!")
        self.assertTrue(all(a.stopped for a in adapters), "Not all underlying adapters were stopped!")
        self.assertEqual(sum(a.stop_call_count for a in adapters), 50)

    async def test_inactivity_reaper_partial_expiry(self) -> None:
        """
        Test selective pruning: 25 expired sessions and 25 active sessions.
        Only the 25 expired sessions must be reaped and stopped; active remain untouched.
        """
        adapters: Dict[str, StressMockAdapter] = {}

        def adapter_factory(cwd: str) -> StressMockAdapter:
            adapter = StressMockAdapter(cwd=cwd, start_delay=0.0)
            return adapter

        router = SessionRouter(inactivity_timeout=14400.0, adapter_factory=adapter_factory)

        sessions = []
        for i in range(50):
            s, _ = await router.get_or_create(channel="C", thread_ts=f"T_{i}")
            adapters[s.session_id] = s.adapter
            sessions.append(s)

        # Make the first 25 sessions expired (last_active_ts = now - 15,000s)
        now = time.time()
        for i in range(25):
            sessions[i].last_active_ts = now - 15000.0
        # Keep remaining 25 sessions fresh (last_active_ts = now)
        for i in range(25, 50):
            sessions[i].last_active_ts = now

        pruned = await router.reap_expired_sessions()

        self.assertEqual(len(pruned), 25)
        self.assertEqual(len(router.sessions), 25)

        # Check that expired sessions are closed and stopped
        for i in range(25):
            self.assertEqual(sessions[i].state, SessionState.CLOSED)
            self.assertTrue(adapters[sessions[i].session_id].stopped)

        # Check that active sessions remain IDLE and not stopped
        for i in range(25, 50):
            self.assertEqual(sessions[i].state, SessionState.IDLE)
            self.assertFalse(adapters[sessions[i].session_id].stopped)
            self.assertIn(sessions[i].session_id, router.sessions)

    async def test_inactivity_reaper_fault_tolerant_adapter_errors(self) -> None:
        """
        Test that exceptions raised by adapter.stop() during reaper sweep do NOT
        halt the reaper or leave subsequent expired sessions unpruned.
        """
        adapters = []

        class FaultyStopAdapter(StressMockAdapter):
            def __init__(self, should_fail_stop: bool, **kwargs):
                super().__init__(**kwargs)
                self.should_fail_stop = should_fail_stop

            async def stop(self) -> None:
                self.stop_call_count += 1
                if self.should_fail_stop:
                    raise RuntimeError("Exploding adapter stop()")
                self.stopped = True

        def adapter_factory(cwd: str):
            # Alternate failing and succeeding stops
            should_fail = (len(adapters) % 2 == 0)
            a = FaultyStopAdapter(should_fail_stop=should_fail, cwd=cwd)
            adapters.append(a)
            return a

        router = SessionRouter(inactivity_timeout=14400.0, adapter_factory=adapter_factory)

        for i in range(20):
            await router.get_or_create(channel="C_FAULT", thread_ts=f"T_{i}")

        future_time = time.time() + 15000.0
        with patch("time.time", return_value=future_time):
            pruned = await router.reap_expired_sessions()

        # All 20 must be pruned despite 10 adapters throwing exceptions on stop()
        self.assertEqual(len(pruned), 20)
        self.assertEqual(len(router.sessions), 0)
        self.assertEqual(sum(a.stop_call_count for a in adapters), 20)

    async def test_failed_session_replacement_lifecycle(self) -> None:
        """
        Examine behavior when a session enters SessionState.FAILED and subsequent
        get_or_create() is invoked on that key.
        """
        adapters = []

        def adapter_factory(cwd: str):
            a = StressMockAdapter(cwd=cwd)
            adapters.append(a)
            return a

        router = SessionRouter(adapter_factory=adapter_factory)
        s1, is_new1 = await router.get_or_create(channel="C", thread_ts="T")
        self.assertTrue(is_new1)
        self.assertEqual(s1.state, SessionState.IDLE)

        # Trigger failure in s1
        s1.adapter.fail_on_turn = True
        with self.assertRaises(RuntimeError):
            async for _ in s1.send_user_message("bad turn"):
                pass
        self.assertEqual(s1.state, SessionState.FAILED)

        # Subsequent get_or_create on the same thread
        s2, is_new2 = await router.get_or_create(channel="C", thread_ts="T")
        self.assertTrue(is_new2, "Expected new session to be spawned to recover from FAILED state")
        self.assertIsNot(s1, s2)
        self.assertEqual(s2.state, SessionState.IDLE)
        self.assertIs(router.sessions["C:T"], s2)

    # -------------------------------------------------------------------------
    # 3. Multi-Turn Turn Locking (5 rapid concurrent user messages)
    # -------------------------------------------------------------------------
    async def test_multi_turn_turn_locking_no_interleaving(self) -> None:
        """
        Fire 5 rapid concurrent user messages into the same SessionHandle.
        Verifies:
        - Turns are strictly serialized through handle.lock.
        - Events from different turns NEVER interleave.
        - State transitions smoothly: IDLE -> WORKING -> IDLE per turn.
        - current_turn increments to 5.
        - 0 deadlocks or race conditions.
        """
        adapter = StressMockAdapter(start_delay=0.0)
        session = SessionHandle(
            session_id="C_LOCK:T_LOCK",
            channel="C_LOCK",
            thread_ts="T_LOCK",
            adapter=adapter,
        )
        session.state = SessionState.IDLE

        event_log: List[Tuple[int, str, float]] = []  # (turn_num, event_type/desc, timestamp)
        active_turns_in_flight = 0
        max_concurrent_turns = 0

        async def run_turn(turn_id: int):
            nonlocal active_turns_in_flight, max_concurrent_turns
            turn_events = []
            async for ev in session.send_user_message(f"Message-{turn_id}"):
                active_turns_in_flight += 1
                if active_turns_in_flight > max_concurrent_turns:
                    max_concurrent_turns = active_turns_in_flight
                
                desc = getattr(ev, "text", "") or getattr(ev, "status", "") or getattr(ev, "response", "")
                turn_events.append((turn_id, f"{ev.event_type}:{desc}"))
                event_log.append((turn_id, f"{ev.event_type}:{desc}", time.time()))
                
                # Small async yield inside event consumer to simulate Slack API dispatch
                await asyncio.sleep(0.002)
                active_turns_in_flight -= 1
            return turn_events

        # Launch 5 rapid concurrent turns on the exact same SessionHandle
        tasks = [asyncio.create_task(run_turn(i)) for i in range(5)]
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10.0)

        # Verification 1: Exactly 1 turn in flight at any given moment
        # Since active_turns_in_flight was checked inside the generator consumption,
        # max_concurrent_turns must never exceed 1.
        self.assertEqual(max_concurrent_turns, 1, f"Interleaving detected! max_concurrent_turns={max_concurrent_turns}")

        # Verification 2: Check event log for strict turn grouping (no interleaving)
        # All events for turn X must appear as a contiguous block in event_log
        seen_turns_in_order = []
        current_turn_in_log = None
        for turn_id, desc, _ in event_log:
            if turn_id != current_turn_in_log:
                if turn_id in seen_turns_in_order:
                    self.fail(f"Interleaving detected! Turn {turn_id} reappeared after turn {current_turn_in_log}")
                seen_turns_in_order.append(turn_id)
                current_turn_in_log = turn_id

        self.assertEqual(len(seen_turns_in_order), 5)

        # Verification 3: Session state and counters
        self.assertEqual(session.current_turn, 5)
        self.assertEqual(session.state, SessionState.IDLE)
        self.assertEqual(len(adapter.turns_handled), 5)

    # -------------------------------------------------------------------------
    # 4. Adversarial Edge Cases & Defect Reproduction
    # -------------------------------------------------------------------------
    async def test_early_generator_break_anonymous_gc_release(self) -> None:
        """
        Adversarial test: Consumer breaks early out of send_user_message() generator
        where the generator is anonymous (not held in variable).
        Verifies:
        - When the generator frame is garbage collected, lock is eventually released.
        - Subsequent turn can run after garbage collection.
        """
        adapter = StressMockAdapter()
        session = SessionHandle(
            session_id="C:T",
            channel="C",
            thread_ts="T",
            adapter=adapter,
        )
        session.state = SessionState.IDLE

        # Turn 1: Anonymous generator, break immediately after first event
        async for ev in session.send_user_message("First Turn"):
            break

        # Give event loop a micro-tick to finalize unreferenced generator
        await asyncio.sleep(0.01)

        # Turn 2: Must execute without deadlock within 2.0s
        turn2_events = []
        async def turn2():
            async for ev in session.send_user_message("Second Turn"):
                turn2_events.append(ev)

        await asyncio.wait_for(turn2(), timeout=2.0)
        self.assertTrue(len(turn2_events) > 0)
        self.assertEqual(session.current_turn, 2)
        self.assertEqual(session.state, SessionState.IDLE)

    async def test_defect_1_early_generator_break_referenced_deadlock(self) -> None:
        """
        Remediation verification of Defect 1:
        When a caller retains a generator reference in a variable, task, or closure,
        and breaks early, the active generator tracking and lock cleanup guarantee that
        the subsequent turn does NOT deadlock and executes cleanly.
        """
        adapter = StressMockAdapter()
        session = SessionHandle("C:T_DEFECT1", "C", "T_DEFECT1", adapter)
        session.state = SessionState.IDLE

        gen = session.send_user_message("Turn 1 with retained generator ref")
        async for _ in gen:
            break

        # Verification: Subsequent turn executes without deadlock
        deadlocked = False
        turn2_events = []
        try:
            async for ev in session.send_user_message("Turn 2"):
                turn2_events.append(ev)
        except asyncio.TimeoutError:
            deadlocked = True

        self.assertFalse(
            deadlocked,
            "Defect 1 resolved: Subsequent turn must not deadlock after early generator break",
        )
        self.assertTrue(len(turn2_events) > 0, "Turn 2 must yield events")
        self.assertFalse(session.lock.locked(), "Session lock must be released after Turn 2")

    async def test_defect_2_zombie_state_resurrection_after_concurrent_close(self) -> None:
        """
        Remediation verification of Defect 2:
        When session.close() is invoked (via /wrapup, /archive, or inactivity reaper)
        while a conversational turn is in-flight, close() sets state = CLOSED and stops the adapter.
        When the in-flight turn finishes, the CLOSED state must NOT be overwritten to IDLE.
        """
        adapter = StressMockAdapter(start_delay=0.0)
        session = SessionHandle("C:T_ZOMBIE", "C", "T_ZOMBIE", adapter)
        session.state = SessionState.IDLE

        async def run_turn():
            try:
                async for _ in session.send_user_message("long turn"):
                    pass
            except Exception:
                pass

        turn_task = asyncio.create_task(run_turn())
        await asyncio.sleep(0.01)  # Allow turn to start and enter WORKING state
        self.assertEqual(session.state, SessionState.WORKING)

        # Concurrent close occurs (e.g. /wrapup or reaper)
        await session.close(reason="wrapup")
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertTrue(adapter.stopped)

        # Wait for the in-flight turn to complete
        await turn_task

        # Defect 2 resolved: session must NOT be resurrected to IDLE; remains CLOSED!
        self.assertEqual(
            session.state,
            SessionState.CLOSED,
            "Defect 2 resolved: Session must remain CLOSED upon turn completion",
        )

        # A subsequent message will now raise RuntimeError because session is CLOSED
        with self.assertRaises(RuntimeError):
            async for _ in session.send_user_message("Turn 2 on closed session"):
                pass

    async def test_defect_3_adapter_subprocess_leak_on_failed_session_replacement(self) -> None:
        """
        Remediation verification of Defect 3:
        When a session enters SessionState.FAILED, the next get_or_create() call
        instantiates a new session and replaces the failed session.
        Before replacing, existing.close() is called, terminating the old adapter and subprocess.
        """
        adapters: List[StressMockAdapter] = []

        def factory(cwd: str) -> StressMockAdapter:
            a = StressMockAdapter(cwd=cwd)
            adapters.append(a)
            return a

        router = SessionRouter(adapter_factory=factory)
        s1, is_new1 = await router.get_or_create("C", "T")
        self.assertTrue(is_new1)
        self.assertEqual(s1.state, SessionState.IDLE)

        # Trigger failure on s1
        s1.state = SessionState.FAILED

        # New session replaces s1 in registry
        s2, is_new2 = await router.get_or_create("C", "T")
        self.assertTrue(is_new2)
        self.assertIsNot(s1, s2)

        # Defect 3 resolved: s1.adapter.stop() was called upon replacement!
        self.assertTrue(
            adapters[0].stopped,
            "Defect 3 resolved: s1.adapter was stopped when replaced in registry",
        )

    async def test_adapter_failure_releases_lock_and_sets_failed_state(self) -> None:
        """
        Adversarial test: Underlying adapter raises an unhandled exception during streaming.
        Verifies:
        - Exception is propagated to caller.
        - Session state transitions to FAILED.
        - handle.lock is released (not permanently held).
        """
        adapter = StressMockAdapter()
        adapter.fail_on_turn = True
        session = SessionHandle(
            session_id="C:T_FAIL",
            channel="C",
            thread_ts="T_FAIL",
            adapter=adapter,
        )
        session.state = SessionState.IDLE

        with self.assertRaises(RuntimeError) as ctx:
            async for _ in session.send_user_message("Explosive prompt"):
                pass

        self.assertIn("Simulated adapter failure", str(ctx.exception))
        self.assertEqual(session.state, SessionState.FAILED)
        self.assertFalse(session.lock.locked(), "handle.lock was left acquired after exception!")

    async def test_closed_session_blocks_send_user_message(self) -> None:
        """
        Verifies that attempting to send a message to a CLOSED session raises RuntimeError
        both prior to lock acquisition and inside the lock.
        """
        adapter = StressMockAdapter()
        session = SessionHandle(
            session_id="C:T_CLOSED",
            channel="C",
            thread_ts="T_CLOSED",
            adapter=adapter,
        )
        await session.close()
        self.assertEqual(session.state, SessionState.CLOSED)

        with self.assertRaises(RuntimeError) as ctx:
            async for _ in session.send_user_message("Should fail"):
                pass
        self.assertIn("CLOSED", str(ctx.exception))

    async def test_mixed_concurrency_race_conditions(self) -> None:
        """
        High-concurrency chaotic test:
        Concurrently run 150 tasks executing a mixture of:
        - get_or_create() on keys
        - send_user_message() on acquired sessions
        - teardown() on keys
        - reap_expired_sessions()
        Verifies no unhandled crashes, no deadlock, and system stability under chaos.
        """
        def adapter_factory(cwd: str) -> StressMockAdapter:
            return StressMockAdapter(cwd=cwd, start_delay=0.002)

        router = SessionRouter(inactivity_timeout=0.05, adapter_factory=adapter_factory)

        async def worker_get_or_create(channel: str, thread_ts: str):
            s, _ = await router.get_or_create(channel=channel, thread_ts=thread_ts)
            return s

        async def worker_send_msg(channel: str, thread_ts: str, msg: str):
            try:
                s, _ = await router.get_or_create(channel=channel, thread_ts=thread_ts)
                async for _ in s.send_user_message(msg):
                    pass
            except RuntimeError:
                pass  # Closed session during race is expected

        async def worker_teardown(channel: str, thread_ts: str):
            await router.teardown(channel=channel, thread_ts=thread_ts)

        async def worker_reaper():
            await router.reap_expired_sessions()

        tasks = []
        for i in range(40):
            c, t = f"C_{i % 5}", f"T_{i % 5}"
            tasks.append(asyncio.create_task(worker_get_or_create(c, t)))
            tasks.append(asyncio.create_task(worker_send_msg(c, t, f"Msg-{i}")))
            if i % 3 == 0:
                tasks.append(asyncio.create_task(worker_teardown(c, t)))
            if i % 5 == 0:
                tasks.append(asyncio.create_task(worker_reaper()))

        # Must all complete within 10.0 seconds with zero deadlocks
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=False), timeout=10.0)

        # Cleanup
        await router.stop_reaper()

    async def test_real_subprocess_lifecycle_and_reaper(self) -> None:
        """
        End-to-end integration test with real OS subprocess:
        - Spawn real subprocess running mock_agy_proc.py via HeadlessAgentAdapter
        - Route conversational turn through session.send_user_message()
        - Verify real subprocess PID exists and handles turn
        - Advance mock time and run reap_expired_sessions()
        - Verify real subprocess process is terminated and session CLOSED
        """
        mock_cli = os.path.abspath("tests/mock_agy_proc.py")

        def adapter_factory(cwd: str) -> HeadlessAgentAdapter:
            return HeadlessAgentAdapter(
                cwd=cwd,
                agy_bin=mock_cli,
            )

        router = SessionRouter(inactivity_timeout=14400.0, adapter_factory=adapter_factory)
        session, is_new = await router.get_or_create(channel="C_REAL", thread_ts="T_REAL")
        self.assertTrue(is_new)
        self.assertIsNotNone(session.adapter.process)
        proc = session.adapter.process
        self.assertIsNone(proc.returncode, "Subprocess exited prematurely")

        # Run a real turn through the live subprocess
        text_chunks = []
        async for ev in session.send_user_message("Hello Real Subprocess"):
            if ev.event_type == "text_delta":
                text_chunks.append(getattr(ev, "text_delta", ""))
            elif ev.event_type == "finish":
                self.assertEqual(ev.status, "SUCCESS")

        full_text = "".join(text_chunks)
        self.assertIn("Echo: Hello Real Subprocess", full_text)
        self.assertEqual(session.state, SessionState.IDLE)
        self.assertEqual(session.current_turn, 1)

        # Advance mock time by 14,401 seconds
        future_time = time.time() + 14401.0
        with patch("time.time", return_value=future_time):
            pruned = await router.reap_expired_sessions()

        self.assertEqual(pruned, ["C_REAL:T_REAL"])
        self.assertEqual(len(router.sessions), 0)
        self.assertEqual(session.state, SessionState.CLOSED)

        # Allow event loop a moment to reap the process
        await asyncio.sleep(0.05)
        self.assertIsNotNone(proc.returncode, "Subprocess was not terminated by reaper close!")


if __name__ == "__main__":
    unittest.main()
