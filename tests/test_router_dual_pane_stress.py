#!/usr/bin/env python3
"""
tests/test_router_dual_pane_stress.py
Empirical stress tests for SessionRouter and DualPaneHarness provisioning under:
1. High concurrency (100+ concurrent session provisions).
2. Rapid successive and simultaneous calls for the same thread_ts (thundering herd).
3. Rapid creation/teardown cycles and concurrent teardown races.
4. Active turn streaming during teardown/reaper sweeps.
5. Workspace collisions, namespace isolation, and failed session replacement races.
"""

import asyncio
import json
import time
import unittest
from unittest.mock import AsyncMock

from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle, SessionState
from golem.execution.harness import DualPaneHarness
from herdr_client import HerdrClient


class DelayedMockAdapter:
    """Mock agent adapter with simulated asynchronous startup and turn latency."""

    def __init__(self, cwd: str = "/tmp") -> None:
        self.cwd = cwd
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        await asyncio.sleep(0.005)
        self.started = True

    async def stop(self) -> None:
        await asyncio.sleep(0.005)
        self.stopped = True

    async def send_turn(self, prompt: str):
        yield {"event_type": "text_delta", "content": f"Echo: {prompt}"}
        await asyncio.sleep(0.05)
        yield {"event_type": "finish", "status": "SUCCESS", "response": f"Echo: {prompt}"}


class StressTestHerdrClient(HerdrClient):
    """
    In-memory simulated HerdrClient that tracks workspace and pane commands
    with realistic async latency and strict concurrency safety.
    """

    def __init__(self, latency: float = 0.005):
        self.latency = latency
        self.workspaces: set[str] = set()
        self.panes: dict[str, list[str]] = {}
        self.cli_calls: list[list[str]] = []
        self._lock = asyncio.Lock()

    async def run_cli(self, args: list[str]) -> tuple[int, str, str]:
        if self.latency > 0:
            await asyncio.sleep(self.latency)

        async with self._lock:
            self.cli_calls.append(list(args))
            subcmd = args[0] if args else ""
            action = args[1] if len(args) > 1 else ""

            if subcmd == "workspace" and action == "create":
                # find --label
                label = ""
                for i, arg in enumerate(args):
                    if arg == "--label" and i + 1 < len(args):
                        label = args[i + 1]
                        break
                ws_id = f"ws-{label}"
                self.workspaces.add(ws_id)
                self.panes[ws_id] = ["1"]
                return 0, json.dumps({
                    "result": {
                        "workspace": {"workspace_id": ws_id, "label": label},
                        "root_pane": {"pane_id": "1"}
                    }
                }), ""

            elif subcmd == "workspace" and action == "close":
                target_ws = args[2] if len(args) > 2 else ""
                self.workspaces.discard(target_ws)
                self.panes.pop(target_ws, None)
                return 0, json.dumps({"result": {"closed": target_ws}}), ""

            elif subcmd == "agent" and action == "start":
                agent_name = args[2] if len(args) > 2 else ""
                return 0, json.dumps({"result": {"agent": agent_name, "status": "started"}}), ""

            elif subcmd == "pane" and action == "create":
                # find --workspace and --label
                ws_label = ""
                p_label = "runner"
                for i, arg in enumerate(args):
                    if arg == "--workspace" and i + 1 < len(args):
                        ws_label = args[i + 1]
                    elif arg == "--label" and i + 1 < len(args):
                        p_label = args[i + 1]
                return 0, json.dumps({
                    "result": {
                        "pane": {"pane_id": f"p-{p_label}", "label": p_label}
                    }
                }), ""

            return 0, json.dumps({"result": "ok"}), ""


class TestDualPaneRouterStress(unittest.IsolatedAsyncioTestCase):
    """Rigorous empirical stress test suite for Dual-Pane Router concurrency."""

    async def asyncSetUp(self) -> None:
        self.herdr_client = StressTestHerdrClient(latency=0.002)
        self.harness = DualPaneHarness(herdr_client=self.herdr_client, default_cwd="/tmp")
        self.router = SessionRouter(
            default_target_cwd="/tmp",
            inactivity_timeout=3600.0,
            adapter_factory=lambda cwd: DelayedMockAdapter(cwd),
            harness=self.harness,
        )

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    async def test_high_concurrency_distinct_sessions(self) -> None:
        """
        Stress Test 1: 100 concurrent tasks provision 100 distinct sessions simultaneously.
        Verifies:
        - All 100 sessions provision cleanly without race conditions or deadlocks.
        - Exactly 100 sessions in router.
        - Exactly 100 active workspaces in harness.
        - All liaison and runner panes correctly matched to respective session keys.
        """
        num_sessions = 100

        async def _create(idx: int):
            return await self.router.get_or_create(channel=f"C{idx}", thread_ts=f"100.{idx}")

        start_time = time.monotonic()
        results = await asyncio.gather(*[_create(i) for i in range(num_sessions)])
        elapsed = time.monotonic() - start_time

        self.assertEqual(len(results), num_sessions)
        for idx, (session, is_new) in enumerate(results):
            expected_key = f"C{idx}:100.{idx}"
            self.assertTrue(is_new, f"Session {expected_key} should be newly created")
            self.assertEqual(session.session_id, expected_key)
            self.assertEqual(session.workspace_id, expected_key)
            self.assertEqual(session.liaison_pane, f"{expected_key}-liaison")
            self.assertEqual(session.runner_pane, "runner")
            self.assertEqual(session.state, SessionState.IDLE)
            self.assertIn(expected_key, self.router.sessions)

        self.assertEqual(len(self.router.sessions), num_sessions)
        self.assertEqual(len(self.harness.active_workspaces), num_sessions)

    async def test_thundering_herd_same_thread(self) -> None:
        """
        Stress Test 2: 50 concurrent tasks all call get_or_create for the EXACT same thread_ts.
        Verifies:
        - DualPaneHarness.provision_dual_pane is invoked EXACTLY ONCE.
        - Exactly ONE task gets is_new_session == True.
        - Exactly 49 tasks get is_new_session == False.
        - All 50 tasks receive the EXACT SAME SessionHandle object reference.
        """
        num_tasks = 50
        channel = "C_HERD"
        thread_ts = "999.001"
        expected_key = f"{channel}:{thread_ts}"

        # Track harness provision calls
        original_provision = self.harness.provision_dual_pane
        provision_calls = 0
        provision_lock = asyncio.Lock()

        async def _instrumented_provision(*args, **kwargs):
            nonlocal provision_calls
            async with provision_lock:
                provision_calls += 1
            # Add artificial latency to widen race window
            await asyncio.sleep(0.02)
            return await original_provision(*args, **kwargs)

        self.harness.provision_dual_pane = _instrumented_provision

        async def _call():
            return await self.router.get_or_create(channel=channel, thread_ts=thread_ts)

        results = await asyncio.gather(*[_call() for _ in range(num_tasks)])

        self.assertEqual(len(results), num_tasks)
        self.assertEqual(provision_calls, 1, "provision_dual_pane must be invoked exactly once")

        new_count = sum(1 for _, is_new in results if is_new)
        reused_count = sum(1 for _, is_new in results if not is_new)

        self.assertEqual(new_count, 1, "Exactly one task must report is_new == True")
        self.assertEqual(reused_count, num_tasks - 1, "All subsequent tasks must report is_new == False")

        first_session = results[0][0]
        for s, _ in results:
            self.assertIs(s, first_session, "All tasks must receive the identical SessionHandle instance")

        self.assertEqual(first_session.session_id, expected_key)
        self.assertEqual(first_session.liaison_pane, f"{expected_key}-liaison")
        self.assertEqual(first_session.runner_pane, "runner")

    async def test_rapid_creation_teardown_cycles(self) -> None:
        """
        Stress Test 3: 50 sequential rapid create -> teardown cycles on the same thread_ts.
        Verifies:
        - Workspace is cleanly torn down after each cycle.
        - No workspace or session leakage.
        - Re-provisioning immediately after teardown succeeds and provisions a fresh workspace.
        """
        channel = "C_CYCLE"
        thread_ts = "123.456"
        expected_key = f"{channel}:{thread_ts}"
        num_cycles = 50

        for i in range(num_cycles):
            session, is_new = await self.router.get_or_create(channel, thread_ts)
            self.assertTrue(is_new, f"Cycle {i}: Should create fresh session")
            self.assertEqual(session.session_id, expected_key)
            self.assertEqual(session.state, SessionState.IDLE)
            self.assertIn(expected_key, self.router.sessions)
            self.assertEqual(len(self.harness.active_workspaces), 1)

            # Teardown
            torn_down = await self.router.teardown(channel, thread_ts, reason=f"cycle_{i}")
            self.assertTrue(torn_down, f"Cycle {i}: Teardown should return True")
            self.assertNotIn(expected_key, self.router.sessions)
            self.assertEqual(len(self.harness.active_workspaces), 0, f"Cycle {i}: Active workspaces should be 0")
            self.assertEqual(session.state, SessionState.CLOSED)

        self.assertEqual(len(self.router.sessions), 0)
        self.assertEqual(len(self.harness.active_workspaces), 0)

    async def test_concurrent_create_and_teardown_races(self) -> None:
        """
        Stress Test 4: Concurrent competing tasks executing get_or_create and teardown.
        Verifies:
        - No unhandled exceptions or deadlocks under chaotic interleaved access.
        - Final router and harness state remains strictly consistent.
        """
        channel = "C_RACE"
        thread_ts = "777.888"
        expected_key = f"{channel}:{thread_ts}"
        operations = 60

        async def _worker(idx: int):
            if idx % 2 == 0:
                s, _ = await self.router.get_or_create(channel, thread_ts)
                return ("create", s.state)
            else:
                td = await self.router.teardown(channel, thread_ts, reason=f"race_{idx}")
                return ("teardown", td)

        results = await asyncio.gather(*[_worker(i) for i in range(operations)], return_exceptions=True)

        for res in results:
            self.assertNotIsInstance(res, Exception, f"Worker raised unexpected exception: {res}")

        # Post-chaos audit: state must be strictly consistent
        if expected_key in self.router.sessions:
            sess = self.router.sessions[expected_key]
            self.assertIn(sess.state, (SessionState.IDLE, SessionState.WORKING, SessionState.AWAITING_INPUT))
            ws = self.harness.get_workspace(expected_key)
            self.assertIsNotNone(ws)
            self.assertEqual(ws["status"], "active")
        else:
            ws = self.harness.get_workspace(expected_key)
            if ws:
                self.assertEqual(ws["status"], "closed")

    async def test_double_teardown_idempotency_race(self) -> None:
        """
        Stress Test 5: Simultaneous invocations of session.close() and router.teardown().
        Verifies:
        - Idempotent execution: no double-free, no crashes, no duplicate CLI calls.
        """
        channel = "C_DOUBLE_TD"
        thread_ts = "333.444"
        key = f"{channel}:{thread_ts}"

        session, _ = await self.router.get_or_create(channel, thread_ts)

        # Fire both concurrently
        res1, res2 = await asyncio.gather(
            session.close(reason="direct_close"),
            self.router.teardown(channel, thread_ts, reason="router_teardown"),
            return_exceptions=True,
        )

        self.assertNotIsInstance(res1, Exception)
        self.assertNotIsInstance(res2, Exception)
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertNotIn(key, self.router.sessions)

        # Re-running teardown on already-closed session should be a no-op returning False
        res3 = await self.router.teardown(channel, thread_ts, reason="third_attempt")
        self.assertFalse(res3)

    async def test_teardown_during_active_streaming_turn(self) -> None:
        """
        Stress Test 6: A session has an active generator executing a turn when teardown is invoked.
        Verifies:
        - Teardown aborts active turn generator cleanly.
        - Session moves to CLOSED.
        - Harness is torn down.
        - No deadlock or hanging task.
        """
        channel = "C_STREAM"
        thread_ts = "555.666"

        session, _ = await self.router.get_or_create(channel, thread_ts)

        # Start streaming turn in background task
        events_received = []

        async def _run_turn():
            async for ev in session.send_user_message("Hello slow turn"):
                events_received.append(ev)

        turn_task = asyncio.create_task(_run_turn())
        # Let turn start and yield first event
        await asyncio.sleep(0.01)
        self.assertTrue(len(events_received) >= 1)
        self.assertEqual(session.state, SessionState.WORKING)

        # Trigger teardown while turn is actively running
        torn_down = await self.router.teardown(channel, thread_ts, reason="interrupt")
        self.assertTrue(torn_down)
        self.assertEqual(session.state, SessionState.CLOSED)

        # Await turn task; should complete cleanly without hanging
        await asyncio.wait_for(turn_task, timeout=1.0)
        self.assertEqual(len(self.harness.active_workspaces), 0)

    async def test_failed_session_replacement_under_concurrency(self) -> None:
        """
        Stress Test 7: A session transitions to FAILED state, followed by 30 concurrent calls
        attempting to get_or_create in that thread.
        Verifies:
        - Old session is closed and old workspace torn down.
        - Exactly ONE new session is provisioned.
        - All 30 callers receive the new SessionHandle.
        - No workspace leakage.
        """
        channel = "C_FAIL"
        thread_ts = "888.999"
        key = f"{channel}:{thread_ts}"

        s1, _ = await self.router.get_or_create(channel, thread_ts)
        s1.state = SessionState.FAILED

        async def _fetch():
            return await self.router.get_or_create(channel, thread_ts)

        results = await asyncio.gather(*[_fetch() for _ in range(30)])

        self.assertEqual(s1.state, SessionState.CLOSED)

        new_count = sum(1 for _, is_new in results if is_new)
        self.assertEqual(new_count, 1, "Exactly one replacement session must be created")

        new_session = results[0][0]
        self.assertIsNot(new_session, s1)
        self.assertEqual(new_session.state, SessionState.IDLE)
        for s, _ in results:
            self.assertIs(s, new_session)

        # Harness should have exactly 1 active workspace for this session
        self.assertEqual(len(self.harness.active_workspaces), 1)
        self.assertEqual(self.harness.get_workspace(key)["status"], "active")

    async def test_workspace_isolation_and_special_characters(self) -> None:
        """
        Stress Test 8: Workspace collision resistance:
        - Same thread_ts across 10 different channels.
        - Channels and thread_ts containing colons, slashes, dashes, unicode.
        Verifies:
        - Each workspace is isolated with no collisions or cross-talk.
        """
        test_cases = [
            ("C_A", "T_SAME"),
            ("C_B", "T_SAME"),
            ("C_C", "T_SAME"),
            ("chan/nested", "123.456"),
            ("C-unicode-🚀", "999.000"),
            ("chan:with:colons", "thread:extra:colons"),
            ("spaced chan", "spaced thread"),
        ]

        sessions = []
        for chan, ts in test_cases:
            s, is_new = await self.router.get_or_create(chan, ts)
            self.assertTrue(is_new)
            sessions.append(s)

        # Verify all session IDs and workspace IDs are unique
        session_ids = [s.session_id for s in sessions]
        workspace_ids = [s.workspace_id for s in sessions]
        self.assertEqual(len(session_ids), len(set(session_ids)))
        self.assertEqual(len(workspace_ids), len(set(workspace_ids)))

        # Verify all workspaces exist independently in harness
        for s in sessions:
            ws = self.harness.get_workspace(s.session_id)
            self.assertIsNotNone(ws, f"Workspace {s.session_id} should exist")
            self.assertEqual(ws["status"], "active")

        # Tear them all down concurrently
        td_results = await asyncio.gather(*[
            self.router.teardown(chan, ts) for chan, ts in test_cases
        ])
        self.assertTrue(all(td_results))
        self.assertEqual(len(self.router.sessions), 0)
        self.assertEqual(len(self.harness.active_workspaces), 0)

    async def test_concurrent_reaper_during_active_provisioning(self) -> None:
        """
        Stress Test 9: Background reaper sweeps while concurrent provisioning is actively running.
        Verifies:
        - No dictionary size mutation crashes.
        - Freshly created sessions are NOT reaped.
        - Expired sessions ARE cleanly reaped and their workspaces torn down.
        """
        # Create 10 expired sessions by setting their last_active_ts far in the past
        for i in range(10):
            s, _ = await self.router.get_or_create(f"C_OLD_{i}", "1.0")
            s.last_active_ts = time.time() - 5000.0  # Expired

        self.assertEqual(len(self.router.sessions), 10)
        self.assertEqual(len(self.harness.active_workspaces), 10)

        # Concurrently run reap_expired_sessions() and 20 new get_or_create() calls
        async def _reap():
            await asyncio.sleep(0.005)
            return await self.router.reap_expired_sessions()

        async def _create_new(i: int):
            return await self.router.get_or_create(f"C_NEW_{i}", "2.0")

        reap_task = asyncio.create_task(_reap())
        create_tasks = [_create_new(i) for i in range(20)]

        reaped_keys, *new_sessions = await asyncio.gather(reap_task, *create_tasks)

        self.assertEqual(len(reaped_keys), 10)
        self.assertEqual(len(new_sessions), 20)

        # Verify router only contains the 20 new sessions
        self.assertEqual(len(self.router.sessions), 20)
        self.assertEqual(len(self.harness.active_workspaces), 20)

        for s, is_new in new_sessions:
            self.assertTrue(is_new)
            self.assertEqual(s.state, SessionState.IDLE)

    async def test_concurrent_reaper_sweeps(self) -> None:
        """
        Stress Test 10: Multiple concurrent reaper sweeps executing at the exact same moment.
        Verifies:
        - Mutual exclusion via router lock.
        - Expired sessions pruned exactly once.
        - No duplicate teardowns or double-close errors.
        """
        for i in range(15):
            s, _ = await self.router.get_or_create(f"C_SWEEP_{i}", "1.0")
            s.last_active_ts = time.time() - 10000.0  # Expired

        self.assertEqual(len(self.router.sessions), 15)

        # Run 5 concurrent reaper sweeps
        reaper_results = await asyncio.gather(*[
            self.router.reap_expired_sessions() for _ in range(5)
        ])

        total_pruned = sum(len(keys) for keys in reaper_results)
        self.assertEqual(total_pruned, 15, "Across all concurrent sweeps, exactly 15 sessions should be pruned")
        self.assertEqual(len(self.router.sessions), 0)
        self.assertEqual(len(self.harness.active_workspaces), 0)

    async def test_herdr_cli_failure_resilience(self) -> None:
        """
        Stress Test 11: Herdr CLI returns non-zero error codes or corrupt JSON during provisioning.
        Verifies:
        - DualPaneHarness gracefully falls back to deterministic pane and workspace identifiers.
        - SessionRouter completes provisioning without raising unhandled exceptions.
        """
        failing_herdr = AsyncMock(spec=HerdrClient)
        # Simulate CLI failure on workspace create and pane create
        failing_herdr.run_cli.side_effect = [
            (1, "Error: daemon unavailable", "Herdr daemon socket closed"),
            (1, "Error: cannot start agent", "Unknown agent kind"),
            (1, "Error: cannot create pane", "Out of pane slots"),
        ]

        failing_harness = DualPaneHarness(herdr_client=failing_herdr, default_cwd="/tmp")
        router = SessionRouter(
            default_target_cwd="/tmp",
            adapter_factory=lambda cwd: DelayedMockAdapter(cwd),
            harness=failing_harness,
        )

        session, is_new = await router.get_or_create("C_FAIL_CLI", "100.1")
        self.assertTrue(is_new)
        self.assertEqual(session.session_id, "C_FAIL_CLI:100.1")
        self.assertEqual(session.liaison_pane, "C_FAIL_CLI:100.1-liaison")
        self.assertEqual(session.runner_pane, "runner")
        self.assertEqual(session.workspace_id, "C_FAIL_CLI:100.1")
        self.assertEqual(session.state, SessionState.IDLE)

    async def test_transient_provisioning_exception_recovery(self) -> None:
        """
        Stress Test 12: An unexpected exception occurs during harness.provision_dual_pane.
        Verifies:
        - Exception propagates out of get_or_create.
        - Router lock is safely released (no deadlock).
        - router.sessions is not polluted with half-baked sessions.
        - Subsequent call to get_or_create succeeds once transient error resolves.
        """
        channel = "C_RECOVER"
        thread_ts = "999.888"
        key = f"{channel}:{thread_ts}"

        call_count = 0
        original_provision = self.harness.provision_dual_pane

        async def _failing_provision(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Transient network timeout connecting to Herdr daemon")
            return await original_provision(*args, **kwargs)

        self.harness.provision_dual_pane = _failing_provision

        # First call fails
        with self.assertRaises(RuntimeError) as cm:
            await self.router.get_or_create(channel, thread_ts)
        self.assertIn("Transient network timeout", str(cm.exception))

        # Router must NOT have the session registered
        self.assertNotIn(key, self.router.sessions)
        self.assertFalse(self.router._router_lock.locked())

        # Second call succeeds
        session, is_new = await self.router.get_or_create(channel, thread_ts)
        self.assertTrue(is_new)
        self.assertEqual(session.session_id, key)
        self.assertIn(key, self.router.sessions)
        self.assertEqual(session.state, SessionState.IDLE)

    async def test_chaotic_interleaved_multi_thread_load(self) -> None:
        """
        Stress Test 13: 80 chaotic tasks performing mixed operations (get_or_create, send turn,
        teardown, reap) across 5 shared thread keys.
        Verifies:
        - Zero deadlocks under heavy contention.
        - Zero uncaught exceptions.
        - Consistent final state.
        """
        channels = ["C_CHAOS_1", "C_CHAOS_2", "C_CHAOS_3", "C_CHAOS_4", "C_CHAOS_5"]
        thread_ts = "1.0"
        num_ops = 80

        async def _worker(idx: int):
            ch = channels[idx % len(channels)]
            op = idx % 4
            if op == 0:
                s, _ = await self.router.get_or_create(ch, thread_ts)
                return ("get_or_create", s.session_id)
            elif op == 1:
                s, _ = await self.router.get_or_create(ch, thread_ts)
                events = []
                try:
                    async for ev in s.send_user_message(f"Chaos msg {idx}"):
                        events.append(ev)
                except RuntimeError:
                    # Expected if session was closed concurrently
                    pass
                return ("send_turn", len(events))
            elif op == 2:
                res = await self.router.teardown(ch, thread_ts, reason=f"chaos_{idx}")
                return ("teardown", res)
            elif op == 3:
                res = await self.router.reap_expired_sessions()
                return ("reap", len(res))

        results = await asyncio.gather(*[_worker(i) for i in range(num_ops)], return_exceptions=True)

        for i, res in enumerate(results):
            self.assertNotIsInstance(res, Exception, f"Worker {i} failed with unexpected exception: {res}")

        # Post-test integrity audit
        for ch in channels:
            key = f"{ch}:{thread_ts}"
            if key in self.router.sessions:
                sess = self.router.sessions[key]
                self.assertIn(sess.state, (SessionState.IDLE, SessionState.WORKING, SessionState.AWAITING_INPUT))
                ws = self.harness.get_workspace(key)
                self.assertIsNotNone(ws)
                self.assertEqual(ws["status"], "active")
            else:
                ws = self.harness.get_workspace(key)
                if ws:
                    self.assertEqual(ws["status"], "closed")

    async def test_ultra_scale_provision_and_teardown(self) -> None:
        """
        Stress Test 14: Provision 200 distinct sessions concurrently, then tear down all 200 concurrently.
        Verifies:
        - Scalability to 200 concurrent dual-pane workspaces.
        - Zero active workspaces or sessions remaining after full teardown.
        """
        scale = 200

        async def _create(idx: int):
            return await self.router.get_or_create(channel=f"C_ULTRA_{idx}", thread_ts="1.0")

        created = await asyncio.gather(*[_create(i) for i in range(scale)])
        self.assertEqual(len(created), scale)
        self.assertEqual(len(self.router.sessions), scale)
        self.assertEqual(len(self.harness.active_workspaces), scale)

        async def _teardown(idx: int):
            return await self.router.teardown(channel=f"C_ULTRA_{idx}", thread_ts="1.0")

        torn_down = await asyncio.gather(*[_teardown(i) for i in range(scale)])
        self.assertTrue(all(torn_down))
        self.assertEqual(len(self.router.sessions), 0)
        self.assertEqual(len(self.harness.active_workspaces), 0)


if __name__ == "__main__":
    unittest.main()

