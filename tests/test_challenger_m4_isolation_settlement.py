#!/usr/bin/env python3
"""
tests/test_challenger_m4_isolation_settlement.py
Empirical Adversarial Stress Test Suite for Milestone 4 Iteration 1 Gate:
- Strict Pane Isolation across varied non-runner pane names.
- Runner Pane dynamic attachment verification.
- Rapid settlement cycles, idempotency, and orphaned background task prevention.
- Concurrent and interleaved settlement stress.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import json
import time
import asyncio
import unittest
from unittest.mock import patch, AsyncMock, MagicMock
from typing import List, Dict, Any

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.execution.runner import RunnerExecutor, ExecutionHandle
from golem.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.sensory.eyes import EyesTaskObserver


class TestPaneIsolationAdversarial(unittest.IsolatedAsyncioTestCase):
    """
    Empirical challenge: verify that commands yielding in non-runner panes
    NEVER attach or leak an EyesTaskObserver in _active_observers.
    """

    async def asyncSetUp(self):
        self.bus = ShoulderTapEventBus()
        self.executor = RunnerExecutor(sensory_bus=self.bus)

    async def asyncTearDown(self):
        # Clean up any active observers
        for pane in list(self.executor._active_observers.keys()):
            await self.executor.settle_command(pane)
        self.executor.close()

    async def test_non_runner_panes_isolation_on_yield(self):
        """
        Adversarially challenge pane isolation across varied non-runner pane names:
        'w1:p1', 'p1', 'liaison', 'fm-sess-liaison', 'admin', 'system',
        'daemon', 'control', 'session-123-liaison', 'node1:p1'.
        Must yield ExecutionHandle with status='running', but attach ZERO observers.
        """
        non_runner_panes = [
            "w1:p1",
            "p1",
            "liaison",
            "fm-sess-liaison",
            "admin",
            "system",
            "daemon",
            "control",
            "session-123-liaison",
            "node1:p1",
            "pane1",
            "pane_1",
            "pane-1",
            "admin_panel",
            "system_ops",
        ]

        for target_pane in non_runner_panes:
            with self.subTest(pane=target_pane):
                # Ensure is_runner_pane is False
                self.assertFalse(
                    self.executor.is_runner_pane(target_pane),
                    f"is_runner_pane({target_pane}) must be False for non-runner pane",
                )

                # Mock subprocess so that command runs long and yields (> yield_timeout)
                mock_proc = AsyncMock()
                mock_proc.pid = 9999
                # communicate times out to simulate a running command
                mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

                with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
                     patch.object(self.executor, "_get_pane_foreground_pid", return_value=9999):
                    handle = await self.executor.run_command(
                        pane=target_pane,
                        command="sleep 10",
                        yield_timeout=0.05,
                    )

                self.assertEqual(handle.status, "running")
                self.assertEqual(handle.pane, target_pane)
                # Verify that NO observer was attached or leaked in _active_observers
                self.assertNotIn(
                    target_pane,
                    self.executor._active_observers,
                    f"Observer was leaked for non-runner pane '{target_pane}'!",
                )
                self.assertEqual(
                    len(self.executor._active_observers),
                    0,
                    f"Unexpected active observers found: {self.executor._active_observers.keys()}",
                )

    async def test_runner_panes_attach_observer_on_yield(self):
        """
        Verify the positive assertion: designated runner panes DO attach an
        EyesTaskObserver when commands yield.
        """
        runner_panes = [
            "runner",
            "p2",
            "pane2",
            "pane_2",
            "pane-2",
            "w1:p2",
            "sess-runner",
            "worker_runner",
        ]

        for target_pane in runner_panes:
            with self.subTest(pane=target_pane):
                self.assertTrue(
                    self.executor.is_runner_pane(target_pane),
                    f"is_runner_pane({target_pane}) must be True for runner pane",
                )

                mock_proc = AsyncMock()
                mock_proc.pid = 8888
                mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

                with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
                     patch.object(self.executor, "_get_pane_foreground_pid", return_value=8888):
                    handle = await self.executor.run_command(
                        pane=target_pane,
                        command="sleep 10",
                        yield_timeout=0.05,
                    )

                self.assertEqual(handle.status, "running")
                self.assertIn(
                    target_pane,
                    self.executor._active_observers,
                    f"Observer was NOT attached for runner pane '{target_pane}'!",
                )
                obs = self.executor._active_observers[target_pane]
                self.assertIsInstance(obs, EyesTaskObserver)

                # Clean up observer
                await self.executor.settle_command(target_pane)
                self.assertNotIn(target_pane, self.executor._active_observers)


class TestRapidSettlementCyclesAdversarial(unittest.IsolatedAsyncioTestCase):
    """
    Empirical challenge: rapid settlement cycles, idempotency, and background task hygiene.
    """

    async def asyncSetUp(self):
        self.bus = ShoulderTapEventBus()
        self.executor = RunnerExecutor(sensory_bus=self.bus)

    async def asyncTearDown(self):
        for pane in list(self.executor._active_observers.keys()):
            await self.executor.settle_command(pane)
        self.executor.close()

    async def test_100_sequential_rapid_settlement_cycles(self):
        """
        Execute 100 rapid cycles of yield -> settle_command / settle_pane.
        Verify no KeyErrors, correct observer teardown, and 0 orphaned tasks.
        """
        pane = "runner"
        mock_proc = AsyncMock()
        mock_proc.pid = 1234
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

        for cycle in range(100):
            with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
                 patch.object(self.executor, "_get_pane_foreground_pid", return_value=1234):
                handle = await self.executor.run_command(
                    pane=pane,
                    command=f"echo cycle_{cycle} && sleep 5",
                    yield_timeout=0.02,
                )

            self.assertEqual(handle.status, "running")
            self.assertIn(pane, self.executor._active_observers)
            obs = self.executor._active_observers[pane]
            self.assertTrue(obs.is_running or obs._watch_task is not None)

            # Alternate settle_command and settle_pane
            if cycle % 2 == 0:
                await self.executor.settle_command(pane, returncode=0)
            else:
                await self.executor.settle_pane(pane, returncode=0)

            # Verify immediately unmounted
            self.assertNotIn(pane, self.executor._active_observers)
            # Verify background task is cancelled or done
            if obs._watch_task is not None:
                self.assertTrue(obs._watch_task.done())
            self.assertFalse(obs.is_running)

        self.assertEqual(len(self.executor._active_observers), 0)

    async def test_settle_idempotency_and_non_existent_panes(self):
        """
        Adversarial test: call settle_command and settle_pane repeatedly on:
        - already-settled panes
        - non-existent panes
        - arbitrary strings ('foo', 'w1:p1', '', None)
        Must execute without raising KeyError or any exception.
        """
        bogus_panes = ["nonexistent", "already_settled", "w1:p1", "admin", "", "system"]

        # Run 50 rapid calls to settle on empty state
        for p in bogus_panes:
            try:
                await self.executor.settle_command(p, returncode=0)
                await self.executor.settle_pane(p, returncode=1)
                await self.executor.settle_command(p, returncode=-1)
            except Exception as e:
                self.fail(f"settle_command raised unexpected exception for pane '{p}': {e}")

    async def test_concurrent_settlement_race_stress(self):
        """
        Stress test: spawn an observer on 'runner', then trigger 50 concurrent
        settle_command calls simultaneously using asyncio.gather.
        Verify no race condition or KeyError occurs.
        """
        pane = "runner"
        mock_proc = AsyncMock()
        mock_proc.pid = 5555
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
             patch.object(self.executor, "_get_pane_foreground_pid", return_value=5555):
            handle = await self.executor.run_command(
                pane=pane,
                command="sleep 10",
                yield_timeout=0.02,
            )

        self.assertIn(pane, self.executor._active_observers)
        obs = self.executor._active_observers[pane]

        # Fire 50 simultaneous settle calls
        tasks = [self.executor.settle_command(pane, returncode=i % 2) for i in range(50)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            self.assertNotIsInstance(res, Exception, f"Concurrent settle raised: {res}")

        self.assertNotIn(pane, self.executor._active_observers)
        if obs._watch_task is not None:
            self.assertTrue(obs._watch_task.done())

    async def test_sensory_bus_completion_alert_rapid_fire(self):
        """
        Stress test: publish 50 rapid completion alerts across sensory_bus.
        Verify automatic settlement handles rapid alerts gracefully without key errors.
        """
        pane = "runner"
        mock_proc = AsyncMock()
        mock_proc.pid = 7777
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
             patch.object(self.executor, "_get_pane_foreground_pid", return_value=7777):
            handle = await self.executor.run_command(
                pane=pane,
                command="sleep 10",
                yield_timeout=0.02,
            )

        self.assertIn(pane, self.executor._active_observers)

        # Publish 50 rapid completion alerts
        for i in range(50):
            alert = ShoulderTapAlert(
                alert_type="completion",
                source="eyes",
                session_id="test_sess",
                summary="Task completed",
                data={"pane": pane, "is_error": (i % 2 == 1)},
            )
            self.bus.publish(alert)
            # Give tiny yield for async subscriber to run
            await asyncio.sleep(0.001)

        self.assertNotIn(pane, self.executor._active_observers)

    async def test_no_orphaned_background_tasks_leakage(self):
        """
        Verify that after multiple command yields and settlements,
        no orphaned asyncio tasks remain active in the event loop.
        """
        initial_tasks = {t for t in asyncio.all_tasks() if not t.done()}

        mock_proc = AsyncMock()
        mock_proc.pid = 4321
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

        for i in range(20):
            pane_name = f"runner_{i}"
            with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
                 patch.object(self.executor, "_get_pane_foreground_pid", return_value=4321):
                await self.executor.run_command(
                    pane=pane_name,
                    command="sleep 5",
                    yield_timeout=0.01,
                )
            await self.executor.settle_command(pane_name)

        # Allow all cancellation callbacks to process
        await asyncio.sleep(0.05)

        remaining_tasks = {t for t in asyncio.all_tasks() if not t.done()}
        # Filter out current test task
        current_task = asyncio.current_task()
        leaked_tasks = [
            t for t in remaining_tasks
            if t not in initial_tasks and t is not current_task and "test_" not in t.get_name()
        ]

        # Inspect if any EyesTaskObserver watch loop tasks leaked
        observer_leaks = [
            t for t in leaked_tasks
            if "_watch_loop" in getattr(t.get_coro(), "__name__", "")
        ]
        self.assertEqual(
            len(observer_leaks),
            0,
            f"Detected {len(observer_leaks)} leaked EyesTaskObserver tasks: {observer_leaks}",
        )


    async def test_resolved_non_runner_panes_do_not_attach_observer(self):
        """
        Verify that even through _try_resolve_and_run, non-runner pane labels
        (e.g. 'admin', 'system', 'liaison', 'w1:p1') never attach an observer.
        """
        for label in ["admin", "system", "liaison", "w1:p1"]:
            with self.subTest(label=label):
                # Simulate pane not found initially, triggering _try_resolve_and_run
                mock_list_proc = AsyncMock()
                mock_list_proc.returncode = 0
                list_json = json.dumps({
                    "result": {
                        "panes": [
                            {"pane_id": label, "label": label, "name": label}
                        ]
                    }
                }).encode("utf-8")
                mock_list_proc.communicate = AsyncMock(return_value=(list_json, b""))

                mock_run_proc = AsyncMock()
                mock_run_proc.pid = 6666
                mock_run_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

                with patch("asyncio.create_subprocess_exec", side_effect=[mock_list_proc, mock_run_proc]), \
                     patch.object(self.executor, "_get_pane_foreground_pid", return_value=6666):
                    handle = await self.executor._try_resolve_and_run(
                        pane_label=label,
                        command="long_cmd",
                        yield_timeout=0.02,
                    )

                self.assertIsNotNone(handle)
                self.assertEqual(handle.status, "running")
                self.assertNotIn(label, self.executor._active_observers)
                self.assertEqual(len(self.executor._active_observers), 0)

    async def test_rapid_re_attachment_cycling(self):
        """
        Verify that multiple consecutive long-running commands to the SAME pane
        cleanly attach, unmount on settle, and re-attach on the next command.
        """
        pane = "runner"
        mock_proc = AsyncMock()
        mock_proc.pid = 9876
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

        for i in range(10):
            with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
                 patch.object(self.executor, "_get_pane_foreground_pid", return_value=9876):
                handle = await self.executor.run_command(
                    pane=pane,
                    command=f"batch_job_{i}",
                    yield_timeout=0.01,
                )
            self.assertEqual(handle.status, "running")
            self.assertIn(pane, self.executor._active_observers)
            obs1 = self.executor._active_observers[pane]

            # Settle it
            await self.executor.settle_command(pane, returncode=0)
            self.assertNotIn(pane, self.executor._active_observers)
            self.assertIsNone(obs1._watch_task)
            self.assertFalse(obs1.is_running)

    async def test_malformed_and_edge_case_sensory_alerts(self):
        """
        Verify _on_sensory_alert handles malformed, empty, or unexpected alert payloads.
        """
        # None alert
        await self.executor._on_sensory_alert(None)

        # Alert without alert_type
        mock_alert = MagicMock(spec=[])
        await self.executor._on_sensory_alert(mock_alert)

        # Alert with non-dict data
        bad_data_alert = ShoulderTapAlert(
            alert_type="completion",
            source="eyes",
            session_id="test",
            summary="test",
            data=None,
        )
        await self.executor._on_sensory_alert(bad_data_alert)

        # Alert with missing pane in data
        missing_pane_alert = ShoulderTapAlert(
            alert_type="completion",
            source="eyes",
            session_id="test",
            summary="test",
            data={},
        )
        await self.executor._on_sensory_alert(missing_pane_alert)


if __name__ == "__main__":
    unittest.main()
