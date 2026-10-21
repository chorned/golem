#!/usr/bin/env python3
"""
Regression tests for the Golem 02:32 AM router freeze & settlement storm:
  1. CPython 3.11 subprocess stdout pipe-pause deadlock in AntigravityDriver.terminate_session()
     when >256KB of unread stdout pauses _UnixReadPipeTransport and blocks proc.wait().
  2. SessionRouter._router_lock starvation when reap_expired_sessions() or teardown_session()
     awaits slow session/adapter teardown.
  3. RunnerExecutor event-bus subscriber leak and duplicate settlement storm across repeated
     SessionHandle.get_runner_status() and DualPaneHarness.execute_command() calls.
"""

from __future__ import annotations

import asyncio
import gc
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock

from golem.bus import ShoulderTapAlert, ShoulderTapEventBus
from golem.drivers.antigravity import AntigravityDriver
from golem.router import SessionRouter
from golem.runner import RunnerExecutor, TaskHandle
from golem.session import SessionHandle, SessionState


class TestReaperPipeDeadlockRegression(unittest.IsolatedAsyncioTestCase):
    async def test_terminate_session_does_not_hang_on_paused_stdout_pipe(self) -> None:
        """
        Reproduce the exact CPython 3.11 BaseSubprocessTransport deadlock:
        A child process ignores SIGTERM and writes >256KB to stdout while nobody reads
        proc.stdout, causing StreamReader to call transport.pause_reading() with 64KB
        stuck in the kernel pipe buffer. Verify terminate_session() completes cleanly
        and closes the pipe transports instead of hanging forever in proc.wait().
        """
        driver = AntigravityDriver(cwd="/tmp", agy_bin=sys.executable)
        child_code = (
            "import signal, sys, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "signal.signal(signal.SIGINT, signal.SIG_IGN)\n"
            "sys.stdout.write('x' * 524288)\n"
            "sys.stdout.flush()\n"
            "time.sleep(60)\n"
        )
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            child_code,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        driver.process = proc

        # Allow child to fill the 128KB StreamReader buffer + 64KB kernel pipe buffer
        # so StreamReader pauses the read pipe transport (_paused == True).
        await asyncio.sleep(0.25)
        stdout_transport = getattr(proc.stdout, "_transport", None)
        if stdout_transport is not None:
            self.assertTrue(
                getattr(stdout_transport, "_paused", False),
                "Expected stdout _UnixReadPipeTransport to be paused after >256KB unread output",
            )

        # Must complete within ~4s (2.0s stdin wait + 1.5s terminate wait + kill & pipe close)
        t0 = time.monotonic()
        await asyncio.wait_for(driver.terminate_session(), timeout=6.0)
        elapsed = time.monotonic() - t0

        self.assertIsNone(driver.process)
        self.assertLess(elapsed, 5.5)

    async def test_reap_expired_sessions_does_not_block_concurrent_get_or_create(self) -> None:
        """
        Verify SessionRouter.reap_expired_sessions() pops expired sessions under _router_lock
        and performs slow session.close() / harness.teardown() outside _router_lock so
        inbound messages calling get_or_create() are never blocked.
        """
        close_started = asyncio.Event()
        release_close = asyncio.Event()

        class SlowClosingAdapter:
            conversation_id = "conv-slow"

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                close_started.set()
                await release_close.wait()

        class FastAdapter:
            conversation_id = "conv-fast"

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                pass

        adapters = [SlowClosingAdapter(), FastAdapter()]
        router = SessionRouter(
            adapter_factory=lambda cwd, extra_args=None: adapters.pop(0),
            inactivity_timeout=10.0,
        )

        expired_session, _ = await router.get_or_create("C1", "t_expired")
        expired_session.last_active_ts = time.time() - 100.0  # mark expired

        reap_task = asyncio.create_task(router.reap_expired_sessions())
        await asyncio.wait_for(close_started.wait(), timeout=1.0)

        # While reap_expired_sessions() is blocked inside SlowClosingAdapter.stop(),
        # a new inbound user message calls get_or_create() and MUST succeed immediately.
        t0 = time.monotonic()
        active_session, is_new = await asyncio.wait_for(
            router.get_or_create("C1", "t_inbound"),
            timeout=0.5,
        )
        elapsed = time.monotonic() - t0

        self.assertTrue(is_new)
        self.assertEqual(active_session.session_id, "C1:t_inbound")
        self.assertLess(elapsed, 0.2)

        release_close.set()
        pruned = await asyncio.wait_for(reap_task, timeout=1.0)
        self.assertEqual(pruned, ["C1:t_expired"])

    async def test_runner_executor_no_bus_leak_or_duplicate_settlement_storm(self) -> None:
        """
        Verify:
        1. Ephemeral RunnerExecutor instances use weak references on ShoulderTapEventBus
           and do not leak subscribers after garbage collection.
        2. Idle RunnerExecutor instances do not settle completion alerts for panes they
           are not actively observing/handling.
        3. SessionHandle.get_runner_status() caches self.runner_executor and closes it
           when SessionHandle.close() runs.
        """
        bus = ShoulderTapEventBus()
        baseline_subs = len(bus._subscribers)

        # Create 10 ephemeral executors without keeping a local reference
        for _ in range(10):
            RunnerExecutor(sensory_bus=bus)
        gc.collect()
        self.assertEqual(
            len(bus._subscribers),
            baseline_subs,
            "Ephemeral RunnerExecutor instances must not leak onto ShoulderTapEventBus._subscribers",
        )

        # Create one idle executor and one active executor with a tracked handle on pane 'w1:p2'
        idle_executor = RunnerExecutor(sensory_bus=bus)
        active_executor = RunnerExecutor(sensory_bus=bus)
        active_executor._active_handles["w1:p2"] = TaskHandle(
            status="running",
            pane="w1:p2",
            opaque_coordinate="w1:p2",
            command="pytest",
        )

        idle_settle_mock = AsyncMock(wraps=idle_executor.settle_command)
        active_settle_mock = AsyncMock(wraps=active_executor.settle_command)
        idle_executor.settle_command = idle_settle_mock  # type: ignore[method-assign]
        active_executor.settle_command = active_settle_mock  # type: ignore[method-assign]

        alert = ShoulderTapAlert(
            alert_type="completion",
            source="eyes",
            session_id="s1",
            summary="Done",
            data={"pane": "w1:p2", "is_error": False},
        )
        bus.publish(alert)
        await asyncio.sleep(0.05)

        idle_settle_mock.assert_not_awaited()
        active_settle_mock.assert_awaited_once_with("w1:p2", returncode=0)

        # Publishing a duplicate completion alert after settlement must be a no-op
        bus.publish(alert)
        await asyncio.sleep(0.05)
        self.assertEqual(active_settle_mock.await_count, 1)

        idle_executor.close()
        active_executor.close()

        # Verify SessionHandle.get_runner_status() caches runner_executor and close() cleans it up
        mock_obs = MagicMock()
        mock_obs.read_buffer = AsyncMock(return_value="")
        mock_obs.extract_all_blockers = MagicMock(return_value=[])
        session = SessionHandle(
            session_id="C1:t1",
            channel="C1",
            thread_ts="t1",
            adapter=MagicMock(),
            runner_pane="w1:p2",
            buffer_observer=mock_obs,
            bus=bus,
        )
        session.state = SessionState.IDLE
        await session.get_runner_status()
        first_exec = session.runner_executor
        self.assertIsNotNone(first_exec)
        await session.get_runner_status()
        self.assertIs(session.runner_executor, first_exec)

        await session.close()
        self.assertIsNone(session.runner_executor)


if __name__ == "__main__":
    unittest.main()
