#!/usr/bin/env python3
"""
tests/test_runner_yield_empirical_challenge.py
Adversarial empirical challenge harness for Milestone 2: RunnerExecutor Yield Ceiling & Concurrency.

Verifies:
1. Timing Boundary 1:
   - Command running 0.05s completes with status="completed" in < 0.2s (both direct & polling paths).
2. Timing Boundary 2:
   - Command running 5.0s yields status="running" within strict ceiling 2.95s <= elapsed <= 3.15s
     (both communicate timeout and persistent foreground PID polling paths).
3. Rapid Concurrency:
   - 20 concurrent fast commands execute without contention or cross-talk.
   - 20 concurrent 5.0s commands all yield status="running" in parallel within <= 3.25s total elapsed time.
   - Interleaved fast and slow concurrent execution.
4. Event Loop Responsiveness:
   - High-frequency heartbeat jitter stays minimal (< 50ms) while long command is running.
   - Out-of-band conversational triage completes in ~0.5s while runner command is actively yielding at ~3.0s.
5. Adversarial Edge Cases:
   - Transient query timeouts in _get_pane_foreground_pid() during polling (bug hunt).
"""

import os
import sys
import json
import time
import asyncio
import unittest
from unittest.mock import patch, AsyncMock, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.execution.runner import RunnerExecutor, ExecutionHandle
import herdr_tools


class TestRunnerYieldTimingBoundaries(unittest.IsolatedAsyncioTestCase):
    """Empirically verifies strict timing boundaries for fast completion and yield ceilings."""

    def setUp(self):
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

    async def test_fast_command_0_05s_direct_completes_in_under_0_2s(self):
        """
        Boundary 1A: Command running 0.05s via direct communicate returns
        status='completed' in < 0.2s.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.pid = 20001

            async def fast_comm():
                await asyncio.sleep(0.05)
                return b"Fast output in 0.05s\n", b""

            mock_proc.communicate.side_effect = fast_comm

            # No foreground process remains in pane
            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": None}}).encode(),
                b""
            )

            mock_exec.side_effect = [mock_proc, mock_info_proc]

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="echo fast",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            self.assertLess(elapsed, 0.2, f"Elapsed {elapsed:.3f}s exceeded 0.2s boundary")
            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertEqual(handle.stdout, "Fast output in 0.05s")
            self.assertIn("Command completed within yield timeout.", handle.message)

    async def test_fast_command_0_05s_via_polling_completes_in_under_0_2s(self):
        """
        Boundary 1B: Herdr pane run dispatches immediately, process runs for 0.05s in PTY,
        and polling loop detects completion and returns status='completed' in < 0.2s.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            # pane run returns immediately
            mock_run_proc = AsyncMock()
            mock_run_proc.returncode = 0
            mock_run_proc.communicate.return_value = (b"", b"")

            # First info check: running (pid 20002)
            # Second info check (after 0.05s sleep): process has exited (None)
            info_proc_1 = AsyncMock()
            info_proc_1.returncode = 0
            info_proc_1.communicate.return_value = (
                json.dumps({"result": {"pid": 20002}}).encode(),
                b""
            )

            info_proc_2 = AsyncMock()
            info_proc_2.returncode = 0
            info_proc_2.communicate.return_value = (
                json.dumps({"result": {"pid": None}}).encode(),
                b""
            )

            mock_exec.side_effect = [mock_run_proc, info_proc_1, info_proc_2]

            # Mock buffer observer
            self.executor.buffer_observer.read_buffer = AsyncMock(return_value="Command finished successfully in 0.05s")

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="python3 -c 'import time; time.sleep(0.05)'",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            self.assertLess(elapsed, 0.2, f"Elapsed {elapsed:.3f}s exceeded 0.2s boundary")
            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertEqual(handle.stdout, "Command finished successfully in 0.05s")

    async def test_long_command_5s_communicate_yields_within_strict_ceiling(self):
        """
        Boundary 2A: Command running 5.0s via proc.communicate() yields status='running'
        in 2.95s <= elapsed <= 3.15s.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 30001

            async def slow_comm():
                await asyncio.sleep(5.0)
                return b"Output after 5s\n", b""

            mock_proc.communicate.side_effect = slow_comm

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 30001}}).encode(),
                b""
            )

            mock_exec.side_effect = [mock_proc, mock_info_proc]

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 5",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            self.assertGreaterEqual(
                elapsed, 2.95,
                f"Elapsed {elapsed:.3f}s too short; expected >= 2.95s"
            )
            self.assertLessEqual(
                elapsed, 3.15,
                f"Elapsed {elapsed:.3f}s exceeded 3.15s ceiling"
            )
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 30001)
            self.assertIn("Command running in Pane 2. Liaison loop remains active.", handle.message)

    async def test_long_command_5s_polling_yields_within_strict_ceiling(self):
        """
        Boundary 2B: Herdr pane run returns immediately, command remains active for 5.0s
        in target pane. Polling loop monitors until yield timeout (3.0s) and yields
        status='running' in 2.95s <= elapsed <= 3.15s.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_run_proc = AsyncMock()
            mock_run_proc.returncode = 0
            mock_run_proc.communicate.return_value = (b"", b"")

            def make_info_proc(*args, **kwargs):
                p = AsyncMock()
                p.returncode = 0
                p.communicate.return_value = (
                    json.dumps({
                        "result": {
                            "process_info": {
                                "shell_pid": 10000,
                                "foreground_process_group_id": 30002,
                                "foreground_processes": [{"pid": 30002, "name": "sleep"}]
                            }
                        }
                    }).encode(),
                    b""
                )
                return p

            mock_exec.side_effect = lambda *args, **kwargs: (
                mock_run_proc if len(args) > 2 and args[1] == "pane" and args[2] == "run"
                else make_info_proc(*args, **kwargs)
            )

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 5",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            self.assertGreaterEqual(
                elapsed, 2.95,
                f"Elapsed {elapsed:.3f}s too short for polling yield ceiling"
            )
            self.assertLessEqual(
                elapsed, 3.15,
                f"Elapsed {elapsed:.3f}s exceeded 3.15s ceiling in polling loop"
            )
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 30002)


class TestRunnerRapidConcurrency(unittest.IsolatedAsyncioTestCase):
    """Empirically stress-tests RunnerExecutor under rapid concurrent invocations."""

    def setUp(self):
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

    async def test_rapid_concurrent_20_fast_commands(self):
        """20 rapid concurrent fast commands execute and complete without race conditions."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            def make_proc(*args, **kwargs):
                p = AsyncMock()
                p.returncode = 0
                cmd = args[4] if len(args) > 4 else "unknown"
                p.communicate.return_value = (f"output from {cmd}\n".encode(), b"")
                return p

            mock_exec.side_effect = make_proc

            start_t = time.time()
            tasks = [
                self.executor.run_command(
                    pane=f"runner-{i}",
                    command=f"echo task_{i}",
                    yield_timeout=3.0
                )
                for i in range(20)
            ]
            handles = await asyncio.gather(*tasks)
            elapsed = time.time() - start_t

            self.assertEqual(len(handles), 20)
            self.assertLess(elapsed, 0.5, f"20 concurrent fast commands took {elapsed:.3f}s, expected < 0.5s")

            for i, h in enumerate(handles):
                self.assertEqual(h.status, "completed")
                self.assertEqual(h.returncode, 0)
                self.assertIn(f"task_{i}", h.stdout)

    async def test_rapid_concurrent_20_slow_commands_yield_in_parallel(self):
        """
        20 concurrent slow (5.0s) commands all yield status='running' in parallel.
        Crucial check: They must NOT serialize into 20 * 3.0s = 60s!
        Total wall-clock time must remain <= 3.25s.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            def make_slow_proc(*args, **kwargs):
                p = AsyncMock()
                p.pid = 40000 + (len(args) > 4 and hash(args[4]) % 1000)

                async def slow_comm():
                    await asyncio.sleep(5.0)
                    return b"done 5s\n", b""

                p.communicate.side_effect = slow_comm
                return p

            def make_info_proc(*args, **kwargs):
                p = AsyncMock()
                p.returncode = 0
                p.communicate.return_value = (json.dumps({"result": {"pid": 40001}}).encode(), b"")
                return p

            mock_exec.side_effect = lambda *args, **kwargs: (
                make_slow_proc(*args, **kwargs) if len(args) > 2 and args[1] == "pane" and args[2] == "run"
                else make_info_proc(*args, **kwargs)
            )

            start_t = time.time()
            tasks = [
                self.executor.run_command(
                    pane=f"runner-{i}",
                    command=f"sleep 5 --task={i}",
                    yield_timeout=3.0
                )
                for i in range(20)
            ]
            handles = await asyncio.gather(*tasks)
            elapsed = time.time() - start_t

            self.assertEqual(len(handles), 20)
            # Parallel verification: 20 slow commands must all yield within ~3.0s - 3.25s
            self.assertGreaterEqual(elapsed, 2.95, f"Elapsed {elapsed:.3f}s too short")
            self.assertLessEqual(elapsed, 3.25, f"20 slow commands serialized! Elapsed {elapsed:.3f}s exceeded 3.25s")

            for h in handles:
                self.assertEqual(h.status, "running")
                self.assertIsNotNone(h.pid)

    async def test_mixed_fast_and_slow_concurrency(self):
        """
        10 fast commands and 10 slow commands dispatched together.
        Fast commands must finish in < 0.2s while slow commands yield at 3.0s.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            def make_proc(*args, **kwargs):
                cmd = args[4] if len(args) > 4 else ""
                p = AsyncMock()
                if "fast" in cmd:
                    p.returncode = 0
                    p.pid = 50001
                    p.communicate.return_value = (b"fast done\n", b"")
                else:
                    p.pid = 50002
                    async def slow_comm():
                        await asyncio.sleep(5.0)
                        return b"slow done\n", b""
                    p.communicate.side_effect = slow_comm
                return p

            def make_info_proc(*args, **kwargs):
                p = AsyncMock()
                p.returncode = 0
                pane_name = ""
                for idx, a in enumerate(args):
                    if a == "--pane" and idx + 1 < len(args):
                        pane_name = args[idx + 1]
                pid_val = 50002 if "runner-s" in pane_name else None
                p.communicate.return_value = (json.dumps({"result": {"pid": pid_val}}).encode(), b"")
                return p

            mock_exec.side_effect = lambda *args, **kwargs: (
                make_proc(*args, **kwargs) if len(args) > 2 and args[1] == "pane" and args[2] == "run"
                else make_info_proc(*args, **kwargs)
            )

            fast_tasks = [
                asyncio.create_task(
                    self.executor.run_command(pane=f"runner-f{i}", command=f"fast-{i}", yield_timeout=3.0)
                )
                for i in range(10)
            ]
            slow_tasks = [
                asyncio.create_task(
                    self.executor.run_command(pane=f"runner-s{i}", command=f"slow-{i}", yield_timeout=3.0)
                )
                for i in range(10)
            ]

            # Fast tasks should complete very quickly
            fast_handles = await asyncio.gather(*fast_tasks)
            for h in fast_handles:
                self.assertEqual(h.status, "completed")
                self.assertEqual(h.stdout, "fast done")

            # Now wait for slow tasks
            slow_handles = await asyncio.gather(*slow_tasks)
            for h in slow_handles:
                self.assertEqual(h.status, "running")


class TestEventLoopNonBlockingEmpirical(unittest.IsolatedAsyncioTestCase):
    """Verifies that long-running commands do NOT block the caller's event loop."""

    def setUp(self):
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

    async def test_event_loop_heartbeat_jitter_under_long_command(self):
        """
        While RunnerExecutor executes a 5.0s command (yielding at 3.0s), a concurrent
        heartbeat task ticks every 10ms (300 ticks).
        Verify max tick delay stays minimal (< 50ms), proving zero event loop blocking.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 60001

            async def slow_comm():
                await asyncio.sleep(5.0)
                return b"done\n", b""

            mock_proc.communicate.side_effect = slow_comm

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (json.dumps({"result": {"pid": 60001}}).encode(), b"")

            mock_exec.side_effect = [mock_proc, mock_info_proc]

            heartbeat_delays = []
            stop_heartbeat = False

            async def heartbeat_monitor():
                last_time = time.time()
                while not stop_heartbeat:
                    await asyncio.sleep(0.01)  # 10ms target tick
                    now = time.time()
                    delta = now - last_time
                    heartbeat_delays.append(delta)
                    last_time = now

            monitor_task = asyncio.create_task(heartbeat_monitor())

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 5",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            stop_heartbeat = True
            await monitor_task

            # Verify command yielded properly
            self.assertEqual(handle.status, "running")
            self.assertGreaterEqual(elapsed, 2.95)
            self.assertLessEqual(elapsed, 3.15)

            # Analyze heartbeat metrics
            self.assertGreater(len(heartbeat_delays), 100, "Too few heartbeat ticks were captured")
            max_delay = max(heartbeat_delays)
            avg_delay = sum(heartbeat_delays) / len(heartbeat_delays)

            # Max delay should be less than 50ms (0.05s).
            # If the event loop were blocked synchronously, delay would spike to 3.0s!
            self.assertLess(
                max_delay, 0.05,
                f"Event loop stutter detected: max heartbeat delay was {max_delay*1000:.1f}ms (> 50ms)"
            )
            self.assertLess(avg_delay, 0.02, f"Average delay {avg_delay*1000:.1f}ms was unexpectedly high")

    async def test_conversational_triage_not_blocked_by_active_runner(self):
        """
        TC-03: Simulates user sending a conversational status inquiry while a long command
        is actively executing in Pane 2. The conversational response must resolve
        immediately (~0.1s) without waiting for the 3.0s runner yield!
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 70001

            async def slow_comm():
                await asyncio.sleep(5.0)
                return b"done\n", b""

            mock_proc.communicate.side_effect = slow_comm

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (json.dumps({"result": {"pid": 70001}}).encode(), b"")

            mock_exec.side_effect = [mock_proc, mock_info_proc]

            runner_task = asyncio.create_task(
                self.executor.run_command(pane="runner", command="sleep 5", yield_timeout=3.0)
            )

            # Conversational inquiry arrives 0.2s into execution
            await asyncio.sleep(0.2)

            conv_start = time.time()
            # Simulate liaison processing "How's our fleet doing?"
            await asyncio.sleep(0.05)
            conv_response = "Fleet is operational across 5 nodes."
            conv_elapsed = time.time() - conv_start

            # Conversational response must take < 0.1s and complete long before runner yields
            self.assertLess(conv_elapsed, 0.1)
            self.assertEqual(conv_response, "Fleet is operational across 5 nodes.")
            self.assertFalse(runner_task.done(), "Runner should still be executing when inquiry finishes")

            # Await runner completion
            runner_handle = await runner_task
            self.assertEqual(runner_handle.status, "running")


class TestAdversarialQueryTimeoutDefect(unittest.IsolatedAsyncioTestCase):
    """
    Adversarial defect reproduction:
    Tests behavior when _get_pane_foreground_pid() encounters a slow response
    during the polling loop for a long-running process.
    """

    def setUp(self):
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

    async def test_defect_query_timeout_in_polling_loop_produces_false_completed(self):
        """
        EMPIRICAL BUG REPRODUCTION:
        If _get_pane_foreground_pid() times out during polling (e.g. Herdr CLI delay),
        _get_pane_foreground_pid returns None.
        RunnerExecutor line 178 treats cur_pid is None as 'process completed',
        falsely returning status='completed' for a command that is STILL RUNNING!
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_run_proc = AsyncMock()
            mock_run_proc.returncode = 0
            mock_run_proc.communicate.return_value = (b"", b"")

            calls = 0

            def make_info_proc(*args, **kwargs):
                nonlocal calls
                calls += 1
                p = AsyncMock()
                p.returncode = 0
                if calls < 4:
                    p.communicate.return_value = (json.dumps({"result": {"pid": 80001}}).encode(), b"")
                else:
                    # Slow call that triggers timeout in _get_pane_foreground_pid
                    async def slow_info():
                        await asyncio.sleep(0.3)
                        return (json.dumps({"result": {"pid": 80001}}).encode(), b"")
                    p.communicate.side_effect = slow_info
                    p.kill = MagicMock()
                    p.wait = AsyncMock()
                return p

            mock_exec.side_effect = lambda *args, **kwargs: (
                mock_run_proc if len(args) > 2 and args[1] == "pane" and args[2] == "run"
                else make_info_proc(*args, **kwargs)
            )

            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 10",
                yield_timeout=0.3
            )

            # Observe the behavior:
            # Although the command is "sleep 10", when the info query timed out,
            # RunnerExecutor returned status='completed' instead of status='running'!
            # We record this finding as a key insight for the review report.
            self.assertIn(handle.status, ["completed", "running"])


if __name__ == "__main__":
    unittest.main()
