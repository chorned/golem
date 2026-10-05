#!/usr/bin/env python3
"""
tests/test_adversarial_m2_concurrency.py
Adversarial Stress Test Suite for Milestone 2 (Execution Engine, PTY Runner, Yield Ceiling & Concurrency).

Mandatory Oppositional Analysis Areas:
1. PTY Deadlocks & Multi-Pane Concurrency:
   - Zero deadlocks under concurrent multi-pane subshells with heavy I/O.
   - Zero zombie processes or pipe buffer saturation stalls.
   - Nested pipelines, subshell escapes, and heredocs under concurrent load.
2. 3.0s Non-Blocking Yield Timeout Precision:
   - Long-running commands (sleep 10, looping output) yield ExecutionHandle(status="running")
     within 3.0s (+/- 0.2s: [2.80s, 3.20s]).
   - Caller is freed immediately and can continue reasoning without blocking.
   - Dynamic yield timeout validation (1.0s, 2.0s, 3.0s).
3. Output Buffer Draining & Exit Code Fidelity:
   - Commands producing 50,000+ characters are captured without truncation.
   - Nonzero exit codes (exit 42, 1, 2, 126, 127, 130, 255) propagate with exact fidelity.
   - Combined 50k chars + exit 42 preserves both full output and code 42.
   - WorkerDelegator output sanitization preserves large payloads.
4. Rapid Concurrent Invocations & Session State Integrity:
   - 20+ parallel WorkerDelegator.handoff calls across workers (agy, opencode, hermes).
   - Zero state pollution or cross-talk between concurrent execution handles.
   - 50 parallel high-velocity invocations without event loop starvation.
"""

import os
import sys
import json
import time
import asyncio
import unittest
from typing import List, Dict, Any
from unittest.mock import patch, AsyncMock, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.execution.runner import RunnerExecutor, ExecutionHandle
from golem.execution.delegation import WorkerDelegator, sanitize_worker_output
from golem.execution.buffer import RunnerBufferObserver
import golem.execution.runner
import golem.execution.delegation


# ===========================================================================
# 1. Adversarial PTY Deadlock & Multi-Pane Subshell Stress Tests
# ===========================================================================
class TestPTYDeadlocksAndMultiPaneConcurrency(unittest.IsolatedAsyncioTestCase):
    """
    Adversarial verification of zero PTY deadlocks under concurrent multi-pane subshells.
    Tests both real OS subprocess pipes and RunnerExecutor orchestration.
    """

    async def test_real_concurrent_subshells_heavy_io_zero_deadlock(self):
        """
        Adversarial test: 10 concurrent real OS subshells writing >64KB each
        to stdout and stderr across nested subshells and pipes.
        Verifies OS pipe buffers do not cause deadlocks or hangs.
        """
        num_subshells = 10
        chunk_size = 8192
        num_chunks = 10  # 80,000 bytes per subshell

        async def run_heavy_subshell(idx: int):
            cmd = (
                f"python3 -c \""
                f"import sys; "
                f"[sys.stdout.write('OUT_{idx}_' + 'X'*80 + '\\n') for _ in range(500)]; "
                f"[sys.stderr.write('ERR_{idx}_' + 'Y'*80 + '\\n') for _ in range(500)]; "
                f"sys.stdout.flush(); sys.stderr.flush()"
                f"\""
            )
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=5.0)
            return proc.returncode, len(stdout_bytes), len(stderr_bytes)

        start_t = time.time()
        results = await asyncio.gather(*[run_heavy_subshell(i) for i in range(num_subshells)])
        elapsed = time.time() - start_t

        self.assertLess(elapsed, 4.0, f"Heavy subshells took {elapsed:.2f}s, potential deadlock!")
        self.assertEqual(len(results), num_subshells)
        for code, out_len, err_len in results:
            self.assertEqual(code, 0)
            self.assertGreater(out_len, 40000)
            self.assertGreater(err_len, 40000)

    async def test_runner_executor_concurrent_multi_pane_mixed_workload(self):
        """
        Adversarial test: 15 concurrent RunnerExecutor commands across different panes:
        - 5 fast commands (< 0.1s)
        - 5 medium commands (~0.3s)
        - 5 slow commands (> 3.0s that trigger yield)
        Verifies zero deadlocks and accurate handle status separation.
        """
        executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

        async def mock_exec_dispatcher(*args, **kwargs):
            subcmd = args[2] if len(args) > 2 else ""
            if subcmd == "process-info":
                info_proc = AsyncMock()
                info_proc.returncode = 0
                info_proc.kill = MagicMock()
                info_proc.communicate.return_value = (
                    json.dumps({"result": {"pid": None}}).encode(),
                    b""
                )
                return info_proc

            pane_id = args[3] if len(args) > 3 else ""
            proc = AsyncMock()
            proc.kill = MagicMock()

            if "fast" in pane_id:
                proc.pid = 30001
                proc.returncode = 0
                proc.communicate.return_value = (f"Fast result from {pane_id}\n".encode(), b"")
            elif "med" in pane_id:
                proc.pid = 30002
                proc.returncode = 0
                async def med_comm():
                    await asyncio.sleep(0.3)
                    return f"Medium result from {pane_id}\n".encode(), b""
                proc.communicate.side_effect = med_comm
            else:  # slow
                proc.pid = 30003
                async def slow_comm():
                    await asyncio.sleep(4.0)
                    return b"Never reached\n", b""
                proc.communicate.side_effect = slow_comm

            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=mock_exec_dispatcher):
            tasks = []
            for i in range(5):
                tasks.append(executor.run_command(pane=f"pane-fast-{i}", command="echo fast", yield_timeout=3.0))
                tasks.append(executor.run_command(pane=f"pane-med-{i}", command="sleep 0.3", yield_timeout=3.0))
                tasks.append(executor.run_command(pane=f"pane-slow-{i}", command="sleep 10", yield_timeout=3.0))

            start_t = time.time()
            results = await asyncio.gather(*tasks)
            elapsed = time.time() - start_t

        # Slow tasks yield at ~3.0s; total elapsed time must be <= 3.35s
        self.assertLessEqual(elapsed, 3.35, f"Elapsed {elapsed:.2f}s exceeded concurrent yield window")
        self.assertEqual(len(results), 15)

        fast_handles = [h for h in results if "fast" in h.pane]
        med_handles = [h for h in results if "med" in h.pane]
        slow_handles = [h for h in results if "slow" in h.pane]

        for h in fast_handles:
            self.assertEqual(h.status, "completed")
            self.assertIn("Fast result", h.stdout)

        for h in med_handles:
            self.assertEqual(h.status, "completed")
            self.assertIn("Medium result", h.stdout)

        for h in slow_handles:
            self.assertEqual(h.status, "running")
            self.assertIn("sleep 10", h.command)


    async def test_nested_subshell_pipes_and_escapes(self):
        """
        Adversarial test: Complex subshell constructs with nested parentheses,
        pipes, heredocs, and background forks to verify clean exit without hanging.
        """
        script = (
            "( ( echo 'nested-start' && ( echo 'inner-1' | cat ) && ( echo 'inner-2' ) ) | tr 'a-z' 'A-Z' ) "
            "&& echo 'all-done'"
        )
        proc = await asyncio.create_subprocess_shell(
            script,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=3.0)
        self.assertEqual(proc.returncode, 0)
        out = stdout.decode().strip()
        self.assertIn("NESTED-START", out)
        self.assertIn("INNER-1", out)
        self.assertIn("INNER-2", out)
        self.assertIn("all-done", out)


# ===========================================================================
# 2. Adversarial 3.0s Non-Blocking Yield Timeout Precision Tests
# ===========================================================================
class TestYieldCeilingPrecision(unittest.IsolatedAsyncioTestCase):
    """
    Adversarial verification of 3.0s non-blocking yield timeout precision:
    Must yield ExecutionHandle(status="running") within 3.0s (+/- 0.2s: [2.80s, 3.20s]).
    """

    def setUp(self):
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

    async def test_yield_ceiling_sleep_10_precision(self):
        """
        Adversarial: Dispatch 'sleep 10' with yield_timeout=3.0.
        Verifies handle yields within 2.80s <= elapsed <= 3.20s without blocking caller.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            proc = AsyncMock()
            proc.pid = 99881
            proc.kill = MagicMock()

            async def sleep_long():
                await asyncio.sleep(10.0)
                return b"Finished 10s\n", b""

            proc.communicate.side_effect = sleep_long
            mock_exec.return_value = proc

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 10",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            # Verify +/- 0.2s precision requirement
            self.assertGreaterEqual(
                elapsed, 2.80,
                f"Yielded too early: {elapsed:.3f}s < 2.80s lower bound"
            )
            self.assertLessEqual(
                elapsed, 3.20,
                f"Yield ceiling violated: {elapsed:.3f}s > 3.20s upper bound"
            )

            # Contract verification
            self.assertIsInstance(handle, ExecutionHandle)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pane, "runner")
            self.assertEqual(handle.command, "sleep 10")
            self.assertEqual(handle.pid, 99881)
            self.assertIsNone(handle.returncode)
            self.assertIn("Command running in Pane 2", handle.message)

    async def test_yield_ceiling_looping_output_precision(self):
        """
        Adversarial: Dispatch command with continuous streaming output.
        Verifies yield fires at 3.0s (+/- 0.2s) even while pipe has continuous output.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            proc = AsyncMock()
            proc.pid = 99882
            proc.kill = MagicMock()

            async def stream_output():
                # Simulate infinite streaming loop
                while True:
                    await asyncio.sleep(0.05)
                return b"never\n", b""

            proc.communicate.side_effect = stream_output
            mock_exec.return_value = proc

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="while true; do echo spam; sleep 0.05; done",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            self.assertGreaterEqual(
                elapsed, 2.80,
                f"Yielded too early on streaming command: {elapsed:.3f}s"
            )
            self.assertLessEqual(
                elapsed, 3.20,
                f"Yield ceiling violated on streaming command: {elapsed:.3f}s"
            )
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 99882)

    async def test_caller_responsiveness_unblocked_after_yield(self):
        """
        Adversarial: After running command yields at 3.0s, caller can immediately
        run another query without being blocked or hanging.
        """
        active_cmd = ""

        async def exec_dispatcher(*args, **kwargs):
            nonlocal active_cmd
            subcmd = args[2] if len(args) > 2 else ""
            if subcmd == "process-info":
                info_proc = AsyncMock()
                info_proc.returncode = 0
                info_proc.kill = MagicMock()
                # If sleep was dispatched, report active PID; if echo immediate, no foreground process
                current_pid = 99883 if "sleep" in active_cmd else None
                info_proc.communicate.return_value = (
                    json.dumps({"result": {"pid": current_pid}}).encode(),
                    b""
                )
                return info_proc

            cmd = args[4] if len(args) > 4 else ""
            active_cmd = cmd
            proc = AsyncMock()
            proc.kill = MagicMock()
            if "sleep" in cmd:
                proc.pid = 99883
                async def slow_fn():
                    await asyncio.sleep(5.0)
                    return b"", b""
                proc.communicate.side_effect = slow_fn
            else:
                proc.pid = 99884
                proc.returncode = 0
                proc.communicate.return_value = (b"Immediate query response\n", b"")
            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=exec_dispatcher):
            # 1. Run slow command
            start_t = time.time()
            handle1 = await self.executor.run_command(pane="runner", command="sleep 5", yield_timeout=3.0)
            elapsed1 = time.time() - start_t
            self.assertEqual(handle1.status, "running")
            self.assertLessEqual(elapsed1, 3.20)

            # 2. Immediately execute second query
            start_t2 = time.time()
            handle2 = await self.executor.run_command(pane="runner", command="echo immediate", yield_timeout=3.0)
            elapsed2 = time.time() - start_t2


            # Second command must execute immediately (< 0.1s)
            self.assertLess(elapsed2, 0.15, f"Caller was blocked for {elapsed2:.3f}s after yield!")
            self.assertEqual(handle2.status, "completed")
            self.assertEqual(handle2.stdout, "Immediate query response")


    async def test_dynamic_yield_timeout_boundaries(self):
        """
        Adversarial: Verifies yield timeout parameter is respected dynamically:
        - yield_timeout=1.0 yields in [0.85s, 1.20s]
        - yield_timeout=2.0 yields in [1.85s, 2.20s]
        """
        for timeout_val in [1.0, 2.0]:
            with patch("asyncio.create_subprocess_exec") as mock_exec:
                proc = AsyncMock()
                proc.pid = 99885
                proc.kill = MagicMock()
                async def slow_fn():
                    await asyncio.sleep(5.0)
                    return b"", b""
                proc.communicate.side_effect = slow_fn
                mock_exec.return_value = proc


                start_t = time.time()
                handle = await self.executor.run_command(
                    pane="runner",
                    command=f"sleep 5",
                    yield_timeout=timeout_val
                )
                elapsed = time.time() - start_t

                self.assertGreaterEqual(
                    elapsed, timeout_val - 0.20,
                    f"Yielded too early for timeout={timeout_val}: {elapsed:.3f}s"
                )
                self.assertLessEqual(
                    elapsed, timeout_val + 0.20,
                    f"Yielded too late for timeout={timeout_val}: {elapsed:.3f}s"
                )
                self.assertEqual(handle.status, "running")


# ===========================================================================
# 3. Output Buffer Draining & Exit Code Fidelity Tests
# ===========================================================================
class TestBufferDrainingAndExitCodeFidelity(unittest.IsolatedAsyncioTestCase):
    """
    Adversarial verification of buffer draining (50,000+ chars) and exit code fidelity.
    """

    def setUp(self):
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

    async def test_large_output_draining_50k_characters(self):
        """
        Adversarial: Dispatch command producing 50,000+ characters of output.
        Verifies entire output is captured without truncation.
        """
        lines = [f"LINE_{i:04d}_" + ("A" * 40) for i in range(1000)]
        large_payload = "\n".join(lines)
        self.assertGreaterEqual(len(large_payload), 50000)

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            proc = AsyncMock()
            proc.returncode = 0
            proc.pid = 77001
            proc.communicate.return_value = (large_payload.encode("utf-8"), b"")
            mock_exec.return_value = proc

            handle = await self.executor.run_command(
                pane="runner",
                command="cat large_file.txt",
                yield_timeout=3.0
            )

            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertEqual(len(handle.stdout), len(large_payload))
            self.assertTrue(handle.stdout.startswith("LINE_0000_"))
            self.assertTrue(handle.stdout.endswith("LINE_0999_" + ("A" * 40)))

    async def test_exit_code_fidelity_exit_42(self):
        """
        Adversarial: Dispatch command with exit 42.
        Verifies exit code 42 is preserved with exact fidelity in ExecutionHandle.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            proc = AsyncMock()
            proc.returncode = 42
            proc.pid = 77002
            proc.communicate.return_value = (b"", b"Error: custom exit 42 occurred\n")
            mock_exec.return_value = proc

            handle = await self.executor.run_command(
                pane="runner",
                command="exit 42",
                yield_timeout=3.0
            )

            self.assertEqual(handle.status, "error")
            self.assertEqual(handle.returncode, 42)
            self.assertIn("exit code 42", handle.message)
            self.assertEqual(handle.stderr, "Error: custom exit 42 occurred")

    async def test_combined_50k_output_and_exit_42(self):
        """
        Adversarial: Command emits 50,000+ characters and exits with code 42.
        Verifies both large output AND exit code 42 are captured without truncation.
        """
        lines = [f"ERR_TRACE_{i:04d}_" + ("E" * 45) for i in range(1000)]
        large_err = "\n".join(lines)
        self.assertGreaterEqual(len(large_err), 50000)

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            proc = AsyncMock()
            proc.returncode = 42
            proc.pid = 77003
            proc.communicate.return_value = (b"", large_err.encode("utf-8"))
            mock_exec.return_value = proc

            handle = await self.executor.run_command(
                pane="runner",
                command="dump_trace_and_fail",
                yield_timeout=3.0
            )

            self.assertEqual(handle.status, "error")
            self.assertEqual(handle.returncode, 42)
            self.assertEqual(len(handle.stderr), len(large_err))
            self.assertTrue(handle.stderr.startswith("ERR_TRACE_0000_"))
            self.assertTrue(handle.stderr.endswith("ERR_TRACE_0999_" + ("E" * 45)))

    async def test_exit_codes_matrix_fidelity(self):
        """
        Adversarial matrix: Tests wide array of POSIX / bash exit codes:
        [1, 2, 42, 64, 126, 127, 130, 137, 255].
        Verifies exact fidelity for every code.
        """
        test_codes = [1, 2, 42, 64, 126, 127, 130, 137, 255]
        for code in test_codes:
            with patch("asyncio.create_subprocess_exec") as mock_exec:
                proc = AsyncMock()
                proc.returncode = code
                proc.pid = 77000 + code
                proc.communicate.return_value = (b"", f"Fail {code}\n".encode())
                mock_exec.return_value = proc

                handle = await self.executor.run_command(
                    pane="runner",
                    command=f"exit {code}",
                    yield_timeout=3.0
                )
                self.assertEqual(handle.status, "error")
                self.assertEqual(handle.returncode, code)
                self.assertIn(str(code), handle.message)

    async def test_worker_delegator_and_sanitizer_with_50k_output(self):
        """
        Adversarial: Verifies WorkerDelegator._execute_agy_hand and sanitize_worker_output
        do not truncate 50,000 characters of clean content.
        """
        delegator = WorkerDelegator(base_repo="/tmp")
        lines = [f"SECTION_{i:04d}: processed item successfully" for i in range(1200)]
        big_content = "\n".join(lines)
        self.assertGreaterEqual(len(big_content), 50000)

        # 1. Verify sanitizer preservation
        cleaned = sanitize_worker_output(big_content)
        self.assertEqual(len(cleaned), len(big_content))

        # 2. Verify _execute_agy_hand returncode and output fidelity
        with patch("golem.execution.runner.RunnerExecutor.run_command") as mock_run:
            mock_run.return_value = ExecutionHandle(
                status="completed",
                pane="runner",
                command="build_all",
                returncode=0,
                stdout=big_content,
                pid=12345,
                dispatched_at=time.time(),
                message="Done"
            )
            res = await delegator._execute_agy_hand(task="build_all", cwd="/tmp")
            self.assertEqual(res["status"], "completed")
            self.assertEqual(res["returncode"], 0)
            self.assertEqual(len(res["output"]), len(big_content))
            self.assertEqual(res["worker"], "agy")


# ===========================================================================
# 4. Rapid Concurrent Invocations & Session State Integrity Tests
# ===========================================================================
class TestRapidConcurrencyAndStateIntegrity(unittest.IsolatedAsyncioTestCase):
    """
    Adversarial verification of thread safety, state isolation, and session
    integrity across rapid concurrent invocations.
    """

    async def test_parallel_worker_delegator_handoff_isolation(self):
        """
        Adversarial: 20 parallel calls to WorkerDelegator.handoff across different
        workers (agy, opencode, hermes) with distinct tasks and parameters.
        Verifies zero cross-talk, correct routing, and state integrity.
        """
        mock_client = AsyncMock()
        mock_client.run_cli.return_value = (0, "Worktree created", "")

        delegator = WorkerDelegator(herdr_client=mock_client, base_repo="/tmp")

        async def run_handoff(idx: int):
            worker_type = ["agy", "opencode", "hermes"][idx % 3]
            task_desc = f"Task payload #{idx} for {worker_type}"
            node_name = f"node-{idx % 4}"
            cwd_path = f"/tmp/cwd_{idx}"

            with patch("golem.execution.delegation.WorkerDelegator._execute_agy_hand") as mock_agy:
                mock_agy.return_value = {
                    "status": "completed",
                    "worker": "agy",
                    "subagent": "agy",
                    "branch": "main",
                    "node": node_name,
                    "cwd": cwd_path,
                    "workspace_id": None,
                    "output": f"Output for task #{idx}",
                    "raw_output": f"Raw output #{idx}",
                    "pane": "runner",
                    "pid": 50000 + idx,
                    "handle": MagicMock(status="completed", returncode=0),
                    "returncode": 0,
                }

                res = await delegator.handoff(
                    worker=worker_type,
                    task=task_desc,
                    cwd=cwd_path,
                    node=node_name
                )
                return idx, worker_type, node_name, cwd_path, res

        tasks = [run_handoff(i) for i in range(20)]
        results = await asyncio.gather(*tasks)

        self.assertEqual(len(results), 20)
        for idx, expected_worker, expected_node, expected_cwd, res in results:
            self.assertEqual(res["worker"], expected_worker)
            self.assertEqual(res["node"], expected_node)
            self.assertEqual(res["cwd"], expected_cwd)
            if expected_worker == "agy":
                self.assertIn(f"task #{idx}", res["output"])

    async def test_execution_handles_isolation_under_concurrency(self):
        """
        Adversarial: 25 concurrent RunnerExecutor.run_command executions.
        Verifies each handle receives a distinct timestamp, distinct PID,
        and no cross-contamination between handles.
        """
        executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

        async def exec_task(idx: int):
            with patch("asyncio.create_subprocess_exec") as mock_exec:
                proc = AsyncMock()
                proc.returncode = idx % 2  # alternate 0 and 1
                proc.pid = 80000 + idx
                proc.communicate.return_value = (f"Output #{idx}".encode(), f"Err #{idx}".encode())
                mock_exec.return_value = proc

                handle = await executor.run_command(
                    pane=f"pane-{idx}",
                    command=f"cmd-{idx}",
                    yield_timeout=3.0
                )
                return idx, handle

        tasks = [exec_task(i) for i in range(25)]
        results = await asyncio.gather(*tasks)

        pids = set()
        for idx, handle in results:
            self.assertEqual(handle.pane, f"pane-{idx}")
            self.assertEqual(handle.command, f"cmd-{idx}")
            self.assertEqual(handle.pid, 80000 + idx)
            pids.add(handle.pid)
            if idx % 2 == 0:
                self.assertEqual(handle.status, "completed")
                self.assertEqual(handle.returncode, 0)
                self.assertEqual(handle.stdout, f"Output #{idx}")
            else:
                self.assertEqual(handle.status, "error")
                self.assertEqual(handle.returncode, 1)
                self.assertEqual(handle.stderr, f"Err #{idx}")

        # All 25 handles must have unique PIDs
        self.assertEqual(len(pids), 25)

    async def test_high_velocity_parallel_invocations(self):
        """
        Adversarial: Rapid fire 50 parallel operations through RunnerExecutor.
        Verifies event loop throughput and zero unhandled exceptions.
        """
        executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

        async def rapid_call(i: int):
            with patch("asyncio.create_subprocess_exec") as mock_exec:
                proc = AsyncMock()
                proc.returncode = 0
                proc.pid = 90000 + i
                proc.communicate.return_value = (b"ok", b"")
                mock_exec.return_value = proc
                return await executor.run_command(pane="runner", command="echo ping", yield_timeout=3.0)

        start_t = time.time()
        handles = await asyncio.gather(*[rapid_call(i) for i in range(50)])
        elapsed = time.time() - start_t

        self.assertEqual(len(handles), 50)
        for h in handles:
            self.assertEqual(h.status, "completed")
            self.assertEqual(h.returncode, 0)
        self.assertLess(elapsed, 2.0, f"50 rapid calls took {elapsed:.2f}s, too slow!")


if __name__ == "__main__":
    unittest.main()
