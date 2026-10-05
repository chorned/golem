#!/usr/bin/env python3
"""
tests/test_runner_yield_ceiling.py
Comprehensive test suite verifying:
1. TC-02: Commands exceeding 3.0s yield an active ExecutionHandle (status='running', pid) within <= 3.15s.
2. Fast commands (< 3.0s) return status='completed' with returncode, stdout, and stderr.
3. Fast failing commands return status='error' with returncode and stderr.
4. Auxiliary timeout clamping:
   - _get_pane_foreground_pid() timeout clamped to remaining yield budget.
   - _try_resolve_and_run() pane listing timeout clamped to remaining yield budget.
5. Empty command string returns error handle immediately.
6. ExecutionHandle dictionary access and attribute contract.
7. Liaison agent prompt constraints in prompts.py and calibration.py prohibiting local blocking subprocesses.
8. herdr_tools.run_command non-blocking execution via RunnerExecutor targeting Pane 2 (no 15s blocking).
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
from golem.agent.prompts import (
    CALIBRATION_PROMPT,
    DUAL_PANE_EXECUTION_RULES,
    build_calibrated_system_prompt,
    get_calibration_rules,
)
import herdr_tools


class TestRunnerYieldCeiling(unittest.IsolatedAsyncioTestCase):
    """Verifies RunnerExecutor strict <= 3.0s yield ceiling and auxiliary timeout clamping."""

    def setUp(self):
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

    async def test_tc02_command_exceeding_3s_yields_running_handle_within_ceiling(self):
        """
        TC-02: Commands exceeding 3.0s yield an active ExecutionHandle (status='running')
        with foreground PID within <= 3.15s real elapsed time.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 98765

            # Simulate subprocess running longer than 3.0s
            async def slow_communicate():
                await asyncio.sleep(3.5)
                return b"Finished after 3.5s\n", b""

            mock_proc.communicate.side_effect = slow_communicate

            # Mock process-info returning foreground PID
            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 98765}}).encode("utf-8"),
                b""
            )

            mock_exec.side_effect = [mock_proc, mock_info_proc]

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 10",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            # Verify yield ceiling invariant: elapsed <= 3.15 seconds
            self.assertLessEqual(
                elapsed,
                3.15,
                f"Yield ceiling violated: elapsed {elapsed:.3f}s exceeds 3.15s ceiling"
            )
            self.assertGreaterEqual(
                elapsed,
                2.95,
                f"Elapsed time {elapsed:.3f}s too short for a 3.0s yield timeout"
            )

            # Verify ExecutionHandle running contract
            self.assertIsInstance(handle, ExecutionHandle)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pane, "runner")
            self.assertEqual(handle.command, "sleep 10")
            self.assertEqual(handle.pid, 98765)
            self.assertIsNone(handle.returncode)
            self.assertIn("Command running in Pane 2. Liaison loop remains active.", handle.message)
            self.assertGreater(handle.dispatched_at, 0)

            # Verify dictionary access
            self.assertEqual(handle["status"], "running")
            self.assertEqual(handle.get("pid"), 98765)

    async def test_fast_command_completes_within_yield_timeout(self):
        """Fast commands (< 3.0s) return status='completed' with returncode=0 and output."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.pid = 12345
            mock_proc.communicate.return_value = (b"Deploy succeeded in 0.2s\n", b"")

            mock_exec.return_value = mock_proc

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="echo deploy",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            self.assertLess(elapsed, 0.5)
            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertEqual(handle.stdout, "Deploy succeeded in 0.2s")
            self.assertEqual(handle.stderr, "")
            self.assertEqual(handle.pane, "runner")
            self.assertEqual(handle.command, "echo deploy")
            self.assertIn("Command completed within yield timeout.", handle.message)

    async def test_fast_command_error_within_yield_timeout(self):
        """Failing fast commands return status='error' with non-zero returncode and stderr."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 127
            mock_proc.pid = 23456
            mock_proc.communicate.return_value = (b"", b"command not found: nonexistent_tool\n")

            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="nonexistent_tool",
                yield_timeout=3.0
            )

            self.assertEqual(handle.status, "error")
            self.assertEqual(handle.returncode, 127)
            self.assertIn("nonexistent_tool", handle.stderr)
            self.assertIn("Command failed with exit code 127.", handle.message)

    async def test_empty_command_returns_error_handle(self):
        """Empty command string returns error handle without executing subprocess."""
        handle = await self.executor.run_command(pane="runner", command="")
        self.assertEqual(handle.status, "error")
        self.assertEqual(handle.returncode, -1)
        self.assertIn("Command string is required.", handle.stderr)

    async def test_auxiliary_pid_query_timeout_clamped_to_remaining_budget(self):
        """
        Verify that _get_pane_foreground_pid() is dynamically clamped to the remaining budget.
        Even if process-info would take 2.0s, clamping to remaining budget ensures <= 3.15s ceiling.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            # Main command proc times out after 3.0s
            mock_proc = AsyncMock()
            mock_proc.pid = 55555

            async def sleep_3s():
                await asyncio.sleep(3.0)
                raise asyncio.TimeoutError()

            mock_proc.communicate.side_effect = sleep_3s

            # Info proc would take 2.0s if unbounded
            mock_info_proc = AsyncMock()
            mock_info_proc.pid = 99999

            async def slow_info():
                await asyncio.sleep(2.0)
                return b'{"result": {"pid": 55555}}', b""

            mock_info_proc.communicate.side_effect = slow_info
            mock_info_proc.kill = MagicMock()
            mock_info_proc.wait = AsyncMock()

            mock_exec.side_effect = [mock_proc, mock_info_proc]

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="long_build.sh",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            # Verify that elapsed does NOT stretch to 3.0 + 2.0 = 5.0s!
            # It must strictly yield within <= 3.15s!
            self.assertLessEqual(
                elapsed,
                3.15,
                f"Timing leak detected: elapsed {elapsed:.3f}s exceeds 3.15s ceiling"
            )
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 55555)

    async def test_auxiliary_label_resolution_clamped_to_remaining_budget(self):
        """
        Verify that _try_resolve_and_run() clamps pane list timeout to the remaining budget.
        Total elapsed time remains strictly <= 3.15s.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            # Initial pane run fails with pane_not_found after 2.8s
            mock_proc = AsyncMock()
            mock_proc.returncode = 1
            mock_proc.pid = 11111

            async def run_fail():
                await asyncio.sleep(2.8)
                return b"", b"error: pane_not_found"

            mock_proc.communicate.side_effect = run_fail

            # List proc is slow (would take 2.0s if unbounded)
            mock_list_proc = AsyncMock()
            mock_list_proc.pid = 22222

            async def slow_list():
                await asyncio.sleep(2.0)
                return b'{"result": {"panes": [{"label": "runner", "pane_id": "w1:p2"}]}}', b""

            mock_list_proc.communicate.side_effect = slow_list
            mock_list_proc.kill = MagicMock()
            mock_list_proc.wait = AsyncMock()

            mock_exec.side_effect = [mock_proc, mock_list_proc]

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="make test",
                yield_timeout=3.0
            )
            elapsed = time.time() - start_t

            # Must not stretch to 2.8 + 2.0 = 4.8s! Clamped to <= 3.15s
            self.assertLessEqual(
                elapsed,
                3.15,
                f"Label resolution timing leak: elapsed {elapsed:.3f}s exceeds 3.15s ceiling"
            )
            self.assertEqual(handle.status, "error")  # List timed out, returned None, original error returned


class TestLiaisonSubprocessConstraints(unittest.TestCase):
    """Verifies Liaison agent prompt calibration restricts local blocking subprocesses."""

    def test_calibration_prompt_contains_dual_pane_execution_guardrails(self):
        """Verify CALIBRATION_PROMPT contains the mandatory dual-pane execution rules."""
        rules = get_calibration_rules()
        self.assertIn("--- Dual-Pane Execution & Delegation Guardrail ---", rules)
        self.assertIn("No Blocking Subprocesses in Liaison Space", rules)
        self.assertIn("All Shell Execution in Pane 2 (The Hands)", rules)
        self.assertIn("3-Second Non-Blocking Yield", rules)
        self.assertIn("Out-of-Band Status Checking", rules)

    def test_prompts_reexports_dual_pane_rules(self):
        """Verify golem.agent.prompts re-exports DUAL_PANE_EXECUTION_RULES."""
        self.assertTrue(len(DUAL_PANE_EXECUTION_RULES) > 50)
        self.assertIn("herdr_pane_run", DUAL_PANE_EXECUTION_RULES)
        self.assertIn("RunnerExecutor", DUAL_PANE_EXECUTION_RULES)

    def test_build_calibrated_system_prompt_enforces_pane2_delegation(self):
        """Verify full system prompt includes explicit Pane 2 delegation mandate."""
        sys_prompt = build_calibrated_system_prompt()
        self.assertIn("No Blocking Subprocesses in Liaison Space", sys_prompt)
        self.assertIn("Pane 2 (`runner`)", sys_prompt)
        self.assertIn("RunnerExecutor", sys_prompt)


class TestHerdrToolsRunCommandDelegation(unittest.IsolatedAsyncioTestCase):
    """Verifies herdr_tools.run_command routes through RunnerExecutor and is non-blocking."""

    async def test_run_command_delegates_to_runner_executor_fast_completion(self):
        """Verify run_command delegates to RunnerExecutor and returns stdout on success."""
        with patch("golem.execution.runner.RunnerExecutor.run_command") as mock_run:
            mock_run.return_value = ExecutionHandle(
                status="completed",
                pane="runner",
                command="git status -s",
                returncode=0,
                stdout="M golem/execution/runner.py",
                stderr=""
            )

            result = await herdr_tools.run_command(cmd="git status -s", cwd="/Users/hermes")
            self.assertEqual(result, "M golem/execution/runner.py")
            mock_run.assert_called_once()
            called_kwargs = mock_run.call_args[1]
            self.assertEqual(called_kwargs.get("pane"), "runner")
            self.assertEqual(called_kwargs.get("yield_timeout"), 3.0)

    async def test_run_command_returns_nonblocking_message_on_yield(self):
        """Verify run_command returns non-blocking status message when command runs > 3.0s."""
        with patch("golem.execution.runner.RunnerExecutor.run_command") as mock_run:
            mock_run.return_value = ExecutionHandle(
                status="running",
                pane="runner",
                command="rclone config reconnect",
                pid=77777,
                message="Command running in Pane 2. Liaison loop remains active."
            )

            result = await herdr_tools.run_command(cmd="rclone config reconnect")
            self.assertIn("[Running in Pane 2 (pid=77777)]", result)
            self.assertIn("Command running in Pane 2. Liaison loop remains active.", result)

    async def test_run_command_returns_error_message_on_failure(self):
        """Verify run_command returns formatted error message when command fails."""
        with patch("golem.execution.runner.RunnerExecutor.run_command") as mock_run:
            mock_run.return_value = ExecutionHandle(
                status="error",
                pane="runner",
                command="cat missing.txt",
                returncode=1,
                stderr="No such file or directory"
            )

            result = await herdr_tools.run_command(cmd="cat missing.txt")
            self.assertIn("Command exited with code 1:", result)
            self.assertIn("No such file or directory", result)


if __name__ == "__main__":
    unittest.main()
