#!/usr/bin/env python3
"""
tests/test_execution_agy_default.py
Comprehensive test suite verifying Requirement R2: Pluggable Hand Execution Harness (AGY Default).

Covers:
1. Centralized Hand Worker Resolution (golem.config.get_default_hand_worker):
   - Defaults to 'agy'
   - GOLEM_DEFAULT_HAND_WORKER environment override
   - FIRSTMATE_DEFAULT_HAND_WORKER / FIRSTMATE_DEFAULT_HAND_WORKER fallback
   - Precedence: GOLEM_DEFAULT_HAND_WORKER > FIRSTMATE > GOLEM_ENGINE > GOLEM_ENGINE
   - Whitespace stripping and case insensitivity
   - Standardization of 'antigravity' to 'agy'
   - Re-exported in golem.execution, golem.execution.delegation, golem.execution, herdr_tools

2. herdr_tools Standardized Signatures & AGY Default:
   - herdr_spawn defaults worker to 'agy' without error when omitted or None
   - Explicit worker='opencode' routes properly
   - Explicit worker='hermes' routes properly
   - HERDR_TOOLS_DECLARATIONS schema reflects optional worker

3. herdr_handoff Parameter Precedence & Routing:
   - Defaults to 'agy'
   - worker parameter takes precedence over subagent
   - Legacy subagent fallback
   - Whitespace and case normalization

4. WorkerDelegator Pluggable Routing (golem.execution.delegation):
   - handoff defaults to 'agy' and executes directly in Pane 2 runner
   - worker parameter takes precedence over subagent
   - Explicit 'opencode' provisions worktree task-<ts>-opencode and sanitizes output
   - Explicit 'hermes' routes to hermes worker
   - Security validation rejects unknown workers, path traversal, shell injection
   - SUPPORTED_WORKERS catalog validation

5. Dual-Pane Isolation & 3.0s Non-Blocking Yield Semantics:
   - DualPaneHarness preserves Pane 1 (Mouth) and Pane 2 (Hands) isolation
   - RunnerExecutor maintains strict 3.0s non-blocking yield timeout ceiling
   - AGY hand execution yields non-blocking status='running' for long subshells

6. PTY Concurrency & Deadlock Stress (M2-EXEC Invariants):
   - Multi-pane concurrent subshell runs simulate zero deadlocks
   - Exit code fidelity without truncation
   - Clean process termination and buffer drain

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
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

import golem.config
import golem.config
import golem.execution
import golem.execution
from golem.execution.harness import DualPaneHarness
from golem.execution.runner import RunnerExecutor, ExecutionHandle
from golem.execution.delegation import WorkerDelegator, sanitize_worker_output
import herdr_tools


# ===========================================================================
# 1. Centralized Hand Worker Resolution Tests
# ===========================================================================
class TestCentralizedHandWorkerResolution(unittest.TestCase):
    """Verifies get_default_hand_worker resolution rules, normalization, and re-exports."""

    def setUp(self):
        self.orig_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.orig_env)

    def test_resolution_defaults_to_agy(self):
        """When no environment variables or file .env overrides are set, defaults to 'agy'."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {}, clear=True):
                worker = golem.config.get_default_hand_worker()
                self.assertEqual(worker, "agy")

    def test_resolution_respects_golem_env(self):
        """GOLEM_DEFAULT_HAND_WORKER environment variable sets the default worker."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": "opencode"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "opencode")

    def test_resolution_respects_firstmate_env(self):
        """FIRSTMATE_DEFAULT_HAND_WORKER sets the default worker if GOLEM var is unset."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {"FIRSTMATE_DEFAULT_HAND_WORKER": "hermes"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "hermes")

        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {"FIRSTMATE_DEFAULT_HAND_WORKER": "hermes"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "hermes")

    def test_resolution_precedence_hierarchy(self):
        """GOLEM_DEFAULT_HAND_WORKER takes precedence over FIRSTMATE and legacy engine vars."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            # All set: GOLEM_DEFAULT_HAND_WORKER wins
            with patch.dict(os.environ, {
                "GOLEM_DEFAULT_HAND_WORKER": "worker_golem",
                "FIRSTMATE_DEFAULT_HAND_WORKER": "worker_firstmate",
                "GOLEM_ENGINE": "worker_legacy"
            }, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "worker_golem")

            # Only FIRSTMATE and GOLEM_ENGINE set: FIRSTMATE wins
            with patch.dict(os.environ, {
                "FIRSTMATE_DEFAULT_HAND_WORKER": "worker_firstmate",
                "GOLEM_ENGINE": "worker_legacy"
            }, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "worker_firstmate")

            # Only legacy engine set: returns legacy engine
            with patch.dict(os.environ, {
                "GOLEM_ENGINE": "worker_legacy"
            }, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "worker_legacy")

    def test_resolution_whitespace_stripping(self):
        """Leading/trailing whitespace is stripped; whitespace-only strings fall back to 'agy'."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": "   opencode   "}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "opencode")

            # Whitespace-only falls back
            with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": "     "}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

            # Whitespace-only in GOLEM falls back to FIRSTMATE
            with patch.dict(os.environ, {
                "GOLEM_DEFAULT_HAND_WORKER": "   ",
                "FIRSTMATE_DEFAULT_HAND_WORKER": "hermes"
            }, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "hermes")

    def test_resolution_case_insensitivity(self):
        """Worker names are normalized to lowercase."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": "AGY"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

            with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": "OpenCode"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "opencode")

            with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": "HERMES"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "hermes")

    def test_resolution_standardizes_antigravity_to_agy(self):
        """Legacy 'antigravity' name standardizes to 'agy'."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": "antigravity"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

            with patch.dict(os.environ, {"GOLEM_ENGINE": "antigravity"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

    def test_re_exports_across_modules(self):
        """get_default_hand_worker is exported across golem.execution, golem.execution, herdr_tools."""
        self.assertTrue(callable(golem.config.get_default_hand_worker))
        self.assertTrue(callable(golem.config.get_default_hand_worker))
        self.assertTrue(callable(golem.execution.get_default_hand_worker))
        self.assertTrue(callable(golem.execution.get_default_hand_worker))
        self.assertTrue(callable(herdr_tools.get_default_hand_worker))

        # Check values match
        self.assertEqual(golem.config.get_default_hand_worker(), golem.execution.get_default_hand_worker())
        self.assertEqual(golem.config.get_default_hand_worker(), golem.execution.get_default_hand_worker())
        self.assertEqual(golem.config.get_default_hand_worker(), herdr_tools.get_default_hand_worker())


# ===========================================================================
# 2. herdr_tools Standardized Signatures & AGY Default Tests
# ===========================================================================
class TestHerdrToolsSignaturesAndDefaults(unittest.IsolatedAsyncioTestCase):
    """Verifies herdr_spawn and herdr_handoff default handling and signatures."""

    def setUp(self):
        self.orig_env = os.environ.copy()
        os.environ.pop("GOLEM_DEFAULT_HAND_WORKER", None)
        os.environ.pop("FIRSTMATE_DEFAULT_HAND_WORKER", None)
        os.environ.pop("FIRSTMATE_DEFAULT_HAND_WORKER", None)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.orig_env)

    async def test_herdr_spawn_defaults_to_agy_without_error(self):
        """herdr_spawn with worker=None or omitted resolves to 'agy' without raising error."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"Spawned w1:p42\n", b"")
            mock_exec.return_value = mock_proc

            # Call with worker omitted / None
            res = await herdr_tools.herdr_spawn(
                node="hermes",
                cwd="/tmp",
                prompt="Run sanity check",
                isolate_branch=False
            )

            self.assertNotIn("error", res)
            self.assertEqual(res["worker"], "agy")
            self.assertEqual(res["pane_id"], "w1:p42")

    async def test_herdr_spawn_explicit_opencode(self):
        """Explicit worker='opencode' is respected and sets branch task-<ts>-opencode."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"Spawned w1:p43\n", b"")
            mock_exec.return_value = mock_proc

            res = await herdr_tools.herdr_spawn(
                node="hermes",
                cwd="/tmp",
                worker="opencode",
                prompt="Write unit tests",
                isolate_branch=False
            )

            self.assertEqual(res["worker"], "opencode")
            self.assertEqual(res["pane_id"], "w1:p43")

    async def test_herdr_spawn_explicit_hermes(self):
        """Explicit worker='hermes' is respected and sets worker in return dictionary."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"Spawned w1:p44\n", b"")
            mock_exec.return_value = mock_proc

            res = await herdr_tools.herdr_spawn(
                node="hermes",
                cwd="/tmp",
                worker="hermes",
                prompt="Check docker containers",
                isolate_branch=False
            )

            self.assertEqual(res["worker"], "hermes")
            self.assertEqual(res["pane_id"], "w1:p44")

    def test_declarations_schema_worker_is_optional(self):
        """HERDR_TOOLS_DECLARATIONS schema does not require 'worker' for herdr_spawn."""
        spawn_decl = next((d for d in herdr_tools.HERDR_TOOLS_DECLARATIONS if d["name"] == "herdr_spawn"), None)
        self.assertIsNotNone(spawn_decl)
        req = spawn_decl["parameters"]["required"]
        self.assertNotIn("worker", req, "'worker' must be optional in herdr_spawn parameters schema")
        self.assertIn("node", req)
        self.assertIn("cwd", req)
        self.assertIn("prompt", req)


# ===========================================================================
# 3. herdr_handoff Precedence & Pluggable Routing Tests
# ===========================================================================
class TestHerdrHandoffPrecedence(unittest.IsolatedAsyncioTestCase):
    """Verifies herdr_handoff parameter precedence (worker over subagent) and defaulting."""

    def setUp(self):
        self.orig_env = os.environ.copy()
        os.environ.pop("GOLEM_DEFAULT_HAND_WORKER", None)
        os.environ.pop("FIRSTMATE_DEFAULT_HAND_WORKER", None)
        os.environ.pop("FIRSTMATE_DEFAULT_HAND_WORKER", None)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.orig_env)

    async def test_herdr_handoff_defaults_to_agy(self):
        """herdr_handoff with neither worker nor subagent defaults to 'agy'."""
        with patch("herdr_tools.herdr_spawn", new_callable=AsyncMock) as mock_spawn:
            mock_spawn.return_value = {"worker": "agy", "pane_id": "w1:p1", "branch": "task-1-agy"}

            res = await herdr_tools.herdr_handoff(
                task="Inspect fleet health",
                cwd="/tmp",
                node="hermes"
            )

            mock_spawn.assert_called_once()
            call_kwargs = mock_spawn.call_args[1]
            self.assertEqual(call_kwargs["worker"], "agy")
            self.assertEqual(res["worker"], "agy")

    async def test_herdr_handoff_worker_precedence_over_subagent(self):
        """worker parameter overrides subagent parameter."""
        with patch("herdr_tools.herdr_spawn", new_callable=AsyncMock) as mock_spawn:
            mock_spawn.return_value = {"worker": "opencode", "pane_id": "w1:p2", "branch": "task-1-opencode"}

            res = await herdr_tools.herdr_handoff(
                worker="opencode",
                subagent="hermes",
                task="Refactor transport normalizer",
                cwd="/tmp",
                node="hermes"
            )

            mock_spawn.assert_called_once()
            call_kwargs = mock_spawn.call_args[1]
            self.assertEqual(call_kwargs["worker"], "opencode")
            self.assertEqual(res["worker"], "opencode")

    async def test_herdr_handoff_legacy_subagent_fallback(self):
        """When worker is omitted, subagent is respected."""
        with patch("herdr_tools.herdr_spawn", new_callable=AsyncMock) as mock_spawn:
            mock_spawn.return_value = {"worker": "hermes", "pane_id": "w1:p3", "branch": "task-1-hermes"}

            res = await herdr_tools.herdr_handoff(
                subagent="hermes",
                task="Prune docker networks",
                cwd="/tmp",
                node="hermes"
            )

            mock_spawn.assert_called_once()
            call_kwargs = mock_spawn.call_args[1]
            self.assertEqual(call_kwargs["worker"], "hermes")
            self.assertEqual(res["worker"], "hermes")

    async def test_herdr_handoff_normalizes_case_and_whitespace(self):
        """Whitespace and casing in worker parameter are normalized."""
        with patch("herdr_tools.herdr_spawn", new_callable=AsyncMock) as mock_spawn:
            mock_spawn.return_value = {"worker": "opencode", "pane_id": "w1:p4", "branch": "task-1-opencode"}

            res = await herdr_tools.herdr_handoff(
                worker="   OPENCODE\n",
                task="Clean architecture",
                cwd="/tmp"
            )

            call_kwargs = mock_spawn.call_args[1]
            self.assertEqual(call_kwargs["worker"], "opencode")


# ===========================================================================
# 4. WorkerDelegator Pluggable Routing Tests
# ===========================================================================
class TestWorkerDelegatorRouting(unittest.IsolatedAsyncioTestCase):
    """Verifies WorkerDelegator delegation routing to AGY, OpenCode, and Hermes."""

    def setUp(self):
        self.orig_env = os.environ.copy()
        os.environ.pop("GOLEM_DEFAULT_HAND_WORKER", None)
        os.environ.pop("FIRSTMATE_DEFAULT_HAND_WORKER", None)
        os.environ.pop("FIRSTMATE_DEFAULT_HAND_WORKER", None)
        self.mock_client = AsyncMock()
        self.delegator = WorkerDelegator(
            herdr_client=self.mock_client,
            base_repo="/tmp/mock_repo"
        )

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.orig_env)

    async def test_delegator_defaults_to_agy(self):
        """WorkerDelegator.handoff defaults to 'agy' and executes directly in Pane 2 runner."""
        with patch("golem.execution.runner.RunnerExecutor.run_command") as mock_run:
            mock_handle = ExecutionHandle(
                status="completed",
                pane="runner",
                command="pytest tests/",
                returncode=0,
                stdout="All 1364 tests passed",
                dispatched_at=time.time()
            )
            mock_run.return_value = mock_handle

            res = await self.delegator.handoff(
                task="pytest tests/",
                cwd="/tmp/mock_repo"
            )

            self.assertEqual(res["status"], "completed")
            self.assertEqual(res["worker"], "agy")
            self.assertEqual(res["subagent"], "agy")
            self.assertEqual(res["pane"], "runner")
            self.assertIn("All 1364 tests passed", res["output"])

    async def test_delegator_worker_precedence_over_subagent(self):
        """worker parameter overrides subagent parameter in WorkerDelegator.handoff."""
        mock_wt_out = json.dumps({"result": {"worktree": {"path": "/tmp/wt/task-1"}}})
        self.mock_client.run_cli.side_effect = [
            (0, mock_wt_out, ""),              # worktree create
            (0, "started", ""),                 # agent start
            (0, "task accepted", ""),          # agent prompt
            (0, "idle", ""),                   # agent wait
            (0, "OpenCode refactor complete", ""), # agent read
        ]

        res = await self.delegator.handoff(
            worker="opencode",
            subagent="agy",
            task="Refactor normalizer",
            cwd="/tmp/mock_repo",
            isolate_branch=True
        )

        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["worker"], "opencode")
        self.assertTrue(res["branch"].endswith("-opencode"))

    async def test_delegator_routes_opencode_to_worktree(self):
        """worker='opencode' creates isolated worktree and sanitizes scratchpad output."""
        mock_wt_out = json.dumps({"result": {"worktree": {"path": "/tmp/wt/task-2"}}})
        raw_worker_text = (
            "<scratchpad>\nThinking about imports...\n</scratchpad>\n"
            "╭────────────────────────────────────╮\n"
            "│ OpenCode Output Summary            │\n"
            "╰────────────────────────────────────╯\n"
            "Applied 4 AST diffs successfully."
        )
        self.mock_client.run_cli.side_effect = [
            (0, mock_wt_out, ""),              # worktree create
            (0, "started", ""),                 # agent start
            (0, "task accepted", ""),          # agent prompt
            (0, "idle", ""),                   # agent wait
            (0, raw_worker_text, ""),          # agent read
        ]

        res = await self.delegator.handoff(
            worker="opencode",
            task="Refactor ast diffs",
            cwd="/tmp/mock_repo",
            isolate_branch=True
        )

        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["worker"], "opencode")
        # Verify sanitization stripped scratchpad and box drawing
        self.assertNotIn("<scratchpad>", res["output"])
        self.assertNotIn("╭", res["output"])
        self.assertIn("Applied 4 AST diffs successfully.", res["output"])

    async def test_delegator_routes_hermes_to_worker(self):
        """worker='hermes' executes hermes worker agent on target node."""
        self.mock_client.run_cli.side_effect = [
            (0, "started", ""),                 # agent start
            (0, "task accepted", ""),          # agent prompt
            (0, "idle", ""),                   # agent wait
            (0, "Docker containers all healthy", ""), # agent read
        ]

        res = await self.delegator.handoff(
            worker="hermes",
            task="Audit docker containers",
            cwd="/tmp/mock_repo",
            isolate_branch=False
        )

        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["worker"], "hermes")
        self.assertEqual(res["branch"], "main")
        self.assertIn("Docker containers all healthy", res["output"])

    def test_delegator_worker_validation_rejects_hazards(self):
        """validate_worker rejects path traversals, injection characters, and unknown names."""
        for dangerous in ["../../bin/sh", "/bin/bash", "../etc/passwd", "; rm -rf /", "`id`", "$USER"]:
            with self.assertRaises(ValueError):
                self.delegator.validate_worker(dangerous)

        with self.assertRaises(ValueError):
            self.delegator.validate_worker("unrecognized_bogus_worker")

    def test_delegator_supported_workers_catalog(self):
        """SUPPORTED_WORKERS contains agy, opencode, and hermes."""
        catalog = self.delegator.SUPPORTED_WORKERS
        self.assertIn("agy", catalog)
        self.assertIn("opencode", catalog)
        self.assertIn("hermes", catalog)


# ===========================================================================
# 5. Dual-Pane Isolation & 3.0s Yield Semantics Tests
# ===========================================================================
class TestDualPaneIsolationAndYieldSemantics(unittest.IsolatedAsyncioTestCase):
    """Verifies dual-pane isolation (Mouth in Pane 1, Hands in Pane 2) and 3.0s yield."""

    async def test_dual_pane_workspace_allocation(self):
        """DualPaneHarness provisions Pane 1 (Liaison) and Pane 2 (Runner)."""
        mock_client = AsyncMock()
        mock_client.run_cli.side_effect = [
            (0, json.dumps({"result": {"workspace_id": "ws-100"}}), ""), # workspace create
            (0, "liaison agent started", ""),                             # agent start (pane 1)
            (0, json.dumps({"result": {"pane_id": "p2"}}), ""),           # pane create (pane 2 runner)
        ]
        harness = DualPaneHarness(herdr_client=mock_client)

        liaison_pane, runner_pane = await harness.provision_dual_pane("test-session", "/tmp/cwd")
        self.assertEqual(liaison_pane, "test-session-liaison")
        self.assertEqual(runner_pane, "runner")

        ws = harness.get_workspace("test-session")
        self.assertIsNotNone(ws)
        self.assertEqual(ws["status"], "active")

    async def test_runner_executor_3s_yield_preservation(self):
        """Commands taking > 3.0s yield non-blocking ExecutionHandle(status='running')."""
        executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")
        try:
            with patch("asyncio.create_subprocess_exec") as mock_exec:
                mock_proc = AsyncMock()
                mock_proc.pid = 98765
                mock_proc.kill = MagicMock()

                async def slow_cmd():
                    await asyncio.sleep(3.5)
                    return b"Done\n", b""

                mock_proc.communicate.side_effect = slow_cmd
                mock_exec.return_value = mock_proc

                start_t = time.time()
                handle = await executor.run_command(pane="runner", command="sleep 10", yield_timeout=3.0)
                elapsed = time.time() - start_t

                self.assertEqual(handle.status, "running")
                self.assertEqual(handle.pid, 98765)
                self.assertLessEqual(elapsed, 3.2, "Yield must occur within ~3.0s ceiling")
        finally:
            await executor.async_close()

    async def test_agy_hand_execution_yield_nonblocking(self):
        """_execute_agy_hand yields non-blocking status='running' when command runs long."""
        delegator = WorkerDelegator(base_repo="/tmp")
        with patch("golem.execution.runner.RunnerExecutor.run_command") as mock_run:
            mock_run.return_value = ExecutionHandle(
                status="running",
                pane="runner",
                command="long_build.sh",
                pid=54321,
                dispatched_at=time.time(),
                message="Dispatched to Pane 2"
            )

            res = await delegator._execute_agy_hand(
                task="long_build.sh",
                cwd="/tmp",
                timeout_ms=120000
            )

            self.assertEqual(res["status"], "running")
            self.assertEqual(res["pid"], 54321)
            self.assertEqual(res["worker"], "agy")


# ===========================================================================
# 6. PTY Concurrency & Deadlock Stress Tests (M2-EXEC Invariants)
# ===========================================================================
class TestPTYConcurrencyAndDeadlockStress(unittest.IsolatedAsyncioTestCase):
    """
    Stress tests for headless PTY subshell execution:
    - Zero deadlocks under concurrent multi-pane subshells
    - Exit code fidelity without truncation
    - Clean process termination and buffer drain
    """

    async def test_concurrent_multi_pane_worker_runs_zero_deadlock(self):
        """Simulates simultaneous concurrent worker runs across multiple panes without deadlock."""
        num_workers = 10
        tasks = []

        async def simulate_worker_turn(worker_id: int):
            # Each worker runs a task through RunnerExecutor mock
            executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")
            with patch("asyncio.create_subprocess_exec") as mock_exec:
                mock_proc = AsyncMock()
                mock_proc.returncode = 0
                mock_proc.pid = 1000 + worker_id
                mock_proc.communicate.return_value = (f"Worker {worker_id} completed\n".encode(), b"")
                mock_exec.return_value = mock_proc

                handle = await executor.run_command(
                    pane=f"runner-{worker_id}",
                    command=f"echo worker-{worker_id}",
                    yield_timeout=3.0
                )
                return handle

        # Launch all 10 concurrent worker subshells simultaneously
        start_t = time.time()
        results = await asyncio.gather(*[simulate_worker_turn(i) for i in range(num_workers)])
        elapsed = time.time() - start_t

        self.assertEqual(len(results), num_workers)
        for i, handle in enumerate(results):
            self.assertEqual(handle.status, "completed")
            self.assertIn(f"Worker {i} completed", handle.stdout)
        self.assertLess(elapsed, 5.0, "Concurrent multi-pane runs must complete without deadlocking")

    async def test_exit_code_fidelity_untruncated(self):
        """Ensures non-zero exit codes (1, 2, 127) propagate accurately without truncation."""
        executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")
        for test_code in [1, 2, 127, 255]:
            with patch("asyncio.create_subprocess_exec") as mock_exec:
                mock_proc = AsyncMock()
                mock_proc.returncode = test_code
                mock_proc.pid = 4444
                mock_proc.communicate.return_value = (b"", f"Process failed with {test_code}\n".encode())
                mock_exec.return_value = mock_proc

                handle = await executor.run_command(
                    pane="runner",
                    command="faulty_command",
                    yield_timeout=3.0
                )

                self.assertEqual(handle.status, "error")
                self.assertEqual(handle.returncode, test_code)
                self.assertIn(f"Process failed with {test_code}", handle.stderr)

    async def test_clean_buffer_drain_upon_completion(self):
        """Verifies buffer sanitization cleanly strips ANSI, escapes, and whitespace."""
        noisy_output = (
            "\x1b[32m[SUCCESS]\x1b[0m Build finished.\n"
            "\x1b[1m\x1b[34mSummary:\x1b[0m 100% passed.\n"
            "⠋ Spinner debris...\n"
            "❯ echo finished\n"
        )
        clean = sanitize_worker_output(noisy_output)
        self.assertNotIn("\x1b[", clean)
        self.assertNotIn("⠋", clean)
        self.assertNotIn("❯ echo finished", clean)
        self.assertIn("Build finished.", clean)
        self.assertIn("100% passed.", clean)


# ===========================================================================
# 7. Ingress Dispatch & Tool Declaration Default Tests
# ===========================================================================
class TestIngressDispatchAgyDefault(unittest.IsolatedAsyncioTestCase):
    """Verifies IngressGateway dispatch_task tool declaration and execution defaults to AGY."""

    async def asyncSetUp(self):
        from golem.gateway.ingress import IngressGateway, DISPATCH_TASK_TOOL_DECLARATION
        from golem.gateway.router import SessionRouter
        from golem.agent.protocol import AgentEvent, TextDeltaEvent, FinishEvent

        self.DISPATCH_DECL = DISPATCH_TASK_TOOL_DECLARATION
        self.router = MagicMock()
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")

    def test_dispatch_task_tool_declaration_schema(self):
        """DISPATCH_TASK_TOOL_DECLARATION defines worker parameter and defaults target_node to localhost."""
        props = self.DISPATCH_DECL["parameters"]["properties"]
        self.assertIn("worker", props)
        self.assertIn("agy", props["worker"]["description"].lower())
        self.assertIn("target_node", props)
        self.assertIn("localhost", props["target_node"]["description"].lower())
        self.assertNotIn("defaults to 'hermes'", props["target_node"]["description"].lower())

    async def test_ingress_execute_backstage_task_defaults_to_agy(self):
        """_execute_backstage_task defaults to AGY and yields acknowledgment naming agy and node."""
        from golem.agent.protocol import TextDeltaEvent, FinishEvent

        session = MagicMock()
        session.session_id = "test-session-1"

        async def slow_events(*args, **kwargs):
            await asyncio.sleep(0.1)
            yield TextDeltaEvent(text_delta="Task complete")
            yield FinishEvent(status="SUCCESS", response="Task complete")

        client = MagicMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True})

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=slow_events):
            with patch.object(self.gateway, "_post_response", new_callable=AsyncMock) as mock_post:
                accumulator = await self.gateway._execute_backstage_task(
                    task_desc="Run benchmark",
                    target_node=None,  # should default to localhost
                    worker=None,       # should default to agy
                    session=session,
                    client=client,
                    channel="C123",
                    thread_ts="100.1",
                    yield_timeout=0.03,
                )

                self.assertTrue(mock_post.called)
                ack_msg = mock_post.call_args[0][3]
                self.assertIn("Dispatched backstage task to `localhost` via `agy`", ack_msg)
                self.assertNotIn("to `hermes`", ack_msg)

    async def test_ingress_execute_backstage_task_explicit_delegation(self):
        """_execute_backstage_task delegates to WorkerDelegator when worker='hermes' or 'opencode'."""
        session = MagicMock()
        session.session_id = "test-session-2"

        with patch("golem.gateway.ingress.WorkerDelegator") as MockDelegatorCls:
            mock_delegator = MagicMock()
            mock_delegator.handoff = AsyncMock(return_value={
                "status": "completed",
                "worker": "hermes",
                "output": "Homelab containers inspected.",
            })
            MockDelegatorCls.return_value = mock_delegator
            MockDelegatorCls.normalize_worker_name = lambda w: w

            res = await self.gateway._execute_backstage_task(
                task_desc="Check docker",
                target_node="homelab-ops",
                worker="hermes",
                session=session,
            )

            self.assertTrue(mock_delegator.handoff.called)
            call_kw = mock_delegator.handoff.call_args[1]
            self.assertEqual(call_kw["worker"], "hermes")
            self.assertEqual(res["worker"], "hermes")

    async def test_ingress_execute_backstage_task_fast_tool_start_node_label_defined(self):
        """_execute_backstage_task does not raise UnboundLocalError when tool_start arrives before yield timeout."""
        session = MagicMock()
        session.session_id = "test-session-fast"

        async def fast_tool_events(*args, **kwargs):
            yield {"event_type": "tool_start", "tool_name": "run_command"}
            yield {"event_type": "finish", "status": "SUCCESS", "response": "done"}

        client = MagicMock()

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=fast_tool_events):
            with patch("golem.ingress.set_assistant_status", new_callable=AsyncMock) as mock_status:
                accumulator = await self.gateway._execute_backstage_task(
                    task_desc="Quick tool execution",
                    target_node="personal-laptop",
                    worker="agy",
                    session=session,
                    client=client,
                    channel="C123",
                    thread_ts="100.2",
                    yield_timeout=3.0,
                )

                self.assertIsNotNone(accumulator)
                self.assertEqual(accumulator.turn_status, "SUCCESS")
                mock_status.assert_any_call(client, "C123", "100.2", "Running run_command on personal-laptop...")


if __name__ == "__main__":
    unittest.main()
