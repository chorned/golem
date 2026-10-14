#!/usr/bin/env python3
"""
tests/test_execution_m4.py
Unit test suite for Milestone 4 (Dual-Pane Execution Harness & Herdr Fleet Mesh):
- DualPaneHarness provisioning, pane ID tracking, and teardown
- RunnerExecutor fast execution vs yield timeout (status="running", non-blocking PID yield)
- RunnerBufferObserver interactive blocker extraction (OAuth URLs & prompts, TC-03)
- WorkerDelegator branch/worktree isolation and output sanitization (TC-04)
- FleetMesh 5-node topology resolution, per-pane concurrency locking, and wrap_windows_command

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import time
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.execution import (
    DualPaneHarness,
    ExecutionHandle,
    RunnerExecutor,
    RunnerBufferObserver,
    WorkerDelegator,
    sanitize_worker_output,
    FleetMesh,
    TOPOLOGY_SPECS,
    wrap_windows_command,
)


class TestDualPaneHarness(unittest.IsolatedAsyncioTestCase):
    """Verifies DualPaneHarness workspace provisioning, pane tracking, and teardown."""

    def setUp(self):
        self.mock_client = AsyncMock()
        self.harness = DualPaneHarness(herdr_client=self.mock_client, default_cwd="/tmp/project")

    async def test_provision_dual_pane_spawns_both_panes(self):
        """Verifies provision_dual_pane creates workspace, starts liaison, and creates runner pane."""
        mock_ws_output = json.dumps({
            "result": {
                "workspace": {"workspace_id": "ws-m4-1"},
                "root_pane": {"pane_id": "ws-m4-1:p1"}
            }
        })
        mock_runner_output = json.dumps({
            "result": {
                "pane": {"pane_id": "ws-m4-1:p2"}
            }
        })

        self.mock_client.run_cli.side_effect = [
            (0, mock_ws_output, ""),       # workspace create
            (0, "started liaison", ""),   # agent start fm-sess-liaison
            (0, mock_runner_output, ""),  # pane create --label runner
        ]

        liaison_pane, runner_pane = await self.harness.provision_dual_pane(
            session_id="fm-sess",
            cwd="/tmp/project"
        )

        self.assertEqual(liaison_pane, "fm-sess-liaison")
        self.assertEqual(runner_pane, "runner")

        # Verify CLI calls
        calls = self.mock_client.run_cli.call_args_list
        self.assertEqual(len(calls), 3)

        # 1. workspace create
        self.assertEqual(calls[0][0][0][:2], ["workspace", "create"])
        self.assertIn("--label", calls[0][0][0])
        self.assertIn("fm-sess", calls[0][0][0])
        self.assertIn("--cwd", calls[0][0][0])

        # 2. agent start (Pane 1)
        self.assertEqual(calls[1][0][0], ["agent", "start", "fm-sess-liaison", "--kind", "agy", "--pane", "ws-m4-1:p1"])

        # 3. pane create (Pane 2)
        self.assertEqual(calls[2][0][0], ["pane", "create", "--workspace", "fm-sess", "--label", "runner"])

        # Verify tracking state
        ws = self.harness.get_workspace("fm-sess")
        self.assertIsNotNone(ws)
        self.assertEqual(ws["status"], "active")
        self.assertEqual(ws["liaison_agent"], "fm-sess-liaison")
        self.assertEqual(ws["runner_pane"], "runner")
        self.assertIn("fm-sess", self.harness.active_workspaces)

    async def test_create_workspace_with_channel_metadata(self):
        """Verifies create_workspace includes channel and thread_ts metadata."""
        self.mock_client.run_cli.side_effect = [
            (0, json.dumps({"result": {"workspace": {"workspace_id": "ws-100"}}}), ""),
            (0, "ok", ""),
            (0, json.dumps({"result": {"pane": {"pane_id": "p2"}}}), ""),
        ]

        meta = await self.harness.create_workspace(
            session_name="sess-2",
            cwd="/tmp/sess2",
            channel="C12345",
            thread_ts="1790000000.0001"
        )

        self.assertEqual(meta["workspace_id"], "ws-100")
        self.assertEqual(meta["session_name"], "sess-2")
        self.assertEqual(meta["channel"], "C12345")
        self.assertEqual(meta["thread_ts"], "1790000000.0001")
        self.assertEqual(meta["runner_pane"], "runner")
        self.assertEqual(meta["status"], "active")

    async def test_teardown_and_close_workspace(self):
        """Verifies workspace teardown closes the workspace via Herdr and updates tracking."""
        self.mock_client.run_cli.side_effect = [
            (0, json.dumps({"result": {"workspace": {"workspace_id": "ws-close"}}}), ""),
            (0, "ok", ""),
            (0, "ok", ""),
            (0, "closed", ""),  # teardown
        ]

        await self.harness.provision_dual_pane("fm-close", "/tmp/close")
        self.assertIn("fm-close", self.harness.active_workspaces)

        await self.harness.teardown("fm-close")
        close_call = self.mock_client.run_cli.call_args[0][0]
        self.assertEqual(close_call, ["workspace", "close", "ws-close"])
        self.assertNotIn("fm-close", self.harness.active_workspaces)

        ws = self.harness.get_workspace("fm-close")
        self.assertEqual(ws["status"], "closed")

    async def test_close_workspace_by_workspace_id_and_idempotency(self):
        """Verifies close_workspace updates state by workspace_id and teardown is idempotent."""
        self.mock_client.run_cli.side_effect = [
            (0, json.dumps({"result": {"workspace": {"workspace_id": "ws-100"}}}), ""),
            (0, "ok", ""),
            (0, json.dumps({"result": {"pane": {"pane_id": "p2"}}}), ""),
            (0, "closed", ""),
        ]

        await self.harness.provision_dual_pane("sess-1", "/tmp")
        await self.harness.close_workspace("ws-100")
        ws = self.harness.get_workspace("sess-1")
        self.assertIsNotNone(ws)
        self.assertEqual(ws["status"], "closed")
        self.assertEqual(len(self.harness.active_workspaces), 0)

        # Call teardown again on already-closed workspace -> must be an idempotent no-op
        await self.harness.teardown("sess-1")
        self.assertEqual(self.mock_client.run_cli.call_count, 4)


class TestRunnerExecutor(unittest.IsolatedAsyncioTestCase):
    """Verifies RunnerExecutor fast completion vs yield timeout (TC-03 non-blocking invariant)."""

    def setUp(self):
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")

    async def test_fast_command_completion_within_yield_timeout(self):
        """Fast commands (< yield_timeout) return status='completed' with returncode and output."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.pid = 11111
            mock_proc.communicate.return_value = (b"All 159 tests passed\n", b"")
            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="pytest",
                yield_timeout=3.0
            )

            self.assertIsInstance(handle, ExecutionHandle)
            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertEqual(handle.stdout, "All 159 tests passed")
            self.assertEqual(handle.stderr, "")
            self.assertEqual(handle.pane, "runner")
            self.assertEqual(handle.command, "pytest")
            # Dict compatibility access
            self.assertEqual(handle["status"], "completed")
            self.assertEqual(handle.get("returncode"), 0)
            self.assertIn("status", handle)

    async def test_fast_command_failure_within_yield_timeout(self):
        """Failing fast commands return status='error' with exit code and stderr."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 2
            mock_proc.pid = 22222
            mock_proc.communicate.return_value = (b"", b"Error: file not found\n")
            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="cat missing.txt",
                yield_timeout=3.0
            )

            self.assertEqual(handle.status, "error")
            self.assertEqual(handle.returncode, 2)
            self.assertEqual(handle.stderr, "Error: file not found")

    async def test_long_running_command_yields_running_handle_without_blocking(self):
        """
        Long-running commands exceeding yield_timeout must yield status='running',
        PID, and a non-blocking notification immediately.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 48210
            # communicate raises TimeoutError when yield_timeout expires
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 48210}}).encode(),
                b""
            )

            # First call for pane run, second call for pane process-info
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            start_t = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="npm run dev",
                yield_timeout=0.05
            )
            elapsed = time.time() - start_t

            # Verify non-blocking: completed in well under 1 second
            self.assertLess(elapsed, 0.5)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pane, "runner")
            self.assertEqual(handle.command, "npm run dev")
            self.assertEqual(handle.pid, 48210)
            self.assertIn("Command running in Pane 2. Liaison loop remains active.", handle.message)

    async def test_empty_command_fails_gracefully(self):
        """Empty command string returns error handle."""
        handle = await self.executor.run_command(pane="runner", command="")
        self.assertEqual(handle.status, "error")
        self.assertIn("required", handle.message.lower())

    async def test_long_running_command_foreground_polling_yields_running(self):
        """
        Simulates live Herdr execution: 'herdr pane run' exits immediately in 12ms,
        but foreground process continues running past yield_timeout -> yields status='running'.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            # 1. pane run exits immediately with returncode 0
            mock_run_proc = AsyncMock()
            mock_run_proc.returncode = 0
            mock_run_proc.communicate.return_value = (b"", b"")

            # 2. pane process-info reports active sleep command (pid 91520)
            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({
                    "result": {
                        "process_info": {
                            "shell_pid": 80000,
                            "foreground_process_group_id": 91520,
                            "foreground_processes": [{"pid": 91520, "name": "sleep"}]
                        }
                    }
                }).encode(),
                b""
            )

            mock_exec.side_effect = [mock_run_proc, mock_info_proc, mock_info_proc, mock_info_proc]

            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 10",
                yield_timeout=0.08
            )

            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 91520)
            self.assertEqual(handle.pane, "runner")
            self.assertEqual(handle.command, "sleep 10")
            self.assertIn("Command running in Pane 2. Liaison loop remains active.", handle.message)

    async def test_foreground_polling_completes_within_yield_timeout(self):
        """
        Simulates fast command in pane where process finishes during polling,
        reads buffer slice to populate stdout, and returns status='completed'.
        """
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            # 1. pane run exits with 0
            mock_run_proc = AsyncMock()
            mock_run_proc.returncode = 0
            mock_run_proc.communicate.return_value = (b"", b"")

            # 2. 1st process-info check: process active (pid 77777)
            mock_info_proc1 = AsyncMock()
            mock_info_proc1.returncode = 0
            mock_info_proc1.communicate.return_value = (
                json.dumps({"result": {"pid": 77777}}).encode(),
                b""
            )

            # 3. 2nd process-info check: process completed (only shell running)
            mock_info_proc2 = AsyncMock()
            mock_info_proc2.returncode = 0
            mock_info_proc2.communicate.return_value = (
                json.dumps({
                    "result": {
                        "process_info": {
                            "shell_pid": 80000,
                            "foreground_process_group_id": 80000,
                            "foreground_processes": [{"pid": 80000, "name": "zsh"}]
                        }
                    }
                }).encode(),
                b""
            )

            # 4. read buffer
            mock_read_proc = AsyncMock()
            mock_read_proc.returncode = 0
            mock_read_proc.communicate.return_value = (b"Command output from pane buffer", b"")

            mock_exec.side_effect = [mock_run_proc, mock_info_proc1, mock_info_proc2, mock_read_proc]

            handle = await self.executor.run_command(
                pane="runner",
                command="echo hello",
                yield_timeout=0.5
            )

            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertEqual(handle.stdout, "Command output from pane buffer")
            self.assertEqual(handle.message, "Command completed within yield timeout.")

    async def test_resolved_pane_timeout_yields_running_handle(self):
        """Reviewer 2 Finding 1: Command exceeding yield_timeout on resolved pane yields running handle."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            p1 = AsyncMock(returncode=1, communicate=AsyncMock(return_value=(b"", b"error: pane_not_found")))
            p2 = AsyncMock(returncode=0, communicate=AsyncMock(return_value=(json.dumps({"panes": [{"label": "runner", "pane_id": "w1:p2"}]}).encode(), b"")))
            p3 = AsyncMock(pid=99999, communicate=AsyncMock(side_effect=asyncio.TimeoutError()))
            p4 = AsyncMock(returncode=0, communicate=AsyncMock(return_value=(json.dumps({"result": {"pid": 99999}}).encode(), b"")))
            mock_exec.side_effect = [p1, p2, p3, p4]

            handle = await self.executor.run_command(pane="runner", command="sleep 10", yield_timeout=0.5)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 99999)
            self.assertEqual(handle.pane, "w1:p2")
            self.assertIn("Command running in Pane 2. Liaison loop remains active.", handle.message)

    async def test_live_cli_schema_without_result_wrapper(self):
        """Reviewer 2 Finding 2: Live Herdr CLI schema {'panes': [...]} resolves correctly."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            p1 = AsyncMock(returncode=1, communicate=AsyncMock(return_value=(b"", b"error: pane_not_found")))
            p2 = AsyncMock(returncode=0, communicate=AsyncMock(return_value=(json.dumps({"panes": [{"label": "runner", "pane_id": "w1:p2"}]}).encode(), b"")))
            p3 = AsyncMock(returncode=0, pid=33333, communicate=AsyncMock(return_value=(b"Success", b"")))
            p4 = AsyncMock(returncode=0, communicate=AsyncMock(return_value=(json.dumps({
                "result": {"process_info": {"shell_pid": 100, "foreground_process_group_id": 100, "foreground_processes": [{"pid": 100, "name": "zsh"}]}}
            }).encode(), b"")))
            mock_exec.side_effect = [p1, p2, p3, p4]

            handle = await self.executor.run_command(pane="runner", command="ls -la", yield_timeout=1.0)
            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.pane, "w1:p2")


class TestRunnerBufferObserver(unittest.IsolatedAsyncioTestCase):
    """Verifies buffer observation and interactive blocker extraction (TC-03)."""

    def setUp(self):
        self.observer = RunnerBufferObserver(herdr_bin="/usr/local/bin/herdr")

    def test_extract_oauth_callback_urls(self):
        """TC-03: Detects OAuth localhost/127.0.0.1 callback URLs in runner buffer."""
        samples = [
            (
                "Rclone OAuth listener active at http://127.0.0.1:53682/auth\nWaiting for code...",
                "http://127.0.0.1:53682/auth"
            ),
            (
                "Opening browser to http://localhost:8080/callback?state=xyz for authentication.",
                "http://localhost:8080/callback?state=xyz"
            ),
            (
                "Listening on http://0.0.0.0:4000/oauth/token for credentials.",
                "http://0.0.0.0:4000/oauth/token"
            ),
        ]
        for buf, expected_url in samples:
            blocker = self.observer.extract_interactive_blockers(buf)
            self.assertIsNotNone(blocker, f"Should detect OAuth in: {buf}")
            self.assertEqual(blocker["type"], "oauth")
            self.assertEqual(blocker["url"], expected_url)

    def test_extract_interactive_prompts(self):
        """TC-03: Detects interactive confirmation prompts and password requests."""
        samples = [
            "Do you want to continue? [y/N]",
            "Apply migrations to production database? (y/n)",
            "Enter password: ",
            "Enter passphrase:",
            "Delete 15 files? [yes/no]",
        ]
        for buf in samples:
            blocker = self.observer.extract_interactive_blockers(buf)
            self.assertIsNotNone(blocker, f"Should detect prompt in: {buf}")
            self.assertEqual(blocker["type"], "prompt")
            self.assertIsNone(blocker["url"])

    def test_extract_oauth_callback_urls_with_ansi_and_brackets(self):
        """TC-03: OAuth extraction strips ANSI escape sequences and trailing bracket."""
        samples = [
            (
                "\x1b[32mAuth at \x1b[4mhttp://127.0.0.1:53682/auth\x1b[0m now\x1b[0m",
                "http://127.0.0.1:53682/auth"
            ),
            (
                "Trailing bracket: [http://127.0.0.1:53682/auth]",
                "http://127.0.0.1:53682/auth"
            ),
            (
                "Callback: (http://localhost:8080/callback?code=123)",
                "http://localhost:8080/callback?code=123"
            ),
        ]
        for buf, expected_url in samples:
            blocker = self.observer.extract_interactive_blockers(buf)
            self.assertIsNotNone(blocker, f"Should detect OAuth in: {buf}")
            self.assertEqual(blocker["type"], "oauth")
            self.assertEqual(blocker["url"], expected_url)

    def test_extract_interactive_prompts_extended(self):
        """TC-03: Detects (yes/no), [yes/no], and sudo/credential prompts."""
        samples = [
            "Continue? (yes/no)",
            "Delete 15 files? [yes/no]",
            "[sudo] password for hermes: ",
            "Password for 'https://github.com': ",
            "Enter passphrase for key '/id_rsa': ",
        ]
        for buf in samples:
            blocker = self.observer.extract_interactive_blockers(buf)
            self.assertIsNotNone(blocker, f"Should detect prompt in: {buf}")
            self.assertEqual(blocker["type"], "prompt")

    def test_no_blocker_in_clean_output(self):
        """Plain command outputs without interactive listeners return None."""
        clean_bufs = [
            "Building target...\nCompiled 14 modules in 1.2s.\nAll tests passed.",
            "total 16\ndrwxr-xr-x 4 hermes staff 128 Sep 29 22:00 src",
            "",
            "   \n\n  "
        ]
        for buf in clean_bufs:
            blocker = self.observer.extract_interactive_blockers(buf)
            self.assertIsNone(blocker)

    async def test_read_buffer_falls_back_to_pane_read(self):
        """Verifies read_buffer falls back to pane read if agent read fails."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            # 1. agent read fails (code 1)
            mock_agent_proc = AsyncMock()
            mock_agent_proc.returncode = 1
            mock_agent_proc.communicate.return_value = (b"", b"agent_not_found\n")

            # 2. pane read succeeds (code 0)
            mock_pane_proc = AsyncMock()
            mock_pane_proc.returncode = 0
            mock_pane_proc.communicate.return_value = (b"Settled runner buffer output\n", b"")

            mock_exec.side_effect = [mock_agent_proc, mock_pane_proc]

            buf = await self.observer.read_buffer(pane="runner", lines=30)
            self.assertEqual(buf, "Settled runner buffer output")


class TestWorkerDelegator(unittest.IsolatedAsyncioTestCase):
    """Verifies isolated worker delegation and output sanitization (TC-04)."""

    def setUp(self):
        self.mock_client = AsyncMock()
        self.delegator = WorkerDelegator(
            herdr_client=self.mock_client,
            base_repo="/Users/hermes/projects/golem"
        )

    async def test_worker_delegation_branch_isolation_tc04(self):
        """TC-04: Delegates task on dedicated isolated branch and retrieves output."""
        mock_wt_out = json.dumps({
            "result": {
                "worktree": {"path": "/tmp/wt/task-123"},
                "workspace": {"workspace_id": "ws-wt-1"}
            }
        })
        self.mock_client.run_cli.side_effect = [
            (0, mock_wt_out, ""),              # worktree create
            (0, "started", ""),                 # agent start
            (0, "task accepted", ""),          # agent prompt
            (0, "idle", ""),                   # agent wait
            (0, "Clean summary of changes", ""), # agent read
        ]

        res = await self.delegator.handoff(
            subagent="opencode",
            task="Refactor auth module",
            cwd="/Users/hermes/projects/golem",
            isolate_branch=True
        )

        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["subagent"], "opencode")
        self.assertTrue(res["branch"].startswith("task-"))
        self.assertTrue(res["branch"].endswith("-opencode"))
        self.assertEqual(res["cwd"], "/tmp/wt/task-123")
        self.assertIn("Clean summary of changes", res["output"])

        # Check worktree create called with branch
        wt_call = self.mock_client.run_cli.call_args_list[0][0][0]
        self.assertEqual(wt_call[:2], ["worktree", "create"])
        self.assertIn("--branch", wt_call)

    async def test_worker_delegation_worktree_failure_returns_error(self):
        """Reviewer 2 Finding 6: If isolate_branch=True and worktree fails, returns error."""
        self.mock_client.run_cli.side_effect = [
            (1, "", "fatal: not a valid git repository"),
        ]
        res = await self.delegator.handoff(
            subagent="opencode",
            task="Task on broken repo",
            isolate_branch=True
        )
        self.assertEqual(res["status"], "error")
        self.assertIn("Worktree isolation failed", res["error"])
        # Ensure no subsequent worker commands were run
        self.assertEqual(len(self.mock_client.run_cli.call_args_list), 1)

    def test_worker_output_sanitization_tc04(self):
        """TC-04: Strips internal scratchpads, tool crumbs, VT100 boxes, prompt echoes, and ANSI."""
        raw_output = (
            "<scratchpad>\n"
            "Checking imports in src/auth.py...\n"
            "Found 2 deprecated endpoints.\n"
            "</scratchpad>\n"
            "[scratchpad] calculating AST diff\n"
            "╭────────────────────────────────────╮\n"
            "│ Refactored Authentication Module   │\n"
            "╰────────────────────────────────────╯\n"
            "3...)\n"
            "(ctrl+o to expand)\n"
            "❯ git diff --stat\n"
            "\x1b[32mSuccessfully migrated 4 routes.\x1b[0m\n"
            "All unit tests pass 100% green."
        )

        cleaned = sanitize_worker_output(raw_output)

        # Assert scratchpads stripped
        self.assertNotIn("<scratchpad>", cleaned)
        self.assertNotIn("Checking imports in src/auth.py", cleaned)
        self.assertNotIn("[scratchpad]", cleaned)

        # Assert VT100 box characters stripped
        self.assertNotIn("╭", cleaned)
        self.assertNotIn("│", cleaned)
        self.assertNotIn("╰", cleaned)
        self.assertNotIn("─", cleaned)

        # Assert TUI crumbs stripped
        self.assertNotIn("3...)", cleaned)
        self.assertNotIn("(ctrl+o to expand)", cleaned)

        # Assert prompt echo stripped
        self.assertNotIn("❯ git diff", cleaned)

        # Assert ANSI stripped
        self.assertNotIn("\x1b[32m", cleaned)
        self.assertNotIn("\x1b[0m", cleaned)

        # Assert substantive content preserved
        self.assertIn("Refactored Authentication Module", cleaned)
        self.assertIn("Successfully migrated 4 routes.", cleaned)
        self.assertIn("All unit tests pass 100% green.", cleaned)


class TestFleetMesh(unittest.IsolatedAsyncioTestCase):
    """Verifies FleetMesh 5-node topology, per-pane locking, and wrap_windows_command."""

    def setUp(self):
        self.mesh = FleetMesh()

    def test_5_node_topology_specs(self):
        """Verifies all 5 required fleet nodes are properly registered."""
        nodes = self.mesh.list_nodes()
        self.assertEqual(len(nodes), 5)
        self.assertIn("fleet-control", nodes)
        self.assertIn("homelab-ops", nodes)
        self.assertIn("compute-grid", nodes)
        self.assertIn("dev-grid", nodes)
        self.assertIn("cloud-ops", nodes)

        self.assertEqual(self.mesh.topology["fleet-control"]["target_machine"], "local")
        self.assertEqual(self.mesh.topology["homelab-ops"]["agent_name"], "hermes-worker")
        self.assertEqual(self.mesh.topology["compute-grid"]["target_machine"], "desktop-pc")
        self.assertEqual(self.mesh.topology["dev-grid"]["target_machine"], "personal-laptop")
        self.assertEqual(self.mesh.topology["cloud-ops"]["target_machine"], "oci-machine")

    def test_node_resolution_and_aliases(self):
        """Verifies node resolution by name, alias, and target machine."""
        self.assertEqual(self.mesh.resolve_node("fleet-control")["label"], "fleet-control")
        self.assertEqual(self.mesh.resolve_node("hermes")["label"], "homelab-ops")
        self.assertEqual(self.mesh.resolve_node("desktop-pc")["label"], "compute-grid")
        self.assertEqual(self.mesh.resolve_node("personal-laptop")["label"], "dev-grid")
        self.assertEqual(self.mesh.resolve_node("oci-machine")["label"], "cloud-ops")
        self.assertEqual(self.mesh.resolve_node("gpu")["label"], "compute-grid")

        with self.assertRaises(KeyError):
            self.mesh.resolve_node("non-existent-node")

    async def test_per_pane_locking_concurrency(self):
        """Verifies per-pane locks enforce sequential execution on the same pane."""
        execution_order = []

        async def worker(task_id: int, delay: float, pane_id: str):
            async def work():
                execution_order.append(f"start-{task_id}")
                await asyncio.sleep(delay)
                execution_order.append(f"end-{task_id}")
                return task_id
            return await self.mesh.run_locked(pane_id, work)

        # Run two tasks on the SAME pane concurrently
        t1 = asyncio.create_task(worker(1, 0.05, "w1:p1"))
        t2 = asyncio.create_task(worker(2, 0.01, "w1:p1"))
        await asyncio.gather(t1, t2)

        # Task 2 cannot start until Task 1 finishes on the same pane!
        self.assertEqual(execution_order, ["start-1", "end-1", "start-2", "end-2"])

        # Different panes can run concurrently
        execution_order.clear()
        p1 = asyncio.create_task(worker(1, 0.05, "w1:p1"))
        p2 = asyncio.create_task(worker(2, 0.01, "w2:p1"))
        await asyncio.gather(p1, p2)

        # Task 2 on w2:p1 finishes before Task 1 on w1:p1 finishes
        self.assertEqual(execution_order[0], "start-1")
        self.assertIn("start-2", execution_order)
        self.assertEqual(execution_order[2], "end-2")
        self.assertEqual(execution_order[3], "end-1")

    def test_wrap_windows_command(self):
        """Verifies wrap_windows_command converts POSIX idioms to Windows cmd.exe /c."""
        # POSIX &&
        wrapped = wrap_windows_command("cd C:\\repo && git status")
        self.assertEqual(wrapped, 'cmd.exe /c "cd C:\\repo && git status"')

        # POSIX /dev/null redirect
        wrapped2 = wrap_windows_command("npm test 2>/dev/null")
        self.assertEqual(wrapped2, 'cmd.exe /c "npm test 2>nul"')

        # POSIX ./ runner
        wrapped3 = wrap_windows_command("./build.bat")
        self.assertEqual(wrapped3, 'cmd.exe /c ".\\build.bat"')

        # Existing PowerShell command preserved as-is
        ps = "powershell.exe -Command Get-Service"
        self.assertEqual(wrap_windows_command(ps), ps)

        # Existing cmd command preserved as-is
        cmd_run = 'cmd.exe /c "dir /w"'
        self.assertEqual(wrap_windows_command(cmd_run), cmd_run)

    def test_wrap_windows_command_edge_cases(self):
        """Verifies wrap_windows_command edge cases: pipes, subshells, empty strings."""
        self.assertEqual(wrap_windows_command(""), "")
        self.assertEqual(wrap_windows_command("   "), "")

        # Pipe ||
        wrapped = wrap_windows_command("npm test || exit 1")
        self.assertEqual(wrapped, 'cmd.exe /c "npm test || exit 1"')

        # Subshell $(cmd)
        wrapped_sub = wrap_windows_command("echo $(date)")
        self.assertEqual(wrapped_sub, 'cmd.exe /c "echo $(date)"')

    async def test_mesh_lock_inspection_and_sync_runner(self):
        """Verifies get_pane_io_lock, get_all_locks, has_active_locks, and run_locked sync callback."""
        exec_lock = self.mesh.get_pane_lock("w1:p1")
        self.assertIsInstance(exec_lock, asyncio.Lock)

        io_lock = self.mesh.get_pane_io_lock("w1:p1")
        self.assertIsInstance(io_lock, asyncio.Lock)

        all_locks = self.mesh.get_all_locks()
        self.assertIn("w1:p1", all_locks)
        self.assertEqual(self.mesh.has_active_locks(), [])

        # Synchronous function inside run_locked
        def sync_calc(a, b):
            return a + b

        val = await self.mesh.run_locked("w1:p1", sync_calc, 10, 20)
        self.assertEqual(val, 30)

    def test_fleet_mesh_lock_alias_resolution(self):
        """Reviewer 2 Finding 5: Locking with node alias or canonical ID resolves to same lock."""
        lock1 = self.mesh.get_pane_lock("dev-grid")
        lock2 = self.mesh.get_pane_lock("w4:p1")
        lock3 = self.mesh.get_pane_lock("personal-laptop")
        self.assertIs(lock1, lock2)
        self.assertIs(lock2, lock3)


class TestDelegatorExtra(unittest.IsolatedAsyncioTestCase):
    """Extra coverage for WorkerDelegator without branch isolation and edge sanitization."""

    async def test_worker_delegation_without_isolation(self):
        mock_client = AsyncMock()
        mock_client.run_cli.side_effect = [
            (0, "started", ""),
            (0, "accepted", ""),
            (0, "done", ""),
            (0, "Task finished without branch isolation", "")
        ]
        delegator = WorkerDelegator(herdr_client=mock_client)
        res = await delegator.handoff(
            subagent="hermes",
            task="Check disk usage",
            isolate_branch=False
        )
        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["branch"], "main")
        self.assertIn("Task finished without branch isolation", res["output"])

    def test_sanitize_empty_and_whitespace(self):
        self.assertEqual(sanitize_worker_output(""), "")
        self.assertEqual(sanitize_worker_output("   \n\n  "), "")


if __name__ == "__main__":
    unittest.main()
