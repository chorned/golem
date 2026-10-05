#!/usr/bin/env python3
"""
tests/test_execution_m4_empirical_challenge.py
Adversarial empirical verification suite for Milestone 4 (Dual-Pane Execution Harness & Herdr Fleet Mesh).
Challenger 2 verification covering:
  - TC-04: Worker Delegation & Sanitization
    * WorkerDelegator.handoff() worktree isolation and branch naming conventions (task-<ts>-<subagent>)
    * WorkerDelegator.handoff() fallback to main when isolate_branch=False
    * sanitize_worker_output() adversarial resilience across scratchpad tags, VT100 boxes, prompt echoes, TUI debris, and ANSI codes
  - Fleet Mesh Per-Pane Locking Concurrency:
    * 50 concurrent command dispatches to the exact same pane ID
    * Strict serialization by the pane lock with 0 interleaving and 0 race conditions
    * Parallel execution across multiple distinct panes
    * Exception release resilience preventing deadlock
  - DualPaneHarness Lifecycle & Blocker Resilience:
    * Workspace provisioning, liaison/runner pane assignment, and tracking
    * Teardown idempotency and active workspace filtering
"""

import os
import sys
import json
import time
import random
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

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
    wrap_windows_command,
)


class TestWorkerDelegationEmpiricalTC04(unittest.IsolatedAsyncioTestCase):
    """Empirical verification of WorkerDelegator worktree isolation and TC-04 branch naming."""

    def setUp(self):
        self.mock_client = AsyncMock()
        self.delegator = WorkerDelegator(
            herdr_client=self.mock_client,
            base_repo="/Users/hermes/projects/first-mate"
        )

    async def test_handoff_branch_isolation_and_naming(self):
        """TC-04: Verifies branch naming follows task-<ts>-<subagent> and worktree is created."""
        mock_wt_resp = json.dumps({
            "result": {
                "worktree": {"path": "/Users/hermes/projects/first-mate-wt-123"},
                "workspace": {"workspace_id": "ws-wt-123"}
            }
        })
        self.mock_client.run_cli.side_effect = [
            (0, mock_wt_resp, ""),             # worktree create
            (0, "started worker", ""),          # agent start
            (0, "prompt accepted", ""),         # agent prompt
            (0, "idle", ""),                    # agent wait
            (0, "<scratchpad>AST analysis</scratchpad>\x1b[32mRefactored cleanly.\x1b[0m", "") # agent read
        ]

        start_time = int(time.time())
        res = await self.delegator.handoff(
            subagent="opencode",
            task="Refactor telemetry hooks",
            cwd="/Users/hermes/projects/first-mate",
            isolate_branch=True
        )

        # 1. Status & Subagent
        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["subagent"], "opencode")

        # 2. Branch naming convention: task-<ts>-<subagent>
        branch = res["branch"]
        self.assertTrue(branch.startswith("task-"))
        self.assertTrue(branch.endswith("-opencode"))
        parts = branch.split("-")
        self.assertEqual(len(parts), 3)
        branch_ts = int(parts[1])
        self.assertGreaterEqual(branch_ts, start_time - 1)
        self.assertLessEqual(branch_ts, start_time + 5)

        # 3. Worktree path tracking
        self.assertEqual(res["cwd"], "/Users/hermes/projects/first-mate-wt-123")
        self.assertEqual(res["workspace_id"], "ws-wt-123")

        # 4. Sanitized output delivered to caller
        self.assertEqual(res["output"], "Refactored cleanly.")
        self.assertNotIn("scratchpad", res["output"])
        self.assertNotIn("\x1b[32m", res["output"])

        # 5. Raw output preserved in raw_output
        self.assertIn("<scratchpad>AST analysis</scratchpad>", res["raw_output"])

    async def test_handoff_without_branch_isolation(self):
        """Verifies isolate_branch=False uses main branch and skips worktree creation."""
        self.mock_client.run_cli.side_effect = [
            (0, "started", ""),
            (0, "accepted", ""),
            (0, "done", ""),
            (0, "Operation complete.", "")
        ]

        res = await self.delegator.handoff(
            subagent="hermes",
            task="Inspect system disk",
            cwd="/Users/hermes",
            isolate_branch=False
        )

        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["branch"], "main")
        self.assertEqual(res["cwd"], "/Users/hermes")
        self.assertIsNone(res["workspace_id"])
        self.assertIn("Operation complete.", res["output"])

        # Verify no 'worktree' CLI commands executed
        called_commands = [call[0][0][0] for call in self.mock_client.run_cli.call_args_list]
        self.assertNotIn("worktree", called_commands)


class TestSanitizeWorkerOutputAdversarial(unittest.TestCase):
    """Adversarial stress-testing of sanitize_worker_output with diverse chrome and debris."""

    def test_scratchpad_tags_stripping(self):
        """Verifies multi-line, case-insensitive, and diverse bracketed scratchpad tags are stripped."""
        samples = [
            ("<scratchpad>secret thoughts</scratchpad>\nPublic output", "Public output"),
            ("<SCRATCHPAD>\nStep 1: Inspect\nStep 2: Mutate\n</SCRATCHPAD>\nFinal result", "Final result"),
            ("[scratchpad] calculating AST diff\nValid line", "Valid line"),
            ("[thought] internal planning\nValid line", "Valid line"),
            ("[internal trace] stack trace info\nValid line", "Valid line"),
            ("scratchpad: note to self\nValid line", "Valid line"),
        ]
        for raw, expected in samples:
            cleaned = sanitize_worker_output(raw)
            self.assertEqual(cleaned, expected)
            self.assertNotIn("scratchpad", cleaned.lower())
            self.assertNotIn("internal trace", cleaned.lower())

    def test_vt100_box_drawing_and_curses_stripping(self):
        """Verifies VT100 box characters are stripped while retaining substantive text."""
        raw_box = (
            "╭────────────────────────────────────╮\n"
            "│ Refactored Authentication Module   │\n"
            "╰────────────────────────────────────╯\n"
            "┌─────────┬─────────┐\n"
            "│ Route A │ Route B │\n"
            "├─────────┼─────────┤\n"
            "│ 200 OK  │ 200 OK  │\n"
            "└─────────┴─────────┘"
        )
        cleaned = sanitize_worker_output(raw_box)
        self.assertNotIn("╭", cleaned)
        self.assertNotIn("│", cleaned)
        self.assertNotIn("╰", cleaned)
        self.assertNotIn("┌", cleaned)
        self.assertNotIn("├", cleaned)
        self.assertNotIn("└", cleaned)
        self.assertIn("Refactored Authentication Module", cleaned)
        self.assertIn("Route A", cleaned)
        self.assertIn("200 OK", cleaned)

    def test_prompt_echoes_and_debris_stripping(self):
        """Verifies Starship prompts, user@host prompts, spinners, and crumbs are stripped."""
        raw_debris = (
            "3...)\n"
            "mate/supervisor.py...)\n"
            "(ctrl+o to expand)\n"
            "expand)\n"
            "⠋ Analyzing dependencies...\n"
            "⠙ Resolving...\n"
            "⠹\n"
            "[-]\n"
            "[/]\n"
            "↑/↓ Navigate · enter Select · esc Skip\n"
            "? for shortcuts\n"
            "└ Tip: Use --verbose for debug\n"
            "❯ git status\n"
            "❯ git diff --stat\n"
            "hermes@desktop-pc:~/first-mate$\n"
            ">\n"
            "> \n"
            "All 419 unit tests pass 100% green."
        )
        cleaned = sanitize_worker_output(raw_debris)
        self.assertNotIn("3...)", cleaned)
        self.assertNotIn("mate/supervisor.py...)", cleaned)
        self.assertNotIn("(ctrl+o to expand)", cleaned)
        self.assertNotIn("expand)", cleaned)
        self.assertNotIn("⠋", cleaned)
        self.assertNotIn("Analyzing dependencies...", cleaned)
        self.assertNotIn("[-]", cleaned)
        self.assertNotIn("Navigate", cleaned)
        self.assertNotIn("Tip:", cleaned)
        self.assertNotIn("❯ git status", cleaned)
        self.assertNotIn("❯ git diff", cleaned)
        self.assertNotIn("hermes@desktop-pc", cleaned)
        self.assertFalse(cleaned.startswith(">"))
        self.assertEqual(cleaned, "All 419 unit tests pass 100% green.")

    def test_ansi_escape_code_stripping(self):
        """Verifies 8-color, 256-color, 24-bit RGB, and cursor escapes are stripped."""
        raw_ansi = (
            "\x1b[32mOK\x1b[0m\n"
            "\x1b[1;31mCRITICAL ERROR\x1b[0m\n"
            "\x1b[38;5;196m256-color text\x1b[0m\n"
            "\x1b[38;2;100;200;50mRGB text\x1b[0m\n"
            "\x1b]0;Terminal Window Title\x07\x1b[2K\x1b[1GLine cleared"
        )
        cleaned = sanitize_worker_output(raw_ansi)
        self.assertNotIn("\x1b", cleaned)
        self.assertIn("OK", cleaned)
        self.assertIn("CRITICAL ERROR", cleaned)
        self.assertIn("256-color text", cleaned)
        self.assertIn("RGB text", cleaned)
        self.assertIn("Line cleared", cleaned)

    def test_code_blocks_preservation_inside_worker_output(self):
        """Verifies code blocks inside worker output retain code syntax verbatim."""
        raw_code = (
            "Here is the patch:\n"
            "```python\n"
            "# ╭─ Box drawing inside code ─╮\n"
            "❯ git command inside code\n"
            "return 42\n"
            "```\n"
            "Done."
        )
        cleaned = sanitize_worker_output(raw_code)
        self.assertIn("```python", cleaned)
        self.assertIn("# ╭─ Box drawing inside code ─╮", cleaned)
        self.assertIn("❯ git command inside code", cleaned)
        self.assertIn("return 42", cleaned)
        self.assertIn("Done.", cleaned)

    def test_empty_and_whitespace_inputs(self):
        """Verifies None, empty, and whitespace strings do not raise exceptions."""
        self.assertEqual(sanitize_worker_output(None), "")
        self.assertEqual(sanitize_worker_output(""), "")
        self.assertEqual(sanitize_worker_output("   \n\n\t  "), "")


class TestFleetMeshPaneLocking50Concurrent(unittest.IsolatedAsyncioTestCase):
    """Empirically stress-tests FleetMesh per-pane locking with 50 concurrent dispatches."""

    async def test_50_concurrent_dispatches_same_pane_strictly_serialized(self):
        """
        Fires 50 concurrent command dispatches to the exact same pane ID.
        Verifies that:
        - All 50 dispatches execute and complete
        - max_active inside the critical section is strictly 1 (0 race conditions)
        - Start and end events are strictly paired with 0 interleaving
        """
        mesh = FleetMesh()
        pane_id = "test-pane-50-mesh"
        num_tasks = 50
        events = []
        active_count = 0
        max_active = 0
        race_conditions = 0

        async def worker(task_id: int):
            nonlocal active_count, max_active, race_conditions
            async def critical_section():
                nonlocal active_count, max_active, race_conditions
                active_count += 1
                if active_count > max_active:
                    max_active = active_count
                if active_count > 1:
                    race_conditions += 1
                events.append(("start", task_id, time.perf_counter()))
                # Variable duration work
                await asyncio.sleep(random.uniform(0.001, 0.004))
                events.append(("end", task_id, time.perf_counter()))
                active_count -= 1
                return task_id

            return await mesh.run_locked(pane_id, critical_section)

        # Fire 50 concurrent tasks simultaneously
        tasks = [asyncio.create_task(worker(i)) for i in range(num_tasks)]
        results = await asyncio.gather(*tasks)

        self.assertEqual(len(results), 50)
        self.assertEqual(max_active, 1, f"Expected strictly 1 active worker, observed {max_active}")
        self.assertEqual(race_conditions, 0, f"Expected 0 race conditions, observed {race_conditions}")

        # Verify strict pairwise serialization
        self.assertEqual(len(events), 100)
        for idx in range(0, len(events), 2):
            start_ev = events[idx]
            end_ev = events[idx + 1]
            self.assertEqual(start_ev[0], "start")
            self.assertEqual(end_ev[0], "end")
            self.assertEqual(start_ev[1], end_ev[1], f"Mismatched start/end task_id at {idx}")

    async def test_concurrent_dispatches_different_panes_execute_in_parallel(self):
        """Verifies that while individual panes are serialized, distinct panes run in parallel."""
        mesh = FleetMesh()
        panes = ["pane-alpha", "pane-beta", "pane-gamma", "pane-delta", "pane-epsilon"]
        active_count = 0
        max_active = 0

        async def worker(idx: int):
            nonlocal active_count, max_active
            target_pane = panes[idx % len(panes)]
            async def work():
                nonlocal active_count, max_active
                active_count += 1
                if active_count > max_active:
                    max_active = active_count
                await asyncio.sleep(0.015)
                active_count -= 1
                return idx
            return await mesh.run_locked(target_pane, work)

        tasks = [asyncio.create_task(worker(i)) for i in range(50)]
        results = await asyncio.gather(*tasks)

        self.assertEqual(len(results), 50)
        self.assertGreater(max_active, 1, f"Expected parallel execution across panes, got max_active={max_active}")

    async def test_lock_release_on_exception(self):
        """Verifies that exceptions raised inside run_locked do not leave the lock deadlocked."""
        mesh = FleetMesh()
        pane_id = "pane-exception-recovery"

        async def failing_worker():
            raise RuntimeError("Subprocess execution crash")

        with self.assertRaises(RuntimeError):
            await mesh.run_locked(pane_id, failing_worker)

        # Lock must be released and available for subsequent tasks
        lock = mesh.get_pane_lock(pane_id)
        self.assertFalse(lock.locked())

        async def success_worker():
            return "recovered"

        res = await mesh.run_locked(pane_id, success_worker)
        self.assertEqual(res, "recovered")


class TestDualPaneHarnessLifecycleStress(unittest.IsolatedAsyncioTestCase):
    """Stress tests DualPaneHarness provisioning, metadata tracking, and teardown."""

    def setUp(self):
        self.mock_client = AsyncMock()
        self.harness = DualPaneHarness(herdr_client=self.mock_client)

    async def test_provision_and_teardown_lifecycle(self):
        """Verifies complete dual-pane provisioning, inspection, and clean teardown."""
        self.mock_client.run_cli.side_effect = [
            (0, json.dumps({"result": {"workspace": {"workspace_id": "ws-life-1"}}}), ""),
            (0, "started", ""),
            (0, json.dumps({"result": {"pane": {"pane_id": "p2"}}}), ""),
        ]

        meta = await self.harness.create_workspace(
            session_name="life-sess",
            cwd="/Users/hermes/workspace",
            channel="C-TEST",
            thread_ts="1790000000.1234"
        )

        self.assertEqual(meta["workspace_id"], "ws-life-1")
        self.assertEqual(meta["liaison_agent"], "life-sess-liaison")
        self.assertEqual(meta["runner_pane"], "runner")
        self.assertEqual(meta["status"], "active")
        self.assertIn("life-sess", self.harness.active_workspaces)

        # Teardown
        self.mock_client.run_cli.side_effect = None
        self.mock_client.run_cli.return_value = (0, "closed", "")
        await self.harness.teardown("life-sess")

        self.assertNotIn("life-sess", self.harness.active_workspaces)
        ws_info = self.harness.get_workspace("life-sess")
        self.assertEqual(ws_info["status"], "closed")


class TestEmpiricalRunnerAndBufferChallenges(unittest.IsolatedAsyncioTestCase):
    """
    Empirical challenge suite addressing Challenger 1 and Reviewer 2 defect findings:
    - Live Herdr CLI execution polling with synchronous yield timeout
    - Buffer-populated stdout on fast pane commands
    - Resolved pane timeout handling without swallowing TimeoutError
    - Adversarial interactive blocker extraction (ANSI, bracketed URLs, sudo prompts)
    - Teardown idempotency and workspace_id state resolution
    """

    async def test_empirical_live_herdr_execution_polling_yields_running(self):
        """
        Challenger 1 Finding 1: 'herdr pane run' terminates in ~12ms, but command is
        actively running in pane PTY. Polling detects process-info foreground PID and
        yields status='running' when yield_timeout expires.
        """
        executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            # Live Herdr: pane run returns immediately with code 0
            mock_run_proc = AsyncMock()
            mock_run_proc.returncode = 0
            mock_run_proc.communicate.return_value = (b"", b"")

            # process-info reports sleep command running under shell
            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({
                    "id": "cli:pane:process_info",
                    "result": {
                        "process_info": {
                            "shell_pid": 90777,
                            "foreground_process_group_id": 90885,
                            "foreground_processes": [
                                {"pid": 90885, "name": "sleep", "argv": ["sleep", "10"]}
                            ],
                            "pane_id": "w0:p1"
                        }
                    }
                }).encode(),
                b""
            )

            mock_exec.side_effect = [mock_run_proc, mock_info_proc, mock_info_proc, mock_info_proc, mock_info_proc]

            handle = await executor.run_command(pane="w0:p1", command="sleep 10", yield_timeout=0.08)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 90885)
            self.assertEqual(handle.pane, "w0:p1")
            self.assertEqual(handle.command, "sleep 10")
            self.assertIn("Command running in Pane 2. Liaison loop remains active.", handle.message)

    async def test_empirical_fast_command_populates_stdout_from_buffer(self):
        """
        Challenger 1 Finding 2: Fast commands in live Herdr output to terminal buffer,
        not CLI stdout. RunnerExecutor reads buffer to populate handle.stdout.
        """
        executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_run_proc = AsyncMock()
            mock_run_proc.returncode = 0
            mock_run_proc.communicate.return_value = (b"", b"")

            # Idle shell: foreground process group == shell_pid
            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({
                    "id": "cli:pane:process_info",
                    "result": {
                        "process_info": {
                            "shell_pid": 22866,
                            "foreground_process_group_id": 22866,
                            "foreground_processes": [{"pid": 22866, "name": "zsh"}],
                            "pane_id": "w0:p1"
                        }
                    }
                }).encode(),
                b""
            )

            # read buffer returns the command's screen output
            mock_read_proc = AsyncMock()
            mock_read_proc.returncode = 0
            mock_read_proc.communicate.return_value = (b"hello world\n", b"")

            mock_exec.side_effect = [mock_run_proc, mock_info_proc, mock_read_proc]

            handle = await executor.run_command(pane="w0:p1", command="echo hello world", yield_timeout=1.0)
            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertEqual(handle.stdout, "hello world")
            self.assertEqual(handle.message, "Command completed within yield timeout.")

    async def test_empirical_resolved_pane_timeout_yields_running(self):
        """
        Reviewer 2 Finding 1: Command dispatched by label requiring resolution
        properly yields status='running' when exceeding yield_timeout.
        """
        executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            p1 = AsyncMock(returncode=1, communicate=AsyncMock(return_value=(b"", b"error: pane_not_found")))
            # Live schema: top-level "panes" list
            p2 = AsyncMock(returncode=0, communicate=AsyncMock(return_value=(json.dumps({"panes": [{"label": "runner", "pane_id": "w1:p2"}]}).encode(), b"")))
            p3 = AsyncMock(pid=99999, communicate=AsyncMock(side_effect=asyncio.TimeoutError()))
            p4 = AsyncMock(returncode=0, communicate=AsyncMock(return_value=(json.dumps({"result": {"pid": 99999}}).encode(), b"")))
            mock_exec.side_effect = [p1, p2, p3, p4]

            handle = await executor.run_command(pane="runner", command="sleep 10", yield_timeout=0.1)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 99999)
            self.assertEqual(handle.pane, "w1:p2")

    def test_empirical_interactive_blocker_adversarial_cases(self):
        """
        Challenger 1 Finding 4 & Reviewer 2 Finding 3: Adversarial test of interactive blocker detection.
        - Bracketed OAuth URL must strip trailing ']'
        - ANSI-styled OAuth URL must strip color codes
        - (yes/no) and [sudo] password prompts must be matched
        """
        obs = RunnerBufferObserver()

        # 1. Bracketed OAuth URL
        b_res = obs.extract_interactive_blockers("[http://127.0.0.1:53682/auth]")
        self.assertIsNotNone(b_res)
        self.assertEqual(b_res["type"], "oauth")
        self.assertEqual(b_res["url"], "http://127.0.0.1:53682/auth")

        # 2. ANSI styled OAuth URL
        ansi_buf = "\x1b[32mPlease visit \x1b[4mhttp://127.0.0.1:53682/auth\x1b[0m to authenticate.\x1b[0m"
        a_res = obs.extract_interactive_blockers(ansi_buf)
        self.assertIsNotNone(a_res)
        self.assertEqual(a_res["type"], "oauth")
        self.assertEqual(a_res["url"], "http://127.0.0.1:53682/auth")

        # 3. Quoted OAuth URLs (double and single quotes)
        q_res1 = obs.extract_interactive_blockers('"http://localhost:8080/callback?code=abc"')
        self.assertIsNotNone(q_res1)
        self.assertEqual(q_res1["type"], "oauth")
        self.assertEqual(q_res1["url"], "http://localhost:8080/callback?code=abc")

        q_res2 = obs.extract_interactive_blockers("'http://0.0.0.0:3000/oauth'")
        self.assertIsNotNone(q_res2)
        self.assertEqual(q_res2["type"], "oauth")
        self.assertEqual(q_res2["url"], "http://0.0.0.0:3000/oauth")

        # 4. Interactive prompts (parenthesized and bracketed yes/no)
        yn_res1 = obs.extract_interactive_blockers("Continue? (yes/no)")
        self.assertIsNotNone(yn_res1)
        self.assertEqual(yn_res1["type"], "prompt")

        yn_res2 = obs.extract_interactive_blockers("[yes/no]")
        self.assertIsNotNone(yn_res2)
        self.assertEqual(yn_res2["type"], "prompt")

        # 5. Sudo prompt
        sudo_res = obs.extract_interactive_blockers("[sudo] password for hermes: ")
        self.assertIsNotNone(sudo_res)
        self.assertEqual(sudo_res["type"], "prompt")

        # 6. Git credential and SSH passphrase prompts
        git_res = obs.extract_interactive_blockers("Password for 'https://github.com': ")
        self.assertIsNotNone(git_res)
        self.assertEqual(git_res["type"], "prompt")

        ssh_res = obs.extract_interactive_blockers("Enter passphrase for key: ")
        self.assertIsNotNone(ssh_res)
        self.assertEqual(ssh_res["type"], "prompt")

    async def test_empirical_teardown_by_workspace_id_and_idempotency(self):
        """
        Reviewer 2 Finding 4 & Challenger 4: Teardown by workspace_id updates state correctly,
        and repeated calls are idempotent no-ops with 0 redundant CLI calls and unmodified closed_at.
        """
        mock_client = AsyncMock()
        mock_client.run_cli.side_effect = [
            (0, json.dumps({"result": {"workspace": {"workspace_id": "ws-empirical-100"}}}), ""),
            (0, "ok", ""),
            (0, json.dumps({"result": {"pane": {"pane_id": "p2"}}}), ""),
            (0, "closed", ""),
        ]

        harness = DualPaneHarness(herdr_client=mock_client)
        await harness.provision_dual_pane("session-key-1", "/tmp")
        self.assertIn("session-key-1", harness.active_workspaces)

        # Close by workspace_id
        await harness.close_workspace("ws-empirical-100")
        ws = harness.get_workspace("session-key-1")
        self.assertIsNotNone(ws)
        self.assertEqual(ws["status"], "closed")
        self.assertEqual(len(harness.active_workspaces), 0)
        closed_at_first = ws.get("closed_at")
        self.assertIsNotNone(closed_at_first)

        # Repeated teardown must be idempotent: 0 redundant CLI calls and unmodified closed_at
        await harness.teardown("session-key-1")
        await harness.close_workspace("ws-empirical-100")
        self.assertEqual(mock_client.run_cli.call_count, 4)
        self.assertEqual(harness.get_workspace("session-key-1")["closed_at"], closed_at_first)

    async def test_empirical_worktree_failure_isolation_tc04(self):
        """
        Challenger 4: Simulate worktree creation failure when isolate_branch=True.
        Verify error dict returned, workspace_id is None, and base repo is untouched.
        """
        mock_client = AsyncMock()
        mock_client.run_cli.side_effect = [
            (1, "", "fatal: cannot create worktree: branch conflict"),
        ]

        delegator = WorkerDelegator(
            herdr_client=mock_client,
            base_repo="/Users/hermes/projects/first-mate"
        )
        res = await delegator.handoff(
            subagent="opencode",
            task="Refactor auth engine",
            isolate_branch=True
        )

        self.assertEqual(res["status"], "error")
        self.assertIn("Worktree isolation failed", res["error"])
        self.assertIn("branch conflict", res["error"])
        self.assertIsNone(res["workspace_id"])
        self.assertEqual(res["output"], "")
        # Exactly 1 CLI call for worktree creation; zero commands dispatched to base repo
        self.assertEqual(mock_client.run_cli.call_count, 1)
        called_cmd = mock_client.run_cli.call_args[0][0]
        self.assertEqual(called_cmd[:2], ["worktree", "create"])



if __name__ == "__main__":
    unittest.main()
