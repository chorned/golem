#!/usr/bin/env python3
"""
tests/test_supervisor.py
Comprehensive unit test suite for AgentSupervisor and Tiered Blast-Radius Safety Gates:
- Crucible Isolation: Task branch creation (task/<timestamp>_<worker_id>).
- Automated test verification and "Intern Review" loop (re-prompting on failure up to 2 retries).
- Passing tests on retry produces structured success package.
- Exhausted retries produces structured failure diagnosis.
- Tier 1 Read / Audit fast-path.
- Tier 2 Worker Task Pre-Flight Alignment Card ("Grill-Me") with Recommendation (Default) latching on `go` or 👍.
- Tier 3 Mutating Production / Merging / Push Safety Gate:
  * Rejects simple `go` or 👍 reaction.
  * Strictly requires two-key explicit confirmation (`approve` or `do it`).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import time
import signal
import tempfile
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app
from herdr_tools import validate_action_payload
from herdr_fleet import HerdrFleet
from herdr_client import HerdrClient, InfrastructureError
from supervisor import AgentSupervisor, cleanup_rogue_servers, SPAWNED_BACKGROUND_SERVERS


class TestAgentSupervisor(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.mock_herdr = HerdrClient()
        self.supervisor = AgentSupervisor(herdr_client=self.mock_herdr)
        app.supervisor = self.supervisor
        app.app.supervisor = self.supervisor
        app.client_herdr = self.mock_herdr
        app.app.herdr_client = self.mock_herdr
        self.fleet = HerdrFleet()
        app.fleet = self.fleet
        app.app.fleet = self.fleet
        app.sessions.clear()
        app.pending_alignment.clear()
        app.pending_alignment_by_msg_ts.clear()
        app.fetch_recent_thread_context = AsyncMock(return_value="")
        self.patch_log_event = patch("memory_manager.MemoryManager.log_event")
        self.mock_log_event = self.patch_log_event.start()
        self.addCleanup(self.patch_log_event.stop)

    async def test_crucible_isolation_branch_creation(self):
        """Verify supervisor creates isolated task branch without mutating root repo via git checkout."""
        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            branch = await self.supervisor.create_task_branch("opencode-dev", "/tmp/repo")
            self.assertTrue(branch.startswith("task-"))
            self.assertIn("opencode-dev", branch)

            # Strictly verify NO git checkout -b was executed against the root workspace
            mock_exec.assert_not_called()

    async def test_supervisor_review_loop_reprompts_on_failure_and_succeeds_on_retry(self):
        """
        Verify supervisor re-prompts Herdr worker via `herdr agent prompt ... --wait`
        when tests fail, and succeeds when second test run passes.
        """
        worker_id = "opencode-dev"
        user_intent = "Refactor JWT auth middleware"

        # Mock dispatch_worker
        mock_dispatch = {
            "worker_id": worker_id,
            "pane_id": "w4:p2",
            "output": "Initial implementation finished",
            "branch": "task/test_branch"
        }

        reprompt_calls = []

        async def fake_prompt_worker(worker_id=None, prompt=None, wait=True, timeout_ms=120000, *args, **kwargs):
            reprompt_calls.append({"worker_id": worker_id, "prompt": prompt})
            return "Fixed auth middleware based on test error"

        mock_wt_success = {
            "success": True,
            "branch": "task/123_opencode_dev",
            "workspace_id": "w9",
            "pane_id": "w4:p2",
            "worktree_path": "/tmp/repo"
        }
        self.mock_herdr.create_worktree = AsyncMock(return_value=mock_wt_success)
        self.mock_herdr.dispatch_worker = AsyncMock(return_value=mock_dispatch)
        self.mock_herdr.prompt_worker = AsyncMock(side_effect=fake_prompt_worker)

        # First test fails, second test passes
        test_run_count = 0

        async def fake_run_tests(repo_path, test_cmd=None, timeout=60.0):
            nonlocal test_run_count
            test_run_count += 1
            if test_run_count == 1:
                return 1, "", "AssertionError: token expired not handled"
            return 0, "Ran 12 tests in 0.2s: OK", ""

        with patch.object(self.supervisor, "create_task_branch", new_callable=AsyncMock, return_value="task/123_opencode_dev"), \
             patch.object(self.supervisor, "run_tests", side_effect=fake_run_tests), \
             patch.object(self.supervisor, "get_git_diff", new_callable=AsyncMock, return_value=" auth.py | 12 +++---\n 1 file changed"):

            report = await self.supervisor.supervise_task(
                worker_id=worker_id,
                user_intent=user_intent,
                repo_path="/tmp/repo",
                max_retries=2
            )

            self.assertEqual(report["status"], "success")
            self.assertEqual(report["test_status"], "passed")
            self.assertEqual(report["iterations"], 2)
            self.assertEqual(len(reprompt_calls), 1)
            self.assertIn("Tests failed:\nAssertionError: token expired not handled", reprompt_calls[0]["prompt"])
            self.assertIn("auth.py | 12 +++---", report["diff_stat"])

            briefing = self.supervisor.format_review_briefing(report)
            self.assertIn(":white_check_mark: *Intern Review Passed:*", briefing)
            self.assertIn("opencode-dev", briefing)

    async def test_supervisor_review_loop_exhausts_retries_and_reports_failure(self):
        """Verify supervisor exhausts retries and returns diagnostic report when tests persistently fail."""
        worker_id = "opencode-dev"
        mock_wt_fail = {
            "success": True,
            "branch": "task/999_opencode_dev",
            "workspace_id": "w9",
            "pane_id": "w9:p1",
            "worktree_path": "/tmp/repo"
        }
        self.mock_herdr.create_worktree = AsyncMock(return_value=mock_wt_fail)
        self.mock_herdr.dispatch_worker = AsyncMock(return_value={"worker_id": worker_id, "output": ""})
        self.mock_herdr.prompt_worker = AsyncMock(return_value="Attempted fix")

        # Tests fail continuously
        async def fake_failing_tests(repo_path, test_cmd=None, timeout=60.0):
            return 1, "", "SyntaxError: invalid syntax in auth.py"

        with patch.object(self.supervisor, "create_task_branch", new_callable=AsyncMock, return_value="task/999_opencode_dev"), \
             patch.object(self.supervisor, "run_tests", side_effect=fake_failing_tests), \
             patch.object(self.supervisor, "get_git_diff", new_callable=AsyncMock, return_value=""):

            report = await self.supervisor.supervise_task(
                worker_id=worker_id,
                user_intent="Refactor auth",
                repo_path="/tmp/repo",
                max_retries=2
            )

            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["test_status"], "failed")
            self.assertEqual(report["iterations"], 2)
            self.assertIn("SyntaxError: invalid syntax in auth.py", report["test_errors"])

            briefing = self.supervisor.format_review_briefing(report)
            self.assertIn(":warning: *Intern Review Blocked:*", briefing)
            self.assertIn("Failure Diagnosis", briefing)


    def test_banned_python_c_command_sanitization(self):
        """Verify hallucinated python -c dummy commands are banned and sanitized for OpenCode."""
        hallucinated = 'python -c "import os; os.environ[\'AGY_MODE\']=\'dev\'; print(\'ready\')"'
        sanitized = validate_action_payload(hallucinated, target_node="hermes", worker_id="opencode-dev")
        self.assertEqual(
            sanitized,
            "Herdr worktree create -> Dispatch opencode-dev in dev-grid (FrugaLLM :61721) -> no-mistakes review"
        )
        self.assertNotIn("python -c", sanitized)


    async def test_supervisor_dispatches_with_unique_agent_name(self):
        """Verify supervise_task generates and passes unique dynamic agent name to dispatch_worker."""
        mock_wt = {
            "success": True,
            "branch": "task-test-branch",
            "workspace_id": "w9",
            "pane_id": "w9:p1",
            "worktree_path": "/tmp/test_wt"
        }
        self.mock_herdr.create_worktree = AsyncMock(return_value=mock_wt)
        self.mock_herdr.dispatch_worker = AsyncMock(return_value={"output": "Done", "status": "settled"})
        self.supervisor.run_no_mistakes = AsyncMock(return_value=(0, "All passed", "", {"passed": True, "findings": []}))
        self.supervisor.commit_worktree_changes = AsyncMock(return_value=True)
        self.supervisor.github_manager.create_pull_request = AsyncMock(return_value={"pr_url": "https://github.com/carlhorned/AGY/pull/1", "pr_number": 1})

        report = await self.supervisor.supervise_task(
            worker_id="opencode-dev",
            user_intent="Fix bug"
        )

        self.assertEqual(report["status"], "success")
        self.mock_herdr.dispatch_worker.assert_called_once()
        call_kwargs = self.mock_herdr.dispatch_worker.call_args[1]
        agent_name = call_kwargs.get("agent_name")
        self.assertIsNotNone(agent_name)
        self.assertTrue(agent_name.startswith("opencode-"))

    async def test_supervisor_dispatch_infrastructure_error_fails_fast_and_tears_down(self):
        """Verify that an InfrastructureError during dispatch aborts immediately and tears down worktree and agent."""
        mock_wt = {
            "success": True,
            "branch": "task-test-branch",
            "workspace_id": "w9",
            "pane_id": "w9:p1",
            "worktree_path": "/tmp/test_wt"
        }
        self.mock_herdr.create_worktree = AsyncMock(return_value=mock_wt)
        self.mock_herdr.dispatch_worker = AsyncMock(side_effect=InfrastructureError("Agent blocked waiting for user input"))
        self.mock_herdr.remove_worktree = AsyncMock(return_value={"success": True})
        self.supervisor.run_no_mistakes = AsyncMock()

        report = await self.supervisor.supervise_task(
            worker_id="opencode-dev",
            user_intent="Fix bug"
        )

        self.assertEqual(report["status"], "failed")
        self.assertTrue(report.get("is_infra_error"))
        self.assertIn("Agent blocked", report["summary"])

        # Quality gate / test suite was NOT run
        self.supervisor.run_no_mistakes.assert_not_called()

        # Worktree and agent were torn down
        self.mock_herdr.remove_worktree.assert_called_once()
        rm_kwargs = self.mock_herdr.remove_worktree.call_args[1]
        self.assertEqual(rm_kwargs.get("workspace_id"), "w9")
        self.assertTrue(rm_kwargs.get("agent_name", "").startswith("opencode-"))


    async def test_supervisor_sandbox_task_execution(self):
        """Verify supervisor executes generic spikes in isolated sandbox directory without creating PR or running repo tests."""
        worker_id = "opencode-dev"
        user_intent = "create a simple test page and host it in a python server"

        self.mock_herdr.allocate_worker_pane = AsyncMock(return_value={"pane_id": "dev-grid:p5"})
        self.mock_herdr.dispatch_worker = AsyncMock(return_value={"worker_id": worker_id, "output": "Created server.py"})
        self.mock_herdr.run_cli = AsyncMock(return_value={"success": True})
        self.supervisor.github_manager.create_pull_request = AsyncMock()

        with patch("os.makedirs"), \
             patch("asyncio.create_subprocess_exec") as mock_exec, \
             patch.object(self.supervisor, "check_protected_files", new_callable=AsyncMock, return_value=(True, [])):

            # Mock git init and git diff
            mock_proc = MagicMock()
            mock_proc.communicate = AsyncMock(return_value=(b"server.py\n", b""))
            mock_proc.returncode = 0
            mock_exec.return_value = mock_proc

            report = await self.supervisor.supervise_task(
                worker_id=worker_id,
                user_intent=user_intent,
                repo_path="/Users/hermes/projects/sandbox"
            )

            self.assertEqual(report["status"], "success")
            self.assertEqual(report["test_status"], "passed")
            self.assertTrue(report.get("is_sandbox"))
            self.assertIn("/Users/hermes/projects/sandbox/", report.get("sandbox_path", ""))
            # PR creation must NOT be called for sandbox tasks
            self.supervisor.github_manager.create_pull_request.assert_not_called()
            self.assertIsNone(report.get("pr"))

    async def test_supervisor_rejects_prs_modifying_plist_files(self):
        """Verify supervisor automatically rejects PRs and worktrees where junior workers attempt to modify .plist files."""
        worker_id = "opencode-dev"
        user_intent = "Update launchd daemon configuration"

        mock_wt = {
            "success": True,
            "branch": "task/test_plist_rejection",
            "workspace_id": "w9",
            "pane_id": "w4:p2",
            "worktree_path": "/tmp/test_wt"
        }
        self.mock_herdr.create_worktree = AsyncMock(return_value=mock_wt)
        self.mock_herdr.dispatch_worker = AsyncMock(return_value={"worker_id": worker_id, "output": "Modified plist"})
        self.mock_herdr.remove_worktree = AsyncMock(return_value={"success": True})
        self.supervisor.github_manager.create_pull_request = AsyncMock()

        with patch.object(self.supervisor, "get_modified_files", new_callable=AsyncMock, return_value=["com.user.antigravity-slack.plist"]), \
             patch.object(self.supervisor, "get_git_diff", new_callable=AsyncMock, return_value=" com.user.antigravity-slack.plist | 4 ++--"):

            report = await self.supervisor.supervise_task(
                worker_id=worker_id,
                user_intent=user_intent,
                repo_path="/tmp/test_wt"
            )

            # Verification:
            # 1. Status indicates failure/rejection
            self.assertIn(report["status"], ("failed", "rejected"))
            self.assertEqual(report.get("test_status"), "rejected")
            self.assertTrue(report.get("is_security_violation"))
            # 2. PR creation was NEVER called
            self.supervisor.github_manager.create_pull_request.assert_not_called()
            self.assertIsNone(report.get("pr"))
            self.assertIsNone(report.get("pr_url"))
            # 3. Worktree was cleaned up / torn down
            self.mock_herdr.remove_worktree.assert_called_once()
            # 4. Summary notes protected file rejection
            self.assertIn("protected system file", report["summary"].lower())
            self.assertIn("com.user.antigravity-slack.plist", report["summary"])


    async def test_local_branch_merge_squashes_into_base_branch_without_remote_pr(self):
        """Verify merge_task_branch switches to base branch, squash merges, commits, and deletes task branch without remote calls."""
        executed_cmds = []

        async def fake_subprocess_exec(*args, **kwargs):
            executed_cmds.append(list(args))
            mock_proc = MagicMock()
            mock_proc.communicate = AsyncMock(return_value=(b"", b""))
            mock_proc.returncode = 0
            return mock_proc

        with patch("asyncio.create_subprocess_exec", side_effect=fake_subprocess_exec):
            res = await self.supervisor.merge_task_branch(
                target_cwd="/Users/hermes/projects/FrugaLLM",
                task_branch="task-12345-opencode",
                base_branch="main"
            )

            self.assertTrue(res["success"])
            self.assertEqual(res["base_branch"], "main")
            self.assertEqual(res["task_branch"], "task-12345-opencode")
            self.assertIn("Merged `task-12345-opencode` into `main`", res["message"])

            # Verify local git actions occurred in order
            cmd_strs = [" ".join(c) for c in executed_cmds]
            self.assertTrue(any("git checkout main" in c for c in cmd_strs))
            self.assertTrue(any("git merge --squash task-12345-opencode" in c for c in cmd_strs))
            self.assertTrue(any("git commit -m" in c for c in cmd_strs))
            self.assertTrue(any("git branch -D task-12345-opencode" in c for c in cmd_strs))

            # Strictly verify NO remote push or gh pr commands
            for c in cmd_strs:
                self.assertNotIn("push", c)
                self.assertNotIn("gh ", c)

    async def test_undo_cleans_working_tree_and_deletes_task_branch_cleanly(self):
        """Verify undo_task_branch runs git reset --hard, git clean -fd, git checkout, and git branch -D."""
        executed_cmds = []

        async def fake_subprocess_exec(*args, **kwargs):
            executed_cmds.append(list(args))
            mock_proc = MagicMock()
            mock_proc.communicate = AsyncMock(return_value=(b"", b""))
            mock_proc.returncode = 0
            return mock_proc

        with patch("asyncio.create_subprocess_exec", side_effect=fake_subprocess_exec):
            res = await self.supervisor.undo_task_branch(
                target_cwd="/Users/hermes/projects/FrugaLLM",
                task_branch="task-12345-opencode",
                base_branch="main"
            )

            self.assertTrue(res["success"])
            self.assertEqual(res["base_branch"], "main")
            self.assertEqual(res["task_branch"], "task-12345-opencode")
            self.assertIn("Scrapped `task-12345-opencode`", res["message"])

            cmd_strs = [" ".join(c) for c in executed_cmds]
            self.assertTrue(any("git reset --hard" in c for c in cmd_strs))
            self.assertTrue(any("git clean -fd" in c for c in cmd_strs))
            self.assertTrue(any("git checkout main" in c for c in cmd_strs))
            self.assertTrue(any("git branch -D task-12345-opencode" in c for c in cmd_strs))

    async def test_supervise_task_direct_execution_without_git_branch(self):
        """Verify supervise_task with requires_git_branch=False executes directly in target_cwd without branch creation."""
        worker_id = "opencode-dev"
        user_intent = "why is FrugaLLM down"

        self.mock_herdr.dispatch_worker = AsyncMock(return_value={"output": "Checked FrugaLLM in ~/projects/FrugaLLM. Process is dead, port 61721 is free."})
        self.mock_herdr.create_worktree = AsyncMock()

        with patch.object(self.supervisor, "create_task_branch", new_callable=AsyncMock) as mock_create_branch:
            report = await self.supervisor.supervise_task(
                worker_id=worker_id,
                user_intent=user_intent,
                target_cwd="/Users/hermes",
                target_node="hermes",
                requires_git_branch=False,
                action_kind="query_or_operate"
            )

            self.assertEqual(report["status"], "success")
            self.assertEqual(report["action_kind"], "query_or_operate")
            self.assertFalse(report["requires_git_branch"])
            self.assertEqual(report["target_cwd"], "/Users/hermes")
            self.assertIsNone(report["branch"])

            # Worktree and task branch creation must NOT be called
            self.mock_herdr.create_worktree.assert_not_called()
            mock_create_branch.assert_not_called()

            # Verify dispatch_worker called with repo_path and empty branch_name
            self.mock_herdr.dispatch_worker.assert_called_once()
            call_kwargs = self.mock_herdr.dispatch_worker.call_args[1]
            self.assertEqual(call_kwargs["repo_path"], "/Users/hermes")
            self.assertEqual(call_kwargs["branch_name"], "")

            # Formatted briefing should be a direct summary without intern review / diff chrome
            briefing = self.supervisor.format_review_briefing(report)
            self.assertIn("Checked FrugaLLM in ~/projects/FrugaLLM. Process is dead, port 61721 is free.", briefing)
            self.assertNotIn("Intern Review Passed", briefing)
            self.assertNotIn("Say *merge* to land this", briefing)

    async def test_ensure_server_running_directory_scoping(self):
        """Verify ensure_server_running launches server with --directory <target_cwd> and cwd=target_cwd."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir = os.path.realpath(tmp_dir)
            # Create index.html in the temporary sandbox directory
            index_path = os.path.join(tmp_dir, "index.html")
            with open(index_path, "w") as f:
                f.write("<h1>Sandbox Test</h1>")

            mock_proc = MagicMock()
            mock_proc.pid = 42424

            with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec, \
                 patch("supervisor.get_listening_ports_in_cwd", new_callable=AsyncMock, return_value=[]):

                res = await self.supervisor.ensure_server_running(tmp_dir, "serve it")

                self.assertIsNotNone(res)
                self.assertTrue(res["running"])
                self.assertEqual(res["pid"], 42424)
                self.assertEqual(res["cwd"], tmp_dir)
                self.assertTrue(res["url"].startswith("http://localhost:"))

                # Verify subprocess invocation flags
                mock_exec.assert_called_once()
                exec_args = mock_exec.call_args[0]
                exec_kwargs = mock_exec.call_args[1]

                # Must explicitly pass --directory <tmp_dir>
                self.assertIn("--directory", exec_args)
                dir_idx = exec_args.index("--directory")
                self.assertEqual(exec_args[dir_idx + 1], tmp_dir)

                # Must explicitly set cwd=tmp_dir
                self.assertEqual(exec_kwargs.get("cwd"), tmp_dir)

                # Verify recorded in SPAWNED_BACKGROUND_SERVERS
                self.assertIn(tmp_dir, SPAWNED_BACKGROUND_SERVERS)
                self.assertEqual(SPAWNED_BACKGROUND_SERVERS[tmp_dir]["pid"], 42424)

            # Clean up
            SPAWNED_BACKGROUND_SERVERS.pop(tmp_dir, None)

    async def test_ensure_server_running_missing_index_html(self):
        """Verify ensure_server_running refuses to start server and returns None if index.html is missing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir = os.path.realpath(tmp_dir)
            # Do NOT create index.html
            with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec, \
                 patch("supervisor.get_listening_ports_in_cwd", new_callable=AsyncMock, return_value=[]):

                res = await self.supervisor.ensure_server_running(tmp_dir, "serve it")

                self.assertIsNone(res)
                mock_exec.assert_not_called()
                self.assertNotIn(tmp_dir, SPAWNED_BACKGROUND_SERVERS)

    async def test_terminate_server_in_dir(self):
        """Verify AgentSupervisor.terminate_server_in_dir terminates background server for target_cwd."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir = os.path.realpath(tmp_dir)
            SPAWNED_BACKGROUND_SERVERS[tmp_dir] = {
                "pid": 55555,
                "port": 8080,
                "cwd": tmp_dir,
                "url": "http://localhost:8080"
            }

            with patch("os.kill") as mock_kill, \
                 patch("supervisor.get_listening_ports_in_cwd", new_callable=AsyncMock, return_value=[]):

                terminated = await AgentSupervisor.terminate_server_in_dir(tmp_dir)

                self.assertTrue(terminated)
                self.assertNotIn(tmp_dir, SPAWNED_BACKGROUND_SERVERS)
                # Verify SIGTERM sent to PID 55555
                mock_kill.assert_any_call(55555, signal.SIGTERM)

    async def test_cleanup_rogue_servers(self):
        """Verify cleanup_rogue_servers identifies and kills rogue processes on banned ports (e.g. 54028)."""
        mock_lsof_out = (
            b"COMMAND     PID   USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME\n"
            b"Python    67890 hermes    3u  IPv4 0x12345678      0t0  TCP *:54028 (LISTEN)\n"
        )
        mock_proc = MagicMock()
        mock_proc.communicate = AsyncMock(return_value=(mock_lsof_out, b""))

        mock_ps_proc = MagicMock()
        mock_ps_proc.communicate = AsyncMock(return_value=(b"", b""))

        async def fake_exec(*args, **kwargs):
            if args[0] == "lsof":
                return mock_proc
            return mock_ps_proc

        with patch("asyncio.create_subprocess_exec", side_effect=fake_exec), \
             patch("os.kill") as mock_kill:

            killed = await cleanup_rogue_servers(banned_ports=[54028])

            self.assertIn(67890, killed)
            mock_kill.assert_called_with(67890, signal.SIGKILL)


if __name__ == "__main__":
    unittest.main()
