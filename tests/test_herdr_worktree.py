#!/usr/bin/env python3
"""
tests/test_herdr_worktree.py
Unit tests for Native Herdr Git Worktree Management:
- `herdr worktree create --cwd <path> --branch <name> --no-focus` JSON parsing.
- `herdr worktree remove --workspace <id> --force`.
- `herdr worktree list --cwd <path>`.
- Attaching worker execution pane and workspace to the created worktree directory.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import golem  # noqa: F401
from herdr_client import HerdrClient
from github_manager import GitHubManager


class TestHerdrWorktree(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.client = HerdrClient()

    async def test_create_worktree_json_parsing(self):
        """Verify create_worktree executes `herdr worktree create` and extracts root pane and workspace."""
        mock_output = {
            "id": "cli:worktree:create",
            "result": {
                "root_pane": {
                    "cwd": "/Users/hermes/.herdr/worktrees/AGY/task-feature",
                    "pane_id": "w9:p1",
                    "tab_id": "w9:t1",
                    "workspace_id": "w9"
                },
                "workspace": {
                    "workspace_id": "w9",
                    "label": "task-feature",
                    "worktree": {
                        "checkout_path": "/Users/hermes/.herdr/worktrees/AGY/task-feature"
                    }
                },
                "worktree": {
                    "branch": "task-feature",
                    "path": "/Users/hermes/.herdr/worktrees/AGY/task-feature"
                }
            }
        }

        with patch.object(self.client, "run_cli", new_callable=AsyncMock, return_value=(0, json.dumps(mock_output), "")) as mock_run, \
             patch("os.makedirs"), \
             patch("os.path.isdir", return_value=True):
            info = await self.client.create_worktree(
                branch_name="task-feature",
                repo_path="/Users/hermes/projects/AGY"
            )

            mock_run.assert_called_once_with([
                "worktree", "create",
                "--cwd", "/Users/hermes/projects/AGY",
                "--branch", "task-feature",
                "--no-focus"
            ], timeout=20.0)

            self.assertTrue(info["success"])
            self.assertEqual(info["workspace_id"], "w9")
            self.assertEqual(info["pane_id"], "w9:p1")
            self.assertEqual(info["worktree_path"], "/Users/hermes/.herdr/worktrees/AGY/task-feature")
            self.assertEqual(info["branch"], "task-feature")

    async def test_remove_worktree_with_workspace_id(self):
        """Verify remove_worktree runs `herdr worktree remove --workspace <id> --force`."""
        mock_remove_output = {
            "id": "cli:worktree:remove",
            "result": {
                "forced": True,
                "path": "/Users/hermes/.herdr/worktrees/AGY/task-feature",
                "workspace_id": "w9"
            }
        }

        with patch.object(self.client, "run_cli", new_callable=AsyncMock, return_value=(0, json.dumps(mock_remove_output), "")) as mock_run, \
             patch("asyncio.create_subprocess_exec", new_callable=AsyncMock):

            res = await self.client.remove_worktree(
                workspace_id="w9",
                branch_name="task-feature",
                repo_path="/Users/hermes/projects/AGY",
                force=True
            )

            mock_run.assert_called_once_with([
                "worktree", "remove",
                "--workspace", "w9",
                "--force"
            ], timeout=15.0)
            self.assertTrue(res["success"])
            self.assertEqual(res["workspace_id"], "w9")

    async def test_list_worktrees(self):
        """Verify list_worktrees parses worktree listings from Herdr."""
        mock_list_output = {
            "id": "cli:worktree:list",
            "result": {
                "worktrees": [
                    {
                        "branch": "main",
                        "path": "/Users/hermes/projects/AGY",
                        "is_linked_worktree": False
                    },
                    {
                        "branch": "task-auth",
                        "path": "/Users/hermes/.herdr/worktrees/AGY/task-auth",
                        "open_workspace_id": "w8",
                        "is_linked_worktree": True
                    }
                ]
            }
        }

        with patch.object(self.client, "run_cli", new_callable=AsyncMock, return_value=(0, json.dumps(mock_list_output), "")):
            wts = await self.client.list_worktrees("/Users/hermes/projects/AGY")
            self.assertEqual(len(wts), 2)
            self.assertEqual(wts[1]["branch"], "task-auth")
            self.assertEqual(wts[1]["open_workspace_id"], "w8")

    async def test_dispatch_worker_attaches_to_worktree_pane(self):
        """
        Verify dispatch_worker uses the worktree's allocated pane and path directly,
        without executing a redundant git checkout -b in the primary repo.
        """
        worktree_info = {
            "success": True,
            "branch": "task-auth",
            "workspace_id": "w9",
            "pane_id": "w9:p1",
            "worktree_path": "/Users/hermes/.herdr/worktrees/AGY/task-auth"
        }

        executed_cmds = []

        async def fake_run_cli(args, timeout=30.0):
            executed_cmds.append(list(args))
            sub = args[0]
            if sub == "agent" and args[1] == "start":
                return 0, "ok", ""
            elif sub == "agent" and args[1] == "prompt":
                return 0, "done", ""
            elif sub == "agent" and args[1] == "read":
                return 0, "Task completed in worktree pane", ""
            return 0, "", ""

        with patch.object(self.client, "run_cli", side_effect=fake_run_cli):
            res = await self.client.dispatch_worker(
                worker_id="opencode-dev",
                prompt="Refactor auth",
                branch_name="task-auth",
                worktree_info=worktree_info
            )

            self.assertEqual(res["pane_id"], "w9:p1")
            self.assertEqual(res["worktree_path"], "/Users/hermes/.herdr/worktrees/AGY/task-auth")
            self.assertEqual(res["status"], "settled")

            # Verify NO git checkout -b was run because worktree was already prepared
            branch_runs = [c for c in executed_cmds if c[0] == "pane" and c[1] == "run" and any("git checkout" in str(arg) for arg in c)]
            self.assertEqual(len(branch_runs), 0)

            # Verify agent started in the worktree pane w9:p1
            start_calls = [c for c in executed_cmds if c[0] == "agent" and c[1] == "start"]
            self.assertEqual(len(start_calls), 1)
            self.assertEqual(start_calls[0][6], "w9:p1")

    async def test_create_worktree_sanitizes_slashes_in_branch_name(self):
        """
        Verify create_worktree sanitizes branch names containing slashes (e.g. task/1798563139_opencode_dev)
        into filesystem-safe directory names (task-1798563139_opencode_dev) without creating nested
        non-existent subdirectories or FileNotFoundError.
        """
        branch_with_slash = "task/1798563139_opencode_dev"
        expected_dir_name = "task-1798563139_opencode_dev"

        mock_output = {
            "id": "cli:worktree:create",
            "result": {
                "root_pane": {
                    "cwd": f"/Users/hermes/.herdr/worktrees/AGY/{expected_dir_name}",
                    "pane_id": "w9:p1",
                    "workspace_id": "w9"
                }
            }
        }

        with patch.object(self.client, "run_cli", new_callable=AsyncMock, return_value=(0, json.dumps(mock_output), "")) as mock_run, \
             patch("os.makedirs"), \
             patch("os.path.isdir", return_value=True):
            info = await self.client.create_worktree(
                branch_name=branch_with_slash,
                repo_path="/Users/hermes/projects/AGY"
            )

            self.assertTrue(info["success"])
            self.assertEqual(info["branch"], branch_with_slash)
            self.assertIn(expected_dir_name, info["worktree_path"])
            self.assertNotIn("task/1798563139", info["worktree_path"])
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            self.assertIn(branch_with_slash, args)

    async def test_root_repo_branch_immutable_during_worktree_creation(self):
        """Verify root repository branch remains untouched on main when creating a worktree."""
        import tempfile
        import subprocess

        with tempfile.TemporaryDirectory() as temp_dir:
            subprocess.run(["git", "init", "-b", "main", temp_dir], check=True, capture_output=True)
            subprocess.run(["git", "-C", temp_dir, "config", "user.email", "tester@example.com"], check=True)
            subprocess.run(["git", "-C", temp_dir, "config", "user.name", "Tester"], check=True)
            subprocess.run(["git", "-C", temp_dir, "commit", "--allow-empty", "-m", "Initial commit"], check=True, capture_output=True)

            branch_before = subprocess.check_output(
                ["git", "-C", temp_dir, "branch", "--show-current"], text=True
            ).strip()
            self.assertEqual(branch_before, "main")

            dest_dir = await self.client.create_task_worktree(
                repo_path=temp_dir,
                task_id="task-immutable-test",
                base_ref="HEAD"
            )

            branch_after = subprocess.check_output(
                ["git", "-C", temp_dir, "branch", "--show-current"], text=True
            ).strip()
            self.assertEqual(branch_after, "main")
            self.assertEqual(branch_before, branch_after)
            self.assertTrue(os.path.isdir(dest_dir))

            # Cleanup worktree
            await self.client.remove_worktree(
                worktree_path=dest_dir,
                repo_path=temp_dir,
                branch_name="task-immutable-test"
            )

    async def test_create_task_worktree_atomic_git_command(self):
        """Verify create_task_worktree executes atomic `git -C <repo> worktree add -b <branch> <path> origin/main`."""
        executed_commands = []

        async def fake_subprocess_exec(*args, **kwargs):
            executed_commands.append(list(args))
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate = AsyncMock(return_value=(b"", b""))
            return mock_proc

        with patch("asyncio.create_subprocess_exec", side_effect=fake_subprocess_exec), \
             patch("os.makedirs"), \
             patch("os.path.isdir", return_value=True):

            dest_dir = await self.client.create_task_worktree(
                repo_path="/Users/hermes/projects/AGY",
                task_id="task-12345-worker",
                base_ref="origin/main"
            )

            expected_branch = "task-12345-worker"
            self.assertEqual(len(executed_commands), 1)
            cmd = executed_commands[0]
            self.assertEqual(cmd[0], "git")
            self.assertEqual(cmd[1], "-C")
            self.assertEqual(cmd[2], "/Users/hermes/projects/AGY")
            self.assertEqual(cmd[3], "worktree")
            self.assertEqual(cmd[4], "add")
            self.assertEqual(cmd[5], "-b")
            self.assertEqual(cmd[6], expected_branch)
            self.assertEqual(cmd[7], dest_dir)
            self.assertEqual(cmd[8], "origin/main")

    async def test_remote_push_executes_before_gh_pr_create(self):
        """Verify remote push executes before `gh pr create` in GitHubManager."""
        gh_mgr = GitHubManager()
        call_sequence = []

        async def fake_push_branch(repo_path, branch, remote="origin"):
            call_sequence.append(("push_branch", branch, remote))
            return 0, "Everything up-to-date", ""

        async def fake_run_gh(args, cwd=None, timeout=30.0):
            call_sequence.append(("run_gh", args[0], args[1]))
            return 0, "https://github.com/carlhorned/AGY/pull/42\n", ""

        with patch.object(gh_mgr, "push_branch", side_effect=fake_push_branch), \
             patch.object(gh_mgr, "run_gh", side_effect=fake_run_gh):

            res = await gh_mgr.create_pull_request(
                repo_path="/Users/hermes/projects/AGY",
                title="feat: test feature",
                body="Test body",
                branch="task-test-branch",
                push=True
            )

            self.assertTrue(res["success"])
            self.assertEqual(res["pr_url"], "https://github.com/carlhorned/AGY/pull/42")
            self.assertEqual(len(call_sequence), 2)
            self.assertEqual(call_sequence[0], ("push_branch", "task-test-branch", "origin"))
            self.assertEqual(call_sequence[1], ("run_gh", "pr", "create"))

    async def test_teardown_removes_worktree_and_prunes_metadata(self):
        """Verify remove_worktree invokes herdr worktree remove, git worktree remove, prune, and branch -D."""
        executed_git_cmds = []

        async def fake_git_exec(*args, **kwargs):
            executed_git_cmds.append(list(args))
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate = AsyncMock(return_value=(b"", b""))
            return mock_proc

        with patch.object(self.client, "run_cli", new_callable=AsyncMock, return_value=(0, "ok", "")) as mock_run_cli, \
             patch("asyncio.create_subprocess_exec", side_effect=fake_git_exec), \
             patch("os.path.exists", return_value=False):

            res = await self.client.remove_worktree(
                workspace_id="w9",
                branch_name="task-test-branch",
                worktree_path="/Users/hermes/.herdr/worktrees/AGY/task-test-branch",
                repo_path="/Users/hermes/projects/AGY",
                force=True
            )

            self.assertTrue(res["success"])

            # 1. Herdr CLI worktree remove
            mock_run_cli.assert_called_once_with(["worktree", "remove", "--workspace", "w9", "--force"], timeout=15.0)

            # 2. git worktree remove --force <path>
            git_rm = [c for c in executed_git_cmds if "worktree" in c and "remove" in c]
            self.assertEqual(len(git_rm), 1)
            self.assertEqual(git_rm[0], ["git", "-C", "/Users/hermes/projects/AGY", "worktree", "remove", "--force", "/Users/hermes/.herdr/worktrees/AGY/task-test-branch"])

            # 3. git worktree prune
            git_prune = [c for c in executed_git_cmds if "worktree" in c and "prune" in c]
            self.assertEqual(len(git_prune), 1)
            self.assertEqual(git_prune[0], ["git", "-C", "/Users/hermes/projects/AGY", "worktree", "prune"])

            # 4. git branch -D <branch>
            git_branch = [c for c in executed_git_cmds if "branch" in c and "-D" in c]
            self.assertEqual(len(git_branch), 1)
            self.assertEqual(git_branch[0], ["git", "-C", "/Users/hermes/projects/AGY", "branch", "-D", "task-test-branch"])


if __name__ == "__main__":
    unittest.main()

