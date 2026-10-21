#!/usr/bin/env python3
"""
tests/test_herdr_tools.py
Unit tests for direct native Herdr Python tools (herdr_tools.py).
Zero-MCP architecture verification: spawn_agent, read_agent, stop_agent.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import shutil
import tempfile
import asyncio
import unittest
from unittest.mock import patch, AsyncMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import golem  # noqa: F401
import herdr_tools
from herdr_tools import spawn_agent, read_agent, stop_agent, SPAWNED_AGENTS


_REAL_SUBPROCESS_EXEC = asyncio.create_subprocess_exec


class TestHerdrTools(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="firstmate_test_herdr_")
        SPAWNED_AGENTS.clear()

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)
        SPAWNED_AGENTS.clear()

    async def test_spawn_agent_ensures_cwd_and_isolates_git_branch(self):
        """Verify spawn_agent creates cwd and creates task branch if git repo."""
        # Initialize a temporary git repository
        git_repo = os.path.join(self.test_dir, "my_repo")
        os.makedirs(git_repo, exist_ok=True)
        init_p = await _REAL_SUBPROCESS_EXEC("git", "init", "-b", "main", cwd=git_repo)
        await init_p.communicate()
        cfg_e = await _REAL_SUBPROCESS_EXEC("git", "config", "user.email", "test@example.com", cwd=git_repo)
        await cfg_e.communicate()
        cfg_n = await _REAL_SUBPROCESS_EXEC("git", "config", "user.name", "Test Runner", cwd=git_repo)
        await cfg_n.communicate()
        commit_p = await _REAL_SUBPROCESS_EXEC(
            "git", "commit", "--allow-empty", "-m", "init", cwd=git_repo
        )
        await commit_p.communicate()

        # Mock herdr CLI calls to avoid requiring active Herdr daemon in isolated unit test
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            async def side_effect(*args, **kwargs):
                cmd_bin = args[0]
                if "herdr" in str(cmd_bin):
                    mock_proc = AsyncMock()
                    mock_proc.returncode = 0
                    mock_proc.communicate.return_value = (b"Spawned agent in pane w1:p2\n", b"")
                    return mock_proc
                return await _REAL_SUBPROCESS_EXEC(*args, **kwargs)

            mock_exec.side_effect = side_effect

            res = await spawn_agent(
                node="hermes",
                cwd=git_repo,
                worker="opencode-dev",
                instructions="implement unit test",
                isolate_branch=True
            )

            self.assertEqual(res["node"], "hermes")
            self.assertEqual(res["cwd"], os.path.realpath(git_repo))
            self.assertTrue(res["branch"].startswith("task-"))
            self.assertIn("opencode", res["branch"])
            self.assertEqual(res["pane_id"], "w1:p2")

            # Check that SPAWNED_AGENTS recorded the entry
            self.assertIn("w1:p2", SPAWNED_AGENTS)
            self.assertEqual(SPAWNED_AGENTS["w1:p2"]["branch"], res["branch"])

            # Verify git repo is on the new branch
            chk = await asyncio.create_subprocess_exec(
                "git", "rev-parse", "--abbrev-ref", "HEAD",
                cwd=git_repo,
                stdout=asyncio.subprocess.PIPE
            )
            out, _ = await chk.communicate()
            current_branch = out.decode().strip()
            self.assertEqual(current_branch, res["branch"])

    async def test_spawn_agent_non_git_skips_branch_isolation(self):
        """Verify spawn_agent does not create git branch when isolate_branch=False."""
        target_dir = os.path.join(self.test_dir, "non_git_dir")

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"w2:p1\n", b"")
            mock_exec.return_value = mock_proc

            res = await spawn_agent(
                node="hermes",
                cwd=target_dir,
                worker="hermes",
                instructions="check system disk space",
                isolate_branch=False
            )

            self.assertEqual(res["node"], "hermes")
            self.assertIsNone(res["branch"])
            self.assertTrue(os.path.isdir(target_dir))

    async def test_read_agent_fetches_terminal_buffer(self):
        """Verify read_agent calls herdr agent read and returns settled output."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"Listening on http://localhost:8000\nDone.\n", b"")
            mock_exec.return_value = mock_proc

            output = await read_agent("w1:p2")
            self.assertIn("http://localhost:8000", output)
            self.assertIn("Done.", output)

            # Verify herdr CLI args
            called_args = mock_exec.call_args[0]
            self.assertEqual(called_args[1], "agent")
            self.assertEqual(called_args[2], "read")
            self.assertEqual(called_args[3], "w1:p2")
            self.assertEqual(called_args[4], "--lines")
            self.assertEqual(called_args[5], "30")

    async def test_stop_agent_terminates_and_rolls_back_git(self):
        """Verify stop_agent kills pane and performs git reset/checkout rollback if requested."""
        git_repo = os.path.join(self.test_dir, "rollback_repo")
        os.makedirs(git_repo, exist_ok=True)
        p1 = await asyncio.create_subprocess_exec("git", "init", "-b", "main", cwd=git_repo)
        await p1.communicate()
        p_ce = await asyncio.create_subprocess_exec("git", "config", "user.email", "test@example.com", cwd=git_repo)
        await p_ce.communicate()
        p_cn = await asyncio.create_subprocess_exec("git", "config", "user.name", "Test Runner", cwd=git_repo)
        await p_cn.communicate()
        p2 = await asyncio.create_subprocess_exec("git", "commit", "--allow-empty", "-m", "init", cwd=git_repo)
        await p2.communicate()

        # Checkout task branch and add uncommitted file
        task_branch = "task-999-opencode"
        p3 = await asyncio.create_subprocess_exec("git", "checkout", "-b", task_branch, cwd=git_repo)
        await p3.communicate()

        dirty_file = os.path.join(git_repo, "dirty.txt")
        with open(dirty_file, "w") as f:
            f.write("uncommitted work")

        pane_id = "w3:p1"
        SPAWNED_AGENTS[pane_id] = {
            "pane_id": pane_id,
            "node": "hermes",
            "cwd": git_repo,
            "branch": task_branch,
            "base_branch": "main",
            "worker": "opencode-dev"
        }

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"", b"")
            mock_exec.return_value = mock_proc

            msg = await stop_agent(pane_id, rollback=True)
            self.assertIn("Stopped agent pane `w3:p1`", msg)
            self.assertIn("Rolled back working directory", msg)
            self.assertNotIn(pane_id, SPAWNED_AGENTS)

        # Verify git baseline restored: branch is main, dirty.txt deleted
        chk = await asyncio.create_subprocess_exec(
            "git", "rev-parse", "--abbrev-ref", "HEAD",
            cwd=git_repo,
            stdout=asyncio.subprocess.PIPE
        )
        out, _ = await chk.communicate()
        self.assertEqual(out.decode().strip(), "main")
        self.assertFalse(os.path.exists(dirty_file))


if __name__ == "__main__":
    unittest.main()
