#!/usr/bin/env python3
"""
tests/test_no_mistakes.py
Unit tests for no-mistakes automated review/testing gate,
autonomous GitHub PR creation, and Tier 3 auto-merge execution.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import golem  # noqa: F401
from supervisor import AgentSupervisor
from github_manager import GitHubManager
from herdr_client import HerdrClient


class TestNoMistakesGate(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.herdr = HerdrClient()
        self.gh = GitHubManager()
        self.supervisor = AgentSupervisor(
            herdr_client=self.herdr,
            github_manager=self.gh,
            default_repo_path="/tmp/mock_repo"
        )
        self.patch_log_event = patch("memory_manager.MemoryManager.log_event")
        self.mock_log_event = self.patch_log_event.start()
        self.addCleanup(self.patch_log_event.stop)

    def test_parse_no_mistakes_toon_passed(self):
        """Verify parsing TOON report indicating clean pass."""
        toon_output = (
            "run:\n"
            "  id: run_84710\n"
            "  status: passed\n"
            "  outcome: accepted\n"
            "  gate: review\n"
            "findings:\n"
        )
        report = self.supervisor.parse_no_mistakes_report(toon_output, exit_code=0)
        self.assertTrue(report["passed"])
        self.assertEqual(report["outcome"], "accepted")
        self.assertEqual(len(report["findings"]), 0)

    def test_parse_no_mistakes_toon_findings_rejected(self):
        """Verify parsing TOON report with lint/review findings."""
        toon_output = (
            "run:\n"
            "  id: run_84711\n"
            "  status: blocked\n"
            "  outcome: rejected\n"
            "findings:\n"
            "  - auth.py:42: Unused import 'jwt_decode'\n"
            "  - auth.py:98: Missing docstring on authenticate_request\n"
        )
        report = self.supervisor.parse_no_mistakes_report(toon_output, exit_code=1)
        self.assertFalse(report["passed"])
        self.assertEqual(report["outcome"], "rejected")
        self.assertEqual(len(report["findings"]), 2)
        self.assertIn("auth.py:42", report["findings"][0])

    def test_parse_no_mistakes_json_format(self):
        """Verify parsing JSON structured output from no-mistakes."""
        json_output = json.dumps({
            "status": "passed",
            "outcome": "accepted",
            "findings": [],
            "summary": "All tests, linters, and review gates passed cleanly."
        })
        report = self.supervisor.parse_no_mistakes_report(json_output, exit_code=0)
        self.assertTrue(report["passed"])
        self.assertEqual(report["outcome"], "accepted")
        self.assertIn("All tests", report["summary"])

    async def test_run_no_mistakes_invokes_cli(self):
        """Verify run_no_mistakes executes `no-mistakes axi run --intent <intent> -y`."""
        toon_passed = "outcome: accepted\nfindings:\n"
        with patch("os.path.exists", return_value=True), \
             patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:

            mock_proc = AsyncMock()
            mock_proc.communicate.return_value = (toon_passed.encode("utf-8"), b"")
            mock_proc.returncode = 0
            mock_exec.return_value = mock_proc

            exit_code, stdout, stderr, parsed = await self.supervisor.run_no_mistakes(
                repo_path="/tmp/mock_repo",
                intent="Refactor authentication handler"
            )

            mock_exec.assert_called_once()
            args = mock_exec.call_args[0]
            self.assertIn("axi", args)
            self.assertIn("run", args)
            self.assertIn("--intent", args)
            self.assertIn("Refactor authentication handler", args)
            self.assertTrue(parsed["passed"])

    async def test_supervise_task_creates_github_pr_on_pass(self):
        """Verify supervisor creates GitHub PR via gh CLI when no-mistakes validation succeeds."""
        mock_wt = {
            "success": True,
            "branch": "task/auth_jwt",
            "workspace_id": "w9",
            "pane_id": "w9:p1",
            "worktree_path": "/tmp/mock_repo/wt"
        }
        mock_dispatch = {
            "worker_id": "opencode-dev",
            "pane_id": "w9:p1",
            "output": "Implementation completed"
        }
        mock_nm = {
            "passed": True,
            "outcome": "accepted",
            "findings": [],
            "summary": "Checks passed"
        }
        mock_pr = {
            "success": True,
            "pr_url": "https://github.com/carlhorned/AGY/pull/42",
            "pr_number": 42
        }

        with patch.object(self.herdr, "create_worktree", new_callable=AsyncMock, return_value=mock_wt), \
             patch.object(self.herdr, "dispatch_worker", new_callable=AsyncMock, return_value=mock_dispatch), \
             patch.object(self.supervisor, "run_no_mistakes", new_callable=AsyncMock, return_value=(0, "accepted", "", mock_nm)), \
             patch.object(self.supervisor, "get_git_diff", new_callable=AsyncMock, return_value="auth.py | 4 ++--"), \
             patch.object(self.gh, "create_pull_request", new_callable=AsyncMock, return_value=mock_pr) as mock_create_pr:

            report = await self.supervisor.supervise_task(
                worker_id="opencode-dev",
                user_intent="Refactor JWT tokens",
                repo_path="/tmp/mock_repo"
            )

            self.assertEqual(report["status"], "success")
            self.assertEqual(report["test_status"], "passed")
            self.assertEqual(report["pr_url"], "https://github.com/carlhorned/AGY/pull/42")
            self.assertEqual(report["pr_number"], 42)

            mock_create_pr.assert_called_once()
            call_kwargs = mock_create_pr.call_args[1]
            self.assertIn("Refactor JWT tokens", call_kwargs["title"])
            self.assertEqual(call_kwargs["branch"], report["branch"])

    async def test_secondary_account_attribution_for_desktop_pc_worker(self):
        """Verify secondary GitHub machine account on desktop-pc is recognized during PR creation."""
        with patch.object(self.gh, "run_gh", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = (0, "https://github.com/carlhorned/AGY/pull/77\n", "")

            pr_res = await self.gh.create_pull_request(
                repo_path="/tmp/mock_repo",
                title="CUDA model eval update",
                body="Upgraded PyTorch benchmark suite.",
                branch="task/cuda_eval",
                author_node="desktop-pc"
            )

            self.assertTrue(pr_res["success"])
            self.assertEqual(pr_res["pr_number"], 77)

            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            body_arg = args[args.index("--body") + 1]
            self.assertIn("Authored by Secondary Machine Account on `desktop-pc`", body_arg)

    async def test_merge_pull_request_tier3_execution(self):
        """Verify Tier 3 auto-merge executes `gh pr merge <selector> --auto --squash --delete-branch`."""
        with patch.object(self.gh, "run_gh", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = (0, "Merged PR #42 via squash\n", "")

            res = await self.gh.merge_pull_request(
                repo_path="/tmp/mock_repo",
                pr_selector="42",
                auto=True,
                squash=True,
                delete_branch=True
            )

            self.assertTrue(res["success"])
            mock_run.assert_called_once()
            cmd_args = mock_run.call_args[0][0]
            self.assertEqual(cmd_args, ["pr", "merge", "42", "--auto", "--squash", "--delete-branch"])

    async def test_infrastructure_error_on_worktree_creation_failure(self):
        """
        Verify supervisor fails fast when Herdr worktree creation fails:
        aborts immediately with test_status='infrastructure_error' and iterations=0,
        without calling prompt_worker or entering remediation loops.
        """
        mock_wt_failure = {
            "success": False,
            "error": "no herdr server is running at /Users/hermes/.config/herdr/herdr.sock",
            "branch": "task/999_opencode_dev",
            "workspace_id": None,
            "pane_id": None,
            "worktree_path": "/Users/hermes/.herdr/worktrees/AGY/task-999_opencode_dev"
        }

        with patch.object(self.herdr, "create_worktree", new_callable=AsyncMock, return_value=mock_wt_failure), \
             patch.object(self.herdr, "dispatch_worker", new_callable=AsyncMock) as mock_dispatch, \
             patch.object(self.herdr, "prompt_worker", new_callable=AsyncMock) as mock_prompt, \
             patch.object(self.supervisor, "run_no_mistakes", new_callable=AsyncMock) as mock_nm:

            report = await self.supervisor.supervise_task(
                worker_id="opencode-dev",
                user_intent="Refactor JWT tokens",
                repo_path="/tmp/mock_repo"
            )

            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["test_status"], "infrastructure_error")
            self.assertTrue(report.get("is_infra_error"))
            self.assertEqual(report["iterations"], 0)
            self.assertIn("Infrastructure Error", report["summary"])
            mock_dispatch.assert_not_called()
            mock_prompt.assert_not_called()
            mock_nm.assert_not_called()

            briefing = self.supervisor.format_review_briefing(report)
            self.assertIn(":x: *Infrastructure Error:*", briefing)
            self.assertIn("0 remediation attempts", briefing)

    async def test_infrastructure_error_on_missing_directory_fails_fast(self):
        """
        Verify run_no_mistakes and supervise_task fail fast with an infrastructure error
        when the worktree directory is not found on disk, avoiding wasting remediation attempts.
        """
        non_existent_path = "/non/existent/worktree/path"
        exit_code, stdout, stderr, report = await self.supervisor.run_no_mistakes(
            repo_path=non_existent_path,
            intent="Refactor authentication handler"
        )

        self.assertEqual(exit_code, 2)
        self.assertTrue(report.get("is_infra_error"))
        self.assertFalse(report.get("passed"))
        self.assertEqual(report.get("outcome"), "infrastructure_error")
        self.assertIn(non_existent_path, report.get("summary", ""))


if __name__ == "__main__":
    unittest.main()
