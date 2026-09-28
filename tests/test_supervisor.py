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
"""

import os
import sys
import json
import time
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app
import triage_engine
from herdr_fleet import HerdrFleet
from herdr_client import HerdrClient, InfrastructureError
from supervisor import AgentSupervisor


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

    async def test_tier2_worker_alignment_and_supervision_on_go(self):
        """
        Verify Tier 2 task formats First-Person Pre-Flight Alignment Card,
        latches on `go` or 👍, and triggers supervise_task.
        """
        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "2000.002"})
        channel = "C_DEV"
        thread_ts = "2000.001"
        session_key = f"{channel}:{thread_ts}"

        event = {
            "channel": channel,
            "ts": thread_ts,
            "text": "brief our OpenCode worker in dev-grid (routed via FrugaLLM) to refactor the auth handler on an isolated branch"
        }

        # Step 1: Initial user prompt
        await app.process_user_prompt(event, client)

        # Verify Pre-Flight Alignment Card was posted
        client.chat_postMessage.assert_called_once()
        card_text = client.chat_postMessage.call_args[1]["text"]
        self.assertIn("I'll have OpenCode", card_text)
        self.assertIn("isolated worktree via FrugaLLM (:61721)", card_text)
        self.assertIn("Hit me with `go` (or 👍) to start", card_text)
        self.assertNotIn("• *Assumption:*", card_text)
        self.assertIn(session_key, app.pending_alignment)

        plan = app.pending_alignment[session_key]["plan"]
        self.assertEqual(plan.get("safety_tier"), 2)
        self.assertTrue(plan.get("use_supervisor"))

        # Step 2: Carl replies "go" to approve
        client.chat_postMessage.reset_mock()
        event_go = {
            "channel": channel,
            "ts": "2000.002",
            "thread_ts": thread_ts,
            "text": "go"
        }

        mock_sup_report = {
            "status": "success",
            "test_status": "passed",
            "worker_id": "opencode-dev",
            "branch": "task/2000_opencode_dev",
            "iterations": 1,
            "diff_stat": "auth.py | 4 ++--",
            "diff": "--- auth.py\n+++ auth.py",
            "summary": "Worker passed all tests."
        }

        with patch.object(self.supervisor, "supervise_task", new_callable=AsyncMock, return_value=mock_sup_report) as mock_sup:
            await app.process_user_prompt(event_go, client)

            self.assertNotIn(session_key, app.pending_alignment)
            mock_sup.assert_called_once()

            # Verify executive briefing was posted with test results
            client.chat_postMessage.assert_called_once()
            final_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Intern Review Passed", final_text)
            self.assertIn("opencode-dev", final_text)

    async def test_tier3_mutating_action_blocks_go_and_requires_approve_or_do_it(self):
        """
        Verify Tier 3 action (git push / merge to main / live restart):
        1. Displays Tier 3 gate warning in alignment card.
        2. Rejects simple 'go' or 👍 with warning; plan remains latched.
        3. Executes only when explicitly replied with 'approve' or 'do it'.
        """
        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "3000.002"})
        channel = "C_PROD"
        thread_ts = "3000.001"
        session_key = f"{channel}:{thread_ts}"

        event = {
            "channel": channel,
            "ts": thread_ts,
            "text": "git push origin main and restart container"
        }

        # Step 1: Initial prompt
        await app.process_user_prompt(event, client)

        self.assertIn(session_key, app.pending_alignment)
        plan = app.pending_alignment[session_key]["plan"]
        self.assertEqual(plan.get("safety_tier"), 3)

        card_text = client.chat_postMessage.call_args[1]["text"]
        self.assertIn("Tier 3 Blast-Radius Safety Gate", card_text)
        self.assertIn("Reply with `approve` or `do it` to execute.", card_text)

        # Step 2: Carl replies "go" -> MUST BE REJECTED
        client.chat_postMessage.reset_mock()
        event_go = {
            "channel": channel,
            "ts": "3000.002",
            "thread_ts": thread_ts,
            "text": "go"
        }

        with patch.object(app, "execute_approved_plan", new_callable=AsyncMock) as mock_exec:
            await app.process_user_prompt(event_go, client)

            # Plan MUST still be latched
            self.assertIn(session_key, app.pending_alignment)
            mock_exec.assert_not_called()

            # Rejection warning posted
            client.chat_postMessage.assert_called_once()
            reject_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Tier 3 Blast-Radius Safety Gate", reject_text)
            self.assertIn("A simple `go` is disabled", reject_text)

        # Step 3: Carl replies "do it" -> APPROVED AND EXECUTED
        client.chat_postMessage.reset_mock()
        event_do_it = {
            "channel": channel,
            "ts": "3000.003",
            "thread_ts": thread_ts,
            "text": "do it"
        }

        with patch.object(app, "execute_approved_plan", new_callable=AsyncMock) as mock_exec:
            await app.process_user_prompt(event_do_it, client)

            # Plan unlatched
            self.assertNotIn(session_key, app.pending_alignment)
            mock_exec.assert_called_once()

    async def test_tier3_emoji_reaction_approval_is_rejected(self):
        """Verify emoji reaction approval (👍) is rejected for Tier 3 mutating actions."""
        client = AsyncMock()
        channel = "C_REACTION"
        msg_ts = "4000.001"
        session_key = f"{channel}:{msg_ts}"

        app.pending_alignment[session_key] = {
            "plan": {
                "safety_tier": 3,
                "dispatched_payload": "git push origin main",
                "intent_summary": "Push changes to production"
            },
            "target_info": {
                "workspace_label": "fleet-control",
                "target_machine": "local",
                "role": "Chief of Staff"
            },
            "channel": channel,
            "thread_ts": msg_ts,
            "alignment_msg_ts": msg_ts
        }
        app.pending_alignment_by_msg_ts[msg_ts] = session_key

        reaction_event = {
            "type": "reaction_added",
            "user": "U12345",
            "reaction": "thumbsup",
            "item": {
                "type": "message",
                "channel": channel,
                "ts": msg_ts
            }
        }

        with patch.object(app, "execute_approved_plan", new_callable=AsyncMock) as mock_exec:
            await app.handle_reaction_added(reaction_event, client)

            # Execution blocked
            mock_exec.assert_not_called()
            # Plan still intact
            self.assertIn(session_key, app.pending_alignment)

            client.chat_postMessage.assert_called_once()
            reject_msg = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Tier 3 Blast-Radius Safety Gate", reject_msg)
            self.assertIn("Emoji reaction approval is disabled", reject_msg)

    def test_banned_python_c_command_sanitization(self):
        """Verify hallucinated python -c dummy commands are banned and sanitized for OpenCode."""
        hallucinated = 'python -c "import os; os.environ[\'AGY_MODE\']=\'dev\'; print(\'ready\')"'
        sanitized = triage_engine.validate_action_payload(hallucinated, target_node="hermes", worker_id="opencode-dev")
        self.assertEqual(
            sanitized,
            "Herdr worktree create -> Dispatch opencode-dev in dev-grid (FrugaLLM :61721) -> no-mistakes review"
        )
        self.assertNotIn("python -c", sanitized)

    def test_tier2_alignment_card_payload_cleans_up_python_c(self):
        """Verify format_alignment_briefing formats streamlined 2-line card and excludes python -c."""
        plan = {
            "is_self": False,
            "target_node": "hermes",
            "delegated_worker": "OpenCode",
            "worker_id": "opencode-dev",
            "domain": "codebase development",
            "dispatched_payload": 'python -c "import os; os.environ[\'AGY_MODE\']=\'dev\'"',
            "intent_summary": "refactor the auth handler",
            "assumptions": "Isolated task branch in dev-grid.",
            "choice": "Automated review vs manual inspect.",
            "recommendation": "Proceed with branch creation and automated test review.",
            "safety_tier": 2,
            "use_supervisor": True
        }
        card = app.format_alignment_briefing(plan)
        self.assertIn("I'll have OpenCode", card)
        self.assertIn("isolated worktree via FrugaLLM (:61721)", card)
        self.assertNotIn("python -c", card)
        self.assertNotIn("• *Assumption:*", card)

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

    async def test_supervisor_heartbeat_loop_triggers_callback(self):
        """Verify _run_heartbeat queries snapshot and executes callback with status and message."""
        mock_snapshot = {
            "result": {
                "snapshot": {
                    "panes": [
                        {
                            "pane_id": "w9:p1",
                            "agent_status": "blocked",
                            "terminal_title": "OC | Fixing missing slack_bolt dependency …"
                        }
                    ]
                }
            }
        }
        self.mock_herdr.get_snapshot = AsyncMock(return_value=mock_snapshot)

        received_heartbeats = []

        async def heartbeat_cb(data):
            received_heartbeats.append(data)

        task = asyncio.create_task(
            self.supervisor._run_heartbeat(
                pane_id="w9:p1",
                workspace_id="w9",
                worker_id="opencode-dev",
                agent_name="opencode-12345",
                callback=heartbeat_cb,
                interval=0.05
            )
        )

        await asyncio.sleep(0.12)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

        self.assertTrue(len(received_heartbeats) >= 1)
        hb = received_heartbeats[0]
        self.assertEqual(hb["agent_status"], "blocked")
        self.assertIn("Fixing missing slack_bolt dependency", hb["terminal_title"])
        self.assertIn("OpenCode appears blocked on", hb["message"])

    async def test_in_flight_interrogation_returns_ground_truth(self):
        """Verify in-flight message in active task thread returns immediate snapshot ground truth."""
        channel = "C_ACTIVE"
        thread_ts = "1720000000.123"
        session_key = f"{channel}:{thread_ts}"

        app.active_supervised_tasks[session_key] = {
            "worker_id": "opencode-dev",
            "start_time": time.time() - 45.0,
            "pane_id": "w9:p1",
            "workspace_id": "w9",
            "agent_name": "opencode-task-99"
        }

        mock_snapshot = {
            "result": {
                "snapshot": {
                    "panes": [
                        {
                            "pane_id": "w9:p1",
                            "workspace_id": "w9",
                            "agent_status": "blocked",
                            "terminal_title": "OC | Fixing missing slack_bolt dependency …"
                        }
                    ]
                }
            }
        }

        mock_client = AsyncMock()
        with patch.object(self.mock_herdr, "get_snapshot", return_value=mock_snapshot), \
             patch("app.client_herdr", self.mock_herdr):

            event = {
                "channel": channel,
                "ts": "1720000045.001",
                "thread_ts": thread_ts,
                "text": "Are you sure nothing is wrong?"
            }

            await app.process_user_prompt(event, mock_client)

            mock_client.chat_postMessage.assert_called_once()
            call_text = mock_client.chat_postMessage.call_args[1]["text"]
            self.assertIn("I just checked the Herdr pane", call_text)
            self.assertIn("OpenCode status is 'blocked'", call_text)
            self.assertIn("OC | Fixing missing slack_bolt dependency …", call_text)
            self.assertIn("I can kill this pane and have Hermes Butler take over if you'd prefer", call_text)
            self.assertIn("kill", call_text)
            self.assertIn("abort", call_text)

        # Cleanup
        app.active_supervised_tasks.clear()

    async def test_supervisor_heartbeat_loop_working_status_posts_peer_message(self):
        """Verify _run_heartbeat posts peer working message when agent_status is 'working'."""
        mock_snapshot = {
            "result": {
                "snapshot": {
                    "panes": [
                        {
                            "pane_id": "w4:p2",
                            "agent_status": "working",
                            "terminal_title": "opencode active turn"
                        }
                    ]
                }
            }
        }
        self.mock_herdr.get_snapshot = AsyncMock(return_value=mock_snapshot)

        received_heartbeats = []

        async def heartbeat_cb(data):
            received_heartbeats.append(data)

        task = asyncio.create_task(
            self.supervisor._run_heartbeat(
                pane_id="w4:p2",
                workspace_id="w4",
                worker_id="opencode-dev",
                agent_name="opencode-working-1",
                callback=heartbeat_cb,
                interval=0.05
            )
        )

        await asyncio.sleep(0.12)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

        self.assertTrue(len(received_heartbeats) >= 1)
        hb = received_heartbeats[0]
        self.assertEqual(hb["agent_status"], "working")
        self.assertEqual(hb["message"], "FYI, OpenCode is actively working in the worktree. FrugaLLM stream is flowing.")

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

    def test_alignment_card_strings_dynamically_derived_from_input(self):
        """Verify alignment card strings dynamically derive target_file and action from input without hardcoded templates."""
        # 1. Test creation of helper in tests/test_token_calc.py
        plan_create = {
            "is_self": False,
            "target_node": "hermes",
            "delegated_worker": "OpenCode",
            "worker_id": "opencode-dev",
            "target_file": "tests/test_token_calc.py",
            "intent_summary": "create tests/test_token_calc.py",
            "safety_tier": 2,
            "use_supervisor": True
        }
        card_create = app.format_alignment_briefing(plan_create)
        self.assertIn("I'll have OpenCode create `tests/test_token_calc.py` on an isolated worktree via FrugaLLM (:61721).", card_create)
        self.assertIn("Hit me with `go` (or 👍) to start, or let me know what to tweak.", card_create)
        self.assertNotIn("refactor the auth handler", card_create)
        self.assertNotIn("• *Assumption:*", card_create)

        # 2. Test refactor of utils/token.py
        plan_refactor = {
            "is_self": False,
            "target_node": "hermes",
            "delegated_worker": "OpenCode",
            "worker_id": "opencode-dev",
            "target_file": "utils/token.py",
            "intent_summary": "refactor utils/token.py",
            "safety_tier": 2,
            "use_supervisor": True
        }
        card_refactor = app.format_alignment_briefing(plan_refactor)
        self.assertIn("I'll have OpenCode refactor `utils/token.py` on an isolated worktree via FrugaLLM (:61721).", card_refactor)
        self.assertIn("Hit me with `go` (or 👍) to start, or let me know what to tweak.", card_refactor)
        self.assertNotIn("refactor the auth handler", card_refactor)
        self.assertNotIn("• *Assumption:*", card_refactor)


if __name__ == "__main__":
    unittest.main()
