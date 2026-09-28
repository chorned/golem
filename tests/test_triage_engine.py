#!/usr/bin/env python3
"""
tests/test_triage_engine.py
Unit test suite for triage_engine.py:
- Strict conversational classification for system state and task history inquiries.
- Direct 1-2 sentence conversational turn handling without worktree spawning.
- Elimination of hardcoded alignment card templates ("refactor the auth handler").
- Dynamic derivation of target_file and intent from user prompt.
"""

import os
import sys
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import triage_engine
from triage_engine import (
    is_conversational_query,
    handle_conversational_turn,
    direct_gemini_triage,
    build_fallback_intent_triage,
    build_fallback_preflight_plan
)


class TestTriageEngine(unittest.IsolatedAsyncioTestCase):

    def test_conversational_query_detection_for_state_and_history_inquiries(self):
        """Verify queries about past actions, task status, and cleanup classify as conversational."""
        state_queries = [
            "well did you clean up opencodes work?",
            "did you clean up?",
            "did you clean up opencode's work?",
            "what branches are open?",
            "what worktrees are open?",
            "is opencode finished?",
            "is opencode done?",
            "have you cleaned up?",
            "what happened?",
            "why did that fail?",
            "who are you?",
            "can you explain this?",
            "status on opencode?"
        ]
        for query in state_queries:
            with self.subTest(query=query):
                self.assertTrue(
                    is_conversational_query(query),
                    f"Query '{query}' should be classified as conversational query"
                )

    def test_operational_action_queries_not_classified_as_conversational(self):
        """Verify imperative action commands are NOT classified as conversational."""
        action_queries = [
            "clean up the worktree wA",
            "run docker ps",
            "refactor auth.py",
            "git status -s",
            "restart the plex container",
            "have opencode create tests/test_token_calc.py"
        ]
        for query in action_queries:
            with self.subTest(query=query):
                self.assertFalse(
                    is_conversational_query(query),
                    f"Action command '{query}' should NOT be conversational"
                )

    async def test_well_did_you_clean_up_opencodes_work_classifies_as_converse_and_returns_direct_answer(self):
        """
        Verify 'well did you clean up opencodes work?' classifies as converse
        and returns a direct 1-2 sentence answer without generating a worker plan or Herdr worktree.
        """
        prompt = "well did you clean up opencodes work?"

        # Test via direct_gemini_triage (with mocked or real herdr client)
        mock_herdr = AsyncMock()
        mock_herdr.list_worktrees = AsyncMock(return_value=[
            {"branch": "main", "is_linked_worktree": False, "path": PROJECT_DIR}
        ])

        with patch("triage_engine.handle_conversational_turn", wraps=handle_conversational_turn) as mock_handle:
            result = await direct_gemini_triage(
                user_prompt=prompt,
                target_info={"workspace_label": "fleet-control", "target_machine": "local", "role": "Chief of Staff"}
            )

            # Verification:
            # 1. Classified as converse
            self.assertEqual(result.get("turn_type"), "CONVERSE")
            self.assertEqual(result.get("action_type"), "converse")
            self.assertIsNone(result.get("action_plan"))

            # 2. Returns direct conversational reply
            reply = result.get("conversational_reply")
            self.assertIsNotNone(reply)
            self.assertTrue(len(reply) > 5)
            self.assertIn("clean", reply.lower())

            # 3. No action plan or worktree created
            self.assertIsNone(result.get("action_plan"))

    async def test_handle_conversational_turn_returns_ground_truth_for_open_worktrees(self):
        """Verify handle_conversational_turn accurately reports open worktrees when present."""
        mock_herdr = AsyncMock()
        mock_herdr.list_worktrees = AsyncMock(return_value=[
            {"branch": "main", "is_linked_worktree": False, "path": PROJECT_DIR},
            {"branch": "task-test-active", "is_linked_worktree": True, "path": "/tmp/test_wt"}
        ])

        result = await handle_conversational_turn(
            user_prompt="what branches are open?",
            repo_path=PROJECT_DIR,
            herdr_client_inst=mock_herdr
        )

        self.assertEqual(result["turn_type"], "CONVERSE")
        self.assertEqual(result["action_type"], "converse")
        self.assertIn("task-test-active", result["conversational_reply"])

    async def test_handle_conversational_turn_reports_clean_when_no_worktrees(self):
        """Verify handle_conversational_turn reports clean state when no linked worktrees exist."""
        mock_herdr = AsyncMock()
        mock_herdr.list_worktrees = AsyncMock(return_value=[
            {"branch": "main", "is_linked_worktree": False, "path": PROJECT_DIR}
        ])

        result = await handle_conversational_turn(
            user_prompt="did you clean up opencodes work?",
            repo_path=PROJECT_DIR,
            herdr_client_inst=mock_herdr
        )

        self.assertEqual(result["turn_type"], "CONVERSE")
        self.assertEqual(result["action_type"], "converse")
        self.assertIn("cleaned up", result["conversational_reply"].lower())

    def test_fallback_plan_extracts_target_file_and_excludes_auth_handler(self):
        """Verify fallback plan dynamically extracts target_file and excludes hardcoded auth handler."""
        prompt = "Have opencode create a helper in tests/test_token_calc.py that computes token savings"
        plan = build_fallback_preflight_plan(prompt, {"workspace_label": "fleet-control", "target_machine": "local"})

        self.assertEqual(plan.get("target_file"), "tests/test_token_calc.py")
        self.assertNotIn("refactor the auth handler", plan.get("intent_summary", ""))
        self.assertIn("tests/test_token_calc.py", plan.get("intent_summary", ""))

    def test_fallback_intent_triage_classifies_conversational_questions(self):
        """Verify build_fallback_intent_triage classifies state questions as converse."""
        res = build_fallback_intent_triage("well did you clean up opencodes work?")
        self.assertEqual(res["turn_type"], "CONVERSE")
        self.assertEqual(res["action_type"], "converse")
        self.assertIsNotNone(res["conversational_reply"])
        self.assertIn("clean", res["conversational_reply"].lower())


if __name__ == "__main__":
    unittest.main()
