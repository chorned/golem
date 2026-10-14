#!/usr/bin/env python3
"""
tests/test_hop_budget_and_reaper.py
Covers:
  1. Hop-budget exhaustion: Step 2 synthesis is told the budget is spent so it reports done vs not-started
     instead of promising future execution.
  2. Session reaper records the TRUE prune reason (expired vs FAILED vs CLOSED) rather than always
     'inactivity_timeout'.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.ingress import IngressGateway, build_hop_budget_note
from golem.router import SessionRouter
from golem.session import SessionState


class TestHopBudgetNote(unittest.TestCase):
    def test_note_forbids_promissory_language_and_lists_pending(self):
        note = build_hop_budget_note(8, ["CHO-168", "CHO-169"])
        self.assertIn("TOOL BUDGET EXHAUSTED", note)
        self.assertIn("all 8 tool hops", note)
        self.assertIn("CHO-168, CHO-169", note)
        self.assertIn("NOT been started", note)
        self.assertIn("Nothing is running", note)
        self.assertIn("continue", note)

    def test_note_without_pending(self):
        note = build_hop_budget_note(5, [])
        self.assertNotIn("Pending subtasks", note)


class TestStep2BudgetNoteInjection(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        self.gateway = IngressGateway(router=router, bot_user_id="U_GOLEM")
        self.captured = []

        async def fake_call(payload, model=None, api_key=None):
            self.captured.append(payload)
            return {"candidates": [{"content": {"parts": [{"text": "briefing"}]}}]}

        self.gateway._call_gemini_api = fake_call

    async def _run(self, **extra):
        return await self.gateway._execute_step2_synthesis(
            sys_prompt="SYS",
            prompt="deploy",
            candidate={"content": {"parts": [{"functionCall": {"name": "dispatch_task", "args": {}}}]}},
            fn_call={"name": "dispatch_task", "args": {}},
            fn_name="dispatch_task",
            tool_result={"status": "SUCCESS"},
            model="m",
            api_key="k",
            **extra,
        )

    async def test_budget_note_appended_to_system_prompt(self):
        note = build_hop_budget_note(8, ["CHO-168"])
        text, err = await self._run(budget_note=note)
        self.assertEqual(text, "briefing")
        self.assertIsNone(err)
        sys_text = self.captured[0]["systemInstruction"]["parts"][0]["text"]
        self.assertIn("TOOL BUDGET EXHAUSTED", sys_text)

    async def test_no_note_by_default(self):
        await self._run()
        sys_text = self.captured[0]["systemInstruction"]["parts"][0]["text"]
        self.assertNotIn("TOOL BUDGET EXHAUSTED", sys_text)


class TestReaperReason(unittest.IsolatedAsyncioTestCase):
    def _session(self, state, expired):
        s = MagicMock()
        s.state = state
        s.is_expired.return_value = expired
        s.last_active_ts = time.time() - 30
        s.timeout_seconds = 14400.0
        s.last_prompt = "deploy tunnel"
        s.workspace_id = None
        s.close = AsyncMock()
        return s

    async def test_reasons_reflect_actual_cause(self):
        router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        router.sessions = {
            "expired": self._session(SessionState.IDLE, True),
            "failed": self._session(SessionState.FAILED, False),
            "closed": self._session(SessionState.CLOSED, False),
        }
        sessions = dict(router.sessions)
        pruned = await router.reap_expired_sessions()
        self.assertCountEqual(pruned, ["expired", "failed", "closed"])
        sessions["expired"].close.assert_awaited_once_with(reason="inactivity_timeout")
        sessions["failed"].close.assert_awaited_once_with(reason="session_failed")
        sessions["closed"].close.assert_awaited_once_with(reason="session_closed")


if __name__ == "__main__":
    unittest.main()
