#!/usr/bin/env python3
"""
tests/test_memory_manager.py
Unit tests for MemoryManager:
- Verify log_event appends valid JSONL.
- Verify get_recent_events retrieves tail in chronological order.
- Verify get_latest_task_state consolidates milestones and retrieves correct status.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import tempfile
import unittest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import golem  # noqa: F401
from memory_manager import MemoryManager


class TestMemoryManager(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.ledger_file = os.path.join(self.temp_dir.name, "test_ledger.jsonl")
        self.manager = MemoryManager(ledger_path=self.ledger_file)

    def tearDown(self):
        self.temp_dir.cleanup()
        MemoryManager._default_instance = None

    def test_log_event_appends_valid_jsonl(self):
        """Verify log_event appends valid JSONL records with ISO timestamp and type."""
        ev1 = self.manager.log_event(
            "task_dispatched",
            task_id="task-100",
            repo="/Users/hermes/projects/AGY",
            worker="opencode-dev",
            worktree="w9",
            cleaned=False
        )
        self.assertEqual(ev1["type"], "task_dispatched")
        self.assertEqual(ev1["task_id"], "task-100")
        self.assertFalse(ev1["cleaned"])
        self.assertIn("ts", ev1)

        # Inspect raw file
        self.assertTrue(os.path.isfile(self.ledger_file))
        with open(self.ledger_file, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        self.assertEqual(len(lines), 1)

        parsed = json.loads(lines[0])
        self.assertEqual(parsed["type"], "task_dispatched")
        self.assertEqual(parsed["worker"], "opencode-dev")
        self.assertEqual(parsed["worktree"], "w9")

    def test_get_recent_events_retrieves_chronological_tail(self):
        """Verify get_recent_events retrieves last N events in order."""
        for i in range(5):
            self.manager.log_event("step_milestone", step=i, task_id="task-100")

        recent = self.manager.get_recent_events(limit=3)
        self.assertEqual(len(recent), 3)
        self.assertEqual(recent[0]["step"], 2)
        self.assertEqual(recent[1]["step"], 3)
        self.assertEqual(recent[2]["step"], 4)

    def test_get_latest_task_state_consolidates_milestones(self):
        """Verify get_latest_task_state merges milestones for the active task."""
        # 1. Dispatch
        self.manager.log_event(
            "task_dispatched",
            task_id="task-427",
            repo="/Users/hermes/projects/AGY",
            worker="opencode-dev",
            worktree="w9",
            cleaned=False
        )
        # 2. PR Opened
        self.manager.log_event(
            "pr_opened",
            task_id="task-427",
            repo="/Users/hermes/projects/AGY",
            worker="opencode-dev",
            pr_number=427,
            branch="task/427_opencode_dev",
            worktree="w9",
            cleaned=False
        )

        state = self.manager.get_latest_task_state(repo="/Users/hermes/projects/AGY")
        self.assertEqual(state["task_id"], "task-427")
        self.assertEqual(state["pr_number"], 427)
        self.assertEqual(state["worktree"], "w9")
        self.assertFalse(state["cleaned"])

        # 3. Merge
        self.manager.log_event(
            "task_merged",
            task_id="task-427",
            repo="/Users/hermes/projects/AGY",
            worker="opencode-dev",
            pr_number=427,
            worktree="w9",
            cleaned=True
        )

        final_state = self.manager.get_latest_task_state(repo="/Users/hermes/projects/AGY")
        self.assertEqual(final_state["task_id"], "task-427")
        self.assertEqual(final_state["pr_number"], 427)
        self.assertEqual(final_state["worktree"], "w9")
        self.assertTrue(final_state["cleaned"])
        self.assertEqual(final_state["type"], "task_merged")

    def test_class_method_dual_invocation(self):
        """Verify MemoryManager supports direct class method invocation."""
        MemoryManager._default_instance = self.manager
        ev = MemoryManager.log_event("task_aborted", task_id="task-999", worktree="wA", cleaned=True)
        self.assertEqual(ev["type"], "task_aborted")

        latest = MemoryManager.get_latest_task_state()
        self.assertEqual(latest["task_id"], "task-999")
        self.assertTrue(latest["cleaned"])


if __name__ == "__main__":
    unittest.main()
