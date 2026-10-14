#!/usr/bin/env python3
"""
tests/test_hand_discovery.py
Unit tests for HerdrHandSync and is_hand_pane filtering.
Verifies that:
1. Mouth liaison panes (labeled liaison, g_*, *-liaison, or unlabeled :p1 running agy) are never misidentified as Hands.
2. Fleet worker bridge panes (oci-worker, laptop-worker, pc-worker, chief-of-staff) are hidden.
3. Actual worker hands (worktree panes, dedicated hand tabs, task panes) are correctly discovered.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock
from golem.webui.hand_discovery import (
    is_hand_pane,
    hand_fields,
    DEFAULT_HIDE_LABELS,
)


class TestHandDiscoveryFiltering(unittest.TestCase):
    def setUp(self):
        self.hide_labels = {s.strip() for s in DEFAULT_HIDE_LABELS.split(",") if s.strip()}

    def test_liaison_panes_hidden(self):
        """Verifies liaison panes with various naming patterns are never identified as hand panes."""
        # 1. Unlabeled root pane running agy (like w2T:p1 from the incident)
        incident_pane = {
            "agent": "agy",
            "agent_status": "idle",
            "cwd": "/Users/hermes/projects/AGY",
            "focused": False,
            "foreground_cwd": "/Users/hermes/projects/AGY",
            "pane_id": "w2T:p1",
            "workspace_id": "w2T",
        }
        self.assertFalse(is_hand_pane(incident_pane, self.hide_labels))

        # 2. Slack session liaison labeled with g_ prefix
        slack_liaison_pane = {
            "agent": "agy",
            "agent_status": "idle",
            "cwd": "/Users/hermes/projects/AGY",
            "label": "g_d0c2j1hknn7_1791822135_115089",
            "pane_id": "w2T:p1",
        }
        self.assertFalse(is_hand_pane(slack_liaison_pane, self.hide_labels))

        # 3. Explicit liaison label
        explicit_liaison = {
            "agent": "agy",
            "agent_status": "idle",
            "label": "liaison",
            "pane_id": "w2T:p1",
        }
        self.assertFalse(is_hand_pane(explicit_liaison, self.hide_labels))

        # 4. Standard session-liaison label
        session_liaison = {
            "agent": "agy",
            "agent_status": "idle",
            "label": "fm-sess-liaison",
            "pane_id": "w2T:p1",
        }
        self.assertFalse(is_hand_pane(session_liaison, self.hide_labels))

        # 5. chief-of-staff liaison
        cos_pane = {
            "agent": "agy",
            "agent_status": "idle",
            "label": "chief-of-staff",
            "pane_id": "w6:p1",
        }
        self.assertFalse(is_hand_pane(cos_pane, self.hide_labels))

    def test_fleet_worker_panes_hidden(self):
        """Verifies transport bridge panes to remote nodes are excluded from hand cards."""
        for label in ("oci-worker", "laptop-worker", "pc-worker"):
            pane = {
                "agent_status": "unknown",
                "cwd": "/Users/hermes",
                "label": label,
                "pane_id": "w3:p1",
            }
            self.assertFalse(is_hand_pane(pane, self.hide_labels))

    def test_actual_worker_hands_detected(self):
        """Verifies actual worker hands (worktrees or non-liaison agent panes) are discovered."""
        # Task pane in Herdr worktree
        worktree_pane = {
            "agent_status": "working",
            "cwd": "/Users/hermes/.herdr/worktrees/task-123",
            "foreground_cwd": "/Users/hermes/.herdr/worktrees/task-123",
            "label": "task-worker-123",
            "pane_id": "w2T:p2",
        }
        self.assertTrue(is_hand_pane(worktree_pane, self.hide_labels))

        # Dedicated worker hand pane
        hand_pane = {
            "agent": "opencode",
            "agent_status": "running",
            "label": "hand-refactor",
            "pane_id": "w2T:p3",
            "cwd": "/Users/hermes/projects/first-mate",
        }
        self.assertTrue(is_hand_pane(hand_pane, self.hide_labels))


if __name__ == "__main__":
    unittest.main()
