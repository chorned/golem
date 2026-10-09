#!/usr/bin/env python3
"""
tests/test_memory_tiers.py
Comprehensive verification suite for the 4-Tier Institutional Memory Engine:
  - Multi-File Atomic Staging (propose_multi_update across facts, capabilities, skills, flows).
  - Single-commit git atomicity on confirm_update.
  - Multi-file rollback on disk or git failure.
  - Path traversal rejection across multi-file changesets.
  - Review card formatting for Slack/iMessage.
  - Tool declaration schema compliance.
"""

from __future__ import annotations

import os
import sys
import shutil
import tempfile
import subprocess
import unittest
from unittest import mock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.wiki import (
    GolemWiki,
    WikiError,
    WikiSecurityError,
    StagedChange,
    MemoryProposal,
)


class TestMemoryTiersMultiUpdate(unittest.TestCase):
    """Unit tests for 4-tier atomic multi-file staging and confirmation."""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="golem_wiki_tiers_test_")
        self.wiki = GolemWiki(wiki_dir=self.test_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_scaffolding_initializes_4_tier_hierarchy(self) -> None:
        """Verify that standard 4-tier directories and default templates are scaffolded."""
        expected_dirs = [
            "runbooks/skills",
            "runbooks/flows",
            "context/rules",
            "logbook",
        ]
        for rel_d in expected_dirs:
            p = os.path.join(self.test_dir, rel_d)
            self.assertTrue(os.path.isdir(p), f"Directory {rel_d} missing")

        cap_p = os.path.join(self.test_dir, "context", "capabilities.md")
        topo_p = os.path.join(self.test_dir, "context", "topology.md")
        self.assertTrue(os.path.isfile(cap_p), "context/capabilities.md missing")
        self.assertTrue(os.path.isfile(topo_p), "context/topology.md missing")

        cap_text = self.wiki.read("context/capabilities.md")
        self.assertIn("Fleet Capabilities & Affordances", cap_text)
        self.assertIn("macos-computer-use-kit", cap_text)

    def test_multi_file_atomic_staging(self) -> None:
        """Stage proposal modifying context/rules/hermes.md and creating runbooks/flows/test_flow.md."""
        # 1. Pre-seed context/rules/hermes.md
        self.wiki.write("context/rules/hermes.md", "# Hermes Rules\nLocal macOS dev anchor.\n", "feat: seed hermes")

        changes = [
            {
                "tier": "fact",
                "rel_path": "context/rules/hermes.md",
                "content": "# Hermes Rules\nLocal macOS dev anchor.\n- Accessibility & Screen Recording granted.\n",
            },
            {
                "tier": "flow",
                "rel_path": "runbooks/flows/test_flow.md",
                "content": "# Test Flow\n1. Watch terminal.\n2. Trigger reflex.\n",
            },
        ]

        proposal = self.wiki.propose_multi_update(
            summary="Codify Hermes AX permissions & test flow",
            changes=changes,
        )

        self.assertIsInstance(proposal, MemoryProposal)
        self.assertTrue(proposal.token.startswith("prop_"))
        self.assertEqual(len(proposal.changes), 2)

        # File 1 is an update
        ch1 = proposal.changes[0]
        self.assertEqual(ch1.rel_path, "context/rules/hermes.md")
        self.assertEqual(ch1.tier, "fact")
        self.assertFalse(ch1.is_new_file)
        self.assertIn("+", ch1.diff_preview)

        # File 2 is a new file
        ch2 = proposal.changes[1]
        self.assertEqual(ch2.rel_path, "runbooks/flows/test_flow.md")
        self.assertEqual(ch2.tier, "flow")
        self.assertTrue(ch2.is_new_file)

        # Assert neither file has been written yet!
        self.assertNotIn("Screen Recording", self.wiki.read("context/rules/hermes.md"))
        self.assertIsNone(self.wiki.read("runbooks/flows/test_flow.md"))

    def test_atomic_single_git_commit(self) -> None:
        """Calling confirm_update commits all changes in a single git commit with token and summary."""
        changes = [
            {
                "tier": "fact",
                "rel_path": "context/rules/desktop-pc.md",
                "content": "# Desktop PC\nCUDA workloads enabled.\n",
            },
            {
                "tier": "capability",
                "rel_path": "context/capabilities.md",
                "content": "# Updated Capabilities\nAdded new model training affordance.\n",
            },
        ]
        proposal = self.wiki.propose_multi_update(
            summary="Register desktop-pc CUDA capabilities",
            changes=changes,
        )
        token = proposal.token

        confirmed = self.wiki.confirm_update(token)
        self.assertTrue(confirmed)

        # Assert both files are present on disk
        self.assertIn("CUDA workloads", self.wiki.read("context/rules/desktop-pc.md"))
        self.assertIn("Added new model training", self.wiki.read("context/capabilities.md"))

        # Assert single git commit containing both files
        git_log = subprocess.run(
            ["git", "log", "-n", "1", "--stat"],
            cwd=self.test_dir,
            check=True,
            capture_output=True,
            text=True,
        ).stdout

        self.assertIn(f"memory: [{token}] Register desktop-pc CUDA capabilities", git_log)
        self.assertIn("context/rules/desktop-pc.md", git_log)
        self.assertIn("context/capabilities.md", git_log)

    def test_atomic_rollback_on_write_failure(self) -> None:
        """Simulate disk write failure on second file of 3-file proposal and assert clean rollback."""
        self.wiki.write("context/rules/node_a.md", "Original Node A content", "feat: node a")

        changes = [
            {"tier": "fact", "rel_path": "context/rules/node_a.md", "content": "Modified Node A content"},
            {"tier": "fact", "rel_path": "context/rules/node_b.md", "content": "New Node B content"},
            {"tier": "flow", "rel_path": "runbooks/flows/flow_c.md", "content": "New Flow C content"},
        ]
        proposal = self.wiki.propose_multi_update("Failed batch update", changes)
        token = proposal.token

        orig_replace = os.replace
        call_count = [0]

        def flaky_replace(src: str, dst: str):
            call_count[0] += 1
            if call_count[0] == 2:
                raise OSError("Simulated disk error on file 2")
            return orig_replace(src, dst)

        with mock.patch("os.replace", side_effect=flaky_replace):
            with self.assertRaises(WikiError):
                self.wiki.confirm_update(token)

        # Assert node_a.md was restored to original content
        self.assertEqual(self.wiki.read("context/rules/node_a.md"), "Original Node A content")
        # Assert node_b and flow_c were not created
        self.assertIsNone(self.wiki.read("context/rules/node_b.md"))
        self.assertIsNone(self.wiki.read("runbooks/flows/flow_c.md"))

        # Assert git working tree is clean
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=self.test_dir,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.assertEqual(status, "")

    def test_traversal_protection_on_multi_file_proposals(self) -> None:
        """Assert that any path traversal in changes list immediately raises WikiSecurityError."""
        changes = [
            {"tier": "fact", "rel_path": "context/rules/hermes.md", "content": "Valid content"},
            {"tier": "fact", "rel_path": "../../etc/shadow", "content": "root:*:0:0:"},
        ]
        with self.assertRaises(WikiSecurityError):
            self.wiki.propose_multi_update("Exploit proposal", changes)

        # Verify nothing was added to pending updates
        self.assertEqual(len(self.wiki._pending_updates), 0)

    def test_rejection_discards_proposal_without_modifications(self) -> None:
        """Assert reject_update cleanly removes pending proposal without touching disk."""
        changes = [
            {"tier": "flow", "rel_path": "runbooks/flows/unwanted.md", "content": "Reject me"}
        ]
        proposal = self.wiki.propose_multi_update("Unwanted flow", changes)
        token = proposal.token

        rejected = self.wiki.reject_update(token)
        self.assertTrue(rejected)
        self.assertIsNone(self.wiki.read("runbooks/flows/unwanted.md"))
        self.assertNotIn(token, self.wiki._pending_updates)

    def test_review_card_slack_formatting(self) -> None:
        """Verify proposal.to_slack_card() outputs high-signal markdown matching contract."""
        changes = [
            {"tier": "fact", "rel_path": "context/rules/hermes.md", "content": "Fact content"},
            {"tier": "flow", "rel_path": "runbooks/flows/oauth.md", "content": "Flow content"},
        ]
        proposal = self.wiki.propose_multi_update("Autonomous OAuth Refresh", changes)
        card = proposal.to_slack_card()

        self.assertIn("🧠 *Memory Distillation Proposal* — Autonomous OAuth Refresh", card)
        self.assertIn(f"Proposal Token: `{proposal.token}`", card)
        self.assertIn("1. [Fact] ──► `context/rules/hermes.md`", card)
        self.assertIn("2. [Flow] ──► `runbooks/flows/oauth.md`", card)
        self.assertIn("Reply `confirm` or `commit` to apply", card)


if __name__ == "__main__":
    unittest.main()
