#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

tests/test_stage6_wiki.py
Stage 6 Verification Suite: Git-Backed Memory Wiki & /learn Gate:
  - Repository initialization at isolated path (runbooks/, context/, logbook/).
  - Concurrency safety with asyncio.Lock and fcntl.flock().
  - Atomic write, git commit metadata, and rollback verification.
  - Path traversal security rejection.
  - Auto-commit logbook append vs [y/n] user confirmation gate for runbooks/context.
  - Migration script execution ingesting legacy rules and skills.
"""

from __future__ import annotations

import os
import sys
import shutil
import tempfile
import asyncio
import subprocess
import unittest
from unittest import mock
from typing import List

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.wiki import GolemWiki, WikiSecurityError
from golem.scripts.migrate_learned import migrate_legacy_knowledge


class TestGolemWikiInitialization(unittest.TestCase):
    """Unit tests for GolemWiki git repository initialization and structure."""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="golem_wiki_init_test_")

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_init_creates_standard_hierarchy_and_git(self) -> None:
        wiki = GolemWiki(wiki_dir=self.test_dir)
        for subdir in ("runbooks", "context", "logbook"):
            self.assertTrue(
                os.path.isdir(os.path.join(self.test_dir, subdir)),
                f"Subdirectory {subdir} was not created",
            )
        self.assertTrue(os.path.isdir(os.path.join(self.test_dir, ".git")))
        self.assertTrue(os.path.isfile(os.path.join(self.test_dir, "README.md")))
        self.assertTrue(os.path.isfile(os.path.join(self.test_dir, ".gitignore")))

        # Check git log has initial commit
        res = subprocess.run(
            ["git", "log", "-n", "1", "--oneline"],
            cwd=self.test_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("chore(wiki): initialize", res.stdout)


class TestGolemWikiOperations(unittest.TestCase):
    """Unit tests for atomic write, read, search, and rollback in GolemWiki."""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="golem_wiki_ops_test_")
        self.wiki = GolemWiki(wiki_dir=self.test_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_write_and_read_roundtrip(self) -> None:
        content = "# Production Deploy Runbook\n\n1. Run migrations.\n2. Restart service.\n"
        self.wiki.write("runbooks/deploy.md", content, "feat(runbook): add deploy procedure")

        read_back = self.wiki.read("runbooks/deploy.md")
        self.assertEqual(read_back, content)

        # Verify git commit was created
        log = subprocess.run(
            ["git", "log", "-n", "1", "--format=%s (%an)"],
            cwd=self.test_dir,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.assertIn("feat(runbook): add deploy procedure (Golem)", log)

    def test_path_traversal_rejection(self) -> None:
        with self.assertRaises(WikiSecurityError):
            self.wiki.read("../../etc/passwd")

        with self.assertRaises(WikiSecurityError):
            self.wiki.write("../outside.txt", "hostile", "exploit")

    def test_search_and_list_documents(self) -> None:
        self.wiki.write("context/topology.md", "Oracle VPS IP: 100.113.173.33\nTailscale mesh node.", "feat: topology")
        self.wiki.write("runbooks/backup.md", "Nightly ZFS backup to storage pool.", "feat: backup")

        docs = self.wiki.list_documents()
        self.assertIn("context/topology.md", docs)
        self.assertIn("runbooks/backup.md", docs)

        runbooks = self.wiki.list_documents("runbooks")
        self.assertEqual(runbooks, ["runbooks/backup.md"])

        hits = self.wiki.search("100.113.173.33")
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["path"], "context/topology.md")
        self.assertIn("Oracle VPS", hits[0]["snippet"])

    def test_rollback_on_write_failure(self) -> None:
        initial = "Original safe content"
        self.wiki.write("runbooks/critical.md", initial, "chore: initial")

        # Simulate git error by making git config broken during a write
        with unittest.mock.patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, ["git", "commit"])):
            with self.assertRaises(Exception):
                self.wiki.write("runbooks/critical.md", "Corrupted new content", "feat: broken")

        # Ensure rollback preserved the original content
        restored = self.wiki.read("runbooks/critical.md")
        self.assertEqual(restored, initial)


class TestGolemWikiConcurrencyAndGates(unittest.IsolatedAsyncioTestCase):
    """Unit tests for concurrency safety and user confirmation gates."""

    async def asyncSetUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="golem_wiki_async_test_")
        self.wiki = GolemWiki(wiki_dir=self.test_dir)

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    async def test_concurrent_writes_under_async_lock(self) -> None:
        """Verify multiple coroutines writing concurrently don't collide or corrupt git."""
        async def _worker(idx: int) -> None:
            async with self.wiki.async_lock():
                self.wiki.write(
                    f"logbook/entry_{idx}.md",
                    f"# Log entry {idx}\nExecuted parallel task {idx}.\n",
                    f"feat(logbook): parallel write {idx}",
                )

        tasks = [_worker(i) for i in range(10)]
        await asyncio.gather(*tasks)

        docs = self.wiki.list_documents("logbook")
        self.assertEqual(len(docs), 10)

    async def test_logbook_auto_commit_without_approval(self) -> None:
        rel_path = self.wiki.logbook_append(
            entry="Deploy completed successfully in 12s.",
            title="Production Deployment",
        )
        self.assertTrue(rel_path.startswith("logbook/"))
        content = self.wiki.read(rel_path)
        self.assertIsNotNone(content)
        self.assertIn("Deploy completed successfully", content)
        self.assertIn("Production Deployment", content)

    async def test_runbook_propose_and_confirm_gate(self) -> None:
        rel_path = "runbooks/database_recovery.md"
        new_content = "# Database Recovery\n\n1. Stop replicas.\n2. Restore snapshot.\n"

        # 1. Propose update -> returns pending token & diff
        proposal = self.wiki.propose_update(rel_path, new_content, reason="Critical recovery procedure")
        token = proposal["token"]
        self.assertTrue(proposal["pending_approval"])
        self.assertIn("Database Recovery", proposal["diff"])

        # File is not written yet!
        self.assertIsNone(self.wiki.read(rel_path))

        # 2. Confirm update with token -> written and committed
        approved = self.wiki.confirm_update(token)
        self.assertTrue(approved)
        self.assertEqual(self.wiki.read(rel_path), new_content)

    async def test_runbook_propose_and_reject_gate(self) -> None:
        rel_path = "context/experimental.md"
        proposal = self.wiki.propose_update(rel_path, "Experimental context", reason="Testing")
        token = proposal["token"]

        rejected = self.wiki.reject_update(token)
        self.assertTrue(rejected)
        # Content was never applied
        self.assertIsNone(self.wiki.read(rel_path))


class TestGolemWikiMigration(unittest.TestCase):
    """Unit tests for migrate_legacy_knowledge script."""

    def setUp(self) -> None:
        self.wiki_dir = tempfile.mkdtemp(prefix="golem_wiki_mig_test_")
        self.source_dir = tempfile.mkdtemp(prefix="golem_source_test_")
        self.wiki = GolemWiki(wiki_dir=self.wiki_dir)

        # Set up mock source files
        rules_dir = os.path.join(self.source_dir, ".agents", "rules")
        skills_dir = os.path.join(self.source_dir, ".agents", "skills", "sample-skill")
        memory_dir = os.path.join(self.source_dir, "memory")
        os.makedirs(rules_dir, exist_ok=True)
        os.makedirs(skills_dir, exist_ok=True)
        os.makedirs(memory_dir, exist_ok=True)

        with open(os.path.join(rules_dir, "sensory.md"), "w") as f:
            f.write("# Sensory Architecture Rule\nZero-regex quiescent gate.\n")
        with open(os.path.join(skills_dir, "SKILL.md"), "w") as f:
            f.write("# Sample Skill\nStep-by-step instructions.\n")
        with open(os.path.join(memory_dir, "ledger.jsonl"), "w") as f:
            f.write('{"ts": "2026-10-09T08:00:00Z", "type": "task_dispatched", "notes": "Dispatched build"}\n')

    def tearDown(self) -> None:
        shutil.rmtree(self.wiki_dir, ignore_errors=True)
        shutil.rmtree(self.source_dir, ignore_errors=True)

    def test_migration_ingests_all_categories(self) -> None:
        counts = migrate_legacy_knowledge(self.source_dir, self.wiki)
        self.assertEqual(counts["rules"], 1)
        self.assertEqual(counts["skills"], 1)
        self.assertEqual(counts["ledger"], 1)

        rule_content = self.wiki.read("context/rules/sensory.md")
        self.assertIn("Zero-regex quiescent gate", rule_content)

        skill_content = self.wiki.read("runbooks/skills/sample-skill/SKILL.md")
        self.assertIn("Step-by-step instructions", skill_content)

        ledger_content = self.wiki.read("logbook/legacy_ledger.md")
        self.assertIn("Dispatched build", ledger_content)


if __name__ == "__main__":
    unittest.main()
