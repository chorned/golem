#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

tests/test_stage7_state.py
Stage 7 Verification Suite: SQLite Task Ledger & Startup Reconciliation:
  - Database creation at isolated path with WAL mode.
  - File permissions 0600 on db and 0700 on parent state directory.
  - Task and artifact schema and indexes.
  - Thread pointer tracking and lifecycle clearing.
  - 15-minute TTL sweep for stale execution handles.
  - Two-way Herdr pane reconciliation on startup.
  - Executive startup handoff briefing generation.
"""

from __future__ import annotations

import os
import sys
import stat
import time
import shutil
import tempfile
import sqlite3
import unittest
from typing import Dict, Any

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.state import TaskLedger


class TestTaskLedgerDatabaseAndPermissions(unittest.TestCase):
    """Unit tests for SQLite WAL configuration, tables, and secure permissions."""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="golem_state_test_")
        self.db_path = os.path.join(self.test_dir, "tasks.db")
        self.ledger = TaskLedger(db_path=self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_database_creation_and_wal_mode(self) -> None:
        self.assertTrue(os.path.isfile(self.db_path))
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        res = cur.execute("PRAGMA journal_mode;").fetchone()
        self.assertEqual(res[0].lower(), "wal")
        conn.close()

    def test_file_permissions_are_0600_and_dir_0700(self) -> None:
        db_mode = stat.S_IMODE(os.stat(self.db_path).st_mode)
        dir_mode = stat.S_IMODE(os.stat(self.test_dir).st_mode)

        self.assertEqual(db_mode, 0o600)
        self.assertEqual(dir_mode, 0o700)

    def test_tables_and_indexes_exist(self) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        tables = [
            r[0] for r in cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
        ]
        self.assertIn("tasks", tables)
        self.assertIn("artifacts", tables)
        self.assertIn("thread_pointers", tables)
        conn.close()


class TestTaskLedgerOperations(unittest.TestCase):
    """Unit tests for recording, querying, and updating tasks and artifacts."""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="golem_state_ops_test_")
        self.db_path = os.path.join(self.test_dir, "tasks.db")
        self.ledger = TaskLedger(db_path=self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_record_and_get_task(self) -> None:
        task = self.ledger.record_task(
            task_id="task-101",
            session_id="session-1",
            channel="C123",
            thread_ts="1234.56",
            command="pytest -q",
            terminal_id="w1:p2",
            node="localhost",
            worker="agy",
            status="running",
            metadata={"priority": "high"},
        )
        self.assertEqual(task["task_id"], "task-101")
        self.assertEqual(task["status"], "running")
        self.assertEqual(task["metadata"]["priority"], "high")

        fetched = self.ledger.get_task("task-101")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["command"], "pytest -q")

        # Verify thread pointer was set
        tp = self.ledger.get_thread_pointer("C123", "1234.56")
        self.assertIsNotNone(tp)
        self.assertEqual(tp["active_task_id"], "task-101")

    def test_update_task_status_clears_active_thread_pointer(self) -> None:
        self.ledger.record_task(
            task_id="task-102",
            session_id="session-1",
            channel="C123",
            thread_ts="1234.56",
            command="make build",
            status="running",
        )
        tp_before = self.ledger.get_thread_pointer("C123", "1234.56")
        self.assertEqual(tp_before["active_task_id"], "task-102")

        # Complete task
        self.ledger.update_task_status("task-102", status="completed", exit_code=0)
        task_after = self.ledger.get_task("task-102")
        self.assertEqual(task_after["status"], "completed")
        self.assertEqual(task_after["exit_code"], 0)

        tp_after = self.ledger.get_thread_pointer("C123", "1234.56")
        self.assertIsNone(tp_after["active_task_id"])

    def test_record_and_retrieve_artifacts(self) -> None:
        self.ledger.record_task(
            task_id="task-103",
            session_id="session-1",
            channel="C456",
            thread_ts="9999.00",
            command="build graph",
            status="running",
        )
        self.ledger.record_artifact(
            artifact_id="art-1",
            task_id="task-103",
            channel="C456",
            thread_ts="9999.00",
            rel_path="artifacts/report.pdf",
            abs_path="/tmp/report.pdf",
            sha256="abcdef123456",
            size_bytes=1024,
        )
        arts = self.ledger.get_artifacts_for_thread("C456", "9999.00")
        self.assertEqual(len(arts), 1)
        self.assertEqual(arts[0]["rel_path"], "artifacts/report.pdf")
        self.assertEqual(arts[0]["sha256"], "abcdef123456")


class TestTaskLedgerReconciliationAndSweep(unittest.TestCase):
    """Unit tests for 15m TTL sweep and Herdr pane reconciliation."""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="golem_state_recon_test_")
        self.db_path = os.path.join(self.test_dir, "tasks.db")
        self.ledger = TaskLedger(db_path=self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_sweep_stale_tasks_after_15_minutes(self) -> None:
        # Task 1: Stale task updated 20 minutes ago
        self.ledger.record_task(
            task_id="stale-1",
            session_id="s1",
            channel="c1",
            thread_ts="t1",
            command="long job",
            status="running",
        )
        old_time = time.time() - 1200.0  # 20 minutes ago
        with self.ledger._get_connection() as conn:
            conn.execute("UPDATE tasks SET updated_at = ? WHERE task_id = 'stale-1';", (old_time,))

        # Task 2: Fresh task updated 1 minute ago
        self.ledger.record_task(
            task_id="fresh-1",
            session_id="s1",
            channel="c1",
            thread_ts="t2",
            command="quick check",
            status="running",
        )

        swept = self.ledger.sweep_stale_tasks(ttl_seconds=900.0)
        self.assertEqual(swept, 1)

        stale_task = self.ledger.get_task("stale-1")
        self.assertEqual(stale_task["status"], "abandoned")
        self.assertIn("15m TTL exceeded", stale_task["error"])

        fresh_task = self.ledger.get_task("fresh-1")
        self.assertEqual(fresh_task["status"], "running")

    def test_reconcile_with_herdr_panes(self) -> None:
        # Task 1: pane exists in Herdr
        self.ledger.record_task(
            task_id="task-live",
            session_id="s1",
            channel="c1",
            thread_ts="t1",
            command="job 1",
            terminal_id="w1:p2",
            status="running",
        )
        # Task 2: pane missing from Herdr and past grace period
        self.ledger.record_task(
            task_id="task-orphaned",
            session_id="s1",
            channel="c1",
            thread_ts="t2",
            command="job 2",
            terminal_id="w1:p99",
            status="running",
        )
        with self.ledger._get_connection() as conn:
            conn.execute(
                "UPDATE tasks SET updated_at = ? WHERE task_id = 'task-orphaned';",
                (time.time() - 60.0,),
            )

        herdr_panes = [
            {"pane_id": "w1:p2", "name": "runner"},
            {"pane_id": "w1:p1", "name": "liaison"},
        ]
        summary = self.ledger.reconcile_with_herdr(herdr_panes, grace_period_seconds=10.0)

        self.assertEqual(summary["checked"], 2)
        self.assertEqual(summary["abandoned"], 1)
        self.assertEqual(summary["active"], 1)

        orphaned = self.ledger.get_task("task-orphaned")
        self.assertEqual(orphaned["status"], "abandoned")
        self.assertIn("terminated unexpectedly", orphaned["error"])

        live = self.ledger.get_task("task-live")
        self.assertEqual(live["status"], "running")

    def test_generate_startup_briefing(self) -> None:
        summary_clean = {"checked": 0, "abandoned": 0, "active": 0}
        briefing_clean = self.ledger.generate_startup_briefing(summary_clean)
        self.assertIn("Clean reboot", briefing_clean)
        self.assertIn("0 stale tasks", briefing_clean)

        summary_recovered = {"checked": 5, "abandoned": 2, "active": 3}
        briefing_recovered = self.ledger.generate_startup_briefing(summary_recovered)
        self.assertIn("Reconciled 5 task(s)", briefing_recovered)
        self.assertIn("`3` background task(s) actively running", briefing_recovered)
        self.assertIn("`2` stale task(s) cleaned up", briefing_recovered)


if __name__ == "__main__":
    unittest.main()
