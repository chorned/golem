#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

golem/state.py
SQLite WAL Task Ledger & Startup Reconciliation:
  - Database at ~/.golem/state/tasks.db (configurable via GOLEM_STATE_DB).
  - WAL mode (PRAGMA journal_mode=WAL) with 0600 file permissions.
  - Tables: tasks, artifacts, thread_pointers.
  - Two-way reconciliation against Herdr pane state.
  - 15-minute TTL sweep for stale or orphaned execution handles.
  - Executive startup handoff briefing.
"""

from __future__ import annotations

import os
import sys
import time
import json
import sqlite3
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger("Golem.State")

DEFAULT_STATE_DIR = os.path.expanduser("~/.golem/state")
DEFAULT_DB_PATH = os.path.join(DEFAULT_STATE_DIR, "tasks.db")


class TaskLedger:
    """
    Persistent SQLite Task Ledger managing tasks, artifacts, and thread state
    under WAL mode with secure 0600 permissions.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = os.path.abspath(
            db_path or os.environ.get("GOLEM_STATE_DB") or DEFAULT_DB_PATH
        )
        self._init_db()

    def _ensure_permissions(self) -> None:
        """Enforce 0700 on state directory and 0600 on sqlite files."""
        parent = os.path.dirname(self.db_path)
        try:
            if os.path.exists(parent):
                os.chmod(parent, 0o700)
            if os.path.exists(self.db_path):
                os.chmod(self.db_path, 0o600)
            for ext in ("-wal", "-shm"):
                aux = self.db_path + ext
                if os.path.exists(aux):
                    os.chmod(aux, 0o600)
        except OSError as e:
            logger.debug(f"TaskLedger: error enforcing file permissions: {e}")

    def _get_connection(self) -> sqlite3.Connection:
        """Open a SQLite connection with WAL journal mode and foreign keys enabled."""
        os.makedirs(os.path.dirname(self.db_path), mode=0o700, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        self._ensure_permissions()
        return conn

    def _init_db(self) -> None:
        """Create tables and indexes if they do not exist."""
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    thread_ts TEXT NOT NULL,
                    command TEXT NOT NULL,
                    terminal_id TEXT,
                    node TEXT NOT NULL DEFAULT 'localhost',
                    worker TEXT NOT NULL DEFAULT 'agy',
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    exit_code INTEGER,
                    error TEXT,
                    metadata TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_session ON tasks(session_id);
                CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
                CREATE INDEX IF NOT EXISTS idx_tasks_terminal ON tasks(terminal_id);

                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    thread_ts TEXT NOT NULL,
                    rel_path TEXT NOT NULL,
                    abs_path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_artifacts_task ON artifacts(task_id);
                CREATE INDEX IF NOT EXISTS idx_artifacts_channel_thread ON artifacts(channel, thread_ts);

                CREATE TABLE IF NOT EXISTS thread_pointers (
                    channel TEXT NOT NULL,
                    thread_ts TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    active_task_id TEXT,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (channel, thread_ts)
                );
            """)
        self._ensure_permissions()

    def record_task(
        self,
        task_id: str,
        session_id: str,
        channel: str,
        thread_ts: str,
        command: str,
        terminal_id: Optional[str] = None,
        node: str = "localhost",
        worker: str = "agy",
        status: str = "queued",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Insert or replace a task record in the ledger."""
        now = time.time()
        meta_json = json.dumps(metadata or {})
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO tasks (
                    task_id, session_id, channel, thread_ts, command,
                    terminal_id, node, worker, status, created_at, updated_at, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status=excluded.status,
                    terminal_id=coalesce(excluded.terminal_id, tasks.terminal_id),
                    updated_at=excluded.updated_at,
                    metadata=excluded.metadata;
                """,
                (
                    task_id, session_id, channel, thread_ts, command,
                    terminal_id, node, worker, status, now, now, meta_json
                ),
            )
            # Also update thread pointer
            conn.execute(
                """
                INSERT INTO thread_pointers (channel, thread_ts, session_id, active_task_id, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(channel, thread_ts) DO UPDATE SET
                    session_id=excluded.session_id,
                    active_task_id=excluded.active_task_id,
                    updated_at=excluded.updated_at;
                """,
                (channel, thread_ts, session_id, task_id if status in ("queued", "running") else None, now),
            )
        return self.get_task(task_id) or {}

    def update_task_status(
        self,
        task_id: str,
        status: str,
        exit_code: Optional[int] = None,
        error: Optional[str] = None,
        terminal_id: Optional[str] = None,
    ) -> bool:
        """Update status, exit_code, error, and terminal coordinate for a task."""
        now = time.time()
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    exit_code = coalesce(?, exit_code),
                    error = coalesce(?, error),
                    terminal_id = coalesce(?, terminal_id),
                    updated_at = ?
                WHERE task_id = ?;
                """,
                (status, exit_code, error, terminal_id, now, task_id),
            )
            updated = cur.rowcount > 0
            if updated and status in ("completed", "failed", "abandoned"):
                # Clear active_task_id on thread pointer if this was active
                conn.execute(
                    """
                    UPDATE thread_pointers
                    SET active_task_id = NULL, updated_at = ?
                    WHERE active_task_id = ?;
                    """,
                    (now, task_id),
                )
            return updated

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a task record by task_id."""
        with self._get_connection() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE task_id = ?;", (task_id,)).fetchone()
            if row is None:
                return None
            d = dict(row)
            if d.get("metadata"):
                try:
                    d["metadata"] = json.loads(d["metadata"])
                except Exception:
                    pass
            return d

    def list_tasks(
        self,
        status: Optional[str] = None,
        session_id: Optional[str] = None,
        channel: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Query tasks with optional status, session, and channel filtering."""
        query = "SELECT * FROM tasks WHERE 1=1"
        params: List[Any] = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if session_id:
            query += " AND session_id = ?"
            params.append(session_id)
        if channel:
            query += " AND channel = ?"
            params.append(channel)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)

        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            results = []
            for r in rows:
                d = dict(r)
                if d.get("metadata"):
                    try:
                        d["metadata"] = json.loads(d["metadata"])
                    except Exception:
                        pass
                results.append(d)
            return results

    def record_artifact(
        self,
        artifact_id: str,
        task_id: str,
        channel: str,
        thread_ts: str,
        rel_path: str,
        abs_path: str,
        sha256: str,
        size_bytes: int,
    ) -> Dict[str, Any]:
        """Record a sandboxed artifact generated by a task."""
        now = time.time()
        with self._get_connection() as conn:
            # Ensure foreign key requirement is satisfied by stubbing task if needed
            conn.execute(
                """
                INSERT OR IGNORE INTO tasks (
                    task_id, session_id, channel, thread_ts, command, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'artifact_staging', 'completed', ?, ?);
                """,
                (task_id, channel or "default", channel or "default", thread_ts or "default", now, now),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO artifacts (
                    artifact_id, task_id, channel, thread_ts, rel_path, abs_path, sha256, size_bytes, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (artifact_id, task_id, channel, thread_ts, rel_path, abs_path, sha256, size_bytes, now),
            )
        return {
            "artifact_id": artifact_id,
            "task_id": task_id,
            "channel": channel,
            "thread_ts": thread_ts,
            "rel_path": rel_path,
            "abs_path": abs_path,
            "sha256": sha256,
            "size_bytes": size_bytes,
            "created_at": now,
        }

    def get_artifact(self, artifact_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific artifact record by artifact_id."""
        with self._get_connection() as conn:
            row = conn.execute("SELECT * FROM artifacts WHERE artifact_id = ?;", (artifact_id,)).fetchone()
            return dict(row) if row else None

    def get_artifacts_for_thread(self, channel: str, thread_ts: str) -> List[Dict[str, Any]]:
        """Retrieve all artifacts linked to a specific channel thread."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM artifacts WHERE channel = ? AND thread_ts = ? ORDER BY created_at ASC;",
                (channel, thread_ts),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_last_artifact_for_thread(self, channel: str, thread_ts: str) -> Optional[Dict[str, Any]]:
        """Retrieve the most recent artifact linked to a specific channel thread."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM artifacts WHERE channel = ? AND thread_ts = ? ORDER BY created_at DESC LIMIT 1;",
                (channel, thread_ts),
            ).fetchone()
            return dict(row) if row else None

    def set_thread_pointer(
        self,
        channel: str,
        thread_ts: str,
        session_id: str,
        active_task_id: Optional[str] = None,
    ) -> None:
        """Register or update active session and task pointer for a Slack/iMessage thread."""
        now = time.time()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO thread_pointers (channel, thread_ts, session_id, active_task_id, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(channel, thread_ts) DO UPDATE SET
                    session_id=excluded.session_id,
                    active_task_id=excluded.active_task_id,
                    updated_at=excluded.updated_at;
                """,
                (channel, thread_ts, session_id, active_task_id, now),
            )

    def get_thread_pointer(self, channel: str, thread_ts: str) -> Optional[Dict[str, Any]]:
        """Retrieve thread pointer metadata by channel and thread_ts."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM thread_pointers WHERE channel = ? AND thread_ts = ?;",
                (channel, thread_ts),
            ).fetchone()
            return dict(row) if row else None

    def sweep_stale_tasks(self, ttl_seconds: float = 900.0) -> int:
        """
        15-minute TTL sweep: marks un-updated queued or running tasks older than ttl_seconds as abandoned.
        Returns count of swept tasks.
        """
        cutoff = time.time() - ttl_seconds
        now = time.time()
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE tasks
                SET status = 'abandoned',
                    error = 'Task execution timed out (15m TTL exceeded)',
                    updated_at = ?
                WHERE status IN ('queued', 'running') AND updated_at < ?;
                """,
                (now, cutoff),
            )
            swept_count = cur.rowcount
            if swept_count > 0:
                conn.execute(
                    """
                    UPDATE thread_pointers
                    SET active_task_id = NULL, updated_at = ?
                    WHERE active_task_id IN (
                        SELECT task_id FROM tasks WHERE status = 'abandoned'
                    );
                    """,
                    (now,),
                )
            return swept_count

    def reconcile_with_herdr(
        self,
        herdr_panes: List[Dict[str, Any]],
        grace_period_seconds: float = 30.0,
    ) -> Dict[str, Any]:
        """
        Reconcile running tasks against live Herdr panes.
        If a running task's terminal_id is not in active Herdr panes, marks it as abandoned/failed.
        """
        active_terminal_ids = set()
        for p in herdr_panes:
            pid = p.get("id") or p.get("pane_id") or p.get("terminal_id") or p.get("name")
            if pid:
                active_terminal_ids.add(str(pid))

        running_tasks = self.list_tasks(status="running", limit=200)
        reconciled = 0
        abandoned = 0
        now = time.time()

        for t in running_tasks:
            tid = t.get("terminal_id")
            updated = t.get("updated_at", 0)
            # If task has a terminal_id and it's missing from Herdr
            if tid and tid not in active_terminal_ids:
                if (now - updated) > grace_period_seconds:
                    self.update_task_status(
                        task_id=t["task_id"],
                        status="abandoned",
                        error=f"Herdr pane '{tid}' terminated unexpectedly across restart",
                    )
                    abandoned += 1
                    reconciled += 1
            elif tid and tid in active_terminal_ids:
                # Live pane confirmed
                reconciled += 1

        active_count = len(self.list_tasks(status="running", limit=200))
        return {
            "checked": len(running_tasks),
            "reconciled": reconciled,
            "abandoned": abandoned,
            "active": active_count,
        }

    def generate_startup_briefing(self, reconciliation_summary: Dict[str, Any]) -> str:
        """Generate an executive startup handoff briefing in Slack mrkdwn format."""
        active = reconciliation_summary.get("active", 0)
        abandoned = reconciliation_summary.get("abandoned", 0)
        checked = reconciliation_summary.get("checked", 0)

        lines = ["*Executive Startup Handoff Briefing*"]
        if active == 0 and abandoned == 0:
            lines.append("• *Status:* Clean reboot. 0 stale tasks; fleet ready on standby.")
        else:
            lines.append(f"• *Status:* Reconciled {checked} task(s) across Herdr mesh.")
            if active > 0:
                lines.append(f"• *Active Hands:* `{active}` background task(s) actively running.")
            if abandoned > 0:
                lines.append(f"• *Recovered / Abandoned:* `{abandoned}` stale task(s) cleaned up.")

        return "\n".join(lines)


_DEFAULT_TASK_LEDGER: Optional[TaskLedger] = None


def get_default_task_ledger() -> TaskLedger:
    """Return or initialize global default TaskLedger instance."""
    global _DEFAULT_TASK_LEDGER
    if _DEFAULT_TASK_LEDGER is None:
        _DEFAULT_TASK_LEDGER = TaskLedger()
    return _DEFAULT_TASK_LEDGER
