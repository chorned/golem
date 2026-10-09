#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

golem/wiki.py
Git-backed Operational Memory Wiki:
  - Standalone Git repository at ~/.golem/wiki/ (configurable via GOLEM_WIKI_DIR).
  - Standard hierarchy: runbooks/, context/, logbook/.
  - Concurrency safety with in-process asyncio.Lock() and cross-process fcntl.flock().
  - Atomic writes with tempfile, replace, git commit, and automated rollback on failure.
  - User confirmation gate: auto-commit logbook entries, explicit [y/n] gate for runbooks/context.
"""

from __future__ import annotations

import os
import sys
import fcntl
import uuid
import difflib
import logging
import asyncio
import threading
import subprocess
import contextlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

logger = logging.getLogger("Golem.Wiki")

DEFAULT_WIKI_DIR = os.path.expanduser("~/.golem/wiki")


class WikiSecurityError(Exception):
    """Raised when a path attempts directory traversal outside the wiki repository."""
    pass


class GolemWiki:
    """
    Git-backed Operational Memory Wiki for Project Golem.
    Stores operational runbooks, architecture context, and daily logbook journals.
    """

    def __init__(self, wiki_dir: Optional[str] = None):
        self.wiki_dir = os.path.abspath(
            wiki_dir or os.environ.get("GOLEM_WIKI_DIR") or DEFAULT_WIKI_DIR
        )
        self.lock_path = os.path.join(self.wiki_dir, ".wiki.lock")
        self._async_lock = asyncio.Lock()
        self._thread_lock = threading.RLock()
        self._lock_depth = 0
        self._lock_file = None
        self._pending_updates: Dict[str, Dict[str, Any]] = {}
        self.init_repo()

    def _safe_path(self, rel_path: str) -> str:
        """Resolve rel_path inside wiki_dir and ensure no directory traversal."""
        clean = rel_path.lstrip("/\\")
        abs_path = os.path.abspath(os.path.join(self.wiki_dir, clean))
        if not abs_path.startswith(self.wiki_dir):
            raise WikiSecurityError(f"Path traversal detected outside wiki directory: {rel_path}")
        return abs_path

    @contextlib.contextmanager
    def file_lock(self):
        """Cross-process and re-entrant advisory lock on .wiki.lock via fcntl.flock."""
        with self._thread_lock:
            if self._lock_depth == 0:
                os.makedirs(self.wiki_dir, exist_ok=True)
                self._lock_file = open(self.lock_path, "a+")
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)
            self._lock_depth += 1
        try:
            yield
        finally:
            with self._thread_lock:
                self._lock_depth -= 1
                if self._lock_depth == 0 and self._lock_file is not None:
                    try:
                        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                    except Exception:
                        pass
                    try:
                        self._lock_file.close()
                    except Exception:
                        pass
                    self._lock_file = None

    @contextlib.asynccontextmanager
    async def async_lock(self):
        """Async context manager combining asyncio.Lock and re-entrant fcntl.flock."""
        async with self._async_lock:
            with self.file_lock():
                yield

    def init_repo(self) -> None:
        """Ensure wiki directory, standard subdirectories, and git repository are initialized."""
        with self.file_lock():
            for subdir in ("runbooks", "context", "logbook"):
                os.makedirs(os.path.join(self.wiki_dir, subdir), exist_ok=True)

            git_dir = os.path.join(self.wiki_dir, ".git")
            if not os.path.exists(git_dir):
                subprocess.run(
                    ["git", "init", "-b", "main"],
                    cwd=self.wiki_dir,
                    check=True,
                    capture_output=True,
                )
                readme_path = os.path.join(self.wiki_dir, "README.md")
                if not os.path.exists(readme_path):
                    with open(readme_path, "w", encoding="utf-8") as f:
                        f.write("# Golem Operational Wiki\n\nGit-backed operational memory for Project Golem.\n")

                ignore_path = os.path.join(self.wiki_dir, ".gitignore")
                if not os.path.exists(ignore_path):
                    with open(ignore_path, "w", encoding="utf-8") as f:
                        f.write(".wiki.lock\n*.tmp\n")

                subprocess.run(["git", "add", "-A"], cwd=self.wiki_dir, check=True, capture_output=True)
                subprocess.run(
                    [
                        "git",
                        "-c", "user.name=Golem",
                        "-c", "user.email=golem@fleet.local",
                        "commit",
                        "-m", "chore(wiki): initialize operational memory wiki",
                    ],
                    cwd=self.wiki_dir,
                    check=True,
                    capture_output=True,
                )

    def read(self, rel_path: str) -> Optional[str]:
        """Read content of a wiki document relative to the wiki root."""
        target = self._safe_path(rel_path)
        with self.file_lock():
            if not os.path.isfile(target):
                return None
            with open(target, "r", encoding="utf-8", errors="replace") as f:
                return f.read()

    def write(
        self,
        rel_path: str,
        content: str,
        commit_msg: str,
        author: str = "Golem <golem@fleet.local>",
    ) -> bool:
        """
        Atomically write content to rel_path, commit to git, and rollback on failure.
        """
        target = self._safe_path(rel_path)
        parent = os.path.dirname(target)
        tmp_target = target + f".{uuid.uuid4().hex[:6]}.tmp"

        with self.file_lock():
            os.makedirs(parent, exist_ok=True)
            prev_content: Optional[str] = None
            if os.path.exists(target):
                with open(target, "r", encoding="utf-8", errors="replace") as f:
                    prev_content = f.read()

            try:
                # 1. Atomic write via tempfile & fsync
                with open(tmp_target, "w", encoding="utf-8") as f:
                    f.write(content)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_target, target)

                # 2. Stage and commit
                rel = os.path.relpath(target, self.wiki_dir)
                subprocess.run(["git", "add", rel], cwd=self.wiki_dir, check=True, capture_output=True)
                subprocess.run(
                    [
                        "git",
                        "-c", "user.name=Golem",
                        "-c", "user.email=golem@fleet.local",
                        "commit",
                        "-m", commit_msg,
                        "--author", author,
                    ],
                    cwd=self.wiki_dir,
                    check=True,
                    capture_output=True,
                )
                return True
            except Exception as e:
                logger.error(f"Error during atomic wiki write to {rel_path}: {e}")
                # Rollback on failure
                if os.path.exists(tmp_target):
                    try:
                        os.remove(tmp_target)
                    except OSError:
                        pass
                if prev_content is not None:
                    try:
                        with open(target, "w", encoding="utf-8") as f:
                            f.write(prev_content)
                    except OSError:
                        pass
                elif os.path.exists(target):
                    try:
                        os.remove(target)
                    except OSError:
                        pass
                raise

    def search(self, query: str) -> List[Dict[str, Any]]:
        """Search markdown documents in the wiki for query string."""
        results: List[Dict[str, Any]] = []
        if not query:
            return results

        q_lower = query.lower()
        with self.file_lock():
            for root, _, files in os.walk(self.wiki_dir):
                if ".git" in root:
                    continue
                for fname in sorted(files):
                    if not fname.endswith((".md", ".txt")):
                        continue
                    full_p = os.path.join(root, fname)
                    rel_p = os.path.relpath(full_p, self.wiki_dir)
                    try:
                        with open(full_p, "r", encoding="utf-8", errors="replace") as f:
                            for idx, line in enumerate(f, 1):
                                if q_lower in line.lower():
                                    results.append({
                                        "path": rel_p,
                                        "line_no": idx,
                                        "snippet": line.strip(),
                                    })
                    except Exception as e:
                        logger.debug(f"Wiki search: error reading {rel_p}: {e}")
        return results

    def list_documents(self, category: Optional[str] = None) -> List[str]:
        """List relative document paths under category ('runbooks', 'context', 'logbook') or all."""
        with self.file_lock():
            search_dir = os.path.join(self.wiki_dir, category) if category else self.wiki_dir
            if not os.path.exists(search_dir):
                return []
            docs: List[str] = []
            for root, _, files in os.walk(search_dir):
                if ".git" in root:
                    continue
                for fname in sorted(files):
                    if fname.endswith((".md", ".txt")) and fname != "README.md":
                        rel = os.path.relpath(os.path.join(root, fname), self.wiki_dir)
                        docs.append(rel)
            return sorted(docs)

    def logbook_append(self, entry: str, title: Optional[str] = None) -> str:
        """
        Append an operational journal entry to today's logbook file (logbook/YYYY-MM-DD.md).
        Auto-committed immediately without requiring confirmation.
        """
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        rel_path = f"logbook/{today}.md"
        time_str = now.strftime("%H:%M:%S UTC")

        header = f"## [{time_str}] {title or 'Operational Entry'}\n\n"
        body = f"{entry.strip()}\n\n"

        existing = self.read(rel_path)
        if existing is None:
            full_content = f"# Logbook: {today}\n\n{header}{body}"
        else:
            full_content = existing.rstrip() + f"\n\n{header}{body}"

        self.write(
            rel_path=rel_path,
            content=full_content,
            commit_msg=f"feat(logbook): append entry for {today} [{time_str}]",
        )
        return rel_path

    def propose_update(self, rel_path: str, content: str, reason: str = "") -> Dict[str, Any]:
        """
        Stage a proposed runbook or context update requiring explicit [y/n] user confirmation.
        Returns approval card payload with diff summary and pending token.
        """
        current_content = self.read(rel_path) or ""
        diff_lines = list(
            difflib.unified_diff(
                current_content.splitlines(keepends=True),
                content.splitlines(keepends=True),
                fromfile=f"a/{rel_path}",
                tofile=f"b/{rel_path}",
            )
        )
        diff_summary = "".join(diff_lines) if diff_lines else "(No content change)"

        token = f"prop_{uuid.uuid4().hex[:8]}"
        self._pending_updates[token] = {
            "token": token,
            "path": rel_path,
            "content": content,
            "reason": reason,
            "diff": diff_summary,
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        return {
            "token": token,
            "path": rel_path,
            "pending_approval": True,
            "reason": reason,
            "diff": diff_summary,
            "prompt": (
                f":warning: *Proposed update to `{rel_path}` requires human approval:*\n"
                f"```diff\n{diff_summary}\n```\n"
                f"Approve update? Reply `approve {token}` or `reject {token}` [y/n]."
            ),
        }

    def confirm_update(self, token: str) -> bool:
        """Execute and commit a pending update that received human confirmation."""
        proposal = self._pending_updates.get(token)
        if not proposal or proposal.get("status") != "pending":
            return False

        rel_path = proposal["path"]
        content = proposal["content"]
        reason = proposal.get("reason", "Approved operational update")

        self.write(
            rel_path=rel_path,
            content=content,
            commit_msg=f"feat(wiki): update {rel_path} [{reason}] (approved: {token})",
        )
        proposal["status"] = "approved"
        return True

    def reject_update(self, token: str) -> bool:
        """Reject and discard a pending update."""
        proposal = self._pending_updates.get(token)
        if not proposal or proposal.get("status") != "pending":
            return False
        proposal["status"] = "rejected"
        return True
