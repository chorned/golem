#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

golem/wiki.py
Git-backed Operational Memory Wiki:
  - Standalone Git repository at ~/.golem/wiki/ (configurable via GOLEM_WIKI_DIR).
  - 4-Tier Memory Topology:
      • context/rules/: Tier 1 Facts (node invariants, OS quirks, TCC/AX permissions)
      • context/topology.md: Tier 1 Facts (fleet-wide IPs, Tailscale endpoints)
      • context/capabilities.md: Tier 2 Capabilities (available tools, CLIs, package affordances)
      • runbooks/skills/: Tier 3 Skills (procedural execution recipes)
      • runbooks/flows/: Tier 4 Flows (reactive reflexes & event-driven orchestration)
      • logbook/: Chronological operational journal (logbook/YYYY-MM-DD.md)
  - Concurrency safety with in-process asyncio.Lock() and cross-process fcntl.flock().
  - Atomic multi-file and single-file changesets with rollback and single Git commits.
  - User confirmation gate: auto-commit logbook entries, explicit [y/n] gate for proposals.
"""

from __future__ import annotations

import os
import sys
import time
import fcntl
import uuid
import difflib
import logging
import asyncio
import threading
import subprocess
import contextlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

logger = logging.getLogger("Golem.Wiki")

DEFAULT_WIKI_DIR = Path(os.environ.get("GOLEM_WIKI_DIR") or os.path.expanduser("~/.golem/wiki")).resolve()

DEFAULT_CAPABILITIES_TEMPLATE = """# Fleet Capabilities & Affordances

Institutional inventory of tools, CLIs, libraries, and package affordances available across fleet nodes.
Teach Golem new capabilities and skills using `/learn` or conversational memory updates.
"""

DEFAULT_TOPOLOGY_TEMPLATE = """# Fleet Topology & Host Mapping

| Machine | Role | Host / Tailscale IP | Execution Mode |
| :--- | :--- | :--- | :--- |
"""


class WikiError(Exception):
    """Base exception for Golem Wiki errors."""
    pass


class WikiSecurityError(WikiError):
    """Raised when a path attempts directory traversal outside the wiki repository."""
    pass


class CommitResult(str):
    """
    Commit hash result string that evaluates equal to True and non-empty string,
    ensuring full backward compatibility with legacy bool assertions.
    """
    def __eq__(self, other: Any) -> bool:
        if other is True:
            return True
        return super().__eq__(other)

    def __bool__(self) -> bool:
        return True


@dataclass
class StagedChange:
    """Represents a single file modification staged in a memory proposal."""
    tier: str                 # "fact" | "capability" | "skill" | "flow" | "logbook"
    rel_path: str             # e.g., "context/rules/hermes.md"
    content: str              # Full new file content
    diff_preview: str         # Generated via difflib.unified_diff
    is_new_file: bool = False


@dataclass
class MemoryProposal:
    """Represents an atomic multi-tier institutional memory proposal."""
    token: str                # e.g., "prop_3f8a1b2c"
    summary: str
    changes: List[StagedChange]
    created_at: float = field(default_factory=time.time)
    commit_hash: Optional[str] = None
    status: str = "pending"

    def consolidated_diff(self) -> str:
        """Produce unified diff preview across all staged changes."""
        snippets = []
        for change in self.changes:
            action = "New file" if change.is_new_file else "Modified"
            snippets.append(
                f"--- a/{change.rel_path} ({change.tier})\n"
                f"+++ b/{change.rel_path} ({action})\n"
                f"{change.diff_preview.strip()}"
            )
        return "\n\n".join(snippets)

    def to_slack_card(self) -> str:
        """Format proposal as actionable Slack/iMessage review card."""
        lines = [
            f"🧠 *Memory Distillation Proposal* — {self.summary}",
            f"Proposal Token: `{self.token}`\n",
            "Changes to apply:",
        ]
        for idx, change in enumerate(self.changes, 1):
            tier_name = change.tier.capitalize()
            action = "New file" if change.is_new_file else "Updated"
            lines.append(f"{idx}. [{tier_name}] ──► `{change.rel_path}` ({action})")

        diffs = self.consolidated_diff()
        if diffs:
            lines.append(f"\n```{diffs}\n```")
        lines.append("\nReply `confirm` or `commit` to apply, or describe adjustments.")
        return "\n".join(lines)


class GolemWiki:
    """
    Git-backed Operational Memory Wiki for Project Golem.
    Stores operational runbooks, architecture context, and daily logbook journals.
    Supports atomic multi-file changesets across 4 memory tiers under single prop_ tokens.
    """

    _global_pending_updates: Dict[str, Any] = {}
    _test_hermetic_wiki_dir: Optional[str] = None

    @classmethod
    def _get_or_create_test_wiki_dir(cls) -> str:
        if cls._test_hermetic_wiki_dir and os.path.exists(cls._test_hermetic_wiki_dir):
            return cls._test_hermetic_wiki_dir
        import tempfile
        import shutil
        tmp_dir = tempfile.mkdtemp(prefix="golem_hermetic_wiki_")
        for subdir in ("context", "runbooks"):
            src = Path(DEFAULT_WIKI_DIR) / subdir
            dst = Path(tmp_dir) / subdir
            if src.exists():
                shutil.copytree(src, dst, symlinks=False, dirs_exist_ok=True)
        cls._test_hermetic_wiki_dir = tmp_dir
        os.environ["GOLEM_WIKI_DIR"] = tmp_dir
        return tmp_dir

    def __init__(self, wiki_dir: Optional[Union[str, Path]] = None):
        if wiki_dir is None:
            env_dir = os.environ.get("GOLEM_WIKI_DIR")
            if env_dir:
                resolved = env_dir
            elif (
                os.environ.get("GOLEM_TEST_MODE") == "1"
                or os.environ.get("FIRSTMATE_TEST_MODE") == "1"
                or "unittest" in sys.modules
                or "pytest" in sys.modules
            ):
                resolved = self._get_or_create_test_wiki_dir()
            else:
                resolved = DEFAULT_WIKI_DIR
        else:
            resolved = wiki_dir
        self.wiki_dir = os.path.abspath(str(resolved))
        self.lock_path = os.path.join(self.wiki_dir, ".wiki.lock")
        self._async_lock = asyncio.Lock()
        self._thread_lock = threading.RLock()
        self._lock_depth = 0
        self._lock_file = None
        self._pending_updates = GolemWiki._global_pending_updates
        self.last_commit_hash: Optional[str] = None
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
        """Ensure wiki directory, standard 4-tier subdirectories, and git repository are initialized."""
        with self.file_lock():
            for subdir in (
                "runbooks",
                "runbooks/skills",
                "runbooks/flows",
                "context",
                "context/rules",
                "logbook",
            ):
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

                cap_path = os.path.join(self.wiki_dir, "context", "capabilities.md")
                if not os.path.exists(cap_path):
                    with open(cap_path, "w", encoding="utf-8") as f:
                        f.write(DEFAULT_CAPABILITIES_TEMPLATE)

                topo_path = os.path.join(self.wiki_dir, "context", "topology.md")
                if not os.path.exists(topo_path):
                    with open(topo_path, "w", encoding="utf-8") as f:
                        f.write(DEFAULT_TOPOLOGY_TEMPLATE)

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
            else:
                created_any = False
                cap_path = os.path.join(self.wiki_dir, "context", "capabilities.md")
                if not os.path.exists(cap_path):
                    with open(cap_path, "w", encoding="utf-8") as f:
                        f.write(DEFAULT_CAPABILITIES_TEMPLATE)
                    created_any = True

                topo_path = os.path.join(self.wiki_dir, "context", "topology.md")
                if not os.path.exists(topo_path):
                    with open(topo_path, "w", encoding="utf-8") as f:
                        f.write(DEFAULT_TOPOLOGY_TEMPLATE)
                    created_any = True

                if created_any:
                    try:
                        subprocess.run(["git", "add", "-A"], cwd=self.wiki_dir, check=True, capture_output=True)
                        subprocess.run(
                            [
                                "git",
                                "-c", "user.name=Golem",
                                "-c", "user.email=golem@fleet.local",
                                "commit",
                                "-m", "chore(wiki): scaffold 4-tier default context templates",
                            ],
                            cwd=self.wiki_dir,
                            check=True,
                            capture_output=True,
                        )
                    except Exception as e:
                        logger.debug(f"Git commit for default templates notice: {e}")

    def exists(self, rel_path: str) -> bool:
        """Check if relative path exists within wiki repository."""
        target = self._safe_path(rel_path)
        with self.file_lock():
            return os.path.exists(target)

    def list_files(self, category: Optional[str] = None) -> List[str]:
        """Alias for list_documents."""
        return self.list_documents(category)

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
                res = subprocess.run(
                    ["git", "rev-parse", "--short", "HEAD"],
                    cwd=self.wiki_dir,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                self.last_commit_hash = res.stdout.strip()
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
                raise WikiError(f"Write failure for {rel_path}: {e}") from e

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
        Preserves legacy single-file contract.
        """
        self._safe_path(rel_path)
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
        payload = {
            "token": token,
            "path": rel_path,
            "content": content,
            "reason": reason,
            "diff": diff_summary,
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._pending_updates[token] = payload

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

    def propose_multi_update(
        self,
        summary: str,
        changes: List[Union[Dict[str, Any], StagedChange]],
    ) -> MemoryProposal:
        """
        Stage an atomic multi-file proposal across the 4 memory tiers under a single token.
        Strictly validates all paths for directory traversal before staging.
        """
        # Validate all paths upfront for directory traversal
        for ch in changes:
            rel_p = ch.rel_path if isinstance(ch, StagedChange) else str(ch.get("rel_path", ""))
            self._safe_path(rel_p)

        staged_changes: List[StagedChange] = []
        for ch in changes:
            if isinstance(ch, StagedChange):
                staged_changes.append(ch)
                continue

            tier = str(ch.get("tier", "fact")).lower()
            rel_path = str(ch.get("rel_path", "")).lstrip("/\\")
            content = str(ch.get("content", ""))

            current_content = self.read(rel_path) or ""
            target_p = self._safe_path(rel_path)
            is_new = not os.path.exists(target_p)

            diff_lines = list(
                difflib.unified_diff(
                    current_content.splitlines(keepends=True),
                    content.splitlines(keepends=True),
                    fromfile=f"a/{rel_path}",
                    tofile=f"b/{rel_path}",
                )
            )
            diff_summary = "".join(diff_lines) if diff_lines else ("(New file)" if is_new else "(No content change)")

            staged_changes.append(
                StagedChange(
                    tier=tier,
                    rel_path=rel_path,
                    content=content,
                    diff_preview=diff_summary,
                    is_new_file=is_new,
                )
            )

        token = f"prop_{uuid.uuid4().hex[:8]}"
        proposal = MemoryProposal(
            token=token,
            summary=summary,
            changes=staged_changes,
            created_at=time.time(),
        )

        self._pending_updates[token] = proposal
        return proposal

    def confirm_update(self, token: str) -> Union[CommitResult, bool]:
        """
        Execute and commit a pending update (single-file or multi-file MemoryProposal)
        that received human confirmation.
        Acquires file_lock, performs atomic replacement of all files, stages in git,
        and creates a single atomic commit. Rolls back on failure and raises WikiError.
        """
        proposal = self._pending_updates.get(token)
        if not proposal:
            return False

        if isinstance(proposal, dict) and proposal.get("status") != "pending":
            return False

        is_proposal_obj = isinstance(proposal, MemoryProposal)

        if is_proposal_obj:
            changes_to_apply = proposal.changes
            summary = proposal.summary or "Institutional memory update"
        else:
            rel_p = proposal["path"]
            content = proposal["content"]
            target_p = self._safe_path(rel_p)
            changes_to_apply = [
                StagedChange(
                    tier="context",
                    rel_path=rel_p,
                    content=content,
                    diff_preview=proposal.get("diff", ""),
                    is_new_file=not os.path.exists(target_p),
                )
            ]
            summary = proposal.get("reason", "Approved operational update")

        # Validate all paths
        for ch in changes_to_apply:
            self._safe_path(ch.rel_path)

        backups: Dict[str, Optional[str]] = {}
        written_targets: List[str] = []
        tmp_targets: List[str] = []

        with self.file_lock():
            try:
                # 1. Atomically write each file
                for ch in changes_to_apply:
                    target = self._safe_path(ch.rel_path)
                    parent = os.path.dirname(target)
                    os.makedirs(parent, exist_ok=True)

                    if os.path.exists(target):
                        with open(target, "r", encoding="utf-8", errors="replace") as f:
                            backups[target] = f.read()
                    else:
                        backups[target] = None

                    tmp_target = target + f".{uuid.uuid4().hex[:6]}.tmp"
                    tmp_targets.append(tmp_target)

                    with open(tmp_target, "w", encoding="utf-8") as f:
                        f.write(ch.content)
                        f.flush()
                        os.fsync(f.fileno())

                    os.replace(tmp_target, target)
                    tmp_targets.remove(tmp_target)
                    written_targets.append(target)

                # 2. Stage all files in git
                for ch in changes_to_apply:
                    target = self._safe_path(ch.rel_path)
                    rel = os.path.relpath(target, self.wiki_dir)
                    subprocess.run(["git", "add", rel], cwd=self.wiki_dir, check=True, capture_output=True)

                # 3. Single atomic commit
                if is_proposal_obj:
                    commit_msg = f"memory: [{token}] {summary}"
                else:
                    commit_msg = f"feat(wiki): update {changes_to_apply[0].rel_path} [{summary}] (approved: {token})"

                subprocess.run(
                    [
                        "git",
                        "-c", "user.name=Golem",
                        "-c", "user.email=golem@fleet.local",
                        "commit",
                        "-m", commit_msg,
                        "--author", "Golem <golem@fleet.local>",
                    ],
                    cwd=self.wiki_dir,
                    check=True,
                    capture_output=True,
                )

                res = subprocess.run(
                    ["git", "rev-parse", "--short", "HEAD"],
                    cwd=self.wiki_dir,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                commit_hash = res.stdout.strip()
                result = CommitResult(commit_hash)
                self.last_commit_hash = commit_hash
                if is_proposal_obj:
                    proposal.commit_hash = commit_hash
                    proposal.status = "approved"
                else:
                    proposal["status"] = "approved"
                    proposal["commit_hash"] = commit_hash

                del self._pending_updates[token]
                return result
            except Exception as e:
                logger.error(f"Error during atomic memory confirm_update for {token}: {e}")
                # Rollback temp files
                for tmp in list(tmp_targets):
                    try:
                        if os.path.exists(tmp):
                            os.remove(tmp)
                    except OSError:
                        pass

                # Rollback written targets
                for target, prev_content in backups.items():
                    try:
                        if prev_content is not None:
                            with open(target, "w", encoding="utf-8") as f:
                                f.write(prev_content)
                        elif os.path.exists(target):
                            os.remove(target)
                    except OSError:
                        pass

                # Reset git index
                try:
                    subprocess.run(["git", "reset", "HEAD", "--", "."], cwd=self.wiki_dir, capture_output=True)
                except Exception:
                    pass

                raise WikiError(f"Atomic memory commit failed for {token}: {e}") from e

    def reject_update(self, token: str) -> bool:
        """Reject and discard a pending update."""
        proposal = self._pending_updates.get(token)
        if not proposal:
            return False
        if isinstance(proposal, dict):
            if proposal.get("status") != "pending":
                return False
            proposal["status"] = "rejected"
        else:
            proposal.status = "rejected"
        del self._pending_updates[token]
        return True

    def list_pending_updates(self) -> List[Any]:
        """Returns a list of all currently pending updates/proposals."""
        return list(self._pending_updates.values())


def logbook_append(entry: str, title: Optional[str] = None, wiki_dir: Optional[Union[str, Path]] = None) -> str:
    """Append an operational journal entry to today's logbook file."""
    wiki = GolemWiki(wiki_dir=str(wiki_dir) if wiki_dir else None)
    return wiki.logbook_append(entry=entry, title=title)


def propose_update(
    rel_path: str,
    content: str,
    reason: str = "",
    summary: Optional[str] = None,
    tier: Optional[str] = None,
    wiki_dir: Optional[Union[str, Path]] = None,
) -> Union[MemoryProposal, Dict[str, Any]]:
    """Stage a proposed runbook or memory update requiring confirmation."""
    wiki = GolemWiki(wiki_dir=str(wiki_dir) if wiki_dir else None)
    return wiki.propose_update(rel_path=rel_path, content=content, reason=reason or summary or "")


def read_wiki_document(rel_path: str, wiki_dir: Optional[Union[str, Path]] = None) -> str:
    """Read content of a wiki document relative to ~/.golem/wiki/."""
    wiki = GolemWiki(wiki_dir=str(wiki_dir) if wiki_dir else None)
    return wiki.read(rel_path=rel_path)


__all__ = [
    "GolemWiki",
    "WikiError",
    "WikiSecurityError",
    "CommitResult",
    "StagedChange",
    "MemoryProposal",
    "DEFAULT_WIKI_DIR",
    "DEFAULT_CAPABILITIES_TEMPLATE",
    "DEFAULT_TOPOLOGY_TEMPLATE",
    "logbook_append",
    "propose_update",
    "read_wiki_document",
]

