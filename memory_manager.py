#!/usr/bin/env python3
"""
memory_manager.py
Episodic Ledger & 3-Tier Memory Manager for First Mate / AGY.
Provides human-inspectable, git-trackable operational event logging via JSONL,
grounded conversational task recovery, and recent milestone retrieval.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

logger = logging.getLogger("AGY.MemoryManager")

HOST_DIR = os.environ.get("GOLEM_HOST_DIR", os.path.dirname(os.path.abspath(__file__)))
DEFAULT_LEDGER_PATH = os.environ.get(
    "GOLEM_LEDGER_PATH",
    os.path.join(HOST_DIR, "memory", "ledger.jsonl")
)


class dualmethod:
    """Descriptor that dispatches to instance or owner's default instance."""
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        if instance is not None:
            return lambda *args, **kwargs: self.func(instance, *args, **kwargs)
        return lambda *args, **kwargs: self.func(owner.get_instance(), *args, **kwargs)


class MemoryManager:
    """
    Manages episodic memory stored in memory/ledger.jsonl.
    Human-inspectable, append-only JSONL format for tracking operational milestones.
    Supports both instance methods and class methods for seamless invocation.
    """

    _default_instance: Optional["MemoryManager"] = None

    def __init__(self, ledger_path: Optional[str] = None):
        self.ledger_path = ledger_path or DEFAULT_LEDGER_PATH
        self._ensure_ledger_file()

    def _ensure_ledger_file(self):
        """Ensure parent directory and ledger.jsonl file exist."""
        try:
            parent = os.path.dirname(self.ledger_path)
            if parent and not os.path.exists(parent):
                os.makedirs(parent, exist_ok=True)
            if not os.path.exists(self.ledger_path):
                with open(self.ledger_path, "a", encoding="utf-8") as f:
                    pass
        except Exception as e:
            logger.warning(f"Error ensuring ledger file {self.ledger_path}: {e}")

    @classmethod
    def get_instance(cls, ledger_path: Optional[str] = None) -> "MemoryManager":
        """Get or initialize the singleton MemoryManager instance."""
        if cls._default_instance is None or (ledger_path and cls._default_instance.ledger_path != ledger_path):
            cls._default_instance = cls(ledger_path=ledger_path)
        return cls._default_instance

    @dualmethod
    def log_event(self, event_type: str, **kwargs) -> Dict[str, Any]:
        """
        Append a single JSON event to ledger.jsonl:
        {"ts": "<ISO-timestamp>", "type": "task_dispatched"|"pr_opened"|"task_merged"|"task_aborted", ...}
        """
        self._ensure_ledger_file()

        event: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "type": event_type,
        }
        event.update(kwargs)

        try:
            line = json.dumps(event)
            with open(self.ledger_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            logger.info(f"Logged memory event [{event_type}]: {event}")
        except Exception as e:
            logger.error(f"Failed to append to ledger {self.ledger_path}: {e}")

        return event

    @dualmethod
    def get_recent_events(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Reads the tail of ledger.jsonl for fast operational grounding.
        Returns the last `limit` events in chronological order.
        """
        if not os.path.exists(self.ledger_path):
            return []

        events: List[Dict[str, Any]] = []
        try:
            with open(self.ledger_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        events.append(json.loads(line))
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"Error reading ledger {self.ledger_path}: {e}")
            return []

        return events[-limit:] if limit > 0 else events

    @dualmethod
    def get_latest_task_state(self, repo: Optional[str] = None) -> Dict[str, Any]:
        """
        Finds and returns the most recent task record to answer status/cleanup questions.
        If multiple milestone events belong to the same task_id, merges them chronologically
        so the returned state reflects the final consolidated status (e.g. worktree, pr_number, cleaned).
        """
        if not os.path.exists(self.ledger_path):
            return {}

        events: List[Dict[str, Any]] = []
        try:
            with open(self.ledger_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        events.append(json.loads(line))
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"Error reading ledger {self.ledger_path}: {e}")
            return {}

        if not events:
            return {}

        # Filter by repo if provided
        filtered: List[Dict[str, Any]] = []
        for ev in events:
            if repo:
                ev_repo = ev.get("repo")
                if ev_repo:
                    if (
                        os.path.abspath(str(ev_repo)) == os.path.abspath(str(repo))
                        or os.path.basename(str(ev_repo)) == os.path.basename(str(repo))
                    ):
                        filtered.append(ev)
                else:
                    filtered.append(ev)
            else:
                filtered.append(ev)

        if not filtered:
            return {}

        last_ev = filtered[-1]
        latest_task_id = last_ev.get("task_id")

        # If last event didn't carry task_id, look backwards for linked task_id
        if not latest_task_id:
            for ev in reversed(filtered):
                if ev.get("task_id"):
                    if (
                        (last_ev.get("pr_number") and ev.get("pr_number") == last_ev.get("pr_number"))
                        or (last_ev.get("worktree") and ev.get("worktree") == last_ev.get("worktree"))
                        or (last_ev.get("branch") and ev.get("branch") == ev.get("branch"))
                    ):
                        latest_task_id = ev.get("task_id")
                        break
            if not latest_task_id:
                # Fallback to the latest event's task_id if any event had one
                for ev in reversed(filtered):
                    if ev.get("task_id"):
                        latest_task_id = ev.get("task_id")
                        break

        if latest_task_id:
            task_state: Dict[str, Any] = {}
            for ev in filtered:
                if ev.get("task_id") == latest_task_id:
                    task_state.update(ev)
            # If the very last event in filtered was a terminal event (task_merged/task_aborted) for this repo,
            # make sure its status overrides
            if last_ev.get("type") in ("task_merged", "task_aborted") and not last_ev.get("task_id"):
                task_state.update(last_ev)
            return task_state

        return dict(last_ev)


# Module-level convenience functions
def log_event(event_type: str, **kwargs) -> Dict[str, Any]:
    return MemoryManager.log_event(event_type, **kwargs)


def get_recent_events(limit: int = 10) -> List[Dict[str, Any]]:
    return MemoryManager.get_recent_events(limit=limit)


def get_latest_task_state(repo: Optional[str] = None) -> Dict[str, Any]:
    return MemoryManager.get_latest_task_state(repo=repo)
