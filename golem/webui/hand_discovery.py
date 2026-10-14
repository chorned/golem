#!/usr/bin/env python3
"""
golem/webui/hand_discovery.py
Herdr -> WebUI hands reconciliation.

The hands registry only knows about agents that Golem's runner explicitly registers, so
any agent Herdr is running that was never registered is invisible in the dashboard. This
module treats Herdr as the source of truth: it polls ``herdr pane list`` and keeps a hand
card for every pane that looks like a worker, with live pane mirroring via its real pane id.

``herdr agent list`` is deliberately not used: it only returns panes whose agent session
Herdr detected, and misses e.g. task panes (``agent_status: unknown``, no ``agent`` field).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("golem.webui.hand_discovery")

DEFAULT_INTERVAL = 3.0
LIST_TIMEOUT = 4.0
MISSING_BINARY_BACKOFF = 30.0
# Transport panes to remote nodes and the Mouth liaison are not hands; each has its own place in the UI.
DEFAULT_HIDE_LABELS = "oci-worker,laptop-worker,pc-worker,chief-of-staff,liaison,golem_liaison"
WORKTREE_MARKER = "/.herdr/worktrees/"

_STATUS_MAP = {
    "working": "RUNNING",
    "running": "RUNNING",
    "busy": "RUNNING",
    "idle": "IDLE",
    "blocked": "BLOCKED",
    "waiting": "BLOCKED",
    "done": "EXIT_0",
    "unknown": "RUNNING",
    "": "RUNNING",
}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def parse_pane_list(raw: str) -> List[Dict[str, Any]]:
    """Extract the panes array from ``herdr pane list`` JSON output."""
    data = json.loads(raw)
    result = data.get("result", data) if isinstance(data, dict) else {}
    panes = result.get("panes") if isinstance(result, dict) else None
    return [p for p in (panes or []) if isinstance(p, dict) and p.get("pane_id")]


def is_hand_pane(pane: Dict[str, Any], hide_labels: Set[str]) -> bool:
    """A pane is a hand if Herdr sees an agent in it, or it is a task worktree pane."""
    label = (pane.get("label") or pane.get("name") or "").strip()
    if label:
        if (
            label in hide_labels
            or label.endswith("-liaison")
            or label.endswith("_liaison")
            or label.startswith("g_")
            or label == "chief-of-staff"
        ):
            return False

    pane_id = str(pane.get("pane_id") or "").strip()
    agent = (pane.get("agent") or (pane.get("agent_session") or {}).get("agent") or "").strip()
    agent_session = pane.get("agent_session") or {}
    agent_name = (pane.get("agent_name") or agent_session.get("name") or "").strip()
    if agent_name:
        if (
            agent_name in hide_labels
            or agent_name.endswith("-liaison")
            or agent_name.endswith("_liaison")
            or agent_name.startswith("g_")
            or agent_name == "chief-of-staff"
        ):
            return False

    has_worktree = False
    for key in ("cwd", "foreground_cwd"):
        if WORKTREE_MARKER in (pane.get(key) or ""):
            has_worktree = True
            break

    # Root pane 1 running agy without a worktree marker is the session liaison pane, not a worker hand
    if (pane_id.endswith(":p1") or pane_id == "1") and agent in ("agy", "antigravity") and not has_worktree:
        return False

    if pane.get("agent") or pane.get("agent_session"):
        return True
    if has_worktree:
        return True
    return False


def hand_fields(pane: Dict[str, Any]) -> Dict[str, Any]:
    """Map a Herdr pane to the registry fields of a hand card."""
    label = (pane.get("label") or pane.get("name") or "").strip()
    agent = pane.get("agent") or (pane.get("agent_session") or {}).get("agent") or ""
    cwd = pane.get("foreground_cwd") or pane.get("cwd") or ""
    kind = agent or "task pane"
    status = _STATUS_MAP.get(str(pane.get("agent_status") or "").lower(), str(pane.get("agent_status")).upper())
    return {
        "title": label or pane["pane_id"],
        "command": f"{kind} @ {cwd}" if cwd else kind,
        "worker": agent or "pane",
        "status": status,
    }


class HerdrHandSync:
    """Keeps the WebUI hands registry in step with ``herdr pane list``."""

    def __init__(
        self,
        server: Any,
        herdr_bin: Optional[str] = None,
        interval: Optional[float] = None,
        hide_labels: Optional[Set[str]] = None,
    ):
        self.server = server
        self.herdr_bin = herdr_bin or os.environ.get("GOLEM_HERDR_BIN", "herdr")
        self.interval = interval if interval is not None else _env_float("GOLEM_WEBUI_HERDR_SYNC_INTERVAL", DEFAULT_INTERVAL)
        if hide_labels is None:
            raw = os.environ.get("GOLEM_WEBUI_HERDR_HIDE_LABELS", DEFAULT_HIDE_LABELS)
            hide_labels = {s.strip() for s in raw.split(",") if s.strip()}
        self.hide_labels = hide_labels
        # herdr pane_id -> registry key of the hand we created for it
        self.discovered: Dict[str, str] = {}
        self.killed_panes: Set[str] = set()
        self._warned_missing = False

    def mark_killed(self, *pane_ids: Optional[str]) -> None:
        """Mark one or more pane IDs or labels as permanently killed so discovery never re-adds them."""
        for pid in pane_ids:
            if pid:
                self.killed_panes.add(pid)
                self.discovered.pop(pid, None)

    async def list_panes(self) -> List[Dict[str, Any]]:
        proc = await asyncio.create_subprocess_exec(
            self.herdr_bin, "pane", "list",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=LIST_TIMEOUT)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()
            raise RuntimeError("herdr pane list timed out")
        except asyncio.CancelledError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            raise
        if proc.returncode != 0:
            msg = err.decode("utf-8", errors="replace").strip().splitlines()
            raise RuntimeError(msg[-1] if msg else f"herdr exited {proc.returncode}")
        return parse_pane_list(out.decode("utf-8", errors="replace"))

    def _represented_by_registered_hand(self, pane_id: str) -> bool:
        """True if a hand registered by the runner already targets this Herdr pane."""
        own = set(self.discovered.values())
        for key, meta in self.server._hand_meta.items():
            if key in own or key not in self.server._active_hands:
                continue
            if meta.get("identity", (None, None))[1] == pane_id:
                return True
        for key, hand in self.server._active_hands.items():
            if key in own:
                continue
            if hand.get("herdr_pane_id") == pane_id:
                return True
        return False

    def reconcile(self, panes: List[Dict[str, Any]]) -> bool:
        """Apply one snapshot of panes to the registry. Returns True if anything changed."""
        srv = self.server
        wanted = {
            p["pane_id"]: p for p in panes
            if is_hand_pane(p, self.hide_labels)
            and p["pane_id"] not in self.killed_panes
            and (p.get("label") or "").strip() not in self.killed_panes
        }
        changed = False

        for pane_id, key in list(self.discovered.items()):
            if pane_id not in wanted or key not in srv._active_hands:
                srv.unregister_active_hand(key)  # broadcasts on its own
                self.discovered.pop(pane_id, None)

        for pane_id, pane in wanted.items():
            fields = hand_fields(pane)
            key = self.discovered.get(pane_id)
            if key is not None and key in srv._active_hands:
                hand = srv._active_hands[key]
                for field in ("status", "command", "worker"):
                    if hand.get(field) != fields[field]:
                        hand[field] = fields[field]
                        changed = True
                continue
            if self._represented_by_registered_hand(pane_id):
                continue
            key = srv._register_hand(
                fields["title"], "localhost", fields["command"], None, fields["worker"],
                herdr_pane_id=pane_id,
            )
            srv._active_hands[key]["status"] = fields["status"]
            self.discovered[pane_id] = key
            changed = True

        if changed:
            srv._broadcast_hands()
        return changed

    async def run(self) -> None:
        delay = self.interval
        try:
            while True:
                await asyncio.sleep(delay)
                try:
                    panes = await self.list_panes()
                except asyncio.CancelledError:
                    raise
                except FileNotFoundError:
                    if not self._warned_missing:
                        logger.info("'%s' not found; Herdr hand discovery idle", self.herdr_bin)
                        self._warned_missing = True
                    delay = MISSING_BINARY_BACKOFF
                    continue
                except Exception as err:
                    logger.debug("herdr pane list failed: %s", err)
                    delay = min(30.0, max(self.interval, delay) * 2)
                    continue
                delay = self.interval
                self.reconcile(panes)
        except asyncio.CancelledError:
            pass
