#!/usr/bin/env python3
"""
golem/webui/pane_mirror.py
Live Herdr pane mirror for the WebUI.

Herdr only exposes *snapshots* of a pane (``herdr pane read``), not a raw PTY byte
stream. This module polls those snapshots for every attached pane, hash-gates them
so unchanged panes cost no bandwidth, and broadcasts full-frame ``pane_frame`` SSE
events that the dashboard renders in a read-only xterm.js terminal.

Because every frame is a complete snapshot, dropped SSE frames self-heal on the
next change and late-joining browsers can be primed from the cached last frame.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("golem.webui.pane_mirror")

DEFAULT_INTERVAL = 0.2
DEFAULT_IDLE_INTERVAL = 1.0
DEFAULT_LINES = 300
DEFAULT_SOURCE = "recent-unwrapped"
READ_TIMEOUT = 3.0
MAX_ERROR_BACKOFF = 5.0
MAX_CONCURRENT_READS = 4


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def is_safe_pane_id(pane_id: str) -> bool:
    """Reject empty ids and anything that could be parsed as a CLI option."""
    return bool(pane_id) and not pane_id.startswith("-") and "\x00" not in pane_id


# Human labels shared by many panes/workspaces; `herdr pane read <label>` is ambiguous.
GENERIC_PANE_LABELS = frozenset({"runner", "liaison", "control", "system", "daemon"})


def is_generic_pane_label(pane_id: str) -> bool:
    """True for non-concrete labels such as "runner" that must never be read directly."""
    return (pane_id or "").strip().lower() in GENERIC_PANE_LABELS


class _PaneWatch:
    __slots__ = ("pane_id", "herdr_pane", "task", "digest", "rev", "text", "state")

    def __init__(self, pane_id: str, herdr_pane: str):
        self.pane_id = pane_id
        self.herdr_pane = herdr_pane
        self.task: Optional[asyncio.Task] = None
        self.digest: str = ""
        self.rev: int = 0
        self.text: str = ""
        self.state: str = "connecting"


class PaneMirror:
    """
    Polls ``herdr pane read`` for attached panes and broadcasts ``pane_frame`` events.

    Must be created and driven on the WebUI server's event loop. ``broadcast`` is the
    server's SSE fan-out callable and ``has_subscribers`` gates polling so nothing
    runs while no browser is watching.
    """

    def __init__(
        self,
        broadcast: Callable[[str, Any], Any],
        has_subscribers: Callable[[], bool],
        herdr_bin: Optional[str] = None,
        interval: Optional[float] = None,
        idle_interval: Optional[float] = None,
        lines: Optional[int] = None,
        source: Optional[str] = None,
    ):
        self._broadcast = broadcast
        self._has_subscribers = has_subscribers
        self.herdr_bin = herdr_bin or os.environ.get("GOLEM_HERDR_BIN", "herdr")
        self.interval = interval if interval is not None else _env_float("GOLEM_WEBUI_PANE_INTERVAL", DEFAULT_INTERVAL)
        self.idle_interval = (
            idle_interval if idle_interval is not None
            else _env_float("GOLEM_WEBUI_PANE_IDLE_INTERVAL", DEFAULT_IDLE_INTERVAL)
        )
        self.lines = lines if lines is not None else _env_int("GOLEM_WEBUI_PANE_LINES", DEFAULT_LINES)
        self.source = source or os.environ.get("GOLEM_WEBUI_PANE_SOURCE", DEFAULT_SOURCE)
        self._watches: Dict[str, _PaneWatch] = {}
        self._read_slots: Optional[asyncio.Semaphore] = None

    # ------------------------------------------------------------------ lifecycle

    def attach(self, pane_id: str, herdr_pane: Optional[str] = None) -> bool:
        """Start mirroring ``pane_id`` (idempotent). Returns False if the id is unusable."""
        target = herdr_pane or pane_id
        if not is_safe_pane_id(target):
            logger.debug("Refusing to mirror unsafe pane id %r", target)
            return False
        if is_generic_pane_label(target):
            logger.debug("Refusing to mirror generic pane label %r; a concrete Herdr pane id is required", target)
            return False
        existing = self._watches.get(pane_id)
        if existing is not None and existing.herdr_pane == target:
            return True
        if existing is not None:
            self.detach(pane_id)
        watch = _PaneWatch(pane_id, target)
        if self._has_subscribers():
            try:
                watch.task = asyncio.get_running_loop().create_task(self._poll_loop(watch))
            except RuntimeError:
                pass
        self._watches[pane_id] = watch
        return True

    def _ensure_tasks(self) -> None:
        """Start polling tasks for any watches that do not yet have an active task."""
        if not self._has_subscribers():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        for watch in self._watches.values():
            if watch.task is None or watch.task.done():
                watch.task = loop.create_task(self._poll_loop(watch))

    def detach(self, pane_id: str) -> None:
        watch = self._watches.pop(pane_id, None)
        if watch is not None and watch.task is not None:
            watch.task.cancel()

    async def shutdown(self) -> None:
        tasks: List[asyncio.Task] = []
        for watch in self._watches.values():
            if watch.task is not None:
                watch.task.cancel()
                tasks.append(watch.task)
        self._watches.clear()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    @property
    def attached(self) -> List[str]:
        return list(self._watches)

    def snapshots(self) -> List[Dict[str, Any]]:
        """Last known frame per pane, used to prime newly connected browsers."""
        self._ensure_tasks()
        return [
            {"pane_id": w.pane_id, "rev": w.rev, "text": w.text}
            for w in self._watches.values()
            if w.rev > 0
        ]

    # ------------------------------------------------------------------ polling

    async def read_pane(self, herdr_pane: str) -> str:
        """Run ``herdr pane read`` once and return ANSI text. Raises on failure."""
        if self._read_slots is None:
            self._read_slots = asyncio.Semaphore(MAX_CONCURRENT_READS)
        async with self._read_slots:
            return await self._read_pane_unbounded(herdr_pane)

    async def _read_pane_unbounded(self, herdr_pane: str) -> str:
        proc = await asyncio.create_subprocess_exec(
            self.herdr_bin, "pane", "read", herdr_pane,
            "--lines", str(self.lines),
            "--source", self.source,
            "--format", "ansi",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=READ_TIMEOUT)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()
            raise RuntimeError(f"herdr pane read timed out after {READ_TIMEOUT:.0f}s")
        except asyncio.CancelledError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            raise
        if proc.returncode != 0:
            msg = err.decode("utf-8", errors="replace").strip().splitlines()
            raise RuntimeError(msg[-1] if msg else f"herdr exited {proc.returncode}")
        return out.decode("utf-8", errors="replace")

    def _set_state(self, watch: _PaneWatch, state: str, error: str = "") -> None:
        if watch.state == state and not error:
            return
        changed = watch.state != state
        watch.state = state
        if changed:
            self._broadcast("pane_state", {"pane_id": watch.pane_id, "state": state, "error": error})

    async def _poll_loop(self, watch: _PaneWatch) -> None:
        delay = self.interval
        failures = 0
        last_change = time.monotonic()
        try:
            while True:
                if not self._has_subscribers():
                    watch.task = None
                    return
                try:
                    text = await self.read_pane(watch.herdr_pane)
                except asyncio.CancelledError:
                    raise
                except FileNotFoundError:
                    failures += 1
                    self._set_state(watch, "unavailable", f"'{self.herdr_bin}' not found on this host")
                    delay = MAX_ERROR_BACKOFF
                except Exception as err:
                    failures += 1
                    self._set_state(watch, "unavailable", str(err))
                    delay = min(MAX_ERROR_BACKOFF, self.interval * (2 ** min(failures, 5)))
                else:
                    failures = 0
                    self._set_state(watch, "live")
                    digest = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
                    if digest != watch.digest:
                        watch.digest = digest
                        watch.rev += 1
                        watch.text = text
                        last_change = time.monotonic()
                        self._broadcast("pane_frame", {
                            "pane_id": watch.pane_id,
                            "rev": watch.rev,
                            "text": text,
                        })
                        delay = self.interval
                    else:
                        # Back off while the pane is quiet; snap back on the next change.
                        quiet = time.monotonic() - last_change
                        delay = self.interval if quiet < 3.0 else min(self.idle_interval, self.interval * 4)
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            pass
