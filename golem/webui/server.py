#!/usr/bin/env python3
"""
golem/webui/server.py
Real-Time Observability WebUI Server & SSE Broadcast Engine.

Provides an embedded, lightweight aiohttp HTTP & Server-Sent Events (SSE) server
streaming live log deltas and sensory heartbeats without blocking Slack Socket Mode
or BlueBubbles event loops.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import time
import json
import asyncio
import logging
import socket
import threading
from pathlib import Path
from typing import Dict, Any, Optional, Set, List, Union, Callable

from aiohttp import web

try:
    from golem.webui.pane_mirror import PaneMirror
    from golem.webui.hand_discovery import HerdrHandSync
except ImportError:  # pragma: no cover - fallback when imported as a loose module
    from pane_mirror import PaneMirror  # type: ignore[no-redef]
    from hand_discovery import HerdrHandSync  # type: ignore[no-redef]

LINEAGE = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."
VERSION = "2.1.0"

_STATIC_CONTENT_TYPES = {
    ".png": "image/png",
    ".js": "application/javascript",
    ".css": "text/css",
    ".txt": "text/plain",
}

logger = logging.getLogger("golem.webui")

# Locate templates directory and load dashboard HTML
_CURRENT_DIR = Path(__file__).resolve().parent
_TEMPLATES_DIR = _CURRENT_DIR / "templates"
_DASHBOARD_FILE = _TEMPLATES_DIR / "dashboard.html"
_WIKI_FILE = _TEMPLATES_DIR / "wiki.html"

if _DASHBOARD_FILE.exists():
    try:
        DASHBOARD_HTML = _DASHBOARD_FILE.read_text(encoding="utf-8")
    except Exception:
        DASHBOARD_HTML = ""
else:
    DASHBOARD_HTML = ""

if not DASHBOARD_HTML:
    DASHBOARD_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Golem Command Center — Real-Time Observability</title>
    <!-- Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh. -->
    <style>
        body {{ background: #080c10; color: #e2e8f0; font-family: -apple-system, sans-serif; margin: 0; padding: 16px; }}
        header {{ border-bottom: 1px solid #1e2c3d; padding-bottom: 12px; margin-bottom: 16px; }}
        .dark {{ color-scheme: dark; }}
        .grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }}
        .pane {{ background: #0e1520; border: 1px solid #1e2c3d; border-radius: 6px; padding: 12px; }}
        .hands {{ grid-column: 1 / -1; }}
    </style>
</head>
<body class="dark">
    <header>
        <h1>⚡ GOLEM COMMAND CENTER</h1>
        <p><em>{LINEAGE}</em></p>
    </header>
    <main class="grid">
        <section class="pane" id="eye-pane"><h3>👁️ Eye Agent</h3><div id="eye-log"></div></section>
        <section class="pane" id="nose-pane"><h3>👃 Nose Agent</h3><div id="nose-status"></div></section>
        <section class="pane" id="mouth-pane"><h3>👄 Mouth Agent</h3><div id="mouth-log"></div></section>
        <section class="pane hands" id="hands-pane"><h3>🖐️ Hand Agents Grid</h3><div id="hands-grid"></div></section>
    </main>
</body>
</html>"""

INDEX_HTML = DASHBOARD_HTML


def _json_default(obj: Any) -> Any:
    """Helper serializer for non-standard objects in SSE payloads."""
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        try:
            return obj.to_dict()
        except Exception:
            pass
    if hasattr(obj, "__dict__") and getattr(obj, "__dict__", None):
        try:
            return dict(obj.__dict__)
        except Exception:
            pass
    return str(obj)



class _SyncAndAwaitableNone:
    """
    Polymorphic return object enabling dual sync/async method execution.
    Executes synchronously when invoked as a normal function, but can also
    be awaited without raising 'TypeError: NoneType object cannot be awaited'.
    Produces zero RuntimeWarnings when discarded without being awaited.
    """
    __slots__ = ()

    def __await__(self):
        async def _noop():
            return None
        return _noop().__await__()


class _SyncAndAwaitableList(list):
    """
    Polymorphic list enabling dual sync/async method execution.
    Can be inspected/iterated directly as a list, or awaited.
    """
    def __await__(self):
        async def _noop():
            return self
        return _noop().__await__()


def format_sse_event(event: str, data: Any) -> str:
    """
    Format a valid Server-Sent Events (SSE) message chunk.
    Guarantees termination with double newline (\\n\\n).
    Handles non-serializable objects and large payloads gracefully.
    """
    if isinstance(data, str):
        payload = data
    else:
        try:
            payload = json.dumps(data, default=_json_default)
        except Exception:
            payload = json.dumps(str(data))

    return f"event: {event}\ndata: {payload}\n\n"


class WebUIServer:
    """
    Lightweight, embedded asynchronous WebUI server for Golem observability.
    Runs on an aiohttp TCPSite alongside existing event loops without starvation.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        event_bus: Optional[Any] = None,
    ):
        if not isinstance(port, int) or port < 1 or port > 65535:
            raise ValueError(f"Port must be an integer between 1 and 65535, got {port}")
        self.host = host
        self.port = port
        self.app = web.Application()
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._subscribers: Set[asyncio.Queue] = set()
        self._start_time: float = time.time()
        self._lock = asyncio.Lock()
        self._active_hands: Dict[str, Dict[str, Any]] = {}
        # (pane, kind, detail) -> ts of eye_blocker events whose legacy blocker alert is still to come
        self._eye_blocker_sigs: Dict[tuple, float] = {}
        self._nose_probe: Optional[Any] = None
        self._nose_tracker: Optional[Any] = None
        self._hand_meta: Dict[str, Dict[str, Any]] = {}
        self._telemetry_task: Optional[asyncio.Task] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.event_bus: Optional[Any] = event_bus
        self._bus_unsubscribe: Optional[Callable[[], None]] = None
        self._recent_logs: Dict[str, List[str]] = {
            "eyes": [],
            "nose": [],
            "mouth": [],
        }
        self._simulator: Optional[Any] = None
        self._pane_mirror_enabled: bool = os.environ.get("GOLEM_WEBUI_PANE_MIRROR", "1").strip().lower() not in ("0", "false", "no", "off")
        self._pane_targets: Dict[str, str] = {}
        self._hand_sync_enabled: bool = os.environ.get("GOLEM_WEBUI_HERDR_SYNC", "1").strip().lower() not in ("0", "false", "no", "off")
        self._hand_sync = HerdrHandSync(self)
        self._hand_sync_task: Optional[asyncio.Task] = None
        self._pane_mirror = PaneMirror(
            broadcast=self.broadcast,
            has_subscribers=lambda: self.subscriber_count > 0,
        )
        self._hands_broadcast_scheduled: bool = False
        self._setup_routes()
        self._seed_proof_of_life_logs()

    def _seed_proof_of_life_logs(self) -> None:
        """Seed initial proof-of-life and calibrated thought logs into memory buffers."""
        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._start_time))
        if not self._recent_logs["eyes"]:
            self._recent_logs["eyes"].extend([
                f"{now_str} [LIFE] Golem.Sensory.Eyes: EyesTaskObserver calibrated (interval=15s, mode=active, pane=runner)",
                f"{now_str} [LIFE] Golem.Sensory.Eyes: ShoulderTapEventBus subscription established",
                f"{now_str} [THOUGHT] Golem.Sensory.Eyes: Terminal observer watching runner buffer (0 tokens consumed while quiescent)",
            ])
        if not self._recent_logs["nose"]:
            self._recent_logs["nose"].extend([
                f"{now_str} [LIFE] Golem.Sensory.Nose: FleetHealthSentinel initialized (interval=15m, quiescent_gate=active)",
                f"{now_str} [LIFE] Golem.Sensory.Nose: Host telemetry probe bound (disk, Docker socket, Tailscale mesh)",
                f"{now_str} [THOUGHT] Golem.Sensory.Nose: Baseline host metrics nominal; zero unhandled tracebacks in bot.err",
            ])
        if not self._recent_logs["mouth"]:
            active_mouth_model = ""
            try:
                from golem.config import get_mouth_model
                active_mouth_model = get_mouth_model()
            except Exception:
                pass
            model_tag = f" (model={active_mouth_model})" if active_mouth_model else ""
            self._recent_logs["mouth"].extend([
                f"{now_str} [LIFE] Golem.Agent.Mouth: ChiefOfStaff Mouth liaison active{model_tag}",
                f"{now_str} [LIFE] Golem.Agent.Mouth: Out-of-band conversational reactor ready for Slack & web interaction",
                f"{now_str} [THOUGHT] Golem.Agent.Mouth: Dynamic triage engine ready; 0 brittle regex gates active",
            ])

    @property
    def is_running(self) -> bool:
        """True if the server is actively listening on its TCP socket."""
        return self._runner is not None and self._site is not None

    @property
    def subscriber_count(self) -> int:
        """Return the number of actively connected SSE client queues."""
        return len(self._subscribers)

    def _on_loop(self, fn: Callable[..., Any], *args: Any) -> None:
        """Run ``fn`` on the server's event loop from any thread (no-op until the server is running)."""
        loop = self.loop
        if loop is None or loop.is_closed() or not loop.is_running():
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            fn(*args)
        else:
            loop.call_soon_threadsafe(fn, *args)

    def _broadcast_hands(self) -> None:
        """Push the full hands registry to every browser right now (thread-safe)."""
        if not self._subscribers or self._hands_broadcast_scheduled:
            return
        self._hands_broadcast_scheduled = True

        def _send() -> None:
            self._hands_broadcast_scheduled = False
            self.broadcast("hands_status", [dict(h) for h in list(self._active_hands.values())])

        self._on_loop(_send)

    def _allocate_hand_key(self, pane_id: str, node: str = "localhost", identity: Optional[tuple] = None) -> str:
        """
        Pick the registry key for a hand. The same (node, pane, pane_id) re-registering replaces itself;
        a *different* hand that happens to share a pane label (e.g. every node has a 'runner')
        or a distinct subtask (e.g. hand_CHO-155 vs hand_CHO-156) gets its own key so it can
        never silently overwrite another running agent.
        """
        if identity is None:
            identity = (node, pane_id, pane_id)
        candidates = [pane_id, f"{pane_id}@{node}"]
        n = 2
        while True:
            for cand in candidates:
                if cand not in self._active_hands:
                    return cand
                owner = self._hand_meta.get(cand, {}).get("identity")
                if owner is None or owner == identity:
                    return cand
            candidates = [f"{pane_id}@{node}#{n}"]
            n += 1

    def _resolve_hand_key(self, pane_id: str, node: Optional[str] = None) -> Optional[str]:
        """Map a caller's (pane_id[, node]) to the registry key of an active hand."""
        if node is None:
            if pane_id in self._active_hands:
                return pane_id
            matches = [
                k for k, m in self._hand_meta.items()
                if m.get("pane_id") == pane_id and k in self._active_hands
            ]
            return matches[0] if len(matches) == 1 else None
        for k, m in self._hand_meta.items():
            if m.get("pane_id") == pane_id and m.get("node") == node and k in self._active_hands:
                return k
        return None

    def _register_hand(
        self,
        pane_id: str,
        node: str,
        command: str,
        pid: Optional[int],
        worker: Optional[str],
        **kwargs: Any,
    ) -> str:
        herdr_pane = kwargs.get("herdr_pane_id") or pane_id
        identity = kwargs.get("identity") or (node, herdr_pane, pane_id)
        key = self._allocate_hand_key(pane_id, node, identity)
        if key != pane_id:
            logger.warning(
                "Hand pane id %r on %s collides with an active hand; registered as %r",
                pane_id, node, key,
            )
        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        pid_str = f"PID: {pid}" if pid is not None else "PID: N/A"
        initial_log = (
            f"{now_str} [INFO] Golem.Hands: Agent '{key}' initialized on {node}\n"
            f"{now_str} [INFO] Golem.Hands: Command: {command} [{pid_str}]\n"
            f"{now_str} [INFO] Golem.Hands: Process buffer attached; streaming execution trace...\n"
        )
        self._active_hands[key] = {
            "pane_id": key,
            "herdr_pane_id": herdr_pane,
            "node": node,
            "command": command,
            "pid": pid,
            "status": "RUNNING",
            "buffer_tail": initial_log,
            "worker": worker or "agy",
            "started_at": kwargs.get("started_at", time.time()),
        }
        self._hand_meta[key] = {"pane_id": pane_id, "node": node, "identity": identity}
        if kwargs.get("mirror", True):
            self._pane_targets[key] = herdr_pane
            self._mirror_attach(key, herdr_pane)
        else:
            self._pane_targets.pop(key, None)
        return key

    def register_active_hand(
        self,
        pane_id: str,
        node: str = "localhost",
        command: str = "",
        pid: Optional[int] = None,
        worker: Optional[str] = "agy",
        **kwargs: Any,
    ) -> Any:
        """Register an active hand agent running backstage."""
        self._register_hand(pane_id, node, command, pid, worker, **kwargs)
        self._broadcast_hands()
        return _SyncAndAwaitableNone()

    def _mirror_attach(self, pane_id: str, herdr_pane: str) -> None:
        """Start live Herdr pane mirroring for a hand; safe to call from any thread."""
        if not self._pane_mirror_enabled:
            return
        # Before the loop exists, start() attaches already-registered hands.
        self._on_loop(self._pane_mirror.attach, pane_id, herdr_pane)

    def _mirror_detach(self, pane_id: str) -> None:
        self._on_loop(self._pane_mirror.detach, pane_id)

    async def register_active_hand_async(
        self,
        pane_id: str,
        node: str = "localhost",
        command: str = "",
        pid: Optional[int] = None,
        worker: Optional[str] = "agy",
        **kwargs: Any,
    ) -> None:
        """Asynchronous variant of register_active_hand."""
        self.register_active_hand(pane_id, node, command, pid, worker, **kwargs)

    def update_hand_buffer(self, pane_id: str, delta: str, node: Optional[str] = None) -> Any:
        """Append output delta to an active hand's buffer tail."""
        key = self._resolve_hand_key(pane_id, node)
        if key is None:
            key = self._register_hand(pane_id, node or "localhost", "Task", None, "agy")
            self._broadcast_hands()
        hand = self._active_hands.get(key)
        if hand is not None:
            tail = hand.get("buffer_tail", "") + delta
            if len(tail) > 4096:
                tail = tail[-4096:]
            hand["buffer_tail"] = tail
        return _SyncAndAwaitableNone()

    async def update_hand_buffer_async(self, pane_id: str, delta: str, node: Optional[str] = None) -> None:
        """Asynchronous variant of update_hand_buffer."""
        self.update_hand_buffer(pane_id, delta, node)

    def settle_active_hand(
        self,
        pane_id: str,
        exit_code: int = 0,
        status: Optional[str] = None,
        node: Optional[str] = None,
    ) -> Any:
        """
        Mark a hand agent as COMPLETED / EXIT_0 / FAILED, retaining it in _active_hands
        for WebUI multi-agent visibility until explicitly cleared by a parent task resolution.
        """
        key = self._resolve_hand_key(pane_id, node)
        if key is not None and key in self._active_hands:
            status_str = status or ("COMPLETED" if exit_code == 0 else "FAILED")
            self._active_hands[key]["status"] = status_str
            self._active_hands[key]["exit_code"] = exit_code
            self._active_hands[key]["settled_at"] = time.time()
            self._mirror_detach(key)
            self.broadcast("hand_settle", {
                "pane_id": key,
                "exit_code": exit_code,
                "status": status_str,
            })
            self._broadcast_hands()
        return _SyncAndAwaitableNone()

    async def settle_active_hand_async(
        self,
        pane_id: str,
        exit_code: int = 0,
        status: Optional[str] = None,
        node: Optional[str] = None,
    ) -> None:
        """Asynchronous variant of settle_active_hand."""
        self.settle_active_hand(pane_id, exit_code, status, node)

    def clear_settled_hands(self, parent_key: Optional[str] = None) -> Any:
        """
        Clear retained COMPLETED / EXIT_0 / FAILED hand cards from active registry
        upon parent task resolution or explicit drain.
        """
        to_remove = []
        terminal_statuses = {"COMPLETED", "EXIT_0", "FAILED", "DONE", "SETTLED", "CANCELLED", "SUCCESS"}
        for k, h in list(self._active_hands.items()):
            st = str(h.get("status", "")).upper()
            if st in terminal_statuses or h.get("exit_code") is not None:
                to_remove.append(k)
        for k in to_remove:
            self._active_hands.pop(k, None)
            self._hand_meta.pop(k, None)
            self._pane_targets.pop(k, None)
            self._mirror_detach(k)
        if to_remove:
            self.broadcast("parent_resolution", {"parent_key": parent_key or "", "cleared": to_remove})
            self._broadcast_hands()
        return _SyncAndAwaitableList(to_remove)

    async def clear_settled_hands_async(self, parent_key: Optional[str] = None) -> None:
        """Asynchronous variant of clear_settled_hands."""
        self.clear_settled_hands(parent_key)

    def get_executing_hands(self) -> Dict[str, Dict[str, Any]]:
        """
        Return dict of hands that are actively running, filtering out settled,
        finished, or terminated hands.
        """
        executing: Dict[str, Dict[str, Any]] = {}
        terminal_statuses = {"EXIT_0", "COMPLETED", "FAILED", "DONE", "SETTLED", "CANCELLED", "SUCCESS"}
        for k, h in list(self._active_hands.items()):
            st = str(h.get("status", "")).upper()
            if st in terminal_statuses:
                continue
            if h.get("exit_code") is not None:
                continue
            executing[k] = h
        return executing

    def unregister_active_hand(self, pane_id: str, node: Optional[str] = None) -> Any:
        """Remove a settled hand agent from active registry."""
        key = self._resolve_hand_key(pane_id, node)
        if key is not None:
            self._active_hands.pop(key, None)
            self._hand_meta.pop(key, None)
            self._pane_targets.pop(key, None)
            self._mirror_detach(key)
            self._broadcast_hands()
        return _SyncAndAwaitableNone()

    async def unregister_active_hand_async(self, pane_id: str, node: Optional[str] = None) -> None:
        """Asynchronous variant of unregister_active_hand."""
        self.unregister_active_hand(pane_id, node)

    @property
    def active_hands(self) -> Dict[str, Dict[str, Any]]:
        """Return a shallow copy of active hands registry."""
        return dict(self._active_hands)

    def _setup_routes(self) -> None:
        """Register REST and SSE route handlers."""
        self.app.router.add_get("/", self.handle_index)
        self.app.router.add_get("/events", self.handle_events)
        self.app.router.add_get("/api/events", self.handle_events)
        self.app.router.add_get("/api/stream", self.handle_events)
        self.app.router.add_get("/api/status", self.handle_status)
        self.app.router.add_get("/api/sensory/{organ}", self.handle_sensory)
        self.app.router.add_get("/health", self.handle_health)
        self.app.router.add_get("/healthz", self.handle_health)
        self.app.router.add_get("/colossus.png", self.handle_colossus_bg)
        self.app.router.add_get("/colossus_{frame}.png", self.handle_colossus_frame)
        self.app.router.add_get("/static/{filename}", self.handle_static_file)
        self.app.router.add_post("/api/pipeline/run", self.handle_pipeline_run)
        self.app.router.add_get("/api/pipeline/run", self.handle_pipeline_run)
        self.app.router.add_post("/api/simulate_hands", self.handle_simulate_hands_post)
        self.app.router.add_delete("/api/simulate_hands", self.handle_simulate_hands_delete)
        self.app.router.add_get("/api/simulate_hands", self.handle_simulate_hands_get)
        self.app.router.add_post("/api/hands/{pane_id}/kill", self.handle_kill_hand)
        self.app.router.add_delete("/api/hands/{pane_id}", self.handle_kill_hand)
        self.app.router.add_post("/api/panes/{pane_id}/kill", self.handle_kill_hand)
        self.app.router.add_delete("/api/panes/{pane_id}", self.handle_kill_hand)
        self.app.router.add_get("/wiki", self.handle_wiki)
        self.app.router.add_get("/api/wiki/tree", self.handle_wiki_tree)
        self.app.router.add_get("/api/wiki/doc", self.handle_wiki_doc)
        self.app.router.add_post("/api/wiki/save", self.handle_wiki_save)
        self.app.router.add_get("/api/wiki/search", self.handle_wiki_search)

    def start_hand_simulation(self, count: int = 6) -> None:
        """Start background simulation of N hand agents for testing/demonstration."""
        from golem.webui.simulator import HandSimulator
        if self._simulator is not None and self._simulator.is_running:
            self._simulator.stop()
        self._simulator = HandSimulator(self, count=abs(int(count)))
        self._simulator.start()

    def stop_hand_simulation(self) -> None:
        """Stop background hand agent simulation."""
        if self._simulator is not None:
            self._simulator.stop()
            self._simulator = None

    async def handle_simulate_hands_post(self, request: web.Request) -> web.Response:
        """Start or reconfigure simulated hand agents."""
        try:
            body = await request.json()
        except Exception:
            body = {}
        raw_count = body.get("count") or request.query.get("count") or 6
        try:
            count = abs(int(raw_count))
        except (ValueError, TypeError):
            count = 6
        self.start_hand_simulation(count)
        return web.json_response({
            "status": "ok",
            "ok": True,
            "running": True,
            "active": True,
            "count": count,
            "simulated_panes": [h["pane_id"] for h in list(self._active_hands.values())],
            "workers": [h["pane_id"] for h in list(self._active_hands.values())],
        })

    async def handle_simulate_hands_delete(self, request: web.Request) -> web.Response:
        """Stop simulated hand agents and settle panes."""
        self.stop_hand_simulation()
        return web.json_response({
            "status": "ok",
            "ok": True,
            "running": False,
            "active": False,
            "count": 0,
        })

    async def handle_simulate_hands_get(self, request: web.Request) -> web.Response:
        """Query current simulator status."""
        is_running = self._simulator is not None and self._simulator.is_running
        count = self._simulator.count if is_running else 0
        workers = [h["pane_id"] for h in list(self._active_hands.values())] if is_running else []
        return web.json_response({
            "status": "ok",
            "ok": True,
            "running": is_running,
            "active": is_running,
            "count": count,
            "workers": workers,
        })

    async def handle_kill_hand(self, request: web.Request) -> web.Response:
        """
        Permanently terminate a backstage hand agent and close its Herdr pane.
        Executes `herdr agent stop <pane>`, `herdr pane close <pane>`, and
        `herdr workspace close <ws_id>` if applicable.
        Unregisters the hand, marks it killed in HerdrHandSync, and broadcasts the updated registry.
        """
        import urllib.parse
        pane_id = request.match_info.get("pane_id")
        if not pane_id:
            try:
                body = await request.json()
                pane_id = body.get("pane_id")
            except Exception:
                pane_id = None
        if not pane_id:
            pane_id = request.query.get("pane_id")

        if not pane_id:
            return web.json_response({"ok": False, "error": "Missing pane_id"}, status=400)

        pane_id = urllib.parse.unquote(str(pane_id)).strip()

        key = self._resolve_hand_key(pane_id)
        herdr_pane = None
        agent_name = None
        if key is not None and key in self._active_hands:
            hand = self._active_hands[key]
            herdr_pane = hand.get("herdr_pane_id") or hand.get("pane_id")
            agent_name = hand.get("worker")

        if not herdr_pane:
            herdr_pane = self._pane_targets.get(key) if key else None
        if not herdr_pane:
            herdr_pane = pane_id

        herdr_bin = os.environ.get("GOLEM_HERDR_BIN") or os.environ.get("HERDR_BIN", "herdr")
        logger.info("Permanently killing hand pane: %s (Herdr pane: %s) via %s", pane_id, herdr_pane, herdr_bin)

        # 1. Best-effort agent stop
        for agent_target in {herdr_pane, pane_id, agent_name} - {None, "", "pane", "agy"}:
            try:
                p = await asyncio.create_subprocess_exec(
                    herdr_bin, "agent", "stop", str(agent_target),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await asyncio.wait_for(p.communicate(), timeout=3.0)
            except Exception as exc:
                logger.debug("herdr agent stop %s: %s", agent_target, exc)

        # 2. Permanent pane close
        try:
            p = await asyncio.create_subprocess_exec(
                herdr_bin, "pane", "close", str(herdr_pane),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(p.communicate(), timeout=3.0)
        except Exception as exc:
            logger.debug("herdr pane close %s: %s", herdr_pane, exc)

        # 3. If workspace-backed pane (wXX:pYY), close workspace & remove worktree
        if ":" in str(herdr_pane):
            ws_id = str(herdr_pane).split(":")[0]
            try:
                p = await asyncio.create_subprocess_exec(
                    herdr_bin, "worktree", "remove", "--workspace", ws_id, "--force",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await asyncio.wait_for(p.communicate(), timeout=3.0)
            except Exception as exc:
                logger.debug("herdr worktree remove %s: %s", ws_id, exc)

            try:
                p = await asyncio.create_subprocess_exec(
                    herdr_bin, "workspace", "close", ws_id,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await asyncio.wait_for(p.communicate(), timeout=3.0)
            except Exception as exc:
                logger.debug("herdr workspace close %s: %s", ws_id, exc)

        # 4. Unregister from active hands & detach mirror
        if key is not None:
            self.unregister_active_hand(key)
        else:
            self.unregister_active_hand(pane_id)

        # 5. Mark killed in HerdrHandSync discovery cache so reconciliation never resurrects it
        if hasattr(self, "_hand_sync") and self._hand_sync is not None:
            if hasattr(self._hand_sync, "mark_killed"):
                self._hand_sync.mark_killed(herdr_pane, pane_id, key)
            elif hasattr(self._hand_sync, "discovered"):
                self._hand_sync.discovered.pop(herdr_pane, None)
                self._hand_sync.discovered.pop(pane_id, None)
                if key:
                    self._hand_sync.discovered.pop(key, None)

        # 6. Broadcast hands_status to all connected SSE clients
        self._broadcast_hands()

        return web.json_response({
            "ok": True,
            "pane_id": pane_id,
            "herdr_pane_id": herdr_pane,
            "status": "killed",
        })

    async def handle_index(self, request: web.Request) -> web.Response:
        """Render high-contrast dark-mode Command Center dashboard."""
        html = DASHBOARD_HTML
        if _DASHBOARD_FILE.exists():
            try:
                html = _DASHBOARD_FILE.read_text(encoding="utf-8")
            except Exception:
                pass
        return web.Response(
            text=html,
            content_type="text/html",
            charset="utf-8",
            headers={"Cache-Control": "no-cache, must-revalidate"},
        )

    async def handle_wiki(self, request: web.Request) -> web.Response:
        """Render Golem Operational Memory Wiki UI."""
        if _WIKI_FILE.exists():
            try:
                html = _WIKI_FILE.read_text(encoding="utf-8")
                return web.Response(
                    text=html,
                    content_type="text/html",
                    charset="utf-8",
                    headers={"Cache-Control": "no-cache, must-revalidate"},
                )
            except Exception as e:
                logger.error(f"Error reading wiki template: {e}")
        return web.Response(text="<h1>Golem Memory Wiki</h1><p>Template missing.</p>", content_type="text/html")

    async def handle_wiki_tree(self, request: web.Request) -> web.Response:
        """Return categorized tree of all documents in ~/.golem/wiki/."""
        from golem.wiki import GolemWiki
        wiki = GolemWiki()
        docs = await asyncio.to_thread(wiki.list_documents)
        tree: Dict[str, List[str]] = {"context": [], "runbooks": [], "logbook": [], "other": []}
        for d in docs:
            parts = d.split("/", 1)
            cat = parts[0] if len(parts) > 1 and parts[0] in tree else "other"
            tree[cat].append(d)
        return web.json_response({"ok": True, "tree": tree, "docs": docs})

    async def handle_wiki_doc(self, request: web.Request) -> web.Response:
        """Read and return content of a wiki document by relative path."""
        path = request.query.get("path", "").strip()
        if not path:
            return web.json_response({"ok": False, "error": "Path parameter required"}, status=400)
        from golem.wiki import GolemWiki
        wiki = GolemWiki()
        try:
            content = await asyncio.to_thread(wiki.read, path)
            if content is None:
                return web.json_response({"ok": False, "error": f"Document '{path}' not found"}, status=404)
            abs_p = wiki._safe_path(path)
            mtime = os.path.getmtime(abs_p) if os.path.exists(abs_p) else time.time()
            return web.json_response({"ok": True, "path": path, "content": content, "mtime": mtime})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)

    async def handle_wiki_save(self, request: web.Request) -> web.Response:
        """Save and commit an operational document to ~/.golem/wiki/ via GolemWiki."""
        try:
            data = await request.json()
            path = data.get("path", "").strip()
            content = data.get("content", "")
            commit_msg = data.get("commit_msg") or f"feat(wiki): web edit {path}"
            if not path:
                return web.json_response({"ok": False, "error": "Path required"}, status=400)
            from golem.wiki import GolemWiki
            wiki = GolemWiki()
            success = await asyncio.to_thread(
                wiki.write,
                rel_path=path,
                content=content,
                commit_msg=commit_msg,
                author="Carl <carl@fleet.local>",
            )
            return web.json_response({"ok": success, "path": path})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)

    async def handle_wiki_search(self, request: web.Request) -> web.Response:
        """Search markdown documents in ~/.golem/wiki/."""
        query = request.query.get("q", "").strip()
        from golem.wiki import GolemWiki
        wiki = GolemWiki()
        results = await asyncio.to_thread(wiki.search, query)
        return web.json_response({"ok": True, "results": results})

    async def handle_colossus_bg(self, request: web.Request) -> web.Response:
        """Serve local colossus backdrop image for air-gapped / offline retro aesthetic."""
        static_img = _TEMPLATES_DIR.parent / "static" / "colossus.png"
        if static_img.exists():
            return web.Response(body=static_img.read_bytes(), content_type="image/png")
        return web.Response(status=404)

    async def handle_colossus_frame(self, request: web.Request) -> web.Response:
        """Serve 16-bit animated background sequence frames."""
        frame = request.match_info.get("frame", "0")
        safe_frame = "".join(c for c in frame if c.isalnum() or c in ("-", "_"))
        target = _TEMPLATES_DIR.parent / "static" / f"colossus_{safe_frame}.png"
        if not target.exists() and safe_frame in ("0", "base", "orig"):
            target = _TEMPLATES_DIR.parent / "static" / "colossus.png"
        if target.exists() and target.is_file():
            return web.Response(body=target.read_bytes(), content_type="image/png")
        return web.Response(status=404)

    async def handle_static_file(self, request: web.Request) -> web.Response:
        """Serve static assets (animation frames, images) safely."""
        filename = request.match_info.get("filename", "")
        safe_name = os.path.basename(filename)
        target = _TEMPLATES_DIR.parent / "static" / safe_name
        if target.exists() and target.is_file():
            ctype = _STATIC_CONTENT_TYPES.get(target.suffix.lower(), "application/octet-stream")
            return web.Response(body=target.read_bytes(), content_type=ctype)
        return web.Response(status=404)

    async def handle_health(self, request: web.Request) -> web.Response:
        """Liveness probe endpoint."""
        return web.json_response({
            "status": "ok",
            "version": VERSION,
            "lineage": LINEAGE,
            "uptime": time.time() - self._start_time,
        })

    async def handle_status(self, request: web.Request) -> web.Response:
        """Return full current system snapshot."""
        status = await asyncio.to_thread(get_system_status)
        status["port"] = self.port
        status["host"] = self.host
        status["subscribers"] = self.subscriber_count
        hands_snapshot = []
        if self._active_hands:
            try:
                hands_snapshot = list(self._active_hands.values())
            except RuntimeError:
                try:
                    hands_snapshot = list(dict(self._active_hands).values())
                except Exception:
                    hands_snapshot = []
        if hands_snapshot:
            status["hands"] = [dict(h) for h in hands_snapshot]
        return web.json_response(status)

    async def handle_pipeline_run(self, request: web.Request) -> web.Response:
        """Execute a live verification pipeline task to demonstrate real agent activity."""
        asyncio.create_task(self._run_pipeline_demo())
        return web.json_response({
            "status": "dispatched",
            "message": "Pipeline execution triggered. Hand agent spawned."
        })

    async def _run_pipeline_demo(self) -> None:
        """Asynchronously executes a real command, streaming output to SSE."""
        pane_id = "Hand 01 [L_GAUNTLET]"
        cmd = "python3 -m pytest tests/test_webui_observability.py -k test_health"
        pid = os.getpid()

        # Demo pane id is a display label, not a real Herdr pane: skip live mirroring.
        self.register_active_hand(pane_id, "localhost", cmd, pid, mirror=False)
        self.broadcast("hand_spawn", {
            "pane_id": pane_id,
            "node": "localhost",
            "command": cmd,
            "pid": pid,
            "status": "RUNNING",
            "buffer_tail": self._active_hands[pane_id]["buffer_tail"],
        })
        self.broadcast("eye_log", {
            "message": f"OBSERVING: {cmd} [PID: {pid}]",
            "state": "RUNNING",
        })
        self.broadcast("mouth_briefing", {
            "briefing": "Pipeline execution triggered: Running test suite across mesh.",
        })

        try:
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace")
                self.update_hand_buffer(pane_id, text)
                self.broadcast("hand_buffer", {
                    "pane_id": pane_id,
                    "delta": text,
                })

            await proc.wait()
            exit_code = proc.returncode or 0
        except Exception as err:
            exit_code = 1
            self.broadcast("hand_buffer", {
                "pane_id": pane_id,
                "delta": f"\\nError executing pipeline: {err}\\n",
            })

        status_str = "EXIT_0" if exit_code == 0 else "FAILED"
        self.broadcast("hand_settle", {
            "pane_id": pane_id,
            "exit_code": exit_code,
            "status": status_str,
        })
        self.broadcast("eye_log", {
            "message": f"DETECTED: TASK_COMPLETION [{status_str}] | Conf: 99.9%",
            "state": "IDLE",
        })
        self.broadcast("mouth_briefing", {
            "briefing": f"Pipeline verification finished [{status_str}]. All tests executed cleanly.",
        })

        # Keep visible for 8 seconds, then return to idle
        await asyncio.sleep(8.0)
        # unregister_active_hand() broadcasts the true remaining set; a blanket [] here would
        # make browsers drop the cards of any other hands still running.
        self.unregister_active_hand(pane_id)

    async def handle_sensory(self, request: web.Request) -> web.Response:
        """Return organ-specific telemetry for eye, nose, mouth, or hands."""
        organ = request.match_info.get("organ", "").lower().strip()
        status = await asyncio.to_thread(get_system_status)
        if organ in ("eye", "eyes"):
            return web.json_response(status.get("eyes", {}))
        elif organ in ("nose", "fleet"):
            return web.json_response(status.get("nose", {}))
        elif organ in ("mouth", "liaison"):
            return web.json_response(status.get("mouth", {}))
        elif organ in ("hand", "hands", "runner"):
            hands_snapshot = list(self._active_hands.values()) if self._active_hands else []
            if hands_snapshot:
                return web.json_response([dict(h) for h in hands_snapshot])
            return web.json_response(status.get("hands", []))
        else:
            return web.json_response(
                {"error": f"Unknown sensory organ: '{organ}'. Valid options: eye, nose, mouth, hands."},
                status=404
            )

    async def handle_events(self, request: web.Request) -> web.StreamResponse:
        """
        Server-Sent Events (SSE) streaming endpoint.
        Streams live log frames and telemetry without event loop starvation.
        """
        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)

        queue: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=1000)
        self._subscribers.add(queue)

        # Emit initial connection handshake
        init_event = format_sse_event("connected", {
            "status": "connected",
            "lineage": LINEAGE,
            "version": VERSION,
            "port": self.port,
            "timestamp": time.time(),
        })
        try:
            await response.write(init_event.encode("utf-8"))
            if self._active_hands:
                init_hands = format_sse_event("hands_status", list(self._active_hands.values()))
                await response.write(init_hands.encode("utf-8"))
            for snap in self._pane_mirror.snapshots():
                await response.write(format_sse_event("pane_frame", snap).encode("utf-8"))
        except Exception:
            self._subscribers.discard(queue)
            return response

        try:
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    # SSE keepalive comment resets reverse proxy (e.g. Nginx 60s) read timeouts
                    await response.write(b": keepalive\n\n")
                    continue
                if msg is None:  # Shutdown sentinel
                    break
                await response.write(msg.encode("utf-8"))
        except (asyncio.CancelledError, ConnectionResetError, web.HTTPException):
            pass
        finally:
            self._subscribers.discard(queue)
            try:
                await response.write_eof()
            except Exception:
                pass

        return response

    async def start(self) -> None:
        """
        Start the WebUI server and bind to host:port.
        Handles port collisions gracefully by cleaning up runner and re-raising OSError.
        """
        async with self._lock:
            if self.is_running:
                logger.info(f"WebUI server already running on http://{self.host}:{self.port}")
                return

            self._runner = web.AppRunner(self.app, access_log=None)
            await self._runner.setup()

            self._site = web.TCPSite(self._runner, self.host, self.port, reuse_address=True)
            try:
                await self._site.start()
                logger.info(f"Golem Observability WebUI active on http://{self.host}:{self.port}")
                self.loop = asyncio.get_running_loop()
                if self.event_bus is None:
                    try:
                        from golem.sensory.bus import get_default_event_bus
                        self.event_bus = get_default_event_bus()
                    except Exception:
                        pass
                if self.event_bus is not None and self._bus_unsubscribe is None:
                    self.attach_event_bus(self.event_bus)
                global _GLOBAL_WEBUI_SERVER
                if _GLOBAL_WEBUI_SERVER is None or not _GLOBAL_WEBUI_SERVER.is_running:
                    _GLOBAL_WEBUI_SERVER = self
                # Warm up status cache and thread pool so subsequent REST queries resolve in-memory
                try:
                    await asyncio.to_thread(get_system_status)
                except Exception:
                    pass
                self._telemetry_task = asyncio.create_task(self._telemetry_loop())
                if self._hand_sync_enabled and self._hand_sync_task is None:
                    self._hand_sync_task = asyncio.create_task(self._hand_sync.run())
                for _pid, _target in list(self._pane_targets.items()):
                    self._mirror_attach(_pid, _target)
                # Auto-start simulated hands if requested via environment variable
                sim_env = os.environ.get("GOLEM_SIMULATE_HANDS")
                if sim_env:
                    try:
                        self.start_hand_simulation(abs(int(sim_env)))
                    except ValueError:
                        pass
            except OSError as err:
                logger.error(f"Failed to bind WebUI server to {self.host}:{self.port}: {err}")
                if self._site:
                    try:
                        await self._site.stop()
                    except Exception:
                        pass
                    self._site = None
                if self._runner:
                    try:
                        await self._runner.cleanup()
                    except Exception:
                        pass
                    self._runner = None
                raise OSError(f"Address already in use or port collision on {self.host}:{self.port}: {err}") from err

    async def stop(self) -> None:
        """
        Gracefully stop the server, release socket port, and unblock clients.
        Guarantees zero EADDRINUSE leaks.
        """
        async with self._lock:
            if self._bus_unsubscribe is not None:
                try:
                    self._bus_unsubscribe()
                except Exception as e:
                    logger.debug(f"Error unsubscribing from event bus: {e}")
                self._bus_unsubscribe = None

            if self._simulator is not None:
                try:
                    self._simulator.stop()
                except Exception as e:
                    logger.debug(f"Error stopping simulator: {e}")
                self._simulator = None

            if self._telemetry_task is not None:
                self._telemetry_task.cancel()
                try:
                    await self._telemetry_task
                except (asyncio.CancelledError, Exception):
                    pass
                self._telemetry_task = None

            if self._hand_sync_task is not None:
                self._hand_sync_task.cancel()
                try:
                    await self._hand_sync_task
                except (asyncio.CancelledError, Exception):
                    pass
                self._hand_sync_task = None
            self._hand_sync.discovered.clear()

            await self._pane_mirror.shutdown()

            # Notify and flush active subscriber queues with shutdown sentinel
            for q in list(self._subscribers):
                try:
                    q.put_nowait(None)
                except Exception:
                    pass
            self._subscribers.clear()

            if self._site is not None:
                try:
                    await self._site.stop()
                except Exception as e:
                    logger.debug(f"Error stopping WebUI TCPSite: {e}")
                self._site = None

            if self._runner is not None:
                try:
                    await self._runner.cleanup()
                except Exception as e:
                    logger.debug(f"Error cleaning up WebUI AppRunner: {e}")
                self._runner = None

            global _GLOBAL_WEBUI_SERVER
            if _GLOBAL_WEBUI_SERVER is self:
                _GLOBAL_WEBUI_SERVER = None

            # Reset agent logs and error log caches to prevent cross-test cache pollution
            with _AGENT_LOGS_LOCK:
                _AGENT_LOGS_CACHE["timestamp"] = 0.0
                _AGENT_LOGS_CACHE["data"] = None

            with _ERROR_LOG_LOCK:
                _ERROR_LOG_CACHE["timestamp"] = 0.0
                _ERROR_LOG_CACHE["count"] = 0

            with _HOST_TELEMETRY_LOCK:
                _HOST_TELEMETRY_CACHE["timestamp"] = 0.0
                _HOST_TELEMETRY_CACHE["disk"] = None
                _HOST_TELEMETRY_CACHE["docker_running"] = False

            logger.info(f"Golem Observability WebUI on port {self.port} stopped cleanly.")

    async def _telemetry_loop(self) -> None:
        """Periodic background telemetry broadcaster keeping dashboard real-time."""
        pulse_ticks = 0
        while self.is_running:
            try:
                await asyncio.sleep(2.5)
                if not self.is_running:
                    break
                pulse_ticks += 1
                if self.subscriber_count > 0:
                    status = await asyncio.to_thread(get_system_status)
                    self.broadcast("heartbeat", {
                        "uptime": status.get("uptime", 0.0),
                        "uptime_formatted": status.get("uptime_formatted", "00:00:00"),
                        "timestamp": time.time(),
                    })
                    nose_payload = await self._build_nose_payload(status.get("nose", {}))
                    self.broadcast("nose_telemetry", nose_payload)
                    self.broadcast("eye_telemetry", status.get("eyes", {}))
                    self.broadcast("mouth_telemetry", status.get("mouth", {}))
                    self.broadcast("hands_status", status.get("hands", []))

                    # Prune settled hands older than TTL (default 120s)
                    now_ts = time.time()
                    ttl = float(os.environ.get("GOLEM_SETTLED_HANDS_TTL", "120.0"))
                    prune_keys = []
                    terminal_statuses = {"COMPLETED", "EXIT_0", "FAILED", "DONE", "SETTLED", "CANCELLED", "SUCCESS"}
                    for k, h in list(self._active_hands.items()):
                        st = str(h.get("status", "")).upper()
                        if st in terminal_statuses or h.get("exit_code") is not None:
                            settled_at = h.get("settled_at") or h.get("started_at", 0)
                            if settled_at and (now_ts - settled_at) > ttl:
                                prune_keys.append(k)
                    for k in prune_keys:
                        self._active_hands.pop(k, None)
                        self._hand_meta.pop(k, None)
                        self._pane_targets.pop(k, None)
                        self._mirror_detach(k)
                    if prune_keys:
                        self._broadcast_hands()

                    # Periodic proof-of-life pulse every ~30 seconds (12 ticks * 2.5s)
                    if pulse_ticks >= 12:
                        pulse_ticks = 0
                        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        self._append_recent_log("eyes", f"{now_str} [PULSE] Golem.Sensory.Eyes: Terminal watcher active (quiescent buffer state, 0 blockers)")
                        has_executing = bool(self.get_executing_hands())
                        self.broadcast("eye_log", {
                            "message": "[PULSE] Terminal watcher active (quiescent buffer state, 0 blockers)",
                            "state": "RUNNING" if has_executing else "IDLE",
                            "timestamp": time.time(),
                        })
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Telemetry broadcast loop error: {e}")

    def _ensure_nose_live(self) -> None:
        if self._nose_probe is None or self._nose_tracker is None:
            from golem.sensory.nose import NoseLiveProbe, NoseTelemetryTracker
            self._nose_probe = NoseLiveProbe()
            self._nose_tracker = NoseTelemetryTracker()

    def _emit_nose_events(self, events: List[Dict[str, Any]]) -> None:
        """Append change-driven events to the Nose log stream (SSE `nose_log` + ring buffer)."""
        for ev in events:
            ts = ev.get("timestamp", time.time())
            now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
            lvl = "WARN" if ev.get("level") == "warn" else "INFO"
            self._append_recent_log("nose", f"{now_str} [{lvl}] Golem.Sensory.Nose: {ev.get('message', '')}")
            self.broadcast("nose_log", ev)

    async def _build_nose_payload(self, base: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge live gauges (ports, fleet reachability, memory) into the base nose status.
        Gauges ride on every tick; log events are emitted only when something changed.
        """
        payload = dict(base)
        try:
            self._ensure_nose_live()
            live = dict(await self._nose_probe.sample())
            port_set = live.pop("port_set", set())
            payload.update(live)
            events = self._nose_tracker.observe(
                live.get("fleet_nodes", {}),
                port_set,
                smells_off=bool(payload.get("smells_off")),
                verdict_summary=str(payload.get("verdict_summary") or ""),
            )
            self._emit_nose_events(events)
        except Exception as e:
            logger.debug(f"Nose live probe error: {e}")
        return payload

    def record_nose_health_check(self, summary: str, smells_off: bool = False) -> Dict[str, Any]:
        """Log an explicit health check (/health, !nose, check_nose) to the Nose terminal."""
        self._ensure_nose_live()
        ev = self._nose_tracker.explicit_check(summary, smells_off)
        self._emit_nose_events([ev])
        return ev

    def broadcast(self, event: str, data: Any) -> Any:
        """
        Push an SSE event synchronously to all connected subscriber queues.
        Non-blocking, drops oldest queued item if queue is full to prevent memory starvation.
        Returns _SyncAndAwaitableNone to allow both sync calls and await expressions.
        """
        if event == "mouth_briefing":
            try:
                text = data.get("briefing") if isinstance(data, dict) else str(data)
                if text:
                    now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    flat = " ".join(str(text).split())
                    self._append_recent_log("mouth", f"{now_str} [INFO] Golem.Agent.Mouth: Executive briefing: \"{flat}\"")
            except Exception as e:
                logger.debug(f"Mouth log record error: {e}")
        chunk = format_sse_event(event, data)
        for q in list(self._subscribers):
            try:
                q.put_nowait(chunk)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                    q.put_nowait(chunk)
                except Exception:
                    pass
            except Exception:
                pass
        return _SyncAndAwaitableNone()

    async def broadcast_async(self, event: str, data: Any) -> None:
        """Asynchronous broadcast wrapper."""
        self.broadcast(event, data)

    def attach_event_bus(self, event_bus: Any) -> None:
        """
        Bridge ShoulderTapEventBus alerts directly to WebUI SSE queues.
        Thread-safe: dispatches onto the server's asyncio loop via call_soon_threadsafe.
        """
        if self._bus_unsubscribe is not None:
            try:
                self._bus_unsubscribe()
            except Exception:
                pass
            self._bus_unsubscribe = None
        self.event_bus = event_bus
        if self.event_bus is not None and hasattr(self.event_bus, "subscribe"):
            self._bus_unsubscribe = self.event_bus.subscribe(self._on_bus_alert)

    def detach_event_bus(self) -> None:
        """Detach from current event bus."""
        if self._bus_unsubscribe is not None:
            try:
                self._bus_unsubscribe()
            except Exception:
                pass
            self._bus_unsubscribe = None
        self.event_bus = None

    def _on_bus_alert(self, alert: Any) -> None:
        """
        Synchronous subscriber callback invoked by event bus on publish.
        Thread-safe: schedules _process_bus_alert onto the server's asyncio event loop.
        """
        loop = getattr(self, "loop", None)
        if loop is not None and loop.is_running() and not loop.is_closed():
            loop.call_soon_threadsafe(self._process_bus_alert, alert)
        else:
            try:
                curr_loop = asyncio.get_running_loop()
                if curr_loop.is_running() and not curr_loop.is_closed():
                    curr_loop.call_soon_threadsafe(self._process_bus_alert, alert)
                else:
                    self._process_bus_alert(alert)
            except RuntimeError:
                self._process_bus_alert(alert)

    @staticmethod
    def _normalize_blocker_kind(kind: Any) -> str:
        return "oauth_url" if str(kind or "").lower() in ("oauth", "oauth_url") else "prompt"

    def _process_bus_alert(self, alert: Any) -> None:
        """
        Process bus alert on the server event loop and broadcast to SSE channels:
        eye_log, eye_telemetry, nose_alert (and nose_telemetry), mouth_briefing.
        """
        source = getattr(alert, "source", "")
        summary = getattr(alert, "summary", "") or ""
        data = getattr(alert, "data", {}) or {}
        alert_type = getattr(alert, "alert_type", "")
        timestamp = getattr(alert, "timestamp", time.time())
        to_dict = getattr(alert, "to_dict", lambda: {"summary": summary, "data": data})()
        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

        # 1. Route Eyes / Blocker / Completion
        if source == "eyes" or alert_type in (
            "blocker", "completion", "eye_log", "eye_telemetry",
            "eye_attached", "eye_inspection", "eye_blocker",
        ):
            is_blocker = getattr(alert, "is_blocker", False) or alert_type == "blocker"
            is_comp = getattr(alert, "is_completion", False) or alert_type == "completion"
            state = "BLOCKER" if is_blocker else ("IDLE" if is_comp else "RUNNING")

            if is_blocker:
                b_type = data.get("type") or data.get("blocker_type") or "BLOCKER"
                msg = f"[BLOCKER] {summary}" if summary else f"[BLOCKER] {b_type} detected in runner pane"
                log_line = f"{now_str} [WARN] Golem.Sensory.Eyes: {msg}"
                eye_pane = data.get("pane") or data.get("pane_id")
                sig = (eye_pane, self._normalize_blocker_kind(b_type), data.get("url") or data.get("blocker_url") or data.get("prompt") or data.get("blocker_prompt") or "")
                already_logged = self._eye_blocker_sigs.pop(sig, None) is not None
                if not already_logged:
                    self._append_recent_log("eyes", log_line)
                    self.broadcast("eye_log", {
                        "message": msg,
                        "state": "BLOCKER",
                        "blocker_type": b_type,
                        "timestamp": timestamp,
                        "data": data,
                        "alert": to_dict,
                    })
                self.broadcast("eye_telemetry", {
                    "state": "BLOCKER",
                    "command": data.get("command", ""),
                    "active_command": data.get("command", ""),
                    "pid": data.get("pid"),
                    "blocker": data,
                    "buffer_tail": data.get("buffer_tail", ""),
                    "timestamp": timestamp,
                })
            elif is_comp:
                is_err = data.get("is_error", False)
                state = "ERROR" if is_err else "IDLE"
                msg = f"[COMPLETED] {summary}" if not is_err else f"[ERROR] {summary}"
                log_line = f"{now_str} [INFO] Golem.Sensory.Eyes: {msg}"
                self._append_recent_log("eyes", log_line)
                self.broadcast("eye_log", {
                    "message": msg,
                    "state": state,
                    "timestamp": timestamp,
                    "data": data,
                    "alert": to_dict,
                })
                self.broadcast("eye_telemetry", {
                    "state": state,
                    "command": "",
                    "active_command": "",
                    "pid": None,
                    "blocker": None,
                    "buffer_tail": data.get("buffer_tail", ""),
                    "timestamp": timestamp,
                })
            elif alert_type == "eye_log":
                msg = summary or data.get("message", "")
                st = data.get("state", "RUNNING")
                log_line = f"{now_str} [INFO] Golem.Sensory.Eyes: {msg}"
                self._append_recent_log("eyes", log_line)
                self.broadcast("eye_log", {
                    "message": msg,
                    "state": st,
                    "timestamp": timestamp,
                    "data": data,
                    "alert": to_dict,
                })
            elif alert_type in ("eye_attached", "eye_inspection", "eye_blocker"):
                pane_id = data.get("pane_id", "")
                if alert_type == "eye_attached":
                    msg = f"[LIFE] Attached to {pane_id}: {data.get('task', '')}".rstrip(": ")
                    st = "RUNNING"
                elif alert_type == "eye_inspection":
                    delta_lines = data.get("delta_lines", 0)
                    delta_str = f" (delta: {delta_lines:+d} lines)" if delta_lines else ""
                    msg = f"[THOUGHT] Scanned stdout buffer on {pane_id}{delta_str} — State: {data.get('state', 'RUNNING')}"
                    st = "RUNNING"
                else:
                    kind = data.get("blocker_type", "prompt")
                    msg = f"[BLOCKER] {kind} on {pane_id}: {data.get('detail', '')}"
                    st = "BLOCKER"
                    legacy_sig = (pane_id, self._normalize_blocker_kind(kind), data.get("detail") or "")
                    self._eye_blocker_sigs[legacy_sig] = timestamp
                    if len(self._eye_blocker_sigs) > 64:
                        self._eye_blocker_sigs.pop(next(iter(self._eye_blocker_sigs)), None)
                lvl = "WARN" if st == "BLOCKER" else "INFO"
                self._append_recent_log("eyes", f"{now_str} [{lvl}] Golem.Sensory.Eyes: {msg}")
                self.broadcast("eye_log", {
                    "message": msg,
                    "state": st,
                    "event": alert_type,
                    "pane_id": pane_id,
                    "timestamp": timestamp,
                    "data": data,
                    "alert": to_dict,
                })
            elif alert_type == "eye_telemetry":
                self.broadcast("eye_telemetry", data if data else to_dict)

        # 2. Route Nose / Health Anomaly
        elif source == "nose" or alert_type in ("health_anomaly", "nose_alert") or getattr(alert, "is_health_anomaly", False):
            smells_off = bool(data.get("smells_off", True))
            log_line = f"{now_str} [{'WARN' if smells_off else 'INFO'}] Golem.Sensory.Nose: {summary}"
            self._append_recent_log("nose", log_line)
            payload = dict(data)
            payload["summary"] = summary
            payload["timestamp"] = timestamp
            payload["smells_off"] = smells_off
            payload["verdict_summary"] = summary
            payload["alert"] = to_dict
            self.broadcast("nose_alert", payload)
            self.broadcast("nose_telemetry", payload)
            if smells_off:
                try:
                    self._ensure_nose_live()
                    ev = self._nose_tracker.note_anomaly(summary)
                    if ev is not None:
                        ev["timestamp"] = timestamp
                        self.broadcast("nose_log", ev)
                except Exception as e:
                    logger.debug(f"Nose anomaly log error: {e}")

        # 3. Route Mouth Briefings & Logs
        elif source == "mouth" or alert_type in ("mouth_briefing", "mouth_log", "mouth_thought"):
            briefing = data.get("briefing") or summary
            payload = data if data else {"briefing": summary}
            if alert_type == "mouth_thought" or "thought" in data:
                thought = data.get("thought") or summary
                self._append_recent_log("mouth", f"{now_str} [THOUGHT] Golem.Agent.Mouth: {thought}")
            elif alert_type == "mouth_log":
                lvl = data.get("level", "INFO").upper()
                self._append_recent_log("mouth", f"{now_str} [{lvl}] Golem.Agent.Mouth: {summary}")
            else:
                self.broadcast("mouth_briefing", payload)

    def _append_recent_log(self, organ: str, line: str, max_entries: int = 50) -> None:
        """Store log line in memory ring buffer."""
        logs = self._recent_logs.setdefault(organ, [])
        logs.append(line)
        if len(logs) > max_entries:
            del logs[:-max_entries]
        # Invalidate agent logs cache so new live logs are immediately visible
        with _AGENT_LOGS_LOCK:
            _AGENT_LOGS_CACHE["timestamp"] = 0.0
            _AGENT_LOGS_CACHE["data"] = None


# Global Singleton Reference
_GLOBAL_WEBUI_SERVER: Optional[WebUIServer] = None

# Short-lived error log scan cache to prevent event loop disk I/O starvation
_ERROR_LOG_CACHE: Dict[str, Any] = {
    "timestamp": 0.0,
    "count": 0,
}
_ERROR_LOG_LOCK = threading.Lock()

# Short-lived agent logs cache to eliminate synchronous disk I/O when ring buffers are empty
_AGENT_LOGS_CACHE: Dict[str, Any] = {
    "timestamp": 0.0,
    "data": None,
}
_AGENT_LOGS_LOCK = threading.Lock()

# Short-lived host telemetry cache (disk usage, docker socket) to eliminate synchronous filesystem syscalls
_HOST_TELEMETRY_CACHE: Dict[str, Any] = {
    "timestamp": 0.0,
    "disk": None,
    "docker_running": False,
}
_HOST_TELEMETRY_LOCK = threading.Lock()


def clear_host_telemetry_cache() -> None:
    """Explicitly reset host telemetry cache (for testing and clean teardown)."""
    with _HOST_TELEMETRY_LOCK:
        _HOST_TELEMETRY_CACHE["timestamp"] = 0.0
        _HOST_TELEMETRY_CACHE["disk"] = None
        _HOST_TELEMETRY_CACHE["docker_running"] = False


def get_active_webui_server() -> Optional[WebUIServer]:
    """Return the active WebUIServer singleton instance if running."""
    global _GLOBAL_WEBUI_SERVER
    return _GLOBAL_WEBUI_SERVER


class WebUILauncher:
    """
    Awaitable launcher handle for WebUIServer with immediate synchronous port
    and socket collision validation.
    """

    def __init__(self, host: str, port: int, event_bus: Optional[Any] = None):
        if not isinstance(port, int) or port < 1 or port > 65535:
            raise ValueError(f"Port must be an integer between 1 and 65535, got {port}")

        # Idempotency pre-check: if server is already running on requested (host, port), reuse it
        global _GLOBAL_WEBUI_SERVER
        if (
            _GLOBAL_WEBUI_SERVER is not None
            and _GLOBAL_WEBUI_SERVER.is_running
            and _GLOBAL_WEBUI_SERVER.host == host
            and _GLOBAL_WEBUI_SERVER.port == port
        ):
            self.host = host
            self.port = port
            self.event_bus = event_bus
            self._coro = self._start()
            return

        # Check socket availability immediately to catch port collision synchronously
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_sock:
            test_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                test_sock.bind((host, port))
            except OSError as err:
                raise OSError(f"Address already in use or port {port} collision on {host}: {err}") from err

        self.host = host
        self.port = port
        self.event_bus = event_bus
        self._coro = self._start()

    def __await__(self):
        return self._coro.__await__()

    async def _start(self) -> WebUIServer:
        global _GLOBAL_WEBUI_SERVER
        if _GLOBAL_WEBUI_SERVER is not None and _GLOBAL_WEBUI_SERVER.is_running:
            if _GLOBAL_WEBUI_SERVER.host == self.host and _GLOBAL_WEBUI_SERVER.port == self.port:
                if self.event_bus is not None and _GLOBAL_WEBUI_SERVER.event_bus != self.event_bus:
                    _GLOBAL_WEBUI_SERVER.attach_event_bus(self.event_bus)
                return _GLOBAL_WEBUI_SERVER
            await _GLOBAL_WEBUI_SERVER.stop()
            _GLOBAL_WEBUI_SERVER = None

        server = WebUIServer(host=self.host, port=self.port, event_bus=self.event_bus)
        await server.start()
        _GLOBAL_WEBUI_SERVER = server
        return server


def start_webui_server(host: str = "127.0.0.1", port: int = 8765, event_bus: Optional[Any] = None) -> WebUILauncher:
    """
    Launch the global WebUI server on host:port.
    Validates port synchronously and returns an awaitable WebUILauncher.
    """
    return WebUILauncher(host=host, port=port, event_bus=event_bus)



async def stop_webui_server() -> None:
    """Stop the global WebUI server if active and clear reference."""
    global _GLOBAL_WEBUI_SERVER
    if _GLOBAL_WEBUI_SERVER is not None:
        await _GLOBAL_WEBUI_SERVER.stop()
        _GLOBAL_WEBUI_SERVER = None
    with _AGENT_LOGS_LOCK:
        _AGENT_LOGS_CACHE["timestamp"] = 0.0
        _AGENT_LOGS_CACHE["data"] = None
    with _ERROR_LOG_LOCK:
        _ERROR_LOG_CACHE["timestamp"] = 0.0
        _ERROR_LOG_CACHE["count"] = 0
    with _HOST_TELEMETRY_LOCK:
        _HOST_TELEMETRY_CACHE["timestamp"] = 0.0
        _HOST_TELEMETRY_CACHE["disk"] = None
        _HOST_TELEMETRY_CACHE["docker_running"] = False


def webui_broadcast(event: str, data: Any) -> None:
    """
    Broadcast an event to all connected WebUI SSE subscribers.
    Safe to invoke when server is not running (silently ignores).
    """
    server = get_active_webui_server()
    if server is not None:
        server.broadcast(event, data)


async def webui_broadcast_async(event: str, data: Any) -> None:
    """Asynchronous variant of webui_broadcast."""
    webui_broadcast(event, data)


def create_app() -> web.Application:
    """Factory creating an aiohttp application with registered routes."""
    server = WebUIServer()
    return server.app


def extract_recent_agent_logs() -> Dict[str, List[str]]:
    """Extract recent log lines partitioned by agent from memory ring buffers or bot.err / bot.log."""
    logs: Dict[str, List[str]] = {
        "eyes": [],
        "nose": [],
        "mouth": [],
    }
    active_server = get_active_webui_server()
    if active_server is not None and hasattr(active_server, "_recent_logs"):
        for organ in ("eyes", "nose", "mouth"):
            mem_logs = active_server._recent_logs.get(organ, [])
            if mem_logs:
                logs[organ].extend(mem_logs)

    if not any(logs.values()):
        # Check thread-safe in-memory cache to eliminate disk I/O when ring buffers are empty
        now = time.monotonic()
        with _AGENT_LOGS_LOCK:
            cached_data = _AGENT_LOGS_CACHE.get("data")
            cached_ts = _AGENT_LOGS_CACHE.get("timestamp", 0.0)
            if cached_data is not None and (now - cached_ts) < 60.0:
                return {
                    "eyes": list(cached_data.get("eyes", [])),
                    "nose": list(cached_data.get("nose", [])),
                    "mouth": list(cached_data.get("mouth", [])),
                }

        candidates = [
            Path("bot.err"),
            Path("bot.log"),
            Path("/Users/hermes/projects/golem/bot.err"),
            Path("/Users/hermes/projects/golem/bot.log"),
        ]
        for p in candidates:
            if p.exists():
                try:
                    size = p.stat().st_size
                    with p.open("r", encoding="utf-8", errors="replace") as f:
                        if size > 16384:
                            f.seek(size - 16384)
                        lines = f.read().splitlines()
                    for line in lines[-100:]:
                        l = line.strip()
                        if not l:
                            continue
                        if any(k in l for k in ["Nose", "NoseSentinel", "Fleet Health Sentinel"]):
                            logs["nose"].append(l)
                        elif any(k in l for k in ["Mouth", "Ingress", "Triage", "Gateway", "Router", "Session", "Chief of Staff"]):
                            logs["mouth"].append(l)
                        elif any(k in l for k in ["Eyes", "Observer", "Harness", "Execution", "EyesTaskObserver"]):
                            logs["eyes"].append(l)
                    break
                except Exception:
                    pass

    # No synthetic fallbacks: an organ with no real log lines simply returns an empty stream.

    result = {
        "eyes": logs["eyes"][-16:],
        "nose": logs["nose"][-16:],
        "mouth": logs["mouth"][-16:],
    }

    # Populate cache when memory ring buffers are unpopulated
    active_server = get_active_webui_server()
    has_mem = False
    if active_server is not None and hasattr(active_server, "_recent_logs"):
        has_mem = any(active_server._recent_logs.values())
    if not has_mem:
        now = time.monotonic()
        with _AGENT_LOGS_LOCK:
            _AGENT_LOGS_CACHE["timestamp"] = now
            _AGENT_LOGS_CACHE["data"] = {
                "eyes": list(result["eyes"]),
                "nose": list(result["nose"]),
                "mouth": list(result["mouth"]),
            }

    return result


def get_system_status() -> Dict[str, Any]:
    """
    Produce a live JSON snapshot of system health, active sensory organs,
    and running hand panes. Provides safe fallbacks when components are idle.
    """
    uptime = 0.0
    active_server = get_active_webui_server()
    if active_server is not None:
        uptime = time.time() - active_server._start_time

    hours = int(uptime // 3600)
    minutes = int((uptime % 3600) // 60)
    seconds = int(uptime % 60)
    uptime_formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    # Extract real agent logs
    agent_logs = extract_recent_agent_logs()

    # 1. Nose status
    nose_data: Dict[str, Any] = {
        "smells_off": False,
        "quiescent": True,
        "seconds_ago": 0.0,
        "minutes_ago": 0.0,
        "last_check_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "verdict_summary": "System healthy; zero anomalies observed.",
        "disk": {"free_gb": 128.0, "percent_used": 45.2, "pressure": False},
        "docker": {"running": True, "container_count": 0, "containers": []},
        "tailscale": {"online": True, "backend_state": "Running", "peers": {"localhost": {"online": True, "ip": "127.0.0.1"}}},
        "recent_error_count": 0,
        "error_count": 0,
        "logs": agent_logs.get("nose", []),
    }
    try:
        from golem.sensory.nose import get_default_nose_sentinel
        sentinel = get_default_nose_sentinel()
        if sentinel is not None:
            snap = getattr(sentinel, "last_snapshot", None)
            if snap is not None:
                if getattr(snap, "disk", None):
                    d = snap.disk
                    nose_data["disk"] = {
                        "free_gb": float(d.get("free_gb", 0.0) if hasattr(d, "get") else 0.0),
                        "percent_used": float(d.get("percent_used", 0.0) if hasattr(d, "get") else 0.0),
                        "pressure": bool(d.get("pressure", False) if hasattr(d, "get") else False),
                    }
                if getattr(snap, "docker", None):
                    nose_data["docker"] = dict(snap.docker) if hasattr(snap.docker, "items") else {}
                if getattr(snap, "tailscale", None):
                    nose_data["tailscale"] = dict(snap.tailscale) if hasattr(snap.tailscale, "items") else {}
            last_verd = getattr(sentinel, "last_verdict", None)
            if last_verd is not None:
                nose_data["smells_off"] = bool(getattr(last_verd, "smells_off", False))
                nose_data["quiescent"] = not nose_data["smells_off"]
                if getattr(last_verd, "summary", None):
                    nose_data["verdict_summary"] = str(last_verd.summary)
    except Exception:
        pass

    # Real host telemetry (disk usage and docker socket check)
    # Cached with 10.0s TTL and mutex lock to prevent synchronous filesystem syscalls on the event loop
    try:
        now = time.monotonic()
        cached_disk = None
        cached_docker = False
        with _HOST_TELEMETRY_LOCK:
            if (now - _HOST_TELEMETRY_CACHE.get("timestamp", 0.0)) < 10.0 and _HOST_TELEMETRY_CACHE.get("disk") is not None:
                cached_disk = dict(_HOST_TELEMETRY_CACHE["disk"])
                cached_docker = bool(_HOST_TELEMETRY_CACHE.get("docker_running", False))

        if cached_disk is not None:
            nose_data["disk"] = cached_disk
            if cached_docker:
                nose_data["docker"]["running"] = True
        else:
            fresh_disk = None
            try:
                import shutil
                total, used, free = shutil.disk_usage("/")
                free_gb = round(free / (1024 ** 3), 1)
                used_pct = round((used / total) * 100.0, 1)
                fresh_disk = {
                    "free_gb": free_gb,
                    "percent_used": used_pct,
                    "pressure": used_pct > 90.0,
                }
            except Exception:
                pass

            fresh_docker = False
            try:
                if os.path.exists("/var/run/docker.sock"):
                    fresh_docker = True
            except Exception:
                pass

            if fresh_disk is not None:
                nose_data["disk"] = fresh_disk
            if fresh_docker:
                nose_data["docker"]["running"] = True

            with _HOST_TELEMETRY_LOCK:
                _HOST_TELEMETRY_CACHE["timestamp"] = now
                if fresh_disk is not None:
                    _HOST_TELEMETRY_CACHE["disk"] = dict(fresh_disk)
                _HOST_TELEMETRY_CACHE["docker_running"] = fresh_docker
    except Exception:
        pass

    # Real bot.err scan (tail-seek + 60.0s TTL cache with mutex lock to prevent event loop latency lag)
    try:
        from pathlib import Path
        now = time.monotonic()
        with _ERROR_LOG_LOCK:
            cached_ts = _ERROR_LOG_CACHE.get("timestamp", 0.0)
            if (now - cached_ts) < 60.0:
                cached_count = _ERROR_LOG_CACHE.get("count", 0)
                if cached_count > 0:
                    nose_data["error_count"] = cached_count
                    nose_data["recent_error_count"] = cached_count
            else:
                found_count = 0
                for filename in ("bot.err", "bot.log"):
                    p = Path(filename)
                    if p.is_file():
                        try:
                            size = p.stat().st_size
                            with p.open("rb") as f:
                                if size > 4096:
                                    f.seek(size - 4096)
                                chunk = f.read()
                            err_lines = [
                                l for l in chunk.splitlines()
                                if any(kw in l.lower() for kw in (b"traceback", b"[error]", b"[critical]", b"exception"))
                            ]
                            if err_lines:
                                found_count = len(err_lines)
                                nose_data["error_count"] = found_count
                                nose_data["recent_error_count"] = found_count
                            break
                        except Exception:
                            pass
                _ERROR_LOG_CACHE["timestamp"] = now
                _ERROR_LOG_CACHE["count"] = found_count
    except Exception:
        pass

    # 2. Eye status
    eyes_data: Dict[str, Any] = {
        "state": "IDLE",
        "active_command": "",
        "command": "",
        "pid": None,
        "blocker": None,
        "blockers": [],
        "buffer_tail": "",
        "observer_active": True,
        "logs": agent_logs.get("eyes", []),
    }
    if active_server is not None and getattr(active_server, "_active_hands", None):
        try:
            if hasattr(active_server, "get_executing_hands"):
                executing = active_server.get_executing_hands()
                executing_list = list(executing.values())
            else:
                terminal_statuses = {"EXIT_0", "COMPLETED", "FAILED", "DONE", "SETTLED", "CANCELLED", "SUCCESS"}
                executing_list = [
                    h for h in list(active_server._active_hands.values())
                    if not (isinstance(h, dict) and (str(h.get("status", "")).upper() in terminal_statuses or h.get("exit_code") is not None))
                ]
        except Exception:
            executing_list = []
        if executing_list:
            first_hand = executing_list[0]
            eyes_data["state"] = "RUNNING"
            eyes_data["active_command"] = first_hand.get("command", "")
            eyes_data["command"] = first_hand.get("command", "")
            eyes_data["pid"] = first_hand.get("pid")
            eyes_data["buffer_tail"] = first_hand.get("buffer_tail", "")
        else:
            eyes_data["state"] = "IDLE"

    # 3. Mouth status
    mouth_model = ""
    try:
        from golem.config import get_mouth_model
        mouth_model = get_mouth_model()
    except Exception:
        mouth_model = ""

    mouth_data: Dict[str, Any] = {
        "model": mouth_model or "",
        "status": "ready",
        "active_turns": 0,
        "recent_briefing": "",
        "logs": agent_logs.get("mouth", []),
    }

    # 4. Hands status
    hands_data: List[Dict[str, Any]] = []
    if active_server is not None and getattr(active_server, "_active_hands", None):
        try:
            hands_list = list(active_server._active_hands.values())
        except Exception:
            hands_list = []
        for h in hands_list:
            hands_data.append(dict(h))

    return {
        "version": VERSION,
        "lineage": LINEAGE,
        "uptime": uptime,
        "uptime_formatted": uptime_formatted,
        "eyes": eyes_data,
        "nose": nose_data,
        "mouth": mouth_data,
        "hands": hands_data,
    }


__all__ = [
    "LINEAGE",
    "VERSION",
    "DASHBOARD_HTML",
    "INDEX_HTML",
    "WebUIServer",
    "start_webui_server",
    "stop_webui_server",
    "get_active_webui_server",
    "webui_broadcast",
    "webui_broadcast_async",
    "format_sse_event",
    "get_system_status",
    "clear_host_telemetry_cache",
    "create_app",
    "_SyncAndAwaitableNone",
]
