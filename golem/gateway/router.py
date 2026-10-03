#!/usr/bin/env python3
"""
golem/gateway/router.py
Thread-safe asynchronous router mapping channel:thread_ts to dedicated SessionHandle instances
with concurrency synchronization and 4-hour inactivity reaping watchdog.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import json
import time
import asyncio
import inspect
import logging
from typing import Dict, List, Tuple, Optional, Callable, Any, TYPE_CHECKING

from golem.gateway.session import SessionHandle, SessionState
from golem.agent.adapter import HeadlessAgentAdapter
from golem.config import get_active_engine
from golem.agent.drivers import create_driver, BaseAgentDriver

if TYPE_CHECKING:
    from golem.execution.harness import DualPaneHarness

logger = logging.getLogger("Golem.Gateway.Router")

DEFAULT_REGISTRY_PATH = os.path.expanduser("~/.gemini/first-mate/sessions_registry.json")


class SessionRouter:
    """
    Session registry and lifecycle manager mapping conversational threads to warm SessionHandles.
    Includes background inactivity reaper task and disk-backed session persistence for multi-day continuity.
    """

    def __init__(
        self,
        default_target_cwd: str = "/Users/hermes/projects/AGY",
        inactivity_timeout: float = 14400.0,  # 4 hours
        reaper_interval: float = 60.0,
        adapter_factory: Optional[Callable[[str], Any]] = None,
        harness: Optional[DualPaneHarness] = None,
        registry_path: Optional[str] = None,
        sensory_bus: Optional[Any] = None,
        bus: Optional[Any] = None,
        engine: Optional[str] = None,
        driver_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.default_target_cwd = default_target_cwd
        self.inactivity_timeout = inactivity_timeout
        self.reaper_interval = reaper_interval
        self.adapter_factory = adapter_factory
        self.driver_factory = driver_factory
        self._explicit_engine: Optional[str] = engine
        self.harness = harness
        self.bus: Optional[Any] = sensory_bus or bus
        self.registry_path = (
            registry_path
            or os.environ.get("FIRSTMATE_SESSIONS_REGISTRY_PATH")
            or DEFAULT_REGISTRY_PATH
        )

        self.sessions: Dict[str, SessionHandle] = {}
        self._router_lock: asyncio.Lock = asyncio.Lock()
        self._reaper_task: Optional[asyncio.Task] = None
        self._is_running: bool = False
        self._registry: Dict[str, Dict[str, Any]] = self._load_registry()

    @property
    def engine(self) -> str:
        """
        Return active agent driver engine.
        Uses explicit engine if supplied; otherwise dynamically resolves via get_active_engine().
        """
        if self._explicit_engine is not None:
            return self._explicit_engine.lower().strip()
        return get_active_engine()

    @engine.setter
    def engine(self, value: Optional[str]) -> None:
        self._explicit_engine = value

    def _load_registry(self) -> Dict[str, Dict[str, Any]]:
        """Load persistent session mappings from disk registry."""
        if not self.registry_path or not os.path.exists(self.registry_path):
            return {}
        try:
            with open(self.registry_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception as e:
            logger.warning(f"Failed to load session registry from '{self.registry_path}': {e}")
        return {}

    def _save_registry(self) -> None:
        """Persist session mappings to disk atomically."""
        if not self.registry_path:
            return
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.registry_path)), exist_ok=True)
            tmp_path = f"{self.registry_path}.tmp.{os.getpid()}"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._registry, f, indent=2)
            os.replace(tmp_path, self.registry_path)
        except Exception as e:
            logger.warning(f"Failed to save session registry to '{self.registry_path}': {e}")

    def record_session(
        self,
        session_key: str,
        conversation_id: str,
        target_cwd: str,
        channel: str = "",
        thread_ts: str = "",
    ) -> None:
        """Record session mapping to disk registry for persistent thread continuity."""
        self._registry[session_key] = {
            "conversation_id": conversation_id,
            "target_cwd": target_cwd,
            "channel": channel,
            "thread_ts": thread_ts,
            "updated_at": time.time(),
        }
        self._save_registry()

    def get_recorded_session(self, session_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve recorded metadata for a session from the registry."""
        return self._registry.get(session_key)

    def has_thread(self, channel: str, thread_ts: str) -> bool:
        """Check if a channel:thread_ts is tracked in active memory or disk registry."""
        key = self.build_session_key(channel, thread_ts)
        if key in self.sessions:
            s = self.sessions[key]
            state = getattr(s, "state", None)
            state_val = getattr(state, "value", state)
            if state_val != "closed":
                return True
        return key in self._registry

    @staticmethod
    def build_session_key(channel: str, thread_ts: str) -> str:
        """Construct deterministic session identifier from channel and thread_ts."""
        return f"{channel}:{thread_ts}"

    async def get_or_create(
        self,
        channel: str,
        thread_ts: str,
        target_cwd: Optional[str] = None,
        is_imessage: bool = False,
    ) -> Tuple[SessionHandle, bool]:
        """
        Retrieve active session or instantiate a new warm SessionHandle.
        Resumes conversation from persistent disk registry if previously recorded.
        Returns: (session_handle, is_new_session)
        """
        key = self.build_session_key(channel, thread_ts)
        async with self._router_lock:
            existing = self.sessions.get(key)
            if existing and existing.state not in (SessionState.CLOSED, SessionState.FAILED):
                existing.touch()
                return existing, False

            if existing and existing.state in (SessionState.CLOSED, SessionState.FAILED):
                try:
                    await existing.close(reason="failed_session_replacement")
                except Exception as e:
                    logger.warning(f"Error closing previous session '{key}' during replacement: {e}")

            # Check persistent registry for previous conversation ID
            recorded = self._registry.get(key) or {}
            saved_conversation_id = recorded.get("conversation_id")
            saved_cwd = recorded.get("target_cwd")

            cwd = target_cwd or saved_cwd or self.default_target_cwd
            extra_args = ["--conversation", saved_conversation_id] if saved_conversation_id else []

            adapter = None
            if self.driver_factory:
                try:
                    sig = inspect.signature(self.driver_factory)
                    kwargs = {}
                    if "engine" in sig.parameters:
                        kwargs["engine"] = self.engine
                    if "cwd" in sig.parameters:
                        kwargs["cwd"] = cwd
                    if "extra_args" in sig.parameters:
                        kwargs["extra_args"] = extra_args
                    if kwargs:
                        adapter = self.driver_factory(**kwargs)
                    elif len(sig.parameters) >= 2:
                        adapter = self.driver_factory(cwd, extra_args)
                    else:
                        adapter = self.driver_factory(cwd)
                except Exception:
                    adapter = self.driver_factory(cwd)
            elif self.adapter_factory:
                try:
                    sig = inspect.signature(self.adapter_factory)
                    if "extra_args" in sig.parameters or len(sig.parameters) >= 2:
                        adapter = self.adapter_factory(cwd, extra_args)
                    else:
                        adapter = self.adapter_factory(cwd)
                except Exception:
                    adapter = self.adapter_factory(cwd)
            else:
                adapter = create_driver(engine=self.engine, cwd=cwd, extra_args=extra_args)

            if hasattr(adapter, "start"):
                try:
                    res = adapter.start()
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    if saved_conversation_id:
                        logger.warning(
                            f"Failed to resume conversation '{saved_conversation_id}' for '{key}': {e}. "
                            "Falling back to fresh agent adapter."
                        )
                        if self.driver_factory:
                            adapter = self.driver_factory(cwd)
                        elif self.adapter_factory:
                            adapter = self.adapter_factory(cwd)
                        else:
                            adapter = create_driver(engine=self.engine, cwd=cwd)

                        res = adapter.start()
                        if inspect.isawaitable(res):
                            await res
                    else:
                        raise

            # Record newly initialized or resumed conversation ID into registry
            conv_id = getattr(adapter, "conversation_id", None)
            if conv_id and isinstance(conv_id, str):
                self.record_session(
                    session_key=key,
                    conversation_id=conv_id,
                    target_cwd=cwd,
                    channel=channel,
                    thread_ts=thread_ts,
                )

            liaison_pane: Optional[str] = None
            runner_pane: Optional[str] = None
            workspace_id: Optional[str] = None
            if self.harness is not None:
                res = self.harness.provision_dual_pane(session_id=key, cwd=cwd)
                if inspect.isawaitable(res):
                    liaison_pane, runner_pane = await res
                else:
                    liaison_pane, runner_pane = res
                workspace_id = key

            session = SessionHandle(
                session_id=key,
                channel=channel,
                thread_ts=thread_ts,
                adapter=adapter,
                target_cwd=cwd,
                is_imessage=is_imessage,
                timeout_seconds=self.inactivity_timeout,
                harness=self.harness,
                workspace_id=workspace_id,
                liaison_pane=liaison_pane,
                runner_pane=runner_pane,
                router=self,
                bus=self.bus,
            )
            session.state = SessionState.IDLE
            self.sessions[key] = session
            logger.info(f"Initialized new warm session '{key}' in cwd '{cwd}'")
            return session, True

    async def get_or_create_session(
        self,
        channel: str,
        thread_ts: str,
        cwd: Optional[str] = None,
        is_imessage: bool = False,
    ) -> SessionHandle:
        """Convenience alias returning SessionHandle directly."""
        session, _ = await self.get_or_create(channel, thread_ts, target_cwd=cwd, is_imessage=is_imessage)
        return session

    async def teardown_session(self, session_id: str, reason: str = "explicit") -> bool:
        """
        Explicitly teardown a session by its session_id key and purge its event bus queue.
        """
        async with self._router_lock:
            session = self.sessions.pop(session_id, None)
            if session:
                await session.close(reason=reason)
                if not getattr(session, "_harness_torn_down", False) and self.harness and getattr(session, "workspace_id", None):
                    try:
                        res = self.harness.teardown(session.workspace_id)
                        if inspect.isawaitable(res):
                            await res
                    except Exception as e:
                        logger.warning(f"Error in router tearing down harness for '{session.workspace_id}': {e}")
                logger.info(f"Tore down session '{session_id}' (reason: {reason})")
                return True
            else:
                # Defensively purge session queue in bus even if session was already removed
                try:
                    bus = self.bus
                    if bus is None:
                        from golem.sensory.bus import peek_default_event_bus
                        bus = peek_default_event_bus()
                    if bus is not None and hasattr(bus, "remove_session_queue"):
                        bus.remove_session_queue(session_id)
                except Exception:
                    pass
            return False

    async def teardown(self, channel: str, thread_ts: str, reason: str = "explicit") -> bool:
        """Explicitly teardown session (e.g. on /wrapup or /archive)."""
        key = self.build_session_key(channel, thread_ts)
        return await self.teardown_session(key, reason=reason)

    handle_teardown = teardown

    async def reap_expired_sessions(self) -> List[str]:
        """Audit all sessions and prune those inactive > timeout_seconds or in dead state."""
        pruned_keys: List[str] = []
        async with self._router_lock:
            expired_keys = [
                key
                for key, s in self.sessions.items()
                if s.is_expired() or s.state in (SessionState.CLOSED, SessionState.FAILED)
            ]
            for key in expired_keys:
                s = self.sessions.pop(key, None)
                if s:
                    await s.close(reason="inactivity_timeout")
                    if not getattr(s, "_harness_torn_down", False) and self.harness and getattr(s, "workspace_id", None):
                        try:
                            res = self.harness.teardown(s.workspace_id)
                            if inspect.isawaitable(res):
                                await res
                        except Exception as e:
                            logger.warning(f"Error in router reaping harness for '{s.workspace_id}': {e}")
                    pruned_keys.append(key)
        if pruned_keys:
            logger.info(f"Reaped {len(pruned_keys)} expired session(s): {pruned_keys}")
        return pruned_keys

    sweep_expired_sessions = reap_expired_sessions

    def start_reaper(self) -> None:
        """Start periodic background reaper task."""
        if self._reaper_task is None or self._reaper_task.done():
            self._is_running = True
            self._reaper_task = asyncio.create_task(self._reaper_loop())
            logger.info(f"Started session reaper watchdog (interval: {self.reaper_interval}s)")

    async def stop_reaper(self) -> None:
        """Stop reaper and gracefully close all sessions."""
        self._is_running = False
        if self._reaper_task:
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except (asyncio.CancelledError, Exception):
                pass
            self._reaper_task = None

        # Gracefully close remaining sessions
        async with self._router_lock:
            for s in list(self.sessions.values()):
                try:
                    await s.close(reason="shutdown")
                    if not getattr(s, "_harness_torn_down", False) and self.harness and getattr(s, "workspace_id", None):
                        res = self.harness.teardown(s.workspace_id)
                        if inspect.isawaitable(res):
                            await res
                except Exception as e:
                    logger.warning(f"Error during shutdown for session '{s.session_id}': {e}")
            self.sessions.clear()
        logger.info("Stopped session reaper watchdog and cleared sessions")

    async def _reaper_loop(self) -> None:
        while self._is_running:
            try:
                await asyncio.sleep(self.reaper_interval)
                if not self._is_running:
                    break
                await self.reap_expired_sessions()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in session reaper loop: {e}")
