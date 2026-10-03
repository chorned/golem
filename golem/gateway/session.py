#!/usr/bin/env python3
"""
golem/gateway/session.py
Thread-scoped session handle managing long-running HeadlessAgentAdapter instances and tracking conversational state.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import time
import asyncio
import logging
import inspect
from contextvars import ContextVar
from enum import Enum
from typing import Optional, AsyncGenerator, Any, Dict, List

from golem.agent.protocol import (
    AgentEvent,
    FinishEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
)
from golem.agent.mouth import (
    generate_mouth_response,
    is_cancellation_message,
    is_status_inquiry,
    is_conversational_checkin,
    is_conversational_inquiry,
    is_out_of_band_message,
)

logger = logging.getLogger("Golem.Gateway.Session")

CURRENT_SESSION: ContextVar[Optional["SessionHandle"]] = ContextVar("current_session", default=None)


class SessionState(str, Enum):
    INITIALIZING = "initializing"
    IDLE = "idle"
    WORKING = "working"
    AWAITING_INPUT = "awaiting_input"
    CLOSED = "closed"
    FAILED = "failed"


class EyesStatusResult(dict):
    """Structured dictionary representing The Eyes' observation of the runner pane."""

    def __init__(self, coro_func=None, initial_dict=None):
        super().__init__(initial_dict or {})
        self._coro_func = coro_func

    def __await__(self):
        if self._coro_func:
            return self._coro_func().__await__()
        async def _c():
            return self
        return _c().__await__()


class SessionHandle:
    """
    Thread-scoped session handle managing a dedicated, long-running
    HeadlessAgentAdapter instance and tracking conversational state.
    """

    def __init__(
        self,
        session_id: str,
        channel: str,
        thread_ts: str,
        adapter: Any = None,
        target_cwd: str = "",
        is_imessage: bool = False,
        timeout_seconds: float = 14400.0,  # 4 hours default
        harness: Optional[Any] = None,
        workspace_id: Optional[str] = None,
        liaison_pane: Optional[str] = None,
        runner_pane: Optional[str] = None,
        runner_executor: Optional[Any] = None,
        buffer_observer: Optional[Any] = None,
        router: Optional[Any] = None,
        bus: Optional[Any] = None,
        driver: Optional[Any] = None,
    ) -> None:
        self.session_id: str = session_id
        self.channel: str = channel
        self.thread_ts: str = thread_ts
        self.adapter: Any = driver if driver is not None else adapter
        self.target_cwd: str = target_cwd
        self.is_imessage: bool = is_imessage
        self.timeout_seconds: float = timeout_seconds
        self.harness: Optional[Any] = harness
        self.workspace_id: Optional[str] = workspace_id
        self.liaison_pane: Optional[str] = liaison_pane
        self.runner_pane: Optional[str] = runner_pane
        self.router: Optional[Any] = router
        self.bus: Optional[Any] = bus

        # Active execution & buffer observer
        self.runner_executor: Optional[Any] = runner_executor
        if buffer_observer is not None:
            self.buffer_observer = buffer_observer
        else:
            herdr_cl = getattr(self.harness, "herdr_client", None)
            from golem.execution.buffer import RunnerBufferObserver
            self.buffer_observer = RunnerBufferObserver(herdr_client=herdr_cl)

        self.active_execution: Optional[Any] = None

        self.state: SessionState = SessionState.INITIALIZING
        self.created_at: float = time.time()
        self.last_active_ts: float = time.time()
        self.lock: asyncio.Lock = asyncio.Lock()
        self._harness_torn_down: bool = False

        # Conversational metadata
        self.current_turn: int = 0
        self.approval_msg_ts: Optional[str] = None
        self.last_prompt: str = ""
        self.last_response: str = ""
        self._active_generator: Optional[AsyncGenerator[Any, None]] = None
        self._owner_task: Optional[asyncio.Task[Any]] = None

    @property
    def driver(self) -> Any:
        """Alias for adapter representing the underlying BaseAgentDriver instance."""
        return self.adapter

    @driver.setter
    def driver(self, value: Any) -> None:
        self.adapter = value

    @property
    def is_executing(self) -> bool:
        """True if an execution is active and running in Pane 2."""
        return (
            self.active_execution is not None
            and getattr(self.active_execution, "status", None) == "running"
        )

    @property
    def is_busy(self) -> bool:
        """True if session lock is held or Pane 2 is executing."""
        return self.lock.locked() or self.is_executing

    @property
    def last_activity_at(self) -> float:
        """Alias for last_active_ts."""
        return self.last_active_ts

    @last_activity_at.setter
    def last_activity_at(self, value: float) -> None:
        self.last_active_ts = value

    def touch(self) -> None:
        """Update last_active_ts to current time."""
        self.last_active_ts = time.time()

    def is_expired(self) -> bool:
        """Return True if session has been inactive longer than timeout_seconds."""
        return (time.time() - self.last_active_ts) > self.timeout_seconds

    async def get_runner_status(self) -> Dict[str, Any]:
        """
        Reads runner buffer non-intrusively via RunnerBufferObserver.
        Extracts any interactive blockers (OAuth callback URLs, confirmation prompts).
        Returns status dict:
          {
             "status": "running" | "completed" | "idle",
             "blockers": [...],
             "blocker": blocker,
             "buffer_tail": ...,
             "output_tail": ...,
             "pid": ...,
             "command": ...
          }
        """
        pane = self.runner_pane or "runner"
        buffer_text = ""
        try:
            if hasattr(self.buffer_observer, "read_buffer"):
                buffer_text = await self.buffer_observer.read_buffer(pane, lines=30)
        except Exception as e:
            logger.warning(f"Error reading runner buffer for session '{self.session_id}': {e}")
            buffer_text = f"Error reading runner buffer: {e}"

        # Extract blockers
        blocker = None
        blockers = []
        try:
            if hasattr(self.buffer_observer, "extract_all_blockers"):
                blockers = self.buffer_observer.extract_all_blockers(buffer_text)
                blocker = blockers[0] if blockers else None
            elif hasattr(self.buffer_observer, "extract_interactive_blockers"):
                blocker = self.buffer_observer.extract_interactive_blockers(buffer_text)
                if blocker:
                    blockers = [blocker]
        except Exception as e:
            logger.warning(f"Error extracting interactive blockers: {e}")

        # Compute buffer tail (clean stripped lines)
        from golem.transport.normalizer import strip_ansi
        clean_buf = strip_ansi(buffer_text).strip()
        lines = [l for l in clean_buf.splitlines() if l.strip()]
        buffer_tail = "\n".join(lines[-10:]) if lines else ""

        # Determine status
        status = "idle"
        pid = None
        cmd = None
        if self.active_execution is not None:
            exec_status = getattr(self.active_execution, "status", None)
            pid = getattr(self.active_execution, "pid", None)
            cmd = getattr(self.active_execution, "command", None)
            if exec_status == "running":
                status = "running"
            elif exec_status == "completed":
                status = "completed"
            elif exec_status == "error":
                status = "error"
        elif self.lock.locked():
            status = "running"
        elif blocker:
            status = "running"

        if status == "idle" and self.active_execution is None and self.runner_pane:
            try:
                executor = self.runner_executor
                if executor is None:
                    from golem.execution.runner import RunnerExecutor
                    herdr_cl = getattr(self.harness, "herdr_client", None)
                    executor = RunnerExecutor(herdr_client=herdr_cl, buffer_observer=self.buffer_observer)
                if hasattr(executor, "_get_pane_foreground_pid"):
                    fg_pid = await executor._get_pane_foreground_pid(self.runner_pane, timeout=0.2)
                    if fg_pid is not None and fg_pid > 0:
                        status = "running"
                        pid = fg_pid
                        from golem.execution.runner import ExecutionHandle
                        self.active_execution = ExecutionHandle(
                            status="running",
                            pane=self.runner_pane,
                            command=cmd or "",
                            pid=fg_pid,
                            message="Process running in runner pane.",
                        )
            except Exception as e:
                logger.debug(f"Error checking pane foreground PID in get_runner_status: {e}")

        return {
            "status": status,
            "blockers": blockers,
            "blocker": blocker,
            "buffer_tail": buffer_tail,
            "output_tail": buffer_tail,
            "pid": pid,
            "command": cmd,
            "raw_buffer": buffer_text,
        }

    def _compute_eyes_status_from_dict(self, status_info: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize status_info into standard Eyes status contract."""
        blocker_obj = status_info.get("blocker")
        if not blocker_obj and status_info.get("blockers"):
            b_list = status_info["blockers"]
            if isinstance(b_list, list) and b_list:
                blocker_obj = b_list[0]

        normalized_blocker: Optional[Dict[str, Any]] = None
        if blocker_obj and isinstance(blocker_obj, dict):
            b_type = blocker_obj.get("type") or blocker_obj.get("blocker_type")
            b_prompt = (
                blocker_obj.get("prompt")
                or blocker_obj.get("content")
                or blocker_obj.get("blocker_prompt")
            )
            b_url = blocker_obj.get("url") or blocker_obj.get("blocker_url")
            normalized_blocker = {
                "type": b_type,
                "prompt": b_prompt,
                "url": b_url,
            }

        active_command = (
            status_info.get("command")
            or getattr(getattr(self, "active_execution", None), "command", "")
            or ""
        )
        pid = (
            status_info.get("pid")
            or getattr(getattr(self, "active_execution", None), "pid", None)
        )
        buffer_tail = (
            status_info.get("buffer_tail")
            or status_info.get("output_tail")
            or ""
        )

        status_val = str(status_info.get("status", "")).lower()
        if normalized_blocker is not None and normalized_blocker.get("type") in ("oauth", "prompt", "confirmation", "secret"):
            state = "BLOCKER"
        elif status_val in ("completed", "error", "done", "success", "failed"):
            state = "COMPLETED"
        elif status_val == "running" or self.is_executing:
            state = "RUNNING"
        elif status_val == "idle":
            state = "IDLE"
        elif pid is not None and pid != "":
            state = "RUNNING"
        else:
            state = "IDLE"

        return {
            "state": state,
            "active_command": active_command,
            "command": active_command,
            "pid": pid,
            "blocker": normalized_blocker,
            "blockers": [normalized_blocker] if normalized_blocker else [],
            "buffer_tail": buffer_tail,
        }

    def get_eyes_status(self, status_info: Optional[Dict[str, Any]] = None) -> Any:
        """
        Return structured dictionary reflecting The Eyes' observation of the runner pane.
        Pure JSON-serializable dictionary with: state, active_command, pid, blocker, buffer_tail.
        Supports both synchronous invocation with status_info and asynchronous awaiting.
        """
        if status_info is not None:
            computed = self._compute_eyes_status_from_dict(status_info)
            return EyesStatusResult(initial_dict=computed)

        # Fallback initial dict in case called synchronously without status_info
        fallback_status = {
            "status": "running" if self.is_executing else "idle",
            "command": getattr(getattr(self, "active_execution", None), "command", ""),
            "pid": getattr(getattr(self, "active_execution", None), "pid", None),
            "buffer_tail": "",
            "blockers": [],
            "blocker": None,
        }
        fallback_computed = self._compute_eyes_status_from_dict(fallback_status)

        async def _fetch():
            si = await self.get_runner_status()
            return EyesStatusResult(initial_dict=self._compute_eyes_status_from_dict(si))

        return EyesStatusResult(coro_func=_fetch, initial_dict=fallback_computed)

    async def cancel_execution(self) -> bool:
        """
        Sends interrupt / cancellation to Pane 2 out-of-band and resets active_execution.
        Returns True on successful cancellation dispatch.
        """
        pane = self.runner_pane or "runner"
        logger.info(f"Cancelling execution in session '{self.session_id}' (pane={pane})")

        # 1. Kill active PID if known and local
        if self.active_execution is not None:
            pid = getattr(self.active_execution, "pid", None)
            if pid and isinstance(pid, int) and pid > 0:
                try:
                    import signal
                    os.kill(pid, signal.SIGINT)
                except (ProcessLookupError, PermissionError, Exception):
                    pass

        # 2. Send Ctrl+C to pane via Herdr CLI or client
        try:
            herdr_cl = getattr(self.harness, "herdr_client", None)
            if herdr_cl and hasattr(herdr_cl, "run_cli"):
                await herdr_cl.run_cli(["pane", "send-keys", pane, "C-c"], timeout=5.0)
            else:
                import shutil
                herdr_bin = shutil.which("herdr") or os.path.expanduser("~/.local/bin/herdr")
                proc = await asyncio.create_subprocess_exec(
                    herdr_bin, "pane", "send-keys", pane, "C-c",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await asyncio.wait_for(proc.communicate(), timeout=3.0)
        except Exception as e:
            logger.debug(f"Sending C-c to runner pane {pane} encountered error: {e}")

        # 3. Cancel owner task if active and working
        if self._owner_task and not self._owner_task.done():
            cur_task = asyncio.current_task()
            if self._owner_task is not cur_task:
                self._owner_task.cancel()

        # 4. Reset active_execution and state
        self.active_execution = None
        if self.state == SessionState.WORKING:
            self.state = SessionState.IDLE
        return True

    async def _stream_out_of_band_impl(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        """
        Dedicated non-blocking generator for out-of-band messages (status, check-in, cancellation).
        Does NOT acquire self.lock.
        """
        self.touch()
        if is_cancellation_message(prompt):
            await self.cancel_execution()
            msg = "🛑 *Execution Cancelled:* Stopped active command in runner pane."
            yield StatusUpdateEvent(status="cancelled")
            yield TextDeltaEvent(text_delta=msg)
            yield FinishEvent(status="SUCCESS", response=msg)
            return

        # Status inquiry, conversational check-in, or mid-stream inquiry handled by Mouth LLM
        status_info = await self.get_runner_status()
        eyes_status = self.get_eyes_status(status_info)
        nose_status = None
        try:
            from golem.sensory.nose import get_default_nose_sentinel
            sentinel = get_default_nose_sentinel()
            if sentinel is not None:
                nose_status = await sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
        except Exception as e:
            logger.debug(f"Error resolving nose_status in _stream_out_of_band_impl: {e}")
        msg = await generate_mouth_response(
            prompt,
            status_info,
            session=self,
            eyes_status=eyes_status,
            nose_status=nose_status,
        )

        blocker = status_info.get("blocker")
        if not blocker and status_info.get("blockers"):
            b_list = status_info["blockers"]
            if b_list and isinstance(b_list, list):
                blocker = b_list[0]

        if blocker and blocker.get("type") in ("oauth", "prompt"):
            yield StatusUpdateEvent(status="blocked")
        elif status_info.get("status") == "running" or self.is_executing:
            yield StatusUpdateEvent(status="running")
        else:
            yield StatusUpdateEvent(status="idle")

        yield TextDeltaEvent(text_delta=msg)
        yield FinishEvent(status="SUCCESS", response=msg)

    async def send_user_message(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        """
        Execute a conversational turn by routing the prompt to the warm agent adapter.
        Guarantees serialized execution per thread via session lock, UNLESS prompt
        is an out-of-band check-in, status query, or cancellation while Pane 2 is executing
        (or lock is held), in which case it is processed immediately through a dedicated
        non-blocking path without waiting for self.lock.acquire().
        """
        if self.state == SessionState.CLOSED:
            raise RuntimeError(f"Session '{self.session_id}' is CLOSED; cannot send message.")

        cur_task = asyncio.current_task()
        # Clean up prior generator only if abandoned by the same task (or dead task), never if another task is executing
        if self._active_generator is not None:
            if self._owner_task is cur_task or (self._owner_task and self._owner_task.done()):
                old_gen = self._active_generator
                self._active_generator = None
                self._owner_task = None
                try:
                    await old_gen.aclose()
                except Exception:
                    pass

        token = CURRENT_SESSION.set(self)
        try:
            # Out-of-band bypass check: never block behind lock if executing or busy and message is out-of-band
            if (self.is_executing or self.lock.locked()) and is_out_of_band_message(prompt, session=self):
                async for ev in self._stream_out_of_band_impl(prompt):
                    yield ev
                return

            gen_holder: List[Any] = [None]
            gen = self._stream_turn_impl(prompt, gen_holder)
            gen_holder[0] = gen
            async for event in gen:
                yield event
        finally:
            try:
                CURRENT_SESSION.reset(token)
            except (ValueError, LookupError):
                pass

    async def _stream_turn_impl(
        self, prompt: str, gen_holder: Optional[List[Any]] = None
    ) -> AsyncGenerator[AgentEvent, None]:
        """Internal turn execution loop under self.lock."""
        await self.lock.acquire()
        try:
            self._owner_task = asyncio.current_task()
            if gen_holder and len(gen_holder) > 0 and gen_holder[0] is not None:
                self._active_generator = gen_holder[0]

            if self.state == SessionState.CLOSED:
                raise RuntimeError(f"Session '{self.session_id}' is CLOSED; cannot send message.")

            self.touch()
            self.state = SessionState.WORKING
            self.last_prompt = prompt
            self.current_turn += 1

            try:
                adapter_fn = (
                    getattr(self.adapter, "send_turn", None)
                    or getattr(self.adapter, "stream_turn", None)
                    or getattr(self.adapter, "send_prompt", None)
                )
                if adapter_fn is None:
                    raise AttributeError(f"Adapter {self.adapter} does not provide 'send_prompt', 'send_turn', or 'stream_turn'")

                async for event in adapter_fn(prompt):
                    ev_type = getattr(event, "event_type", None) or (
                        event.get("event_type") if isinstance(event, dict) else None
                    )

                    if ev_type in ("text_delta", "tool_start", "tool_end", "status_update"):
                        self.touch()
                    elif ev_type == "awaiting_input":
                        if self.state != SessionState.CLOSED:
                            self.state = SessionState.AWAITING_INPUT
                    elif ev_type == "finish":
                        status = getattr(event, "status", None) or (
                            event.get("status") if isinstance(event, dict) else None
                        )
                        if status == "SUCCESS" or status is None:
                            if self.state != SessionState.CLOSED:
                                self.state = SessionState.IDLE
                            self.last_response = getattr(event, "response", "") or (
                                event.get("response", "") if isinstance(event, dict) else ""
                            )
                        else:
                            if self.state != SessionState.CLOSED:
                                self.state = SessionState.FAILED

                    yield event

                # If turn ended without explicit finish event and not awaiting input, restore IDLE
                if self.state == SessionState.WORKING:
                    self.state = SessionState.IDLE

            except Exception as e:
                if self.state != SessionState.CLOSED:
                    self.state = SessionState.FAILED
                logger.error(f"Error executing turn in session '{self.session_id}': {e}")
                raise
        finally:
            conv_id = getattr(self.adapter, "conversation_id", None)
            if conv_id and getattr(self, "router", None) and hasattr(self.router, "record_session"):
                try:
                    self.router.record_session(
                        session_key=self.session_id,
                        conversation_id=conv_id,
                        target_cwd=self.target_cwd,
                        channel=self.channel,
                        thread_ts=self.thread_ts,
                    )
                except Exception as e:
                    logger.debug(f"Error persisting session registry on turn complete: {e}")

            self._owner_task = None
            self._active_generator = None
            if self.lock.locked():
                self.lock.release()

    # Bidirectional alias
    send_turn = send_user_message

    async def close(self, reason: str = "closed") -> None:
        """
        Gracefully terminate underlying agent process and set state to CLOSED.
        """
        logger.info(f"Closing session '{self.session_id}' (reason: {reason})")
        self.state = SessionState.CLOSED
        if self.active_execution is not None:
            try:
                await self.cancel_execution()
            except Exception:
                pass
        self.active_execution = None
        if self._active_generator is not None:
            old_gen = self._active_generator
            self._active_generator = None
            self._owner_task = None
            try:
                await old_gen.aclose()
            except Exception:
                pass
        if self.adapter:
            try:
                if hasattr(self.adapter, "stop"):
                    res = self.adapter.stop()
                    if inspect.isawaitable(res):
                        await res
                elif hasattr(self.adapter, "terminate_session"):
                    res = self.adapter.terminate_session()
                    if inspect.isawaitable(res):
                        await res
            except Exception as e:
                logger.warning(f"Error stopping adapter during close for session '{self.session_id}': {e}")
        if self.harness and self.workspace_id:
            harness = self.harness
            ws_id = self.workspace_id
            self.harness = None
            self._harness_torn_down = True
            try:
                res = harness.teardown(ws_id)
                if inspect.isawaitable(res):
                    await res
            except Exception as e:
                logger.warning(
                    f"Error tearing down harness workspace '{ws_id}' for session '{self.session_id}': {e}"
                )

        # Remove session queue from ShoulderTapEventBus to prevent memory leak
        try:
            bus = getattr(self, "bus", None)
            if bus is None and getattr(self, "router", None) is not None:
                bus = getattr(self.router, "bus", None)
            if bus is None:
                from golem.sensory.bus import peek_default_event_bus
                bus = peek_default_event_bus()
            if bus is not None and hasattr(bus, "remove_session_queue"):
                bus.remove_session_queue(self.session_id)
        except Exception as e:
            logger.warning(f"Error removing session queue for '{self.session_id}': {e}")
