#!/usr/bin/env python3
"""
first_mate/gateway/session.py
Thread-scoped session handle managing long-running HeadlessAgentAdapter instances and tracking conversational state.
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

from first_mate.agent.protocol import (
    AgentEvent,
    FinishEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
)

logger = logging.getLogger("FirstMate.Gateway.Session")

CURRENT_SESSION: ContextVar[Optional["SessionHandle"]] = ContextVar("current_session", default=None)



def is_cancellation_message(text: str) -> bool:
    """Check if message expresses cancellation intent."""
    clean = text.strip().lower()
    stripped = clean.rstrip("?.!").strip()
    if stripped in (
        "/cancel", "!cancel", "/stop", "!stop", "/abort", "!abort",
        "cancel", "stop", "abort", "kill", "quit", "interrupt",
        "please cancel", "please stop", "please abort", "please interrupt",
        "cancel please", "stop please", "abort please",
    ):
        return True
    cancellation_patterns = [
        r"^(please\s+)?(cancel|stop|abort|kill|terminate|interrupt)(\s+(that|this|it|the))?(\s+(task|command|execution|job|runner|process))?[\.!\?]?$",
        r"^(can|could|would)\s+you\s+(please\s+)?(cancel|stop|abort|kill|terminate|interrupt)(\s+(that|this|it|the))?(\s+(task|command|execution|job|runner|process))?[\.!\?]?$",
        r"^(please\s+)?(cancel|stop|abort)\s+(that|this|it|the\s+\w+|task|command|execution|job|runner|process)\b",
        r"\b(can|could|would)\s+you\s+(please\s+)?(cancel|stop|abort)\b",
        r"\bplease\s+(cancel|stop|abort|terminate)\b",
        r"\b(cancel|stop|abort)\s+(the\s+)?(task|command|execution|job|runner|process)\b",
    ]
    return any(bool(re.search(p, clean)) for p in cancellation_patterns)


def is_status_inquiry(text: str) -> bool:
    """Check if message is inquiring about status, progress, buffer, or blockers."""
    clean = text.strip().lower()
    if clean in ("status", "progress", "url", "oauth", "link", "/status", "!status"):
        return True
    patterns = [
        r"\b(status|progress|check\s+status|show\s+buffer)\b",
        r"\b(what('?s|\s+is)\s+(the\s+)?(status|url|link|oauth|progress))\b",
        r"\b(how('?s|\s+is)\s+it\s+going)\b",
        r"\b(where\s+are\s+we|what('?s|\s+is)\s+happening)\b",
        r"\b(what('?s|\s+is)\s+the\s+url)\b",
    ]
    return any(bool(re.search(p, clean)) for p in patterns)


def is_conversational_checkin(text: str) -> bool:
    """Check if message is a conversational check-in during background execution."""
    clean = text.strip().lower()
    if clean in ("ping", "update", "updates", "updates?"):
        return True
    patterns = [
        r"\b(are\s+you\s+(still\s+)?(there|working|running))\b",
        r"\b(is\s+it\s+(still\s+)?running)\b",
        r"\b(still\s+running\??)\b",
        r"\b(any\s+updates?\??)\b",
        r"\b(what\s+are\s+you\s+doing\??)\b",
        r"\b(are\s+we\s+done\??)\b",
    ]
    return any(bool(re.search(p, clean)) for p in patterns)


def is_out_of_band_message(text: str) -> bool:
    """Check if message qualifies for out-of-band processing while session is busy."""
    return is_cancellation_message(text) or is_status_inquiry(text) or is_conversational_checkin(text)


class SessionState(str, Enum):
    INITIALIZING = "initializing"
    IDLE = "idle"
    WORKING = "working"
    AWAITING_INPUT = "awaiting_input"
    CLOSED = "closed"
    FAILED = "failed"


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
        adapter: Any,
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
    ) -> None:
        self.session_id: str = session_id
        self.channel: str = channel
        self.thread_ts: str = thread_ts
        self.adapter: Any = adapter
        self.target_cwd: str = target_cwd
        self.is_imessage: bool = is_imessage
        self.timeout_seconds: float = timeout_seconds
        self.harness: Optional[Any] = harness
        self.workspace_id: Optional[str] = workspace_id
        self.liaison_pane: Optional[str] = liaison_pane
        self.runner_pane: Optional[str] = runner_pane
        self.router: Optional[Any] = router

        # Active execution & buffer observer
        self.runner_executor: Optional[Any] = runner_executor
        if buffer_observer is not None:
            self.buffer_observer = buffer_observer
        else:
            herdr_cl = getattr(self.harness, "herdr_client", None)
            from first_mate.execution.buffer import RunnerBufferObserver
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
        from first_mate.transport.normalizer import strip_ansi
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
                    from first_mate.execution.runner import RunnerExecutor
                    herdr_cl = getattr(self.harness, "herdr_client", None)
                    executor = RunnerExecutor(herdr_client=herdr_cl, buffer_observer=self.buffer_observer)
                if hasattr(executor, "_get_pane_foreground_pid"):
                    fg_pid = await executor._get_pane_foreground_pid(self.runner_pane, timeout=0.2)
                    if fg_pid is not None and fg_pid > 0:
                        status = "running"
                        pid = fg_pid
                        from first_mate.execution.runner import ExecutionHandle
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

        # Status inquiry or conversational check-in
        status_info = await self.get_runner_status()
        blocker = status_info.get("blocker")
        if not blocker and status_info.get("blockers"):
            b_list = status_info["blockers"]
            if b_list and isinstance(b_list, list):
                blocker = b_list[0]

        if blocker and blocker.get("type") == "oauth":
            url = blocker.get("url")
            msg = f"🔐 *Authentication Required:* Runner is waiting for OAuth authorization:\n<{url}|Click here to authenticate>"
            yield StatusUpdateEvent(status="blocked")
            yield TextDeltaEvent(text_delta=msg)
            yield FinishEvent(status="SUCCESS", response=msg)
        elif blocker and blocker.get("type") == "prompt":
            content = blocker.get("content")
            msg = f"⚠️ *Confirmation Required:* Runner is waiting for input:\n> `{content}`"
            yield StatusUpdateEvent(status="blocked")
            yield TextDeltaEvent(text_delta=msg)
            yield FinishEvent(status="SUCCESS", response=msg)
        elif status_info.get("status") == "running" or self.is_executing:
            pid = status_info.get("pid") or getattr(self.active_execution, "pid", None) or "active"
            tail = status_info.get("buffer_tail") or status_info.get("output_tail") or ""
            if is_conversational_checkin(prompt) and not is_status_inquiry(prompt):
                msg = f"⚓ *Liaison Update:* Active execution is underway in Pane 2 (PID {pid}). Still working on your task."
            else:
                msg = f"⚙️ *Runner In Progress (PID {pid}):*\n> {tail or 'Executing command in Pane 2...'}"
            yield StatusUpdateEvent(status="running")
            yield TextDeltaEvent(text_delta=msg)
            yield FinishEvent(status="SUCCESS", response=msg)
        else:
            msg = "⚓ *Status Digest:* Runner pane is currently idle. No active background jobs."
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
            if (self.is_executing or self.lock.locked()) and is_out_of_band_message(prompt):
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
                adapter_fn = getattr(self.adapter, "send_turn", None) or getattr(self.adapter, "stream_turn", None)
                if adapter_fn is None:
                    raise AttributeError(f"Adapter {self.adapter} does not provide 'send_turn' or 'stream_turn'")

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
