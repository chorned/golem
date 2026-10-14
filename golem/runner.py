#!/usr/bin/env python3
"""
golem.execution.runner - Runner pane command executor with non-blocking yield timeouts.
Dispatches commands into the runner pane (Pane 2). Commands that exceed the yield timeout (e.g. 3.0s)
yield an ExecutionHandle with status="running", keeping the liaison loop responsive.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import json
import time
import shutil
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional

try:
    from herdr_client import HerdrClient
except ImportError:
    HerdrClient = None  # type: ignore

logger = logging.getLogger("Golem.Execution.Runner")
hands_logger = logging.getLogger("Golem.Hands")

HERDR_BIN_DEFAULT = shutil.which("herdr") or os.path.expanduser("~/.local/bin/herdr")


class TerminalValidationError(RuntimeError):
    """Raised when pre-write validation of a pane's terminal_id fails."""
    pass


class PaneNotFoundError(TerminalValidationError):
    """Raised when target pane does not exist."""
    pass


class TerminalMismatchError(TerminalValidationError):
    """Raised when current terminal_id does not match expected Hand terminal_id."""
    pass


class TaskHandle:
    """
    Strongly typed handle representing a Hand command execution.
    Canonical specification for Stage 3 Herdr Coordination & Outbox Harness.
    Fully backwards-compatible with ExecutionHandle.
    """

    def __init__(
        self,
        status: str = "running",
        pane: Optional[str] = None,
        command: str = "",
        pid: Optional[int] = None,
        returncode: Optional[int] = None,
        stdout: str = "",
        stderr: str = "",
        dispatched_at: Optional[float] = None,
        message: str = "",
        task_id: Optional[str] = None,
        terminal_id: Optional[str] = None,
        opaque_coordinate: Optional[str] = None,
        start_time: Optional[float] = None,
        exit_code: Optional[int] = None,
        output_tail: Optional[str] = None,
        precedent_status: Optional[str] = None,
        precedent_source: Optional[str] = None,
        target_node: Optional[str] = None,
        description: Optional[str] = None,
        linear_issue_id: Optional[str] = None,
        subtask_issue_id: Optional[str] = None,
    ):
        now = time.time()
        self.status = status
        self.command = command
        self.pid = pid
        self.stderr = stderr
        self.message = message
        self.terminal_id = terminal_id
        self.task_id = task_id or f"task_{int(now * 1000)}_{os.urandom(3).hex()}"
        self.precedent_status = precedent_status or ""
        self.precedent_source = precedent_source or ""
        self.target_node = target_node or "localhost"
        self.description = description or command
        self.linear_issue_id = linear_issue_id or subtask_issue_id or ""

        coord = opaque_coordinate or pane or ""
        self._coordinate = coord
        self._pane = pane

        rc = exit_code if exit_code is not None else returncode
        self._exit_code = rc

        t = start_time if start_time is not None else (dispatched_at if dispatched_at is not None else now)
        self._start_time = t

        out = output_tail if output_tail is not None else stdout
        self._output = out

    @property
    def opaque_coordinate(self) -> str:
        return self._coordinate

    @opaque_coordinate.setter
    def opaque_coordinate(self, val: str) -> None:
        self._coordinate = val
        self._pane = val

    @property
    def pane(self) -> str:
        return self._pane if self._pane is not None else self._coordinate

    @pane.setter
    def pane(self, val: str) -> None:
        self._pane = val
        self._coordinate = val

    @property
    def exit_code(self) -> Optional[int]:
        return self._exit_code

    @exit_code.setter
    def exit_code(self, val: Optional[int]) -> None:
        self._exit_code = val

    @property
    def returncode(self) -> Optional[int]:
        return self._exit_code

    @returncode.setter
    def returncode(self, val: Optional[int]) -> None:
        self._exit_code = val

    @property
    def start_time(self) -> float:
        return self._start_time

    @start_time.setter
    def start_time(self, val: float) -> None:
        self._start_time = val

    @property
    def dispatched_at(self) -> float:
        return self._start_time

    @dispatched_at.setter
    def dispatched_at(self, val: float) -> None:
        self._start_time = val

    @property
    def output_tail(self) -> str:
        return self._output

    @output_tail.setter
    def output_tail(self, val: str) -> None:
        self._output = val

    @property
    def stdout(self) -> str:
        return self._output

    @stdout.setter
    def stdout(self, val: str) -> None:
        self._output = val

    @property
    def is_running(self) -> bool:
        return self.status == "running"

    @property
    def is_completed(self) -> bool:
        return self.status == "completed" and (self._exit_code == 0 or self._exit_code is None)

    @property
    def is_failed(self) -> bool:
        return self.status in ("failed", "error") or (self._exit_code is not None and self._exit_code != 0)

    def to_dict(self) -> Dict[str, Any]:
        """Convert handle to dict representation for API and legacy compatibility."""
        return {
            "task_id": self.task_id,
            "terminal_id": self.terminal_id,
            "opaque_coordinate": self.opaque_coordinate,
            "pane": self.pane,
            "command": self.command,
            "status": self.status,
            "pid": self.pid,
            "exit_code": self._exit_code,
            "returncode": self._exit_code,
            "stdout": self._output,
            "output_tail": self._output,
            "stderr": self.stderr,
            "start_time": self._start_time,
            "dispatched_at": self._start_time,
            "message": self.message,
        }

    def __getitem__(self, key: str) -> Any:
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def __repr__(self) -> str:
        return (
            f"TaskHandle(task_id={self.task_id!r}, status={self.status!r}, "
            f"coordinate={self.opaque_coordinate!r}, pid={self.pid}, exit_code={self._exit_code})"
        )


ExecutionHandle = TaskHandle


class RunnerExecutor:
    """
    Executes commands in the dedicated runner pane (Pane 2) without blocking liaison reasoning.
    Uses synchronous yield timeout (default: 3.0s). If execution completes within timeout,
    returns completed handle; otherwise yields running handle and continues in background.
    """

    def __init__(
        self,
        herdr_bin: Optional[str] = None,
        herdr_client: Optional[Any] = None,
        buffer_observer: Optional[Any] = None,
        sensory_bus: Optional[Any] = None,
        bus: Optional[Any] = None,
        event_bus: Optional[Any] = None,
        webui_server: Optional[Any] = None,
    ):
        self.herdr_bin = herdr_bin or shutil.which("herdr") or HERDR_BIN_DEFAULT
        self.herdr_client = herdr_client or (HerdrClient(herdr_bin=self.herdr_bin) if HerdrClient is not None else None)
        from golem.buffer import RunnerBufferObserver
        self.buffer_observer = buffer_observer or RunnerBufferObserver(herdr_bin=self.herdr_bin, herdr_client=self.herdr_client)
        from golem.bus import get_default_event_bus
        self.sensory_bus = sensory_bus or bus or event_bus or (get_default_event_bus() if callable(get_default_event_bus) else None)
        self.webui_server = webui_server
        self._active_observers: Dict[str, Any] = {}
        self._active_handles: Dict[str, Any] = {}
        if hasattr(self.sensory_bus, "subscribe"):
            try:
                self._bus_unsubscribe = self.sensory_bus.subscribe(
                    self._on_sensory_alert, alert_types=["completion"]
                )
            except Exception as e:
                logger.debug(f"Could not subscribe to sensory bus: {e}")
                self._bus_unsubscribe = None
        else:
            self._bus_unsubscribe = None

    def is_runner_pane(self, pane: str) -> bool:
        """
        Determine if target pane is designated as a Hand/worker execution pane.
        Discriminates Hand panes (Pane 2+, opaque coordinates w<ws>:p2+) from Pane 1 (liaison).
        """
        p = (pane or "").lower().strip()
        if not p:
            return False
        if "liaison" in p or p in ("p1", "pane1", "pane_1", "pane-1") or p.endswith(":p1"):
            return False
        if p in ("control", "system", "daemon"):
            return False
        # Opaque physical coordinate match: w<id>:p2, w<id>:p3, etc.
        if re.match(r"^w[a-zA-Z0-9_-]+:p([2-9]|[1-9]\d+)$", p):
            return True
        if p in ("runner", "p2", "pane2", "pane_2", "pane-2", "hand", "worker"):
            return True
        if p.endswith(":p2") or p.endswith("/p2") or p.endswith("-runner") or p.endswith("_runner") or p.endswith("-hand"):
            return True
        return "runner" in p or "hand" in p

    async def _on_sensory_alert(self, alert: Any) -> None:
        """Handle completion alerts from ShoulderTapEventBus to settle active panes."""
        try:
            alert_type = getattr(alert, "alert_type", "")
            if alert_type == "completion":
                data = getattr(alert, "data", {}) or {}
                pane = data.get("pane") or "runner"
                is_err = data.get("is_error", False)
                returncode = 1 if is_err else 0
                await self.settle_command(pane, returncode=returncode)
        except Exception as e:
            logger.debug(f"Error handling sensory alert in RunnerExecutor: {e}")

    async def settle_command(
        self,
        pane: str = "runner",
        returncode: int = 0,
        task_handle: Optional[Any] = None,
    ) -> None:
        """
        Record command settlement, emit structured settlement log marker,
        cleanly unmount/stop any active EyesTaskObserver for the pane,
        and trigger non-blocking post-flight reflection to logbook.
        """
        marker = f"[INFO] Golem.Hands: Command settled with exit code {returncode}"
        hands_logger.info(marker)
        logger.info(marker)
        if hasattr(self, "_active_observers"):
            for p in {pane, "runner"}:
                if p in self._active_observers:
                    obs = self._active_observers.pop(p)
                    try:
                        if hasattr(obs, "stop_monitoring"):
                            await obs.stop_monitoring()
                        elif hasattr(obs, "stop"):
                            await obs.stop()
                    except Exception as e:
                        logger.debug(f"Error stopping observer for pane '{p}': {e}")

        # Asynchronous Post-Flight Settlement Reflection
        try:
            from golem.memory.reflection import record_task_settlement_async
            th = task_handle or (getattr(self, "_active_handles", {}).get(pane) if hasattr(self, "_active_handles") else None)
            t_id = getattr(th, "task_id", f"task_{pane}_{int(time.time())}") if th else f"task_{pane}_{int(time.time())}"
            t_desc = getattr(th, "description", getattr(th, "command", "")) if th else ""
            t_node = getattr(th, "target_node", "localhost") if th else "localhost"
            t_out = (getattr(th, "stdout", "") or getattr(th, "output_tail", "")) if th else ""
            p_source = getattr(th, "precedent_source", "") if th else ""
            p_status = getattr(th, "precedent_status", "") if th else ""
            asyncio.create_task(
                record_task_settlement_async(
                    task_id=t_id,
                    task_description=t_desc,
                    target_node=t_node,
                    exit_code=returncode,
                    output_tail=t_out,
                    precedent_source=p_source,
                    precedent_status=p_status,
                )
            )
        except Exception as e:
            logger.debug(f"Failed to spawn reflection task in settle_command: {e}")

        # Linear Subtask Settlement & Parent Evaluation
        try:
            th = task_handle or (getattr(self, "_active_handles", {}).get(pane) if hasattr(self, "_active_handles") else None)
            subtask_id = getattr(th, "linear_issue_id", None) if th else None
            if subtask_id:
                from golem.linear_bridge import LinearBridge
                linear_bridge = LinearBridge()
                coord = getattr(th, "opaque_coordinate", pane)
                t_id = getattr(th, "task_id", f"task_{int(time.time())}")
                out_str = (getattr(th, "stdout", "") or getattr(th, "output_tail", "") or "")
                tail_out = out_str[-800:] if out_str else "(empty)"
                receipt = (
                    f"### Subtask Hand Settled (`{coord}`)\n"
                    f"- **Exit Code:** `{returncode}`\n"
                    f"- **Task ID:** `{t_id}`\n"
                    f"- **Outbox:** `~/.golem/outbox/{t_id}/`\n\n"
                    f"**Tail Output:**\n```\n{tail_out}\n```"
                )

                async def _settle_and_check():
                    try:
                        res = await linear_bridge.settle_subtask_and_evaluate_parent(
                            subtask_id=subtask_id,
                            settlement_receipt=receipt,
                        )
                        if res.get("parent_settled"):
                            logger.info(f"All subtasks finished. Parent {res.get('parent_key')} marked Claimed Fixed.")
                    except Exception as ex:
                        logger.debug(f"Linear subtask settlement failed in runner: {ex}")

                asyncio.create_task(_settle_and_check())
        except Exception as e:
            logger.debug(f"Failed to initiate linear settlement in settle_command: {e}")

    async def settle_pane(self, pane: str = "runner", returncode: int = 0) -> None:
        """Alias for settle_command."""
        await self.settle_command(pane, returncode=returncode)

    def close(self) -> None:
        """Unsubscribe from event bus and clean up resources."""
        if hasattr(self, "_bus_unsubscribe") and callable(self._bus_unsubscribe):
            try:
                self._bus_unsubscribe()
            except Exception:
                pass
            self._bus_unsubscribe = None

    async def verify_terminal_id(
        self,
        pane: str,
        expected_terminal_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Pre-write terminal_id validation:
        Queries `herdr pane get <pane>` to verify:
        1. Pane exists and is actively running.
        2. Underlying PTY has an active terminal_id.
        3. Matches expected_terminal_id if provided (preventing writes to stale/recycled PTYs).
        """
        if not self.herdr_bin or not os.path.exists(self.herdr_bin):
            return None  # Skip when Herdr CLI binary unavailable (mock/unit test mode)

        try:
            proc = await asyncio.create_subprocess_exec(
                self.herdr_bin, "pane", "get", str(pane),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=1.5)
            if proc.returncode != 0:
                err_msg = err_b.decode("utf-8", errors="replace").strip()
                raise PaneNotFoundError(f"Pane '{pane}' not found in Herdr mesh: {err_msg}")

            data = json.loads(out_b.decode("utf-8", errors="replace"))
            pane_obj = (data.get("result", {}) or {}).get("pane") or data.get("pane") or {}
            current_terminal_id = pane_obj.get("terminal_id")

            if not current_terminal_id:
                raise TerminalValidationError(f"Pane '{pane}' has no active PTY terminal_id.")

            if expected_terminal_id and current_terminal_id != expected_terminal_id:
                raise TerminalMismatchError(
                    f"Pane '{pane}' terminal_id mismatch: expected '{expected_terminal_id}', "
                    f"got '{current_terminal_id}'. Aborting write to prevent PTY corruption."
                )
            return current_terminal_id
        except (PaneNotFoundError, TerminalValidationError, TerminalMismatchError):
            raise
        except Exception as e:
            logger.debug(f"Pre-write terminal verification skipped due to error: {e}")
            return None

    async def run_command(
        self,
        pane: Optional[str] = "runner",
        command: str = "",
        yield_timeout: float = 3.0,
        session_id: Optional[str] = None,
        opaque_coordinate: Optional[str] = None,
        terminal_id: Optional[str] = None,
        expected_terminal_id: Optional[str] = None,
        task_id: Optional[str] = None,
        **kwargs: Any,
    ) -> TaskHandle:
        """
        Execute command in specified Hand pane with synchronous yield timeout.
        Uses authoritative opaque coordinates, pre-write terminal validation,
        and strict 3.0s non-blocking yield window.

        - If command completes within yield_timeout: returns TaskHandle with
          status="completed" (or "error" if returncode != 0).
        - If command runs longer than yield_timeout: yields TaskHandle with
          status="running", foreground PID, and message, freeing the liaison loop immediately.
        """
        dispatched_at = time.time()
        effective_pane = pane or "runner"
        target_pane = opaque_coordinate or effective_pane
        if target_pane == "runner":
            logger.warning("[DEPRECATION] 'runner' literal is deprecated for pane addressing; prefer opaque coordinate.")

        effective_expected_tid = expected_terminal_id or terminal_id
        if effective_expected_tid:
            await self.verify_terminal_id(str(target_pane), effective_expected_tid)

        if not command:
            return TaskHandle(
                status="error",
                pane=str(effective_pane),
                opaque_coordinate=str(target_pane),
                command="",
                returncode=-1,
                exit_code=-1,
                stderr="Command string is required.",
                message="Command string is required.",
                dispatched_at=dispatched_at,
                start_time=dispatched_at,
                task_id=task_id,
                terminal_id=terminal_id,
            )

        cmd_args = [self.herdr_bin, "pane", "run", str(target_pane), command]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            pid = getattr(proc, "pid", None)
            sub_id = kwargs.get("subtask_issue_id") or kwargs.get("linear_issue_id")
            provisional_handle = TaskHandle(
                status="running",
                pane=str(effective_pane),
                opaque_coordinate=str(target_pane),
                command=command,
                pid=pid,
                dispatched_at=dispatched_at,
                start_time=dispatched_at,
                task_id=task_id,
                terminal_id=terminal_id,
                linear_issue_id=sub_id,
                message="Command dispatched to pane.",
            )
            self._active_handles[str(target_pane)] = provisional_handle
            self._active_handles[str(effective_pane)] = provisional_handle
            self._active_handles["runner"] = provisional_handle

            disp_marker = f"[INFO] Golem.Hands: Dispatched command to pane '{effective_pane}' [PID: {pid}]"
            hands_logger.info(disp_marker)
            logger.info(disp_marker)

            try:
                rem_initial = yield_timeout - (time.time() - dispatched_at)
                if rem_initial <= 0:
                    raw_pid = await self._get_pane_foreground_pid(str(target_pane), timeout=0.05)
                    final_pid = raw_pid if (raw_pid is not None and raw_pid > 0) else pid
                    yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
                    hands_logger.info(yield_marker)
                    logger.info(yield_marker)
                    obs_target = str(effective_pane)
                    if (self.is_runner_pane(obs_target) or self.is_runner_pane(str(target_pane))) and obs_target not in self._active_observers:
                        from golem.eyes import EyesTaskObserver
                        obs = EyesTaskObserver(
                            sensory_bus=self.sensory_bus,
                            buffer_observer=self.buffer_observer,
                            session_id=session_id or obs_target,
                            pane=obs_target,
                            webui_server=self.webui_server,
                        )
                        obs.start_monitoring(session_id=session_id or obs_target, pane=obs_target)
                        self._active_observers[obs_target] = obs
                        if str(target_pane) != obs_target:
                            self._active_observers[str(target_pane)] = obs
                    return TaskHandle(
                        status="running",
                        pane=str(effective_pane),
                        opaque_coordinate=str(target_pane),
                        command=command,
                        pid=final_pid,
                        dispatched_at=dispatched_at,
                        start_time=dispatched_at,
                        task_id=task_id,
                        terminal_id=terminal_id,
                        message="Command running in Pane 2. Liaison loop remains active.",
                    )

                stdout_bytes, stderr_bytes = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=max(0.05, rem_initial),
                )
                code = proc.returncode if proc.returncode is not None else 0
                out = stdout_bytes.decode("utf-8", errors="replace").strip()
                err = stderr_bytes.decode("utf-8", errors="replace").strip()

                # If pane not found, attempt label resolution
                if code != 0 and ("pane_not_found" in err or "not found" in err.lower()):
                    resolved_handle = await self._try_resolve_and_run(
                        str(target_pane), command, yield_timeout=yield_timeout, dispatched_at=dispatched_at, session_id=session_id
                    )
                    if resolved_handle:
                        return resolved_handle

                if code != 0:
                    await self.settle_command(str(target_pane), returncode=code)
                    return TaskHandle(
                        status="error",
                        pane=str(effective_pane),
                        opaque_coordinate=str(target_pane),
                        command=command,
                        returncode=code,
                        exit_code=code,
                        stdout=out,
                        output_tail=out,
                        stderr=err,
                        pid=pid,
                        dispatched_at=dispatched_at,
                        start_time=dispatched_at,
                        task_id=task_id,
                        terminal_id=terminal_id,
                        message=f"Command failed with exit code {code}.",
                    )

                # Command dispatched to pane successfully.
                # Check whether a foreground process is running in the target pane.
                rem_check = yield_timeout - (time.time() - dispatched_at)
                fg_timeout = max(0.05, min(1.0, rem_check))
                fg_pid = await self._get_pane_foreground_pid(str(target_pane), timeout=fg_timeout)
                if fg_pid is not None:
                    # Poll in a loop up to yield_timeout
                    poll_interval = 0.05
                    cur_pid = fg_pid
                    while time.time() - dispatched_at < yield_timeout:
                        rem_sleep = yield_timeout - (time.time() - dispatched_at)
                        if rem_sleep <= 0:
                            break
                        await asyncio.sleep(min(poll_interval, rem_sleep))

                        rem_pid = yield_timeout - (time.time() - dispatched_at)
                        if rem_pid <= 0:
                            break
                        cur_pid = await self._get_pane_foreground_pid(
                            str(target_pane),
                            timeout=max(0.05, min(0.5, rem_pid)),
                            last_pid=cur_pid if (cur_pid is not None and cur_pid > 0) else None,
                        )
                        if cur_pid is None:
                            # Foreground process completed within yield_timeout!
                            buffer_text = ""
                            try:
                                buffer_text = await self.buffer_observer.read_buffer(str(target_pane), lines=30)
                            except Exception:
                                pass
                            valid_pid = fg_pid if (fg_pid is not None and fg_pid > 0) else pid
                            await self.settle_command(str(target_pane), returncode=0)
                            return TaskHandle(
                                status="completed",
                                pane=str(effective_pane),
                                opaque_coordinate=str(target_pane),
                                command=command,
                                returncode=0,
                                exit_code=0,
                                stdout=buffer_text or out,
                                output_tail=buffer_text or out,
                                stderr=err,
                                pid=valid_pid,
                                dispatched_at=dispatched_at,
                                start_time=dispatched_at,
                                task_id=task_id,
                                terminal_id=terminal_id,
                                message="Command completed within yield timeout.",
                            )

                    # Still running when yield_timeout expired -> yield running handle
                    final_pid = cur_pid if (cur_pid is not None and cur_pid > 0) else (fg_pid if (fg_pid is not None and fg_pid > 0) else pid)
                    yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
                    hands_logger.info(yield_marker)
                    logger.info(yield_marker)
                    obs_target = str(effective_pane)
                    if (self.is_runner_pane(obs_target) or self.is_runner_pane(str(target_pane))) and obs_target not in self._active_observers:
                        from golem.eyes import EyesTaskObserver
                        obs = EyesTaskObserver(
                            sensory_bus=self.sensory_bus,
                            buffer_observer=self.buffer_observer,
                            session_id=session_id or obs_target,
                            pane=obs_target,
                            webui_server=self.webui_server,
                        )
                        obs.start_monitoring(session_id=session_id or obs_target, pane=obs_target)
                        self._active_observers[obs_target] = obs
                        if str(target_pane) != obs_target:
                            self._active_observers[str(target_pane)] = obs
                    return TaskHandle(
                        status="running",
                        pane=str(effective_pane),
                        opaque_coordinate=str(target_pane),
                        command=command,
                        pid=final_pid,
                        dispatched_at=dispatched_at,
                        start_time=dispatched_at,
                        task_id=task_id,
                        terminal_id=terminal_id,
                        message="Command running in Pane 2. Liaison loop remains active.",
                    )

                # No foreground process detected (fast command already finished or non-pty runner)
                if not out:
                    try:
                        buffer_text = await self.buffer_observer.read_buffer(str(target_pane), lines=30)
                        if buffer_text and "no terminal output recorded" not in buffer_text.lower():
                            out = buffer_text
                    except Exception:
                        pass

                await self.settle_command(str(target_pane), returncode=0)
                return TaskHandle(
                    status="completed",
                    pane=str(effective_pane),
                    opaque_coordinate=str(target_pane),
                    command=command,
                    returncode=0,
                    exit_code=0,
                    stdout=out,
                    output_tail=out,
                    stderr=err,
                    pid=pid,
                    dispatched_at=dispatched_at,
                    start_time=dispatched_at,
                    task_id=task_id,
                    terminal_id=terminal_id,
                    message="Command completed within yield timeout.",
                )

            except asyncio.TimeoutError:
                # Command exceeded yield_timeout directly -> Yield running handle immediately!
                rem_timeout = max(0.05, min(0.5, yield_timeout - (time.time() - dispatched_at)))
                pid_fg = await self._get_pane_foreground_pid(str(target_pane), timeout=rem_timeout)
                if (pid_fg is None or pid_fg <= 0) and hasattr(proc, "pid"):
                    pid_fg = proc.pid
                if pid_fg is not None and pid_fg <= 0:
                    pid_fg = None

                yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
                hands_logger.info(yield_marker)
                logger.info(yield_marker)
                obs_target = str(effective_pane)
                if (self.is_runner_pane(obs_target) or self.is_runner_pane(str(target_pane))) and obs_target not in self._active_observers:
                    from golem.eyes import EyesTaskObserver
                    obs = EyesTaskObserver(
                        sensory_bus=self.sensory_bus,
                        buffer_observer=self.buffer_observer,
                        session_id=session_id or obs_target,
                        pane=obs_target,
                        webui_server=self.webui_server,
                    )
                    obs.start_monitoring(session_id=session_id or obs_target, pane=obs_target)
                    self._active_observers[obs_target] = obs
                    if str(target_pane) != obs_target:
                        self._active_observers[str(target_pane)] = obs

                return TaskHandle(
                    status="running",
                    pane=str(effective_pane),
                    opaque_coordinate=str(target_pane),
                    command=command,
                    pid=pid_fg,
                    dispatched_at=dispatched_at,
                    start_time=dispatched_at,
                    task_id=task_id,
                    terminal_id=terminal_id,
                    message="Command running in Pane 2. Liaison loop remains active.",
                )

        except Exception as e:
            logger.error(f"Error executing command in runner pane {target_pane}: {e}")
            return TaskHandle(
                status="error",
                pane=str(effective_pane),
                opaque_coordinate=str(target_pane),
                command=command,
                returncode=-1,
                exit_code=-1,
                stderr=str(e),
                message=f"Execution error: {e}",
                dispatched_at=dispatched_at,
                start_time=dispatched_at,
                task_id=task_id,
                terminal_id=terminal_id,
            )

    async def _try_resolve_and_run(
        self,
        pane_label: str,
        command: str,
        yield_timeout: float,
        dispatched_at: Optional[float] = None,
        session_id: Optional[str] = None,
    ) -> Optional[ExecutionHandle]:
        """Attempt to resolve a pane label to an active pane ID from herdr pane list."""
        if dispatched_at is None:
            dispatched_at = time.time()

        rem_start = yield_timeout - (time.time() - dispatched_at)
        if rem_start <= 0:
            yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
            hands_logger.info(yield_marker)
            logger.info(yield_marker)
            if self.is_runner_pane(pane_label) and pane_label not in self._active_observers:
                from golem.eyes import EyesTaskObserver
                obs = EyesTaskObserver(
                    sensory_bus=self.sensory_bus,
                    buffer_observer=self.buffer_observer,
                    session_id=session_id or pane_label,
                    pane=pane_label,
                    webui_server=self.webui_server,
                )
                obs.start_monitoring(session_id=session_id or pane_label, pane=pane_label)
                self._active_observers[pane_label] = obs
            return ExecutionHandle(
                status="running",
                pane=pane_label,
                command=command,
                pid=None,
                dispatched_at=dispatched_at,
                message="Command running in Pane 2. Liaison loop remains active."
            )

        try:
            list_proc = await asyncio.create_subprocess_exec(
                self.herdr_bin, "pane", "list",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            list_timeout = max(0.05, min(1.0, rem_start))
            try:
                l_out, _ = await asyncio.wait_for(list_proc.communicate(), timeout=list_timeout)
            except asyncio.TimeoutError:
                try:
                    list_proc.kill()
                    await list_proc.wait()
                except Exception:
                    pass
                return None

            if list_proc.returncode == 0 and l_out:
                data = json.loads(l_out.decode("utf-8", errors="replace"))
                panes_list = (data.get("result", {}) or {}).get("panes") or data.get("panes", [])
                for p_info in panes_list:
                    if p_info.get("label") == pane_label or p_info.get("name") == pane_label:
                        resolved_id = p_info.get("pane_id")
                        if resolved_id:
                            # Re-run with resolved pane_id
                            rem_rerun = yield_timeout - (time.time() - dispatched_at)
                            if rem_rerun <= 0:
                                yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
                                hands_logger.info(yield_marker)
                                logger.info(yield_marker)
                                if self.is_runner_pane(resolved_id) and resolved_id not in self._active_observers:
                                    from golem.eyes import EyesTaskObserver
                                    obs = EyesTaskObserver(
                                        sensory_bus=self.sensory_bus,
                                        buffer_observer=self.buffer_observer,
                                        session_id=session_id or resolved_id,
                                        pane=resolved_id,
                                        webui_server=self.webui_server,
                                    )
                                    obs.start_monitoring(session_id=session_id or resolved_id, pane=resolved_id)
                                    self._active_observers[resolved_id] = obs
                                return ExecutionHandle(
                                    status="running",
                                    pane=resolved_id,
                                    command=command,
                                    pid=None,
                                    dispatched_at=dispatched_at,
                                    message="Command running in Pane 2. Liaison loop remains active."
                                )

                            r_proc = await asyncio.create_subprocess_exec(
                                self.herdr_bin, "pane", "run", resolved_id, command,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            r_pid = getattr(r_proc, "pid", None)
                            disp_marker = f"[INFO] Golem.Hands: Dispatched command to pane '{resolved_id}' [PID: {r_pid}]"
                            hands_logger.info(disp_marker)
                            logger.info(disp_marker)

                            try:
                                r_out_b, r_err_b = await asyncio.wait_for(
                                    r_proc.communicate(),
                                    timeout=max(0.05, rem_rerun)
                                )
                                r_code = r_proc.returncode if r_proc.returncode is not None else 0
                                out = r_out_b.decode("utf-8", errors="replace").strip()
                                err = r_err_b.decode("utf-8", errors="replace").strip()

                                if r_code == 0:
                                    rem_pid = yield_timeout - (time.time() - dispatched_at)
                                    fg_pid = await self._get_pane_foreground_pid(
                                        resolved_id,
                                        timeout=max(0.05, min(0.5, rem_pid))
                                    )
                                    if fg_pid is not None:
                                        cur_pid = fg_pid
                                        while time.time() - dispatched_at < yield_timeout:
                                            rem_loop = yield_timeout - (time.time() - dispatched_at)
                                            if rem_loop <= 0:
                                                break
                                            await asyncio.sleep(min(0.05, rem_loop))
                                            rem_loop = yield_timeout - (time.time() - dispatched_at)
                                            if rem_loop <= 0:
                                                break
                                            cur_pid = await self._get_pane_foreground_pid(
                                                resolved_id,
                                                timeout=max(0.05, min(0.5, rem_loop)),
                                                last_pid=cur_pid if (cur_pid is not None and cur_pid > 0) else None,
                                            )
                                            if cur_pid is None:
                                                buf_out = ""
                                                try:
                                                    buf_out = await self.buffer_observer.read_buffer(resolved_id, lines=30)
                                                except Exception:
                                                    pass
                                                valid_pid = fg_pid if (fg_pid is not None and fg_pid > 0) else getattr(r_proc, "pid", None)
                                                await self.settle_command(resolved_id, returncode=0)
                                                return ExecutionHandle(
                                                    status="completed",
                                                    pane=resolved_id,
                                                    command=command,
                                                    returncode=0,
                                                    stdout=buf_out or out,
                                                    stderr=err,
                                                    pid=valid_pid,
                                                    dispatched_at=dispatched_at,
                                                    message="Resolved pane and completed command."
                                                )
                                        final_pid = cur_pid if (cur_pid is not None and cur_pid > 0) else (fg_pid if (fg_pid is not None and fg_pid > 0) else getattr(r_proc, "pid", None))
                                        yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
                                        hands_logger.info(yield_marker)
                                        logger.info(yield_marker)
                                        if self.is_runner_pane(resolved_id) and resolved_id not in self._active_observers:
                                            from golem.eyes import EyesTaskObserver
                                            obs = EyesTaskObserver(
                                                sensory_bus=self.sensory_bus,
                                                buffer_observer=self.buffer_observer,
                                                session_id=session_id or resolved_id,
                                                pane=resolved_id,
                                                webui_server=self.webui_server,
                                            )
                                            obs.start_monitoring(session_id=session_id or resolved_id, pane=resolved_id)
                                            self._active_observers[resolved_id] = obs
                                        return ExecutionHandle(
                                            status="running",
                                            pane=resolved_id,
                                            command=command,
                                            pid=final_pid,
                                            dispatched_at=dispatched_at,
                                            message="Command running in Pane 2. Liaison loop remains active."
                                        )

                                await self.settle_command(resolved_id, returncode=r_code)
                                return ExecutionHandle(
                                    status="completed" if r_code == 0 else "error",
                                    pane=resolved_id,
                                    command=command,
                                    returncode=r_code,
                                    stdout=out,
                                    stderr=err,
                                    pid=getattr(r_proc, "pid", None),
                                    dispatched_at=dispatched_at,
                                    message="Resolved pane and completed command."
                                )
                            except asyncio.TimeoutError:
                                rem_to = yield_timeout - (time.time() - dispatched_at)
                                pid = await self._get_pane_foreground_pid(
                                    resolved_id,
                                    timeout=max(0.05, min(0.5, rem_to))
                                )
                                if (pid is None or pid <= 0) and hasattr(r_proc, "pid"):
                                    pid = r_proc.pid
                                if pid is not None and pid <= 0:
                                    pid = None
                                yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
                                hands_logger.info(yield_marker)
                                logger.info(yield_marker)
                                if self.is_runner_pane(resolved_id) and resolved_id not in self._active_observers:
                                    from golem.eyes import EyesTaskObserver
                                    obs = EyesTaskObserver(
                                        sensory_bus=self.sensory_bus,
                                        buffer_observer=self.buffer_observer,
                                        session_id=session_id or resolved_id,
                                        pane=resolved_id,
                                        webui_server=self.webui_server,
                                    )
                                    obs.start_monitoring(session_id=session_id or resolved_id, pane=resolved_id)
                                    self._active_observers[resolved_id] = obs
                                return ExecutionHandle(
                                    status="running",
                                    pane=resolved_id,
                                    command=command,
                                    pid=pid,
                                    dispatched_at=dispatched_at,
                                    message="Command running in Pane 2. Liaison loop remains active."
                                )
        except Exception as e:
            logger.debug(f"Could not resolve pane label {pane_label}: {e}")
        return None

    async def _get_pane_foreground_pid(
        self,
        pane: str,
        timeout: float = 1.0,
        last_pid: Optional[int] = None
    ) -> Optional[int]:
        """Query foreground PID running in the target pane, bounded by timeout."""
        effective_timeout = max(0.05, timeout)
        try:
            proc = await asyncio.create_subprocess_exec(
                self.herdr_bin, "pane", "process-info", "--pane", pane,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            try:
                out_b, _ = await asyncio.wait_for(proc.communicate(), timeout=effective_timeout)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
                if last_pid is not None:
                    return last_pid
                return -1

            if proc.returncode == 0 and out_b:
                data = json.loads(out_b.decode("utf-8", errors="replace"))
                res = data.get("result", {}) if isinstance(data, dict) else {}

                # Check process_info structure from live Herdr
                pinfo = res.get("process_info") or (data.get("process_info") if isinstance(data, dict) else None)
                if isinstance(pinfo, dict):
                    shell_pid = pinfo.get("shell_pid")
                    fg_pgrp = pinfo.get("foreground_process_group_id")
                    fg_procs = pinfo.get("foreground_processes") or []

                    # Look for non-shell processes running in foreground
                    for p in fg_procs:
                        if isinstance(p, dict):
                            p_pid = p.get("pid")
                            p_name = (p.get("name") or "").lower()
                            if shell_pid is not None and p_pid is not None and p_pid != shell_pid:
                                return int(p_pid)
                            if shell_pid is None and p_name not in ("zsh", "bash", "sh", "fish", "csh", "tcsh", "-zsh", "-bash") and p_pid:
                                return int(p_pid)

                    # If foreground process group differs from shell_pid, a command is running
                    if fg_pgrp is not None and shell_pid is not None and fg_pgrp != shell_pid:
                        return int(fg_pgrp)

                    # If only shell is in foreground_processes and pgrp == shell_pid, no command is active
                    if shell_pid is not None and fg_pgrp == shell_pid:
                        return None

                # Check direct pid / foreground_pid keys (common in mocks and simple responses)
                direct_pid = (
                    res.get("pid")
                    or res.get("foreground_pid")
                    or (data.get("pid") if isinstance(data, dict) else None)
                    or (data.get("foreground_pid") if isinstance(data, dict) else None)
                )
                if direct_pid is not None:
                    return int(direct_pid)
        except Exception:
            pass
        return None
