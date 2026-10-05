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


@dataclass
class ExecutionHandle:
    """Strongly typed handle representing a runner command execution."""
    status: str  # "completed" | "running" | "error"
    pane: str
    command: str
    pid: Optional[int] = None
    returncode: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    dispatched_at: float = field(default_factory=time.time)
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert handle to dict representation for API and legacy compatibility."""
        return {
            "status": self.status,
            "pane": self.pane,
            "command": self.command,
            "pid": self.pid,
            "returncode": self.returncode,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "dispatched_at": self.dispatched_at,
            "message": self.message,
        }

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)


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
        from golem.execution.buffer import RunnerBufferObserver
        self.buffer_observer = buffer_observer or RunnerBufferObserver(herdr_bin=self.herdr_bin, herdr_client=self.herdr_client)
        from golem.sensory.bus import get_default_event_bus
        self.sensory_bus = sensory_bus or bus or event_bus or (get_default_event_bus() if callable(get_default_event_bus) else None)
        self.webui_server = webui_server
        self._active_observers: Dict[str, Any] = {}
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
        """Determine if target pane is designated as Pane 2 (the runner)."""
        p = (pane or "").lower().strip()
        if not p:
            return False
        if "liaison" in p or p in ("p1", "pane1", "pane_1", "pane-1") or p.endswith(":p1"):
            return False
        if p in ("control", "system", "daemon"):
            return False
        if p in ("runner", "p2", "pane2", "pane_2", "pane-2"):
            return True
        if p.endswith(":p2") or p.endswith("/p2") or p.endswith("-runner") or p.endswith("_runner"):
            return True
        return "runner" in p

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

    async def settle_command(self, pane: str = "runner", returncode: int = 0) -> None:
        """
        Record command settlement, emit structured settlement log marker,
        and cleanly unmount/stop any active EyesTaskObserver for the pane.
        """
        marker = f"[INFO] Golem.Hands: Command settled with exit code {returncode}"
        hands_logger.info(marker)
        logger.info(marker)
        if hasattr(self, "_active_observers") and pane in self._active_observers:
            obs = self._active_observers.pop(pane)
            try:
                if hasattr(obs, "stop_monitoring"):
                    await obs.stop_monitoring()
                elif hasattr(obs, "stop"):
                    await obs.stop()
            except Exception as e:
                logger.debug(f"Error stopping observer for pane '{pane}': {e}")

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

    async def run_command(
        self,
        pane: str = "runner",
        command: str = "",
        yield_timeout: float = 3.0,
        session_id: Optional[str] = None,
        **kwargs: Any,
    ) -> ExecutionHandle:
        """
        Execute command in specified pane with synchronous yield timeout.

        - If command completes within yield_timeout: returns ExecutionHandle with
          status="completed" (or "error" if returncode != 0).
        - If command runs longer than yield_timeout: yields ExecutionHandle with
          status="running", foreground PID, and message, freeing the liaison loop immediately.
        """
        dispatched_at = time.time()
        if not command:
            return ExecutionHandle(
                status="error",
                pane=pane,
                command="",
                returncode=-1,
                stderr="Command string is required.",
                message="Command string is required.",
                dispatched_at=dispatched_at,
            )

        cmd_args = [self.herdr_bin, "pane", "run", pane, command]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            pid = getattr(proc, "pid", None)
            disp_marker = f"[INFO] Golem.Hands: Dispatched command to pane '{pane}' [PID: {pid}]"
            hands_logger.info(disp_marker)
            logger.info(disp_marker)

            try:
                rem_initial = yield_timeout - (time.time() - dispatched_at)
                if rem_initial <= 0:
                    raw_pid = await self._get_pane_foreground_pid(pane, timeout=0.05)
                    final_pid = raw_pid if (raw_pid is not None and raw_pid > 0) else pid
                    yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
                    hands_logger.info(yield_marker)
                    logger.info(yield_marker)
                    if self.is_runner_pane(pane) and pane not in self._active_observers:
                        from golem.sensory.eyes import EyesTaskObserver
                        obs = EyesTaskObserver(
                            sensory_bus=self.sensory_bus,
                            buffer_observer=self.buffer_observer,
                            session_id=session_id or pane,
                            pane=pane,
                            webui_server=self.webui_server,
                        )
                        obs.start_monitoring(session_id=session_id or pane, pane=pane)
                        self._active_observers[pane] = obs
                    return ExecutionHandle(
                        status="running",
                        pane=pane,
                        command=command,
                        pid=final_pid,
                        dispatched_at=dispatched_at,
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
                        pane, command, yield_timeout=yield_timeout, dispatched_at=dispatched_at, session_id=session_id
                    )
                    if resolved_handle:
                        return resolved_handle

                if code != 0:
                    await self.settle_command(pane, returncode=code)
                    return ExecutionHandle(
                        status="error",
                        pane=pane,
                        command=command,
                        returncode=code,
                        stdout=out,
                        stderr=err,
                        pid=pid,
                        dispatched_at=dispatched_at,
                        message=f"Command failed with exit code {code}.",
                    )

                # Command dispatched to pane successfully.
                # Check whether a foreground process is running in the target pane.
                rem_check = yield_timeout - (time.time() - dispatched_at)
                fg_timeout = max(0.05, min(1.0, rem_check))
                fg_pid = await self._get_pane_foreground_pid(pane, timeout=fg_timeout)
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
                            pane,
                            timeout=max(0.05, min(0.5, rem_pid)),
                            last_pid=cur_pid if (cur_pid is not None and cur_pid > 0) else None,
                        )
                        if cur_pid is None:
                            # Foreground process completed within yield_timeout!
                            buffer_text = ""
                            try:
                                buffer_text = await self.buffer_observer.read_buffer(pane, lines=30)
                            except Exception:
                                pass
                            valid_pid = fg_pid if (fg_pid is not None and fg_pid > 0) else pid
                            await self.settle_command(pane, returncode=0)
                            return ExecutionHandle(
                                status="completed",
                                pane=pane,
                                command=command,
                                returncode=0,
                                stdout=buffer_text or out,
                                stderr=err,
                                pid=valid_pid,
                                dispatched_at=dispatched_at,
                                message="Command completed within yield timeout.",
                            )

                    # Still running when yield_timeout expired -> yield running handle
                    final_pid = cur_pid if (cur_pid is not None and cur_pid > 0) else (fg_pid if (fg_pid is not None and fg_pid > 0) else pid)
                    yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
                    hands_logger.info(yield_marker)
                    logger.info(yield_marker)
                    if self.is_runner_pane(pane) and pane not in self._active_observers:
                        from golem.sensory.eyes import EyesTaskObserver
                        obs = EyesTaskObserver(
                            sensory_bus=self.sensory_bus,
                            buffer_observer=self.buffer_observer,
                            session_id=session_id or pane,
                            pane=pane,
                            webui_server=self.webui_server,
                        )
                        obs.start_monitoring(session_id=session_id or pane, pane=pane)
                        self._active_observers[pane] = obs
                    return ExecutionHandle(
                        status="running",
                        pane=pane,
                        command=command,
                        pid=final_pid,
                        dispatched_at=dispatched_at,
                        message="Command running in Pane 2. Liaison loop remains active.",
                    )

                # No foreground process detected (fast command already finished or non-pty runner)
                if not out:
                    try:
                        buffer_text = await self.buffer_observer.read_buffer(pane, lines=30)
                        if buffer_text and "no terminal output recorded" not in buffer_text.lower():
                            out = buffer_text
                    except Exception:
                        pass

                await self.settle_command(pane, returncode=0)
                return ExecutionHandle(
                    status="completed",
                    pane=pane,
                    command=command,
                    returncode=0,
                    stdout=out,
                    stderr=err,
                    pid=pid,
                    dispatched_at=dispatched_at,
                    message="Command completed within yield timeout.",
                )

            except asyncio.TimeoutError:
                # Command exceeded yield_timeout directly -> Yield running handle immediately!
                rem_timeout = max(0.05, min(0.5, yield_timeout - (time.time() - dispatched_at)))
                pid_fg = await self._get_pane_foreground_pid(pane, timeout=rem_timeout)
                if (pid_fg is None or pid_fg <= 0) and hasattr(proc, "pid"):
                    pid_fg = proc.pid
                if pid_fg is not None and pid_fg <= 0:
                    pid_fg = None

                yield_marker = "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
                hands_logger.info(yield_marker)
                logger.info(yield_marker)
                if self.is_runner_pane(pane) and pane not in self._active_observers:
                    from golem.sensory.eyes import EyesTaskObserver
                    obs = EyesTaskObserver(
                        sensory_bus=self.sensory_bus,
                        buffer_observer=self.buffer_observer,
                        session_id=session_id or pane,
                        pane=pane,
                        webui_server=self.webui_server,
                    )
                    obs.start_monitoring(session_id=session_id or pane, pane=pane)
                    self._active_observers[pane] = obs

                return ExecutionHandle(
                    status="running",
                    pane=pane,
                    command=command,
                    pid=pid_fg,
                    dispatched_at=dispatched_at,
                    message="Command running in Pane 2. Liaison loop remains active.",
                )

        except Exception as e:
            logger.error(f"Error executing command in runner pane {pane}: {e}")
            return ExecutionHandle(
                status="error",
                pane=pane,
                command=command,
                returncode=-1,
                stderr=str(e),
                message=f"Execution error: {e}",
                dispatched_at=dispatched_at,
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
                from golem.sensory.eyes import EyesTaskObserver
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
                                    from golem.sensory.eyes import EyesTaskObserver
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
                                            from golem.sensory.eyes import EyesTaskObserver
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
                                    from golem.sensory.eyes import EyesTaskObserver
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
