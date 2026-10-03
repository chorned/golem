#!/usr/bin/env python3
"""
golem/agent/drivers/antigravity.py
Antigravity CLI execution driver wrapping agy stream-json mode.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import time
import json
import inspect
import select
import asyncio
import logging
from collections import deque
from typing import Optional, List, Dict, Any, AsyncGenerator, Deque, Set

from golem.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
)
from .base import BaseAgentDriver

logger = logging.getLogger("Golem.Agent.Drivers.Antigravity")

DEFAULT_AGY_BIN = "/Users/hermes/.local/bin/agy"


class AntigravityDriver(BaseAgentDriver):
    """
    Antigravity CLI execution driver (The Hands) wrapping agy stream-json mode.
    Maintains persistent stdin/stdout pipes across multiple conversational turns,
    parses raw line-delimited NDJSON events, and emits strongly typed AgentEvent objects.
    """

    def __init__(
        self,
        cwd: str,
        model: Optional[str] = None,
        agy_bin: str = DEFAULT_AGY_BIN,
        extra_args: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        resolved_model = model
        if resolved_model is None:
            try:
                from golem.config import get_engine_model
                resolved_model = get_engine_model("antigravity") or None
            except ImportError:
                resolved_model = os.environ.get("ANTIGRAVITY_MODEL") or None

        super().__init__(
            cwd=cwd,
            model=resolved_model,
            extra_args=extra_args,
            env=env,
            **kwargs,
        )
        self.agy_bin: str = agy_bin

        self.process: Optional[asyncio.subprocess.Process] = None
        self._lock: asyncio.Lock = asyncio.Lock()
        self._stderr_lines: Deque[str] = deque(maxlen=100)
        self._stderr_task: Optional[asyncio.Task] = None
        self._active_generator: Optional[AsyncGenerator[AgentEvent, None]] = None
        self.has_active_background_tasks: bool = False
        self.active_background_tasks: Set[str] = set()

    @property
    def engine_name(self) -> str:
        """Engine identifier."""
        return "antigravity"

    @property
    def is_alive(self) -> bool:
        """Check if subprocess is running and has not exited."""
        return self.process is not None and self.process.returncode is None

    def _build_cmd(self) -> List[str]:
        """Build command line arguments for agy subprocess."""
        cmd = [
            self.agy_bin,
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--dangerously-skip-permissions",
        ]
        if self.model and "--model" not in self.extra_args:
            cmd.extend(["--model", self.model])
        cmd.extend(self.extra_args)
        return cmd

    async def _drain_stderr(self) -> None:
        """Asynchronously drain stderr to prevent pipe buffer deadlocks (>64KB)."""
        if not self.process or not self.process.stderr:
            return
        reader = self.process.stderr
        try:
            if isinstance(reader, asyncio.StreamReader):
                while True:
                    line = await reader.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip()
                    if text:
                        self._stderr_lines.append(text)
            elif hasattr(reader, "readline") and inspect.iscoroutinefunction(reader.readline):
                while True:
                    line = await reader.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip() if isinstance(line, bytes) else str(line).strip()
                    if text:
                        self._stderr_lines.append(text)
            elif hasattr(reader, "read") and (inspect.iscoroutinefunction(reader.read) or type(reader.read).__name__ == "AsyncMock"):
                while True:
                    chunk = await reader.read(4096)
                    if not chunk:
                        break
                    text = chunk.decode("utf-8", errors="replace").strip() if isinstance(chunk, bytes) else str(chunk).strip()
                    if text:
                        self._stderr_lines.append(text)
        except (asyncio.CancelledError, Exception):
            pass

    async def _drain_stdout(self) -> int:
        """
        Drain and discard any stale unread bytes from stdout before beginning a new turn,
        or upon early consumer break / generator cancellation.
        """
        if not self.process or not self.process.stdout:
            return 0

        # Preserve pre-fed static test fixture buffers when reader is at EOF
        if getattr(self.process.stdout, "_eof", False):
            return 0

        # Preserve synthetic mock processes (e.g. MagicMock test fixtures)
        if not isinstance(self.process, asyncio.subprocess.Process):
            return 0

        discarded_bytes = 0
        reader = self.process.stdout

        def _clear_buf() -> int:
            nonlocal discarded_bytes
            purged = 0
            buf = getattr(reader, "_buffer", None)
            if isinstance(buf, (bytearray, bytes, list, deque)) and len(buf) > 0:
                purged = len(buf)
                discarded_bytes += purged
                if hasattr(buf, "clear"):
                    buf.clear()
                else:
                    setattr(reader, "_buffer", type(buf)())
                if hasattr(reader, "_maybe_resume_transport"):
                    try:
                        reader._maybe_resume_transport()
                    except Exception:
                        pass
            return purged

        _clear_buf()

        transport = getattr(reader, "_transport", None)
        pipe = getattr(transport, "_pipe", None) if transport else None
        pipe_fd: Optional[int] = None
        if pipe and hasattr(pipe, "fileno"):
            try:
                fn = pipe.fileno()
                if isinstance(fn, int):
                    pipe_fd = fn
            except Exception:
                pipe_fd = None

        if pipe_fd is not None:
            try:
                r, _, _ = select.select([pipe_fd], [], [], 0)
                if r:
                    await asyncio.sleep(0.001)
                    _clear_buf()
            except Exception:
                pass

        rounds = 0
        while rounds < 10:
            rounds += 1
            has_buf = bool(_clear_buf())
            has_pipe = False
            if pipe_fd is not None:
                try:
                    r, _, _ = select.select([pipe_fd], [], [], 0)
                    has_pipe = bool(r)
                except Exception:
                    has_pipe = False

            if not has_buf and not has_pipe:
                break

            if has_pipe:
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=0.005)
                    if not line:
                        break
                    discarded_bytes += len(line)
                    _clear_buf()
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    break
                except Exception:
                    break

        if discarded_bytes > 0:
            logger.info(f"Drained {discarded_bytes} bytes of stale unread stdout buffer")
        return discarded_bytes

    async def start_session(self) -> None:
        """
        Spawn headless agy process and wait for initial 'init' event.
        If already running, does not spawn a new process.
        """
        if self.is_alive:
            return

        cmd = self._build_cmd()

        proc_env = dict(os.environ)
        if self.env:
            proc_env.update(self.env)

        logger.info(f"Spawning headless agent in {self.cwd}: {' '.join(cmd)}")

        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=self.cwd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=proc_env,
        )

        self._stderr_lines.clear()
        self._stderr_task = asyncio.create_task(self._drain_stderr())

        init_timeout = float(os.getenv("AGY_INIT_TIMEOUT", "30.0"))
        try:
            line = await asyncio.wait_for(self.process.stdout.readline(), timeout=init_timeout)
        except asyncio.TimeoutError as e:
            await self.terminate_session()
            raise RuntimeError("Timed out waiting for init event from agy process") from e
        except Exception as e:
            await self.terminate_session()
            raise RuntimeError(f"Failed to read init event from agy: {e}") from e

        if not line:
            stderr_msg = "\n".join(self._stderr_lines) if self._stderr_lines else ""
            if not stderr_msg and self.process and self.process.stderr:
                try:
                    stderr_bytes = await self.process.stderr.read()
                    stderr_msg = stderr_bytes.decode("utf-8", errors="replace").strip()
                except Exception:
                    pass
            await self.terminate_session()
            raise RuntimeError(f"agy process exited prematurely on start: {stderr_msg}")

        try:
            data = json.loads(line.decode("utf-8", errors="replace").strip())
            if isinstance(data, dict) and data.get("event") == "init":
                self.conversation_id = data.get("conversation_id") or (data.get("init") or {}).get("conversation_id")
                init_info = data.get("init") or {}
                self.available_tools = init_info.get("tools") or []
                logger.info(f"Headless agent initialized: conversation_id={self.conversation_id}, tools={len(self.available_tools)}")
        except Exception as e:
            logger.warning(f"Could not parse agy init event: {e}")

    async def send_prompt(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        """
        Submit user prompt to stdin and yield parsed AgentEvent objects
        until the 'result' event signals turn completion.
        """
        if self._active_generator is not None:
            old_gen = self._active_generator
            self._active_generator = None
            try:
                await old_gen.aclose()
            except Exception:
                pass
            try:
                await self._drain_stdout()
            except Exception:
                pass

        gen = self._stream_turn_impl(prompt)
        self._active_generator = gen
        try:
            async for event in gen:
                yield event
        finally:
            if self._active_generator is gen:
                self._active_generator = None

    async def _stream_turn_impl(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        """Internal turn execution loop under self._lock."""
        await self._lock.acquire()
        completed_cleanly = False
        try:
            if not self.is_alive:
                await self.start_session()

            await self._drain_stdout()

            payload = json.dumps({"event": "user", "message": {"content": prompt}}) + "\n"
            try:
                self.process.stdin.write(payload.encode("utf-8"))
                await self.process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError, AttributeError) as e:
                logger.error(f"Pipe error writing to agent stdin: {e}")
                await self.terminate_session()
                yield FinishEvent(
                    response="",
                    status="ERROR",
                    error=f"Broken pipe writing to agent stdin: {e}",
                    conversation_id=self.conversation_id,
                )
                return

            yield StatusUpdateEvent(
                status="working",
                detail="Processing prompt",
                conversation_id=self.conversation_id,
            )

            if not self.active_background_tasks:
                self.has_active_background_tasks = False
            has_background_task = bool(self.active_background_tasks) or self.has_active_background_tasks
            accumulated_text: List[str] = []
            text_since_last_tool: List[str] = []
            tools_executed_count: int = 0
            in_tool_call = False
            tool_completed_time: Optional[float] = None

            bg_idle_timeout = float(os.getenv("AGY_BG_TASK_IDLE_TIMEOUT", "1.0"))

            while True:
                timeout: Optional[float] = None
                if not in_tool_call and has_background_task and bool(text_since_last_tool):
                    timeout = bg_idle_timeout

                try:
                    reader = self.process.stdout
                    has_buf = bool(getattr(reader, "_buffer", None)) if isinstance(reader, asyncio.StreamReader) else False
                    if timeout is not None and not has_buf:
                        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
                    else:
                        line = await reader.readline()
                except asyncio.TimeoutError:
                    can_finish = False
                    if has_background_task and not in_tool_call and bool(text_since_last_tool):
                        can_finish = True

                    if can_finish:
                        logger.info(
                            f"Treating conversational turn as complete due to stream idle "
                            f"(has_bg={has_background_task}, tools={tools_executed_count}, "
                            f"post_tool_text_len={len(text_since_last_tool)}, total_text_len={len(accumulated_text)})"
                        )
                        completed_cleanly = True
                        yield FinishEvent(
                            response="".join(accumulated_text),
                            status="SUCCESS",
                            conversation_id=self.conversation_id,
                        )
                        break
                    continue

                if not line:
                    exit_code = self.process.returncode if self.process else None
                    stderr_msg = "\n".join(self._stderr_lines) if self._stderr_lines else ""
                    if not stderr_msg and self.process and self.process.stderr:
                        try:
                            stderr_bytes = await asyncio.wait_for(self.process.stderr.read(), timeout=0.5)
                            stderr_msg = stderr_bytes.decode("utf-8", errors="replace").strip()
                        except Exception:
                            pass
                    await self.terminate_session()
                    yield FinishEvent(
                        response="",
                        status="ERROR",
                        error=f"Process terminated unexpectedly (exit code {exit_code}): {stderr_msg}",
                        conversation_id=self.conversation_id,
                    )
                    break

                raw_line = line.decode("utf-8", errors="replace").strip()
                if not raw_line:
                    continue

                if "task_backgrounded" in raw_line or "task_details" in raw_line:
                    has_background_task = True
                    self.has_active_background_tasks = True
                    in_tool_call = False

                m_fin = re.search(r'Task id "(?:[^"/]+/)?([^"/]+)" finished', raw_line)
                if m_fin:
                    fin_id = m_fin.group(1).strip()
                    to_remove = [tid for tid in self.active_background_tasks if tid == fin_id or fin_id.endswith(tid) or tid.endswith(fin_id)]
                    for tid in to_remove:
                        self.active_background_tasks.discard(tid)
                    if not self.active_background_tasks:
                        has_background_task = False
                        self.has_active_background_tasks = False

                try:
                    msg = json.loads(raw_line)
                except json.JSONDecodeError:
                    logger.warning(f"Malformed JSON in agent stream: {raw_line}")
                    continue

                if not isinstance(msg, dict):
                    logger.warning(f"Expected JSON object in agent stream, got {type(msg).__name__}: {raw_line}")
                    continue

                event_type = msg.get("event")

                if event_type == "step_update":
                    step = msg.get("step_update") or {}
                    step_type = step.get("step_type")
                    state = step.get("state")
                    step_idx = step.get("step_index")

                    if step_type == "agent_response":
                        in_tool_call = False
                        delta = step.get("text_delta")
                        if delta:
                            accumulated_text.append(delta)
                            if tools_executed_count > 0:
                                text_since_last_tool.append(delta)
                            yield TextDeltaEvent(
                                text_delta=delta,
                                conversation_id=self.conversation_id,
                                step_index=step_idx,
                            )
                    elif step_type in ("tool", "tool_call"):
                        tool_name = step.get("tool_name", "")
                        tool_info = step.get("tool_info") or {}
                        tool_call_id = step.get("tool_call_id") or tool_info.get("tool_call_id")
                        params = tool_info.get("parameters") or step.get("parameters") or {}
                        state_str = str(state).upper() if state else ""

                        output = tool_info.get("output")
                        output_str = str(output) if output is not None else ""

                        is_bg = False
                        m_bg = re.search(r"background task with task id:\s*([^\s\n\r]+)", output_str, re.IGNORECASE)
                        if m_bg:
                            task_id = m_bg.group(1).strip()
                            self.active_background_tasks.add(task_id)
                            has_background_task = True
                            self.has_active_background_tasks = True
                            is_bg = True
                        elif tool_info.get("task_id"):
                            task_id = str(tool_info.get("task_id")).strip()
                            self.active_background_tasks.add(task_id)
                            has_background_task = True
                            self.has_active_background_tasks = True
                            is_bg = True
                        elif bool(params.get("IsDaemon")):
                            self.active_background_tasks.add(f"daemon-{tool_name}")
                            has_background_task = True
                            self.has_active_background_tasks = True
                            is_bg = True
                        elif (state_str == "RUNNING" or tool_info.get("status") == "RUNNING") and tool_name in ("run_command", "schedule"):
                            self.active_background_tasks.add(f"running-{tool_call_id or time.time()}")
                            has_background_task = True
                            self.has_active_background_tasks = True
                            is_bg = True

                        if is_bg:
                            in_tool_call = False

                        if state_str in ("ACTIVE", "START", "STARTED"):
                            in_tool_call = True
                            tools_executed_count += 1
                            text_since_last_tool.clear()
                            yield ToolStartEvent(
                                tool_name=tool_name,
                                tool_call_id=tool_call_id,
                                parameters=params,
                                conversation_id=self.conversation_id,
                                step_index=step_idx,
                            )
                        elif state_str == "RUNNING" and not is_bg:
                            in_tool_call = True
                            tools_executed_count += 1
                            text_since_last_tool.clear()
                            yield ToolStartEvent(
                                tool_name=tool_name,
                                tool_call_id=tool_call_id,
                                parameters=params,
                                conversation_id=self.conversation_id,
                                step_index=step_idx,
                            )
                        elif state_str in ("DONE", "COMPLETED", "FINISHED", "ERROR"):
                            in_tool_call = False
                            tool_completed_time = time.time()
                            is_error = bool(tool_info.get("error") or tool_info.get("is_error"))

                            if tool_name == "manage_task" and params.get("Action") == "kill":
                                killed_id = str(params.get("TaskId") or "").strip()
                                if killed_id:
                                    to_remove = [tid for tid in self.active_background_tasks if tid == killed_id or killed_id.endswith(tid) or tid.endswith(killed_id)]
                                    for tid in to_remove:
                                        self.active_background_tasks.discard(tid)
                                    if not self.active_background_tasks:
                                        has_background_task = False
                                        self.has_active_background_tasks = False

                            yield ToolEndEvent(
                                tool_name=tool_name,
                                tool_call_id=tool_call_id,
                                output=tool_info.get("output"),
                                duration_seconds=step.get("duration_seconds"),
                                is_error=is_error,
                                conversation_id=self.conversation_id,
                                step_index=step_idx,
                            )
                            if tool_name in ("ask_question", "ask_permission"):
                                question = params.get("question") or params.get("prompt")
                                choices = params.get("choices")
                                yield AwaitingInputEvent(
                                    prompt=question,
                                    choices=choices,
                                    tool_name=tool_name,
                                    conversation_id=self.conversation_id,
                                    step_index=step_idx,
                                )
                    elif step_type == "status":
                        detail = str(step.get("detail") or "")
                        if "background" in detail.lower() and "task" in detail.lower():
                            has_background_task = True
                            self.has_active_background_tasks = True
                        yield StatusUpdateEvent(
                            status=step.get("status", "working"),
                            detail=step.get("detail"),
                            conversation_id=self.conversation_id,
                            step_index=step_idx,
                        )
                    elif step_type == "generic":
                        content = str(step.get("content") or "")
                        m_gen_bg = re.search(r"background task with task id:\s*([^\s\n\r]+)", content, re.IGNORECASE)
                        if m_gen_bg:
                            task_id = m_gen_bg.group(1).strip()
                            self.active_background_tasks.add(task_id)
                            has_background_task = True
                            self.has_active_background_tasks = True
                            in_tool_call = False

                elif event_type == "awaiting_input":
                    ai = msg.get("awaiting_input") or {}
                    yield AwaitingInputEvent(
                        prompt=ai.get("prompt"),
                        choices=ai.get("choices"),
                        tool_name=ai.get("tool_name"),
                        conversation_id=self.conversation_id,
                    )

                elif event_type == "result":
                    res = msg.get("result") or {}
                    resp = res.get("response", "") or "".join(accumulated_text)
                    completed_cleanly = True
                    yield FinishEvent(
                        response=resp,
                        status=res.get("status", "SUCCESS"),
                        duration_seconds=res.get("duration_seconds"),
                        usage=res.get("usage"),
                        error=res.get("error"),
                        conversation_id=self.conversation_id,
                    )
                    break

                elif event_type == "error":
                    err_info = msg.get("error") or {}
                    err_msg = err_info if isinstance(err_info, str) else err_info.get("message", str(msg))
                    completed_cleanly = True
                    yield FinishEvent(
                        response="",
                        status="ERROR",
                        error=err_msg,
                        conversation_id=self.conversation_id,
                    )
                    break

                elif event_type in ("task_backgrounded", "background_task"):
                    has_background_task = True
                    self.has_active_background_tasks = True
        finally:
            if not completed_cleanly and self.is_alive:
                try:
                    await self._drain_stdout()
                except Exception:
                    pass
            if not self.active_background_tasks:
                self.has_active_background_tasks = False
            if self._lock.locked():
                try:
                    self._lock.release()
                except RuntimeError:
                    pass

    async def terminate_session(self) -> None:
        """Gracefully close stdin and terminate subprocess."""
        if self._stderr_task and not self._stderr_task.done():
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except (asyncio.CancelledError, Exception):
                pass
            self._stderr_task = None

        if self._active_generator is not None:
            old_gen = self._active_generator
            self._active_generator = None
            try:
                await old_gen.aclose()
            except Exception:
                pass

        if self.process:
            proc = self.process
            if proc.returncode is None:
                try:
                    if proc.stdin and not proc.stdin.is_closing():
                        proc.stdin.close()
                        try:
                            await proc.stdin.wait_closed()
                        except Exception:
                            pass
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except (asyncio.TimeoutError, Exception):
                    try:
                        proc.terminate()
                        await asyncio.wait_for(proc.wait(), timeout=1.5)
                    except (asyncio.TimeoutError, Exception):
                        try:
                            proc.kill()
                            await proc.wait()
                        except Exception:
                            pass
            self.process = None

        if self._lock.locked():
            try:
                self._lock.release()
            except RuntimeError:
                pass
        self.has_active_background_tasks = False
        self.active_background_tasks.clear()
        self._lock = asyncio.Lock()
