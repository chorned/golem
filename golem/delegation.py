#!/usr/bin/env python3
"""
golem.execution.delegation - Isolated worker delegation sandbox.
Manages isolated worker subagent delegation via herdr_handoff / worktree creation
for coding tasks (opencode-dev, opencode) on dedicated branches and sandboxes.
Cleans and sanitizes worker output (stripping internal scratchpads, tool crumbs,
terminal boxes, ANSI codes, and prompt echoes) before delivery.
Satisfies Acceptance Criterion TC-04.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import json
import time
import shutil
import asyncio
import builtins
import logging
from typing import Dict, Any, Optional

if not hasattr(builtins, "SecurityError"):
    class SecurityError(ValueError):
        pass
    builtins.SecurityError = SecurityError

from golem.config import get_default_hand_worker
try:
    from herdr_client import HerdrClient
except ImportError:
    HerdrClient = None  # type: ignore
from golem.normalizer import (
    strip_terminal_chrome,
    strip_ansi,
    strip_box_drawing,
    strip_tui_debris,
    strip_prompt_echoes
)

logger = logging.getLogger("Golem.Execution.Delegation")

SCRATCHPAD_BLOCK_PATTERN = re.compile(r"<scratchpad>.*?</scratchpad>", re.DOTALL | re.IGNORECASE)
SCRATCHPAD_LINE_PATTERN = re.compile(r"^\s*\[?(?:scratchpad|internal\s+trace|thought)\]?.*$", re.IGNORECASE)


def sanitize_worker_output(raw_text: str) -> str:
    """
    Cleans and sanitizes worker output before delivery:
    - Strips internal scratchpads (<scratchpad>...</scratchpad>, [scratchpad] ...)
    - Strips ANSI escape sequences
    - Strips VT100 box-drawing glyphs and curses borders
    - Strips terminal crumbs, spinners, and progress debris
    - Strips prompt echoes
    - Consolidates excess whitespace
    """
    if not raw_text:
        return ""

    # 1. Strip scratchpad blocks
    text = SCRATCHPAD_BLOCK_PATTERN.sub("", raw_text)

    # 2. Filter scratchpad lines
    lines = []
    for line in text.splitlines():
        if SCRATCHPAD_LINE_PATTERN.match(line):
            continue
        lines.append(line)
    text = "\n".join(lines)

    # 3. Strip terminal chrome, ANSI, box drawing, TUI debris, prompt echoes
    text = strip_terminal_chrome(text)
    text = strip_box_drawing(text)
    text = strip_tui_debris(text)
    text = strip_prompt_echoes(text)

    # 4. Strip leftover empty lines > 2
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


class WorkerDelegator:
    """
    Manages pluggable backstage worker delegation (AGY default, OpenCode, Hermes).
    - Default worker: 'agy', executing directly in Pane 2 with 3.0s non-blocking yield preservation.
    - Specialized worker: 'opencode', creating isolated worktrees (task-<ts>-opencode) and sanitizing output.
    - Specialized worker: 'hermes', routing to homelab/Docker operational workspace.
    """

    SUPPORTED_WORKERS = ["agy", "opencode", "hermes", "antigravity", "hermes-worker", "opencode-dev"]

    def __init__(
        self,
        herdr_client: Optional[Any] = None,
        base_repo: Optional[str] = None
    ):
        self.herdr_client = herdr_client or (HerdrClient() if HerdrClient is not None else None)
        self.base_repo = base_repo or "/Users/hermes/projects/golem"

    @classmethod
    def normalize_worker_name(cls, worker: Optional[str]) -> str:
        """Strip whitespace and lowercase worker name; standardizes 'antigravity' to 'agy'."""
        if not worker or not isinstance(worker, str):
            return get_default_hand_worker()
        w = worker.strip().lower()
        if w == "antigravity":
            return "agy"
        return w or get_default_hand_worker()

    @classmethod
    def validate_worker(cls, worker: str) -> str:
        """
        Validate worker name against security hazards (path traversal, shell injection)
        and verify against supported worker catalog. Raises ValueError if invalid.
        """
        if not worker or not isinstance(worker, str):
            raise ValueError("Worker name must be a non-empty string.")
        if any(c in worker for c in [";", "&", "|", "`", "$", "\n", "\r", "\t"]):
            raise ValueError(f"Dangerous characters detected in worker name: {worker}")
        if ".." in worker or "/" in worker or "\\" in worker:
            raise ValueError(f"Path traversal detected in worker name: {worker}")
        norm = cls.normalize_worker_name(worker)
        if norm not in cls.SUPPORTED_WORKERS and norm not in ("agy", "opencode", "hermes"):
            raise ValueError(f"Unknown worker '{worker}'. Supported: {cls.SUPPORTED_WORKERS}")
        return norm

    async def _execute_agy_hand(
        self,
        task: str,
        cwd: str,
        node: str = "hermes",
        timeout_ms: int = 120000,
        pane: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Routes execution directly to Hand pane or AGY execution handle.
        Enforces non-blocking 3.0s yield timeout ceiling.
        """
        from golem.runner import RunnerExecutor, ExecutionHandle, TaskHandle
        executor = RunnerExecutor(
            herdr_bin=getattr(self.herdr_client, "herdr_bin", None) if self.herdr_client else None,
            herdr_client=self.herdr_client
        )
        yield_timeout = min(3.0, float(timeout_ms) / 1000.0)
        target_pane = pane or "runner"
        try:
            handle = await executor.run_command(
                pane=target_pane,
                command=task,
                yield_timeout=yield_timeout
            )
        except Exception as e:
            if self.herdr_client and hasattr(self.herdr_client, "run_cli"):
                code, out, err = await self.herdr_client.run_cli(["pane", "run", target_pane, task])
                handle = TaskHandle(
                    status="completed" if code == 0 else "error",
                    pane=target_pane,
                    command=task,
                    returncode=code,
                    stdout=out,
                    stderr=err,
                    pid=None,
                    dispatched_at=time.time(),
                    message=out or err,
                )
            else:
                raise e

        raw_output = handle.stdout or handle.stderr or handle.message or ""
        clean_output = sanitize_worker_output(raw_output)
        if not clean_output and handle.status == "completed":
            clean_output = f"AGY hand execution completed in Pane 2."

        return {
            "status": handle.status,
            "worker": "agy",
            "subagent": "agy",
            "branch": "main",
            "node": node,
            "cwd": cwd,
            "workspace_id": None,
            "output": clean_output,
            "raw_output": raw_output,
            "pane": handle.pane,
            "pid": handle.pid,
            "handle": handle,
            "returncode": handle.returncode,
        }

    async def handoff(
        self,
        subagent: Optional[str] = None,
        task: str = "",
        cwd: Optional[str] = None,
        node: str = "hermes",
        timeout_ms: int = 120000,
        isolate_branch: bool = True,
        worker: Optional[str] = None,
        pane: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Delegates task to worker hand agent (default: 'agy'; supports 'opencode', 'hermes').
        - worker parameter takes precedence over subagent; defaults to get_default_hand_worker() ('agy').
        - If worker is 'agy', routes directly to Hand pane with 3.0s yield preservation.
        - If worker is 'opencode', provisions isolated worktree task-<ts>-opencode and sanitizes output.
        - If worker is 'hermes', routes to hermes worker.
        """
        effective_worker = worker if worker is not None else (subagent if subagent is not None else get_default_hand_worker())
        target_worker = self.validate_worker(effective_worker)
        target_cwd = cwd or self.base_repo

        if target_worker == "agy":
            return await self._execute_agy_hand(task=task, cwd=target_cwd, node=node, timeout_ms=timeout_ms, pane=pane)

        ts = int(time.time())
        branch_name = f"task-{ts}-{target_worker}" if isolate_branch else "main"
        effective_cwd = target_cwd
        workspace_id = None

        # 1. Worktree / branch isolation
        if isolate_branch:
            wt_cmd = [
                "worktree", "create",
                "--cwd", target_cwd,
                "--branch", branch_name,
                "--no-focus"
            ]
            wt_code, wt_out, wt_err = await self.herdr_client.run_cli(wt_cmd)
            if wt_code != 0:
                logger.error(f"Worktree isolation failed ({wt_code}): {wt_err or wt_out}")
                return {
                    "status": "error",
                    "error": f"Worktree isolation failed: {wt_err or wt_out or 'exit code ' + str(wt_code)}",
                    "worker": target_worker,
                    "subagent": target_worker,
                    "branch": branch_name,
                    "node": node,
                    "cwd": target_cwd,
                    "workspace_id": None,
                    "output": "",
                    "raw_output": wt_err or wt_out or ""
                }
            if wt_code == 0 and wt_out:
                try:
                    data = json.loads(wt_out)
                    wt_info = data.get("result", {}).get("worktree", {}) or data.get("worktree", {})
                    if wt_info.get("path"):
                        effective_cwd = wt_info["path"]
                    if data.get("result", {}).get("workspace", {}).get("workspace_id"):
                        workspace_id = data["result"]["workspace"]["workspace_id"]
                except Exception:
                    pass

        # 2. Worker agent name & spawn
        worker_agent_name = f"{target_worker}-{ts}"
        start_cmd = [
            "agent", "start", worker_agent_name,
            "--kind", target_worker,
            "--pane", "1",
            "--cwd", effective_cwd
        ]
        await self.herdr_client.run_cli(start_cmd)

        # 3. Prompt delivery
        prompt_cmd = [
            "agent", "prompt", worker_agent_name, task,
            "--timeout", str(timeout_ms)
        ]
        p_code, p_out, p_err = await self.herdr_client.run_cli(prompt_cmd, timeout=float(timeout_ms) / 1000.0 + 10.0)

        # 4. Wait for idle/done if prompt returned immediately
        wait_cmd = ["agent", "wait", worker_agent_name, "--until", "done", "--until", "idle"]
        await self.herdr_client.run_cli(wait_cmd, timeout=min(30.0, float(timeout_ms) / 1000.0))

        # 5. Read terminal output buffer
        read_cmd = ["agent", "read", worker_agent_name, "--lines", "100"]
        r_code, r_out, r_err = await self.herdr_client.run_cli(read_cmd, timeout=10.0)

        raw_output = r_out or p_out or ""
        clean_output = sanitize_worker_output(raw_output)

        status = "completed" if (p_code == 0 or r_code == 0) else "error"
        if not clean_output and status == "completed":
            clean_output = f"Worker `{target_worker}` finished task successfully on branch `{branch_name}`."

        return {
            "status": status,
            "worker": target_worker,
            "subagent": target_worker,
            "branch": branch_name,
            "node": node,
            "cwd": effective_cwd,
            "workspace_id": workspace_id,
            "output": clean_output,
            "raw_output": raw_output
        }


__all__ = [
    "WorkerDelegator",
    "sanitize_worker_output",
    "get_default_hand_worker",
]
