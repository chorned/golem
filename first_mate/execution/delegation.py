#!/usr/bin/env python3
"""
first_mate.execution.delegation - Isolated worker delegation sandbox.
Manages isolated worker subagent delegation via herdr_handoff / worktree creation
for coding tasks (opencode-dev, opencode) on dedicated branches and sandboxes.
Cleans and sanitizes worker output (stripping internal scratchpads, tool crumbs,
terminal boxes, ANSI codes, and prompt echoes) before delivery.
Satisfies Acceptance Criterion TC-04.
"""

import os
import re
import json
import time
import shutil
import asyncio
import logging
from typing import Dict, Any, Optional

from herdr_client import HerdrClient
from first_mate.transport.normalizer import (
    strip_terminal_chrome,
    strip_ansi,
    strip_box_drawing,
    strip_tui_debris,
    strip_prompt_echoes
)

logger = logging.getLogger("FirstMate.Execution.Delegation")

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
    Manages isolated delegation to specialized subagents (OpenCode, Hermes).
    Creates isolated worktrees/sandboxes on dedicated branches, executes tasks,
    monitors completion, and delivers clean, sanitized summaries.
    """

    def __init__(
        self,
        herdr_client: Optional[HerdrClient] = None,
        base_repo: Optional[str] = None
    ):
        self.herdr_client = herdr_client or HerdrClient()
        self.base_repo = base_repo or "/Users/hermes/projects/first-mate"

    async def handoff(
        self,
        subagent: str = "opencode",
        task: str = "",
        cwd: Optional[str] = None,
        node: str = "hermes",
        timeout_ms: int = 120000,
        isolate_branch: bool = True
    ) -> Dict[str, Any]:
        """
        Delegates task to worker subagent in an isolated environment.
        - Generates isolated branch name task-<timestamp>-<subagent>
        - Provisions worktree or sandbox
        - Executes task and waits up to timeout_ms
        - Retrieves output and applies comprehensive sanitization
        """
        ts = int(time.time())
        branch_name = f"task-{ts}-{subagent}" if isolate_branch else "main"
        target_cwd = cwd or self.base_repo
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
                    "subagent": subagent,
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
        worker_agent_name = f"{subagent}-{ts}"
        start_cmd = [
            "agent", "start", worker_agent_name,
            "--kind", subagent,
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
            clean_output = f"Worker `{subagent}` finished task successfully on branch `{branch_name}`."

        return {
            "status": status,
            "subagent": subagent,
            "branch": branch_name,
            "node": node,
            "cwd": effective_cwd,
            "workspace_id": workspace_id,
            "output": clean_output,
            "raw_output": raw_output
        }
