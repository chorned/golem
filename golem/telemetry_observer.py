#!/usr/bin/env python3
"""
telemetry_observer.py
Real-time fleet delegation telemetry, out-of-band PTY observation,
and conversational milestone notifications for First Mate.
Eliminates robotic Datadog metric grids and curses terminal blocks in favor
of a natural Chief of Staff voice that posts at true execution milestones.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import json
import time
import inspect
import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple, Set

from normalizer import sanitize_for_slack

logger = logging.getLogger("AGY.TelemetryObserver")

# ------------------------------------------------------------------------------
# Process Filter & Scrollback Sanitizer
# ------------------------------------------------------------------------------
INTERNAL_PLUMBING_PATTERNS = [
    r"^node\b",
    r"\bherdr-mesh\b",
    r"^herdr\b",
    r"^agy\b",
    r"^opencode\b",
    r"^(?:/bin/)?(?:zsh|bash|sh)\b",
    r"^cat\b",
    r"^tee\b",
    r"^sleep\b",
    r"^which\b",
    r"^stty\b",
    r"^pwd\b",
    r"^launchctl\b",
    r"^git\s+(?:status|diff|branch|checkout|worktree|rev-parse)\b",
    r"\bjsonrpc\b",
    r"\bherdr_\w+\b",
    r"msg=interrupt"
]

MEANINGFUL_COMMAND_PATTERNS = [
    r"(?:python(?:3)?\s+-m\s+http\.server(?:\s+\d+)?[^\n]*)",
    r"(?:python(?:3)?\s+[a-zA-Z0-9_\-\./]+\.py[^\n]*)",
    r"(?:npm\s+(?:run\s+[a-zA-Z0-9_\-]+|start|test|build)[^\n]*)",
    r"(?:curl\s+[^\n]*)",
    r"(?:cargo\s+(?:run|build|test)[^\n]*)",
    r"(?:docker\s+[^\n]*)",
    r"(?:pytest\s+[^\n]*)"
]


def is_plumbing_command(cmdline: str) -> bool:
    """Check if command line belongs to internal infrastructure plumbing."""
    if not cmdline:
        return True
    c = cmdline.strip()
    return any(re.search(pat, c, re.IGNORECASE) for pat in INTERNAL_PLUMBING_PATTERNS)


def strip_terminal_sequences(raw_text: str) -> str:
    """Strip ANSI color codes and graphics/query protocol sequences."""
    clean = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", raw_text)
    clean = re.sub(r"\x1b\][^\x07\x1b]*[\x07\x1b]", "", clean)
    clean = re.sub(r"\^\[_G[^\^]*\^\\", "", clean)
    clean = re.sub(r"G[a-z0-9_=,;+]+(?:==)?", "", clean)
    clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", clean)
    return clean


def sanitize_scrollback(text: str) -> str:
    """
    Filter raw terminal text from Antigravity / OpenCode:
    - Strips ANSI sequences, curses chrome, and splash headers.
    - Suppresses shell prompts, git branch tags, and CLI versions.
    """
    clean = strip_terminal_sequences(text)
    noise_indicators = [
        "antigravity cli",
        "gemini 3.8",
        "welcome to the antigravity",
        "signing in...",
        "? for shortcuts",
        "esc to cancel",
        "last progress:",
        "navigate · enter confirm",
        "do you trust the contents",
        "yes, i trust this folder",
        "1.18.",
        "thought for",
        "thinking..."
    ]

    filtered = []
    for line in clean.splitlines():
        s = line.strip()
        if not s:
            continue
        # Skip pure borders
        if all(c in "─━│┃-=_* " for c in s):
            continue
        # Skip ASCII blocks
        if any(c in "▄▀█▌▐" for c in s):
            continue
        lower = s.lower()
        if any(ind in lower for ind in noise_indicators):
            continue
        # Suppress shell prompts (e.g. hermes@Mac % ..., ❯ ..., > ...)
        if re.search(r"^[a-zA-Z0-9_\-\.]+@[a-zA-Z0-9_\-\.]+\s+[%$#>]", s) or s in (">", "> "):
            continue
        # Suppress path and git branch tags (e.g. ~/projects/...:main)
        if re.search(r"~/projects/.*:(?:main|task/)", s):
            continue
        filtered.append(s)

    return "\n".join(filtered)


def sanitize_scrollback_command(text: str) -> Optional[str]:
    """Extract clean meaningful command from terminal scrollback, ignoring plumbing."""
    if not text:
        return None
    clean = strip_terminal_sequences(text)
    for pat in MEANINGFUL_COMMAND_PATTERNS:
        match = re.search(pat, clean, re.IGNORECASE)
        if match:
            cmd = match.group(0).strip()
            cmd = re.sub(r"^[>%#$]\s*", "", cmd)
            return cmd
    return None


def extract_pending_command(snippet: str, fallback: str = "") -> str:
    """
    Extract pending command requiring user approval from Ask Mode output.
    Example: '● Run command python3 -m http.server 8090' -> 'python3 -m http.server 8090'
    """
    if not snippet:
        return fallback or "the requested command"

    clean = strip_terminal_sequences(snippet)
    patterns = [
        r"(?:●\s*run\s*(?:command)?\s*[:]?\s*)([^\n]+)",
        r"(?:do\s+you\s+want\s+to\s+run\s*[:]?\s*)([^\n]+)",
        r"(?:allow\s+this\s+action\s*[:]?\s*)([^\n]+)",
        r"(?:confirm\s*[:]?\s*)([^\n]+)"
    ]
    for pat in patterns:
        m = re.search(pat, clean, re.IGNORECASE)
        if m:
            cmd = m.group(1).strip()
            cmd = re.sub(r"\s*\[[yYnN/]+\]\s*$", "", cmd)
            cmd = re.sub(r"\s*\?\s*$", "", cmd)
            cmd = cmd.strip("`'\" ")
            if cmd and not is_plumbing_command(cmd):
                return cmd

    meaningful = sanitize_scrollback_command(clean)
    if meaningful:
        return meaningful

    ports = extract_listening_ports(clean)
    if ports and ("serving http" in clean.lower() or "http" in clean.lower() or "server" in clean.lower()):
        return f"python3 -m http.server {ports[0]}"

    if fallback and not is_plumbing_command(fallback):
        return fallback

    return "the requested command"


# ------------------------------------------------------------------------------
# Conversational Formatting & Node Labeling
# ------------------------------------------------------------------------------
def resolve_machine_name(target_machine: Optional[str] = None) -> str:
    """Resolve concise fleet machine name for conversational messages."""
    m = (target_machine or "").lower().strip()
    if m in ("desktop-pc", "compute-grid", "100.80.59.45"):
        return "desktop-pc"
    elif m in ("oci-machine", "cloud-ops", "100.113.173.33"):
        return "oci-machine"
    elif m in ("personal-laptop", "dev-grid", "100.96.38.5"):
        return "personal-laptop"
    return "hermes"


def resolve_machine_label(target_machine: Optional[str] = None, cwd: Optional[str] = None) -> str:
    """Legacy helper for hardware profiles (retained for backward compatibility)."""
    machine = (target_machine or "").lower().strip()
    if machine in ("desktop-pc", "compute-grid", "100.80.59.45"):
        return "desktop-pc (Windows CUDA)"
    elif machine in ("oci-machine", "cloud-ops", "100.113.173.33"):
        return "oci-machine (Cloud Operations)"
    elif machine in ("personal-laptop", "dev-grid", "100.96.38.5"):
        return "personal-laptop (Active Workstation)"
    elif machine in ("local", "hermes", "localhost", "fleet-control", "homelab-ops") or not machine:
        return "hermes (Local Mac mini)"
    return f"{machine} (Fleet Node)"


def format_runner_label(root_runner: str = "agy", child_runners: Optional[List[str]] = None) -> str:
    """Format runner hierarchy label."""
    root = root_runner or "agy"
    if child_runners:
        unique_children = []
        for c in child_runners:
            if c and c.lower() != root.lower() and c not in unique_children:
                unique_children.append(c)
        if unique_children:
            return f"{root} ➔ {' ➔ '.join(unique_children)}"
    return root


def extract_listening_ports(text: str) -> List[str]:
    """Extract port numbers referenced in terminal output."""
    matches = re.findall(
        r"(?:port\s+|localhost:|127\.0\.0\.1:|0\.0\.0\.0:)(\d{4,5})",
        text,
        re.IGNORECASE
    )
    return list(dict.fromkeys(matches))


def format_terminal_tail(raw_text: str, max_lines: int = 3) -> str:
    """Clean rolling tail snippet."""
    clean = sanitize_scrollback(raw_text)
    filtered = [l.strip() for l in clean.splitlines() if l.strip()]
    tail = filtered[-max_lines:]
    return "\n".join(tail) if tail else ""


def summarize_task_intent(prompt: str) -> str:
    """Extract clean intent phrase from user prompt."""
    if not prompt:
        return "execute the task"
    p = prompt.strip()
    p = re.sub(r"^<@[A-Z0-9]+>\s*", "", p)
    p = re.sub(r"^@\w+\s*", "", p)
    prefix_pattern = re.compile(
        r"^(?:can you(?:\s+please)?|could you(?:\s+please)?|please|have opencode|ask opencode to|tell opencode to|use opencode to)\s+",
        re.IGNORECASE
    )
    while True:
        new_p = prefix_pattern.sub("", p)
        if new_p == p:
            break
        p = new_p.strip()
    p = p.strip(" .?!")
    if p.lower().startswith("to "):
        p = p[3:].strip()
    return p or "execute the task"


def format_startup_message(runner: str, machine: str, prompt: str) -> str:
    """Build conversational startup / delegation notification."""
    intent = summarize_task_intent(prompt)
    if "opencode" in runner.lower():
        return f"On it. Handing this to OpenCode on `{machine}` to {intent}."
    return f"On it. Working on `{machine}` to {intent}."


def format_approval_blocks(runner: str, command: str, session_key: str) -> Tuple[str, List[Dict[str, Any]]]:
    """Build conversational Block Kit approval card with Approve/Reject buttons."""
    text_content = f"*{runner} wants to run `{command}`. Good to go?*"
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text_content
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Approve", "emoji": True},
                    "style": "primary",
                    "value": session_key,
                    "action_id": "approve_action"
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Reject", "emoji": True},
                    "style": "danger",
                    "value": session_key,
                    "action_id": "reject_action"
                }
            ]
        }
    ]
    return text_content, blocks


def format_completion_message(
    machine: str,
    urls: Optional[List[str]] = None,
    files: Optional[List[str]] = None,
    summary_snippet: str = ""
) -> str:
    """Build conversational completion announcement."""
    clean_urls = urls or []
    clean_files = files or []

    if clean_urls:
        if clean_files:
            return f"All set! The test server is running at {clean_urls[0]} on `{machine}` (created `{', '.join(clean_files)}`)."
        return f"All set! The test server is running at {clean_urls[0]} on `{machine}`."
    elif clean_files:
        return f"All set! Created `{', '.join(clean_files)}` on `{machine}`."
    elif summary_snippet:
        return f"All set! {summary_snippet} on `{machine}`."
    return f"All set! Completed on `{machine}`."


def format_failure_message(error: str) -> str:
    """Build conversational error message."""
    clean = (error or "Unknown failure").strip()
    return f"Hit an issue running the server: `{clean}`."


def build_status_card_blocks(
    workspace_id: str,
    machine_label: str,
    runner_label: str,
    status_phase: str,
    elapsed_seconds: int,
    current_action: str,
    terminal_tail: str,
    artifacts: Optional[Dict[str, List[str]]] = None,
    error_detail: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Conversational Block Kit card representation (suppressing robotic metrics)."""
    machine = machine_label.split()[0].strip()
    runner = runner_label.split("➔")[-1].strip()

    if status_phase == "TASK COMPLETE":
        urls = (artifacts or {}).get("urls", [])
        files = (artifacts or {}).get("files", [])
        text = format_completion_message(machine, urls, files)
    elif status_phase == "TASK FAILED":
        text = format_failure_message(error_detail or "Execution error")
    elif status_phase == "APPROVAL REQUIRED":
        text = f"*{runner} wants to run `{current_action}`. Good to go?*"
    else:
        text = f"On it. Working on `{machine}` with {runner}..."

    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text
            }
        }
    ]


# ------------------------------------------------------------------------------
# Session Telemetry Observer (Conversational Milestone Engine)
# ------------------------------------------------------------------------------
class SessionTelemetryObserver:
    """
    Asynchronous telemetry observer loop querying native Herdr state
    and emitting conversational milestone messages directly into Slack/iMessage threads.
    """

    def __init__(
        self,
        session: Any,
        client: Any,
        herdr_client: Any,
        poll_interval: float = 1.0,
        min_update_interval: float = 1.5
    ):
        self.session = session
        self.client = client
        self.herdr_client = herdr_client
        self.poll_interval = poll_interval
        self.min_update_interval = min_update_interval

        self.workspace_id = getattr(session, "workspace_id", None) or getattr(session, "agent_name", "w1")
        self.agent_name = getattr(session, "agent_name", "agy")
        self.channel = getattr(session, "channel", "")
        self.thread_ts = getattr(session, "thread_ts", "")
        self.is_imessage = getattr(session, "is_imessage", False)
        self.user_prompt = getattr(session, "last_prompt", "")

        self.machine: str = resolve_machine_name()
        self.last_runner: str = "agy"
        self.last_runner_label: str = "agy"
        self.last_machine_label: str = resolve_machine_label()
        self.card_msg_ts: Optional[str] = None
        self.start_time: float = time.time()
        self.paused_elapsed: Optional[int] = None
        self.last_state_hash: Optional[str] = None
        self.last_update_time: float = 0.0
        self.current_action: str = ""
        self.terminal_tail: str = ""

        self.milestones_posted: Set[str] = set()
        self.task: Optional[asyncio.Task] = None
        self.stop_event: asyncio.Event = asyncio.Event()

    def start(self) -> asyncio.Task:
        """Spawn the asynchronous observation loop in the background."""
        self.task = asyncio.create_task(self.run())
        return self.task

    async def stop(self) -> None:
        """Cancel and clean up the observer loop."""
        self.stop_event.set()
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def initialize_dashboard(self) -> None:
        """Initialize telemetry state without sending canned startup text."""
        if "startup" in self.milestones_posted:
            return
        self.milestones_posted.add("startup")

    async def run(self) -> None:
        """Main observation loop monitoring milestones out-of-band."""
        await self.initialize_dashboard()

        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(self.poll_interval)
                if self.stop_event.is_set():
                    break
                await self.poll_and_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Telemetry observer loop exception: {e}")

    async def poll_and_update(self) -> None:
        """Query Herdr state, detect subagent delegation milestones, and silence noise."""
        current_status = getattr(self.session, "status", "working")
        now = time.time()

        # Handle blocked state pause
        if current_status == "blocked":
            if self.paused_elapsed is None:
                self.paused_elapsed = int(now - self.start_time)
        else:
            if self.paused_elapsed is not None:
                self.start_time = now - self.paused_elapsed
                self.paused_elapsed = None

        # 1. Query workspace inspect to detect subagents
        target_machine = None
        child_runners: List[str] = []
        active_pane_id = getattr(self.session, "pane_id", None)
        active_agent_target = self.agent_name

        code, ws_out, _ = await self.herdr_client.run_cli(["workspace", "inspect", self.workspace_id, "--json"])
        if code != 0 or not ws_out:
            code, ws_out, _ = await self.herdr_client.run_cli(["workspace", "get", self.workspace_id])

        if code == 0 and ws_out:
            try:
                ws_data = json.loads(ws_out)
                panes = ws_data.get("panes") or ws_data.get("result", {}).get("panes", [])
                for p in panes:
                    agent_kind = p.get("agent") or p.get("agent_kind")
                    p_machine = p.get("target_machine") or p.get("machine")
                    if p_machine:
                        target_machine = p_machine
                    if agent_kind and agent_kind.lower() != "agy":
                        child_runners.append(agent_kind)
                        active_pane_id = p.get("pane_id", active_pane_id)
                        active_agent_target = p.get("name", active_agent_target)
            except Exception:
                pass

        # Also inspect agent list, strictly scoped to this workspace
        agent_code, agent_out, _ = await self.herdr_client.run_cli(["agent", "list"])
        if agent_code == 0 and agent_out:
            try:
                adata = json.loads(agent_out)
                for a in adata.get("result", {}).get("agents", []):
                    if a.get("workspace_id") == self.workspace_id:
                        akind = a.get("agent")
                        if akind and akind.lower() not in ("agy",):
                            if a.get("agent_status") in ("working", "starting"):
                                if akind not in child_runners:
                                    child_runners.append(akind)
                                active_agent_target = a.get("name", active_agent_target)
            except Exception:
                pass

        machine = resolve_machine_name(target_machine)
        self.machine = machine
        self.last_machine_label = resolve_machine_label(target_machine)
        runner_label = format_runner_label("agy", child_runners)
        self.last_runner_label = runner_label

        # 2. Delegation Milestone Notification (sole authority: fires strictly when child pane appears in Herdr)
        if child_runners:
            subagent = child_runners[-1]
            delegation_key = f"delegation_{subagent.lower()}"
            if delegation_key not in self.milestones_posted:
                self.milestones_posted.add(delegation_key)
                runner_display = "OpenCode" if "opencode" in subagent.lower() else subagent
                notice = f"Handing this to {runner_display} on `{machine}` to execute."
                try:
                    if self.is_imessage:
                        await self.client.chat_postMessage(channel=self.channel, text=notice)
                    elif hasattr(self.client, "chat_postMessage"):
                        await self.client.chat_postMessage(
                            channel=self.channel,
                            thread_ts=self.thread_ts,
                            text=sanitize_for_slack(notice)
                        )
                except Exception as e:
                    logger.debug(f"Error posting delegation notice: {e}")

        # 3. Foreground process check (filtered for internal plumbing)
        cmdline = ""
        code, p_out, _ = await self.herdr_client.run_cli(["agent", "inspect", active_agent_target, "--json"])
        if code == 0 and p_out:
            try:
                p_data = json.loads(p_out)
                fg = p_data.get("foreground_process", {}) or p_data
                raw_cmd = fg.get("cmdline") or fg.get("name", "")
                if raw_cmd and not is_plumbing_command(raw_cmd):
                    cmdline = raw_cmd
            except Exception:
                pass

        # 4. Clean Terminal Read
        code, read_out, _ = await self.herdr_client.run_cli([
            "agent", "read", active_agent_target,
            "--source", "recent-unwrapped",
            "--lines", "3"
        ])
        if code != 0 or not read_out:
            code, read_out, _ = await self.herdr_client.run_cli([
                "agent", "read", self.agent_name,
                "--source", "recent-unwrapped",
                "--lines", "3"
            ])

        clean_scrollback = sanitize_scrollback(read_out if code == 0 else "")
        self.terminal_tail = clean_scrollback

        if cmdline:
            self.current_action = cmdline
        else:
            meaningful = sanitize_scrollback_command(read_out or "")
            if meaningful:
                self.current_action = meaningful

    async def settle_complete(
        self,
        artifacts: Optional[Dict[str, List[str]]] = None,
        terminal_output: str = ""
    ) -> None:
        """
        Mark task completion milestone and stop observer loop.
        Completion messages are solely emitted by monitor_agent_lifecycle in app.py.
        """
        self.stop_event.set()
        if "complete" in self.milestones_posted:
            return
        self.milestones_posted.add("complete")

    async def settle_failed(
        self,
        error_detail: str,
        terminal_output: str = ""
    ) -> None:
        """Post conversational failure milestone."""
        self.stop_event.set()
        if "failed" in self.milestones_posted:
            return
        self.milestones_posted.add("failed")

        msg = format_failure_message(error_detail)
        if hasattr(self.client, "chat_postMessage"):
            try:
                if self.is_imessage:
                    await self.client.chat_postMessage(channel=self.channel, text=msg)
                else:
                    await self.client.chat_postMessage(
                        channel=self.channel,
                        thread_ts=self.thread_ts,
                        text=sanitize_for_slack(msg)
                    )
            except Exception as e:
                logger.debug(f"Error posting failure milestone: {e}")
