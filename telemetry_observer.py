#!/usr/bin/env python3
"""
telemetry_observer.py
Real-time fleet delegation telemetry, out-of-band PTY observation,
and in-place Slack progress card management for First Mate.
"""

import os
import re
import json
import time
import inspect
import asyncio
import hashlib
import logging
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger("AGY.TelemetryObserver")

PHASE_MAP = {
    "starting": "BOOTING",
    "booting": "BOOTING",
    "working": "WORKING",
    "blocked": "APPROVAL REQUIRED",
    "done": "TASK COMPLETE",
    "failed": "TASK FAILED"
}


def resolve_machine_label(target_machine: Optional[str] = None, cwd: Optional[str] = None) -> str:
    """Format human-readable fleet node and hardware profile label."""
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
    """Format delegation hierarchy label (e.g. agy ➔ opencode)."""
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


def strip_terminal_sequences(raw_text: str) -> str:
    """Strip ANSI color codes and graphics/query protocol sequences."""
    clean = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", raw_text)
    clean = re.sub(r"\x1b\][^\x07\x1b]*[\x07\x1b]", "", clean)
    clean = re.sub(r"\^\[_G[^\^]*\^\\", "", clean)
    clean = re.sub(r"G[a-z0-9_=,;+]+(?:==)?", "", clean)
    clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", clean)
    return clean


def format_terminal_tail(raw_text: str, max_lines: int = 3) -> str:
    """Format clean rolling 2-3 line scrollback snippet."""
    clean = strip_terminal_sequences(raw_text)
    filtered = []
    for line in clean.splitlines():
        s = line.strip()
        if not s:
            continue
        if all(c in "─━│┃-=_* " for c in s):
            continue
        if any(c in "▄▀█▌▐" for c in s):
            continue
        if s in (">", "> "):
            continue
        lower = s.lower()
        if any(x in lower for x in ["antigravity cli", "gemini 3.8", "? for shortcuts", "esc to cancel", "last progress:"]):
            continue
        filtered.append(s)
    
    tail = filtered[-max_lines:]
    return "\n".join(tail) if tail else "(waiting for output...)"


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
    """Build Slack Block Kit elements for the living status card."""
    if status_phase == "TASK COMPLETE":
        header_text = f"✅ FIRST MATE OPS ({workspace_id})"
    elif status_phase == "TASK FAILED":
        header_text = f"❌ FIRST MATE OPS ({workspace_id})"
    elif status_phase == "APPROVAL REQUIRED":
        header_text = f"⚠️ FIRST MATE OPS ({workspace_id})"
    else:
        header_text = f"⚙️ FIRST MATE OPS ({workspace_id})"

    blocks: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": header_text,
                "emoji": True
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Machine Node:*\n`{machine_label}`"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Active Runner:*\n`{runner_label}`"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Status Phase:*\n*{status_phase}*"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Elapsed Time:*\n`{elapsed_seconds}s`"
                }
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Current Action:*\n`{current_action or 'Initializing runner environment...'}`"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"```{terminal_tail}```"
            }
        }
    ]

    # Additional banners for completed / failed cards
    if status_phase == "TASK COMPLETE" and artifacts:
        urls = artifacts.get("urls", [])
        files = artifacts.get("files", [])
        details = []
        if urls:
            details.append(f"🌐 *Live Endpoint:* {urls[0]}")
        if files:
            details.append(f"📁 *Files Created:* {', '.join(files)}")
        if details:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join(details)
                }
            })

    if status_phase == "TASK FAILED" and error_detail:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"❌ *Error Detail:*\n```{error_detail}```"
            }
        })

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "Out-of-band PTY telemetry active • Throttled to 1.5s"
            }
        ]
    })
    return blocks


class SessionTelemetryObserver:
    """
    Asynchronous telemetry observer loop querying native Herdr OS-level PTY state
    and updating in-place Slack progress cards with rate limits and state hashing.
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

        self.card_msg_ts: Optional[str] = None
        self.start_time: float = time.time()
        self.paused_elapsed: Optional[int] = None
        self.last_state_hash: Optional[str] = None
        self.last_update_time: float = 0.0
        self.last_runner_label: str = "agy"
        self.last_machine_label: str = resolve_machine_label()
        self.current_action: str = "Initializing runner environment..."
        self.terminal_tail: str = "(waiting for output...)"

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
        """Post the initial living Slack status card or iMessage notification."""
        if not self.is_imessage and hasattr(self.client, "chat_postMessage"):
            blocks = build_status_card_blocks(
                workspace_id=self.workspace_id,
                machine_label=self.last_machine_label,
                runner_label=self.last_runner_label,
                status_phase=PHASE_MAP.get(getattr(self.session, "status", "starting"), "BOOTING"),
                elapsed_seconds=0,
                current_action=self.current_action,
                terminal_tail=self.terminal_tail
            )
            try:
                resp = await self.client.chat_postMessage(
                    channel=self.channel,
                    thread_ts=self.thread_ts,
                    text=f"⚙️ FIRST MATE OPS ({self.workspace_id})",
                    blocks=blocks
                )
                if resp:
                    ts_val = None
                    if isinstance(resp, dict):
                        ts_val = resp.get("ts")
                    elif hasattr(resp, "data") and isinstance(resp.data, dict):
                        ts_val = resp.data.get("ts")
                    elif hasattr(resp, "get") and not inspect.iscoroutinefunction(resp.get):
                        try:
                            res = resp.get("ts")
                            if not inspect.iscoroutine(res):
                                ts_val = res
                        except Exception:
                            pass
                    if isinstance(ts_val, (str, int)):
                        self.card_msg_ts = str(ts_val)
            except Exception as e:
                logger.debug(f"Error posting initial telemetry status card: {e}")
        elif self.is_imessage and hasattr(self.client, "chat_postMessage"):
            # BlueBubbles milestone 1: Initial placement
            try:
                await self.client.chat_postMessage(
                    channel=self.channel,
                    text=f"⚙️ [Herdr] Started task on {self.last_machine_label} with runner {self.last_runner_label}."
                )
            except Exception as e:
                logger.debug(f"Error posting BlueBubbles milestone: {e}")

    async def run(self) -> None:
        """Main observation loop executing out-of-band PTY queries."""
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
        """Query native Herdr state, detect subagents, hash state, and update Slack card."""
        current_status = getattr(self.session, "status", "working")
        now = time.time()

        # 1. Calculate elapsed time (freezing timer if BLOCKED in Ask Mode)
        if current_status == "blocked":
            if self.paused_elapsed is None:
                self.paused_elapsed = int(now - self.start_time)
            elapsed = self.paused_elapsed
            status_phase = "APPROVAL REQUIRED"
        else:
            if self.paused_elapsed is not None:
                self.start_time = now - self.paused_elapsed
                self.paused_elapsed = None
            elapsed = int(now - self.start_time)
            status_phase = PHASE_MAP.get(current_status, "WORKING")

        # 2. Workspace Pane Hierarchy & Subagent Delegation Detection
        target_machine = None
        child_runners: List[str] = []
        active_pane_id = getattr(self.session, "pane_id", None)
        active_agent_target = self.agent_name

        # Query workspace inspect (or fallback to get)
        code, ws_out, _ = await self.herdr_client.run_cli(["workspace", "inspect", self.workspace_id, "--json"])
        if code != 0 or not ws_out:
            code, ws_out, _ = await self.herdr_client.run_cli(["workspace", "get", self.workspace_id])

        if code == 0 and ws_out:
            try:
                ws_data = json.loads(ws_out)
                # Parse panes array
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

        # Also inspect agent list for any active delegated workers
        agent_code, agent_out, _ = await self.herdr_client.run_cli(["agent", "list"])
        if agent_code == 0 and agent_out:
            try:
                adata = json.loads(agent_out)
                for a in adata.get("result", {}).get("agents", []):
                    akind = a.get("agent")
                    if akind and akind.lower() not in ("agy", "hermes"):
                        if a.get("agent_status") in ("working", "starting", "idle"):
                            child_runners.append(akind)
                            active_agent_target = a.get("name", active_agent_target)
            except Exception:
                pass

        machine_label = resolve_machine_label(target_machine, getattr(self.session, "target_cwd", None))
        runner_label = format_runner_label("agy", child_runners)

        # BlueBubbles milestone 2: Delegation alert
        if self.is_imessage and runner_label != self.last_runner_label and "➔" in runner_label:
            try:
                subagent_name = runner_label.split("➔")[-1].strip()
                await self.client.chat_postMessage(
                    channel=self.channel,
                    text=f"⚡️ [Herdr] Delegated task to {subagent_name} on {machine_label.split('(')[0].strip()}."
                )
            except Exception as e:
                logger.debug(f"Error posting BlueBubbles delegation milestone: {e}")

        self.last_runner_label = runner_label
        self.last_machine_label = machine_label

        # 3. Foreground Process Inspection
        cmdline = ""
        # Try agent inspect --json
        code, p_out, _ = await self.herdr_client.run_cli(["agent", "inspect", active_agent_target, "--json"])
        if code == 0 and p_out:
            try:
                p_data = json.loads(p_out)
                fg = p_data.get("foreground_process", {}) or p_data
                cmdline = fg.get("cmdline") or fg.get("name", "")
            except Exception:
                pass

        # Fallback to pane process-info
        if not cmdline and active_pane_id:
            code, pi_out, _ = await self.herdr_client.run_cli(["pane", "process-info", "--pane", active_pane_id])
            if code == 0 and pi_out:
                try:
                    p_info = json.loads(pi_out).get("result", {}).get("process_info", {})
                    procs = p_info.get("foreground_processes", [])
                    if procs:
                        cmdline = procs[0].get("cmdline") or procs[0].get("name", "")
                except Exception:
                    pass

        # 4. Clean Terminal Tail & Action Extraction
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

        terminal_tail = format_terminal_tail(read_out if code == 0 else "")
        self.terminal_tail = terminal_tail

        # Derive current action
        if cmdline and not any(cmdline.strip().startswith(x) for x in ("agy", "opencode", "zsh", "bash", "sh")):
            current_action = cmdline.strip()
        else:
            action_match = re.search(r"●\s*([a-zA-Z0-9_\-/\.]+.*?)(?:\n|$)", read_out or "")
            if action_match:
                current_action = action_match.group(1).strip()
            elif "thought for" in (read_out or "").lower() or "thinking" in (read_out or "").lower():
                current_action = "Synthesizing execution strategy..."
            elif current_status == "done":
                current_action = "Execution completed."
            elif current_status == "blocked":
                current_action = "Awaiting human confirmation."
            else:
                current_action = "Processing in workspace..."

        self.current_action = current_action

        # 5. State Hashing & Throttling
        payload_str = f"{machine_label}|{runner_label}|{status_phase}|{current_action}|{terminal_tail}"
        state_hash = hashlib.sha256(payload_str.encode()).hexdigest()

        # Update Slack card only when hash changes AND min_update_interval has passed
        if (state_hash != self.last_state_hash) and (now - self.last_update_time >= self.min_update_interval):
            self.last_state_hash = state_hash
            self.last_update_time = now

            if not self.is_imessage and self.card_msg_ts and hasattr(self.client, "chat_update"):
                blocks = build_status_card_blocks(
                    workspace_id=self.workspace_id,
                    machine_label=machine_label,
                    runner_label=runner_label,
                    status_phase=status_phase,
                    elapsed_seconds=elapsed,
                    current_action=current_action,
                    terminal_tail=terminal_tail
                )
                try:
                    await self.client.chat_update(
                        channel=self.channel,
                        ts=self.card_msg_ts,
                        text=f"⚙️ FIRST MATE OPS ({self.workspace_id})",
                        blocks=blocks
                    )
                except Exception as e:
                    logger.debug(f"Error updating status card in-place: {e}")

    async def settle_complete(
        self,
        artifacts: Optional[Dict[str, List[str]]] = None,
        terminal_output: str = ""
    ) -> None:
        """Settle the status card to TASK COMPLETE and stop observer."""
        self.stop_event.set()
        elapsed = int(time.time() - self.start_time)
        clean_tail = format_terminal_tail(terminal_output) if terminal_output else self.terminal_tail

        if not self.is_imessage and self.card_msg_ts and hasattr(self.client, "chat_update"):
            blocks = build_status_card_blocks(
                workspace_id=self.workspace_id,
                machine_label=self.last_machine_label,
                runner_label=self.last_runner_label,
                status_phase="TASK COMPLETE",
                elapsed_seconds=elapsed,
                current_action="Task completed successfully.",
                terminal_tail=clean_tail,
                artifacts=artifacts
            )
            try:
                await self.client.chat_update(
                    channel=self.channel,
                    ts=self.card_msg_ts,
                    text=f"✅ FIRST MATE OPS ({self.workspace_id})",
                    blocks=blocks
                )
            except Exception as e:
                logger.debug(f"Error settling complete status card: {e}")

    async def settle_failed(
        self,
        error_detail: str,
        terminal_output: str = ""
    ) -> None:
        """Settle the status card to TASK FAILED and stop observer."""
        self.stop_event.set()
        elapsed = int(time.time() - self.start_time)
        clean_tail = format_terminal_tail(terminal_output) if terminal_output else self.terminal_tail

        if not self.is_imessage and self.card_msg_ts and hasattr(self.client, "chat_update"):
            blocks = build_status_card_blocks(
                workspace_id=self.workspace_id,
                machine_label=self.last_machine_label,
                runner_label=self.last_runner_label,
                status_phase="TASK FAILED",
                elapsed_seconds=elapsed,
                current_action="Task aborted.",
                terminal_tail=clean_tail,
                error_detail=error_detail
            )
            try:
                await self.client.chat_update(
                    channel=self.channel,
                    ts=self.card_msg_ts,
                    text=f"❌ FIRST MATE OPS ({self.workspace_id})",
                    blocks=blocks
                )
            except Exception as e:
                logger.debug(f"Error settling failed status card: {e}")
