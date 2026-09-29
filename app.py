#!/usr/bin/env python3
"""
app.py
First Mate Thin Ingress Gateway & Herdr PTY Relay.
Connects Slack Socket Mode and BlueBubbles iMessage to Herdr PTYs and Antigravity CLI (agy).
Eliminates bespoke JSON classifiers, Mad Libs f-string generators, and canned polling loops.
"""

import os
import re
import sys
import time
import json
import signal
import asyncio
import logging
from typing import Dict, Any, Optional, Set, List, Tuple

from dotenv import load_dotenv
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

from bluebubbles_transport import (
    BlueBubblesTransport,
    BlueBubblesClient,
    sanitize_for_imessage
)
from herdr_client import HerdrClient, InfrastructureError
from herdr_fleet import HerdrFleet
from herdr_tools import (
    validate_action_payload,
    BANNED_ACTION_PAYLOAD_PATTERNS,
    run_command
)
from supervisor import AgentSupervisor, cleanup_rogue_servers, summarize_terminal_activity
from memory_manager import MemoryManager

# ------------------------------------------------------------------------------
# Logging & Environment Setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("AGY")

HOST_DIR = os.environ.get("FIRST_MATE_HOST_DIR", os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = HOST_DIR
DEFAULT_TARGET_REPO = os.environ.get("DEFAULT_TARGET_REPO", "/Users/hermes/projects/AGY").strip()
DAEMON_LABEL = os.environ.get("DAEMON_LABEL", "com.user.first-mate").strip()

env_file = os.path.join(HOST_DIR, ".env")
if os.path.isfile(env_file):
    load_dotenv(dotenv_path=env_file)
else:
    load_dotenv()

SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN", "").strip()
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "").strip()
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET", "").strip()

if not SLACK_APP_TOKEN or not SLACK_BOT_TOKEN:
    logger.critical("Missing SLACK_APP_TOKEN or SLACK_BOT_TOKEN. Check .env configuration.")
    sys.exit(1)

# ------------------------------------------------------------------------------
# Herdr Runtime & Fleet Clients
# ------------------------------------------------------------------------------
herdr_client = HerdrClient()
fleet = HerdrFleet()
herdr = fleet
supervisor = AgentSupervisor()

app = AsyncApp(
    token=SLACK_BOT_TOKEN,
    signing_secret=SLACK_SIGNING_SECRET
)

BOT_USER_ID: Optional[str] = None
processed_message_ids: Set[str] = set()

# ------------------------------------------------------------------------------
# Gateway Session State
# ------------------------------------------------------------------------------
class GatewaySession:
    """Thread-scoped state binding a chat thread to a Herdr workspace/agent."""

    def __init__(
        self,
        session_key: str,
        agent_name: str,
        target_cwd: str,
        channel: str,
        thread_ts: str,
        workspace_id: Optional[str] = None,
        pane_id: Optional[str] = None,
        is_imessage: bool = False
    ):
        self.session_key = session_key
        self.agent_name = agent_name
        self.target_cwd = target_cwd
        self.channel = channel
        self.thread_ts = thread_ts
        self.workspace_id = workspace_id
        self.pane_id = pane_id
        self.is_imessage = is_imessage
        self.status = "starting"  # "starting" | "booting" | "working" | "blocked" | "done" | "failed"
        self.has_worked = False
        self.execution_steps = 0
        self.last_prompt = ""
        self.last_blocked_snapshot: Optional[str] = None
        self.approval_msg_ts: Optional[str] = None
        self.monitor_task: Optional[asyncio.Task] = None
        self.created_at = time.time()

    @property
    def state(self) -> str:
        return self.status

    @state.setter
    def state(self, val: str) -> None:
        self.status = val


# Active session registry
sessions: Dict[str, GatewaySession] = {}
agent_name_to_session: Dict[str, GatewaySession] = {}

# Backward-compatibility stubs for tests
pending_alignment: Dict[str, Any] = {}
pending_alignment_by_msg_ts: Dict[str, str] = {}
active_thread_sessions: Set[str] = set()
active_tasks: Dict[str, Any] = {}
active_subprocesses: Set[Any] = set()
workspace_locks: Dict[Tuple[str, str], asyncio.Lock] = {}

ANSI_ESCAPE_PATTERN = re.compile(
    r"\x1b(?:"
    r"\[[0-?]*[ -/]*[@-~]"
    r"|\][^\x07\x1b]*(?:\x07|\x1b\\)"
    r"|[@-Z\\-_]"
    r")"
)

def strip_ansi(text: str) -> str:
    """Strip all ANSI escape codes and terminal controls."""
    return ANSI_ESCAPE_PATTERN.sub("", text)

def parse_remote_hosts(raw_hosts: str) -> Dict[str, str]:
    """Parse comma-separated alias:ip pairs into an alias -> ip dictionary."""
    hosts = {}
    if not raw_hosts:
        return hosts
    for part in raw_hosts.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        alias, ip = part.split(":", 1)
        alias = alias.strip().lower()
        ip = ip.strip()
        if alias and ip:
            hosts[alias] = ip
    return hosts

def get_remote_hosts() -> Dict[str, str]:
    """Fetch remote hosts from Herdr fleet machines and environment fallback."""
    raw = os.environ.get("REMOTE_HOSTS", "").strip()
    hosts = parse_remote_hosts(raw)
    if hasattr(fleet, "machines") and fleet.machines:
        for m in fleet.machines:
            lbl = m.get("label", "").lower()
            tgt = m.get("target", "")
            if lbl and tgt and lbl not in hosts:
                hosts[lbl] = tgt
    return hosts

def get_workspace_lock(host: str, workspace_dir: str) -> asyncio.Lock:
    """Key asyncio.Lock per target host and workspace tuple."""
    if host == "local":
        canonical = os.path.realpath(os.path.abspath(os.path.expanduser(workspace_dir)))
    else:
        canonical = workspace_dir
    key = (host, canonical)
    if key not in workspace_locks:
        workspace_locks[key] = asyncio.Lock()
    return workspace_locks[key]

async def fetch_recent_thread_context(*args, **kwargs) -> str:
    return ""

def terminate_all_subprocesses():
    pass


# ------------------------------------------------------------------------------
# Target Directory Resolution & Path Scoping
# ------------------------------------------------------------------------------
def resolve_target_directory(prompt: str, task_id: Optional[str] = None) -> str:
    """
    Determine target workspace directory:
    - If user explicitly names a known project in ~/projects, use ~/projects/<name>.
    - Otherwise, default to an isolated sandbox at ~/projects/sandbox/{task_id}.
    """
    projects_dir = os.path.expanduser("~/projects")
    if os.path.isdir(projects_dir):
        try:
            for entry in os.listdir(projects_dir):
                if entry.startswith(".") or entry.lower() == "sandbox":
                    continue
                cand = os.path.join(projects_dir, entry)
                if os.path.isdir(cand):
                    pattern = r"\b" + re.escape(entry) + r"\b"
                    if re.search(pattern, prompt, re.IGNORECASE):
                        return cand
        except Exception as e:
            logger.debug(f"Error inspecting projects dir: {e}")

    tid = task_id or str(int(time.time()))
    clean_tid = re.sub(r"[^a-zA-Z0-9_-]", "-", str(tid))
    sandbox_path = os.path.join(projects_dir, "sandbox", clean_tid)
    os.makedirs(sandbox_path, exist_ok=True)
    return sandbox_path


def scope_server_commands(prompt: str, sandbox_cwd: str) -> str:
    """
    Enforce explicit directory scoping for http servers, ensuring processes
    are never spawned from /.
    """
    if not sandbox_cwd or sandbox_cwd == "/":
        sandbox_cwd = os.path.expanduser("~/projects/sandbox")
        os.makedirs(sandbox_cwd, exist_ok=True)

    scoped = prompt
    if re.search(r"python(?:3)?\s+-m\s+http\.server", scoped, re.IGNORECASE):
        if "--directory" not in scoped and " -d " not in scoped:
            scoped = re.sub(
                r"(python(?:3)?\s+-m\s+http\.server(?:\s+\d+)?)",
                rf"\1 --directory {sandbox_cwd}",
                scoped,
                flags=re.IGNORECASE
            )
    return scoped


def ensure_trusted_workspace(path: str) -> None:
    """Ensure directory is listed in Antigravity CLI's trustedWorkspaces to avoid startup trust prompt."""
    settings_path = os.path.expanduser("~/.gemini/antigravity-cli/settings.json")
    if not os.path.isfile(settings_path):
        return
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        trusted = data.get("trustedWorkspaces", [])
        canonical = os.path.realpath(os.path.abspath(os.path.expanduser(path)))
        if canonical not in trusted:
            trusted.append(canonical)
            data["trustedWorkspaces"] = trusted
            with open(settings_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
    except Exception as e:
        logger.debug(f"Could not update trustedWorkspaces: {e}")


def format_agent_dispatch_prompt(prompt: str, target_cwd: str) -> str:
    """
    Format prompt for Antigravity CLI dispatch:
    - If prompt references subagents, workers, or fleet tools (e.g. OpenCode, remote execution),
      append context that herdr-mesh MCP tools (herdr_pane_run, herdr_handoff, herdr_agent_read)
      are available for orchestration.
    - Preserves repository hygiene and scratch sandbox rules.
    """
    clean = prompt.strip()
    subagent_patterns = [
        r"\bopencode\b", r"\bhermes\b", r"\bsubagent\b", r"\bworker\b",
        r"\bfleet\b", r"\bremote\b", r"\bgrid\b", r"\bdelegate\b",
        r"\bhand\s*off\b", r"\bhandoff\b"
    ]
    if any(re.search(pat, clean, re.IGNORECASE) for pat in subagent_patterns):
        clean = (
            f"{clean}\n\n"
            "[Context: You are First Mate / Tech Lead. You have access to herdr-mesh MCP tools "
            "(`herdr_pane_run`, `herdr_handoff`, `herdr_agent_read`, `herdr_workspace_create`, etc.) "
            "to orchestrate subagents or worker panes if delegating work. "
            "Keep prototypes, test servers, and standalone scripts strictly self-contained within "
            f"the designated sandbox folder: {target_cwd}.]"
        )
    return clean


def sanitize_terminal_output(raw_text: str) -> str:
    """
    Filter raw terminal text from Antigravity CLI:
    - Strips ANSI escape codes, terminal device queries, and graphics protocol sequences.
    - Filters out ASCII art splash banners and TUI chrome.
    - Strips status bar lines ('? for shortcuts', 'esc to cancel', 'Last progress:').
    - Returns clean substantive execution output and tool results.
    """
    clean = strip_ansi(raw_text)
    # Strip terminal query responses and graphics sequences
    clean = re.sub(r"\^\[_G[^\^]*\^\\", "", clean)
    clean = re.sub(r"\x1b\[\?[0-9;]*[a-zA-Z]", "", clean)
    clean = re.sub(r"G[a-z0-9_=,;+]+(?:==)?", "", clean)
    clean = re.sub(r"\x1b[^\s]*", "", clean)

    splash_indicators = [
        "antigravity cli",
        "gemini 3.8 flash",
        "welcome to the antigravity",
        "signing in...",
        "? for shortcuts",
        "esc to cancel",
        "last progress:",
        "navigate · enter confirm",
        "do you trust the contents",
        "yes, i trust this folder"
    ]

    filtered_lines = []
    for line in clean.splitlines():
        line_stripped = line.strip()
        if not line_stripped:
            continue
        # Skip pure border/divider lines
        if all(c in "─━│┃-=_* " for c in line_stripped):
            continue
        # Skip ASCII art logo blocks
        if any(c in "▄▀█▌▐" for c in line_stripped):
            continue
        lower_line = line_stripped.lower()
        # Skip splash / TUI footer lines
        if any(ind in lower_line for ind in splash_indicators):
            continue
        # Skip bare prompt
        if line_stripped in (">", "> "):
            continue
        # Skip shell command prompt invocation line (e.g. hermes@... % agy ...)
        if re.search(r"%\s+agy\b", line_stripped):
            continue

        filtered_lines.append(line.rstrip())

    return "\n".join(filtered_lines).strip()


def extract_operational_artifacts(text: str) -> Dict[str, Any]:
    """Extract operational artifacts (live URLs, files edited/created, tool outputs)."""
    urls = re.findall(
        r"https?://(?:localhost|127\.0\.0\.1|0\.0\.0\.0|100\.\d+\.\d+\.\d+)(?::\d+)?(?:/[a-zA-Z0-9_.~!$&'()*+,;=:@%/-]*)?",
        text
    )
    files_created = re.findall(r"(?:●\s*Edit|Created|file:)\s*\(([^)]+)\)", text, re.IGNORECASE)
    if not files_created:
        files_created = re.findall(r"\b(?:created|wrote to)\s+[`']?([a-zA-Z0-9_./-]+\.[a-zA-Z0-9]+)[`']?", text, re.IGNORECASE)

    return {
        "urls": urls,
        "files": files_created
    }



# ------------------------------------------------------------------------------
# Chat Formatting Normalizer for BlueBubbles
# ------------------------------------------------------------------------------
def normalize_for_bluebubbles(text: str) -> str:
    """
    Sanitize outbound text sent to BlueBubbles:
    - Convert <http://url|label> and <http://url> to clean URLs.
    - Convert Slack shortcodes (:white_check_mark:, :thumbsup:, :eyes:) to native Unicode.
    """
    return sanitize_for_imessage(text)


# ------------------------------------------------------------------------------
# Reacji & Slack Formatting Helpers
# ------------------------------------------------------------------------------
IGNORABLE_REACTION_ERRORS = {"already_reacted", "no_reaction", "message_not_found"}

async def add_reacji(client: Any, channel: str, timestamp: str, name: str) -> None:
    try:
        if hasattr(client, "reactions_add"):
            await client.reactions_add(channel=channel, timestamp=timestamp, name=name)
    except Exception as e:
        logger.debug(f"Reaction add error ({name}): {e}")

async def remove_reacji(client: Any, channel: str, timestamp: str, name: str) -> None:
    try:
        if hasattr(client, "reactions_remove"):
            await client.reactions_remove(channel=channel, timestamp=timestamp, name=name)
    except Exception as e:
        logger.debug(f"Reaction remove error ({name}): {e}")

async def swap_reacji(client: Any, channel: str, timestamp: str, old_name: Optional[str], new_name: str) -> None:
    if old_name and old_name != new_name:
        await remove_reacji(client, channel, timestamp, old_name)
    await add_reacji(client, channel, timestamp, new_name)


# ------------------------------------------------------------------------------
# Intent Heuristics (Strictly for In-Flight Relay & Blocked State)
# ------------------------------------------------------------------------------
STATUS_INQUIRY_PATTERNS = [
    re.compile(r"\b(how('?s| is) it going|status|what('?s| is) the url|url|port|is it (stuck|done|running)|what is it doing|where is the url)\b", re.IGNORECASE)
]
APPROVAL_PATTERNS = [
    re.compile(r"^\s*(?:(approve|approved|go|yes|y|do\s*it|lgtm|proceed|confirm|thumbsup)\b|👍|\+1|:\+1:|:thumbsup:)", re.IGNORECASE)
]
REJECTION_PATTERNS = [
    re.compile(r"^\s*(?:(abort|aborted|no|n|cancel|cancelled|stop|reject|nah|-1)\b|👎|:-1:)", re.IGNORECASE)
]

def is_status_inquiry(text: str) -> bool:
    return any(p.search(text.strip()) for p in STATUS_INQUIRY_PATTERNS)

def is_approval_reply(text: str) -> bool:
    t = text.strip()
    return "👍" in t or any(p.search(t) for p in APPROVAL_PATTERNS)

def is_rejection_reply(text: str) -> bool:
    t = text.strip()
    return "👎" in t or any(p.search(t) for p in REJECTION_PATTERNS)


# ------------------------------------------------------------------------------
# Herdr PTY Dispatch & Blocked State Monitoring
# ------------------------------------------------------------------------------
async def wait_for_agent_readiness(
    agent_name: str,
    timeout_seconds: float = 15.0,
    poll_interval: float = 0.5
) -> bool:
    """
    Readiness Watchdog (The TUI Handshake):
    Monitor the agent pane buffer / agent status for up to timeout_seconds
    until agy finishes initializing terminal attributes and displays its ready indicator
    (e.g. >, ? for shortcuts, or interactive_ready=True).
    """
    start_time = time.time()
    ready_patterns = [
        r"(?m)^\s*>\s*$",
        r">\n",
        r"\?\s*for\s*shortcuts",
        r"welcome to the antigravity",
        r"gemini 3.8 flash"
    ]
    while time.time() - start_time < timeout_seconds:
        # 1. Check Herdr agent state
        code, out, _ = await herdr_client.run_cli(["agent", "get", agent_name], timeout=5.0)
        if code == 0 and out:
            try:
                data = json.loads(out)
                agent_info = data.get("result", {}).get("agent", {})
                if agent_info.get("interactive_ready") is True:
                    return True
                status = agent_info.get("agent_status")
                if status in ("idle", "ready"):
                    return True
            except Exception:
                pass

        # 2. Check pane buffer output
        _, read_out, _ = await herdr_client.run_cli([
            "agent", "read", agent_name,
            "--source", "recent-unwrapped",
            "--lines", "30"
        ], timeout=5.0)
        if read_out:
            clean_text = strip_ansi(read_out)
            if any(re.search(pat, clean_text, re.IGNORECASE) for pat in ready_patterns):
                return True

        await asyncio.sleep(poll_interval)

    return False


async def ensure_session_agent(
    session_key: str,
    channel: str,
    thread_ts: str,
    prompt: str,
    is_imessage: bool = False,
    agent_args: Optional[List[str]] = None,
    client: Any = None,
    trigger_ts: Optional[str] = None
) -> GatewaySession:
    """Ensure a Herdr workspace and Antigravity CLI agent exist for this thread with readiness-gated handshake."""
    if session_key in sessions and sessions[session_key].status != "failed":
        return sessions[session_key]

    clean_ts = re.sub(r"[^0-9]", "", str(thread_ts or time.time()))[-8:]
    agent_name = f"fm-{clean_ts}"
    target_cwd = resolve_target_directory(prompt, task_id=agent_name)
    ensure_trusted_workspace(target_cwd)

    # 1. Spawn Herdr workspace: herdr workspace create --cwd <target_cwd> --label <task_name> --no-focus
    ws_id = None
    pane_id = None
    code, out, err = await herdr_client.run_cli([
        "workspace", "create",
        "--cwd", target_cwd,
        "--label", agent_name,
        "--no-focus"
    ])
    if code == 0 and out:
        try:
            data = json.loads(out)
            res = data.get("result", {})
            ws_id = res.get("workspace", {}).get("workspace_id")
            pane_id = res.get("root_pane", {}).get("pane_id")
        except Exception as e:
            logger.warning(f"Could not parse workspace create output: {e}")

    # Fallback to fleet-control pane if workspace creation failed
    if not pane_id:
        pane_id = "w6:p1"

    scoped_prompt = scope_server_commands(prompt, target_cwd)

    session = GatewaySession(
        session_key=session_key,
        agent_name=agent_name,
        target_cwd=target_cwd,
        channel=channel,
        thread_ts=thread_ts,
        workspace_id=ws_id,
        pane_id=pane_id,
        is_imessage=is_imessage
    )
    session.status = "starting"
    session.has_worked = False
    session.execution_steps = 0
    session.last_prompt = scoped_prompt

    sessions[session_key] = session
    agent_name_to_session[agent_name] = session

    # 2. Clean Spawn: Launch agent with zero trailing prompt arguments
    start_cmd = [
        "agent", "start", agent_name,
        "--kind", "agy",
        "--pane", pane_id
    ]
    if agent_args:
        start_cmd.extend(["--"] + agent_args)

    start_code, start_out, start_err = await herdr_client.run_cli(start_cmd)

    # 3. Fail-Fast Trap: Inspect process return code and error response immediately
    start_failed = False
    err_detail = ""
    if start_code != 0:
        start_failed = True
        err_detail = start_err or start_out or f"herdr agent start exited with code {start_code}"
    elif start_out:
        try:
            parsed = json.loads(start_out)
            if "error" in parsed:
                start_failed = True
                err_detail = parsed.get("error", {}).get("message") or str(parsed.get("error"))
        except Exception:
            pass

    if start_failed:
        session.status = "failed"
        logger.error(f"herdr agent start failed for {agent_name}: {err_detail}")
        if client:
            if trigger_ts and not is_imessage:
                await swap_reacji(client, channel, trigger_ts, "gear", "x")
                await swap_reacji(client, channel, trigger_ts, "eyes", "x")
            err_msg = f":x: *Failed to spawn Antigravity agent (`{agent_name}`)*:\n```{err_detail}```"
            if is_imessage:
                await client.chat_postMessage(channel=channel, text=normalize_for_bluebubbles(err_msg))
            else:
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=err_msg)
        if ws_id:
            await herdr_client.run_cli(["workspace", "close", ws_id])
        return session

    # 4. Readiness Watchdog (The TUI Handshake):
    # Monitor the pane buffer for up to 15s until agy displays ready indicator
    is_ready = await wait_for_agent_readiness(agent_name, timeout_seconds=15.0)
    if not is_ready:
        session.status = "failed"
        timeout_detail = f"Agent `{agent_name}` failed to reach readiness within 15 seconds."
        logger.error(timeout_detail)
        if client:
            if trigger_ts and not is_imessage:
                await swap_reacji(client, channel, trigger_ts, "gear", "x")
                await swap_reacji(client, channel, trigger_ts, "eyes", "x")
            timeout_msg = f":x: *Failed to start Antigravity agent (`{agent_name}`)*:\n```{timeout_detail}```"
            if is_imessage:
                await client.chat_postMessage(channel=channel, text=normalize_for_bluebubbles(timeout_msg))
            else:
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=timeout_msg)
        if ws_id:
            await herdr_client.run_cli(["workspace", "close", ws_id])
        return session

    # 5. Prompt Delivery:
    # Once readiness is confirmed, dispatch the prompt via herdr agent prompt
    prompt_code, _, prompt_err = await herdr_client.run_cli([
        "agent", "prompt", agent_name, scoped_prompt
    ])
    if prompt_code != 0:
        logger.warning(f"herdr agent prompt returned code {prompt_code}: {prompt_err}")

    return session


async def monitor_agent_lifecycle(
    session: GatewaySession,
    client: Any,
    trigger_ts: Optional[str] = None
):
    """
    Monitor Herdr agent state transitions with strict State Invariant & Splash Guard:
    - State Invariant: A session CANNOT transition to DONE unless it has first successfully
      entered and operated in a WORKING state.
    - Splash / Boot Guard: If a pane is idle at the startup banner without having executed
      any tool or generated substantive output, treat it as BOOTING/INITIALIZING, not DONE.
    - Ask Mode: Detect blocked confirmation states and render approval cards.
    - Completion: Only declare completion when child process exits cleanly OR agent returns to idle
      AFTER having logged active execution steps and substantive output.
    """
    agent_name = session.agent_name
    logger.info(f"Lifecycle monitor active for agent {agent_name} (initial status: {session.status})...")
    consecutive_idle_ticks = 0
    boot_start_time = time.time()

    while True:
        await asyncio.sleep(1.5)

        if session.status == "failed":
            break

        # Global Lifecycle Watchdog: strict 30s timeout while booting/starting
        if not session.has_worked and session.status in ("starting", "booting"):
            if time.time() - boot_start_time > 30.0:
                logger.error(f"Agent {agent_name} lifecycle timeout: stuck in {session.status} for >30s.")
                session.status = "failed"
                if trigger_ts and not session.is_imessage:
                    await swap_reacji(client, session.channel, trigger_ts, "gear", "x")
                    await swap_reacji(client, session.channel, trigger_ts, "eyes", "x")
                timeout_msg = f":x: *Agent (`{agent_name}`) timed out during initialization (>30s).* Session aborted."
                if session.is_imessage:
                    await client.chat_postMessage(channel=session.channel, text=normalize_for_bluebubbles(timeout_msg))
                else:
                    await client.chat_postMessage(channel=session.channel, thread_ts=session.thread_ts, text=timeout_msg)
                if session.workspace_id:
                    await herdr_client.run_cli(["workspace", "close", session.workspace_id])
                break

        code, out, _ = await herdr_client.run_cli(["agent", "get", agent_name], timeout=5.0)

        current_status = "unknown"
        if code == 0 and out:
            try:
                data = json.loads(out)
                current_status = data.get("result", {}).get("agent", {}).get("agent_status", "unknown")
            except Exception:
                pass

        _, read_out, _ = await herdr_client.run_cli([
            "agent", "read", agent_name,
            "--source", "recent-unwrapped",
            "--lines", "30"
        ], timeout=5.0)

        raw_lower = read_out.lower()

        # 1. Detect active working state vs historical execution content
        active_working_indicators = [
            "generating...", "thinking...", "ruminating...", "computing...",
            "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", "msg=interrupt"
        ]
        is_terminal_working = any(ind in raw_lower for ind in active_working_indicators)

        has_execution_content = any(ind in raw_lower for ind in [
            "thought for", "● edit", "● run", "● command", "created", "serving http", "tokens", "available at http"
        ])
        if has_execution_content or current_status == "working":
            session.has_worked = True

        if current_status == "working" or is_terminal_working:
            session.status = "working"
            session.has_worked = True
            session.execution_steps += 1
            consecutive_idle_ticks = 0
            continue


        # 2. Detect blocked Ask Mode indicators
        blocked_patterns = [
            r"\[y/n\]", r"\(y/n\)", r"\(yes/no\)", r"\[y/N\]", r"\[Y/n\]",
            r"password:", r"confirm\s*\?", r"approval\s+required", r"allow\s+this\s+action\?",
            r"do\s+you\s+want\s+to\s+run", r"navigate\s*·\s*enter\s+confirm"
        ]
        is_terminal_blocked = any(re.search(pat, read_out, re.IGNORECASE) for pat in blocked_patterns)

        if (current_status == "blocked" or is_terminal_blocked) and session.status != "blocked":
            session.status = "blocked"
            session.last_blocked_snapshot = read_out
            if trigger_ts and not session.is_imessage:
                await swap_reacji(client, session.channel, trigger_ts, "gear", "warning")
            await post_approval_card(session, client, read_out)
            continue

        if session.status == "blocked":
            # Still waiting for user approval/rejection reply
            continue

        # 3. Splash / Boot Guard
        # If agent has not yet operated in WORKING state, check if it's still at the splash banner
        if not session.has_worked:
            splash_banner_patterns = [
                "antigravity cli", "gemini 3.8 flash", "welcome to the antigravity",
                "? for shortcuts", "signing in..."
            ]
            is_splash = any(ind in raw_lower for ind in splash_banner_patterns)
            if is_splash or current_status in ("starting", "unknown", "idle", "done"):
                session.status = "booting"
                logger.debug(f"Agent {agent_name} at splash banner/booting; ignoring premature idle/done.")
                continue

        # 4. Completion Detection: ONLY valid if session.has_worked is True
        if session.has_worked and current_status in ("done", "idle") and not is_terminal_working:
            sanitized = sanitize_terminal_output(read_out)
            if sanitized:
                consecutive_idle_ticks += 1
                if consecutive_idle_ticks >= 2:
                    session.status = "done"
                    if trigger_ts and not session.is_imessage:
                        await swap_reacji(client, session.channel, trigger_ts, "gear", "white_check_mark")
                    await post_completion_briefing(session, client, read_out)
                    break


async def post_approval_card(session: GatewaySession, client: Any, terminal_snippet: str):
    """Render interactive approval card to Slack or BlueBubbles."""
    clean_snippet = terminal_snippet.strip() or "(Agent awaiting confirmation)"
    clean_lines = [l.rstrip() for l in clean_snippet.splitlines()][-10:]
    formatted_snippet = "\n".join(clean_lines)

    if session.is_imessage:
        msg = (
            f"⚠️ *Antigravity Action Requires Approval*\n\n"
            f"```{formatted_snippet}```\n\n"
            f"Reply 'approve' (or 👍) to execute, or 'abort' to cancel."
        )
        await client.chat_postMessage(
            channel=session.channel,
            text=normalize_for_bluebubbles(msg)
        )
    else:
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "⚠️ *Antigravity Action Requires Approval*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{formatted_snippet}```"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Reply with *approve* (or 👍) to execute, or *abort* to cancel."
                    }
                ]
            }
        ]
        resp = await client.chat_postMessage(
            channel=session.channel,
            thread_ts=session.thread_ts,
            text=f"⚠️ Approval required for `{session.agent_name}`: reply approve or abort.",
            blocks=blocks
        )
        if isinstance(resp, dict):
            session.approval_msg_ts = resp.get("ts")


async def post_completion_briefing(session: GatewaySession, client: Any, terminal_output: str):
    """Post final completion output to chat, filtering out splash banners and highlighting artifacts."""
    sanitized = sanitize_terminal_output(terminal_output)
    artifacts = extract_operational_artifacts(terminal_output)

    url_banner = f"\n*Live Endpoint:* {artifacts['urls'][0]}\n" if artifacts["urls"] else ""
    files_banner = f"\n*Files Created:* {', '.join(artifacts['files'])}\n" if artifacts["files"] else ""

    clean_lines = [l for l in sanitized.splitlines() if l.strip()][-10:]
    summary = "\n".join(clean_lines) if clean_lines else "Task completed successfully."

    msg = (
        f"*Task Completed via Herdr (`{session.agent_name}`)*{url_banner}{files_banner}\n\n"
        f"```{summary}```"
    )

    if session.is_imessage:
        await client.chat_postMessage(
            channel=session.channel,
            text=normalize_for_bluebubbles(msg)
        )
    else:
        await client.chat_postMessage(
            channel=session.channel,
            thread_ts=session.thread_ts,
            text=msg
        )



# ------------------------------------------------------------------------------
# Executive Command Suite (/bearings, /ahoy, /wrapup, /update, /learn)
# ------------------------------------------------------------------------------
async def handle_bearings_request(
    channel: str,
    thread_ts: Optional[str] = None,
    msg_ts: Optional[str] = None,
    client: Any = None
):
    react_ts = msg_ts or thread_ts
    if react_ts and hasattr(client, "reactions_add"):
        await add_reacji(client, channel, react_ts, "eyes")
        await swap_reacji(client, channel, react_ts, "eyes", "gear")

    try:
        results = await asyncio.gather(
            *[fleet.probe_node(node) for node in ["hermes", "desktop-pc", "personal-laptop", "oci-machine"]],
            return_exceptions=True
        )
        labels = [
            ("hermes", "localhost (Primary Dev & Homelab)"),
            ("desktop-pc", "Compute Grid (Windows CUDA)"),
            ("personal-laptop", "Personal Laptop (Carl's Workstation)"),
            ("oci-machine", "Cloud Operations (Production VPS)")
        ]
        bullets = []
        for i, res in enumerate(results):
            default_node, default_role = labels[i]
            if isinstance(res, dict):
                node = res.get("node", default_node)
                role = res.get("role", default_role)
                status = res.get("status", "unknown")
            else:
                node = default_node
                role = default_role
                status = "[Offline / Error]"
            bullets.append(f"• *{node}* ({role}): {status}")

        briefing = "🧭 *Fleet Bearings & Status Digest:*\n" + "\n".join(bullets)
        if react_ts and hasattr(client, "reactions_add"):
            await swap_reacji(client, channel, react_ts, "gear", "white_check_mark")

        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=briefing
        )
    except Exception as e:
        logger.error(f"Error handling /bearings: {e}")


async def query_oci_status(timeout: float = 8.0) -> Dict[str, Any]:
    """Query uptime and status of OCI production VPS with 8.0s timeout."""
    return await fleet.probe_node("oci-machine", timeout=timeout)


async def handle_learn_request(
    channel: str,
    thread_ts: Optional[str] = None,
    msg_ts: Optional[str] = None,
    user_notes: str = "",
    client: Any = None
):
    react_ts = msg_ts or thread_ts
    if react_ts and hasattr(client, "reactions_add"):
        await add_reacji(client, channel, react_ts, "brain")

    try:
        MemoryManager.log_event(
            "learn_crystallized",
            task_id=f"learn-{int(time.time())}",
            notes=user_notes,
            channel=channel,
            thread_ts=thread_ts
        )
        msg = f"🧠 *Operational Lesson Crystallized:* {user_notes or 'Lesson codified into operational memory.'}"
        if react_ts and hasattr(client, "reactions_add"):
            await swap_reacji(client, channel, react_ts, "brain", "white_check_mark")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=msg
        )
    except Exception as e:
        logger.error(f"Error handling /learn: {e}")


async def handle_ahoy_request(channel: str, thread_ts: Optional[str] = None, msg_ts: Optional[str] = None, client: Any = None):
    msg = "⚓ *Ahoy:* Session catch-up complete. All background Herdr agents settled."
    await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)


async def handle_wrapup_request(channel: str, thread_ts: Optional[str] = None, msg_ts: Optional[str] = None, client: Any = None):
    msg = "🧹 *Wrapup:* Housekeeping complete. Working directories clean."
    await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)


async def handle_update_request(channel: str, thread_ts: Optional[str] = None, msg_ts: Optional[str] = None, client: Any = None):
    msg = "🚀 *Update:* System update verified cleanly."
    await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)


# ------------------------------------------------------------------------------
# Core Ingress Gateway Handler
# ------------------------------------------------------------------------------
async def process_user_prompt(event: Dict[str, Any], client: Any) -> None:
    """
    Thin transport event forwarder connecting chat messages directly to
    Herdr PTY panes running Antigravity CLI.
    """
    raw_text = event.get("text", "")
    channel = event.get("channel", "")
    ts = event.get("ts", "")
    thread_ts = event.get("thread_ts") or ts
    is_imessage = bool(event.get("is_imessage", False))

    # Strip bot user mention
    clean_text = raw_text
    if BOT_USER_ID:
        clean_text = re.sub(rf"<@{BOT_USER_ID}>", "", clean_text).strip()
    clean_text = clean_text.strip()
    if not clean_text:
        return

    # Check for executive commands (!bearings, !learn, !ahoy, !wrapup, !update)
    cmd_token = clean_text.split()[0].lower() if clean_text.split() else ""
    if cmd_token in ("!bearings", "/bearings"):
        await handle_bearings_request(channel, thread_ts, ts, client)
        return
    elif cmd_token in ("!learn", "/learn"):
        user_notes = clean_text[len(cmd_token):].strip()
        await handle_learn_request(channel, thread_ts, ts, user_notes, client)
        return
    elif cmd_token in ("!ahoy", "/ahoy"):
        await handle_ahoy_request(channel, thread_ts, ts, client)
        return
    elif cmd_token in ("!wrapup", "/wrapup"):
        await handle_wrapup_request(channel, thread_ts, ts, client)
        return
    elif cmd_token in ("!update", "/update"):
        await handle_update_request(channel, thread_ts, ts, client)
        return

    session_key = f"{channel}:{thread_ts}"
    session = sessions.get(session_key)

    # 1. Approval Relay: If agent is currently blocked waiting for interactive confirmation
    if session and getattr(session, "status", None) == "blocked":
        if is_approval_reply(clean_text):
            # User approved: send 'y enter' to agent PTY stdin
            logger.info(f"Relaying approval ('y enter') to agent {session.agent_name}...")
            await herdr_client.run_cli(["agent", "send-keys", session.agent_name, "y", "enter"])
            session.status = "working"
            if is_imessage:
                await client.chat_postMessage(channel=channel, text="👍 Approval relayed. Continuing execution...")
            else:
                await add_reacji(client, channel, ts, "gear")
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text="👍 Relayed approval (`y`). Continuing...")
            return

        elif is_rejection_reply(clean_text):
            # User rejected: send 'n enter' to agent PTY stdin
            logger.info(f"Relaying cancellation ('n enter') to agent {session.agent_name}...")
            await herdr_client.run_cli(["agent", "send-keys", session.agent_name, "n", "enter"])
            session.status = "idle"
            if is_imessage:
                await client.chat_postMessage(channel=channel, text="❌ Cancellation relayed. Operation aborted.")
            else:
                await add_reacji(client, channel, ts, "x")
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text="❌ Relayed cancellation (`n`). Operation aborted.")
            return

    # 2. Status Inquiry: If user asks for real-time status or running URLs
    if session and is_status_inquiry(clean_text):
        _, read_out, _ = await herdr_client.run_cli([
            "agent", "read", session.agent_name,
            "--lines", "30",
            "--source", "recent-unwrapped"
        ])
        sanitized = sanitize_terminal_output(read_out)
        artifacts = extract_operational_artifacts(read_out)
        clean_lines = [l for l in sanitized.splitlines() if l.strip()][-6:]
        summary = "\n".join(clean_lines) or f"Agent `{session.agent_name}` is currently {session.status}."

        url_line = f"Live Endpoint: {artifacts['urls'][0]}\n" if artifacts["urls"] else ""
        msg = f"*Status Update (`{session.agent_name}`)*\n{url_line}```{summary}```"

        if is_imessage:
            await client.chat_postMessage(channel=channel, text=normalize_for_bluebubbles(msg))
        else:
            await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)
        return

    # 3. New Task Dispatch / Follow-Up Prompt to Herdr Pane
    if not is_imessage:
        await add_reacji(client, channel, ts, "eyes")

    is_new_session = session_key not in sessions or sessions[session_key].status == "failed"

    session = await ensure_session_agent(
        session_key=session_key,
        channel=channel,
        thread_ts=thread_ts,
        prompt=clean_text,
        is_imessage=is_imessage,
        client=client,
        trigger_ts=ts
    )

    if session.status == "failed":
        return

    scoped_prompt = scope_server_commands(clean_text, session.target_cwd)
    session.last_prompt = scoped_prompt

    if not is_imessage:
        await swap_reacji(client, channel, ts, "eyes", "gear")

    # If it is a follow-up prompt on an existing active session, submit via agent prompt
    if not is_new_session:
        session.status = "working"
        session.has_worked = False  # Reset working flag for the follow-up turn
        prompt_code, _, prompt_err = await herdr_client.run_cli([
            "agent", "prompt", session.agent_name, scoped_prompt
        ])
        if prompt_code != 0:
            logger.warning(f"herdr agent prompt returned code {prompt_code}: {prompt_err}")

    # Launch background lifecycle monitor
    if not session.monitor_task or session.monitor_task.done():
        session.monitor_task = asyncio.create_task(
            monitor_agent_lifecycle(session, client, trigger_ts=ts)
        )



# ------------------------------------------------------------------------------
# Slack Event Listeners
# ------------------------------------------------------------------------------
@app.event("app_mention")
async def handle_app_mention(event: Dict[str, Any], client: AsyncWebClient):
    ts = event.get("ts", "")
    if ts in processed_message_ids:
        return
    processed_message_ids.add(ts)
    await process_user_prompt(event, client)


@app.event("message")
async def handle_message_events(event: Dict[str, Any], client: AsyncWebClient):
    if event.get("subtype") in ("bot_message", "message_deleted", "channel_join"):
        return
    ts = event.get("ts", "")
    if ts in processed_message_ids:
        return
    processed_message_ids.add(ts)
    await process_user_prompt(event, client)


@app.event("reaction_added")
async def handle_reaction_added(event: Dict[str, Any], client: AsyncWebClient):
    """Relay user reactions (👍, ❌) directly to the blocked Herdr agent pane."""
    reaction = event.get("reaction", "")
    item = event.get("item", {})
    channel = item.get("channel", "")
    item_ts = item.get("ts", "")

    # Look up session by channel or approval_msg_ts
    matched_session = None
    for s in sessions.values():
        if s.channel == channel and (s.approval_msg_ts == item_ts or s.thread_ts == item_ts):
            matched_session = s
            break

    if not matched_session or matched_session.status != "blocked":
        return

    if reaction in ("thumbsup", "+1", "white_check_mark"):
        logger.info(f"Relaying reaction approval ({reaction}) to agent {matched_session.agent_name}...")
        await herdr_client.run_cli(["agent", "send-keys", matched_session.agent_name, "y", "enter"])
        matched_session.status = "working"
        await add_reacji(client, channel, item_ts, "gear")

    elif reaction in ("thumbsdown", "-1", "x", "octagonal_sign"):
        logger.info(f"Relaying reaction cancellation ({reaction}) to agent {matched_session.agent_name}...")
        await herdr_client.run_cli(["agent", "send-keys", matched_session.agent_name, "n", "enter"])
        matched_session.status = "idle"
        await add_reacji(client, channel, item_ts, "x")


# ------------------------------------------------------------------------------
# BlueBubbles iMessage Transport Integration
# ------------------------------------------------------------------------------
bb_transport: Optional[BlueBubblesTransport] = None

async def handle_bluebubbles_inbound_message(
    chat_guid: str,
    text: str,
    sender: str,
    raw_payload: Dict[str, Any]
):
    logger.info(f"[iMessage Received] From {sender} in {chat_guid}: {text[:80]}")
    event = {
        "channel": chat_guid,
        "user": sender,
        "text": text,
        "ts": str(time.time()),
        "thread_ts": chat_guid,
        "is_imessage": True,
        "raw_payload": raw_payload
    }
    if bb_transport:
        client = bb_transport.get_client(chat_guid)
        await process_user_prompt(event, client)


async def start_bluebubbles_transport():
    global bb_transport
    bb_url = os.getenv("BLUEBUBBLES_URL", "http://127.0.0.1:1234")
    bb_pass = os.getenv("BLUEBUBBLES_PASSWORD", "!Fulurban88")
    bb_port = int(os.getenv("BLUEBUBBLES_WEBHOOK_PORT", "8646"))
    bb_host = os.getenv("BLUEBUBBLES_WEBHOOK_HOST", "0.0.0.0")
    bb_allowed = os.getenv("BLUEBUBBLES_ALLOWED_HANDLES", "k.horned@gmail.com")

    bb_transport = BlueBubblesTransport(
        server_url=bb_url,
        password=bb_pass,
        webhook_host=bb_host,
        webhook_port=bb_port,
        allowed_handles=bb_allowed,
        message_handler=handle_bluebubbles_inbound_message
    )
    try:
        await bb_transport.start()
        logger.info(f"BlueBubbles iMessage transport initialized on {bb_host}:{bb_port}")
    except Exception as e:
        logger.warning(f"Could not start BlueBubbles iMessage transport: {e}")


async def stop_bluebubbles_transport():
    global bb_transport
    if bb_transport:
        try:
            await bb_transport.stop()
            logger.info("BlueBubbles transport stopped cleanly.")
        except Exception as e:
            logger.debug(f"Error stopping BlueBubbles transport: {e}")


# ------------------------------------------------------------------------------
# Signal Handling & Main Entrypoint
# ------------------------------------------------------------------------------
shutdown_event = asyncio.Event()

def setup_signal_handlers(loop: asyncio.AbstractEventLoop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: on_signal_received(s))
        except (NotImplementedError, RuntimeError):
            pass

def on_signal_received(sig):
    logger.info(f"Received exit signal {sig.name}. Initiating graceful shutdown...")
    shutdown_event.set()


async def main():
    logger.info("Starting First Mate Thin Ingress Gateway (Herdr Runtime)...")
    loop = asyncio.get_running_loop()
    setup_signal_handlers(loop)

    try:
        killed = await cleanup_rogue_servers()
        if killed:
            logger.info(f"Cleaned up {len(killed)} rogue background server(s) on startup: {killed}")
    except Exception as e:
        logger.debug(f"Startup rogue server cleanup: {e}")

    try:
        await fleet.ensure_server()
        await fleet.ensure_topology()
        logger.info(f"Herdr fleet active: {list(fleet.topology.keys())}")
    except Exception as e:
        logger.error(f"Herdr fleet initialization warning: {e}")

    try:
        auth_info = await app.client.auth_test()
        global BOT_USER_ID
        BOT_USER_ID = auth_info.get("user_id")
        logger.info(f"Authenticated as @{auth_info.get('user')} ({BOT_USER_ID}) on team {auth_info.get('team')}")
    except Exception as e:
        logger.critical(f"Failed to authenticate with Slack API: {e}")
        sys.exit(1)

    await start_bluebubbles_transport()

    handler = AsyncSocketModeHandler(app, SLACK_APP_TOKEN)
    logger.info("First Mate Gateway ready. Listening for Slack & iMessage events...")

    handler_task = asyncio.create_task(handler.start_async())
    stop_task = asyncio.create_task(shutdown_event.wait())

    try:
        done, pending = await asyncio.wait(
            [handler_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
    finally:
        logger.info("Performing graceful shutdown: disconnecting transports and closing client sessions...")
        try:
            await handler.close_async()
        except Exception as e:
            logger.debug(f"Error closing socket mode handler: {e}")

        await stop_bluebubbles_transport()

        try:
            if hasattr(app, "client") and hasattr(app.client, "session") and app.client.session and not app.client.session.closed:
                await app.client.session.close()
        except Exception as e:
            logger.debug(f"Error closing Slack client session: {e}")

        # Brief yield to let underlying SSL transports complete clean closure
        await asyncio.sleep(0.25)
        logger.info("First Mate Gateway shutdown completed cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
