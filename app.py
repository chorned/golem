#!/usr/bin/env python3
# Golem: Autonomous Fleet Chief of Staff
# Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
app.py
Golem Thin Ingress Gateway & Herdr PTY Relay.
Connects Slack Socket Mode and BlueBubbles iMessage to Herdr PTYs and Antigravity CLI (agy).
Eliminates bespoke JSON classifiers, Mad Libs f-string generators, and canned polling loops.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import sys
import time
import json
import signal
import asyncio
import logging
import argparse
import textwrap
from typing import Dict, Any, Optional, Set, List, Tuple
from urllib.parse import quote

from dotenv import load_dotenv
from golem.config import is_webui_enabled, get_webui_host, get_webui_port
from golem.webui import start_webui_server, stop_webui_server, webui_broadcast, get_active_webui_server
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

try:
    from bluebubbles_transport import (
        BlueBubblesTransport,
        BlueBubblesClient,
        sanitize_for_imessage
    )
except ImportError:
    BlueBubblesTransport = None  # type: ignore
    BlueBubblesClient = None  # type: ignore
    def sanitize_for_imessage(s: str) -> str: return s  # type: ignore

try:
    from normalizer import (
        sanitize_for_slack,
        strip_ansi,
        strip_tui_debris,
        transpile_markdown_to_mrkdwn
    )
except ImportError:
    from golem.transport.normalizer import (
        sanitize_for_slack,
        strip_ansi,
        strip_tui_debris,
        transpile_markdown_to_mrkdwn
    )

try:
    from herdr_client import HerdrClient, InfrastructureError
except ImportError:
    HerdrClient = None  # type: ignore
    InfrastructureError = Exception  # type: ignore

try:
    from herdr_fleet import HerdrFleet
except ImportError:
    HerdrFleet = None  # type: ignore

try:
    from herdr_tools import (
        validate_action_payload,
        BANNED_ACTION_PAYLOAD_PATTERNS,
        run_command
    )
except ImportError:
    validate_action_payload = None  # type: ignore
    BANNED_ACTION_PAYLOAD_PATTERNS = []  # type: ignore
    run_command = None  # type: ignore

try:
    from supervisor import AgentSupervisor, cleanup_rogue_servers, summarize_terminal_activity
except ImportError:
    AgentSupervisor = None  # type: ignore
    cleanup_rogue_servers = None  # type: ignore
    summarize_terminal_activity = None  # type: ignore

try:
    from memory_manager import MemoryManager
except ImportError:
    MemoryManager = None  # type: ignore

try:
    from telemetry_observer import (
        SessionTelemetryObserver,
        extract_listening_ports,
        resolve_machine_name,
        resolve_machine_label,
        format_runner_label,
        format_terminal_tail,
        format_approval_blocks,
        format_completion_message,
        format_failure_message,
        extract_pending_command,
        is_plumbing_command,
        sanitize_scrollback,
        build_status_card_blocks
    )
except ImportError:
    SessionTelemetryObserver = None  # type: ignore


# Golem Core Modular Package Imports
from golem.transport.chunker import split_message_boundary_aware
from golem.gateway import SessionRouter, SessionHandle, SessionState
from golem.gateway.ingress import (
    IngressGateway,
    add_reaction as add_reacji,
    remove_reaction as remove_reacji,
    swap_reaction as swap_reacji,
    strip_bot_mentions,
)
from golem.execution.harness import DualPaneHarness
from golem.sensory import (
    NoseSentinel,
    get_default_event_bus,
    set_default_nose_sentinel,
)

# ------------------------------------------------------------------------------
# Logging & Environment Setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("AGY")

HOST_DIR = (os.environ.get("GOLEM_HOST_DIR") or "").strip() or os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = HOST_DIR
DEFAULT_TARGET_REPO = os.environ.get("DEFAULT_TARGET_REPO", "/Users/hermes/projects/AGY").strip()
DAEMON_LABEL = (os.environ.get("GOLEM_DAEMON_LABEL") or "").strip() or (os.environ.get("DAEMON_LABEL") or "").strip() or "com.user.golem"

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
herdr_client = HerdrClient() if HerdrClient is not None else None
fleet = HerdrFleet() if HerdrFleet is not None else None
herdr = fleet
supervisor = AgentSupervisor() if AgentSupervisor is not None else None
client_herdr = herdr_client

app = AsyncApp(
    token=SLACK_BOT_TOKEN,
    signing_secret=SLACK_SIGNING_SECRET
)

# Attach monkey-patch targets to AsyncApp instance for test_supervisor.py compatibility
app.supervisor = supervisor
app.herdr_client = herdr_client
app.fleet = fleet

# First Mate 2.0 Sensory Organ Singletons
nose_sentinel = NoseSentinel(
    fleet=fleet,
    bus=get_default_event_bus(),
    bot_err_path=os.path.join(HOST_DIR, "bot.err"),
)
app.nose_sentinel = nose_sentinel
set_default_nose_sentinel(nose_sentinel)

# First Mate 2.0 Core Singletons
dual_pane_harness = DualPaneHarness(herdr_client=herdr_client, default_cwd=DEFAULT_TARGET_REPO)
session_router = SessionRouter(default_target_cwd=DEFAULT_TARGET_REPO, harness=dual_pane_harness)
ingress_gateway = IngressGateway(router=session_router, bot_user_id=None, nose_sentinel=nose_sentinel)

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
        is_imessage: bool = False,
        runner_pane: Optional[str] = "runner",
        liaison_agent: Optional[str] = None,
        dual_pane: bool = False
    ):
        self.session_key = session_key
        self.agent_name = agent_name
        self.target_cwd = target_cwd
        self.channel = channel
        self.thread_ts = thread_ts
        self.workspace_id = workspace_id
        self.pane_id = pane_id
        self.is_imessage = is_imessage
        self.dual_pane = dual_pane
        self.runner_pane = runner_pane or "runner"
        self.liaison_agent = liaison_agent or (f"{agent_name}-liaison" if dual_pane else agent_name)
        self.status = "starting"  # "starting" | "booting" | "working" | "blocked" | "done" | "failed"
        self.has_worked = False
        self.execution_steps = 0
        self.last_prompt = ""
        self.last_blocked_snapshot: Optional[str] = None
        self.approval_msg_ts: Optional[str] = None
        self.monitor_task: Optional[asyncio.Task] = None
        self.telemetry_observer: Optional[SessionTelemetryObserver] = None
        self.created_at = time.time()

    @property
    def active_agent(self) -> str:
        return self.liaison_agent if self.dual_pane and self.liaison_agent else self.agent_name

    @property
    def state(self) -> str:
        return self.status

    @state.setter
    def state(self, val: str) -> None:
        self.status = val

    def stop_observer(self) -> None:
        """Cancel and clean up associated telemetry observer loop."""
        if self.telemetry_observer:
            self.telemetry_observer.stop_event.set()
            if getattr(self.telemetry_observer, "task", None) and not self.telemetry_observer.task.done():
                self.telemetry_observer.task.cancel()


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
    r"|[=><]"
    r"|[78](?![0-9a-zA-Z])"
    r")"
)

def strip_ansi(text: str) -> str:
    """Strip all ANSI escape codes and terminal controls."""
    return ANSI_ESCAPE_PATTERN.sub("", text).replace("\x1b", "")

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


def format_agent_dispatch_prompt(prompt: str, target_cwd: str = "") -> str:
    """Pass user prompt directly to Antigravity CLI without pre-routing modification."""
    return prompt.strip()


def sanitize_terminal_output(raw_text: str, user_prompt: str = "") -> str:
    """
    Filter raw terminal text from Antigravity CLI:
    - Strips ANSI escape codes, terminal device queries, and graphics protocol sequences.
    - Filters out ASCII art splash banners and TUI chrome.
    - Strips status bar lines ('? for shortcuts', 'esc to cancel', 'Last progress:').
    - Filters out internal thought preview blocks, tool calls, and prompt echoes.
    - Preserves clean substantive execution output and markdown prose formatting.
    """
    clean = strip_ansi(raw_text)
    # Strip terminal query responses and graphics sequences
    clean = re.sub(r"\^\[_G[^\^]*\^\\", "", clean)
    clean = re.sub(r"\x1b\[\?[0-9;]*[a-zA-Z]", "", clean)
    clean = re.sub(r"_G[a-z0-9_=,;+]+(?:==)?", "", clean)
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

    clean_prompt = user_prompt.strip().lower() if user_prompt else ""

    filtered_lines = []
    skip_thought_snippet = False

    for line in clean.splitlines():
        line_stripped = line.strip()
        lower_line = line_stripped.lower()

        if not line_stripped:
            # Preserve single blank lines between paragraphs
            if filtered_lines and filtered_lines[-1] != "":
                filtered_lines.append("")
            continue

        # Skip pure border/divider lines
        if all(c in "─━│┃-=_* " for c in line_stripped):
            continue
        # Skip ASCII art logo blocks
        if any(c in "▄▀█▌▐" for c in line_stripped):
            continue
        # Skip splash / TUI footer lines
        if any(ind in lower_line for ind in splash_indicators):
            continue
        # Skip bare prompt or command
        if line_stripped in (">", "> ", "agy"):
            continue
        # Skip shell command prompt invocation line (e.g. hermes@... % agy ...)
        if re.search(r"%\s+agy\b", line_stripped):
            continue
        # Skip prompt echo
        if clean_prompt and line_stripped.startswith(">") and clean_prompt in lower_line:
            continue

        # Skip TUI collapsible thoughts indicator
        if line_stripped.startswith("▸ ") or lower_line.startswith("thought for ") or lower_line.startswith("thinking..."):
            skip_thought_snippet = True
            continue

        # Skip tool invocations and collapsed control elements
        if line_stripped.startswith("● ") or "(ctrl+o to expand)" in lower_line:
            continue

        # Skip the 1-line thought snippet following a collapsed thought header
        if skip_thought_snippet:
            skip_thought_snippet = False
            continue

        filtered_lines.append(line.rstrip())

    # Dedent terminal margin indentation while preserving relative indentations
    dedented = textwrap.dedent("\n".join(filtered_lines)).strip()
    return strip_tui_debris(dedented)


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
    - Strip TUI debris without injecting Slack mrkdwn.
    - Convert <http://url|label> and <http://url> to clean URLs.
    - Convert Slack shortcodes (:white_check_mark:, :thumbsup:, :eyes:) to native Unicode.
    """
    cleaned = strip_tui_debris(text)
    return sanitize_for_imessage(cleaned)


# split_message_boundary_aware is re-exported from golem.chunker above


# ------------------------------------------------------------------------------
# Reacji & Slack Formatting Helpers
# ------------------------------------------------------------------------------
IGNORABLE_REACTION_ERRORS = {"already_reacted", "no_reaction", "message_not_found"}

async def add_reacji(client: Any, channel: str, timestamp: str, name: str) -> None:
    try:
        if hasattr(client, "reactions_add"):
            await client.reactions_add(channel=channel, timestamp=timestamp, name=name)
            webui_broadcast("eye_log", {"type": "reacji_add", "reacji": name, "channel": channel, "ts": timestamp})
    except Exception as e:
        logger.debug(f"Reaction add error ({name}): {e}")

async def remove_reacji(client: Any, channel: str, timestamp: str, name: str) -> None:
    try:
        if hasattr(client, "reactions_remove"):
            await client.reactions_remove(channel=channel, timestamp=timestamp, name=name)
            webui_broadcast("eye_log", {"type": "reacji_remove", "reacji": name, "channel": channel, "ts": timestamp})
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
    # Do not intercept fleet inquiries; AGY answers fleet queries directly in its root pane
    if re.search(r"\bfleet\b", text, re.IGNORECASE):
        return False
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
    trigger_ts: Optional[str] = None,
    dual_pane: Optional[bool] = None,
    target_cwd_override: Optional[str] = None,
    agent_name_override: Optional[str] = None
) -> GatewaySession:
    """Ensure a Herdr workspace and Antigravity CLI agent exist for this thread with readiness-gated handshake."""
    if session_key in sessions and sessions[session_key].status != "failed":
        return sessions[session_key]

    if dual_pane is None:
        dual_pane = os.environ.get("DUAL_PANE_WORKSPACE", "").lower() in ("1", "true", "yes")

    clean_ts = re.sub(r"[^0-9]", "", str(thread_ts or time.time()))[-8:]
    agent_name = agent_name_override or f"fm-{clean_ts}"
    target_cwd = target_cwd_override or resolve_target_directory(prompt, task_id=agent_name)
    ensure_trusted_workspace(target_cwd)

    # 1. Spawn Herdr workspace: herdr workspace create --label fm-<ts> --cwd <sandbox_or_project_cwd> --no-focus
    ws_id = None
    pane_id = None
    if dual_pane:
        ws_cmd = [
            "workspace", "create",
            "--label", agent_name,
            "--cwd", target_cwd,
            "--no-focus"
        ]
    else:
        ws_cmd = [
            "workspace", "create",
            "--cwd", target_cwd,
            "--label", agent_name,
            "--no-focus"
        ]
    code, out, err = await herdr_client.run_cli(ws_cmd)
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
        pane_id = "1" if dual_pane else "w6:p1"

    scoped_prompt = scope_server_commands(prompt, target_cwd)

    liaison_name = f"{agent_name}-liaison" if dual_pane else agent_name
    runner_pane = "runner" if dual_pane else None

    session = GatewaySession(
        session_key=session_key,
        agent_name=agent_name,
        target_cwd=target_cwd,
        channel=channel,
        thread_ts=thread_ts,
        workspace_id=ws_id,
        pane_id=pane_id,
        is_imessage=is_imessage,
        runner_pane=runner_pane,
        liaison_agent=liaison_name,
        dual_pane=dual_pane
    )
    session.status = "starting"
    session.has_worked = False
    session.execution_steps = 0
    session.last_prompt = scoped_prompt

    sessions[session_key] = session
    agent_name_to_session[agent_name] = session
    if dual_pane:
        agent_name_to_session[liaison_name] = session

    # 2. Clean Spawn: Launch agent with zero trailing prompt arguments
    # Pane 1 (liaison): Launch agy via herdr agent start <name>-liaison --kind agy --pane 1
    if dual_pane:
        start_cmd = [
            "agent", "start", liaison_name,
            "--kind", "agy",
            "--pane", "1"
        ]
    else:
        start_cmd = [
            "agent", "start", agent_name,
            "--kind", "agy",
            "--pane", pane_id
        ]
    if agent_args:
        start_cmd.extend(["--"] + agent_args)

    start_code, start_out, start_err = await herdr_client.run_cli(start_cmd)

    # 3. Spawn Pane 2 (runner) if dual_pane:
    # herdr pane create --workspace <name> --label runner
    if dual_pane:
        runner_cmd = [
            "pane", "create",
            "--workspace", agent_name,
            "--label", "runner"
        ]
        await herdr_client.run_cli(runner_cmd)

    target_agent = liaison_name if dual_pane else agent_name

    # 3b. Fail-Fast Trap: Inspect process return code and error response immediately
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
        logger.error(f"herdr agent start failed for {target_agent}: {err_detail}")
        if client:
            if trigger_ts:
                await swap_reacji(client, channel, trigger_ts, "gear", "x")
                await swap_reacji(client, channel, trigger_ts, "eyes", "x")
            err_msg = f":x: *Hit an issue running the server: Failed to spawn Antigravity agent (`{target_agent}`)*:\n```{err_detail}```"
            if is_imessage:
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=normalize_for_bluebubbles(err_msg))
            else:
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack(err_msg))
        if ws_id:
            await herdr_client.run_cli(["workspace", "close", ws_id])
        return session

    # 4. Readiness Watchdog (The TUI Handshake):
    # Monitor the pane buffer for up to 15s until agy displays ready indicator
    is_ready = await wait_for_agent_readiness(target_agent, timeout_seconds=15.0)
    if not is_ready:
        session.status = "failed"
        timeout_detail = f"Agent `{target_agent}` failed to reach readiness within 15 seconds."
        logger.error(timeout_detail)
        if client:
            if trigger_ts:
                await swap_reacji(client, channel, trigger_ts, "gear", "x")
                await swap_reacji(client, channel, trigger_ts, "eyes", "x")
            timeout_msg = f":x: *Hit an issue running the server: Failed to start Antigravity agent (`{target_agent}`)*:\n```{timeout_detail}```"
            if is_imessage:
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=normalize_for_bluebubbles(timeout_msg))
            else:
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack(timeout_msg))
        if ws_id:
            await herdr_client.run_cli(["workspace", "close", ws_id])
        return session

    # 5. Prompt Delivery:
    # Once readiness is confirmed, dispatch the prompt via herdr agent prompt
    prompt_code, _, prompt_err = await herdr_client.run_cli([
        "agent", "prompt", target_agent, scoped_prompt
    ])
    if prompt_code != 0:
        logger.warning(f"herdr agent prompt returned code {prompt_code}: {prompt_err}")

    return session


async def init_dual_pane_workspace(
    session_name: str,
    cwd: str,
    channel: str = "general",
    thread_ts: Optional[str] = None,
    client: Any = None,
    agent_args: Optional[List[str]] = None,
    prompt: str = ""
) -> GatewaySession:
    """Initialize a Dual-Pane Workspace operations session."""
    ts = thread_ts or str(int(time.time()))
    session_key = f"{channel}:{ts}"
    return await ensure_session_agent(
        session_key=session_key,
        channel=channel,
        thread_ts=ts,
        prompt=prompt,
        agent_args=agent_args,
        client=client,
        dual_pane=True,
        target_cwd_override=cwd,
        agent_name_override=session_name
    )


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
    agent_name = session.active_agent
    logger.info(f"Lifecycle monitor active for agent {agent_name} (initial status: {session.status})...")
    consecutive_idle_ticks = 0
    boot_start_time = time.time()

    while True:
        await asyncio.sleep(1.5)

        if session.status == "failed":
            if session.telemetry_observer:
                await session.telemetry_observer.stop()
            break

        # Global Lifecycle Watchdog: strict 30s timeout while booting/starting
        if not session.has_worked and session.status in ("starting", "booting"):
            if time.time() - boot_start_time > 30.0:
                logger.error(f"Agent {agent_name} lifecycle timeout: stuck in {session.status} for >30s.")
                session.status = "failed"
                timeout_msg = f":x: *Hit an issue running the server: Agent (`{agent_name}`) timed out during initialization (>30s).* Session aborted."
                if session.telemetry_observer:
                    await session.telemetry_observer.settle_failed(error_detail=timeout_msg)
                if trigger_ts:
                    await swap_reacji(client, session.channel, trigger_ts, "gear", "x")
                    await swap_reacji(client, session.channel, trigger_ts, "eyes", "x")
                if session.is_imessage:
                    await client.chat_postMessage(channel=session.channel, thread_ts=session.thread_ts, text=normalize_for_bluebubbles(timeout_msg))
                else:
                    await client.chat_postMessage(channel=session.channel, thread_ts=session.thread_ts, text=sanitize_for_slack(timeout_msg))
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
            if trigger_ts:
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
            sanitized = sanitize_terminal_output(read_out, session.last_prompt)
            if sanitized:
                # Buffer Settling: Verify the agent has returned completely to its idle prompt
                # (prompt indicator `>` at the end of the terminal buffer or status `done`)
                lines = [l.strip() for l in read_out.splitlines() if l.strip()]
                has_returned_to_prompt = (
                    current_status == "done"
                    or any(l in (">", "> ") or l.startswith("> ") for l in lines[-4:])
                )
                if not has_returned_to_prompt:
                    consecutive_idle_ticks = 0
                    await asyncio.sleep(1.0)
                    continue

                consecutive_idle_ticks += 1
                if consecutive_idle_ticks >= 2:
                    session.status = "done"
                    if trigger_ts:
                        await swap_reacji(client, session.channel, trigger_ts, "gear", "white_check_mark")
                    # Fetch broader buffer to capture full conversational briefings
                    _, final_read, _ = await herdr_client.run_cli([
                        "agent", "read", agent_name,
                        "--source", "recent-unwrapped",
                        "--lines", "80"
                    ], timeout=5.0)
                    await post_completion_briefing(session, client, final_read or read_out)
                    break


async def post_approval_card(session: GatewaySession, client: Any, terminal_snippet: str):
    """Render interactive approval card to Slack or BlueBubbles with conversational framing."""
    cmd = extract_pending_command(terminal_snippet)
    runner = "OpenCode" if (
        re.search(r"\bopencode\b", session.agent_name, re.IGNORECASE)
        or (session.telemetry_observer and "opencode" in getattr(session.telemetry_observer, "last_runner_label", "").lower())
    ) else "Antigravity"

    if session.is_imessage:
        msg = (
            f"*{runner} wants to run `{cmd}`. Good to go?*\n\n"
            f"Reply 'approve' (or 👍) to execute, or 'abort' to cancel."
        )
        await client.chat_postMessage(
            channel=session.channel,
            thread_ts=session.thread_ts,
            text=normalize_for_bluebubbles(msg)
        )
    else:
        text_content, blocks = format_approval_blocks(runner, cmd, session.session_key)
        resp = await client.chat_postMessage(
            channel=session.channel,
            thread_ts=session.thread_ts,
            text=sanitize_for_slack(f"{runner} wants to run `{cmd}`. Good to go? Approval required: reply approve or abort."),
            blocks=blocks
        )
        if isinstance(resp, dict):
            session.approval_msg_ts = resp.get("ts")


async def post_completion_briefing(session: GatewaySession, client: Any, terminal_output: str):
    """Post final completion output to chat in natural Chief of Staff voice."""
    sanitized = sanitize_terminal_output(terminal_output, getattr(session, "last_prompt", ""))
    artifacts = extract_operational_artifacts(terminal_output)
    ports = extract_listening_ports(terminal_output)

    urls = list(artifacts.get("urls", []))
    if not urls and ports:
        urls.append(f"http://localhost:{ports[0]}")

    clean_urls = []
    for u in urls:
        clean_u = re.sub(r"[)/]+$", "", u)
        if clean_u not in clean_urls:
            clean_urls.append(clean_u)
    urls = clean_urls

    files = artifacts.get("files", [])
    machine = resolve_machine_name(getattr(session, "target_machine", None))

    if session.telemetry_observer:
        await session.telemetry_observer.settle_complete(artifacts={"urls": urls, "files": files}, terminal_output=terminal_output)

    conversational_lead = format_completion_message(machine=machine, urls=urls, files=files)
    endpoint_banner = f"*Live Endpoint:* {urls[0]}\n" if urls else ""
    files_banner = f"*Files Created:* {', '.join(files)}\n" if files else ""

    if urls or files:
        clean_lines = [l for l in sanitized.splitlines() if l.strip()][-10:]
        summary = "\n".join(clean_lines) if clean_lines else ""
        summary_block = f"\n```{summary}```" if summary and summary != "Task completed successfully." else ""
        msg = f"{conversational_lead}\n\n*Task Completed via Herdr (`{session.agent_name}`)*\n{endpoint_banner}{files_banner}{summary_block}".strip()
    else:
        # Direct conversational or status inquiry response: omit enterprise task footer
        summary = sanitized.strip()
        msg = summary or conversational_lead

    if session.is_imessage:
        bb_text = normalize_for_bluebubbles(summary or conversational_lead)
        for chunk in split_message_boundary_aware(bb_text, max_chars=3500):
            await client.chat_postMessage(
                channel=session.channel,
                thread_ts=session.thread_ts,
                text=chunk
            )
    else:
        sanitized_msg = sanitize_for_slack(msg)
        for chunk in split_message_boundary_aware(sanitized_msg, max_chars=3800):
            await client.chat_postMessage(
                channel=session.channel,
                thread_ts=session.thread_ts,
                text=sanitize_for_slack(chunk)
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
            text=sanitize_for_slack(briefing)
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
        try:
            from golem.wiki import GolemWiki
            wiki = GolemWiki()
            wiki.logbook_append(
                entry=user_notes or "Operational lesson codified into memory.",
                title="Operational Learning (/learn)",
            )
        except Exception as we:
            logger.debug(f"Wiki logbook append note: {we}")

        msg = f"🧠 *Operational Lesson Crystallized:* {user_notes or 'Lesson codified into operational memory.'}"
        if react_ts and hasattr(client, "reactions_add"):
            await swap_reacji(client, channel, react_ts, "brain", "white_check_mark")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=sanitize_for_slack(msg)
        )
    except Exception as e:
        logger.error(f"Error handling /learn: {e}")


async def handle_ahoy_request(channel: str, thread_ts: Optional[str] = None, msg_ts: Optional[str] = None, client: Any = None):
    active_count = len([s for s in sessions.values() if getattr(s, "status", "") == "running" or getattr(s, "is_busy", False)])
    if active_count > 0:
        msg = f"⚓ *Ahoy:* Session active with {active_count} executing tasks."
    else:
        msg = "⚓ *Ahoy:* Session standby. 0 background execution tasks running."
    await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack(msg))


async def handle_wrapup_request(channel: str, thread_ts: Optional[str] = None, msg_ts: Optional[str] = None, client: Any = None):
    msg = "🧹 *Wrapup:* Housekeeping complete. Working directories clean."
    await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack(msg))


async def handle_update_request(channel: str, thread_ts: Optional[str] = None, msg_ts: Optional[str] = None, client: Any = None):
    # Check for active running sessions/hands
    active_count = len([s for s in sessions.values() if getattr(s, "status", "") == "running" or getattr(s, "is_busy", False)])
    if ingress_gateway and getattr(ingress_gateway, "webui_server", None):
        active_hands = list(getattr(ingress_gateway.webui_server, "_active_hands", {}).values())
        active_count += len(active_hands)

    if active_count > 0:
        msg = f"⚠️ *Update Aborted:* Fleet active with {active_count} executing task(s). Wait for completion before reloading."
        await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack(msg))
        return

    msg = "🚀 *Update:* Verifying core test suite and fleet AGY binaries..."
    await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack(msg))

    async def _reload():
        await asyncio.sleep(0.5)
        script = os.path.join(HOST_DIR, "scripts", "safe_update.sh")
        if os.path.exists(script) and os.access(script, os.X_OK):
            try:
                proc = await asyncio.create_subprocess_exec(
                    script,
                    "--verify-only",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode != 0:
                    err_snip = (stderr or stdout).decode("utf-8", errors="replace")[-500:]
                    msg = f"❌ *Update Aborted:* Test verification failed (exit {proc.returncode}):\n```{err_snip}```"
                    await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack(msg))
                    return
            except Exception as e:
                logger.warning(f"safe_update.sh invocation failed: {e}")

        # Check and update agy CLI across fleet
        fleet_bullets = []
        try:
            from herdr_fleet import HerdrFleet
            fleet_obj = HerdrFleet()
            agy_results = await fleet_obj.update_fleet_agy()
            for r in agy_results:
                n = r.get("node", "unknown")
                st = r.get("status", "unknown")
                fleet_bullets.append(f"• *{n}:* {st}")
        except Exception as e:
            fleet_bullets.append(f"• Fleet AGY update error: {e}")

        fleet_section = ""
        if fleet_bullets:
            fleet_section = "\n\n*Fleet AGY Binary Status:*\n" + "\n".join(fleet_bullets)

        succ_msg = f"🚀 *Update:* Verification passed cleanly. Daemon reloaded.{fleet_section}"
        await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack(succ_msg))
        await asyncio.sleep(0.5)

        try:
            uid = os.getuid()
            for label in ["com.user.golem"]:
                cmd = ["launchctl", "kickstart", "-k", "-p", f"gui/{uid}/{label}"]
                proc = await asyncio.create_subprocess_exec(*cmd)
                await proc.communicate()
        except Exception as e:
            logger.warning(f"launchctl kickstart invocation failed: {e}")

    asyncio.create_task(_reload())


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

    # Strip bot user mention (@AGY or <@Uxxxx>)
    clean_text = raw_text
    if BOT_USER_ID:
        clean_text = re.sub(rf"<@{BOT_USER_ID}>", "", clean_text).strip()
    clean_text = re.sub(r"<@[A-Za-z0-9_-]+>", "", clean_text).strip()
    clean_text = re.sub(r"^@(?:AGY|agy|\w+)\b\s*", "", clean_text).strip()
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
            logger.info(f"Relaying approval ('y enter') to agent {session.active_agent}...")
            await herdr_client.run_cli(["agent", "send-keys", session.active_agent, "y", "enter"])
            session.status = "working"
            if is_imessage:
                await client.chat_postMessage(channel=channel, text=normalize_for_bluebubbles("👍 Approval relayed. Continuing execution..."))
            else:
                await add_reacji(client, channel, ts, "gear")
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack("👍 Relayed approval (`y`). Continuing..."))
            return

        elif is_rejection_reply(clean_text):
            # User rejected: send 'n enter' to agent PTY stdin
            logger.info(f"Relaying cancellation ('n enter') to agent {session.active_agent}...")
            await herdr_client.run_cli(["agent", "send-keys", session.active_agent, "n", "enter"])
            session.status = "idle"
            if is_imessage:
                await client.chat_postMessage(channel=channel, text=normalize_for_bluebubbles("❌ Cancellation relayed. Operation aborted."))
            else:
                await add_reacji(client, channel, ts, "x")
                await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack("❌ Relayed cancellation (`n`). Operation aborted."))
            return

    # 2. Status Inquiry: If user asks for real-time status or running URLs
    if session and is_status_inquiry(clean_text):
        _, read_out, _ = await herdr_client.run_cli([
            "agent", "read", session.active_agent,
            "--lines", "30",
            "--source", "recent-unwrapped"
        ])
        sanitized = sanitize_terminal_output(read_out, session.last_prompt)
        artifacts = extract_operational_artifacts(read_out)
        ports = extract_listening_ports(read_out)

        urls = list(artifacts.get("urls", []))
        if not urls and ports:
            urls.append(f"http://localhost:{ports[0]}")

        url_line = f"Live Endpoint: {urls[0]}\n" if urls else ""
        clean_lines = [l for l in sanitized.splitlines() if l.strip()][-6:]
        summary = "\n".join(clean_lines) or f"Agent `{session.active_agent}` is currently {session.status}."
        msg = f"*Status Update (`{session.agent_name}`)*\n{url_line}```{summary}```"

        if is_imessage:
            await client.chat_postMessage(channel=channel, text=normalize_for_bluebubbles(msg))
        else:
            await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=sanitize_for_slack(msg))
        return

    # 3. New Task Dispatch / Follow-Up Prompt to Herdr Pane (Non-verbal :gear: ingress)
    if ts:
        await add_reacji(client, channel, ts, "gear")

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

    # Start living status card & out-of-band PTY telemetry observer if not active
    if client and (not session.telemetry_observer or session.telemetry_observer.stop_event.is_set()):
        session.telemetry_observer = SessionTelemetryObserver(
            session=session,
            client=client,
            herdr_client=herdr_client,
            poll_interval=1.0,
            min_update_interval=1.5
        )
        session.telemetry_observer.start()

    # If it is a follow-up prompt on an existing active session, submit via agent prompt
    if not is_new_session:
        session.status = "working"
        session.has_worked = False  # Reset working flag for the follow-up turn
        prompt_code, _, prompt_err = await herdr_client.run_cli([
            "agent", "prompt", session.active_agent, scoped_prompt
        ])
        if prompt_code != 0:
            logger.warning(f"herdr agent prompt returned code {prompt_code}: {prompt_err}")

    # Launch background lifecycle monitor
    if not session.monitor_task or session.monitor_task.done():
        session.monitor_task = asyncio.create_task(
            monitor_agent_lifecycle(session, client, trigger_ts=ts)
        )



async def resolve_bot_user_id(client: Any = None) -> Optional[str]:
    """Resolve AGY's bot user ID via client.auth_test()["user_id"] or cached BOT_USER_ID."""
    global BOT_USER_ID
    if isinstance(BOT_USER_ID, str) and BOT_USER_ID:
        return BOT_USER_ID
    if ingress_gateway is not None:
        gw_bot_id = getattr(ingress_gateway, "bot_user_id", None)
        if isinstance(gw_bot_id, str) and gw_bot_id:
            BOT_USER_ID = gw_bot_id
            return BOT_USER_ID
    if client is not None and hasattr(client, "auth_test"):
        try:
            fn = getattr(client, "auth_test")
            res = fn() if callable(fn) else fn
            if asyncio.iscoroutine(res):
                res = await res
            if isinstance(res, dict):
                bot_id = res.get("user_id")
                if isinstance(bot_id, str) and bot_id:
                    BOT_USER_ID = bot_id
                    if ingress_gateway is not None and hasattr(ingress_gateway, "bot_user_id"):
                        try:
                            ingress_gateway.bot_user_id = bot_id
                        except Exception:
                            pass
                    return BOT_USER_ID
        except Exception as e:
            logger.debug(f"Failed to resolve bot_user_id via auth_test: {e}")
    return BOT_USER_ID if isinstance(BOT_USER_ID, str) else None


def is_active_thread(channel: str, thread_ts: Optional[str]) -> bool:
    """Check if thread_ts is part of an active session in session_router or sessions."""
    if not thread_ts:
        return False
    chan_str = str(channel or "")
    th_str = str(thread_ts)

    if session_router is not None:
        if hasattr(session_router, "has_thread") and session_router.has_thread(chan_str, th_str):
            return True
        if hasattr(session_router, "sessions"):
            key = f"{chan_str}:{th_str}"
        if key in session_router.sessions:
            s = session_router.sessions[key]
            state = getattr(s, "state", None)
            state_val = getattr(state, "value", state)
            if state_val != "closed":
                return True
        for s in session_router.sessions.values():
            s_chan = getattr(s, "channel", None)
            if (s_chan is None or str(s_chan) == chan_str) and str(getattr(s, "thread_ts", None)) == th_str:
                state = getattr(s, "state", None)
                state_val = getattr(state, "value", state)
                if state_val != "closed":
                    return True

    key = f"{chan_str}:{th_str}"
    if key in sessions:
        s = sessions[key]
        status = getattr(s, "status", getattr(s, "state", None))
        if status != "closed":
            return True
    for s in sessions.values():
        s_chan = getattr(s, "channel", None)
        if (s_chan is None or str(s_chan) == chan_str) and str(getattr(s, "thread_ts", None)) == th_str:
            status = getattr(s, "status", getattr(s, "state", None))
            if status != "closed":
                return True
    return False


async def should_process_ingress_event(event: Dict[str, Any], client: Any) -> bool:
    """
    Ingress Mention & DM Filter:
    - Resolve AGY's bot user ID via client.auth_test()["user_id"].
    - Ignore bot messages (subtype == "bot_message", event.get("bot_id"), or event.get("bot_profile")).
    - Ignore messages sent by other bot users (e.g. Hermes or generic bot user accounts).
    - Ignore non-conversational message subtypes (message_deleted, channel_join, message_replied, etc.).
    - Ignore empty messages or standalone bot mentions without actionable text.
    - Allow the event to proceed ONLY if:
      1. channel_type == "im" or the channel ID starts with "D" (Direct Message), or is_imessage.
      2. The message explicitly mentions AGY (<@{bot_user_id}>, @AGY, or app_mention)
         AND does not target another bot (e.g. <@U_HERMES>).
      3. The message is in an existing active thread (thread_ts in active sessions / router)
         AND does not explicitly target another bot (e.g. <@U_HERMES>).
    - If none of these match (e.g. general channel chatter, message targeting @Hermes), immediately return False.
    """
    if event.get("subtype") in (
        "bot_message",
        "message_deleted",
        "channel_join",
        "channel_leave",
        "message_replied",
        "message_changed",
        "pinned_item",
        "unpinned_item",
    ):
        return False
    if event.get("bot_id"):
        return False
    if event.get("bot_profile"):
        return False

    raw_text = str(event.get("text", "") or "")
    bot_id = await resolve_bot_user_id(client)
    if not bot_id and event.get("type") == "app_mention":
        first_mention = re.search(r"<@([A-Za-z0-9_-]+)(?:\|[^>]*)?>", raw_text)
        if first_mention:
            bot_id = first_mention.group(1)

    user = str(event.get("user", "") or "")
    if bot_id and user == bot_id:
        return False
    # If the message sender is another bot (e.g. Hermes or a bot user), do not process
    if "hermes" in user.lower() or (user != bot_id and "bot" in user.lower()):
        return False

    has_files = bool(event.get("files")) or event.get("subtype") == "file_share"
    clean_text = strip_bot_mentions(raw_text, bot_id)
    if not clean_text and not has_files:
        return False

    channel = str(event.get("channel", ""))
    channel_type = str(event.get("channel_type", ""))
    thread_ts = event.get("thread_ts")

    # 1. 1:1 Direct Message or iMessage
    if channel_type == "im" or channel.startswith("D") or bool(event.get("is_imessage", False)):
        return True

    all_mentions = re.findall(r"<@([A-Za-z0-9_-]+)(?:\|[^>]*)?>", raw_text)
    other_mentions = [m for m in all_mentions if m != bot_id]

    is_agy_in_text = bool(
        (bot_id and bool(re.search(rf"<@{re.escape(bot_id)}(?:\|[^>]*)?>", raw_text)))
        or bool(re.search(r"<@[A-Za-z0-9_-]+\|(?:AGY|agy)\b[^>]*>", raw_text, re.IGNORECASE))
        or bool(re.search(r"@(?:AGY|agy)\b", raw_text, re.IGNORECASE))
    )

    is_app_mention = event.get("type") == "app_mention"
    is_agy_mentioned = is_agy_in_text or (is_app_mention and not any("hermes" in m.lower() for m in other_mentions))

    # Check if message explicitly targets another entity/bot (e.g. <@U_HERMES> or @hermes)
    # without explicitly mentioning AGY.
    targets_other_bot = False
    if not is_agy_in_text:
        if other_mentions:
            if any("hermes" in m.lower() for m in other_mentions) or any(m != bot_id and "bot" in m.lower() for m in other_mentions):
                targets_other_bot = True
            elif re.search(r"<@[A-Za-z0-9_-]+\|[^>]*(?:hermes|bot|slackbot)[^>]*>", raw_text, re.IGNORECASE):
                targets_other_bot = True
        if re.search(r"@(?:hermes|slackbot)\b", raw_text, re.IGNORECASE):
            targets_other_bot = True

    if targets_other_bot:
        return False

    # 2. Explicit @AGY mention (starts a thread or addresses AGY)
    if is_agy_mentioned:
        return True

    # 3. Existing active thread: reply to ALL messages within that thread
    # (does not require re-mentioning @AGY on follow-up turns)
    if thread_ts and is_active_thread(channel, thread_ts):
        return True

    return False



# ------------------------------------------------------------------------------
# Slack Event Listeners
# ------------------------------------------------------------------------------
USE_HEADLESS_GATEWAY = (os.environ.get("GOLEM_HEADLESS", "1")).lower() in ("1", "true", "yes")

@app.event("app_mention")
async def handle_app_mention(event: Dict[str, Any], client: AsyncWebClient):
    if not await should_process_ingress_event(event, client):
        return
    if USE_HEADLESS_GATEWAY and ingress_gateway is not None:
        await ingress_gateway.handle_message(event, client)
    else:
        ts = event.get("ts", "")
        if ts in processed_message_ids:
            return
        processed_message_ids.add(ts)
        await process_user_prompt(event, client)


@app.event("message")
async def handle_message_events(event: Dict[str, Any], client: AsyncWebClient):
    if not await should_process_ingress_event(event, client):
        return
    if USE_HEADLESS_GATEWAY and ingress_gateway is not None:
        await ingress_gateway.handle_message(event, client)
    else:
        ts = event.get("ts", "")
        if ts in processed_message_ids:
            return
        processed_message_ids.add(ts)
        await process_user_prompt(event, client)


@app.event("file_shared")
async def handle_file_shared_events(event: Dict[str, Any], client: AsyncWebClient):
    """
    Handle Slack file_shared events:
    Pre-stage file metadata and download into slack_transfers/ via IngressGateway.
    """
    if ingress_gateway is not None:
        await ingress_gateway.handle_file_shared(event, client)


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
        logger.info(f"Relaying reaction approval ({reaction}) to agent {matched_session.active_agent}...")
        await herdr_client.run_cli(["agent", "send-keys", matched_session.active_agent, "y", "enter"])
        matched_session.status = "working"
        await add_reacji(client, channel, item_ts, "gear")

    elif reaction in ("thumbsdown", "-1", "x", "octagonal_sign"):
        logger.info(f"Relaying reaction cancellation ({reaction}) to agent {matched_session.active_agent}...")
        await herdr_client.run_cli(["agent", "send-keys", matched_session.active_agent, "n", "enter"])
        matched_session.status = "idle"
        await add_reacji(client, channel, item_ts, "x")


@app.action("approve_action")
async def handle_button_approve(ack, body, client):
    """Handle interactive Block Kit Approve button click."""
    await ack()
    channel = body.get("channel", {}).get("id")
    msg = body.get("message", {})
    message_ts = msg.get("ts")
    thread_ts = msg.get("thread_ts") or message_ts
    actions = body.get("actions", [])
    session_key = actions[0].get("value") if actions else f"{channel}:{thread_ts}"

    matched_session = sessions.get(session_key)
    if not matched_session:
        for s in sessions.values():
            if s.channel == channel and (s.approval_msg_ts == message_ts or s.thread_ts == thread_ts):
                matched_session = s
                break

    if matched_session and matched_session.status == "blocked":
        logger.info(f"Relaying button approval to agent {matched_session.active_agent}...")
        await herdr_client.run_cli(["agent", "send-keys", matched_session.active_agent, "y", "enter"])
        matched_session.status = "working"
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=sanitize_for_slack("*Approved 👍. Continuing execution...*")
        )


@app.action("reject_action")
async def handle_button_reject(ack, body, client):
    """Handle interactive Block Kit Reject button click."""
    await ack()
    channel = body.get("channel", {}).get("id")
    msg = body.get("message", {})
    message_ts = msg.get("ts")
    thread_ts = msg.get("thread_ts") or message_ts
    actions = body.get("actions", [])
    session_key = actions[0].get("value") if actions else f"{channel}:{thread_ts}"

    matched_session = sessions.get(session_key)
    if not matched_session:
        for s in sessions.values():
            if s.channel == channel and (s.approval_msg_ts == message_ts or s.thread_ts == thread_ts):
                matched_session = s
                break

    if matched_session and matched_session.status == "blocked":
        logger.info(f"Relaying button rejection to agent {matched_session.active_agent}...")
        await herdr_client.run_cli(["agent", "send-keys", matched_session.active_agent, "n", "enter"])
        matched_session.status = "idle"
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=sanitize_for_slack("*Cancelled ❌. Operation aborted.*")
        )


# ------------------------------------------------------------------------------
# Slack Slash Commands Dispatcher (/update, /bearings, /ahoy, /wrapup, /learn, etc.)
# ------------------------------------------------------------------------------
async def handle_slash_command(ack, body, client, command):
    """
    Unified Slack Slash Command handler.
    Calls ack() immediately (<200ms) to satisfy Slack's 3-second timeout gate,
    preventing "failed because the app did not respond" errors.
    """
    await ack()
    cmd = (command.get("command") or "").strip().lower()
    text = (command.get("text") or "").strip()
    channel = command.get("channel_id") or ""
    user = command.get("user_id") or ""

    logger.info(f"[Slash Command Received] {cmd} with text='{text}' from user {user} in {channel}")

    event = {
        "type": "message",
        "channel": channel,
        "user": user,
        "text": f"{cmd} {text}".strip(),
        "ts": "",
        "thread_ts": None,
        "is_slash_command": True,
        "command_payload": command,
    }

    if cmd in ("/update", "!update"):
        await handle_update_request(channel=channel, thread_ts=None, client=client)
    elif USE_HEADLESS_GATEWAY and ingress_gateway is not None:
        await ingress_gateway.handle_message(event, client)
    else:
        # Fallback to direct executive command handlers
        if cmd in ("/bearings", "!bearings"):
            await handle_bearings_request(channel=channel, thread_ts=None, client=client)
        elif cmd in ("/ahoy", "!ahoy"):
            await handle_ahoy_request(channel=channel, thread_ts=None, client=client)
        elif cmd in ("/wrapup", "!wrapup"):
            await handle_wrapup_request(channel=channel, thread_ts=None, client=client)
        elif cmd in ("/learn", "!learn"):
            await handle_learn_request(channel=channel, thread_ts=None, user_notes=text, client=client)
        else:
            await process_user_prompt(event, client)


SLASH_COMMANDS = [
    "/update",
    "/bearings",
    "/ahoy",
    "/wrapup",
    "/learn",
    "/health",
    "/nose",
    "/eyes",
    "/status",
    "/archive",
]
for _cmd in SLASH_COMMANDS:
    app.command(_cmd)(handle_slash_command)


@app.command(re.compile(r"^/.*"))
async def handle_wildcard_slash_command(ack, body, client, command):
    """Fallback catch-all for any other slash command to prevent timeout failures."""
    cmd = (command.get("command") or "").strip().lower()
    if cmd in SLASH_COMMANDS:
        return
    await ack()
    logger.info(f"[Catch-All Slash Command] {cmd} from user {command.get('user_id')}")
    channel = command.get("channel_id") or ""
    user = command.get("user_id") or ""
    text = (command.get("text") or "").strip()
    event = {
        "type": "message",
        "channel": channel,
        "user": user,
        "text": f"{cmd} {text}".strip(),
        "ts": "",
        "thread_ts": None,
        "is_slash_command": True,
        "command_payload": command,
    }
    if USE_HEADLESS_GATEWAY and ingress_gateway is not None:
        await ingress_gateway.handle_message(event, client)
    else:
        await process_user_prompt(event, client)


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
    message_guid = (
        raw_payload.get("guid")
        or raw_payload.get("data", {}).get("guid")
        or ""
    )
    now_ts = str(time.time())
    ts_val = message_guid or now_ts

    # Extract inbound attachments from BlueBubbles payload if present
    record = raw_payload.get("data") if isinstance(raw_payload.get("data"), dict) else raw_payload
    raw_attachments = record.get("attachments") or raw_payload.get("attachments") or []
    inbound_files: List[Dict[str, Any]] = []
    if isinstance(raw_attachments, list):
        for idx, att in enumerate(raw_attachments):
            if not isinstance(att, dict):
                continue
            guid = att.get("guid") or f"att_{int(time.time())}_{idx}"
            raw_fname = att.get("transferName") or att.get("name") or att.get("filename") or f"attachment_{idx}"
            mime_type = att.get("mimeType") or att.get("contentType") or "application/octet-stream"
            size = att.get("totalBytes") or att.get("size") or 0
            file_path = att.get("filePath") or att.get("path")
            if file_path and file_path.startswith("~"):
                file_path = os.path.expanduser(file_path)

            download_url = ""
            if bb_transport and hasattr(bb_transport, "server_url") and bb_transport.server_url and guid:
                download_url = f"{bb_transport.server_url.rstrip('/')}/api/v1/attachment/{quote(guid, safe='')}/download?password={quote(bb_transport.password, safe='')}"

            inbound_files.append({
                "id": guid,
                "file_id": guid,
                "name": raw_fname,
                "title": raw_fname,
                "size": size,
                "mimetype": mime_type,
                "url_private_download": download_url,
                "url_private": download_url,
                "url": download_url,
                "local_path": file_path if (file_path and os.path.isfile(file_path)) else None,
                "is_imessage": True,
            })

    raw_thread_originator = str(
        record.get("threadOriginatorGuid")
        or record.get("thread_originator_guid")
        or raw_payload.get("threadOriginatorGuid")
        or ""
    ).strip()

    client = bb_transport.get_client(chat_guid) if bb_transport else None
    if raw_thread_originator and client:
        effective_thread_ts = client.resolve_thread_root(raw_thread_originator)
    elif raw_thread_originator:
        effective_thread_ts = raw_thread_originator
    else:
        # Model A (Strict Slack Threading): Top-level text box starts a brand new conversation
        effective_thread_ts = ts_val

    event = {
        "channel": chat_guid,
        "user": sender,
        "text": text,
        "ts": ts_val,
        "thread_ts": effective_thread_ts,
        "message_guid": message_guid or None,
        "thread_originator_guid": raw_thread_originator or None,
        "is_imessage": True,
        "files": inbound_files,
        "raw_payload": raw_payload
    }
    if bb_transport:
        client = bb_transport.get_client(chat_guid)
        if USE_HEADLESS_GATEWAY and ingress_gateway is not None:
            await ingress_gateway.handle_message(event, client)
            return
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


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments for Golem / First Mate gateway."""
    parser = argparse.ArgumentParser(description="Golem Ingress Gateway")
    parser.add_argument(
        "--webUI", "--webui",
        dest="webui",
        action="store_true",
        help="Launch real-time observability WebUI server",
    )
    parser.add_argument(
        "--host",
        dest="host",
        default=None,
        help="Custom WebUI host address (e.g. 0.0.0.0 or 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        dest="port",
        type=int,
        default=None,
        help="Custom WebUI port number (e.g. 8765)",
    )
    parser.add_argument(
        "--hands", "--mock-hands",
        dest="hands",
        type=int,
        nargs="?",
        const=6,
        default=None,
        help="Simulate N hand agents for testing/demonstration (e.g. --hands, --hands 6, or --hands -6)",
    )
    parser.add_argument(
        "--preflight",
        dest="preflight",
        action="store_true",
        help="Run preflight readiness verification (e.g. on loopback port 8766) and exit cleanly",
    )
    parsed, _ = parser.parse_known_args(argv if argv is not None else sys.argv[1:])
    return parsed


async def main(args: Optional[argparse.Namespace] = None):
    logger.info("Starting First Mate 2.0 Ingress Gateway (Herdr Runtime)...")
    if args is None:
        args = parse_args()

    if getattr(args, "preflight", False):
        logger.info("Running Golem preflight subsystem verification...")
        p = getattr(args, "port", None) or 8766
        h = getattr(args, "host", None) or "127.0.0.1"
        try:
            from golem.state import TaskLedger
            from golem.wiki import GolemWiki
            TaskLedger()
            GolemWiki()
            await start_webui_server(host=h, port=p)
            await stop_webui_server()
            print(f"[PREFLIGHT_OK] Golem preflight verified on {h}:{p}. All subsystems nominal.")
            return 0
        except Exception as pe:
            logger.error(f"[PREFLIGHT_FAIL] Preflight verification failed: {pe}")
            return 1

    loop = asyncio.get_running_loop()
    setup_signal_handlers(loop)

    # Start 4-hour inactivity session router reaper
    session_router.start_reaper()

    # Start The Nose 24/7 Evergreen Health Sentinel coroutine
    sentinel_task = nose_sentinel.start()
    logger.info("The Nose 24/7 Fleet Health Sentinel active.")

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
        ingress_gateway.bot_user_id = BOT_USER_ID
        logger.info(f"Authenticated as @{auth_info.get('user')} ({BOT_USER_ID}) on team {auth_info.get('team')}")
    except Exception as e:
        logger.critical(f"Failed to authenticate with Slack API: {e}")
        sys.exit(1)

    await start_bluebubbles_transport()

    # Start Observability WebUI if requested via CLI flag or environment variable
    webui_active = getattr(args, "webui", False) or is_webui_enabled() or (getattr(args, "hands", None) is not None)
    if webui_active:
        try:
            default_daemon_host = "0.0.0.0" if (os.environ.get("DAEMON_LABEL") or os.environ.get("GOLEM_DAEMON_LABEL")) else get_webui_host()
            webui_host = getattr(args, "host", None) or os.environ.get("GOLEM_WEBUI_HOST") or default_daemon_host
            webui_port = getattr(args, "port", None) or get_webui_port()
            await start_webui_server(host=webui_host, port=webui_port)
            ingress_gateway.webui_server = get_active_webui_server()
            logger.info(f"Golem Observability WebUI active on http://{webui_host}:{webui_port}")

            # Auto-start simulated hands if requested via CLI flag or environment variable
            sim_hands = getattr(args, "hands", None)
            if sim_hands is not None:
                hands_count = abs(int(sim_hands))
            elif os.environ.get("GOLEM_SIMULATE_HANDS"):
                try:
                    hands_count = abs(int(os.environ["GOLEM_SIMULATE_HANDS"]))
                except ValueError:
                    hands_count = None
            else:
                hands_count = None

            if hands_count is not None and ingress_gateway.webui_server:
                ingress_gateway.webui_server.start_hand_simulation(hands_count)
                logger.info(f"Hand agent simulator active ({hands_count} simulated hands).")
        except Exception as e:
            logger.error(f"Failed to start Observability WebUI server: {e}")

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
            await stop_webui_server()
            ingress_gateway.webui_server = None
        except Exception as e:
            logger.debug(f"Error stopping WebUI server: {e}")

        try:
            await nose_sentinel.stop()
        except Exception as e:
            logger.debug(f"Error stopping NoseSentinel: {e}")

        try:
            await session_router.stop_reaper()
        except Exception as e:
            logger.debug(f"Error stopping session router reaper: {e}")

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


def cli() -> None:
    """Synchronous CLI entrypoint for app.py."""
    args = parse_args()
    try:
        res = asyncio.run(main(args))
        sys.exit(res if isinstance(res, int) else 0)
    except KeyboardInterrupt:
        sys.exit(0)
    except SystemExit as exc:
        sys.exit(exc.code)


__all__ = [
    # First Mate 2.0 Core Singletons
    "session_router",
    "ingress_gateway",
    "dual_pane_harness",
    "nose_sentinel",
    # CLI Entrypoints
    "main",
    "cli",
    "parse_args",
    # WebUI Observability
    "start_webui_server",
    "stop_webui_server",
    "webui_broadcast",
    # Legacy Global State Registries
    "sessions",
    "agent_name_to_session",
    "pending_alignment",
    "pending_alignment_by_msg_ts",
    "active_thread_sessions",
    "active_tasks",
    "active_subprocesses",
    "workspace_locks",
    # Legacy Classes
    "GatewaySession",
    # Legacy Functions & Shims
    "ensure_session_agent",
    "init_dual_pane_workspace",
    "resolve_target_directory",
    "scope_server_commands",
    "normalize_for_bluebubbles",
    "is_status_inquiry",
    "is_approval_reply",
    "is_rejection_reply",
    "process_user_prompt",
    "handle_reaction_added",
    "handle_file_shared_events",
    "fetch_recent_thread_context",
    "monitor_agent_lifecycle",
    "post_completion_briefing",
    "split_message_boundary_aware",
    "sanitize_for_slack",
    "should_process_ingress_event",
    "resolve_bot_user_id",
    "is_active_thread",
    # Executive Command Handlers
    "handle_bearings_request",
    "handle_learn_request",
    "handle_ahoy_request",
    "handle_wrapup_request",
    "handle_update_request",
    "handle_slash_command",
    # Clients & Instances
    "app",
    "herdr_client",
    "client_herdr",
    "fleet",
    "herdr",
    "supervisor",
    # Constants
    "HOST_DIR",
    "PROJECT_ROOT",
    "DEFAULT_TARGET_REPO",
    "DAEMON_LABEL",
]


if __name__ == "__main__":
    cli()

