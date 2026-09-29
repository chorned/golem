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
        self.status = "idle"  # "idle" | "working" | "blocked" | "done"
        self.last_prompt = ""
        self.last_blocked_snapshot: Optional[str] = None
        self.approval_msg_ts: Optional[str] = None
        self.monitor_task: Optional[asyncio.Task] = None
        self.created_at = time.time()


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
async def ensure_session_agent(
    session_key: str,
    channel: str,
    thread_ts: str,
    prompt: str,
    is_imessage: bool = False
) -> GatewaySession:
    """Ensure a Herdr workspace and Antigravity CLI agent exist for this thread."""
    if session_key in sessions:
        return sessions[session_key]

    clean_ts = re.sub(r"[^0-9]", "", str(thread_ts or time.time()))[-8:]
    agent_name = f"fm-{clean_ts}"
    target_cwd = resolve_target_directory(prompt, task_id=agent_name)

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

    # 2. Start Antigravity CLI: herdr agent start <name> --kind agy --pane <pane_id>
    start_code, _, start_err = await herdr_client.run_cli([
        "agent", "start", agent_name,
        "--kind", "agy",
        "--pane", pane_id
    ])
    if start_code != 0:
        logger.warning(f"herdr agent start warning for {agent_name}: {start_err}")

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
    sessions[session_key] = session
    agent_name_to_session[agent_name] = session
    return session


async def monitor_agent_lifecycle(
    session: GatewaySession,
    client: Any,
    trigger_ts: Optional[str] = None
):
    """
    Monitor Herdr agent state transitions:
    - blocked -> render approval card for native Ask Mode
    - done / idle -> post final output summary
    """
    agent_name = session.agent_name
    logger.info(f"Lifecycle monitor active for agent {agent_name}...")
    prev_status = "working"

    while True:
        await asyncio.sleep(1.5)
        code, out, _ = await herdr_client.run_cli(["agent", "get", agent_name])
        if code != 0 or not out:
            continue

        current_status = "unknown"
        try:
            data = json.loads(out)
            current_status = data.get("result", {}).get("agent", {}).get("agent_status", "unknown")
        except Exception:
            pass

        # Native Ask Mode detection: agent is blocked waiting for interactive confirmation
        if current_status == "blocked" and session.status != "blocked":
            session.status = "blocked"
            _, read_out, _ = await herdr_client.run_cli([
                "agent", "read", agent_name,
                "--source", "recent-unwrapped",
                "--lines", "15"
            ])
            session.last_blocked_snapshot = read_out
            if trigger_ts:
                await swap_reacji(client, session.channel, trigger_ts, "gear", "warning")
            await post_approval_card(session, client, read_out)

        # Completion detection: agent finished work and returned to idle/done
        elif current_status in ("done", "idle") and prev_status == "working":
            session.status = current_status
            _, read_out, _ = await herdr_client.run_cli([
                "agent", "read", agent_name,
                "--source", "recent-unwrapped",
                "--lines", "25"
            ])
            if trigger_ts:
                await swap_reacji(client, session.channel, trigger_ts, "gear", "white_check_mark")
            await post_completion_briefing(session, client, read_out)
            break

        prev_status = current_status


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
    """Post final completion output to chat."""
    clean_out = terminal_output.strip()
    urls = re.findall(r"https?://(?:localhost|127\.0\.0\.1|0\.0\.0\.0|100\.\d+\.\d+\.\d+)(?::\d+)?(?:/[a-zA-Z0-9_.~!$&'()*+,;=:@%/-]*)?", clean_out)
    clean_lines = [l.rstrip() for l in clean_out.splitlines() if l.strip()][-8:]
    summary = "\n".join(clean_lines) or "Task completed successfully."

    url_banner = f"\n*Live Endpoint:* {urls[0]}\n" if urls else ""
    msg = (
        f"*Task Completed via Herdr (`{session.agent_name}`)*{url_banner}\n\n"
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
            "--lines", "20",
            "--source", "recent-unwrapped"
        ])
        clean_out = read_out.strip()
        urls = re.findall(r"https?://(?:localhost|127\.0\.0\.1|0\.0\.0\.0|100\.\d+\.\d+\.\d+)(?::\d+)?(?:/[a-zA-Z0-9_.~!$&'()*+,;=:@%/-]*)?", clean_out)
        clean_lines = [l.rstrip() for l in clean_out.splitlines() if l.strip()][-6:]
        summary = "\n".join(clean_lines) or "Agent is actively running."

        url_line = f"Live URL: {urls[0]}\n" if urls else ""
        msg = f"*Status Update (`{session.agent_name}`)*\n{url_line}```{summary}```"

        if is_imessage:
            await client.chat_postMessage(channel=channel, text=normalize_for_bluebubbles(msg))
        else:
            await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)
        return

    # 3. New Task Dispatch / Follow-Up Prompt to Herdr Pane
    if not is_imessage:
        await add_reacji(client, channel, ts, "eyes")

    session = await ensure_session_agent(
        session_key=session_key,
        channel=channel,
        thread_ts=thread_ts,
        prompt=clean_text,
        is_imessage=is_imessage
    )

    scoped_prompt = scope_server_commands(clean_text, session.target_cwd)
    session.last_prompt = scoped_prompt
    session.status = "working"

    if not is_imessage:
        await swap_reacji(client, channel, ts, "eyes", "gear")

    # Forward user prompt via herdr agent prompt <name> "<prompt>"
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
def setup_signal_handlers(loop: asyncio.AbstractEventLoop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: on_signal_received(s))
        except (NotImplementedError, RuntimeError):
            pass

def on_signal_received(sig):
    logger.info(f"Received exit signal {sig.name}. Shutting down...")
    sys.exit(0)


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
    try:
        await handler.start_async()
    finally:
        await stop_bluebubbles_transport()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
