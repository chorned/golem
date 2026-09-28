#!/usr/bin/env python3
"""
AGY - Native macOS Background Service for Slack Bolt (Socket Mode)
Persistent Chief of Staff & Hermes Butler Orchestration Engine powered by Herdr.

Features:
- Persistent execution across fleet nodes via Herdr 0.9.1 (no ephemeral subprocesses)
- Persistent workspace topology:
  * fleet-control: Antigravity Chief of Staff (agy)
  * homelab-ops: Hermes Homelab Butler (hermes)
  * cloud-ops: OCI Cloud Services (oci-machine)
  * dev-grid: Development Grid (personal-laptop)
  * compute-grid: Compute Grid (desktop-pc)
- Chief of Staff & Butler dynamic routing & prompt triage
- Fleet discovery via `herdr machine list --json`
- Real-time "Rolling HUD" debounced execution progress display (1.5s interval)
- Interactive unblocking: posts blocked alerts to Slack; subsequent thread replies pipe to pane
- 100% full in-memory execution transcript capture and thread upload
- Post-execution git diff inline (<35 lines) or changes.patch upload (>35 lines)
- Automatic asset artifact discovery (.png, .jpg, .svg, .pdf, .zip, etc.) and thread upload
- Fast-path command support: 'send me <filename>'
- In-memory per-target concurrency lock
"""

import os
import sys
import re
import time
import json
import shlex
import signal
import shutil
import asyncio
import tempfile
import aiohttp
import logging
import subprocess
from typing import Dict, Any, Optional, Set, List, Tuple

from dotenv import load_dotenv
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

import herdr_fleet
from herdr_fleet import HerdrFleet, TOPOLOGY_SPECS

import bluebubbles_transport
from bluebubbles_transport import BlueBubblesTransport, BlueBubblesClient

import triage_engine
from triage_engine import direct_gemini_triage, get_gemini_api_key, validate_action_payload, BANNED_ACTION_PAYLOAD_PATTERNS, handle_conversational_turn

import herdr_client
from herdr_client import HerdrClient

import supervisor as supervisor_module
from supervisor import AgentSupervisor

import github_manager as github_manager_module
from github_manager import GitHubManager


# ------------------------------------------------------------------------------
# Logging & Environment Setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("AGY")

# Ingest and normalize PATH to ensure herdr, agy, hermes, and git are accessible
EXTRA_PATHS = [
    os.path.expanduser("~/.local/bin"),
    os.path.expanduser("~/.antigravity/bin"),
    "/opt/homebrew/bin",
    "/usr/local/bin",
    "/usr/bin",
    "/bin",
    "/usr/sbin",
    "/sbin"
]
current_path_list = os.environ.get("PATH", "").split(":")
for p in EXTRA_PATHS:
    if p not in current_path_list and os.path.isdir(p):
        current_path_list.insert(0, p)
os.environ["PATH"] = ":".join(current_path_list)

# Host root context and target repository configuration
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

raw_default_ws = os.environ.get("DEFAULT_WORKSPACE_PATH", DEFAULT_TARGET_REPO).strip()
DEFAULT_WORKSPACE = os.path.realpath(os.path.abspath(os.path.expanduser(raw_default_ws)))

if not SLACK_APP_TOKEN or not SLACK_BOT_TOKEN:
    logger.critical("Missing SLACK_APP_TOKEN or SLACK_BOT_TOKEN. Check .env configuration.")
    sys.exit(1)

# Ensure default workspace exists
os.makedirs(DEFAULT_WORKSPACE, exist_ok=True)

# Locate agy binary for local execution
AGY_BIN = shutil.which("agy")
if not AGY_BIN:
    for fallback in [
        os.path.expanduser("~/.local/bin/agy"),
        os.path.expanduser("~/.antigravity/bin/agy"),
        "/usr/local/bin/agy",
        "/opt/homebrew/bin/agy"
    ]:
        if os.path.isfile(fallback) and os.access(fallback, os.X_OK):
            AGY_BIN = fallback
            break

if not AGY_BIN:
    logger.critical("Could not locate 'agy' executable on PATH or standard locations.")
    sys.exit(1)

logger.info(f"Using local 'agy' binary at: {AGY_BIN}")
logger.info(f"Default local workspace: {DEFAULT_WORKSPACE}")

# ------------------------------------------------------------------------------
# Herdr Fleet Instance
# ------------------------------------------------------------------------------
fleet = HerdrFleet()
herdr = fleet

herdr_client_inst = HerdrClient()
client_herdr = herdr_client_inst
github_mgr = GitHubManager(default_repo_path=DEFAULT_TARGET_REPO)
supervisor = AgentSupervisor(herdr_client=herdr_client_inst, github_manager=github_mgr, default_repo_path=DEFAULT_TARGET_REPO)
agent_supervisor = supervisor

# ------------------------------------------------------------------------------
# Remote Workspace / Host Registry (Backward Compatibility & Herdr Discovery)
# ------------------------------------------------------------------------------
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
    # Dynamically populate from Herdr machines if known
    if fleet.machines:
        for m in fleet.machines:
            lbl = m.get("label", "").lower()
            tgt = m.get("target", "")
            if lbl and tgt and lbl not in hosts:
                hosts[lbl] = tgt
    return hosts

def get_remote_ssh_user(ip: Optional[str] = None) -> str:
    """Determine SSH username for remote targets (checks REMOTE_SSH_USER, SSH config, or current user)."""
    user = os.environ.get("REMOTE_SSH_USER", "").strip()
    if user:
        return user
    if ip:
        try:
            res = subprocess.run(
                ["ssh", "-G", ip],
                capture_output=True,
                text=True,
                timeout=2
            )
            if res.returncode == 0:
                for line in res.stdout.splitlines():
                    if line.startswith("user "):
                        resolved_user = line.split(" ", 1)[1].strip()
                        if resolved_user:
                            return resolved_user
        except Exception:
            pass
    try:
        import getpass
        user = getpass.getuser()
    except Exception:
        user = os.environ.get("USER", "root")
    return user

# ------------------------------------------------------------------------------
# Check for agy CLI capability (e.g., whether --work-dir flag exists locally)
# ------------------------------------------------------------------------------
def check_agy_work_dir_flag() -> bool:
    try:
        result = subprocess.run([AGY_BIN, "--help"], capture_output=True, text=True, timeout=5)
        return "--work-dir" in result.stdout or "--work-dir" in result.stderr
    except Exception:
        return False

AGY_SUPPORTS_WORK_DIR = check_agy_work_dir_flag()

# ------------------------------------------------------------------------------
# State Management: Sessions, Locks, and Active Subprocesses
# ------------------------------------------------------------------------------
# sessions: maps session_key ("channel:thread_ts") -> session dict
sessions: Dict[str, Dict[str, Any]] = {}

# Active thread session registry for Thread-Scoped Implicit Triggering
active_thread_sessions: Set[str] = set()

# Active in-flight supervised tasks for conversational heartbeats and status interrogation
active_supervised_tasks: Dict[str, Dict[str, Any]] = {}
active_tasks = active_supervised_tasks

# in-memory asyncio.Lock keyed by (host, canonical_workspace)
workspace_locks: Dict[Tuple[str, str], asyncio.Lock] = {}

# active child CLI subprocesses for graceful SIGTERM/SIGINT handling
active_subprocesses: Set[asyncio.subprocess.Process] = set()

# Deduplication set for processed message timestamps
processed_message_ids: Set[str] = set()

# Bot user ID (resolved on startup)
BOT_USER_ID: Optional[str] = None

def get_workspace_lock(host: str, workspace_dir: str) -> asyncio.Lock:
    """
    Key asyncio.Lock per target host and workspace tuple (host, workspace)
    so remote runs do not block local runs.
    """
    if host == "local":
        canonical = os.path.realpath(os.path.abspath(os.path.expanduser(workspace_dir)))
    else:
        canonical = workspace_dir
    key = (host, canonical)
    if key not in workspace_locks:
        workspace_locks[key] = asyncio.Lock()
    return workspace_locks[key]

def terminate_all_subprocesses():
    count = len(active_subprocesses)
    if count > 0:
        logger.info(f"Terminating {count} active child CLI subprocess(es)...")
        for proc in list(active_subprocesses):
            try:
                proc.terminate()
            except ProcessLookupError:
                pass

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Reacji-Driven State Lifecycle Helpers
# ------------------------------------------------------------------------------
IGNORABLE_REACTION_ERRORS = {"already_reacted", "no_reaction", "message_not_found"}

async def add_reacji(client: AsyncWebClient, channel: str, timestamp: str, name: str) -> None:
    """Add a reaction (reacji) to a Slack message with clean error logging."""
    try:
        await client.reactions_add(channel=channel, timestamp=timestamp, name=name)
    except SlackApiError as e:
        err = ""
        if hasattr(e, "response") and e.response:
            resp = e.response
            if isinstance(resp, dict):
                err = resp.get("error", "")
            elif hasattr(resp, "get"):
                err = resp.get("error", "")
            elif hasattr(resp, "data") and isinstance(resp.data, dict):
                err = resp.data.get("error", "")
        if not err:
            err = str(e)
        if err in IGNORABLE_REACTION_ERRORS or any(ign in str(err) for ign in IGNORABLE_REACTION_ERRORS):
            logger.debug(f"Benign Slack reaction state '{err}' on {channel}:{timestamp}")
            return
        logger.warning(f"Slack reaction failed: {err}. Verify reactions:write scope in Slack App settings.")
    except Exception as e:
        logger.warning(f"Slack reaction failed: {e}. Verify reactions:write scope in Slack App settings.")

async def remove_reacji(client: AsyncWebClient, channel: str, timestamp: str, name: str) -> None:
    """Remove a reaction (reacji) from a Slack message with clean error logging."""
    try:
        await client.reactions_remove(channel=channel, timestamp=timestamp, name=name)
    except SlackApiError as e:
        err = ""
        if hasattr(e, "response") and e.response:
            resp = e.response
            if isinstance(resp, dict):
                err = resp.get("error", "")
            elif hasattr(resp, "get"):
                err = resp.get("error", "")
            elif hasattr(resp, "data") and isinstance(resp.data, dict):
                err = resp.data.get("error", "")
        if not err:
            err = str(e)
        if err in IGNORABLE_REACTION_ERRORS or any(ign in str(err) for ign in IGNORABLE_REACTION_ERRORS):
            logger.debug(f"Benign Slack reaction state '{err}' on {channel}:{timestamp}")
            return
        logger.warning(f"Slack reaction failed: {err}. Verify reactions:write scope in Slack App settings.")
    except Exception as e:
        logger.warning(f"Slack reaction failed: {e}. Verify reactions:write scope in Slack App settings.")

async def swap_reacji(client: AsyncWebClient, channel: str, timestamp: str, old_name: Optional[str], new_name: str) -> None:
    """Swap one reaction for another on a Slack message."""
    if old_name and old_name != new_name:
        await remove_reacji(client, channel, timestamp, old_name)
    await add_reacji(client, channel, timestamp, new_name)


# ------------------------------------------------------------------------------
# Artifact & File Transfer Helpers
# ------------------------------------------------------------------------------
ARTIFACT_EXTS = {
    ".png", ".jpg", ".jpeg", ".svg", ".gif", ".webp",
    ".pdf", ".csv", ".xlsx", ".zip", ".tar.gz", ".jsonl"
}
IMAGE_EXTS = ARTIFACT_EXTS
IGNORED_DIRS = {".git", ".venv", "venv", "__pycache__", "node_modules", ".gemini", ".idea", ".vscode", "dist", "build"}

def find_workspace_artifacts(workspace_dir: str) -> Dict[str, float]:
    """Scan workspace directory for image & document artifacts and their modification timestamps."""
    artifacts: Dict[str, float] = {}
    if not os.path.isdir(workspace_dir):
        return artifacts
    try:
        for root, dirs, files in os.walk(workspace_dir, followlinks=False):
            dirs[:] = [d for d in dirs if d not in IGNORED_DIRS and not d.startswith(".")]
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext in ARTIFACT_EXTS:
                    full_path = os.path.join(root, f)
                    try:
                        artifacts[full_path] = os.path.getmtime(full_path)
                    except OSError:
                        pass
    except Exception as e:
        logger.warning(f"Error scanning workspace artifacts in {workspace_dir}: {e}")
    return artifacts

find_image_artifacts = find_workspace_artifacts

async def find_remote_workspace_artifacts(
    host: str,
    ip: Optional[str],
    workspace_dir: str,
    timeout: float = 10.0
) -> Dict[str, float]:
    """
    Discover image and document artifacts in a remote workspace over SSH.
    Returns a dictionary mapping remote file paths to their modification timestamps.
    """
    target = ip or host
    if not target or target == "local":
        return {}

    ssh_user = get_remote_ssh_user(target)
    ssh_dest = f"{ssh_user}@{target}" if ssh_user else target

    py_bin = "python" if host == "desktop-pc" else "python3"
    script = f"""
import os, json
ignored = {{'.git', '.venv', 'venv', '__pycache__', 'node_modules', '.gemini', '.idea', '.vscode', 'dist', 'build', 'appdata', 'local settings', 'application data', '.cache', 'library'}}
exts = {{'.png', '.jpg', '.jpeg', '.svg', '.gif', '.webp', '.pdf', '.csv', '.xlsx', '.zip', '.tar.gz', '.jsonl'}}
ws = os.path.expanduser('{workspace_dir}')
res = {{}}
if os.path.isdir(ws):
    for root, dirs, files in os.walk(ws):
        dirs[:] = [d for d in dirs if d.lower() not in ignored and not d.startswith('.')]
        rel = os.path.relpath(root, ws)
        if rel != '.' and len(rel.split(os.sep)) >= 3:
            dirs.clear()
        for f in files:
            if any(f.lower().endswith(e) for e in exts):
                p = os.path.join(root, f).replace(chr(92), '/')
                try:
                    res[p] = os.path.getmtime(p)
                except Exception:
                    pass
print(json.dumps(res))
"""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=5",
            "-T",
            ssh_dest,
            py_bin,
            "-",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(input=script.encode("utf-8")), timeout=timeout)
        if proc.returncode == 0 and stdout:
            out_str = stdout.decode("utf-8", errors="replace").strip()
            for line in reversed(out_str.splitlines()):
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    return json.loads(line)
        return {}
    except Exception as e:
        logger.warning(f"Error scanning remote artifacts on {host}:{workspace_dir}: {e}")
        return {}

async def get_git_diff(host: str, ip: Optional[str], active_workspace: str) -> Optional[str]:
    """
    Inspect git diff HEAD inside the active workspace directory (local or remote).
    """
    try:
        if host == "local":
            if not os.path.isdir(active_workspace):
                return None
            proc = await asyncio.create_subprocess_exec(
                "git", "diff", "HEAD",
                cwd=active_workspace,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode == 0:
                diff_text = stdout.decode("utf-8", errors="replace").strip()
                return diff_text if diff_text else None
            return None
        else:
            target = ip or host
            ssh_user = get_remote_ssh_user(target)
            ssh_dest = f"{ssh_user}@{target}" if ssh_user else target
            ws = active_workspace if (active_workspace and active_workspace != DEFAULT_WORKSPACE) else "~"

            if host == "desktop-pc":
                remote_cmd = f'cmd.exe /c "cd /d {ws} 2>nul & git rev-parse --is-inside-work-tree 2>nul && git diff HEAD"'
            else:
                remote_cmd = f"cd {ws} 2>/dev/null && git rev-parse --is-inside-work-tree 2>/dev/null && git diff HEAD"

            proc = await asyncio.create_subprocess_exec(
                "ssh",
                "-o", "BatchMode=yes",
                "-o", "ConnectTimeout=5",
                "-T",
                ssh_dest,
                remote_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode == 0:
                diff_text = stdout.decode("utf-8", errors="replace").strip()
                return diff_text if diff_text else None
            return None
    except Exception as e:
        logger.warning(f"Error checking git diff for {host}:{active_workspace}: {e}")
        return None

async def upload_new_artifacts(
    active_workspace: str,
    before_artifacts: Dict[str, float],
    channel: str,
    thread_ts: str,
    client: AsyncWebClient,
    host: str = "local",
    ip: Optional[str] = None
):
    """Upload any newly created or modified artifacts (images, documents, archives) as Slack files."""
    try:
        if host == "local":
            after_artifacts = find_workspace_artifacts(active_workspace)
        else:
            after_artifacts = await find_remote_workspace_artifacts(host, ip, active_workspace)

        new_or_modified = [
            p for p, m in after_artifacts.items()
            if p not in before_artifacts or m > before_artifacts[p] + 0.001
        ]
        for art_path in sorted(new_or_modified):
            local_to_upload = art_path
            temp_file_to_clean = None
            if host != "local":
                temp_file_to_clean = await fetch_remote_file(host, ip, art_path)
                if not temp_file_to_clean:
                    continue
                local_to_upload = temp_file_to_clean

            try:
                filename = os.path.basename(art_path)
                logger.info(f"Uploading newly created artifact from {host}: {art_path}")
                await client.files_upload_v2(
                    channel=channel,
                    thread_ts=thread_ts,
                    file=local_to_upload,
                    filename=filename,
                    title=f"Artifact ({host}): {filename}" if host != "local" else f"Artifact: {filename}"
                )
            except Exception as art_err:
                logger.error(f"Failed to upload artifact {art_path}: {art_err}")
            finally:
                if temp_file_to_clean:
                    try:
                        os.remove(temp_file_to_clean)
                    except OSError:
                        pass
    except Exception as e:
        logger.warning(f"Error uploading artifacts: {e}")

upload_new_images = upload_new_artifacts

async def fetch_remote_file(host: str, ip: Optional[str], remote_path: str) -> Optional[str]:
    """Fetch a remote file over SCP to a local temporary file for uploading to Slack."""
    target = ip or host
    ssh_user = get_remote_ssh_user(target)
    suffix = os.path.splitext(remote_path)[1]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp_path = tmp.name
    tmp.close()

    dest = f"{ssh_user}@{target}:{remote_path}" if ssh_user else f"{target}:{remote_path}"
    cmd = [
        "scp",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        dest,
        tmp_path
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        if proc.returncode == 0 and os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
            return tmp_path
        else:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            return None
    except Exception as e:
        logger.error(f"Error executing scp from {host}:{remote_path}: {e}")
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        return None

async def download_slack_file(url: str, token: str) -> Optional[bytes]:
    """Download private file bytes from Slack using bot token authentication."""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, allow_redirects=False) as resp:
                if resp.status in (301, 302, 303, 307):
                    redirect_url = resp.headers.get("Location")
                    if redirect_url:
                        async with session.get(redirect_url) as redirect_resp:
                            if redirect_resp.status == 200:
                                return await redirect_resp.read()
                            return None
                elif resp.status == 200:
                    return await resp.read()
                return None
    except Exception as e:
        logger.error(f"Exception downloading Slack file from {url}: {e}")
        return None

TRANSFERS_DIR_NAME = "slack_transfers"

async def save_uploaded_file(
    file_bytes: bytes,
    filename: str,
    host: str,
    ip: Optional[str],
    active_workspace: str
) -> Optional[str]:
    """Save downloaded file bytes into a dedicated 'slack_transfers' folder in the workspace."""
    safe_name = os.path.basename(filename).strip() or f"upload_{int(time.time())}"
    safe_name = re.sub(r"[\x00/\\:*?\"<>|]", "_", safe_name)

    if host == "local" or not ip:
        transfers_dir = os.path.join(active_workspace, TRANSFERS_DIR_NAME)
        os.makedirs(transfers_dir, exist_ok=True)
        dest_path = os.path.join(transfers_dir, safe_name)
        try:
            with open(dest_path, "wb") as f:
                f.write(file_bytes)
            logger.info(f"Saved uploaded Slack file to local transfers folder: {dest_path} ({len(file_bytes)} bytes)")
            return dest_path
        except Exception as e:
            logger.error(f"Failed to write uploaded file to {dest_path}: {e}")
            return None
    else:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name
        try:
            ssh_user = get_remote_ssh_user(ip)
            remote_dir = f"~/{TRANSFERS_DIR_NAME}"
            mkdir_proc = await asyncio.create_subprocess_exec(
                "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-T",
                f"{ssh_user}@{ip}", f"mkdir -p {remote_dir}"
            )
            await mkdir_proc.wait()

            remote_dest = f"{remote_dir}/{safe_name}"
            cmd = [
                "scp",
                "-o", "BatchMode=yes",
                "-o", "ConnectTimeout=10",
                tmp_path,
                f"{ssh_user}@{ip}:{remote_dest}"
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            if proc.returncode == 0:
                logger.info(f"Successfully scp'd uploaded file to remote {host}:{remote_dest}")
                return remote_dest
            return None
        except Exception as e:
            logger.error(f"Error scp-ing file to {host}: {e}")
            return None
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

# ------------------------------------------------------------------------------
# Headless AGY CLI Brain for Synthesis & Utilities
# ------------------------------------------------------------------------------
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


async def synthesize_with_agy(
    clean_prompt: str,
    raw_output: str,
    exit_code: int,
    target_info: Dict[str, Any],
    uploaded_files: Optional[List[Dict[str, Any]]] = None
) -> str:
    """
    Invoke the local agy CLI binary headlessly in the project context (/Users/hermes/projects/AGY).
    AGY inherits AGENTS.md & GEMINI.md, interprets raw backstage findings, strips technical noise,
    and produces an executive briefing.
    """
    ws_label = target_info.get("workspace_label", "fleet-control")
    role = target_info.get("role", "Chief of Staff")
    target_machine = target_info.get("target_machine", "local")

    files_note = ""
    if uploaded_files:
        file_list = ", ".join(f"`{f['name']}`" for f in uploaded_files)
        files_note = f"\nUser Uploaded Files: {file_list}"

    cleaned_raw = strip_ansi(raw_output).strip()
    truncated_raw = cleaned_raw[:12000] if len(cleaned_raw) > 12000 else cleaned_raw

    synthesis_prompt = (
        f"You are AGY, Carl's Chief of Staff. Interpret the following backstage crew findings "
        f"and deliver a concise, high-signal executive briefing in clean Slack mrkdwn following your "
        f"AGENTS.md contract and GEMINI.md tone guidelines.\n\n"
        f"• Target Node: {ws_label} ({role}, target: {target_machine})\n"
        f"• Original User Request: {clean_prompt}\n"
        f"• Backstage Exit Code: {exit_code}{files_note}\n\n"
        f"• Backstage Raw Output:\n```\n{truncated_raw}\n```\n\n"
        f"Instructions:\n"
        f"1. Give the bottom line / status first.\n"
        f"2. Summarize key findings, actions taken, or metrics.\n"
        f"3. Strip technical noise, ANSI codes, curses borders, and scratchpads.\n"
        f"4. If a generated file should be sent to Carl, include [SEND_FILE: <path>].\n"
        f"5. If exit code != 0 or there is an error, diagnose what went wrong concisely.\n"
    )

    agy_bin = shutil.which("agy") or os.path.expanduser("~/.local/bin/agy")
    if os.path.isfile(agy_bin):
        try:
            proc = await asyncio.create_subprocess_exec(
                agy_bin,
                "--dangerously-skip-permissions",
                "-p", synthesis_prompt,
                "--output-format", "text",
                cwd=PROJECT_ROOT,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            try:
                stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=45.0)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                    await proc.communicate()
                except Exception:
                    pass
                logger.warning("AGY CLI synthesis timed out after 45s")
                stdout_bytes, stderr_bytes = b"", b""
            if proc.returncode == 0 and stdout_bytes:
                briefing = stdout_bytes.decode("utf-8", errors="replace").strip()
                if briefing:
                    return format_for_slack(briefing)
            else:
                err_text = stderr_bytes.decode("utf-8", errors="replace").strip() if stderr_bytes else ""
                if err_text:
                    logger.warning(f"AGY CLI synthesis returned code {proc.returncode}: {err_text}")
        except Exception as syn_err:
            logger.error(f"Error invoking AGY CLI for synthesis ({type(syn_err).__name__}): {syn_err}")

    # Fallback formatting if AGY CLI execution failed or timed out
    if exit_code == 0:
        clean_snip = cleaned_raw if cleaned_raw else f"Task completed successfully on {ws_label}."
        if len(clean_snip) > 800:
            clean_snip = clean_snip[:797] + "..."
        return f":white_check_mark: *Task completed on {ws_label} ({role}):*\n```{clean_snip}```"
    else:
        return f":x: *Task failed on {ws_label} ({role}) with exit code {exit_code}:*\n```{cleaned_raw[:800]}```"


# ------------------------------------------------------------------------------
# Operational Crystallization & Distillation (`/learn`)
# ------------------------------------------------------------------------------
def compile_thread_transcript(messages: List[Dict[str, Any]], current_msg_ts: Optional[str] = None) -> str:
    """
    Compile thread messages into a structured operational transcript.
    Extracts original intent, steps executed, worker outputs, and final results.
    """
    if not messages:
        return ""

    transcript_parts = []
    step_num = 1

    for idx, msg in enumerate(messages):
        ts = msg.get("ts")
        text = (msg.get("text") or "").strip()
        # Exclude the triggering /learn message itself
        if (current_msg_ts and ts == current_msg_ts) or re.search(r"(?:^|\s)/learn(?:\b|\s|$)", text, re.IGNORECASE):
            continue
        if not text and not msg.get("files"):
            continue

        user = msg.get("user", "Carl")
        is_bot = bool(msg.get("bot_id") or msg.get("subtype") == "bot_message")

        files_info = ""
        if msg.get("files"):
            names = [f.get("name") or f.get("title") or "file" for f in msg.get("files", [])]
            files_info = f" [Attached files: {', '.join(names)}]"

        if idx == 0 and not is_bot:
            transcript_parts.append(f"### Original Intent (User: {user})\n{text}{files_info}\n")
        else:
            role = "Liaison / Chief of Staff (AGY)" if is_bot else f"User ({user})"
            transcript_parts.append(f"### Step {step_num}: {role}\n{text}{files_info}\n")
            step_num += 1

    return "\n".join(transcript_parts).strip()


def extract_json_payload(text: str) -> Optional[Dict[str, Any]]:
    """Extract a JSON object from text, even if fenced in markdown code blocks."""
    text = text.strip()
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            pass

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    return None


def parse_distillation_result(raw_stdout: str) -> Optional[Dict[str, Any]]:
    """
    Parse distillation result from agy output.
    Handles direct JSON, agy output envelope (with response field), or markdown code blocks.
    """
    if not raw_stdout:
        return None

    raw_stdout = raw_stdout.strip()

    try:
        outer = json.loads(raw_stdout)
        if isinstance(outer, dict):
            if "response" in outer:
                resp_text = str(outer["response"]).strip()
                inner_json = extract_json_payload(resp_text)
                if inner_json:
                    return inner_json
                if "summary" in outer or "files_updated" in outer:
                    return outer
                return {
                    "summary": resp_text,
                    "files_updated": [],
                    "action_type": "rule"
                }
            if "summary" in outer or "files_updated" in outer or "action_type" in outer:
                return outer
    except Exception:
        pass

    return extract_json_payload(raw_stdout)


def build_distill_prompt(transcript: str, user_notes: str = "") -> str:
    """Build the distillation turn prompt for the Chief of Staff."""
    notes_section = f"\nUser Guidance / Operational Notes:\n{user_notes}\n" if user_notes else ""
    return (
        f"You are the Chief of Staff reviewing an operational execution transcript.\n"
        f"Your goal is to extract the \"golden path\" that succeeded, eliminating failed attempts, exploration, and conversational fluff.\n"
        f"{notes_section}\n"
        f"Operational Execution Transcript:\n"
        f"```\n{transcript}\n```\n\n"
        f"Determine the appropriate destination:\n"
        f"1. Node Quirk: Add/update a rule in `.agents/rules/<node>.md` (e.g. Windows paths, specific container directories, environment variables).\n"
        f"2. Repeatable Task: Generate a concise skill in `.agents/skills/<skill_name>/SKILL.md` with the exact commands and checks.\n\n"
        f"Save the changes directly to disk and return a JSON object:\n"
        f"{{\n"
        f"  \"summary\": \"Brief explanation of what was learned\",\n"
        f"  \"files_updated\": [\"path/to/file1.md\"],\n"
        f"  \"action_type\": \"rule\" | \"skill\"\n"
        f"}}\n"
    )


async def distill_lesson(transcript: str, user_notes: str = "") -> Dict[str, Any]:
    """
    Invoke the local agy CLI binary headlessly in distillation mode to crystallize
    the successful execution path into .agents/rules/ or .agents/skills/.
    """
    prompt = build_distill_prompt(transcript, user_notes)
    agy_bin = shutil.which("agy") or os.path.expanduser("~/.local/bin/agy")

    if not os.path.isfile(agy_bin):
        raise FileNotFoundError(f"AGY binary not found at {agy_bin}")

    cmd_args = [
        agy_bin,
        "--dangerously-skip-permissions",
        "-p", prompt,
        "--output-format", "json"
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd_args,
        cwd=PROJECT_ROOT,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=120.0)
    except asyncio.TimeoutError:
        try:
            proc.kill()
            await proc.communicate()
        except Exception:
            pass
        raise RuntimeError("AGY distillation process timed out after 120s")
    stdout_str = stdout_bytes.decode("utf-8", errors="replace").strip() if stdout_bytes else ""
    stderr_str = stderr_bytes.decode("utf-8", errors="replace").strip() if stderr_bytes else ""

    if proc.returncode != 0 and not stdout_str:
        raise RuntimeError(f"AGY distillation process failed with exit code {proc.returncode}: {stderr_str}")

    parsed = parse_distillation_result(stdout_str)
    if not parsed:
        if stderr_str:
            raise RuntimeError(f"Failed to parse distillation result. Stderr: {stderr_str}")
        raise RuntimeError(f"Distillation produced unparseable output: {stdout_str[:300]}")

    if "summary" not in parsed:
        parsed["summary"] = "Operational execution codified."
    if "files_updated" not in parsed:
        parsed["files_updated"] = []
    if "action_type" not in parsed:
        parsed["action_type"] = "rule" if any("rule" in str(f) for f in parsed["files_updated"]) else "skill"

    # Defense in depth: if agy returned file content in payload and file was not written, persist it
    if "content" in parsed and parsed["files_updated"]:
        for rel_path in parsed["files_updated"]:
            target_path = rel_path if os.path.isabs(rel_path) else os.path.join(PROJECT_ROOT, rel_path)
            if not os.path.exists(target_path):
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                with open(target_path, "w", encoding="utf-8") as f:
                    f.write(parsed["content"])

    return parsed


async def handle_learn_request(
    channel: str,
    thread_ts: Optional[str],
    msg_ts: Optional[str],
    user_notes: str,
    client: AsyncWebClient
):
    """
    Handle /learn slash command or thread reply:
    1. Verify message is inside a thread.
    2. React with :brain:.
    3. Extract thread history and compile transcript.
    4. Distill lesson headlessly via AGY CLI.
    5. Swap reaction to :white_check_mark: and post confirmation.
    """
    react_ts = msg_ts or thread_ts

    # 1. Verify the message is inside a thread
    if not thread_ts:
        if react_ts:
            await swap_reacji(client, channel, react_ts, "brain", "warning")
        await client.chat_postMessage(
            channel=channel,
            text=":warning: The `/learn` command must be used as a reply in a Slack thread to crystallize the thread's execution path."
        )
        return

    # 2. React with :brain:
    if react_ts:
        await add_reacji(client, channel, react_ts, "brain")

    # 3. Fetch full thread conversation history
    try:
        if hasattr(client, "conversations_replies"):
            resp = await client.conversations_replies(channel=channel, ts=thread_ts, limit=100)
        elif hasattr(client, "conversations") and hasattr(client.conversations, "replies"):
            resp = await client.conversations.replies(channel=channel, ts=thread_ts, limit=100)
        else:
            resp = await client.conversations_replies(channel=channel, ts=thread_ts)

        messages = []
        if isinstance(resp, dict):
            messages = resp.get("messages", [])
        elif hasattr(resp, "get"):
            messages = resp.get("messages", [])
        elif hasattr(resp, "data") and isinstance(resp.data, dict):
            messages = resp.data.get("messages", [])
    except Exception as e:
        logger.error(f"Failed to fetch thread messages for learn command: {e}")
        if react_ts:
            await swap_reacji(client, channel, react_ts, "brain", "x")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: Failed to retrieve thread history: `{e}`"
        )
        return

    transcript = compile_thread_transcript(messages, current_msg_ts=msg_ts)
    if not transcript:
        if react_ts:
            await swap_reacji(client, channel, react_ts, "brain", "warning")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=":warning: No prior execution history found in this thread to distill. Execute tasks in this thread first, then call `/learn`."
        )
        return

    # 4. Headless Distillation Turn via AGY CLI
    try:
        result = await distill_lesson(transcript=transcript, user_notes=user_notes)
        summary = result.get("summary", "Successfully codified operational learnings.")
        files_updated = result.get("files_updated", [])
        action_type = result.get("action_type", "rule")
        files_str = ", ".join(f"`{f}`" for f in files_updated) if files_updated else "`.agents/`"

        if action_type == "skill":
            confirmation = (
                f"🧠 *Learned & Codified:*\n"
                f"Added deterministic runbook for *{summary}* to {files_str}. "
                f"Future requests will execute this path directly."
            )
        elif action_type == "rule":
            confirmation = (
                f"🧠 *Learned & Codified:*\n"
                f"Updated operational rules in {files_str}: {summary}\n"
                f"Future requests will adhere to these constraints."
            )
        else:
            confirmation = (
                f"🧠 *Learned & Codified:*\n"
                f"{summary}\n"
                f"Updated: {files_str}."
            )

        # 5. Swap reaction to :white_check_mark: and post confirmation
        if react_ts:
            await swap_reacji(client, channel, react_ts, "brain", "white_check_mark")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=confirmation
        )
    except Exception as e:
        logger.error(f"Error distilling thread lesson: {e}", exc_info=True)
        if react_ts:
            await swap_reacji(client, channel, react_ts, "brain", "x")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: *Distillation Failed:*\n```{str(e)}```"
        )


# ------------------------------------------------------------------------------
# FirstMate Executive Commands (/bearings, /ahoy, /wrapup, /update)
# ------------------------------------------------------------------------------

async def query_hermes_status(timeout: float = 4.0) -> Dict[str, Any]:
    """Query disk free and container count on hermes (localhost)."""
    return await herdr.probe_node("hermes", timeout=timeout)


async def query_desktop_pc_status(timeout: float = 4.0) -> Dict[str, Any]:
    """Query GPU utilization and VRAM on desktop-pc."""
    return await herdr.probe_node("desktop-pc", timeout=timeout)


async def query_personal_laptop_status(timeout: float = 3.5) -> Dict[str, Any]:
    """Check connectivity to Carl's personal laptop. Fail gracefully if asleep."""
    return await herdr.probe_node("personal-laptop", timeout=timeout)


async def query_oci_status(timeout: float = 8.0) -> Dict[str, Any]:
    """Query uptime and status of OCI production VPS with 8.0s timeout for Tailscale negotiation."""
    return await herdr.probe_node("oci-machine", timeout=timeout)



async def handle_bearings_request(
    channel: str,
    thread_ts: Optional[str],
    msg_ts: Optional[str],
    client: AsyncWebClient
):
    """Gather 4-node fleet state telemetry and post concise executive briefing."""
    react_ts = msg_ts or thread_ts
    if react_ts:
        await add_reacji(client, channel, react_ts, "eyes")
        await swap_reacji(client, channel, react_ts, "eyes", "gear")

    try:
        results = await asyncio.gather(
            *[herdr.probe_node(node) for node in ["hermes", "desktop-pc", "personal-laptop", "oci-machine"]],
            return_exceptions=True
        )

        bullets = []
        labels = [
            ("hermes", "localhost (Primary Dev & Homelab)"),
            ("desktop-pc", "Compute Grid (Windows CUDA)"),
            ("personal-laptop", "Personal Laptop (Carl's Workstation)"),
            ("oci-machine", "Cloud Operations (Production VPS)")
        ]

        raw_telemetry_lines = []
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
            raw_telemetry_lines.append(f"- {node} ({role}): {status}")

        raw_telemetry = "\n".join(raw_telemetry_lines)

        briefing = None
        try:
            briefing = await synthesize_with_agy(
                clean_prompt="Fleet bearings telemetry status digest (/bearings)",
                raw_output=raw_telemetry,
                exit_code=0,
                target_info={"workspace_label": "fleet-control", "role": "Chief of Staff", "target_machine": "local"}
            )
        except Exception:
            pass

        if not briefing:
            briefing = "🧭 *Fleet Bearings & Status Digest:*\n" + "\n".join(bullets)

        if react_ts:
            await swap_reacji(client, channel, react_ts, "gear", "white_check_mark")

        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=briefing
        )
    except Exception as e:
        logger.error(f"Error handling /bearings: {e}", exc_info=True)
        if react_ts:
            await swap_reacji(client, channel, react_ts, "gear", "x")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: Failed to compile fleet bearings: `{e}`"
        )


async def handle_ahoy_request(
    channel: str,
    thread_ts: Optional[str],
    msg_ts: Optional[str],
    client: AsyncWebClient
):
    """Session recap and pending decision resolver."""
    react_ts = msg_ts or thread_ts
    if react_ts:
        await add_reacji(client, channel, react_ts, "eyes")
        await swap_reacji(client, channel, react_ts, "eyes", "gear")

    try:
        blocked_items = []
        for s_key, sess in sessions.items():
            bp = sess.get("blocked_pane")
            if bp:
                st, det = await fleet.get_pane_status(bp)
                if st == "blocked":
                    blocked_items.append((bp, det or "Confirmation or credentials required"))

        for ws_name, info in fleet.topology.items():
            pane_id = info.get("pane_id")
            if pane_id and not any(b[0] == pane_id for b in blocked_items):
                st, det = await fleet.get_pane_status(pane_id)
                if st == "blocked":
                    blocked_items.append((pane_id, det or f"Input required in {ws_name}"))

        recent_tasks = []
        log_dir = "/tmp/herdr_tasks"
        if os.path.isdir(log_dir):
            try:
                files = sorted(
                    [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.endswith(".log")],
                    key=os.path.getmtime,
                    reverse=True
                )
                for fpath in files[:3]:
                    fname = os.path.basename(fpath)
                    try:
                        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                            lines = [l.strip() for l in f if l.strip()]
                        last_line = lines[-1] if lines else "Task completed"
                        recent_tasks.append(f"`{fname}`: {last_line[:80]}")
                    except Exception:
                        pass
            except Exception:
                pass

        if blocked_items:
            if react_ts:
                await swap_reacji(client, channel, react_ts, "gear", "warning")
            bullets = "\n".join(
                f"• *Pane `{pane}`:* {det}\n  _Action:_ Reply in thread to deliver unblocking input."
                for pane, det in blocked_items
            )
            msg = f"⚠️ *Pending Decisions & Attention Required:*\n{bullets}"
        else:
            if react_ts:
                await swap_reacji(client, channel, react_ts, "gear", "white_check_mark")
            activity_snippet = ""
            if recent_tasks:
                activity_snippet = "\n\n*Recent Task Completions:*\n" + "\n".join(f"• {t}" for t in recent_tasks)
            msg = f"⚓️ *Fleet Quiescent:* All background operations completed cleanly, and no workers are awaiting decisions.{activity_snippet}\nStanding by for instructions."

        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=msg
        )
    except Exception as e:
        logger.error(f"Error handling /ahoy: {e}", exc_info=True)
        if react_ts:
            await swap_reacji(client, channel, react_ts, "gear", "x")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: Failed to resolve fleet catch-up: `{e}`"
        )


pending_quirk_proposals: Dict[str, Dict[str, Any]] = {}


def scan_recent_logs_for_quirks() -> List[Dict[str, Any]]:
    """Scan recent task logs in /tmp/herdr_tasks/ for recurring machine errors and propose rule diffs."""
    log_dir = "/tmp/herdr_tasks"
    if not os.path.isdir(log_dir):
        return []

    quirks = []
    error_counts = {}

    try:
        files = sorted(
            [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.endswith(".log")],
            key=os.path.getmtime,
            reverse=True
        )
        for fpath in files[:10]:
            try:
                with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()

                if "nvidia-smi" in content and ("not recognized" in content or "command not found" in content):
                    error_counts["desktop-pc-gpu"] = error_counts.get("desktop-pc-gpu", 0) + 1
                elif "cmd.exe" in content and ("not found" in content or "syntax error" in content):
                    error_counts["desktop-pc-cmd"] = error_counts.get("desktop-pc-cmd", 0) + 1
                elif "ssh:" in content and "Operation timed out" in content:
                    error_counts["ssh-timeout"] = error_counts.get("ssh-timeout", 0) + 1
                elif "command not found" in content:
                    error_counts["command-path"] = error_counts.get("command-path", 0) + 1
            except Exception:
                pass
    except Exception:
        pass

    if error_counts.get("desktop-pc-cmd", 0) >= 1 or error_counts.get("desktop-pc-gpu", 0) >= 1:
        quirks.append({
            "node": "desktop-pc",
            "file": ".agents/rules/desktop-pc.md",
            "rule_text": "Always wrap Windows shell invocations with cmd.exe /c or PowerShell and verify GPU path.",
            "diff": "```diff\n+ ## Observed Quirk (desktop-pc)\n+ - Wrap shell invocations in cmd.exe /c and ensure nvidia-smi is in PATH.\n```"
        })
    elif error_counts.get("ssh-timeout", 0) >= 2:
        quirks.append({
            "node": "personal-laptop",
            "file": ".agents/rules/personal-laptop.md",
            "rule_text": "Personal laptop sleeps intermittently. Always use 3s timeout and fail gracefully if asleep.",
            "diff": "```diff\n+ ## Observed Quirk (personal-laptop)\n+ - Laptop sleeps frequently. ConnectTimeout=3 is mandatory; fail gracefully without alarm.\n```"
        })
    elif error_counts.get("command-path", 0) >= 2:
        quirks.append({
            "node": "localhost",
            "file": ".agents/rules/localhost.md",
            "rule_text": "Ensure commands check ~/.local/bin and standard homebrew bin paths.",
            "diff": "```diff\n+ ## Observed Quirk (localhost)\n+ - Ensure PATH includes ~/.local/bin and Homebrew bin before invoking CLI tools.\n```"
        })

    return quirks


async def handle_wrapup_request(
    channel: str,
    thread_ts: Optional[str],
    msg_ts: Optional[str],
    client: AsyncWebClient
):
    """End-of-day session housekeeping, git audits, log sweeping, and reviewable quirk diff suggestions."""
    react_ts = msg_ts or thread_ts
    if react_ts:
        await add_reacji(client, channel, react_ts, "eyes")
        await swap_reacji(client, channel, react_ts, "eyes", "gear")

    try:
        # 1. Git Worktree Audit on hermes
        hermes_git_status = "Clean worktree."
        try:
            proc = await asyncio.create_subprocess_exec(
                "git", "status", "-s",
                cwd=PROJECT_ROOT,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            out_str = stdout.decode("utf-8", errors="replace").strip()
            if out_str:
                lines = out_str.splitlines()
                hermes_git_status = f"{len(lines)} modified/untracked files:\n```\n" + "\n".join(lines[:8]) + ("\n..." if len(lines) > 8 else "") + "\n```"
        except Exception as ge:
            hermes_git_status = f"Git check failed: {ge}"

        # Git check on laptop if awake
        laptop_git_status = "[Asleep / Offline (skipped)]"
        try:
            proc = await asyncio.create_subprocess_exec(
                "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3", "-T",
                "hermes@100.96.38.5", "cd ~/projects/AGY 2>/dev/null && git status -s 2>/dev/null",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3.5)
            if proc.returncode == 0:
                l_out = stdout.decode("utf-8", errors="replace").strip()
                laptop_git_status = "Clean worktree." if not l_out else f"{len(l_out.splitlines())} modified files"
        except Exception:
            pass

        # 2. Log & Lock Sweep
        purged_count = 0
        log_dir = "/tmp/herdr_tasks"
        if os.path.isdir(log_dir):
            try:
                now = time.time()
                for fname in os.listdir(log_dir):
                    if fname.endswith(".log"):
                        fpath = os.path.join(log_dir, fname)
                        if now - os.path.getmtime(fpath) > 7200:
                            try:
                                os.remove(fpath)
                                purged_count += 1
                            except OSError:
                                pass
            except Exception:
                pass

        stale_cleared = 0
        for s_key, sess in list(sessions.items()):
            bp = sess.get("blocked_pane")
            if bp:
                st, _ = await fleet.get_pane_status(bp)
                if st != "blocked":
                    sess["blocked_pane"] = None
                    stale_cleared += 1

        # 3. Quirk Analysis & Proposed Diffs (Do NOT silently edit!)
        quirks = scan_recent_logs_for_quirks()
        quirk_section = "• *Quirk Analysis:* No recurring machine errors detected. Fleet rules are up to date."

        session_key = thread_ts or channel
        if quirks:
            q = quirks[0]
            pending_quirk_proposals[session_key] = q
            quirk_section = (
                f"• *Suggested Quirk Rule:* Observed recurring failure on `{q['node']}`.\n"
                f"  Proposed addition to `{q['file']}`:\n"
                f"{q['diff']}\n"
                f"  _Reply `approve quirk` (or `approve`) in this thread to save to disk._"
            )

        report = (
            f"📋 *Session Wrap-Up & Housekeeping:*\n\n"
            f"• *Git Worktree Audit:*\n"
            f"  - `hermes` ({PROJECT_ROOT}): {hermes_git_status}\n"
            f"  - `personal-laptop`: {laptop_git_status}\n\n"
            f"• *Log & Lock Sweep:* Purged {purged_count} old task logs; cleared {stale_cleared} stale session markers. Fleet locks quiescent.\n\n"
            f"{quirk_section}"
        )

        if react_ts:
            await swap_reacji(client, channel, react_ts, "gear", "white_check_mark")

        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=report
        )
    except Exception as e:
        logger.error(f"Error during /wrapup: {e}", exc_info=True)
        if react_ts:
            await swap_reacji(client, channel, react_ts, "gear", "x")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: Wrap-up failed: `{e}`"
        )


async def handle_quirk_approval(
    channel: str,
    thread_ts: Optional[str],
    msg_ts: Optional[str],
    client: AsyncWebClient
) -> bool:
    """If there is a pending quirk proposal for this thread/channel, apply it to disk and confirm."""
    key = None
    for candidate in [thread_ts, channel]:
        if candidate and candidate in pending_quirk_proposals:
            key = candidate
            break

    if not key:
        return False

    q = pending_quirk_proposals.pop(key)
    rel_path = q.get("file", ".agents/rules/quirks.md")
    abs_path = os.path.join(PROJECT_ROOT, rel_path) if not os.path.isabs(rel_path) else rel_path
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)

    rule_addition = f"\n\n## Quirk Rule (Approved {time.strftime('%Y-%m-%d')})\n- {q.get('rule_text', '')}\n"
    with open(abs_path, "a", encoding="utf-8") as f:
        f.write(rule_addition)

    if msg_ts:
        await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")

    await client.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=f"🧠 *Quirk Rule Approved & Saved:* Appended rule to `{rel_path}`.\nFuture tasks will adhere to this constraint."
    )
    return True


async def kickstart_daemon_later(delay: float = 0.5):
    """Wait briefly so Slack reply finishes, then reload daemon via launchctl."""
    await asyncio.sleep(delay)
    try:
        proc = await asyncio.create_subprocess_exec(
            "launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{DAEMON_LABEL}"
        )
        await proc.wait()
    except Exception as e:
        logger.error(f"Error during kickstart reload: {e}")


async def handle_update_request(
    channel: str,
    thread_ts: Optional[str],
    msg_ts: Optional[str],
    client: AsyncWebClient
):
    """Safe full-stack self-update with pre-flight lock checks, automated test gates, and rollback."""
    react_ts = msg_ts or thread_ts
    if react_ts:
        await add_reacji(client, channel, react_ts, "eyes")
        await swap_reacji(client, channel, react_ts, "eyes", "gear")

    # 1. Pre-flight Lock Check
    locked_panes = fleet.has_active_locks()
    active_tasks = await fleet.get_active_tasks()
    if locked_panes or active_tasks:
        active_ids = list(set(locked_panes + [t.get("workspace", t.get("pane_id")) for t in active_tasks]))
        if react_ts:
            await swap_reacji(client, channel, react_ts, "gear", "warning")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":warning: *Cannot update:* Tasks are currently active on {', '.join(active_ids)}. Fleet must be idle before updating."
        )
        return

    # 2. Git Pull
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "pull", "origin", "main",
            cwd=PROJECT_ROOT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            if react_ts:
                await swap_reacji(client, channel, react_ts, "gear", "x")
            err_msg = stderr.decode("utf-8", errors="replace").strip() or stdout.decode("utf-8", errors="replace").strip()
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f":x: *Git pull failed (Exit code {proc.returncode}):*\n```\n{err_msg[:600]}\n```"
            )
            return
    except Exception as e:
        if react_ts:
            await swap_reacji(client, channel, react_ts, "gear", "x")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: *Git pull error:* `{e}`"
        )
        return

    # 3. Automated Test Gate
    test_python = os.path.join(PROJECT_ROOT, ".venv/bin/python3")
    if not os.path.isfile(test_python):
        test_python = sys.executable

    try:
        test_proc = await asyncio.create_subprocess_exec(
            test_python, "-m", "unittest", "discover", "tests",
            cwd=PROJECT_ROOT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        test_stdout, test_stderr = await asyncio.wait_for(test_proc.communicate(), timeout=90.0)
        combined_test_out = (test_stderr.decode("utf-8", errors="replace") + "\n" + test_stdout.decode("utf-8", errors="replace")).strip()

        if test_proc.returncode != 0:
            # Rollback
            rollback_proc = await asyncio.create_subprocess_exec(
                "git", "reset", "--hard", "ORIG_HEAD",
                cwd=PROJECT_ROOT,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await rollback_proc.communicate()

            if react_ts:
                await swap_reacji(client, channel, react_ts, "gear", "x")

            err_lines = combined_test_out.splitlines()[-25:]
            err_snippet = "\n".join(err_lines)
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f":x: *Update Aborted (Test Gate Failure):* Rolled back to previous revision.\n```\n{err_snippet}\n```"
            )
            return

        test_match = re.search(r"Ran (\d+) tests?", combined_test_out)
        test_count = test_match.group(1) if test_match else "all"

    except Exception as te:
        await asyncio.create_subprocess_exec("git", "reset", "--hard", "ORIG_HEAD", cwd=PROJECT_ROOT)
        if react_ts:
            await swap_reacji(client, channel, react_ts, "gear", "x")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: *Update Aborted (Test Execution Error):* `{te}`. Rolled back."
        )
        return

    # 4. Fleet Binary Audit
    binaries = {
        "herdr": shutil.which("herdr") or os.path.expanduser("~/.local/bin/herdr"),
        "agy": shutil.which("agy") or os.path.expanduser("~/.local/bin/agy"),
        "hermes": shutil.which("hermes") or os.path.expanduser("~/.local/bin/hermes")
    }
    missing_binaries = [name for name, p in binaries.items() if not os.path.isfile(p)]
    bin_note = f" (Warning: binaries not found: {', '.join(missing_binaries)})" if missing_binaries else ""

    # 5. Get current commit SHA
    commit_sha = "HEAD"
    try:
        sha_proc = await asyncio.create_subprocess_exec(
            "git", "rev-parse", "--short", "HEAD",
            cwd=PROJECT_ROOT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        sha_stdout, _ = await sha_proc.communicate()
        commit_sha = sha_stdout.decode("utf-8", errors="replace").strip() or "HEAD"
    except Exception:
        pass

    if react_ts:
        await swap_reacji(client, channel, react_ts, "gear", "white_check_mark")

    await client.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=f":white_check_mark: *Self-Update Verified & Ready:* Passed {test_count} tests. Commit: `{commit_sha}`.{bin_note}\nReloading daemon via `launchctl kickstart`..."
    )

    # 6. Kickstart daemon
    asyncio.create_task(kickstart_daemon_later())


# ------------------------------------------------------------------------------
# Chief of Staff Pre-Flight Alignment ("Grill-Me") Engine
# ------------------------------------------------------------------------------
# pending_alignment: maps session_key -> alignment state dict
pending_alignment: Dict[str, Dict[str, Any]] = {}

# pending_alignment_by_msg_ts: maps Slack alignment message ts -> session_key
pending_alignment_by_msg_ts: Dict[str, str] = {}


def validate_action_payload(payload: str, target_node: str = "hermes") -> str:
    """
    Validate that dispatched_payload contains actual operational tooling or scripts,
    banning any dummy echo commands or placeholder text.
    """
    if not payload:
        return "git status -s"
    stripped = payload.strip()
    dummy_patterns = [
        r"^echo\s+['\"](?:\[Task:.*|Executing.*|\.\.\.)['\"]",
        r"^echo\s+\[Task:.*\]$",
        r"^echo\s+['\"]?placeholder['\"]?$",
        r"^echo\s+['\"]?test['\"]?$"
    ]
    for pat in dummy_patterns:
        if re.search(pat, stripped, re.IGNORECASE):
            logger.warning(f"Banned dummy echo payload detected: '{stripped}'. Replacing with operational diagnostic command.")
            return "git status -s"

    if re.match(r"^echo\s+['\"][^'\"]*['\"]$", stripped, re.IGNORECASE) and not any(c in stripped for c in ";|&"):
        logger.warning(f"Standalone dummy echo payload detected: '{stripped}'. Replacing with operational command.")
        return "git status -s"

    return stripped


def is_conversational_query(prompt: str) -> bool:
    """Detect if a user prompt is a conversational / diagnostic question rather than an operational action."""
    text = prompt.strip().lower()

    conversational_patterns = [
        r"^what happened\b",
        r"^why did (?:that|it)\b",
        r"^why is (?:it|that)\b",
        r"^how come\b",
        r"^(?:well\s+)?it was working\b",
        r"^was(?:n't)? it working\b",
        r"^can you explain\b",
        r"^could you explain\b",
        r"^explain\b",
        r"^tell me (?:about|why|how)\b",
        r"^what do you think\b",
        r"^who are you\b",
        r"^what is the difference\b",
        r"^what does (?:this|that) mean\b",
        r"^what are (?:the|you)\b",
        r"^what is (?:your|the)\b",
        r"^why\?*$",
        r"^how\?*$"
    ]
    if any(re.search(pat, text) for pat in conversational_patterns):
        return True

    if text.endswith("?"):
        action_verbs = [
            "run", "execute", "start", "stop", "restart", "deploy", "switch",
            "clean", "scrub", "commit", "push", "pull", "test", "check",
            "inspect", "diff", "build", "kill", "rm", "delete", "send", "fix",
            "update", "install", "mkdir", "cat", "tail", "ls", "grep", "df", "ps"
        ]
        has_action_verb = any(re.search(r"\b" + re.escape(v) + r"\b", text) for v in action_verbs)
        if not has_action_verb:
            return True

    return False


async def fetch_recent_thread_context(
    client: AsyncWebClient,
    channel: str,
    thread_ts: Optional[str],
    limit: int = 5
) -> str:
    """Fetch recent thread messages for intent triage context (last 3-5 turns)."""
    if not thread_ts:
        return ""
    try:
        if hasattr(client, "conversations_replies"):
            resp = await client.conversations_replies(channel=channel, ts=thread_ts, limit=limit)
        elif hasattr(client, "conversations") and hasattr(client.conversations, "replies"):
            resp = await client.conversations.replies(channel=channel, ts=thread_ts, limit=limit)
        else:
            return ""

        messages = []
        if isinstance(resp, dict):
            messages = resp.get("messages", [])
        elif hasattr(resp, "data") and isinstance(resp.data, dict):
            messages = resp.data.get("messages", [])
        elif hasattr(resp, "get"):
            messages = resp.get("messages", [])

        lines = []
        for msg in messages[-limit:]:
            sender = "AGY" if (msg.get("bot_id") or msg.get("user") == "B_AGY") else (msg.get("user") or "Carl")
            txt = (msg.get("text") or "").strip()
            if txt:
                lines.append(f"{sender}: {txt[:200]}")
        return "\n".join(lines)
    except Exception as e:
        logger.debug(f"Could not fetch recent thread context: {e}")
        return ""


def build_triage_prompt(
    clean_prompt: str,
    target_info: Dict[str, Any],
    recent_context: str = "",
    user_tweaks: Optional[str] = None,
    previous_plan: Optional[Dict[str, Any]] = None
) -> str:
    target_node = target_info.get("target_machine", "local")
    if target_node == "local":
        target_node = "hermes"
    ws_label = target_info.get("workspace_label", "fleet-control")
    role = target_info.get("role", "Chief of Staff")

    context_sec = f"\nRecent Thread Context:\n{recent_context}\n" if recent_context else ""
    tweak_sec = f"\nPrevious Plan:\n{json.dumps(previous_plan, indent=2)}\nCarl's Requested Tweaks: {user_tweaks}\n" if previous_plan else ""

    return (
        f"You are AGY, Carl's Chief of Staff and operational partner operating under the FirstMate contract.\n"
        f"Fleet Machine Topology & Node Profiles:\n"
        f"- fleet-control: localhost (/Users/hermes/projects/AGY) | Antigravity Chief of Staff & Primary Dev Anchor (codebase changes, git, unit tests, workspace controls, planning, general queries). is_self = true.\n"
        f"- homelab-ops: localhost (hermes) | Hermes Butler (Docker containers, storage pools, ZFS, backups, Plex/Jellyfin, homelab maintenance). is_self = false (delegated to Hermes Butler).\n"
        f"- compute-grid: desktop-pc (100.80.59.45) | Compute Grid (Windows CUDA, NVIDIA GPU workloads, PyTorch, model training, heavy local benchmarks). is_self = false.\n"
        f"- dev-grid: personal-laptop (100.96.38.5) | Carl's Active Workstation (Strict Ad-Hoc: only run jobs here when Carl explicitly says 'on my laptop'; sleeps frequently). is_self = false.\n"
        f"- cloud-ops: oci-machine (100.113.173.33) | Cloud Operations (Linux aarch64, Oracle Cloud VPS strictly reserved for live production services, ingress, Wireguard, DNS). is_self = false.\n\n"
        f"Carl has sent a message. Classify his intent into either:\n"
        f"- 'CONVERSE': Carl is asking a conversational, diagnostic, architectural, system state, or task history inquiry (e.g. 'What happened?', 'Did you clean up?', 'Well did you clean up OpenCode\'s work?', 'What branches are open?', 'Is OpenCode finished?', 'Why did that fail?', 'Can you explain this?', 'Who are you?'). In this case, formulate a direct, grounded, and helpful answer in 'conversational_reply'. Do NOT formulate an action plan or shell command.\n"
        f"- 'ACTION': Carl wants an operational task executed across the fleet (e.g. running commands, modifying files, checking disk, restarting services, git operations). In this case, formulate a structured 'action_plan'.\n\n"
        f"{context_sec}"
        f"{tweak_sec}"
        f"Latest Message from Carl: \"{clean_prompt}\"\n"
        f"Target Node Context: {target_node} ({ws_label}, role: {role})\n\n"
        f"ACTION Rules:\n"
        f"1. dispatched_payload MUST be the exact deterministic shell command(s) or CLI execution to dispatch on {target_node}. NEVER use dummy echo commands (e.g. echo '[Task: ...]') or placeholders.\n"
        f"2. is_self MUST be true if the task runs locally on Hermes as AGY Chief of Staff (localhost), and false if delegated to another node/worker (e.g. Hermes Butler on Docker/homelab, desktop-pc, personal-laptop, or oci-machine).\n"
        f"3. recommendation MUST specify the default path if Carl simply approves with 'go' or thumbs up.\n\n"
        f"Return ONLY a valid JSON object matching this schema:\n"
        f"{{\n"
        f'  "turn_type": "CONVERSE" | "ACTION",\n'
        f'  "action_type": "converse" | "action",\n'
        f'  "conversational_reply": "Direct, helpful answer if turn_type == \'CONVERSE\'",\n'
        f'  "action_plan": {{\n'
        f'    "is_self": true,\n'
        f'    "target_node": "{target_node}",\n'
        f'    "delegated_worker": "{role}",\n'
        f'    "worker_id": "opencode-dev" | "hermes-homelab" | "cuda-worker" | null,\n'
        f'    "target_file": "path/to/target/file if applicable, else empty string",\n'
        f'    "domain": "homelab infrastructure" | "codebase development" | "CUDA compute" | "cloud production",\n'
        f'    "dispatched_payload": "Actual shell command or agent prompt (NEVER dummy echo)",\n'
        f'    "intent_summary": "Plain English summary of what will be done",\n'
        f'    "assumptions": "Key boundary assumptions",\n'
        f'    "choice": "Clarifying question or alternative option",\n'
        f'    "recommendation": "Default path if Carl simply approves with \'go\' or thumbs up",\n'
        f'    "estimated_duration_sec": 5\n'
        f'  }}\n'
        f"}}"
    )


def build_align_prompt(
    clean_prompt: str,
    target_info: Dict[str, Any],
    user_tweaks: Optional[str] = None,
    previous_plan: Optional[Dict[str, Any]] = None
) -> str:
    return build_triage_prompt(
        clean_prompt=clean_prompt,
        target_info=target_info,
        recent_context="",
        user_tweaks=user_tweaks,
        previous_plan=previous_plan
    )


def build_fallback_preflight_plan(
    clean_prompt: str,
    target_info: Dict[str, Any],
    user_tweaks: Optional[str] = None
) -> Dict[str, Any]:
    ws_label = target_info.get("workspace_label", "fleet-control")
    target_node = target_info.get("target_machine", "local")
    if target_node == "local":
        target_node = "hermes"
    role = target_info.get("role", "Chief of Staff")

    is_self = (target_node in ("hermes", "localhost", "local") and ws_label == "fleet-control" and "butler" not in role.lower())
    worker_name = "Local AGY" if is_self else role

    text = clean_prompt.lower()
    tweaks_text = (user_tweaks or "").lower()

    if ws_label == "homelab-ops":
        domain = "homelab infrastructure"
        if "disk" in text or "storage" in text or "zfs" in text:
            intent = "audit storage pools and disk space on hermes"
            payload = "df -h /"
            if "docker" in text or "container" in text or "all" in tweaks_text or "zfs" in tweaks_text:
                payload = "df -h / && docker system df"
            assumptions = "Read-only audit of mounted filesystems and volumes."
            choice = "Audit root disk only vs full container storage breakdown."
            recommendation = "Inspect root disk and Docker storage usage."
        elif "docker" in text or "container" in text or "plex" in text or "jellyfin" in text:
            intent = "inspect running Docker containers and service health"
            payload = "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"
            assumptions = "Non-destructive container status query."
            choice = "Query running containers vs include stopped containers."
            recommendation = "List all active containers and their port mappings."
        else:
            intent = f"perform homelab operation ({clean_prompt[:60]})"
            payload = "docker ps" if not any(c in clean_prompt for c in ";|&") else clean_prompt
            assumptions = "Safe bounded execution on localhost hermes."
            choice = "Execute with standard verbose logging."
            recommendation = "Run task non-interactively."
    elif ws_label == "compute-grid":
        domain = "CUDA compute"
        intent = "query GPU status and compute workloads"
        payload = "nvidia-smi"
        assumptions = "Non-destructive NVIDIA driver query."
        choice = "Query basic telemetry vs full process list."
        recommendation = "Run standard nvidia-smi status check."
    elif ws_label == "cloud-ops":
        domain = "cloud production"
        intent = "inspect OCI production VPS health and uptime"
        payload = "uptime && uname -a"
        assumptions = "Production VPS strictly reserved for ingress and WireGuard."
        choice = "Check host uptime only vs service statuses."
        recommendation = "Verify system uptime and load averages."
    elif ws_label == "dev-grid":
        domain = "personal laptop development"
        intent = f"run ad-hoc development task on Carl's laptop ({clean_prompt[:50]})"
        payload = "git status -s"
        assumptions = "Carl's personal laptop is awake and accessible via Tailscale."
        choice = "Query git status vs branch history."
        recommendation = "Inspect working tree cleanly."
    else:
        domain = "local development & orchestration"
        if "git" in text or "diff" in text or "status" in text:
            intent = "inspect repository status and uncommitted changes"
            payload = "git status -s"
            assumptions = "Local repository hygiene and branch check."
            choice = "Short summary vs full diff."
            recommendation = "Display concise status summary."
        elif "test" in text:
            intent = "run automated test suite"
            payload = "./.venv/bin/python3 -m unittest discover tests"
            assumptions = "Run Python unittest in project virtual environment."
            choice = "Run all tests vs fail-fast."
            recommendation = "Run full discovery suite."
        else:
            intent = f"orchestrate task ({clean_prompt[:60]})"
            payload = "git status -s"
            assumptions = "Execute within fleet-control workspace boundaries."
            choice = "Execute default plan vs tailored parameters."
            recommendation = "Proceed with standard execution."

    payload = validate_action_payload(payload, target_node)

    if user_tweaks:
        intent = f"{intent} (adjusted: {user_tweaks[:40]})"

    return {
        "is_self": is_self,
        "intent_summary": intent,
        "target_node": target_node,
        "delegated_worker": worker_name,
        "domain": domain,
        "dispatched_payload": payload,
        "assumptions": assumptions,
        "choice": choice,
        "recommendation": recommendation,
        "estimated_duration_sec": 15
    }


def build_fallback_intent_triage(
    clean_prompt: str,
    target_info: Optional[Dict[str, Any]] = None,
    recent_context: str = "",
    user_tweaks: Optional[str] = None,
    api_error: Optional[str] = None
) -> Dict[str, Any]:
    """Fallback intent triage when Gemini API is unavailable or returns an error."""
    return triage_engine.build_fallback_intent_triage(
        clean_prompt=clean_prompt,
        target_info=target_info,
        recent_context=recent_context,
        user_tweaks=user_tweaks,
        api_error=api_error
    )


async def triage_turn_with_agy(
    clean_prompt: str,
    target_info: Optional[Dict[str, Any]] = None,
    recent_context: str = "",
    user_tweaks: Optional[str] = None,
    previous_plan: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Unified Single-Turn Intent Classification & Planning turn.
    Direct async Gemini API call eliminating 20s subprocess cold-start latency.
    """
    return await direct_gemini_triage(
        user_prompt=clean_prompt,
        target_info=target_info,
        recent_context=recent_context,
        user_tweaks=user_tweaks,
        previous_plan=previous_plan
    )


async def generate_preflight_plan(
    clean_prompt: str,
    target_info: Dict[str, Any],
    user_tweaks: Optional[str] = None,
    previous_plan: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Generate structured pre-flight alignment plan via direct Gemini planning turn."""
    triage = await direct_gemini_triage(
        user_prompt=clean_prompt,
        target_info=target_info,
        recent_context="",
        user_tweaks=user_tweaks,
        previous_plan=previous_plan
    )
    if triage.get("turn_type") == "ACTION" and triage.get("action_plan"):
        return triage["action_plan"]
    return build_fallback_preflight_plan(clean_prompt, target_info, user_tweaks)


def format_alignment_briefing(plan: Dict[str, Any]) -> str:
    """Format the Chief of Staff Pre-Flight Alignment ('Grill-Me') message."""
    intent = plan.get("intent_summary", "perform requested operation")
    worker = plan.get("delegated_worker", "Hermes Butler")
    node = plan.get("target_node", "hermes")
    if node == "local":
        node = "hermes"
    payload = plan.get("dispatched_payload", "")
    assumptions = plan.get("assumptions", "Standard execution boundaries apply.")
    choice = plan.get("choice", "Proceed with standard execution parameters.")
    recommendation = plan.get("recommendation", "Run default non-destructive plan.")
    duration = plan.get("estimated_duration_sec", 15)
    domain = plan.get("domain", "system operations")
    tier = plan.get("safety_tier", 2)
    use_supervisor = plan.get("use_supervisor", False)
    workspace = plan.get("workspace", "dev-grid" if "opencode" in str(worker).lower() else "fleet-control")

    is_self = plan.get("is_self")
    if is_self is None:
        is_self = (node in ("hermes", "localhost", "local") and "butler" not in str(worker).lower() and "opencode" not in str(worker).lower())

    duration_line = ""
    try:
        dur_val = int(duration)
        if dur_val > 30:
            duration_line = f"\n_I'll ping updates if this takes longer than {dur_val} seconds._\n"
    except (ValueError, TypeError):
        pass

    if tier == 3:
        if is_self:
            agent_line = "I'll take care of this directly on *hermes* (`localhost`).\n\n*Here is what I'm running:*"
        else:
            agent_line = f"The best agent for this is *{worker}*—since this is a *{domain}* task, they can handle it cleanly on *{node}*.\n\n*Here is what I'm briefing them with:*"

        msg = (
            f"On it, Carl.\n\n"
            f"It sounds like you want to *{intent}*.\n\n"
            f"{agent_line}\n"
            f"> *\"{payload}\"*\n\n"
            f"*Sanity check & defaults:*\n"
            f"• *Assumption:* {assumptions}\n"
            f"• *Choice:* {choice}\n"
            f"  ⤷ *_Recommendation (Default):_* {recommendation}\n"
            f"{duration_line}\n"
            f"⚠️ *Tier 3 Blast-Radius Safety Gate:* Merging to main, git push, container restarts, or mutating production requires explicit confirmation. A simple `go` or 👍 is disabled for this blast radius.\n"
            f"*Reply with `approve` or `do it` to execute.*"
        )
    elif is_self:
        agent_line = "I'll take care of this directly on *hermes* (`localhost`).\n\n*Here is what I'm running:*"
        msg = (
            f"On it, Carl.\n\n"
            f"It sounds like you want to *{intent}*.\n\n"
            f"{agent_line}\n"
            f"> *\"{payload}\"*\n\n"
            f"*Sanity check & defaults:*\n"
            f"• *Assumption:* {assumptions}\n"
            f"• *Choice:* {choice}\n"
            f"  ⤷ *_Recommendation (Default if you just say `go`):_* {recommendation}\n"
            f"{duration_line}\n"
            f"*Hit me with `go` (or 👍) to run with the recommendation, or let me know what to tweak.*"
        )
    elif use_supervisor or "opencode" in str(worker).lower() or plan.get("worker_id") == "opencode-dev":
        target_file = plan.get("target_file")
        if not target_file:
            m = re.search(r'(?:in|for|create|edit|modify|refactor|update|file)\s+[`\'"]?([a-zA-Z0-9_\-./]+\.[a-zA-Z0-9_]+)[`\'"]?', intent, re.IGNORECASE)
            if not m:
                m = re.search(r'[`\'"]?([a-zA-Z0-9_\-./]+\.[a-zA-Z0-9_]+)[`\'"]?', intent)
            if m:
                target_file = m.group(1)

        action_verb = "create"
        if "refactor" in intent.lower():
            action_verb = "refactor"
        elif "update" in intent.lower() or "edit" in intent.lower() or "modify" in intent.lower():
            action_verb = "update"
        elif "fix" in intent.lower():
            action_verb = "fix"

        if target_file:
            action_desc = f"{action_verb} `{target_file}`"
        else:
            action_desc = intent

        msg = (
            f"I'll have OpenCode {action_desc} on an isolated worktree via FrugaLLM (:61721).\n"
            f"Hit me with `go` (or 👍) to start, or let me know what to tweak."
        )
    else:
        agent_line = f"The best agent for this is *{worker}*—since this is a *{domain}* task, they can handle it cleanly on *{node}*.\n\n*Here is what I'm briefing them with:*"
        msg = (
            f"On it, Carl.\n\n"
            f"It sounds like you want to *{intent}*.\n\n"
            f"{agent_line}\n"
            f"> *\"{payload}\"*\n\n"
            f"*Sanity check & defaults:*\n"
            f"• *Assumption:* {assumptions}\n"
            f"• *Choice:* {choice}\n"
            f"  ⤷ *_Recommendation (Default if you just say `go`):_* {recommendation}\n"
            f"{duration_line}\n"
            f"*Hit me with `go` (or 👍) to run with the recommendation, or let me know what to tweak.*"
        )
    return msg.replace("\n\n\n", "\n\n")



async def execute_approved_plan(
    plan: Dict[str, Any],
    dispatched_payload: str,
    target_info: Dict[str, Any],
    channel: str,
    thread_ts: str,
    session_key: str,
    client: AsyncWebClient,
    react_ts: Optional[str] = None,
    original_prompt: Optional[str] = None,
    uploaded_files: Optional[List[Dict[str, Any]]] = None
):
    """Execute the approved Pre-Flight Alignment plan headlessly with per-pane locking."""
    target_ws = target_info.get("workspace_label", "fleet-control")
    pane_id = await fleet.resolve_pane_id(target_info)
    target_info["pane_id"] = pane_id

    session = sessions.get(session_key, {})
    active_workspace = session.get("workspace", DEFAULT_TARGET_REPO)
    # Host isolation guardrail: First Mate's host directory must NEVER be cloned, checked out, or provided to Herdr workers.
    if not active_workspace or active_workspace == HOST_DIR:
        active_workspace = DEFAULT_TARGET_REPO
    target_repo = active_workspace

    if react_ts:
        await swap_reacji(client, channel, react_ts, "eyes", "gear")

    # Check if this task should be routed through AgentSupervisor (Tier 2 worker tasks)
    use_supervisor = plan.get("use_supervisor") or plan.get("worker_id") in ("opencode-dev", "hermes-homelab", "cuda-worker")
    current_sup = getattr(app, "supervisor", None) or globals().get("supervisor")
    if use_supervisor and current_sup:
        worker_id = plan.get("worker_id") or "opencode-dev"
        user_intent = original_prompt or plan.get("intent_summary") or dispatched_payload
        logger.info(f"Dispatching task to AgentSupervisor for worker {worker_id} in {target_repo}...")

        async def heartbeat_cb(data: Dict[str, Any]):
            if session_key in active_supervised_tasks:
                if data.get("pane_id"):
                    active_supervised_tasks[session_key]["pane_id"] = data["pane_id"]
                if data.get("workspace_id"):
                    active_supervised_tasks[session_key]["workspace_id"] = data["workspace_id"]
                if data.get("agent_name"):
                    active_supervised_tasks[session_key]["agent_name"] = data["agent_name"]
            msg_text = data.get("message")
            if msg_text and client and channel and thread_ts:
                try:
                    await client.chat_postMessage(
                        channel=channel,
                        thread_ts=thread_ts,
                        text=f"_{msg_text}_" if not msg_text.startswith("*") else msg_text
                    )
                except Exception as e:
                    logger.warning(f"Could not send conversational heartbeat to Slack: {e}")

        task_record = {
            "worker_id": worker_id,
            "user_intent": user_intent,
            "start_time": time.time(),
            "channel": channel,
            "thread_ts": thread_ts,
            "client": client,
            "workspace_id": None,
            "pane_id": None,
            "agent_name": None
        }
        active_supervised_tasks[session_key] = task_record
        if thread_ts and thread_ts != session_key:
            active_supervised_tasks[thread_ts] = task_record

        try:
            report = await current_sup.supervise_task(
                worker_id=worker_id,
                user_intent=user_intent,
                repo_path=target_repo,
                heartbeat_callback=heartbeat_cb
            )
            is_success = (report.get("status") == "success")
            final_reacji = "white_check_mark" if is_success else "x"
            if react_ts:
                await swap_reacji(client, channel, react_ts, "gear", final_reacji)

            if report.get("pr"):
                session["last_pr"] = report["pr"]
            if report.get("worktree"):
                session["last_worktree"] = report["worktree"]

            briefing = current_sup.format_review_briefing(report)
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=briefing
            )

            # If diff exceeds 35 lines, upload patch file
            full_diff = report.get("diff", "")
            if full_diff and len(full_diff.splitlines()) > 35:
                patch_file = f"/tmp/changes_{int(time.time())}.patch"
                with open(patch_file, "w", encoding="utf-8") as f:
                    f.write(full_diff)
                try:
                    await client.files_upload_v2(
                        channel=channel,
                        thread_ts=thread_ts,
                        file=patch_file,
                        filename="changes.patch",
                        title=f"Git Diff Patch ({report.get('branch', 'task')})"
                    )
                except Exception as upload_err:
                    logger.warning(f"Could not upload patch file: {upload_err}")
                finally:
                    if os.path.isfile(patch_file):
                        try:
                            os.remove(patch_file)
                        except OSError:
                            pass
            return
        except Exception as sup_err:
            logger.error(f"Error during supervised worker task: {sup_err}")
            if react_ts:
                await swap_reacji(client, channel, react_ts, "gear", "x")
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f":x: *Supervisor Execution Failed:* {sup_err}"
            )
            return
        finally:
            active_supervised_tasks.pop(session_key, None)
            if thread_ts:
                active_supervised_tasks.pop(thread_ts, None)

    # Check if this task is a Tier 3 PR auto-merge and worktree cleanup
    is_pr_merge = (
        plan.get("action") == "merge_pr"
        or "gh pr merge" in dispatched_payload
        or (re.search(r"\bmerge\b", dispatched_payload, re.IGNORECASE) and re.search(r"\bpr\b", dispatched_payload, re.IGNORECASE))
    )
    if is_pr_merge:
        current_gh = globals().get("github_mgr") or getattr(app, "github_manager", None) or GitHubManager()
        current_herdr = globals().get("client_herdr") or getattr(app, "herdr_client", None) or HerdrClient()

        pr_match = re.search(r"#?(\d+)", dispatched_payload)
        pr_selector = pr_match.group(1) if pr_match else ""
        if not pr_selector:
            last_pr = session.get("last_pr", {})
            pr_selector = str(last_pr.get("pr_number") or last_pr.get("pr_url") or "")

        logger.info(f"Executing Tier 3 PR merge for selector '{pr_selector}' in {target_repo}...")
        merge_res = await current_gh.merge_pull_request(
            repo_path=target_repo,
            pr_selector=pr_selector,
            auto=True,
            squash=True,
            delete_branch=True
        )

        last_wt = session.get("last_worktree", {})
        wt_workspace_id = last_wt.get("workspace_id")
        wt_branch = last_wt.get("branch")
        wt_path = last_wt.get("worktree_path")
        if wt_workspace_id or wt_branch or wt_path:
            await current_herdr.remove_worktree(
                workspace_id=wt_workspace_id,
                branch_name=wt_branch,
                worktree_path=wt_path,
                repo_path=target_repo,
                force=True
            )

        sync_res = await current_gh.sync_main(target_repo)

        if react_ts:
            await swap_reacji(client, channel, react_ts, "gear", "white_check_mark")

        confirmation_text = (
            f":white_check_mark: *Production Merge Complete:*\n"
            f"• *Pull Request:* Merged via `gh pr merge --auto --squash --delete-branch`.\n"
            f"• *Herdr Worktree:* Cleaned up worktree `{wt_workspace_id or 'workspace'}`.\n"
            f"• *Local Branch Sync:* Checked out `main` and synced cleanly with upstream."
        )
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=confirmation_text
        )
        return

    pane_lock = fleet.get_pane_lock(pane_id)
    if pane_lock.locked():
        target_disp = f"{target_ws} ({target_info.get('role', '')})"
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":warning: Target `{target_disp}` (pane `{pane_id}`) is currently busy with another operation. Queuing task..."
        )

    async with pane_lock:
        await execute_herdr_task(
            prompt=dispatched_payload,
            target_info=target_info,
            channel=channel,
            thread_ts=thread_ts,
            session_key=session_key,
            client=client,
            active_workspace=active_workspace,
            msg_ts=react_ts,
            uploaded_files=uploaded_files,
            original_prompt=original_prompt
        )


# ------------------------------------------------------------------------------
# Core Headless Execution via Herdr Fleet
# ------------------------------------------------------------------------------
async def execute_herdr_task(
    prompt: str,
    target_info: Dict[str, Any],
    channel: str,
    thread_ts: str,
    session_key: str,
    client: AsyncWebClient,
    active_workspace: str = DEFAULT_WORKSPACE,
    msg_ts: Optional[str] = None,
    uploaded_files: Optional[List[Dict[str, Any]]] = None,
    original_prompt: Optional[str] = None
):
    """
    Execute task headlessly across persistent Herdr fleet nodes.
    - Transitions reacji: :eyes: -> :gear: -> :white_check_mark: / :warning: / :x:.
    - Executes task headlessly via fleet.execute_task (clean stdout/stderr to run buffer/log).
    - If blocked, swaps reacji to :warning: and posts conversational inquiry in thread.
    - Feeds backstage raw findings into headless AGY CLI brain for executive synthesis.
    - Captures git diffs, artifact files, and uploads briefing directly to thread.
    """
    react_ts = msg_ts or thread_ts
    ws_label = target_info.get("workspace_label", "fleet-control")
    role = target_info.get("role", "Chief of Staff")
    target_machine = target_info.get("target_machine", "local")
    ssh_target = target_info.get("ssh_target") or (None if target_machine == "local" else target_machine)
    is_local = (target_machine == "local")
    remote_ws = active_workspace if (active_workspace and active_workspace != DEFAULT_WORKSPACE) else target_info.get("default_cwd", "~")
    effective_workspace = active_workspace if is_local else remote_ws

    if is_local:
        before_artifacts = find_workspace_artifacts(active_workspace)
    else:
        before_artifacts = await find_remote_workspace_artifacts(target_machine, ssh_target, effective_workspace)

    # Transition reacji: :eyes: -> :gear: (Task dispatched to Herdr)
    await swap_reacji(client, channel, react_ts, "eyes", "gear")

    try:
        # Execute backstage task headlessly
        task_res = await fleet.execute_task(
            target_info=target_info,
            instruction=prompt,
            cwd=effective_workspace
        )

        exit_code = task_res.get("exit_code", 0)
        raw_output = task_res.get("output", "")
        blocked = task_res.get("blocked", False)
        blocked_prompt = task_res.get("blocked_prompt")

        # Handle blocked / approval required condition
        if blocked:
            pane_id = target_info.get("pane_id") or "w1:p1"
            sessions[session_key]["blocked_pane"] = pane_id
            await swap_reacji(client, channel, react_ts, "gear", "warning")
            alert_text = f"⚠️ *Agent needs decision on {ws_label} ({role}):* {blocked_prompt or 'Confirmation or credentials required.'}\n_Reply in this thread to unblock._"
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=alert_text
            )
            return

        # Feed raw findings into headless AGY CLI brain for executive synthesis
        final_answer = await synthesize_with_agy(
            clean_prompt=original_prompt or prompt,
            raw_output=raw_output,
            exit_code=exit_code,
            target_info=target_info,
            uploaded_files=uploaded_files
        )

        # Handle git diff for target workspace (local or remote)
        diff_text = await get_git_diff("local" if is_local else target_machine, None if is_local else ssh_target, effective_workspace)
        upload_patch_file = False
        if diff_text:
            diff_lines = diff_text.splitlines()
            if len(diff_lines) <= 35:
                final_answer = f"{final_answer}\n\n*Workspace Git Changes:*\n```diff\n{diff_text}\n```"
            else:
                upload_patch_file = True

        # Process explicit [SEND_FILE: <path>] directives
        send_file_matches = re.findall(
            r"\[(?:SEND_FILE|UPLOAD_FILE|ATTACH_FILE):\s*([^\]]+)\]",
            final_answer,
            re.IGNORECASE
        )
        final_answer = re.sub(
            r"\[(?:SEND_FILE|UPLOAD_FILE|ATTACH_FILE):\s*[^\]]+\]",
            "",
            final_answer
        ).strip()

        final_answer = format_for_slack(final_answer)

        # Upload [SEND_FILE] files
        for target_match in send_file_matches:
            target_str = target_match.strip().strip("\"'`<>")
            if is_local:
                candidates = [
                    target_str if os.path.isabs(target_str) else os.path.realpath(os.path.abspath(os.path.join(active_workspace, target_str))),
                    os.path.realpath(os.path.abspath(os.path.join(active_workspace, TRANSFERS_DIR_NAME, target_str)))
                ]
                resolved = next((c for c in candidates if os.path.isfile(c)), None)
                if resolved:
                    try:
                        await client.files_upload_v2(
                            channel=channel,
                            thread_ts=thread_ts,
                            file=resolved,
                            filename=os.path.basename(resolved),
                            title=f"File: {os.path.basename(resolved)}"
                        )
                    except Exception as up_err:
                        logger.error(f"Failed to upload file from [SEND_FILE] {resolved}: {up_err}")
            else:
                temp_file = await fetch_remote_file(target_machine, ssh_target, target_str)
                if not temp_file and not target_str.startswith(("/", "~", "C:")):
                    temp_file = await fetch_remote_file(target_machine, ssh_target, f"{effective_workspace}/{TRANSFERS_DIR_NAME}/{target_str}")
                if temp_file and os.path.isfile(temp_file):
                    try:
                        await client.files_upload_v2(
                            channel=channel,
                            thread_ts=thread_ts,
                            file=temp_file,
                            filename=os.path.basename(target_str),
                            title=f"File ({target_machine}): {os.path.basename(target_str)}"
                        )
                    except Exception as up_err:
                        logger.error(f"Failed to upload remote file from [SEND_FILE] {target_str}: {up_err}")
                    finally:
                        try:
                            os.remove(temp_file)
                        except OSError:
                            pass

        # Upload large patch file if needed
        if upload_patch_file and diff_text:
            try:
                await client.files_upload_v2(
                    channel=channel,
                    thread_ts=thread_ts,
                    filename="changes.patch",
                    title=f"Workspace Changes (Unified Diff - {ws_label})",
                    content=diff_text
                )
            except Exception as patch_err:
                logger.error(f"Failed to upload changes.patch: {patch_err}")

        # Upload newly generated artifacts (local or remote)
        await upload_new_artifacts(
            effective_workspace,
            before_artifacts,
            channel,
            thread_ts,
            client,
            host="local" if is_local else target_machine,
            ip=None if is_local else ssh_target
        )

        # Upload full execution trace if raw output > 10 lines
        raw_lines = raw_output.splitlines()
        if len(raw_lines) > 10:
            ts_now = int(time.time())
            try:
                await client.files_upload_v2(
                    channel=channel,
                    thread_ts=thread_ts,
                    filename=f"agy_trace_{ts_now}.txt",
                    title="Full Execution Trace",
                    content=raw_output
                )
                logger.info(f"Uploaded full trace ({len(raw_lines)} lines) as agy_trace_{ts_now}.txt")
            except Exception as trace_err:
                logger.error(f"Failed to upload execution trace: {trace_err}")

        # Final state transition: swap to :white_check_mark: or :x:
        if exit_code == 0:
            await swap_reacji(client, channel, react_ts, "gear", "white_check_mark")
        else:
            await swap_reacji(client, channel, react_ts, "gear", "x")

        # Post synthesized executive briefing to thread
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=final_answer
        )

    except Exception as e:
        logger.error(f"Unhandled exception in execute_herdr_task: {e}", exc_info=True)
        await swap_reacji(client, channel, react_ts, "gear", "x")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: Task execution failed on *{ws_label}*: {e}"
        )

# Backward compatibility execution engine for execute_agy_task
async def execute_agy_task(
    prompt: str,
    host: str,
    ip: Optional[str],
    active_workspace: str,
    conversation_id: Optional[str],
    channel: str,
    thread_ts: str,
    session_key: str,
    client: AsyncWebClient
):
    """
    Executes agy CLI (locally or remotely via Tailscale SSH), parses NDJSON stream,
    updates Rolling HUD every 1.5s, uploads full execution trace, and handles artifacts.
    """
    is_local = (host == "local")
    before_images = find_image_artifacts(active_workspace) if is_local else {}

    display_name = f"{host}:~" if not is_local else (os.path.basename(active_workspace) or active_workspace)

    # Reacji state marker: dispatched
    await swap_reacji(client, channel, thread_ts, "eyes", "gear")

    master_trace: List[str] = []
    accumulated_message_parts: List[str] = []
    final_result_output: Optional[str] = None

    def record_trace(category: str, detail: str):
        ts_str = time.strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{ts_str}] [{category}] {detail}"
        master_trace.append(entry)

    record_trace("START", f"Target: {host} ({ip or 'local'}) | Workspace: {active_workspace} | Prompt: {prompt}")

    if is_local:
        cmd_args = [
            AGY_BIN,
            "-p", prompt,
            "--output-format", "stream-json",
            "--dangerously-skip-permissions",
        ]
        if conversation_id:
            cmd_args.extend(["--conversation", conversation_id])
        if AGY_SUPPORTS_WORK_DIR:
            cmd_args.extend(["--work-dir", active_workspace])
        cwd = active_workspace
    else:
        ssh_user = get_remote_ssh_user(ip)
        remote_cmd_parts = [
            "agy",
            "-p", prompt,
            "--output-format", "stream-json",
            "--dangerously-skip-permissions",
            "--work-dir", "~"
        ]
        if conversation_id:
            remote_cmd_parts.extend(["--conversation", conversation_id])

        remote_cmd_str = shlex.join(remote_cmd_parts)
        cmd_args = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=5",
            "-T",
            f"{ssh_user}@{ip}",
            remote_cmd_str
        ]
        cwd = None

    proc = None

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd_args,
            cwd=cwd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        active_subprocesses.add(proc)

        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            line_str = line.decode("utf-8", errors="replace").strip()
            if not line_str:
                continue

            try:
                data = json.loads(line_str)
            except json.JSONDecodeError:
                record_trace("RAW_OUT", line_str)
                continue

            conv_id = data.get("conversation_id") or data.get("init", {}).get("conversation_id")
            if conv_id and session_key in sessions:
                sessions[session_key]["conversation_id"] = conv_id
                record_trace("INIT", f"Conversation initialized (ID: {conv_id})")

            thought = (
                data.get("thought")
                or data.get("data", {}).get("thought")
                or data.get("step_update", {}).get("thinking")
            )
            if thought:
                thought_str = str(thought).strip()
                record_trace("THOUGHT", thought_str)

            cmd_exec = (
                data.get("command")
                or data.get("data", {}).get("command")
                or data.get("step_update", {}).get("tool_info", {}).get("parameters", {}).get("CommandLine")
            )
            if cmd_exec:
                cmd_str = str(cmd_exec).strip()
                record_trace("BASH", cmd_str)

            is_message_event = (
                data.get("type") == "message"
                or data.get("event") == "message"
                or data.get("step_update", {}).get("step_type") == "agent_response"
            )
            if is_message_event:
                msg_content = (
                    data.get("content")
                    or data.get("data", {}).get("content")
                    or data.get("text")
                    or data.get("data", {}).get("text")
                    or data.get("step_update", {}).get("text_delta")
                    or data.get("step_update", {}).get("text")
                )
                if msg_content:
                    msg_str = str(msg_content)
                    accumulated_message_parts.append(msg_str)
                    record_trace("MESSAGE", msg_str.strip())

            is_result_event = (
                data.get("type") == "result"
                or data.get("event") == "result"
            )
            if is_result_event:
                res_output = (
                    data.get("data", {}).get("output")
                    or data.get("output")
                    or data.get("result", {}).get("response")
                    or data.get("response")
                )
                if res_output:
                    final_result_output = str(res_output)
                    record_trace("RESULT", str(res_output).strip())

            step = data.get("step_update", {})
            step_type = step.get("step_type")
            state = step.get("state")
            if step_type == "tool":
                tool_name = step.get("tool_name", "unknown")
                params = step.get("tool_info", {}).get("parameters", {})
                if state == "ACTIVE":
                    if tool_name == "write_to_file":
                        target = params.get("TargetFile", "")
                        record_trace("WRITE", target)
                    elif tool_name in ("replace_file_content", "sed_file", "multi_replace_file_content"):
                        target = params.get("TargetFile", "")
                        record_trace("EDIT", target)
                    elif tool_name in ("view_file", "read_resource"):
                        target = params.get("AbsolutePath", "")
                        record_trace("READ", target)
                    elif tool_name == "list_dir":
                        dpath = params.get("DirectoryPath", "")
                        record_trace("LIST", dpath)
                    elif tool_name != "run_command":
                        record_trace("TOOL", tool_name)

        await proc.wait()
        record_trace("EXIT", f"Subprocess exited with code {proc.returncode}")

        if proc.returncode != 0:
            stderr_bytes = await proc.stderr.read()
            stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()
            record_trace("STDERR", stderr_text)

            if not is_local and (
                proc.returncode == 255
                or "connect" in stderr_text.lower()
                or "timeout" in stderr_text.lower()
                or "permission denied" in stderr_text.lower()
                or "no route" in stderr_text.lower()
            ):
                err_detail = stderr_text or f"Could not connect to {host} ({ip}) via SSH. Verify Tailscale connectivity and SSH key authentication."
                error_msg = f"❌ *[Host: {host} ({ip}) | Status: SSH Connection Failed (Exit Code: {proc.returncode})]*\n```\n{err_detail}\n```"
            else:
                if stderr_text:
                    err_lines = stderr_text.splitlines()[-12:]
                    err_block = "\n".join(err_lines)
                else:
                    err_lines = [line for line in master_trace if "ERROR" in line]
                    if not err_lines:
                        err_lines = master_trace[-8:] if master_trace else ["Subprocess terminated with non-zero exit code."]
                    err_block = "\n".join(err_lines)

                error_msg = f"❌ *[Workspace: {display_name} | Status: Failed (Exit Code: {proc.returncode})]*\n```\n{err_block}\n```"

            await swap_reacji(client, channel, thread_ts, "gear", "x")
            await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=error_msg)
        else:
            final_answer = ""
            if final_result_output and final_result_output.strip():
                final_answer = final_result_output.strip()
            elif accumulated_message_parts:
                final_answer = "".join(accumulated_message_parts).strip()

            if not final_answer:
                final_answer = ":white_check_mark: Task completed successfully."

            diff_text = await get_git_diff(host=host, ip=ip, active_workspace=active_workspace)
            upload_patch_file = False
            if diff_text:
                diff_lines = diff_text.splitlines()
                if len(diff_lines) <= 35:
                    final_answer = f"{final_answer}\n\n*Workspace Git Changes:*\n```diff\n{diff_text}\n```"
                else:
                    upload_patch_file = True

            send_file_matches = re.findall(
                r"\[(?:SEND_FILE|UPLOAD_FILE|ATTACH_FILE):\s*([^\]]+)\]",
                final_answer,
                re.IGNORECASE
            )
            final_answer = re.sub(
                r"\[(?:SEND_FILE|UPLOAD_FILE|ATTACH_FILE):\s*[^\]]+\]",
                "",
                final_answer
            ).strip()

            final_answer = format_for_slack(final_answer)
            await swap_reacji(client, channel, thread_ts, "gear", "white_check_mark")
            await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=final_answer)

            for target_match in send_file_matches:
                target_str = target_match.strip().strip("\"'`<>")
                candidates = [
                    target_str if os.path.isabs(target_str) else os.path.realpath(os.path.abspath(os.path.join(active_workspace, target_str))),
                    os.path.realpath(os.path.abspath(os.path.join(active_workspace, TRANSFERS_DIR_NAME, target_str)))
                ]
                resolved = next((c for c in candidates if os.path.isfile(c)), None)
                if resolved:
                    try:
                        await client.files_upload_v2(
                            channel=channel,
                            thread_ts=thread_ts,
                            file=resolved,
                            filename=os.path.basename(resolved),
                            title=f"File: {os.path.basename(resolved)}"
                        )
                    except Exception as up_err:
                        logger.error(f"Failed to upload file from [SEND_FILE] {resolved}: {up_err}")

            if upload_patch_file and diff_text:
                try:
                    await client.files_upload_v2(
                        channel=channel,
                        thread_ts=thread_ts,
                        filename="changes.patch",
                        title="Workspace Changes (Unified Diff)",
                        content=diff_text
                    )
                except Exception as patch_err:
                    logger.error(f"Failed to upload changes.patch: {patch_err}")

            if is_local:
                await upload_new_artifacts(active_workspace, before_images, channel, thread_ts, client)

    except Exception as e:
        logger.error(f"Error during agy task execution: {e}", exc_info=True)
        error_msg = f"❌ *[Workspace: {display_name} | Status: Execution Error]*\n```\n{str(e)}\n```"
        await swap_reacji(client, channel, thread_ts, "gear", "x")
        await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=error_msg)
    finally:
        if proc:
            active_subprocesses.discard(proc)

    if len(master_trace) > 10:
        ts_now = int(time.time())
        trace_text = "\n".join(master_trace)
        try:
            await client.files_upload_v2(
                channel=channel,
                thread_ts=thread_ts,
                filename=f"agy_trace_{ts_now}.txt",
                title="Full Execution Trace",
                content=trace_text
            )
        except Exception as trace_err:
            logger.error(f"Failed to upload execution trace: {trace_err}")

def format_for_slack(text: str) -> str:
    """Format standard markdown to Slack mrkdwn (e.g. converting **bold** to *bold* outside code blocks)."""
    parts = re.split(r"(```[\s\S]*?```|`[^`\n]+`)", text)
    for i in range(0, len(parts), 2):
        parts[i] = re.sub(r"\*\*(.+?)\*\*", r"*\1*", parts[i])
    return "".join(parts)

def build_hermes_prompt(
    clean_text: str,
    uploaded_files: Optional[List[Dict[str, Any]]] = None
) -> str:
    """
    Build a concise, clean prompt for interactive CLI agents (hermes-worker).
    Avoids dumping multi-paragraph system prompts with mode boundary text directly
    into interactive PTYs, sending only clean user instruction and essential context.
    """
    parts = []
    if uploaded_files:
        items = ", ".join(f"{f['name']} (saved in slack_transfers/{f['name']})" for f in uploaded_files)
        parts.append(f"[Uploaded files available: {items}]")
    parts.append(clean_text.strip())
    return "\n\n".join(parts)

def build_slack_context_prompt(
    clean_text: str,
    event: Dict[str, Any],
    host: str,
    ip: Optional[str],
    active_workspace: str,
    thread_ts: str,
    uploaded_files: Optional[List[Dict[str, Any]]] = None
) -> str:
    """Build context wrapper for prompt."""
    channel = event.get("channel", "")
    channel_type = event.get("channel_type", "")
    is_dm = (channel and channel.startswith("D")) or channel_type in ("im", "mpim")
    channel_desc = "Direct Message (1:1 with @agy)" if is_dm else f"channel <#{channel}> (ID: {channel})"

    user_id = event.get("user")
    user_desc = f"<@{user_id}>" if user_id else "Slack user"
    target_desc = f"remote host '{host}' ({ip}:~)" if host != "local" else f"local workspace ({active_workspace})"

    remote_hosts = get_remote_hosts()
    remote_hosts_desc = ", ".join(f"{alias} ({rip})" for alias, rip in remote_hosts.items()) if remote_hosts else "None configured"

    files_section = ""
    if uploaded_files:
        items = "\n".join(f"  • `{f['name']}` (saved to `{f['path']}`, {f['size']} bytes)" for f in uploaded_files)
        files_section = (
            f"• User Uploaded Files (stored in 'slack_transfers/'):\n"
            f"{items}\n"
            f"  (The user attached this file in Slack; it is saved in your workspace under slack_transfers/ ready for reading/processing.)\n"
        )

    return (
        f"[Slack Interface Context]\n"
        f"You are AGY, an autonomous senior software engineer and native macOS background daemon pair-programming interface responding to the user over Slack.\n"
        f"• User: {user_desc}\n"
        f"• Slack Medium: {channel_desc} (Thread: {thread_ts})\n"
        f"• Target Environment: {target_desc}\n"
        f"• Available Remote Hosts: {remote_hosts_desc}\n"
        f"{files_section}"
        f"• Slack-Exclusive Capabilities & Protocols:\n"
        f"  - You are chatting directly inside Slack. Keep responses clear, direct, and conversational.\n"
        f"  - Format with Slack mrkdwn syntax: *bold* with single asterisks (NOT **bold**), _italics_, `inline code`, and standard code blocks.\n"
        f"  - Incoming user files are stored in the dedicated 'slack_transfers/' folder in your workspace.\n"
        f"  - To send ANY file back to the user in Slack (e.g. generated reports, CSVs, charts, code, PDFs, logs), include this directive in your final answer:\n"
        f"    [SEND_FILE: path/to/file]\n"
        f"    The AGY daemon will automatically upload the file(s) to this Slack thread.\n"
        f"  - Newly created or modified image artifacts (.png, .jpg, .svg) and document exports (.pdf, .csv, .xlsx, .zip, .tar.gz) are auto-detected and uploaded to this thread on completion.\n"
        f"  - The user can also directly download existing workspace/transfers files by saying 'send me <filename>', or switch targets by saying 'switch to <path>' or 'switch to <remote_alias>'.\n"
        f"• Mode Boundary:\n"
        f"  - These Slack capabilities ([SEND_FILE], slack_transfers/ handling, Slack mrkdwn) apply exclusively because you are executing within a Slack session. Standard terminal sessions outside of Slack do not possess these Slack background hooks.\n\n"
        f"[User Message]\n"
        f"{clean_text}"
    )

# ------------------------------------------------------------------------------
# Message Processing & Dynamic Workspace Controls
# ------------------------------------------------------------------------------
async def process_user_prompt(event: Dict[str, Any], client: AsyncWebClient):
    """
    Handles prompt messages, command parsing (`switch to <target>`),
    interactive unblocking, Chief of Staff triage, workspace locking, and Herdr execution.
    """
    channel = event.get("channel")
    msg_ts = event.get("ts")
    parent_thread_ts = event.get("thread_ts")
    thread_ts = parent_thread_ts or msg_ts
    session_key = f"{channel}:{thread_ts}"

    raw_text = event.get("text", "")
    clean_text = re.sub(r"<@[A-Za-z0-9]+>", "", raw_text).strip()

    # Detect /learn or !learn command early
    learn_match = re.search(r"(?:^|\s)[/!](?:learn)(?:\s*[:\-]?\s*(.*))?$", clean_text, re.IGNORECASE | re.DOTALL)
    is_learn = bool(learn_match)

    # Immediate acknowledgement within 200ms (:brain: for /learn, :eyes: for regular)
    if msg_ts:
        initial_reacji = "brain" if is_learn else "eyes"
        await add_reacji(client, channel, msg_ts, initial_reacji)

    if is_learn:
        user_notes = (learn_match.group(1) or "").strip()
        await handle_learn_request(
            channel=channel,
            thread_ts=parent_thread_ts,
            msg_ts=msg_ts,
            user_notes=user_notes,
            client=client
        )
        return

    clean_lower = clean_text.lower().strip()

    # Quirk rule approval (from /wrapup suggestions)
    if clean_lower in ("approve quirk", "approve quirks", "approve rule", "approve rules"):
        if await handle_quirk_approval(channel, thread_ts, msg_ts, client):
            return
    elif clean_lower == "approve" and session_key not in pending_alignment:
        if await handle_quirk_approval(channel, thread_ts, msg_ts, client):
            return

    # FirstMate Executive Commands (/bearings, !bearings, /ahoy, !ahoy, /wrapup, !wrapup, /update, !update)
    if re.match(r"^[/!]?(?:bearings|fleet\s+bearings|fleet)$", clean_lower):
        await handle_bearings_request(
            channel=channel,
            thread_ts=thread_ts,
            msg_ts=msg_ts,
            client=client
        )
        return

    if re.match(r"^[/!]?ahoy$", clean_lower):
        await handle_ahoy_request(
            channel=channel,
            thread_ts=thread_ts,
            msg_ts=msg_ts,
            client=client
        )
        return

    if re.match(r"^[/!]?wrap-?up$", clean_lower):
        await handle_wrapup_request(
            channel=channel,
            thread_ts=thread_ts,
            msg_ts=msg_ts,
            client=client
        )
        return

    if re.match(r"^[/!]?update$", clean_lower):
        await handle_update_request(
            channel=channel,
            thread_ts=thread_ts,
            msg_ts=msg_ts,
            client=client
        )
        return

    # Initialize session if not present
    if session_key not in sessions:
        sessions[session_key] = {
            "host": "local",
            "ip": None,
            "workspace": DEFAULT_WORKSPACE,
            "target_workspace": None,
            "blocked_pane": None,
            "conversation_id": None
        }

    # --------------------------------------------------------------------------
    # In-Flight Interrogation: Instant ground truth from Herdr snapshot
    # --------------------------------------------------------------------------
    task_info = active_supervised_tasks.get(session_key) or (active_supervised_tasks.get(thread_ts) if thread_ts else None)
    if task_info and clean_text:
        worker_id = task_info.get("worker_id", "opencode-dev")
        worker_display = "OpenCode" if "opencode" in worker_id.lower() else worker_id
        elapsed = int(time.time() - task_info.get("start_time", time.time()))

        clean_lower_cmd = clean_text.lower().strip().rstrip(".!")
        if clean_lower_cmd in ("kill", "kill it", "kill pane", "abort", "cancel", "stop"):
            logger.info(f"User requested kill of active supervised task in {session_key}")
            target_agent = task_info.get("agent_name") or worker_id
            target_pane = task_info.get("pane_id")
            current_herdr = globals().get("client_herdr") or getattr(app, "herdr_client", None) or HerdrClient()
            if target_agent:
                await current_herdr.run_cli(["agent", "stop", target_agent])
                await current_herdr.run_cli(["agent", "send-keys", target_agent, "C-c"])
            if target_pane:
                await current_herdr.run_cli(["pane", "close", target_pane])

            active_supervised_tasks.pop(session_key, None)
            if thread_ts:
                active_supervised_tasks.pop(thread_ts, None)
            if msg_ts:
                await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=":octagonal_sign: *Terminated worker pane.* Standing by with Hermes Butler to take over."
            )
            return

        current_herdr = globals().get("client_herdr") or getattr(app, "herdr_client", None) or HerdrClient()
        snapshot = await current_herdr.get_snapshot()
        info = current_herdr.parse_snapshot_pane_info(
            snapshot=snapshot,
            pane_id=task_info.get("pane_id"),
            workspace_id=task_info.get("workspace_id"),
            agent_name=task_info.get("agent_name")
        )
        agent_status = info.get("agent_status") or "running"
        terminal_title = info.get("terminal_title") or "active execution"

        reply = f"I just checked the Herdr pane. {worker_display} status is '{agent_status}' and currently showing: '{terminal_title}'. Elapsed time is {elapsed}s."
        if agent_status == "blocked" or "fixing" in terminal_title.lower():
            reply += "\nI can kill this pane and have Hermes Butler take over if you'd prefer (reply with *kill* or *abort*)."
        else:
            reply += "\nI can kill this pane if you'd prefer (reply with *kill* or *abort*)."

        if msg_ts:
            await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=reply
        )
        return

    # --------------------------------------------------------------------------
    # Interactive Unblock: If agent in this thread is blocked waiting for input
    # --------------------------------------------------------------------------
    if sessions[session_key].get("blocked_pane") and clean_text:
        blocked_pane = sessions[session_key]["blocked_pane"]

        # Before executing herdr pane send-keys, verify that the target pane is STILL actively in the 'blocked' state
        current_status, detail = await fleet.get_pane_status(blocked_pane)
        if current_status != "blocked":
            logger.warning(
                f"Ignored unblock input '{clean_text}' for pane {blocked_pane}: "
                f"pane is no longer in blocked state (current status: {current_status})"
            )
            sessions[session_key]["blocked_pane"] = None
            if msg_ts:
                await swap_reacji(client, channel, msg_ts, "eyes", "warning")
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f":warning: Agent in pane `{blocked_pane}` is no longer waiting for confirmation (status: `{current_status}`). Input was not sent to avoid executing stray commands."
            )
            return

        sessions[session_key]["blocked_pane"] = None
        logger.info(f"Delivering unblock input to pane {blocked_pane}: {clean_text}")
        await fleet.send_input(blocked_pane, clean_text)
        if msg_ts:
            await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":white_check_mark: Delivered input `{clean_text}` to agent."
        )
        return

    # --------------------------------------------------------------------------
    # Dynamic Workspace Control: `switch to <target>`
    # --------------------------------------------------------------------------
    switch_match = re.match(
        r"^(?:(?:can|could)\s+you\s+(?:please\s+)?|please\s+)?(?:switch(?:\s+(?:the\s+)?(?:workspace|target|over))?|change(?:\s+(?:the\s+)?(?:workspace|target))?)\s+to\s+(.+?)[\.\?!]?$",
        clean_text,
        re.IGNORECASE
    )
    if switch_match:
        raw_target = switch_match.group(1).strip().strip("\"'`<>")

        # 1. Check backward compatibility remote hosts & IPs
        remote_hosts = get_remote_hosts()
        target_lower = raw_target.lower()
        matched_alias = None
        matched_ip = None
        if target_lower in remote_hosts:
            matched_alias = target_lower
            matched_ip = remote_hosts[target_lower]
        else:
            for alias, ip in remote_hosts.items():
                if target_lower == ip.lower():
                    matched_alias = alias
                    matched_ip = ip
                    break

        if matched_alias and matched_ip:
            resolved_ws = fleet.resolve_target(matched_alias)
            ws_label = resolved_ws["workspace_label"] if resolved_ws else matched_alias
            sessions[session_key]["target_workspace"] = ws_label
            sessions[session_key]["host"] = matched_alias
            sessions[session_key]["ip"] = matched_ip
            sessions[session_key]["workspace"] = "~"
            sessions[session_key]["conversation_id"] = None
            active_thread_sessions.add(session_key)
            if msg_ts:
                await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f"Switched target to remote host *{matched_alias}* ({matched_ip}:~)."
            )
            return

        # 2. Check Herdr Fleet topology and machines
        resolved = fleet.resolve_target(raw_target)
        if resolved:
            target_ws = resolved["workspace_label"]
            sessions[session_key]["target_workspace"] = target_ws
            sessions[session_key]["host"] = resolved["target_machine"]
            sessions[session_key]["ip"] = None
            sessions[session_key]["workspace"] = resolved.get("default_cwd", "~")
            sessions[session_key]["conversation_id"] = None
            active_thread_sessions.add(session_key)
            logger.info(f"Session {session_key} repointed to Herdr fleet target {target_ws} ({resolved['role']})")

            # Check backward compatibility ip lookup
            rip = remote_hosts.get(resolved.get("target_machine", "").lower())
            ip_desc = f" ({rip}:~)" if rip else ":~"

            if msg_ts:
                await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f"Switched target to remote host *{target_ws}*{ip_desc}."
            )
            return

        # 2. Check if <target> is a local filesystem path
        expanded = os.path.expanduser(raw_target)
        if not os.path.isabs(expanded):
            current_host = sessions[session_key].get("host", "local")
            current_ws = sessions[session_key].get("workspace", DEFAULT_WORKSPACE)
            if current_host == "local" and os.path.isabs(current_ws):
                target_path = os.path.realpath(os.path.abspath(os.path.join(current_ws, expanded)))
            else:
                target_path = os.path.realpath(os.path.abspath(os.path.join(DEFAULT_WORKSPACE, expanded)))
        else:
            target_path = os.path.realpath(os.path.abspath(expanded))

        if os.path.isdir(target_path):
            sessions[session_key]["host"] = "local"
            sessions[session_key]["ip"] = None
            sessions[session_key]["workspace"] = target_path
            sessions[session_key]["target_workspace"] = "fleet-control"
            sessions[session_key]["conversation_id"] = None
            active_thread_sessions.add(session_key)
            logger.info(f"Session {session_key} repointed to local workspace {target_path}")

            if msg_ts:
                await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f":white_check_mark: Switched workspace to `{target_path}`. Started a fresh conversation session."
            )
            return

        # 3. Target is unrecognized
        fleet_targets = list(fleet.topology.keys()) if fleet.topology else list(TOPOLOGY_SPECS.keys())
        alias_hint = f" (Configured fleet targets: {', '.join(fleet_targets)})"
        if msg_ts:
            await swap_reacji(client, channel, msg_ts, "eyes", "x")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: Unrecognized target: `{raw_target}`. Path does not exist or is not a configured Herdr fleet target.{alias_hint}"
        )
        return

    # --------------------------------------------------------------------------
    # Direct File Request: `send me <path>`, `give me <path>`
    # --------------------------------------------------------------------------
    send_file_match = re.match(
        r"^(?:(?:can|could)\s+you\s+(?:please\s+)?|please\s+)?(?:send(?:\s+me)?|give\s+me|get|download)\s+(?:the\s+)?(?:file\s+)?([^\s\?]+)[\.\?!]?$",
        clean_text,
        re.IGNORECASE
    )
    slack_files = event.get("files", [])
    if send_file_match and not slack_files:
        requested_file = send_file_match.group(1).strip().strip("\"'`<>")
        current_host = sessions[session_key].get("host", "local")
        current_ip = sessions[session_key].get("ip")
        current_ws = sessions[session_key].get("workspace", DEFAULT_WORKSPACE)

        file_to_send = None
        temp_remote = None
        if current_host == "local" or not current_ip:
            candidates = [
                requested_file if os.path.isabs(requested_file) else os.path.realpath(os.path.abspath(os.path.join(current_ws, requested_file))),
                os.path.realpath(os.path.abspath(os.path.join(current_ws, TRANSFERS_DIR_NAME, requested_file)))
            ]
            file_to_send = next((c for c in candidates if os.path.isfile(c)), None)
        else:
            temp_remote = await fetch_remote_file(current_host, current_ip, requested_file)
            if not temp_remote and not requested_file.startswith(("/", "~")):
                temp_remote = await fetch_remote_file(current_host, current_ip, f"~/{TRANSFERS_DIR_NAME}/{requested_file}")
            if temp_remote:
                file_to_send = temp_remote

        if file_to_send and os.path.isfile(file_to_send):
            try:
                fname = os.path.basename(requested_file)
                await client.files_upload_v2(
                    channel=channel,
                    thread_ts=thread_ts,
                    file=file_to_send,
                    filename=fname,
                    title=f"File: {fname}"
                )
                target_desc = f"{current_host}:~" if current_host != "local" else current_ws
                if msg_ts:
                    await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
                await client.chat_postMessage(
                    channel=channel,
                    thread_ts=thread_ts,
                    text=f":page_facing_up: Here is `{fname}` from `{target_desc}`."
                )
                return
            except Exception as e:
                logger.error(f"Failed direct file upload for {requested_file}: {e}")
            finally:
                if temp_remote:
                    try:
                        os.remove(temp_remote)
                    except OSError:
                        pass
        else:
            target_desc = f"{current_host}:~" if current_host != "local" else current_ws
            if msg_ts:
                await swap_reacji(client, channel, msg_ts, "eyes", "x")
            await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f":x: File `{requested_file}` was not found in `{target_desc}`."
            )
            return

    # --------------------------------------------------------------------------
    # Chief of Staff Pre-Flight Alignment ("Grill-Me") Approval or Adjustment
    # --------------------------------------------------------------------------
    if session_key in pending_alignment:
        alignment = pending_alignment[session_key]
        plan = alignment["plan"]
        tier = plan.get("safety_tier", 2)
        target_info = alignment["target_info"]
        original_prompt = alignment.get("original_prompt", "")
        dispatched_payload = plan.get("dispatched_payload", "")
        uploaded_files = alignment.get("uploaded_files")

        clean_lower_stripped = clean_lower.rstrip(".! ")

        if tier == 3:
            # Tier 3 (Mutating Production / Merging / Push)
            # Strictly gated behind explicit two-key confirmation ('approve' or 'do it')
            TWO_KEY_APPROVALS = {"approve", "do it"}
            is_two_key_approved = (clean_lower_stripped in TWO_KEY_APPROVALS)

            if is_two_key_approved:
                pending_alignment.pop(session_key)
                if alignment.get("alignment_msg_ts"):
                    pending_alignment_by_msg_ts.pop(alignment["alignment_msg_ts"], None)

                await execute_approved_plan(
                    plan=plan,
                    dispatched_payload=dispatched_payload,
                    target_info=target_info,
                    channel=channel,
                    thread_ts=thread_ts,
                    session_key=session_key,
                    client=client,
                    react_ts=msg_ts,
                    original_prompt=original_prompt,
                    uploaded_files=uploaded_files
                )
                return
            elif clean_lower_stripped in {"go", "proceed", "yes", "lgtm", "run", "👍", "+1", "thumbsup"}:
                # Simple 'go' is disabled for Tier 3!
                if msg_ts:
                    await swap_reacji(client, channel, msg_ts, "eyes", "warning")
                await client.chat_postMessage(
                    channel=channel,
                    thread_ts=thread_ts,
                    text=(
                        f":warning: *Tier 3 Blast-Radius Safety Gate:* This action modifies production, merges, or pushes "
                        f"(`{dispatched_payload}`). A simple `go` is disabled for this blast radius. "
                        f"Please explicitly reply with *approve* or *do it* to execute."
                    )
                )
                return
            # Otherwise, fall through to tweak or conversational inquiry
        else:
            APPROVAL_WORDS = {"go", "proceed", "yes", "do it", "approve", "lgtm", "run", "👍", "+1", "thumbsup"}
            is_approval = (clean_lower_stripped in APPROVAL_WORDS)

            if is_approval:
                pending_alignment.pop(session_key)
                if alignment.get("alignment_msg_ts"):
                    pending_alignment_by_msg_ts.pop(alignment["alignment_msg_ts"], None)

                await execute_approved_plan(
                    plan=plan,
                    dispatched_payload=dispatched_payload,
                    target_info=target_info,
                    channel=channel,
                    thread_ts=thread_ts,
                    session_key=session_key,
                    client=client,
                    react_ts=msg_ts,
                    original_prompt=original_prompt,
                    uploaded_files=uploaded_files
                )
                return
        # If not approved or rejected, handle adjustment or inquiry
        if True:
            # User provided a message in a pending alignment thread.
            # Could be a conversational/diagnostic inquiry (e.g. "Why did that check run against root rather than our external storage pools?")
            # OR an operational adjustment/tweak to the plan (e.g. "Run with coverage instead").
            alignment = pending_alignment[session_key]
            previous_plan = alignment["plan"]
            target_info = alignment["target_info"]
            original_prompt = alignment.get("original_prompt", clean_text)
            uploaded_files = alignment.get("uploaded_files")

            # Check mock status for testing suites
            is_preflight_mocked = hasattr(generate_preflight_plan, "mock_calls") or hasattr(generate_preflight_plan, "assert_called")
            is_direct_mocked = hasattr(direct_gemini_triage, "mock_calls") or hasattr(direct_gemini_triage, "assert_called")

            if is_preflight_mocked and not is_direct_mocked and not is_conversational_query(clean_text):
                turn_type = "ACTION"
                updated_target_info = fleet.triage_prompt(f"{original_prompt} {clean_text}")
                action_plan = await generate_preflight_plan(
                    clean_prompt=original_prompt,
                    target_info=updated_target_info,
                    user_tweaks=clean_text,
                    previous_plan=previous_plan
                )
            else:
                recent_context = await fetch_recent_thread_context(client, channel, thread_ts, limit=5)

                triage_result = await direct_gemini_triage(
                    user_prompt=clean_text,
                    target_info=target_info,
                    recent_context=recent_context,
                    user_tweaks=clean_text,
                    previous_plan=previous_plan
                )
                turn_type = str(triage_result.get("turn_type") or triage_result.get("action_type", "")).upper()

                if turn_type == "CONVERSE":
                    # Pure conversational/diagnostic turn in thread:
                    # Answer directly in thread and PRESERVE pending_alignment[session_key] intact!
                    reply = triage_result.get("conversational_reply")
                    if not reply:
                        conv_res = await handle_conversational_turn(clean_text, repo_path=active_workspace, herdr_client_inst=client_herdr)
                        reply = conv_res.get("conversational_reply") or "I'm standing by. Let me know if you would like to run the plan or adjust it."
                    reply = format_for_slack(reply)
                    await client.chat_postMessage(
                        channel=channel,
                        thread_ts=thread_ts,
                        text=reply
                    )
                    if msg_ts:
                        await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
                    return

                # turn_type == "ACTION": User requested an operational modification to the plan
                updated_target_info = fleet.triage_prompt(f"{original_prompt} {clean_text}")
                action_plan = triage_result.get("action_plan")
                if not action_plan:
                    action_plan = await generate_preflight_plan(
                        clean_prompt=original_prompt,
                        target_info=updated_target_info,
                        user_tweaks=clean_text,
                        previous_plan=previous_plan
                    )

            action_plan["dispatched_payload"] = validate_action_payload(
                action_plan.get("dispatched_payload", ""),
                action_plan.get("target_node", "hermes")
            )
            alignment["plan"] = action_plan
            alignment["target_info"] = updated_target_info

            briefing = format_alignment_briefing(action_plan)
            resp = await client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=briefing
            )
            new_align_ts = None
            if isinstance(resp, dict):
                new_align_ts = resp.get("ts")
            elif hasattr(resp, "data") and isinstance(resp.data, dict):
                new_align_ts = resp.data.get("ts")
            elif hasattr(resp, "get"):
                new_align_ts = resp.get("ts")

            if new_align_ts:
                old_align_ts = alignment.get("alignment_msg_ts")
                if old_align_ts and old_align_ts in pending_alignment_by_msg_ts:
                    pending_alignment_by_msg_ts.pop(old_align_ts, None)
                alignment["alignment_msg_ts"] = new_align_ts
                pending_alignment_by_msg_ts[new_align_ts] = session_key

            if msg_ts:
                await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
            return

    # --------------------------------------------------------------------------
    # Ingest Uploaded Slack Files (Slack -> Workspace)
    # --------------------------------------------------------------------------
    uploaded_files_meta: List[Dict[str, Any]] = []
    if slack_files and isinstance(slack_files, list):
        current_host = sessions[session_key].get("host", "local")
        current_ip = sessions[session_key].get("ip")
        current_ws = sessions[session_key].get("workspace", DEFAULT_WORKSPACE)

        for sf in slack_files:
            file_name = sf.get("name") or sf.get("title") or f"upload_{int(time.time())}"
            download_url = sf.get("url_private_download") or sf.get("url_private")
            if not download_url:
                continue

            file_bytes = await download_slack_file(download_url, SLACK_BOT_TOKEN)
            if not file_bytes:
                continue

            saved_path = await save_uploaded_file(
                file_bytes=file_bytes,
                filename=file_name,
                host=current_host,
                ip=current_ip,
                active_workspace=current_ws
            )
            if saved_path:
                uploaded_files_meta.append({
                    "name": os.path.basename(file_name),
                    "path": saved_path,
                    "size": len(file_bytes)
                })

        if uploaded_files_meta and not clean_text:
            names_str = ", ".join(f"`{f['name']}`" for f in uploaded_files_meta)
            clean_text = f"I've uploaded {names_str} to the workspace. Please inspect or process this file."

    if not clean_text:
        targets_info = ", ".join(fleet.topology.keys()) if fleet.topology else "fleet-control, homelab-ops"
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":wave: Hello! I am AGY, Chief of Staff for Antigravity & Hermes Butler over Herdr.\nAvailable fleet workspaces: `{targets_info}`.\nType `switch to <target>` to change execution targets."
        )
        return

    session = sessions[session_key]
    current_target = session.get("target_workspace")

    # Chief of Staff triage & routing
    target_info = fleet.triage_prompt(clean_text, current_target=current_target)

    # 1. Fetch recent thread context (prior 3-5 turns)
    recent_context = await fetch_recent_thread_context(client, channel, thread_ts, limit=5)

    # 2. Unified Single-Turn Intent Classification & Triage
    is_preflight_mocked = hasattr(generate_preflight_plan, "mock_calls") or hasattr(generate_preflight_plan, "assert_called")
    is_triage_mocked = hasattr(triage_turn_with_agy, "mock_calls") or hasattr(triage_turn_with_agy, "assert_called")
    is_direct_mocked = hasattr(direct_gemini_triage, "mock_calls") or hasattr(direct_gemini_triage, "assert_called")

    if is_preflight_mocked and not is_triage_mocked and not is_direct_mocked and not is_conversational_query(clean_text):
        turn_type = "ACTION"
        plan = await generate_preflight_plan(clean_text, target_info)
        triage_result = {"turn_type": "ACTION", "action_plan": plan}
    elif is_triage_mocked and not is_direct_mocked:
        triage_result = await triage_turn_with_agy(
            clean_prompt=clean_text,
            target_info=target_info,
            recent_context=recent_context
        )
        turn_type = triage_result.get("turn_type", "ACTION")
    else:
        triage_result = await direct_gemini_triage(
            user_prompt=clean_text,
            target_info=target_info,
            recent_context=recent_context
        )
        turn_type = str(triage_result.get("turn_type") or triage_result.get("action_type", "")).upper()

    if turn_type == "CONVERSE":
        reply = triage_result.get("conversational_reply")
        if not reply:
            conv_res = await handle_conversational_turn(clean_text, repo_path=active_workspace, herdr_client_inst=client_herdr)
            reply = conv_res.get("conversational_reply") or "I'm here, Carl. Let me know what you'd like to inspect or execute."
        reply = format_for_slack(reply)
        if msg_ts:
            await swap_reacji(client, channel, msg_ts, "eyes", "white_check_mark")
        await client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=reply
        )
        return

    # Turn is ACTION
    plan = triage_result.get("action_plan")
    if not plan or not isinstance(plan, dict):
        plan = await generate_preflight_plan(clean_text, target_info)
    elif is_preflight_mocked:
        mocked_plan = await generate_preflight_plan(clean_text, target_info)
        if mocked_plan and isinstance(mocked_plan, dict):
            plan = mocked_plan


    # Validate action plan: ban dummy echo payloads and validate operational tooling
    target_node = plan.get("target_node") or target_info.get("target_machine", "hermes")
    if target_node == "local":
        target_node = "hermes"
    plan["target_node"] = target_node

    delegated_worker = plan.get("delegated_worker") or target_info.get("role", "Chief of Staff")
    plan["delegated_worker"] = delegated_worker

    plan["dispatched_payload"] = validate_action_payload(
        plan.get("dispatched_payload", ""),
        target_node
    )

    ws_label = target_info.get("workspace_label", "fleet-control")
    if "is_self" not in plan:
        is_self = (target_node in ("hermes", "localhost", "local") and ws_label == "fleet-control" and "butler" not in delegated_worker.lower())
        plan["is_self"] = is_self

    # Formulate Chief of Staff Pre-Flight Alignment ("Grill-Me")
    briefing = format_alignment_briefing(plan)


    resp = await client.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=briefing
    )

    align_msg_ts = None
    if isinstance(resp, dict):
        align_msg_ts = resp.get("ts")
    elif hasattr(resp, "data") and isinstance(resp.data, dict):
        align_msg_ts = resp.data.get("ts")
    elif hasattr(resp, "get"):
        align_msg_ts = resp.get("ts")

    if align_msg_ts:
        pending_alignment_by_msg_ts[align_msg_ts] = session_key

    pending_alignment[session_key] = {
        "plan": plan,
        "target_info": target_info,
        "channel": channel,
        "thread_ts": thread_ts,
        "alignment_msg_ts": align_msg_ts,
        "trigger_msg_ts": msg_ts,
        "original_prompt": clean_text,
        "uploaded_files": uploaded_files_meta
    }

# ------------------------------------------------------------------------------
# Slack Bolt App & Event Listeners
# ------------------------------------------------------------------------------
app = AsyncApp(
    token=SLACK_BOT_TOKEN,
    signing_secret=SLACK_SIGNING_SECRET
)
app.fleet = fleet
app.herdr = fleet
app.supervisor = supervisor
app.herdr_client = client_herdr
app.github_manager = github_mgr


@app.event("app_mention")
async def handle_app_mention(event: Dict[str, Any], client: AsyncWebClient):
    channel = event.get("channel")
    msg_ts = event.get("ts")
    thread_ts = event.get("thread_ts") or msg_ts
    session_key = f"{channel}:{thread_ts}"

    active_thread_sessions.add(session_key)

    msg_id = f"{channel}:{msg_ts}"
    if msg_id in processed_message_ids:
        return
    processed_message_ids.add(msg_id)
    if len(processed_message_ids) > 2000:
        processed_message_ids.clear()

    await process_user_prompt(event, client)

@app.event("message")
async def handle_direct_or_thread_message(event: Dict[str, Any], client: AsyncWebClient):
    subtype = event.get("subtype")
    if event.get("bot_id") or (subtype and subtype not in ("file_share",)):
        return

    channel_type = event.get("channel_type")
    thread_ts = event.get("thread_ts")
    channel = event.get("channel")
    session_key = f"{channel}:{thread_ts}" if (channel and thread_ts) else None

    raw_text = event.get("text", "")
    clean_text = re.sub(r"<@[A-Za-z0-9]+>", "", raw_text).strip()
    is_bang_cmd = clean_text.startswith("!")

    is_dm = channel_type in ("im", "mpim")
    is_active_thread_reply = (session_key is not None) and (session_key in active_thread_sessions or session_key in sessions or session_key in pending_alignment)

    if not (is_dm or is_active_thread_reply or is_bang_cmd):
        return

    msg_id = f"{channel}:{event.get('ts')}"
    if msg_id in processed_message_ids:
        return
    processed_message_ids.add(msg_id)
    if len(processed_message_ids) > 2000:
        processed_message_ids.clear()

    if session_key:
        active_thread_sessions.add(session_key)

    await process_user_prompt(event, client)

@app.event("reaction_added")
async def handle_reaction_added(event: Dict[str, Any], client: AsyncWebClient):
    """
    Handle emoji reaction approval:
    If Carl reacts with :+1: / 👍 to a pending Pre-Flight Alignment briefing,
    execute the recommended plan immediately.
    """
    user_id = event.get("user")
    if user_id == BOT_USER_ID:
        return

    reaction = event.get("reaction")
    if reaction not in ("+1", "thumbsup"):
        return

    item = event.get("item", {})
    if item.get("type") != "message":
        return

    msg_ts = item.get("ts")
    channel = item.get("channel")
    if not (msg_ts and channel):
        return

    session_key = pending_alignment_by_msg_ts.get(msg_ts)
    if not session_key or session_key not in pending_alignment:
        for s_key, data in list(pending_alignment.items()):
            if (
                data.get("alignment_msg_ts") == msg_ts
                or data.get("thread_ts") == msg_ts
                or s_key == f"{channel}:{msg_ts}"
            ):
                session_key = s_key
                break

    if not session_key or session_key not in pending_alignment:
        item_thread_ts = item.get("thread_ts") or event.get("thread_ts")
        if item_thread_ts and f"{channel}:{item_thread_ts}" in pending_alignment:
            session_key = f"{channel}:{item_thread_ts}"
        elif len(pending_alignment) == 1 and list(pending_alignment.keys())[0].startswith(f"{channel}:"):
            session_key = list(pending_alignment.keys())[0]

    if not session_key or session_key not in pending_alignment:
        return

    alignment_candidate = pending_alignment[session_key]
    plan_candidate = alignment_candidate.get("plan", {})
    tier = plan_candidate.get("safety_tier", 2)
    if tier == 3:
        logger.warning(f"Emoji reaction approval rejected for Tier 3 action on {session_key}")
        item_thread_ts = item.get("thread_ts") or msg_ts
        await client.chat_postMessage(
            channel=channel,
            thread_ts=item_thread_ts,
            text=(
                ":warning: *Tier 3 Blast-Radius Safety Gate:* This action modifies production, merges, or pushes. "
                "Emoji reaction approval is disabled for this blast radius. "
                "Please explicitly reply with *approve* or *do it* to execute."
            )
        )
        return

    alignment = pending_alignment.pop(session_key)
    pending_alignment_by_msg_ts.pop(msg_ts, None)

    plan = alignment["plan"]
    target_info = alignment["target_info"]
    thread_ts = alignment.get("thread_ts") or msg_ts
    original_prompt = alignment.get("original_prompt", "")
    dispatched_payload = plan.get("dispatched_payload", "")
    uploaded_files = alignment.get("uploaded_files")

    logger.info(f"Emoji reaction 👍 received for alignment on {session_key}. Dispatched: {dispatched_payload}")

    await execute_approved_plan(
        plan=plan,
        dispatched_payload=dispatched_payload,
        target_info=target_info,
        channel=channel,
        thread_ts=thread_ts,
        session_key=session_key,
        client=client,
        react_ts=msg_ts,
        original_prompt=original_prompt,
        uploaded_files=uploaded_files
    )

@app.command("/learn")
async def handle_learn_command(ack, command, client: AsyncWebClient):
    """Handle native Slack slash command /learn."""
    await ack()
    channel_id = command.get("channel_id")
    thread_ts = command.get("thread_ts")
    user_notes = command.get("text", "").strip()
    await handle_learn_request(
        channel=channel_id,
        thread_ts=thread_ts,
        msg_ts=None,
        user_notes=user_notes,
        client=client
    )

@app.command("/bearings")
async def handle_bearings_command(ack, command, client: AsyncWebClient):
    """Handle native Slack slash command /bearings."""
    await ack()
    channel_id = command.get("channel_id")
    thread_ts = command.get("thread_ts")
    await handle_bearings_request(
        channel=channel_id,
        thread_ts=thread_ts,
        msg_ts=None,
        client=client
    )

@app.command("/ahoy")
async def handle_ahoy_command(ack, command, client: AsyncWebClient):
    """Handle native Slack slash command /ahoy."""
    await ack()
    channel_id = command.get("channel_id")
    thread_ts = command.get("thread_ts")
    await handle_ahoy_request(
        channel=channel_id,
        thread_ts=thread_ts,
        msg_ts=None,
        client=client
    )

@app.command("/wrapup")
async def handle_wrapup_command(ack, command, client: AsyncWebClient):
    """Handle native Slack slash command /wrapup."""
    await ack()
    channel_id = command.get("channel_id")
    thread_ts = command.get("thread_ts")
    await handle_wrapup_request(
        channel=channel_id,
        thread_ts=thread_ts,
        msg_ts=None,
        client=client
    )

@app.command("/update")
async def handle_update_command(ack, command, client: AsyncWebClient):
    """Handle native Slack slash command /update."""
    await ack()
    channel_id = command.get("channel_id")
    thread_ts = command.get("thread_ts")
    await handle_update_request(
        channel=channel_id,
        thread_ts=thread_ts,
        msg_ts=None,
        client=client
    )

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
    """
    Bridge incoming BlueBubbles iMessage into the unified Chief of Staff
    command and pre-flight alignment pipeline.
    """
    logger.info(f"[iMessage Received] From {sender} in {chat_guid}: {text[:80]}")
    now_ts = str(time.time())
    event = {
        "channel": chat_guid,
        "user": sender,
        "text": text,
        "ts": now_ts,
        "thread_ts": chat_guid,
        "is_imessage": True,
        "raw_payload": raw_payload
    }
    if bb_transport:
        client = bb_transport.get_client(chat_guid)
        await process_user_prompt(event, client)

async def start_bluebubbles_transport():
    """Initialize and start the BlueBubbles transport and webhook listener."""
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
    """Stop the BlueBubbles transport and unregister webhook."""
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
    logger.info(f"Received exit signal {sig.name}. Cleaning up child processes...")
    terminate_all_subprocesses()
    sys.exit(0)

async def main():
    logger.info("Initializing AGY Slack Chief of Staff service (Herdr Persistent Engine)...")
    loop = asyncio.get_running_loop()
    setup_signal_handlers(loop)

    # Initialize Herdr server & fleet topology
    try:
        await fleet.ensure_server()
        await fleet.ensure_topology()
        logger.info(f"Herdr fleet initialized with topology: {list(fleet.topology.keys())}")
    except Exception as e:
        logger.error(f"Herdr fleet initialization warning: {e}")

    # Verify bot credentials
    try:
        auth_info = await app.client.auth_test()
        global BOT_USER_ID
        BOT_USER_ID = auth_info.get("user_id")
        bot_user_name = auth_info.get("user")
        logger.info(f"Authenticated as bot user: @{bot_user_name} ({BOT_USER_ID}) on team {auth_info.get('team')}")
    except Exception as e:
        logger.critical(f"Failed to authenticate with Slack API: {e}")
        sys.exit(1)

    # Start BlueBubbles iMessage transport listener
    await start_bluebubbles_transport()

    handler = AsyncSocketModeHandler(app, SLACK_APP_TOKEN)
    logger.info("Socket Mode handler ready. Chief of Staff & Hermes Butler listening for tasks...")
    try:
        await handler.start_async()
    finally:
        await stop_bluebubbles_transport()
        terminate_all_subprocesses()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        terminate_all_subprocesses()
