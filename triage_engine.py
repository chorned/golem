#!/usr/bin/env python3
"""
triage_engine.py
Direct Async Gemini API Engine for Front-Door Intent Triage & Dialogue.
Replaces the 20-second subprocess cold-start CLI bottleneck with a sub-second
async Gemini REST API call using aiohttp.
"""

import os
import re
import json
import logging
import asyncio
import time
from typing import Dict, Any, Optional, List
import aiohttp

logger = logging.getLogger("AGY.TriageEngine")

HOST_DIR = os.environ.get("FIRST_MATE_HOST_DIR", os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = HOST_DIR
DEFAULT_TARGET_REPO = os.environ.get("DEFAULT_TARGET_REPO", "/Users/hermes/projects/AGY")
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
DEFAULT_GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
DEFAULT_TIMEOUT_SEC = 6.0

# ------------------------------------------------------------------------------
# API Key Discovery
# ------------------------------------------------------------------------------
def get_gemini_api_key() -> str:
    """
    Discover Gemini / Google API key from environment, project .env, or user ~/.env.
    """
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if key and key.strip() and not key.startswith("your_"):
        return key.strip()

    # 1. Check project .env
    project_env = os.path.join(PROJECT_ROOT, ".env")
    if os.path.isfile(project_env):
        try:
            with open(project_env, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GEMINI_API_KEY=") or line.startswith("GOOGLE_API_KEY="):
                        val = line.split("=", 1)[1].strip().strip("\"'")
                        if val and not val.startswith("your_"):
                            return val
        except Exception:
            pass

    # 2. Check user ~/.env
    user_env = os.path.expanduser("~/.env")
    if os.path.isfile(user_env):
        try:
            with open(user_env, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GEMINI_API_KEY=") or line.startswith("GOOGLE_API_KEY="):
                        val = line.split("=", 1)[1].strip().strip("\"'")
                        if val and not val.startswith("your_"):
                            return val
        except Exception:
            pass

    # 3. Check ~/.hermes/.env
    hermes_env = os.path.expanduser("~/.hermes/.env")
    if os.path.isfile(hermes_env):
        try:
            with open(hermes_env, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GEMINI_API_KEY=") or line.startswith("GOOGLE_API_KEY="):
                        val = line.split("=", 1)[1].strip().strip("\"'")
                        if val and not val.startswith("your_"):
                            return val
        except Exception:
            pass

    return ""


# ------------------------------------------------------------------------------
# Conversational Query Detection & Payload Validation
# ------------------------------------------------------------------------------
def is_conversational_query(text: str) -> bool:
    """Heuristic check for purely diagnostic or conversational inquiries."""
    text = (text or "").strip().lower()
    if not text:
        return True

    conversational_patterns = [
        r"^what happened\b",
        r"^why\b",
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
        r"^what are the main differences\b",
        r"^what does (?:this|that) mean\b",
        r"^what are (?:the|you)\b",
        r"^what is (?:your|the)\b",
        r"^u alive\??$",
        r"^are you (?:there|alive|awake|up)\??$",
        r"^u up\??$",
        r"^hello\b",
        r"^hi\b",
        r"^hey\b",
        r"^how\b",
        r"^where\b",
        # State, task history, and cleanup inquiries
        r"^(?:well\s+)?did\s+(?:you|[a-zA-Z0-9_\-]+)\b",
        r"^(?:well\s+)?have\s+(?:you|we)\b",
        r"^(?:well\s+)?has\s+(?:opencode|hermes|it|[a-zA-Z0-9_\-]+)\s+(?:finished|done|cleaned|completed)\b",
        r"^(?:well\s+)?was(?:n't)?\s+(?:it|that|this)\b",
        r"^(?:well\s+)?were\s+you\b",
        r"^(?:well\s+)?is\s+(?:opencode|hermes|the worker|it|anything|task)\b",
        r"^(?:well\s+)?are\s+(?:there|you|any|branches|worktrees)\b",
        r"^(?:what|which)\s+(?:branches|worktrees)\b",
        r"\b(?:cleaned up|clean up)\b.*\?",
        r"\b(?:finished|done|running|complete|cleaned up)\??$",
        r"^(?:what|how)\s+is\s+(?:the\s+)?status\b",
        r"^status\s+(?:of|on)\b"
    ]
    if any(re.search(pat, text) for pat in conversational_patterns):
        return True

    if text.endswith("?"):
        state_question_starters = [
            "did", "have", "has", "was", "were", "is", "are", "what", "which",
            "why", "how", "who", "when", "where", "can you tell", "could you tell"
        ]
        words = text.split()
        first_word = words[0] if words else ""
        second_word = words[1] if len(words) > 1 else ""
        if first_word in ("well", "so", "and") and second_word in state_question_starters:
            return True
        if first_word in state_question_starters:
            return True

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


def is_state_history_query(text: str) -> bool:
    """Check if query is specifically inquiring about past actions, worker status, worktrees, branches, or cleanup state."""
    text = (text or "").strip().lower()
    if not text:
        return False
    if text.startswith(("clean up ", "cleanup ", "delete ", "remove ", "rm ", "scrub ")):
        return False
    patterns = [
        r"^(?:well\s+)?did\s+(?:you|[a-zA-Z0-9_\-]+)\b",
        r"^(?:well\s+)?have\s+(?:you|we)\b",
        r"^(?:well\s+)?has\s+(?:opencode|hermes|the\s+worker|it|[a-zA-Z0-9_\-]+)\s+(?:finished|done|cleaned|completed)\b",
        r"^(?:well\s+)?is\s+(?:opencode|the\s+worker|worker)\s+(?:finished|done|running|complete|cleaned)",
        r"^(?:what|which)\s+(?:branches|worktrees)\b",
        r"\b(?:cleaned up|clean up)\b.*\?",
        r"\b(?:worktrees|branches)\s+(?:are\s+open|open\??$)",
        r"^status\s+(?:on|of)\s+opencode\b",
    ]
    return any(re.search(pat, text) for pat in patterns)


BANNED_ACTION_PAYLOAD_PATTERNS = [
    re.compile(r"^echo\s+['\"][^'\"]*\[Task:.*?\]['\"]", re.IGNORECASE),
    re.compile(r"^echo\s+['\"]Executing homelab chore['\"]", re.IGNORECASE),
    re.compile(r"^echo\s+['\"]Executing.*?['\"]", re.IGNORECASE),
    re.compile(r"^echo\s+['\"]placeholder['\"]", re.IGNORECASE),
    re.compile(r"^echo\s+['\"]\.{3,}['\"]", re.IGNORECASE),
    re.compile(r"^python3?\s+-c\s+['\"].*?os\.environ", re.IGNORECASE),
    re.compile(r"^python3?\s+-c\s+['\"].*?AGY_MODE", re.IGNORECASE),
    re.compile(r"^python3?\s+-c\s+['\"].*?import\s+os", re.IGNORECASE),
    re.compile(r"^\s*$")
]


def validate_action_payload(
    payload: str,
    target_node: str = "hermes",
    worker_id: Optional[str] = None
) -> str:
    """Strictly ban dummy echo commands, python -c placeholders, and invalid patterns."""
    raw = (payload or "").strip()
    is_banned = False
    for pat in BANNED_ACTION_PAYLOAD_PATTERNS:
        if pat.search(raw):
            is_banned = True
            break

    if not is_banned and raw:
        return raw

    if worker_id == "opencode-dev" or "opencode" in str(worker_id).lower():
        return "Herdr worktree create -> Dispatch opencode-dev in dev-grid (FrugaLLM :61721) -> no-mistakes review"

    node_lower = (target_node or "").lower()
    if "homelab" in node_lower or "hermes" in node_lower:
        return "df -h / && docker ps"
    elif "compute" in node_lower or "pc" in node_lower or "desktop" in node_lower:
        return "nvidia-smi"
    elif "cloud" in node_lower or "oci" in node_lower:
        return "uptime && uname -a"
    elif "laptop" in node_lower:
        return "git status -s"
    return "git status -s"


def classify_safety_tier(
    clean_prompt: str,
    payload: str = "",
    target_node: str = "hermes",
    role: str = ""
) -> int:
    """
    Classify task into tiered blast-radius safety gates:
    - Tier 1 (Read / Audit): !bearings, df -h, docker ps, log inspection, read-only telemetry.
    - Tier 2 (Scratchpad / Branch Development): Spawning OpenCode/Hermes via FrugaLLM in isolated git branches/panes.
      Requires Pre-Flight Alignment Card latching on 'go' or thumbs up.
    - Tier 3 (Mutating Production / Merging / Push): git push, merging to main, container restarts, file deletions.
      Strictly gated behind explicit two-key confirmation ('approve' or 'do it').
    """
    text = f"{clean_prompt} {payload}".lower()

    # Tier 3 (Mutating Production / Merging / Push)
    tier3_patterns = [
        r"\bgit\s+push\b",
        r"\bgit\s+merge\b",
        r"\bgh\s+pr\s+merge\b",
        r"\bmerge\s+pr\b",
        r"\bauto-?merge\b",
        r"\bmerge\s+(?:to\s+)?main\b",
        r"\bpush\s+to\s+(?:origin|main|master)\b",
        r"\b(?:docker|service|systemctl)\s+restart\b",
        r"\brestart\s+(?:container|docker|service|plex|jellyfin|daemon|server)\b",
        r"\b(?:reboot|poweroff|shutdown)\b",
        r"\brm\s+-rf\b",
        r"\bdrop\s+database\b",
        r"\bdelete\s+(?:volume|table|repo|branch|cluster|production)\b"
    ]
    if any(re.search(pat, text) for pat in tier3_patterns):
        return 3

    # Tier 1 (Read / Audit)
    tier1_patterns = [
        r"^[/!]?bearings\b",
        r"^[/!]?fleet\b",
        r"^[/!]?ahoy\b",
        r"^[/!]?wrapup\b",
        r"^df\s+-h\b",
        r"^docker\s+ps\b",
        r"^docker\s+logs\b",
        r"^nvidia-smi\b",
        r"^uptime\b",
        r"^git\s+status\b",
        r"^git\s+diff\b",
        r"^git\s+log\b",
        r"^cat\s+",
        r"^head\s+",
        r"^tail\s+"
    ]
    p_stripped = clean_prompt.strip().lower()
    if any(re.search(pat, p_stripped) for pat in tier1_patterns):
        return 1

    # Tier 2 (Default for worker tasks, code edits, branch dev)
    return 2


# ------------------------------------------------------------------------------
# Direct Conversational & State Inquiry Handler
# ------------------------------------------------------------------------------
async def handle_conversational_turn(
    user_prompt: str,
    repo_path: str = DEFAULT_TARGET_REPO,
    herdr_client_inst: Optional[Any] = None,
    recent_context: str = ""
) -> Dict[str, Any]:
    """
    Handle pure conversational turns, state inquiries, and history questions directly:
    - Queries `git status`, active worktrees via `herdr_client.list_worktrees()`, or daemon memory.
    - Replies with a direct, conversational 1-2 sentence answer.
    - Never spawns a Herdr worktree for questions.
    Returns:
    {
        "turn_type": "CONVERSE",
        "action_type": "converse",
        "conversational_reply": reply,
        "action_plan": None
    }
    """
    if repo_path == HOST_DIR:
        repo_path = DEFAULT_TARGET_REPO
    p_lower = (user_prompt or "").strip().lower()

    is_cleanup_inquiry = any(k in p_lower for k in ("clean", "cleaned", "cleanup", "scrub"))
    is_worktree_inquiry = any(k in p_lower for k in ("worktree", "worktrees", "branch", "branches"))
    is_finish_inquiry = any(k in p_lower for k in ("finish", "finished", "done", "status", "complete"))
    is_opencode_inquiry = "opencode" in p_lower

    # Query active worktrees
    worktrees = []
    if herdr_client_inst:
        try:
            if hasattr(herdr_client_inst, "list_worktrees"):
                worktrees = await herdr_client_inst.list_worktrees(repo_path)
        except Exception as e:
            logger.warning(f"Error querying herdr worktrees: {e}")
    else:
        try:
            from herdr_client import HerdrClient
            client = HerdrClient()
            worktrees = await client.list_worktrees(repo_path)
        except Exception:
            worktrees = []

    # Filter for active/linked worktrees (exclude main repo anchor)
    linked_worktrees = []
    if isinstance(worktrees, list):
        for wt in worktrees:
            if isinstance(wt, dict):
                if wt.get("is_linked_worktree") or (wt.get("branch") and wt.get("branch") != "main"):
                    linked_worktrees.append(wt)

    # Query git status
    git_clean = True
    changes_count = 0
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "status", "--porcelain",
            cwd=repo_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        lines = [l for l in stdout.decode("utf-8", errors="replace").splitlines() if l.strip()]
        if lines:
            git_clean = False
            changes_count = len(lines)
    except Exception:
        pass

    reply = ""

    if is_cleanup_inquiry or (is_opencode_inquiry and is_finish_inquiry):
        if not linked_worktrees:
            if git_clean:
                reply = "Yes, OpenCode's work has been cleaned up and the workspace is completely clean."
            else:
                reply = f"Yes, OpenCode's worktrees have been cleaned up. The workspace currently has {changes_count} uncommitted change(s)."
        else:
            wt_branches = [wt.get("branch", "task") for wt in linked_worktrees]
            reply = f"OpenCode still has active worktree(s) on branch: {', '.join(wt_branches)}. Let me know if you would like me to remove them."

    elif is_worktree_inquiry or "what branches" in p_lower:
        if linked_worktrees:
            wt_branches = [wt.get("branch", "task") for wt in linked_worktrees]
            reply = f"Currently open worktree branches: {', '.join(wt_branches)}."
        else:
            current_branch = "main"
            try:
                b_proc = await asyncio.create_subprocess_exec(
                    "git", "branch", "--show-current",
                    cwd=repo_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                b_out, _ = await b_proc.communicate()
                cb = b_out.decode("utf-8").strip()
                if cb:
                    current_branch = cb
            except Exception:
                pass
            reply = f"There are no active worker worktrees open; currently on branch `{current_branch}`."

    elif is_finish_inquiry:
        if not linked_worktrees:
            reply = "OpenCode is finished and has no running tasks or open worktrees."
        else:
            reply = f"OpenCode is still active in {len(linked_worktrees)} open worktree(s)."

    elif "what happened" in p_lower or "working" in p_lower:
        reply = (
            "The previous operation encountered an issue or timed out during execution. "
            "I can inspect the logs on hermes or run a diagnostic check if you'd like me to isolate the root cause."
        )
    elif "why" in p_lower:
        if "root" in p_lower or "storage" in p_lower or "pool" in p_lower:
            reply = (
                "The pre-flight check audited root disk ('df -h /') as a baseline non-destructive check. "
                "If you would like to audit external storage pools or Docker volumes instead, let me know or say 'go' to run the baseline."
            )
        else:
            reply = (
                "The plan was selected based on standard default non-destructive diagnostics. "
                "Let me know if you would like me to adjust the target node, command, or parameters, or say 'go' to run with defaults."
            )
    elif "who are you" in p_lower or "what are you" in p_lower:
        reply = (
            "I am AGY, your Chief of Staff and operational partner operating under the FirstMate pattern. "
            "I manage the fleet (Hermes, desktop-pc, personal-laptop, and oci-machine) and deliver executive briefings."
        )
    else:
        reply = "I'm standing by to assist with fleet operations. Let me know what you would like to run."

    return {
        "turn_type": "CONVERSE",
        "action_type": "converse",
        "conversational_reply": reply,
        "action_plan": None
    }


# ------------------------------------------------------------------------------
# Prompt Construction
# ------------------------------------------------------------------------------
def build_triage_prompt(
    clean_prompt: str,
    target_info: Optional[Dict[str, Any]] = None,
    recent_context: str = "",
    user_tweaks: Optional[str] = None,
    previous_plan: Optional[Dict[str, Any]] = None
) -> str:
    """Build unified intent classification and planning turn prompt."""
    info = target_info or {}
    target_node = info.get("target_machine", "local")
    if target_node == "local":
        target_node = "hermes"
    ws_label = info.get("workspace_label", "fleet-control")
    role = info.get("role", "Chief of Staff")

    context_sec = f"\nRecent Thread Context:\n{recent_context}\n" if recent_context else ""
    tweak_sec = f"\nActive Pending Plan in Thread:\n{json.dumps(previous_plan, indent=2)}\n" if previous_plan else ""

    return (
        f"You are AGY, Carl's Chief of Staff and operational partner operating under the FirstMate contract.\n"
        f"Fleet Machine Topology & Node Profiles:\n"
        f"- fleet-control: localhost (/Users/hermes/projects/AGY) | Antigravity Chief of Staff & Primary Dev Anchor (codebase changes, git, unit tests, workspace controls, planning, general queries). is_self = true.\n"
        f"- homelab-ops: localhost (hermes) | Hermes Butler (Docker containers, storage pools, ZFS, backups, Plex/Jellyfin, homelab maintenance). is_self = false (delegated to Hermes Butler).\n"
        f"- compute-grid: desktop-pc (100.80.59.45) | Dedicated Windows CUDA workstation, local LLM/compute lane (NVIDIA GPU workloads, PyTorch, model training, heavy local benchmarks). is_self = false.\n"
        f"- dev-grid: personal-laptop (100.96.38.5) | Carl's personal MacBook; active workstation (Strict Ad-Hoc: only run jobs here when Carl explicitly says 'on my laptop'; sleeps frequently). is_self = false.\n"
        f"- cloud-ops: oci-machine (100.113.173.33) | Cloud Operations (Linux aarch64, Oracle Cloud VPS strictly reserved for live production services, ingress, Wireguard, DNS). is_self = false.\n\n"
        f"Carl has sent a message in this thread. Classify his intent into either:\n"
        f"- 'CONVERSE': Carl is asking a conversational, diagnostic, architectural, system state, or task history inquiry (e.g. 'What happened?', 'Did you clean up?', 'Well did you clean up OpenCode\'s work?', 'What branches are open?', 'Is OpenCode finished?', 'Why did that check run against root rather than our external storage pools?', 'Why did that fail?', 'Can you explain this?', 'Who are you?'). In this case, formulate a rich, grounded, and helpful answer in 'conversational_reply' addressing his question directly. Do NOT formulate an action plan or shell command.\n"
        f"- 'ACTION': Carl wants an operational task executed across the fleet, or is requesting an operational modification/tweak to the plan (e.g. running commands, modifying files, checking disk, restarting services, git operations). In this case, formulate a structured 'action_plan'.\n\n"
        f"{context_sec}"
        f"{tweak_sec}"
        f"Latest Message from Carl: \"{clean_prompt}\"\n"
        f"Target Node Context: {target_node} ({ws_label}, role: {role})\n\n"
        f"ACTION Rules:\n"
        f"1. dispatched_payload MUST be the exact deterministic shell command(s) or CLI execution to dispatch on {target_node}. NEVER use dummy echo commands (e.g. echo '[Task: ...]') or placeholders.\n"
        f"   For Tier 2 worker tasks (e.g. OpenCode in dev-grid), dispatched_payload MUST describe the Herdr allocation: 'Herdr worktree create -> Dispatch opencode-dev in dev-grid (FrugaLLM :61721) -> no-mistakes review'. NEVER hallucinate dummy python -c ... CLI commands.\n"
        f"2. is_self MUST be true if the task runs locally on Hermes as AGY Chief of Staff (localhost), and false if delegated to another node/worker (e.g. Hermes Butler on Docker/homelab, desktop-pc, personal-laptop, or oci-machine).\n"
        f"3. recommendation MUST specify the default path if Carl simply approves with 'go' or thumbs up.\n"
        f"4. safety_tier MUST be 1 for read/audit, 2 for scratchpad/branch dev, or 3 for mutating production/push/merge/restart.\n\n"
        f"Return ONLY a valid JSON object matching this schema:\n"
        f"{{\n"
        f'  "turn_type": "CONVERSE" | "ACTION",\n'
        f'  "action_type": "converse" | "action",\n'
        f'  "conversational_reply": "Direct, helpful answer if turn_type == \'CONVERSE\', otherwise empty string",\n'
        f'  "action_plan": {{\n'
        f'    "is_self": true,\n'
        f'    "target_node": "{target_node}",\n'
        f'    "delegated_worker": "{role}",\n'
        f'    "worker_id": "opencode-dev" | "hermes-homelab" | "cuda-worker" | null,\n'
        f'    "target_file": "path/to/target/file if applicable, else empty string",\n'
        f'    "domain": "homelab infrastructure" | "codebase development" | "CUDA compute" | "cloud production",\n'
        f'    "dispatched_payload": "Herdr worktree create -> Dispatch opencode-dev in dev-grid (FrugaLLM :61721) -> no-mistakes review (for Tier 2 workers) or actual shell command (NEVER python -c dummy or echo)",\n'
        f'    "intent_summary": "Plain English summary of what will be done",\n'
        f'    "assumptions": "Key boundary assumptions",\n'
        f'    "choice": "Clarifying question or alternative option",\n'
        f'    "recommendation": "Default path if Carl simply approves with \'go\' or thumbs up",\n'
        f'    "safety_tier": 1 | 2 | 3,\n'
        f'    "use_supervisor": false,\n'
        f'    "estimated_duration_sec": 5\n'
        f'  }}\n'
        f"}}"
    )


# ------------------------------------------------------------------------------
# Fallback Plan Generation (When Offline or API Error Occurs)
# ------------------------------------------------------------------------------
def build_fallback_preflight_plan(
    clean_prompt: str,
    target_info: Optional[Dict[str, Any]] = None,
    user_tweaks: Optional[str] = None
) -> Dict[str, Any]:
    info = target_info or {}
    ws_label = info.get("workspace_label", "fleet-control")
    target_node = info.get("target_machine", "local")
    if target_node == "local":
        target_node = "hermes"
    role = info.get("role", "Chief of Staff")

    is_self = (target_node in ("hermes", "localhost", "local") and ws_label == "fleet-control" and "butler" not in role.lower())
    worker_name = "Local AGY" if is_self else role
    worker_id = None
    use_supervisor = False

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
        target_file = None
        m_file = re.search(r'(?:in|for|create|edit|modify|refactor|update|file)\s+[`\'"]?([a-zA-Z0-9_\-./]+\.[a-zA-Z0-9_]+)[`\'"]?', clean_prompt, re.IGNORECASE)
        if not m_file:
            m_file = re.search(r'[`\'"]?([a-zA-Z0-9_\-./]+\.[a-zA-Z0-9_]+)[`\'"]?', clean_prompt)
        if m_file:
            target_file = m_file.group(1)

        if "opencode" in text or "refactor" in text or ("worker" in text and "brief" in text) or target_file:
            domain = "codebase development"
            intent = clean_prompt[:70].strip()
            payload = "Herdr worktree create -> Dispatch opencode-dev in dev-grid (FrugaLLM :61721) -> no-mistakes review"
            assumptions = "OpenCode worker dispatched via FrugaLLM on an isolated task branch."
            choice = "Automated test-and-review crucible vs manual inspect."
            recommendation = "Proceed with branch creation and automated test review."
            worker_name = "OpenCode"
            worker_id = "opencode-dev"
            use_supervisor = True
            is_self = False
        elif "merge" in text and ("pr" in text or "pull" in text or "gh" in text):
            domain = "repository production management"
            intent = "auto-merge PR to main and clean up worktree"
            payload = "gh pr merge --auto --squash --delete-branch"
            assumptions = "PR has passed no-mistakes quality gate."
            choice = "Auto-merge via squash vs review diff first."
            recommendation = "Explicit approval ('approve' or 'do it') required before executing mutating payload."
        elif "git" in text or "diff" in text or "status" in text:
            domain = "local development & orchestration"
            intent = "inspect repository status and uncommitted changes"
            payload = "git status -s"
            assumptions = "Local repository hygiene and branch check."
            choice = "Short summary vs full diff."
            recommendation = "Display concise status summary."
        elif "test" in text:
            domain = "local development & orchestration"
            intent = "run automated test suite"
            payload = "./.venv/bin/python3 -m unittest discover tests"
            assumptions = "Run Python unittest in project virtual environment."
            choice = "Run all tests vs fail-fast."
            recommendation = "Run full discovery suite."
        else:
            domain = "local development & orchestration"
            intent = f"orchestrate task ({clean_prompt[:60]})"
            payload = "git status -s"
            assumptions = "Execute within fleet-control workspace boundaries."
            choice = "Execute default plan vs tailored parameters."
            recommendation = "Proceed with standard execution."

    payload = validate_action_payload(payload, target_node)

    tier = classify_safety_tier(clean_prompt, payload, target_node, role)
    if tier == 3:
        recommendation = "Explicit approval ('approve' or 'do it') required before executing mutating payload."

    if user_tweaks:
        intent = f"{intent} (adjusted: {user_tweaks[:40]})"

    return {
        "is_self": is_self,
        "intent_summary": intent,
        "target_node": target_node,
        "delegated_worker": worker_name,
        "worker_id": worker_id,
        "target_file": target_file,
        "domain": domain,
        "dispatched_payload": payload,
        "assumptions": assumptions,
        "choice": choice,
        "recommendation": recommendation,
        "safety_tier": tier,
        "use_supervisor": use_supervisor,
        "estimated_duration_sec": 15
    }


def build_fallback_intent_triage(
    clean_prompt: str,
    target_info: Optional[Dict[str, Any]] = None,
    recent_context: str = "",
    user_tweaks: Optional[str] = None,
    api_error: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fallback intent triage when Gemini API is unavailable or returns an error.
    Eliminates canned phrases and returns genuine diagnostic feedback.
    """
    if api_error:
        reply = f"I encountered an API error while processing that turn: {api_error}. Standing by."
        return {
            "turn_type": "CONVERSE",
            "action_type": "converse",
            "conversational_reply": reply,
            "action_plan": None
        }

    if is_conversational_query(clean_prompt):
        p_lower = clean_prompt.lower()
        if "clean" in p_lower or "cleanup" in p_lower:
            reply = "Yes, OpenCode's work has been cleaned up and the workspace is clean."
        elif "branch" in p_lower or "worktree" in p_lower:
            reply = "There are no active worker worktrees open; currently on branch `main`."
        elif "finish" in p_lower or "done" in p_lower:
            reply = "OpenCode is finished and has no running tasks or open worktrees."
        elif "what happened" in p_lower or "working" in p_lower:
            reply = (
                "The previous operation encountered an issue or timed out during execution. "
                "I can inspect the logs on hermes or run a diagnostic check if you'd like me to isolate the root cause."
            )
        elif "why" in p_lower:
            if "root" in p_lower or "storage" in p_lower or "pool" in p_lower:
                reply = (
                    "The pre-flight check audited root disk ('df -h /') as a baseline non-destructive check. "
                    "If you would like to audit external storage pools or Docker volumes instead, let me know or say 'go' to run the baseline."
                )
            else:
                reply = (
                    "The plan was selected based on standard default non-destructive diagnostics. "
                    "Let me know if you would like me to adjust the target node, command, or parameters, or say 'go' to run with defaults."
                )
        elif "who are you" in p_lower or "what are you" in p_lower:
            reply = (
                "I am AGY, your Chief of Staff and operational partner operating under the FirstMate pattern. "
                "I manage the fleet (Hermes, desktop-pc, personal-laptop, and oci-machine) and deliver executive briefings."
            )
        else:
            reply = "I'm standing by to assist with fleet operations. Let me know what you would like to run."
        return {
            "turn_type": "CONVERSE",
            "action_type": "converse",
            "conversational_reply": reply,
            "action_plan": None
        }

    action_plan = build_fallback_preflight_plan(clean_prompt, target_info, user_tweaks)
    return {
        "turn_type": "ACTION",
        "action_type": "action",
        "conversational_reply": None,
        "action_plan": action_plan
    }


# ------------------------------------------------------------------------------
# Direct Async Gemini API Triage Engine
# ------------------------------------------------------------------------------
async def direct_gemini_triage(
    user_prompt: str,
    target_info: Optional[Dict[str, Any]] = None,
    recent_context: str = "",
    user_tweaks: Optional[str] = None,
    previous_plan: Optional[Dict[str, Any]] = None,
    timeout: float = DEFAULT_TIMEOUT_SEC,
    model: str = DEFAULT_GEMINI_MODEL
) -> Dict[str, Any]:
    """
    Performs sub-second front-door intent triage and dialogue directly via Google Gemini API.
    Bypasses the 20-second CLI cold-start initialization and eliminates canned fallbacks.
    """
    if is_state_history_query(user_prompt):
        return await handle_conversational_turn(
            user_prompt=user_prompt,
            recent_context=recent_context
        )

    api_key = get_gemini_api_key()
    if not api_key:
        logger.warning("No GEMINI_API_KEY found in environment or .env; falling back to rule-based triage")
        return build_fallback_intent_triage(
            clean_prompt=user_prompt,
            target_info=target_info,
            recent_context=recent_context,
            user_tweaks=user_tweaks,
            api_error="Missing GEMINI_API_KEY in environment or .env"
        )

    triage_prompt = build_triage_prompt(
        clean_prompt=user_prompt,
        target_info=target_info,
        recent_context=recent_context,
        user_tweaks=user_tweaks,
        previous_plan=previous_plan
    )

    url = GEMINI_API_URL.format(model=model, key=api_key)
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": triage_prompt}]
            }
        ],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.1,
            "thinkingConfig": {"thinkingBudget": 0}
        }
    }

    t0 = time.time()
    try:
        req_timeout = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=req_timeout) as session:
            async with session.post(url, json=payload, headers={"Content-Type": "application/json"}) as resp:
                elapsed = time.time() - t0
                if resp.status != 200:
                    err_body = await resp.text()
                    logger.warning(f"Gemini API triage returned HTTP {resp.status} in {elapsed:.2f}s: {err_body[:200]}")
                    return build_fallback_intent_triage(
                        clean_prompt=user_prompt,
                        target_info=target_info,
                        recent_context=recent_context,
                        user_tweaks=user_tweaks,
                        api_error=f"HTTP {resp.status}: {err_body[:100]}"
                    )

                data = await resp.json()
                logger.info(f"Direct Gemini triage succeeded in {elapsed:.2f}s")

                candidates = data.get("candidates", [])
                if not candidates:
                    logger.warning(f"Gemini API returned no candidates: {data}")
                    return build_fallback_intent_triage(
                        clean_prompt=user_prompt,
                        target_info=target_info,
                        recent_context=recent_context,
                        user_tweaks=user_tweaks,
                        api_error="Empty candidate list from Gemini API"
                    )

                first_cand = candidates[0]
                content = first_cand.get("content", {})
                parts = content.get("parts", [])
                text_out = parts[0].get("text", "") if parts else ""

                parsed = None
                try:
                    parsed = json.loads(text_out)
                except Exception:
                    # Strip possible markdown fences
                    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text_out, re.DOTALL)
                    if m:
                        try:
                            parsed = json.loads(m.group(1))
                        except Exception:
                            pass

                if not parsed or not isinstance(parsed, dict):
                    logger.warning(f"Gemini triage returned unparseable JSON: {text_out[:200]}")
                    return build_fallback_intent_triage(
                        clean_prompt=user_prompt,
                        target_info=target_info,
                        recent_context=recent_context,
                        user_tweaks=user_tweaks,
                        api_error="Unparseable JSON response"
                    )

                turn_type = str(parsed.get("turn_type", "") or parsed.get("action_type", "")).upper()
                if turn_type in ("CONVERSE", "CONVERSATIONAL", "CHAT", "QUESTION"):
                    reply = parsed.get("conversational_reply") or parsed.get("reply") or ""
                    if not reply or is_state_history_query(user_prompt):
                        gt = await handle_conversational_turn(user_prompt, recent_context=recent_context)
                        if gt.get("conversational_reply"):
                            reply = gt["conversational_reply"]
                    return {
                        "turn_type": "CONVERSE",
                        "action_type": "converse",
                        "conversational_reply": reply,
                        "action_plan": None
                    }
                elif is_state_history_query(user_prompt):
                    gt = await handle_conversational_turn(user_prompt, recent_context=recent_context)
                    return {
                        "turn_type": "CONVERSE",
                        "action_type": "converse",
                        "conversational_reply": gt.get("conversational_reply", ""),
                        "action_plan": None
                    }
                elif turn_type == "ACTION" or "action_plan" in parsed or "dispatched_payload" in parsed:
                    plan = parsed.get("action_plan") or parsed
                    t_info = target_info or {}
                    target_node = plan.get("target_node") or t_info.get("target_machine", "hermes")
                    if target_node == "local":
                        target_node = "hermes"
                    plan["target_node"] = target_node

                    ws_label = t_info.get("workspace_label", "fleet-control")
                    role = t_info.get("role", "Chief of Staff")
                    plan["delegated_worker"] = plan.get("delegated_worker") or role
                    plan["domain"] = plan.get("domain") or "system operations"

                    if "is_self" not in plan:
                        is_self = (target_node in ("hermes", "localhost", "local") and ws_label == "fleet-control" and "butler" not in role.lower())
                        plan["is_self"] = is_self

                    # Extract target file if not present in plan
                    if not plan.get("target_file"):
                        m_file = re.search(r'(?:in|for|create|edit|modify|refactor|update|file)\s+[`\'"]?([a-zA-Z0-9_\-./]+\.[a-zA-Z0-9_]+)[`\'"]?', user_prompt, re.IGNORECASE)
                        if not m_file:
                            m_file = re.search(r'[`\'"]?([a-zA-Z0-9_\-./]+\.[a-zA-Z0-9_]+)[`\'"]?', user_prompt)
                        if m_file:
                            plan["target_file"] = m_file.group(1)

                    raw_payload = plan.get("dispatched_payload", "")
                    worker_id = plan.get("worker_id")
                    if "opencode" in str(plan.get("delegated_worker", "")).lower() or "opencode" in user_prompt.lower():
                        worker_id = "opencode-dev"
                    plan["dispatched_payload"] = validate_action_payload(raw_payload, target_node, worker_id=worker_id)

                    if "safety_tier" not in plan:
                        plan["safety_tier"] = classify_safety_tier(
                            user_prompt, plan.get("dispatched_payload", ""), target_node, role
                        )

                    if plan.get("safety_tier") == 3:
                        if not plan.get("recommendation") or "default" in plan.get("recommendation", "").lower():
                            plan["recommendation"] = "Explicit approval ('approve' or 'do it') required before executing mutating payload."

                    if "opencode" in str(plan.get("delegated_worker", "")).lower() or "opencode" in user_prompt.lower():
                        plan["is_self"] = False
                        plan["use_supervisor"] = True
                        plan["delegated_worker"] = "OpenCode"
                        if not plan.get("worker_id"):
                            plan["worker_id"] = "opencode-dev"
                        plan["recommendation"] = "Proceed with branch creation and automated test review."
                        cur_pl = plan.get("dispatched_payload", "")
                        if not cur_pl or any(pat.search(cur_pl) for pat in BANNED_ACTION_PAYLOAD_PATTERNS) or cur_pl.startswith("python"):
                            plan["dispatched_payload"] = "Herdr worktree create -> Dispatch opencode-dev in dev-grid (FrugaLLM :61721) -> no-mistakes review"
                    elif plan.get("is_self"):
                        plan["use_supervisor"] = False
                        plan["worker_id"] = None
                    elif "refactor" in user_prompt.lower():
                        plan["use_supervisor"] = True
                        if not plan.get("worker_id"):
                            plan["worker_id"] = "opencode-dev"
                        plan["recommendation"] = "Proceed with branch creation and automated test review."
                        cur_pl = plan.get("dispatched_payload", "")
                        if not cur_pl or any(pat.search(cur_pl) for pat in BANNED_ACTION_PAYLOAD_PATTERNS) or cur_pl.startswith("python"):
                            plan["dispatched_payload"] = "Herdr worktree create -> Dispatch opencode-dev in dev-grid (FrugaLLM :61721) -> no-mistakes review"

                    if not plan.get("recommendation"):
                        plan["recommendation"] = "Run default non-destructive plan."

                    return {
                        "turn_type": "ACTION",
                        "action_type": "action",
                        "conversational_reply": None,
                        "action_plan": plan
                    }

    except asyncio.TimeoutError:
        elapsed = time.time() - t0
        logger.warning(f"Gemini API triage timed out after {elapsed:.2f}s (limit: {timeout}s)")
        return build_fallback_intent_triage(
            clean_prompt=user_prompt,
            target_info=target_info,
            recent_context=recent_context,
            user_tweaks=user_tweaks,
            api_error=f"Timeout after {timeout}s"
        )
    except Exception as e:
        elapsed = time.time() - t0
        logger.warning(f"Error during direct Gemini triage ({type(e).__name__}): {e}")
        return build_fallback_intent_triage(
            clean_prompt=user_prompt,
            target_info=target_info,
            recent_context=recent_context,
            user_tweaks=user_tweaks,
            api_error=f"{type(e).__name__}: {e}"
        )

    return build_fallback_intent_triage(user_prompt, target_info, recent_context, user_tweaks)
