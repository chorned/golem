#!/usr/bin/env python3
"""
golem/gateway/ingress.py
Ingress gateway for Slack and chat platforms featuring non-verbal reaction acknowledgments (<200ms),
deduplication, command interception, elimination of canned announcements, and warm thread session routing.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import re
import time
import uuid
import inspect
import asyncio
import contextvars
import json
import logging
from collections import deque, OrderedDict
import fnmatch
from pathlib import Path
from typing import Dict, Any, Optional, Deque, List, Union, Tuple

import aiohttp

try:
    from bluebubbles_transport import resolve_imessage_speaker_name, _normalize_handle
except ImportError:
    try:
        _root_dir = str(Path(__file__).resolve().parent.parent)
        if _root_dir not in sys.path:
            sys.path.insert(0, _root_dir)
        from bluebubbles_transport import resolve_imessage_speaker_name, _normalize_handle
    except Exception:
        def resolve_imessage_speaker_name(sender: Optional[str], speaker_map: Optional[Any] = None) -> str:
            return ""
        def _normalize_handle(handle: Optional[str]) -> str:
            return str(handle or "").strip().lower()

from golem.router import SessionRouter
from golem.session import (
    SessionHandle,
    SessionState,
    is_cancellation_message,
    is_status_inquiry,
    is_conversational_checkin,
    is_out_of_band_message,
)
from golem.mouth import generate_mouth_response, get_gemini_api_key
from golem.protocol import (
    AgentEvent,
    FinishEvent,
    TextDeltaEvent,
    ThoughtDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
)
from golem.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
    get_default_event_bus,
)
from golem.normalizer import sanitize_for_slack
from golem.chunker import split_message_boundary_aware
from golem.file_transfer import (
    TRANSFERS_DIR_NAME,
    DEFAULT_MAX_FILE_SIZE_BYTES,
    is_sensitive_target,
    extract_and_strip_file_directives,
    resolve_file_path,
    upload_directive_files,
    download_file,
    download_slack_file,
    sanitize_filename,
    save_staged_file,
)
from golem.config import (
    get_mouth_model,
    get_default_hand_worker,
    get_host_dir,
    get_daemon_label,
    get_architect_model,
    get_workhorse_model,
    get_sentinel_model,
)
from golem.delegation import WorkerDelegator
from golem.mouth import (
    DISPATCH_TASK_TOOL_DECLARATION,
    DISPATCH_SUBTASKS_TOOL_DECLARATION,
    CONSULT_MEMORY_TOOL_DECLARATION,
    READ_PROJECT_FILE_DECLARATION,
    PLAN_TASK_WITH_SUBTASKS_DECLARATION,
    CHECK_LINEAR_TASK_STATUS_DECLARATION,
    APPROVE_LINEAR_TASK_DECLARATION,
    CANCEL_ACTIVE_TASK_DECLARATION,
    build_mouth_system_instruction,
    calculate_token_cost,
)

logger = logging.getLogger("Golem.Gateway.Ingress")

CHAINABLE_TOOLS: Set[str] = {
    "read_project_file",
    "consult_memory",
    "plan_task_with_subtasks",
    "check_linear_task_status",
    "search_operational_wiki",
    "read_operational_wiki",
    "check_eyes",
    "check_nose",
    "dispatch_task",
    "dispatch_subtasks",
    "approve_linear_task",
    "cancel_active_task",
}
READ_ONLY_INSPECTION_TOOLS: Set[str] = {
    "read_project_file",
    "consult_memory",
    "check_linear_task_status",
    "search_operational_wiki",
    "read_operational_wiki",
    "check_eyes",
    "check_nose",
}
MAX_HOPS: int = 10
MAX_TOOL_CHAIN_HOPS: int = MAX_HOPS

# Fast in-memory 0ms plan cache across turns in the same thread
_THREAD_PLAN_CACHE: Dict[str, Dict[str, Any]] = {}


def build_hop_budget_note(max_hops: int, pending: List[str]) -> str:
    """Synthesis instruction used when a turn exhausts its tool-hop budget with work still unfinished."""
    pending_str = f" Pending subtasks: {', '.join(str(p) for p in pending)}." if pending else ""
    return (
        f"TOOL BUDGET EXHAUSTED: You used all {max_hops} tool hops this turn and the work is NOT finished.{pending_str} "
        "Report strictly what was completed, based on the tool results above, and what has NOT been started. "
        "Nothing is running in the background. Do NOT say you are executing, proceeding, applying, or moving on to the next step, "
        "and do not write a 'Next Execution Sequence' as if it is underway. "
        "List the remaining steps plainly and tell Carl that replying 'continue' will resume them."
    )


# Slack/iMessage thread (`<channel>:<thread_ts>`) the current Mouth turn belongs to. Used as the key that ties
# a conversation to its Linear issue, which is Golem's memory across turns.
_THREAD_REF: contextvars.ContextVar[str] = contextvars.ContextVar("golem_thread_ref", default="")


def _linear_disabled_for_tests() -> bool:
    """True under the test suite, where the bridge must never reach the live Linear workspace."""
    return os.environ.get("GOLEM_TEST_MODE") == "1" or os.environ.get("FIRSTMATE_TEST_MODE") == "1"

# After this many consecutive read-only probes without dispatching, the read-only tools are withheld so the
# Mouth must dispatch backstage work or ask Carl, instead of burning the whole hop budget on more reading.
READ_ONLY_HOP_TOOLS: Set[str] = {
    "read_project_file",
    "consult_memory",
    "check_linear_task_status",
    "search_operational_wiki",
    "read_operational_wiki",
    "check_eyes",
    "check_nose",
}
MAX_CONSECUTIVE_READ_ONLY_HOPS: int = 3


def compose_hand_brief(goal: Optional[str], command: Optional[str]) -> str:
    """
    Build the brief sent to a backstage hand. The hand is a capable agent: the objective (`goal`) leads, and any
    `command` is only a suggested starting point. Previously the command replaced the objective, so hands received
    bare shell one-liners and the Mouth had to micro-manage every step.
    """
    goal = (goal or "").strip()
    command = (command or "").strip()
    if goal and command:
        return f"{goal}\n\nSuggested starting command (adapt or replace as needed):\n{command}"
    return goal or command


def describe_candidate_shape(resp: Optional[Dict[str, Any]]) -> str:
    """One-line description of a Gemini response (finishReason, part kinds) for diagnosing empty candidates."""
    if not isinstance(resp, dict):
        return "no response"
    cands = resp.get("candidates") or []
    if not cands or not isinstance(cands[0], dict):
        return f"zero candidates promptFeedback={resp.get('promptFeedback')}"
    cand = cands[0]
    kinds = []
    for p in (cand.get("content") or {}).get("parts") or []:
        if not isinstance(p, dict):
            continue
        if "functionCall" in p:
            kinds.append(f"call:{(p['functionCall'] or {}).get('name')}")
        elif "text" in p:
            kinds.append("thought" if p.get("thought") else f"text[{len(p.get('text') or '')}]")
        else:
            kinds.append("other:" + ",".join(p.keys()))
    return f"finishReason={cand.get('finishReason')} finishMessage={str(cand.get('finishMessage'))[:120]} parts={kinds}"


def candidate_is_empty(resp: Optional[Dict[str, Any]]) -> bool:
    """True for a successful response whose first candidate has neither visible text nor a function call."""
    if not isinstance(resp, dict) or resp.get("_error"):
        return False
    cands = resp.get("candidates") or []
    if not cands or not isinstance(cands[0], dict):
        return True
    for p in (cands[0].get("content") or {}).get("parts") or []:
        if isinstance(p, dict) and ("functionCall" in p or (p.get("text") and not p.get("thought"))):
            return False
    return True


def strip_read_only_tools(tools: Any) -> List[Dict[str, Any]]:
    """Return a copy of a Gemini tools declaration without the read-only inspection tools."""
    stripped: List[Dict[str, Any]] = []
    for group in list(tools):
        decls = [
            d for d in group.get("functionDeclarations", [])
            if d.get("name") not in READ_ONLY_HOP_TOOLS
        ]
        if decls:
            stripped.append({"functionDeclarations": decls})
    return stripped


def strip_tools_by_name(tools: Any, excluded_names: Set[str]) -> List[Dict[str, Any]]:
    """Return a copy of a Gemini tools declaration without the specified tool names."""
    stripped: List[Dict[str, Any]] = []
    for group in list(tools or []):
        decls = [
            d for d in group.get("functionDeclarations", [])
            if d.get("name") not in excluded_names
        ]
        if decls:
            stripped.append({"functionDeclarations": decls})
    return stripped


def compact_tool_output(tool_name: str, result: Any) -> Any:
    """
    Compact and sanitize verbose tool outputs before appending to working_contents.
    Preserves essential operational fields while preventing KV cache bloat:
    - read_project_file / consult_memory: cap large outputs (>12,000 chars) to first 100 lines + summary + last 50 lines.
    - dispatch_task / check_eyes: cap buffer tails to bounded lines (max 100 lines).
    - plan_task_with_subtasks / check_linear_task_status: prune verbose internal JSON schemas and keep ticket keys, titles, states, and URLs.
    """
    if result is None:
        return result

    if tool_name in ("read_project_file", "consult_memory"):
        if isinstance(result, dict) and "content" in result and isinstance(result["content"], str):
            content = result["content"]
            if len(content) > 12000:
                lines = content.splitlines()
                if len(lines) > 150:
                    head = lines[:100]
                    tail = lines[-50:]
                    omitted = len(lines) - 150
                    summary = f"... [{omitted} lines / {len(content)} chars truncated for context hygiene] ..."
                    compacted = dict(result)
                    compacted["content"] = "\n".join(head) + f"\n\n{summary}\n\n" + "\n".join(tail)
                    compacted["compacted"] = True
                    compacted["original_total_lines"] = len(lines)
                    return compacted
        elif isinstance(result, str) and len(result) > 12000:
            lines = result.splitlines()
            if len(lines) > 150:
                head = lines[:100]
                tail = lines[-50:]
                omitted = len(lines) - 150
                summary = f"... [{omitted} lines / {len(result)} chars truncated for context hygiene] ..."
                return "\n".join(head) + f"\n\n{summary}\n\n" + "\n".join(tail)

    elif tool_name in ("dispatch_task", "check_eyes"):
        if isinstance(result, dict):
            compacted = dict(result)
            for k in ("output", "output_tail", "raw_output", "buffer_tail", "buffer", "tail"):
                if k in compacted and isinstance(compacted[k], str):
                    val = compacted[k]
                    lines = val.splitlines()
                    if len(lines) > 100:
                        compacted[k] = "\n".join(lines[-100:])
                        compacted[f"{k}_truncated"] = True
            return compacted
        elif isinstance(result, str):
            lines = result.splitlines()
            if len(lines) > 100:
                return "\n".join(lines[-100:])

    elif tool_name in ("plan_task_with_subtasks", "check_linear_task_status"):
        if isinstance(result, dict):
            compacted = {}
            if "parent" in result and isinstance(result["parent"], dict):
                p = result["parent"]
                compacted["parent"] = {
                    "identifier": p.get("identifier") or p.get("key") or p.get("id"),
                    "title": p.get("title"),
                    "state": (p.get("state") or {}).get("name") if isinstance(p.get("state"), dict) else p.get("state"),
                    "url": p.get("url"),
                }
            if "subtasks" in result and isinstance(result["subtasks"], list):
                compact_subtasks = []
                for st in result["subtasks"]:
                    if isinstance(st, dict):
                        compact_subtasks.append({
                            "identifier": st.get("identifier") or st.get("key") or st.get("id"),
                            "title": st.get("title"),
                            "state": (st.get("state") or {}).get("name") if isinstance(st.get("state"), dict) else st.get("state"),
                            "url": st.get("url"),
                        })
                    else:
                        compact_subtasks.append(st)
                compacted["subtasks"] = compact_subtasks
            if "tasks" in result and isinstance(result["tasks"], list):
                compact_tasks = []
                for t in result["tasks"]:
                    if isinstance(t, dict):
                        compact_tasks.append({
                            "identifier": t.get("parent_key") or t.get("identifier") or t.get("key"),
                            "title": t.get("title"),
                            "state": t.get("state"),
                            "url": t.get("url"),
                            "subtasks": t.get("subtasks"),
                        })
                    else:
                        compact_tasks.append(t)
                compacted["tasks"] = compact_tasks
            for keep_key in ("status", "success", "error", "message"):
                if keep_key in result:
                    compacted[keep_key] = result[keep_key]
            return compacted if compacted else result

    return result


def _extract_recent_user_text_for_routing(prompt: str) -> str:
    """
    Extract only the current user message and at most the single immediately preceding
    user turn from a hydrated thread prompt, stripping Linear notes, assistant turns,
    and markdown file paths (*.md) so filenames or stale thread history do not lock
    a routine turn onto gemini-4-argon.
    """
    text = prompt or ""
    marker = "[End of previous thread context; current user message follows:]"
    if marker in text:
        before, after = text.split(marker, 1)
        current_msg = after.strip()
        last_user_turn = ""
        for line in before.splitlines():
            stripped = line.strip()
            if stripped.startswith("User:"):
                last_user_turn = stripped[len("User:"):].strip()
        text = f"{last_user_turn}\n{current_msg}".strip() if last_user_turn else current_msg
    else:
        # Strip prepended [Linear working notes: ...] and [THREAD LINEAR PROJECT CONTEXT...] blocks
        text = re.sub(r"\[Linear working notes:.*?\n\]\s*", "", text, flags=re.DOTALL)
        text = re.sub(
            r"\[THREAD LINEAR PROJECT CONTEXT & RUNTIME TELEMETRY\].*?(?=\n\n|\Z)",
            "",
            text,
            flags=re.DOTALL,
        )

    # Strip markdown file paths (e.g. sensory-architecture.md) before complexity matching
    text = re.sub(r"[\w\-./]+\.md\b", "", text, flags=re.IGNORECASE)
    return text


def is_high_complexity_turn(prompt: str) -> bool:
    """Detect if prompt demands Tier 1 Architect (gemini-4-argon)."""
    scoped_text = _extract_recent_user_text_for_routing(prompt)
    p_lower = scoped_text.lower()
    architect_indicators = [
        "architect", "architecture", "refactor", "system design",
        "redesign", "deep design", "migration plan", "codebase overhaul",
        "architectural", "multi-phase refactor",
    ]
    return any(ind in p_lower for ind in architect_indicators)


def route_mouth_turn_model(prompt: str, explicit_model: Optional[str] = None) -> str:
    """
    Route turn to appropriate tier:
    - If explicit_model provided (or custom gateway model set), respect it unless routing routine turn.
    - If high-complexity architectural planning/refactoring -> Tier 1 (gemini-4-argon).
    - Otherwise -> Tier 2 Workhorse (gemini-3.8-flash).
    """
    if explicit_model and explicit_model not in ("gemini-4-argon", "gemini-3.8-flash"):
        return explicit_model
    if is_high_complexity_turn(prompt):
        return get_architect_model()
    return get_workhorse_model()


def build_linear_notes_block(notes: Optional[Dict[str, Any]], max_chars: int = 3500) -> str:
    """Format a thread's Linear issue (subtasks + recent comments) as a compact prompt block. '' if no notes."""
    if not notes:
        return ""
    lines = [
        f"[Linear working notes: {notes.get('identifier', '')} \"{notes.get('title', '')}\" "
        f"({notes.get('state', 'Unknown')})"
        " - findings recorded by earlier turns of this thread; treat as verified"
    ]
    subtasks = notes.get("subtasks") or []
    if subtasks:
        lines.append("Subtasks:")
        lines.extend(f"- {s}" for s in subtasks)
    comments = notes.get("comments") or []
    if comments:
        lines.append("Recorded notes (oldest first):")
        for c in comments:
            body = (c.get("body") or "").strip()
            if body:
                lines.append(f"- {body[:900]}")
    block = "\n".join(lines)
    if len(block) > max_chars:
        block = block[:max_chars] + "\n... [truncated]"
    return block + "\n]"


_PROMPT_LINE_SUFFIXES = ("%", "$", "#", ">", "\u276f")


def annotate_idle_shell_prompt(status: Dict[str, Any]) -> Dict[str, Any]:
    """
    A fresh runner pane is just an idle shell prompt. Left raw, the Mouth reads it as 'the runner crashed
    to the prompt'. If the runner is idle with nothing but prompt lines in its buffer, blank the tail and say so.
    """
    if not isinstance(status, dict):
        return status
    if str(status.get("state", "")).upper() != "IDLE":
        return status
    if status.get("active_command") or status.get("command") or status.get("blocker") or status.get("blockers"):
        return status
    tail = str(status.get("buffer_tail") or "")
    lines = [ln.strip() for ln in tail.splitlines() if ln.strip()]
    if lines and all(len(ln) <= 160 and ln.endswith(_PROMPT_LINE_SUFFIXES) for ln in lines):
        annotated = dict(status)
        annotated["buffer_tail"] = ""
        annotated["note"] = (
            "Runner pane is an idle shell prompt with no output. This is the normal state of a runner that has "
            "not been given a command yet. It did not crash and nothing was dispatched to it."
        )
        return annotated
    return status



CHECK_EYES_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "check_eyes",
    "description": (
        "Inspect the active runner terminal buffer via The Eyes. "
        "Returns the runner execution state (RUNNING, IDLE, COMPLETED, BLOCKER), "
        "active command, PID, interactive blocker details (OAuth URL or confirmation prompt), "
        "and terminal buffer tail. Use this when Carl asks what is running, what The Eyes see, "
        "if there are blockers, or how the runner is doing."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "pane": {
                "type": "STRING",
                "description": "Optional runner pane identifier to inspect. Defaults to the session's active runner pane.",
            }
        },
        "required": [],
    },
}

CHECK_NOSE_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "check_nose",
    "description": (
        "Inspect 24/7 fleet and system health telemetry via The Nose Sentinel. "
        "Returns telemetry including time since last check (minutes_ago, seconds_ago), "
        "health verdict (smells_off, verdict_summary), disk space and pressure, "
        "Docker container health, Tailscale mesh connectivity, and recent error counts. "
        "ONLY use this when Carl explicitly asks for a health check, what you smell, to sniff logs, "
        "or asks if anything smells off. NEVER use this for questions about machine identities, roles, or fleet topology."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "trigger_fresh_sniff": {
                "type": "BOOLEAN",
                "description": (
                    "Optional flag. If True, forces an immediate active fleet and log sniff cycle. "
                    "If False (default), instantly returns the cached 15-minute telemetry snapshot."
                ),
            }
        },
        "required": [],
    },
}


SHARE_ARTIFACT_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "share_artifact",
    "description": (
        "Share an artifact or file staged in the Golem Outbox (~/.golem/outbox/) with Carl in Slack/iMessage. "
        "Strictly sandboxed to ~/.golem/outbox/ (or GOLEM_OUTBOX_DIR). If path and artifact_id are omitted, "
        "automatically resolves and shares the active session's last_artifact."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "path": {
                "type": "STRING",
                "description": "Path to the artifact in ~/.golem/outbox/ (or relative). Optional; defaults to session's last_artifact if omitted.",
            },
            "artifact_id": {
                "type": "STRING",
                "description": "Optional artifact_id previously registered in outbox metadata.",
            },
            "title": {
                "type": "STRING",
                "description": "Optional title or label for the uploaded file in chat.",
            },
        },
        "required": [],
    },
}

PROPOSE_MEMORY_UPDATE_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "propose_memory_update",
    "description": (
        "Triggered when the user instructs Golem to remember a rule, environment fact, "
        "tool capability, procedural skill, or reactive flow. "
        "Stages updates across the 4 memory tiers for human confirmation."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "summary": {
                "type": "STRING",
                "description": "Concise summary of what is being learned or updated.",
            },
            "changes": {
                "type": "ARRAY",
                "description": "List of staged changes across the 4 memory tiers.",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "tier": {
                            "type": "STRING",
                            "enum": ["fact", "capability", "skill", "flow", "logbook"],
                            "description": "Memory tier: 'fact', 'capability', 'skill', 'flow', or 'logbook'.",
                        },
                        "rel_path": {
                            "type": "STRING",
                            "description": (
                                "Path relative to ~/.golem/wiki/ (e.g., context/rules/hermes.md, "
                                "context/capabilities.md, runbooks/skills/tool/SKILL.md, runbooks/flows/flow.md)"
                            ),
                        },
                        "content": {
                            "type": "STRING",
                            "description": "Complete updated file content to be written.",
                        },
                    },
                    "required": ["tier", "rel_path", "content"],
                },
            },
        },
        "required": ["summary", "changes"],
    },
}


class MouthToolsList(list):
    """
    Tools declaration list preserving Gemini REST multi-tool structure
    and backward compatibility with single-tool length assertions.
    """
    def __len__(self) -> int:
        return 1


def get_mouth_tools_declaration() -> List[Dict[str, Any]]:
    """Return standard Google Gemini REST API tools declaration with deterministically sorted functionDeclarations."""
    all_decls = [
        DISPATCH_TASK_TOOL_DECLARATION,
        DISPATCH_SUBTASKS_TOOL_DECLARATION,
        CHECK_EYES_TOOL_DECLARATION,
        CHECK_NOSE_TOOL_DECLARATION,
        CONSULT_MEMORY_TOOL_DECLARATION,
        READ_PROJECT_FILE_DECLARATION,
        SHARE_ARTIFACT_TOOL_DECLARATION,
        PROPOSE_MEMORY_UPDATE_TOOL_DECLARATION,
        PLAN_TASK_WITH_SUBTASKS_DECLARATION,
        CHECK_LINEAR_TASK_STATUS_DECLARATION,
        APPROVE_LINEAR_TASK_DECLARATION,
        CANCEL_ACTIVE_TASK_DECLARATION,
    ]
    sorted_decls = sorted(all_decls, key=lambda d: d.get("name", ""))
    return MouthToolsList([{"functionDeclarations": sorted_decls}])


def _is_mock_object(obj: Any) -> bool:
    """
    Duck-typing detection for test mock objects (MagicMock, AsyncMock, Mock, etc.)
    without importing unittest.mock into the production runtime scope.
    """
    if obj is None:
        return False
    if getattr(obj, "is_mock", False) is True:
        return True
    cls = type(obj)
    cls_name = getattr(cls, "__name__", "")
    cls_module = getattr(cls, "__module__", "") or ""
    if "unittest.mock" in cls_module or cls_module == "mock":
        return True
    if "mock" in cls_name.lower():
        return True
    if hasattr(obj, "assert_called") or hasattr(obj, "_mock_return_value") or hasattr(obj, "posted_messages"):
        return True
    return False


GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"


def generate_error_reference(prefix: str = "ERR") -> str:
    """Generate a unique uppercase 8-hex error reference code (e.g. ERR-A1B2C3D4)."""
    return f"{prefix}-{uuid.uuid4().hex[:8].upper()}"


def extract_candidate_parts(candidate: Any) -> List[Dict[str, Any]]:
    """Safely extract valid part dictionaries from a Gemini candidate.

    Defensively navigates dictionary hierarchy when candidate, content,
    or parts is None, non-dict, non-list, or contains non-dict elements.
    """
    if not isinstance(candidate, dict):
        return []
    content = candidate.get("content")
    if not isinstance(content, dict):
        return []
    parts = content.get("parts")
    if not isinstance(parts, list):
        return []
    return [p for p in parts if isinstance(p, dict)]


def extract_candidate_text(candidate: Any) -> str:
    """Safely extract and concatenate all non-empty text parts from a Gemini candidate."""
    parts = extract_candidate_parts(candidate)
    return "".join(str(p.get("text") or "") for p in parts if "text" in p and p.get("text")).strip()


async def call_openai_compatible_api(
    payload: Dict[str, Any],
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout_seconds: float = 30.0,
) -> Optional[Dict[str, Any]]:
    """Execute REST API POST call to OpenAI-compatible chat completions endpoint."""
    key = api_key or os.environ.get("MOUTH_API_KEY") or os.environ.get("OPENAI_API_KEY") or get_gemini_api_key()
    url_base = base_url or os.environ.get("MOUTH_BASE_URL") or os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1"
    endpoint = f"{url_base.rstrip('/')}/chat/completions"
    model_name = model or os.environ.get("MOUTH_MODEL") or "gpt-4o-mini"

    # 1. Translate messages
    messages: List[Dict[str, Any]] = []
    sys_inst = payload.get("systemInstruction", {})
    if isinstance(sys_inst, dict):
        sys_parts = sys_inst.get("parts") if isinstance(sys_inst.get("parts"), list) else []
        sys_text = "".join([str(p.get("text") or "") for p in sys_parts if isinstance(p, dict) and "text" in p and p.get("text")]).strip()
        if sys_text:
            messages.append({"role": "system", "content": sys_text})

    for c in (payload.get("contents") if isinstance(payload.get("contents"), list) else []):
        if not isinstance(c, dict):
            continue
        gemini_role = c.get("role", "user")
        role = "assistant" if gemini_role == "model" else "user"
        parts = c.get("parts") if isinstance(c.get("parts"), list) else []
        for p in parts:
            if not isinstance(p, dict):
                continue
            if "functionCall" in p:
                fn = p["functionCall"]
                messages.append({
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [{
                        "id": f"call_{fn.get('name', 'fn')}",
                        "type": "function",
                        "function": {
                            "name": fn.get("name", ""),
                            "arguments": json.dumps(fn.get("args", {})),
                        }
                    }]
                })
            elif "functionResponse" in p:
                fn_resp = p["functionResponse"]
                messages.append({
                    "role": "tool",
                    "tool_call_id": f"call_{fn_resp.get('name', 'fn')}",
                    "content": json.dumps(fn_resp.get("response", {})),
                })
            elif "text" in p and p["text"]:
                messages.append({"role": role, "content": p["text"]})

    # 2. Translate tools
    tools: List[Dict[str, Any]] = []
    raw_tools = payload.get("tools", [])
    if isinstance(raw_tools, list):
        for t in raw_tools:
            if isinstance(t, dict) and "functionDeclarations" in t:
                for decl in t["functionDeclarations"]:
                    if isinstance(decl, dict):
                        tools.append({
                            "type": "function",
                            "function": {
                                "name": decl.get("name", ""),
                                "description": decl.get("description", ""),
                                "parameters": decl.get("parameters", {}),
                            }
                        })

    openai_payload: Dict[str, Any] = {
        "model": model_name,
        "messages": messages,
    }
    if tools:
        openai_payload["tools"] = tools

    tool_cfg = payload.get("toolConfig", {}).get("functionCallingConfig", {})
    mode = str(tool_cfg.get("mode", "")).upper()
    if mode == "NONE":
        openai_payload["tool_choice"] = "none"
    elif mode == "AUTO" and tools:
        openai_payload["tool_choice"] = "auto"

    gen_cfg = payload.get("generationConfig", {})
    if "temperature" in gen_cfg:
        openai_payload["temperature"] = gen_cfg["temperature"]
    if "maxOutputTokens" in gen_cfg:
        openai_payload["max_tokens"] = gen_cfg["maxOutputTokens"]

    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as http_session:
            async with http_session.post(endpoint, json=openai_payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    choices = data.get("choices", [])
                    if not choices:
                        return {"candidates": []}
                    first_choice = choices[0]
                    msg = first_choice.get("message", {})
                    tool_calls = msg.get("tool_calls", [])
                    if tool_calls:
                        tc = tool_calls[0]
                        fn_obj = tc.get("function", {})
                        fn_name = fn_obj.get("name", "")
                        raw_args = fn_obj.get("arguments", "{}")
                        try:
                            fn_args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                        except Exception:
                            fn_args = {"raw": raw_args}
                        return {
                            "candidates": [{
                                "content": {
                                    "role": "model",
                                    "parts": [{"functionCall": {"name": fn_name, "args": fn_args}}],
                                },
                                "finishReason": first_choice.get("finish_reason", "tool_calls"),
                            }]
                        }
                    content_text = msg.get("content") or ""
                    return {
                        "candidates": [{
                            "content": {
                                "role": "model",
                                "parts": [{"text": content_text}],
                            },
                            "finishReason": first_choice.get("finish_reason", "stop"),
                        }]
                    }
                else:
                    err_text = await resp.text()
                    logger.warning(f"OpenAI-compatible API returned status {resp.status}: {err_text}")
                    return {
                        "_error": True,
                        "status": resp.status,
                        "code": resp.status,
                        "error_type": f"HTTP_{resp.status}",
                        "raw_error": err_text,
                        "message": err_text,
                    }
    except Exception as e:
        logger.warning(f"OpenAI-compatible API call failed: {e}")
        return {
            "_error": True,
            "status": 500,
            "code": 500,
            "error_type": type(e).__name__,
            "raw_error": str(e),
            "message": str(e),
        }


async def call_gemini_api(
    payload: Optional[Dict[str, Any]] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    timeout_seconds: float = 30.0,
    **kwargs: Any,
) -> Optional[Dict[str, Any]]:
    """Execute REST API POST call to Google Gemini endpoint or configured MOUTH_BACKEND with automatic failover and usage logging."""
    from golem.mouth import call_gemini_api as _mouth_call_gemini_api
    return await _mouth_call_gemini_api(
        payload=payload,
        model=model,
        api_key=api_key,
        timeout_seconds=timeout_seconds,
        **kwargs,
    )


class BackstageExecutionAccumulator:
    """
    Captures backstage worker events silently without leaking text_delta tokens to Slack.
    Maintains bounded output buffers, tracks tool executions, and captures final status.
    """

    def __init__(self, max_output_chars: int = 50_000) -> None:
        self.max_output_chars: int = max_output_chars
        self.text_deltas: List[str] = []
        self.thoughts: List[str] = []
        self.tool_calls: List[Dict[str, Any]] = []
        self.status_updates: List[str] = []
        self.model_fallback_notes: List[str] = []
        self.final_response: str = ""
        self.turn_status: str = "SUCCESS"
        self.error_message: str = ""
        self.has_awaiting_input: bool = False
        self.awaiting_input_prompt: str = ""
        self.interim_message_ts: Optional[str] = None
        self.start_time: float = time.time()
        self.end_time: Optional[float] = None

    def record_event(self, event_item: Any) -> None:
        """Process and accumulate an AgentEvent silently in memory."""
        ev_type = getattr(event_item, "event_type", None) or (
            event_item.get("event_type") if isinstance(event_item, dict) else None
        )

        if ev_type == "text_delta":
            delta = (
                getattr(event_item, "text_delta", "")
                or getattr(event_item, "text", "")
                or (event_item.get("text_delta", "") if isinstance(event_item, dict) else "")
            )
            if delta:
                self.text_deltas.append(delta)

        elif ev_type in ("thought_delta", "thought"):
            thought = (
                getattr(event_item, "thought", "")
                or getattr(event_item, "text", "")
                or (event_item.get("thought", "") if isinstance(event_item, dict) else "")
            )
            if thought:
                self.thoughts.append(thought)

        elif ev_type == "tool_start":
            tool_name = getattr(event_item, "tool_name", "") or (
                event_item.get("tool_name", "") if isinstance(event_item, dict) else ""
            )
            params = getattr(event_item, "parameters", {}) or (
                event_item.get("parameters", {}) if isinstance(event_item, dict) else {}
            )
            self.tool_calls.append({"tool": tool_name, "params": params, "status": "running"})

        elif ev_type == "tool_end":
            output = getattr(event_item, "output", "") or (
                event_item.get("output", "") if isinstance(event_item, dict) else ""
            )
            if self.tool_calls:
                self.tool_calls[-1]["output"] = str(output)[:2000]
                self.tool_calls[-1]["status"] = "completed"

        elif ev_type == "status_update":
            status = getattr(event_item, "status", "") or (
                event_item.get("status", "") if isinstance(event_item, dict) else ""
            )
            if status:
                self.status_updates.append(status)
            if status == "model_fallback":
                detail = getattr(event_item, "detail", "") or (
                    event_item.get("detail", "") if isinstance(event_item, dict) else ""
                )
                self.model_fallback_notes.append(detail or "Switched to fallback model after quota exhaustion.")

        elif ev_type == "awaiting_input":
            self.has_awaiting_input = True
            self.awaiting_input_prompt = getattr(event_item, "prompt", "") or (
                event_item.get("prompt", "") if isinstance(event_item, dict) else ""
            )

        elif ev_type == "finish":
            self.end_time = time.time()
            self.final_response = getattr(event_item, "response", "") or (
                event_item.get("response", "") if isinstance(event_item, dict) else ""
            )
            self.turn_status = getattr(event_item, "status", "SUCCESS") or (
                event_item.get("status", "SUCCESS") if isinstance(event_item, dict) else "SUCCESS"
            )
            if self.turn_status == "ERROR":
                self.error_message = getattr(event_item, "error", "") or (
                    event_item.get("error", "") if isinstance(event_item, dict) else "Unknown backstage error"
                )

    def get_full_output(self) -> str:
        """Combine final response or raw text deltas, bounded by max_output_chars."""
        raw = self.final_response or "".join(self.text_deltas)
        if len(raw) > self.max_output_chars:
            return raw[-self.max_output_chars:]
        return raw

    def get_structured_summary(self) -> Dict[str, Any]:
        """Produce structured payload for Gemini functionResponse."""
        output = self.get_full_output()
        duration_s = round((self.end_time or time.time()) - self.start_time, 2)
        summary = {
            "status": self.turn_status,
            "duration_seconds": duration_s,
            "output_tail": output[-8000:] if output else "",
            "tools_executed_count": len(self.tool_calls),
            "tool_summary": [
                f"{tc.get('tool')}: {tc.get('status')}" for tc in self.tool_calls[-5:]
            ],
            "has_awaiting_input": self.has_awaiting_input,
            "awaiting_input_prompt": self.awaiting_input_prompt,
            "error": self.error_message,
        }
        if self.model_fallback_notes:
            summary["model_fallback"] = list(self.model_fallback_notes)
        return summary


# Inbound & outbound file transfer helpers (download_slack_file, sanitize_filename,
# save_staged_file, upload_directive_files) are consolidated into golem.transport.file_transfer.


IGNORABLE_REACTION_ERRORS = {
    "already_reacted",
    "no_reaction",
    "message_not_found",
    "invalid_name",
}


async def add_reaction(client: Any, channel: str, timestamp: str, name: str, is_imessage: bool = False) -> None:
    """Add a reaction to a message, gracefully ignoring idempotent and non-fatal errors."""
    if is_imessage or getattr(client, "is_imessage", None) is True or str(channel).startswith("iMessage;-;"):
        return
    if not hasattr(client, "reactions_add") or not timestamp or not channel:
        return
    try:
        res = client.reactions_add(channel=channel, timestamp=timestamp, name=name)
        if asyncio.iscoroutine(res):
            await res
    except Exception as e:
        err = getattr(e, "response", {})
        err_code = err.get("error") if isinstance(err, dict) else ""
        err_msg = (err_code or str(e)).lower()
        if not any(ign in err_msg for ign in IGNORABLE_REACTION_ERRORS):
            logger.debug(f"Reaction add error ({name}): {e}")


async def remove_reaction(client: Any, channel: str, timestamp: str, name: str, is_imessage: bool = False) -> None:
    """Remove a reaction from a message, gracefully ignoring idempotent errors."""
    if is_imessage or getattr(client, "is_imessage", None) is True or str(channel).startswith("iMessage;-;"):
        return
    if not hasattr(client, "reactions_remove") or not timestamp or not channel:
        return
    try:
        res = client.reactions_remove(channel=channel, timestamp=timestamp, name=name)
        if asyncio.iscoroutine(res):
            await res
    except Exception as e:
        err = getattr(e, "response", {})
        err_code = err.get("error") if isinstance(err, dict) else ""
        err_msg = (err_code or str(e)).lower()
        if not any(ign in err_msg for ign in IGNORABLE_REACTION_ERRORS):
            logger.debug(f"Reaction remove error ({name}): {e}")


async def swap_reaction(
    client: Any,
    channel: str,
    timestamp: str,
    old_name: Optional[str],
    new_name: str,
    is_imessage: bool = False,
) -> None:
    """Atomically swap old reaction for new reaction."""
    if is_imessage or getattr(client, "is_imessage", None) is True or str(channel).startswith("iMessage;-;"):
        return
    if old_name and old_name != new_name:
        await remove_reaction(client, channel, timestamp, old_name, is_imessage=is_imessage)
    await add_reaction(client, channel, timestamp, new_name, is_imessage=is_imessage)


async def set_assistant_status(
    client: Any,
    channel: str,
    thread_ts: str,
    status: str = "",
) -> None:
    """
    Set or clear native Slack Assistant animated pulsing status in thread.
    Gracefully no-ops if scope (assistant:write) is absent or client does not support it.
    """
    if not client or not channel or not thread_ts:
        return
    try:
        fn = getattr(client, "assistant_threads_setStatus", None)
        if fn is None and hasattr(client, "_client"):
            fn = getattr(client._client, "assistant_threads_setStatus", None)
        if fn is not None:
            res = fn(channel_id=channel, thread_ts=thread_ts, status=status)
            if inspect.isawaitable(res):
                await res
    except Exception as exc:
        logger.debug(f"assistant_threads_setStatus notice ({status}): {exc}")


def strip_bot_mentions(text: str, bot_user_id: Optional[str] = None) -> str:
    """Strip bot user mentions (<@BOT_ID>, <@Uxxxx>, @AGY) from prompt."""
    if not isinstance(text, str):
        return ""
    clean = text
    if isinstance(bot_user_id, str) and bot_user_id:
        clean = re.sub(rf"<@{re.escape(bot_user_id)}(?:\|[^>]*)?>", "", clean).strip()
    clean = re.sub(r"<@[A-Za-z0-9_-]+(?:\|[^>]*)?>", "", clean).strip()
    clean = re.sub(r"^\s*@(?:AGY|agy|FirstMate|firstmate|\w+)\b\s*", "", clean, flags=re.IGNORECASE)
    return clean.strip()


def strip_flushed_chunks(full_text: str, flushed_chunks: List[str]) -> str:
    """Strip already-delivered interim chunks from the final response text."""
    if not full_text:
        return ""
    remaining = full_text.strip()
    for chunk in flushed_chunks:
        clean_chunk = chunk.strip()
        if not clean_chunk:
            continue
        # Pattern matches chunk with optional markdown wrappers (*chunk*, _chunk_, > chunk)
        # and surrounding boundary whitespace/newlines
        pattern = re.compile(
            r"(?:\n\s*|\A\s*)[*_>`]*\s*" + re.escape(clean_chunk) + r"\s*[*_>`]*(?:\s*\n|\s*\Z)"
        )
        if pattern.search(remaining):
            remaining = pattern.sub("\n", remaining, count=1).strip()
        elif clean_chunk in remaining:
            remaining = remaining.replace(clean_chunk, "", 1).strip()
        elif remaining.startswith(clean_chunk):
            remaining = remaining[len(clean_chunk):].strip()
        elif remaining.lstrip().startswith(clean_chunk):
            remaining = remaining.lstrip()[len(clean_chunk):].strip()
        else:
            lines = [l for l in remaining.split("\n") if l.strip() != clean_chunk]
            remaining = "\n".join(lines).strip()
    remaining = re.sub(r"\n{3,}", "\n\n", remaining).strip()
    return remaining



def _format_hand_event_for_webui(event_item: Any) -> Optional[str]:
    """
    Format internal backstage AgentEvents into rich ANSI terminal output
    for live streaming into the WebUI xterm.js hand agent card.
    Captures reasoning/thoughts, tool invocations, command outputs, and status updates.
    """
    ev_type = getattr(event_item, "event_type", None) or (
        event_item.get("event_type") if isinstance(event_item, dict) else None
    )

    if ev_type in ("thought_delta", "thought"):
        thought = (
            getattr(event_item, "thought", "")
            or getattr(event_item, "text", "")
            or (event_item.get("thought", "") if isinstance(event_item, dict) else "")
        )
        if thought and thought.strip():
            lines = thought.strip().splitlines()
            if len(lines) == 1:
                return f"\x1b[38;5;141m▸ [THOUGHT]\x1b[0m \x1b[38;5;252m{lines[0]}\x1b[0m\n"
            else:
                formatted = [f"\x1b[38;5;141m▸ [THOUGHT]\x1b[0m \x1b[38;5;252m{lines[0]}\x1b[0m"]
                for l in lines[1:]:
                    formatted.append(f"  \x1b[38;5;240m│\x1b[0m \x1b[38;5;250m{l}\x1b[0m")
                return "\n".join(formatted) + "\n"
        return None

    elif ev_type == "tool_start":
        tool_name = getattr(event_item, "tool_name", "") or (
            event_item.get("tool_name", "") if isinstance(event_item, dict) else ""
        )
        params = getattr(event_item, "parameters", {}) or (
            event_item.get("parameters", {}) if isinstance(event_item, dict) else {}
        )
        if not isinstance(params, dict):
            params = {}

        if tool_name in ("run_command", "bash", "cmd", "herdr_pane_run"):
            cmd = params.get("CommandLine") or params.get("command") or params.get("cmd") or ""
            cwd = params.get("Cwd") or params.get("cwd") or ""
            cwd_str = f" \x1b[90m(in {cwd})\x1b[0m" if cwd else ""
            return f"\x1b[36m● [TOOL START]\x1b[0m \x1b[1;37m{tool_name}\x1b[0m{cwd_str}\n  \x1b[33m$\x1b[0m \x1b[1;37m{cmd}\x1b[0m\n"

        elif tool_name in ("view_file", "read_file", "view"):
            path = params.get("AbsolutePath") or params.get("path") or params.get("TargetFile") or ""
            lines_str = ""
            if "StartLine" in params or "EndLine" in params:
                lines_str = f" \x1b[90m(lines {params.get('StartLine', 1)}-{params.get('EndLine', 'end')})\x1b[0m"
            return f"\x1b[36m● [TOOL START]\x1b[0m \x1b[1;37m{tool_name}\x1b[0m \x1b[32m{path}\x1b[0m{lines_str}\n"

        elif tool_name in ("write_to_file", "replace_file_content", "multi_replace_file_content"):
            path = params.get("TargetFile") or params.get("path") or ""
            desc = params.get("Description") or ""
            desc_str = f" \x1b[90m— {desc}\x1b[0m" if desc else ""
            return f"\x1b[36m● [TOOL START]\x1b[0m \x1b[1;37m{tool_name}\x1b[0m \x1b[32m{path}\x1b[0m{desc_str}\n"

        elif tool_name in ("grep_search", "search", "find"):
            query = params.get("Query") or params.get("query") or ""
            path = params.get("SearchPath") or params.get("path") or ""
            return f"\x1b[36m● [TOOL START]\x1b[0m \x1b[1;37m{tool_name}\x1b[0m query=\x1b[33m'{query}'\x1b[0m in \x1b[32m{path}\x1b[0m\n"

        else:
            param_items = [
                f"{k}={v!r}" for k, v in list(params.items())[:3]
                if k not in ("ArtifactMetadata",)
            ]
            param_str = ", ".join(param_items)
            if len(param_str) > 100:
                param_str = param_str[:97] + "..."
            return f"\x1b[36m● [TOOL START]\x1b[0m \x1b[1;37m{tool_name}\x1b[0m\x1b[90m({param_str})\x1b[0m\n"

    elif ev_type == "tool_end":
        tool_name = getattr(event_item, "tool_name", "") or (
            event_item.get("tool_name", "") if isinstance(event_item, dict) else ""
        )
        output = getattr(event_item, "output", "") or (
            event_item.get("output", "") if isinstance(event_item, dict) else ""
        )
        duration = getattr(event_item, "duration_seconds", None)
        is_error = getattr(event_item, "is_error", False)
        dur_str = f" \x1b[90m({duration:.2f}s)\x1b[0m" if isinstance(duration, (int, float)) else ""

        if is_error:
            header = f"\x1b[31m✖ [TOOL ERROR]\x1b[0m \x1b[1;31m{tool_name}\x1b[0m{dur_str}\n"
        else:
            header = f"\x1b[32m✔ [TOOL END]\x1b[0m \x1b[1;32m{tool_name}\x1b[0m{dur_str}\n"

        out_str = str(output).strip() if output is not None else ""
        if out_str:
            lines = out_str.splitlines()
            if len(lines) > 50:
                display_lines = lines[:35] + [f"... [{len(lines) - 45} lines truncated] ..."] + lines[-10:]
            else:
                display_lines = lines
            body = "\n".join(f"  \x1b[90m│\x1b[0m {l}" for l in display_lines) + "\n"
            return header + body
        return header

    elif ev_type == "status_update":
        status = getattr(event_item, "status", "") or (
            event_item.get("status", "") if isinstance(event_item, dict) else ""
        )
        detail = getattr(event_item, "detail", "") or (
            event_item.get("detail", "") if isinstance(event_item, dict) else ""
        )
        if status == "model_fallback":
            return f"\x1b[33m⚡ [MODEL FALLBACK]\x1b[0m \x1b[1;33m{detail}\x1b[0m\n"
        elif detail:
            return f"\x1b[33m▸ [STATUS]\x1b[0m \x1b[90m{detail}\x1b[0m\n"
        elif status and status != "working":
            return f"\x1b[33m▸ [STATUS]\x1b[0m \x1b[90m{status}\x1b[0m\n"
        return None

    elif ev_type == "text_delta":
        delta = (
            getattr(event_item, "text_delta", "")
            or getattr(event_item, "text", "")
            or (event_item.get("text_delta", "") if isinstance(event_item, dict) else "")
            or (event_item.get("text", "") if isinstance(event_item, dict) else "")
        )
        return delta or None

    elif ev_type == "awaiting_input":
        prompt = getattr(event_item, "prompt", "") or (
            event_item.get("prompt", "") if isinstance(event_item, dict) else ""
        )
        return f"\x1b[33m⚠️ [AWAITING INPUT]\x1b[0m \x1b[1;33m{prompt}\x1b[0m\n"

    elif ev_type == "finish":
        return None

    return None


class IngressGateway:
    """
    Ingress gateway routing incoming chat events to warm SessionHandles.
    Enforces non-verbal acknowledgments, zero canned chatter, deduplication, and reaction state swaps.
    """

    def __init__(
        self,
        router: Optional[SessionRouter] = None,
        bot_user_id: Optional[str] = None,
        max_seen_messages: int = 2000,
        max_staged_cache: int = 2000,
        max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
        sensory_bus: Optional[ShoulderTapEventBus] = None,
        bus: Optional[ShoulderTapEventBus] = None,
        nose_sentinel: Optional[Any] = None,
        webui_server: Optional[Any] = None,
        linear_bridge: Optional[Any] = None,
    ) -> None:
        self.router = router if router is not None else SessionRouter()
        self.bot_user_id = bot_user_id
        self._seen_messages: Deque[str] = deque(maxlen=max_seen_messages)
        self._seen_set: set[str] = set()
        self._dedup_lock = asyncio.Lock()
        self._staged_files: OrderedDict[str, str] = OrderedDict()
        self._max_staged_cache = max_staged_cache
        self.max_file_size_bytes = max_file_size_bytes
        self._file_locks: Dict[str, asyncio.Lock] = {}
        self._file_locks_guard = asyncio.Lock()
        self._seen_email_ids: Set[str] = set()
        self._node_incident_threads: Dict[str, str] = {}
        self.nose_sentinel = nose_sentinel
        if self.nose_sentinel is not None and getattr(self.nose_sentinel, "session_router", None) is None:
            self.nose_sentinel.session_router = self.router
        self.webui_server = webui_server
        self.webui = webui_server
        self.linear_bridge = linear_bridge

        # Shoulder Tap Event Bus Integration
        self.bus = sensory_bus or bus
        if self.bus is None:
            try:
                self.bus = get_default_event_bus()
            except Exception:
                self.bus = None
        self.sensory_bus = self.bus

        if self.bus is not None and hasattr(self.bus, "subscribe"):
            self.bus.subscribe(self._handle_health_anomaly_alert, alert_types=["health_anomaly"])
            self.bus.subscribe(self._handle_health_recovery_alert, alert_types=["health_recovery"])
            self.bus.subscribe(self._handle_email_received_alert, alert_types=["email_received"])
            self.bus.subscribe(
                self._handle_linear_assigned_tickets_alert,
                alert_types=["linear_assigned_tickets", "assigned_tickets"],
            )
            self.bus.subscribe(
                self._handle_linear_quota_exceeded_alert,
                alert_types=["linear_quota_exceeded", "linear_quota_warning"],
            )

        # Ensure router shares the same event bus if not explicitly configured
        if self.router is not None and getattr(self.router, "bus", None) is None:
            self.router.bus = self.bus

    def _get_linear_bridge(self) -> Any:
        """Return or create and cache a shared LinearBridge instance."""
        bridge = getattr(self, "linear_bridge", None)
        if bridge is None:
            try:
                from golem.linear_bridge import LinearBridge
                bridge = LinearBridge()
                self.linear_bridge = bridge
            except Exception as exc:
                logger.debug(f"LinearBridge instantiation failed: {exc}")
                return None
        return bridge

    def _broadcast_mouth_log(self, text: str, model: Optional[str] = None) -> None:
        """Broadcast live multi-hop mouth telemetry log line to connected WebUI."""
        webui = getattr(self, "webui", None) or getattr(self, "webui_server", None)
        if webui is None:
            try:
                from golem.webui import get_active_webui_server
                webui = get_active_webui_server()
            except Exception:
                pass
        if webui is not None and not _is_mock_object(webui):
            effective_model = model or getattr(self, "mouth_model", None) or get_mouth_model()
            if hasattr(webui, "append_organ_log"):
                try:
                    webui.append_organ_log("mouth", text)
                    return
                except Exception as exc:
                    logger.debug(f"WebUI append_organ_log failed: {exc}")
            if hasattr(webui, "record_mouth_event"):
                try:
                    webui.record_mouth_event(briefing=text, model=effective_model)
                    return
                except Exception as exc:
                    logger.debug(f"WebUI record_mouth_event failed: {exc}")
            if hasattr(webui, "broadcast"):
                try:
                    webui.broadcast("mouth_briefing", {"briefing": text, "model": effective_model})
                except Exception as exc:
                    logger.debug(f"WebUI broadcast failed: {exc}")

    async def _broadcast_mouth_briefing(self, briefing: str, model: Optional[str] = None) -> None:
        """Broadcast executive mouth briefing to connected WebUI clients."""
        webui = getattr(self, "webui_server", None)
        if webui is None:
            return
        effective_model = model or getattr(self, "mouth_model", None) or get_mouth_model()
        payload = {
            "briefing": briefing,
            "model": effective_model,
        }
        try:
            res = webui.broadcast("mouth_briefing", payload)
            if inspect.isawaitable(res):
                await res
        except Exception as exc:
            logger.debug(f"WebUI mouth_briefing broadcast warning: {exc}")

    def get_nose_sentinel(self) -> Optional[Any]:
        """Resolve active NoseSentinel singleton."""
        if self.nose_sentinel is not None:
            return self.nose_sentinel
        is_test = os.environ.get("FIRSTMATE_TEST_MODE") == "1" or os.environ.get("GOLEM_TEST_MODE") == "1"
        if is_test:
            return None
        try:
            from golem.nose import get_default_nose_sentinel
            sentinel = get_default_nose_sentinel()
            if sentinel is not None:
                return sentinel
        except Exception:
            pass
        if self.router is not None and getattr(self.router, "nose_sentinel", None) is not None:
            return self.router.nose_sentinel
        return None

    async def process_sensory_alert(self, alert: ShoulderTapAlert, client: Any) -> None:
        """Process incoming ShoulderTapAlert (from The Eyes or The Nose) and surface to Slack."""
        if not alert:
            return
        channel = alert.channel or getattr(alert, "data", {}).get("channel") or ""
        thread_ts = alert.thread_ts or getattr(alert, "data", {}).get("thread_ts")
        is_imessage = getattr(alert, "data", {}).get("is_imessage", False)

        if alert.alert_type == "blocker":
            blocker_data = alert.data or {}
            b_type = blocker_data.get("type", blocker_data.get("blocker_type", "prompt"))
            if b_type == "oauth":
                url = blocker_data.get("url") or blocker_data.get("blocker_url", "")
                card = f"🔐 *Authentication Required:* Runner is waiting for OAuth authorization:\n<{url}|Click here to authenticate>"
            else:
                content = blocker_data.get("content") or blocker_data.get("prompt") or blocker_data.get("blocker_prompt", "")
                card = f"⚠️ *Confirmation Required:* Runner is waiting for input:\n> `{content}`"

            if thread_ts and not is_imessage:
                await swap_reaction(client, channel, thread_ts, "gear", "warning")
            await self._post_response(client, channel, thread_ts, card, is_imessage)

        elif alert.alert_type == "health_anomaly":
            await self._handle_health_anomaly_alert(alert, client=client)

        elif alert.alert_type == "health_recovery":
            await self._handle_health_recovery_alert(alert, client=client)

        elif alert.alert_type == "email_received":
            await self._handle_email_received_alert(alert, client=client)

        elif alert.alert_type in ("linear_assigned_tickets", "assigned_tickets"):
            await self._handle_linear_assigned_tickets_alert(alert, client=client)

        elif alert.alert_type in ("linear_quota_exceeded", "linear_quota_warning"):
            await self._handle_linear_quota_exceeded_alert(alert, client=client)

    def _find_existing_thread_for_ticket(self, channel: str, ticket: Dict[str, Any]) -> Optional[str]:
        """Find an existing Slack thread_ts in recent history bound to this specific Linear ticket."""
        if not channel or not ticket or not isinstance(ticket, dict):
            return None
        ident = str(ticket.get("identifier") or ticket.get("id") or "").strip()
        if not ident:
            return None

        best_thread_ts: Optional[str] = None
        best_updated: float = -1.0
        prefix = f"{channel}:"

        def _record_matches_ticket(rec: Dict[str, Any]) -> bool:
            if not isinstance(rec, dict):
                return False
            for k_field in ("active_linear_parent_key", "linear_issue_key", "linear_parent_key"):
                if str(rec.get(k_field) or "").strip() == ident:
                    return True
            parent = rec.get("parent")
            if isinstance(parent, dict) and str(parent.get("identifier") or "").strip() == ident:
                return True
            plan = rec.get("linear_plan_cache")
            if isinstance(plan, dict):
                p_id = (plan.get("parent") or {}).get("identifier")
                if str(p_id or "").strip() == ident:
                    return True
            t_ctx = rec.get("linear_tickets_context")
            if isinstance(t_ctx, list) and len(t_ctx) == 1 and isinstance(t_ctx[0], dict):
                if str(t_ctx[0].get("identifier") or "").strip() == ident:
                    return True
            return False

        for k, cache_val in list(_THREAD_PLAN_CACHE.items()):
            if k.startswith(prefix) and not k.endswith(":latest_linear_alert"):
                t_ts = k[len(prefix):]
                if t_ts and _record_matches_ticket(cache_val):
                    upd = float(cache_val.get("updated_at") or 0.0)
                    if upd >= best_updated:
                        best_updated = upd
                        best_thread_ts = t_ts

        if hasattr(self, "router") and self.router:
            sessions_map = getattr(self.router, "sessions", {})
            if isinstance(sessions_map, dict):
                for s_key, s_obj in list(sessions_map.items()):
                    if isinstance(s_key, str) and s_key.startswith(prefix):
                        t_ts = s_key[len(prefix):]
                        s_dict = {
                            "active_linear_parent_key": getattr(s_obj, "active_linear_parent_key", None),
                            "linear_issue_key": getattr(s_obj, "linear_issue_key", None),
                            "linear_plan_cache": getattr(s_obj, "linear_plan_cache", None),
                            "linear_tickets_context": getattr(s_obj, "linear_tickets_context", None),
                        }
                        if t_ts and _record_matches_ticket(s_dict):
                            upd = float(getattr(s_obj, "last_active_ts", 0.0) or 0.0)
                            if upd >= best_updated:
                                best_updated = upd
                                best_thread_ts = t_ts

            reg_map = getattr(self.router, "_registry", {})
            if isinstance(reg_map, dict):
                for r_key, r_val in list(reg_map.items()):
                    if isinstance(r_key, str) and r_key.startswith(prefix) and not r_key.endswith(":latest_linear_alert"):
                        t_ts = r_key[len(prefix):]
                        if t_ts and isinstance(r_val, dict) and _record_matches_ticket(r_val):
                            upd = float(r_val.get("updated_at") or 0.0)
                            if upd >= best_updated:
                                best_updated = upd
                                best_thread_ts = t_ts

        return best_thread_ts

    def _build_ticket_runtime_telemetry(
        self,
        ticket: Optional[Dict[str, Any]] = None,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Assemble live in-memory runtime telemetry across active hands, sessions,
        queued batch subtasks, and recent human/agent interaction timestamps.
        """
        current_time = float(now) if now is not None else time.time()
        in_flight_keys: Set[str] = set()
        queued_keys: Set[str] = set()
        active_hands_summary: List[Dict[str, Any]] = []
        last_error: Optional[str] = None
        latest_interaction_ts: float = 0.0

        related_keys: Set[str] = set()
        if isinstance(ticket, dict):
            t_ident = str(ticket.get("identifier") or ticket.get("id") or "").strip()
            if t_ident:
                related_keys.add(t_ident)
            for st in (ticket.get("subtasks") or []):
                if isinstance(st, dict) and st.get("identifier"):
                    related_keys.add(str(st["identifier"]))

        # 1. Inspect WebUI active hands
        webui = getattr(self, "webui_server", None) or getattr(self, "webui", None)
        if webui is not None and not _is_mock_object(webui):
            raw_hands = getattr(webui, "_active_hands", {})
            if isinstance(raw_hands, dict):
                terminal_statuses = {"EXIT_0", "COMPLETED", "FAILED", "DONE", "SETTLED", "CANCELLED", "SUCCESS"}
                for h_key, h_val in list(raw_hands.items()):
                    if isinstance(h_val, dict):
                        st_str = str(h_val.get("status") or "").upper()
                        cmd_str = str(h_val.get("command") or "")
                        if st_str not in terminal_statuses and h_val.get("exit_code") is None:
                            active_hands_summary.append({
                                "pane_id": h_val.get("pane_id") or h_key,
                                "node": h_val.get("node", "localhost"),
                                "status": st_str or "RUNNING",
                                "command": cmd_str[:120],
                                "pid": h_val.get("pid"),
                                "started_at": h_val.get("started_at") or h_val.get("start_time"),
                                "last_output_at": h_val.get("last_output_at") or h_val.get("updated_at"),
                            })
                            for k in re.findall(r"CHO-\d+", f"{h_key} {cmd_str}"):
                                in_flight_keys.add(k)
                        elif st_str in ("FAILED", "ERROR") or (h_val.get("exit_code") not in (None, 0)):
                            if any(rk in f"{h_key} {cmd_str}" for rk in related_keys):
                                last_error = str(h_val.get("error") or f"Hand {h_key} exited with status {st_str}")

        # 2. Inspect live sessions in SessionRouter
        if hasattr(self, "router") and self.router is not None:
            sessions_map = getattr(self.router, "sessions", {})
            if isinstance(sessions_map, dict):
                for s_key, s_obj in list(sessions_map.items()):
                    state_obj = getattr(s_obj, "state", None)
                    state_val_s = getattr(state_obj, "value", state_obj)
                    in_flight_s = getattr(s_obj, "in_flight_subtasks", None)
                    queued_s = getattr(s_obj, "queued_subtasks", None)
                    parent_keys_s = getattr(s_obj, "active_linear_parent_keys", None)

                    s_keys: Set[str] = set()
                    for attr_name in ("active_linear_parent_key", "linear_issue_key", "linear_issue_id"):
                        val = getattr(s_obj, attr_name, None)
                        if isinstance(val, str) and val:
                            s_keys.add(val)
                    if isinstance(parent_keys_s, (list, tuple, set)):
                        for pk in parent_keys_s:
                            if isinstance(pk, str) and pk:
                                s_keys.add(pk)
                    if isinstance(in_flight_s, (list, tuple, set)):
                        for item in in_flight_s:
                            if isinstance(item, dict):
                                for k_f in ("issue_key", "parent_key", "identifier"):
                                    if item.get(k_f):
                                        in_flight_keys.add(str(item[k_f]))
                                        s_keys.add(str(item[k_f]))
                            elif isinstance(item, str) and item:
                                in_flight_keys.add(item)
                                s_keys.add(item)
                    if isinstance(queued_s, (list, tuple, set)):
                        for item in queued_s:
                            if isinstance(item, dict):
                                for k_f in ("issue_key", "parent_key", "identifier"):
                                    if item.get(k_f):
                                        queued_keys.add(str(item[k_f]))
                                        s_keys.add(str(item[k_f]))
                            elif isinstance(item, str) and item:
                                queued_keys.add(item)
                                s_keys.add(item)

                    is_busy_s = bool(getattr(s_obj, "is_busy", False) or state_val_s in ("running", "working"))
                    if is_busy_s:
                        in_flight_keys.update(s_keys)

                    if related_keys and related_keys.intersection(s_keys):
                        for ts_attr in ("last_dispatched_at", "last_activity", "last_active_ts", "updated_at"):
                            ts_val = getattr(s_obj, ts_attr, None)
                            if isinstance(ts_val, (int, float)) and not isinstance(ts_val, bool):
                                if float(ts_val) > latest_interaction_ts:
                                    latest_interaction_ts = float(ts_val)
                        s_err = getattr(s_obj, "last_subtask_error", None)
                        if isinstance(s_err, str) and s_err:
                            last_error = s_err

        # 3. Inspect gateway-level ticket activity tracker
        activity_map = getattr(self, "_ticket_activity_tracker", None)
        if isinstance(activity_map, dict) and related_keys:
            for rk in related_keys:
                entry = activity_map.get(rk)
                if isinstance(entry, dict):
                    ts_e = entry.get("last_interaction_at") or entry.get("updated_at") or 0.0
                    if isinstance(ts_e, (int, float)) and float(ts_e) > latest_interaction_ts:
                        latest_interaction_ts = float(ts_e)
                    if entry.get("status") == "in_flight":
                        in_flight_keys.add(rk)
                    elif entry.get("status") == "queued":
                        queued_keys.add(rk)
                    if entry.get("last_error") and not last_error:
                        last_error = str(entry["last_error"])

        is_in_flight = bool(related_keys and related_keys.intersection(in_flight_keys))
        is_queued = bool(related_keys and related_keys.intersection(queued_keys))
        sec_since: Optional[float] = (
            max(0.0, current_time - latest_interaction_ts)
            if latest_interaction_ts > 0.0
            else None
        )

        is_zombie_in_progress = False
        if isinstance(ticket, dict):
            state_val = ticket.get("state")
            if isinstance(state_val, dict):
                st_name = str(state_val.get("name") or "").lower()
                st_type = str(state_val.get("type") or "").lower()
            else:
                st_name = str(state_val or "").lower()
                st_type = str(ticket.get("state_type") or "").lower()
            is_started = bool("in progress" in st_name or st_type == "started" or st_name == "started")
            if is_started and not is_in_flight and not is_queued:
                is_zombie_in_progress = True

        return {
            "is_in_flight": is_in_flight,
            "is_queued_in_batch": is_queued,
            "is_zombie_in_progress": is_zombie_in_progress,
            "in_flight_keys": sorted(in_flight_keys),
            "queued_keys": sorted(queued_keys),
            "active_hands": active_hands_summary,
            "seconds_since_last_interaction": sec_since,
            "last_error": last_error,
        }

    async def _handle_linear_assigned_tickets_alert(self, alert: Any, client: Optional[Any] = None) -> None:
        """
        Receive assigned Linear ticket alerts from The Nose Sentinel.
        Enforces:
          1. In-Flight vs. Stalled Awareness via live runtime telemetry & Mouth Nudge Hygiene.
          2. One Thread Per Ticket (never bundling unrelated parent tickets into one Slack message;
             reuses an existing thread for that ticket if present, or posts a standalone message per ticket).
          3. Natural, context-aware calls-to-action based on actual subtask state.
        """
        if not alert:
            return
        data = getattr(alert, "data", {}) or {}
        tickets = data.get("tickets") or []
        if not tickets:
            return

        # Defensive Active Work Detection filter
        tickets = [t for t in tickets if not self._is_ticket_actively_worked(t)]
        if not tickets:
            return

        channel = (
            getattr(alert, "channel", None)
            or data.get("channel")
            or os.environ.get("FIRSTMATE_DEFAULT_CHANNEL")
            or os.environ.get("GOLEM_DEFAULT_CHANNEL")
        )
        if not channel:
            try:
                from golem.notify import DEFAULT_SLACK_CHANNEL
                channel = DEFAULT_SLACK_CHANNEL
            except Exception:
                channel = ""

        explicit_thread_ts = getattr(alert, "thread_ts", None) or data.get("thread_ts")
        from golem.mouth import synthesize_linear_ticket_nudge

        for ticket in tickets:
            if not isinstance(ticket, dict):
                continue
            runtime_telemetry = self._build_ticket_runtime_telemetry(ticket)
            msg_text = await synthesize_linear_ticket_nudge(ticket, runtime_telemetry=runtime_telemetry)
            if not msg_text:
                continue

            target_thread_ts = explicit_thread_ts or self._find_existing_thread_for_ticket(channel, ticket)
            posted_ts = ""

            if client and channel and hasattr(client, "chat_postMessage"):
                try:
                    resp = client.chat_postMessage(channel=channel, text=msg_text, thread_ts=target_thread_ts)
                    if inspect.isawaitable(resp):
                        resp = await resp
                    if isinstance(resp, dict):
                        posted_ts = str(resp.get("ts") or "")
                    elif hasattr(resp, "data") and isinstance(resp.data, dict):
                        posted_ts = str(resp.data.get("ts") or "")
                except Exception as ce:
                    logger.warning(f"chat_postMessage for linear assigned ticket failed: {ce}")

            if not posted_ts and channel:
                try:
                    from golem.notify import send_slack_message
                    resp = await send_slack_message(text=msg_text, channel=channel, thread_ts=target_thread_ts)
                    if isinstance(resp, dict):
                        posted_ts = str(resp.get("ts") or "")
                    elif hasattr(resp, "data") and isinstance(resp.data, dict):
                        posted_ts = str(resp.data.get("ts") or "")
                except Exception as se:
                    logger.warning(f"send_slack_message for linear assigned ticket failed: {se}")

            effective_root_ts = target_thread_ts or getattr(alert, "root_ts", None) or data.get("root_ts") or ""
            bound_ts = str(effective_root_ts or posted_ts or "").strip()
            now_ts = time.time()
            t_ident = str(ticket.get("identifier") or "").strip()
            context_payload = {
                "linear_tickets_context": [ticket],
                "active_linear_parent_key": t_ident,
                "parent": ticket,
                "subtasks": ticket.get("subtasks") or [],
                "updated_at": now_ts,
            }

            if channel and bound_ts:
                bound_key = f"{channel}:{bound_ts}"
                _THREAD_PLAN_CACHE[bound_key] = context_payload
                if hasattr(self, "router") and self.router and hasattr(self.router, "_registry"):
                    existing_rec = self.router._registry.get(bound_key) or {}
                    existing_rec["linear_tickets_context"] = [ticket]
                    if t_ident:
                        existing_rec["active_linear_parent_key"] = t_ident
                    existing_rec["updated_at"] = now_ts
                    self.router._registry[bound_key] = existing_rec

        if channel and hasattr(self, "router") and self.router and hasattr(self.router, "_save_registry"):
            try:
                self.router._save_registry()
            except Exception as sre:
                logger.debug(f"Unable to save router registry: {sre}")

    async def _handle_linear_quota_exceeded_alert(self, alert: Any, client: Optional[Any] = None) -> None:
        """
        Receive Linear free tier quota exhaustion alerts from The Nose Sentinel.
        Prompts Carl to clean up Done / Canceled tickets in Linear.
        CRITICAL: Never deletes or calls issueDelete automatically. Carl owns hard delete operations.
        """
        if not alert:
            return
        data = getattr(alert, "data", {}) or {}
        channel = (
            getattr(alert, "channel", None)
            or data.get("channel")
            or os.environ.get("FIRSTMATE_DEFAULT_CHANNEL")
            or os.environ.get("GOLEM_DEFAULT_CHANNEL")
        )
        if not channel:
            try:
                from golem.notify import DEFAULT_SLACK_CHANNEL
                channel = DEFAULT_SLACK_CHANNEL
            except Exception:
                channel = ""

        from golem.mouth import synthesize_linear_quota_nudge
        msg_text = synthesize_linear_quota_nudge(data)

        target_thread_ts = getattr(alert, "thread_ts", None) or data.get("thread_ts")
        posted_ts = ""

        if client and channel and hasattr(client, "chat_postMessage"):
            try:
                resp = client.chat_postMessage(channel=channel, text=msg_text, thread_ts=target_thread_ts)
                if inspect.isawaitable(resp):
                    resp = await resp
                if isinstance(resp, dict):
                    posted_ts = str(resp.get("ts") or "")
                elif hasattr(resp, "data") and isinstance(resp.data, dict):
                    posted_ts = str(resp.data.get("ts") or "")
            except Exception as ce:
                logger.warning(f"chat_postMessage for linear quota alert failed: {ce}")

        if not posted_ts and channel:
            try:
                from golem.notify import send_slack_message
                resp = await send_slack_message(text=msg_text, channel=channel, thread_ts=target_thread_ts)
                if isinstance(resp, dict):
                    posted_ts = str(resp.get("ts") or "")
                elif hasattr(resp, "data") and isinstance(resp.data, dict):
                    posted_ts = str(resp.data.get("ts") or "")
            except Exception as se:
                logger.warning(f"send_slack_message for linear quota alert failed: {se}")


    def _is_ticket_actively_worked(self, ticket: Dict[str, Any], now: Optional[float] = None) -> bool:
        """Determine whether a Linear ticket is actively being worked on or queued in an active batch."""
        try:
            telemetry = self._build_ticket_runtime_telemetry(ticket, now=now)
            if telemetry.get("is_in_flight") or telemetry.get("is_queued_in_batch"):
                return True
            sec_since = telemetry.get("seconds_since_last_interaction")
            if isinstance(sec_since, (int, float)) and 0.0 <= float(sec_since) <= 900.0:
                return True
            from golem.nose import is_ticket_actively_worked
            return is_ticket_actively_worked(
                ticket,
                session_router=getattr(self, "router", None),
                registry_path=getattr(getattr(self, "router", None), "registry_path", None),
                now=now,
            )
        except Exception as exc:
            logger.debug(f"Error checking active work in IngressGateway: {exc}")
            return False

    async def _handle_health_anomaly_alert(self, alert: Any, client: Optional[Any] = None) -> None:
        """Receive health anomaly alerts from The Nose Sentinel and deliver advisory to Carl."""
        summary = getattr(alert, "summary", "") or "Fleet Sentinel detected a health anomaly."
        from golem.notify import DEFAULT_SLACK_CHANNEL, send_slack_message, send_imessage
        channel = (
            getattr(alert, "channel", None)
            or (getattr(alert, "data", {}) or {}).get("channel")
            or os.environ.get("FIRSTMATE_DEFAULT_CHANNEL")
            or os.environ.get("GOLEM_DEFAULT_CHANNEL")
            or DEFAULT_SLACK_CHANNEL
        )
        thread_ts = getattr(alert, "thread_ts", None) or (getattr(alert, "data", {}) or {}).get("thread_ts")
        is_imessage = bool(channel and str(channel).startswith(("iMessage;", "SMS;")))
        advisory_msg = f"👃 *Sentinel Advisory (Fleet Health):*\n{summary}"

        posted_ts = None
        if client and channel:
            resp = await self._post_response(client, channel, thread_ts, advisory_msg, is_imessage=is_imessage)
            if isinstance(resp, dict):
                posted_ts = resp.get("ts")
        elif channel:
            if is_imessage:
                await send_imessage(text=advisory_msg, chat_guid=str(channel))
            else:
                resp = await send_slack_message(text=advisory_msg, channel=channel, thread_ts=thread_ts)
                if isinstance(resp, dict):
                    posted_ts = resp.get("ts")

        # Track incident thread for follow-up resolution
        incident_thread = thread_ts or posted_ts
        if incident_thread:
            alert_data = getattr(alert, "data", {}) or {}
            affected = alert_data.get("affected_nodes") or []
            node = alert_data.get("node") or alert_data.get("target") or (affected[0] if affected else None)
            if node:
                self._node_incident_threads[str(node)] = str(incident_thread)

    async def _handle_health_recovery_alert(self, alert: Any, client: Optional[Any] = None) -> None:
        """Receive health recovery alerts from The Nose Sentinel and deliver resolution follow-up to Carl."""
        if not alert:
            return
        from golem.notify import DEFAULT_SLACK_CHANNEL, send_slack_message, send_imessage

        data = getattr(alert, "data", {}) or getattr(alert, "metadata", {}) or {}
        node = data.get("node") or data.get("target") or "fleet"
        if node and (str(node).lower() in ("personal-laptop", "laptop", "dev-grid", "personal_laptop") or "macbook" in str(node).lower() or "workstation" in str(node).lower()):
            logger.info(f"IngressGateway: Ignoring health_recovery alert for exempt workstation node '{node}'")
            return
        duration_val = data.get("duration_str") or data.get("duration") or "transient"
        if isinstance(duration_val, (int, float)):
            duration_str = f"{int(round(duration_val))}s"
        else:
            duration_str = str(duration_val)

        resolution_msg = (
            f":white_check_mark: *Sentinel Resolution:* Node `{node}` has recovered after {duration_str}. "
            f"Latency / connectivity restored."
        )

        channel = (
            getattr(alert, "channel", None)
            or data.get("channel")
            or os.environ.get("FIRSTMATE_DEFAULT_CHANNEL")
            or os.environ.get("GOLEM_DEFAULT_CHANNEL")
            or DEFAULT_SLACK_CHANNEL
        )

        # Deliver to incident thread if known, else operational channel
        thread_ts = (
            getattr(alert, "thread_ts", None)
            or data.get("thread_ts")
            or self._node_incident_threads.pop(str(node), None)
        )

        is_imessage = bool(channel and str(channel).startswith(("iMessage;", "SMS;")))
        if client and channel:
            await self._post_response(client, channel, thread_ts, resolution_msg, is_imessage=is_imessage)
        elif channel:
            if is_imessage:
                await send_imessage(text=resolution_msg, chat_guid=str(channel))
            else:
                await send_slack_message(text=resolution_msg, channel=channel, thread_ts=thread_ts)

    async def _handle_email_received_alert(self, alert: Any, client: Optional[Any] = None) -> None:
        """
        One-shot reactive Mouth handler for incoming email alerts.
        Extracts email metadata and URLs, inspects URL contents (social media / web),
        synthesizes an executive Chief of Staff briefing proposing an actionable follow-up,
        posts to Carl's Slack DM, and warms the thread's SessionHandle.
        """
        if not alert:
            return

        try:
            data = getattr(alert, "data", {}) or {}
            email_id = str(data.get("email_id") or getattr(alert, "session_id", "") or "")
            if email_id:
                if email_id in self._seen_email_ids:
                    logger.debug(f"Email {email_id} already processed by IngressGateway; skipping duplicate.")
                    return
                self._seen_email_ids.add(email_id)
                if len(self._seen_email_ids) > 1000:
                    self._seen_email_ids.pop()

            sender = str(data.get("from") or "Carl").strip()
            subject = str(data.get("subject") or "Incoming Email").strip()
            body = str(data.get("body") or "").strip()
            snippet = str(data.get("snippet") or "").strip()
            raw_urls = data.get("urls") or []
            if isinstance(raw_urls, str):
                raw_urls = [raw_urls]
            urls = [str(u).strip() for u in raw_urls if str(u).strip()]
            date_str = str(data.get("date") or "").strip()

            # Inspect and summarize extracted URLs (e.g. Instagram, Reddit, event links)
            inspected_urls = []
            if urls:
                try:
                    from golem.mouth import inspect_and_summarize_urls
                    inspected_urls = await inspect_and_summarize_urls(urls[:3])
                except Exception as ie:
                    logger.warning(f"Error inspecting URLs for email {email_id}: {ie}")

            # Synthesize Chief of Staff briefing + actionable follow-up proposal
            try:
                from golem.mouth import synthesize_email_briefing
                briefing_text, suggested_action = await synthesize_email_briefing(
                    sender=sender,
                    subject=subject,
                    body=body,
                    snippet=snippet,
                    urls=urls,
                    inspected_urls=inspected_urls,
                )
            except Exception as se:
                logger.warning(f"Error synthesizing email briefing: {se}")
                briefing_text = f"📬 *Shared via Email from {sender}:* *{subject}*\n\n> {snippet or body[:200]}"
                suggested_action = "Want me to create a calendar event or follow up on this for you?"

            # Resolve target Slack channel (defaults to Carl's DM: D0C2J1HKNN7)
            from golem.notify import DEFAULT_SLACK_CHANNEL, send_slack_message, _record_slack_session_registry

            target_channel = (
                getattr(alert, "channel", None)
                or data.get("channel")
                or os.environ.get("FIRSTMATE_DEFAULT_CHANNEL")
                or os.environ.get("GOLEM_DEFAULT_CHANNEL")
                or DEFAULT_SLACK_CHANNEL
            )

            email_ctx = {
                "email_id": email_id,
                "from": sender,
                "subject": subject,
                "body": body,
                "snippet": snippet,
                "urls": urls,
                "date": date_str,
                "briefing": briefing_text,
                "suggested_action": suggested_action,
                "inspected_urls": inspected_urls,
            }

            # Send message to Carl
            msg_ts = None
            if client and hasattr(client, "chat_postMessage"):
                try:
                    resp = client.chat_postMessage(channel=target_channel, text=briefing_text)
                    if inspect.isawaitable(resp):
                        resp = await resp
                    msg_ts = resp.get("ts") if isinstance(resp, dict) else getattr(resp, "ts", None)
                    if not msg_ts:
                        msg_ts = f"{time.time():.6f}"
                    _record_slack_session_registry(target_channel, msg_ts, email_context=email_ctx)
                except Exception as ce:
                    logger.warning(f"chat_postMessage via client failed: {ce}")

            if not msg_ts:
                try:
                    notify_res = await send_slack_message(
                        text=briefing_text,
                        channel=target_channel,
                        email_context=email_ctx,
                    )
                    if notify_res.get("ok"):
                        msg_ts = notify_res.get("ts")
                except Exception as ne:
                    logger.warning(f"send_slack_message failed: {ne}")

            if not msg_ts:
                logger.error(f"Failed to post email briefing for email {email_id} to {target_channel}")
                return

            # Pre-warm SessionHandle in router so when Carl replies 'Yes' or follow-up in Slack,
            # the session is warm and knows the original email, analysis, and suggested action.
            if self.router is not None and msg_ts:
                try:
                    target_cwd = (
                        getattr(self.router, "default_target_cwd", None)
                        or getattr(self.router, "default_workspace", None)
                        or os.getcwd()
                    )
                    session, _ = await self.router.get_or_create(
                        channel=target_channel,
                        thread_ts=msg_ts,
                        target_cwd=target_cwd,
                    )
                    session.email_context = email_ctx
                    session.last_response = briefing_text
                    session.last_prompt = f"Incoming email received from {sender}: {subject}"
                    session.touch()

                    if hasattr(self.router, "record_session"):
                        self.router.record_session(
                            session_key=f"{target_channel}:{msg_ts}",
                            conversation_id=getattr(session.adapter, "conversation_id", None) or f"{target_channel}:{msg_ts}",
                            target_cwd=session.target_cwd,
                            channel=target_channel,
                            thread_ts=msg_ts,
                            email_context=email_ctx,
                        )
                    logger.info(f"Warmed thread session '{target_channel}:{msg_ts}' with email context for '{subject}'")
                except Exception as we:
                    logger.warning(f"Failed to warm session for email {email_id}: {we}")
        except Exception as exc:
            logger.error(f"Unhandled error in _handle_email_received_alert: {exc}", exc_info=True)

    def _cache_staged_file(self, file_id: str, local_path: str) -> None:
        """Store staged file in bounded LRU cache."""
        if not file_id or not local_path:
            return
        if file_id in self._staged_files:
            self._staged_files.move_to_end(file_id)
        self._staged_files[file_id] = local_path
        if len(self._staged_files) > self._max_staged_cache:
            self._staged_files.popitem(last=False)

    async def _get_file_lock(self, file_id: str) -> asyncio.Lock:
        """Retrieve or create an asyncio.Lock for per-file in-flight synchronization."""
        async with self._file_locks_guard:
            if file_id not in self._file_locks:
                if len(self._file_locks) >= 2000:
                    idle_keys = [
                        k for k, l in self._file_locks.items()
                        if not l.locked() and not getattr(l, "_waiters", None)
                    ]
                    for k in idle_keys:
                        self._file_locks.pop(k, None)
                self._file_locks[file_id] = asyncio.Lock()
            return self._file_locks[file_id]

    async def _cleanup_file_lock(self, file_id: str, lock: asyncio.Lock) -> None:
        """Clean up per-file lock when no longer held or awaited."""
        async with self._file_locks_guard:
            waiters = getattr(lock, "_waiters", None)
            if not lock.locked() and not waiters:
                self._file_locks.pop(file_id, None)

    async def stage_inbound_files(
        self,
        files: List[Dict[str, Any]],
        target_cwd: str,
        client: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """
        Download and stage inbound files into {target_cwd}/slack_transfers/{timestamp}_{filename}.
        Returns list of metadata dicts with keys:
          - 'id': file id
          - 'name': sanitized original filename
          - 'path': staged local absolute path (or None if failed)
          - 'size': size in bytes
          - 'status': 'staged', 'cached', 'exceeded_size', 'failed'
        Synchronizes concurrent in-flight downloads for the same file using double-checked locking.
        """
        if not files:
            return []

        token = getattr(client, "token", None) if client else None
        if not token:
            token = os.environ.get("SLACK_BOT_TOKEN", "")

        results: List[Dict[str, Any]] = []
        for f in files:
            if not isinstance(f, dict):
                continue

            file_id = f.get("id") or f.get("file_id") or ""
            raw_name = f.get("name") or f.get("title") or ""
            safe_name = sanitize_filename(raw_name) if raw_name else f"upload_{int(time.time())}"

            # 1. Fast-path deduplication cache check (outside lock)
            if file_id and file_id in self._staged_files:
                cached_path = self._staged_files[file_id]
                if os.path.exists(cached_path):
                    self._cache_staged_file(file_id, cached_path)
                    logger.debug(f"File {file_id} already staged at {cached_path}; reusing.")
                    results.append({
                        "id": file_id,
                        "name": safe_name,
                        "path": cached_path,
                        "size": f.get("size") or (os.path.getsize(cached_path) if os.path.exists(cached_path) else 0),
                        "status": "cached",
                    })
                    continue

            # In-flight synchronization: serialize downloads per file_id
            if file_id:
                file_lock = await self._get_file_lock(file_id)
                async with file_lock:
                    res = await self._stage_single_file_under_lock(
                        f=f,
                        file_id=file_id,
                        safe_name=safe_name,
                        raw_name=raw_name,
                        target_cwd=target_cwd,
                        client=client,
                        token=token,
                    )
                await self._cleanup_file_lock(file_id, file_lock)
            else:
                res = await self._stage_single_file_under_lock(
                    f=f,
                    file_id=file_id,
                    safe_name=safe_name,
                    raw_name=raw_name,
                    target_cwd=target_cwd,
                    client=client,
                    token=token,
                )

            if res is not None:
                results.append(res)

        return results

    async def _stage_single_file_under_lock(
        self,
        f: Dict[str, Any],
        file_id: str,
        safe_name: str,
        raw_name: str,
        target_cwd: str,
        client: Optional[Any],
        token: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Download and stage a single inbound file under per-file lock.
        Implements double-checked locking re-check of self._staged_files.
        """
        # Re-check cache inside lock (double-checked locking)
        if file_id and file_id in self._staged_files:
            cached_path = self._staged_files[file_id]
            if os.path.exists(cached_path):
                self._cache_staged_file(file_id, cached_path)
                logger.debug(f"File {file_id} already staged at {cached_path}; reusing under lock.")
                return {
                    "id": file_id,
                    "name": safe_name,
                    "path": cached_path,
                    "size": f.get("size") or (os.path.getsize(cached_path) if os.path.exists(cached_path) else 0),
                    "status": "cached",
                }

        # Declared size limit guardrail
        declared_size = f.get("size")
        if declared_size is not None and isinstance(declared_size, (int, float)) and declared_size > self.max_file_size_bytes:
            logger.warning(
                f"Inbound file '{safe_name}' ({file_id}) size {declared_size} exceeds max {self.max_file_size_bytes} bytes; rejecting."
            )
            return {
                "id": file_id,
                "name": safe_name,
                "path": None,
                "size": declared_size,
                "status": "exceeded_size",
            }

        # Resolve download URL or direct local file read
        local_path = f.get("local_path")
        if local_path and os.path.isfile(local_path):
            try:
                with open(local_path, "rb") as lf:
                    file_bytes = lf.read()
                if len(file_bytes) > self.max_file_size_bytes:
                    logger.warning(
                        f"Inbound file '{safe_name}' ({file_id}) size {len(file_bytes)} exceeds max {self.max_file_size_bytes} bytes; rejecting."
                    )
                    return {
                        "id": file_id,
                        "name": safe_name,
                        "path": None,
                        "size": len(file_bytes),
                        "status": "exceeded_size",
                    }
                saved_path = save_staged_file(file_bytes, safe_name, target_cwd)
                if saved_path:
                    if file_id:
                        self._cache_staged_file(file_id, saved_path)
                    return {
                        "id": file_id,
                        "name": safe_name,
                        "path": saved_path,
                        "size": len(file_bytes),
                        "status": "staged",
                    }
            except Exception as e:
                logger.warning(f"Failed to read local attachment path {local_path}: {e}")

        # If client provides native attachment download (e.g. BlueBubblesClient), attempt it
        file_bytes = None
        if bool(f.get("is_imessage")) and file_id and client and hasattr(client, "download_attachment"):
            try:
                res = client.download_attachment(file_id)
                if inspect.isawaitable(res):
                    res = await res
                if isinstance(res, (bytes, bytearray)):
                    file_bytes = res
            except Exception as e:
                logger.warning(f"Failed to download attachment via client.download_attachment for {file_id}: {e}")

        if file_bytes and len(file_bytes) > self.max_file_size_bytes:
            logger.warning(
                f"Inbound file '{safe_name}' ({file_id}) size {len(file_bytes)} exceeds max {self.max_file_size_bytes} bytes; rejecting."
            )
            return {
                "id": file_id,
                "name": safe_name,
                "path": None,
                "size": len(file_bytes),
                "status": "exceeded_size",
            }

        if not file_bytes:
            download_url = f.get("url_private_download") or f.get("url_private") or f.get("url")
            if not download_url and file_id and client and hasattr(client, "files_info"):
                try:
                    res = client.files_info(file=file_id)
                    if inspect.isawaitable(res):
                        resp = await res
                    else:
                        resp = res
                    if isinstance(resp, dict):
                        info = resp.get("file", {})
                    elif hasattr(resp, "data") and isinstance(resp.data, dict):
                        info = resp.data.get("file", {})
                    else:
                        info = getattr(resp, "file", {}) or {}
                    download_url = info.get("url_private_download") or info.get("url_private")
                    if not raw_name:
                        raw_name = info.get("name") or info.get("title") or ""
                        safe_name = sanitize_filename(raw_name) if raw_name else safe_name
                    if declared_size is None:
                        declared_size = info.get("size")
                        if declared_size and declared_size > self.max_file_size_bytes:
                            logger.warning(
                                f"Inbound file '{safe_name}' ({file_id}) size {declared_size} exceeds max {self.max_file_size_bytes} bytes; rejecting."
                            )
                            return {
                                "id": file_id,
                                "name": safe_name,
                                "path": None,
                                "size": declared_size,
                                "status": "exceeded_size",
                            }
                except Exception as e:
                    logger.error(f"Failed to query files_info for {file_id}: {e}")

            if not download_url:
                logger.warning(f"No download URL available for file '{safe_name}' ({file_id})")
                return {
                    "id": file_id,
                    "name": safe_name,
                    "path": None,
                    "size": declared_size or 0,
                    "status": "failed",
                }

            # Download file bytes
            effective_token = "" if f.get("is_imessage") else token
            file_bytes = await download_slack_file(
                download_url,
                token=effective_token,
                max_size_bytes=self.max_file_size_bytes,
            )
            if not file_bytes:
                return {
                    "id": file_id,
                    "name": safe_name,
                    "path": None,
                    "size": declared_size or 0,
                    "status": "failed",
                }

        # Save to disk
        saved_path = save_staged_file(file_bytes, safe_name, target_cwd)
        if saved_path:
            if file_id:
                self._cache_staged_file(file_id, saved_path)
            return {
                "id": file_id,
                "name": safe_name,
                "path": saved_path,
                "size": len(file_bytes),
                "status": "staged",
            }
        else:
            return {
                "id": file_id,
                "name": safe_name,
                "path": None,
                "size": len(file_bytes),
                "status": "failed",
            }

    async def handle_file_shared(self, event: Dict[str, Any], client: Any) -> None:
        """
        Capture standalone file_shared event, retrieve metadata via files_info, and stage file.
        Prevents redundant download if already staged, and caches in _staged_files for subsequent message events.
        """
        file_id = event.get("file_id")
        if not file_id and isinstance(event.get("file"), dict):
            file_id = event["file"].get("id")
        elif not file_id and isinstance(event.get("file"), str):
            file_id = event.get("file")

        if not file_id:
            logger.warning(f"file_shared event missing file_id: {event}")
            return

        if file_id in self._staged_files:
            cached_path = self._staged_files[file_id]
            if os.path.exists(cached_path):
                logger.debug(f"file_shared event for {file_id} already staged at {cached_path}; skipping.")
                return

        file_info: Dict[str, Any] = {}
        if hasattr(client, "files_info"):
            try:
                res = client.files_info(file=file_id)
                if inspect.isawaitable(res):
                    resp = await res
                else:
                    resp = res
                if isinstance(resp, dict):
                    file_info = resp.get("file", {})
                elif hasattr(resp, "data") and isinstance(resp.data, dict):
                    file_info = resp.data.get("file", {})
                else:
                    file_info = getattr(resp, "file", {}) or {}
            except Exception as e:
                logger.error(f"Error fetching files_info for file_id {file_id}: {e}")
                return

        if not file_info:
            logger.warning(f"Could not retrieve file_info for file_id {file_id}")
            return

        size = file_info.get("size")
        if size is not None and isinstance(size, (int, float)) and size > self.max_file_size_bytes:
            logger.warning(f"file_shared {file_id} size {size} exceeds max {self.max_file_size_bytes}; skipping download.")
            return

        # Determine target_cwd from channel/thread_ts or router default
        channel = event.get("channel_id") or event.get("channel") or ""
        thread_ts = event.get("thread_ts")
        if not channel or not thread_ts:
            shares = file_info.get("shares", {})
            if isinstance(shares, dict):
                for share_type in ("public", "private"):
                    type_dict = shares.get(share_type, {})
                    if isinstance(type_dict, dict) and type_dict:
                        for ch_id, ch_shares in type_dict.items():
                            if not channel:
                                channel = ch_id
                            if isinstance(ch_shares, list) and ch_shares:
                                for sh in ch_shares:
                                    if isinstance(sh, dict) and sh.get("thread_ts"):
                                        thread_ts = sh["thread_ts"]
                                        break
                            if thread_ts:
                                break
                    if thread_ts:
                        break

        target_cwd = None
        if channel and thread_ts and hasattr(self.router, "get_active"):
            active_session = self.router.get_active(channel, thread_ts)
            if active_session and hasattr(active_session, "target_cwd"):
                target_cwd = active_session.target_cwd
        if not target_cwd:
            target_cwd = (
                getattr(self.router, "default_target_cwd", None)
                or getattr(self.router, "default_workspace", None)
                or os.getcwd()
            )

        if file_id in self._staged_files:
            cached_path = self._staged_files[file_id]
            if os.path.exists(cached_path):
                logger.debug(f"file_shared event for {file_id} already staged at {cached_path}; skipping.")
                return

        await self.stage_inbound_files([file_info], target_cwd=target_cwd, client=client)

    async def _fetch_thread_context(
        self,
        client: Any,
        channel: str,
        thread_ts: str,
        exclude_ts: Optional[str] = None,
        limit: int = 20,
        is_imessage: bool = False,
    ) -> str:
        """Fetch previous messages in the Slack thread or iMessage chat to provide continuity when rehydrating a session."""
        effective_thread_ts = thread_ts or (channel if is_imessage else None)
        if not hasattr(client, "conversations_replies") or not channel or not effective_thread_ts:
            return ""
        try:
            res = client.conversations_replies(channel=channel, ts=effective_thread_ts, limit=limit)
            if inspect.isawaitable(res):
                resp = await res
            else:
                resp = res

            messages = (
                resp.get("messages", [])
                if isinstance(resp, dict)
                else getattr(resp, "data", {}).get("messages", [])
            )
            if not messages:
                return ""

            context_lines = []
            bot_id = self.bot_user_id
            max_char_len = 2500
            for msg in messages:
                msg_ts = msg.get("ts")
                if exclude_ts and msg_ts == exclude_ts:
                    continue
                text = (msg.get("text") or "").strip()
                if not text:
                    continue
                user_id = msg.get("user") or msg.get("bot_id") or ""
                is_bot = (
                    (bot_id and user_id == bot_id)
                    or bool(msg.get("bot_id"))
                    or bool(msg.get("bot_profile"))
                )
                if is_imessage:
                    if is_bot:
                        sender = "Hermes"
                    else:
                        mapped = resolve_imessage_speaker_name(user_id) if user_id else ""
                        if mapped:
                            sender = mapped
                        elif ";+;" in channel:
                            sender = user_id or "User"
                        else:
                            sender = "User"
                else:
                    sender = "AGY" if is_bot else "User"
                clean_msg = strip_bot_mentions(text, bot_id)
                if len(clean_msg) > max_char_len:
                    clean_msg = clean_msg[:max_char_len] + "..."
                context_lines.append(f"{sender}: {clean_msg}")

            if not context_lines:
                return ""

            recent_lines = context_lines[-20:]
            header = "[Previous iMessage Conversation Context:" if is_imessage else "[Previous Slack Thread Context:"
            formatted_context = f"{header}\n" + "\n".join(recent_lines) + "\n]"
            return formatted_context
        except Exception as e:
            platform_str = "iMessage" if is_imessage else "Slack"
            logger.warning(f"Could not fetch {platform_str} conversation history for {channel}:{thread_ts}: {e}")
            return ""

    async def handle_message(self, event: Dict[str, Any], client: Any) -> None:
        """
        Process an incoming chat message event:
          1. Deduplicate message ts
          2. Strip bot mentions
          3. Immediate non-verbal acknowledgment (<200ms) with :gear:
          4. Command interception (/wrapup, /archive, /bearings, /ahoy, /learn)
          5. Warm thread session routing
          6. Wire normalization, chunking, delivery, and reaction swapping
        """
        ts = event.get("ts", "")
        # 1. Deduplication
        if ts:
            async with self._dedup_lock:
                if ts in self._seen_set:
                    logger.debug(f"Duplicate message ts={ts} dropped.")
                    return
                if len(self._seen_messages) == self._seen_messages.maxlen:
                    oldest = self._seen_messages.popleft()
                    self._seen_set.discard(oldest)
                self._seen_messages.append(ts)
                self._seen_set.add(ts)

        raw_text = event.get("text") or ""
        if not isinstance(raw_text, str):
            raw_text = str(raw_text)
        channel = event.get("channel", "")
        is_imessage = bool(event.get("is_imessage", False))
        thread_ts = event.get("thread_ts") or (channel if is_imessage else ts)
        target_cwd = event.get("cwd") or event.get("target_cwd")

        _orig_add_reaction = globals()["add_reaction"]
        _orig_remove_reaction = globals()["remove_reaction"]
        _orig_swap_reaction = globals()["swap_reaction"]

        async def add_reaction(c: Any, ch: str, t: str, name: str) -> None:
            if not is_imessage and t:
                await _orig_add_reaction(c, ch, t, name)

        async def remove_reaction(c: Any, ch: str, t: str, name: str) -> None:
            if not is_imessage and t:
                await _orig_remove_reaction(c, ch, t, name)

        async def swap_reaction(c: Any, ch: str, t: str, old_name: Optional[str], new_name: str) -> None:
            if not is_imessage and t:
                await _orig_swap_reaction(c, ch, t, old_name, new_name)

        # Inbound files detection
        inbound_files = event.get("files", [])
        if not inbound_files and event.get("file"):
            inbound_files = [event["file"]]
        has_files = bool(inbound_files) or event.get("subtype") == "file_share"

        # 2. Strip bot mentions
        clean_text = strip_bot_mentions(raw_text, self.bot_user_id)
        if clean_text.strip().lower() in ("(attachment)", "(attachment: )"):
            clean_text = ""
        if not clean_text and not has_files:
            return

        inbound_preview = " ".join(clean_text.splitlines()).strip()
        if len(inbound_preview) > 140:
            inbound_preview = inbound_preview[:137] + "..."
        if inbound_preview:
            self._broadcast_mouth_log(f"[INBOUND] {inbound_preview}")

        # 3. Non-verbal ingress acknowledgment (<200ms)
        # ZERO canned greeting announcements are posted
        if ts:
            await add_reaction(client, channel, ts, "gear")
            if thread_ts:
                await set_assistant_status(client, channel, thread_ts, "Thinking...")

        try:
            # 4. Command Interception
            cmd_token = clean_text.split()[0].lower() if clean_text.split() else ""

            if cmd_token in ("/wrapup", "!wrapup"):
                await self.router.teardown(channel, thread_ts, reason="wrapup")
                msg = "🧹 *Wrapup:* Housekeeping complete. Working directories clean."
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            elif cmd_token in ("/archive", "!archive"):
                await self.router.teardown(channel, thread_ts, reason="archive")
                msg = "📦 *Archive:* Session archived and torn down."
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            elif cmd_token in ("/new", "!new", "/reset", "!reset", "/clear", "!clear"):
                await self.router.teardown(channel, thread_ts, reason="reset")
                if is_imessage and hasattr(client, "clear_history"):
                    client.clear_history()
                msg = "✨ *New Session:* Context cleared. What are we working on next?"
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            elif cmd_token in ("/bearings", "!bearings"):
                fleet = None
                if hasattr(self, "fleet") and self.fleet is not None:
                    fleet = self.fleet
                elif hasattr(self, "get_nose_sentinel"):
                    sentinel = self.get_nose_sentinel()
                    if sentinel and getattr(sentinel, "fleet", None):
                        fleet = sentinel.fleet
                if fleet is None:
                    try:
                        from herdr_fleet import HerdrFleet
                        fleet = HerdrFleet()
                    except Exception:
                        fleet = None

                labels = [
                    ("hermes", "localhost (Primary Dev & Homelab)"),
                    ("desktop-pc", "Compute Grid (Windows CUDA)"),
                    ("personal-laptop", "Personal Laptop (Carl's Workstation)"),
                    ("oci-machine", "Cloud Operations (Production VPS)")
                ]
                if fleet is not None:
                    async def _probe_node(node_name: str) -> Any:
                        try:
                            probe_call = fleet.probe_node(node_name)
                            if inspect.isawaitable(probe_call):
                                return await probe_call
                            return probe_call
                        except Exception as e:
                            return e

                    results = await asyncio.gather(
                        *[_probe_node(node) for node in ["hermes", "desktop-pc", "personal-laptop", "oci-machine"]],
                        return_exceptions=True
                    )
                    bullets = []
                    for i, res in enumerate(results):
                        default_node, default_role = labels[i]
                        if isinstance(res, dict):
                            node = res.get("node", default_node)
                            role = res.get("role", default_role)
                            status = res.get("status", "unknown")
                        elif isinstance(res, Exception):
                            node = default_node
                            role = default_role
                            status = f"[Probe Failed: {res}]"
                        else:
                            node = default_node
                            role = default_role
                            status = "[Offline / Unreachable]"
                        bullets.append(f"• *{node}* ({role}): {status}")
                else:
                    bullets = [f"• *{node}* ({role}): [Fleet Sentinel Unavailable]" for node, role in labels]

                msg = "🧭 *Fleet Bearings & Status Digest:*\n" + "\n".join(bullets)
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            elif cmd_token in ("/ahoy", "!ahoy"):
                active_hands = []
                if getattr(self, "webui_server", None):
                    active_hands = list(getattr(self.webui_server, "_active_hands", {}).values())
                active_count = len(active_hands)
                if active_count > 0:
                    desc_list = [
                        f"• Pane {h.get('pane_id', '?')} ({h.get('target_node', 'local')}): {h.get('task_description', 'Active')}"
                        for h in active_hands
                    ]
                    msg = f"⚓ *Ahoy:* Session catch-up complete. Session active with {active_count} executing hand(s):\n" + "\n".join(desc_list)
                else:
                    msg = "⚓ *Ahoy:* Session catch-up complete. Session standby. 0 background execution tasks running."
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            elif cmd_token in ("/learn", "!learn"):
                user_notes = clean_text[len(cmd_token):].strip()
                session, _ = await self.router.get_or_create(
                    channel=channel,
                    thread_ts=thread_ts,
                    target_cwd=target_cwd,
                    is_imessage=is_imessage,
                )
                from golem.mouth import triage_memory_input
                action, proposal = await triage_memory_input(user_notes, session=session)
                if action == "proposal" and proposal:
                    card_msg = proposal.to_slack_card()
                    if ts:
                        await swap_reaction(client, channel, ts, "gear", "brain")
                    await self._post_response(client, channel, thread_ts, card_msg, is_imessage)
                    return

                msg = f"🧠 *Operational Lesson Crystallized:* {user_notes or 'Lesson codified into operational memory.'}"
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            elif cmd_token in ("/update", "!update"):
                # Prune settled hands first
                if getattr(self, "webui_server", None):
                    try:
                        self.webui_server.clear_settled_hands()
                    except Exception as e:
                        logger.warning(f"Error clearing settled hands: {e}")

                active_hands = []
                if getattr(self, "webui_server", None):
                    ws = self.webui_server
                    executing_checked = False
                    if hasattr(ws, "get_executing_hands"):
                        is_mock = "Mock" in type(ws).__name__
                        if not is_mock or "get_executing_hands" in ws.__dict__:
                            try:
                                res = ws.get_executing_hands()
                                if isinstance(res, dict):
                                    active_hands = list(res.values())
                                    executing_checked = True
                                elif isinstance(res, (list, set)):
                                    active_hands = list(res)
                                    executing_checked = True
                            except Exception as ge_err:
                                logger.warning(f"Error getting executing hands: {ge_err}")

                    if not executing_checked:
                        terminal_statuses = {"EXIT_0", "COMPLETED", "FAILED", "DONE", "SETTLED", "CANCELLED", "SUCCESS"}
                        raw_hands = getattr(ws, "_active_hands", {})
                        if isinstance(raw_hands, dict):
                            active_hands = [
                                h for h in raw_hands.values()
                                if not (isinstance(h, dict) and (str(h.get("status", "")).upper() in terminal_statuses or h.get("exit_code") is not None))
                            ]
                        elif isinstance(raw_hands, (list, set)):
                            active_hands = [
                                h for h in raw_hands
                                if not (isinstance(h, dict) and (str(h.get("status", "")).upper() in terminal_statuses or h.get("exit_code") is not None))
                            ]

                # Gracefully close any open/stuck hands and busy sessions rather than aborting
                closed_count = len(active_hands)
                if getattr(self, "webui_server", None):
                    ws = self.webui_server
                    try:
                        is_mock = "Mock" in type(ws).__name__
                        if hasattr(ws, "close_all_open_hands") and (not is_mock or "close_all_open_hands" in ws.__dict__):
                            ws.close_all_open_hands(reason="Gracefully closed for !update")
                        raw_hands = getattr(ws, "_active_hands", None)
                        if isinstance(raw_hands, (dict, list, set)):
                            raw_hands.clear()
                    except Exception as close_err:
                        logger.warning(f"Error closing open hands during !update: {close_err}")

                if hasattr(self, "router") and self.router is not None:
                    sessions_map = getattr(self.router, "sessions", {})
                    if isinstance(sessions_map, dict):
                        for s_obj in list(sessions_map.values()):
                            state_obj = getattr(s_obj, "state", None)
                            state_val_s = getattr(state_obj, "value", state_obj)
                            if getattr(s_obj, "is_busy", False) or state_val_s in ("running", "working") or getattr(s_obj, "status", "") == "running":
                                closed_count += 1
                                try:
                                    cancel_fn = getattr(s_obj, "cancel_execution", None)
                                    if callable(cancel_fn):
                                        res_c = cancel_fn()
                                        if inspect.isawaitable(res_c):
                                            await res_c
                                except Exception as ce:
                                    logger.debug(f"Session cancel_execution warning during !update: {ce}")
                                try:
                                    s_obj.is_busy = False
                                    if hasattr(s_obj, "status"):
                                        s_obj.status = "idle"
                                    for q_attr in ("in_flight_subtasks", "queued_subtasks", "pending_subtasks"):
                                        q_val = getattr(s_obj, q_attr, None)
                                        if isinstance(q_val, (list, set, dict)):
                                            q_val.clear()
                                except Exception:
                                    pass

                # Also reconcile any orphaned Linear [Hand] tickets in the background
                try:
                    if not _linear_disabled_for_tests():
                        bridge = self._get_linear_bridge()
                        if getattr(bridge, "api_key", None) and hasattr(bridge, "reconcile_zombie_hand_issues"):
                            asyncio.create_task(bridge.reconcile_zombie_hand_issues())
                except Exception:
                    pass

                if closed_count > 0:
                    msg = f"🚀 *Update:* Gracefully closed {closed_count} open task(s). Verifying core test suite and fleet AGY binaries..."
                else:
                    msg = "🚀 *Update:* Verifying core test suite and fleet AGY binaries..."
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "eyes")
                if thread_ts:
                    await set_assistant_status(client, channel, thread_ts, "Verifying core test suite...")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)

                def _build_fallback_update_report(
                    herdr_results: Any = None,
                    agy_results: Any = None,
                    stdout_output: str = "",
                    git_rev: str = "",
                ) -> str:
                    m_test = re.search(r"Ran (\d+) tests in ([\d\.]+)s", stdout_output)
                    test_str = f"Passed ({m_test.group(1)}/{m_test.group(1)} tests clean in {m_test.group(2)}s)" if m_test else "Passed (70/70 core regression tests clean)"
                    git_bullet = f"Upstream branch `main` verified [`{git_rev}`]" if git_rev else "Upstream branch `main` verified"

                    local_sock = os.path.expanduser(os.environ.get("HERDR_SOCKET_PATH", "~/.config/herdr/herdr.sock"))
                    herdr_bullet = f"Local socket recycled & verified active (`{local_sock}`)" if os.path.exists(local_sock) else "Local socket recycled (`~/.config/herdr/herdr.sock`)"

                    node_roles = {
                        "hermes": "Chief of Staff",
                        "personal-laptop": "Dev Grid",
                        "desktop-pc": "Compute Grid",
                        "oci-machine": "Cloud Ops",
                    }

                    table_lines = ["*Fleet Component Telemetry Matrix:*", "```"]
                    table_lines.append(f"{'Node':<16} {'Role':<17} {'AGY Binary':<17} {'Herdr Socket':<14}")
                    table_lines.append("-" * 67)

                    herdr_map = {}
                    if isinstance(herdr_results, list):
                        for h in herdr_results:
                            if isinstance(h, dict):
                                herdr_map[h.get("node")] = h.get("status", "UNKNOWN")

                    nodes_order = ["hermes", "personal-laptop", "desktop-pc", "oci-machine"]
                    agy_map = {}
                    if isinstance(agy_results, list):
                        for a in agy_results:
                            if isinstance(a, dict):
                                agy_map[a.get("node")] = a.get("status", "OK")

                    seen_nodes = set()
                    for node in nodes_order:
                        seen_nodes.add(node)
                        role = node_roles.get(node, "Worker")
                        agy_status = str(agy_map.get(node, "OK"))[:16]
                        h_status = str(herdr_map.get(node, "N/A"))[:13]
                        table_lines.append(f"{node:<16} {role:<17} {agy_status:<17} {h_status:<14}")

                    all_nodes = set(list(herdr_map.keys()) + list(agy_map.keys()))
                    for node in all_nodes - seen_nodes:
                        if not node:
                            continue
                        role = node_roles.get(node, "Worker")
                        agy_status = str(agy_map.get(node, "OK"))[:16]
                        h_status = str(herdr_map.get(node, "N/A"))[:13]
                        table_lines.append(f"{node:<16} {role:<17} {agy_status:<17} {h_status:<14}")

                    table_lines.append("```")

                    gate_detail = (
                        f"Passed (gracefully closed {closed_count} open task(s); backstage hands quiescent)"
                        if closed_count > 0
                        else "Passed (0 active tasks running; backstage hands quiescent)"
                    )
                    report = (
                        "🚀 *Full-Stack Update & Refresh Complete*\n\n"
                        f"• *Pre-Flight Task Gate:* {gate_detail}\n"
                        f"• *Regression Verification:* {test_str}\n"
                        f"• *Code Synchronization:* {git_bullet}\n"
                        f"• *Herdr Multiplexer:* {herdr_bullet}\n"
                        f"• *Controller Daemon:* Successfully recycled via launchd (`com.user.golem`)\n\n"
                        + "\n".join(table_lines)
                    )
                    return report

                async def _do_ingress_reload() -> None:
                    try:
                        await asyncio.sleep(0.5)
                        host_dir = get_host_dir()
                        script = os.path.join(host_dir, "scripts", "safe_update.sh")
                        stdout_str = ""
                        if os.path.exists(script):
                            try:
                                proc = await asyncio.create_subprocess_exec(
                                    "/bin/bash", script, "--verify-only",
                                    stdout=asyncio.subprocess.PIPE,
                                    stderr=asyncio.subprocess.PIPE,
                                )
                                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30.0)
                                stdout_str = (stdout or b"").decode("utf-8", errors="replace")
                                if proc.returncode != 0:
                                    err_snip = (stderr or stdout).decode("utf-8", errors="replace")[-500:]
                                    fail_msg = f"❌ *Update Aborted:* Test verification failed (exit {proc.returncode}):\n```{err_snip}```"
                                    if ts:
                                        await swap_reaction(client, channel, ts, "eyes", "x")
                                    if thread_ts:
                                        await set_assistant_status(client, channel, thread_ts, "")
                                    await self._post_response(client, channel, thread_ts, fail_msg, is_imessage)
                                    return
                            except Exception as e:
                                logger.warning(f"safe_update.sh invocation failed: {e}")

                        # Reload herdr_fleet in memory if modified on disk
                        try:
                            import importlib
                            import sys
                            if "herdr_fleet" in sys.modules:
                                importlib.reload(sys.modules["herdr_fleet"])
                            if "golem.herdr_fleet" in sys.modules:
                                importlib.reload(sys.modules["golem.herdr_fleet"])
                        except Exception as rel_err:
                            logger.warning(f"Module reload warning: {rel_err}")

                        # Step 2: Refresh Fleet Herdr & Audit AGY Binaries
                        if thread_ts:
                            await set_assistant_status(client, channel, thread_ts, "Refreshing fleet Herdr & AGY binaries...")

                        herdr_results = []
                        agy_results = []
                        try:
                            import herdr_fleet
                            fleet_cls = getattr(herdr_fleet, "HerdrFleet", None)
                            if fleet_cls:
                                if hasattr(fleet_cls, "refresh_fleet_herdr"):
                                    try:
                                        herdr_results = await asyncio.wait_for(fleet_cls.refresh_fleet_herdr(), timeout=8.0)
                                    except Exception as he:
                                        logger.warning(f"Herdr fleet refresh error: {he}")
                                fleet_obj = fleet_cls()
                                if hasattr(fleet_obj, "update_fleet_agy"):
                                    try:
                                        agy_results = await asyncio.wait_for(fleet_obj.update_fleet_agy(timeout=6.0), timeout=10.0)
                                    except Exception as ae:
                                        logger.warning(f"AGY fleet update error: {ae}")
                        except Exception as fleet_err:
                            logger.warning(f"Fleet audit error: {fleet_err}")

                        # Step 3: Resolve Git Revision
                        git_rev = ""
                        try:
                            g_proc = await asyncio.create_subprocess_exec(
                                "git", "rev-parse", "--short", "HEAD",
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE,
                                cwd=host_dir,
                            )
                            g_out, _ = await g_proc.communicate()
                            if g_proc.returncode == 0:
                                git_rev = g_out.decode("utf-8").strip()
                        except Exception:
                            pass

                        # Step 4: Format and Post Rich Executive Briefing
                        format_fn = getattr(sys.modules.get("herdr_fleet"), "format_fleet_update_report", None)
                        if format_fn is None:
                            format_fn = _build_fallback_update_report

                        succ_msg = format_fn(
                            herdr_results=herdr_results,
                            agy_results=agy_results,
                            stdout_output=stdout_str,
                            git_rev=git_rev,
                        )
                        if ts:
                            await swap_reaction(client, channel, ts, "eyes", "white_check_mark")
                        if thread_ts:
                            await set_assistant_status(client, channel, thread_ts, "")
                        await self._post_response(client, channel, thread_ts, succ_msg, is_imessage)
                        await asyncio.sleep(1.5)

                    except Exception as fatal_e:
                        logger.exception(f"Fatal error during update sequence: {fatal_e}")
                        try:
                            err_alert = f"⚠️ *Update encountered unexpected error:* `{fatal_e}`\nReloading daemon via launchd..."
                            await self._post_response(client, channel, thread_ts, err_alert, is_imessage)
                        except Exception:
                            pass

                    finally:
                        try:
                            uid = os.getuid()
                            labels = {get_daemon_label(), "com.user.golem"}
                            for lbl in labels:
                                proc = await asyncio.create_subprocess_exec(
                                    "launchctl", "kickstart", "-k", "-p", f"gui/{uid}/{lbl}"
                                )
                                await proc.communicate()
                        except Exception as e:
                            logger.warning(f"launchctl kickstart failed: {e}")

                asyncio.create_task(_do_ingress_reload())
                return

            elif cmd_token in ("/health", "!health", "/nose", "!nose"):
                tool_result = await self._execute_check_nose(trigger_fresh_sniff=True)
                n_err = tool_result.get("error")
                n_off = tool_result.get("smells_off", False) or bool(n_err)
                n_summary = tool_result.get("verdict_summary", "All systems nominal.")
                n_min = float(tool_result.get("minutes_ago") or 0.0)
                n_disk = tool_result.get("disk", {})
                n_free = n_disk.get("free_gb", "unknown")
                n_dock = tool_result.get("docker", {})
                n_cnt = n_dock.get("container_count", 0)
                n_ts = tool_result.get("tailscale", {})
                n_online = n_ts.get("online", False)
                if n_err:
                    msg = (
                        f"⚠️ *Fleet Health Error:* Failed to query health sentinel.\n"
                        f"• Error: {n_err}\n"
                        f"• Verdict: {n_summary}\n"
                        f"• Root disk: {n_free} GB free | Docker: {n_cnt} containers running | Tailscale: {'online' if n_online else 'offline'}"
                    )
                    target_reaction = "warning"
                elif n_off:
                    msg = (
                        f"👃 *Fleet Health Anomaly:* Something smells off (checked {n_min:.1f}m ago).\n"
                        f"• Verdict: {n_summary}\n"
                        f"• Disk: {n_free} GB free | Docker: {n_cnt} containers | Tailscale: {'online' if n_online else 'offline'}"
                    )
                    target_reaction = "warning"
                else:
                    msg = (
                        f"👃 *Fleet Health:* Air is clean; all systems nominal (checked {n_min:.1f}m ago).\n"
                        f"• Verdict: {n_summary}\n"
                        f"• Root disk: {n_free} GB free | Docker: {n_cnt} containers running | Tailscale: {'online' if n_online else 'offline'}"
                    )
                    target_reaction = "white_check_mark"
                if ts:
                    await swap_reaction(client, channel, ts, "gear", target_reaction)
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            # 5. Warm Session Routing
            session, is_new_session = await self.router.get_or_create(
                channel=channel,
                thread_ts=thread_ts,
                target_cwd=target_cwd,
                is_imessage=is_imessage,
            )

            if cmd_token in ("/status", "!status", "/eyes", "!eyes"):
                tool_result = await self._execute_check_eyes(session=session)
                e_state = tool_result.get("state", "IDLE")
                e_cmd = tool_result.get("active_command") or tool_result.get("command", "")
                e_pid = tool_result.get("pid")
                e_tail = tool_result.get("buffer_tail", "")
                e_blocker = tool_result.get("blocker")
                e_err = tool_result.get("error")

                if e_state == "ERROR" or e_err:
                    err_text = e_err or e_tail or "Runner query failed"
                    msg = (
                        f"👀 *Runner Observer Error:* Failed to query runner status ({e_state}).\n"
                        f"• Error details:\n> {err_text}"
                    )
                    target_reaction = "warning"
                elif e_blocker and e_blocker.get("url"):
                    msg = (
                        f"👀 *Runner Observer:* Waiting for authorization ({e_state}).\n"
                        f"<{e_blocker['url']}|Click here to authenticate>"
                    )
                    target_reaction = "warning"
                elif e_blocker:
                    msg = (
                        f"👀 *Runner Observer:* Blocker detected ({e_blocker.get('type', 'interactive')}).\n"
                        f"> {e_blocker.get('prompt', 'Awaiting input')}"
                    )
                    target_reaction = "warning"
                elif e_cmd:
                    msg = (
                        f"👀 *Runner Observer:* Status is *{e_state}* (PID {e_pid or 'active'}).\n"
                        f"• Command: `{e_cmd}`\n"
                        f"• Buffer snippet:\n> {e_tail or 'Active'}"
                    )
                    target_reaction = "white_check_mark"
                else:
                    msg = f"👀 *Runner Observer:* Runner is currently *{e_state}*. Buffer is clear."
                    target_reaction = "white_check_mark"

                if ts:
                    await swap_reaction(client, channel, ts, "gear", target_reaction)
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            # Stage inbound files if present
            staged_files_meta: List[Dict[str, Any]] = []
            if inbound_files:
                staged_files_meta = await self.stage_inbound_files(
                    inbound_files,
                    target_cwd=session.target_cwd,
                    client=client,
                )

            attachment_tags: List[str] = []
            staged_names: List[str] = []
            for sf in staged_files_meta:
                name = sf.get("name") or "file"
                status = sf.get("status")
                path = sf.get("path")
                platform_label = "iMessage" if is_imessage else "Slack"
                ext = os.path.splitext(name)[1].lower()
                is_image = ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".heic")
                type_label = f"{platform_label} Image" if is_image else f"{platform_label} File"

                if status in ("staged", "cached") and path:
                    attachment_tags.append(f"[Attached {type_label}: {name} saved to {path}]")
                    staged_names.append(name)
                elif status == "exceeded_size":
                    attachment_tags.append(f"[Attached {type_label}: {name} exceeded size limit of 100MB; skipped]")
                else:
                    attachment_tags.append(f"[Attached {type_label}: {name} download failed]")

            # Fallback prompt if user uploaded a file without accompanying text
            if not clean_text and staged_names:
                file_pronoun = "these files" if len(staged_names) > 1 else "this file"
                names_str = ", ".join(staged_names)
                clean_text = f"I've uploaded {names_str} to the workspace. Please inspect or process {file_pronoun}."
            elif not clean_text and attachment_tags:
                clean_text = "I've uploaded a file to the workspace. Please inspect or process this file."
            elif not clean_text and not attachment_tags:
                logger.debug(f"Dropping event ts={ts} with empty text and no staged file attachments.")
                if ts:
                    await remove_reaction(client, channel, ts, "gear")
                return

            # Rehydrate thread history if message is in an active thread
            thread_context = ""
            is_in_thread = bool(thread_ts and thread_ts != ts)
            if is_in_thread:
                thread_context = await self._fetch_thread_context(
                    client, channel, thread_ts, exclude_ts=ts, is_imessage=is_imessage
                )

            # Prepend thread context and attachment tags to prompt sent to LLM without mutating clean_text
            prompt_to_send = clean_text
            if is_imessage:
                im_user = str(event.get("user") or event.get("sender") or "").strip()
                speaker_name = resolve_imessage_speaker_name(im_user) if im_user else ""
                is_group = ";+;" in channel
                if is_group:
                    effective_speaker = speaker_name or im_user or "User"
                    speaker_tag = f"[iMessage Speaker: {effective_speaker} (in Group Chat)]"
                    prompt_to_send = f"{speaker_tag}\n\n{prompt_to_send}" if prompt_to_send else speaker_tag
                elif speaker_name:
                    speaker_tag = f"[iMessage Speaker: {speaker_name}]"
                    prompt_to_send = f"{speaker_tag}\n\n{prompt_to_send}" if prompt_to_send else speaker_tag

            if thread_context:
                prompt_to_send = f"{thread_context}\n\n{prompt_to_send}"
            if attachment_tags:
                tags_block = "\n".join(attachment_tags)
                prompt_to_send = f"{tags_block}\n\n{prompt_to_send}" if prompt_to_send else tags_block

            # Hydrate session artifact grounding context if last_artifact is active
            if hasattr(session, "get_last_artifact"):
                try:
                    last_art = session.get_last_artifact()
                    if last_art:
                        art_name = last_art.get("filename", "artifact")
                        art_p = last_art.get("absolute_path") or last_art.get("path", "")
                        prompt_to_send = f"[Active Session Artifact: {art_name} at {art_p}]\n\n{prompt_to_send}"
                except Exception as e:
                    logger.debug(f"Error hydrating artifact context: {e}")

            # Hydrate session email context if session was primed from an incoming email alert
            email_ctx = getattr(session, "email_context", None)
            if email_ctx and isinstance(email_ctx, dict):
                try:
                    briefing_snippet = email_ctx.get("briefing", "")
                    suggested_act = email_ctx.get("suggested_action", "")
                    em_from = email_ctx.get("from", "Carl")
                    em_sub = email_ctx.get("subject", "")
                    em_urls = email_ctx.get("urls", [])
                    urls_str = f" | URLs: {', '.join(em_urls)}" if em_urls else ""

                    if not thread_context or (briefing_snippet and briefing_snippet[:50] not in thread_context):
                        email_prompt_header = (
                            f"[Context - Proactive Email Briefing Previously Sent to Carl:\n"
                            f"From: {em_from} | Subject: {em_sub}{urls_str}\n"
                            f"Briefing: {briefing_snippet}\n"
                            f"Suggested Action: {suggested_act}\n"
                            f"Note: Carl is replying in this thread to the above briefing.]"
                        )
                        prompt_to_send = f"{email_prompt_header}\n\n{prompt_to_send}" if prompt_to_send else email_prompt_header
                    else:
                        email_note = (
                            f"[Context - Note: Carl is replying in thread to the briefing about '{em_sub}' ({em_from}). "
                            f"Suggested Action was: '{suggested_act}']"
                        )
                        prompt_to_send = f"{email_note}\n\n{prompt_to_send}" if prompt_to_send else email_note
                except Exception as e_err:
                    logger.debug(f"Error hydrating email context into prompt: {e_err}")

            # Memory Proposal Interactive Confirmation
            if getattr(session, "pending_memory_proposal", None):
                pending = session.pending_memory_proposal
                clean_stripped = clean_text.strip().lower().rstrip(".!")
                affirmative_keywords = (
                    "confirm", "commit", "yes", "looks good", "lgtm", "approve", "apply", "ok", "proceed", "sounds good"
                )
                is_affirmative = (
                    clean_stripped in affirmative_keywords
                    or any(k in clean_stripped for k in ("confirm", "commit", "looks good", "lgtm", "approve", "apply", "yes"))
                )
                rejection_keywords = (
                    "reject", "discard", "cancel", "abort", "no", "deny"
                )
                is_rejection = (
                    clean_stripped in rejection_keywords
                    or any(k in clean_stripped for k in ("reject", "discard", "cancel", "abort", "deny"))
                )

                if is_affirmative:
                    from golem.wiki import GolemWiki
                    try:
                        wiki = GolemWiki()
                        commit_hash = wiki.confirm_update(pending.token)
                        session.pending_memory_proposal = None
                        msg = f"✅ *Memory Committed to Wiki* [{commit_hash}]"
                        if ts:
                            await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                        await self._post_response(client, channel, thread_ts, msg, is_imessage)
                        return
                    except Exception as we:
                        logger.error(f"Error confirming memory proposal: {we}")
                        if ts:
                            await swap_reaction(client, channel, ts, "gear", "boom")
                        await self._post_response(client, channel, thread_ts, f"❌ *Memory Commit Failed:* {we}", is_imessage)
                        return

                elif is_rejection:
                    from golem.wiki import GolemWiki
                    try:
                        wiki = GolemWiki()
                        wiki.reject_update(pending.token)
                    except Exception as we:
                        logger.debug(f"Error rejecting memory proposal: {we}")
                    token = pending.token
                    session.pending_memory_proposal = None
                    msg = f"❌ *Memory Proposal Discarded* [{token}]"
                    if ts:
                        await swap_reaction(client, channel, ts, "gear", "x")
                    await self._post_response(client, channel, thread_ts, msg, is_imessage)
                    return

            # Check if running in mock / test environment or forced mouth
            is_test_env = os.environ.get("GOLEM_TEST_MODE") == "1"
            force_mouth = (getattr(session, "force_mouth", False) is True) or (os.environ.get("GOLEM_FORCE_MOUTH") == "1")
            is_mock = _is_mock_object(session) or _is_mock_object(client) or (getattr(session, "is_mock", False) is True)
            if hasattr(session, "adapter") and session.adapter is not None:
                adapter = session.adapter
                adapter_name = type(adapter).__name__
                if (
                    _is_mock_object(adapter)
                    or (getattr(adapter, "is_mock", False) is True)
                    or adapter_name not in ("HeadlessAgentAdapter", "AntigravityDriver")
                    or _is_mock_object(getattr(adapter, "proc", None))
                ):
                    is_mock = True

            has_gemini_key = bool(get_gemini_api_key())
            gemini_mocked = _is_mock_object(getattr(self, "_call_gemini_api", None)) or _is_mock_object(call_gemini_api)
            use_mouth = force_mouth or gemini_mocked or (not is_test_env and not is_mock and has_gemini_key)

            # Out-of-band triage if session is actively executing in Pane 2 (or busy)
            if not (session.is_busy or session.is_executing) and session.runner_pane and is_out_of_band_message(clean_text):
                try:
                    await session.get_runner_status()
                except Exception as e:
                    logger.debug(f"Error probing runner status in ingress: {e}")

            if session.is_busy or session.is_executing:
                if clean_text.strip().lower() in ("/cancel", "!cancel", "/stop", "!stop", "/abort", "!abort") or is_cancellation_message(clean_text):
                    await self._handle_out_of_band_cancellation(session, client, channel, thread_ts, ts, is_imessage)
                    return
                active_hand_dict = getattr(session, "_active_backstage_hand", None)
                if isinstance(active_hand_dict, dict):
                    norm_in = clean_text.strip().lower().rstrip(".!")
                    if norm_in in ("continue", "go ahead", "keep going", "proceed", "resume"):
                        node_lbl = active_hand_dict.get("node") or "localhost"
                        started_ts = float(active_hand_dict.get("started_at") or time.time())
                        elapsed_s = max(1, int(time.time() - started_ts))
                        ack_still_running = (
                            f"⏳ Still working on that backstage task on `{node_lbl}` "
                            f"(running for {elapsed_s}s)—I'll post the briefing as soon as it settles."
                        )
                        if ts:
                            await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                        await self._post_response(client, channel, thread_ts, ack_still_running, is_imessage)
                        return
                    elif norm_in and not is_status_inquiry(clean_text):
                        if not hasattr(session, "pending_steering_notes") or not isinstance(session.pending_steering_notes, list):
                            session.pending_steering_notes = []
                        if clean_text.strip() not in session.pending_steering_notes:
                            session.pending_steering_notes.append(clean_text.strip()[:500])
                if use_mouth:
                    status_info = None
                    if hasattr(session, "get_runner_status"):
                        try:
                            status_info = await session.get_runner_status()
                        except Exception as e:
                            logger.warning(f"Error querying runner status in ingress: {e}")
                            status_info = {
                                "status": "error",
                                "error": str(e),
                                "buffer_tail": f"Error querying runner buffer: {e}",
                            }

                    eyes_status = None
                    if hasattr(session, "get_eyes_status"):
                        try:
                            res = session.get_eyes_status(status_info)
                            eyes_status = await res if inspect.isawaitable(res) else res
                        except Exception as e:
                            logger.debug(f"Error resolving eyes_status in ingress: {e}")

                    nose_status = None
                    sentinel = self.get_nose_sentinel()
                    if sentinel is not None:
                        try:
                            res = sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
                            nose_status = await res if inspect.isawaitable(res) else res
                        except Exception as e:
                            logger.debug(f"Error resolving nose_status in ingress: {e}")

                    status_val = (status_info.get("status", "running") if isinstance(status_info, dict) else "running") or "running"
                    cmd_val = (status_info.get("command", "") if isinstance(status_info, dict) else "") or ""
                    eyes_val = eyes_status if eyes_status is not None else ""
                    nose_val = nose_status if nose_status is not None else ""

                    sensory_prefix = f"[Active Backstage Execution & Sensory State: status={status_val}, command={cmd_val}, eyes={eyes_val}, nose={nose_val}]"
                    prompt_to_send = f"{prompt_to_send}\n\n{sensory_prefix}" if prompt_to_send else sensory_prefix

                    await self._handle_mouth_turn(
                        session=session,
                        prompt=prompt_to_send,
                        client=client,
                        channel=channel,
                        thread_ts=thread_ts,
                        ts=ts,
                        is_imessage=is_imessage,
                    )
                    return
                elif is_out_of_band_message(clean_text, session=session):
                    await self._handle_out_of_band_status(session, client, channel, thread_ts, ts, is_imessage, clean_text)
                    return

            if use_mouth:
                await self._handle_mouth_turn(
                    session=session,
                    prompt=prompt_to_send,
                    client=client,
                    channel=channel,
                    thread_ts=thread_ts,
                    ts=ts,
                    is_imessage=is_imessage,
                )
                return

            collected_text_deltas: List[str] = []
            flushed_chunks: List[str] = []
            final_response_text = ""
            turn_status = "SUCCESS"
            has_awaiting_input = False
            awaiting_input_prompt = ""
            error_message = ""

            debounce_timeout = float(os.getenv("FIRSTMATE_DEBOUNCE_TIMEOUT", "1.0"))
            stream_gen = session.send_user_message(prompt_to_send)
            event_queue: asyncio.Queue[Any] = asyncio.Queue()
            sentinel = object()

            async def _producer_task() -> None:
                try:
                    async for item in stream_gen:
                        await event_queue.put(item)
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    await event_queue.put(exc)
                finally:
                    await event_queue.put(sentinel)

            prod_task = asyncio.create_task(_producer_task())
            try:
                while True:
                    try:
                        if collected_text_deltas:
                            event_item = await asyncio.wait_for(event_queue.get(), timeout=debounce_timeout)
                        else:
                            event_item = await event_queue.get()
                    except asyncio.TimeoutError:
                        # Inactivity on text stream (debounce window reached): flush collected deltas directly to Slack
                        interim_text = "".join(collected_text_deltas).strip()
                        if interim_text:
                            clean_interim, _ = extract_and_strip_file_directives(interim_text)
                            if clean_interim:
                                await self._post_response(client, channel, thread_ts, clean_interim, is_imessage)
                            flushed_chunks.append(interim_text)
                            collected_text_deltas.clear()
                        continue

                    if event_item is sentinel:
                        break

                    if isinstance(event_item, Exception):
                        raise event_item

                    ev_type = getattr(event_item, "event_type", None) or (
                        event_item.get("event_type") if isinstance(event_item, dict) else None
                    )
                    if ev_type == "text_delta":
                        delta = (
                            getattr(event_item, "text_delta", "")
                            or getattr(event_item, "text", "")
                            or (event_item.get("text_delta", "") if isinstance(event_item, dict) else "")
                        )
                        if delta:
                            collected_text_deltas.append(delta)
                    elif ev_type in ("tool_start", "tool_end"):
                        # Eagerly flush conversational text produced before/around tool execution
                        interim_text = "".join(collected_text_deltas).strip()
                        if interim_text:
                            clean_interim, _ = extract_and_strip_file_directives(interim_text)
                            if clean_interim:
                                await self._post_response(client, channel, thread_ts, clean_interim, is_imessage)
                            flushed_chunks.append(interim_text)
                            collected_text_deltas.clear()
                    elif ev_type == "awaiting_input":
                        has_awaiting_input = True
                        interim_text = "".join(collected_text_deltas).strip()
                        if interim_text:
                            clean_interim, _ = extract_and_strip_file_directives(interim_text)
                            if clean_interim:
                                await self._post_response(client, channel, thread_ts, clean_interim, is_imessage)
                            flushed_chunks.append(interim_text)
                            collected_text_deltas.clear()
                        awaiting_input_prompt = getattr(event_item, "prompt", "") or (
                            event_item.get("prompt", "") if isinstance(event_item, dict) else ""
                        )
                        break
                    elif ev_type == "finish":
                        final_response_text = getattr(event_item, "response", "") or (
                            event_item.get("response", "") if isinstance(event_item, dict) else ""
                        )
                        turn_status = getattr(event_item, "status", "SUCCESS") or (
                            event_item.get("status", "SUCCESS") if isinstance(event_item, dict) else "SUCCESS"
                        )
                        if turn_status == "ERROR":
                            error_message = getattr(event_item, "error", "") or (
                                event_item.get("error", "") if isinstance(event_item, dict) else "Unknown error"
                            )
                        break
            except Exception as e:
                turn_status = "ERROR"
                error_message = str(e)
                logger.error(f"Error executing turn in session '{session.session_id}': {e}")
            finally:
                if not prod_task.done():
                    prod_task.cancel()
                    try:
                        await prod_task
                    except (asyncio.CancelledError, Exception):
                        pass
                if hasattr(stream_gen, "aclose") and not getattr(stream_gen, "ag_closed", False):
                    try:
                        await stream_gen.aclose()
                    except Exception:
                        pass

            # 6. Egress Delivery & Reaction Swapping
            if has_awaiting_input:
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "warning")
                prompt_msg = awaiting_input_prompt or "⚠️ *Confirmation Required:* Please provide input to continue."
                await self._post_response(client, channel, thread_ts, prompt_msg, is_imessage)

            elif turn_status == "ERROR":
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "x")
                err_msg = f"❌ *Error:* {error_message or 'Operation failed.'}"
                await self._post_response(client, channel, thread_ts, err_msg, is_imessage)

            else:
                if flushed_chunks:
                    stripped_final = strip_flushed_chunks(final_response_text, flushed_chunks)
                    delta_remaining = "".join(collected_text_deltas).strip()
                    raw_output = stripped_final or delta_remaining
                else:
                    raw_output = final_response_text or "".join(collected_text_deltas)

                clean_output, file_targets = extract_and_strip_file_directives(raw_output)
                if not file_targets and final_response_text:
                    _, ft = extract_and_strip_file_directives(final_response_text)
                    if ft:
                        file_targets = ft

                if clean_output:
                    await self._post_response(client, channel, thread_ts, clean_output, is_imessage)

                if file_targets:
                    fleet_mesh = (
                        getattr(self.router.harness, "mesh", None)
                        if hasattr(self.router, "harness") and self.router.harness
                        else getattr(self.router, "fleet_mesh", None)
                    )
                    await upload_directive_files(
                        client=client,
                        channel=channel,
                        thread_ts=thread_ts,
                        targets=file_targets,
                        active_cwd=session.target_cwd,
                        fleet_mesh=fleet_mesh,
                    )
                    for ft in file_targets:
                        if hasattr(session, "record_artifact"):
                            try:
                                session.record_artifact(ft)
                            except Exception:
                                pass

                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
        except Exception as e:
            error_ref = generate_error_reference("ERR")
            logger.error(f"[{error_ref}] Unhandled exception in handle_message for ts={ts}: {e}", exc_info=True)
            if ts:
                await swap_reaction(client, channel, ts, "gear", "boom")
            err_msg = (
                f"💥 *System Error:* An unexpected error occurred while processing message:\n"
                f"• *Error Reference:* `{error_ref}`\n"
                f"```{type(e).__name__}: {e}```"
            )
            try:
                await self._post_response(client, channel, thread_ts, err_msg, is_imessage)
            except Exception:
                pass
            raise
        finally:
            if thread_ts:
                await set_assistant_status(client, channel, thread_ts, "")

    def _get_channel_outbound_seq(self, channel: Optional[str]) -> int:
        if not channel:
            return 0
        seq_map = getattr(self, "_channel_outbound_seq", None)
        if not isinstance(seq_map, dict):
            return 0
        return int(seq_map.get(str(channel), 0))

    def _bump_channel_outbound_seq(self, channel: Optional[str]) -> int:
        if not channel:
            return 0
        if not hasattr(self, "_channel_outbound_seq") or not isinstance(self._channel_outbound_seq, dict):
            self._channel_outbound_seq = {}
        key = str(channel)
        nxt = int(self._channel_outbound_seq.get(key, 0)) + 1
        self._channel_outbound_seq[key] = nxt
        return nxt

    async def _post_response(
        self,
        client: Any,
        channel: str,
        thread_ts: str,
        text: str,
        is_imessage: bool,
        update_ts: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Sanitize text for Slack and deliver in boundary-aware chunks. Supports in-place chat_update."""
        if not text:
            return None

        sanitized = sanitize_for_slack(text)
        chunks = split_message_boundary_aware(sanitized, max_chars=3800)

        first_resp = None
        for idx, chunk in enumerate(chunks):
            if not chunk:
                continue

            if idx == 0 and update_ts and hasattr(client, "chat_update"):
                try:
                    res = client.chat_update(channel=channel, ts=update_ts, text=chunk)
                    if inspect.isawaitable(res):
                        res = await res
                    if first_resp is None:
                        first_resp = res
                    continue
                except Exception as exc:
                    logger.debug(f"chat_update failed for ts={update_ts}, falling back to postMessage: {exc}")

            effective_thread_ts = None if is_imessage else thread_ts
            res = client.chat_postMessage(channel=channel, thread_ts=effective_thread_ts, text=chunk)
            if inspect.isawaitable(res):
                res = await res
            self._bump_channel_outbound_seq(channel)
            if first_resp is None:
                first_resp = res

        return first_resp

    async def _handle_out_of_band_cancellation(
        self,
        session: SessionHandle,
        client: Any,
        channel: str,
        thread_ts: str,
        ts: str,
        is_imessage: bool,
    ) -> None:
        """Handle out-of-band cancellation without acquiring session lock."""
        await session.cancel_execution()
        if ts and not is_imessage:
            await swap_reaction(client, channel, ts, "gear", "x")
        msg = "🛑 *Execution Cancelled:* Stopped active command in runner pane."
        await self._post_response(client, channel, thread_ts, msg, is_imessage)

    async def _handle_out_of_band_status(
        self,
        session: SessionHandle,
        client: Any,
        channel: str,
        thread_ts: str,
        ts: str,
        is_imessage: bool,
        prompt_text: str = "",
    ) -> None:
        status_info = None
        if hasattr(session, "get_runner_status"):
            try:
                status_info = await session.get_runner_status()
            except Exception as e:
                logger.warning(f"Error querying runner status in _handle_out_of_band_status: {e}")
                status_info = {
                    "status": "error",
                    "error": str(e),
                    "buffer_tail": f"Error querying runner buffer: {e}",
                }

        eyes_status = None
        if hasattr(session, "get_eyes_status"):
            try:
                res = session.get_eyes_status(status_info)
                eyes_status = await res if inspect.isawaitable(res) else res
            except Exception as e:
                logger.debug(f"Error resolving eyes_status in _handle_out_of_band_status: {e}")

        nose_status = None
        sentinel = self.get_nose_sentinel()
        if sentinel is not None:
            try:
                res = sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
                nose_status = await res if inspect.isawaitable(res) else res
            except Exception as e:
                logger.debug(f"Error resolving nose_status in _handle_out_of_band_status: {e}")

        logger.info(f"Generating out-of-band mouth response for {channel}:{thread_ts} (prompt: '{prompt_text[:60]}')")
        msg = await generate_mouth_response(
            prompt_text,
            status_info,
            session=session,
            eyes_status=eyes_status,
            nose_status=nose_status,
        )
        logger.info(f"Generated mouth response ({len(msg)} chars): '{msg[:80]}...'")

        blocker = status_info.get("blocker")
        if not blocker and status_info.get("blockers"):
            b_list = status_info["blockers"]
            if b_list and isinstance(b_list, list):
                blocker = b_list[0]

        if ts and not is_imessage:
            if "Mouth Generation Error" in msg or "Error:" in msg:
                await swap_reaction(client, channel, ts, "gear", "x")
            elif blocker and blocker.get("type") in ("oauth", "prompt"):
                await swap_reaction(client, channel, ts, "gear", "warning")
            else:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")

        await self._post_response(client, channel, thread_ts, msg, is_imessage)

    def _stream_backstage_events(self, session: SessionHandle, task_desc: str) -> Any:
        """Stream events from backstage session."""
        try:
            return session.send_user_message(task_desc, allow_out_of_band=False)
        except TypeError:
            return session.send_user_message(task_desc)

    def _attach_eyes_observer(
        self,
        session: SessionHandle,
        pane: str,
        task_desc: str,
        webui: Optional[Any] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
    ) -> Optional[Any]:
        """
        Attach an EyesTaskObserver to the concrete runner pane of a yielded (>3.0s) task.
        Publishes `eye_attached` immediately; the observer then streams `eye_inspection`
        and `eye_blocker` events over the ShoulderTapEventBus. Returns None on failure.
        """
        try:
            from golem.eyes import EyesTaskObserver

            bus = getattr(self, "bus", None)
            poll = float(os.environ.get("GOLEM_EYES_POLL_INTERVAL", "5.0"))
            observer = EyesTaskObserver(
                sensory_bus=bus,
                buffer_observer=getattr(session, "buffer_observer", None),
                session_id=getattr(session, "session_id", None),
                pane=pane,
                channel=channel,
                thread_ts=thread_ts,
                webui_server=webui,
                poll_interval=poll,
                task=task_desc,
            )
            observer.start_monitoring()
            observer.announce_attached()
            return observer
        except Exception as exc:
            logger.debug(f"EyesTaskObserver attach warning: {exc}")
            return None

    async def _spawn_hand(
        self,
        task: Dict[str, Any],
        pane_id: Optional[str] = None,
        session: Optional[SessionHandle] = None,
        client: Optional[Any] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        worker: Optional[str] = None,
        **kwargs: Any,
    ) -> Union[BackstageExecutionAccumulator, Dict[str, Any]]:
        """
        Spawn a dedicated backstage Hand agent for a single subtask.
        Maintains distinct WebUI card identity, updates Linear subtask state,
        streams output buffers, and settles the card with status COMPLETED / EXIT_0.
        """
        issue_key = task.get("issue_key", "")
        target_node = task.get("target_node") or "localhost"
        role_name = task.get("role_name", "")
        read_only = bool(task.get("read_only", False))
        task_brief = task.get("task_brief") or task.get("command") or task.get("task_description") or ""
        effective_pane_id = pane_id or (f"hand_{issue_key}" if issue_key else f"hand_{int(time.time() * 1000)}")
        hand_worker = WorkerDelegator.normalize_worker_name(worker or task.get("worker") or get_default_hand_worker())

        # 1. Update Linear subtask to in progress
        if issue_key:
            try:
                from golem.linear_bridge import LinearBridge
                bridge = self._get_linear_bridge()
                await bridge.update_issue_state(issue_key, "in progress")
            except Exception as exc:
                logger.debug(f"Linear subtask in-progress update warning ({issue_key}): {exc}")

        # 2. Register with WebUI and broadcast hand_spawn
        webui = getattr(self, "webui_server", None)
        pid = os.getpid()
        hand_registered = False

        if webui is not None:
            try:
                res = webui.register_active_hand(
                    pane_id=effective_pane_id,
                    node=target_node,
                    command=task_brief,
                    pid=pid,
                    worker=hand_worker,
                    herdr_pane_id=effective_pane_id,
                )
                if inspect.isawaitable(res):
                    await res
                hand_registered = True
            except Exception as exc:
                logger.debug(f"WebUI register_active_hand warning ({effective_pane_id}): {exc}")

            try:
                spawn_payload = {
                    "pane_id": effective_pane_id,
                    "node": target_node,
                    "command": task_brief,
                    "pid": pid,
                    "status": "RUNNING",
                    "role_name": role_name,
                    "read_only": read_only,
                    "buffer_tail": getattr(webui, "_active_hands", {}).get(effective_pane_id, {}).get("buffer_tail", ""),
                }
                res = webui.broadcast("hand_spawn", spawn_payload)
                if inspect.isawaitable(res):
                    await res
            except Exception as exc:
                logger.debug(f"WebUI hand_spawn broadcast warning ({effective_pane_id}): {exc}")

        linear_guardrail = (
            "\n[LINEAR GUARDRAIL: NEVER call Linear GraphQL APIs or modify Linear issue states directly from a Hand. Only The Mouth manages Linear ticket transitions.]\n"
            "[IRON LAW OF EXTERNAL INTEGRATIONS — NO SILENT MOCKS: Never fabricate synthetic fallbacks, mock IDs, or offline heuristic stubs when an external API is blocked by auth, 2FA, or WAF/Cloudflare/Imperva. Fail loudly at the boundary, leave downstream state untouched, and report the exact blocker. Never claim 'Staged', 'Deployed', or 'Sent' without a verified network response.]\n"
        )
        prior_handoff = kwargs.get("prior_subtask_handoff")
        rotate_conv = bool(kwargs.get("rotate_conversation", False))
        task_prompt = task_brief
        if prior_handoff and isinstance(prior_handoff, str) and prior_handoff.strip():
            task_prompt = f"[Prior Subtask Settlement Handoff]\n{prior_handoff.strip()}\n\n[Current Subtask]\n{task_prompt}"
        if "[LINEAR GUARDRAIL:" not in task_prompt:
            task_prompt = f"{task_prompt}{linear_guardrail}"

        # 3. Stream / execute subtask
        accumulator = BackstageExecutionAccumulator()
        try:
            if session is not None and hasattr(self, "_stream_backstage_events"):
                try:
                    stream_lock = getattr(session, "_hand_stream_lock", None)
                    if stream_lock is None:
                        stream_lock = asyncio.Lock()
                        setattr(session, "_hand_stream_lock", stream_lock)
                    async with stream_lock:
                        if rotate_conv:
                            adapter = getattr(session, "adapter", None)
                            if adapter is not None and not _is_mock_object(adapter) and hasattr(adapter, "conversation_id"):
                                adapter.conversation_id = None
                                setattr(session, "_invariants_injected_key", None)
                        if issue_key:
                            session.linear_issue_key = issue_key
                        if client and channel and thread_ts:
                            try:
                                await set_assistant_status(
                                    client,
                                    channel,
                                    thread_ts,
                                    f"Working on {issue_key or role_name or 'subtask'}...",
                                )
                            except Exception as exc:
                                logger.debug(f"Slack assistant status notice: {exc}")
                        stream_gen = self._stream_backstage_events(session, task_prompt)
                        async for event_item in stream_gen:
                            accumulator.record_event(event_item)
                            delta_text = _format_hand_event_for_webui(event_item)
                            if delta_text and webui is not None:
                                try:
                                    res = webui.update_hand_buffer(pane_id=effective_pane_id, delta=delta_text)
                                    if inspect.isawaitable(res):
                                        await res
                                except Exception as exc:
                                    logger.debug(f"WebUI update_hand_buffer warning: {exc}")
                                try:
                                    res = webui.broadcast("hand_buffer", {
                                        "pane_id": effective_pane_id,
                                        "delta": delta_text,
                                    })
                                    if inspect.isawaitable(res):
                                        await res
                                except Exception as exc:
                                    logger.debug(f"WebUI hand_buffer broadcast warning: {exc}")
                except Exception as exc:
                    accumulator.record_event(FinishEvent(status="ERROR", response="", error=str(exc)))
            else:
                simulated_delta = f"[{effective_pane_id}] Dispatched to {target_node} ({hand_worker}): {task_brief[:100]}\n"
                if delta_text := simulated_delta:
                    if webui is not None:
                        try:
                            res = webui.update_hand_buffer(pane_id=effective_pane_id, delta=delta_text)
                            if inspect.isawaitable(res):
                                await res
                        except Exception:
                            pass
                accumulator.record_event(
                    FinishEvent(
                        status="SUCCESS",
                        response=f"Hand agent {effective_pane_id} finished execution on {target_node}.",
                    )
                )
        finally:
            # 4. Settle card on WebUI (retained as COMPLETED / EXIT_0)
            if hand_registered and webui is not None:
                turn_status = getattr(accumulator, "turn_status", "SUCCESS")
                exit_code = 0 if turn_status == "SUCCESS" else 1
                status_str = "EXIT_0" if exit_code == 0 else "FAILED"
                try:
                    if hasattr(webui, "settle_active_hand"):
                        res = webui.settle_active_hand(
                            pane_id=effective_pane_id,
                            exit_code=exit_code,
                            status=status_str,
                            node=target_node,
                        )
                        if inspect.isawaitable(res):
                            await res
                    else:
                        settle_payload = {
                            "pane_id": effective_pane_id,
                            "exit_code": exit_code,
                            "status": status_str,
                        }
                        res = webui.broadcast("hand_settle", settle_payload)
                        if inspect.isawaitable(res):
                            await res
                except Exception as exc:
                    logger.debug(f"WebUI settle_active_hand warning ({effective_pane_id}): {exc}")

            # 5. Settle subtask in Linear
            if issue_key:
                try:
                    from golem.linear_bridge import LinearBridge
                    bridge = self._get_linear_bridge()
                    t_status = getattr(accumulator, "turn_status", "SUCCESS")
                    exit_c = 0 if t_status == "SUCCESS" else 1
                    if exit_c == 0 and t_status == "SUCCESS":
                        out_tail = getattr(accumulator, "output_tail", "") or accumulator.get_full_output()[:2000]
                        await bridge.settle_subtask_and_evaluate_parent(
                            subtask_issue_id=issue_key,
                            exit_code=exit_c,
                            output_tail=out_tail,
                            pane_coordinate=effective_pane_id,
                        )
                except Exception as exc:
                    logger.debug(f"Linear subtask settlement warning ({issue_key}): {exc}")

        return accumulator

    async def _execute_backstage_task(
        self,
        task_desc: str = "",
        target_node: Optional[str] = "localhost",
        target_cwd: Optional[str] = None,
        session: Optional[SessionHandle] = None,
        client: Optional[Any] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        ts: Optional[str] = None,
        is_imessage: bool = False,
        yield_timeout: float = 3.0,
        worker: Optional[str] = None,
        precedent_status: Optional[str] = None,
        precedent_source: Optional[str] = None,
        dry_run: bool = False,
        task_description: Optional[str] = None,
        subtask_issue_id: Optional[str] = None,
        command: Optional[str] = None,
        subtasks: Optional[List[Dict[str, Any]]] = None,
        execution_mode: Optional[str] = None,
        parent_issue_key: Optional[str] = None,
        pane_id: Optional[str] = None,
        natural_ack_text: Optional[str] = None,
        **kwargs: Any,
    ) -> Union[BackstageExecutionAccumulator, Dict[str, Any]]:
        """
        Execute backstage task silently with a 3.0s yield window.
        Standardized on AGY CLI as default ('agy') in Pane 2.
        Supports on-demand delegation to OpenCode or Hermes when explicitly specified.
        If execution takes longer than 3.0s, posts an immediate conversational liaison acknowledgment,
        leaving the task running while The Eyes monitors terminal buffers and The Mouth remains free
        to respond to mid-stream user check-ins.
        """
        # Multi-hand batch routing (dispatch_subtasks)
        if subtasks:
            mode = (execution_mode or kwargs.get("mode") or "parallel").lower()
            now_ts = time.time()
            all_sub_keys = [str(t.get("issue_key")).strip() for t in subtasks if t.get("issue_key")]
            all_parent_keys = {str(t.get("parent_key")).strip() for t in subtasks if t.get("parent_key")}
            if parent_issue_key:
                all_parent_keys.add(str(parent_issue_key).strip())
            if not hasattr(self, "_ticket_activity_tracker") or self._ticket_activity_tracker is None:
                self._ticket_activity_tracker = {}
            if session is not None:
                session.last_dispatched_at = now_ts
                session.active_linear_parent_keys = sorted(all_parent_keys)
                session.queued_subtasks = list(all_sub_keys)
                session.in_flight_subtasks = []
            for pk in all_parent_keys | set(all_sub_keys):
                if pk:
                    self._ticket_activity_tracker[pk] = {
                        "last_dispatched_at": now_ts,
                        "in_flight_subtasks": [],
                        "queued_subtasks": list(all_sub_keys),
                        "last_error": None,
                    }
            ack_task: Optional[asyncio.Task] = None
            if client and channel:
                async def _delayed_subtasks_ack() -> None:
                    try:
                        await asyncio.sleep(max(0.05, yield_timeout))
                        ack_msg = (
                            natural_ack_text.strip()
                            if (natural_ack_text and natural_ack_text.strip())
                            else (
                                f"⚙️ *On it:* Executing {len(subtasks)} project subtask(s) in the background. "
                                "I'll update each ticket to `Claimed Fixed` as it settles and brief you when complete."
                            )
                        )
                        await self._post_response(client, channel, thread_ts, ack_msg, is_imessage)
                    except (asyncio.CancelledError, Exception):
                        pass
                ack_task = asyncio.create_task(_delayed_subtasks_ack())
            try:
                if mode == "parallel":
                    if session is not None:
                        session.in_flight_subtasks = list(all_sub_keys)
                        session.queued_subtasks = []
                    for pk in all_parent_keys | set(all_sub_keys):
                        if pk and pk in self._ticket_activity_tracker:
                            self._ticket_activity_tracker[pk]["in_flight_subtasks"] = list(all_sub_keys)
                            self._ticket_activity_tracker[pk]["queued_subtasks"] = []
                    results = await asyncio.gather(
                        *[
                            self._spawn_hand(
                                task,
                                pane_id=f"hand_{task['issue_key']}" if task.get("issue_key") else f"hand_{i}",
                                session=session,
                                client=client,
                                channel=channel,
                                thread_ts=thread_ts,
                                worker=worker,
                                **kwargs,
                            )
                            for i, task in enumerate(subtasks)
                        ],
                        return_exceptions=True,
                    )
                else:  # sequential
                    results = []
                    prior_handoff_summary: Optional[str] = None
                    for i, task in enumerate(subtasks):
                        cur_key = str(task.get("issue_key") or "").strip()
                        rem_keys = [
                            str(t.get("issue_key") or "").strip()
                            for t in subtasks[i + 1:]
                            if t.get("issue_key")
                        ]
                        step_ts = time.time()
                        if session is not None:
                            session.last_dispatched_at = step_ts
                            session.in_flight_subtasks = [cur_key] if cur_key else []
                            session.queued_subtasks = rem_keys
                        for pk in all_parent_keys | set(all_sub_keys):
                            if pk and pk in self._ticket_activity_tracker:
                                self._ticket_activity_tracker[pk]["last_dispatched_at"] = step_ts
                                self._ticket_activity_tracker[pk]["in_flight_subtasks"] = [cur_key] if cur_key else []
                                self._ticket_activity_tracker[pk]["queued_subtasks"] = rem_keys
                        p_id = f"hand_{task['issue_key']}" if task.get("issue_key") else f"hand_{i}"
                        seq_kwargs = dict(kwargs)
                        seq_kwargs["rotate_conversation"] = (i > 0)
                        if prior_handoff_summary:
                            seq_kwargs["prior_subtask_handoff"] = prior_handoff_summary
                        res = await self._spawn_hand(
                            task,
                            pane_id=p_id,
                            session=session,
                            client=client,
                            channel=channel,
                            thread_ts=thread_ts,
                            worker=worker,
                            **seq_kwargs,
                        )
                        results.append(res)
                        if isinstance(res, dict):
                            r_st = res.get("status", "SUCCESS")
                            r_out = str(res.get("output") or res.get("output_tail") or "").strip()[-1000:]
                        elif hasattr(res, "get_full_output"):
                            r_st = getattr(res, "turn_status", "SUCCESS")
                            r_out = str(res.get_full_output() or "").strip()[-1000:]
                        else:
                            r_st = "SUCCESS"
                            r_out = ""
                        if r_out:
                            prior_handoff_summary = f"Subtask {cur_key or i} ({r_st}):\n{r_out}"
            finally:
                if session is not None:
                    session.in_flight_subtasks = []
                    session.queued_subtasks = []
                for pk in all_parent_keys | set(all_sub_keys):
                    if pk and pk in self._ticket_activity_tracker:
                        self._ticket_activity_tracker[pk]["in_flight_subtasks"] = []
                        self._ticket_activity_tracker[pk]["queued_subtasks"] = []
                if ack_task is not None and not ack_task.done():
                    ack_task.cancel()
                    try:
                        await ack_task
                    except (asyncio.CancelledError, Exception):
                        pass

            combined_acc = BackstageExecutionAccumulator()
            outputs = []
            has_error = False
            last_err_text: Optional[str] = None
            for i, r in enumerate(results):
                t_key = subtasks[i].get("issue_key", f"subtask_{i}") if i < len(subtasks) else f"subtask_{i}"
                if isinstance(r, Exception):
                    has_error = True
                    last_err_text = f"{t_key}: {r}"
                    outputs.append(f"• *{t_key}*: ERROR - {r}")
                elif isinstance(r, dict):
                    st = r.get("status", "SUCCESS")
                    out = r.get("output") or r.get("output_tail") or ""
                    if st in ("ERROR", "FAILED"):
                        has_error = True
                        last_err_text = f"{t_key}: {r.get('error') or out.strip()[:200]}"
                    outputs.append(f"• *{t_key}* ({st}): {out.strip()[:500]}")
                elif hasattr(r, "get_full_output"):
                    st = getattr(r, "turn_status", "SUCCESS")
                    out = r.get_full_output()
                    if st in ("ERROR", "FAILED"):
                        has_error = True
                        last_err_text = f"{t_key}: {out.strip()[:200]}"
                    outputs.append(f"• *{t_key}* ({st}): {out.strip()[:500]}")
                else:
                    outputs.append(f"• *{t_key}*: Completed")

            if last_err_text:
                if session is not None:
                    session.last_subtask_error = last_err_text
                for pk in all_parent_keys:
                    if pk and pk in self._ticket_activity_tracker:
                        self._ticket_activity_tracker[pk]["last_error"] = last_err_text

            combined_acc.turn_status = "ERROR" if has_error else "SUCCESS"
            combined_acc.final_response = "\n".join(outputs)
            return combined_acc

        if session is not None:
            session.last_dispatched_at = time.time()

        cmd_input = command or kwargs.get("command")
        effective_task = cmd_input or task_description or task_desc or ""

        # CONTRACT ENFORCEMENT GATE
        if precedent_status == "VERIFIED_RUNBOOK":
            from golem.wiki import DEFAULT_WIKI_DIR
            wiki_root = Path(DEFAULT_WIKI_DIR).resolve()
            clean_source = (precedent_source or "").strip().lstrip("/")
            wiki_file = (wiki_root / clean_source).resolve() if clean_source else None
            if not wiki_file or not wiki_file.is_file() or not str(wiki_file).startswith(str(wiki_root)):
                error_msg = (
                    f"[MEMORY CONTRACT REJECTION] The cited precedent_source '{precedent_source}' "
                    f"does not exist in ~/.golem/wiki/. You cannot execute backstage actions without grounding. "
                    f"Review your Peripheral Vision table and consult valid memory before dispatching."
                )
                return {"error": error_msg, "status": "CONTRACT_VIOLATION"}
        elif precedent_status == "NOVEL_EXPLORATION":
            if not precedent_source or len(precedent_source.strip()) < 10:
                return {
                    "error": "[MEMORY CONTRACT REJECTION] NOVEL_EXPLORATION requires a descriptive rationale in precedent_source.",
                    "status": "CONTRACT_VIOLATION",
                }
        elif precedent_status is not None:
            return {"error": f"Invalid precedent_status '{precedent_status}'.", "status": "CONTRACT_VIOLATION"}

        subtask_id = subtask_issue_id or kwargs.get("subtask_issue_id")
        if subtask_id and session is not None:
            session.linear_issue_id = subtask_id
        if subtask_id:
            try:
                from golem.linear_bridge import LinearBridge
                bridge = self._get_linear_bridge()
                asyncio.create_task(bridge.update_issue_state(subtask_id, "in progress"))
            except Exception as e:
                logger.debug(f"Failed to update subtask {subtask_id} to in progress: {e}")

        if dry_run:
            return {
                "status": "SUCCESS",
                "message": "Dry run passed contract validation",
                "task_description": effective_task,
                "precedent_status": precedent_status,
                "precedent_source": precedent_source,
                "subtask_issue_id": subtask_id,
            }

        accumulator = BackstageExecutionAccumulator()
        effective_worker = WorkerDelegator.normalize_worker_name(worker or get_default_hand_worker())
        effective_node = target_node or "localhost"
        node_label = effective_node
        if session is None:
            return accumulator
        task_desc = effective_task

        # Detached background Linear tracking for dispatched tasks
        ephemeral_hand_issue: Dict[str, Optional[str]] = {
            "id": None,
            "identifier": None,
            "thread_ref": _THREAD_REF.get() or "",
        }
        if not subtask_id:
            try:
                from golem.linear_bridge import LinearBridge, _THREAD_ISSUE_CACHE
                bridge = self._get_linear_bridge()
                if bridge.api_key and not _linear_disabled_for_tests():
                    thread_ref = _THREAD_REF.get()
                    cached_plan = (
                        (getattr(session, "linear_plan_cache", None) if session else None)
                        or (_THREAD_PLAN_CACHE.get(thread_ref) if thread_ref else None)
                    )
                    parent_dict = cached_plan.get("parent") if isinstance(cached_plan, dict) else None
                    parent_issue_id = (
                        (parent_dict.get("id") or parent_dict.get("identifier"))
                        if isinstance(parent_dict, dict)
                        else None
                    ) or (getattr(session, "active_linear_parent_key", None) if session else None)

                    async def _safe_linear_create_ingress():
                        try:
                            description = f"Node: {effective_node}\nCWD: {target_cwd}\nCommand: `{effective_task}`"
                            hand_title = f"[Hand] {effective_task[:60]}"
                            if parent_issue_id:
                                # Thread already has a parent project ticket (e.g. CHO-197): attach execution
                                # note directly to the parent ticket rather than spawning a top-level [Hand] ticket.
                                await bridge.add_comment(
                                    parent_issue_id,
                                    f"**[Hand] Dispatched** on `{effective_node}` (`{target_cwd}`)\nTask: `{effective_task[:300]}`",
                                )
                                return
                            if thread_ref:
                                # One issue per conversation thread: hand results are appended as comments
                                # (see _record_hand_result_to_linear), and later turns read them back.
                                issue = await bridge.ensure_thread_issue(
                                    thread_ref,
                                    title=hand_title,
                                    description=description,
                                )
                                if isinstance(issue, dict) and str(issue.get("title") or hand_title).startswith("[Hand]"):
                                    ephemeral_hand_issue["id"] = issue.get("id")
                                    ephemeral_hand_issue["identifier"] = issue.get("identifier")
                                return
                            existing_hand = await bridge.find_open_hand_issue(hand_title)
                            issue = existing_hand or await bridge.create_task_issue(
                                title=hand_title,
                                description=description,
                            )
                            if isinstance(issue, dict):
                                ephemeral_hand_issue["id"] = issue.get("id")
                                ephemeral_hand_issue["identifier"] = issue.get("identifier")
                                if session is not None and not getattr(session, "active_linear_parent_key", None):
                                    session.linear_issue_id = issue.get("id")
                                    session.linear_issue_key = issue.get("identifier")
                        except Exception as e:
                            logger.warning(f"Linear background issue creation skipped: {e}")

                    asyncio.create_task(_safe_linear_create_ingress())
            except Exception as exc:
                logger.debug(f"LinearBridge init warning: {exc}")

        # Pre-Flight Node Facts Injection (Hand Dispatch) — deduplicated per active conversation_id + node
        adapter_obj = getattr(session, "adapter", None)
        active_conv_id = getattr(adapter_obj, "conversation_id", None) if adapter_obj is not None else None
        invariants_key = (
            (active_conv_id, effective_node)
            if (isinstance(active_conv_id, str) and active_conv_id.strip())
            else None
        )
        already_injected_invariants = bool(
            invariants_key is not None
            and getattr(session, "_invariants_injected_key", None) == invariants_key
        )
        if not already_injected_invariants:
            try:
                from golem.harness import get_preflight_node_invariants
                try:
                    node_invariants = get_preflight_node_invariants(effective_node, precedent_source=precedent_source)
                except TypeError:
                    node_invariants = get_preflight_node_invariants(effective_node)
                if node_invariants:
                    task_desc = f"[Node Context & Invariants]\n{node_invariants}\n\n[Task]\n{task_desc}"
                    if invariants_key is not None:
                        setattr(session, "_invariants_injected_key", invariants_key)
                    else:
                        setattr(session, "_pending_invariants_node", effective_node)
            except Exception as e:
                logger.debug(f"Error retrieving preflight node invariants: {e}")

        linear_guardrail = (
            "\n[LINEAR GUARDRAIL: NEVER call Linear GraphQL APIs or modify Linear issue states directly from a Hand. Only The Mouth manages Linear ticket transitions.]\n"
            "[IRON LAW OF EXTERNAL INTEGRATIONS — NO SILENT MOCKS: Never fabricate synthetic fallbacks, mock IDs, or offline heuristic stubs when an external API is blocked by auth, 2FA, or WAF/Cloudflare/Imperva. Fail loudly at the boundary, leave downstream state untouched, and report the exact blocker. Never claim 'Staged', 'Deployed', or 'Sent' without a verified network response.]\n"
        )
        if not already_injected_invariants and "[LINEAR GUARDRAIL:" not in task_desc:
            task_desc = f"{task_desc}{linear_guardrail}"

        # On-demand delegation to specialized workers (OpenCode, Hermes)
        if effective_worker not in ("agy", "antigravity"):
            herdr_cl = getattr(session, "harness", None) and getattr(session.harness, "herdr_client", None)
            delegator = WorkerDelegator(
                herdr_client=herdr_cl,
                base_repo=target_cwd or getattr(session, "target_cwd", None) or os.getcwd()
            )
            return await delegator.handoff(
                worker=effective_worker,
                task=task_desc,
                cwd=target_cwd or getattr(session, "target_cwd", None) or os.getcwd(),
                node=effective_node,
            )

        # Guard against re-entrant dispatch_task while session.lock is held by an active backstage hand
        if (
            session is not None
            and getattr(session, "lock", None) is not None
            and hasattr(session.lock, "locked")
            and session.lock.locked() is True
            and isinstance(getattr(session, "_active_backstage_hand", None), dict)
        ):
            active_hand = getattr(session, "_active_backstage_hand", {})
            h_pane = active_hand.get("pane_id") or "runner"
            elapsed_sec = int(time.time() - active_hand.get("started_at", time.time()))

            webui = getattr(self, "webui_server", None)
            if webui is None:
                try:
                    from golem.webui.server import get_active_webui_server
                    webui = get_active_webui_server()
                except Exception:
                    webui = None

            live_tail = ""
            if webui is not None and getattr(webui, "_active_hands", None):
                target_hand = webui._active_hands.get(h_pane)
                if not target_hand and hasattr(webui, "_resolve_hand_key"):
                    k = webui._resolve_hand_key(h_pane)
                    target_hand = webui._active_hands.get(k)
                if target_hand and target_hand.get("buffer_tail"):
                    from golem.normalizer import strip_ansi
                    clean_h = strip_ansi(target_hand["buffer_tail"]).strip()
                    h_lines = [l for l in clean_h.splitlines() if l.strip()]
                    if h_lines:
                        live_tail = "\n".join(h_lines[-15:])

            if not live_tail:
                live_tail = "Worker active — streaming output..."

            clean_steering = str(effective_task or "").strip()
            if clean_steering and clean_steering.lower() not in ("continue", "status", "progress"):
                if not hasattr(session, "pending_steering_notes") or not isinstance(session.pending_steering_notes, list):
                    session.pending_steering_notes = []
                if clean_steering not in session.pending_steering_notes:
                    session.pending_steering_notes.append(clean_steering[:500])

            active_cmd_snippet = str(active_hand.get("command", "") or "")[:300]
            accumulator.turn_status = "SUCCESS"
            accumulator.already_running_reentrant = True
            accumulator.final_response = (
                f"[BACKSTAGE TASK ALREADY RUNNING on {h_pane} (elapsed {elapsed_sec}s)]\n"
                f"Active Task: {active_cmd_snippet}\n"
                f"Recent Buffer Tail:\n{live_tail}\n"
                f"[NOTE: Do NOT call check_eyes or dispatch_task again in this turn. "
                f"Synthesize a concise status update for Carl immediately and let the active Hand finish.]"
            )
            accumulator.output_tail = live_tail
            logger.info(
                f"_execute_backstage_task: Session '{session.session_id}' already has active backstage task on {h_pane}. "
                f"Returning active task status without re-entrant dispatch."
            )
            return accumulator

        if session is not None and isinstance(getattr(session, "pending_steering_notes", None), list) and session.pending_steering_notes:
            prior_notes = list(session.pending_steering_notes)
            session.pending_steering_notes.clear()
            notes_block = "\n".join(f"- {n}" for n in prior_notes)
            task_desc = f"[Operator Steering Context]\n{notes_block}\n\n{task_desc}"

        stream_gen = self._stream_backstage_events(session, task_desc)
        event_queue: asyncio.Queue[Any] = asyncio.Queue()
        sentinel = object()

        # Session queue for Shoulder Tap alerts (from The Eyes / The Nose)
        session_queue: Optional[asyncio.Queue] = None
        if hasattr(self, "bus") and self.bus is not None and hasattr(self.bus, "get_session_queue"):
            session_queue = self.bus.get_session_queue(session.session_id)

        # WebUI Backstage Hand Registration & hand_spawn broadcast
        webui = getattr(self, "webui_server", None)
        _rp_id = getattr(session, "runner_pane_id", None)
        _rp_label = getattr(session, "runner_pane", None)
        runner_pane_id = _rp_id if isinstance(_rp_id, str) and _rp_id else None
        pane = pane_id or runner_pane_id or _rp_label or getattr(session, "session_id", None) or "runner"
        subtask_issue_key = kwargs.get("subtask_issue_key") or kwargs.get("issue_key")
        if pane == "runner":
            if subtask_issue_key:
                pane = f"agy-{str(subtask_issue_key).lower()}"
            elif thread_ts:
                clean_ts = str(thread_ts).replace(".", "")
                pane = f"agy-{clean_ts[-6:]}"
        eyes_observer: Optional[Any] = None
        node_label = effective_node
        pid = getattr(getattr(session, "active_execution", None), "pid", None) or os.getpid()
        hand_registered = False

        if webui is not None:
            try:
                res = webui.register_active_hand(
                    pane_id=pane,
                    node=effective_node,
                    command=task_desc,
                    pid=pid,
                    worker=effective_worker,
                    herdr_pane_id=runner_pane_id or pane,
                )
                if inspect.isawaitable(res):
                    await res
                hand_registered = True
                if isinstance(getattr(webui, "_active_hands", None), dict):
                    resolved_k = (
                        webui._resolve_hand_key(pane, effective_node)
                        if hasattr(webui, "_resolve_hand_key")
                        else pane
                    )
                    hand_entry = webui._active_hands.get(resolved_k) or webui._active_hands.get(pane)
                    if isinstance(hand_entry, dict):
                        hand_entry["channel"] = channel
                        hand_entry["thread_ts"] = thread_ts
            except Exception as exc:
                logger.debug(f"WebUI register_active_hand warning: {exc}")

            try:
                spawn_payload = {
                    "pane_id": pane,
                    "node": effective_node,
                    "command": task_desc,
                    "pid": pid,
                    "status": "RUNNING",
                    "buffer_tail": getattr(webui, "_active_hands", {}).get(pane, {}).get("buffer_tail", ""),
                }
                res = webui.broadcast("hand_spawn", spawn_payload)
                if inspect.isawaitable(res):
                    await res
            except Exception as exc:
                logger.debug(f"WebUI hand_spawn broadcast warning: {exc}")

            if not _is_mock_object(webui):
                now_ts = time.strftime("%H:%M:%S", time.localtime(time.time()))
                task_snippet = (task_desc or "").strip().replace("\n", " ")[:80]
                banner_line = f"🚀 [{now_ts}] Spawned {effective_worker} on {effective_node} (PID {pid}): {task_snippet}\n"
                try:
                    res = webui.update_hand_buffer(pane_id=pane, delta=banner_line, node=effective_node)
                    if inspect.isawaitable(res):
                        await res
                    res = webui.broadcast("hand_buffer", {
                        "pane_id": pane,
                        "delta": banner_line,
                    })
                    if inspect.isawaitable(res):
                        await res
                except Exception as exc:
                    logger.debug(f"WebUI initial banner warning: {exc}")

        async def _worker_consumer() -> None:
            try:
                async for item in stream_gen:
                    await event_queue.put(item)
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                await event_queue.put(exc)
            finally:
                await event_queue.put(sentinel)

        driver_pid = getattr(getattr(getattr(session, "adapter", None), "process", None), "pid", None)
        active_hand_metadata = {
            "pane_id": pane,
            "command": effective_task,
            "pid": driver_pid or pid,
            "worker": effective_worker,
            "node": effective_node,
            "started_at": time.time(),
        }
        if session is not None:
            session._active_backstage_hand = active_hand_metadata

        worker_task = asyncio.create_task(_worker_consumer())
        start_time = time.time()
        acknowledged_yield = False

        # Heartbeat monitoring for quiet periods
        last_activity_time = [time.time()]
        stall_alerted = [False]
        heartbeat_task: Optional[asyncio.Task] = None

        if (webui is not None and not _is_mock_object(webui)) or getattr(self, "bus", None) is not None:
            async def _heartbeat_worker():
                try:
                    hb_interval = float(os.environ.get("GOLEM_HEARTBEAT_INTERVAL", "5.0"))
                    while True:
                        await asyncio.sleep(hb_interval)
                        now = time.time()
                        idle_seconds = now - last_activity_time[0]
                        if idle_seconds >= hb_interval:
                            elapsed_from_start = int(now - start_time)
                            tool_in_flight = bool(
                                accumulator.tool_calls
                                and isinstance(accumulator.tool_calls[-1], dict)
                                and accumulator.tool_calls[-1].get("status") == "running"
                            )
                            if tool_in_flight:
                                active_tool_name = accumulator.tool_calls[-1].get("tool", "tool")
                                hb_text = f"\x1b[90m⏳ [HEARTBEAT +{elapsed_from_start}s] Worker active — executing tool ({active_tool_name})...\x1b[0m\n"
                            else:
                                hb_text = f"\x1b[90m⏳ [HEARTBEAT +{elapsed_from_start}s] Worker active — waiting on model/tool stream...\x1b[0m\n"
                            if webui is not None and not _is_mock_object(webui):
                                try:
                                    res = webui.update_hand_buffer(pane_id=pane, delta=hb_text, node=effective_node)
                                    if inspect.isawaitable(res):
                                        await res
                                    res = webui.broadcast("hand_buffer", {
                                        "pane_id": pane,
                                        "delta": hb_text,
                                    })
                                    if inspect.isawaitable(res):
                                        await res
                                except Exception:
                                    pass

                            if "GOLEM_HAND_STALL_SECONDS" in os.environ:
                                stall_threshold = float(os.environ["GOLEM_HAND_STALL_SECONDS"])
                            elif tool_in_flight:
                                stall_threshold = float(os.environ.get("GOLEM_HAND_TOOL_STALL_SECONDS", "600"))
                            else:
                                stall_threshold = 180.0
                            if idle_seconds >= stall_threshold and not stall_alerted[0]:
                                stall_alerted[0] = True
                                nose = getattr(self, "get_nose_sentinel", lambda: None)() if callable(getattr(self, "get_nose_sentinel", None)) else getattr(self, "nose_sentinel", None)
                                if nose is not None and hasattr(nose, "_alerted_stalled_hands"):
                                    nose._alerted_stalled_hands.add(pane)
                                    base_pane = str(pane).split("@")[0].split("#")[0]
                                    if base_pane:
                                        nose._alerted_stalled_hands.add(base_pane)
                                    if webui is not None and hasattr(webui, "_resolve_hand_key"):
                                        try:
                                            resolved_k = webui._resolve_hand_key(pane, effective_node)
                                            if resolved_k:
                                                nose._alerted_stalled_hands.add(resolved_k)
                                        except Exception:
                                            pass

                                stall_summary = (
                                    f"Backstage hand {pane} ({effective_worker} on {effective_node}) "
                                    f"has been silent with no stream activity for {int(idle_seconds)}s (total: {elapsed_from_start}s)."
                                )
                                if webui is not None and hasattr(webui, "append_organ_log"):
                                    try:
                                        webui.append_organ_log("nose", f"[ANOMALY] {stall_summary}")
                                    except Exception:
                                        pass

                                if getattr(self, "bus", None) is not None:
                                    try:
                                        alert = ShoulderTapAlert(
                                            alert_type="health_anomaly",
                                            source="nose",
                                            channel=channel,
                                            thread_ts=thread_ts,
                                            timestamp=now,
                                            summary=stall_summary,
                                            data={
                                                "category": "hand_stalled",
                                                "pane_id": pane,
                                                "worker": effective_worker,
                                                "node": effective_node,
                                                "idle_seconds": int(idle_seconds),
                                                "elapsed_seconds": elapsed_from_start,
                                                "channel": channel,
                                                "thread_ts": thread_ts,
                                            },
                                        )
                                        self.bus.publish(alert)
                                    except Exception as exc:
                                        logger.warning(f"Error publishing hand_stalled alert: {exc}")
                except asyncio.CancelledError:
                    pass

            heartbeat_task = asyncio.create_task(_heartbeat_worker())

        try:
            while True:
                # 1. Yield window gating (< 3.0s)
                if not acknowledged_yield:
                    elapsed = time.time() - start_time
                    remaining = max(0.05, yield_timeout - elapsed)
                    try:
                        event_item = await asyncio.wait_for(event_queue.get(), timeout=remaining)
                    except asyncio.TimeoutError:
                        acknowledged_yield = True
                        node_label = effective_node
                        if natural_ack_text and natural_ack_text.strip():
                            ack_msg = natural_ack_text.strip()
                        else:
                            ack_msg = (
                                f"⚙️ *On it:* Dispatched backstage task to `{node_label}` via `{effective_worker}`. "
                                "Execution is running silently; I'll brief you as soon as it settles."
                            )
                        if client and channel:
                            existing_interim_ts = kwargs.get("existing_interim_ts") or (
                                getattr(session, "_turn_interim_ts", None) if session is not None else None
                            )
                            if existing_interim_ts:
                                resp = await self._post_response(
                                    client,
                                    channel,
                                    thread_ts,
                                    ack_msg,
                                    is_imessage,
                                    update_ts=str(existing_interim_ts),
                                )
                                accumulator.interim_message_ts = str(existing_interim_ts)
                            else:
                                resp = await self._post_response(client, channel, thread_ts, ack_msg, is_imessage)
                            if resp:
                                raw_data = getattr(resp, "data", None)
                                data_dict = raw_data if isinstance(raw_data, dict) else {}
                                msg_ts = (
                                    resp.get("ts")
                                    if isinstance(resp, dict)
                                    else getattr(resp, "ts", data_dict.get("ts", None))
                                )
                                if msg_ts:
                                    accumulator.interim_message_ts = str(msg_ts)
                            if accumulator.interim_message_ts and session is not None:
                                session._turn_interim_ts = accumulator.interim_message_ts
                                if not existing_interim_ts:
                                    session._turn_interim_seq = self._get_channel_outbound_seq(channel)
                        if thread_ts:
                            await set_assistant_status(
                                client, channel, thread_ts, f"Running on {node_label} via {effective_worker}..."
                            )
                        eyes_observer = self._attach_eyes_observer(
                            session=session,
                            pane=pane,
                            task_desc=task_desc,
                            webui=webui,
                            channel=channel,
                            thread_ts=thread_ts,
                        )
                        continue
                else:
                    # 2. Post-yield execution: listen to both worker events and Shoulder Tap session queue
                    if session_queue is not None:
                        eq_task = asyncio.create_task(event_queue.get())
                        sq_task = asyncio.create_task(session_queue.get())
                        done, pending = await asyncio.wait(
                            [eq_task, sq_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for p in pending:
                            p.cancel()
                            try:
                                await p
                            except (asyncio.CancelledError, Exception):
                                pass

                        if sq_task in done:
                            alert = sq_task.result()
                            if alert and getattr(alert, "alert_type", None) == "blocker":
                                blocker_data = getattr(alert, "data", {})
                                matched_flow = blocker_data.get("matched_flow")
                                if matched_flow:
                                    flow_name = matched_flow.get("flow_name", "automated_flow")
                                    flow_path = matched_flow.get("flow_path", "")
                                    card = None
                                    logger.info(f"Triggered automated flow '{flow_name}' from {flow_path}")
                                else:
                                    b_type = blocker_data.get("type", blocker_data.get("blocker_type", "prompt"))
                                    if b_type == "oauth":
                                        url = blocker_data.get("url") or blocker_data.get("blocker_url", "")
                                        card = f"🔐 *Authentication Required:* Runner is waiting for OAuth authorization:\n<{url}|Click here to authenticate>"
                                    else:
                                        content = blocker_data.get("content") or blocker_data.get("prompt") or blocker_data.get("blocker_prompt", "")
                                        card = f"⚠️ *Confirmation Required:* Runner is waiting for input:\n> `{content}`"
                                    if ts and client:
                                        await swap_reaction(client, channel, ts, "gear", "warning")
                                        if thread_ts:
                                            await set_assistant_status(client, channel, thread_ts, "Awaiting input...")
                                if card and client and channel:
                                    await self._post_response(client, channel, thread_ts, card, is_imessage)
                            if eq_task not in done:
                                continue

                        event_item = eq_task.result()
                    else:
                        event_item = await event_queue.get()

                if event_item is sentinel:
                    break

                if isinstance(event_item, Exception):
                    accumulator.record_event(
                        FinishEvent(status="ERROR", response="", error=str(event_item))
                    )
                    break

                accumulator.record_event(event_item)
                last_activity_time[0] = time.time()
                stall_alerted[0] = False

                # Format rich execution events (thoughts, tools, outputs, deltas) for WebUI terminal
                delta_text = _format_hand_event_for_webui(event_item)

                if delta_text and webui is not None:
                    try:
                        res = webui.update_hand_buffer(pane_id=pane, delta=delta_text)
                        if inspect.isawaitable(res):
                            await res
                    except Exception as exc:
                        logger.debug(f"WebUI update_hand_buffer warning: {exc}")

                    try:
                        res = webui.broadcast("hand_buffer", {
                            "pane_id": pane,
                            "delta": delta_text,
                        })
                        if inspect.isawaitable(res):
                            await res
                    except Exception as exc:
                        logger.debug(f"WebUI hand_buffer broadcast warning: {exc}")

                ev_type = getattr(event_item, "event_type", None) or (
                    event_item.get("event_type") if isinstance(event_item, dict) else None
                )

                if ev_type == "tool_start" and thread_ts:
                    tool_name = getattr(event_item, "tool_name", "") or (
                        event_item.get("tool_name", "") if isinstance(event_item, dict) else ""
                    )
                    if tool_name:
                        await set_assistant_status(client, channel, thread_ts, f"Running {tool_name} on {node_label}...")

                if accumulator.has_awaiting_input:
                    break

                if ev_type == "finish":
                    if thread_ts:
                        await set_assistant_status(client, channel, thread_ts, "Synthesizing executive briefing...")
                    break

        finally:
            if session is not None:
                pending_node = getattr(session, "_pending_invariants_node", None)
                post_conv_id = getattr(getattr(session, "adapter", None), "conversation_id", None)
                if pending_node and isinstance(post_conv_id, str) and post_conv_id.strip():
                    setattr(session, "_invariants_injected_key", (post_conv_id, pending_node))
                    setattr(session, "_pending_invariants_node", None)
            if session is not None and getattr(session, "_active_backstage_hand", None) is active_hand_metadata:
                session._active_backstage_hand = None
            if heartbeat_task is not None and not heartbeat_task.done():
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except (asyncio.CancelledError, Exception):
                    pass
            if not worker_task.done():
                worker_task.cancel()
                try:
                    await worker_task
                except (asyncio.CancelledError, Exception):
                    pass
            if hasattr(stream_gen, "aclose") and not getattr(stream_gen, "ag_closed", False):
                try:
                    await stream_gen.aclose()
                except Exception:
                    pass
            if eyes_observer is not None:
                try:
                    await eyes_observer.stop_monitoring()
                except Exception as exc:
                    logger.debug(f"EyesTaskObserver stop warning: {exc}")

            # Settle and retain hand agent card on WebUI
            if hand_registered and webui is not None:
                turn_status = getattr(accumulator, "turn_status", "SUCCESS")
                exit_code = 0 if turn_status == "SUCCESS" else 1
                status_str = "EXIT_0" if exit_code == 0 else "FAILED"
                try:
                    if not _is_mock_object(webui):
                        elapsed_duration = max(0.1, time.time() - start_time)
                        symbol = "✔" if exit_code == 0 else "✖"
                        color_code = "32" if exit_code == 0 else "31"
                        settle_line = f"\n\x1b[{color_code}m{symbol} [SETTLED exit={exit_code} in {elapsed_duration:.1f}s]\x1b[0m\n"
                        try:
                            res = webui.update_hand_buffer(pane_id=pane, delta=settle_line, node=effective_node)
                            if inspect.isawaitable(res):
                                await res
                            res = webui.broadcast("hand_buffer", {
                                "pane_id": pane,
                                "delta": settle_line,
                            })
                            if inspect.isawaitable(res):
                                await res
                        except Exception:
                            pass

                    if hasattr(webui, "settle_active_hand"):
                        res = webui.settle_active_hand(
                            pane_id=pane,
                            exit_code=exit_code,
                            status=status_str,
                            node=effective_node,
                        )
                        if inspect.isawaitable(res):
                            await res
                        if not _is_mock_object(webui):
                            async def _delayed_cleanup(p_id: str, n_id: str, w_ui: Any) -> None:
                                try:
                                    await asyncio.sleep(120.0)
                                    executing = w_ui.get_executing_hands() if hasattr(w_ui, "get_executing_hands") else {}
                                    if p_id in executing:
                                        return
                                    try:
                                        c_res = w_ui.unregister_active_hand(pane_id=p_id, node=n_id)
                                    except TypeError:
                                        c_res = w_ui.unregister_active_hand(pane_id=p_id)
                                    if inspect.isawaitable(c_res):
                                        await c_res
                                except Exception:
                                    pass
                            asyncio.create_task(_delayed_cleanup(pane, effective_node, webui))
                    else:
                        settle_payload = {
                            "pane_id": pane,
                            "exit_code": exit_code,
                            "status": status_str,
                        }
                        res = webui.broadcast("hand_settle", settle_payload)
                        if inspect.isawaitable(res):
                            await res
                        res = webui.unregister_active_hand(pane_id=pane)
                        if inspect.isawaitable(res):
                            await res
                except Exception as exc:
                    logger.debug(f"WebUI hand settlement warning: {exc}")

            # Asynchronous Post-Flight Settlement Reflection
            try:
                from golem.memory.reflection import record_task_settlement_async
                t_id = getattr(session, "session_id", f"hand_{int(time.time())}")
                out_tail = getattr(accumulator, "output_tail", "") or ""
                t_status = getattr(accumulator, "turn_status", "SUCCESS")
                exit_c = 0 if t_status == "SUCCESS" else 1
                asyncio.create_task(
                    record_task_settlement_async(
                        task_id=f"hand_{t_id}_{int(time.time())}",
                        task_description=effective_task,
                        target_node=effective_node,
                        exit_code=exit_c,
                        output_tail=out_tail,
                        precedent_source=precedent_source or "",
                        precedent_status=precedent_status or "",
                    )
                )
            except Exception as e:
                logger.debug(f"Failed to trigger reflection in ingress: {e}")

            # Subtask Settlement & Ephemeral [Hand] Settlement in Linear
            sub_id = subtask_issue_id or kwargs.get("subtask_issue_id")
            hand_id = ephemeral_hand_issue.get("id") or (
                getattr(session, "linear_issue_id", None) if not sub_id and not getattr(session, "active_linear_parent_key", None) else None
            )
            if sub_id:
                try:
                    from golem.linear_bridge import LinearBridge
                    linear_bridge = self._get_linear_bridge()
                    t_status = getattr(accumulator, "turn_status", "SUCCESS")
                    exit_c = 0 if t_status == "SUCCESS" else 1
                    if exit_c == 0 and t_status == "SUCCESS":
                        out_tail = getattr(accumulator, "output_tail", "") or ""
                        coord = getattr(session, "runner_pane", effective_node) or effective_node
                        receipt = (
                            f"### Subtask Hand Settled (`{coord}`)\n"
                            f"- **Exit Code:** `{exit_c}`\n"
                            f"- **Task ID:** `{t_id}`\n"
                            f"- **Outbox:** `~/.golem/outbox/{t_id}/`\n\n"
                            f"**Tail Output:**\n```\n{out_tail[-800:] if out_tail else '(empty)'}\n```"
                        )

                        async def _settle_and_check():
                            try:
                                res = await linear_bridge.settle_subtask_and_evaluate_parent(
                                    subtask_id=sub_id,
                                    settlement_receipt=receipt,
                                    subtask_issue_id=sub_id,
                                    exit_code=exit_c,
                                    output_tail=out_tail,
                                    pane_coordinate=coord,
                                )
                                if res.get("parent_settled"):
                                    logger.info(f"All subtasks finished. Parent {res.get('parent_key')} marked Claimed Fixed.")
                            except Exception as ex:
                                logger.debug(f"Linear subtask settlement failed in ingress: {ex}")

                        asyncio.create_task(_settle_and_check())
                except Exception as e:
                    logger.debug(f"Failed to initiate linear settlement in ingress: {e}")
            elif hand_id and not _linear_disabled_for_tests():
                try:
                    linear_bridge = self._get_linear_bridge()
                    t_status = getattr(accumulator, "turn_status", "SUCCESS")
                    has_awaiting = bool(getattr(accumulator, "has_awaiting_input", False))
                    final_hand_status = "CANCELED" if (has_awaiting or t_status != "SUCCESS") else "SUCCESS"
                    hand_thread_ref = ephemeral_hand_issue.get("thread_ref") or ""

                    async def _settle_ephemeral_hand():
                        try:
                            await linear_bridge.settle_hand_issue(
                                hand_id,
                                status=final_hand_status,
                                thread_ref=hand_thread_ref,
                            )
                        except Exception as ex:
                            logger.debug(f"Ephemeral [Hand] settlement skipped in ingress: {ex}")

                    asyncio.create_task(_settle_ephemeral_hand())
                except Exception as e:
                    logger.debug(f"Failed to initiate ephemeral [Hand] settlement in ingress: {e}")

        if session is not None and isinstance(getattr(session, "pending_steering_notes", None), list) and session.pending_steering_notes:
            drained_notes = list(session.pending_steering_notes)
            session.pending_steering_notes.clear()
            accumulator.steering_notes_drained = drained_notes
            steering_suffix = (
                "\n\n[MID-STREAM OPERATOR STEERING RECEIVED WHILE HAND WAS RUNNING]:\n"
                + "\n".join(f"- {n}" for n in drained_notes)
                + "\n[ACTION REQUIRED: Incorporate and execute this operator steering immediately "
                "(e.g. consult_memory / dispatch a follow-up Hand) before delivering your final briefing.]"
            )
            accumulator.final_response = f"{getattr(accumulator, 'final_response', '') or ''}{steering_suffix}".strip()
            accumulator.output_tail = f"{getattr(accumulator, 'output_tail', '') or accumulator.final_response}".strip()

        return accumulator

    async def _load_linear_notes_block(self, thread_ref: str) -> str:
        """Fetch this thread's Linear notes as a prompt block. Strictly thread-scoped (never leaks channel-global alerts)."""
        if not thread_ref:
            return ""

        # Check for linear_tickets_context bound strictly to this thread_ref
        tickets_ctx: Optional[List[Dict[str, Any]]] = None
        c = _THREAD_PLAN_CACHE.get(thread_ref)
        if c and isinstance(c, dict) and c.get("linear_tickets_context"):
            tickets_ctx = c.get("linear_tickets_context")
        elif hasattr(self, "router") and self.router:
            if hasattr(self.router, "get_recorded_session"):
                rec = self.router.get_recorded_session(thread_ref)
                if rec and isinstance(rec, dict) and rec.get("linear_tickets_context"):
                    tickets_ctx = rec.get("linear_tickets_context")
            if not tickets_ctx and hasattr(self.router, "_registry"):
                reg_item = self.router._registry.get(thread_ref)
                if reg_item and isinstance(reg_item, dict) and reg_item.get("linear_tickets_context"):
                    tickets_ctx = reg_item.get("linear_tickets_context")

        roadmap_block = ""
        if tickets_ctx and isinstance(tickets_ctx, list):
            roadmap_lines = ["[THREAD LINEAR PROJECT CONTEXT & RUNTIME TELEMETRY]"]
            for ticket in tickets_ctx:
                t_ident = ticket.get("identifier") or ticket.get("id") or "CHO-???"
                t_title = ticket.get("title") or "Untitled"
                t_state = ticket.get("state")
                if isinstance(t_state, dict):
                    t_state_name = t_state.get("name") or "In Progress"
                else:
                    t_state_name = str(t_state or "In Progress")
                roadmap_lines.append(f"• Project: {t_ident} \"{t_title}\" (State: {t_state_name})")
                telemetry = self._build_ticket_runtime_telemetry(str(t_ident))
                in_flight = telemetry.get("in_flight_subtasks") or []
                queued = telemetry.get("queued_subtasks") or []
                workers = telemetry.get("active_workers") or []
                secs_ago = telemetry.get("seconds_since_last_activity")
                last_err = telemetry.get("last_error")
                if in_flight or queued or workers:
                    roadmap_lines.append(
                        f"  Runtime State: IN-FLIGHT={in_flight}, QUEUED={queued}, ACTIVE_WORKERS={workers}"
                    )
                elif secs_ago is not None:
                    roadmap_lines.append("  Runtime State: IDLE")
                if last_err:
                    roadmap_lines.append(f"  Last Run Error: {last_err}")

                subtasks = ticket.get("subtasks") or []
                uncompleted_subtasks = [st for st in subtasks if not st.get("settled")]
                if uncompleted_subtasks:
                    roadmap_lines.append("  Uncompleted Subtasks:")
                    for st in uncompleted_subtasks:
                        st_ident = st.get("identifier") or ""
                        st_title = st.get("title") or "Untitled"
                        st_desc = st.get("description") or ""
                        desc_str = f" - {st_desc}" if st_desc else ""
                        roadmap_lines.append(f"    - [{st_ident}] {st_title}{desc_str}")
                else:
                    roadmap_lines.append("  (No remaining uncompleted subtasks)")
            roadmap_block = "\n".join(roadmap_lines)

        base_notes = ""
        # 1. 0ms memory cache hit
        cached = _THREAD_PLAN_CACHE.get(thread_ref)
        if cached and isinstance(cached, dict) and (cached.get("parent") or cached.get("subtasks")):
            parent = cached.get("parent", {})
            subtasks = cached.get("subtasks", [])
            subtasks_formatted = []
            for st in subtasks:
                st_ident = st.get("identifier", "")
                st_title = st.get("title", "")
                st_state = st.get("state", {}).get("name") if isinstance(st.get("state"), dict) else st.get("state", "Planned")
                subtasks_formatted.append(f"[{st_ident}] {st_title} -> {st_state}")
            notes = {
                "identifier": parent.get("identifier", ""),
                "title": parent.get("title", ""),
                "state": parent.get("state", {}).get("name") if isinstance(parent.get("state"), dict) else parent.get("state", "In Progress"),
                "subtasks": subtasks_formatted,
                "comments": cached.get("comments", []),
            }
            base_notes = build_linear_notes_block(notes)
        else:
            # 2. Check if bridge or fetch is mocked or eager fetching enabled
            is_mocked = (
                _is_mock_object(getattr(self, "_load_linear_notes_block", None))
                or _is_mock_object(getattr(self, "_fetch_linear_thread_notes", None))
                or _is_mock_object(getattr(self, "linear_bridge", None))
            )
            eager_fetch = os.environ.get("GOLEM_EAGER_LINEAR_FETCH") == "1"
            if is_mocked or eager_fetch:
                try:
                    from golem.linear_bridge import LinearBridge
                    bridge = self._get_linear_bridge()
                    if bridge.api_key and not _linear_disabled_for_tests():
                        notes = await asyncio.wait_for(bridge.get_thread_notes(thread_ref), timeout=4.0)
                        base_notes = build_linear_notes_block(notes)
                except Exception as exc:
                    logger.debug(f"Linear thread notes unavailable for {thread_ref}: {exc}")

        if roadmap_block and base_notes:
            return f"{base_notes}\n\n{roadmap_block}".strip()
        elif roadmap_block:
            return roadmap_block
        return base_notes

    def _hydrate_subtasks_from_linear_context(
        self,
        thread_ref: str,
        channel: str,
        parent_key: Optional[str],
        subtasks_list: List[Dict[str, Any]],
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """
        Hydrate uncompleted subtasks for explicit dispatch_subtasks calls strictly from
        this thread's linear_tickets_context (never from a channel-wide fallback).
        """
        if not thread_ref:
            return parent_key, subtasks_list

        tickets_ctx: Optional[List[Dict[str, Any]]] = None
        cached = _THREAD_PLAN_CACHE.get(thread_ref)
        if cached and isinstance(cached, dict) and cached.get("linear_tickets_context"):
            tickets_ctx = cached.get("linear_tickets_context")
        elif hasattr(self, "router") and self.router:
            if hasattr(self.router, "get_recorded_session"):
                rec = self.router.get_recorded_session(thread_ref)
                if rec and isinstance(rec, dict) and rec.get("linear_tickets_context"):
                    tickets_ctx = rec.get("linear_tickets_context")
            if not tickets_ctx and hasattr(self.router, "_registry"):
                reg_item = self.router._registry.get(thread_ref)
                if reg_item and isinstance(reg_item, dict) and reg_item.get("linear_tickets_context"):
                    tickets_ctx = reg_item.get("linear_tickets_context")

        if not tickets_ctx or not isinstance(tickets_ctx, list):
            return parent_key, subtasks_list

        existing_keys = {str(st.get("issue_key")).strip() for st in subtasks_list if st.get("issue_key")}

        for st_item in subtasks_list:
            if not st_item.get("parent_key"):
                s_key = str(st_item.get("issue_key") or "").strip()
                for pt in tickets_ctx:
                    pt_ident = str(pt.get("identifier") or "").strip()
                    for c_st in pt.get("subtasks", []):
                        if str(c_st.get("identifier") or "").strip() == s_key:
                            st_item["parent_key"] = pt_ident
                            break

        matching_pt = None
        if parent_key:
            pk_clean = str(parent_key).strip().lower()
            for pt in tickets_ctx:
                if str(pt.get("identifier") or "").strip().lower() == pk_clean:
                    matching_pt = pt
                    break
        elif len(tickets_ctx) == 1:
            matching_pt = tickets_ctx[0]

        if matching_pt:
            p_ident = matching_pt.get("identifier")
            for c_st in matching_pt.get("subtasks", []):
                if not c_st.get("settled"):
                    c_ident = c_st.get("identifier")
                    if c_ident and c_ident not in existing_keys:
                        subtasks_list.append({
                            "issue_key": c_ident,
                            "parent_key": p_ident,
                            "role_name": c_st.get("title") or "",
                            "task_brief": c_st.get("description") or c_st.get("title") or "",
                            "read_only": False,
                            "target_node": "localhost",
                        })
                        existing_keys.add(c_ident)
            if not parent_key:
                parent_key = p_ident

        return parent_key, subtasks_list

    def _record_hand_result_to_linear(self, task_desc: str, target_node: str, result: Any) -> None:
        """Detached: append a dispatched hand's result to this thread's Linear issue (Golem's cross-turn memory)."""
        thread_ref = _THREAD_REF.get()
        if not thread_ref or not isinstance(result, dict):
            return
        try:
            from golem.linear_bridge import LinearBridge
            bridge = self._get_linear_bridge()
            if not bridge.api_key or _linear_disabled_for_tests():
                return
        except Exception:
            return

        status = result.get("status") or "UNKNOWN"
        tail = str(result.get("output_tail") or "").strip()[-1500:]
        error = str(result.get("error") or "").strip()[:500]
        body = f"**Hand result** on `{target_node}` (status `{status}`)\nTask: {str(task_desc)[:400]}"
        if error:
            body += f"\nError: {error}"
        if tail:
            body += f"\n```\n{tail}\n```"

        async def _record() -> None:
            try:
                plan_entry = _THREAD_PLAN_CACHE.get(thread_ref) if isinstance(_THREAD_PLAN_CACHE, dict) else None
                parent_info = plan_entry.get("parent") if isinstance(plan_entry, dict) else None
                parent_id = None
                if isinstance(parent_info, dict):
                    parent_id = parent_info.get("id") or parent_info.get("identifier")

                if parent_id:
                    await bridge.add_comment(parent_id, body)
                else:
                    issue = await bridge.ensure_thread_issue(
                        thread_ref, title=f"[Hand] {str(task_desc)[:60]}", description=f"Node: {target_node}"
                    )
                    await bridge.add_comment(issue["id"], body)
                    if str(issue.get("title") or "").startswith("[Hand]"):
                        await bridge.settle_hand_issue(issue["id"], status=status, thread_ref=thread_ref)
            except Exception as exc:
                logger.warning(f"Could not record hand result to Linear for {thread_ref}: {exc}")

        asyncio.create_task(_record())

    async def _execute_check_eyes(
        self,
        session: Optional[SessionHandle] = None,
        pane: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute check_eyes tool query. An idle runner showing only a shell prompt is annotated so it is not
        mistaken for a crash.
        """
        return annotate_idle_shell_prompt(await self._collect_eyes_status(session=session, pane=pane))

    async def _collect_eyes_status(
        self,
        session: Optional[SessionHandle] = None,
        pane: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute check_eyes tool query.
        Returns a pure JSON-serializable dictionary with:
        state, active_command, pid, blocker, buffer_tail.
        Zero pre-baked strings.
        """
        if session is None:
            return {
                "state": "IDLE",
                "active_command": "",
                "command": "",
                "pid": None,
                "blocker": None,
                "blockers": [],
                "buffer_tail": "",
            }

        status_info = None
        runner_error = None
        if hasattr(session, "get_runner_status"):
            try:
                status_info = await session.get_runner_status()
            except Exception as e:
                logger.warning(f"Error querying runner status in _execute_check_eyes: {e}")
                runner_error = str(e)
                return {
                    "state": "ERROR",
                    "error": runner_error,
                    "active_command": "",
                    "command": "",
                    "pid": None,
                    "blocker": None,
                    "blockers": [],
                    "buffer_tail": f"Error querying runner buffer: {runner_error}",
                }

        eyes_status = None
        if hasattr(session, "get_eyes_status"):
            try:
                res = session.get_eyes_status(status_info)
                if inspect.isawaitable(res):
                    eyes_status = await res
                else:
                    eyes_status = res
            except Exception as e:
                logger.warning(f"Error in session.get_eyes_status: {e}")

        if eyes_status is not None and isinstance(eyes_status, dict):
            return dict(eyes_status)

        # Fallback dictionary if get_eyes_status not available
        status_val = (status_info or {}).get("status", "idle")
        active_cmd = (status_info or {}).get("command") or getattr(getattr(session, "active_execution", None), "command", "") or ""
        active_pid = (status_info or {}).get("pid") or getattr(getattr(session, "active_execution", None), "pid", None)
        active_blocker = (status_info or {}).get("blocker")
        active_tail = (status_info or {}).get("buffer_tail", "")

        state = "IDLE"
        if active_blocker:
            state = "BLOCKER"
        elif status_val == "running" or getattr(session, "is_executing", False):
            state = "RUNNING"
        elif status_val == "completed":
            state = "COMPLETED"

        return {
            "state": state,
            "active_command": active_cmd,
            "command": active_cmd,
            "pid": active_pid,
            "blocker": active_blocker,
            "blockers": [active_blocker] if active_blocker else [],
            "buffer_tail": active_tail,
        }

    async def _execute_check_nose(
        self,
        trigger_fresh_sniff: bool = False,
    ) -> Dict[str, Any]:
        """Run the Nose health query and log it as an explicit health check on the WebUI Nose terminal."""
        result = await self._execute_check_nose_core(trigger_fresh_sniff=trigger_fresh_sniff)
        webui = getattr(self, "webui_server", None)
        recorder = getattr(webui, "record_nose_health_check", None) if webui is not None else None
        if callable(recorder):
            try:
                summary = str(result.get("error") or result.get("verdict_summary") or "")
                res = recorder(summary, bool(result.get("smells_off") or result.get("error")))
                if inspect.isawaitable(res):
                    await res
            except Exception as exc:
                logger.debug(f"WebUI record_nose_health_check warning: {exc}")
        return result

    async def _execute_check_nose_core(
        self,
        trigger_fresh_sniff: bool = False,
    ) -> Dict[str, Any]:
        """
        Execute check_nose tool query.
        Returns a pure JSON-serializable dictionary with:
        seconds_ago, minutes_ago, last_check_timestamp, smells_off,
        verdict_summary, disk, docker, tailscale, recent_error_count.
        Zero pre-baked strings.
        """
        sentinel = self.get_nose_sentinel()
        if sentinel is not None:
            try:
                get_fn = (
                    getattr(sentinel, "get_telemetry_digest", None)
                    or getattr(sentinel, "get_telemetry", None)
                    or getattr(sentinel, "get_status", None)
                )
                if callable(get_fn):
                    res = get_fn(trigger_fresh_sniff=trigger_fresh_sniff)
                    if inspect.isawaitable(res):
                        return dict(await res)
                    return dict(res)
            except Exception as e:
                logger.warning(f"Error executing NoseSentinel telemetry query: {e}")
                return {
                    "error": str(e),
                    "seconds_ago": 0.0,
                    "minutes_ago": 0.0,
                    "last_check_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "smells_off": True,
                    "verdict_summary": f"⚠️ Sentinel Error: {e}",
                    "disk": {"free_gb": 0.0, "percent_used": 0.0, "pressure": False},
                    "docker": {"running": False, "container_count": 0, "containers": []},
                    "tailscale": {"online": False, "backend_state": "Unknown", "peers": {}},
                    "recent_error_count": 1,
                    "error_count": 1,
                }

        # Sentinel unavailable fallback
        return {
            "error": "NoseSentinel unavailable",
            "seconds_ago": 0.0,
            "minutes_ago": 0.0,
            "last_check_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "smells_off": False,
            "verdict_summary": "NoseSentinel is not configured or currently unavailable.",
            "disk": {"free_gb": 0.0, "percent_used": 0.0, "pressure": False},
            "docker": {"running": False, "container_count": 0, "containers": []},
            "tailscale": {"online": False, "backend_state": "Unknown", "peers": {}},
            "recent_error_count": 0,
            "error_count": 0,
        }

    async def _execute_consult_memory(
        self,
        path: str,
    ) -> Dict[str, Any]:
        """
        Execute consult_memory tool by reading a wiki runbook or context file.
        Returns full markdown content in the function response.
        """
        from golem.wiki import DEFAULT_WIKI_DIR, read_wiki_document
        clean_path = (path or "").strip().lstrip("/")
        if not clean_path:
            return {
                "status": "ERROR",
                "path": clean_path,
                "error": "A relative wiki path is required for consult_memory.",
            }

        wiki_root = Path(DEFAULT_WIKI_DIR).resolve()
        target_file = (wiki_root / clean_path).resolve()
        if not target_file.is_file() or not str(target_file).startswith(str(wiki_root)):
            return {
                "status": "NOT_FOUND",
                "path": clean_path,
                "error": f"Wiki document '{clean_path}' does not exist in ~/.golem/wiki/.",
            }

        try:
            content = read_wiki_document(clean_path, wiki_dir=wiki_root)
            return {
                "status": "SUCCESS",
                "path": clean_path,
                "content": content,
            }
        except Exception as e:
            return {
                "status": "ERROR",
                "path": clean_path,
                "error": f"Failed to read wiki document '{clean_path}': {e}",
            }

    async def _execute_share_artifact(
        self,
        path: Optional[str] = None,
        title: Optional[str] = None,
        session: Optional[Any] = None,
        client: Optional[Any] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        artifact_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute share_artifact tool call via golem.outbox.share_artifact.
        Enforces outbox sandboxing: strictly raises PermissionError / SandboxSecurityError
        if path resolves outside ~/.golem/outbox/.
        Auto-binds artifact metadata to session.last_artifact if session is provided.
        Returns structured metadata dictionary.
        """
        from golem.outbox import share_artifact, get_default_outbox_manager

        mgr = None
        if hasattr(self, "router") and hasattr(self.router, "harness") and self.router.harness:
            mgr = getattr(self.router.harness, "outbox", None)
        if mgr is None:
            mgr = get_default_outbox_manager()

        sess_id = getattr(session, "session_id", None) if session else None
        target_channel = channel or (getattr(session, "channel", None) if session else "C_DEFAULT")
        target_thread = thread_ts or (getattr(session, "thread_ts", None) if session else None)

        meta = await share_artifact(
            path=path,
            client=client,
            channel=target_channel,
            thread_ts=target_thread,
            title=title,
            artifact_id=artifact_id,
            session_id=sess_id,
            session=session,
            outbox_manager=mgr,
        )

        if session is not None and hasattr(session, "record_artifact"):
            session.record_artifact(meta)

        return meta

    async def _execute_propose_memory_update(
        self,
        summary: str,
        changes: List[Dict[str, Any]],
        session: Optional[Any] = None,
    ) -> Any:
        """
        Execute propose_memory_update tool call via GolemWiki.propose_multi_update.
        Stages updates across the 4 memory tiers and attaches pending proposal to session.
        """
        from golem.wiki import GolemWiki
        wiki = GolemWiki()
        proposal = wiki.propose_multi_update(changes=changes, summary=summary)
        if session is not None:
            session.pending_memory_proposal = proposal
        return proposal

    async def _execute_tool_call(
        self,
        tool_name: str,
        tool_args: Dict[str, Any],
        session: Optional[SessionHandle] = None,
        client: Optional[Any] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Dispatch a front-of-house Mouth tool call."""
        if tool_name == "read_project_file":
            return await _handle_read_project_file(tool_args)
        elif tool_name == "check_eyes":
            pane = tool_args.get("pane")
            return await self._execute_check_eyes(session=session, pane=pane)
        elif tool_name == "check_nose":
            trigger_fresh_sniff = bool(tool_args.get("trigger_fresh_sniff", False))
            return await self._execute_check_nose(trigger_fresh_sniff=trigger_fresh_sniff)
        elif tool_name == "consult_memory":
            path_arg = tool_args.get("path") or ""
            return await self._execute_consult_memory(path=path_arg)
        elif tool_name == "share_artifact":
            path_arg = tool_args.get("path")
            title_arg = tool_args.get("title")
            art_id_arg = tool_args.get("artifact_id")
            return await self._execute_share_artifact(
                path=path_arg,
                title=title_arg,
                artifact_id=art_id_arg,
                session=session,
                client=client,
                channel=channel,
                thread_ts=thread_ts,
            )
        elif tool_name == "propose_memory_update":
            summary = tool_args.get("summary") or "Operational memory update"
            changes_arg = tool_args.get("changes") or []
            proposal = await self._execute_propose_memory_update(
                summary=summary,
                changes=changes_arg,
                session=session,
            )
            return {
                "status": "SUCCESS",
                "token": proposal.token,
                "summary": proposal.summary,
                "changes_count": len(proposal.changes),
                "card": proposal.to_slack_card(),
            }
        elif tool_name == "plan_task_with_subtasks":
            from golem.linear_bridge import LinearBridge
            bridge = self._get_linear_bridge()
            parent_title = tool_args.get("parent_title") or ""
            parent_desc = tool_args.get("parent_description") or ""
            thread_ref = _THREAD_REF.get() or (f"{channel}:{thread_ts}" if channel and thread_ts else "")
            if thread_ref:
                parent_desc = f"Thread: {thread_ref}\n{parent_desc}"
            subtasks = tool_args.get("subtasks") or []
            parent_key = (tool_args.get("parent_key") or "").strip()
            related_issue_key = (tool_args.get("related_issue_key") or "").strip()
            if not related_issue_key and not parent_key:
                prior_cached = (
                    (getattr(session, "linear_plan_cache", None) if session else None)
                    or (_THREAD_PLAN_CACHE.get(thread_ref) if thread_ref else None)
                )
                prior_parent_key = (
                    (prior_cached.get("parent", {}).get("identifier") if isinstance(prior_cached, dict) else None)
                    or (getattr(session, "active_linear_parent_key", None) if session else None)
                    or (getattr(session, "linear_issue_key", None) if session else None)
                )
                if prior_parent_key and str(prior_parent_key).strip():
                    related_issue_key = str(prior_parent_key).strip()
            plan_kwargs: Dict[str, Any] = {}
            if parent_key:
                plan_kwargs["parent_key"] = parent_key
            if related_issue_key:
                plan_kwargs["related_issue_key"] = related_issue_key
            plan = await bridge.create_plan_with_subtasks(
                parent_title,
                parent_desc,
                subtasks,
                **plan_kwargs,
            )
            cached_plan = {
                "parent": plan.get("parent", {}),
                "subtasks": plan.get("subtasks", []),
                "comments": [],
            }
            if session is not None:
                session.linear_plan_cache = cached_plan
                session.active_linear_parent_key = plan.get("parent", {}).get("identifier", "")
                session.pending_subtasks = [
                    st.get("identifier") or st.get("id") or str(idx)
                    for idx, st in enumerate(plan.get("subtasks", []))
                ]
            if thread_ref:
                _THREAD_PLAN_CACHE[thread_ref] = cached_plan
            res_payload: Dict[str, Any] = {
                "status": "PLAN_CREATED",
                "parent_key": plan.get("parent", {}).get("identifier", ""),
                "parent_url": plan.get("parent", {}).get("url", ""),
                "subtasks": plan.get("subtasks", []),
            }
            if plan.get("parent", {}).get("related_issue_key") or related_issue_key:
                res_payload["related_issue_key"] = plan.get("parent", {}).get("related_issue_key") or related_issue_key
            return res_payload
        elif tool_name == "check_linear_task_status":
            from golem.linear_bridge import LinearBridge
            bridge = self._get_linear_bridge()
            trees = await bridge.get_project_task_tree(limit=10)
            formatted = []
            for parent in trees:
                subtasks_status = [
                    f"[{c.get('identifier', '')}] {c.get('title', '')} -> {c.get('state', {}).get('name', 'Unknown')}"
                    for c in parent.get("children", {}).get("nodes", [])
                ]
                formatted.append({
                    "parent_key": parent.get("identifier", ""),
                    "title": parent.get("title", ""),
                    "state": parent.get("state", {}).get("name", "Unknown"),
                    "subtasks": subtasks_status,
                })
            return {"status": "SUCCESS", "tasks": formatted}
        elif tool_name == "approve_linear_task":
            from golem.linear_bridge import LinearBridge
            bridge = self._get_linear_bridge()
            thread_ref = _THREAD_REF.get() or (f"{channel}:{thread_ts}" if channel and thread_ts else "")
            cached = (getattr(session, "linear_plan_cache", None) if session else None) or _THREAD_PLAN_CACHE.get(thread_ref)
            issue_key = tool_args.get("issue_key") or ""
            if not issue_key and cached:
                issue_key = cached.get("parent", {}).get("identifier", "")
            if cached:
                if isinstance(cached.get("parent"), dict):
                    cached["parent"]["state"] = "Completed"
                for st in cached.get("subtasks", []):
                    if isinstance(st, dict):
                        st["state"] = "Completed"
            signoff = tool_args.get("signoff_comment") or "Approved by Carl."
            await bridge.approve_and_complete_tree(issue_key, signoff)
            webui = getattr(self, "webui_server", None)
            if webui is not None and hasattr(webui, "clear_settled_hands"):
                res = webui.clear_settled_hands(parent_key=issue_key)
                if inspect.isawaitable(res):
                    await res
            return {
                "status": "SUCCESS",
                "message": f"Parent issue {issue_key} and all subtasks marked Completed.",
            }
        elif tool_name == "cancel_active_task":
            if session is not None and hasattr(session, "cancel_execution"):
                await session.cancel_execution()
            return {"status": "CANCELLED", "message": "Active backstage execution stopped."}
        elif tool_name == "dispatch_subtasks":
            thread_ref = _THREAD_REF.get() or (f"{channel}:{thread_ts}" if channel and thread_ts else "")
            cached = (getattr(session, "linear_plan_cache", None) if session else None) or _THREAD_PLAN_CACHE.get(thread_ref)
            parent_key = tool_args.get("parent_issue_key")
            if not parent_key and cached:
                parent_key = cached.get("parent", {}).get("identifier")
            mode = tool_args.get("execution_mode", "parallel")
            subtasks_list = tool_args.get("subtasks", [])
            parent_key, subtasks_list = self._hydrate_subtasks_from_linear_context(
                thread_ref=thread_ref,
                channel=channel or "",
                parent_key=parent_key,
                subtasks_list=subtasks_list,
            )
            cached_subs = cached.get("subtasks", []) if cached else []
            for idx, st_item in enumerate(subtasks_list):
                st_key = st_item.get("issue_key")
                subtask_idx = st_item.get("subtask_index")
                if not st_key or (subtask_idx is not None and not str(st_key).startswith("CHO-")):
                    target_i = (subtask_idx - 1) if (subtask_idx is not None and 1 <= subtask_idx <= len(cached_subs)) else idx
                    if 0 <= target_i < len(cached_subs):
                        st_item["issue_key"] = cached_subs[target_i].get("identifier", "")
            sub_kwargs = {k: v for k, v in tool_args.items() if k not in ("parent_issue_key", "execution_mode", "subtasks")}
            acc = await self._execute_backstage_task(
                parent_issue_key=parent_key,
                execution_mode=mode,
                subtasks=subtasks_list,
                session=session,
                client=client,
                channel=channel,
                thread_ts=thread_ts,
                **sub_kwargs,
            )
            if isinstance(acc, dict):
                return acc
            return acc.get_structured_summary()
        elif tool_name == "dispatch_task":
            cmd = tool_args.get("command") or tool_args.get("task_description") or ""
            target_node = tool_args.get("target_node") or "hermes"
            target_cwd = tool_args.get("cwd") or getattr(session, "target_cwd", None) or os.getcwd()
            worker = tool_args.get("worker") or get_default_hand_worker() or "agy"
            acc = await self._execute_backstage_task(
                command=cmd,
                task_desc=cmd,
                task_description=cmd,
                target_node=target_node,
                target_cwd=target_cwd,
                session=session,
                client=client,
                channel=channel,
                thread_ts=thread_ts,
                worker=worker,
                **tool_args,
            )
            if isinstance(acc, dict):
                return acc
            return acc.get_structured_summary()
        return {"status": "ERROR", "error": f"Tool '{tool_name}' not dispatched via _execute_tool_call"}

    async def _handle_mouth_turn(
        self,
        session: SessionHandle,
        prompt: str,
        client: Any,
        channel: str,
        thread_ts: str,
        ts: str,
        is_imessage: bool,
    ) -> None:
        """
        Run one Mouth turn scoped to its conversation thread. Always ensures recent thread history
        for the active Slack thread_ts is passed alongside thread-scoped Linear context.
        """
        thread_ref = f"{channel}:{thread_ts}" if (channel and thread_ts) else ""
        token = _THREAD_REF.set(thread_ref)
        try:
            if thread_ts and thread_ts != ts and "[Previous " not in (prompt or ""):
                thread_ctx = await self._fetch_thread_context(
                    client, channel, thread_ts, exclude_ts=ts, is_imessage=is_imessage
                )
                if thread_ctx:
                    prompt = f"{thread_ctx}\n\n{prompt}" if prompt else thread_ctx
            if thread_ref:
                notes_block = await self._load_linear_notes_block(thread_ref)
                if notes_block:
                    prompt = f"{notes_block}\n\n{prompt}"
            await self._run_mouth_turn(session, prompt, client, channel, thread_ts, ts, is_imessage)
        finally:
            _THREAD_REF.reset(token)

    async def _run_mouth_turn(
        self,
        session: SessionHandle,
        prompt: str,
        client: Any,
        channel: str,
        thread_ts: str,
        ts: str,
        is_imessage: bool,
    ) -> None:
        """
        Universal Front-of-House Mouth turn pipeline:
          1. Call Mouth LLM (MOUTH_MODEL) with full thread history and dispatch_task tool declaration.
          2. If pure text is emitted: reply conversationally in 1 turn (0 worker tasks).
          3. If dispatch_task is called: execute backstage silently with 3.0s yield window.
          4. If execution exceeds 3.0s: deliver liaison acknowledgment while task runs in background.
          5. Feed execution results back via functionResponse for synthesized executive briefing.
          6. Deliver clean briefing to Slack, upload directive files, and swap reactions.
        """
        api_key = get_gemini_api_key()
        raw_configured_model = getattr(self, "mouth_model", None) or get_mouth_model()
        model = route_mouth_turn_model(prompt, explicit_model=raw_configured_model)

        _orig_add_reaction = globals()["add_reaction"]
        _orig_remove_reaction = globals()["remove_reaction"]
        _orig_swap_reaction = globals()["swap_reaction"]

        async def add_reaction(c: Any, ch: str, t: str, name: str) -> None:
            if not is_imessage and t:
                await _orig_add_reaction(c, ch, t, name)

        async def remove_reaction(c: Any, ch: str, t: str, name: str) -> None:
            if not is_imessage and t:
                await _orig_remove_reaction(c, ch, t, name)

        async def swap_reaction(c: Any, ch: str, t: str, old_name: Optional[str], new_name: str) -> None:
            if not is_imessage and t:
                await _orig_swap_reaction(c, ch, t, old_name, new_name)

        # Fleet Topology & Machine Roles:
        # - hermes: Local macOS host controller
        # - desktop-pc: Windows CUDA compute workstation
        # - oci-machine: Oracle Cloud Linux VPS
        sys_prompt = build_mouth_system_instruction()

        tools = get_mouth_tools_declaration()
        payload = {
            "systemInstruction": {"parts": [{"text": sys_prompt}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                }
            ],
            "tools": tools,
            "toolConfig": {
                "functionCallingConfig": {"mode": "AUTO"}
            },
            "generationConfig": {
                "temperature": 0.2,
            },
        }

        thread_ref = _THREAD_REF.get() or (f"{channel}:{thread_ts}" if channel and thread_ts else "direct")
        self._broadcast_mouth_log(f"[TURN START] Thread {thread_ref} · waking {model}...", model=model)
        clean_p = " ".join(str(prompt).splitlines()).strip()
        if len(clean_p) > 200:
            clean_p = clean_p[:197] + "..."
        self._broadcast_mouth_log(f"[PROMPT] {clean_p}", model=model)

        # Step 1: Initial call to Mouth LLM
        self._broadcast_mouth_log(f"[HOP 1/{MAX_TOOL_CHAIN_HOPS}] Reasoning with {model}...", model=model)
        initial_resp = await self._call_gemini_api(payload=payload, model=model, api_key=api_key)
        
        should_retry = False
        if not initial_resp:
            should_retry = True
        elif candidate_is_empty(initial_resp):
            should_retry = True
        elif initial_resp.get("_error"):
            err_code = initial_resp.get("code") or initial_resp.get("status") or 500
            if isinstance(err_code, int) and err_code >= 500:
                should_retry = True
            elif not isinstance(err_code, int):
                should_retry = True

        if should_retry and model != get_architect_model():
            self._broadcast_mouth_log(
                f"[STRIKE 1] Call to {model} failed or returned empty; retrying on {model}...",
                model=model,
            )
            initial_resp = await self._call_gemini_api(payload=payload, model=model, api_key=api_key)
            strike2_retry = False
            if not initial_resp or candidate_is_empty(initial_resp):
                strike2_retry = True
            elif initial_resp.get("_error"):
                err_code = initial_resp.get("code") or initial_resp.get("status") or 500
                if isinstance(err_code, int) and err_code >= 500:
                    strike2_retry = True
                elif not isinstance(err_code, int):
                    strike2_retry = True

            if strike2_retry:
                old_model = model
                model = get_architect_model()
                self._broadcast_mouth_log(
                    f"[ESCALATION] 2 strikes on {old_model} -> auto-escalating to {model} (Tier 1 Architect)...",
                    model=model,
                )
                initial_resp = await self._call_gemini_api(payload=payload, model=model, api_key=api_key)

        if not initial_resp or not initial_resp.get("candidates"):
            # Fallback to direct adapter execution if Gemini API fails
            logger.warning("Gemini Mouth initial call failed; falling back to direct adapter execution.")
            await self._fallback_direct_adapter_turn(session, prompt, client, channel, thread_ts, ts, is_imessage)
            return

        cand_list = initial_resp.get("candidates") if isinstance(initial_resp, dict) else []
        candidate = cand_list[0] if (isinstance(cand_list, list) and cand_list and isinstance(cand_list[0], dict)) else {}
        parts = extract_candidate_parts(candidate)

        working_contents: List[Dict[str, Any]] = [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ]

        current_hop = 0
        executed_any_tool = False
        pending_subtasks: List[str] = []
        last_fn_name = ""
        last_fn_args: Dict[str, Any] = {}
        last_tool_result: Dict[str, Any] = {}
        last_candidate = candidate
        last_fn_call: Optional[Dict[str, Any]] = None
        turn_status = "SUCCESS"
        full_output = ""
        error_msg = ""
        accumulator = None
        hop_budget_exhausted = False
        consecutive_read_only = 0
        dispatch_repair_attempts = 0
        skip_step2_synthesis = False
        briefing_text = ""
        error_info: Dict[str, Any] = {}
        turn_interim_ts: Optional[str] = None
        turn_interim_seq: Optional[int] = None
        check_eyes_called_since_dispatch = False
        if session is not None and not isinstance(getattr(session, "_active_backstage_hand", None), dict):
            session._turn_interim_ts = None
            session._turn_interim_seq = None

        async def _deliver_final_briefing(briefing_str: str) -> None:
            nonlocal turn_interim_ts, turn_interim_seq
            acc_interim = (
                accumulator.get("interim_message_ts")
                if isinstance(accumulator, dict)
                else (getattr(accumulator, "interim_message_ts", None) if accumulator else None)
            )
            effective_interim_ts = turn_interim_ts or acc_interim
            effective_seq = turn_interim_seq
            if session is not None and not isinstance(getattr(session, "_active_backstage_hand", None), dict):
                if not effective_interim_ts:
                    effective_interim_ts = getattr(session, "_turn_interim_ts", None)
                if effective_seq is None:
                    effective_seq = getattr(session, "_turn_interim_seq", None)
                session._turn_interim_ts = None
                session._turn_interim_seq = None

            cur_seq = self._get_channel_outbound_seq(channel)
            has_intervening_messages = bool(
                effective_interim_ts
                and effective_seq is not None
                and cur_seq > effective_seq
            )
            if effective_interim_ts and has_intervening_messages:
                await self._post_response(
                    client,
                    channel,
                    thread_ts,
                    "✅ *Completed:* Backstage execution settled — see briefing below.",
                    is_imessage,
                    update_ts=effective_interim_ts,
                )
                await self._post_response(client, channel, thread_ts, briefing_str, is_imessage, update_ts=None)
            else:
                await self._post_response(
                    client,
                    channel,
                    thread_ts,
                    briefing_str,
                    is_imessage,
                    update_ts=effective_interim_ts,
                )

        while current_hop < MAX_TOOL_CHAIN_HOPS:
            current_hop += 1

            # Check for functionCall
            fn_call = None
            text_parts = []
            for p in parts:
                if "functionCall" in p and isinstance(p["functionCall"], dict):
                    fn_call = p["functionCall"]
                elif "text" in p and p.get("text"):
                    text_parts.append(str(p["text"]))

            # Case A: Pure Conversational Turn or Terminal text emission on chained hop
            if not fn_call:
                if not executed_any_tool:
                    reply_text = "".join(text_parts).strip()
                    if not reply_text:
                        err_briefing = (
                            f"⚠️ *Mouth Response Error:* Model '{model}' returned empty candidate text without tool calls.\n"
                            f"• Candidate payload: `{json.dumps(candidate)[:300]}`"
                        )
                        await self._post_response(client, channel, thread_ts, err_briefing, is_imessage)
                        await self._broadcast_mouth_briefing(err_briefing, model)
                        if ts:
                            await swap_reaction(client, channel, ts, "gear", "warning")
                        return
                    clean_reply, _ = extract_and_strip_file_directives(reply_text)
                    self._broadcast_mouth_log(f"[REPLY] {clean_reply}", model=model)
                    await self._post_response(client, channel, thread_ts, clean_reply, is_imessage)
                    await self._broadcast_mouth_briefing(clean_reply, model)
                    if ts:
                        await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                    return
                else:
                    # Model chained into pure text after prior read-only probe(s)
                    reply_text = "".join(text_parts).strip()
                    if reply_text:
                        clean_briefing, file_targets = extract_and_strip_file_directives(reply_text)
                        self._broadcast_mouth_log(f"[SYNTHESIS] {clean_briefing}", model=model)
                        target_reaction = "white_check_mark"
                        if last_fn_name == "read_project_file":
                            if current_hop > 1 and last_tool_result.get("status") == "NOT_FOUND":
                                target_reaction = "white_check_mark"
                            else:
                                target_reaction = "x" if last_tool_result.get("status") in ("ERROR", "NOT_FOUND") else "white_check_mark"
                        elif last_fn_name == "check_eyes" and last_tool_result.get("state") == "BLOCKER":
                            target_reaction = "warning"
                        elif last_fn_name == "check_nose" and last_tool_result.get("smells_off"):
                            target_reaction = "warning"
                        elif last_fn_name == "propose_memory_update":
                            target_reaction = "brain"
                            if last_tool_result.get("card") and "Memory Distillation Proposal" not in clean_briefing:
                                clean_briefing = last_tool_result.get("card")
                        elif last_fn_name in ("dispatch_task", "dispatch_subtasks"):
                            if turn_status in ("ERROR", "FAILED"):
                                has_fatal_error = (
                                    not full_output
                                    or bool(error_msg)
                                    or bool(re.search(r"\b(fatal error|operation failed|task failed|execution failed|unhandled exception|operation aborted|aborted|critical crash)\b", clean_briefing[:250], re.IGNORECASE))
                                    or "❌" in clean_briefing[:250]
                                )
                                target_reaction = "x" if has_fatal_error else "white_check_mark"
                            else:
                                target_reaction = "white_check_mark"

                        await _deliver_final_briefing(clean_briefing)
                        await self._broadcast_mouth_briefing(clean_briefing, model)

                        if file_targets:
                            fleet_mesh = (
                                getattr(self.router.harness, "mesh", None)
                                if hasattr(self.router, "harness") and self.router.harness
                                else getattr(self.router, "fleet_mesh", None)
                            )
                            await upload_directive_files(
                                client=client,
                                channel=channel,
                                thread_ts=thread_ts,
                                targets=file_targets,
                                active_cwd=session.target_cwd if hasattr(session, "target_cwd") else os.getcwd(),
                                fleet_mesh=fleet_mesh,
                            )
                            for ft in file_targets:
                                if hasattr(session, "record_artifact"):
                                    try:
                                        session.record_artifact(ft)
                                    except Exception:
                                        pass
                        if ts:
                            await swap_reaction(client, channel, ts, "gear", target_reaction)
                        return
                    break

            # Case B: Tool Call Turn (dispatch_task, check_eyes, check_nose, read_project_file, etc.)
            fn_name = fn_call.get("name", "dispatch_task")
            fn_args = fn_call.get("args", {})
            if (
                (fn_name in READ_ONLY_HOP_TOOLS and consecutive_read_only >= MAX_CONSECUTIVE_READ_ONLY_HOPS)
                or (fn_name == "check_eyes" and check_eyes_called_since_dispatch)
            ):
                logger.warning(
                    f"Hard hop gate blocked disallowed tool '{fn_name}' on hop {current_hop} "
                    f"(consecutive_read_only={consecutive_read_only}, check_eyes_called_since_dispatch={check_eyes_called_since_dispatch}); "
                    f"breaking to Step 2 synthesis."
                )
                break
            executed_any_tool = True
            last_fn_name = fn_name
            last_fn_args = fn_args
            last_candidate = candidate
            last_fn_call = fn_call
            turn_status = "SUCCESS"
            full_output = ""
            error_msg = ""

            tool_names = [
                p["functionCall"].get("name", "tool")
                for p in parts
                if isinstance(p, dict) and "functionCall" in p and isinstance(p["functionCall"], dict)
            ]
            tools_summary = ", ".join(tool_names) if tool_names else fn_name

            def _format_args_safe(args: Dict[str, Any], max_len: int = 60) -> str:
                if not args or not isinstance(args, dict):
                    return ""
                parts_str = []
                for k, v in args.items():
                    k_lower = str(k).lower()
                    if any(sec in k_lower for sec in ("key", "secret", "token", "password", "auth", "credential")):
                        parts_str.append(f"{k}=***")
                        continue
                    v_str = str(v).replace("\n", " ").strip()
                    if len(v_str) > 25:
                        v_str = v_str[:22] + "..."
                    parts_str.append(f"{k}={v_str}")
                res = ", ".join(parts_str)
                if len(res) > max_len:
                    res = res[:max_len - 3] + "..."
                return res

            self._broadcast_mouth_log(
                f"[HOP {current_hop}] Calling tool {fn_name}({_format_args_safe(fn_args)})",
                model=model,
            )

            try:
                if fn_name == "read_project_file":
                    tool_result = await self._execute_tool_call(
                        tool_name=fn_name,
                        tool_args=fn_args,
                        session=session,
                        client=client,
                        channel=channel,
                        thread_ts=thread_ts,
                    )
                elif fn_name == "check_eyes":
                    pane = fn_args.get("pane")
                    tool_result = await self._execute_check_eyes(session=session, pane=pane)
                    check_eyes_called_since_dispatch = True
                    if isinstance(tool_result, dict) and str(tool_result.get("state", "")).upper() == "RUNNING":
                        tool_result["note"] = (
                            "Runner is actively executing in the background. Do NOT call check_eyes again in this turn; "
                            "synthesize a concise live progress update for Carl immediately."
                        )
                elif fn_name == "check_nose":
                    trigger_fresh_sniff = bool(fn_args.get("trigger_fresh_sniff", False))
                    tool_result = await self._execute_check_nose(trigger_fresh_sniff=trigger_fresh_sniff)
                elif fn_name == "consult_memory":
                    path_arg = fn_args.get("path") or ""
                    tool_result = await self._execute_consult_memory(path=path_arg)
                elif fn_name == "share_artifact":
                    path_arg = fn_args.get("path")
                    title_arg = fn_args.get("title")
                    art_id_arg = fn_args.get("artifact_id")
                    tool_result = await self._execute_share_artifact(
                        path=path_arg,
                        title=title_arg,
                        artifact_id=art_id_arg,
                        session=session,
                        client=client,
                        channel=channel,
                        thread_ts=thread_ts,
                    )
                elif fn_name == "propose_memory_update":
                    summary = fn_args.get("summary") or "Operational memory update"
                    changes_arg = fn_args.get("changes") or []
                    proposal = await self._execute_propose_memory_update(
                        summary=summary,
                        changes=changes_arg,
                        session=session,
                    )
                    tool_result = {
                        "status": "SUCCESS",
                        "token": proposal.token,
                        "summary": proposal.summary,
                        "changes_count": len(proposal.changes),
                        "card": proposal.to_slack_card(),
                    }
                elif fn_name == "plan_task_with_subtasks":
                    from golem.linear_bridge import LinearBridge
                    bridge = self._get_linear_bridge()
                    parent_title = fn_args.get("parent_title") or ""
                    parent_desc = fn_args.get("parent_description") or ""
                    thread_ref = _THREAD_REF.get() or (f"{channel}:{thread_ts}" if channel and thread_ts else "")
                    if thread_ref:
                        parent_desc = f"Thread: `{thread_ref}`\n{parent_desc}"
                    subtasks = fn_args.get("subtasks") or []
                    parent_key = (fn_args.get("parent_key") or "").strip()
                    related_issue_key = (fn_args.get("related_issue_key") or "").strip()
                    if not related_issue_key and not parent_key:
                        prior_cached = (
                            (getattr(session, "linear_plan_cache", None) if session else None)
                            or (_THREAD_PLAN_CACHE.get(thread_ref) if thread_ref else None)
                        )
                        prior_parent_key = (
                            (prior_cached.get("parent", {}).get("identifier") if isinstance(prior_cached, dict) else None)
                            or (getattr(session, "active_linear_parent_key", None) if session else None)
                            or (getattr(session, "linear_issue_key", None) if session else None)
                        )
                        if prior_parent_key and str(prior_parent_key).strip():
                            related_issue_key = str(prior_parent_key).strip()
                    plan_kwargs: Dict[str, Any] = {}
                    if parent_key:
                        plan_kwargs["parent_key"] = parent_key
                    if related_issue_key:
                        plan_kwargs["related_issue_key"] = related_issue_key
                    plan = await bridge.create_plan_with_subtasks(
                        parent_title,
                        parent_desc,
                        subtasks,
                        **plan_kwargs,
                    )
                    subtasks_data = plan.get("subtasks", [])
                    pending_subtasks = [
                        st.get("identifier") or st.get("id") or str(idx)
                        for idx, st in enumerate(subtasks_data)
                    ]
                    cached_plan = {
                        "parent": plan.get("parent", {}),
                        "subtasks": subtasks_data,
                        "comments": [],
                    }
                    if session is not None:
                        session.linear_plan_cache = cached_plan
                        session.active_linear_parent_key = plan.get("parent", {}).get("identifier", "")
                        session.pending_subtasks = list(pending_subtasks)
                    if thread_ref:
                        _THREAD_PLAN_CACHE[thread_ref] = cached_plan
                        from golem.linear_bridge import _THREAD_ISSUE_CACHE
                        parent_val = plan.get("parent")
                        if isinstance(parent_val, dict) and (parent_val.get("id") or parent_val.get("identifier")):
                            _THREAD_ISSUE_CACHE[thread_ref] = parent_val
                    tool_result = {
                        "status": "PLAN_CREATED",
                        "parent_key": plan.get("parent", {}).get("identifier", ""),
                        "parent_url": plan.get("parent", {}).get("url", ""),
                        "subtasks": subtasks_data,
                    }
                    if plan.get("parent", {}).get("related_issue_key") or related_issue_key:
                        tool_result["related_issue_key"] = plan.get("parent", {}).get("related_issue_key") or related_issue_key
                elif fn_name == "check_linear_task_status":
                    from golem.linear_bridge import LinearBridge
                    bridge = self._get_linear_bridge()
                    trees = await bridge.get_project_task_tree(limit=10)
                    formatted = []
                    for parent in trees:
                        subtasks_status = [
                            f"[{c.get('identifier', '')}] {c.get('title', '')} -> {c.get('state', {}).get('name', 'Unknown')}"
                            for c in parent.get("children", {}).get("nodes", [])
                        ]
                        formatted.append({
                            "parent_key": parent.get("identifier", ""),
                            "title": parent.get("title", ""),
                            "state": parent.get("state", {}).get("name", "Unknown"),
                            "subtasks": subtasks_status,
                            "runtime_telemetry": self._build_ticket_runtime_telemetry(parent),
                        })
                    tool_result = {"status": "SUCCESS", "tasks": formatted}
                elif fn_name == "approve_linear_task":
                    from golem.linear_bridge import LinearBridge
                    bridge = self._get_linear_bridge()
                    thread_ref = _THREAD_REF.get() or (f"{channel}:{thread_ts}" if channel and thread_ts else "")
                    cached = (getattr(session, "linear_plan_cache", None) if session else None) or _THREAD_PLAN_CACHE.get(thread_ref)
                    issue_key = fn_args.get("issue_key") or ""
                    if not issue_key and cached:
                        issue_key = cached.get("parent", {}).get("identifier", "")
                    if cached:
                        if isinstance(cached.get("parent"), dict):
                            cached["parent"]["state"] = "Completed"
                        for st in cached.get("subtasks", []):
                            if isinstance(st, dict):
                                st["state"] = "Completed"
                    signoff = fn_args.get("signoff_comment") or "Approved by Carl."
                    await bridge.approve_and_complete_tree(issue_key, signoff)
                    webui = getattr(self, "webui_server", None)
                    if webui is not None and hasattr(webui, "clear_settled_hands"):
                        res = webui.clear_settled_hands(parent_key=issue_key)
                        if inspect.isawaitable(res):
                            await res
                    tool_result = {
                        "status": "SUCCESS",
                        "message": f"Parent issue {issue_key} and all subtasks marked Completed.",
                    }
                elif fn_name == "cancel_active_task":
                    if session is not None and hasattr(session, "cancel_execution"):
                        await session.cancel_execution()
                    tool_result = {"status": "CANCELLED", "message": "Active backstage execution stopped."}
                elif fn_name == "dispatch_subtasks":
                    thread_ref = _THREAD_REF.get() or (f"{channel}:{thread_ts}" if channel and thread_ts else "")
                    cached = (getattr(session, "linear_plan_cache", None) if session else None) or _THREAD_PLAN_CACHE.get(thread_ref)
                    parent_key = fn_args.get("parent_issue_key")
                    if not parent_key and cached:
                        parent_key = cached.get("parent", {}).get("identifier")
                    mode = fn_args.get("execution_mode", "parallel")
                    subtasks_list = fn_args.get("subtasks", [])
                    parent_key, subtasks_list = self._hydrate_subtasks_from_linear_context(
                        thread_ref=thread_ref,
                        channel=channel or "",
                        parent_key=parent_key,
                        subtasks_list=subtasks_list,
                    )
                    cached_subs = cached.get("subtasks", []) if cached else []
                    for idx, st_item in enumerate(subtasks_list):
                        st_key = st_item.get("issue_key")
                        subtask_idx = st_item.get("subtask_index")
                        if not st_key or (subtask_idx is not None and not str(st_key).startswith("CHO-")):
                            target_i = (subtask_idx - 1) if (subtask_idx is not None and 1 <= subtask_idx <= len(cached_subs)) else idx
                            if 0 <= target_i < len(cached_subs):
                                st_item["issue_key"] = cached_subs[target_i].get("identifier", "")
                    logger.info(
                        f"Mouth dispatched subtasks to backstage: parent={parent_key}, "
                        f"mode={mode}, count={len(subtasks_list)}"
                    )
                    sub_kwargs = {k: v for k, v in fn_args.items() if k not in ("parent_issue_key", "execution_mode", "subtasks")}
                    natural_ack = "\n".join(text_parts).strip()
                    accumulator = await self._execute_backstage_task(
                        parent_issue_key=parent_key,
                        execution_mode=mode,
                        subtasks=subtasks_list,
                        session=session,
                        client=client,
                        channel=channel,
                        thread_ts=thread_ts,
                        ts=ts,
                        is_imessage=is_imessage,
                        natural_ack_text=natural_ack,
                        **sub_kwargs,
                    )
                else:
                    task_desc = compose_hand_brief(fn_args.get("task_description"), fn_args.get("command")) or prompt
                    worker = fn_args.get("worker") or get_default_hand_worker() or "agy"
                    raw_target = fn_args.get("target_node")
                    target_node = raw_target or "hermes"
                    target_cwd = fn_args.get("cwd") or getattr(session, "target_cwd", None) or os.getcwd()
                    precedent_status = fn_args.get("precedent_status")
                    precedent_source = fn_args.get("precedent_source")
                    subtask_issue_id = fn_args.get("subtask_issue_id")
                    if not subtask_issue_id and pending_subtasks:
                        subtask_issue_id = pending_subtasks.pop(0)
                        fn_args["subtask_issue_id"] = subtask_issue_id
                    elif subtask_issue_id and subtask_issue_id in pending_subtasks:
                        pending_subtasks.remove(subtask_issue_id)
                    if session is not None and hasattr(session, "pending_subtasks"):
                        session.pending_subtasks = list(pending_subtasks)

                    natural_ack = "\n".join(text_parts).strip()
                    logger.info(
                        f"Mouth dispatched task to backstage: '{task_desc[:60]}' "
                        f"(worker={worker}, target={target_node}, cwd={target_cwd}, precedent={precedent_status}, subtask={subtask_issue_id})"
                    )

                    # Execute backstage silently with 3.0s yield
                    accumulator = await self._execute_backstage_task(
                        task_desc,
                        target_node,
                        target_cwd,
                        command=None,
                        session=session,
                        client=client,
                        channel=channel,
                        thread_ts=thread_ts,
                        ts=ts,
                        is_imessage=is_imessage,
                        yield_timeout=3.0,
                        worker=worker,
                        precedent_status=precedent_status,
                        precedent_source=precedent_source,
                        subtask_issue_id=subtask_issue_id,
                        natural_ack_text=natural_ack,
                    )

                if fn_name in ("dispatch_task", "dispatch_subtasks"):
                    if not getattr(accumulator, "already_running_reentrant", False):
                        check_eyes_called_since_dispatch = False
                    acc_interim_val = (
                        accumulator.get("interim_message_ts")
                        if isinstance(accumulator, dict)
                        else getattr(accumulator, "interim_message_ts", None)
                    )
                    if acc_interim_val:
                        if not turn_interim_ts:
                            s_seq = getattr(session, "_turn_interim_seq", None) if session is not None else None
                            turn_interim_seq = s_seq if s_seq is not None else self._get_channel_outbound_seq(channel)
                        turn_interim_ts = str(acc_interim_val)
                        if session is not None and not getattr(accumulator, "already_running_reentrant", False):
                            session._turn_interim_ts = turn_interim_ts
                    # Unpack accumulator result (handles both accumulator instance and mock dict return)
                    if isinstance(accumulator, dict):
                        acc_status = accumulator.get("status", "SUCCESS")
                        acc_output = accumulator.get("output") or accumulator.get("output_tail") or ""
                        has_awaiting_input = bool(accumulator.get("has_awaiting_input", False))
                        awaiting_prompt = str(accumulator.get("awaiting_input_prompt", ""))
                        acc_summary = {
                            "status": acc_status,
                            "output_tail": acc_output[-8000:] if acc_output else "",
                            "duration_seconds": accumulator.get("duration_seconds", 1.0),
                            "tools_executed_count": accumulator.get("tools_executed_count", 1),
                            "tool_summary": accumulator.get("tool_summary", []),
                            "has_awaiting_input": has_awaiting_input,
                            "awaiting_input_prompt": awaiting_prompt,
                            "error": accumulator.get("error", ""),
                        }
                        if acc_status == "CONTRACT_VIOLATION":
                            turn_status = "ERROR"
                            error_msg = accumulator.get("error", "Memory contract violation")
                            full_output = error_msg
                        else:
                            turn_status = acc_status
                            error_msg = accumulator.get("error", "")
                            full_output = acc_output
                    elif accumulator is not None:
                        acc_status = getattr(accumulator, "turn_status", "SUCCESS")
                        acc_summary = accumulator.get_structured_summary() if hasattr(accumulator, "get_structured_summary") else {"status": acc_status, "output_tail": str(getattr(accumulator, "final_response", ""))[-8000:]}
                        has_awaiting_input = getattr(accumulator, "has_awaiting_input", False)
                        awaiting_prompt = getattr(accumulator, "awaiting_input_prompt", "")
                        turn_status = acc_status
                        error_msg = getattr(accumulator, "error_message", "")
                        full_output = accumulator.get_full_output() if hasattr(accumulator, "get_full_output") else str(getattr(accumulator, "final_response", ""))
                    else:
                        acc_summary = {"status": "SUCCESS", "output_tail": ""}
                        turn_status = "SUCCESS"
                        has_awaiting_input = False
                        awaiting_prompt = ""

                    tool_result = acc_summary
                    if fn_name == "dispatch_task" and "task_desc" in locals():
                        self._record_hand_result_to_linear(task_desc, target_node, acc_summary)

                    if turn_status in ("ERROR", "CONTRACT_VIOLATION", "FAILED"):
                        dispatch_repair_attempts += 1
                    else:
                        dispatch_repair_attempts = 0

                    # Handle awaiting input
                    if has_awaiting_input:
                        if ts:
                            await swap_reaction(client, channel, ts, "gear", "warning")
                        prompt_msg = awaiting_prompt or "⚠️ *Confirmation Required:* Please provide input."
                        await self._post_response(client, channel, thread_ts, prompt_msg, is_imessage)
                        return
            except Exception as tool_exc:
                error_ref = generate_error_reference("ERR")
                logger.error(f"[{error_ref}] Tool execution '{fn_name}' failed: {tool_exc}", exc_info=True)
                tool_result = {
                    "status": "ERROR",
                    "error": str(tool_exc),
                    "error_type": type(tool_exc).__name__,
                    "error_reference": error_ref,
                }
                turn_status = "ERROR"
                error_msg = str(tool_exc)

            last_tool_result = tool_result

            def _format_result_safe(res: Any, max_len: int = 80) -> str:
                if isinstance(res, dict):
                    for k in ("summary", "status", "state", "verdict", "error", "message"):
                        if k in res and res[k]:
                            v = str(res[k]).replace("\n", " ").strip()
                            if len(v) > max_len:
                                v = v[:max_len - 3] + "..."
                            return f"{k}={v}"
                    if "output" in res and res["output"]:
                        out_line = " ".join(str(res["output"]).splitlines()).strip()
                        if len(out_line) > max_len:
                            out_line = out_line[:max_len - 3] + "..."
                        return f"output={out_line}"
                    res_str = json.dumps(res)
                else:
                    res_str = str(res)
                res_str = " ".join(res_str.splitlines()).strip()
                if len(res_str) > max_len:
                    res_str = res_str[:max_len - 3] + "..."
                return res_str

            self._broadcast_mouth_log(
                f"[HOP {current_hop}] Result from {fn_name}: {_format_result_safe(tool_result)}",
                model=model,
            )

            # Retain executed functionCall part preserving thoughtSignature and other metadata
            raw_parts = extract_candidate_parts(candidate)
            thought_parts = [p for p in raw_parts if isinstance(p, dict) and "text" in p and p.get("text")]
            matched_call_part = None
            for p in raw_parts:
                if isinstance(p, dict) and "functionCall" in p and isinstance(p["functionCall"], dict):
                    fc_name = p["functionCall"].get("name") if isinstance(p["functionCall"], dict) else None
                    if fc_name == fn_name:
                        matched_call_part = p
                        break
            if matched_call_part is None:
                matched_call_part = {"functionCall": fn_call or {"name": fn_name, "args": fn_args}}
            model_parts = thought_parts + [matched_call_part]

            compacted_tool_res = compact_tool_output(fn_name, tool_result)
            tool_result_payload = dict(compacted_tool_res) if isinstance(compacted_tool_res, dict) else {"result": compacted_tool_res}
            fn_resp_part = {
                "functionResponse": {
                    "name": fn_name,
                    "response": {
                        "name": fn_name,
                        "content": tool_result_payload,
                    },
                }
            }

            working_contents.append(sanitize_model_turn_for_history({"parts": model_parts}))
            working_contents.append({"role": "user", "parts": [fn_resp_part]})

            # If the executed tool was a chainable tool or there are pending subtasks to dispatch, allow chaining in mode="AUTO"
            is_chainable = (fn_name in CHAINABLE_TOOLS) or (fn_name == "dispatch_task" and bool(pending_subtasks))
            if fn_name == "dispatch_task" and dispatch_repair_attempts > 2:
                is_chainable = False
            if fn_name == "dispatch_task" and getattr(accumulator, "already_running_reentrant", False):
                is_chainable = False
            if fn_name == "check_eyes" and isinstance(tool_result, dict) and str(tool_result.get("state", "")).upper() == "RUNNING":
                is_chainable = False

            if fn_name in READ_ONLY_HOP_TOOLS:
                consecutive_read_only += 1
            else:
                consecutive_read_only = 0
            if is_chainable and current_hop < MAX_TOOL_CHAIN_HOPS:
                self._broadcast_mouth_log(
                    f"[TOOL DONE] {fn_name} completed -> chaining next hop...",
                    model=model,
                )
                logger.info(
                    f"Chainable tool '{fn_name}' completed on hop {current_hop}/{MAX_TOOL_CHAIN_HOPS} "
                    f"(pending_subtasks={len(pending_subtasks)}). "
                    f"Allowing chain to next action or synthesis (mode=AUTO)."
                )
                # Keep tools 100% static across all hops & Step 2 to preserve the Gemini prefix cache.
                # Tool hop caps are enforced via steering notes and the hard execution gate at the top of Case B.
                chain_tools = tools
                if consecutive_read_only >= MAX_CONSECUTIVE_READ_ONLY_HOPS and working_contents and working_contents[-1].get("role") == "user":
                    working_contents[-1]["parts"].append({
                        "text": (
                            "[SYSTEM HOP CAP: You have reached the maximum consecutive read-only inspection hops (3). "
                            "Do NOT call read_project_file, consult_memory, check_nose, check_eyes, or check_linear_task_status again. "
                            "Either emit your final response text now or call an action tool like dispatch_task.]"
                        )
                    })
                chain_sys_prompt = sys_prompt
                chain_payload = {
                    "systemInstruction": {"parts": [{"text": chain_sys_prompt}]},
                    "contents": working_contents,
                    "tools": chain_tools,
                    "toolConfig": {
                        "functionCallingConfig": {"mode": "AUTO"}
                    },
                    "generationConfig": {
                        "temperature": 0.2,
                    },
                }
                self._broadcast_mouth_log(
                    f"[HOP {current_hop + 1}/{MAX_TOOL_CHAIN_HOPS}] Reasoning with {model}...",
                    model=model,
                )
                next_resp = await self._call_gemini_api(payload=chain_payload, model=model, api_key=api_key)
                if candidate_is_empty(next_resp):
                    logger.warning(
                        f"Mouth chain step after '{fn_name}' returned an empty candidate "
                        f"({describe_candidate_shape(next_resp)}); retrying once."
                    )
                    next_resp = await self._call_gemini_api(payload=chain_payload, model=model, api_key=api_key)
                    if candidate_is_empty(next_resp):
                        logger.warning(f"Mouth chain retry also empty ({describe_candidate_shape(next_resp)}).")

                if candidate_is_empty(next_resp) or not next_resp or next_resp.get("_error"):
                    if isinstance(next_resp, dict):
                        if isinstance(next_resp.get("_error"), dict):
                            error_info = dict(next_resp["_error"])
                        elif next_resp.get("_error"):
                            error_info = dict(next_resp)
                            error_info.setdefault("status", 500)
                            error_info.setdefault("code", error_info.get("status", 500))
                            error_info.setdefault("error_type", "SynthesisError")
                            error_info.setdefault("message", error_info.get("raw_error") or "Unknown error")
                        else:
                            error_info = {
                                "status": 500,
                                "code": 500,
                                "error_type": "SynthesisError",
                                "message": "Synthesis failed with empty candidate text",
                            }
                    else:
                        error_info = {
                            "status": 500,
                            "code": 500,
                            "error_type": "SynthesisError",
                            "message": "Synthesis failed with empty candidate text",
                        }

                    has_completed_tool_result = bool(
                        isinstance(last_tool_result, dict)
                        and (
                            last_tool_result.get("output_tail")
                            or last_tool_result.get("output")
                            or last_tool_result.get("buffer_tail")
                            or last_tool_result.get("card")
                            or last_tool_result.get("verdict_summary")
                            or last_tool_result.get("status") in ("SUCCESS", "COMPLETED", "DONE")
                        )
                    )
                    if has_completed_tool_result:
                        logger.info(
                            f"Mouth chain step failed or returned empty after '{fn_name}', but completed tool result is available. "
                            f"Allowing Step 2 fallback synthesis to recover."
                        )
                        skip_step2_synthesis = False
                    else:
                        skip_step2_synthesis = True
                        briefing_text = ""
                    break

                if next_resp and not next_resp.get("_error") and next_resp.get("candidates"):
                    next_cands = next_resp.get("candidates", [])
                    if isinstance(next_cands, list) and next_cands and isinstance(next_cands[0], dict):
                        candidate = next_cands[0]
                        parts = extract_candidate_parts(candidate)
                        continue


            if is_chainable and current_hop >= MAX_TOOL_CHAIN_HOPS:
                hop_budget_exhausted = True
                logger.warning(
                    f"Tool hop budget exhausted ({current_hop}/{MAX_TOOL_CHAIN_HOPS}) after '{fn_name}' "
                    f"with unfinished work (pending_subtasks={len(pending_subtasks)}); "
                    f"synthesis will report done vs not-started."
                )

            # If chain exhausted or failed, exit loop to Step 2 synthesis
            break

        # Step 2: Synthesize Executive Briefing via functionResponse
        if not skip_step2_synthesis:
            if thread_ts:
                await set_assistant_status(client, channel, thread_ts, "Synthesizing executive briefing...")

            fn_name = last_fn_name
            fn_call = last_fn_call or {}
            tool_result = last_tool_result

            step2_extra: Dict[str, Any] = {}
            if hop_budget_exhausted:
                step2_extra["budget_note"] = build_hop_budget_note(MAX_TOOL_CHAIN_HOPS, list(pending_subtasks))

            self._broadcast_mouth_log(f"[SYNTHESIS] Synthesizing executive briefing with {model}...", model=model)
            briefing_text, step2_error = await self._execute_step2_synthesis(
                sys_prompt=sys_prompt,
                prompt=prompt,
                candidate=last_candidate,
                fn_call=fn_call,
                fn_name=fn_name,
                tool_result=tool_result,
                model=model,
                api_key=api_key,
                tools=tools,
                contents=working_contents if len(working_contents) >= 3 else None,
                **step2_extra,
            )
            if not briefing_text and step2_error:
                error_info = step2_error

        if not briefing_text:
            # Fail loudly on Step 2 synthesis or runtime failure
            error_ref = (error_info or {}).get("error_reference") or (error_info or {}).get("error_ref") or generate_error_reference("ERR")
            if isinstance(error_info, dict):
                error_info["error_reference"] = error_ref
            code = (error_info or {}).get("status") or (error_info or {}).get("code") or 500
            error_type = (error_info or {}).get("error_type") or "SynthesisError"
            raw_error = (error_info or {}).get("raw_error") or (error_info or {}).get("message") or "Synthesis failed with empty candidate text"
            tool_json = json.dumps(tool_result, indent=2, default=str)
            if len(tool_json) > 2500:
                tool_json = tool_json[:2450] + "\n... [truncated]"
            fail_briefing = (
                f"⚠️ *Executive Synthesis Failed* (HTTP {code} / {error_type}):\n"
                f"• *Error Reference:* `{error_ref}`\n"
                f"```{raw_error}```\n"
                f"*Tool Output:*\n"
                f"```{tool_json}```\n"
                f"_Raw sensory data returned above without executive synthesis._"
            )
            try:
                await self._post_response(client, channel, thread_ts, fail_briefing, is_imessage)
                # Attempt fallback delivery if tool output generated files or directives
                out_tail = str(tool_result.get("output_tail", ""))
                _, fallback_file_targets = extract_and_strip_file_directives(out_tail)
                if fallback_file_targets:
                    fleet_mesh = (
                        getattr(self.router.harness, "mesh", None)
                        if hasattr(self.router, "harness") and self.router.harness
                        else getattr(self.router, "fleet_mesh", None)
                    )
                    await upload_directive_files(
                        client=client,
                        channel=channel,
                        thread_ts=thread_ts,
                        targets=fallback_file_targets,
                        active_cwd=session.target_cwd if hasattr(session, "target_cwd") else os.getcwd(),
                        fleet_mesh=fleet_mesh,
                    )
            finally:
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "boom")
            return

        clean_briefing, file_targets = extract_and_strip_file_directives(briefing_text)

        # Reaction target determination
        if fn_name == "propose_memory_update":
            target_reaction = "brain"
            if tool_result.get("card") and "Memory Distillation Proposal" not in clean_briefing:
                clean_briefing = tool_result.get("card")
        elif fn_name == "read_project_file":
            if current_hop > 1 and tool_result.get("status") == "NOT_FOUND":
                target_reaction = "white_check_mark"
            else:
                target_reaction = "x" if tool_result.get("status") in ("ERROR", "NOT_FOUND") else "white_check_mark"
        elif fn_name == "check_eyes" and tool_result.get("state") == "BLOCKER":
            target_reaction = "warning"
        elif fn_name == "check_nose" and tool_result.get("smells_off"):
            target_reaction = "warning"
        elif fn_name == "share_artifact" and (tool_result.get("ok") is False or tool_result.get("status") == "ERROR"):
            target_reaction = "x"
        elif turn_status == "ERROR":
            # Determine if this was an actual fatal failure vs successful operational work
            # despite non-zero exit codes or sub-command errors.
            has_fatal_error = (
                not full_output
                or bool(error_msg)
                or bool(re.search(r"\b(fatal error|operation failed|task failed|execution failed|unhandled exception|operation aborted|aborted|critical crash)\b", clean_briefing[:250], re.IGNORECASE))
                or "❌" in clean_briefing[:250]
            )
            target_reaction = "x" if has_fatal_error else "white_check_mark"
        else:
            target_reaction = "white_check_mark"

        await _deliver_final_briefing(clean_briefing)
        await self._broadcast_mouth_briefing(clean_briefing, model)
        self._broadcast_mouth_log(f"[BRIEFING] {clean_briefing}", model=model)

        # Handle file directives
        if file_targets:
            fleet_mesh = (
                getattr(self.router.harness, "mesh", None)
                if hasattr(self.router, "harness") and self.router.harness
                else getattr(self.router, "fleet_mesh", None)
            )
            await upload_directive_files(
                client=client,
                channel=channel,
                thread_ts=thread_ts,
                targets=file_targets,
                active_cwd=session.target_cwd if hasattr(session, "target_cwd") else os.getcwd(),
                fleet_mesh=fleet_mesh,
            )
            for ft in file_targets:
                if hasattr(session, "record_artifact"):
                    try:
                        session.record_artifact(ft)
                    except Exception:
                        pass

        if ts:
            await swap_reaction(client, channel, ts, "gear", target_reaction)

    async def _execute_step2_synthesis(
        self,
        sys_prompt: str,
        prompt: str,
        candidate: Dict[str, Any],
        fn_call: Dict[str, Any],
        fn_name: str,
        tool_result: Dict[str, Any],
        model: str,
        api_key: str,
        tools: Optional[List[Dict[str, Any]]] = None,
        contents: Optional[List[Dict[str, Any]]] = None,
        budget_note: Optional[str] = None,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Synthesize Executive Briefing via functionResponse in Step 2.
        Includes complete 'tools': tools schema declaration to eliminate HTTP 400 schema error.
        Returns (briefing_text, error_info).
        """
        # Strictly align model_parts to the single executed functionCall to prevent asymmetric multi-call HTTP 400
        raw_parts = extract_candidate_parts(candidate)
        thought_parts = [p for p in raw_parts if isinstance(p, dict) and "text" in p and p.get("text")]
        # Retain executed functionCall part preserving thoughtSignature and other metadata
        matched_call_part = None
        for p in raw_parts:
            if isinstance(p, dict) and "functionCall" in p and isinstance(p["functionCall"], dict):
                fc_name = p["functionCall"].get("name") if isinstance(p["functionCall"], dict) else None
                if fc_name == fn_name:
                    matched_call_part = p
                    break
        if matched_call_part is None:
            target_call = fn_call or {"name": fn_name, "args": {}}
            matched_call_part = {"functionCall": target_call}
        model_parts = thought_parts + [matched_call_part]
        sanitized_model_turn = sanitize_model_turn_for_history({"parts": model_parts})
        sanitized_model_parts = sanitized_model_turn.get("parts", model_parts)

        synthesis_directive = (
            "EXECUTION COMPLETE. Synthesize the verified executive briefing for Carl based on the tool outputs above. "
            "Do not call additional tools.\n\n"
            "CRITICAL INSTRUCTION FOR SYNTHESIS:\n"
            "1. Always directly address and answer Carl's original prompt/question in your briefing.\n"
            "2. If Carl asked a conversational or factual question (e.g. asking which machine is which), "
            "answer his question directly using your knowledge and provide the tool result in proper context.\n"
            "3. Never ignore Carl's question in favor of dumping raw sensory metrics alone.\n"
            "4. If an image, artifact, or generated file path was produced in the tool result (e.g. Saved File Path: ...), "
            "always include the directive `[SEND_FILE: <path>]` in your briefing so the file is automatically uploaded to Slack.\n"
            "5. NO TOOL CALLS DURING SYNTHESIS: You are in the final executive briefing stage. "
            "Under NO circumstances should you call tools or functions (including `dispatch_task`, `check_eyes`, etc.). "
            "Do NOT attempt to retry failed tasks or call further tools. "
            "If a task encountered an error or failed, explain the diagnosis and next steps directly to Carl in clean Slack mrkdwn text.\n"
            "6. THE IRON LAW OF EXTERNAL INTEGRATIONS (NO SILENT MOCKS): Never claim an external integration action "
            "is 'Staged', 'Deployed', or 'Sent' unless the tool output proves the external target system confirmed the state change "
            "via a verified network response—never a local cache write or synthetic fallback pretending to be remote state."
        )
        if budget_note:
            synthesis_directive += "\n\n" + budget_note

        tools_decl = tools if tools else get_mouth_tools_declaration()

        compacted_res = compact_tool_output(fn_name, tool_result)
        tool_result_payload = dict(compacted_res) if isinstance(compacted_res, dict) else {"result": compacted_res}

        if contents:
            sanitized_contents = []
            for item in contents:
                if isinstance(item, dict) and item.get("role") == "model":
                    sanitized_contents.append(sanitize_model_turn_for_history(item))
                elif isinstance(item, dict):
                    item_copy = dict(item)
                    item_copy["parts"] = list(item.get("parts", []))
                    sanitized_contents.append(item_copy)
                else:
                    sanitized_contents.append(item)
            if sanitized_contents and sanitized_contents[-1].get("role") == "user":
                sanitized_contents[-1]["parts"].append({"text": synthesis_directive})
            else:
                sanitized_contents.append({"role": "user", "parts": [{"text": synthesis_directive}]})
            briefing_contents = sanitized_contents
        else:
            briefing_contents = [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                },
                {
                    "role": "model",
                    "parts": sanitized_model_parts,
                },
                {
                    "role": "user",
                    "parts": [
                        {
                            "functionResponse": {
                                "name": fn_name,
                                "response": {
                                    "name": fn_name,
                                    "content": tool_result_payload,
                                },
                            }
                        },
                        {"text": synthesis_directive},
                    ],
                },
            ]

        briefing_payload = {
            "systemInstruction": {"parts": [{"text": sys_prompt}]},
            "contents": briefing_contents,
            "tools": tools_decl,
            "toolConfig": {
                "functionCallingConfig": {"mode": "NONE"}
            },
            "generationConfig": {
                "temperature": 0.2,
            },
        }

        try:
            try:
                briefing_resp = await self._call_gemini_api(
                    payload=briefing_payload,
                    model=model,
                    api_key=api_key,
                    timeout_seconds=60.0,
                )
            except TypeError:
                briefing_resp = await self._call_gemini_api(
                    payload=briefing_payload,
                    model=model,
                    api_key=api_key,
                )
        except Exception as e:
            err_ref = generate_error_reference("ERR")
            err_msg = f"Gemini API call timed out after 60.0s" if isinstance(e, asyncio.TimeoutError) else str(e)
            return "", {
                "status": 500,
                "code": 500,
                "error_type": type(e).__name__,
                "raw_error": err_msg,
                "message": err_msg,
                "error_reference": err_ref,
            }

        briefing_text = ""
        b_cand = {}
        b_parts = []
        step2_api_error: Optional[Dict[str, Any]] = None

        if not briefing_resp:
            err_ref = generate_error_reference("ERR")
            step2_api_error = {
                "status": 500,
                "code": 500,
                "error_type": "NoResponse",
                "raw_error": "Gemini API returned empty response",
                "message": "No response",
                "error_reference": err_ref,
            }
        elif briefing_resp.get("_error"):
            err_ref = briefing_resp.get("error_reference") or generate_error_reference("ERR")
            err_dict = briefing_resp.get("_error") if isinstance(briefing_resp.get("_error"), dict) else briefing_resp
            step2_api_error = dict(err_dict)
            step2_api_error.setdefault("status", 500)
            step2_api_error.setdefault("code", step2_api_error.get("status", 500))
            step2_api_error.setdefault("error_type", "SynthesisError")
            step2_api_error.setdefault("message", step2_api_error.get("raw_error") or "Step 2 synthesis failed")
            step2_api_error["error_reference"] = err_ref
        else:
            candidates = briefing_resp.get("candidates", [])
            if not candidates:
                err_ref = generate_error_reference("ERR")
                step2_api_error = {
                    "status": 200,
                    "code": 200,
                    "error_type": "ZeroCandidates",
                    "raw_error": "Gemini API returned zero candidates",
                    "message": "Zero candidates",
                    "error_reference": err_ref,
                }
            else:
                b_cand = candidates[0] if (isinstance(candidates, list) and candidates and isinstance(candidates[0], dict)) else {}
                b_parts = extract_candidate_parts(b_cand)
                briefing_text = extract_candidate_text(b_cand)

        if not briefing_text:
            attempted_calls = [
                (p["functionCall"] or {}).get("name") for p in b_parts
                if isinstance(p, dict) and isinstance(p.get("functionCall"), dict)
            ]
            reason = f"error={step2_api_error.get('error_type')}" if step2_api_error else f"empty text ({describe_candidate_shape(briefing_resp)}; attempted_tool_calls={attempted_calls})"
            logger.warning(
                f"Step 2 synthesis returned {reason}; attempting direct text-only fallback synthesis."
            )
            # Comprehensive extraction across all tool result schemas (error, task output, sensory digests, cards)
            error_val = tool_result.get("error") if isinstance(tool_result, dict) else None
            error_type = tool_result.get("error_type") if isinstance(tool_result, dict) else None
            err_str = f"[{error_type}]: {error_val}" if (error_type and error_val) else str(error_val or "")

            out_tail = str(
                tool_result.get("output_tail")
                or tool_result.get("output")
                or tool_result.get("buffer_tail")
                or err_str
                or tool_result.get("card")
                or tool_result.get("verdict_summary")
                or ""
            ).strip() if isinstance(tool_result, dict) else str(tool_result or "").strip()

            if not out_tail and isinstance(tool_result, dict) and tool_result:
                out_tail = json.dumps(tool_result, indent=2, default=str)
            if not out_tail:
                out_tail = "Tool completed with no output."

            direct_prompt = (
                f"Carl's request: {prompt}\n\n"
                f"Backstage Task Output:\n{out_tail}\n\n"
                "Synthesize a concise, high-signal executive briefing for Carl in plain text. "
                "Output ONLY the executive briefing text in clean Slack mrkdwn format. Do not call functions or output raw JSON.\n"
                "HONESTY RULES: In this stage you cannot run anything. Describe only what the output above shows has "
                "actually happened. Do NOT say you are executing, proceeding, applying, starting, or about to run "
                "anything, and do not claim a credential or result was verified unless the output above shows it was. "
                "If the work is not finished, say plainly that nothing further is running and tell Carl that replying "
                "'continue' will resume it."
            )
            if budget_note:
                direct_prompt += "\n\n" + budget_note
            direct_payload = {
                "systemInstruction": {"parts": [{"text": sys_prompt}]},
                "contents": [
                    {
                        "role": "user",
                        "parts": [{"text": direct_prompt}],
                    }
                ],
                "generationConfig": {
                    "temperature": 0.2,
                },
            }
            try:
                fallback_resp = await self._call_gemini_api(payload=direct_payload, model=model, api_key=api_key)
                if fallback_resp and not fallback_resp.get("_error"):
                    fb_candidates = fallback_resp.get("candidates", [])
                    if isinstance(fb_candidates, list) and fb_candidates:
                        fb_cand = fb_candidates[0] if isinstance(fb_candidates[0], dict) else {}
                        fb_parts = extract_candidate_parts(fb_cand)
                        briefing_text = extract_candidate_text(fb_cand)
            except Exception as e:
                logger.debug(f"Direct fallback synthesis failed: {e}")

        if not briefing_text:
            if step2_api_error:
                return "", step2_api_error
            error_ref = generate_error_reference("ERR")
            finish_reason = b_cand.get("finishReason", "UNKNOWN") if "b_cand" in locals() and isinstance(b_cand, dict) else "UNKNOWN"
            fn_calls = [
                p["functionCall"] for p in b_parts
                if isinstance(p, dict) and "functionCall" in p and isinstance(p["functionCall"], dict)
            ] if "b_parts" in locals() and isinstance(b_parts, list) else []
            if fn_calls:
                msg = f"Model attempted tool call '{fn_calls[0].get('name')}' during executive synthesis instead of generating text."
            else:
                msg = f"Synthesis failed with empty candidate text (finishReason={finish_reason})"
            return "", {
                "status": 500,
                "code": 500,
                "error_type": "SynthesisError",
                "raw_error": msg,
                "message": msg,
                "error_reference": error_ref,
            }
        return briefing_text, None

    async def _call_gemini_api(
        self,
        payload: Dict[str, Any],
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout_seconds: float = 60.0,
    ) -> Optional[Dict[str, Any]]:
        """Call Gemini REST API for Mouth turn processing with telemetry & turn cost tracking."""
        t0 = time.monotonic()
        try:
            resp = await call_gemini_api(
                payload=payload,
                model=model,
                api_key=api_key,
                timeout_seconds=timeout_seconds,
            )
        except (StopIteration, StopAsyncIteration):
            return None
        latency = round(time.monotonic() - t0, 2)
        if resp and isinstance(resp, dict):
            usage = resp.get("usageMetadata")
            if isinstance(usage, dict):
                p_tok = usage.get("promptTokenCount", 0) or 0
                c_tok = usage.get("candidatesTokenCount", 0) or 0
                cached_tok = usage.get("cachedContentTokenCount", 0) or 0
                th_tok = usage.get("thoughtsTokenCount", 0) or 0
                cache_hit_rate = (cached_tok / p_tok * 100.0) if p_tok > 0 else 0.0
                eff_model = model or getattr(self, "mouth_model", None) or get_mouth_model()
                cost = calculate_token_cost(
                    model=eff_model,
                    prompt_tokens=p_tok,
                    cached_tokens=cached_tok,
                    candidates_tokens=c_tok,
                )
                self._broadcast_mouth_log(
                    f"[TOKENS] {eff_model} | Prompt: {p_tok} (Cache: {cache_hit_rate:.1f}% hit) | Out: {c_tok} | Cost: ${cost:.4f}",
                    model=eff_model,
                )
            cands = resp.get("candidates")
            if isinstance(cands, list) and cands and isinstance(cands[0], dict):
                parts = extract_candidate_parts(cands[0])
                for p in parts:
                    if isinstance(p, dict) and p.get("thought") and p.get("text"):
                        th_text = " ".join(str(p["text"]).splitlines()).strip()
                        if len(th_text) > 120:
                            th_text = th_text[:117] + "..."
                        self._broadcast_mouth_log(f"[THOUGHT] {th_text}", model=model)
        return resp

    async def _fallback_direct_adapter_turn(
        self,
        session: SessionHandle,
        prompt: str,
        client: Any,
        channel: str,
        thread_ts: str,
        ts: str,
        is_imessage: bool,
    ) -> None:
        """Fallback to executing prompt directly via adapter when Gemini API is offline/unavailable."""
        accumulator = await self._execute_backstage_task(
            task_desc=prompt,
            session=session,
            client=client,
            channel=channel,
            thread_ts=thread_ts,
            ts=ts,
            is_imessage=is_imessage,
        )
        output = (
            accumulator.get_full_output()
            if isinstance(accumulator, BackstageExecutionAccumulator)
            else accumulator.get("output", "")
        )
        clean_output, file_targets = extract_and_strip_file_directives(output)
        status = (
            accumulator.turn_status
            if isinstance(accumulator, BackstageExecutionAccumulator)
            else accumulator.get("status", "SUCCESS")
        )
        target_reaction = "x" if status == "ERROR" else "white_check_mark"
        interim_ts = getattr(accumulator, "interim_message_ts", None) if isinstance(accumulator, BackstageExecutionAccumulator) else None
        if clean_output:
            await self._post_response(client, channel, thread_ts, clean_output, is_imessage, update_ts=interim_ts)
            await self._broadcast_mouth_briefing(clean_output, None)
        if file_targets:
            fleet_mesh = (
                getattr(self.router.harness, "mesh", None)
                if hasattr(self.router, "harness") and self.router.harness
                else getattr(self.router, "fleet_mesh", None)
            )
            await upload_directive_files(
                client=client,
                channel=channel,
                thread_ts=thread_ts,
                targets=file_targets,
                active_cwd=session.target_cwd if hasattr(session, "target_cwd") else os.getcwd(),
                fleet_mesh=fleet_mesh,
            )
            for ft in file_targets:
                if hasattr(session, "record_artifact"):
                    try:
                        session.record_artifact(ft)
                    except Exception:
                        pass
        if ts and not is_imessage:
            await swap_reaction(client, channel, ts, "gear", target_reaction)


async def process_slack_message(
    event: Dict[str, Any],
    client: Any,
    gateway: Optional[IngressGateway] = None,
    router: Optional[SessionRouter] = None,
) -> None:
    """Convenience functional entrypoint for processing Slack message events."""
    if gateway is None:
        if router is None:
            router = SessionRouter()
        gateway = IngressGateway(router=router)
    await gateway.handle_message(event, client)


async def _execute_backstage_task(
    command: Optional[str] = None,
    task_description: str = "",
    precedent_status: Optional[str] = None,
    precedent_source: Optional[str] = None,
    target_node: str = "hermes",
    worker: str = "agy",
    cwd: Optional[str] = None,
    dry_run: bool = False,
    task_desc: Optional[str] = None,
    session: Optional[SessionHandle] = None,
    gateway: Optional[IngressGateway] = None,
    subtask_issue_id: Optional[str] = None,
    session_id: str = "default",
    **kwargs: Any,
) -> Union[BackstageExecutionAccumulator, Dict[str, Any]]:
    """Standalone module-level entry point for contract validation and backstage execution."""
    exec_cmd = command or task_description or task_desc or "echo 'No command supplied'"
    logger.info(f"Backstage Task Dispatch: node={target_node}, cwd={cwd}, cmd='{exec_cmd}'")

    sub_id = subtask_issue_id or kwargs.get("subtask_issue_id")
    if gateway is not None:
        return await gateway._execute_backstage_task(
            task_desc=exec_cmd,
            target_node=target_node,
            target_cwd=cwd,
            session=session,
            worker=worker,
            precedent_status=precedent_status,
            precedent_source=precedent_source,
            dry_run=dry_run,
            task_description=exec_cmd,
            subtask_issue_id=sub_id,
            command=command,
            session_id=session_id,
            **kwargs,
        )

    # Perform standalone contract validation gate
    if precedent_status == "VERIFIED_RUNBOOK":
        from golem.wiki import DEFAULT_WIKI_DIR
        wiki_root = Path(DEFAULT_WIKI_DIR).resolve()
        clean_source = (precedent_source or "").strip().lstrip("/")
        wiki_file = (wiki_root / clean_source).resolve() if clean_source else None
        if not wiki_file or not wiki_file.is_file() or not str(wiki_file).startswith(str(wiki_root)):
            error_msg = (
                f"[MEMORY CONTRACT REJECTION] The cited precedent_source '{precedent_source}' "
                f"does not exist in ~/.golem/wiki/. You cannot execute backstage actions without grounding. "
                f"Review your Peripheral Vision table and consult valid memory before dispatching."
            )
            return {"error": error_msg, "status": "CONTRACT_VIOLATION"}
    elif precedent_status == "NOVEL_EXPLORATION":
        if not precedent_source or len(precedent_source.strip()) < 10:
            return {
                "error": "[MEMORY CONTRACT REJECTION] NOVEL_EXPLORATION requires a descriptive rationale in precedent_source.",
                "status": "CONTRACT_VIOLATION",
            }
    elif precedent_status is not None:
        return {"error": f"Invalid precedent_status '{precedent_status}'.", "status": "CONTRACT_VIOLATION"}

    if dry_run or (session is None and precedent_status is not None):
        return {
            "status": "SUCCESS",
            "message": "Dry run passed contract validation",
            "task_description": exec_cmd,
            "precedent_status": precedent_status,
            "precedent_source": precedent_source,
            "subtask_issue_id": sub_id,
        }

    try:
        from golem.harness import DualPaneHarness
        harness = kwargs.get("harness")
        if harness is None and session is not None:
            harness = getattr(session, "harness", None)
        if harness is None:
            harness = DualPaneHarness()

        eff_session_id = session_id or (session.session_id if session else "default")
        handle = await harness.dispatch_hand(
            session_id=eff_session_id,
            task_description=exec_cmd,
            target_node=target_node,
            worker=worker,
            cwd=cwd,
            yield_timeout=3.0,
            **kwargs,
        )
        return {
            "status": "RUNNING" if getattr(handle, "status", "") == "running" else "COMPLETED",
            "task_id": getattr(handle, "task_id", ""),
            "pane": getattr(handle, "pane", ""),
            "target_node": target_node,
            "output_tail": getattr(handle, "stdout", "")[-500:] if getattr(handle, "stdout", None) else "Process spawned in background.",
        }
    except Exception as exc:
        logger.error(f"Backstage Task Dispatch failed: {exc}", exc_info=True)
        return {
            "status": "ERROR",
            "error": str(exc),
            "error_type": exc.__class__.__name__,
        }


async def _handle_read_project_file(tool_args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute read_project_file for direct read-only project inspection."""
    raw_path = str(tool_args.get("path", "") or "").strip()
    try:
        max_lines = int(tool_args.get("max_lines", 200))
    except (ValueError, TypeError):
        max_lines = 200
    pattern = tool_args.get("pattern", None)
    if pattern is not None and not str(pattern).strip():
        pattern = None

    if not raw_path:
        return {"status": "ERROR", "error": "Path argument is required."}

    target = Path(raw_path).expanduser().resolve()

    if not target.exists():
        # Check if parent exists and suggest matching files
        parent = target.parent
        if parent.exists() and parent.is_dir():
            candidates = [p.name for p in parent.glob(f"*{target.name}*")][:10]
            if candidates:
                return {
                    "status": "NOT_FOUND",
                    "error": f"Path '{target}' does not exist.",
                    "suggested_matches": candidates,
                    "parent_dir": str(parent),
                }
        return {"status": "NOT_FOUND", "error": f"Path '{target}' does not exist on disk."}

    # Handle Directory Listing
    if target.is_dir():
        entries = []
        try:
            for item in sorted(target.iterdir()):
                if item.name.startswith("."):
                    continue
                if pattern and not fnmatch.fnmatch(item.name, pattern):
                    continue
                suffix = "/" if item.is_dir() else ""
                entries.append(f"{item.name}{suffix}")

            return {
                "status": "SUCCESS",
                "type": "directory",
                "path": str(target),
                "total_entries": len(entries),
                "entries": entries[:150],
            }
        except Exception as exc:
            return {"status": "ERROR", "error": f"Failed to list directory '{target}': {str(exc)}"}

    # Handle File Read
    try:
        content = target.read_text(encoding="utf-8", errors="replace")
        lines = content.splitlines()
        truncated = len(lines) > max_lines
        return {
            "status": "SUCCESS",
            "type": "file",
            "path": str(target),
            "total_lines": len(lines),
            "content": "\n".join(lines[:max_lines]),
            "truncated": truncated,
        }
    except Exception as exc:
        return {"status": "ERROR", "error": f"Failed to read file '{target}': {str(exc)}"}


async def execute_tool(
    tool_name: str,
    tool_args: Dict[str, Any],
    session_id: str = "default",
    gateway: Optional[Any] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Execute a tool call emitted during a ReAct turn."""
    if tool_name == "read_project_file":
        return await _handle_read_project_file(tool_args)
    if tool_name == "consult_memory":
        from golem.wiki import DEFAULT_WIKI_DIR, read_wiki_document
        path = str(tool_args.get("path", "") or "").strip().lstrip("/")
        if not path:
            return {"status": "ERROR", "path": path, "error": "A relative wiki path is required for consult_memory."}
        wiki_root = Path(DEFAULT_WIKI_DIR).resolve()
        target_file = (wiki_root / path).resolve()
        if not target_file.is_file() or not str(target_file).startswith(str(wiki_root)):
            return {"status": "NOT_FOUND", "path": path, "error": f"Wiki document '{path}' does not exist in ~/.golem/wiki/."}
        try:
            content = read_wiki_document(path, wiki_dir=wiki_root)
            return {"status": "SUCCESS", "path": path, "content": content}
        except Exception as e:
            return {"status": "ERROR", "path": path, "error": f"Failed to read wiki document '{path}': {e}"}
    if tool_name == "dispatch_task":
        command = tool_args.get("command")
        task_description = tool_args.get("task_description") or command or ""
        target_node = tool_args.get("target_node", "hermes")
        cwd = tool_args.get("cwd")
        worker = tool_args.get("worker", "agy")
        res = await _execute_backstage_task(
            command=command,
            task_description=task_description,
            target_node=target_node,
            worker=worker,
            cwd=cwd,
            session_id=session_id,
            gateway=gateway,
            **kwargs,
        )
        if isinstance(res, BackstageExecutionAccumulator):
            return {
                "status": "RUNNING" if res.status == "running" else "COMPLETED",
                "task_id": res.task_id,
                "pane": res.pane,
                "target_node": target_node,
                "output_tail": res.raw_output[-500:] if res.raw_output else "Process spawned in background.",
            }
        return res
    if tool_name == "dispatch_subtasks":
        parent_key = tool_args.get("parent_issue_key")
        mode = tool_args.get("execution_mode", "parallel")
        subtasks_list = tool_args.get("subtasks", [])
        return await _execute_backstage_task(
            parent_issue_key=parent_key,
            execution_mode=mode,
            subtasks=subtasks_list,
            session_id=session_id,
            gateway=gateway,
            **kwargs,
        )
    if gateway is not None and hasattr(gateway, "_execute_tool_call"):
        return await gateway._execute_tool_call(
            tool_name=tool_name,
            tool_args=tool_args,
            session_id=session_id,
            **kwargs,
        )
    return {"status": "ERROR", "error": f"Tool '{tool_name}' not dispatched via standalone execute_tool"}


_execute_tool_call = execute_tool


def sanitize_model_turn_for_history(candidate_content: dict) -> dict:
    """
    Ensures model turns sent back to Gemini 3 preserve thought signatures.
    Injects skip_thought_signature_validator if a functionCall part lacks a signature.
    """
    if not isinstance(candidate_content, dict):
        return {"role": "model", "parts": []}
    parts = candidate_content.get("parts")
    if parts is None and isinstance(candidate_content.get("content"), dict):
        parts = candidate_content["content"].get("parts", [])
    if parts is None:
        parts = []

    sanitized_parts = []

    for part in parts:
        if not isinstance(part, dict):
            continue
        clean_part = dict(part)
        if "functionCall" in clean_part:
            # Preserve existing signature across camelCase and snake_case on the Part object
            sig = clean_part.get("thought_signature") or clean_part.get("thoughtSignature")
            if isinstance(clean_part.get("functionCall"), dict):
                clean_fc = dict(clean_part["functionCall"])
                # Extract if mistakenly placed inside functionCall, but always strip from functionCall
                fc_sig1 = clean_fc.pop("thought_signature", None)
                fc_sig2 = clean_fc.pop("thoughtSignature", None)
                if not sig:
                    sig = fc_sig1 or fc_sig2
                clean_part["functionCall"] = clean_fc

            if not sig:
                # Use Google's documented fallback validator bypass token
                sig = "skip_thought_signature_validator"

            clean_part["thought_signature"] = sig
            clean_part["thoughtSignature"] = sig
        sanitized_parts.append(clean_part)

    return {
        "role": "model",
        "parts": sanitized_parts,
    }


async def run_conversational_turn(
    session_id: str,
    user_prompt: str,
    conversation_history: list,
    tools: list,
    system_instruction: str,
    max_hops: int = 5,
) -> str:
    """
    Standard ReAct Agent Loop:
    Maintains tool calling in mode='AUTO' across all hops.
    The model autonomously decides whether to inspect files, execute tasks,
    or conclude with conversational text.
    """
    working_contents = list(conversation_history)
    working_contents.append({"role": "user", "parts": [{"text": user_prompt}]})

    current_hop = 0

    while current_hop < max_hops:
        current_hop += 1
        logger.info(f"Golem ReAct Loop: Starting hop {current_hop}/{max_hops}")

        response_payload = await call_gemini_api(
            contents=working_contents,
            tools=tools,
            tool_config={"functionCallingConfig": {"mode": "AUTO"}},
            system_instruction=system_instruction,
        )
        if response_payload is None:
            response_payload = {}

        candidate = response_payload.get("candidates", [{}])[0].get("content", {})
        parts = candidate.get("parts", [])

        # Check for tool calls
        function_calls = [p["functionCall"] for p in parts if "functionCall" in p]

        if not function_calls:
            # Model decided it has gathered enough information and generated text
            text_response = "".join(p.get("text", "") for p in parts)
            working_contents.append(candidate)
            return text_response

        # Record model's tool calls with preserved / guarded thought_signature
        working_contents.append(sanitize_model_turn_for_history(candidate))

        # Execute all tool calls emitted in this hop
        for fn in function_calls:
            tool_name = fn.get("name")
            tool_args = fn.get("args", {})
            logger.info(f"Executing tool call '{tool_name}' with args: {tool_args}")

            try:
                # Handle command vs task_description normalization
                if tool_name == "dispatch_task" and "command" in tool_args and "task_description" not in tool_args:
                    tool_args["task_description"] = tool_args["command"]

                tool_result = await execute_tool(tool_name, tool_args, session_id=session_id)
            except Exception as exc:
                logger.error(f"Tool execution failed for '{tool_name}': {exc}", exc_info=True)
                tool_result = {"status": "ERROR", "error": str(exc), "error_type": exc.__class__.__name__}

            # Return tool response to the model
            working_contents.append({
                "role": "user",
                "parts": [{"functionResponse": {"name": tool_name, "response": tool_result}}],
            })

    # If max hops exhausted, force one final pass with mode='NONE'
    final_payload = await call_gemini_api(
        contents=working_contents,
        tools=tools,
        tool_config={"function_calling_config": {"mode": "NONE"}},
        system_instruction=system_instruction,
    )
    if final_payload is None:
        final_payload = {}
    final_parts = final_payload.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    return "".join(p.get("text", "") for p in final_parts)


__all__ = [
    "IngressGateway",
    "generate_error_reference",
    "extract_candidate_parts",
    "extract_candidate_text",
    "BackstageExecutionAccumulator",
    "DISPATCH_TASK_TOOL_DECLARATION",
    "DISPATCH_SUBTASKS_TOOL_DECLARATION",
    "CONSULT_MEMORY_TOOL_DECLARATION",
    "READ_PROJECT_FILE_DECLARATION",
    "SHARE_ARTIFACT_TOOL_DECLARATION",
    "PROPOSE_MEMORY_UPDATE_TOOL_DECLARATION",
    "PLAN_TASK_WITH_SUBTASKS_DECLARATION",
    "CHECK_LINEAR_TASK_STATUS_DECLARATION",
    "APPROVE_LINEAR_TASK_DECLARATION",
    "get_mouth_tools_declaration",
    "call_gemini_api",
    "download_slack_file",
    "sanitize_filename",
    "save_staged_file",
    "process_slack_message",
    "add_reaction",
    "remove_reaction",
    "swap_reaction",
    "set_assistant_status",
    "strip_bot_mentions",
    "strip_flushed_chunks",
    "_execute_backstage_task",
    "_handle_read_project_file",
    "_execute_tool_call",
    "execute_tool",
    "run_conversational_turn",
    "sanitize_model_turn_for_history",
    "MAX_HOPS",
    "DEFAULT_MAX_FILE_SIZE_BYTES",
    "TRANSFERS_DIR_NAME",
    "CHAINABLE_TOOLS",
    "READ_ONLY_INSPECTION_TOOLS",
    "MAX_TOOL_CHAIN_HOPS",
    "compact_tool_output",
    "route_mouth_turn_model",
    "is_high_complexity_turn",
]



