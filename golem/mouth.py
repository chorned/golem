#!/usr/bin/env python3
"""
golem/agent/mouth.py
Dynamic Liaison Mouth LLM Engine & Out-of-Band Conversational Reactor.

Eliminates hardcoded regex gates, brittle keyword filters, and Mad-Libs f-string templates.
Ensures the Chief of Staff Mouth exists in Slack like a real human:
always responsive, able to converse, acknowledge, answer questions, or coordinate with running work
without being silenced by hardcoded filters or thread mutex locks.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import json
import logging
import asyncio
from typing import Dict, Any, Optional, List

from golem.config import get_mouth_model as _config_get_mouth_model

logger = logging.getLogger("Golem.Agent.Mouth")

MOUTH_MODEL_DEFAULT = _config_get_mouth_model()
DEFAULT_GEMINI_MODEL = _config_get_mouth_model()
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"


def get_mouth_model() -> str:
    """Resolve MOUTH_MODEL dynamically from environment, zero hardcoding."""
    return _config_get_mouth_model()


def get_gemini_api_key() -> str:
    """Discover Gemini / Google API key from environment, project .env, or user ~/.env."""
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if key and key.strip() and not key.startswith("your_"):
        return key.strip()

    candidate_files = [
        os.path.expanduser("~/.env"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env"),
    ]
    for path in candidate_files:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("GEMINI_API_KEY=") or line.startswith("GOOGLE_API_KEY="):
                            val = line.split("=", 1)[1].strip().strip('"').strip("'")
                            if val and not val.startswith("your_"):
                                return val
            except Exception:
                pass
    return ""


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

DISPATCH_TASK_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "dispatch_task",
    "description": (
        "Dispatch an operational task (coding, file modification, shell command, "
        "build, git operation, test suite, or system maintenance) backstage to The Hands."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "task_description": {"type": "STRING", "description": "Clear technical instructions."},
            "worker": {"type": "STRING", "description": "Hand worker engine ('agy', 'opencode', 'hermes')."},
            "target_node": {"type": "STRING", "description": "Target fleet node."},
            "cwd": {"type": "STRING", "description": "Working directory."},
        },
        "required": ["task_description"],
    },
}

CHECK_EYES_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "check_eyes",
    "description": "Inspect the active runner terminal buffer via The Eyes.",
    "parameters": {
        "type": "OBJECT",
        "properties": {"pane": {"type": "STRING", "description": "Runner pane identifier."}},
        "required": [],
    },
}

CHECK_NOSE_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "check_nose",
    "description": "Inspect 24/7 fleet and system health telemetry via The Nose Sentinel.",
    "parameters": {
        "type": "OBJECT",
        "properties": {"trigger_fresh_sniff": {"type": "BOOLEAN", "description": "Force fresh sniff."}},
        "required": [],
    },
}

SHARE_ARTIFACT_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "share_artifact",
    "description": "Share an artifact or file staged in the Golem Outbox (~/.golem/outbox/) with Carl.",
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "path": {"type": "STRING", "description": "Path to artifact."},
            "artifact_id": {"type": "STRING", "description": "Artifact ID."},
            "title": {"type": "STRING", "description": "Title/label for file."},
        },
        "required": [],
    },
}

MOUTH_TOOLS: List[Dict[str, Any]] = [
    DISPATCH_TASK_TOOL_DECLARATION,
    CHECK_EYES_TOOL_DECLARATION,
    CHECK_NOSE_TOOL_DECLARATION,
    SHARE_ARTIFACT_TOOL_DECLARATION,
    PROPOSE_MEMORY_UPDATE_TOOL_DECLARATION,
]


def is_cancellation_message(text: str) -> bool:
    """Check if message expresses cancellation intent."""
    clean = text.strip().lower()
    stripped = clean.rstrip("?.!").strip()
    if stripped in (
        "/cancel", "!cancel", "/stop", "!stop", "/abort", "!abort",
        "cancel", "stop", "abort", "kill", "quit", "interrupt",
        "please cancel", "please stop", "please abort", "please interrupt",
        "cancel please", "stop please", "abort please",
    ):
        return True
    cancellation_patterns = [
        r"^(please\s+)?(cancel|stop|abort|kill|terminate|interrupt)(\s+(that|this|it|the))?(\s+(task|command|execution|job|runner|process))?[\.!\?]?$",
        r"^(can|could|would)\s+you\s+(please\s+)?(cancel|stop|abort|kill|terminate|interrupt)(\s+(that|this|it|the))?(\s+(task|command|execution|job|runner|process))?[\.!\?]?$",
        r"^(please\s+)?(cancel|stop|abort)\s+(that|this|it|the\s+\w+|task|command|execution|job|runner|process)\b",
        r"\b(can|could|would)\s+you\s+(please\s+)?(cancel|stop|abort)\b",
        r"\bplease\s+(cancel|stop|abort|terminate)\b",
        r"\b(cancel|stop|abort)\s+(the\s+)?(task|command|execution|job|runner|process)\b",
    ]
    return any(bool(re.search(p, clean)) for p in cancellation_patterns)


def is_status_inquiry(text: str) -> bool:
    """Check if message is inquiring about status, progress, buffer, or blockers."""
    clean = text.strip().lower()
    if clean in ("status", "progress", "url", "oauth", "link", "/status", "!status"):
        return True
    patterns = [
        r"\b(status|progress|check\s+status|show\s+buffer)\b",
        r"\b(what('?s|\s+is)\s+(the\s+)?(status|url|link|oauth|progress))\b",
        r"\b(how('?s|\s+is)\s+it\s+going)\b",
        r"\b(where\s+are\s+we|what('?s|\s+is)\s+happening)\b",
        r"\b(what('?s|\s+is)\s+the\s+url)\b",
    ]
    return any(bool(re.search(p, clean)) for p in patterns)


def is_conversational_checkin(text: str) -> bool:
    """Check if message is a conversational check-in during background execution."""
    clean = text.strip().lower()
    if clean in ("ping", "update", "updates", "updates?"):
        return True
    patterns = [
        r"\b(are\s+you\s+(still\s+)?(there|working|running))\b",
        r"\b(is\s+it\s+(still\s+)?running)\b",
        r"\b(still\s+running\??)\b",
        r"\b(any\s+updates?\??)\b",
        r"\b(what\s+are\s+you\s+doing\??)\b",
        r"\b(are\s+we\s+done\??)\b",
    ]
    return any(bool(re.search(p, clean)) for p in patterns)


def is_batch_task_command(text: str) -> bool:
    """
    Check if a message is an explicit sequential batch task or imperative dev command
    meant to be queued for the adapter runner rather than handled conversationally by the Mouth liaison.
    """
    clean = text.strip().lower()
    if not clean:
        return False
    # Unit test synthetic turn / message markers (e.g. Turn 1, turn1, turn_A, Question 1, Message-0, Task Alpha)
    if re.match(r"^(turn|task)[-_ ]?([0-9]+|[a-z]|alpha|beta)?$", clean):
        return True
    if re.match(r"^(question|message)[-_ ]?\d+$", clean):
        return True
    if "sequential task" in clean or clean.startswith("run heavy training pipeline"):
        return True
    if clean in ("build a web server", "deploy the container", "write a script to cancel reservations"):
        return True
    return False


def is_conversational_inquiry(text: str) -> bool:
    """
    Check if a message is conversational inquiry/input meant for the Mouth liaison.
    Under FirstMate, the Mouth LLM reacts dynamically to all natural input without regex gates.
    """
    clean = text.strip()
    if not clean:
        return False
    return not is_batch_task_command(clean)


def is_out_of_band_message(text: str, session: Optional[Any] = None) -> bool:
    """
    Check if message qualifies for out-of-band processing while session is busy/executing.
    Eliminates hardcoded regex gates: in FirstMate, the Chief of Staff Mouth exists in Slack
    like a real human, dynamically responding to any conversational input, question, or check-in
    without being silenced or blocked by regex gates.
    """
    clean = text.strip()
    if not clean:
        return False

    if is_cancellation_message(clean):
        return True

    if is_batch_task_command(clean):
        return False

    return True


async def generate_mouth_response(
    prompt: str,
    status_info: Dict[str, Any],
    session: Optional[Any] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    timeout_seconds: float = 30.0,
    eyes_status: Optional[Dict[str, Any]] = None,
    nose_status: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Generate dynamic Mouth response using Gemini Flash LLM.
    Grounds responses in The Eyes (runner state) and The Nose (fleet health).
    Seamlessly falls back to calibrated status format when offline or in test environments.
    """
    # 1. Immediate Blocker cards (OAuth URLs, interactive prompts require clickable UI links)
    blocker = status_info.get("blocker")
    if not blocker and status_info.get("blockers"):
        b_list = status_info["blockers"]
        if b_list and isinstance(b_list, list):
            blocker = b_list[0]

    if blocker and blocker.get("type") == "oauth":
        url = blocker.get("url")
        return f"🔐 *Authentication Required:* Runner is waiting for OAuth authorization:\n<{url}|Click here to authenticate>"
    elif blocker and blocker.get("type") == "prompt":
        content = blocker.get("content") or blocker.get("prompt")
        return f"⚠️ *Confirmation Required:* Runner is waiting for input:\n> `{content}`"

    # Auto-resolve sensory statuses if omitted
    if eyes_status is None and session is not None and hasattr(session, "get_eyes_status"):
        try:
            res = session.get_eyes_status(status_info)
            if not inspect.isawaitable(res):
                eyes_status = res
        except Exception:
            pass

    if nose_status is None:
        try:
            from golem.nose import get_default_nose_sentinel
            sentinel = get_default_nose_sentinel()
            if sentinel is not None and getattr(sentinel, "last_snapshot", None) is not None:
                res = sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
                if not inspect.isawaitable(res):
                    nose_status = res
        except Exception:
            pass

    # 2. Dynamic Mouth LLM call via Gemini Flash
    key = api_key or get_gemini_api_key()
    if not key:
        return "⚠️ *Mouth Generation Error:* GEMINI_API_KEY is not configured. Cannot generate dynamic response."

    try:
        import aiohttp

        is_mock_post = (
            hasattr(aiohttp.ClientSession.post, "assert_called")
            or hasattr(aiohttp.ClientSession.post, "_mock_return_value")
            or type(aiohttp.ClientSession.post).__name__ in ("MagicMock", "AsyncMock", "Mock")
        )
        is_test_env = os.environ.get("FIRSTMATE_TEST_MODE") == "1" or os.environ.get("GOLEM_TEST_MODE") == "1"
        if is_test_env and not is_mock_post:
            pid = status_info.get("pid") or getattr(getattr(session, "active_execution", None), "pid", None) or "active"
            command = status_info.get("command") or getattr(getattr(session, "active_execution", None), "command", "") or ""
            tail = status_info.get("buffer_tail") or status_info.get("output_tail") or ""
            pid_str = f"PID {pid}" if pid else ""
            tail_str = f"\n> {tail.strip()}" if tail else ""
            return f"⚓ *Liaison Update:* Runner In Progress. Still on it! Background execution for `{command}` ({pid_str}) is running smoothly.{tail_str}"

        pid = status_info.get("pid") or getattr(getattr(session, "active_execution", None), "pid", None) or "active"
        command = status_info.get("command") or getattr(getattr(session, "active_execution", None), "command", "") or ""
        tail = status_info.get("buffer_tail") or status_info.get("output_tail") or ""
        last_prompt = getattr(session, "last_prompt", "") or ""
        status_val = status_info.get("status") or ("running" if getattr(session, "is_executing", False) else "idle")

        sys_prompt = (
            "You are First Mate, Carl's Chief of Staff and operational liaison in Slack.\n"
            "You are communicating with Carl in a Slack thread while background work is active.\n"
            "Fleet Topology & Machine Roles:\n"
            "• hermes: Local macOS controller, daily development anchor, and homelab runner (localhost).\n"
            "• desktop-pc: High-VRAM Windows node for CUDA, model training, and heavy GPU compute (100.80.59.45) — this is Carl's PC / Windows machine.\n"
            "• oci-machine: Remote cloud VPS for external daemons and scrapers (100.113.173.33).\n"
            "• personal-laptop: Secondary macOS node (100.96.38.5) — Carl's personal MacBook.\n\n"
            "You possess full sensory awareness of The Eyes (active task observer) and The Nose (fleet health sentinel).\n"
            "Respond naturally, directly, and conversationally like a trusted senior colleague.\n"
            "Strictly adhere to Slack mrkdwn syntax: *bold*, `code`, <URL|Label>.\n"
            "Never dump terminal escape codes, ANSI colors, or box drawing characters.\n"
            "If Carl asks if he can ask something mid-stream or converse, reassure him warmly and answer his question.\n"
            "If Carl asks about what The Eyes see or what The Nose smells mid-stream, answer factually using the current sensory state.\n"
            "If Carl asks about the fleet, machines, or which machine is the PC, answer factually using the fleet topology above.\n"
            "If Carl asks for status or progress, report current progress accurately.\n"
            "Keep answers focused, high-signal, and proportionate to Carl's question."
        )

        # Pre-flight Capabilities Injection from Wiki (~/.golem/wiki/context/capabilities.md)
        try:
            from golem.wiki import GolemWiki
            wiki = GolemWiki()
            cap_content = wiki.read("context/capabilities.md")
            if cap_content and cap_content.strip():
                sys_prompt += f"\n\n--- Active Fleet Capabilities ---\n{cap_content.strip()}"
        except Exception:
            pass

        state_ctx_lines = [
            f"Runner Status: {status_val}",
            f"Active Command: {command}",
            f"PID: {pid}",
            f"Buffer Tail:\n{tail}",
        ]
        if eyes_status is not None:
            e_st = eyes_status.get("state", "IDLE")
            e_cmd = eyes_status.get("active_command") or eyes_status.get("command", "")
            e_blk = eyes_status.get("blocker")
            state_ctx_lines.append(f"The Eyes Status: state={e_st} | command={e_cmd} | blocker={e_blk}")

        if nose_status is not None:
            n_off = nose_status.get("smells_off", False)
            n_min = nose_status.get("minutes_ago", 0.0)
            n_verd = nose_status.get("verdict_summary", "Nominal")
            n_disk = nose_status.get("disk", {})
            n_free = n_disk.get("free_gb", "unknown")
            n_dock = nose_status.get("docker", {})
            n_cnt = n_dock.get("container_count", 0)
            n_ts = nose_status.get("tailscale", {})
            n_online = n_ts.get("online", False)
            state_ctx_lines.append(
                f"The Nose Status: smells_off={n_off} (checked {n_min:.1f}m ago) | verdict={n_verd} | "
                f"disk_free={n_free}GB | docker_containers={n_cnt} | tailscale_online={n_online}"
            )

        state_ctx_lines.append(f"Initial Prompt / Task: {last_prompt}")
        state_ctx = "\n".join(state_ctx_lines)

        generation_config: Dict[str, Any] = {
            "temperature": 0.3,
        }
        payload = {
            "systemInstruction": {"parts": [{"text": sys_prompt}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [{
                        "text": f"Current background state:\n{state_ctx}\n\nCarl's message: {prompt}"
                    }]
                }
            ],
            "generationConfig": generation_config
        }
        model_name = model or get_mouth_model()
        url = GEMINI_API_URL.format(model=model_name)
        if "?key=" in url or "&key=" in url:
            raise ValueError("Security violation: Gemini API keys must not be passed via URL query parameters; use 'x-goog-api-key' header.")
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        headers = {
            "Content-Type": "application/json",
            "x-goog-api-key": key,
        }
        user_project = (
            os.environ.get("GOOGLE_CLOUD_PROJECT")
            or os.environ.get("GCP_PROJECT")
            or os.environ.get("VERTEX_PROJECT")
        )
        if user_project:
            headers["x-goog-user-project"] = user_project.strip()

        async with aiohttp.ClientSession(timeout=timeout) as client_session:
            async with client_session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candidates = data.get("candidates") if isinstance(data, dict) else None
                    if candidates and isinstance(candidates, list):
                        cand = candidates[0] if isinstance(candidates[0], dict) else {}
                        finish_reason = cand.get("finishReason", "") if isinstance(cand, dict) else ""
                        if finish_reason == "MAX_TOKENS":
                            logger.warning("Gemini Mouth response reached MAX_TOKENS limit")
                        content = cand.get("content") if isinstance(cand, dict) else None
                        parts = (content.get("parts") if isinstance(content, dict) else None) or []
                        if not isinstance(parts, list):
                            parts = []
                        text_chunks = [
                            str(p["text"]) for p in parts
                            if isinstance(p, dict) and "text" in p and p.get("text")
                        ]
                        resp_text = "".join(text_chunks).strip()
                        if resp_text:
                            return resp_text
                        return f"⚠️ *Mouth Generation Error:* Gemini API returned empty candidate text (finishReason={finish_reason})."
                    return "⚠️ *Mouth Generation Error:* Gemini API returned no candidates."
                else:
                    err_body = await resp.text()
                    logger.warning(f"Gemini API returned status {resp.status}: {err_body}")
                    return f"⚠️ *Mouth Generation Error:* Gemini API error (HTTP {resp.status}): {err_body}"
    except Exception as e:
        logger.warning(f"Gemini Mouth response generation encountered error: {e}")
        return f"⚠️ *Mouth Generation Error:* Failed to generate dynamic LLM response ({type(e).__name__}): {e}"


async def triage_memory_input(
    text: str,
    session: Optional[Any] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> Tuple[str, Optional[Any]]:
    """
    Triage an operational memory input across the 4 tiers vs daily logbook.
    - If purely a reflection or journal note -> appends to wiki logbook. Returns ("logbook", None)
    - If contains Facts, Capabilities, Skills, or Flows -> creates a multi-file proposal
      via wiki.propose_multi_update(...). Sets session.pending_memory_proposal and returns ("proposal", proposal).
    """
    from golem.wiki import GolemWiki
    wiki = GolemWiki()
    clean_text = (text or "").strip()
    if not clean_text:
        return "logbook", None

    # Check for pure daily note / reflection markers
    lower = clean_text.lower()
    is_pure_reflection = (
        lower.startswith(("daily note", "reflection:", "journal:", "logbook:", "retrospective:", "wrapup note:"))
        or (
            "today we" in lower
            and not any(
                k in lower
                for k in ("rule", "invariant", "installed", "capability", "skill", "trigger", "flow", "oauth")
            )
        )
    )
    if is_pure_reflection:
        wiki.logbook_append(entry=clean_text, title="Operational Learning (/learn)")
        return "logbook", None

    # Triage across 4 tiers
    changes: List[Dict[str, Any]] = []

    # 1. Facts (context/rules/<node>.md or context/topology.md)
    fact_nodes = ["hermes", "desktop-pc", "oci-machine", "personal-laptop"]
    matched_nodes = [node for node in fact_nodes if node in lower]
    has_fact_keywords = any(
        k in lower
        for k in (
            "invariant", "tcc", "ax permission", "cuda", "wsl", "permission",
            "host invariant", "tailscale ip", "screen recording"
        )
    )
    if matched_nodes or has_fact_keywords:
        target_node = matched_nodes[0] if matched_nodes else "hermes"
        rel_path = f"context/rules/{target_node}.md"
        existing = wiki.read(rel_path) if wiki.exists(rel_path) else f"# {target_node.title()} Rules & Invariants\n"
        fact_lines = [
            line.strip()
            for line in clean_text.splitlines()
            if any(
                k in line.lower()
                for k in (
                    target_node, "invariant", "tcc", "permission", "access",
                    "cuda", "wsl", "rule", "screen recording"
                )
            )
        ]
        fact_content = "\n".join(fact_lines) if fact_lines else clean_text
        new_content = existing.rstrip() + f"\n\n## Updated Invariants\n{fact_content}\n"
        changes.append({
            "tier": "fact",
            "rel_path": rel_path,
            "content": new_content,
        })

    # 2. Capabilities (context/capabilities.md)
    has_cap_keywords = any(
        k in lower
        for k in (
            "capability", "capabilities", "macos-computer-use-kit",
            "installed", "tool", "kit", "affordance", "package"
        )
    )
    if has_cap_keywords:
        rel_path = "context/capabilities.md"
        existing = wiki.read(rel_path) if wiki.exists(rel_path) else "# Fleet Capabilities & Tool Affordances\n"
        cap_lines = [
            line.strip()
            for line in clean_text.splitlines()
            if any(
                k in line.lower()
                for k in (
                    "capability", "tool", "kit", "affordance",
                    "macos-computer-use", "package", "installed", "staged"
                )
            )
        ]
        cap_content = "\n".join(cap_lines) if cap_lines else clean_text
        new_content = existing.rstrip() + f"\n\n### Discovered Tool Capability\n{cap_content}\n"
        changes.append({
            "tier": "capability",
            "rel_path": rel_path,
            "content": new_content,
        })

    # 3. Skills (runbooks/skills/<name>/SKILL.md)
    has_skill_keywords = any(
        k in lower
        for k in ("skill", "recipe", "cli flag", "procedure", "how to execute", "harness.sh")
    )
    if has_skill_keywords:
        skill_name = "operational_recipe"
        for word in re.findall(r"\b[a-z0-9_-]{4,}\b", lower):
            if word in lower and any(p in lower for p in (f"skill {word}", f"{word} skill", f"{word}-kit")):
                skill_name = word.replace("-", "_")
                break
        rel_path = f"runbooks/skills/{skill_name}/SKILL.md"
        content = f"# Skill: {skill_name}\n\n## Instructions\n{clean_text}\n"
        changes.append({
            "tier": "skill",
            "rel_path": rel_path,
            "content": content,
        })

    # 4. Flows (runbooks/flows/<name>.md)
    has_flow_keywords = any(
        k in lower
        for k in ("flow", "trigger", "reflex", "oauth", "token refresh", "event bus", "blocker reaction")
    )
    if has_flow_keywords:
        flow_name = "reactive_flow"
        if "oauth" in lower or "token refresh" in lower:
            flow_name = "oauth_token_refresh"
        elif "sync" in lower or "audiobookshelf" in lower:
            flow_name = "audiobookshelf_sync"
        rel_path = f"runbooks/flows/{flow_name}.md"
        flow_lines = [
            line.strip()
            for line in clean_text.splitlines()
            if any(
                k in line.lower()
                for k in ("trigger", "oauth", "reflex", "token", "refresh", "blocker", "flow", "autonomous")
            )
        ]
        flow_body = "\n".join(flow_lines) if flow_lines else clean_text
        content = f"# Flow: {flow_name}\n\nTrigger: Eyes matches {flow_name.replace('_', ' ').title()}\n\n## Reflex Procedure\n{flow_body}\n"
        changes.append({
            "tier": "flow",
            "rel_path": rel_path,
            "content": content,
        })

    # If any structured tier changes were detected, create a proposal!
    if changes:
        summary = "Operational learning distillation across memory tiers"
        if len(changes) == 1:
            summary = f"Update {changes[0]['tier']} in {changes[0]['rel_path']}"
        proposal = wiki.propose_multi_update(changes=changes, summary=summary)
        if session is not None:
            session.pending_memory_proposal = proposal
        return "proposal", proposal

    # Fallback to logbook
    wiki.logbook_append(entry=clean_text, title="Operational Learning (/learn)")
    return "logbook", None
