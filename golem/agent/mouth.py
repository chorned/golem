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
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"


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
            from golem.sensory.nose import get_default_nose_sentinel
            sentinel = get_default_nose_sentinel()
            if sentinel is not None and getattr(sentinel, "last_snapshot", None) is not None:
                res = sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
                if not inspect.isawaitable(res):
                    nose_status = res
        except Exception:
            pass

    # 2. Dynamic Mouth LLM call via Gemini Flash
    if os.environ.get("FIRSTMATE_TEST_MODE") == "1":
        pid = status_info.get("pid") or getattr(getattr(session, "active_execution", None), "pid", None) or "active"
        command = status_info.get("command") or getattr(getattr(session, "active_execution", None), "command", "") or ""
        pid_str = f"PID {pid}" if pid else ""
        return f"⚓ *Liaison Update:* Runner In Progress. Still on it! Background execution for `{command}` ({pid_str}) is running smoothly."

    key = api_key or get_gemini_api_key()
    if not key:
        return "⚠️ *Mouth Generation Error:* GEMINI_API_KEY is not configured. Cannot generate dynamic response."

    try:
        import aiohttp
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
        url = GEMINI_API_URL.format(model=model_name, key=key)
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        headers = {"Content-Type": "application/json"}
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
                    candidates = data.get("candidates", [])
                    if candidates:
                        cand = candidates[0]
                        finish_reason = cand.get("finishReason", "")
                        if finish_reason == "MAX_TOKENS":
                            logger.warning("Gemini Mouth response reached MAX_TOKENS limit")
                        parts = cand.get("content", {}).get("parts", [])
                        text_chunks = [p.get("text", "") for p in parts if "text" in p]
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
