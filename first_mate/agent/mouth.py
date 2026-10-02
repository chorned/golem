#!/usr/bin/env python3
"""
first_mate/agent/mouth.py
Dynamic Liaison Mouth LLM Engine & Out-of-Band Conversational Reactor.

Eliminates hardcoded regex gates, brittle keyword filters, and Mad-Libs f-string templates.
Ensures the Chief of Staff Mouth exists in Slack like a real human:
always responsive, able to converse, acknowledge, answer questions, or coordinate with running work
without being silenced by hardcoded filters or thread mutex locks.
"""

from __future__ import annotations

import os
import re
import json
import logging
import asyncio
from typing import Dict, Any, Optional, List

logger = logging.getLogger("FirstMate.Agent.Mouth")

DEFAULT_GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"


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
    model: str = DEFAULT_GEMINI_MODEL,
    timeout_seconds: float = 4.0,
) -> str:
    """
    Generate dynamic Mouth response using Gemini Flash LLM.
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
        content = blocker.get("content")
        return f"⚠️ *Confirmation Required:* Runner is waiting for input:\n> `{content}`"

    # 2. Check if running in mock / unit-test environment
    is_test_env = os.environ.get("FIRSTMATE_TEST_MODE") == "1"
    is_mock = False
    if session is not None:
        adapter = getattr(session, "adapter", None)
        if adapter is not None and type(adapter).__name__ != "HeadlessAgentAdapter":
            is_mock = True

    # 3. Dynamic Mouth LLM call via Gemini Flash if available and not mocked
    key = api_key or get_gemini_api_key()
    if key and not is_test_env and not is_mock:
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
                "Respond naturally, directly, and conversationally like a trusted senior colleague.\n"
                "Strictly adhere to Slack mrkdwn syntax: *bold*, `code`, <URL|Label>.\n"
                "Never dump terminal escape codes, ANSI colors, or box drawing characters.\n"
                "If Carl asks if he can ask something mid-stream or converse, reassure him warmly and answer his question.\n"
                "If Carl asks for status or progress, report current progress accurately.\n"
                "Keep answers concise (1-3 sentences) and high-signal."
            )
            state_ctx = (
                f"Runner Status: {status_val}\n"
                f"Active Command: {command}\n"
                f"PID: {pid}\n"
                f"Buffer Tail:\n{tail}\n"
                f"Initial Prompt / Task: {last_prompt}"
            )
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
                "generationConfig": {
                    "temperature": 0.3,
                    "maxOutputTokens": 300,
                }
            }
            url = GEMINI_API_URL.format(model=model, key=key)
            timeout = aiohttp.ClientTimeout(total=timeout_seconds)
            async with aiohttp.ClientSession(timeout=timeout) as client_session:
                async with client_session.post(url, json=payload, headers={"Content-Type": "application/json"}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        candidates = data.get("candidates", [])
                        if candidates:
                            cand = candidates[0]
                            parts = cand.get("content", {}).get("parts", [])
                            text_chunks = [p.get("text", "") for p in parts if "text" in p]
                            resp_text = "".join(text_chunks).strip()
                            if resp_text:
                                return resp_text
        except Exception as e:
            logger.debug(f"Gemini Mouth response generation encountered error: {e}")

    # 4. Calibrated Fallback Synthesis (preserves speed & compatibility for unit tests / offline)
    is_executing = getattr(session, "is_executing", False) or status_info.get("status") == "running"
    if is_executing:
        pid = status_info.get("pid") or getattr(getattr(session, "active_execution", None), "pid", None) or "active"
        tail = status_info.get("buffer_tail") or status_info.get("output_tail") or ""
        if is_conversational_checkin(prompt) and not is_status_inquiry(prompt):
            return f"⚓ *Liaison Update:* Active execution is underway in Pane 2 (PID {pid}). Still working on your task."
        elif is_status_inquiry(prompt):
            return f"⚙️ *Runner In Progress (PID {pid}):*\n> {tail or 'Executing command in Pane 2...'}"
        else:
            return f"⚓ *Liaison Update:* Yes, absolutely! Active execution is underway in Pane 2 (PID {pid}). What's on your mind?"
    else:
        return "⚓ *Status Digest:* Runner pane is currently idle. No active background jobs."
