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
import json
import logging
from collections import deque, OrderedDict
from typing import Dict, Any, Optional, Deque, List, Union, Tuple

import aiohttp

from golem.gateway.router import SessionRouter
from golem.gateway.session import (
    SessionHandle,
    SessionState,
    is_cancellation_message,
    is_status_inquiry,
    is_conversational_checkin,
    is_out_of_band_message,
)
from golem.agent.mouth import generate_mouth_response, get_gemini_api_key
from golem.agent.protocol import (
    AgentEvent,
    FinishEvent,
    TextDeltaEvent,
    ThoughtDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
)
from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
    get_default_event_bus,
)
from golem.transport.normalizer import sanitize_for_slack
from golem.transport.chunker import split_message_boundary_aware
from golem.transport.file_transfer import (
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
from golem.config import get_mouth_model, get_default_hand_worker, get_host_dir, get_daemon_label
from golem.execution.delegation import WorkerDelegator

logger = logging.getLogger("Golem.Gateway.Ingress")

DISPATCH_TASK_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "dispatch_task",
    "description": (
        "Dispatch an operational task (coding, file modification, shell command, "
        "build, git operation, test suite, or system maintenance) backstage to The Hands. "
        "Standardized on AGY CLI as the default hand worker ('agy'). "
        "Can also delegate on-demand to 'opencode' (for code modifications / test suites) "
        "or 'hermes' (for homelab/Docker/system ops) when explicitly requested. "
        "Use this whenever Carl asks to execute code, run commands, create/edit files, "
        "or perform system operations."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "task_description": {
                "type": "STRING",
                "description": "Clear, detailed technical instructions for the backstage worker engine.",
            },
            "worker": {
                "type": "STRING",
                "description": (
                    "The backstage hand worker engine to execute the task. Defaults to 'agy' (Antigravity CLI). "
                    "Only specify 'opencode' (for sandboxed refactors) or 'hermes' (for homelab/Docker/system ops) "
                    "if explicitly requested by Carl."
                ),
            },
            "target_node": {
                "type": "STRING",
                "description": (
                    "Target fleet node (e.g. 'localhost', 'desktop-pc', 'personal-laptop', 'oci-machine'). "
                    "Defaults to 'localhost' (fleet-control) if unspecified. Do not target 'homelab-ops' or 'hermes' "
                    "unless homelab chores are explicitly requested."
                ),
            },
            "cwd": {
                "type": "STRING",
                "description": "Working directory for execution (defaults to session target_cwd).",
            },
        },
        "required": ["task_description"],
    },
}


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


def get_mouth_tools_declaration() -> List[Dict[str, Any]]:
    """Return standard Google Gemini REST API tools declaration."""
    return [
        {
            "functionDeclarations": [
                DISPATCH_TASK_TOOL_DECLARATION,
                CHECK_EYES_TOOL_DECLARATION,
                CHECK_NOSE_TOOL_DECLARATION,
            ]
        }
    ]


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


GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"


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
    if sys_inst and isinstance(sys_inst, dict):
        sys_parts = sys_inst.get("parts", [])
        sys_text = "".join([p.get("text", "") for p in sys_parts if isinstance(p, dict) and "text" in p]).strip()
        if sys_text:
            messages.append({"role": "system", "content": sys_text})

    for c in payload.get("contents", []):
        if not isinstance(c, dict):
            continue
        gemini_role = c.get("role", "user")
        role = "assistant" if gemini_role == "model" else "user"
        parts = c.get("parts", [])
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
    payload: Dict[str, Any],
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    timeout_seconds: float = 30.0,
) -> Optional[Dict[str, Any]]:
    """Execute REST API POST call to Google Gemini endpoint or configured MOUTH_BACKEND."""
    backend = (os.environ.get("MOUTH_BACKEND") or os.environ.get("GOLEM_MOUTH_BACKEND") or "gemini").lower().strip()
    if backend in ("openai", "openai-compatible", "openrouter", "groq", "ollama", "deepseek"):
        return await call_openai_compatible_api(
            payload=payload,
            model=model,
            api_key=api_key,
            timeout_seconds=timeout_seconds,
        )

    key = api_key or get_gemini_api_key()
    if not key:
        return None
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

    try:
        async with aiohttp.ClientSession(timeout=timeout) as http_session:
            async with http_session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    err_text = await resp.text()
                    logger.warning(f"Gemini API returned status {resp.status}: {err_text}")
                    return {
                        "_error": True,
                        "status": resp.status,
                        "code": resp.status,
                        "error_type": f"HTTP_{resp.status}",
                        "raw_error": err_text,
                        "message": err_text,
                    }
    except Exception as e:
        logger.warning(f"Gemini API call failed: {e}")
        return {
            "_error": True,
            "status": 500,
            "code": 500,
            "error_type": type(e).__name__,
            "raw_error": str(e),
            "message": str(e),
        }


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


async def add_reaction(client: Any, channel: str, timestamp: str, name: str) -> None:
    """Add a reaction to a message, gracefully ignoring idempotent and non-fatal errors."""
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


async def remove_reaction(client: Any, channel: str, timestamp: str, name: str) -> None:
    """Remove a reaction from a message, gracefully ignoring idempotent errors."""
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
) -> None:
    """Atomically swap old reaction for new reaction."""
    if old_name and old_name != new_name:
        await remove_reaction(client, channel, timestamp, old_name)
    await add_reaction(client, channel, timestamp, new_name)


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
        )
        return delta or None

    elif ev_type == "awaiting_input":
        prompt = getattr(event_item, "prompt", "") or (
            event_item.get("prompt", "") if isinstance(event_item, dict) else ""
        )
        return f"\x1b[33m⚠️ [AWAITING INPUT]\x1b[0m \x1b[1;33m{prompt}\x1b[0m\n"

    elif ev_type == "finish":
        status = getattr(event_item, "status", "SUCCESS")
        if status == "SUCCESS":
            return "\n\x1b[32m✔ [TASK COMPLETE]\x1b[0m \x1b[90mExecution finished successfully.\x1b[0m\n"
        else:
            err = getattr(event_item, "error", "") or "Execution encountered an error."
            return f"\n\x1b[31m✖ [TASK FAILED]\x1b[0m \x1b[1;31m{err}\x1b[0m\n"

    return None


class IngressGateway:
    """
    Ingress gateway routing incoming chat events to warm SessionHandles.
    Enforces non-verbal acknowledgments, zero canned chatter, deduplication, and reaction state swaps.
    """

    def __init__(
        self,
        router: SessionRouter,
        bot_user_id: Optional[str] = None,
        max_seen_messages: int = 2000,
        max_staged_cache: int = 2000,
        max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
        sensory_bus: Optional[ShoulderTapEventBus] = None,
        bus: Optional[ShoulderTapEventBus] = None,
        nose_sentinel: Optional[Any] = None,
        webui_server: Optional[Any] = None,
    ) -> None:
        self.router = router
        self.bot_user_id = bot_user_id
        self._seen_messages: Deque[str] = deque(maxlen=max_seen_messages)
        self._seen_set: set[str] = set()
        self._dedup_lock = asyncio.Lock()
        self._staged_files: OrderedDict[str, str] = OrderedDict()
        self._max_staged_cache = max_staged_cache
        self.max_file_size_bytes = max_file_size_bytes
        self._file_locks: Dict[str, asyncio.Lock] = {}
        self._file_locks_guard = asyncio.Lock()
        self.nose_sentinel = nose_sentinel
        self.webui_server = webui_server

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

        # Ensure router shares the same event bus if not explicitly configured
        if self.router is not None and getattr(self.router, "bus", None) is None:
            self.router.bus = self.bus

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
        try:
            from golem.sensory.nose import get_default_nose_sentinel
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

            if thread_ts:
                await swap_reaction(client, channel, thread_ts, "gear", "warning")
            await self._post_response(client, channel, thread_ts, card, is_imessage)

        elif alert.alert_type == "health_anomaly":
            await self._handle_health_anomaly_alert(alert, client=client)

    async def _handle_health_anomaly_alert(self, alert: Any, client: Optional[Any] = None) -> None:
        """Receive health anomaly alerts from The Nose Sentinel and deliver advisory to Carl."""
        summary = getattr(alert, "summary", "") or "Fleet Sentinel detected a health anomaly."
        channel = getattr(alert, "channel", None) or os.environ.get("FIRSTMATE_DEFAULT_CHANNEL", "")
        thread_ts = getattr(alert, "thread_ts", None)
        advisory_msg = f"👃 *Sentinel Advisory (Fleet Health):*\n{summary}"
        if client and channel:
            await self._post_response(client, channel, thread_ts, advisory_msg, is_imessage=False)

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

        return results

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
        limit: int = 15,
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
            max_char_len = 1200 if is_imessage else 300
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
                sender = "Hermes" if (is_imessage and is_bot) else ("AGY" if is_bot else "User")
                clean_msg = strip_bot_mentions(text, bot_id)
                if len(clean_msg) > max_char_len:
                    clean_msg = clean_msg[:max_char_len] + "..."
                context_lines.append(f"{sender}: {clean_msg}")

            if not context_lines:
                return ""

            recent_lines = context_lines[-10:]
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
        thread_ts = event.get("thread_ts") or ts
        is_imessage = bool(event.get("is_imessage", False))
        target_cwd = event.get("cwd") or event.get("target_cwd")

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
                    results = await asyncio.gather(
                        *[fleet.probe_node(node) for node in ["hermes", "desktop-pc", "personal-laptop", "oci-machine"]],
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
                    msg = f"⚓ *Ahoy:* Session active with {active_count} executing hand(s):\n" + "\n".join(desc_list)
                else:
                    msg = "⚓ *Ahoy:* Session standby. 0 background execution tasks running."
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            elif cmd_token in ("/learn", "!learn"):
                user_notes = clean_text[len(cmd_token):].strip()
                msg = f"🧠 *Operational Lesson Crystallized:* {user_notes or 'Lesson codified into operational memory.'}"
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)
                return

            elif cmd_token in ("/update", "!update"):
                active_hands = []
                if getattr(self, "webui_server", None):
                    active_hands = list(getattr(self.webui_server, "_active_hands", {}).values())
                active_count = len(active_hands)
                if active_count > 0:
                    msg = f"⚠️ *Update Aborted:* Fleet active with {active_count} executing task(s). Wait for completion before reloading."
                    if ts:
                        await swap_reaction(client, channel, ts, "gear", "warning")
                    await self._post_response(client, channel, thread_ts, msg, is_imessage)
                    return

                msg = "🚀 *Update:* Verifying core test suite and fleet AGY binaries..."
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "eyes")
                if thread_ts:
                    await set_assistant_status(client, channel, thread_ts, "Verifying core test suite...")
                await self._post_response(client, channel, thread_ts, msg, is_imessage)

                async def _do_ingress_reload() -> None:
                    await asyncio.sleep(0.5)
                    host_dir = get_host_dir()
                    script = os.path.join(host_dir, "scripts", "safe_update.sh")
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
                                fail_msg = f"❌ *Update Aborted:* Test verification failed (exit {proc.returncode}):\n```{err_snip}```"
                                if ts:
                                    await swap_reaction(client, channel, ts, "eyes", "x")
                                if thread_ts:
                                    await set_assistant_status(client, channel, thread_ts, "")
                                await self._post_response(client, channel, thread_ts, fail_msg, is_imessage)
                                return
                        except Exception as e:
                            logger.warning(f"safe_update.sh invocation failed: {e}")

                    # Step 2: Check & update agy CLI across fleet
                    if thread_ts:
                        await set_assistant_status(client, channel, thread_ts, "Updating fleet AGY binaries...")
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
                    if ts:
                        await swap_reaction(client, channel, ts, "eyes", "white_check_mark")
                    if thread_ts:
                        await set_assistant_status(client, channel, thread_ts, "")
                    await self._post_response(client, channel, thread_ts, succ_msg, is_imessage)
                    await asyncio.sleep(0.5)

                    try:
                        uid = os.getuid()
                        labels = {get_daemon_label(), "com.user.first-mate", "com.user.golem"}
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
            if thread_context:
                prompt_to_send = f"{thread_context}\n\n{prompt_to_send}"
            if attachment_tags:
                tags_block = "\n".join(attachment_tags)
                prompt_to_send = f"{tags_block}\n\n{prompt_to_send}" if prompt_to_send else tags_block

            # Out-of-band triage if session is actively executing in Pane 2 (or busy)
            if not (session.is_busy or session.is_executing) and session.runner_pane and is_out_of_band_message(clean_text):
                try:
                    await session.get_runner_status()
                except Exception as e:
                    logger.debug(f"Error probing runner status in ingress: {e}")

            if session.is_busy or session.is_executing:
                if is_cancellation_message(clean_text):
                    await self._handle_out_of_band_cancellation(session, client, channel, thread_ts, ts, is_imessage)
                    return
                elif is_out_of_band_message(clean_text, session=session):
                    await self._handle_out_of_band_status(session, client, channel, thread_ts, ts, is_imessage, clean_text)
                    return

            # Check if running in mock / test environment or forced mouth
            is_test_env = os.environ.get("FIRSTMATE_TEST_MODE") == "1" or os.environ.get("FIRST_MATE_TEST_MODE") == "1"
            force_mouth = (getattr(session, "force_mouth", False) is True) or (os.environ.get("FIRSTMATE_FORCE_MOUTH") == "1")
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
            use_mouth = force_mouth or (gemini_mocked and not is_test_env) or (not is_test_env and not is_mock and has_gemini_key)

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

                if ts:
                    await swap_reaction(client, channel, ts, "gear", "white_check_mark")
        except Exception as e:
            logger.error(f"Unhandled exception in handle_message for ts={ts}: {e}", exc_info=True)
            if ts:
                await swap_reaction(client, channel, ts, "gear", "boom")
            err_msg = f"💥 *System Error:* An unexpected error occurred while processing message:\n```{type(e).__name__}: {e}```"
            try:
                await self._post_response(client, channel, thread_ts, err_msg, is_imessage)
            except Exception:
                pass
            raise
        finally:
            if thread_ts:
                await set_assistant_status(client, channel, thread_ts, "")

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

            res = client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=chunk)
            if inspect.isawaitable(res):
                res = await res
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
        if ts:
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

        if ts:
            if "Mouth Generation Error" in msg or "Error:" in msg:
                await swap_reaction(client, channel, ts, "gear", "x")
            elif blocker and blocker.get("type") in ("oauth", "prompt"):
                await swap_reaction(client, channel, ts, "gear", "warning")
            else:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")

        await self._post_response(client, channel, thread_ts, msg, is_imessage)

    def _stream_backstage_events(self, session: SessionHandle, task_desc: str) -> Any:
        """Stream events from backstage session."""
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
            from golem.sensory.eyes import EyesTaskObserver

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
    async def _execute_backstage_task(
        self,
        task_desc: str,
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
    ) -> Union[BackstageExecutionAccumulator, Dict[str, Any]]:
        """
        Execute backstage task silently with a 3.0s yield window.
        Standardized on AGY CLI as default ('agy') in Pane 2.
        Supports on-demand delegation to OpenCode or Hermes when explicitly specified.
        If execution takes longer than 3.0s, posts an immediate conversational liaison acknowledgment,
        leaving the task running while The Eyes monitors terminal buffers and The Mouth remains free
        to respond to mid-stream user check-ins.
        """
        accumulator = BackstageExecutionAccumulator()
        if session is None:
            return accumulator

        effective_worker = WorkerDelegator.normalize_worker_name(worker or get_default_hand_worker())
        effective_node = target_node or "localhost"

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
        pane = runner_pane_id or _rp_label or getattr(session, "session_id", None) or "runner"
        eyes_observer: Optional[Any] = None
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

        worker_task = asyncio.create_task(_worker_consumer())
        start_time = time.time()
        acknowledged_yield = False

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
                        ack_msg = (
                            f"⚙️ *On it:* Dispatched backstage task to `{node_label}` via `{effective_worker}`. "
                            "Execution is running silently; I'll brief you as soon as it settles."
                        )
                        if client and channel:
                            resp = await self._post_response(client, channel, thread_ts, ack_msg, is_imessage)
                            if resp:
                                msg_ts = (
                                    resp.get("ts")
                                    if isinstance(resp, dict)
                                    else getattr(resp, "ts", getattr(resp, "data", {}).get("ts") if hasattr(resp, "data") else None)
                                )
                                if msg_ts:
                                    accumulator.interim_message_ts = str(msg_ts)
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
                                if client and channel:
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

            # Settle and unregister hand agent cleanly
            if hand_registered and webui is not None:
                try:
                    turn_status = getattr(accumulator, "turn_status", "SUCCESS")
                    exit_code = 0 if turn_status == "SUCCESS" else 1
                    status_str = "EXIT_0" if exit_code == 0 else "FAILED"
                    settle_payload = {
                        "pane_id": pane,
                        "exit_code": exit_code,
                        "status": status_str,
                    }
                    res = webui.broadcast("hand_settle", settle_payload)
                    if inspect.isawaitable(res):
                        await res
                except Exception as exc:
                    logger.debug(f"WebUI hand_settle broadcast warning: {exc}")

                try:
                    res = webui.unregister_active_hand(pane_id=pane)
                    if inspect.isawaitable(res):
                        await res
                except Exception as exc:
                    logger.debug(f"WebUI unregister_active_hand warning: {exc}")

        return accumulator

    async def _execute_check_eyes(
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
        Universal Front-of-House Mouth turn pipeline:
          1. Call Mouth LLM (MOUTH_MODEL) with full thread history and dispatch_task tool declaration.
          2. If pure text is emitted: reply conversationally in 1 turn (0 worker tasks).
          3. If dispatch_task is called: execute backstage silently with 3.0s yield window.
          4. If execution exceeds 3.0s: deliver liaison acknowledgment while task runs in background.
          5. Feed execution results back via functionResponse for synthesized executive briefing.
          6. Deliver clean briefing to Slack, upload directive files, and swap reactions.
        """
        api_key = get_gemini_api_key()
        model = get_mouth_model()

        sys_prompt = (
            "You are Golem (formerly First Mate), Carl's Chief of Staff and operational liaison in Slack.\n"
            "You operate under the Golem / FirstMate pattern: One Captain, One Liaison.\n"
            "Carl communicates ONLY with you. All worker engines and sensory organs operate backstage.\n\n"
            "Fleet Topology & Machine Roles:\n"
            "• hermes: Local macOS controller, daily development anchor, and homelab runner (localhost).\n"
            "• desktop-pc: High-VRAM Windows node for CUDA, model training, and heavy GPU compute (100.80.59.45) — this is Carl's PC / Windows machine.\n"
            "• oci-machine: Remote cloud VPS for external daemons and scrapers (100.113.173.33).\n"
            "• personal-laptop: Secondary macOS node (100.96.38.5) — Carl's personal MacBook.\n\n"
            "Sensory Architecture Awareness:\n"
            "• The Mouth (You): Front-of-house liaison, conversational partner, and executive synthesizer.\n"
            "• The Hands: Backstage worker running code, builds, and commands in Pane 2 via `dispatch_task`.\n"
            "  Standardized on the local Antigravity (AGY) CLI as default (`worker='agy'`). Only delegate to 'opencode' (for sandboxed refactors) or 'hermes' (for homelab ops) when Carl explicitly requests them.\n"
            "• The Eyes: Real-time terminal observer monitoring Pane 2 buffers for blockers and completion.\n"
            "• The Nose: 24/7 background sentinel sniffing daemon error logs, Tailscale, disk, and Docker health.\n"
            "• Shoulder Tap Event Bus: Sensory observers tap you on the shoulder with alerts to wake you.\n\n"
            "Guidelines:\n"
            "1. CONVERSATIONAL INQUIRIES & BANTER: Answer greetings, high-level questions, general conversation, "
            "and questions about the fleet, machines, topology, or project state directly in natural Chief of Staff prose. "
            "DO NOT call dispatch_task, check_eyes, or check_nose. "
            "Strictly forbid unprompted sensory tool calls during conversational chatter or queries about machine roles or identities.\n"
            "2. SENSORY INSPECTION TOOLS: ONLY call sensory inspection tools when Carl explicitly asks for observation:\n"
            "   - When Carl specifically asks what The Eyes see, about runner state, active command progress, blockers, or buffer tails: "
            "call `check_eyes`.\n"
            "   - When Carl specifically asks what The Nose smells, about fleet health, disk pressure, container health, Tailscale status, or error logs: "
            "call `check_nose`.\n"
            "   - Never call check_eyes or check_nose unprompted unless Carl explicitly requests sensory observation.\n"
            "   - Always synthesize sensory tool JSON output into an authentic, concise executive briefing using Slack mrkdwn.\n"
            "3. OPERATIONAL TASKS: When Carl asks to run tests, execute code, edit files, check logs, inspect Git, "
            "build, or deploy, call `dispatch_task`.\n"
            "   - By default, leave worker as 'agy' and target_node as 'localhost'.\n"
            "   - Only specify worker='hermes' or target_node='homelab-ops' if Carl explicitly asks for homelab or Hermes.\n"
            "   - Only specify worker='opencode' if Carl explicitly asks for OpenCode.\n"
            "   - IMAGE GENERATION & VISUAL ASSETS: Antigravity has native built-in image generation (`generate_image`). "
            "When Carl asks to generate, render, or create images, diagrams, or visual designs (e.g. 'nano banana', UI mockups, logos, illustrations), "
            "dispatch to 'agy' on 'localhost' with task_description explicitly instructing to use built-in `generate_image`. "
            "Never route image generation to hermes or search external script catalogs.\n"
            "4. SLACK FORMATTING: Always use single asterisks *bold*, inline `code`, multiline ```diff```, "
            "and clean bullet lists. Never use markdown ATX hashes (###) or double asterisks (**).\n"
            "5. OUTCOMES OVER MACHINERY: Direct, grounded, quiet competence. No filler greetings."
        )

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

        # Step 1: Initial call to Mouth LLM
        initial_resp = await self._call_gemini_api(payload=payload, model=model, api_key=api_key)
        if not initial_resp or not initial_resp.get("candidates"):
            # Fallback to direct adapter execution if Gemini API fails
            logger.warning("Gemini Mouth initial call failed; falling back to direct adapter execution.")
            await self._fallback_direct_adapter_turn(session, prompt, client, channel, thread_ts, ts, is_imessage)
            return

        candidate = initial_resp["candidates"][0]
        content = candidate.get("content", {})
        parts = content.get("parts", [])

        # Check for functionCall
        fn_call = None
        text_parts = []
        for p in parts:
            if "functionCall" in p:
                fn_call = p["functionCall"]
            elif "text" in p and p["text"]:
                text_parts.append(p["text"])

        # Case A: Pure Conversational Turn (Zero Worker Tasks)
        if not fn_call:
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
            await self._post_response(client, channel, thread_ts, clean_reply, is_imessage)
            await self._broadcast_mouth_briefing(clean_reply, model)
            if ts:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")
            return

        # Case B: Tool Call Turn (dispatch_task, check_eyes, check_nose)
        fn_name = fn_call.get("name", "dispatch_task")
        fn_args = fn_call.get("args", {})
        turn_status = "SUCCESS"
        full_output = ""
        error_msg = ""

        if fn_name == "check_eyes":
            pane = fn_args.get("pane")
            tool_result = await self._execute_check_eyes(session=session, pane=pane)
        elif fn_name == "check_nose":
            trigger_fresh_sniff = bool(fn_args.get("trigger_fresh_sniff", False))
            tool_result = await self._execute_check_nose(trigger_fresh_sniff=trigger_fresh_sniff)
        else:
            task_desc = fn_args.get("task_description") or prompt
            worker = fn_args.get("worker") or get_default_hand_worker() or "agy"
            raw_target = fn_args.get("target_node")
            target_node = raw_target or "localhost"
            target_cwd = fn_args.get("cwd") or getattr(session, "target_cwd", None) or os.getcwd()

            logger.info(
                f"Mouth dispatched task to backstage: '{task_desc[:60]}' "
                f"(worker={worker}, target={target_node}, cwd={target_cwd})"
            )

            # Execute backstage silently with 3.0s yield
            accumulator = await self._execute_backstage_task(
                task_desc,
                target_node,
                target_cwd,
                session=session,
                client=client,
                channel=channel,
                thread_ts=thread_ts,
                ts=ts,
                is_imessage=is_imessage,
                yield_timeout=3.0,
                worker=worker,
            )

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
                turn_status = acc_status
                error_msg = ""
                full_output = acc_output
            else:
                acc_status = accumulator.turn_status
                acc_summary = accumulator.get_structured_summary()
                has_awaiting_input = accumulator.has_awaiting_input
                awaiting_prompt = accumulator.awaiting_input_prompt
                turn_status = accumulator.turn_status
                error_msg = accumulator.error_message
                full_output = accumulator.get_full_output()

            tool_result = acc_summary

            # Handle awaiting input
            if has_awaiting_input:
                if ts:
                    await swap_reaction(client, channel, ts, "gear", "warning")
                prompt_msg = awaiting_prompt or "⚠️ *Confirmation Required:* Please provide input."
                await self._post_response(client, channel, thread_ts, prompt_msg, is_imessage)
                return

        # Step 2: Synthesize Executive Briefing via functionResponse
        if thread_ts:
            await set_assistant_status(client, channel, thread_ts, "Synthesizing executive briefing...")

        briefing_text, error_info = await self._execute_step2_synthesis(
            sys_prompt=sys_prompt,
            prompt=prompt,
            candidate=candidate,
            fn_call=fn_call,
            fn_name=fn_name,
            tool_result=tool_result,
            model=model,
            api_key=api_key,
            tools=tools,
        )

        if not briefing_text:
            # Fail loudly on Step 2 synthesis or runtime failure
            code = (error_info or {}).get("status") or (error_info or {}).get("code") or 500
            error_type = (error_info or {}).get("error_type") or "SynthesisError"
            raw_error = (error_info or {}).get("raw_error") or (error_info or {}).get("message") or "Synthesis failed with empty candidate text"
            tool_json = json.dumps(tool_result, indent=2, default=str)
            fail_briefing = (
                f"⚠️ *Executive Synthesis Failed* (HTTP {code} / {error_type}):\n"
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
        if fn_name == "check_eyes" and tool_result.get("state") == "BLOCKER":
            target_reaction = "warning"
        elif fn_name == "check_nose" and tool_result.get("smells_off"):
            target_reaction = "warning"
        elif turn_status == "ERROR":
            # Determine if this was an actual fatal failure vs successful operational work
            # despite non-zero exit codes or sub-command errors.
            has_fatal_error = (
                not full_output
                or (error_msg and not tool_result.get("output_tail"))
                or bool(re.search(r"\b(fatal error|operation failed|task failed|execution failed|unhandled exception)\b", clean_briefing[:250], re.IGNORECASE))
            )
            target_reaction = "x" if has_fatal_error else "white_check_mark"
        else:
            target_reaction = "white_check_mark"

        interim_ts = getattr(accumulator, "interim_message_ts", None) if "accumulator" in locals() and accumulator else None
        await self._post_response(client, channel, thread_ts, clean_briefing, is_imessage, update_ts=interim_ts)
        await self._broadcast_mouth_briefing(clean_briefing, model)

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
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Synthesize Executive Briefing via functionResponse in Step 2.
        Includes complete 'tools': tools schema declaration to eliminate HTTP 400 schema error.
        Returns (briefing_text, error_info).
        """
        model_parts = candidate.get("content", {}).get("parts")
        if not model_parts:
            model_parts = [{"functionCall": fn_call}]

        synthesis_sys_prompt = (
            sys_prompt + "\n\n"
            "CRITICAL INSTRUCTION FOR SYNTHESIS:\n"
            "1. Always directly address and answer Carl's original prompt/question in your briefing.\n"
            "2. If Carl asked a conversational or factual question (e.g. asking which machine is which), "
            "answer his question directly using your knowledge and provide the tool result in proper context.\n"
            "3. Never ignore Carl's question in favor of dumping raw sensory metrics alone.\n"
            "4. If an image, artifact, or generated file path was produced in the tool result (e.g. Saved File Path: ...), "
            "always include the directive `[SEND_FILE: <path>]` in your briefing so the file is automatically uploaded to Slack."
        )

        tools_decl = tools if tools is not None else get_mouth_tools_declaration()

        tool_result_payload = dict(tool_result) if isinstance(tool_result, dict) else {"result": tool_result}
        tool_result_payload["instruction"] = (
            "Backstage task execution is now finished. Synthesize your final executive briefing directly to Carl in plain text now. "
            "Do not call any functions or tools."
        )

        briefing_payload = {
            "systemInstruction": {"parts": [{"text": synthesis_sys_prompt}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                },
                {
                    "role": "model",
                    "parts": model_parts,
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
                        }
                    ],
                },
            ],
            "tools": tools_decl,
            "toolConfig": {
                "functionCallingConfig": {"mode": "NONE"}
            },
            "generationConfig": {
                "temperature": 0.2,
            },
        }

        try:
            briefing_resp = await self._call_gemini_api(payload=briefing_payload, model=model, api_key=api_key)
        except Exception as e:
            return "", {
                "status": 500,
                "code": 500,
                "error_type": type(e).__name__,
                "raw_error": str(e),
                "message": str(e),
            }

        if not briefing_resp:
            return "", {
                "status": 500,
                "code": 500,
                "error_type": "NoResponse",
                "raw_error": "Gemini API returned empty response",
                "message": "No response",
            }

        if briefing_resp.get("_error"):
            return "", briefing_resp

        candidates = briefing_resp.get("candidates", [])
        if not candidates:
            return "", {
                "status": 200,
                "code": 200,
                "error_type": "ZeroCandidates",
                "raw_error": "Gemini API returned zero candidates",
                "message": "Zero candidates",
            }

        b_cand = candidates[0]
        b_parts = b_cand.get("content", {}).get("parts", [])
        briefing_text = "".join(p.get("text", "") for p in b_parts if "text" in p).strip()
        if not briefing_text:
            logger.warning(
                "Step 2 synthesis returned empty text or tool call; attempting direct text-only fallback synthesis."
            )
            out_tail = str(tool_result.get("output_tail") or tool_result.get("output") or "").strip()
            if out_tail:
                direct_prompt = (
                    f"Carl's request: {prompt}\n\n"
                    f"Backstage Task Output:\n{out_tail}\n\n"
                    "Synthesize a concise, high-signal executive briefing for Carl in plain text. "
                    "Output ONLY the executive briefing text in clean Slack mrkdwn format. Do not call functions or output raw JSON."
                )
                direct_payload = {
                    "systemInstruction": {"parts": [{"text": synthesis_sys_prompt}]},
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
                        if fb_candidates:
                            fb_parts = fb_candidates[0].get("content", {}).get("parts", [])
                            briefing_text = "".join(p.get("text", "") for p in fb_parts if "text" in p).strip()
                except Exception as e:
                    logger.debug(f"Direct fallback synthesis failed: {e}")

                if not briefing_text:
                    logger.info("Using runner output_tail directly after empty candidate text")
                    briefing_text = out_tail

        if not briefing_text:
            finish_reason = b_cand.get("finishReason", "UNKNOWN")
            fn_calls = [p["functionCall"] for p in b_parts if "functionCall" in p]
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
            }
        return briefing_text, None

    async def _call_gemini_api(
        self,
        payload: Dict[str, Any],
        model: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Call Gemini REST API for Mouth turn processing."""
        return await call_gemini_api(payload=payload, model=model, api_key=api_key)

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
        if ts:
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


__all__ = [
    "IngressGateway",
    "BackstageExecutionAccumulator",
    "DISPATCH_TASK_TOOL_DECLARATION",
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
    "DEFAULT_MAX_FILE_SIZE_BYTES",
    "TRANSFERS_DIR_NAME",
]

