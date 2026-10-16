#!/usr/bin/env python3
"""
golem/notify.py
Unified On-Demand Messaging Engine for First Mate / Golem.

Allows Golem (or any operational component/script) to proactively message Carl
on demand across Slack, iMessage, or both when attention or decision is needed.

Thread Continuity Architecture:
- Slack: Posts root message to Carl's DM channel (or specified channel), captures `ts`,
  and registers `channel:ts` in Golem's persistent session registry (~/.gemini/golem/sessions_registry.json).
  When Carl replies in thread, Golem rehydrates the session and fetches the full thread context.
- iMessage: Posts message via BlueBubbles API to Carl's chat GUID, captures dispatched `guid`,
  and persists it in BlueBubbles conversation history (~/.gemini/golem/imessage_history).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv

logger = logging.getLogger("Golem.Notify")

# Ensure .env is loaded from standard project roots (skip in test mode to allow clean env patching)
_is_test = os.environ.get("GOLEM_TEST_MODE") == "1"
if not _is_test:
    for env_path in [
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"),
        os.path.expanduser("/Users/hermes/projects/golem/.env"),
        os.path.expanduser("~/.hermes/.env"),
    ]:
        if os.path.isfile(env_path):
            load_dotenv(dotenv_path=env_path)

from golem.config import get_default_slack_channel

# Default Recipients & Channels
DEFAULT_SLACK_USER_ID = "UAM3FCV5L"
DEFAULT_SLACK_CHANNEL = get_default_slack_channel()  # Carl's 1:1 DM channel with Golem (@agy)
DEFAULT_IMESSAGE_CHAT_GUID = "iMessage;-;k.horned@gmail.com"
DEFAULT_REGISTRY_PATH = os.path.expanduser("~/.gemini/golem/sessions_registry.json")


def _record_slack_session_registry(
    channel: str,
    thread_ts: str,
    registry_path: Optional[str] = None,
    email_context: Optional[Dict[str, Any]] = None,
) -> None:
    """Register an outbound on-demand root message in Golem's session registry for thread continuity."""
    path = registry_path or os.environ.get("GOLEM_SESSIONS_REGISTRY_PATH") or os.environ.get("FIRSTMATE_SESSIONS_REGISTRY_PATH") or DEFAULT_REGISTRY_PATH
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        registry: Dict[str, Any] = {}
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    registry = json.load(f)
                except Exception:
                    registry = {}
        key = f"{channel}:{thread_ts}"
        rec_data: Dict[str, Any] = {
            "conversation_id": None,
            "target_cwd": os.getcwd(),
            "channel": channel,
            "thread_ts": thread_ts,
            "last_active": time.time(),
            "is_on_demand": True,
        }
        if email_context:
            rec_data["email_context"] = email_context
        registry[key] = rec_data
        tmp_path = f"{path}.tmp.{os.getpid()}"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(registry, f, indent=2)
        os.replace(tmp_path, path)
        logger.debug(f"Registered on-demand session '{key}' in {path}")
    except Exception as e:
        logger.warning(f"Could not record on-demand session to registry: {e}")


async def send_slack_message(
    text: str,
    channel: Optional[str] = None,
    thread_ts: Optional[str] = None,
    token: Optional[str] = None,
    email_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Send an on-demand message to Carl in Slack.
    
    Args:
        text: The message content (Slack mrkdwn supported).
        channel: Target Slack channel ID (defaults to Carl's DM: D0C2J1HKNN7).
        thread_ts: Optional parent message timestamp to reply in an existing thread.
        token: Optional Slack bot token (defaults to SLACK_BOT_TOKEN env).
        email_context: Optional email metadata and analysis to attach for thread continuity.
        
    Returns:
        Dict containing ok, channel, ts, and raw response.
    """
    if not text or not text.strip():
        return {"ok": False, "error": "Message text cannot be empty"}

    bot_token = (token or os.environ.get("SLACK_BOT_TOKEN") or "").strip()
    if not bot_token:
        logger.error("Missing SLACK_BOT_TOKEN for on-demand Slack messaging")
        return {"ok": False, "error": "Missing SLACK_BOT_TOKEN"}

    target_channel = channel or os.environ.get("GOLEM_DEFAULT_CHANNEL") or os.environ.get("DEFAULT_SLACK_CHANNEL") or get_default_slack_channel()

    try:
        from slack_sdk.web.async_client import AsyncWebClient
        client = AsyncWebClient(token=bot_token)
        
        post_kwargs: Dict[str, Any] = {
            "channel": target_channel,
            "text": text.strip(),
        }
        if thread_ts:
            post_kwargs["thread_ts"] = thread_ts

        resp = await client.chat_postMessage(**post_kwargs)
        if resp.get("ok"):
            msg_ts = resp.get("ts")
            # If this is a new root message (no thread_ts specified), register it in Golem's session registry
            # so Carl can reply directly to the thread and Golem immediately recognizes the context.
            effective_root_ts = thread_ts or msg_ts
            if effective_root_ts:
                _record_slack_session_registry(target_channel, effective_root_ts, email_context=email_context)

            return {
                "ok": True,
                "platform": "slack",
                "channel": target_channel,
                "ts": msg_ts,
                "thread_ts": effective_root_ts,
                "text": text,
            }
        else:
            return {"ok": False, "platform": "slack", "error": resp.get("error", "Unknown Slack error")}
    except Exception as e:
        logger.error(f"Failed to send on-demand Slack message: {e}")
        return {"ok": False, "platform": "slack", "error": str(e)}


async def send_imessage(
    text: str,
    chat_guid: Optional[str] = None,
    reply_to_guid: Optional[str] = None,
    server_url: Optional[str] = None,
    password: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Send an on-demand message to Carl in iMessage via BlueBubbles.
    
    Args:
        text: The message content.
        chat_guid: Target chat GUID (defaults to Carl's iMessage: iMessage;-;k.horned@gmail.com).
        reply_to_guid: Optional message GUID to reply directly to an existing message.
        server_url: Optional BlueBubbles server URL.
        password: Optional BlueBubbles password.
        
    Returns:
        Dict containing ok, chat_guid, guid, and raw response.
    """
    if not text or not text.strip():
        return {"ok": False, "error": "Message text cannot be empty"}

    target_chat = chat_guid or os.environ.get("BLUEBUBBLES_DEFAULT_CHAT_GUID") or DEFAULT_IMESSAGE_CHAT_GUID

    try:
        from bluebubbles_transport import (
            BlueBubblesTransport,
            resolve_bluebubbles_password,
            resolve_bluebubbles_url,
        )

        resolved_pwd = password or resolve_bluebubbles_password()
        resolved_url = resolve_bluebubbles_url(server_url=server_url, password=resolved_pwd)

        transport = BlueBubblesTransport(server_url=resolved_url, password=resolved_pwd)
        client = transport.get_client(target_chat)

        # Dispatch via client.chat_postMessage to ensure persistence in client.history and disk cache
        post_kwargs: Dict[str, Any] = {
            "channel": target_chat,
            "text": text.strip(),
            "reply_to_guid": reply_to_guid,
        }
        if reply_to_guid is None:
            post_kwargs["new_thread"] = True

        res = await client.chat_postMessage(**post_kwargs)

        # Close transport session cleanly
        if transport._session and not transport._session.closed:
            await transport._session.close()

        ok = bool(res.get("ok"))
        guid = res.get("guid") or res.get("message_guid") or res.get("ts")
        return {
            "ok": ok,
            "platform": "imessage",
            "chat_guid": target_chat,
            "guid": guid,
            "reply_to_guid": reply_to_guid,
            "text": text,
            "error": res.get("response", {}).get("message") if not ok else None,
        }
    except Exception as e:
        logger.error(f"Failed to send on-demand iMessage: {e}")
        return {"ok": False, "platform": "imessage", "error": str(e)}


async def send_on_demand_message(
    text: str,
    target: str = "both",
    slack_channel: Optional[str] = None,
    slack_thread_ts: Optional[str] = None,
    imessage_chat_guid: Optional[str] = None,
    imessage_reply_to_guid: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Send an on-demand message to Carl on Slack, iMessage, or both concurrently.
    
    Args:
        text: The message content.
        target: "slack", "imessage", or "both".
        slack_channel: Optional custom Slack channel.
        slack_thread_ts: Optional custom Slack thread_ts.
        imessage_chat_guid: Optional custom iMessage chat GUID.
        imessage_reply_to_guid: Optional custom iMessage reply_to_guid.
        
    Returns:
        Dict summarizing results: {"ok": bool, "slack": {...}, "imessage": {...}}
    """
    target_clean = (target or "both").lower().strip()
    results: Dict[str, Any] = {"ok": False}
    tasks = []

    if target_clean in ("slack", "both"):
        tasks.append(("slack", send_slack_message(text=text, channel=slack_channel, thread_ts=slack_thread_ts)))

    if target_clean in ("imessage", "both"):
        tasks.append(("imessage", send_imessage(text=text, chat_guid=imessage_chat_guid, reply_to_guid=imessage_reply_to_guid)))

    if not tasks:
        return {"ok": False, "error": f"Invalid target '{target}'. Must be 'slack', 'imessage', or 'both'."}

    gathered = await asyncio.gather(*(t[1] for t in tasks), return_exceptions=True)

    any_success = False
    for idx, (platform, _) in enumerate(tasks):
        res = gathered[idx]
        if isinstance(res, Exception):
            results[platform] = {"ok": False, "error": str(res)}
        elif isinstance(res, dict):
            results[platform] = res
            if res.get("ok"):
                any_success = True
        else:
            results[platform] = {"ok": False, "error": str(res)}

    results["ok"] = any_success
    return results


# ------------------------------------------------------------------------------
# Synchronous Convenience Wrappers
# ------------------------------------------------------------------------------

def send_slack_message_sync(text: str, channel: Optional[str] = None, thread_ts: Optional[str] = None) -> Dict[str, Any]:
    """Synchronous wrapper for send_slack_message."""
    return asyncio.run(send_slack_message(text=text, channel=channel, thread_ts=thread_ts))


def send_imessage_sync(text: str, chat_guid: Optional[str] = None, reply_to_guid: Optional[str] = None) -> Dict[str, Any]:
    """Synchronous wrapper for send_imessage."""
    return asyncio.run(send_imessage(text=text, chat_guid=chat_guid, reply_to_guid=reply_to_guid))


def send_on_demand_message_sync(
    text: str,
    target: str = "both",
    slack_channel: Optional[str] = None,
    imessage_chat_guid: Optional[str] = None,
) -> Dict[str, Any]:
    """Synchronous wrapper for send_on_demand_message."""
    return asyncio.run(
        send_on_demand_message(
            text=text,
            target=target,
            slack_channel=slack_channel,
            imessage_chat_guid=imessage_chat_guid,
        )
    )
