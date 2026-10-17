"""
BlueBubbles iMessage Transport Adapter for AGY Chief of Staff.
Provides an embedded aiohttp webhook listener on 0.0.0.0:8646 and an outbound client
for sending iMessages and attachments via the local BlueBubbles server.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import inspect
import json
import logging
import mimetypes
import os
import re
import tempfile
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Union
from urllib.parse import quote

import aiohttp
from aiohttp import web

logger = logging.getLogger("AGY.BlueBubbles")

# Tapback associatedMessageType codes: 2000-2005 added, 3000-3005 removed
TAPBACK_CODES = {*range(2000, 2006), *range(3000, 3006)}
MESSAGE_EVENTS = {"new-message", "message", "updated-message"}
MAX_IMESSAGE_CHUNK = 3500

SLACK_EMOJI_MAP = {
    ":white_check_mark:": "✅",
    ":heavy_check_mark:": "✔️",
    ":check:": "✅",
    ":eyes:": "👀",
    ":thumbsup:": "👍",
    ":+1:": "👍",
    ":thumbsdown:": "👎",
    ":-1:": "👎",
    ":warning:": "⚠️",
    ":x:": "❌",
    ":octagonal_sign:": "🛑",
    ":brain:": "🧠",
    ":gear:": "⚙️",
    ":page_facing_up:": "📄",
    ":wave:": "👋",
    ":fire:": "🔥",
    ":rocket:": "🚀",
    ":robot_face:": "🤖",
    ":robot:": "🤖",
    ":bulb:": "💡",
    ":tada:": "🎉",
    ":lock:": "🔒",
    ":clipboard:": "📋",
    ":package:": "📦",
    ":hammer:": "🔨",
    ":wrench:": "🔧",
    ":sparkles:": "✨",
    ":hourglass:": "⏳",
    ":hourglass_flowing_sand:": "⏳",
    ":thinking_face:": "🤔",
    ":memo:": "📝",
    ":chart_with_upwards_trend:": "📈",
    ":moneybag:": "💰",
}


def sanitize_for_imessage(text: str) -> str:
    """
    Sanitize Slack mrkdwn and shortcodes for clean, native BlueBubbles iMessage presentation:
    - Protects multiline fenced code blocks (```...```) so preformatted code/logs remain intact.
    - Links: <http://example.com|label> -> label (http://example.com) or http://example.com if label == url.
             <http://example.com> -> http://example.com.
             Markdown [label](url) -> label (url).
    - Emojis: Slack shortcodes (:white_check_mark:, :eyes:, :+1:, etc.) -> native Unicode.
    - Mentions: <#C123|channel> -> #channel, <@U123> -> @user.
    - Horizontal rules: '---', '___', '***' on their own lines -> clean paragraph spacing.
    - Headers: ATX Markdown headers (# Header) -> clean heading text.
    - Blockquotes: '> quote' -> '│ quote' (clean native vertical bar).
    - Bullets: Normalizes bullet hierarchy:
        • Top-level (*, -, +, •) -> '• '
        • Nested level 1 (2-3 spaces) -> '  ◦ '
        • Nested level 2 (4+ spaces) -> '    ▪ '
    - Inline code: Strips backticks (`identifier`) to reduce punctuation clutter.
    - Bold/Italic: Strips asterisk clutter (*bold*, **bold**, ***bold-italic***) and Slack italics (_italic_).
    - Collapses excessive blank lines (3+ newlines -> 2).
    """
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\u00a0", " ")

    # 1. Protect multiline fenced code blocks
    code_blocks: List[str] = []
    def _save_block(m: re.Match) -> str:
        code_blocks.append(m.group(0))
        return f"\x00CODEBLOCK_{len(code_blocks)-1}\x00"

    sanitized = re.sub(r"```.*?```", _save_block, text, flags=re.DOTALL)

    # 2. Convert Slack links <url|label> or <url>
    def _link_replace(match: re.Match) -> str:
        url = match.group(1).strip()
        label = match.group(2).strip() if match.group(2) else None
        if label:
            if label == url:
                return url
            return f"{label} ({url})"
        return url

    sanitized = re.sub(
        r"<((?:https?://|mailto:)[^|>]+)(?:\|([^>]+))?>",
        _link_replace,
        sanitized
    )

    # Convert standard Markdown links [label](url)
    def _md_link_replace(match: re.Match) -> str:
        label = match.group(1).strip()
        url = match.group(2).strip()
        if label == url:
            return url
        return f"{label} ({url})"

    sanitized = re.sub(r"\[([^\]]+)\]\(((?:https?://|mailto:)[^)]+)\)", _md_link_replace, sanitized)

    # 3. Convert channel mentions <#C123|name> -> #name or <#C123> -> #channel
    sanitized = re.sub(r"<#[A-Z0-9]+\|([^>]+)>", r"#\1", sanitized)
    sanitized = re.sub(r"<#([A-Z0-9]+)>", r"#\1", sanitized)

    # 4. Convert user mentions <@U123|name> -> @name or <@U123> -> @user
    sanitized = re.sub(r"<@[A-Z0-9]+\|([^>]+)>", r"@\1", sanitized)
    sanitized = re.sub(r"<@([A-Z0-9]+)>", r"@\1", sanitized)

    # 5. Convert Slack emoji shortcodes to native Unicode
    for shortcode, unicode_char in SLACK_EMOJI_MAP.items():
        sanitized = sanitized.replace(shortcode, unicode_char)

    # 6. Remove horizontal rule lines (---, ___, ***)
    sanitized = re.sub(r"^[ \t]*[-*_]{3,}[ \t]*$", "", sanitized, flags=re.MULTILINE)

    # 7. Normalize line-by-line: ATX headers, blockquotes, and bullet hierarchies
    lines = sanitized.split("\n")
    cleaned_lines = []
    for line in lines:
        # ATX headings: # Heading -> Heading
        h_m = re.match(r"^[ \t]*#{1,6}[ \t]+(.*?)[ \t]*#*$", line)
        if h_m:
            line = h_m.group(1)

        # Blockquote: > text -> │ text
        bq_m = re.match(r"^[ \t]*>[ \t]*(.*)$", line)
        if bq_m:
            line = f"│ {bq_m.group(1)}"

        # Expand tabs if present
        detabbed = line.replace("\t", "  ")
        # Bullet list hierarchy normalization
        b_m = re.match(r"^([ ]*)(?:[•*+-]|\u25e6|\u25aa)[ \t]+(.*)$", detabbed)
        if b_m:
            indent_len = len(b_m.group(1))
            content = b_m.group(2)
            if indent_len <= 1:
                line = f"• {content}"
            elif indent_len <= 3:
                line = f"  ◦ {content}"
            else:
                line = f"    ▪ {content}"
        cleaned_lines.append(line.rstrip())
    sanitized = "\n".join(cleaned_lines)

    # 8. Strip inline backticks: `code` -> code
    sanitized = re.sub(r"`([^`\n]+?)`", r"\1", sanitized)

    # 9. Strip asterisk bold/italic formatting clutter
    sanitized = re.sub(r"\*{1,3}(?!\s)([^*\n]+?)(?<!\s)\*{1,3}", r"\1", sanitized)

    # 10. Strip Slack italics: _italic_ -> italic (avoiding snake_case identifiers)
    sanitized = re.sub(r"(?<!\w)_(?!\s)([^_\n]+?)(?<!\s)_(?!\w)", r"\1", sanitized)

    # 11. Collapse excessive blank lines
    sanitized = re.sub(r"\n{3,}", "\n\n", sanitized)
    sanitized = sanitized.strip()

    # 12. Restore protected code blocks
    for idx, block in enumerate(code_blocks):
        sanitized = sanitized.replace(f"\x00CODEBLOCK_{idx}\x00", block)

    return sanitized


def chunk_text(text: str, max_chars: int = MAX_IMESSAGE_CHUNK) -> List[str]:
    """Chunk text into segments of at most max_chars, preferably breaking on newlines or spaces."""
    if not text:
        return [""]
    if len(text) <= max_chars:
        return [text]

    chunks = []
    remaining = text
    while len(remaining) > max_chars:
        # Prefer breaking at newline
        split_idx = remaining.rfind("\n", 0, max_chars)
        if split_idx == -1 or split_idx < max_chars // 2:
            split_idx = remaining.rfind(" ", 0, max_chars)
        if split_idx == -1 or split_idx < max_chars // 2:
            split_idx = max_chars

        chunk = remaining[:split_idx].strip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_idx:].strip()

    if remaining:
        chunks.append(remaining)

    return chunks or [text]


SLACK_TO_BLUEBUBBLES_REACTION: Dict[str, str] = {
    # Task Queued / Working
    "gear": "emphasize",
    "eyes": "emphasize",
    "hourglass": "emphasize",
    "thinking": "emphasize",
    # Success Completion
    "white_check_mark": "like",
    "heavy_check_mark": "like",
    "check": "like",
    "thumbsup": "like",
    "+1": "like",
    # Execution Failure & Blockers
    "x": "question",
    "warning": "question",
    "question": "question",
    "boom": "question",
    "octagonal_sign": "question",
    # Others
    "thumbsdown": "dislike",
    "-1": "dislike",
    "heart": "love",
    "laugh": "laugh",
    "joy": "laugh",
    "fire": "emphasize",
    "rocket": "like",
    # Direct BlueBubbles reaction type passthrough
    "love": "love",
    "like": "like",
    "dislike": "dislike",
    "laugh": "laugh",
    "emphasize": "emphasize",
}


class BlueBubblesClient:
    """
    Slack-compatible WebClient shim for iMessage chat sessions with Private API support.
    Allows AGY's command handlers, preflight alignment engine, and Herdr execution
    to operate transparently over iMessage with native tapbacks, typing indicators,
    inline thread replies, and in-place message edits.
    """

    def __init__(self, transport: "BlueBubblesTransport", chat_guid: str, history_dir: Optional[str] = None):
        self.transport = transport
        self.chat_guid = chat_guid
        self.history_dir = history_dir or os.environ.get("BLUEBUBBLES_HISTORY_DIR") or os.path.expanduser("~/.gemini/golem/imessage_history")
        self.history: List[Dict[str, Any]] = []
        self.last_inbound_guid: Optional[str] = None
        self.last_dispatched_guid: Optional[str] = None
        self.ts_to_guid_map: Dict[str, str] = {}
        self.guid_to_ts_map: Dict[str, str] = {}
        self._load_history()

    def _get_history_file(self) -> str:
        try:
            os.makedirs(self.history_dir, exist_ok=True)
            safe_guid = re.sub(r"[^a-zA-Z0-9_.-]", "_", self.chat_guid)
            return os.path.join(self.history_dir, f"{safe_guid}.json")
        except Exception:
            return ""

    def _load_history(self) -> None:
        try:
            path = self._get_history_file()
            if path and os.path.isfile(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    self.history = data[-100:]
                    for item in self.history:
                        ts = item.get("ts")
                        guid = item.get("guid")
                        if ts and guid:
                            self.ts_to_guid_map[str(ts)] = str(guid)
                            self.guid_to_ts_map[str(guid)] = str(ts)
                            if item.get("bot_id"):
                                self.last_dispatched_guid = str(guid)
                            else:
                                self.last_inbound_guid = str(guid)
        except Exception as e:
            logger.debug(f"Failed to load history for {self.chat_guid}: {e}")

    def _save_history(self) -> None:
        try:
            path = self._get_history_file()
            if not path:
                return
            tmp_path = f"{path}.tmp.{os.getpid()}"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self.history[-100:], f, indent=2)
            os.replace(tmp_path, path)
        except Exception as e:
            logger.debug(f"Failed to save history for {self.chat_guid}: {e}")

    def clear_history(self) -> None:
        """Clear cached and persisted conversation history for this chat."""
        self.history = []
        self.last_inbound_guid = None
        self.last_dispatched_guid = None
        self.ts_to_guid_map.clear()
        self.guid_to_ts_map.clear()
        self._save_history()

    def resolve_thread_root(self, guid: str) -> str:
        """Resolve the root message GUID for a thread if guid is an intermediate reply."""
        if not guid:
            return ""
        curr = guid
        for _ in range(10):
            found = False
            for m in self.history:
                if (m.get("guid") == curr or str(m.get("ts")) == curr) and m.get("thread_originator_guid"):
                    parent = m["thread_originator_guid"]
                    if parent and parent != curr:
                        curr = parent
                        found = True
                        break
            if not found:
                break
        return curr

    def record_incoming(
        self,
        user: str,
        text: str,
        ts: str,
        message_guid: Optional[str] = None,
        thread_originator_guid: Optional[str] = None,
    ):
        if message_guid:
            self.last_inbound_guid = message_guid
            self.ts_to_guid_map[ts] = message_guid
            self.guid_to_ts_map[message_guid] = ts
        elif ts and ("-" in ts or len(ts) > 20):
            self.last_inbound_guid = ts
            self.ts_to_guid_map[ts] = ts

        entry: Dict[str, Any] = {
            "user": user,
            "text": text,
            "ts": ts,
            "guid": message_guid or (ts if ("-" in ts) else None),
        }
        if thread_originator_guid:
            entry["thread_originator_guid"] = thread_originator_guid

        self.history.append(entry)
        if len(self.history) > 100:
            self.history.pop(0)
        self._save_history()

    async def chat_postMessage(
        self,
        channel: str,
        text: str = "",
        thread_ts: Optional[str] = None,
        reply_to_guid: Optional[str] = None,
        effect_id: Optional[str] = None,
        force_notify: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        target = channel or self.chat_guid
        clean_text = sanitize_for_imessage(text)
        now_ts = str(time.time())

        # Determine thread reply target GUID
        parent_guid = reply_to_guid
        if reply_to_guid is False or kwargs.get("new_thread") or kwargs.get("is_new_thread"):
            parent_guid = None
        elif not parent_guid and thread_ts:
            if thread_ts in self.ts_to_guid_map:
                parent_guid = self.ts_to_guid_map[thread_ts]
            elif not (thread_ts.startswith("iMessage;") or thread_ts.startswith("SMS;")):
                parent_guid = thread_ts
            else:
                parent_guid = self.last_inbound_guid
        elif not parent_guid:
            parent_guid = self.last_inbound_guid

        res = self.transport.send_imessage(
            target,
            clean_text,
            reply_to_guid=parent_guid,
            effect_id=effect_id,
            force_notify=force_notify,
        )
        if inspect.isawaitable(res):
            res = await res

        dispatched_guid = ""
        if isinstance(res, dict):
            dispatched_guid = str(res.get("data", {}).get("guid") or res.get("guid") or "")

        msg_ts = dispatched_guid or now_ts
        if dispatched_guid:
            self.last_dispatched_guid = dispatched_guid
            self.ts_to_guid_map[now_ts] = dispatched_guid
            self.guid_to_ts_map[dispatched_guid] = now_ts
            self.ts_to_guid_map[dispatched_guid] = dispatched_guid

        entry: Dict[str, Any] = {
            "bot_id": "B_AGY",
            "text": clean_text,
            "ts": msg_ts,
            "guid": dispatched_guid or None,
        }
        if parent_guid:
            entry["thread_originator_guid"] = parent_guid
        self.history.append(entry)
        if len(self.history) > 100:
            self.history.pop(0)
        self._save_history()

        ok_status = (
            (isinstance(res, dict) and (res.get("status") == 200 or res.get("success", False)))
            or bool(dispatched_guid)
        )
        return {
            "ok": ok_status,
            "ts": msg_ts,
            "channel": target,
            "guid": dispatched_guid,
            "message_guid": dispatched_guid,
            "response": res
        }

    async def chat_update(
        self,
        channel: str,
        ts: str,
        text: str = "",
        backwards_text: Optional[str] = None,
        part_index: int = 0,
        **kwargs
    ) -> Dict[str, Any]:
        """Update an existing iMessage in-place via Private API message edit."""
        target = channel or self.chat_guid
        clean_text = sanitize_for_imessage(text)
        target_guid = self.ts_to_guid_map.get(ts, ts)
        await self.transport.edit_message(
            message_guid=target_guid,
            new_text=clean_text,
            backwards_text=backwards_text or clean_text,
            part_index=part_index,
        )
        # Update entry in history if found
        for entry in reversed(self.history):
            if entry.get("ts") == ts or entry.get("guid") == target_guid:
                entry["text"] = clean_text
                break
        return {"ok": True, "ts": ts, "channel": target, "message_guid": target_guid}

    async def reactions_add(
        self,
        channel: str,
        name: str,
        timestamp: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Send a native tapback reaction via BlueBubbles Private API."""
        target = channel or self.chat_guid
        raw_name = name.strip(":").lower()
        reaction_type = SLACK_TO_BLUEBUBBLES_REACTION.get(
            raw_name,
            "like" if any(k in raw_name for k in ("check", "thumbs", "plus", "1")) else ("question" if "x" in raw_name else "emphasize")
        )

        target_guid = self.ts_to_guid_map.get(timestamp) if timestamp else None
        if not target_guid and timestamp and not (timestamp.startswith("iMessage;") or timestamp.startswith("SMS;")):
            target_guid = timestamp
        if not target_guid:
            target_guid = self.last_inbound_guid

        if target_guid:
            logger.debug(f"[iMessage Reaction Add] {reaction_type} (from {name}) on {target_guid} in {target}")
            await self.transport.send_reaction(
                chat_guid=target,
                message_guid=target_guid,
                reaction_type=reaction_type,
            )
        else:
            logger.debug(f"[iMessage Reaction Add] No target message GUID found for reaction :{name}: in {target}")
        return {"ok": True}

    async def reactions_remove(
        self,
        channel: str,
        name: str,
        timestamp: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        logger.debug(f"[iMessage Reaction Remove] :{name}: on {channel} ({timestamp})")
        return {"ok": True}

    async def assistant_threads_setStatus(
        self,
        channel_id: Optional[str] = None,
        thread_ts: Optional[str] = None,
        status: str = "",
        **kwargs
    ) -> Dict[str, Any]:
        """Start or stop typing indicator based on assistant pulsing status."""
        target = channel_id or kwargs.get("channel") or self.chat_guid
        active = bool(status and status.strip())
        await self.transport.set_typing_indicator(chat_guid=target, active=active)
        return {"ok": True}

    async def files_upload_v2(
        self,
        channel: str,
        file: Optional[str] = None,
        filename: Optional[str] = None,
        content: Optional[str] = None,
        title: Optional[str] = None,
        thread_ts: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        target = channel or self.chat_guid
        if file and os.path.isfile(file):
            fname = filename or os.path.basename(file)
            res = await self.transport.send_imessage_attachment(target, file, filename=fname)
            ok = False
            if isinstance(res, dict):
                ok = res.get("status") == 200 or res.get("success") is True or bool(res.get("data"))
            return {"ok": ok, "file": {"name": fname}}

        if content is not None:
            safe_name = filename or f"snippet_{int(time.time())}.txt"
            with tempfile.NamedTemporaryFile("w", suffix=f"_{safe_name}", delete=False) as tf:
                tf.write(content)
                tmp_path = tf.name
            try:
                res = await self.transport.send_imessage_attachment(target, tmp_path, filename=safe_name)
                ok = False
                if isinstance(res, dict):
                    ok = res.get("status") == 200 or res.get("success") is True or bool(res.get("data"))
                return {"ok": ok, "file": {"name": safe_name}}
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        return {"ok": False, "error": "No file or content provided"}

    async def download_attachment(self, attachment_guid: str) -> Optional[bytes]:
        """Download an attachment from BlueBubbles server."""
        return await self.transport.download_attachment(attachment_guid)

    async def conversations_replies(
        self,
        channel: str,
        ts: Optional[str] = None,
        limit: int = 100,
        **kwargs
    ) -> Dict[str, Any]:
        if len(self.history) <= 1 and hasattr(self.transport, "fetch_chat_messages"):
            try:
                res = self.transport.fetch_chat_messages(self.chat_guid, limit=min(limit, 25))
                if inspect.isawaitable(res):
                    server_msgs = await res
                else:
                    server_msgs = res
                if server_msgs and isinstance(server_msgs, list):
                    known_ts = {str(m.get("ts")) for m in self.history if m.get("ts")}
                    known_guids = {str(m.get("guid")) for m in self.history if m.get("guid")}
                    merged = []
                    for sm in server_msgs:
                        sm_ts = str(sm.get("ts", ""))
                        sm_guid = str(sm.get("guid", ""))
                        if (not sm_ts or sm_ts not in known_ts) and (not sm_guid or sm_guid not in known_guids):
                            merged.append(sm)
                    merged.extend(self.history)
                    self.history = merged[-100:]
                    self._save_history()
            except Exception as e:
                logger.debug(f"fetch_chat_messages error: {e}")
        messages = self.history
        if ts and ts != channel and ts != self.chat_guid:
            root_guid = self.resolve_thread_root(ts)
            thread_msgs = [
                m for m in self.history
                if m.get("thread_originator_guid") == root_guid
                or m.get("guid") == root_guid
                or m.get("thread_originator_guid") == ts
                or m.get("guid") == ts
                or str(m.get("ts")) == root_guid
                or str(m.get("ts")) == ts
            ]
            if thread_msgs:
                messages = thread_msgs
        return {"ok": True, "messages": list(messages[-limit:])}

    @property
    def conversations(self):
        class _ConversationsShim:
            def __init__(self, outer):
                self.outer = outer
            async def replies(self, channel: str, ts: Optional[str] = None, limit: int = 100, **kw):
                return await self.outer.conversations_replies(channel, ts, limit=limit, **kw)
        return _ConversationsShim(self)


def resolve_bluebubbles_password() -> str:
    """Resolve BlueBubbles password from env or /Users/hermes/.hermes/.env or fallback."""
    pwd = os.getenv("BLUEBUBBLES_PASSWORD")
    if pwd:
        return pwd
    hermes_env = os.path.expanduser("~/.hermes/.env")
    if os.path.isfile(hermes_env):
        try:
            with open(hermes_env, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("BLUEBUBBLES_PASSWORD="):
                        val = line.split("=", 1)[1].strip().strip("\"'")
                        if val:
                            return val
        except Exception:
            pass
    return "!Fulurban88"


def resolve_bluebubbles_url(server_url: Optional[str] = None, password: Optional[str] = None) -> str:
    """Resolve reachable BlueBubbles server URL across local and containerized environments."""
    if server_url:
        return server_url.strip()
    explicit = os.getenv("BLUEBUBBLES_URL", "").strip()
    if os.getenv("PYTEST_CURRENT_TEST"):
        return explicit or "http://127.0.0.1:1234"

    pwd = password or resolve_bluebubbles_password()
    for cand in [explicit or "http://127.0.0.1:1234", "http://host.docker.internal:1234"]:
        cand_clean = cand.rstrip("/")
        if not cand_clean.startswith("http"):
            cand_clean = f"http://{cand_clean}"
        try:
            import urllib.request
            req = urllib.request.Request(f"{cand_clean}/api/v1/ping?password={pwd}")
            with urllib.request.urlopen(req, timeout=0.8) as resp:
                if resp.status == 200:
                    return cand_clean
        except Exception:
            pass
    return explicit or "http://127.0.0.1:1234"


class BlueBubblesTransport:
    """
    Manages connection to local BlueBubbles macOS server, outbound iMessage REST calls,
    dynamic webhook registration, and an aiohttp webhook listener.
    """

    def __init__(
        self,
        server_url: Optional[str] = None,
        password: Optional[str] = None,
        webhook_host: Optional[str] = None,
        webhook_port: Optional[int] = None,
        webhook_path: str = "/bluebubbles-webhook",
        allowed_handles: Optional[Union[str, Set[str]]] = None,
        message_handler: Optional[Callable] = None
    ):
        self.password = password or resolve_bluebubbles_password()
        raw_url = resolve_bluebubbles_url(server_url=server_url, password=self.password)
        if raw_url and not re.match(r"^https?://", raw_url, flags=re.IGNORECASE):
            raw_url = f"http://{raw_url}"
        self.server_url = raw_url.rstrip("/")
        self.webhook_host = webhook_host or os.getenv("BLUEBUBBLES_WEBHOOK_HOST", "0.0.0.0")
        port_val = webhook_port if webhook_port is not None else os.getenv("BLUEBUBBLES_WEBHOOK_PORT", 8646)
        self.webhook_port = int(port_val)
        self.webhook_path = webhook_path if webhook_path.startswith("/") else f"/{webhook_path}"
        self.message_handler = message_handler

        # Parse allowed handles whitelist
        if allowed_handles is None:
            allowed_handles = os.getenv("BLUEBUBBLES_ALLOWED_HANDLES", "k.horned@gmail.com")

        if isinstance(allowed_handles, str):
            self.allowed_handles = {
                h.strip().lower() for h in allowed_handles.split(",") if h.strip()
            }
        elif isinstance(allowed_handles, (set, list)):
            self.allowed_handles = {str(h).strip().lower() for h in allowed_handles if str(h).strip()}
        else:
            self.allowed_handles = set()


        self._session: Optional[aiohttp.ClientSession] = None
        self._app_runner: Optional[web.AppRunner] = None
        self._clients: Dict[str, BlueBubblesClient] = {}
        self._background_tasks: Set[asyncio.Task] = set()
        self._private_api_supported: Optional[bool] = None
        self._last_private_api_check: float = 0.0
        self._seen_guids: Dict[str, float] = {}

    def _reload_allowed_handles_from_env(self) -> None:
        """Dynamically check .env on disk for newly added allowed handles."""
        env_candidates = [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
            "/Users/hermes/projects/golem/.env",
            "/Users/hermes/projects/first-mate/.env",
            os.path.expanduser("~/.hermes/.env"),
        ]
        for ep in env_candidates:
            if os.path.isfile(ep):
                try:
                    with open(ep, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if line.startswith("BLUEBUBBLES_ALLOWED_HANDLES="):
                                val = line.split("=", 1)[1].strip().strip("\"'")
                                for h in val.split(","):
                                    if h.strip():
                                        self.allowed_handles.add(h.strip().lower())
                                return
                except Exception:
                    pass

    def _api_url(self, path: str) -> str:
        sep = "&" if "?" in path else "?"
        return f"{self.server_url}{path}{sep}password={quote(self.password, safe='')}"

    @property
    def webhook_local_url(self) -> str:
        return f"http://127.0.0.1:{self.webhook_port}{self.webhook_path}?password={quote(self.password, safe='')}"

    async def _get_session(self) -> aiohttp.ClientSession:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        is_real_session = isinstance(self._session, aiohttp.ClientSession)
        session_loop = getattr(self._session, "_loop", None) if is_real_session else None
        loop_mismatch = (
            is_real_session
            and loop is not None
            and session_loop is not None
            and session_loop is not loop
        )
        if getattr(self, "_closing", False):
            if self._session is not None and not getattr(self._session, "closed", False):
                return self._session
            raise RuntimeError("BlueBubblesTransport is closing")

        if (
            self._session is None
            or self._session.closed
            or loop_mismatch
        ):
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45))
        return self._session

    def get_client(self, chat_guid: str) -> BlueBubblesClient:
        if chat_guid not in self._clients:
            self._clients[chat_guid] = BlueBubblesClient(self, chat_guid)
        return self._clients[chat_guid]

    # --------------------------------------------------------------------------
    # REST API Client Methods
    # --------------------------------------------------------------------------
    async def ping(self) -> bool:
        """Check BlueBubbles server reachability."""
        try:
            session = await self._get_session()
            async with session.get(self._api_url("/api/v1/ping")) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("status") == 200
                return False
        except Exception as e:
            logger.warning(f"BlueBubbles ping error: {e}")
            return False

    async def get_server_info(self) -> Dict[str, Any]:
        """Fetch BlueBubbles server configuration and info."""
        try:
            session = await self._get_session()
            async with session.get(self._api_url("/api/v1/server/info")) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
        except Exception as e:
            logger.error(f"Failed to fetch BlueBubbles server info: {e}")
            return {}

    async def find_registered_webhooks(self, target_url: str) -> List[Dict[str, Any]]:
        """List webhooks matching the target URL (with or without query password)."""
        target_base = target_url.split("?")[0]
        try:
            session = await self._get_session()
            async with session.get(self._api_url("/api/v1/webhook")) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    data = payload.get("data", [])
                    if isinstance(data, list):
                        matches = []
                        for wh in data:
                            u = wh.get("url", "")
                            if u == target_url or u.split("?")[0] == target_base:
                                matches.append(wh)
                        return matches
        except Exception as e:
            logger.warning(f"Failed to list registered webhooks: {e}")
        return []

    async def register_webhook(self) -> bool:
        """Register the local aiohttp webhook endpoint with the BlueBubbles server."""
        webhook_url = self.webhook_local_url
        existing = await self.find_registered_webhooks(webhook_url)
        if existing:
            logger.info(f"BlueBubbles webhook already registered: {existing[0].get('url', webhook_url)}")
            return True

        try:
            session = await self._get_session()
            payload = {
                "url": webhook_url,
                "events": ["new-message", "updated-message"]
            }
            async with session.post(self._api_url("/api/v1/webhook"), json=payload) as resp:
                if 200 <= resp.status < 300:
                    data = await resp.json()
                    logger.info(f"Successfully registered BlueBubbles webhook (ID: {data.get('data', {}).get('id')})")
                    return True
                else:
                    err_text = await resp.text()
                    logger.warning(f"Webhook registration returned status {resp.status}: {err_text}")
                    return False
        except Exception as e:
            logger.warning(f"Failed to register webhook with BlueBubbles server: {e}")
            return False

    async def unregister_webhook(self) -> bool:
        """Unregister our webhook endpoint from the BlueBubbles server."""
        webhook_url = self.webhook_local_url
        existing = await self.find_registered_webhooks(webhook_url)
        if not existing:
            return True

        removed = False
        session = await self._get_session()
        for wh in existing:
            wh_id = wh.get("id")
            if wh_id is not None:
                try:
                    async with session.delete(self._api_url(f"/api/v1/webhook/{wh_id}")) as resp:
                        if resp.status == 200:
                            logger.info(f"Unregistered BlueBubbles webhook ID {wh_id}")
                            removed = True
                except Exception as e:
                    logger.debug(f"Failed to delete webhook {wh_id}: {e}")
        return removed

    async def is_private_api_available(self) -> bool:
        """Check if BlueBubbles Server reports Private API Helper connected."""
        now = time.time()
        if self._private_api_supported is not None and (now - self._last_private_api_check) < 30.0:
            return self._private_api_supported
        try:
            session = await self._get_session()
            url = self._api_url("/api/v1/server/info")
            timeout = aiohttp.ClientTimeout(total=2.5)
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    server_data = data.get("data", {})
                    is_avail = bool(server_data.get("private_api") and server_data.get("helper_connected"))
                    self._private_api_supported = is_avail
                    self._last_private_api_check = now
                    return is_avail
        except Exception as e:
            logger.debug(f"Failed to probe BlueBubbles server info: {e}")
        self._private_api_supported = False
        self._last_private_api_check = now
        return False

    async def fetch_chat_messages(
        self,
        chat_guid: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Fetch recent message history for a chat from BlueBubbles server."""
        if not chat_guid:
            return []
        try:
            session = await self._get_session()
            url = self._api_url(f"/api/v1/chat/{quote(chat_guid, safe='')}/message")
            params = {"limit": str(limit), "sort": "DESC"}
            timeout = aiohttp.ClientTimeout(total=3.0)
            async with session.get(url, params=params, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw_messages = data.get("data", [])
                    normalized = []
                    # BlueBubbles returns newest first with sort=DESC, so reverse to chronological order
                    for m in reversed(raw_messages):
                        if not isinstance(m, dict):
                            continue
                        text = m.get("text") or ""
                        if not text:
                            continue
                        is_from_me = bool(m.get("isFromMe", False))
                        guid = m.get("guid")
                        m_ts = guid or str(m.get("dateCreated") or time.time())
                        entry: Dict[str, Any] = {
                            "text": text,
                            "ts": m_ts,
                            "guid": guid,
                        }
                        if is_from_me:
                            entry["bot_id"] = "B_AGY"
                        else:
                            h = m.get("handle")
                            sender_addr = h.get("address") if isinstance(h, dict) else (m.get("sender") or "User")
                            entry["user"] = sender_addr
                        normalized.append(entry)
                    return normalized
                else:
                    logger.debug(f"fetch_chat_messages for {chat_guid} returned [{resp.status}]")
        except Exception as e:
            logger.debug(f"Failed to fetch chat messages for {chat_guid}: {e}")
        return []

    async def set_chat_read(self, chat_guid: str) -> None:
        """Mark a chat as read via BlueBubbles Private API."""
        if not chat_guid:
            return
        try:
            session = await self._get_session()
            url = self._api_url(f"/api/v1/chat/{quote(chat_guid, safe='')}/read")
            timeout = aiohttp.ClientTimeout(total=2.5)
            async with session.post(url, timeout=timeout) as resp:
                if resp.status == 200:
                    logger.debug(f"Marked chat {chat_guid} as read")
                else:
                    err_text = await resp.text()
                    logger.warning(f"set_chat_read for {chat_guid} returned [{resp.status}]: {err_text}")
        except Exception as e:
            logger.warning(f"Failed to set chat read for {chat_guid}: {e}")

    async def set_typing_indicator(self, chat_guid: str, active: bool) -> None:
        """Start or stop the native typing indicator via BlueBubbles Private API."""
        if not chat_guid:
            return
        try:
            session = await self._get_session()
            url = self._api_url(f"/api/v1/chat/{quote(chat_guid, safe='')}/typing")
            timeout = aiohttp.ClientTimeout(total=2.5)
            if active:
                async with session.post(url, timeout=timeout) as resp:
                    if resp.status == 200:
                        logger.debug(f"Started typing indicator in {chat_guid}")
                    else:
                        err_text = await resp.text()
                        logger.warning(f"start typing indicator for {chat_guid} returned [{resp.status}]: {err_text}")
            else:
                async with session.delete(url, timeout=timeout) as resp:
                    if resp.status == 200:
                        logger.debug(f"Stopped typing indicator in {chat_guid}")
                    else:
                        err_text = await resp.text()
                        logger.warning(f"stop typing indicator for {chat_guid} returned [{resp.status}]: {err_text}")
        except Exception as e:
            logger.warning(f"Failed to update typing indicator for {chat_guid} (active={active}): {e}")

    async def send_reaction(
        self,
        chat_guid: str,
        message_guid: str,
        reaction_type: str,
        part_index: int = 0
    ) -> None:
        """Send a native tapback reaction via BlueBubbles Private API."""
        if not chat_guid or not message_guid or not reaction_type:
            return
        payload = {
            "chatGuid": chat_guid,
            "selectedMessageGuid": message_guid,
            "reaction": reaction_type,
            "reactionType": reaction_type,
            "partIndex": part_index,
        }
        try:
            session = await self._get_session()
            url = self._api_url("/api/v1/message/react")
            timeout = aiohttp.ClientTimeout(total=2.5)
            async with session.post(url, json=payload, timeout=timeout) as resp:
                if resp.status == 200:
                    logger.debug(f"Sent tapback '{reaction_type}' to {message_guid} in {chat_guid}")
                else:
                    err_text = await resp.text()
                    logger.warning(f"send_reaction for {message_guid} returned [{resp.status}]: {err_text}")
        except Exception as e:
            logger.warning(f"Failed to send tapback '{reaction_type}' to {message_guid}: {e}")

    async def edit_message(
        self,
        message_guid: str,
        new_text: str,
        backwards_text: Optional[str] = None,
        part_index: int = 0
    ) -> None:
        """Edit an existing iMessage in-place via BlueBubbles Private API."""
        if not message_guid or not new_text:
            return
        clean_new = sanitize_for_imessage(new_text)
        clean_bw = sanitize_for_imessage(backwards_text) if backwards_text else clean_new
        payload = {
            "editedMessage": clean_new,
            "newText": clean_new,
            "backwardsCompatibilityMessage": clean_bw,
            "backwardsCompatibilityText": clean_bw,
            "partIndex": part_index,
        }
        try:
            session = await self._get_session()
            url = self._api_url(f"/api/v1/message/{quote(message_guid, safe='')}/edit")
            timeout = aiohttp.ClientTimeout(total=2.5)
            async with session.post(url, json=payload, timeout=timeout) as resp:
                if resp.status == 200:
                    logger.debug(f"Edited message {message_guid} in-place")
                else:
                    err_text = await resp.text()
                    logger.warning(f"edit_message for {message_guid} returned [{resp.status}]: {err_text}")
        except Exception as e:
            logger.warning(f"Failed to edit message {message_guid}: {e}")

    async def send_message(
        self,
        chat_guid: str,
        text: str,
        reply_to_guid: Optional[str] = None,
        effect_id: Optional[str] = None,
        force_notify: bool = False
    ) -> str:
        """Send an outbound iMessage and capture the dispatched message guid."""
        res = await self.send_imessage(
            chat_guid=chat_guid,
            text=text,
            reply_to_guid=reply_to_guid,
            effect_id=effect_id,
            force_notify=force_notify,
        )
        if isinstance(res, dict):
            guid = res.get("data", {}).get("guid") or res.get("guid") or ""
            return str(guid)
        return ""

    async def send_imessage(
        self,
        chat_guid: str,
        text: str,
        reply_to_guid: Optional[str] = None,
        effect_id: Optional[str] = None,
        force_notify: bool = False,
    ) -> Dict[str, Any]:
        """
        Send an outbound iMessage to a chat GUID, automatically chunking
        messages exceeding MAX_IMESSAGE_CHUNK (3,500 characters).
        Uses Private API if available with inline thread reply / effects,
        falling back cleanly to AppleScript on failure.
        """
        if not text:
            return {"status": 400, "message": "Text cannot be empty"}

        clean_text = sanitize_for_imessage(text)
        session = await self._get_session()
        chunks = chunk_text(clean_text, max_chars=MAX_IMESSAGE_CHUNK)
        last_res = {"status": 200, "message": "Success"}
        timeout = aiohttp.ClientTimeout(total=2.5)
        primary_guid = ""

        use_private_api = await self.is_private_api_available()

        for idx, chunk in enumerate(chunks):
            temp_id = f"temp-{uuid.uuid4().hex[:12]}"
            payload: Dict[str, Any] = {
                "chatGuid": chat_guid,
                "tempGuid": temp_id,
                "message": chunk,
            }
            if use_private_api:
                payload["method"] = "private-api"
                if reply_to_guid and idx == 0:
                    payload["selectedMessageGuid"] = reply_to_guid
                    payload["partIndex"] = 0
                if effect_id and idx == 0:
                    payload["effectId"] = effect_id
                if force_notify:
                    payload["forceNotify"] = True
            else:
                payload["method"] = "apple-script"

            try:
                async with session.post(self._api_url("/api/v1/message/text"), json=payload, timeout=timeout) as resp:
                    if resp.status != 200:
                        err_body = await resp.text()
                        if use_private_api and resp.status in (400, 500):
                            logger.warning(
                                f"BlueBubbles Private API dispatch failed [{resp.status}]: {err_body}. "
                                "Falling back cleanly to AppleScript text dispatch."
                            )
                            self._private_api_supported = False
                            fallback_payload = {
                                "chatGuid": chat_guid,
                                "tempGuid": f"temp-{uuid.uuid4().hex[:12]}",
                                "message": chunk,
                                "method": "apple-script"
                            }
                            try:
                                async with session.post(self._api_url("/api/v1/message/text"), json=fallback_payload, timeout=timeout) as fb_resp:
                                    if fb_resp.status != 200:
                                        fb_err = await fb_resp.text()
                                        logger.error(f"BlueBubbles fallback send failed [{fb_resp.status}]: {fb_err}")
                                        return {"status": fb_resp.status, "message": fb_err, "success": False}
                                    last_res = await fb_resp.json()
                                    if isinstance(last_res, dict):
                                        chunk_guid = last_res.get("data", {}).get("guid") or last_res.get("guid") or fallback_payload["tempGuid"]
                                        if not primary_guid:
                                            primary_guid = chunk_guid
                                        last_res["guid"] = primary_guid
                                    continue
                            except Exception as fb_exc:
                                logger.error(f"Error in AppleScript fallback post: {fb_exc}")
                                return {"status": 500, "message": str(fb_exc), "success": False}

                        logger.error(f"BlueBubbles send failed [{resp.status}]: {err_body}")
                        try:
                            parsed = json.loads(err_body)
                            if isinstance(parsed, dict):
                                last_res = parsed
                                if "status" not in last_res:
                                    last_res["status"] = resp.status
                            else:
                                last_res = {"status": resp.status, "message": str(parsed), "success": False}
                        except Exception:
                            last_res = {"status": resp.status, "message": err_body, "success": False}
                        return last_res

                    last_res = await resp.json()
                    if isinstance(last_res, dict):
                        chunk_guid = last_res.get("data", {}).get("guid") or last_res.get("guid") or payload["tempGuid"]
                        if not primary_guid:
                            primary_guid = chunk_guid
                        last_res["guid"] = primary_guid
            except Exception as e:
                logger.error(f"Error posting iMessage chunk: {e}")
                return {"status": 500, "message": str(e), "success": False}

        if isinstance(last_res, dict) and primary_guid:
            last_res["guid"] = primary_guid
            if "data" in last_res and isinstance(last_res["data"], dict) and not last_res["data"].get("guid"):
                last_res["data"]["guid"] = primary_guid
        return last_res

    async def send_imessage_attachment(
        self,
        chat_guid: str,
        file_path: str,
        filename: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send a file attachment via BlueBubbles multipart upload."""
        if not os.path.isfile(file_path):
            return {"status": 404, "message": f"File not found: {file_path}", "success": False}

        fname = filename or os.path.basename(file_path)
        try:
            with open(file_path, "rb") as f:
                file_bytes = f.read()

            session = await self._get_session()
            form = aiohttp.FormData()
            form.add_field("chatGuid", chat_guid)
            form.add_field("name", fname)
            form.add_field("tempGuid", uuid.uuid4().hex)

            content_type = mimetypes.guess_type(fname)[0] or "application/octet-stream"
            form.add_field("attachment", file_bytes, filename=fname, content_type=content_type)

            use_private_api = await self.is_private_api_available()
            if use_private_api:
                form.add_field("method", "private-api")

            async with session.post(self._api_url("/api/v1/message/attachment"), data=form) as resp:
                res = await resp.json()
                if resp.status == 200:
                    logger.info(f"Successfully uploaded attachment '{fname}' to {chat_guid}")
                else:
                    logger.warning(f"Attachment upload returned status {resp.status}: {res}")
                return res
        except Exception as e:
            logger.error(f"Failed to upload attachment {file_path}: {e}")
            return {"status": 500, "message": str(e), "success": False}

    async def download_attachment(self, attachment_guid: str) -> Optional[bytes]:
        """Download an attachment's raw bytes from BlueBubbles server."""
        if not attachment_guid:
            return None
        try:
            session = await self._get_session()
            url = self._api_url(f"/api/v1/attachment/{quote(attachment_guid, safe='')}/download")
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.read()
                logger.warning(f"Attachment download for {attachment_guid} returned status {resp.status}")
                return None
        except Exception as e:
            logger.error(f"Error downloading BlueBubbles attachment {attachment_guid}: {e}")
            return None

    # --------------------------------------------------------------------------
    # Inbound Webhook Listener
    # --------------------------------------------------------------------------
    def _extract_payload_record(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        if isinstance(payload.get("message"), dict):
            return payload.get("message")
        return payload if isinstance(payload, dict) else {}

    def _resolve_sender(self, record: Dict[str, Any], payload: Dict[str, Any]) -> str:
        handle = record.get("handle") or payload.get("data", {}).get("handle")
        if isinstance(handle, dict) and handle.get("address"):
            return str(handle["address"]).strip()
        for key in ("sender", "from", "address"):
            val = record.get(key) or payload.get(key)
            if val and isinstance(val, str) and val.strip():
                return val.strip()
        return ""

    def _resolve_chat_guid(self, record: Dict[str, Any], payload: Dict[str, Any], sender: str) -> str:
        # 1. Check explicit chatGuid or chat_guid (NEVER plain 'guid' which is the message UUID)
        for key in ("chatGuid", "chat_guid"):
            val = record.get(key) or payload.get(key)
            if val and isinstance(val, str) and val.strip():
                return val.strip()

        # 2. Check nested chats list in record or payload data (BlueBubbles v1.9+)
        chats = (
            record.get("chats")
            or payload.get("chats")
            or payload.get("data", {}).get("chats")
        )
        if isinstance(chats, list) and chats and isinstance(chats[0], dict):
            guid = chats[0].get("guid") or chats[0].get("chatGuid")
            if guid and isinstance(guid, str) and guid.strip():
                return str(guid).strip()

        # 3. Fallback to direct handle GUID (iMessage;-;{sender})
        if sender:
            if sender.startswith("iMessage;-;") or sender.startswith("SMS;-;"):
                return sender
            return f"iMessage;-;{sender}"

        return ""

    async def _handle_webhook(self, request: web.Request) -> web.Response:
        # 1. Authorization verification
        auth_token = request.query.get("password") or request.headers.get("x-password")
        if auth_token != self.password:
            return web.json_response({"error": "unauthorized"}, status=401)

        # 2. Decode JSON body
        try:
            payload = await request.json()
        except Exception as e:
            logger.warning(f"Invalid webhook JSON body: {e}")
            return web.json_response({"error": "invalid json"}, status=400)

        # 3. Check event type
        event_type = payload.get("type") or payload.get("event") or ""
        if event_type and event_type not in MESSAGE_EVENTS:
            return web.json_response({"status": "ignored", "event": event_type})

        record = self._extract_payload_record(payload)

        # 4. Ignore messages sent from this Mac
        if record.get("isFromMe") or record.get("fromMe") or record.get("is_from_me"):
            return web.json_response({"status": "ignored", "reason": "is_from_me"})

        # 5. Ignore tapbacks (love, like, laugh, emphasize, question)
        assoc_type = record.get("associatedMessageType")
        if isinstance(assoc_type, int) and assoc_type in TAPBACK_CODES:
            return web.json_response({"status": "ignored", "reason": "tapback"})

        sender = self._resolve_sender(record, payload)
        chat_guid = self._resolve_chat_guid(record, payload, sender)

        # 6. Whitelist verification
        if self.allowed_handles:
            sender_norm = sender.lower() if sender else ""
            if not sender_norm or sender_norm not in self.allowed_handles:
                self._reload_allowed_handles_from_env()
                if not sender_norm or sender_norm not in self.allowed_handles:
                    logger.warning(f"Unauthorized BlueBubbles sender '{sender}' rejected (allowed: {self.allowed_handles})")
                    return web.json_response({"status": "ignored", "reason": "unauthorized_sender"}, status=403)


        # 7. Extract message text
        text = record.get("text") or record.get("message") or record.get("body") or ""
        if not text and record.get("attachments"):
            text = "(attachment)"

        text = text.strip()
        if not text or not chat_guid:
            return web.json_response({"error": "missing text or chatGuid"}, status=400)

        message_guid = str(record.get("guid") or payload.get("guid") or "").strip()
        if message_guid:
            now = time.time()
            expired = [g for g, t in self._seen_guids.items() if now - t > 120]
            for g in expired:
                self._seen_guids.pop(g, None)
            if message_guid in self._seen_guids:
                logger.info(f"Ignoring duplicate BlueBubbles webhook for message_guid: {message_guid}")
                return web.json_response({"status": "ignored", "reason": "duplicate_message_guid"})
            self._seen_guids[message_guid] = now

        # Ingress Acknowledgment: Immediately mark chat as read (<200ms)
        read_task = asyncio.create_task(self.set_chat_read(chat_guid))
        self._background_tasks.add(read_task)
        read_task.add_done_callback(self._background_tasks.discard)

        # Trigger immediate tapback (emphasize / !!) and typing indicator
        if message_guid:
            react_task = asyncio.create_task(self.send_reaction(chat_guid, message_guid, "emphasize"))
            self._background_tasks.add(react_task)
            react_task.add_done_callback(self._background_tasks.discard)

            typing_task = asyncio.create_task(self.set_typing_indicator(chat_guid, True))
            self._background_tasks.add(typing_task)
            typing_task.add_done_callback(self._background_tasks.discard)

        # 8. Record in client history
        client = self.get_client(chat_guid)
        now_ts = str(time.time())
        ts_val = message_guid or now_ts
        thread_originator_guid = str(
            record.get("threadOriginatorGuid")
            or record.get("thread_originator_guid")
            or payload.get("threadOriginatorGuid")
            or ""
        ).strip()
        client.record_incoming(
            user=sender,
            text=text,
            ts=ts_val,
            message_guid=message_guid,
            thread_originator_guid=thread_originator_guid or None,
        )

        # 9. Dispatch to message handler
        if self.message_handler:
            task = asyncio.create_task(self.message_handler(chat_guid, text, sender, payload))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

        return web.json_response({"status": "ok"})

    async def start(self) -> None:
        """Start the inbound aiohttp webhook server and register with BlueBubbles."""
        app = web.Application()
        app.router.add_get("/health", lambda _: web.Response(text="ok"))
        app.router.add_post(self.webhook_path, self._handle_webhook)

        self._app_runner = web.AppRunner(app)
        await self._app_runner.setup()
        site = web.TCPSite(self._app_runner, self.webhook_host, self.webhook_port)
        await site.start()
        logger.info(f"BlueBubbles webhook server listening on http://{self.webhook_host}:{self.webhook_port}{self.webhook_path}")

        # Attempt auto-registration with BlueBubbles server
        asyncio.create_task(self.register_webhook())

    async def stop(self) -> None:
        """Deregister webhook, shut down webhook server, and close HTTP session."""
        self._closing = True
        for task in list(self._background_tasks):
            if not task.done():
                task.cancel()
        if self._background_tasks:
            try:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
            except Exception:
                pass
        self._background_tasks.clear()

        if self._app_runner:
            try:
                await self.unregister_webhook()
            except Exception as e:
                logger.debug(f"Error unregistering webhook: {e}")

            await self._app_runner.cleanup()
            self._app_runner = None

        if self._session and not getattr(self._session, "closed", False):
            try:
                res = self._session.close()
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
            self._session = None
