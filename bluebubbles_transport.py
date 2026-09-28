"""
BlueBubbles iMessage Transport Adapter for AGY Chief of Staff.
Provides an embedded aiohttp webhook listener on 0.0.0.0:8646 and an outbound client
for sending iMessages and attachments via the local BlueBubbles server.
"""

import asyncio
import json
import logging
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


class BlueBubblesClient:
    """
    Slack-compatible WebClient shim for iMessage chat sessions.
    Allows AGY's command handlers, preflight alignment engine, and Herdr execution
    to operate transparently over iMessage without code changes.
    """

    def __init__(self, transport: "BlueBubblesTransport", chat_guid: str):
        self.transport = transport
        self.chat_guid = chat_guid
        self.history: List[Dict[str, Any]] = []

    def record_incoming(self, user: str, text: str, ts: str):
        self.history.append({
            "user": user,
            "text": text,
            "ts": ts
        })
        if len(self.history) > 100:
            self.history.pop(0)

    async def chat_postMessage(
        self,
        channel: str,
        text: str = "",
        thread_ts: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        target = channel or self.chat_guid
        now_ts = str(time.time())
        self.history.append({
            "bot_id": "B_AGY",
            "text": text,
            "ts": now_ts
        })
        if len(self.history) > 100:
            self.history.pop(0)

        res = await self.transport.send_imessage(target, text)
        return {
            "ok": res.get("status") == 200 or res.get("success", False),
            "ts": now_ts,
            "channel": target,
            "response": res
        }

    async def reactions_add(
        self,
        channel: str,
        name: str,
        timestamp: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        # iMessage lacks custom emoji reactions via standard API
        logger.debug(f"[iMessage Reaction Add] :{name}: on {channel} ({timestamp})")
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
            res = await self.transport.send_imessage_attachment(target, file, filename=filename)
            return {"ok": res.get("status") == 200 or res.get("success", False), "file": {"name": filename}}

        if content is not None:
            safe_name = filename or f"snippet_{int(time.time())}.txt"
            with tempfile.NamedTemporaryFile("w", suffix=f"_{safe_name}", delete=False) as tf:
                tf.write(content)
                tmp_path = tf.name
            try:
                res = await self.transport.send_imessage_attachment(target, tmp_path, filename=safe_name)
                return {"ok": res.get("status") == 200 or res.get("success", False), "file": {"name": safe_name}}
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        return {"ok": False, "error": "No file or content provided"}

    async def conversations_replies(
        self,
        channel: str,
        ts: Optional[str] = None,
        limit: int = 100,
        **kwargs
    ) -> Dict[str, Any]:
        return {"ok": True, "messages": list(self.history[-limit:])}

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
        raw_url = (server_url or os.getenv("BLUEBUBBLES_URL", "http://127.0.0.1:1234")).strip()
        if raw_url and not re.match(r"^https?://", raw_url, flags=re.IGNORECASE):
            raw_url = f"http://{raw_url}"
        self.server_url = raw_url.rstrip("/")
        self.password = password or resolve_bluebubbles_password()
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

    def _api_url(self, path: str) -> str:
        sep = "&" if "?" in path else "?"
        return f"{self.server_url}{path}{sep}password={quote(self.password, safe='')}"

    @property
    def webhook_local_url(self) -> str:
        return f"http://127.0.0.1:{self.webhook_port}{self.webhook_path}?password={quote(self.password, safe='')}"

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
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

    async def send_imessage(self, chat_guid: str, text: str) -> Dict[str, Any]:
        """
        Send an outbound iMessage to a chat GUID, automatically chunking
        messages exceeding MAX_IMESSAGE_CHUNK (3,500 characters).
        """
        if not text:
            return {"status": 400, "message": "Text cannot be empty"}

        session = await self._get_session()
        chunks = chunk_text(text, max_chars=MAX_IMESSAGE_CHUNK)
        last_res = {"status": 200, "message": "Success"}

        for chunk in chunks:
            payload = {
                "chatGuid": chat_guid,
                "tempGuid": f"temp-{uuid.uuid4().hex[:12]}",
                "message": chunk,
                "method": "apple-script"
            }
            try:
                async with session.post(self._api_url("/api/v1/message/text"), json=payload) as resp:
                    if resp.status != 200:
                        err_body = await resp.text()
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
            except Exception as e:
                logger.error(f"Error posting iMessage chunk: {e}")
                return {"status": 500, "message": str(e), "success": False}

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
            form.add_field("attachment", file_bytes, filename=fname, content_type="application/octet-stream")

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
            if not sender or sender.lower() not in self.allowed_handles:
                logger.warning(f"Unauthorized BlueBubbles sender '{sender}' rejected (allowed: {self.allowed_handles})")
                return web.json_response({"status": "ignored", "reason": "unauthorized_sender"}, status=403)


        # 7. Extract message text
        text = record.get("text") or record.get("message") or record.get("body") or ""
        if not text and record.get("attachments"):
            text = "(attachment)"

        text = text.strip()
        if not text or not chat_guid:
            return web.json_response({"error": "missing text or chatGuid"}, status=400)

        # 8. Record in client history
        client = self.get_client(chat_guid)
        client.record_incoming(user=sender, text=text, ts=str(time.time()))

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
        try:
            await self.unregister_webhook()
        except Exception as e:
            logger.debug(f"Error unregistering webhook: {e}")

        if self._app_runner:
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
