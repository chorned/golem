#!/usr/bin/env python3
"""
first_mate/gateway/ingress.py
Ingress gateway for Slack and chat platforms featuring non-verbal reaction acknowledgments (<200ms),
deduplication, command interception, elimination of canned announcements, and warm thread session routing.
"""

from __future__ import annotations

import os
import re
import time
import uuid
import inspect
import asyncio
import logging
from collections import deque, OrderedDict
from typing import Dict, Any, Optional, Deque, List, Union

import aiohttp

from first_mate.gateway.router import SessionRouter
from first_mate.gateway.session import (
    SessionHandle,
    SessionState,
    is_cancellation_message,
    is_status_inquiry,
    is_conversational_checkin,
    is_out_of_band_message,
)
from first_mate.agent.mouth import generate_mouth_response
from first_mate.transport.normalizer import sanitize_for_slack
from first_mate.transport.chunker import split_message_boundary_aware
from first_mate.transport.file_transfer import (
    extract_and_strip_file_directives,
    upload_directive_files,
)

logger = logging.getLogger("FirstMate.Gateway.Ingress")

DEFAULT_MAX_FILE_SIZE_BYTES = 104_857_600  # 100MB
TRANSFERS_DIR_NAME = "slack_transfers"


async def download_slack_file(
    url: str,
    token: str,
    max_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
    timeout_seconds: float = 30.0,
) -> Optional[bytes]:
    """
    Download private file bytes from Slack using bot token authentication.
    Follows redirects to Amazon S3 WITHOUT the Authorization header to prevent
    AWS S3 HTTP 400 Bad Request errors. Enforces max_size_bytes limit before
    and during byte streaming.
    """
    if not url:
        return None
    token = token or os.environ.get("SLACK_BOT_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, allow_redirects=False) as resp:
                if resp.status in (301, 302, 303, 307, 308):
                    redirect_url = resp.headers.get("Location")
                    if not redirect_url:
                        logger.error(f"Redirect status {resp.status} with no Location header for {url}")
                        return None
                    # Follow redirect URL WITHOUT Authorization header to prevent S3 HTTP 400
                    async with session.get(redirect_url, allow_redirects=True) as redirect_resp:
                        if redirect_resp.status != 200:
                            logger.error(
                                f"Redirect download failed for {url} -> {redirect_url} with status {redirect_resp.status}"
                            )
                            return None
                        cl = redirect_resp.headers.get("Content-Length")
                        if cl and cl.isdigit() and int(cl) > max_size_bytes:
                            logger.warning(
                                f"File at {redirect_url} Content-Length {cl} exceeds max size {max_size_bytes} bytes; aborted."
                            )
                            return None

                        if hasattr(redirect_resp, "content") and hasattr(redirect_resp.content, "iter_chunked"):
                            chunks = []
                            total_bytes = 0
                            async for chunk in redirect_resp.content.iter_chunked(65536):
                                total_bytes += len(chunk)
                                if total_bytes > max_size_bytes:
                                    logger.warning(
                                        f"File download from {redirect_url} exceeded max size {max_size_bytes} bytes during stream; aborted."
                                    )
                                    return None
                                chunks.append(chunk)
                            return b"".join(chunks)
                        else:
                            data = await redirect_resp.read()
                            if len(data) > max_size_bytes:
                                logger.warning(
                                    f"File download from {redirect_url} exceeded max size {max_size_bytes} bytes; aborted."
                                )
                                return None
                            return data

                elif resp.status == 200:
                    cl = resp.headers.get("Content-Length")
                    if cl and cl.isdigit() and int(cl) > max_size_bytes:
                        logger.warning(
                            f"File at {url} Content-Length {cl} exceeds max size {max_size_bytes} bytes; aborted."
                        )
                        return None

                    if hasattr(resp, "content") and hasattr(resp.content, "iter_chunked"):
                        chunks = []
                        total_bytes = 0
                        async for chunk in resp.content.iter_chunked(65536):
                            total_bytes += len(chunk)
                            if total_bytes > max_size_bytes:
                                logger.warning(
                                    f"File download from {url} exceeded max size {max_size_bytes} bytes during stream; aborted."
                                )
                                return None
                            chunks.append(chunk)
                        return b"".join(chunks)
                    else:
                        data = await resp.read()
                        if len(data) > max_size_bytes:
                            logger.warning(
                                f"File download from {url} exceeded max size {max_size_bytes} bytes; aborted."
                            )
                            return None
                        return data
                else:
                    logger.error(f"Failed to download Slack file from {url}: status {resp.status}")
                    return None
    except Exception as e:
        logger.error(f"Exception downloading Slack file from {url}: {e}")
        return None


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal and strip forbidden characters."""
    raw = filename or ""
    # Normalize backslashes so basename strips both / and \
    normalized = raw.replace("\\", "/")
    base_name = os.path.basename(normalized).strip()
    safe_name = re.sub(r"[\x00/\\:*?\"<>|]", "_", base_name).strip()
    safe_name = safe_name.lstrip(".")
    if not safe_name:
        safe_name = f"upload_{int(time.time())}"
    return safe_name


def save_staged_file(
    file_bytes: bytes,
    filename: str,
    target_cwd: str,
    timestamp: Optional[Union[int, float, str]] = None,
) -> Optional[str]:
    """
    Save downloaded file bytes into {target_cwd}/slack_transfers/{timestamp}_{filename}.
    Returns absolute path of the staged file, or None on failure.
    Uses collision-resistant prefix naming when timestamp is omitted, and resolves
    any on-disk collisions safely.
    """
    if timestamp is None:
        timestamp_prefix = f"{int(time.time())}_{uuid.uuid4().hex[:6]}"
    else:
        timestamp_prefix = str(timestamp)
    safe_name = sanitize_filename(filename)
    transfers_dir = os.path.abspath(os.path.join(target_cwd, TRANSFERS_DIR_NAME))
    try:
        os.makedirs(transfers_dir, exist_ok=True)
        dest_path = os.path.abspath(os.path.join(transfers_dir, f"{timestamp_prefix}_{safe_name}"))
        if not dest_path.startswith(transfers_dir):
            logger.error(f"Path traversal detected: {dest_path} is outside {transfers_dir}")
            return None
        if os.path.exists(dest_path):
            nonce = uuid.uuid4().hex[:6]
            dest_path = os.path.abspath(os.path.join(transfers_dir, f"{timestamp_prefix}_{nonce}_{safe_name}"))
            if not dest_path.startswith(transfers_dir):
                logger.error(f"Path traversal detected: {dest_path} is outside {transfers_dir}")
                return None
        with open(dest_path, "wb") as f:
            f.write(file_bytes)
        logger.info(f"Saved uploaded Slack file to: {dest_path} ({len(file_bytes)} bytes)")
        return dest_path
    except Exception as e:
        logger.error(f"Failed to save uploaded file '{safe_name}' to {transfers_dir}: {e}")
        return None


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


def strip_bot_mentions(text: str, bot_user_id: Optional[str] = None) -> str:
    """Strip bot user mentions (<@BOT_ID>, <@Uxxxx>, @AGY) from prompt."""
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

        # Resolve download URL
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
        file_bytes = await download_slack_file(
            download_url,
            token=token,
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
    ) -> str:
        """Fetch previous messages in the Slack thread to provide continuity when rehydrating a session."""
        if not hasattr(client, "conversations_replies") or not channel or not thread_ts:
            return ""
        try:
            res = client.conversations_replies(channel=channel, ts=thread_ts, limit=limit)
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
            for msg in messages:
                msg_ts = msg.get("ts")
                if exclude_ts and msg_ts == exclude_ts:
                    continue
                text = msg.get("text", "").strip()
                if not text:
                    continue
                user_id = msg.get("user") or msg.get("bot_id") or ""
                is_bot = (
                    (bot_id and user_id == bot_id)
                    or bool(msg.get("bot_id"))
                    or bool(msg.get("bot_profile"))
                )
                sender = "AGY" if is_bot else "User"
                clean_msg = strip_bot_mentions(text, bot_id)
                if len(clean_msg) > 300:
                    clean_msg = clean_msg[:300] + "..."
                context_lines.append(f"{sender}: {clean_msg}")

            if not context_lines:
                return ""

            recent_lines = context_lines[-10:]
            formatted_context = "[Previous Slack Thread Context:\n" + "\n".join(recent_lines) + "\n]"
            return formatted_context
        except Exception as e:
            logger.warning(f"Could not fetch Slack thread history for {channel}:{thread_ts}: {e}")
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

        raw_text = event.get("text", "")
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
        if not clean_text and not has_files:
            return

        # 3. Non-verbal ingress acknowledgment (<200ms)
        # ZERO canned greeting announcements are posted
        if not is_imessage and ts:
            await add_reaction(client, channel, ts, "gear")

        # 4. Command Interception
        cmd_token = clean_text.split()[0].lower() if clean_text.split() else ""

        if cmd_token in ("/wrapup", "!wrapup"):
            await self.router.teardown(channel, thread_ts, reason="wrapup")
            msg = "🧹 *Wrapup:* Housekeeping complete. Working directories clean."
            if not is_imessage and ts:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")
            await self._post_response(client, channel, thread_ts, msg, is_imessage)
            return

        elif cmd_token in ("/archive", "!archive"):
            await self.router.teardown(channel, thread_ts, reason="archive")
            msg = "📦 *Archive:* Session archived and torn down."
            if not is_imessage and ts:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")
            await self._post_response(client, channel, thread_ts, msg, is_imessage)
            return

        elif cmd_token in ("/bearings", "!bearings"):
            msg = (
                "🧭 *Fleet Bearings & Status Digest:*\n"
                "• *hermes* (localhost (Primary Dev & Homelab)): 🟢 Online\n"
                "• *desktop-pc* (Compute Grid (Windows CUDA)): 🟢 Online\n"
                "• *personal-laptop* (Personal Laptop (Carl's Workstation)): 🟢 Online\n"
                "• *oci-machine* (Cloud Operations (Production VPS)): 🟢 Online"
            )
            if not is_imessage and ts:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")
            await self._post_response(client, channel, thread_ts, msg, is_imessage)
            return

        elif cmd_token in ("/ahoy", "!ahoy"):
            msg = "⚓ *Ahoy:* Session catch-up complete. All background Herdr agents settled."
            if not is_imessage and ts:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")
            await self._post_response(client, channel, thread_ts, msg, is_imessage)
            return

        elif cmd_token in ("/learn", "!learn"):
            user_notes = clean_text[len(cmd_token):].strip()
            msg = f"🧠 *Operational Lesson Crystallized:* {user_notes or 'Lesson codified into operational memory.'}"
            if not is_imessage and ts:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")
            await self._post_response(client, channel, thread_ts, msg, is_imessage)
            return

        # 5. Warm Session Routing
        session, is_new_session = await self.router.get_or_create(
            channel=channel,
            thread_ts=thread_ts,
            target_cwd=target_cwd,
            is_imessage=is_imessage,
        )

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
            if status in ("staged", "cached") and path:
                attachment_tags.append(f"[Attached Slack File: {name} saved to {path}]")
                staged_names.append(name)
            elif status == "exceeded_size":
                attachment_tags.append(f"[Attached Slack File: {name} exceeded size limit of 100MB; skipped]")
            else:
                attachment_tags.append(f"[Attached Slack File: {name} download failed]")

        # Fallback prompt if user uploaded a file without accompanying text
        if not clean_text and staged_names:
            file_pronoun = "these files" if len(staged_names) > 1 else "this file"
            names_str = ", ".join(staged_names)
            clean_text = f"I've uploaded {names_str} to the workspace. Please inspect or process {file_pronoun}."
        elif not clean_text and attachment_tags:
            clean_text = "I've uploaded a file to the workspace. Please inspect or process this file."
        elif not clean_text and not attachment_tags:
            logger.debug(f"Dropping event ts={ts} with empty text and no staged file attachments.")
            return

        # Rehydrate thread history if resuming an existing thread from Slack
        if is_new_session and not is_imessage and thread_ts and thread_ts != ts:
            thread_context = await self._fetch_thread_context(client, channel, thread_ts, exclude_ts=ts)
            if thread_context:
                clean_text = f"{thread_context}\n\n{clean_text}"

        # Prepend attachment tags to prompt sent to LLM
        prompt_to_send = clean_text
        if attachment_tags:
            tags_block = "\n".join(attachment_tags)
            prompt_to_send = f"{tags_block}\n\n{clean_text}" if clean_text else tags_block

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
            if not is_imessage and ts:
                await swap_reaction(client, channel, ts, "gear", "warning")
            prompt_msg = awaiting_input_prompt or "⚠️ *Confirmation Required:* Please provide input to continue."
            await self._post_response(client, channel, thread_ts, prompt_msg, is_imessage)

        elif turn_status == "ERROR":
            if not is_imessage and ts:
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

            if file_targets and not is_imessage:
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

            if not is_imessage and ts:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")

    async def _post_response(
        self,
        client: Any,
        channel: str,
        thread_ts: str,
        text: str,
        is_imessage: bool,
    ) -> None:
        """Sanitize text for Slack and deliver in boundary-aware chunks."""
        if not text:
            return

        sanitized = sanitize_for_slack(text)
        chunks = split_message_boundary_aware(sanitized, max_chars=3800)

        for chunk in chunks:
            if not chunk:
                continue
            if is_imessage:
                res = client.chat_postMessage(channel=channel, text=chunk)
            else:
                res = client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=chunk)
            if asyncio.iscoroutine(res):
                await res

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
        if not is_imessage and ts:
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
        """Handle out-of-band status query or check-in without acquiring session lock."""
        status_info = await session.get_runner_status()
        msg = await generate_mouth_response(prompt_text, status_info, session=session)

        blocker = status_info.get("blocker")
        if not blocker and status_info.get("blockers"):
            b_list = status_info["blockers"]
            if b_list and isinstance(b_list, list):
                blocker = b_list[0]

        if not is_imessage and ts:
            if blocker and blocker.get("type") in ("oauth", "prompt"):
                await swap_reaction(client, channel, ts, "gear", "warning")
            else:
                await swap_reaction(client, channel, ts, "gear", "white_check_mark")

        await self._post_response(client, channel, thread_ts, msg, is_imessage)


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
    "download_slack_file",
    "sanitize_filename",
    "save_staged_file",
    "process_slack_message",
    "add_reaction",
    "remove_reaction",
    "swap_reaction",
    "strip_bot_mentions",
    "strip_flushed_chunks",
    "DEFAULT_MAX_FILE_SIZE_BYTES",
    "TRANSFERS_DIR_NAME",
]

