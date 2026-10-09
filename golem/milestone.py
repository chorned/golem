r"""
golem/transport/milestone.py
In-place milestone updater for Slack with token-bucket debounce and clean finalization.

Features:
- Single evolving status message per thread via chat_update.
- Token-bucket debounce (default 1.0s interval) prevents rate limits on rapid progress updates.
- Per-thread card isolation: maintains independent message tracking across concurrent threads.
- Graceful fallback: handles message_not_found or cant_update_message by falling back to chat_postMessage.
- Clean finalization: resolves milestone card into the final response and emits overflow chunks as needed.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from golem.chunker import split_message_boundary_aware

logger = logging.getLogger("Golem.Transport.Milestone")


@dataclass
class MilestoneCard:
    channel: str
    thread_ts: str
    message_ts: Optional[str] = None
    last_update_time: float = 0.0
    tokens: float = 1.0
    pending_text: Optional[str] = None
    debounce_task: Optional[asyncio.Task] = None
    is_finalized: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class MilestoneUpdater:
    """
    Debounced Slack milestone updater with token-bucket rate limiting,
    in-place chat_update, and clean finalization into the final response.
    Supports per-thread milestone card isolation.
    """

    def __init__(
        self,
        client: Any,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        min_interval: float = 1.0,
        max_chars: int = 3800,
        debounce_interval: Optional[float] = None,
        min_interval_seconds: Optional[float] = None,
        **kwargs: Any
    ) -> None:
        self.client = client
        self.channel = channel
        self.thread_ts = thread_ts

        interval = min_interval
        if min_interval_seconds is not None:
            interval = min_interval_seconds
        elif debounce_interval is not None:
            interval = debounce_interval
        self.min_interval = interval
        self.max_chars = max_chars

        self._cards: Dict[Tuple[str, str], MilestoneCard] = {}
        self._last_key: Optional[Tuple[str, str]] = None
        self._lock = asyncio.Lock()

        # If initial channel and thread_ts are given, pre-seed default key
        if channel and thread_ts:
            self._last_key = (channel, thread_ts)

    @property
    def message_ts(self) -> Optional[str]:
        if self._last_key and self._last_key in self._cards:
            return self._cards[self._last_key].message_ts
        return None

    @message_ts.setter
    def message_ts(self, val: Optional[str]) -> None:
        if self._last_key:
            if self._last_key not in self._cards:
                self._cards[self._last_key] = MilestoneCard(
                    channel=self._last_key[0],
                    thread_ts=self._last_key[1]
                )
            self._cards[self._last_key].message_ts = val

    @property
    def is_finalized(self) -> bool:
        if self._last_key and self._last_key in self._cards:
            return self._cards[self._last_key].is_finalized
        return False

    @is_finalized.setter
    def is_finalized(self, val: bool) -> None:
        if self._last_key and self._last_key in self._cards:
            self._cards[self._last_key].is_finalized = val

    def _get_card(self, channel: str, thread_ts: str) -> MilestoneCard:
        key = (channel, thread_ts)
        self._last_key = key
        if key not in self._cards:
            self._cards[key] = MilestoneCard(channel=channel, thread_ts=thread_ts)
        return self._cards[key]

    def _replenish_tokens(self, card: MilestoneCard) -> None:
        """Replenish tokens based on elapsed time without quadratic accumulation."""
        now = time.time()
        if self.min_interval <= 0:
            card.tokens = 1.0
            return
        elapsed = now - card.last_update_time
        card.tokens = min(1.0, elapsed / self.min_interval)

    async def post_or_update(
        self,
        channel: str,
        thread_ts: str,
        text: str
    ) -> str:
        """
        Post a new milestone or update the existing milestone card in-place.
        Throttled by token bucket debounce (min_interval >= 1.0s) with per-card locking.
        """
        card = self._get_card(channel, thread_ts)
        async with card.lock:
            if card.is_finalized:
                logger.warning("Attempted to update milestone after finalization.")
                return card.message_ts or ""

            # Case 1: First milestone posting in this thread
            if card.message_ts is None:
                return await self._post_initial(channel, thread_ts, text, card)

            # Case 2: Subsequent updates - check token bucket
            self._replenish_tokens(card)

            if card.tokens >= 1.0:
                # Immediate dispatch
                return await self._dispatch_update(card, text)
            else:
                # Debounce trailing update
                card.pending_text = text
                if card.debounce_task is None or card.debounce_task.done():
                    wait_time = (1.0 - card.tokens) * self.min_interval
                    card.debounce_task = asyncio.create_task(self._debounce_worker(card, wait_time))
                return card.message_ts or ""

    async def _post_initial(self, channel: str, thread_ts: str, text: str, card: MilestoneCard) -> str:
        """Send the initial milestone message via chat_postMessage."""
        try:
            if hasattr(self.client, "chat_postMessage"):
                resp = await self.client.chat_postMessage(
                    channel=channel,
                    thread_ts=thread_ts,
                    text=text
                )
                if isinstance(resp, dict):
                    card.message_ts = resp.get("ts")
                else:
                    card.message_ts = getattr(resp, "ts", None)
            card.last_update_time = time.time()
            card.tokens = 0.0
        except Exception as e:
            logger.warning(f"Failed to post initial milestone: {e}")
        return card.message_ts or ""

    async def _dispatch_update(self, card: MilestoneCard, text: str) -> str:
        """Dispatch chat_update to Slack with graceful message_not_found fallback."""
        if not hasattr(self.client, "chat_update") or not card.message_ts:
            return card.message_ts or ""

        try:
            resp = await self.client.chat_update(
                channel=card.channel,
                ts=card.message_ts,
                text=text
            )
            if resp is None:
                logger.warning(f"Milestone message {card.message_ts} not found/updatable (returned None). Re-posting fresh message.")
                card.message_ts = None
                return await self._post_initial(card.channel, card.thread_ts, text, card)
            card.last_update_time = time.time()
            card.tokens = 0.0
            card.pending_text = None
            return card.message_ts or ""
        except Exception as e:
            err_str = str(e).lower()
            if "message_not_found" in err_str or "cant_update_message" in err_str:
                logger.warning(f"Milestone message {card.message_ts} not found/updatable. Re-posting fresh message.")
                card.message_ts = None
                return await self._post_initial(card.channel, card.thread_ts, text, card)
            elif "rate_limited" in err_str:
                logger.warning("Slack rate-limit encountered on chat_update. Backing off.")
                card.pending_text = text
                if card.debounce_task is None or card.debounce_task.done():
                    card.debounce_task = asyncio.create_task(self._debounce_worker(card, self.min_interval))
            else:
                logger.debug(f"Slack chat_update error: {e}")

        return card.message_ts or ""

    async def _debounce_worker(self, card: MilestoneCard, delay: float) -> None:
        """Wait for debounce window to expire and flush latest pending_text."""
        try:
            await asyncio.sleep(delay)
            async with card.lock:
                if card.pending_text and not card.is_finalized and card.channel:
                    await self._dispatch_update(card, card.pending_text)
        except asyncio.CancelledError:
            pass

    async def finalize(
        self,
        channel: str,
        thread_ts: str,
        final_text: str
    ) -> None:
        """
        Finalize execution by updating the milestone card into the final response.
        If final_text exceeds max_chars, Chunk 1 updates the card and Chunks 2..N
        are posted as subsequent threaded replies. Uses per-card locking and handles
        message_not_found fallback when chat_update returns None.
        """
        card = self._get_card(channel, thread_ts)
        async with card.lock:
            card.is_finalized = True

            # Cancel any pending debounce task immediately
            if card.debounce_task and not card.debounce_task.done():
                card.debounce_task.cancel()
                card.debounce_task = None
            card.pending_text = None

            # Split message if it exceeds boundary limit
            chunks = split_message_boundary_aware(final_text, max_chars=self.max_chars)
            if not chunks:
                return

            lead_chunk = chunks[0]
            overflow_chunks = chunks[1:]

            # Resolve the in-place milestone message
            if card.message_ts and hasattr(self.client, "chat_update"):
                try:
                    resp = await self.client.chat_update(
                        channel=channel,
                        ts=card.message_ts,
                        text=lead_chunk
                    )
                    if resp is None:
                        # chat_update failed with message_not_found or non-fatal status
                        if hasattr(self.client, "chat_postMessage"):
                            resp_post = await self.client.chat_postMessage(
                                channel=channel,
                                thread_ts=thread_ts,
                                text=lead_chunk
                            )
                            ts = resp_post.get("ts") if isinstance(resp_post, dict) else getattr(resp_post, "ts", None)
                            card.message_ts = ts
                except Exception as e:
                    err_str = str(e).lower()
                    if "message_not_found" in err_str or "cant_update_message" in err_str:
                        if hasattr(self.client, "chat_postMessage"):
                            resp = await self.client.chat_postMessage(
                                channel=channel,
                                thread_ts=thread_ts,
                                text=lead_chunk
                            )
                            ts = resp.get("ts") if isinstance(resp, dict) else getattr(resp, "ts", None)
                            card.message_ts = ts
                    else:
                        logger.warning(f"Error finalizing milestone update: {e}")
            else:
                if hasattr(self.client, "chat_postMessage"):
                    resp = await self.client.chat_postMessage(
                        channel=channel,
                        thread_ts=thread_ts,
                        text=lead_chunk
                    )
                    ts = resp.get("ts") if isinstance(resp, dict) else getattr(resp, "ts", None)
                    card.message_ts = ts

            # Post any overflow chunks as threaded replies
            if overflow_chunks and hasattr(self.client, "chat_postMessage"):
                for chunk in overflow_chunks:
                    try:
                        await self.client.chat_postMessage(
                            channel=channel,
                            thread_ts=thread_ts,
                            text=chunk
                        )
                    except Exception as e:
                        logger.warning(f"Error posting overflow chunk: {e}")
