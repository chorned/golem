r"""
golem/transport/slack_client.py
Asynchronous Slack API client wrapper and transport façade for First Mate 2.0.

Features:
- Resilient retry loop with exponential backoff on HTTP 429 (ratelimited) honoring Retry-After.
- Idempotent reaction operations (already_reacted, no_reaction treated as non-fatal successes).
- Graceful in-place message update handling with message_not_found recovery.
- SlackTransport façade coordinating wire normalization, message chunking, and milestone updates.
- Clean session and Socket Mode lifecycle teardown.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

from golem.transport.normalizer import sanitize_for_slack
from golem.transport.chunker import split_message_boundary_aware
from golem.transport.milestone import MilestoneUpdater

logger = logging.getLogger("Golem.Transport.Slack")


class SlackTransportError(Exception):
    """Base exception for Slack transport operations."""
    pass


class SlackRateLimitError(SlackTransportError):
    """Raised when HTTP 429 rate limit retries are exhausted."""
    def __init__(self, retry_after: float, message: str = "Slack API rate limit exhausted"):
        super().__init__(f"{message} (retry_after={retry_after:.2f}s)")
        self.retry_after = retry_after


class SlackMessageNotFoundError(SlackTransportError):
    """Raised when target message is not found for update or reaction."""
    pass


class SlackAuthenticationError(SlackTransportError):
    """Raised when authentication credentials (bot_token/app_token) are invalid."""
    pass


class SlackClient:
    """
    Asynchronous Slack API client wrapper with structured error handling,
    automatic rate-limit retry with Retry-After backoff, and clean lifecycle management.
    """

    def __init__(
        self,
        token: Optional[str] = None,
        client: Optional[AsyncWebClient] = None,
        max_retries: int = 3,
        max_backoff: float = 10.0,
    ) -> None:
        if client is not None:
            self._client = client
        elif token is not None:
            self._client = AsyncWebClient(token=token)
        else:
            self._client = AsyncWebClient()
        self.max_retries = max_retries
        self.max_backoff = max_backoff

    @property
    def web_client(self) -> AsyncWebClient:
        return self._client

    async def _execute_with_retry(self, operation: str, coro_fn, *args, **kwargs) -> Any:
        retries = 0
        while True:
            try:
                return await coro_fn(*args, **kwargs)
            except SlackApiError as err:
                err_code = ""
                status_code = getattr(err.response, "status_code", None)
                if hasattr(err, "response") and err.response is not None:
                    try:
                        err_code = err.response.get("error", "")
                    except Exception:
                        pass

                is_rate_limit = (status_code == 429) or (err_code in ("ratelimited", "rate_limited"))
                if is_rate_limit and retries < self.max_retries:
                    retries += 1
                    retry_after = 1.0
                    headers = getattr(err.response, "headers", {}) or {}
                    raw_header = headers.get("Retry-After") or headers.get("retry-after")
                    if raw_header:
                        try:
                            retry_after = float(raw_header)
                        except (ValueError, TypeError):
                            retry_after = 1.0 * (2 ** (retries - 1))
                    else:
                        retry_after = 1.0 * (2 ** (retries - 1))
                    wait_time = min(retry_after, self.max_backoff)
                    logger.warning(
                        f"Slack API [{operation}] rate limited (attempt {retries}/{self.max_retries}). "
                        f"Backing off for {wait_time:.2f}s..."
                    )
                    await asyncio.sleep(wait_time)
                    continue

                if is_rate_limit:
                    retry_after = 1.0
                    headers = getattr(err.response, "headers", {}) or {}
                    raw_header = headers.get("Retry-After") or headers.get("retry-after")
                    if raw_header:
                        try:
                            retry_after = float(raw_header)
                        except (ValueError, TypeError):
                            pass
                    raise SlackRateLimitError(retry_after=retry_after) from err

                raise

    async def chat_postMessage(
        self,
        channel: str,
        text: str = "",
        thread_ts: Optional[str] = None,
        blocks: Optional[List[Dict[str, Any]]] = None,
        sanitize: bool = False,
        **kwargs: Any
    ) -> Dict[str, Any]:
        payload_text = sanitize_for_slack(text) if sanitize else text
        call_kwargs: Dict[str, Any] = {
            "channel": channel,
            "text": payload_text,
            **kwargs
        }
        if thread_ts:
            call_kwargs["thread_ts"] = thread_ts
        if blocks is not None:
            call_kwargs["blocks"] = blocks

        resp = await self._execute_with_retry(
            "chat.postMessage",
            self._client.chat_postMessage,
            **call_kwargs
        )
        return resp.data if hasattr(resp, "data") else resp

    post_message = chat_postMessage

    async def chat_update(
        self,
        channel: str,
        ts: str,
        text: str = "",
        blocks: Optional[List[Dict[str, Any]]] = None,
        sanitize: bool = False,
        **kwargs: Any
    ) -> Optional[Dict[str, Any]]:
        payload_text = sanitize_for_slack(text) if sanitize else text
        call_kwargs: Dict[str, Any] = {
            "channel": channel,
            "ts": ts,
            "text": payload_text,
            **kwargs
        }
        if blocks is not None:
            call_kwargs["blocks"] = blocks

        try:
            resp = await self._execute_with_retry(
                "chat.update",
                self._client.chat_update,
                **call_kwargs
            )
            return resp.data if hasattr(resp, "data") else resp
        except SlackApiError as e:
            err = e.response.get("error", "") if hasattr(e, "response") and e.response else ""
            if err in ("message_not_found", "cant_update_message", "edit_window_closed", "channel_not_found"):
                logger.warning(f"chat_update failed with non-fatal status '{err}' on ts={ts}")
                return None
            logger.error(f"chat_update error on ts={ts}: {e}")
            raise

    update_message = chat_update

    async def reactions_add(
        self,
        channel: str,
        name: str,
        timestamp: str,
        **kwargs: Any
    ) -> bool:
        try:
            await self._execute_with_retry(
                "reactions.add",
                self._client.reactions_add,
                channel=channel,
                name=name,
                timestamp=timestamp,
                **kwargs
            )
            return True
        except SlackApiError as e:
            err = e.response.get("error", "") if hasattr(e, "response") and e.response else ""
            if err == "already_reacted":
                logger.debug(f"Reaction :{name}: already present on message {timestamp}")
                return True
            if err in ("message_not_found", "no_reaction"):
                logger.debug(f"Target message {timestamp} not found for reaction :{name}:")
                return False
            logger.warning(f"Failed to add reaction :{name}: on {timestamp}: {e}")
            return False
        except Exception as e:
            logger.debug(f"Unexpected error adding reaction :{name}: {e}")
            return False

    add_reaction = reactions_add

    async def reactions_remove(
        self,
        channel: str,
        name: str,
        timestamp: str,
        **kwargs: Any
    ) -> bool:
        try:
            await self._execute_with_retry(
                "reactions.remove",
                self._client.reactions_remove,
                channel=channel,
                name=name,
                timestamp=timestamp,
                **kwargs
            )
            return True
        except SlackApiError as e:
            err = e.response.get("error", "") if hasattr(e, "response") and e.response else ""
            if err == "no_reaction":
                logger.debug(f"Reaction :{name}: was not present on message {timestamp}")
                return True
            if err in ("message_not_found", "already_reacted"):
                logger.debug(f"Target message {timestamp} not found for removing reaction :{name}:")
                return False
            logger.warning(f"Failed to remove reaction :{name}: on {timestamp}: {e}")
            return False
        except Exception as e:
            logger.debug(f"Unexpected error removing reaction :{name}: {e}")
            return False

    remove_reaction = reactions_remove

    async def swap_reaction(
        self,
        channel: str,
        timestamp: str,
        old_name: Optional[str],
        new_name: str
    ) -> None:
        if old_name and old_name != new_name:
            await self.reactions_remove(channel=channel, timestamp=timestamp, name=old_name)
        await self.reactions_add(channel=channel, timestamp=timestamp, name=new_name)

    async def files_upload_v2(
        self,
        channel: str,
        file: Optional[str] = None,
        content: Optional[str] = None,
        filename: Optional[str] = None,
        title: Optional[str] = None,
        thread_ts: Optional[str] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        call_kwargs: Dict[str, Any] = {"channel": channel, **kwargs}
        if file:
            call_kwargs["file"] = file
        if content is not None:
            call_kwargs["content"] = content
        if filename:
            call_kwargs["filename"] = filename
        if title:
            call_kwargs["title"] = title
        if thread_ts:
            call_kwargs["thread_ts"] = thread_ts

        resp = await self._execute_with_retry(
            "files.upload_v2",
            self._client.files_upload_v2,
            **call_kwargs
        )
        return resp.data if hasattr(resp, "data") else resp

    upload_file = files_upload_v2

    async def auth_test(self) -> Dict[str, Any]:
        resp = await self._execute_with_retry("auth.test", self._client.auth_test)
        return resp.data if hasattr(resp, "data") else resp

    async def close(self) -> None:
        if hasattr(self._client, "session") and self._client.session and not self._client.session.closed:
            await self._client.session.close()

    async def __aenter__(self) -> "SlackClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


class SlackTransport:
    """
    High-level Slack transport façade for First Mate 2.0.
    Coordinates wire normalization, chunking, milestone updates, and reactions.
    """

    def __init__(
        self,
        bot_token: Optional[str] = None,
        app_token: Optional[str] = None,
        client: Optional[SlackClient] = None,
    ) -> None:
        self.bot_token = bot_token
        self.app_token = app_token
        self.client = client if client is not None else SlackClient(token=bot_token)
        self._handler: Any = None

    async def send_message(
        self,
        channel: str,
        text: str,
        thread_ts: Optional[str] = None,
        blocks: Optional[List[Dict[str, Any]]] = None,
        sanitize: bool = True,
        max_chars: int = 3800,
        **kwargs: Any
    ) -> List[Dict[str, Any]]:
        payload = sanitize_for_slack(text) if sanitize else text
        chunks = split_message_boundary_aware(payload, max_chars=max_chars)
        responses: List[Dict[str, Any]] = []

        if not chunks and blocks:
            resp = await self.client.chat_postMessage(
                channel=channel,
                text="",
                thread_ts=thread_ts,
                blocks=blocks,
                sanitize=False,
                **kwargs
            )
            return [resp]

        for idx, chunk in enumerate(chunks):
            chunk_blocks = blocks if idx == 0 else None
            resp = await self.client.chat_postMessage(
                channel=channel,
                text=chunk,
                thread_ts=thread_ts,
                blocks=chunk_blocks,
                sanitize=False,
                **kwargs
            )
            responses.append(resp)

        return responses

    def create_milestone_updater(
        self,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        debounce_interval: float = 1.0,
    ) -> MilestoneUpdater:
        return MilestoneUpdater(
            client=self.client,
            channel=channel,
            thread_ts=thread_ts,
            debounce_interval=debounce_interval
        )

    def create_socket_mode_handler(
        self,
        app: Any,
        app_token: Optional[str] = None
    ) -> Any:
        from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
        token = app_token or self.app_token
        if not token:
            raise ValueError("app_token must be provided to start Socket Mode")
        self._handler = AsyncSocketModeHandler(app, token)
        return self._handler

    async def add_reaction(self, channel: str, timestamp: str, name: str) -> bool:
        return await self.client.reactions_add(channel=channel, name=name, timestamp=timestamp)

    async def remove_reaction(self, channel: str, timestamp: str, name: str) -> bool:
        return await self.client.reactions_remove(channel=channel, name=name, timestamp=timestamp)

    async def swap_reaction(self, channel: str, timestamp: str, old_name: Optional[str], new_name: str) -> None:
        await self.client.swap_reaction(channel=channel, timestamp=timestamp, old_name=old_name, new_name=new_name)

    async def close(self) -> None:
        if self._handler:
            try:
                await self._handler.close_async()
            except Exception as e:
                logger.debug(f"Error closing socket mode handler: {e}")
        await self.client.close()
