#!/usr/bin/env python3
"""
golem/sensory/eyes.py
The Eyes: Active Task & Terminal Buffer Observer for First Mate 2.0.

Features:
- Periodic buffer watcher coroutine running every 15–30s during active task execution.
- SHA-256 buffer change hashing: returns immediately with 0 tokens and 0 API calls when buffer is unchanged.
- Zero-regex classifier: leverages EYES_MODEL from .env to semantically classify
  terminal state ("RUNNING", "BLOCKER", "COMPLETED", "ERROR") into typed JSON schema.
- Emits ShoulderTapAlert ("blocker", "completion") over ShoulderTapEventBus to wake The Mouth.
- Resilient offline fallback and mock-environment detection preserving test compatibility.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import inspect
import asyncio
import logging
from typing import Dict, Any, Optional

from golem.transport.normalizer import strip_ansi
from golem.sensory.bus import ShoulderTapAlert, ShoulderTapEventBus, get_default_event_bus
from golem.agent.mouth import get_gemini_api_key
from golem.config import get_eyes_model as _config_get_eyes_model

logger = logging.getLogger("Golem.Sensory.Eyes")

EYES_MODEL_DEFAULT = _config_get_eyes_model()
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"


def get_eyes_model() -> str:
    """Resolve EYES_MODEL dynamically from environment, zero hardcoding."""
    return _config_get_eyes_model()


class EyesTaskObserver:
    """
    Watches active runner terminal buffers and emits ShoulderTapAlerts
    to The Mouth liaison upon encountering interactive blockers or task completions.

    Zero-regex semantic classification ensures robust identification of OAuth URLs,
    custom CLI prompts, and error states without brittle regular expressions.
    Buffer hashing ensures 0 tokens are consumed when output is static.
    """

    def __init__(
        self,
        event_bus: Optional[ShoulderTapEventBus] = None,
        bus: Optional[ShoulderTapEventBus] = None,
        sensory_bus: Optional[ShoulderTapEventBus] = None,
        buffer_observer: Optional[Any] = None,
        model: Optional[str] = None,
        poll_interval: float = 15.0,
        interval: Optional[float] = None,
        buffer_lines: int = 50,
        session_id: Optional[str] = None,
        pane: str = "runner",
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout_seconds: float = 15.0,
        webui_server: Optional[Any] = None,
        task: str = "",
        **kwargs: Any,
    ) -> None:
        self.event_bus = sensory_bus or event_bus or bus or get_default_event_bus()
        self.sensory_bus = self.event_bus
        self.bus = self.event_bus
        if buffer_observer is None:
            try:
                from golem.execution.buffer import RunnerBufferObserver
                self.buffer_observer = RunnerBufferObserver()
            except Exception:
                self.buffer_observer = None
        else:
            self.buffer_observer = buffer_observer
        self.model = model or get_eyes_model()
        self.poll_interval = interval if interval is not None else poll_interval
        self.interval = self.poll_interval
        self.buffer_lines = buffer_lines
        self.session_id = session_id
        self.pane = pane
        self.channel = channel
        self.thread_ts = thread_ts
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.webui_server = webui_server
        self.task = task or kwargs.get("task_desc", "") or ""
        self._last_line_count: int = 0
        self._attached_announced: bool = False

        # Buffer hashing & deduplication state
        self._last_buffer_hash: Optional[str] = None
        self._last_blocker_signature: Optional[str] = None
        self._last_emitted_state: Optional[str] = None

        # Coroutine lifecycle state
        self._watch_task: Optional[asyncio.Task[Any]] = None
        self._stop_event: asyncio.Event = asyncio.Event()
        self._is_running: bool = False

    @property
    def is_running(self) -> bool:
        """True if background watcher loop is currently active."""
        return self._is_running and self._watch_task is not None and not self._watch_task.done()

    @property
    def is_monitoring(self) -> bool:
        """Alias for is_running."""
        return self.is_running

    def _publish_eye_event(self, alert_type: str, summary: str, data: Dict[str, Any]) -> None:
        """Publish a typed Eye observation event on the Shoulder Tap bus (never raises)."""
        try:
            payload = dict(data)
            payload.setdefault("session_id", self.session_id)
            self.event_bus.publish(
                ShoulderTapAlert(
                    alert_type=alert_type,
                    source="eyes",
                    session_id=None,
                    channel=self.channel,
                    thread_ts=self.thread_ts,
                    summary=summary,
                    data=payload,
                )
            )
        except Exception as e:
            logger.debug(f"EyesTaskObserver: failed to publish {alert_type}: {e}")

    def announce_attached(self) -> None:
        """Publish `eye_attached` once per observer lifetime."""
        if self._attached_announced:
            return
        self._attached_announced = True
        self._publish_eye_event(
            "eye_attached",
            f"Attached to {self.pane}",
            {"pane_id": self.pane, "task": self.task},
        )

    @staticmethod
    def _eye_blocker_kind(blocker_type: Optional[str]) -> str:
        """Map classifier blocker types onto the WebUI vocabulary: oauth_url | prompt."""
        return "oauth_url" if (blocker_type or "").lower() in ("oauth", "oauth_url") else "prompt"

    def normalize_buffer_text(self, raw_buffer: str) -> str:
        """
        Normalize raw buffer output:
        - Strip ANSI escape sequences.
        - Normalize line endings (\r\n -> \n, \r -> \n).
        - Strip trailing whitespace from each line.
        - Strip empty lines at edges.
        """
        if not raw_buffer:
            return ""
        clean = strip_ansi(raw_buffer)
        lines = [line.rstrip() for line in clean.replace("\r\n", "\n").replace("\r", "\n").splitlines()]
        while lines and not lines[0]:
            lines.pop(0)
        while lines and not lines[-1]:
            lines.pop()
        return "\n".join(lines)

    def compute_buffer_hash(self, clean_buffer: str) -> str:
        """Compute deterministic SHA-256 hash of normalized buffer text."""
        clean = self.normalize_buffer_text(clean_buffer)
        return hashlib.sha256(clean.strip().encode("utf-8")).hexdigest()

    def _is_mock_env(self) -> bool:
        """Detect mock or test environment where real Gemini API calls should be bypassed."""
        if os.environ.get("FIRSTMATE_TEST_MODE") == "1":
            return True
        key = self.api_key or get_gemini_api_key()
        if not key or key.startswith("your_") or key == "test_api_key":
            return True
        return False

    async def read_buffer(self, pane: Optional[str] = None, lines: Optional[int] = None) -> str:
        """Fetch latest buffer text from buffer_observer or herdr client."""
        target_pane = pane or self.pane
        target_lines = lines or self.buffer_lines
        if self.buffer_observer is not None:
            if hasattr(self.buffer_observer, "read_buffer"):
                try:
                    res = self.buffer_observer.read_buffer(target_pane, lines=target_lines)
                    if inspect.isawaitable(res):
                        return await res
                    return str(res)
                except Exception as e:
                    logger.warning(f"Error reading buffer from observer for pane '{target_pane}': {e}")
                    return ""
        return ""

    async def classify_buffer(self, clean_buffer: str) -> Dict[str, Any]:
        """
        Classify buffer state into structured JSON.
        Uses zero-regex semantic classification via EYES_MODEL (Gemini Flash-Lite),
        falling back to deterministic inspection in test/offline environments.
        Uses buffer change hashing to ensure 0 tokens are consumed if unchanged.
        """
        if not clean_buffer or not clean_buffer.strip():
            return {
                "state": "IDLE",
                "blocker_type": None,
                "blocker_url": None,
                "blocker_prompt": None,
                "summary": "Terminal buffer is empty or initializing.",
            }

        norm_buf = self.normalize_buffer_text(clean_buffer)
        buf_hash = self.compute_buffer_hash(norm_buf)

        # Buffer change hashing: 0 tokens consumed if unchanged!
        if buf_hash == self._last_buffer_hash:
            logger.debug(f"EyesTaskObserver: Buffer unchanged (hash={buf_hash[:8]}), skipping classification.")
            return {
                "state": "UNCHANGED",
                "skipped": True,
                "blocker_type": None,
                "blocker_url": None,
                "blocker_prompt": None,
                "summary": "Buffer unchanged",
            }

        # Update last observed buffer hash
        self._last_buffer_hash = buf_hash

        # Check offline / test mock environment fallback
        if self._is_mock_env():
            return self._classify_buffer_offline_fallback(norm_buf)

        # Real Gemini EYES_MODEL classification
        try:
            return await self._call_gemini_classification(norm_buf)
        except Exception as e:
            logger.warning(f"Error calling EYES_MODEL classification; falling back to offline analysis: {e}")
            return self._classify_buffer_offline_fallback(norm_buf)

    async def _call_gemini_classification(self, clean_buffer: str) -> Dict[str, Any]:
        """Call Gemini REST API for semantic buffer classification."""
        key = self.api_key or get_gemini_api_key()
        if not key:
            return self._classify_buffer_offline_fallback(clean_buffer)

        try:
            import aiohttp

            sys_prompt = (
                "You are The Eyes, the active task and terminal buffer observer in First Mate's sensory architecture.\n"
                "You observe Pane 2 (The Hands) during task execution and collaborate with The Mouth (Liaison) "
                "and The Nose (Fleet Health Sentinel) via the Shoulder Tap Event Bus.\n"
                "Analyze the terminal buffer tail and classify the execution state into strictly valid JSON.\n\n"
                "JSON Schema:\n"
                "{\n"
                '  "state": "RUNNING" | "BLOCKER" | "COMPLETED" | "ERROR",\n'
                '  "blocker_type": "oauth" | "prompt" | "secret" | null,\n'
                '  "blocker_url": "<URL string or null>",\n'
                '  "blocker_prompt": "<prompt text string or null>",\n'
                '  "summary": "<concise 1-sentence description>"\n'
                "}\n\n"
                "Classification Guidelines:\n"
                '- "BLOCKER": Execution is paused waiting for user input, interactive confirmation ([y/N], (y/n)), '
                'sudo password, passphrase, secret token, or an OAuth authentication URL (e.g. http://127.0.0.1:.../auth or localhost).\n'
                '- "COMPLETED": The command finished running and returned to the shell prompt with success.\n'
                '- "ERROR": The command failed, crashed, threw an unhandled exception or non-zero exit code.\n'
                '- "RUNNING": The process is actively executing, printing logs, compiling, or downloading.'
            )

            payload = {
                "systemInstruction": {"parts": [{"text": sys_prompt}]},
                "contents": [
                    {
                        "role": "user",
                        "parts": [{"text": f"Terminal Buffer Tail:\n```\n{clean_buffer}\n```"}]
                    }
                ],
                "generationConfig": {
                    "temperature": 0.1,
                    "responseMimeType": "application/json",
                }
            }

            url = GEMINI_API_URL.format(model=self.model, key=key)
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            async with aiohttp.ClientSession(timeout=timeout) as client_session:
                async with client_session.post(url, json=payload, headers={"Content-Type": "application/json"}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        candidates = data.get("candidates", [])
                        if candidates:
                            parts = candidates[0].get("content", {}).get("parts", [])
                            text_chunks = [p.get("text", "") for p in parts if "text" in p]
                            raw_json = "".join(text_chunks).strip()
                            raw_json = re.sub(r"^```json\s*", "", raw_json)
                            raw_json = re.sub(r"\s*```$", "", raw_json)
                            parsed = json.loads(raw_json)
                            if isinstance(parsed, dict) and "state" in parsed:
                                return parsed
                    else:
                        logger.warning(f"EYES_MODEL ({self.model}) returned HTTP {resp.status}: {await resp.text()}")
        except Exception as e:
            logger.warning(f"EYES_MODEL classification failed; falling back to offline analysis: {e}")

        return self._classify_buffer_offline_fallback(clean_buffer)

    def _classify_buffer_offline_fallback(self, clean_buffer: str) -> Dict[str, Any]:
        """
        Deterministic offline / mock fallback analysis when Gemini API is unavailable.
        Uses buffer_observer if available, or structural heuristics.
        """
        # 1. Use buffer_observer blocker extractor if present
        if self.buffer_observer is not None:
            if hasattr(self.buffer_observer, "extract_interactive_blockers"):
                blocker = self.buffer_observer.extract_interactive_blockers(clean_buffer)
                if blocker:
                    b_type = blocker.get("type", "prompt")
                    b_url = blocker.get("url")
                    b_content = blocker.get("content")
                    return {
                        "state": "BLOCKER",
                        "blocker_type": b_type,
                        "blocker_url": b_url,
                        "blocker_prompt": b_content,
                        "summary": f"Interactive blocker ({b_type}) waiting for input.",
                    }

        # 2. Check for OAuth callback URL
        oauth_match = re.search(r"https?://(?:127\.0\.0\.1|localhost):[0-9]+[^\s\"'>)]*", clean_buffer)
        if oauth_match:
            return {
                "state": "BLOCKER",
                "blocker_type": "oauth",
                "blocker_url": oauth_match.group(0),
                "blocker_prompt": None,
                "summary": "OAuth authentication URL detected.",
            }

        # 3. Check for confirmation / password prompt
        lines = [line.strip() for line in clean_buffer.splitlines() if line.strip()]
        last_line = lines[-1] if lines else ""
        if re.search(r"(\[y/N\]|\(y/n\)|password:|passphrase:)", clean_buffer, re.IGNORECASE):
            is_secret = bool(re.search(r"password|passphrase", clean_buffer, re.IGNORECASE))
            return {
                "state": "BLOCKER",
                "blocker_type": "secret" if is_secret else "prompt",
                "blocker_url": None,
                "blocker_prompt": last_line,
                "summary": f"Interactive {'secret' if is_secret else 'confirmation'} prompt waiting for input.",
            }

        # 4. Check for completed shell prompt or test OK at end of buffer
        if re.search(r"(\$|#|%|❯)\s*$", last_line) or "tests passed" in clean_buffer.lower() or "\nOK" in clean_buffer:
            return {
                "state": "COMPLETED",
                "blocker_type": None,
                "blocker_url": None,
                "blocker_prompt": None,
                "summary": "Command returned to shell prompt or test suite passed.",
            }

        return {
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Command actively executing.",
        }

    async def evaluate_buffer_content(
        self,
        buffer_text: str,
        session_id: Optional[str] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        pane: Optional[str] = None,
    ) -> Optional[ShoulderTapAlert]:
        """
        Evaluate buffer text, detect blockers/completions, and emit alerts.
        """
        target_session = session_id or self.session_id
        target_channel = channel or self.channel
        target_thread = thread_ts or self.thread_ts
        target_pane = pane or self.pane
        if not buffer_text or "no terminal output recorded" in buffer_text.lower():
            return None

        clean_buffer = self.normalize_buffer_text(buffer_text)
        if not clean_buffer or "no terminal output recorded" in clean_buffer.lower():
            return None

        classification = await self.classify_buffer(buffer_text)
        if classification.get("skipped") or classification.get("state") in ("UNCHANGED", "IDLE"):
            return None

        state = classification.get("state", "RUNNING")
        summary = classification.get("summary", "")
        blocker_type = classification.get("blocker_type")
        blocker_url = classification.get("blocker_url")
        blocker_prompt = classification.get("blocker_prompt")
        if self.webui_server is not None and hasattr(self.webui_server, "update_hand_buffer"):
            try:
                self.webui_server.update_hand_buffer(target_pane, clean_buffer)
            except Exception as e:
                logger.debug(f"EyesTaskObserver: Failed to update webui hand buffer: {e}")
        lines = [l for l in clean_buffer.splitlines() if l.strip()]
        buffer_tail = "\n".join(lines[-10:]) if lines else ""

        # Structured inspection event: only reached when the buffer actually changed.
        delta_lines = len(lines) - self._last_line_count
        self._last_line_count = len(lines)
        self._publish_eye_event(
            "eye_inspection",
            f"Scanning stdout buffer (delta: {delta_lines:+d} lines)",
            {
                "pane_id": target_pane,
                "summary": f"Scanning stdout buffer (delta: {delta_lines:+d} lines)",
                "delta_lines": delta_lines,
                "state": state,
            },
        )

        # Handle BLOCKER
        if state == "BLOCKER":
            blocker_sig = f"{blocker_type}:{blocker_url or blocker_prompt}"
            if blocker_sig == self._last_blocker_signature:
                logger.debug(f"EyesTaskObserver: Blocker signature unchanged ({blocker_sig}), skipping duplicate alert.")
                return None

            self._last_blocker_signature = blocker_sig
            self._last_emitted_state = "BLOCKER"

            kind = self._eye_blocker_kind(blocker_type)
            self._publish_eye_event(
                "eye_blocker",
                summary or f"{kind} blocker detected",
                {
                    "pane_id": target_pane,
                    "blocker_type": kind,
                    "detail": blocker_url or blocker_prompt or "",
                },
            )

            alert = ShoulderTapAlert(
                alert_type="blocker",
                source="eyes",
                session_id=target_session,
                channel=target_channel,
                thread_ts=target_thread,
                summary=summary or "Interactive blocker encountered in runner pane",
                data={
                    "state": "BLOCKER",
                    "blocker_type": blocker_type,
                    "type": blocker_type,
                    "blocker_url": blocker_url,
                    "url": blocker_url,
                    "blocker_prompt": blocker_prompt,
                    "prompt": blocker_prompt,
                    "pane": target_pane,
                    "buffer_tail": buffer_tail,
                },
            )
            self.event_bus.publish(alert)
            return alert

        # Handle COMPLETION or ERROR
        elif state in ("COMPLETED", "ERROR"):
            if self._last_emitted_state == state:
                return None

            self._last_emitted_state = state
            alert = ShoulderTapAlert(
                alert_type="completion",
                source="eyes",
                session_id=target_session,
                channel=target_channel,
                thread_ts=target_thread,
                summary=summary or (
                    "Task completed in runner pane" if state == "COMPLETED" else "Task encountered error in runner pane"
                ),
                data={
                    "state": state,
                    "is_error": (state == "ERROR"),
                    "pane": target_pane,
                    "buffer_tail": buffer_tail,
                },
            )
            self.event_bus.publish(alert)
            return alert

        # RUNNING state
        self._last_emitted_state = "RUNNING"
        self._last_blocker_signature = None
        return None

    async def tick(self) -> Optional[ShoulderTapAlert]:
        """Execute one observation cycle using read_buffer and evaluate_buffer_content."""
        raw_buffer = await self.read_buffer(self.pane)
        return await self.evaluate_buffer_content(
            buffer_text=raw_buffer,
            session_id=self.session_id,
            channel=self.channel,
            thread_ts=self.thread_ts,
            pane=self.pane,
        )

    def start_monitoring(
        self,
        session_id: Optional[str] = None,
        pane: Optional[str] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
    ) -> asyncio.Task[Any]:
        """
        Start the background observation coroutine task.
        Non-blocking and thread-safe.
        """
        if session_id:
            self.session_id = session_id
        if pane:
            self.pane = pane
        if channel:
            self.channel = channel
        if thread_ts:
            self.thread_ts = thread_ts

        if self._watch_task is not None and not self._watch_task.done():
            return self._watch_task

        self._is_running = True
        self._stop_event.clear()
        loop = asyncio.get_event_loop()
        self._watch_task = loop.create_task(self._watch_loop())
        return self._watch_task

    def start(
        self,
        session_id: Optional[str] = None,
        pane: Optional[str] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
    ) -> asyncio.Task[Any]:
        """Alias for start_monitoring."""
        return self.start_monitoring(session_id=session_id, pane=pane, channel=channel, thread_ts=thread_ts)

    async def _watch_loop(self) -> None:
        """Internal monitoring loop running every poll_interval seconds."""
        logger.info(f"EyesTaskObserver started monitoring pane '{self.pane}' (interval={self.poll_interval}s)")
        try:
            while self._is_running and not self._stop_event.is_set():
                try:
                    alert = await self.tick()
                    if alert and alert.alert_type == "completion":
                        logger.info(f"EyesTaskObserver: Received completion alert; stopping watcher loop for pane '{self.pane}'.")
                        break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error during EyesTaskObserver tick for pane '{self.pane}': {e}", exc_info=True)

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
                    break
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass
        finally:
            self._is_running = False

    async def stop_monitoring(self) -> None:
        """Stop and cancel the background watcher task cleanly."""
        self._is_running = False
        self._stop_event.set()
        if self._watch_task is not None:
            if not self._watch_task.done():
                self._watch_task.cancel()
                try:
                    await self._watch_task
                except asyncio.CancelledError:
                    pass
            self._watch_task = None
        logger.info(f"EyesTaskObserver stopped monitoring pane '{self.pane}'")

    def stop_monitoring_sync(self) -> None:
        """Synchronously stop and cancel the background watcher task cleanly."""
        self._is_running = False
        self._stop_event.set()
        if self._watch_task is not None:
            if not self._watch_task.done():
                self._watch_task.cancel()
            self._watch_task = None
        logger.info(f"EyesTaskObserver stopped monitoring pane '{self.pane}' (sync)")

    async def stop(self) -> None:
        """Alias for stop_monitoring."""
        await self.stop_monitoring()
