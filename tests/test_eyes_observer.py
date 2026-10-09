#!/usr/bin/env python3
"""
tests/test_eyes_observer.py
Unit tests for EyesTaskObserver (The Eyes).
Validates:
  - SHA-256 buffer change hashing (verifying 0 LLM calls when buffer unchanged)
  - Zero-regex classification of OAuth URLs, confirmation prompts, and completion states
  - Alert publishing to ShoulderTapEventBus (blocker, completion)
  - Deduplication of identical blocker alerts across ticks
  - Watcher lifecycle management (clean start, periodic ticking, cancel/stop)
  - Offline/error fallback to deterministic buffer extraction
  - Dynamic model resolution without hardcoding

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import time
import asyncio
import hashlib
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Optional, List, Dict, Any

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.sensory.eyes import EyesTaskObserver, get_eyes_model
from golem.transport.normalizer import strip_ansi


class TestBufferChangeHashing(unittest.IsolatedAsyncioTestCase):
    """Unit tests for SHA-256 buffer change hashing and token minimization."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.observer = EyesTaskObserver(bus=self.bus, model="gemini-flash-lite-latest")

    def test_sha256_hash_deterministic(self) -> None:
        """Verify identical buffer text produces identical SHA-256 hash."""
        text = "Running test suite on hermes (PID 1234)..."
        h1 = self.observer.compute_buffer_hash(text)
        h2 = self.observer.compute_buffer_hash(text)
        expected = hashlib.sha256(text.strip().encode("utf-8")).hexdigest()
        self.assertEqual(h1, expected)
        self.assertEqual(h1, h2)

    def test_ansi_color_sequences_stripped_before_hashing(self) -> None:
        """Verify ANSI styling variations produce the same hash if visible text is identical."""
        raw_ansi = "\x1b[32mRunning test suite\x1b[0m on \x1b[1mhermes\x1b[0m"
        clean = "Running test suite on hermes"
        h_ansi = self.observer.compute_buffer_hash(raw_ansi)
        h_clean = self.observer.compute_buffer_hash(clean)
        self.assertEqual(h_ansi, h_clean)

    async def test_zero_llm_calls_when_buffer_hash_matches(self) -> None:
        """
        CRITICAL ACCEPTANCE CRITERION:
        Verify that when the terminal buffer is unchanged between ticks,
        exactly 0 LLM calls are made (0 tokens consumed).
        """
        mock_classify_llm = AsyncMock(return_value={
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Compiling assets in background",
        })
        self.observer._call_gemini_classification = mock_classify_llm

        buffer_text = "Compiling 45/100 assets... [=======>        ]"

        # Tick 1: First observation of buffer -> LLM MUST be called
        res1 = await self.observer.classify_buffer(buffer_text)
        self.assertEqual(mock_classify_llm.call_count, 1)
        self.assertEqual(res1.get("state"), "RUNNING")

        # Tick 2: Buffer remains identical -> LLM MUST NOT be called (0 tokens!)
        res2 = await self.observer.classify_buffer(buffer_text)
        self.assertEqual(
            mock_classify_llm.call_count, 1,
            "LLM must not be called when buffer hash is unchanged!"
        )
        self.assertTrue(res2.get("skipped", False) or res2.get("state") in ("RUNNING", "UNCHANGED"))

        # Tick 3: Buffer STILL unchanged -> LLM call count remains 1
        res3 = await self.observer.classify_buffer(buffer_text)
        self.assertEqual(
            mock_classify_llm.call_count, 1,
            "LLM must not be called on subsequent identical ticks!"
        )

    async def test_llm_called_when_buffer_hash_changes(self) -> None:
        """Verify LLM is invoked when buffer content changes between ticks."""
        mock_classify_llm = AsyncMock(return_value={
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Step update",
        })
        self.observer._call_gemini_classification = mock_classify_llm

        # Tick 1: Initial buffer
        await self.observer.classify_buffer("Step 1: Cloning repository...")
        self.assertEqual(mock_classify_llm.call_count, 1)

        # Tick 2: Buffer changes
        await self.observer.classify_buffer("Step 2: Installing requirements...")
        self.assertEqual(mock_classify_llm.call_count, 2)

    async def test_empty_or_whitespace_buffer_skips_llm(self) -> None:
        """Verify empty buffer produces 0 LLM calls."""
        mock_classify_llm = AsyncMock()
        self.observer._call_gemini_classification = mock_classify_llm

        res = await self.observer.classify_buffer("   \n\n  \t  ")
        self.assertEqual(mock_classify_llm.call_count, 0)
        self.assertEqual(res.get("state"), "IDLE")


class TestZeroRegexClassification(unittest.IsolatedAsyncioTestCase):
    """Unit tests for zero-regex classification of terminal states via EYES_MODEL."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.observer = EyesTaskObserver(bus=self.bus, model="gemini-flash-lite-latest")

    async def test_classify_oauth_callback_url_arbitrary_port(self) -> None:
        """Verify OAuth URL detection with non-standard ports and complex query parameters."""
        oauth_buffer = (
            "Authenticating with Google Cloud Platform...\n"
            "Open the following URL in your browser to sign in:\n"
            "http://127.0.0.1:53682/oauth2/callback?state=xyz987&code_challenge=abc\n"
            "Waiting for authorization code..."
        )
        mock_llm_response = {
            "state": "BLOCKER",
            "blocker_type": "oauth",
            "blocker_url": "http://127.0.0.1:53682/oauth2/callback?state=xyz987&code_challenge=abc",
            "blocker_prompt": None,
            "summary": "Waiting for OAuth authentication",
        }
        self.observer._call_gemini_classification = AsyncMock(return_value=mock_llm_response)

        res = await self.observer.classify_buffer(oauth_buffer)
        self.assertEqual(res["state"], "BLOCKER")
        self.assertEqual(res["blocker_type"], "oauth")
        self.assertEqual(res["blocker_url"], "http://127.0.0.1:53682/oauth2/callback?state=xyz987&code_challenge=abc")

    async def test_classify_interactive_confirmation_prompt(self) -> None:
        """Verify interactive confirmation prompts are classified as BLOCKER."""
        prompt_buffer = (
            "Preparing database migration for production...\n"
            "WARNING: 3 tables will be truncated.\n"
            "Do you really want to proceed? [y/N]: "
        )
        mock_llm_response = {
            "state": "BLOCKER",
            "blocker_type": "prompt",
            "blocker_url": None,
            "blocker_prompt": "Do you really want to proceed? [y/N]:",
            "summary": "User confirmation required",
        }
        self.observer._call_gemini_classification = AsyncMock(return_value=mock_llm_response)

        res = await self.observer.classify_buffer(prompt_buffer)
        self.assertEqual(res["state"], "BLOCKER")
        self.assertEqual(res["blocker_type"], "prompt")
        self.assertIn("proceed", res["blocker_prompt"])

    async def test_classify_secret_passphrase_prompt(self) -> None:
        """Verify password or passphrase prompts are classified as BLOCKER."""
        secret_buffer = "Enter passphrase for key '/Users/hermes/.ssh/id_ed25519': "
        mock_llm_response = {
            "state": "BLOCKER",
            "blocker_type": "secret",
            "blocker_url": None,
            "blocker_prompt": "Enter passphrase",
            "summary": "SSH passphrase required",
        }
        self.observer._call_gemini_classification = AsyncMock(return_value=mock_llm_response)

        res = await self.observer.classify_buffer(secret_buffer)
        self.assertEqual(res["state"], "BLOCKER")
        self.assertEqual(res["blocker_type"], "secret")

    async def test_classify_running_state(self) -> None:
        """Verify actively progressing jobs are classified as RUNNING."""
        running_buffer = "Building targets: [34%] 120/350 objects compiled..."
        mock_llm_response = {
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Compiling targets",
        }
        self.observer._call_gemini_classification = AsyncMock(return_value=mock_llm_response)

        res = await self.observer.classify_buffer(running_buffer)
        self.assertEqual(res["state"], "RUNNING")
        self.assertIsNone(res["blocker_type"])

    async def test_classify_completed_state(self) -> None:
        """Verify successful command exit is classified as COMPLETED."""
        completed_buffer = "Ran 933 tests in 143.984s\n\nOK"
        mock_llm_response = {
            "state": "COMPLETED",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Test suite passed cleanly",
        }
        self.observer._call_gemini_classification = AsyncMock(return_value=mock_llm_response)

        res = await self.observer.classify_buffer(completed_buffer)
        self.assertEqual(res["state"], "COMPLETED")

    async def test_classify_error_state(self) -> None:
        """Verify fatal process exit is classified as ERROR."""
        error_buffer = "Fatal error: segmentation fault (core dumped)\nExit code 139"
        mock_llm_response = {
            "state": "ERROR",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Process terminated with segmentation fault",
        }
        self.observer._call_gemini_classification = AsyncMock(return_value=mock_llm_response)

        res = await self.observer.classify_buffer(error_buffer)
        self.assertEqual(res["state"], "ERROR")


class TestEyesAlertEmission(unittest.IsolatedAsyncioTestCase):
    """Unit tests for ShoulderTapAlert publishing to ShoulderTapEventBus."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.observer = EyesTaskObserver(bus=self.bus, model="gemini-flash-lite-latest")

    async def test_blocker_state_emits_blocker_alert_to_bus(self) -> None:
        """Verify detecting a blocker emits a ShoulderTapAlert(alert_type='blocker') on the bus."""
        received: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda alert: received.append(alert), alert_types=["blocker"])

        self.observer._call_gemini_classification = AsyncMock(return_value={
            "state": "BLOCKER",
            "blocker_type": "oauth",
            "blocker_url": "http://127.0.0.1:8080/auth",
            "blocker_prompt": None,
            "summary": "OAuth login required",
        })

        alert = await self.observer.evaluate_buffer_content(
            buffer_text="Waiting for http://127.0.0.1:8080/auth",
            session_id="C_MAIN:100.1",
            channel="C_MAIN",
            thread_ts="100.1",
        )

        await asyncio.sleep(0.05)
        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "blocker")
        self.assertEqual(alert.source, "eyes")
        self.assertEqual(alert.session_id, "C_MAIN:100.1")
        self.assertEqual(alert.data.get("url"), "http://127.0.0.1:8080/auth")
        self.assertEqual(len(received), 1)

    async def test_completed_state_emits_completion_alert_to_bus(self) -> None:
        """Verify detecting completion emits a ShoulderTapAlert(alert_type='completion')."""
        received: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda alert: received.append(alert), alert_types=["completion"])

        self.observer._call_gemini_classification = AsyncMock(return_value={
            "state": "COMPLETED",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Build succeeded in 4.2s",
        })

        alert = await self.observer.evaluate_buffer_content(
            buffer_text="Build succeeded",
            session_id="C_MAIN:100.1",
            channel="C_MAIN",
            thread_ts="100.1",
        )

        await asyncio.sleep(0.05)
        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "completion")
        self.assertEqual(len(received), 1)

    async def test_running_state_does_not_emit_alert(self) -> None:
        """Verify normal running output does not emit any shoulder tap alerts."""
        received: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda alert: received.append(alert))

        self.observer._call_gemini_classification = AsyncMock(return_value={
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Compiling",
        })

        alert = await self.observer.evaluate_buffer_content(
            buffer_text="Compiling...",
            session_id="C_MAIN:100.1",
        )

        await asyncio.sleep(0.05)
        self.assertIsNone(alert)
        self.assertEqual(len(received), 0)

    async def test_deduplicate_identical_blocker_emission(self) -> None:
        """Verify observer does not spam duplicate blocker alerts if the prompt remains on screen across ticks."""
        received: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda alert: received.append(alert), alert_types=["blocker"])

        mock_llm = AsyncMock(return_value={
            "state": "BLOCKER",
            "blocker_type": "oauth",
            "blocker_url": "http://127.0.0.1:8080/auth",
            "blocker_prompt": None,
            "summary": "OAuth login required",
        })
        self.observer._call_gemini_classification = mock_llm

        # Tick 1: emits alert
        await self.observer.evaluate_buffer_content(
            buffer_text="Login at http://127.0.0.1:8080/auth (tick 1)",
            session_id="C1:1.0",
        )
        # Tick 2: buffer slightly changed (timestamp update), but same blocker URL
        await self.observer.evaluate_buffer_content(
            buffer_text="Login at http://127.0.0.1:8080/auth (tick 2)",
            session_id="C1:1.0",
        )

        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 1)


class TestEyesObserverLifecycle(unittest.IsolatedAsyncioTestCase):
    """Unit tests for background watcher task lifecycle."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.observer = EyesTaskObserver(bus=self.bus, poll_interval=0.02)

    async def asyncTearDown(self) -> None:
        await self.observer.stop_monitoring()

    def test_model_resolution_zero_hardcoding(self) -> None:
        """Verify EYES_MODEL is resolved dynamically from environment without hardcoding."""
        with patch.dict(os.environ, {"EYES_MODEL": "gemini-test-custom"}, clear=False):
            self.assertEqual(get_eyes_model(), "gemini-test-custom")
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(get_eyes_model(), "gemini-flash-lite-latest")

    async def test_start_monitoring_runs_periodic_ticks(self) -> None:
        """Verify start_monitoring starts a coroutine that periodically reads buffer."""
        tick_count = 0

        async def mock_read_buffer(*args: Any, **kwargs: Any) -> str:
            nonlocal tick_count
            tick_count += 1
            return f"Buffer output tick {tick_count}"

        self.observer.read_buffer = mock_read_buffer
        self.observer._call_gemini_classification = AsyncMock(return_value={"state": "RUNNING"})

        task = self.observer.start_monitoring(session_id="C1:1.0", pane="runner")
        self.assertFalse(task.done())

        # Allow several intervals to execute via dynamic polling
        for _ in range(50):
            if tick_count >= 3:
                break
            await asyncio.sleep(0.01)
        self.assertGreaterEqual(tick_count, 3)

        await self.observer.stop_monitoring()
        self.assertTrue(task.done())

    async def test_stop_monitoring_cancels_cleanly(self) -> None:
        """Verify stop_monitoring cleanly cancels the watcher task without exceptions."""
        self.observer.read_buffer = AsyncMock(return_value="Idle")
        task = self.observer.start_monitoring(session_id="C1:1.0")
        self.assertFalse(task.done())

        await self.observer.stop_monitoring()
        self.assertTrue(task.done() or task.cancelled())
        self.assertFalse(self.observer.is_monitoring)

    async def test_gemini_api_error_fallback_to_buffer_regex(self) -> None:
        """Verify that when Gemini API call fails, observer falls back to deterministic regex extraction."""
        self.observer._call_gemini_classification = AsyncMock(side_effect=Exception("API connection timeout"))

        buffer_text = "Please authenticate at http://127.0.0.1:53682/auth to continue."
        res = await self.observer.classify_buffer(buffer_text)

        self.assertEqual(res.get("state"), "BLOCKER")
        self.assertEqual(res.get("blocker_type"), "oauth")
        self.assertEqual(res.get("blocker_url"), "http://127.0.0.1:53682/auth")

    async def test_completion_detection_and_watcher_shutdown(self) -> None:
        """Verify that receiving a completion alert terminates the watcher loop automatically."""
        done_buffer = "All 42 tests passed in 0.8s.\nhermes@Mac % "
        mock_buf_observer = MagicMock()
        mock_buf_observer.read_buffer = AsyncMock(return_value=done_buffer)

        observer = EyesTaskObserver(
            event_bus=self.bus,
            buffer_observer=mock_buf_observer,
            session_id="sess_done",
            poll_interval=0.05,
        )

        with patch.object(observer, "_call_gemini_classification", new_callable=AsyncMock) as mock_classify:
            mock_classify.return_value = {
                "state": "COMPLETED",
                "blocker_type": None,
                "blocker_url": None,
                "blocker_prompt": None,
                "summary": "Tests completed successfully",
            }

            task = observer.start()
            await asyncio.wait_for(task, timeout=1.0)
            self.assertFalse(observer.is_running)


if __name__ == "__main__":
    unittest.main()
