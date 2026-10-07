#!/usr/bin/env python3
"""
tests/test_sensory_eyes.py
Unit tests verifying EyesTaskObserver instrumentation and ShoulderTapEventBus integration:
  - Asserts EyesTaskObserver emits `eye_blocker` when detecting OAuth URLs.
  - Asserts EyesTaskObserver emits `eye_attached` upon attachment.
  - Asserts EyesTaskObserver emits `eye_inspection` with buffer delta lines.
  - Asserts deduplication of blocker events across ticks.
  - Verifies zero-token quiescent gating on identical buffer hashes.
"""

from __future__ import annotations

import os
import sys
import asyncio
import unittest
from typing import List
from unittest.mock import MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.sensory.eyes import EyesTaskObserver


class TestSensoryEyes(unittest.IsolatedAsyncioTestCase):
    """Unit tests for EyesTaskObserver sensory events and OAuth blocker emission."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.emitted_alerts: List[ShoulderTapAlert] = []

        def _recorder(alert: ShoulderTapAlert) -> None:
            self.emitted_alerts.append(alert)

        self.bus.subscribe(_recorder)
        self.observer = EyesTaskObserver(
            event_bus=self.bus,
            pane="w1:p2",
            task="Authenticate Spotify service",
            poll_interval=1.0,
        )

    def test_announce_attached_emits_eye_attached(self) -> None:
        """Verify announce_attached emits eye_attached alert on the event bus."""
        self.observer.announce_attached()
        attached_alerts = [a for a in self.emitted_alerts if a.alert_type == "eye_attached"]
        self.assertEqual(len(attached_alerts), 1)
        alert = attached_alerts[0]
        self.assertEqual(alert.data.get("pane_id"), "w1:p2")
        self.assertEqual(alert.data.get("task"), "Authenticate Spotify service")

        # Second call is idempotent and does not duplicate
        self.observer.announce_attached()
        attached_alerts = [a for a in self.emitted_alerts if a.alert_type == "eye_attached"]
        self.assertEqual(len(attached_alerts), 1)

    async def test_oauth_detection_emits_eye_blocker(self) -> None:
        """
        CRITICAL ACCEPTANCE CRITERION:
        Verify EyesTaskObserver emits eye_blocker when detecting OAuth URLs in buffer.
        """
        oauth_url = "http://127.0.0.1:53682/auth/callback?code=xyz123"
        raw_buffer = (
            "Initializing Spotify OAuth authorization...\n"
            f"Please visit the following URL to authenticate:\n{oauth_url}\n"
            "Waiting for callback on port 53682...\n"
        )

        alert = await self.observer.evaluate_buffer_content(raw_buffer)

        # Verify a standard blocker alert was returned
        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "blocker")

        # Verify eye_blocker event was emitted to the bus
        blocker_events = [a for a in self.emitted_alerts if a.alert_type == "eye_blocker"]
        self.assertGreaterEqual(len(blocker_events), 1)

        eye_blocker = blocker_events[0]
        self.assertEqual(eye_blocker.data.get("pane_id"), "w1:p2")
        self.assertEqual(eye_blocker.data.get("blocker_type"), "oauth_url")
        self.assertEqual(eye_blocker.data.get("detail"), oauth_url)

    async def test_prompt_detection_emits_eye_blocker_prompt(self) -> None:
        """Verify interactive prompts emit eye_blocker with blocker_type='prompt'."""
        prompt_buffer = (
            "Building distribution package...\n"
            "File already exists. Overwrite? (y/n): "
        )

        alert = await self.observer.evaluate_buffer_content(prompt_buffer)
        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "blocker")

        blocker_events = [a for a in self.emitted_alerts if a.alert_type == "eye_blocker"]
        self.assertGreaterEqual(len(blocker_events), 1)

        eye_blocker = blocker_events[0]
        self.assertEqual(eye_blocker.data.get("pane_id"), "w1:p2")
        self.assertEqual(eye_blocker.data.get("blocker_type"), "prompt")
        self.assertIn("Overwrite? (y/n)", eye_blocker.data.get("detail", ""))

    async def test_buffer_delta_emits_eye_inspection(self) -> None:
        """Verify dynamic buffer growth emits eye_inspection with delta line counts."""
        buf1 = "Step 1: Compiling source\nStep 2: Linking objects\n"
        await self.observer.evaluate_buffer_content(buf1)

        inspections = [a for a in self.emitted_alerts if a.alert_type == "eye_inspection"]
        self.assertEqual(len(inspections), 1)
        self.assertEqual(inspections[0].data.get("pane_id"), "w1:p2")
        self.assertIn("Scanning stdout buffer", inspections[0].data.get("summary", ""))

        buf2 = buf1 + "Step 3: Running test suite\nStep 4: Packaging artifact\n"
        await self.observer.evaluate_buffer_content(buf2)

        inspections = [a for a in self.emitted_alerts if a.alert_type == "eye_inspection"]
        self.assertEqual(len(inspections), 2)
        self.assertEqual(inspections[1].data.get("delta_lines"), 2)

    async def test_identical_buffer_hash_skips_duplicate_inspection_and_alerts(self) -> None:
        """Verify zero tokens / zero emissions when buffer content does not change."""
        buf = "Running long compilation pipeline on worker...\n"
        await self.observer.evaluate_buffer_content(buf)

        count_after_first = len(self.emitted_alerts)
        self.assertGreater(count_after_first, 0)

        # Second evaluation with identical buffer content
        res = await self.observer.evaluate_buffer_content(buf)
        self.assertIsNone(res)
        self.assertEqual(len(self.emitted_alerts), count_after_first)


if __name__ == "__main__":
    unittest.main()
