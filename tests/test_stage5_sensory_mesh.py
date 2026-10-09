#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

tests/test_stage5_sensory_mesh.py
Stage 5 Verification Suite: Deterministic Sensory Mesh & WebUI Dedup:
  - Buffer unwrapping across terminal column width & URLs (unwrap_terminal_buffer).
  - Exact deterministic prompt patterns (SUDO_SSH, CONFIRM, OAUTH_URL, ENTER).
  - Active tap deduplication keyed on (target_pane, pattern_id, prompt_hash).
  - Tap clearing on state transitions away from BLOCKER.
  - Nose fleet telemetry hysteresis (N=3 consecutive failures before alerting).
  - Benign classification of personal workstation sleep without delay.
"""

from __future__ import annotations

import os
import sys
import asyncio
import unittest
from typing import List, Dict, Any
from unittest.mock import AsyncMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.sensory.eyes import (
    EyesTaskObserver,
    unwrap_terminal_buffer,
    SUDO_SSH_PROMPT,
    CONFIRM_PROMPT,
    OAUTH_URL_PROMPT,
    ENTER_PROMPT,
)
from golem.sensory.nose import (
    NoseSentinel,
    StatusDeltaCalculator,
    FleetSnapshot,
)


class TestBufferUnwrapping(unittest.TestCase):
    """Unit tests for PTY buffer column unwrapping and split URL reassembly."""

    def test_empty_or_none_buffer(self) -> None:
        self.assertEqual(unwrap_terminal_buffer(""), "")

    def test_unwraps_split_oauth_url(self) -> None:
        raw = (
            "Please open your browser to authenticate:\n"
            "http://127.0.0.1:8080/oauth2/\n"
            "callback?code=abc123xyz&state=secret_state\n"
            "Waiting for approval..."
        )
        unwrapped = unwrap_terminal_buffer(raw, col_width=80)
        self.assertIn("http://127.0.0.1:8080/oauth2/callback?code=abc123xyz&state=secret_state", unwrapped)

    def test_unwraps_soft_wrapped_terminal_line(self) -> None:
        long_line_part1 = "A" * 78
        long_line_part2 = "continued text on next line"
        raw = f"{long_line_part1}\n{long_line_part2}\nFinished."
        unwrapped = unwrap_terminal_buffer(raw, col_width=80)
        self.assertIn(f"{long_line_part1} {long_line_part2}", unwrapped)

    def test_preserves_punctuated_line_endings(self) -> None:
        line1 = ("A" * 77) + "."
        line2 = "Next sentence begins here."
        raw = f"{line1}\n{line2}"
        unwrapped = unwrap_terminal_buffer(raw, col_width=80)
        self.assertEqual(unwrapped, raw)


class TestDeterministicPromptRegexes(unittest.TestCase):
    """Unit tests for Stage 5 exact deterministic prompt regex patterns."""

    def test_sudo_ssh_prompts(self) -> None:
        samples = [
            "[sudo] password for hermes: ",
            "password for admin: ",
            "Enter passphrase for key '/Users/hermes/.ssh/id_rsa': ",
            "Password: ",
        ]
        for s in samples:
            self.assertTrue(bool(SUDO_SSH_PROMPT.search(s)), f"Failed to match sudo/ssh prompt: {s}")

    def test_confirmation_prompts(self) -> None:
        samples = [
            "Do you want to proceed? [y/N] ",
            "Are you sure you want to continue? (y/n) ",
            "Proceed with operation? (yes/no): ",
            "Apply migrations? (Y/n) ",
            "Are you sure you want to continue?",
            "Proceed with operation?",
            "Do you want to continue?",
        ]
        for s in samples:
            self.assertTrue(bool(CONFIRM_PROMPT.search(s)), f"Failed to match confirm prompt: {s}")

    def test_oauth_url_prompts(self) -> None:
        samples = [
            "http://127.0.0.1:53682/oauth/callback?state=xyz",
            "http://localhost:8080/auth",
            "https://accounts.google.com/o/oauth2/auth?client_id=123",
            "https://github.com/login/oauth/authorize?scope=repo",
        ]
        for s in samples:
            self.assertTrue(bool(OAUTH_URL_PROMPT.search(s)), f"Failed to match OAuth URL: {s}")

    def test_enter_prompts(self) -> None:
        samples = [
            "Press [Enter] to continue...",
            "Press Enter to continue",
            "Press any key to continue...",
        ]
        for s in samples:
            self.assertTrue(bool(ENTER_PROMPT.search(s)), f"Failed to match Enter prompt: {s}")


class TestEyesTapDeduplication(unittest.IsolatedAsyncioTestCase):
    """Unit tests verifying EyesTaskObserver blocker deduplication via _active_taps."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.emitted: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda a: self.emitted.append(a), alert_types=["blocker"])
        self.observer = EyesTaskObserver(bus=self.bus, pane="w1:p2")

    async def test_duplicate_prompt_same_pane_deduplicated(self) -> None:
        raw_buffer = "Database migration check.\nDo you want to proceed? [y/N]: "

        # First evaluation emits blocker alert
        alert1 = await self.observer.evaluate_buffer_content(raw_buffer, pane="w1:p2")
        self.assertIsNotNone(alert1)
        self.assertEqual(len(self.emitted), 1)
        self.assertTrue(alert1.summary.startswith(":warning:"))

        # Second evaluation with identical prompt is suppressed by _active_taps
        alert2 = await self.observer.evaluate_buffer_content(raw_buffer, pane="w1:p2")
        self.assertIsNone(alert2)
        self.assertEqual(len(self.emitted), 1)

    async def test_different_pane_receives_independent_tap(self) -> None:
        raw_buffer = "Do you want to proceed? [y/N]: "

        # Pane 1
        alert1 = await self.observer.evaluate_buffer_content(raw_buffer, pane="w1:p2")
        self.assertIsNotNone(alert1)
        self.assertEqual(len(self.emitted), 1)

        # Pane 2 with same prompt gets its own tap
        alert2 = await self.observer.evaluate_buffer_content(raw_buffer, pane="w1:p3")
        self.assertIsNotNone(alert2)
        self.assertEqual(len(self.emitted), 2)

    async def test_transition_to_running_clears_active_taps(self) -> None:
        prompt_buf = "Enter passphrase for key '/Users/hermes/.ssh/id_rsa': "
        running_buf = "Compiling modules [50%] 100/200..."

        # Blocker emitted
        alert1 = await self.observer.evaluate_buffer_content(prompt_buf, pane="w1:p2")
        self.assertIsNotNone(alert1)
        self.assertEqual(len(self.emitted), 1)

        # User enters passphrase -> task is running again
        await self.observer.evaluate_buffer_content(running_buf, pane="w1:p2")

        # Later, another passphrase prompt appears -> must emit again!
        alert2 = await self.observer.evaluate_buffer_content(prompt_buf, pane="w1:p2")
        self.assertIsNotNone(alert2)
        self.assertEqual(len(self.emitted), 2)


class TestNoseTelemetryHysteresis(unittest.TestCase):
    """Unit tests verifying NoseSentinel N=3 failure hysteresis for node offline alerting."""

    def setUp(self) -> None:
        self.snap_online = FleetSnapshot(
            nodes={
                "oci-machine": {"online": True, "status": "Online (up 40 days)"},
                "personal-laptop": {"online": True, "status": "Online (active)"},
            },
            disk={"pressure": False},
        )
        self.snap_offline = FleetSnapshot(
            nodes={
                "oci-machine": {"online": False, "status": "[Unreachable] Connection refused"},
                "personal-laptop": {"online": False, "status": "Sleeping / Standby"},
            },
            disk={"pressure": False},
        )

    def test_n3_hysteresis_suppresses_transient_failures(self) -> None:
        failures: Dict[str, int] = {}

        # Tick 1: oci-machine fails once -> 0 deltas
        d1 = StatusDeltaCalculator.compute_deltas(
            self.snap_online, self.snap_offline, failure_counts=failures, failure_threshold=3
        )
        oci_deltas1 = [d for d in d1 if d.target == "oci-machine"]
        self.assertEqual(len(oci_deltas1), 0)
        self.assertEqual(failures.get("oci-machine"), 1)

        # Tick 2: oci-machine fails twice -> 0 deltas
        d2 = StatusDeltaCalculator.compute_deltas(
            self.snap_online, self.snap_offline, failure_counts=failures, failure_threshold=3
        )
        oci_deltas2 = [d for d in d2 if d.target == "oci-machine"]
        self.assertEqual(len(oci_deltas2), 0)
        self.assertEqual(failures.get("oci-machine"), 2)

        # Tick 3: oci-machine reaches failure threshold (N=3) -> emits node_offline alert
        d3 = StatusDeltaCalculator.compute_deltas(
            self.snap_online, self.snap_offline, failure_counts=failures, failure_threshold=3
        )
        oci_deltas3 = [d for d in d3 if d.target == "oci-machine"]
        self.assertEqual(len(oci_deltas3), 1)
        self.assertEqual(oci_deltas3[0].category, "node_offline")
        self.assertEqual(oci_deltas3[0].severity, "critical")

    def test_transient_failure_recovery_emits_no_noise(self) -> None:
        failures: Dict[str, int] = {}

        # Tick 1: Blip (fails once)
        StatusDeltaCalculator.compute_deltas(
            self.snap_online, self.snap_offline, failure_counts=failures, failure_threshold=3
        )
        self.assertEqual(failures.get("oci-machine"), 1)

        # Tick 2: Recovers back online before reaching threshold
        d2 = StatusDeltaCalculator.compute_deltas(
            self.snap_offline, self.snap_online, failure_counts=failures, failure_threshold=3
        )
        # Should not emit spurious node_online because no offline alert was ever dispatched
        oci_online = [d for d in d2 if d.target == "oci-machine"]
        self.assertEqual(len(oci_online), 0)
        self.assertEqual(failures.get("oci-machine"), 0)

    def test_personal_laptop_sleep_is_benign_without_delay(self) -> None:
        failures: Dict[str, int] = {}
        deltas = StatusDeltaCalculator.compute_deltas(
            self.snap_online, self.snap_offline, failure_counts=failures, failure_threshold=3
        )
        laptop_deltas = [d for d in deltas if d.target == "personal-laptop"]
        self.assertEqual(len(laptop_deltas), 1)
        self.assertEqual(laptop_deltas[0].category, "workstation_sleep")
        self.assertTrue(laptop_deltas[0].benign)
        self.assertEqual(laptop_deltas[0].severity, "info")

    def test_nose_sentinel_compute_status_deltas_with_hysteresis(self) -> None:
        sentinel = NoseSentinel()
        # 1st call -> suppressed
        d1 = sentinel.compute_status_deltas_with_hysteresis(self.snap_online, self.snap_offline)
        oci1 = [d for d in d1 if d.target == "oci-machine"]
        self.assertEqual(len(oci1), 0)

        # 2nd call -> suppressed
        d2 = sentinel.compute_status_deltas_with_hysteresis(self.snap_online, self.snap_offline)
        oci2 = [d for d in d2 if d.target == "oci-machine"]
        self.assertEqual(len(oci2), 0)

        # 3rd call -> alert
        d3 = sentinel.compute_status_deltas_with_hysteresis(self.snap_online, self.snap_offline)
        oci3 = [d for d in d3 if d.target == "oci-machine"]
        self.assertEqual(len(oci3), 1)
        self.assertEqual(oci3[0].category, "node_offline")


if __name__ == "__main__":
    unittest.main()
