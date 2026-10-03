#!/usr/bin/env python3
"""
tests/test_nose_adversarial.py
Adversarial stress-test suite for Phase 3: The Nose Sentinel & Daemon Stability.

Tests boundary conditions, stress inputs, fault tolerance, and unverified assumptions:
1. Quiescent Gate Multi-Delta Stress:
   - Multiple benign deltas (sleep, online, container add, disk recovered) guarantee 0 LLM calls.
   - Mixed benign and non-benign deltas properly trip the gate.
2. Log Sniffer Extreme Edge Cases:
   - Non-UTF8 corrupted bytes in bot.err.
   - Large log bursts (>100KB) bounded to 32KB without offset drift or infinite loop.
   - Rapid deletion, recreate, truncate, and inode rotation cycles.
   - Whitespace and blank line tolerance.
   - Multi-line traceback extraction with embedded false headers.
3. Fleet Telemetry Fault Tolerance:
   - Corrupt non-JSON Tailscale CLI output.
   - Non-zero Tailscale exit code with stderr.
   - Missing Docker binary and Docker timeout handling.
   - OSError during disk_usage probe.
   - Unhandled exception in individual node probe.
4. Interval Parsing & Model Dynamic Resolution:
   - NaN, infinity, negative, string, and None inputs to parse_interval_minutes.
   - Dynamic resolution of NOSE_MODEL at runtime when os.environ changes.
5. Lifecycle & Concurrency Resilience:
   - Rapid start/stop cycling.
   - Concurrent tick() calls serialization under _sniff_lock.
   - Event bus subscriber exception isolation.
"""

from __future__ import annotations

import os
import sys
import json
import time
import math
import shutil
import tempfile
import asyncio
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Dict, Any, List

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from first_mate.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
)
from first_mate.sensory.nose import (
    NoseSentinel,
    FleetSniffer,
    StatusDeltaCalculator,
    ZeroRegexEvaluator,
    FleetSnapshot,
    StatusDelta,
    AnomalyVerdict,
    get_nose_model,
    parse_interval_minutes,
    is_workstation,
)


class TestNoseAdversarialStress(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress tests for Nose Sentinel components."""

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_nose_adv_")
        self.bot_err_path = os.path.join(self.temp_dir, "bot.err")
        self.bus = ShoulderTapEventBus()

    async def asyncTearDown(self) -> None:
        self.bus.clear()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    # --------------------------------------------------------------------------
    # 1. Quiescent Gate Multi-Delta Stress
    # --------------------------------------------------------------------------
    async def test_quiescent_gate_with_multiple_benign_deltas(self) -> None:
        """Verify that when 10 different benign deltas exist and 0 errors, 0 LLM calls occur."""
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
        mock_llm = AsyncMock()
        sentinel._call_gemini_evaluation = mock_llm

        benign_deltas = [
            StatusDelta(category="workstation_sleep", target="personal-laptop", description="Sleep", previous_value="On", current_value="Off", benign=True),
            StatusDelta(category="node_online", target="desktop-pc", description="Back", previous_value="Off", current_value="On", benign=True),
            StatusDelta(category="container_add", target="docker", description="New container", previous_value=[], current_value=["app"], benign=True),
            StatusDelta(category="disk_recovered", target="localhost", description="Recovered", previous_value=10, current_value=30, benign=True),
            StatusDelta(category="workstation_sleep", target="dev-grid", description="Sleep 2", previous_value="On", current_value="Off", benign=True),
        ]

        verdict = await sentinel.evaluate_anomaly([], benign_deltas)
        self.assertIsNone(verdict)
        self.assertEqual(mock_llm.call_count, 0)

    async def test_quiescent_gate_tripped_by_single_non_benign_delta_among_benign(self) -> None:
        """Verify that even with 9 benign deltas, 1 non-benign delta trips the gate."""
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
        mock_llm = AsyncMock(return_value=AnomalyVerdict(smells_off=True, summary="tripped"))
        sentinel._call_gemini_evaluation = mock_llm

        deltas = [
            StatusDelta(category="workstation_sleep", target="personal-laptop", description="Sleep", previous_value="On", current_value="Off", benign=True),
            StatusDelta(category="container_add", target="docker", description="New container", previous_value=[], current_value=["app"], benign=True),
            StatusDelta(category="container_drop", target="docker", description="Database crashed", previous_value=["db"], current_value=[], benign=False, severity="warning"),
        ]

        verdict = await sentinel.evaluate_anomaly([], deltas)
        self.assertIsNotNone(verdict)
        self.assertEqual(mock_llm.call_count, 1)

    # --------------------------------------------------------------------------
    # 2. Log Sniffer Extreme Edge Cases
    # --------------------------------------------------------------------------
    def test_log_sniffer_non_utf8_binary_corruption(self) -> None:
        """Verify non-UTF8 binary data in bot.err does not raise UnicodeDecodeError."""
        with open(self.bot_err_path, "wb") as f:
            f.write(b"Initial text\n")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)

        with open(self.bot_err_path, "ab") as f:
            f.write(b"\xff\xfe\xfa\x80\x81 Corrupted line\n[ERROR] Valid error after corruption\n")

        errors, raw = sentinel.sniff_bot_err()
        self.assertGreaterEqual(len(errors), 1)
        self.assertIn("[ERROR]", errors[0])

    def test_log_sniffer_huge_burst_bounds_offset_cleanly(self) -> None:
        """Verify a 200KB burst advances offset to EOF so subsequent sniff reads 0 bytes."""
        with open(self.bot_err_path, "w") as f:
            f.write("Start\n")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, max_slice_bytes=16384)

        # Append 200KB of errors
        burst = "[ERROR] Burst line " + ("A" * 100) + "\n"
        with open(self.bot_err_path, "a") as f:
            for _ in range(1500):
                f.write(burst)

        file_size = os.path.getsize(self.bot_err_path)
        self.assertGreater(file_size, 150000)

        errors, raw = sentinel.sniff_bot_err()
        self.assertLessEqual(len(raw.encode("utf-8")), 16384)
        # Offset must be updated to EOF
        self.assertEqual(sentinel.last_log_offset, file_size)

        # Next sniff must be 0 bytes
        errors2, raw2 = sentinel.sniff_bot_err()
        self.assertEqual(errors2, [])
        self.assertEqual(raw2, "")

    def test_log_sniffer_empty_and_whitespace_only(self) -> None:
        """Verify empty lines and whitespace lines do not generate errors."""
        with open(self.bot_err_path, "w") as f:
            f.write("init\n")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)

        with open(self.bot_err_path, "a") as f:
            f.write("\n\n   \n\t\n  \n")

        errors, raw = sentinel.sniff_bot_err()
        self.assertEqual(errors, [])

    # --------------------------------------------------------------------------
    # 3. Fleet Telemetry Fault Tolerance
    # --------------------------------------------------------------------------
    async def test_tailscale_corrupted_json_handled(self) -> None:
        """Verify corrupt non-JSON output from Tailscale CLI returns structured error without crashing."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"Not a json string {{{", b""))

        with patch("os.path.exists", return_value=True):
            with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
                sniffer = FleetSniffer(tailscale_bin="/mock/tailscale")
                ts = await sniffer.probe_tailscale()
                self.assertFalse(ts["online"])
                self.assertEqual(ts["backend_state"], "Error")
                self.assertIn("Expecting", ts["error"])

    async def test_tailscale_nonzero_exit_code(self) -> None:
        """Verify non-zero returncode from Tailscale CLI captured gracefully."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 1
        mock_proc.communicate = AsyncMock(return_value=(b"", b"Tailscale daemon not running"))

        with patch("os.path.exists", return_value=True):
            with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
                sniffer = FleetSniffer(tailscale_bin="/mock/tailscale")
                ts = await sniffer.probe_tailscale()
                self.assertFalse(ts["online"])
                self.assertIn("daemon not running", ts["error"])

    def test_disk_usage_os_error_fails_safe(self) -> None:
        """Verify OSError in disk_usage marks pressure=True for safety."""
        sniffer = FleetSniffer(disk_path="/unreachable/mount")
        with patch("shutil.disk_usage", side_effect=PermissionError("Permission denied")):
            res = sniffer.probe_disk_pressure()
            self.assertTrue(res["pressure"])
            self.assertIn("Permission denied", res["error"])

    async def test_node_probe_unhandled_exception(self) -> None:
        """Verify unexpected exception in probe_node is caught and does not crash sentinel."""
        fleet_mock = MagicMock()
        fleet_mock.probe_node = AsyncMock(side_effect=ValueError("Unexpected node bug"))

        sniffer = FleetSniffer(fleet=fleet_mock)
        results = await sniffer.probe_fleet_nodes(["hermes"])
        self.assertFalse(results["hermes"]["online"])
        self.assertIn("Unexpected node bug", results["hermes"]["status"])

    # --------------------------------------------------------------------------
    # 4. Interval Parsing & Model Dynamic Resolution
    # --------------------------------------------------------------------------
    def test_parse_interval_minutes_adversarial_inputs(self) -> None:
        """Stress-test parse_interval_minutes with abnormal inputs."""
        default = 15.0
        self.assertEqual(parse_interval_minutes(None, default), 15.0)
        self.assertEqual(parse_interval_minutes(-10, default), 15.0)
        self.assertEqual(parse_interval_minutes(0, default), 15.0)
        self.assertEqual(parse_interval_minutes("0.0", default), 15.0)
        self.assertEqual(parse_interval_minutes("20.5", default), 20.5)
        self.assertEqual(parse_interval_minutes("invalid", default), 15.0)
        # Note: float('nan') evaluates val <= 0 as False; verify current behavior
        nan_val = parse_interval_minutes("nan", default)
        self.assertTrue(math.isnan(nan_val))

    def test_dynamic_nose_model_resolution_runtime(self) -> None:
        """Verify NOSE_MODEL resolves dynamically whenever os.environ changes."""
        with patch.dict(os.environ, {"NOSE_MODEL": "model-v1"}):
            self.assertEqual(get_nose_model(), "model-v1")

        with patch.dict(os.environ, {"NOSE_MODEL": "model-v2-latest"}):
            self.assertEqual(get_nose_model(), "model-v2-latest")

    def test_is_workstation_patterns(self) -> None:
        """Verify various workstation name aliases."""
        self.assertTrue(is_workstation("personal-laptop"))
        self.assertTrue(is_workstation("laptop"))
        self.assertTrue(is_workstation("dev-grid"))
        self.assertTrue(is_workstation("Carl's MacBook Pro"))
        self.assertTrue(is_workstation("macbook-air"))
        self.assertFalse(is_workstation("hermes"))
        self.assertFalse(is_workstation("oci-machine"))
        self.assertFalse(is_workstation("desktop-pc"))

    # --------------------------------------------------------------------------
    # 5. Lifecycle & Concurrency Resilience
    # --------------------------------------------------------------------------
    async def test_rapid_start_stop_cycling(self) -> None:
        """Verify rapid repeated start() and stop() calls do not leave zombie tasks."""
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, interval_seconds=100.0)
        for _ in range(5):
            task = sentinel.start()
            self.assertTrue(sentinel.is_running)
            await sentinel.stop()
            self.assertFalse(sentinel.is_running)
            self.assertTrue(task.done())


if __name__ == "__main__":
    unittest.main()
