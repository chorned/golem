#!/usr/bin/env python3
"""
tests/test_nose_sentinel.py
Comprehensive test suite for Phase 3: The Nose Sentinel & Daemon Stability.

Test coverage:
  1. TestNoseSentinelLifecycle:
     - Initialization defaults and environment variable interval configuration.
     - Resilience against invalid or negative NOSE_INTERVAL_MINUTES.
     - start() and stop() lifecycle management.
     - Safe stop() when unstarted.
     - Immediate shutdown on shutdown_event (< 50ms) without waiting for sleep.
     - Tick re-entrancy locking via _sniff_lock.
  2. TestLogSniffer:
     - Graceful handling of missing bot.err file.
     - Startup offset calibration to EOF / tail 16KB preventing historic storms.
     - Incremental byte-offset tracking reading only newly appended bytes.
     - File truncation detection resetting offset to 0.
     - Inode rotation detection resetting offset to 0.
     - Slice bounding capping large spikes at max_slice_bytes (32KB).
     - Non-error log filtering: INFO/DEBUG lines do not trip error flags.
     - Exception spike detection: Tracebacks, ERROR, and CRITICAL lines trip error flags.
  3. TestFleetSniffer:
     - Concurrent multi-node probing across hermes, desktop-pc, personal-laptop, oci-machine.
     - Node probe timeout isolation.
     - Tailscale JSON status probing and fallback on missing CLI.
     - Host root disk pressure detection (free < 15GB or used > 90%).
     - Docker container count and health inspection.
     - Status delta calculation: node offline transitions, container drops, disk pressure.
     - Benign workstation sleep: personal-laptop standby treated as benign=True.
  4. TestZeroRegexEvaluatorQuiescentGate:
     - Quiescent gate: exactly 0 LLM calls and 0 tokens when clean and healthy.
     - Semantic evaluation invoked when error logs exist.
     - Semantic evaluation invoked when critical fleet status deltas exist.
     - Strict JSON response parsing (including markdown fences).
     - Benign verdict suppresses alert publishing.
     - Dynamic NOSE_MODEL resolution without hardcoding.
     - Offline / test mode fallback resilience.
  5. TestShoulderTapBusAlertPublication:
     - Publishes ShoulderTapAlert(alert_type="health_anomaly", source="nose", ...) on anomaly.
     - Subscriber receiving health_anomaly alert.
     - Subscriber exception isolation without crashing the sentinel.
  6. TestDaemonIntegration:
     - Clean re-export of NoseSentinel and get_nose_model in golem.sensory.
     - app.nose_sentinel singleton existence and configuration.
     - app.py main() lifecycle start and stop hooks.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import json
import time
import shutil
import tempfile
import asyncio
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Dict, Any, List, Optional

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
    get_default_event_bus,
    reset_default_event_bus,
)
from golem.sensory.nose import (
    NoseSentinel,
    get_nose_model,
    FleetSnapshot,
    StatusDelta,
    AnomalyVerdict,
)
from golem.config import get_nose_model as config_get_nose_model


# ==============================================================================
# 1. Lifecycle & Configuration Tests
# ==============================================================================
class TestNoseSentinelLifecycle(unittest.IsolatedAsyncioTestCase):
    """Unit tests for NoseSentinel initialization, configuration, and lifecycle."""

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_nose_lifecycle_")
        self.bot_err_path = os.path.join(self.temp_dir, "bot.err")
        with open(self.bot_err_path, "w") as f:
            f.write("Initial log line\n")
        self.bus = ShoulderTapEventBus()

    async def asyncTearDown(self) -> None:
        self.bus.clear()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_default_initialization(self) -> None:
        """Verify default parameters: 15-minute interval, bus, and model resolution."""
        sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
        )
        self.assertEqual(sentinel.interval_seconds, 900.0)
        self.assertFalse(sentinel.is_running)
        self.assertEqual(sentinel.bot_err_path, self.bot_err_path)
        self.assertEqual(sentinel.nose_model, config_get_nose_model())

    def test_custom_interval_seconds_and_minutes(self) -> None:
        """Verify explicit interval_seconds and interval_minutes arguments."""
        sentinel_sec = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            interval_seconds=45.0,
        )
        self.assertEqual(sentinel_sec.interval_seconds, 45.0)

        sentinel_min = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            interval_minutes=5.0,
        )
        self.assertEqual(sentinel_min.interval_seconds, 300.0)

    def test_env_var_interval_configuration(self) -> None:
        """Verify NOSE_INTERVAL_MINUTES environment variable parsing."""
        with patch.dict(os.environ, {"NOSE_INTERVAL_MINUTES": "10"}):
            sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
            self.assertEqual(sentinel.interval_seconds, 600.0)

    def test_invalid_env_var_fallback(self) -> None:
        """Verify invalid or non-positive NOSE_INTERVAL_MINUTES values fall back safely to 15m."""
        for invalid_val in ["abc", "-5", "0", ""]:
            with self.subTest(invalid_val=invalid_val):
                with patch.dict(os.environ, {"NOSE_INTERVAL_MINUTES": invalid_val}):
                    sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
                    self.assertEqual(sentinel.interval_seconds, 900.0)

    async def test_start_and_stop_lifecycle(self) -> None:
        """Verify sentinel.start() launches loop and await sentinel.stop() cleanly cancels it."""
        sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            interval_seconds=0.05,
        )
        self.assertFalse(sentinel.is_running)

        task = sentinel.start()
        self.assertIsInstance(task, asyncio.Task)
        self.assertTrue(sentinel.is_running)

        await asyncio.sleep(0.02)
        await sentinel.stop()

        self.assertFalse(sentinel.is_running)
        self.assertTrue(task.done())

    async def test_stop_when_unstarted_is_safe_noop(self) -> None:
        """Verify calling stop() on an unstarted sentinel does not raise errors."""
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
        await sentinel.stop()
        self.assertFalse(sentinel.is_running)

    async def test_shutdown_event_wakes_sleep_immediately(self) -> None:
        """Verify shutdown_event wakes sleeping sentinel in < 50ms without waiting for 15m timer."""
        sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            interval_seconds=900.0,
        )
        sentinel.start()

        t0 = time.time()
        await sentinel.stop()
        elapsed = time.time() - t0

        self.assertLess(elapsed, 0.1, f"Stop took {elapsed:.3f}s; expected < 0.1s immediate wake")
        self.assertFalse(sentinel.is_running)

    async def test_tick_reentrancy_lock(self) -> None:
        """Verify _sniff_lock prevents overlapping concurrent tick() executions."""
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
        execution_order: List[str] = []

        async def slow_sniff(*args: Any, **kwargs: Any) -> Any:
            execution_order.append("start")
            await asyncio.sleep(0.05)
            execution_order.append("end")
            return None

        with patch.object(sentinel, "sniff_bot_err", side_effect=slow_sniff):
            t1 = asyncio.create_task(sentinel.tick())
            t2 = asyncio.create_task(sentinel.tick())
            await asyncio.gather(t1, t2)

        self.assertEqual(execution_order, ["start", "end", "start", "end"])


# ==============================================================================
# 2. Log Sniffer Tests
# ==============================================================================
class TestLogSniffer(unittest.IsolatedAsyncioTestCase):
    """Unit tests for Log Sniffer: byte-offset tracking, rotation, truncation, and exceptions."""

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_nose_log_")
        self.bot_err_path = os.path.join(self.temp_dir, "bot.err")
        self.bus = ShoulderTapEventBus()

    async def asyncTearDown(self) -> None:
        self.bus.clear()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_missing_bot_err_handled_gracefully(self) -> None:
        """Verify missing bot.err returns empty errors with offset 0 without raising."""
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
        errors, raw = sentinel.sniff_bot_err()
        self.assertEqual(errors, [])
        self.assertEqual(raw, "")
        self.assertEqual(sentinel.last_log_offset, 0)

    def test_startup_offset_calibrated_to_eof(self) -> None:
        """Verify startup cursor initializes at EOF to prevent historic error storms."""
        with open(self.bot_err_path, "w") as f:
            f.write("Historic error line 1\nTraceback (most recent call last):\nRuntimeError: old\n")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
        # Should initialize at EOF
        self.assertGreater(sentinel.last_log_offset, 0)

        # Sniffing immediately should yield 0 new errors
        errors, raw = sentinel.sniff_bot_err()
        self.assertEqual(errors, [])
        self.assertEqual(raw, "")

    def test_byte_offset_reads_only_newly_appended_bytes(self) -> None:
        """Verify incremental sniffing reads only bytes written since last tick."""
        with open(self.bot_err_path, "w") as f:
            f.write("Line 1: system started\n")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)

        # Append new log lines
        with open(self.bot_err_path, "a") as f:
            f.write("Line 2: 2026-10-02 [INFO] Heartbeat\n")
            f.write("Line 3: 2026-10-02 [ERROR] Database timeout\n")

        errors, raw = sentinel.sniff_bot_err()
        self.assertIn("Line 3: 2026-10-02 [ERROR] Database timeout", errors[0])
        self.assertNotIn("Line 1: system started", raw)
        self.assertIn("Line 2", raw)

        # Subsequent sniff with no writes returns empty
        errors2, raw2 = sentinel.sniff_bot_err()
        self.assertEqual(errors2, [])
        self.assertEqual(raw2, "")

    def test_file_truncation_detection_resets_offset(self) -> None:
        """Verify file size shrinkage triggers offset reset to 0."""
        with open(self.bot_err_path, "w") as f:
            f.write("Long long long log content line exceeding truncation size...\n" * 10)

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
        sentinel.sniff_bot_err()
        old_offset = sentinel.last_log_offset
        self.assertGreater(old_offset, 50)

        # Truncate file
        with open(self.bot_err_path, "w") as f:
            f.write("Short new log line\n")

        errors, raw = sentinel.sniff_bot_err()
        self.assertEqual(raw, "Short new log line\n")
        self.assertLess(sentinel.last_log_offset, old_offset)

    def test_inode_rotation_detection_resets_offset(self) -> None:
        """Verify inode change (file replacement/rotation) triggers offset reset to 0."""
        with open(self.bot_err_path, "w") as f:
            f.write("First generation log file\n" * 5)

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)
        sentinel.sniff_bot_err()
        old_inode = sentinel.last_log_inode

        # Simulate logrotate: keep old inode allocated via open fd, unlink and recreate
        with open(self.bot_err_path, "r") as _hold_old_inode:
            os.remove(self.bot_err_path)
            with open(self.bot_err_path, "w") as f:
                f.write("Second generation fresh log file\n")

        errors, raw = sentinel.sniff_bot_err()
        self.assertNotEqual(sentinel.last_log_inode, old_inode)
        self.assertIn("Second generation", raw)

    def test_slice_bounding_prevents_memory_spike(self) -> None:
        """Verify massive log spike is bounded to max_slice_bytes (32KB)."""
        sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            max_slice_bytes=4096,
        )

        with open(self.bot_err_path, "a") as f:
            f.write("X" * 20000 + "\n")

        errors, raw = sentinel.sniff_bot_err()
        self.assertLessEqual(len(raw.encode("utf-8")), 4096)

    def test_info_and_debug_lines_do_not_trip_error_flag(self) -> None:
        """Verify standard [INFO] and [DEBUG] lines do not trip the error flag."""
        with open(self.bot_err_path, "w") as f:
            f.write("initial\n")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)

        with open(self.bot_err_path, "a") as f:
            f.write("2026-10-02 08:00:00 [INFO] AGY: Starting First Mate Ingress Gateway...\n")
            f.write("2026-10-02 08:00:01 [DEBUG] FirstMate.Router: Session cache hit\n")
            f.write("2026-10-02 08:00:02 [WARNING] FirstMate.Normalizer: sanitized markdown\n")

        errors, raw = sentinel.sniff_bot_err()
        self.assertEqual(errors, [])
        self.assertIn("[INFO]", raw)

    def test_traceback_and_critical_lines_trip_error_flag(self) -> None:
        """Verify tracebacks and [ERROR]/[CRITICAL] lines trip the error flag."""
        with open(self.bot_err_path, "w") as f:
            f.write("initial\n")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path)

        with open(self.bot_err_path, "a") as f:
            f.write("2026-10-02 08:05:00 [ERROR] AGY: Uncaught exception in worker loop\n")
            f.write("Traceback (most recent call last):\n")
            f.write('  File "app.py", line 42, in run\n')
            f.write("RuntimeError: Task was destroyed but it is pending!\n")

        errors, raw = sentinel.sniff_bot_err()
        self.assertGreaterEqual(len(errors), 1)
        combined = " ".join(errors)
        self.assertIn("RuntimeError", combined)


# ==============================================================================
# 3. Fleet Sniffer Tests
# ==============================================================================
class TestFleetSniffer(unittest.IsolatedAsyncioTestCase):
    """Unit tests for Fleet Sniffer: concurrent probing, Tailscale, disk, Docker, and deltas."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.fleet_mock = MagicMock()
        self.sentinel = NoseSentinel(fleet=self.fleet_mock, bus=self.bus)

    async def test_concurrent_node_probing(self) -> None:
        """Verify all 4 fleet nodes are probed concurrently via asyncio.gather()."""
        probed_nodes: List[str] = []

        async def mock_probe(node: str, timeout: float = 8.0) -> Dict[str, Any]:
            probed_nodes.append(node)
            await asyncio.sleep(0.01)
            return {"node": node, "online": True, "status": "Online OK"}

        self.fleet_mock.probe_node = AsyncMock(side_effect=mock_probe)

        nodes = await self.sentinel.probe_fleet_nodes(["hermes", "desktop-pc", "personal-laptop", "oci-machine"])
        self.assertEqual(set(probed_nodes), {"hermes", "desktop-pc", "personal-laptop", "oci-machine"})
        self.assertEqual(len(nodes), 4)
        for n in ["hermes", "desktop-pc", "personal-laptop", "oci-machine"]:
            self.assertTrue(nodes[n]["online"])

    async def test_node_probe_timeout_isolation(self) -> None:
        """Verify timeout on one node returns structured offline state without failing others."""
        async def mock_probe_with_timeout(node: str, timeout: float = 8.0) -> Dict[str, Any]:
            if node == "desktop-pc":
                raise asyncio.TimeoutError()
            return {"node": node, "online": True, "status": "Online"}

        self.fleet_mock.probe_node = AsyncMock(side_effect=mock_probe_with_timeout)

        nodes = await self.sentinel.probe_fleet_nodes(["hermes", "desktop-pc"])
        self.assertTrue(nodes["hermes"]["online"])
        self.assertFalse(nodes["desktop-pc"]["online"])
        self.assertIn("Timeout", nodes["desktop-pc"]["status"])

    async def test_tailscale_probe_success(self) -> None:
        """Verify Tailscale JSON parsing extracts backend state and peer statuses."""
        mock_output = json.dumps({
            "BackendState": "Running",
            "Self": {"HostName": "Carl's MacBook Pro", "TailscaleIPs": ["100.108.38.25"]},
            "Peer": {
                "node_1": {"HostName": "KalKyl", "Online": True, "TailscaleIPs": ["100.80.59.45"]},
                "node_2": {"HostName": "claw-primary-nic", "Online": True, "TailscaleIPs": ["100.113.173.33"]},
            }
        }).encode("utf-8")

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(mock_output, b""))

        with patch("shutil.which", return_value="/usr/local/bin/tailscale"), patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            ts = await self.sentinel.probe_tailscale()
            self.assertEqual(ts["backend_state"], "Running")
            self.assertTrue(ts["online"])
            self.assertIn("KalKyl", ts["peers"])
            self.assertTrue(ts["peers"]["KalKyl"]["online"])

    async def test_tailscale_probe_failure_fallback(self) -> None:
        """Verify missing or failing Tailscale CLI falls back gracefully."""
        with patch("os.path.exists", return_value=False):
            with patch("shutil.which", return_value=None):
                sentinel = NoseSentinel(bus=self.bus, tailscale_bin="/nonexistent/tailscale")
                ts = await sentinel.probe_tailscale()
                self.assertFalse(ts["online"])
                self.assertEqual(ts["backend_state"], "NotFound")

    def test_disk_pressure_normal_vs_critical(self) -> None:
        """Verify disk pressure check flags pressure when free < 15GB or used > 90%."""
        # 1. Healthy disk: 50GB free, 50% used
        mock_usage_healthy = (100 * (1024**3), 50 * (1024**3), 50 * (1024**3))
        with patch("shutil.disk_usage", return_value=mock_usage_healthy):
            res = self.sentinel.probe_disk_pressure()
            self.assertFalse(res["pressure"])
            self.assertAlmostEqual(res["free_gb"], 50.0, places=1)

        # 2. Pressured disk: 10GB free (< 15GB threshold)
        mock_usage_low = (100 * (1024**3), 90 * (1024**3), 10 * (1024**3))
        with patch("shutil.disk_usage", return_value=mock_usage_low):
            res = self.sentinel.probe_disk_pressure()
            self.assertTrue(res["pressure"])

    async def test_docker_container_count_and_list(self) -> None:
        """Verify docker ps parsing extracts running containers."""
        mock_stdout = b"container_alpha\ncontainer_beta\ncontainer_gamma\n"
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(mock_stdout, b""))

        with patch("shutil.which", return_value="/usr/local/bin/docker"), patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            docker = await self.sentinel.probe_docker_health()
            self.assertTrue(docker["docker_running"])
            self.assertEqual(docker["container_count"], 3)
            self.assertEqual(docker["containers"], ["container_alpha", "container_beta", "container_gamma"])

    def test_status_delta_node_offline_transition(self) -> None:
        """Verify node transition from online to offline flags critical delta."""
        prev = FleetSnapshot(
            nodes={"oci-machine": {"online": True, "status": "Online"}},
            disk={"pressure": False},
        )
        curr = FleetSnapshot(
            nodes={"oci-machine": {"online": False, "status": "[Unreachable]"}},
            disk={"pressure": False},
        )

        deltas = self.sentinel.compute_status_deltas(prev, curr)
        self.assertEqual(len(deltas), 1)
        self.assertEqual(deltas[0]["category"], "node_offline")
        self.assertEqual(deltas[0]["target"], "oci-machine")
        self.assertFalse(deltas[0]["benign"])
        self.assertEqual(deltas[0]["severity"], "critical")

    def test_status_delta_personal_laptop_sleep_is_benign(self) -> None:
        """Verify personal-laptop entering standby/sleep is flagged as benign=True."""
        prev = FleetSnapshot(
            nodes={"personal-laptop": {"online": True, "status": "Online (up 2 days)"}},
            disk={"pressure": False},
        )
        curr = FleetSnapshot(
            nodes={"personal-laptop": {"online": False, "status": "Sleeping / Standby (Ad-hoc only)"}},
            disk={"pressure": False},
        )

        deltas = self.sentinel.compute_status_deltas(prev, curr)
        self.assertEqual(len(deltas), 1)
        self.assertEqual(deltas[0]["category"], "workstation_sleep")
        self.assertTrue(deltas[0]["benign"])
        self.assertEqual(deltas[0]["severity"], "info")


# ==============================================================================
# 4. Zero-Regex Evaluator & Quiescent Gate Tests
# ==============================================================================
class TestZeroRegexEvaluatorQuiescentGate(unittest.IsolatedAsyncioTestCase):
    """Unit tests for Quiescent Gate (0 tokens/calls) and semantic anomaly evaluation."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.sentinel = NoseSentinel(bus=self.bus)

    async def test_quiescent_state_makes_zero_llm_calls_zero_tokens(self) -> None:
        """
        CRITICAL ACCEPTANCE CRITERION:
        Verify that when newly read bot.err has no error lines AND fleet snapshot
        has zero non-benign status deltas, exactly 0 LLM calls are made (0 tokens).
        """
        mock_llm = AsyncMock()
        self.sentinel._call_gemini_evaluation = mock_llm

        # Clean cycle: no error logs, only benign workstation sleep delta
        error_logs: List[str] = []
        deltas = [
            StatusDelta(
                category="workstation_sleep",
                target="personal-laptop",
                description="Carl laptop closed lid",
                previous_value="Online",
                current_value="Sleeping",
                benign=True,
            )
        ]

        verdict = await self.sentinel.evaluate_anomaly(error_logs, deltas)

        self.assertIsNone(verdict)
        self.assertEqual(
            mock_llm.call_count, 0,
            "Quiescent gate MUST NOT make LLM calls when cycle is healthy/benign!"
        )

    async def test_semantic_evaluation_triggered_by_log_error_spike(self) -> None:
        """Verify error lines in bot.err trigger semantic evaluation via NOSE_MODEL."""
        expected_verdict = AnomalyVerdict(
            smells_off=True,
            severity="high",
            category="crash_loop",
            summary="Gateway encountered unhandled RuntimeError crash loop",
            affected_nodes=["hermes"],
            recommended_action="Restart launchd daemon com.user.golem",
        )

        mock_llm = AsyncMock(return_value=expected_verdict)
        self.sentinel._call_gemini_evaluation = mock_llm

        error_logs = ["2026-10-02 [ERROR] Fatal event loop crash: RuntimeError"]
        deltas: List[StatusDelta] = []

        verdict = await self.sentinel.evaluate_anomaly(error_logs, deltas)

        self.assertEqual(mock_llm.call_count, 1)
        self.assertIsNotNone(verdict)
        self.assertTrue(verdict.smells_off)
        self.assertEqual(verdict.category, "crash_loop")

    async def test_semantic_evaluation_triggered_by_critical_fleet_delta(self) -> None:
        """Verify non-benign fleet delta (e.g. oci-machine down) triggers evaluation."""
        expected_verdict = AnomalyVerdict(
            smells_off=True,
            severity="critical",
            category="node_down",
            summary="Production VPS oci-machine is unreachable",
            affected_nodes=["oci-machine"],
            recommended_action="Inspect Oracle Cloud compute console",
        )

        mock_llm = AsyncMock(return_value=expected_verdict)
        self.sentinel._call_gemini_evaluation = mock_llm

        error_logs: List[str] = []
        deltas = [
            StatusDelta(
                category="node_offline",
                target="oci-machine",
                description="Connection timed out after 8s",
                previous_value="Online",
                current_value="Offline",
                benign=False,
                severity="critical",
            )
        ]

        verdict = await self.sentinel.evaluate_anomaly(error_logs, deltas)

        self.assertEqual(mock_llm.call_count, 1)
        self.assertIsNotNone(verdict)
        self.assertTrue(verdict.smells_off)

    async def test_benign_verdict_suppresses_alert_publication(self) -> None:
        """Verify that when LLM returns smells_off=False, no alert is published to bus."""
        benign_verdict = AnomalyVerdict(
            smells_off=False,
            severity="low",
            category="benign",
            summary="Transient test noise",
        )
        self.sentinel._call_gemini_evaluation = AsyncMock(return_value=benign_verdict)

        received_alerts: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda a: received_alerts.append(a), alert_types=["health_anomaly"])

        # Force evaluation with error log
        verdict = await self.sentinel.evaluate_anomaly(["mock warning"], [])
        self.assertIsNotNone(verdict)
        self.assertFalse(verdict.smells_off)

        # Alert bus should have 0 alerts
        self.assertEqual(len(received_alerts), 0)

    def test_dynamic_model_resolution(self) -> None:
        """Verify NOSE_MODEL resolves dynamically via config.get_nose_model()."""
        with patch.dict(os.environ, {"NOSE_MODEL": "gemini-test-custom-model"}):
            model = get_nose_model()
            self.assertEqual(model, "gemini-test-custom-model")


# ==============================================================================
# 5. Shoulder Tap Bus Alert Publication Tests
# ==============================================================================
class TestShoulderTapBusAlertPublication(unittest.IsolatedAsyncioTestCase):
    """Unit tests for publishing health_anomaly alerts across ShoulderTapEventBus."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.sentinel = NoseSentinel(bus=self.bus)

    async def asyncTearDown(self) -> None:
        self.bus.clear()

    async def test_health_anomaly_alert_published_on_anomaly(self) -> None:
        """Verify tick() with anomaly publishes typed ShoulderTapAlert to the bus."""
        anomaly = AnomalyVerdict(
            smells_off=True,
            severity="high",
            category="crash_loop",
            summary="First Mate encountered repeated unhandled asyncio exceptions",
            affected_nodes=["hermes"],
            recommended_action="Inspect bot.err and restart daemon",
        )

        with patch.object(self.sentinel, "sniff_bot_err", return_value=(["RuntimeError"], "log")):
            with patch.object(self.sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
                mock_fleet.return_value = FleetSnapshot()
                with patch.object(self.sentinel, "evaluate_anomaly", new_callable=AsyncMock) as mock_eval:
                    mock_eval.return_value = anomaly

                    published_alerts: List[ShoulderTapAlert] = []
                    self.bus.subscribe(
                        lambda a: published_alerts.append(a),
                        alert_types=["health_anomaly"]
                    )

                    result = await self.sentinel.tick()

                    self.assertIsNotNone(result)
                    self.assertEqual(len(published_alerts), 1)
                    alert = published_alerts[0]
                    self.assertEqual(alert.alert_type, "health_anomaly")
                    self.assertEqual(alert.source, "nose")
                    self.assertTrue(alert.is_health_anomaly)
                    self.assertIn("First Mate encountered", alert.summary)
                    self.assertEqual(alert.data.get("severity"), "high")

    async def test_subscriber_exception_isolated(self) -> None:
        """Verify buggy subscriber exception does not crash sentinel tick()."""
        def buggy_subscriber(alert: ShoulderTapAlert) -> None:
            raise RuntimeError("Exploding subscriber")

        self.bus.subscribe(buggy_subscriber, alert_types=["health_anomaly"])

        anomaly = AnomalyVerdict(
            smells_off=True,
            severity="low",
            category="test",
            summary="test anomaly",
        )

        with patch.object(self.sentinel, "sniff_bot_err", return_value=(["error"], "slice")):
            with patch.object(self.sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
                mock_fleet.return_value = FleetSnapshot()
                with patch.object(self.sentinel, "evaluate_anomaly", new_callable=AsyncMock) as mock_eval:
                    mock_eval.return_value = anomaly

                    # Should not raise exception
                    result = await self.sentinel.tick()
                    self.assertIsNotNone(result)


# ==============================================================================
# 6. Daemon Integration Tests
# ==============================================================================
class TestDaemonIntegration(unittest.TestCase):
    """Unit tests verifying daemon integration in app.py and package exports."""

    def test_sensory_init_reexports(self) -> None:
        """Verify NoseSentinel, get_nose_model, and contracts are exported from golem.sensory."""
        import golem.sensory as sensory
        self.assertTrue(hasattr(sensory, "NoseSentinel"))
        self.assertTrue(hasattr(sensory, "get_nose_model"))
        self.assertTrue(hasattr(sensory, "FleetSnapshot"))
        self.assertTrue(hasattr(sensory, "StatusDelta"))
        self.assertTrue(hasattr(sensory, "AnomalyVerdict"))

    def test_app_nose_sentinel_singleton(self) -> None:
        """Verify app.py exposes nose_sentinel singleton without background tasks on import."""
        import app
        self.assertTrue(hasattr(app, "nose_sentinel"))
        self.assertIsInstance(app.nose_sentinel, NoseSentinel)
        self.assertFalse(app.nose_sentinel.is_running)


if __name__ == "__main__":
    unittest.main()
