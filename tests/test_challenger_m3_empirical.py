#!/usr/bin/env python3
"""
tests/test_challenger_m3_empirical.py
Empirical Challenger Test Suite for Phase 3: The Nose Sentinel & Daemon Stability.

Covers:
1. Zero-Token Quiescent Gate (repeated cycles, log noise filtering, workstation sleep, benign transitions)
2. Status Delta Calculator (laptop sleep benign vs VPS critical, root disk pressure thresholds, container drops)
3. Tailscale Probe Failures (corrupted JSON, non-zero exit code, timeout, state transitions)
4. Shoulder Tap Bus & Ingress Gateway Handshake (publication, subscriber exception isolation, IngressGateway delivery)
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
)
from golem.sensory.nose import (
    NoseSentinel,
    FleetSniffer,
    StatusDeltaCalculator,
    ZeroRegexEvaluator,
    FleetSnapshot,
    StatusDelta,
    AnomalyVerdict,
    is_workstation,
)
from golem.gateway.ingress import IngressGateway


class TestZeroTokenQuiescentGateEmpirical(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: verify strictly 0 tokens/calls under quiescent conditions."""

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_challenger_quiescent_")
        self.bot_err_path = os.path.join(self.temp_dir, "bot.err")
        with open(self.bot_err_path, "w") as f:
            f.write("2026-10-02 00:00:00 [INFO] System boot\n")
        self.bus = ShoulderTapEventBus()
        self.sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
        )

    async def asyncTearDown(self) -> None:
        self.bus.clear()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_repeated_quiescent_ticks_strictly_zero_llm_zero_tokens(self) -> None:
        """Verify that over 10 consecutive ticks on a clean system, exactly 0 LLM calls are made."""
        mock_eval = AsyncMock()
        self.sentinel._call_gemini_evaluation = mock_eval

        # Fixed healthy fleet snapshot
        healthy_snapshot = FleetSnapshot(
            nodes={
                "hermes": {"online": True, "status": "Online"},
                "desktop-pc": {"online": True, "status": "Online"},
                "personal-laptop": {"online": True, "status": "Online"},
                "oci-machine": {"online": True, "status": "Online"},
            },
            disk={"pressure": False, "free_gb": 45.0, "percent_used": 60.0},
            docker={"docker_running": True, "containers": ["web", "db"]},
            tailscale={"backend_state": "Running", "online": True},
        )

        with patch.object(self.sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
            mock_fleet.return_value = healthy_snapshot
            for i in range(10):
                res = await self.sentinel.tick()
                self.assertIsNone(res, f"Tick {i+1} returned non-None alert under quiescent state")

        self.assertEqual(
            mock_eval.call_count, 0,
            f"Zero-token gate violated! Expected strictly 0 LLM calls, but got {mock_eval.call_count}"
        )

    async def test_massive_non_error_log_stream_strictly_zero_llm(self) -> None:
        """Verify that 1,000 non-error log lines ([INFO], [DEBUG], [WARNING]) consume strictly 0 LLM calls."""
        mock_eval = AsyncMock()
        self.sentinel._call_gemini_evaluation = mock_eval

        healthy_snapshot = FleetSnapshot(
            nodes={"hermes": {"online": True, "status": "Online"}},
            disk={"pressure": False},
            docker={"docker_running": True, "containers": []},
            tailscale={"backend_state": "Running", "online": True},
        )

        with patch.object(self.sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
            mock_fleet.return_value = healthy_snapshot
            # Baseline tick
            await self.sentinel.tick()

            # Append 1,000 noisy operational lines
            with open(self.bot_err_path, "a") as f:
                for i in range(1000):
                    f.write(f"2026-10-02 08:00:{i%60:02d} [INFO] Background tick iteration {i} OK\n")
                    f.write(f"2026-10-02 08:00:{i%60:02d} [DEBUG] Buffer cache hit {i}\n")
                    f.write(f"2026-10-02 08:00:{i%60:02d} [WARNING] High latency in DNS probe {i}ms\n")

            res = await self.sentinel.tick()
            self.assertIsNone(res)
            self.assertEqual(
                mock_eval.call_count, 0,
                f"Noise logs tripped LLM! Expected 0 calls, got {mock_eval.call_count}"
            )

    async def test_workstation_sleep_cycle_repeated_strictly_zero_llm(self) -> None:
        """Verify that personal-laptop repeatedly cycling online <-> sleeping generates 0 LLM calls."""
        mock_eval = AsyncMock()
        self.sentinel._call_gemini_evaluation = mock_eval

        state_sequence = [True, False, True, False, True, False]

        for i, online in enumerate(state_sequence):
            snap = FleetSnapshot(
                nodes={
                    "personal-laptop": {
                        "online": online,
                        "status": "Online" if online else "Sleeping / Standby",
                    },
                    "hermes": {"online": True, "status": "Online"},
                },
                disk={"pressure": False},
                docker={"docker_running": True, "containers": []},
                tailscale={"backend_state": "Running", "online": True},
            )

            with patch.object(self.sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
                mock_fleet.return_value = snap
                res = await self.sentinel.tick()
                self.assertIsNone(res, f"Workstation cycle {i} triggered alert!")

        self.assertEqual(
            mock_eval.call_count, 0,
            f"Workstation transitions triggered LLM! Expected 0 calls, got {mock_eval.call_count}"
        )

    async def test_benign_subsystem_transitions_strictly_zero_llm(self) -> None:
        """Verify that benign deltas (container_add, disk_recovered) generate strictly 0 LLM calls."""
        mock_eval = AsyncMock()
        self.sentinel._call_gemini_evaluation = mock_eval

        # Initial snapshot: 1 container, pressured disk
        snap1 = FleetSnapshot(
            nodes={"hermes": {"online": True, "status": "Online"}},
            disk={"pressure": True, "free_gb": 10.0, "percent_used": 92.0},
            docker={"docker_running": True, "containers": ["alpha"]},
            tailscale={"backend_state": "Running", "online": True},
        )
        self.sentinel.last_snapshot = snap1

        # Second snapshot: 2 containers (alpha, beta), disk recovered (40GB free)
        snap2 = FleetSnapshot(
            nodes={"hermes": {"online": True, "status": "Online"}},
            disk={"pressure": False, "free_gb": 40.0, "percent_used": 60.0},
            docker={"docker_running": True, "containers": ["alpha", "beta"]},
            tailscale={"backend_state": "Running", "online": True},
        )

        deltas = self.sentinel.compute_status_deltas(snap1, snap2)
        # Verify both deltas are categorized as benign
        self.assertTrue(all(d["benign"] for d in deltas), "Expected all deltas to be benign")

        with patch.object(self.sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
            mock_fleet.return_value = snap2
            res = await self.sentinel.tick()
            self.assertIsNone(res)

        self.assertEqual(
            mock_eval.call_count, 0,
            f"Benign deltas triggered LLM! Expected 0 calls, got {mock_eval.call_count}"
        )


class TestStatusDeltaCalculatorEmpirical(unittest.TestCase):
    """Empirical challenge: verify delta calculator categorization and anomaly triggering."""

    def setUp(self) -> None:
        self.calc = StatusDeltaCalculator()

    def test_workstation_identification_coverage(self) -> None:
        """Verify all valid workstation aliases are recognized as workstations."""
        workstation_aliases = [
            "personal-laptop", "PERSONAL-LAPTOP", " personal-laptop ",
            "laptop", "Laptop",
            "dev-grid", "DEV-GRID",
            "Carl's MacBook Pro", "macbook-air", "macbook",
        ]
        non_workstations = [
            "hermes", "desktop-pc", "oci-machine", "cloud-ops",
            "compute-grid", "homelab-ops", "localhost",
        ]

        for alias in workstation_aliases:
            self.assertTrue(is_workstation(alias), f"Expected '{alias}' to be recognized as workstation")

        for non in non_workstations:
            self.assertFalse(is_workstation(non), f"Expected '{non}' NOT to be recognized as workstation")

    def test_node_offline_severities_laptop_sleep_vs_vps_down(self) -> None:
        """Verify laptop sleep is benign, VPS offline is critical, desktop-pc offline is warning."""
        prev = FleetSnapshot(
            nodes={
                "personal-laptop": {"online": True, "status": "Online"},
                "oci-machine": {"online": True, "status": "Online"},
                "desktop-pc": {"online": True, "status": "Online"},
                "hermes": {"online": True, "status": "Online"},
            },
            disk={"pressure": False},
        )
        curr = FleetSnapshot(
            nodes={
                "personal-laptop": {"online": False, "status": "Sleeping"},
                "oci-machine": {"online": False, "status": "Timeout"},
                "desktop-pc": {"online": False, "status": "Unreachable"},
                "hermes": {"online": False, "status": "Dead"},
            },
            disk={"pressure": False},
        )

        deltas = self.calc.compute_deltas(prev, curr)
        delta_map = {d["target"]: d for d in deltas}

        # 1. personal-laptop: benign=True, severity="info"
        self.assertIn("personal-laptop", delta_map)
        self.assertTrue(delta_map["personal-laptop"]["benign"])
        self.assertEqual(delta_map["personal-laptop"]["severity"], "info")
        self.assertEqual(delta_map["personal-laptop"]["category"], "workstation_sleep")

        # 2. oci-machine: benign=False, severity="critical"
        self.assertIn("oci-machine", delta_map)
        self.assertFalse(delta_map["oci-machine"]["benign"])
        self.assertEqual(delta_map["oci-machine"]["severity"], "critical")
        self.assertEqual(delta_map["oci-machine"]["category"], "node_offline")

        # 3. hermes: benign=False, severity="critical"
        self.assertIn("hermes", delta_map)
        self.assertFalse(delta_map["hermes"]["benign"])
        self.assertEqual(delta_map["hermes"]["severity"], "critical")

        # 4. desktop-pc: benign=False, severity="warning"
        self.assertIn("desktop-pc", delta_map)
        self.assertFalse(delta_map["desktop-pc"]["benign"])
        self.assertEqual(delta_map["desktop-pc"]["severity"], "warning")

    def test_root_disk_pressure_boundary_conditions(self) -> None:
        """Verify exact boundary behavior for min_free_gb=15.0 and max_percent_used=90.0."""
        sniffer = FleetSniffer(min_free_gb=15.0, max_percent_used=90.0)

        # Boundary A: 15.1 GB free, 89.9% used -> pressure = False
        with patch("shutil.disk_usage", return_value=(100 * (1024**3), 89.9 * (1024**3), 15.1 * (1024**3))):
            res = sniffer.probe_disk_pressure()
            self.assertFalse(res["pressure"])

        # Boundary B: 14.9 GB free (< 15.0 GB threshold) -> pressure = True
        with patch("shutil.disk_usage", return_value=(100 * (1024**3), 85.1 * (1024**3), 14.9 * (1024**3))):
            res = sniffer.probe_disk_pressure()
            self.assertTrue(res["pressure"])

        # Boundary C: 90.1% used (> 90.0% threshold) -> pressure = True
        with patch("shutil.disk_usage", return_value=(100 * (1024**3), 90.1 * (1024**3), 20.0 * (1024**3))):
            res = sniffer.probe_disk_pressure()
            self.assertTrue(res["pressure"])

        # Boundary D: zero disk total (division safety) -> pressure = True, no crash
        with patch("shutil.disk_usage", return_value=(0, 0, 0)):
            res = sniffer.probe_disk_pressure()
            self.assertTrue(res["pressure"])
            self.assertEqual(res["percent_used"], 0.0)

        # Boundary E: disk_usage raises OSError -> pressure = True, error captured
        with patch("shutil.disk_usage", side_effect=OSError("Drive unmounted")):
            res = sniffer.probe_disk_pressure()
            self.assertTrue(res["pressure"])
            self.assertIn("Drive unmounted", res["error"])

    def test_startup_baseline_anomaly_detection(self) -> None:
        """Verify startup snapshot (prev is None) flags critical disk and VPS down, but ignores sleeping laptop."""
        # 1. VPS down at startup -> critical
        snap_vps_down = FleetSnapshot(
            nodes={"oci-machine": {"online": False, "status": "Failed to connect"}},
            disk={"pressure": False},
            docker={"docker_running": True},
        )
        deltas = self.calc.compute_deltas(None, snap_vps_down)
        self.assertEqual(len(deltas), 1)
        self.assertEqual(deltas[0]["target"], "oci-machine")
        self.assertEqual(deltas[0]["severity"], "critical")

        # 2. Sleeping laptop at startup -> ignored (no false alarms on boot)
        snap_laptop_sleeping = FleetSnapshot(
            nodes={"personal-laptop": {"online": False, "status": "Sleeping"}},
            disk={"pressure": False},
            docker={"docker_running": True},
        )
        deltas = self.calc.compute_deltas(None, snap_laptop_sleeping)
        self.assertEqual(len(deltas), 0)


class TestTailscaleProbeEmpirical(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: verify Tailscale probe failures, corrupted JSON, and state transitions."""

    async def asyncSetUp(self) -> None:
        self.sniffer = FleetSniffer()

    async def test_corrupted_json_handled_without_crash(self) -> None:
        """Verify malformed JSON output from tailscale command returns backend_state='Error'."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b'{"BackendState": "Runn... TRUNCATED', b''))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            res = await self.sniffer.probe_tailscale()
            self.assertFalse(res["online"])
            self.assertEqual(res["backend_state"], "Error")
            self.assertTrue(bool(res["error"]))
            self.assertTrue("Unterminated string" in res["error"] or "JSON" in res["error"])

    async def test_nonzero_exit_code_captured_gracefully(self) -> None:
        """Verify non-zero return code (e.g. daemon stopped) returns error dict without crash."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 1
        mock_proc.communicate = AsyncMock(return_value=(b"", b"tailscale daemon is not running\n"))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            res = await self.sniffer.probe_tailscale()
            self.assertFalse(res["online"])
            self.assertEqual(res["backend_state"], "Error")
            self.assertIn("daemon is not running", res["error"])

    async def test_probe_timeout_returns_timeout_state(self) -> None:
        """Verify subprocess timeout returns backend_state='Timeout'."""
        mock_proc = AsyncMock()
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError())

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            res = await self.sniffer.probe_tailscale(timeout=0.01)
            self.assertFalse(res["online"])
            self.assertEqual(res["backend_state"], "Timeout")
            self.assertIn("timed out", res["error"])

    def test_tailscale_disconnect_transition_trips_critical_delta(self) -> None:
        """Verify transition from Running to Stopped trips tailscale_down critical delta."""
        prev = FleetSnapshot(tailscale={"backend_state": "Running", "online": True})
        curr = FleetSnapshot(tailscale={"backend_state": "Stopped", "online": False})

        deltas = StatusDeltaCalculator.compute_deltas(prev, curr)
        ts_deltas = [d for d in deltas if d["category"] == "tailscale_down"]
        self.assertEqual(len(ts_deltas), 1)
        self.assertEqual(ts_deltas[0]["severity"], "critical")
        self.assertFalse(ts_deltas[0]["benign"])


class TestShoulderTapBusAndIngressHandshakeEmpirical(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: verify Shoulder Tap delivery, exception isolation, and IngressGateway handshake."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.sentinel = NoseSentinel(bus=self.bus)

    async def asyncTearDown(self) -> None:
        self.bus.clear()

    async def test_sentinel_publishes_typed_alert_on_anomaly(self) -> None:
        """Verify tick() with anomaly publishes typed ShoulderTapAlert(alert_type='health_anomaly', source='nose')."""
        received: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda a: received.append(a), alert_types=["health_anomaly"])

        anomaly = AnomalyVerdict(
            smells_off=True,
            severity="critical",
            category="crash_loop",
            summary="Fatal daemon crash loop detected",
            affected_nodes=["hermes"],
            recommended_action="Restart service",
        )

        with patch.object(self.sentinel, "sniff_bot_err", return_value=(["Traceback: crash"], "raw")):
            with patch.object(self.sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
                mock_fleet.return_value = FleetSnapshot()
                with patch.object(self.sentinel, "evaluate_anomaly", new_callable=AsyncMock) as mock_eval:
                    mock_eval.return_value = anomaly

                    alert = await self.sentinel.tick()
                    self.assertIsNotNone(alert)
                    self.assertEqual(len(received), 1)
                    self.assertEqual(received[0].alert_type, "health_anomaly")
                    self.assertEqual(received[0].source, "nose")
                    self.assertTrue(received[0].is_health_anomaly)

    async def test_subscriber_exception_does_not_crash_sentinel(self) -> None:
        """Verify that an exploding subscriber callback does not crash NoseSentinel loop."""
        def bad_sync_sub(alert: ShoulderTapAlert) -> None:
            raise ZeroDivisionError("sync explosion")

        async def bad_async_sub(alert: ShoulderTapAlert) -> None:
            raise KeyError("async explosion")

        self.bus.subscribe(bad_sync_sub, alert_types=["health_anomaly"])
        self.bus.subscribe(bad_async_sub, alert_types=["health_anomaly"])

        anomaly = AnomalyVerdict(smells_off=True, summary="Testing error resilience")

        with patch.object(self.sentinel, "sniff_bot_err", return_value=(["err"], "slice")):
            with patch.object(self.sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
                mock_fleet.return_value = FleetSnapshot()
                with patch.object(self.sentinel, "evaluate_anomaly", new_callable=AsyncMock) as mock_eval:
                    mock_eval.return_value = anomaly

                    # Tick should survive without raising
                    res = await self.sentinel.tick()
                    self.assertIsNotNone(res)

    async def test_ingress_gateway_health_anomaly_subscription_behavior(self) -> None:
        """
        CRITICAL ARCHITECTURAL VERIFICATION:
        Examine IngressGateway._handle_health_anomaly_alert delivery when invoked via ShoulderTapEventBus.
        Observation:
        - IngressGateway subscribes self._handle_health_anomaly_alert to 'health_anomaly'.
        - When ShoulderTapEventBus.publish() dispatches to callbacks, it passes only (alert).
        - IngressGateway._handle_health_anomaly_alert requires 'client' and 'channel' to post.
        - Verify that when client is absent (bus dispatch default), no crash occurs,
          and verify whether an advisory is delivered if client is provided.
        """
        router = MagicMock()
        gw = IngressGateway(router=router, sensory_bus=self.bus)

        posted_messages: List[str] = []
        gw._post_response = AsyncMock(
            side_effect=lambda client, ch, ts, msg, **kw: posted_messages.append(msg)
        )

        alert = ShoulderTapAlert(
            alert_type="health_anomaly",
            source="nose",
            summary="Host root disk critically full (<5GB free)",
            channel="C_ALERTS",
        )

        # 1. Bus dispatch: passes alert to subscriber callback (client is None)
        self.bus.publish(alert)
        await asyncio.sleep(0.05)

        # Without client injection, _post_response is NOT called because client is None
        self.assertEqual(
            len(posted_messages), 0,
            "Bus dispatch without client passed: IngressGateway safely skipped post (client is None)"
        )

        # 2. Directed dispatch via process_sensory_alert with explicit client
        mock_client = MagicMock()
        await gw.process_sensory_alert(alert, mock_client)

        self.assertEqual(
            len(posted_messages), 1,
            "Direct dispatch with client: IngressGateway delivered advisory"
        )
        self.assertIn("Sentinel Advisory (Fleet Health)", posted_messages[0])
        self.assertIn("Host root disk critically full", posted_messages[0])


class TestAdversarialStressHarness(unittest.IsolatedAsyncioTestCase):
    """Hostile environment stress testing: resource limits, concurrency, corruption, and races."""

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_challenger_stress_")
        self.bot_err_path = os.path.join(self.temp_dir, "bot.err")
        with open(self.bot_err_path, "w") as f:
            f.write("Boot\n")
        self.bus = ShoulderTapEventBus()
        self.sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            max_slice_bytes=8192,
        )

    async def asyncTearDown(self) -> None:
        self.bus.clear()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_invalid_utf8_bytes_decoded_and_error_detected(self) -> None:
        """Verify invalid raw binary bytes in bot.err do not crash sniffer and errors are detected."""
        with open(self.bot_err_path, "ab") as f:
            # Corrupted bytes followed by error line
            f.write(b"\xff\xfe\x80\x81\x92\n")
            f.write(b"2026-10-02 [ERROR] Fatal binary crash\n")

        errors, raw = self.sentinel.sniff_bot_err()
        self.assertGreaterEqual(len(errors), 1)
        self.assertIn("Fatal binary crash", errors[0])

    def test_massive_100mb_log_spike_strictly_bounded(self) -> None:
        """Verify 100MB log spike reads at most max_slice_bytes and advances offset to 100MB."""
        # Create a sparse file of 100MB by seeking
        file_size = 100 * 1024 * 1024
        with open(self.bot_err_path, "wb") as f:
            f.seek(file_size - 100)
            f.write(b"2026-10-02 [ERROR] Out of memory condition at EOF\n")

        errors, raw = self.sentinel.sniff_bot_err()
        self.assertLessEqual(len(raw.encode("utf-8")), 8192)
        actual_size = os.path.getsize(self.bot_err_path)
        self.assertEqual(self.sentinel.last_log_offset, actual_size)
        self.assertIn("Out of memory", errors[0])

        # Next sniff with no writes returns empty (cursor preserved)
        errors2, raw2 = self.sentinel.sniff_bot_err()
        self.assertEqual(errors2, [])
        self.assertEqual(raw2, "")

    async def test_concurrent_overlapping_ticks_serialized(self) -> None:
        """Stress-test concurrent tick invocations: lock guarantees strict serialization."""
        tick_counter = 0

        async def slow_sniff(*args, **kwargs):
            nonlocal tick_counter
            current = tick_counter
            await asyncio.sleep(0.02)
            tick_counter = current + 1
            return FleetSnapshot()

        with patch.object(self.sentinel, "sniff_fleet", side_effect=slow_sniff):
            tasks = [self.sentinel.tick() for _ in range(5)]
            await asyncio.gather(*tasks)

        self.assertEqual(tick_counter, 5)

    async def test_rapid_start_stop_idempotency(self) -> None:
        """Verify rapid repeated start() and stop() calls remain stable and idempotent."""
        for _ in range(5):
            t1 = self.sentinel.start()
            t2 = self.sentinel.start()
            self.assertIs(t1, t2)
            self.assertTrue(self.sentinel.is_running)

            await self.sentinel.stop()
            self.assertFalse(self.sentinel.is_running)

            # Idempotent second stop
            await self.sentinel.stop()
            self.assertFalse(self.sentinel.is_running)


if __name__ == "__main__":
    unittest.main()
