#!/usr/bin/env python3
"""
tests/test_sensory_nose.py
Unit tests verifying NoseSentinel and NoseTelemetryTracker telemetry deduplication:
  - Confirms telemetry ticks without state changes do not append duplicate log lines.
  - Asserts node state transitions (online/offline) generate log events.
  - Asserts listening port additions/closures generate log events.
  - Asserts anomalies (smells_off == True) are logged without repeated spam.
  - Asserts explicit health checks (/health, !nose) always log without deduplication.
"""

from __future__ import annotations

import os
import sys
import unittest
import asyncio
from typing import Dict, Any, Set
from unittest.mock import MagicMock, AsyncMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.nose import NoseTelemetryTracker, NoseLiveProbe, FLAGGED_PORTS
from golem.webui.server import WebUIServer


class TestSensoryNose(unittest.IsolatedAsyncioTestCase):
    """Unit tests for Nose telemetry deduplication and change-driven logging."""

    def setUp(self) -> None:
        self.tracker = NoseTelemetryTracker()

    def test_identical_ticks_do_not_produce_duplicate_log_events(self) -> None:
        """
        CRITICAL ACCEPTANCE CRITERION:
        Verify that telemetry ticks without state changes return empty event lists
        so no duplicate log lines are appended to the Nose terminal.
        """
        nodes = {
            "desktop-pc": {"online": True, "status": "Online", "latency_ms": 2.5},
            "oci-machine": {"online": True, "status": "Online", "latency_ms": 15.0},
        }
        ports: Set[int] = {8765, 22}

        # First tick establishes baseline and logs initial node states
        initial_events = self.tracker.observe(nodes, ports, smells_off=False, verdict_summary="Nominal")
        self.assertGreaterEqual(len(initial_events), 2)

        # Subsequent consecutive ticks with identical status must produce ZERO log events
        for _ in range(5):
            events = self.tracker.observe(nodes, ports, smells_off=False, verdict_summary="Nominal")
            self.assertEqual(len(events), 0, "Telemetry tick without changes must not emit duplicate log events")

    def test_node_state_transition_triggers_log_event(self) -> None:
        """Verify when a node drops offline or recovers online, a single log event is emitted."""
        nodes = {
            "desktop-pc": {"online": True, "status": "Online", "latency_ms": 2.5},
            "oci-machine": {"online": True, "status": "Online", "latency_ms": 15.0},
        }
        ports: Set[int] = {8765}

        # Initial baseline
        self.tracker.observe(nodes, ports)

        # desktop-pc goes offline
        nodes_offline = dict(nodes)
        nodes_offline["desktop-pc"] = {"online": False, "status": "Timeout after 1s", "latency_ms": None}

        events = self.tracker.observe(nodes_offline, ports)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["kind"], "node_state")
        self.assertFalse(events[0]["online"])
        self.assertIn("desktop-pc went offline", events[0]["message"])

        # Another tick while still offline must produce 0 events
        events_quiet = self.tracker.observe(nodes_offline, ports)
        self.assertEqual(len(events_quiet), 0)

        # desktop-pc recovers online
        events_recovered = self.tracker.observe(nodes, ports)
        self.assertEqual(len(events_recovered), 1)
        self.assertTrue(events_recovered[0]["online"])
        self.assertIn("desktop-pc went online", events_recovered[0]["message"])

    def test_port_opening_and_closing_triggers_log_event(self) -> None:
        """Verify opening or closing listening ports emits log events."""
        nodes = {"desktop-pc": {"online": True}}
        base_ports: Set[int] = {8765}
        self.tracker.observe(nodes, base_ports)

        # New OAuth port opened (53682 is flagged)
        new_ports = {8765, 53682}
        events = self.tracker.observe(nodes, new_ports)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["kind"], "port_opened")
        self.assertEqual(events[0]["port"], 53682)
        self.assertTrue(events[0]["flagged"])
        self.assertIn("flagged", events[0]["message"])

        # Steady state: no new events
        self.assertEqual(len(self.tracker.observe(nodes, new_ports)), 0)

        # Port closes
        events_close = self.tracker.observe(nodes, base_ports)
        self.assertEqual(len(events_close), 1)
        self.assertEqual(events_close[0]["kind"], "port_closed")
        self.assertEqual(events_close[0]["port"], 53682)

    def test_anomaly_triggers_log_once_without_spam(self) -> None:
        """Verify an anomaly is logged when flagged, but not repeated every tick."""
        nodes = {"desktop-pc": {"online": True}}
        ports: Set[int] = {8765}
        self.tracker.observe(nodes, ports)

        # Flag an anomaly
        events = self.tracker.observe(nodes, ports, smells_off=True, verdict_summary="Disk pressure > 95%")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["kind"], "anomaly")
        self.assertIn("Disk pressure > 95%", events[0]["message"])

        # Identical anomaly on next ticks does not duplicate
        self.assertEqual(len(self.tracker.observe(nodes, ports, smells_off=True, verdict_summary="Disk pressure > 95%")), 0)

        # Clearing anomaly resets state
        self.assertEqual(len(self.tracker.observe(nodes, ports, smells_off=False, verdict_summary="Nominal")), 0)

    def test_explicit_health_check_always_logged(self) -> None:
        """Verify explicit health checks (/health, !nose) are always logged without deduplication."""
        ev1 = self.tracker.explicit_check("Operator requested /health")
        ev2 = self.tracker.explicit_check("Operator requested /health")
        self.assertEqual(ev1["kind"], "health_check")
        self.assertEqual(ev2["kind"], "health_check")
        self.assertIn("/health", ev1["message"])
        self.assertIn("/health", ev2["message"])

    async def test_webui_server_build_nose_payload_deduplication(self) -> None:
        """
        Verify that when WebUIObservabilityServer._build_nose_payload runs on repeated ticks,
        nose_log events are not repeatedly appended to recent logs when state is unchanged.
        """
        server = WebUIServer(port=18765)
        mock_probe = MagicMock()
        mock_probe.sample = AsyncMock(side_effect=lambda: {
            "ports": [8765],
            "port_set": {8765},
            "flagged_ports": [],
            "fleet_nodes": {
                "desktop-pc": {"online": True, "status": "Online", "latency_ms": 1.2},
            },
            "memory": {"percent_used": 42.0},
        })
        server._nose_probe = mock_probe
        server._nose_tracker = NoseTelemetryTracker()

        base = {"smells_off": False, "verdict_summary": "Nominal"}

        # Tick 1: initial baseline
        await server._build_nose_payload(base)
        initial_log_count = len(server._recent_logs.get("nose", []))

        # Ticks 2..6: identical state
        for _ in range(5):
            payload = await server._build_nose_payload(base)
            self.assertEqual(payload["fleet_nodes"]["desktop-pc"]["online"], True)
            self.assertEqual(len(server._recent_logs.get("nose", [])), initial_log_count,
                             "Recent nose logs must not grow during quiescent telemetry ticks")


if __name__ == "__main__":
    unittest.main()
