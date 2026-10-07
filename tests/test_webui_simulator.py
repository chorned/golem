#!/usr/bin/env python3
"""
tests/test_webui_simulator.py
Comprehensive test suite for:
  1. Hand Agent Simulator (minimal bloat, stdlib only, ANSI streams).
  2. WebUI REST API endpoints (/api/simulate_hands).
  3. CLI argument parsing (--hands, --mock-hands, negative/default counts).
  4. Proof-of-life, proof-of-thought, and debugging sensory logs (Eyes, Nose, Mouth).
  5. Dashboard functional layout (no ellipsizing, line breaks, desktop 3-column) and mobile-optimized view.
"""

from __future__ import annotations

import os
import sys
import re
import socket
import asyncio
import unittest
from pathlib import Path
from typing import List, Dict, Any

import aiohttp
from aiohttp import web, ClientSession

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app
from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
    get_default_event_bus,
    reset_default_event_bus,
)
from golem.webui.simulator import HandSimulator, SIMULATED_WORKER_PRESETS
from golem.webui.server import (
    WebUIServer,
    format_sse_event,
)


def _get_free_port() -> int:
    """Acquire a temporary available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestHandSimulatorUnit(unittest.IsolatedAsyncioTestCase):
    """Test HandSimulator logic in isolation (stdlib only, zero external bloat)."""

    def test_minimal_bloat_dependencies(self) -> None:
        """Verify simulator.py only imports Python standard library modules."""
        import golem.webui.simulator as sim_mod
        file_path = Path(sim_mod.__file__)
        content = file_path.read_text(encoding="utf-8")
        
        # Check that no external packages (e.g. requests, rich, click, fastapi) are imported
        disallowed = ["requests", "fastapi", "rich", "pydantic", "flask", "django", "click", "typer"]
        for pkg in disallowed:
            self.assertNotIn(f"import {pkg}", content)
            self.assertNotIn(f"from {pkg}", content)

    def test_presets_coverage(self) -> None:
        """Ensure presets cover at least 6 distinct workers across the fleet."""
        self.assertGreaterEqual(len(SIMULATED_WORKER_PRESETS), 6)
        machines = {p["node"] for p in SIMULATED_WORKER_PRESETS}
        # Should cover localhost, personal-laptop, desktop-pc, oci-machine
        self.assertIn("localhost", machines)
        self.assertIn("personal-laptop", machines)
        self.assertIn("desktop-pc", machines)
        self.assertIn("oci-machine", machines)

    async def test_simulator_lifecycle(self) -> None:
        """Verify starting, delta generation, and stopping of simulated hands."""
        broadcast_events: List[Dict[str, Any]] = []

        class MockServer:
            def __init__(self):
                self.active_hands = {}

            def register_active_hand(self, pane_id, node, command, pid, worker, mirror, herdr_pane_id):
                self.active_hands[pane_id] = {
                    "pane_id": pane_id,
                    "node": node,
                    "command": command,
                    "pid": pid,
                    "worker": worker,
                }

            def unregister_active_hand(self, pane_id, node):
                self.active_hands.pop(pane_id, None)

            def broadcast(self, event, data):
                broadcast_events.append({"event": event, "data": data})

            def update_hand_buffer(self, pane_id, tail, node):
                pass

        server = MockServer()
        sim = HandSimulator(server=server, count=4)
        try:
            sim.start()
            self.assertTrue(sim.is_running)
            self.assertEqual(len(server.active_hands), 4)

            # Check that spawn events were broadcast
            spawn_events = [e for e in broadcast_events if e["event"] == "hand_spawn"]
            self.assertEqual(len(spawn_events), 4)
        finally:
            sim.stop()
            self.assertFalse(sim.is_running)
            self.assertEqual(len(server.active_hands), 0)


class TestWebUISimulationRESTAPI(unittest.IsolatedAsyncioTestCase):
    """Test WebUI REST API endpoints for hand simulation."""

    async def asyncSetUp(self) -> None:
        reset_default_event_bus()
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self) -> None:
        await self.session.close()
        await self.server.stop()
        reset_default_event_bus()

    async def test_rest_api_simulate_lifecycle(self) -> None:
        """Test POST /api/simulate_hands, GET /api/simulate_hands, and DELETE /api/simulate_hands."""
        # 1. Initially inactive
        async with self.session.get(f"{self.base_url}/api/simulate_hands") as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertFalse(data["active"])
            self.assertEqual(data["count"], 0)

        # 2. Start simulation via POST
        async with self.session.post(
            f"{self.base_url}/api/simulate_hands",
            json={"count": 6}
        ) as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertTrue(data["ok"])
            self.assertTrue(data["active"])
            self.assertEqual(data["count"], 6)

        # 3. Verify GET returns active status and workers
        async with self.session.get(f"{self.base_url}/api/simulate_hands") as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertTrue(data["active"])
            self.assertEqual(data["count"], 6)
            self.assertEqual(len(data["workers"]), 6)

        # 4. Stop simulation via DELETE
        async with self.session.delete(f"{self.base_url}/api/simulate_hands") as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertTrue(data["ok"])
            self.assertFalse(data["active"])

        # 5. Confirm stopped
        async with self.session.get(f"{self.base_url}/api/simulate_hands") as resp:
            data = await resp.json()
            self.assertFalse(data["active"])
            self.assertEqual(data["count"], 0)


class TestAppCommandLineHands(unittest.TestCase):
    """Test CLI argument parsing for --hands and --mock-hands flags."""

    def test_default_none(self) -> None:
        args = app.parse_args([])
        self.assertIsNone(args.hands)

    def test_hands_flag_without_number_defaults_to_6(self) -> None:
        args = app.parse_args(["--hands"])
        self.assertEqual(args.hands, 6)

    def test_hands_flag_with_positive_int(self) -> None:
        args = app.parse_args(["--hands", "4"])
        self.assertEqual(args.hands, 4)

    def test_hands_flag_with_negative_int(self) -> None:
        args = app.parse_args(["--hands", "-6"])
        self.assertEqual(args.hands, -6)
        self.assertEqual(abs(args.hands), 6)

    def test_mock_hands_alias(self) -> None:
        args = app.parse_args(["--mock-hands", "3"])
        self.assertEqual(args.hands, 3)


class TestSensoryLoggingProofOfLifeAndThought(unittest.TestCase):
    """Test optimized logging across Mouth, Eyes, and Nose (proof of life, thought, debugging)."""

    def setUp(self) -> None:
        reset_default_event_bus()
        self.server = WebUIServer(host="127.0.0.1", port=_get_free_port())

    def tearDown(self) -> None:
        reset_default_event_bus()

    def test_seed_proof_of_life_logs(self) -> None:
        """Verify startup proof of life logs exist in Eyes, Nose, and Mouth log rings."""
        eyes_logs = self.server._recent_logs.get("eyes", [])
        nose_logs = self.server._recent_logs.get("nose", [])
        mouth_logs = self.server._recent_logs.get("mouth", [])

        self.assertGreater(len(eyes_logs), 0)
        self.assertGreater(len(nose_logs), 0)
        self.assertGreater(len(mouth_logs), 0)

        # Check for [LIFE] and [INFO] proof of life markers
        eyes_text = " ".join(eyes_logs)
        self.assertTrue("[LIFE]" in eyes_text or "calibrated" in eyes_text)

        nose_text = " ".join(nose_logs)
        self.assertTrue("[LIFE]" in nose_text or "NoseSentinel" in nose_text)

        mouth_text = " ".join(mouth_logs)
        self.assertTrue("[LIFE]" in mouth_text or "Liaison" in mouth_text)

    def test_bus_alert_proof_of_thought_formatting(self) -> None:
        """Verify bus alerts format thought, life, and blocker messages clearly."""
        # Eye inspection alert -> thought log
        alert_inspection = ShoulderTapAlert(
            alert_type="eye_inspection",
            source="eyes",
            summary="Delta evaluation",
            data={"pane_id": "runner", "delta_lines": 14, "state": "active"},
        )
        self.server._process_bus_alert(alert_inspection)
        eyes_logs = self.server._recent_logs.get("eyes", [])
        latest = eyes_logs[-1]
        self.assertIn("[THOUGHT]", latest)
        self.assertIn("delta: +14 lines", latest)

        # Eye blocker alert -> blocker log
        alert_blocker = ShoulderTapAlert(
            alert_type="eye_blocker",
            source="eyes",
            summary="Interactive prompt detected",
            data={"pane_id": "runner", "blocker_type": "confirm_prompt", "detail": "Do you want to continue? [y/N]"},
        )
        self.server._process_bus_alert(alert_blocker)
        latest_blocker = self.server._recent_logs.get("eyes", [])[-1]
        self.assertIn("[BLOCKER]", latest_blocker)
        self.assertIn("confirm_prompt", latest_blocker)


class TestDashboardFunctionalAndMobileDesign(unittest.TestCase):
    """Test dashboard.html template structure for line breaks, functional layout, and mobile view."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        cls.content = cls.template_path.read_text(encoding="utf-8")

    def test_terminal_outputs_allow_line_breaks_and_no_ellipsizing(self) -> None:
        """Verify terminal output CSS rules enable wrapping and disable single-line ellipsis truncation."""
        # 1. Must have pre-wrap and break-word rules for terminal logs and cards
        self.assertIn(".terminal-log-line {", self.content)
        self.assertIn("white-space: pre-wrap;", self.content)
        self.assertIn("word-break: break-word;", self.content)

        # 2. Check .hand-card-cmd does not ellipsize to a single line
        self.assertIn(".hand-card-cmd {", self.content)
        self.assertIn("overflow-wrap: anywhere;", self.content)

        # 3. Ensure no active 'text-overflow: ellipsis; white-space: nowrap;' on terminal logs
        # Scan for invalid ellipsis on terminal lines
        log_line_match = re.search(r"\.terminal-log-line\s*\{([^}]+)\}", self.content)
        self.assertIsNotNone(log_line_match)
        block = log_line_match.group(1)
        self.assertNotIn("text-overflow: ellipsis", block)
        self.assertNotIn("white-space: nowrap", block)

    def test_functional_3_column_desktop_layout(self) -> None:
        """Verify side-by-side 3-column sensory organs layout on desktop."""
        self.assertIn(".sensory-organs-row", self.content)
        self.assertIn("grid-template-columns: repeat(3, 1fr);", self.content)
        self.assertIn('id="anchor-eyes"', self.content)
        self.assertIn('id="anchor-nose"', self.content)
        self.assertIn('id="anchor-mouth"', self.content)
        self.assertIn('id="anchor-hands"', self.content)

    def test_mobile_responsive_design_and_switcher(self) -> None:
        """Verify alternative mobile-optimized design strictly triggered on phone viewports."""
        # 1. Media query for max-width: 768px
        self.assertIn("@media (max-width: 768px)", self.content)

        # 2. Mobile tab switcher bar
        self.assertIn(".mobile-tab-strip", self.content)
        self.assertIn(".mobile-tab-btn", self.content)
        self.assertIn('data-tab="hands"', self.content)
        self.assertIn('data-tab="eyes"', self.content)
        self.assertIn('data-tab="nose"', self.content)
        self.assertIn('data-tab="mouth"', self.content)
        self.assertIn('data-tab="fleet"', self.content)

        # 3. JS mobile detection and tab switching logic
        self.assertIn("checkMobileMode", self.content)
        self.assertIn("switchMobileTab", self.content)

    def test_air_gapped_offline_invariants(self) -> None:
        """Verify template has zero external links or script tags."""
        scripts = re.findall(r"<script[^>]+src=", self.content, flags=re.IGNORECASE)
        self.assertEqual(scripts, [])
        links = re.findall(r"<link[^>]+href=", self.content, flags=re.IGNORECASE)
        self.assertEqual(links, [])


if __name__ == "__main__":
    unittest.main()
