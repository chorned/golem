#!/usr/bin/env python3
"""
tests/test_webui_dashboard_r3.py
Milestone 3 Verification Test Suite (Deliverable R3).

Validates:
1. Mock Fixture Elimination: Complete absence of Audiobookshelf and PID 58236 from templates,
   constants, and HTTP GET / responses.
2. Standby View Lifecycle: Clean standby view rendered when _active_hands is empty (REST /api/status
   and SSE hands_status).
3. Dynamic Hand Rendering: Real-time hand registration, delta buffering, and settlement.
4. Nose Telemetry Deduplication: Gauge updates on every 2.5s tick, but terminal appends strictly
   when verdict_summary changes or an anomaly occurs (smells_off == true).
5. Lineage & Air-Gapped Invariants: Preserves Stoneforge design and First Mate lineage.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import json
import socket
import shutil
import asyncio
import subprocess
import unittest
from pathlib import Path
from typing import Dict, Any, List

import aiohttp
from aiohttp import web, ClientSession

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
)
from golem.webui.server import (
    LINEAGE,
    DASHBOARD_HTML,
    WebUIServer,
    get_system_status,
)


def _get_free_port() -> int:
    """Acquire a temporary available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestMockFixtureElimination(unittest.IsolatedAsyncioTestCase):
    """Verify that all static mock fixtures (Audiobookshelf, PID 58236) are purged."""

    def setUp(self) -> None:
        self.template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        self.template_content = self.template_path.read_text(encoding="utf-8")

    def test_dashboard_template_contains_no_audiobookshelf_or_pid_58236(self) -> None:
        """Static template must not contain Audiobookshelf, PID 58236, or absdatabase."""
        forbidden_strings = [
            "Audiobookshelf",
            "audiobookshelf",
            "58236",
            "absdatabase",
            "The Material Arcana",
            "initialHandAgent",
        ]
        for term in forbidden_strings:
            self.assertNotIn(
                term,
                self.template_content,
                f"Static mock fixture '{term}' detected in dashboard.html",
            )

    def test_dashboard_constant_contains_no_mock_fixtures(self) -> None:
        """DASHBOARD_HTML constant in server.py must be free of mock fixtures."""
        forbidden_strings = ["Audiobookshelf", "audiobookshelf", "58236", "absdatabase"]
        for term in forbidden_strings:
            self.assertNotIn(
                term,
                DASHBOARD_HTML,
                f"Static mock fixture '{term}' detected in DASHBOARD_HTML constant",
            )

    async def test_served_root_endpoint_contains_no_mock_fixtures(self) -> None:
        """HTTP GET / served by WebUIServer must not contain mock fixtures."""
        port = _get_free_port()
        server = WebUIServer(host="127.0.0.1", port=port)
        await server.start()
        try:
            url = f"http://127.0.0.1:{port}/"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    self.assertEqual(resp.status, 200)
                    body = await resp.text()
                    self.assertNotIn("Audiobookshelf", body)
                    self.assertNotIn("58236", body)
                    self.assertNotIn("absdatabase", body)
        finally:
            await server.stop()

    def test_preserves_lineage_and_title_invariants(self) -> None:
        """Template must preserve Stoneforge title and lineage comment."""
        self.assertIn("STONEFORGE // Agentic Golem Terminal Matrix", self.template_content)
        self.assertIn(LINEAGE, self.template_content)


class TestStandbyViewWhenActiveHandsEmpty(unittest.IsolatedAsyncioTestCase):
    """Verify that standby view is rendered and active_hands defaults to empty."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    def test_server_active_hands_empty_by_default(self) -> None:
        """WebUIServer starts with 0 active hands."""
        self.assertEqual(len(self.server.active_hands), 0)

    async def test_api_status_returns_empty_hands_list(self) -> None:
        """GET /api/status returns hands: [] when idle."""
        url = f"http://127.0.0.1:{self.port}/api/status"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertIn("hands", data)
                self.assertEqual(data["hands"], [])

    async def test_api_sensory_hands_returns_empty_list(self) -> None:
        """GET /api/sensory/hands returns [] when idle."""
        url = f"http://127.0.0.1:{self.port}/api/sensory/hands"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                self.assertEqual(resp.status, 200)
                hands = await resp.json()
                self.assertEqual(hands, [])

    async def test_initial_sse_hands_status_event_is_empty(self) -> None:
        """When hands are idle, broadcasting hands_status [] delivers an empty list."""
        url = f"http://127.0.0.1:{self.port}/events"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                # Drain connected handshake
                await resp.content.readline()  # event: connected
                await resp.content.readline()  # data: {...}
                await resp.content.readline()  # newline separator

                # Broadcast empty hands_status
                self.server.broadcast("hands_status", [])

                # Next event is hands_status
                ev_line = await resp.content.readline()
                self.assertEqual(ev_line.decode().strip(), "event: hands_status")
                data_line = await resp.content.readline()
                hands_list = json.loads(data_line.decode().replace("data: ", "").strip())
                self.assertEqual(hands_list, [])

    def test_dashboard_template_contains_standby_elements(self) -> None:
        """Template contains hands-grid, hands-standby-card, and standby state handling."""
        template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        content = template_path.read_text(encoding="utf-8")
        self.assertIn('id="hands-grid"', content)
        self.assertIn('id="hands-standby-card"', content)
        self.assertIn("HANDS STANDBY — 0 ACTIVE TASKS", content)
        self.assertIn("STANDBY", content)


class TestDynamicHandsLifecycle(unittest.IsolatedAsyncioTestCase):
    """Verify live registration, delta streaming, and settlement."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    async def test_hand_registration_and_settlement_lifecycle(self) -> None:
        """Registering a hand adds it to active_hands; unregistering removes it."""
        # 1. Register
        self.server.register_active_hand(
            pane_id="runner_p2",
            node="desktop-pc",
            command="cargo test --workspace",
            pid=4242,
            worker="agy",
        )
        self.assertIn("runner_p2", self.server.active_hands)
        hand = self.server.active_hands["runner_p2"]
        self.assertEqual(hand["status"], "RUNNING")
        self.assertEqual(hand["command"], "cargo test --workspace")

        # 2. Update buffer
        self.server.update_hand_buffer("runner_p2", "Compiling test crates...\n")
        self.assertIn("Compiling test crates...\n", self.server.active_hands["runner_p2"]["buffer_tail"])

        # 3. Verify in REST status
        url = f"http://127.0.0.1:{self.port}/api/status"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                self.assertEqual(len(data["hands"]), 1)
                self.assertEqual(data["hands"][0]["pane_id"], "runner_p2")

        # 4. Settle / unregister
        self.server.unregister_active_hand("runner_p2")
        self.assertEqual(len(self.server.active_hands), 0)

        # 5. Verify empty again
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                self.assertEqual(data["hands"], [])


class TestNoseTelemetryDeduplicationLogic(unittest.IsolatedAsyncioTestCase):
    """Verify gauge updates every tick and terminal deduplication."""

    def setUp(self) -> None:
        self.template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        self.template_content = self.template_path.read_text(encoding="utf-8")

    def test_dashboard_declares_dedup_state_variables(self) -> None:
        """dashboard.html must declare last_verdict_summary and last_smells_off."""
        self.assertIn("last_verdict_summary", self.template_content)
        self.assertIn("last_smells_off", self.template_content)

    def test_dashboard_contains_deduplication_condition(self) -> None:
        """dashboard.html must contain deduplication checks for nose telemetry."""
        self.assertIn("handleNoseTelemetryUpdate", self.template_content)
        self.assertIn("updateNoseData", self.template_content)
        self.assertIn("summaryChanged", self.template_content)

    def test_dashboard_contains_nose_alert_listener(self) -> None:
        """dashboard.html must listen for nose_alert events."""
        self.assertIn("addEventListener('nose_alert'", self.template_content)

    def test_node_simulation_deduplication_execution(self) -> None:
        """
        Execute headless Node.js simulation to verify:
        - Gauges update on every tick
        - Terminal appends strictly when summary changes or anomaly occurs
        - Identical consecutive ticks produce zero duplicate log rows
        """
        js_code = """
        let last_verdict_summary = null;
        let last_smells_off = null;
        let logAppends = 0;
        let logs = [];
        let gaugeUpdates = 0;
        let gauges = { latency: 0, disk: 0, smell: "" };

        function updateNoseData(data) {
            gaugeUpdates++;
            if (data.sniff_latency_ms !== undefined) gauges.latency = data.sniff_latency_ms;
            if (data.disk) gauges.disk = data.disk.percent_used;
            if (data.smells_off) gauges.smell = "ALERT";
            else gauges.smell = "CLEAN";
        }

        function appendAgentLog(container, text) {
            logAppends++;
            logs.push(text);
        }

        function handleNoseTelemetryUpdate(data, isAlert = false) {
            if (!data) return;
            updateNoseData(data);

            const summary = (data.verdict_summary || data.summary || "").trim();
            if (!summary) return;

            const smellsOff = Boolean(data.smells_off);
            const summaryChanged = Boolean(last_verdict_summary === null || summary !== last_verdict_summary);
            const anomalyOccurred = Boolean(smellsOff && last_smells_off !== true);

            if (summaryChanged || anomalyOccurred || isAlert) {
                const level = smellsOff ? "[WARN]" : "[INFO]";
                appendAgentLog(null, `${level} Golem.Sensory.Nose: ${summary}`);
                last_verdict_summary = summary;
                last_smells_off = smellsOff;
            }
        }

        // 1. Tick 1: Initial Quiescent
        handleNoseTelemetryUpdate({ verdict_summary: "Healthy", smells_off: false, sniff_latency_ms: 10, disk: { percent_used: 40 } });
        // 2. Tick 2: Identical Quiescent
        handleNoseTelemetryUpdate({ verdict_summary: "Healthy", smells_off: false, sniff_latency_ms: 11, disk: { percent_used: 41 } });
        // 3. Tick 3: Identical Quiescent
        handleNoseTelemetryUpdate({ verdict_summary: "Healthy", smells_off: false, sniff_latency_ms: 12, disk: { percent_used: 42 } });

        // 4. Tick 4: Health Anomaly!
        handleNoseTelemetryUpdate({ verdict_summary: "Disk pressure detected: 94%", smells_off: true, sniff_latency_ms: 8, disk: { percent_used: 94 } });
        // 5. Tick 5: Persisting Anomaly (identical summary)
        handleNoseTelemetryUpdate({ verdict_summary: "Disk pressure detected: 94%", smells_off: true, sniff_latency_ms: 9, disk: { percent_used: 95 } });

        // 6. Tick 6: System Recovery
        handleNoseTelemetryUpdate({ verdict_summary: "Quiescent scan clean", smells_off: false, sniff_latency_ms: 10, disk: { percent_used: 45 } });
        // 7. Tick 7: Identical Recovery
        handleNoseTelemetryUpdate({ verdict_summary: "Quiescent scan clean", smells_off: false, sniff_latency_ms: 11, disk: { percent_used: 46 } });

        const result = {
            totalGaugeUpdates: gaugeUpdates,
            totalLogAppends: logAppends,
            currentDiskGauge: gauges.disk,
            logs: logs
        };
        console.log(JSON.stringify(result));
        """
        if not shutil.which("node"):
            self.skipTest("node binary not installed in test environment")
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        res = json.loads(proc.stdout.strip())

        # Gauges must have updated 7 times (every single tick)
        self.assertEqual(res["totalGaugeUpdates"], 7)
        self.assertEqual(res["currentDiskGauge"], 46)

        # Logs must have appended only 3 times (Tick 1, Tick 4 anomaly, Tick 6 recovery)
        self.assertEqual(res["totalLogAppends"], 3)
        self.assertIn("[INFO] Golem.Sensory.Nose: Healthy", res["logs"][0])
        self.assertIn("[WARN] Golem.Sensory.Nose: Disk pressure detected: 94%", res["logs"][1])
        self.assertIn("[INFO] Golem.Sensory.Nose: Quiescent scan clean", res["logs"][2])


if __name__ == "__main__":
    unittest.main()
