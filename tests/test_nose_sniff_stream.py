#!/usr/bin/env python3
"""
tests/test_nose_sniff_stream.py
Tests for CHO-217: Wiring The Nose sentinel to emit real-time sniff cycle
telemetry receipts to the Nose WebUI terminal across all 5 subsystems:
1. Fleet node reachability
2. Disk usage
3. Docker container status
4. bot.err log byte offset and deltas
5. Linear assigned-ticket watchdog telemetry

LINEAR GUARDRAIL: NEVER call Linear GraphQL APIs or modify Linear issue states
directly from a Hand. Only The Mouth manages Linear ticket transitions.
"""

from __future__ import annotations

import os
import time
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from golem.bus import ShoulderTapAlert, ShoulderTapEventBus
from golem.nose import FleetSnapshot, NoseSentinel
from golem.webui.server import WebUIServer


class TestNoseSniffStream(unittest.IsolatedAsyncioTestCase):
    """Test suite for Nose sentinel real-time sniff cycle receipt streaming."""

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_nose_sniff_")
        self.bot_err_path = os.path.join(self.temp_dir, "bot.err")
        with open(self.bot_err_path, "w") as f:
            f.write("startup line\n")

        self.bus = ShoulderTapEventBus()
        self.server = WebUIServer(port=18991, event_bus=self.bus)

    async def asyncTearDown(self) -> None:
        self.bus.clear()
        if hasattr(self.server, "stop"):
            try:
                await self.server.stop()
            except Exception:
                pass
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_sniff_receipt_all_five_subsystems(self) -> None:
        """
        Verify that each sniff cycle builds and records a complete receipt
        with all 5 subsystems:
        1. Fleet reachability
        2. Disk usage
        3. Docker container status
        4. bot.err byte offset & deltas
        5. Linear assigned-ticket watchdog telemetry
        """
        sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            webui_server=self.server,
            initial_offset=0,
        )

        # Mock snapshot with specific fleet, disk, and docker states
        mock_snapshot = FleetSnapshot(
            timestamp=time.time(),
            nodes={
                "hermes": {"online": True, "status": "Online"},
                "desktop-pc": {"online": True, "status": "Online"},
                "personal-laptop": {"online": False, "status": "Offline"},
            },
            disk={
                "free_gb": 125.4,
                "percent_used": 42.1,
                "pressure": False,
            },
            docker={
                "docker_running": True,
                "container_count": 3,
                "containers": ["redis", "postgres", "golem"],
            },
        )

        mock_linear_tickets = [
            {
                "identifier": "CHO-217",
                "title": "Wire The Nose sentinel telemetry receipts",
                "state": "In Progress",
            }
        ]

        with patch.object(sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
            mock_fleet.return_value = mock_snapshot
            with patch.object(sentinel, "sniff_assigned_tickets", new_callable=AsyncMock) as mock_linear:
                mock_linear.return_value = mock_linear_tickets
                with patch.object(sentinel, "is_ticket_actively_worked", return_value=False):
                    await sentinel.tick()

        receipt = sentinel.last_sniff_receipt
        self.assertIsNotNone(receipt)
        assert receipt is not None

        # 1. Fleet Reachability
        self.assertEqual(receipt["fleet"]["total"], 3)
        self.assertEqual(receipt["fleet"]["online"], 2)

        # 2. Disk Usage
        self.assertAlmostEqual(receipt["disk"]["free_gb"], 125.4)
        self.assertAlmostEqual(receipt["disk"]["percent_used"], 42.1)
        self.assertFalse(receipt["disk"]["pressure"])

        # 3. Docker Container Status
        self.assertTrue(receipt["docker"]["running"])
        self.assertEqual(receipt["docker"]["container_count"], 3)

        # 4. bot.err offset and delta bytes
        self.assertGreater(receipt["bot_err"]["offset"], 0)
        self.assertEqual(receipt["bot_err"]["error_count"], 0)

        # 5. Linear Assigned-Ticket Watchdog Telemetry
        self.assertEqual(receipt["linear"]["assigned_count"], 1)
        self.assertEqual(receipt["linear"]["actionable_count"], 1)
        self.assertIn("CHO-217", receipt["linear"]["ticket_ids"])

        # Check formatted receipt string
        receipt_str = sentinel.last_sniff_receipt_str
        self.assertIn("[SNIFF]", receipt_str)
        self.assertIn("Fleet: 2/3 nodes online", receipt_str)
        self.assertIn("Disk: 125.4GB free (42.1%)", receipt_str)
        self.assertIn("Docker: running (3 containers)", receipt_str)
        self.assertIn("bot.err: offset=", receipt_str)
        self.assertIn("Linear: 1 assigned (1 actionable: CHO-217)", receipt_str)

    async def test_sniff_receipt_streamed_to_webui_terminal(self) -> None:
        """Verify sniff cycle receipts stream directly to WebUI Nose terminal logs and SSE."""
        sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            webui_server=self.server,
            initial_offset=0,
        )

        mock_snapshot = FleetSnapshot(
            timestamp=time.time(),
            nodes={"hermes": {"online": True}},
            disk={"free_gb": 50.0, "percent_used": 75.0, "pressure": False},
            docker={"docker_running": False, "container_count": 0, "containers": []},
        )

        broadcast_events: list[tuple[str, any]] = []
        original_broadcast = self.server.broadcast

        def capture_broadcast(event: str, data: any):
            broadcast_events.append((event, data))
            return original_broadcast(event, data)

        self.server.broadcast = capture_broadcast

        with patch.object(sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
            mock_fleet.return_value = mock_snapshot
            with patch.object(sentinel, "sniff_assigned_tickets", new_callable=AsyncMock) as mock_linear:
                mock_linear.return_value = []
                await sentinel.tick()

        # Verify appended to server recent logs for nose
        nose_logs = self.server._recent_logs.get("nose", [])
        self.assertTrue(any("[SNIFF]" in line and "Fleet: 1/1 nodes online" in line for line in nose_logs))

        # Verify broadcast to nose_log and nose_telemetry SSE channels
        nose_log_events = [data for event, data in broadcast_events if event == "nose_log"]
        self.assertTrue(any("[SNIFF]" in str(d.get("message", "")) for d in nose_log_events))

        nose_telemetry_events = [data for event, data in broadcast_events if event == "nose_telemetry"]
        self.assertTrue(any("sniff_receipt" in d for d in nose_telemetry_events))

    async def test_bus_alert_routing_of_nose_sniff(self) -> None:
        """Verify WebUIServer processes decoupled nose_sniff bus alerts correctly."""
        receipt_data = {
            "fleet": {"total": 4, "online": 4, "nodes": {}},
            "disk": {"free_gb": 200.0, "percent_used": 20.0, "pressure": False},
            "docker": {"running": True, "container_count": 5, "containers": []},
            "bot_err": {"offset": 1024, "delta_bytes": 0, "error_count": 0},
            "linear": {"assigned_count": 0, "actionable_count": 0, "ticket_ids": []},
            "timestamp": time.time(),
        }
        receipt_str = "[SNIFF] Fleet: 4/4 nodes online | Disk: 200.0GB free (20.0%) | Docker: running (5 containers) | bot.err: offset=1024b (+0b delta, 0 errs) | Linear: 0 assigned (0 actionable)"

        alert = ShoulderTapAlert(
            alert_type="nose_sniff",
            source="nose",
            timestamp=time.time(),
            summary=receipt_str,
            data=receipt_data,
        )

        self.server._process_bus_alert(alert)

        nose_logs = self.server._recent_logs.get("nose", [])
        self.assertTrue(any("[SNIFF]" in line and "Fleet: 4/4 nodes online" in line for line in nose_logs))
        self.assertEqual(self.server._organ_states["nose"].get("last_sniff"), receipt_str)

    async def test_linear_guardrail_zero_graphql_mutations(self) -> None:
        """
        Verify strict Linear guardrail:
        NEVER call Linear GraphQL mutations or modify issue states from The Hand.
        Only read-only inspection methods are invoked.
        """
        mock_bridge = MagicMock()
        mock_bridge.get_assigned_issues.return_value = [
            {"identifier": "CHO-217", "title": "Watchdog Test", "state": "Todo"}
        ]

        sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            linear_bridge=mock_bridge,
            webui_server=self.server,
            initial_offset=0,
        )

        with patch.object(sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
            mock_fleet.return_value = FleetSnapshot()
            with patch.object(sentinel, "is_ticket_actively_worked", return_value=False):
                await sentinel.tick()

        # get_assigned_issues was called
        mock_bridge.get_assigned_issues.assert_called_once_with(non_done_only=True)

        # STRICT GUARDRAIL ASSERTION: Zero GraphQL mutations or transition calls allowed
        for forbidden_method in [
            "update_issue",
            "transition_issue",
            "set_issue_state",
            "mutate",
            "execute_mutation",
            "create_issue",
            "close_issue",
        ]:
            if hasattr(mock_bridge, forbidden_method):
                getattr(mock_bridge, forbidden_method).assert_not_called()

    async def test_webui_trigger_sniff_cycle(self) -> None:
        """Verify WebUIServer.trigger_sniff_cycle triggers NoseSentinel.tick() on demand."""
        mock_sentinel = MagicMock()
        mock_sentinel.tick = AsyncMock(return_value=None)
        self.server.nose_sentinel = mock_sentinel
        self.server._nose_sentinel = mock_sentinel

        result = await self.server.trigger_sniff_cycle()
        mock_sentinel.tick.assert_awaited_once()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
