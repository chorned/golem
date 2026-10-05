#!/usr/bin/env python3
"""
tests/test_webui_hands_sse.py
Targeted test suite for Milestone 2: WebUI Server Hands State & SSE Broadcasting.

Validates:
  1. _SyncAndAwaitableNone polymorphic behavior (synchronous invocation vs await expression).
  2. Hands lifecycle methods: register_active_hand, update_hand_buffer, unregister_active_hand.
  3. Auto-registration of unknown panes and 4,096-character buffer tail capping.
  4. /api/stream endpoint route alias and immediate hands_status push upon connection.
  5. ShoulderTapEventBus ↔ WebUI SSE bridge (eye_log, eye_telemetry, nose_alert, nose_telemetry).
  6. Cross-thread alert dispatch via loop.call_soon_threadsafe without event loop deadlock.
  7. In-memory log ring buffering replacing filesystem scraping.
"""

from __future__ import annotations

import os
import sys
import time
import json
import socket
import asyncio
import threading
import unittest
from typing import Dict, Any, Optional, List
from unittest.mock import patch, MagicMock

import aiohttp
from aiohttp import web, ClientSession

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
    get_default_event_bus,
    reset_default_event_bus,
)
from golem.webui.server import (
    WebUIServer,
    WebUILauncher,
    _SyncAndAwaitableNone,
    format_sse_event,
    get_system_status,
    extract_recent_agent_logs,
    start_webui_server,
    stop_webui_server,
    get_active_webui_server,
)


def _get_free_port() -> int:
    """Acquire a temporary available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestSyncAndAwaitableNone(unittest.IsolatedAsyncioTestCase):
    """Verify the dual sync/async behavior of _SyncAndAwaitableNone."""

    def test_sync_execution_returns_instance_without_error(self) -> None:
        """Invoking synchronously returns _SyncAndAwaitableNone without requiring await."""
        obj = _SyncAndAwaitableNone()
        self.assertIsInstance(obj, _SyncAndAwaitableNone)

    async def test_await_execution_returns_none(self) -> None:
        """Awaiting _SyncAndAwaitableNone executes as an awaitable returning None."""
        obj = _SyncAndAwaitableNone()
        res = await obj
        self.assertIsNone(res)


class TestWebUIHandsState(unittest.IsolatedAsyncioTestCase):
    """Test active hands state tracking in WebUIServer."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    def test_register_active_hand_sync(self) -> None:
        """Synchronous register_active_hand creates a correct hand record."""
        ret = self.server.register_active_hand(
            pane_id="runner_01",
            node="desktop-pc",
            command="cargo test",
            pid=4242,
            worker="agy",
        )
        # Verify return type is _SyncAndAwaitableNone
        self.assertIsInstance(ret, _SyncAndAwaitableNone)

        hands = self.server.active_hands
        self.assertIn("runner_01", hands)
        hand = hands["runner_01"]
        self.assertEqual(hand["node"], "desktop-pc")
        self.assertEqual(hand["command"], "cargo test")
        self.assertEqual(hand["pid"], 4242)
        self.assertEqual(hand["worker"], "agy")
        self.assertEqual(hand["status"], "RUNNING")
        self.assertIn("Agent 'runner_01' initialized", hand["buffer_tail"])
        self.assertIn("PID: 4242", hand["buffer_tail"])

    async def test_register_active_hand_awaited(self) -> None:
        """Awaiting register_active_hand succeeds without TypeError."""
        await self.server.register_active_hand(
            pane_id="runner_async",
            node="oci-machine",
            command="docker ps",
            pid=9999,
            worker="opencode",
        )
        hands = self.server.active_hands
        self.assertIn("runner_async", hands)
        self.assertEqual(hands["runner_async"]["worker"], "opencode")

    def test_register_active_hand_optional_pid(self) -> None:
        """Registering with pid=None formats PID: N/A in buffer tail without crash."""
        self.server.register_active_hand(
            pane_id="runner_nopid",
            node="localhost",
            command="npm start",
            pid=None,
        )
        hand = self.server.active_hands["runner_nopid"]
        self.assertIsNone(hand["pid"])
        self.assertIn("PID: N/A", hand["buffer_tail"])

    async def test_update_hand_buffer_sync_and_async(self) -> None:
        """Updating hand buffer appends chunks in FIFO order."""
        self.server.register_active_hand(pane_id="p1", command="build")
        
        # Sync call
        self.server.update_hand_buffer("p1", "Chunk 1\n")
        # Async call
        await self.server.update_hand_buffer("p1", "Chunk 2\n")

        tail = self.server.active_hands["p1"]["buffer_tail"]
        self.assertIn("Chunk 1\n", tail)
        self.assertIn("Chunk 2\n", tail)

    def test_update_hand_buffer_auto_register_unknown_pane(self) -> None:
        """Calling update_hand_buffer on an un-registered pane auto-registers it."""
        self.server.update_hand_buffer("unregistered_pane", "Output from unknown hand\n")
        hands = self.server.active_hands
        self.assertIn("unregistered_pane", hands)
        self.assertIn("Output from unknown hand\n", hands["unregistered_pane"]["buffer_tail"])

    def test_update_hand_buffer_capping_at_4096_chars(self) -> None:
        """Buffer tail is capped at 4,096 characters to prevent unbounded memory growth."""
        self.server.register_active_hand(pane_id="big_buffer", command="stream")
        large_chunk = "X" * 5000
        self.server.update_hand_buffer("big_buffer", large_chunk)
        tail = self.server.active_hands["big_buffer"]["buffer_tail"]
        self.assertEqual(len(tail), 4096)
        self.assertTrue(tail.endswith("XXXXX"))

    async def test_unregister_active_hand_sync_and_async(self) -> None:
        """Unregister removes hand from registry cleanly."""
        self.server.register_active_hand(pane_id="p_remove_1", command="cmd1")
        self.server.register_active_hand(pane_id="p_remove_2", command="cmd2")

        # Sync unregister
        self.server.unregister_active_hand("p_remove_1")
        self.assertNotIn("p_remove_1", self.server.active_hands)

        # Async unregister
        await self.server.unregister_active_hand("p_remove_2")
        self.assertNotIn("p_remove_2", self.server.active_hands)

        # Non-existent pane unregister is a no-op without error
        self.server.unregister_active_hand("non_existent_pane")


class TestWebUISSEEndpointsAndBroadcasting(unittest.IsolatedAsyncioTestCase):
    """Test SSE streaming endpoints, route aliases, and broadcasting."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    async def test_api_stream_alias_route_and_connected_handshake(self) -> None:
        """GET /api/stream returns 200 text/event-stream with initial connected event."""
        url = f"http://127.0.0.1:{self.port}/api/stream"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                self.assertEqual(resp.status, 200)
                self.assertIn("text/event-stream", resp.headers.get("Content-Type", ""))

                # Read first line: event: connected
                line1 = await resp.content.readline()
                self.assertEqual(line1.decode().strip(), "event: connected")

                # Read second line: data: {...}
                line2 = await resp.content.readline()
                data_str = line2.decode().replace("data: ", "").strip()
                data_json = json.loads(data_str)
                self.assertEqual(data_json["status"], "connected")
                self.assertEqual(data_json["port"], self.port)

    async def test_immediate_hands_status_on_client_connect(self) -> None:
        """When hands are active, client receives hands_status immediately upon connection."""
        self.server.register_active_hand(
            pane_id="warm_runner",
            node="localhost",
            command="pytest",
            pid=1234,
        )

        url = f"http://127.0.0.1:{self.port}/events"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                # Read connected event
                await resp.content.readline()  # event: connected
                await resp.content.readline()  # data: {...}
                await resp.content.readline()  # empty separator line

                # Read next event: hands_status
                ev_line = await resp.content.readline()
                self.assertEqual(ev_line.decode().strip(), "event: hands_status")
                data_line = await resp.content.readline()
                hands_list = json.loads(data_line.decode().replace("data: ", "").strip())
                self.assertEqual(len(hands_list), 1)
                self.assertEqual(hands_list[0]["pane_id"], "warm_runner")

    async def test_broadcast_sync_and_async_reaches_subscribers(self) -> None:
        """Broadcast sends formatted event to connected client queues."""
        url = f"http://127.0.0.1:{self.port}/events"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                # Drain initial connected event
                await resp.content.readline()
                await resp.content.readline()
                await resp.content.readline()

                # Broadcast sync
                self.server.broadcast("hand_buffer", {"pane_id": "p1", "delta": "foo"})

                line_ev = await resp.content.readline()
                self.assertEqual(line_ev.decode().strip(), "event: hand_buffer")
                line_data = await resp.content.readline()
                data = json.loads(line_data.decode().replace("data: ", "").strip())
                self.assertEqual(data["delta"], "foo")
                await resp.content.readline()  # separator

                # Broadcast async / awaited
                await self.server.broadcast("mouth_briefing", {"briefing": "Executive brief", "model": "flash"})
                line_ev2 = await resp.content.readline()
                self.assertEqual(line_ev2.decode().strip(), "event: mouth_briefing")
                line_data2 = await resp.content.readline()
                data2 = json.loads(line_data2.decode().replace("data: ", "").strip())
                self.assertEqual(data2["briefing"], "Executive brief")


class TestShoulderTapEventBusWebUIBridge(unittest.IsolatedAsyncioTestCase):
    """Test ShoulderTapEventBus ↔ WebUIServer SSE Bridge and thread-safe dispatch."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port, event_bus=self.bus)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()
        self.bus.clear()

    async def test_eye_blocker_alert_bridges_to_eye_log_and_telemetry(self) -> None:
        """Publishing a blocker alert on the bus emits eye_log (state='BLOCKER') and eye_telemetry."""
        url = f"http://127.0.0.1:{self.port}/events"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                # Drain connected handshake
                await resp.content.readline()
                await resp.content.readline()
                await resp.content.readline()

                # Publish blocker alert on the sensory bus
                alert = ShoulderTapAlert(
                    alert_type="blocker",
                    source="eyes",
                    summary="OAuth interactive login required",
                    data={"type": "oauth", "command": "rclone config", "pid": 555},
                )
                self.bus.publish(alert)

                # Expect eye_log event
                ev_line = await resp.content.readline()
                self.assertEqual(ev_line.decode().strip(), "event: eye_log")
                data_line = await resp.content.readline()
                log_data = json.loads(data_line.decode().replace("data: ", "").strip())
                self.assertEqual(log_data["state"], "BLOCKER")
                self.assertIn("OAuth interactive login required", log_data["message"])
                await resp.content.readline()  # separator

                # Expect eye_telemetry event
                ev_line2 = await resp.content.readline()
                self.assertEqual(ev_line2.decode().strip(), "event: eye_telemetry")
                data_line2 = await resp.content.readline()
                telem_data = json.loads(data_line2.decode().replace("data: ", "").strip())
                self.assertEqual(telem_data["state"], "BLOCKER")
                self.assertEqual(telem_data["pid"], 555)

    async def test_eye_completion_alert_bridges_to_eye_log_and_idle(self) -> None:
        """Publishing a completion alert on the bus emits eye_log (state='IDLE') and resets telemetry."""
        url = f"http://127.0.0.1:{self.port}/events"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                # Drain connected handshake
                await resp.content.readline()
                await resp.content.readline()
                await resp.content.readline()

                # Publish completion alert
                alert = ShoulderTapAlert(
                    alert_type="completion",
                    source="eyes",
                    summary="Build completed with code 0",
                    data={"is_error": False},
                )
                self.bus.publish(alert)

                ev_line = await resp.content.readline()
                self.assertEqual(ev_line.decode().strip(), "event: eye_log")
                data_line = await resp.content.readline()
                log_data = json.loads(data_line.decode().replace("data: ", "").strip())
                self.assertEqual(log_data["state"], "IDLE")
                self.assertIn("Build completed with code 0", log_data["message"])

    async def test_nose_health_anomaly_bridges_to_nose_alert_and_telemetry(self) -> None:
        """Publishing a health_anomaly alert on the bus emits nose_alert and nose_telemetry."""
        url = f"http://127.0.0.1:{self.port}/events"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                # Drain connected handshake
                await resp.content.readline()
                await resp.content.readline()
                await resp.content.readline()

                alert = ShoulderTapAlert(
                    alert_type="health_anomaly",
                    source="nose",
                    summary="Root disk pressure detected: 94% used",
                    data={"smells_off": True, "disk": {"free_gb": 4.1, "pressure": True}},
                )
                self.bus.publish(alert)

                # Expect nose_alert
                ev_line = await resp.content.readline()
                self.assertEqual(ev_line.decode().strip(), "event: nose_alert")
                data_line = await resp.content.readline()
                alert_data = json.loads(data_line.decode().replace("data: ", "").strip())
                self.assertTrue(alert_data["smells_off"])
                self.assertEqual(alert_data["summary"], "Root disk pressure detected: 94% used")
                await resp.content.readline()

                # Expect nose_telemetry
                ev_line2 = await resp.content.readline()
                self.assertEqual(ev_line2.decode().strip(), "event: nose_telemetry")
                data_line2 = await resp.content.readline()
                telem_data = json.loads(data_line2.decode().replace("data: ", "").strip())
                self.assertTrue(telem_data["smells_off"])

    async def test_cross_thread_dispatch_via_call_soon_threadsafe(self) -> None:
        """Publishing alerts from a background OS thread dispatches safely via loop.call_soon_threadsafe."""
        url = f"http://127.0.0.1:{self.port}/events"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                # Drain connected handshake
                await resp.content.readline()
                await resp.content.readline()
                await resp.content.readline()

                def _bg_publisher():
                    # Runs in a separate OS thread without an event loop
                    alert = ShoulderTapAlert(
                        alert_type="eye_log",
                        source="eyes",
                        summary="Background thread observer frame",
                        data={"state": "RUNNING"},
                    )
                    self.bus.publish(alert)

                bg_thread = threading.Thread(target=_bg_publisher)
                bg_thread.start()
                bg_thread.join(timeout=2.0)

                # Verify event arrived on main loop SSE stream
                ev_line = await resp.content.readline()
                self.assertEqual(ev_line.decode().strip(), "event: eye_log")
                data_line = await resp.content.readline()
                data = json.loads(data_line.decode().replace("data: ", "").strip())
                self.assertEqual(data["message"], "Background thread observer frame")

    async def test_in_memory_log_ring_buffer_populated_by_bus_alerts(self) -> None:
        """Bridge alerts populate in-memory logs in WebUIServer and extract_recent_agent_logs reflects them."""
        alert = ShoulderTapAlert(
            alert_type="eye_log",
            source="eyes",
            summary="Real-time in-memory eye trace",
            data={"state": "RUNNING"},
        )
        self.bus.publish(alert)
        # Give event loop a tick to process call_soon_threadsafe
        await asyncio.sleep(0.05)

        logs = self.server._recent_logs.get("eyes", [])
        self.assertTrue(any("Real-time in-memory eye trace" in l for l in logs))


class TestSystemStatusActiveHandsReflection(unittest.IsolatedAsyncioTestCase):
    """Test get_system_status integration with active hands."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = await start_webui_server(host="127.0.0.1", port=self.port)

    async def asyncTearDown(self) -> None:
        await stop_webui_server()

    async def test_get_system_status_reflects_active_hands(self) -> None:
        """get_system_status reflects active hands and updates eyes state."""
        server = get_active_webui_server()
        self.assertIsNotNone(server)

        # Before register: hands empty
        status_before = get_system_status()
        self.assertEqual(len(status_before["hands"]), 0)

        # Register hand
        server.register_active_hand(
            pane_id="test_runner",
            node="localhost",
            command="python3 -m unittest",
            pid=777,
        )

        status_after = get_system_status()
        self.assertEqual(len(status_after["hands"]), 1)
        self.assertEqual(status_after["hands"][0]["pane_id"], "test_runner")
        self.assertEqual(status_after["eyes"]["state"], "RUNNING")
        self.assertEqual(status_after["eyes"]["active_command"], "python3 -m unittest")
        self.assertEqual(status_after["eyes"]["pid"], 777)

        # Unregister hand
        server.unregister_active_hand("test_runner")
        status_cleared = get_system_status()
        self.assertEqual(len(status_cleared["hands"]), 0)


if __name__ == "__main__":
    unittest.main()
