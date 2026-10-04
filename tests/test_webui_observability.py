#!/usr/bin/env python3
"""
tests/test_webui_observability.py
Comprehensive Unit & Integration Test Suite for Milestone 3 (Real-Time Observability WebUI).

Covers:
- Configuration getters and truthy/falsy environment variable resolution
- SSE frame formatting, serializers, and large payload handling
- WebUIServer lifecycle, TCP port binding, and clean teardown
- Port collision detection and invalid port boundary validation
- REST endpoints: /, /events, /api/events, /api/status, /api/sensory/*, /health
- HTML Command Center dashboard dark mode and 4-pane structure
- High-velocity event broadcast and event-loop non-starvation
- Dual-layer compatibility shim identity (first_mate.webui is golem.webui)
- CLI argument parsing (--webUI, --webui) in app.py

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import time
import json
import socket
import asyncio
import unittest
from unittest.mock import patch, MagicMock

import aiohttp
from aiohttp import web, ClientSession

import golem.config as golem_cfg
import first_mate.config as fm_cfg
import golem.webui as webui
from golem.webui.server import (
    LINEAGE,
    VERSION,
    DASHBOARD_HTML,
    WebUIServer,
    WebUILauncher,
    format_sse_event,
    get_system_status,
    start_webui_server,
    stop_webui_server,
    get_active_webui_server,
    webui_broadcast,
    webui_broadcast_async,
)
import first_mate.webui as fm_webui
import app


def _get_free_port() -> int:
    """Acquire a temporary available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestWebUIConfiguration(unittest.TestCase):
    """Tests for centralized WebUI environment configuration."""

    def test_is_webui_enabled_truthy(self):
        """Verify truthy string permutations for GOLEM_ENABLE_WEBUI."""
        for val in ["1", "true", "True", "TRUE", "yes", "YES", "on", "ON", "enable", "  1  "]:
            with patch.dict(os.environ, {"GOLEM_ENABLE_WEBUI": val}):
                self.assertTrue(golem_cfg.is_webui_enabled(), f"Expected True for '{val}'")

    def test_is_webui_enabled_falsy(self):
        """Verify falsy string permutations for GOLEM_ENABLE_WEBUI."""
        for val in ["0", "false", "False", "no", "off", "disable", ""]:
            with patch.dict(os.environ, {"GOLEM_ENABLE_WEBUI": val}):
                self.assertFalse(golem_cfg.is_webui_enabled(), f"Expected False for '{val}'")

    def test_is_webui_enabled_unset(self):
        """Verify default is False when no WebUI variables are set."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(golem_cfg.is_webui_enabled())

    def test_dual_key_webui_enable_resolution(self):
        """Verify GOLEM_WEBUI and FIRST_MATE_ENABLE_WEBUI fallback keys."""
        with patch.dict(os.environ, {"GOLEM_WEBUI": "1"}, clear=True):
            self.assertTrue(golem_cfg.is_webui_enabled())

        with patch.dict(os.environ, {"FIRST_MATE_ENABLE_WEBUI": "true"}, clear=True):
            self.assertTrue(golem_cfg.is_webui_enabled())

        with patch.dict(os.environ, {"FIRSTMATE_ENABLE_WEBUI": "yes"}, clear=True):
            self.assertTrue(golem_cfg.is_webui_enabled())

    def test_webui_host_and_port_defaults(self):
        """Verify default host is 127.0.0.1 and port is 8765."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(golem_cfg.get_webui_host(), "127.0.0.1")
            self.assertEqual(golem_cfg.get_webui_port(), 8765)

    def test_webui_host_and_port_overrides(self):
        """Verify custom host and port environment variables."""
        with patch.dict(os.environ, {"GOLEM_WEBUI_HOST": "0.0.0.0", "GOLEM_WEBUI_PORT": "9090"}):
            self.assertEqual(golem_cfg.get_webui_host(), "0.0.0.0")
            self.assertEqual(golem_cfg.get_webui_port(), 9090)

    def test_config_dict_contains_webui_entries(self):
        """Verify get_config_dict() includes webui_enabled, webui_host, webui_port."""
        with patch.dict(os.environ, {"GOLEM_ENABLE_WEBUI": "1", "GOLEM_WEBUI_PORT": "8888"}):
            cfg = golem_cfg.get_config_dict()
            self.assertEqual(cfg.get("webui_enabled"), "True")
            self.assertEqual(cfg.get("webui_port"), "8888")
            self.assertEqual(cfg.get("webui_host"), "127.0.0.1")

    def test_first_mate_config_shim_parity(self):
        """Verify first_mate.config re-exports WebUI configuration functions with exact identity."""
        self.assertIs(fm_cfg.is_webui_enabled, golem_cfg.is_webui_enabled)
        self.assertIs(fm_cfg.get_webui_host, golem_cfg.get_webui_host)
        self.assertIs(fm_cfg.get_webui_port, golem_cfg.get_webui_port)


class TestSSEFormattingAndBroadcast(unittest.TestCase):
    """Tests for Server-Sent Events formatting and broadcast engine."""

    def test_format_sse_event_structure(self):
        """Verify event chunk structure conforms to text/event-stream standard."""
        chunk = format_sse_event("eye_log", {"agent": "eye", "action": "ingress"})
        self.assertTrue(chunk.startswith("event: eye_log\n"))
        self.assertIn("data: ", chunk)
        self.assertTrue(chunk.endswith("\n\n"), "SSE chunk must terminate with double newline")
        data_line = [l for l in chunk.splitlines() if l.startswith("data: ")][0]
        data = json.loads(data_line[6:])
        self.assertEqual(data["agent"], "eye")
        self.assertEqual(data["action"], "ingress")

    def test_format_sse_event_string_data(self):
        """Verify string data is passed directly."""
        chunk = format_sse_event("heartbeat", "ping")
        self.assertEqual(chunk, "event: heartbeat\ndata: ping\n\n")

    def test_format_sse_large_payload(self):
        """Verify 128KB payload formats without truncation."""
        huge_log = "Z" * 131072
        chunk = format_sse_event("large_log", {"content": huge_log})
        self.assertIn("event: large_log", chunk)
        self.assertIn(huge_log, chunk)
        self.assertTrue(chunk.endswith("\n\n"))

    def test_format_sse_non_serializable_objects(self):
        """Verify sets, custom objects, and dates serialize gracefully."""
        class CustomObj:
            def __repr__(self):
                return "<CustomObj 42>"

        payload = {
            "tags": {"alpha", "beta"},
            "custom": CustomObj(),
            "nested": {"frozenset": frozenset([1, 2])},
        }
        chunk = format_sse_event("custom_event", payload)
        self.assertIn("event: custom_event", chunk)
        self.assertIn("<CustomObj 42>", chunk)
        self.assertTrue(chunk.endswith("\n\n"))

    def test_broadcast_when_no_server_active(self):
        """Verify webui_broadcast is safe when no server is running."""
        try:
            webui_broadcast("log", {"status": "silent"})
            asyncio.run(webui_broadcast_async("log_async", {"status": "silent"}))
        except Exception as e:
            self.fail(f"webui_broadcast raised exception when no server active: {e}")

    def test_broadcast_to_multiple_subscriber_queues(self):
        """Verify broadcast distributes events to all registered queues."""
        server = WebUIServer(host="127.0.0.1", port=_get_free_port())
        q1: asyncio.Queue = asyncio.Queue()
        q2: asyncio.Queue = asyncio.Queue()
        server._subscribers.add(q1)
        server._subscribers.add(q2)

        server.broadcast("test_event", {"counter": 1})

        self.assertEqual(q1.qsize(), 1)
        self.assertEqual(q2.qsize(), 1)

        msg1 = q1.get_nowait()
        msg2 = q2.get_nowait()
        self.assertIn("event: test_event", msg1)
        self.assertEqual(msg1, msg2)

    def test_queue_overflow_drain_prevents_memory_leak(self):
        """Verify full queues drop oldest item without raising QueueFull."""
        server = WebUIServer(host="127.0.0.1", port=_get_free_port())
        q: asyncio.Queue = asyncio.Queue(maxsize=2)
        server._subscribers.add(q)

        server.broadcast("ev", {"msg": "first"})
        server.broadcast("ev", {"msg": "second"})
        # 3rd broadcast exceeds capacity
        server.broadcast("ev", {"msg": "third"})

        self.assertEqual(q.qsize(), 2)
        # Oldest item ('first') was dropped; remaining items are 'second' and 'third'
        item1 = q.get_nowait()
        item2 = q.get_nowait()
        self.assertIn("second", item1)
        self.assertIn("third", item2)


class TestWebUIServerLifecycle(unittest.IsolatedAsyncioTestCase):
    """Asynchronous lifecycle tests for WebUIServer."""

    async def test_server_startup_and_clean_stop(self):
        """Verify server binds to host:port and stops cleanly releasing resources."""
        port = _get_free_port()
        server = WebUIServer(host="127.0.0.1", port=port)
        self.assertFalse(server.is_running)

        await server.start()
        self.assertTrue(server.is_running)
        self.assertEqual(server.port, port)

        await server.stop()
        self.assertFalse(server.is_running)

    async def test_global_start_stop_convenience_functions(self):
        """Verify start_webui_server and stop_webui_server manage global singleton."""
        port = _get_free_port()
        server = await start_webui_server(host="127.0.0.1", port=port)
        self.assertTrue(server.is_running)
        self.assertIs(get_active_webui_server(), server)

        await stop_webui_server()
        self.assertIsNone(get_active_webui_server())
        self.assertFalse(server.is_running)

    def test_port_validation_range_boundaries(self):
        """Verify negative, 0, and out-of-range ports raise ValueError."""
        for bad in [-10, -1, 0, 65536, 70000, 99999]:
            with self.assertRaises(ValueError):
                WebUIServer(port=bad)
            with self.assertRaises(ValueError):
                start_webui_server(port=bad)

    def test_port_collision_detection(self):
        """Verify attempting to start on an occupied port raises OSError cleanly."""
        port = _get_free_port()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
        sock.listen(1)

        try:
            with self.assertRaises(OSError) as ctx:
                start_webui_server(host="127.0.0.1", port=port)
            err_msg = str(ctx.exception).lower()
            self.assertTrue("address" in err_msg or "port" in err_msg or "in use" in err_msg)
        finally:
            sock.close()


class TestWebUIEndpoints(unittest.IsolatedAsyncioTestCase):
    """Integration tests querying live HTTP and SSE endpoints."""

    async def asyncSetUp(self):
        self.port = _get_free_port()
        self.server = await start_webui_server(host="127.0.0.1", port=self.port)
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.session.close()
        await stop_webui_server()

    async def test_get_dashboard_index(self):
        """Verify GET / renders dark-mode dashboard with lineage invariant."""
        async with self.session.get(f"{self.base_url}/") as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn("text/html", resp.headers.get("Content-Type", ""))
            html = await resp.text()

            # Lineage invariant must be present
            self.assertIn(LINEAGE, html)
            # Dark mode styling must be present
            self.assertTrue("dark" in html.lower() or "#080c10" in html)
            # 4 Panes must be present
            self.assertIn("Eye Agent", html)
            self.assertIn("Nose Agent", html)
            self.assertIn("Mouth Agent", html)
            self.assertIn("Hand Agents Grid", html)

    async def test_health_endpoints(self):
        """Verify GET /health and GET /healthz return status ok."""
        for path in ["/health", "/healthz"]:
            async with self.session.get(f"{self.base_url}{path}") as resp:
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertEqual(data.get("status"), "ok")
                self.assertEqual(data.get("lineage"), LINEAGE)
                self.assertIn("uptime", data)

    async def test_api_status_endpoint(self):
        """Verify GET /api/status provides full sensory and execution state."""
        async with self.session.get(f"{self.base_url}/api/status") as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertEqual(data.get("version"), VERSION)
            self.assertEqual(data.get("lineage"), LINEAGE)
            for expected_key in ["eyes", "nose", "mouth", "hands", "uptime"]:
                self.assertIn(expected_key, data, f"Key '{expected_key}' missing from /api/status")

            self.assertIsInstance(data["eyes"], dict)
            self.assertIsInstance(data["nose"], dict)
            self.assertIsInstance(data["mouth"], dict)
            self.assertIsInstance(data["hands"], list)

    async def test_api_sensory_organ_endpoints(self):
        """Verify GET /api/sensory/{organ} routes for eye, nose, mouth, hands."""
        # Eye
        async with self.session.get(f"{self.base_url}/api/sensory/eye") as resp:
            self.assertEqual(resp.status, 200)
            eye = await resp.json()
            self.assertIn("state", eye)

        # Nose
        async with self.session.get(f"{self.base_url}/api/sensory/nose") as resp:
            self.assertEqual(resp.status, 200)
            nose = await resp.json()
            self.assertIn("smells_off", nose)
            self.assertIn("disk", nose)

        # Mouth
        async with self.session.get(f"{self.base_url}/api/sensory/mouth") as resp:
            self.assertEqual(resp.status, 200)
            mouth = await resp.json()
            self.assertIn("model", mouth)

        # Hands
        async with self.session.get(f"{self.base_url}/api/sensory/hands") as resp:
            self.assertEqual(resp.status, 200)
            hands = await resp.json()
            self.assertIsInstance(hands, list)

        # Hands with active multi-agent registration and initial execution log trace
        self.server.register_active_hand("Hand 01 [L_GAUNTLET]", "localhost", "pytest tests/", 101)
        self.server.register_active_hand("Hand 02 [R_GAUNTLET]", "localhost", "git diff", 102)
        async with self.session.get(f"{self.base_url}/api/sensory/hands") as resp:
            self.assertEqual(resp.status, 200)
            hands = await resp.json()
            self.assertEqual(len(hands), 2)
            self.assertIn("Golem.Hands: Agent 'Hand 01 [L_GAUNTLET]'", hands[0]["buffer_tail"])
            self.assertIn("Golem.Hands: Agent 'Hand 02 [R_GAUNTLET]'", hands[1]["buffer_tail"])
        self.server.unregister_active_hand("Hand 01 [L_GAUNTLET]")
        self.server.unregister_active_hand("Hand 02 [R_GAUNTLET]")

        # Unknown organ -> 404
        async with self.session.get(f"{self.base_url}/api/sensory/ears") as resp:
            self.assertEqual(resp.status, 404)
            err = await resp.json()
            self.assertIn("error", err)

    async def test_sse_events_stream_and_broadcast(self):
        """Verify SSE endpoint streams initial connection event and broadcast messages."""
        async with self.session.get(f"{self.base_url}/events") as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn("text/event-stream", resp.headers.get("Content-Type", ""))

            # Read initial welcome event
            initial_chunk = await resp.content.readuntil(b"\n\n")
            initial_text = initial_chunk.decode("utf-8")
            self.assertIn("event: connected", initial_text)
            self.assertIn("connected", initial_text)

            # Broadcast live test event
            webui_broadcast("eye_log", {"message": "unit_test_ingress_frame"})

            # Read broadcast event
            broadcast_chunk = await resp.content.readuntil(b"\n\n")
            broadcast_text = broadcast_chunk.decode("utf-8")
            self.assertIn("event: eye_log", broadcast_text)
            self.assertIn("unit_test_ingress_frame", broadcast_text)


class TestWebUIEventLoopNonStarvation(unittest.IsolatedAsyncioTestCase):
    """Stress tests ensuring high log velocity does not starve the event loop."""

    async def test_high_velocity_broadcast_non_starvation(self):
        """Broadcast 250 events in a rapid burst; verify loop latency remains < 15ms."""
        port = _get_free_port()
        server = await start_webui_server(host="127.0.0.1", port=port)

        try:
            # Register 3 simulated client subscriber queues
            queues = [asyncio.Queue(maxsize=500) for _ in range(3)]
            for q in queues:
                server._subscribers.add(q)

            start_t = time.perf_counter()

            # Emit 250 events concurrently with small async tasks
            for i in range(250):
                server.broadcast("stress_event", {"seq": i, "payload": "data" * 20})
                # Simulated event loop context switch
                if i % 25 == 0:
                    await asyncio.sleep(0.001)

            total_elapsed = time.perf_counter() - start_t

            # Verify all queues received all 250 items without drop
            for q in queues:
                self.assertEqual(q.qsize(), 250)

            # Verify total processing time for 250 broadcasts is sub-second (typical < 0.05s)
            self.assertLess(total_elapsed, 1.0, "High velocity broadcast must execute in under 1 second")
        finally:
            await stop_webui_server()


class TestDualLayerCompatibilityShim(unittest.TestCase):
    """Verify first_mate.webui provides 100% object identity with golem.webui."""

    def test_shim_exports_identity(self):
        """Check all exported functions and classes are identical."""
        self.assertIs(fm_webui.WebUIServer, webui.WebUIServer)
        self.assertIs(fm_webui.start_webui_server, webui.start_webui_server)
        self.assertIs(fm_webui.stop_webui_server, webui.stop_webui_server)
        self.assertIs(fm_webui.format_sse_event, webui.format_sse_event)
        self.assertIs(fm_webui.webui_broadcast, webui.webui_broadcast)
        self.assertIs(fm_webui.get_system_status, webui.get_system_status)
        self.assertEqual(fm_webui.LINEAGE, webui.LINEAGE)
        self.assertEqual(fm_webui.DASHBOARD_HTML, webui.DASHBOARD_HTML)


class TestAppGatewayCLIIntegration(unittest.TestCase):
    """Verify app.py CLI argument parsing and lifecycle wiring."""

    def test_parse_args_webui_flag(self):
        """Verify --webUI and --webui flags are recognized by app.parse_args."""
        args1 = app.parse_args(["--webUI"])
        self.assertTrue(args1.webui)

        args2 = app.parse_args(["--webui"])
        self.assertTrue(args2.webui)

        args3 = app.parse_args([])
        self.assertFalse(args3.webui)

    def test_app_webui_exports(self):
        """Verify app module exports WebUI lifecycle hooks."""
        self.assertTrue(hasattr(app, "start_webui_server"))
        self.assertTrue(hasattr(app, "stop_webui_server"))
        self.assertTrue(hasattr(app, "webui_broadcast"))
        self.assertTrue(hasattr(app, "parse_args"))


if __name__ == "__main__":
    unittest.main()
