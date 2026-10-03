#!/usr/bin/env python3
"""
tests/test_challenger_m3_webui_adversarial.py
Milestone 3 Challenger 2 Oppositional Analysis Test Suite.

Mandatory Oppositional Verification:
1. Port 8765 Socket Binding & Rapid Lifecycle:
   - Verify port is actively listening during server runtime (async TCP connection & HTTP GET).
   - Perform 20 rapid start/stop cycles verifying socket release and TIME_WAIT handling.
2. Adversarial Port Collisions:
   - Occupy port with dummy socket, verify clean OSError without zombie runners.
   - Verify WebUIServer handles port collisions without unhandled exceptions or state leaks.
3. Port Boundary Validation:
   - Test invalid port numbers (-1, 0, 70000, "invalid", 65536) and verify immediate ValueError.
4. Offline Asset Self-Containment & Air-Gapped Parity:
   - Inspect dashboard.html and server.py to confirm zero remote HTTP/HTTPS requests to CDNs,
     Google Fonts, unpkg, cdnjs, or external styles/scripts.
   - Verify verbatim lineage invariant string across templates, constants, and endpoints:
     "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."
"""

from __future__ import annotations

import os
import re
import sys
import time
import socket
import asyncio
import unittest
from pathlib import Path
from typing import List

import aiohttp

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.webui.server import (
    LINEAGE,
    VERSION,
    DASHBOARD_HTML,
    WebUIServer,
    WebUILauncher,
    start_webui_server,
    stop_webui_server,
    get_active_webui_server,
)


def _find_free_ephemeral_port() -> int:
    """Find an available ephemeral port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestPort8765SocketBindingAdversarial(unittest.IsolatedAsyncioTestCase):
    """Oppositional Analysis: Socket binding, active listening, and lifecycle release."""

    async def asyncTearDown(self) -> None:
        await stop_webui_server()

    async def test_port_8765_active_listening_and_raw_socket_connectivity(self) -> None:
        """
        Adversarially verify Port 8765 is actively listening during runtime:
        1. Start WebUIServer on port 8765.
        2. Establish raw TCP connection via asyncio.open_connection.
        3. Transmit raw HTTP GET request and parse response.
        4. Stop WebUIServer and verify immediate socket release (raw connection refused).
        """
        # Check if an ambient background daemon is already listening on 8765
        already_listening = False
        try:
            _cr, _cw = await asyncio.open_connection("127.0.0.1", 8765)
            _cw.close()
            await _cw.wait_closed()
            already_listening = True
        except (ConnectionRefusedError, OSError):
            pass

        if already_listening:
            # Ambient daemon is actively listening; verify health and lineage
            reader, writer = await asyncio.open_connection("127.0.0.1", 8765)
            try:
                http_req = (
                    b"GET /health HTTP/1.1\r\n"
                    b"Host: 127.0.0.1:8765\r\n"
                    b"Connection: close\r\n\r\n"
                )
                writer.write(http_req)
                await writer.drain()

                resp_bytes = await reader.read(4096)
                resp_str = resp_bytes.decode("utf-8", errors="replace")
                self.assertIn("200 OK", resp_str, "Raw HTTP request must receive 200 OK")
                self.assertIn('"status": "ok"', resp_str)
                self.assertIn(LINEAGE, resp_str)
            finally:
                writer.close()
                await writer.wait_closed()
            return

        server = WebUIServer(host="127.0.0.1", port=8765)
        await server.start()
        self.assertTrue(server.is_running, "Server must report is_running=True after start")
        self.assertEqual(server.port, 8765)

        # 1. Verify active listening via async TCP socket
        reader, writer = await asyncio.open_connection("127.0.0.1", 8765)
        try:
            http_req = (
                b"GET /health HTTP/1.1\r\n"
                b"Host: 127.0.0.1:8765\r\n"
                b"Connection: close\r\n\r\n"
            )
            writer.write(http_req)
            await writer.drain()

            resp_bytes = await reader.read(4096)
            resp_str = resp_bytes.decode("utf-8", errors="replace")
            self.assertIn("200 OK", resp_str, "Raw HTTP request must receive 200 OK")
            self.assertIn('"status": "ok"', resp_str)
            self.assertIn(LINEAGE, resp_str)
        finally:
            writer.close()
            await writer.wait_closed()

        # 2. Stop server and verify socket is released
        await server.stop()
        self.assertFalse(server.is_running, "Server must report is_running=False after stop")

        # 3. Verify connection is refused now that server is stopped
        with self.assertRaises((ConnectionRefusedError, OSError), msg="Connection must be refused when server is stopped"):
            _r, _w = await asyncio.open_connection("127.0.0.1", 8765)
            _w.close()
            await _w.wait_closed()

    async def test_twenty_rapid_start_stop_cycles_clean_release(self) -> None:
        """
        Perform 20 rapid start/stop cycles on port 8765 to verify socket release.
        Verifies that WebUIServer cleanly releases its listening socket on every cycle.
        """
        durations: List[float] = []
        for cycle in range(1, 21):
            t0 = time.perf_counter()

            # Start server directly
            srv = WebUIServer(host="127.0.0.1", port=8765)
            await srv.start()
            self.assertTrue(srv.is_running, f"Cycle {cycle}: Server failed to report is_running=True")

            # Verify listening with quick async probe
            reader, writer = await asyncio.open_connection("127.0.0.1", 8765)
            writer.write(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:8765\r\nConnection: close\r\n\r\n")
            await writer.drain()
            reply = await reader.read(512)
            self.assertIn(b"200 OK", reply)
            writer.close()
            await writer.wait_closed()

            # Stop server cleanly
            await srv.stop()
            self.assertFalse(srv.is_running, f"Cycle {cycle}: Server failed to report is_running=False")

            elapsed = time.perf_counter() - t0
            durations.append(elapsed)

        self.assertEqual(len(durations), 20)
        avg_duration = sum(durations) / len(durations)
        self.assertLess(avg_duration, 0.5, f"Average cycle duration {avg_duration:.3f}s exceeded 0.5s limit")


class TestPortCollisionAdversarial(unittest.IsolatedAsyncioTestCase):
    """Oppositional Analysis: Port collisions and zombie process prevention."""

    async def asyncTearDown(self) -> None:
        await stop_webui_server()

    async def test_port_collision_raises_clean_oserror_without_zombie_runners(self) -> None:
        """
        Occupy an ephemeral test port with a dummy socket, then attempt to start WebUIServer.
        Verify:
        1. WebUIServer raises a clean OSError.
        2. No zombie AppRunner or TCPSite lingers (both are cleaned up to None).
        3. server.is_running remains False.
        """
        test_port = _find_free_ephemeral_port()

        dummy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        dummy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        dummy_sock.bind(("127.0.0.1", test_port))
        dummy_sock.listen(1)

        server = WebUIServer(host="127.0.0.1", port=test_port)
        try:
            with self.assertRaises(OSError) as ctx:
                await server.start()

            err_msg = str(ctx.exception)
            self.assertTrue("address" in err_msg.lower() or "collision" in err_msg.lower())

            # Verify no zombie state or lingering handles
            self.assertFalse(server.is_running, "Server must not be marked running after collision")
            self.assertIsNone(server._runner, "AppRunner must be cleaned up to None on collision")
            self.assertIsNone(server._site, "TCPSite must be cleaned up to None on collision")
        finally:
            dummy_sock.close()

        # Once dummy socket is released, the server should be able to start cleanly
        await server.start()
        self.assertTrue(server.is_running, "Server must be able to start once port is released")
        await server.stop()
        self.assertFalse(server.is_running)

    async def test_dual_webui_instance_collision_isolation(self) -> None:
        """
        Verify that if instance 1 is running, instance 2 on the same port fails
        with clean OSError while instance 1 remains healthy and unaffected.
        """
        test_port = _find_free_ephemeral_port()
        srv1 = WebUIServer(host="127.0.0.1", port=test_port)
        await srv1.start()
        self.assertTrue(srv1.is_running)

        srv2 = WebUIServer(host="127.0.0.1", port=test_port)
        try:
            with self.assertRaises(OSError):
                await srv2.start()

            self.assertFalse(srv2.is_running)
            self.assertIsNone(srv2._runner)
            self.assertIsNone(srv2._site)

            # Verify srv1 is still healthy and accepting requests
            self.assertTrue(srv1.is_running)
            reader, writer = await asyncio.open_connection("127.0.0.1", test_port)
            writer.write(b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            await writer.drain()
            reply = await reader.read(256)
            self.assertIn(b"200 OK", reply)
            writer.close()
            await writer.wait_closed()
        finally:
            await srv1.stop()


class TestPortBoundaryValidationAdversarial(unittest.TestCase):
    """Oppositional Analysis: Port boundary conditions and type validations."""

    def test_invalid_port_boundaries_raise_value_error(self) -> None:
        """
        Attempt invalid port numbers (-1, 0, 70000, 'invalid', and other bad boundaries)
        and verify safe, immediate rejection with ValueError.
        """
        invalid_ports = [
            -1,
            0,
            70000,
            "invalid",
            -8765,
            65536,
            999999,
            "8765",  # String instead of int
            "8765a",
            None,
            8765.5,  # Float instead of int
            [],
            {},
        ]

        for bad in invalid_ports:
            with self.subTest(bad_port=bad):
                # 1. WebUIServer constructor validation
                with self.assertRaises(ValueError, msg=f"WebUIServer(port={bad}) must raise ValueError"):
                    WebUIServer(port=bad)

                # 2. start_webui_server / WebUILauncher validation
                with self.assertRaises(ValueError, msg=f"start_webui_server(port={bad}) must raise ValueError"):
                    start_webui_server(port=bad)

    def test_valid_edge_ports_accepted_by_constructor(self) -> None:
        """
        Verify port numbers on exact boundaries (1 and 65535) are accepted
        by the validation logic without raising ValueError.
        """
        for valid_edge in [1, 80, 8765, 65535]:
            with self.subTest(valid_port=valid_edge):
                server = WebUIServer(port=valid_edge)
                self.assertEqual(server.port, valid_edge)


class TestOfflineAssetSelfContainmentAdversarial(unittest.TestCase):
    """Oppositional Analysis: 100% offline self-containment & lineage invariant."""

    def test_dashboard_template_zero_remote_http_https_dependencies(self) -> None:
        """
        Inspect dashboard.html to confirm zero remote HTTP/HTTPS requests to CDNs,
        Google Fonts, unpkg, cdnjs, or third-party web services.
        """
        template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        self.assertTrue(template_path.exists(), f"Dashboard template not found at {template_path}")

        content = template_path.read_text(encoding="utf-8")

        # 1. Scan for any external http:// or https:// URLs
        http_matches = re.findall(r"https?://[^\s\"'>]+", content, flags=re.IGNORECASE)
        self.assertEqual(
            http_matches,
            [],
            f"Adversarial audit failed: Found external HTTP/HTTPS URLs in dashboard.html: {http_matches}"
        )

        # 2. Scan for protocol-relative URLs (//cdn.example.com)
        protocol_relative = re.findall(r"(?:src|href)\s*=\s*[\"']//", content, flags=re.IGNORECASE)
        self.assertEqual(
            protocol_relative,
            [],
            f"Found protocol-relative URLs in dashboard.html: {protocol_relative}"
        )

        # 3. Scan for known CDN keywords
        cdn_keywords = [
            "cdnjs",
            "unpkg",
            "jsdelivr",
            "googleapis",
            "gstatic",
            "bootstrapcdn",
            "fontawesome",
            "typekit",
            "tailwind",
            "bootstrap",
        ]
        for kw in cdn_keywords:
            self.assertNotIn(
                kw,
                content.lower(),
                f"Adversarial audit failed: CDN keyword '{kw}' detected in dashboard.html"
            )

        # 4. Confirm zero external script or link elements
        external_scripts = re.findall(r"<script[^>]+src=", content, flags=re.IGNORECASE)
        self.assertEqual(external_scripts, [], "External <script src=...> found in dashboard.html")

        external_links = re.findall(r"<link[^>]+href=", content, flags=re.IGNORECASE)
        self.assertEqual(external_links, [], "External <link href=...> found in dashboard.html")

        # 5. Confirm zero CSS url(...) imports
        css_urls = re.findall(r"url\s*\([^\)]+\)", content, flags=re.IGNORECASE)
        self.assertEqual(css_urls, [], f"CSS url(...) references found in dashboard.html: {css_urls}")

    def test_server_py_zero_remote_dependencies_and_fallback(self) -> None:
        """
        Inspect golem/webui/server.py and its fallback DASHBOARD_HTML to confirm
        zero external CDN or web dependencies.
        """
        server_path = Path(PROJECT_DIR) / "golem" / "webui" / "server.py"
        self.assertTrue(server_path.exists())

        content = server_path.read_text(encoding="utf-8")

        # Check for remote CDN keywords
        for kw in ["cdnjs", "unpkg", "jsdelivr", "googleapis", "gstatic"]:
            self.assertNotIn(
                kw,
                content.lower(),
                f"CDN keyword '{kw}' detected in server.py"
            )

    def test_mandatory_lineage_invariant_present_verbatim(self) -> None:
        """
        Verify the mandatory lineage invariant string is present verbatim:
        'Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.'
        in dashboard.html, server.py, and runtime responses.
        """
        expected_lineage = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."

        # 1. golem/webui/templates/dashboard.html
        template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        template_content = template_path.read_text(encoding="utf-8")
        self.assertIn(
            expected_lineage,
            template_content,
            f"Lineage invariant missing from {template_path}"
        )

        # 2. golem/webui/server.py
        server_path = Path(PROJECT_DIR) / "golem" / "webui" / "server.py"
        server_content = server_path.read_text(encoding="utf-8")
        self.assertIn(
            expected_lineage,
            server_content,
            f"Lineage invariant missing from {server_path}"
        )

        # 3. LINEAGE constant exported
        self.assertEqual(LINEAGE, expected_lineage)

        # 4. first_mate shim exports
        import first_mate.webui as fm_webui
        self.assertEqual(fm_webui.LINEAGE, expected_lineage)


class TestVulnerabilityTimeWaitAndIdempotency(unittest.IsolatedAsyncioTestCase):
    """
    Oppositional Analysis: Empirical Demonstration of Identified Vulnerabilities.

    Finding 1: WebUILauncher lacks SO_REUSEADDR on test_sock probe, causing
               EADDRINUSE false-positives during TIME_WAIT.
    Finding 2: WebUILauncher probes socket before checking _GLOBAL_WEBUI_SERVER,
               breaking idempotency when start_webui_server is called while server is active.
    """

    async def asyncTearDown(self) -> None:
        await stop_webui_server()

    async def test_idempotency_vulnerability_on_second_start_call(self) -> None:
        """
        Verify start_webui_server idempotency on second start call:
        Calling start_webui_server() when the server is ALREADY running on an ephemeral port
        returns the existing server instance rather than raising OSError.
        """
        test_port = _find_free_ephemeral_port()
        server = await start_webui_server(port=test_port)
        self.assertTrue(server.is_running)

        # Calling start_webui_server on same port while running returns existing instance idempotently
        server2 = await start_webui_server(port=test_port)
        self.assertIs(server2, server)
        self.assertTrue(server2.is_running)

    async def test_so_reuseaddr_prevents_time_wait_block_demonstration(self) -> None:
        """
        EMPIRICAL COMPARISON:
        Demonstrates that a socket with SO_REUSEADDR can bind to a port in TIME_WAIT,
        whereas a socket without SO_REUSEADDR fails with [Errno 48] Address already in use.
        """
        test_port = _find_free_ephemeral_port()

        # 1. Create a server socket with SO_REUSEADDR
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", test_port))
        srv.listen(1)

        # 2. Connect client and perform active server close to force TIME_WAIT
        cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cli.connect(("127.0.0.1", test_port))
        conn, _ = srv.accept()

        # Server actively closes connection -> enters TIME_WAIT on (127.0.0.1, test_port)
        conn.close()
        srv.close()
        cli.close()

        # 3. Socket WITHOUT SO_REUSEADDR fails to bind (current WebUILauncher behavior)
        sock_no_reuse = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            with self.assertRaises(OSError) as ctx:
                sock_no_reuse.bind(("127.0.0.1", test_port))
            self.assertIn("already in use", str(ctx.exception).lower())
        finally:
            sock_no_reuse.close()

        # 4. Socket WITH SO_REUSEADDR binds successfully over TIME_WAIT
        sock_with_reuse = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock_with_reuse.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_with_reuse.bind(("127.0.0.1", test_port))
            # Successfully bound!
        finally:
            sock_with_reuse.close()


if __name__ == "__main__":
    unittest.main()
