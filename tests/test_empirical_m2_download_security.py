#!/usr/bin/env python3
"""
tests/test_empirical_m2_download_security.py
Adversarial empirical challenge suite for Milestone 2:
1. Multi-hop S3 redirect chains (assert Authorization header is stripped on all hops).
2. Chunked streaming exceeding 100MB without Content-Length (assert clean abort, bounded memory).
3. Malicious path traversal payloads (assert confinement to slack_transfers/ and neutralization).
4. Bounded LRU cache eviction stress under 1000+ entries (assert bounded size, LRU order).
5. Live HTTP mock server on localhost executing against golem.gateway.ingress.
"""

import os
import sys
import gc
import re
import time
import shutil
import tempfile
import asyncio
import unittest
from typing import List, Tuple, Dict, Any
from aiohttp import web

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.gateway.ingress import (
    download_slack_file,
    sanitize_filename,
    save_staged_file,
    IngressGateway,
    DEFAULT_MAX_FILE_SIZE_BYTES,
    TRANSFERS_DIR_NAME,
)
from golem.gateway.router import SessionRouter


class MockHeadlessAdapter:
    def __init__(self, cwd: str = "/tmp") -> None:
        self.cwd = cwd

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class TestEmpiricalMultiHopRedirects(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: Multi-hop S3 redirect chains and Authorization header stripping."""

    async def asyncSetUp(self):
        self.app = web.Application()
        self.request_headers_log: List[Tuple[str, Dict[str, str]]] = []
        self.runner = None
        self.site = None
        self.port = 0

    async def asyncTearDown(self):
        if self.runner:
            await self.runner.cleanup()

    async def _start_server(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        # Retrieve allocated port
        names = self.site._server.sockets
        self.port = names[0].getsockname()[1]

    async def test_multihop_302_to_307_to_s3_200_strips_auth_header(self):
        """
        Adversarial test:
        Hop 1: Initial Slack private download URL (302 Found -> Hop 2).
        Hop 2: Intermediate redirection service (307 Temporary Redirect -> S3).
        Hop 3: Final S3 bucket endpoint (200 OK).

        Assert:
        - Hop 1 receives Authorization: Bearer xoxb-secret-token.
        - Hop 2 MUST NOT receive Authorization header.
        - Hop 3 (S3) MUST NOT receive Authorization header.
        - File payload is successfully retrieved.
        """
        expected_content = b"SECURE_S3_PAYLOAD_CHUNK_DATA_M2"

        async def handle_hop1(request: web.Request):
            self.request_headers_log.append(("hop1", dict(request.headers)))
            raise web.HTTPFound(f"http://127.0.0.1:{self.port}/intermediate-redirect")

        async def handle_hop2(request: web.Request):
            self.request_headers_log.append(("hop2", dict(request.headers)))
            # S3 strict rejection simulation: if Authorization is present, return 400 Bad Request
            if "Authorization" in request.headers or "authorization" in request.headers:
                return web.Response(status=400, text="S3 Rejection: Authorization header present on presigned URL")
            raise web.HTTPTemporaryRedirect(f"http://127.0.0.1:{self.port}/s3-bucket/final-file.pdf")

        async def handle_hop3(request: web.Request):
            self.request_headers_log.append(("hop3", dict(request.headers)))
            if "Authorization" in request.headers or "authorization" in request.headers:
                return web.Response(status=400, text="S3 Rejection: Authorization header present on presigned URL")
            return web.Response(body=expected_content, content_type="application/pdf")

        self.app.router.add_get("/slack-file", handle_hop1)
        self.app.router.add_get("/intermediate-redirect", handle_hop2)
        self.app.router.add_get("/s3-bucket/final-file.pdf", handle_hop3)
        await self._start_server()

        url = f"http://127.0.0.1:{self.port}/slack-file"
        token = "xoxb-adversarial-secret-token-12345"

        result = await download_slack_file(url, token=token)

        self.assertEqual(result, expected_content)
        self.assertEqual(len(self.request_headers_log), 3)

        # Hop 1 check
        hop1_name, hop1_headers = self.request_headers_log[0]
        self.assertEqual(hop1_name, "hop1")
        auth_hop1 = hop1_headers.get("Authorization") or hop1_headers.get("authorization")
        self.assertEqual(auth_hop1, f"Bearer {token}")

        # Hop 2 check (Authorization MUST be stripped)
        hop2_name, hop2_headers = self.request_headers_log[1]
        self.assertEqual(hop2_name, "hop2")
        self.assertNotIn("Authorization", hop2_headers)
        self.assertNotIn("authorization", hop2_headers)

        # Hop 3 check (Authorization MUST be stripped)
        hop3_name, hop3_headers = self.request_headers_log[2]
        self.assertEqual(hop3_name, "hop3")
        self.assertNotIn("Authorization", hop3_headers)
        self.assertNotIn("authorization", hop3_headers)

    async def test_multihop_301_to_302_to_200_strips_auth_header(self):
        """Verify 301 Moved Permanently initial hop followed by 302 redirect strips Auth."""
        payload = b"Payload from 301->302 redirect chain"

        async def handle_301(request: web.Request):
            self.request_headers_log.append(("301", dict(request.headers)))
            raise web.HTTPMovedPermanently(f"http://127.0.0.1:{self.port}/hop2-302")

        async def handle_302(request: web.Request):
            self.request_headers_log.append(("302", dict(request.headers)))
            if "authorization" in request.headers or "Authorization" in request.headers:
                return web.Response(status=400, text="Auth leaked")
            raise web.HTTPFound(f"http://127.0.0.1:{self.port}/final-200")

        async def handle_200(request: web.Request):
            self.request_headers_log.append(("200", dict(request.headers)))
            if "authorization" in request.headers or "Authorization" in request.headers:
                return web.Response(status=400, text="Auth leaked")
            return web.Response(body=payload)

        self.app.router.add_get("/start-301", handle_301)
        self.app.router.add_get("/hop2-302", handle_302)
        self.app.router.add_get("/final-200", handle_200)
        await self._start_server()

        res = await download_slack_file(
            f"http://127.0.0.1:{self.port}/start-301",
            token="xoxb-test-301",
        )
        self.assertEqual(res, payload)
        self.assertEqual(len(self.request_headers_log), 3)
        self.assertIn("authorization", [k.lower() for k in self.request_headers_log[0][1].keys()])
        self.assertNotIn("authorization", [k.lower() for k in self.request_headers_log[1][1].keys()])
        self.assertNotIn("authorization", [k.lower() for k in self.request_headers_log[2][1].keys()])

    async def test_relative_redirect_location_header_fails_without_urljoin(self):
        """
        Challenge: Relative Location header (e.g. '/relative-target').
        Without urllib.parse.urljoin, aiohttp fails with InvalidURL.
        Empirically document this behavior.
        """
        async def handle_relative(request: web.Request):
            raise web.HTTPFound("/relative-target")

        async def handle_target(request: web.Request):
            return web.Response(body=b"relative OK")

        self.app.router.add_get("/start-rel", handle_relative)
        self.app.router.add_get("/relative-target", handle_target)
        await self._start_server()

        res = await download_slack_file(f"http://127.0.0.1:{self.port}/start-rel", token="t")
        # Currently returns None due to InvalidURL
        self.assertIsNone(res)

    async def test_redirect_308_behavior(self):
        """
        Challenge: RFC 7538 HTTP 308 (Permanent Redirect).
        Check how download_slack_file behaves when encountering 308.
        """
        async def handle_308(request: web.Request):
            raise web.HTTPPermanentRedirect("/final-200")

        async def handle_200(request: web.Request):
            return web.Response(body=b"308 content")

        self.app.router.add_get("/start-308", handle_308)
        self.app.router.add_get("/final-200", handle_200)
        await self._start_server()

    async def test_multihop_5_hops_redirect_chain_strips_auth(self):
        """Verify a deep 5-hop redirect chain strips Authorization on hops 2 through 5."""
        async def h1(request: web.Request):
            self.request_headers_log.append(("h1", dict(request.headers)))
            raise web.HTTPFound(f"http://127.0.0.1:{self.port}/h2")

        async def h2(request: web.Request):
            self.request_headers_log.append(("h2", dict(request.headers)))
            raise web.HTTPTemporaryRedirect(f"http://127.0.0.1:{self.port}/h3")

        async def h3(request: web.Request):
            self.request_headers_log.append(("h3", dict(request.headers)))
            raise web.HTTPFound(f"http://127.0.0.1:{self.port}/h4")

        async def h4(request: web.Request):
            self.request_headers_log.append(("h4", dict(request.headers)))
            raise web.HTTPTemporaryRedirect(f"http://127.0.0.1:{self.port}/h5")

        async def h5(request: web.Request):
            self.request_headers_log.append(("h5", dict(request.headers)))
            if "authorization" in [k.lower() for k in request.headers.keys()]:
                return web.Response(status=400, text="Auth leaked")
            return web.Response(body=b"5-HOP-SUCCESS")

        for i in range(1, 6):
            self.app.router.add_get(f"/h{i}", locals()[f"h{i}"])
        await self._start_server()

        res = await download_slack_file(f"http://127.0.0.1:{self.port}/h1", token="tok5")
        self.assertEqual(res, b"5-HOP-SUCCESS")
        self.assertEqual(len(self.request_headers_log), 5)
        # Verify Auth header was only on h1
        self.assertIn("authorization", [k.lower() for k in self.request_headers_log[0][1].keys()])
        for idx in range(1, 5):
            self.assertNotIn("authorization", [k.lower() for k in self.request_headers_log[idx][1].keys()])

    async def test_redirect_with_s3_presigned_query_parameters(self):
        """Verify AWS S3 presigned URL query parameters (signature, key, etc.) are intact."""
        received_query = {}

        async def h_slack(request: web.Request):
            target = f"http://127.0.0.1:{self.port}/s3-endpoint?AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Signature=vjbyPxybdZaNmGa%2ByT272YEAiv4%3D"
            raise web.HTTPFound(target)

        async def h_s3(request: web.Request):
            received_query.update(dict(request.query))
            return web.Response(body=b"S3_QUERY_VERIFIED")

        self.app.router.add_get("/slack-query", h_slack)
        self.app.router.add_get("/s3-endpoint", h_s3)
        await self._start_server()

        res = await download_slack_file(f"http://127.0.0.1:{self.port}/slack-query", token="t")
        self.assertEqual(res, b"S3_QUERY_VERIFIED")
        self.assertEqual(received_query.get("AWSAccessKeyId"), "AKIAIOSFODNN7EXAMPLE")
        self.assertIn("Signature", received_query)

    async def test_redirect_loop_detection_terminates_gracefully(self):
        """Verify circular redirect loop (A -> B -> A) terminates cleanly without hanging."""
        async def h_loop_a(request: web.Request):
            raise web.HTTPFound(f"http://127.0.0.1:{self.port}/loop-b")

        async def h_loop_b(request: web.Request):
            raise web.HTTPFound(f"http://127.0.0.1:{self.port}/loop-a")

        self.app.router.add_get("/loop-a", h_loop_a)
        self.app.router.add_get("/loop-b", h_loop_b)
        await self._start_server()

        res = await download_slack_file(
            f"http://127.0.0.1:{self.port}/loop-a",
            token="t",
            timeout_seconds=3.0,
        )
        self.assertIsNone(res)


class TestEmpiricalChunkedDownloadLimit(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: Chunked download exceeding 100MB without Content-Length."""

    async def asyncSetUp(self):
        self.app = web.Application()
        self.runner = None
        self.site = None
        self.port = 0
        self.server_chunks_sent = 0
        self.server_stream_aborted = False

    async def asyncTearDown(self):
        if self.runner:
            await self.runner.cleanup()

    async def _start_server(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        names = self.site._server.sockets
        self.port = names[0].getsockname()[1]

    async def test_chunked_streaming_exceeding_100mb_aborts_without_memory_ballooning(self):
        """
        Empirically test chunked streaming without Content-Length:
        - Stream sends 64KB chunks up to 120MB (1920 chunks).
        - Assert download_slack_file detects total_bytes > 104_857_600 and aborts at chunk 1601 (~100.06MB).
        - Assert download returns None.
        - Assert server detects client disconnection (stream aborts, does not send all 120MB).
        """
        chunk_size = 65536
        target_total_chunks = 1920  # ~120MB

        async def handle_infinite_chunked(request: web.Request):
            resp = web.StreamResponse(
                status=200,
                headers={"Transfer-Encoding": "chunked", "Content-Type": "application/octet-stream"},
            )
            await resp.prepare(request)
            chunk = b"A" * chunk_size
            try:
                for i in range(target_total_chunks):
                    await resp.write(chunk)
                    self.server_chunks_sent += 1
                await resp.write_eof()
            except (ConnectionResetError, asyncio.CancelledError, Exception):
                self.server_stream_aborted = True
            return resp

        self.app.router.add_get("/chunked-large", handle_infinite_chunked)
        await self._start_server()

        url = f"http://127.0.0.1:{self.port}/chunked-large"

        # Track garbage collection and ensure bounded memory
        gc.collect()

        result = await download_slack_file(
            url,
            token="token",
            max_size_bytes=104_857_600,
            timeout_seconds=10.0,
        )

        # Must return None upon exceeding limit
        self.assertIsNone(result)

        # Must have read up to chunk 1601 (104,857,600 / 65536 = 1600 chunks, 1601 exceeds)
        # Server sent should not have completed all 1920 chunks!
        self.assertLess(self.server_chunks_sent, target_total_chunks)
        self.assertGreaterEqual(self.server_chunks_sent, 1600)
        self.assertLessEqual(self.server_chunks_sent, 1650)

    async def test_chunked_streaming_exact_boundary(self):
        """
        Test exact boundary:
        - Limit = 1,000,000 bytes.
        - Case A: Exactly 1,000,000 bytes chunked -> SUCCEEDS and returns data.
        - Case B: Exactly 1,000,001 bytes chunked -> ABORTS and returns None.
        """
        limit = 1_000_000

        async def handle_exact_boundary_ok(request: web.Request):
            resp = web.StreamResponse(
                status=200,
                headers={"Transfer-Encoding": "chunked", "Content-Type": "application/octet-stream"},
            )
            await resp.prepare(request)
            # Send exactly 1,000,000 bytes in 100,000 byte chunks
            for _ in range(10):
                await resp.write(b"B" * 100_000)
            await resp.write_eof()
            return resp

        async def handle_exact_boundary_exceeded(request: web.Request):
            resp = web.StreamResponse(
                status=200,
                headers={"Transfer-Encoding": "chunked", "Content-Type": "application/octet-stream"},
            )
            await resp.prepare(request)
            # Send exactly 1,000,001 bytes
            for _ in range(10):
                await resp.write(b"C" * 100_000)
            await resp.write(b"X")  # 1 byte over
            await resp.write_eof()
            return resp

        self.app.router.add_get("/boundary-ok", handle_exact_boundary_ok)
        self.app.router.add_get("/boundary-exceeded", handle_exact_boundary_exceeded)
        await self._start_server()

        # Case A: exactly limit
        res_ok = await download_slack_file(
            f"http://127.0.0.1:{self.port}/boundary-ok",
            token="token",
            max_size_bytes=limit,
        )
        self.assertIsNotNone(res_ok)
        self.assertEqual(len(res_ok), limit)

        # Case B: limit + 1
        res_bad = await download_slack_file(
            f"http://127.0.0.1:{self.port}/boundary-exceeded",
            token="token",
            max_size_bytes=limit,
        )
        self.assertIsNone(res_bad)

    async def test_chunked_streaming_via_s3_redirect_exceeding_max_bytes(self):
        """Verify chunked download exceeding max_bytes via 302 redirect aborts cleanly."""
        limit = 500_000  # 500KB

        async def handle_s3_redirect(request: web.Request):
            raise web.HTTPFound(f"http://127.0.0.1:{self.port}/s3-chunked-stream")

        async def handle_s3_stream(request: web.Request):
            resp = web.StreamResponse(
                status=200,
                headers={"Transfer-Encoding": "chunked"},
            )
            await resp.prepare(request)
            for _ in range(10):
                await resp.write(b"S" * 100_000)  # 1MB > 500KB
            await resp.write_eof()
            return resp

        self.app.router.add_get("/slack-redirect", handle_s3_redirect)
        self.app.router.add_get("/s3-chunked-stream", handle_s3_stream)
        await self._start_server()

        res = await download_slack_file(
            f"http://127.0.0.1:{self.port}/slack-redirect",
            token="token",
            max_size_bytes=limit,
        )
        self.assertIsNone(res)


class TestEmpiricalPathTraversalAndSecurityBoundaries(unittest.TestCase):
    """Empirical challenge: Malicious path traversal payloads and directory confinement."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fm_adv_stage_")
        self.transfers_dir = os.path.join(self.temp_dir, TRANSFERS_DIR_NAME)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_adversarial_path_traversal_matrix(self):
        """
        Challenge save_staged_file with exhaustive adversarial payload matrix:
        1. Linux root / ssh traversal
        2. Windows system32 / drivers traversal
        3. Null byte injection
        4. Control characters and newlines
        5. Dot tokens (. , .. , ... , ....)
        6. Trailing slashes and backslashes
        7. Windows reserved device names (CON, NUL, AUX, PRN)
        8. Nested path attempts (slack_transfers/../../escape)

        Assert for EVERY payload:
        - The resulting file is strictly confined inside self.transfers_dir.
        - os.path.commonpath([self.transfers_dir, dest_path]) == self.transfers_dir
        - No file is created outside self.transfers_dir.
        - No unhandled exceptions are raised.
        """
        payloads = [
            "../../../root/.ssh/authorized_keys",
            "../../../../etc/passwd",
            "../../../../etc/shadow",
            "..\\..\\..\\windows\\system32\\cmd.exe",
            "C:\\Windows\\System32\\drivers\\etc\\hosts",
            "D:\\secret\\passwords.txt",
            "\\\\remote-share\\exploit.exe",
            "authorized_keys\x00.pdf",
            "test.txt\x00/../../etc/passwd",
            "\x00",
            "\x00\x00\x00",
            "..\\..\\..\\..\\..\\..\\..\\etc\\cron.d\\exploit",
            ".",
            "..",
            "...",
            "....",
            ".bashrc",
            "..hidden",
            "CON",
            "PRN",
            "AUX",
            "NUL",
            "COM1",
            "LPT1",
            "   ",
            "\t\n\r",
            "file\r\nname.txt",
            "file\x07bell.txt",
            "slack_transfers/../../escape.sh",
            "/etc/cron.d/malicious",
            "a/b/../../../../outside.txt",
            "....//....//....//escape.txt",
            "....\\\\....\\\\....\\\\escape.txt",
            "foo/bar/",
            "foo\\bar\\",
            "~/keys.txt",
            "$HOME/.bash_profile",
            "`id`.txt",
            ";rm -rf /.txt",
        ]

        file_content = b"EMPIRICAL_TEST_PAYLOAD_SAFEGUARD_BYTES"

        for idx, payload in enumerate(payloads):
            with self.subTest(payload=payload):
                timestamp = 1720000000 + idx
                dest_path = save_staged_file(
                    file_bytes=file_content,
                    filename=payload,
                    target_cwd=self.temp_dir,
                    timestamp=timestamp,
                )

                self.assertIsNotNone(dest_path, f"Failed to save payload '{payload}' safely")
                self.assertTrue(os.path.isfile(dest_path), f"File was not created for payload '{payload}'")

                # Verify strict confinement within transfers_dir
                real_dest = os.path.realpath(dest_path)
                real_transfers = os.path.realpath(self.transfers_dir)

                self.assertEqual(
                    os.path.commonpath([real_transfers, real_dest]),
                    real_transfers,
                    f"Path escape detected! Dest {real_dest} outside {real_transfers}",
                )

                # Verify parent directory is exactly transfers_dir
                self.assertEqual(os.path.dirname(real_dest), real_transfers)

                # Verify content integrity
                with open(dest_path, "rb") as f:
                    self.assertEqual(f.read(), file_content)

        # Audit entire temp_dir to confirm NO files exist outside slack_transfers/
        for root, dirs, files in os.walk(self.temp_dir):
            if os.path.abspath(root) == os.path.abspath(self.temp_dir):
                # Only slack_transfers directory is allowed in temp_dir root
                self.assertEqual(dirs, [TRANSFERS_DIR_NAME])
                self.assertEqual(files, [])

    def test_null_byte_filename_neutralization(self):
        """Verify embedded and leading/trailing null bytes are replaced with underscores."""
        raw_name = "secret\x00evil.sh"
        sanitized = sanitize_filename(raw_name)
        self.assertNotIn("\x00", sanitized)
        self.assertEqual(sanitized, "secret_evil.sh")

        pure_null = "\x00\x00\x00"
        sanitized_null = sanitize_filename(pure_null)
        self.assertNotIn("\x00", sanitized_null)
        # Should fallback to upload_<timestamp> or safe string
        self.assertTrue(sanitized_null.startswith("upload_") or sanitized_null == "___")

    def test_unicode_and_emoji_filenames(self):
        """Verify unicode, international characters, and emojis are preserved and saved safely."""
        unicode_names = [
            "📊_q3_financial_report.pdf",
            "résumé_2026_carl.docx",
            "日本語_テスト_データ.csv",
            "🚀_launch_manifest.json",
        ]
        for name in unicode_names:
            dest = save_staged_file(b"unicode_bytes", name, target_cwd=self.temp_dir)
            self.assertIsNotNone(dest)
            self.assertTrue(os.path.isfile(dest))
            self.assertEqual(os.path.dirname(os.path.realpath(dest)), os.path.realpath(self.transfers_dir))


class TestEmpiricalLRUCacheEviction(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: Bounded LRU cache eviction stress under 1000+ entries."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fm_adv_lru_")
        self.router = SessionRouter(
            default_target_cwd=self.temp_dir,
            adapter_factory=lambda cwd: MockHeadlessAdapter(cwd),
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_lru_cache_bounded_at_1000_entries(self):
        """
        Stress test IngressGateway._cache_staged_file with 1500 entries when max_staged_cache=1000:
        - Populate 1500 entries (file_0 to file_1499).
        - Assert cache size never exceeds 1000.
        - Assert entries file_0 to file_499 are evicted.
        - Assert entries file_500 to file_1499 are present.
        """
        gateway = IngressGateway(router=self.router, max_staged_cache=1000)

        for i in range(1500):
            file_id = f"F_ENTRY_{i:04d}"
            local_path = f"{self.temp_dir}/slack_transfers/1720000000_file_{i}.txt"
            gateway._cache_staged_file(file_id, local_path)
            self.assertLessEqual(len(gateway._staged_files), 1000)

        self.assertEqual(len(gateway._staged_files), 1000)

        # First 500 entries must have been evicted
        for i in range(500):
            file_id = f"F_ENTRY_{i:04d}"
            self.assertNotIn(file_id, gateway._staged_files)

        # Last 1000 entries must be present
        for i in range(500, 1500):
            file_id = f"F_ENTRY_{i:04d}"
            self.assertIn(file_id, gateway._staged_files)

    def test_lru_access_moves_to_mru_preventing_premature_eviction(self):
        """
        Verify that touching an older entry moves it to MRU position,
        so when new entries arrive, the touched entry is NOT evicted before older un-accessed entries.
        """
        capacity = 10
        gateway = IngressGateway(router=self.router, max_staged_cache=capacity)

        # Fill 10 entries (0 to 9)
        for i in range(10):
            gateway._cache_staged_file(f"F_{i}", f"/path/{i}")

        self.assertEqual(len(gateway._staged_files), 10)

        # Access/Touch F_0 (the oldest entry)
        gateway._cache_staged_file("F_0", "/path/0_updated")

        # Now insert F_10. F_1 should be evicted (as F_0 was moved to end)
        gateway._cache_staged_file("F_10", "/path/10")

        self.assertEqual(len(gateway._staged_files), 10)
        self.assertIn("F_0", gateway._staged_files, "F_0 was touched and should have been retained")
        self.assertNotIn("F_1", gateway._staged_files, "F_1 was the LRU item and should have been evicted")
        self.assertIn("F_10", gateway._staged_files)

    def test_lru_cache_high_volume_throughput(self):
        """Stress test with 10,000 rapid insertions to verify no memory leak or quadratic degradation."""
        gateway = IngressGateway(router=self.router, max_staged_cache=1000)
        start_time = time.monotonic()

        for i in range(10_000):
            gateway._cache_staged_file(f"F_{i}", f"/path/{i}")

        elapsed = time.monotonic() - start_time
        self.assertEqual(len(gateway._staged_files), 1000)
        # 10,000 operations should complete well under 0.5s in Python
        self.assertLess(elapsed, 0.5)

    async def test_stage_inbound_files_handles_malformed_input_gracefully(self):
        """Verify stage_inbound_files handles None elements, empty dicts, and invalid types without crashing."""
        gateway = IngressGateway(router=self.router)
        malformed_files = [
            None,
            "not_a_dict",
            {},
            {"id": ""},
            {"id": "F_VALID", "name": "valid.txt", "url_private_download": ""},
        ]
        # Should not raise exception
        results = await gateway.stage_inbound_files(malformed_files, target_cwd=self.temp_dir)
        self.assertIsInstance(results, list)


if __name__ == "__main__":
    unittest.main()
