#!/usr/bin/env python3
"""
tests/test_inbound_files.py
Unit tests for Milestone 2: Slack Inbound File Receipt & Staging:
1. Authenticated download with redirect decoupling (stripping Authorization header on S3 redirects).
2. Size guardrails (100MB limit via Content-Length and streaming chunk counts).
3. Collision-free local staging into {target_cwd}/slack_transfers/{timestamp}_{filename}.
4. Filename sanitization against path traversal and forbidden characters.
5. Inbound prompt enrichment with [Attached Slack File: ...] tags.
6. Fallback prompt synthesis for comment-less file uploads.
7. Dual delivery deduplication between file_shared and message.file_share events.
8. @app.event("file_shared") routing and empty-text ingress pass-through.
"""

import os
import sys
import time
import shutil
import tempfile
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app
from first_mate.gateway.ingress import (
    IngressGateway,
    download_slack_file,
    sanitize_filename,
    save_staged_file,
    DEFAULT_MAX_FILE_SIZE_BYTES,
    TRANSFERS_DIR_NAME,
)
from first_mate.gateway.router import SessionRouter


class MockHeadlessAdapter:
    """Mock adapter preventing tests from spawning real CLI processes."""

    def __init__(self, cwd: str = "/tmp") -> None:
        self.cwd = cwd
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class TestSlackFileDownload(unittest.IsolatedAsyncioTestCase):
    """Test suite for download_slack_file and S3 redirect decoupling."""

    async def test_download_slack_file_authenticated_success(self):
        """Verify direct 200 OK download sends Bearer token and returns file bytes."""
        expected_bytes = b"Hello from Slack file download!"
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.headers = {"Content-Length": str(len(expected_bytes))}
        mock_resp.read = AsyncMock(return_value=expected_bytes)

        # Content iterator for streaming
        async def iter_chunks(size):
            yield expected_bytes

        mock_content = MagicMock()
        mock_content.iter_chunked = iter_chunks
        mock_resp.content = mock_content

        mock_session_inst = MagicMock()
        mock_session_inst.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_session_inst.get.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_inst)
            mock_client_session.return_value.__aexit__ = AsyncMock(return_value=None)

            file_bytes = await download_slack_file(
                url="https://files.slack.com/files-pri/T123/download/doc.pdf",
                token="xoxb-test-token",
            )

            self.assertEqual(file_bytes, expected_bytes)
            # Verify Authorization header was supplied
            call_kwargs = mock_session_inst.get.call_args[1]
            self.assertEqual(call_kwargs.get("headers"), {"Authorization": "Bearer xoxb-test-token"})
            self.assertFalse(call_kwargs.get("allow_redirects", True))

    async def test_download_slack_file_redirect_decoupling_strips_auth(self):
        """
        Verify on 302 redirect (e.g. to Amazon S3), the subsequent GET request
        follows the Location URL WITHOUT the Authorization header.
        """
        file_content = b"S3 protected binary data"
        s3_url = "https://slack-files2.s3-us-west-2.amazonaws.com/doc.pdf?AWSAccessKeyId=AKIA..."

        # Initial Slack response (302 redirect)
        resp_slack = AsyncMock()
        resp_slack.status = 302
        resp_slack.headers = {"Location": s3_url}

        # Follow-up S3 response (200 OK)
        resp_s3 = AsyncMock()
        resp_s3.status = 200
        resp_s3.headers = {"Content-Length": str(len(file_content))}
        resp_s3.read = AsyncMock(return_value=file_content)

        async def s3_iter_chunks(size):
            yield file_content

        mock_s3_content = MagicMock()
        mock_s3_content.iter_chunked = s3_iter_chunks
        resp_s3.content = mock_s3_content

        session_inst = MagicMock()
        # Return resp_slack for first call, resp_s3 for second call
        get_calls = []

        def get_side_effect(url, **kwargs):
            get_calls.append((url, kwargs))
            context = MagicMock()
            if len(get_calls) == 1:
                context.__aenter__ = AsyncMock(return_value=resp_slack)
            else:
                context.__aenter__ = AsyncMock(return_value=resp_s3)
            context.__aexit__ = AsyncMock(return_value=None)
            return context

        session_inst.get.side_effect = get_side_effect

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value.__aenter__ = AsyncMock(return_value=session_inst)
            mock_client_session.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await download_slack_file(
                url="https://files.slack.com/files-pri/T123/download/doc.pdf",
                token="xoxb-secret-token",
            )

            self.assertEqual(result, file_content)
            self.assertEqual(len(get_calls), 2)

            # First call to Slack MUST include Authorization header and allow_redirects=False
            first_url, first_kwargs = get_calls[0]
            self.assertIn("files.slack.com", first_url)
            self.assertEqual(first_kwargs.get("headers"), {"Authorization": "Bearer xoxb-secret-token"})
            self.assertFalse(first_kwargs.get("allow_redirects", True))

            # Second call to S3 MUST NOT include Authorization header
            second_url, second_kwargs = get_calls[1]
            self.assertEqual(second_url, s3_url)
            self.assertNotIn("headers", second_kwargs)
            self.assertTrue(second_kwargs.get("allow_redirects", False))

    async def test_download_slack_file_exceeds_max_size_header(self):
        """Verify download is aborted if Content-Length exceeds max_size_bytes."""
        oversized = 110 * 1024 * 1024  # 110MB > 100MB
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.headers = {"Content-Length": str(oversized)}

        session_inst = MagicMock()
        session_inst.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)
        session_inst.get.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value.__aenter__ = AsyncMock(return_value=session_inst)
            mock_client_session.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await download_slack_file("https://files.slack.com/huge.iso", "token")
            self.assertIsNone(result)

    async def test_download_slack_file_exceeds_max_size_stream(self):
        """Verify download is aborted if streaming bytes exceed max_size_bytes."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.headers = {}  # No Content-Length provided

        async def stream_chunks(size):
            # Yield 2MB chunks up to 10MB
            for _ in range(5):
                yield b"X" * (2 * 1024 * 1024)

        mock_content = MagicMock()
        mock_content.iter_chunked = stream_chunks
        mock_resp.content = mock_content

        session_inst = MagicMock()
        session_inst.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)
        session_inst.get.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value.__aenter__ = AsyncMock(return_value=session_inst)
            mock_client_session.return_value.__aexit__ = AsyncMock(return_value=None)

            # Limit set to 3MB
            result = await download_slack_file(
                "https://files.slack.com/big.bin", "token", max_size_bytes=3 * 1024 * 1024
            )
            self.assertIsNone(result)

    async def test_download_slack_file_network_error_returns_none(self):
        """Verify network errors gracefully log and return None without unhandled exceptions."""
        session_inst = MagicMock()
        session_inst.get.side_effect = Exception("Connection reset by peer")

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value.__aenter__ = AsyncMock(return_value=session_inst)
            mock_client_session.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await download_slack_file("https://files.slack.com/error.txt", "token")
            self.assertIsNone(result)

    async def test_download_slack_file_handles_308_permanent_redirect(self):
        """Verify HTTP 308 Permanent Redirect strips auth and fetches target without Authorization."""
        mock_init_resp = AsyncMock()
        mock_init_resp.status = 308
        mock_init_resp.headers = {"Location": "https://s3.amazonaws.com/bucket/files/redirect_308.txt"}

        mock_s3_resp = AsyncMock()
        mock_s3_resp.status = 200
        mock_s3_resp.headers = {"Content-Length": "16"}
        mock_s3_resp.read = AsyncMock(return_value=b"CONTENT_FROM_308")

        async def s3_iter_chunks(size):
            yield b"CONTENT_FROM_308"

        mock_s3_content = MagicMock()
        mock_s3_content.iter_chunked = s3_iter_chunks
        mock_s3_resp.content = mock_s3_content

        session_inst = MagicMock()

        class MockGetContext:
            def __init__(self, url, **kwargs):
                self.url = url
                self.kwargs = kwargs

            async def __aenter__(self):
                if "files.slack.com" in self.url:
                    return mock_init_resp
                else:
                    return mock_s3_resp

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        session_inst.get = MagicMock(side_effect=lambda url, **kwargs: MockGetContext(url, **kwargs))

        with patch("aiohttp.ClientSession") as mock_client_session:
            mock_client_session.return_value.__aenter__ = AsyncMock(return_value=session_inst)
            mock_client_session.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await download_slack_file(
                "https://files.slack.com/files-pri/T01/redirect_308.txt", "xoxb-fake-token"
            )

            self.assertEqual(result, b"CONTENT_FROM_308")
            self.assertEqual(session_inst.get.call_count, 2)
            first_call_args = session_inst.get.call_args_list[0]
            second_call_args = session_inst.get.call_args_list[1]
            self.assertEqual(first_call_args[1].get("headers"), {"Authorization": "Bearer xoxb-fake-token"})
            self.assertNotIn("headers", second_call_args[1])


class TestFileSanitizationAndStaging(unittest.TestCase):
    """Test suite for filename sanitization and disk staging."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fm_test_stage_")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_sanitize_filename_path_traversal(self):
        """Verify path traversal sequences are safely stripped."""
        self.assertEqual(sanitize_filename("../../etc/passwd"), "passwd")
        self.assertEqual(sanitize_filename("..\\..\\windows\\system32\\cmd.exe"), "cmd.exe")
        self.assertEqual(sanitize_filename("sub/dir/notes.txt"), "notes.txt")

    def test_sanitize_filename_forbidden_chars(self):
        """Verify forbidden characters are replaced with underscores."""
        raw = 'report:2026*final?"test"|<>.pdf'
        sanitized = sanitize_filename(raw)
        self.assertEqual(sanitized, "report_2026_final__test____.pdf")

    def test_sanitize_filename_fallback_on_empty(self):
        """Verify empty strings or dot-only filenames produce safe upload_<timestamp>."""
        s1 = sanitize_filename("")
        self.assertTrue(s1.startswith("upload_"))
        s2 = sanitize_filename(".")
        self.assertTrue(s2.startswith("upload_"))
        s3 = sanitize_filename("..")
        self.assertTrue(s3.startswith("upload_"))
        s4 = sanitize_filename("   ")
        self.assertTrue(s4.startswith("upload_"))

    def test_save_staged_file_creates_directory_and_writes_content(self):
        """Verify save_staged_file creates slack_transfers/ and saves timestamped file."""
        data = b"def test_app(): pass\n"
        timestamp = 1728000000
        saved_path = save_staged_file(
            file_bytes=data,
            filename="test_solution.py",
            target_cwd=self.temp_dir,
            timestamp=timestamp,
        )

        self.assertIsNotNone(saved_path)
        expected_dir = os.path.join(self.temp_dir, TRANSFERS_DIR_NAME)
        self.assertTrue(os.path.isdir(expected_dir))
        expected_path = os.path.join(expected_dir, f"{timestamp}_test_solution.py")
        self.assertEqual(saved_path, expected_path)
        self.assertTrue(os.path.isfile(saved_path))
        with open(saved_path, "rb") as f:
            self.assertEqual(f.read(), data)

    def test_save_staged_file_collision_resistance(self):
        """Verify saving two files with identical names produces distinct non-colliding paths."""
        data1 = b"Version 1"
        data2 = b"Version 2"

        path1 = save_staged_file(data1, "duplicate.txt", target_cwd=self.temp_dir)
        path2 = save_staged_file(data2, "duplicate.txt", target_cwd=self.temp_dir)

        self.assertIsNotNone(path1)
        self.assertIsNotNone(path2)
        self.assertNotEqual(path1, path2, "Saved paths must not collide for identical filenames!")
        self.assertTrue(os.path.isfile(path1))
        self.assertTrue(os.path.isfile(path2))

        with open(path1, "rb") as f1, open(path2, "rb") as f2:
            self.assertEqual(f1.read(), data1)
            self.assertEqual(f2.read(), data2)


class TestInboundFileIngressAndPromptEnrichment(unittest.IsolatedAsyncioTestCase):
    """Test suite for IngressGateway handling inbound file attachments and prompt synthesis."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fm_test_ingress_")
        self.router = SessionRouter(
            default_target_cwd=self.temp_dir,
            adapter_factory=lambda cwd: MockHeadlessAdapter(cwd),
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = AsyncMock()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_comment_less_upload_synthesizes_prompt_and_attachment_tag(self):
        """
        Verify that a file upload with empty text:
        1. Is NOT dropped by the ingress filter.
        2. Is downloaded and staged into slack_transfers/.
        3. Delivers synthesized fallback prompt and attachment tag to session.send_user_message.
        """
        file_data = b"Quarterly Financial Analysis"
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_TEST_FILES",
            "ts": "1720000001.000100",
            "text": "",
            "files": [
                {
                    "id": "F_FIN_101",
                    "name": "financials.csv",
                    "url_private_download": "https://files.slack.com/fin.csv",
                    "size": len(file_data),
                }
            ],
            "cwd": self.temp_dir,
        }

        # Mock download_slack_file
        with patch("first_mate.gateway.ingress.download_slack_file", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = file_data

            # Track what prompt is passed to session.send_user_message
            sent_prompts = []

            async def mock_send(prompt):
                sent_prompts.append(prompt)
                yield {"event_type": "text_delta", "text_delta": "Understood. Inspecting financials.csv."}
                yield {"event_type": "finish", "response": "Understood. Inspecting financials.csv."}

            # Pre-create session and patch send_user_message
            session, _ = await self.router.get_or_create("C_TEST_FILES", "1720000001.000100", target_cwd=self.temp_dir)
            session.send_user_message = mock_send

            await self.gateway.handle_message(event, self.client)

            self.assertEqual(len(sent_prompts), 1)
            prompt = sent_prompts[0]

            # Verify prompt contains attachment tag
            self.assertIn("[Attached Slack File: financials.csv saved to ", prompt)
            self.assertIn("slack_transfers", prompt)
            self.assertIn("financials.csv]", prompt)

            # Verify fallback prompt synthesis
            self.assertIn(
                "I've uploaded financials.csv to the workspace. Please inspect or process this file.",
                prompt,
            )

            # Verify file was physically saved to disk
            staged_path = self.gateway._staged_files.get("F_FIN_101")
            self.assertIsNotNone(staged_path)
            self.assertTrue(os.path.isfile(staged_path))

    async def test_file_upload_with_accompanying_comment(self):
        """Verify that when a user provides text with an attached file, the user text is preserved."""
        file_data = b"{\"database\": \"postgres\"}"
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_TEST_FILES",
            "ts": "1720000002.000100",
            "text": "Here is the production config. Please validate it.",
            "files": [
                {
                    "id": "F_CFG_202",
                    "name": "config.json",
                    "url_private_download": "https://files.slack.com/cfg.json",
                    "size": len(file_data),
                }
            ],
            "cwd": self.temp_dir,
        }

        with patch("first_mate.gateway.ingress.download_slack_file", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = file_data
            sent_prompts = []

            async def mock_send(prompt):
                sent_prompts.append(prompt)
                yield {"event_type": "finish", "response": "Validated."}

            session, _ = await self.router.get_or_create("C_TEST_FILES", "1720000002.000100", target_cwd=self.temp_dir)
            session.send_user_message = mock_send

            await self.gateway.handle_message(event, self.client)

            self.assertEqual(len(sent_prompts), 1)
            prompt = sent_prompts[0]

            self.assertIn("[Attached Slack File: config.json saved to ", prompt)
            self.assertIn("Here is the production config. Please validate it.", prompt)
            # Should NOT synthesize default prompt when user already provided text
            self.assertNotIn("I've uploaded config.json to the workspace", prompt)

    async def test_multiple_files_upload(self):
        """Verify multiple attached files produce multiple attachment tags and plural fallback prompt."""
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_TEST_FILES",
            "ts": "1720000003.000100",
            "text": "",
            "files": [
                {"id": "F1", "name": "data1.csv", "url_private_download": "http://slack/1"},
                {"id": "F2", "name": "data2.csv", "url_private_download": "http://slack/2"},
            ],
            "cwd": self.temp_dir,
        }

        with patch("first_mate.gateway.ingress.download_slack_file", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = b"sample_data"
            sent_prompts = []

            async def mock_send(prompt):
                sent_prompts.append(prompt)
                yield {"event_type": "finish", "response": "Done."}

            session, _ = await self.router.get_or_create("C_TEST_FILES", "1720000003.000100", target_cwd=self.temp_dir)
            session.send_user_message = mock_send

            await self.gateway.handle_message(event, self.client)

            self.assertEqual(len(sent_prompts), 1)
            prompt = sent_prompts[0]

            self.assertIn("[Attached Slack File: data1.csv saved to ", prompt)
            self.assertIn("[Attached Slack File: data2.csv saved to ", prompt)
            self.assertIn("I've uploaded data1.csv, data2.csv to the workspace.", prompt)

    async def test_inbound_file_exceeding_max_size_skipped(self):
        """Verify inbound file declared over 100MB is rejected without download."""
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_TEST_FILES",
            "ts": "1720000004.000100",
            "text": "Inspect this disk image",
            "files": [
                {
                    "id": "F_HUGE",
                    "name": "disk.iso",
                    "url_private_download": "http://slack/huge",
                    "size": 150_000_000,  # 150MB
                }
            ],
            "cwd": self.temp_dir,
        }

        with patch("first_mate.gateway.ingress.download_slack_file", new_callable=AsyncMock) as mock_download:
            sent_prompts = []

            async def mock_send(prompt):
                sent_prompts.append(prompt)
                yield {"event_type": "finish", "response": "Acknowledged."}

            session, _ = await self.router.get_or_create("C_TEST_FILES", "1720000004.000100", target_cwd=self.temp_dir)
            session.send_user_message = mock_send

            await self.gateway.handle_message(event, self.client)

            # download_slack_file should NOT have been called
            mock_download.assert_not_called()
            self.assertEqual(len(sent_prompts), 1)
            prompt = sent_prompts[0]
            self.assertIn("[Attached Slack File: disk.iso exceeded size limit of 100MB; skipped]", prompt)


class TestDualDeliveryDeduplication(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying dual delivery (file_shared + message.file_share) deduplication."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fm_test_dedup_")
        self.router = SessionRouter(
            default_target_cwd=self.temp_dir,
            adapter_factory=lambda cwd: MockHeadlessAdapter(cwd),
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = AsyncMock()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_file_shared_then_message_file_share_downloads_once(self):
        """
        Verify that when Slack sends file_shared first, followed by message.file_share,
        the file is downloaded exactly once, and the local staging path is reused.
        """
        file_id = "F_SHARED_EARLY"
        file_bytes = b"Standalone file shared content"

        # Mock files_info API
        self.client.files_info.return_value = {
            "ok": True,
            "file": {
                "id": file_id,
                "name": "specs.pdf",
                "size": len(file_bytes),
                "url_private_download": "https://files.slack.com/specs.pdf",
                "shares": {
                    "public": {
                        "C_DEDUP": [{"thread_ts": "1720000010.000100"}]
                    }
                }
            }
        }

        # 1. First event: file_shared
        file_shared_event = {
            "type": "file_shared",
            "file_id": file_id,
            "channel_id": "C_DEDUP",
            "event_ts": "1720000010.000100",
        }

        # 2. Second event: message with file_share
        message_event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_DEDUP",
            "ts": "1720000010.000100",
            "text": "",
            "files": [
                {
                    "id": file_id,
                    "name": "specs.pdf",
                    "url_private_download": "https://files.slack.com/specs.pdf",
                    "size": len(file_bytes),
                }
            ],
            "cwd": self.temp_dir,
        }

        with patch("first_mate.gateway.ingress.download_slack_file", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = file_bytes
            sent_prompts = []

            async def mock_send(prompt):
                sent_prompts.append(prompt)
                yield {"event_type": "finish", "response": "Examining specs."}

            session, _ = await self.router.get_or_create("C_DEDUP", "1720000010.000100", target_cwd=self.temp_dir)
            session.send_user_message = mock_send

            # Step 1: Process file_shared event
            await self.gateway.handle_file_shared(file_shared_event, self.client)
            self.assertEqual(mock_download.call_count, 1)
            self.assertIn(file_id, self.gateway._staged_files)
            initial_path = self.gateway._staged_files[file_id]

            # Step 2: Process subsequent message event
            await self.gateway.handle_message(message_event, self.client)

            # download_slack_file MUST NOT have been called a second time
            self.assertEqual(mock_download.call_count, 1)
            self.assertEqual(self.gateway._staged_files[file_id], initial_path)

            # The session must have received the cached file path in prompt
            self.assertEqual(len(sent_prompts), 1)
            self.assertIn(initial_path, sent_prompts[0])

    async def test_message_file_share_then_file_shared_downloads_once(self):
        """Verify reverse order: message event arrives first, then file_shared arrives."""
        file_id = "F_MSG_FIRST"
        file_bytes = b"Message first content"

        self.client.files_info.return_value = {
            "ok": True,
            "file": {
                "id": file_id,
                "name": "readme.md",
                "url_private_download": "https://files.slack.com/readme.md",
            }
        }

        message_event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_REV_DEDUP",
            "ts": "1720000020.000100",
            "text": "Review this readme",
            "files": [
                {
                    "id": file_id,
                    "name": "readme.md",
                    "url_private_download": "https://files.slack.com/readme.md",
                    "size": len(file_bytes),
                }
            ],
            "cwd": self.temp_dir,
        }

        file_shared_event = {
            "type": "file_shared",
            "file_id": file_id,
            "channel_id": "C_REV_DEDUP",
            "event_ts": "1720000020.000100",
        }

        with patch("first_mate.gateway.ingress.download_slack_file", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = file_bytes

            async def mock_send(prompt):
                yield {"event_type": "finish", "response": "Looks good."}

            session, _ = await self.router.get_or_create("C_REV_DEDUP", "1720000020.000100", target_cwd=self.temp_dir)
            session.send_user_message = mock_send

            # Step 1: Process message event
            await self.gateway.handle_message(message_event, self.client)
            self.assertEqual(mock_download.call_count, 1)

            # Step 2: Process subsequent file_shared event
            await self.gateway.handle_file_shared(file_shared_event, self.client)
            # Must remain 1
            self.assertEqual(mock_download.call_count, 1)

    async def test_concurrent_dual_delivery_downloads_only_once(self):
        """Verify concurrent arrival of file_shared and message.file_share results in exactly 1 download."""
        file_id = "F_CONCURRENT_UNIT_01"
        file_bytes = b"Concurrent dual delivery payload"

        self.client.files_info.return_value = {
            "ok": True,
            "file": {
                "id": file_id,
                "name": "concurrent_unit.txt",
                "size": len(file_bytes),
                "url_private_download": "https://files.slack.com/concurrent_unit.txt",
                "shares": {"public": {"C_CONC": [{"thread_ts": "1720000030.000100"}]}},
            },
        }

        file_shared_event = {
            "type": "file_shared",
            "file_id": file_id,
            "channel_id": "C_CONC",
            "event_ts": "1720000030.000100",
        }

        message_event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_CONC",
            "ts": "1720000030.000100",
            "text": "Concurrent test message",
            "user": "U_USER_CONC",
            "files": [
                {
                    "id": file_id,
                    "name": "concurrent_unit.txt",
                    "url_private_download": "https://files.slack.com/concurrent_unit.txt",
                    "size": len(file_bytes),
                }
            ],
            "cwd": self.temp_dir,
        }

        download_calls = 0

        async def slow_download(url, token, **kwargs):
            nonlocal download_calls
            download_calls += 1
            await asyncio.sleep(0.02)
            return file_bytes

        session, _ = await self.router.get_or_create("C_CONC", "1720000030.000100", target_cwd=self.temp_dir)

        async def mock_send(prompt):
            yield {"event_type": "finish", "response": "Processed."}

        session.send_user_message = mock_send

        with patch("first_mate.gateway.ingress.download_slack_file", side_effect=slow_download):
            await asyncio.gather(
                self.gateway.handle_file_shared(file_shared_event, self.client),
                self.gateway.handle_message(message_event, self.client),
            )

        self.assertEqual(download_calls, 1)


class TestAppEventRouting(unittest.IsolatedAsyncioTestCase):
    """Test suite for app.py file_shared registration and ingress pass-through."""

    def setUp(self):
        self._orig_bot_user_id = app.BOT_USER_ID

    def tearDown(self):
        app.BOT_USER_ID = self._orig_bot_user_id

    async def test_app_event_file_shared_routes_to_ingress_gateway(self):
        """Verify @app.event('file_shared') triggers ingress_gateway.handle_file_shared."""
        client = AsyncMock()
        event = {
            "type": "file_shared",
            "file_id": "F_ROUTE_TEST",
            "channel_id": "C_TEST",
            "event_ts": "1720000030.000100",
        }

        with patch.object(app.ingress_gateway, "handle_file_shared", new_callable=AsyncMock) as mock_hfs:
            await app.handle_file_shared_events(event, client)
            mock_hfs.assert_called_once_with(event, client)

    async def test_should_process_ingress_event_allows_empty_text_when_files_present(self):
        """Verify should_process_ingress_event returns True when clean_text is empty but files are attached."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_BOT_123"})

        # In a 1:1 Direct Message
        dm_event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "D_TEST_DM",
            "channel_type": "im",
            "user": "U_USER_456",
            "text": "",
            "files": [{"id": "F_DM_1", "name": "sample.py"}],
            "ts": "1720000040.000100",
        }

        result = await app.should_process_ingress_event(dm_event, client)
        self.assertTrue(result)

    async def test_should_process_ingress_event_allows_subtype_file_share_with_empty_text(self):
        """Verify should_process_ingress_event returns True for subtype file_share even if text is empty."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_BOT_123"})

        dm_event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "D_TEST_DM",
            "channel_type": "im",
            "user": "U_USER_456",
            "text": "",
            "ts": "1720000041.000100",
        }

        result = await app.should_process_ingress_event(dm_event, client)
        self.assertTrue(result)

    async def test_should_process_ingress_event_drops_empty_text_without_files(self):
        """Verify should_process_ingress_event continues to return False when clean_text is empty and NO files exist."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_BOT_123"})

        empty_event = {
            "type": "message",
            "channel": "D_TEST_DM",
            "channel_type": "im",
            "user": "U_USER_456",
            "text": "",
            "ts": "1720000042.000100",
        }

        result = await app.should_process_ingress_event(empty_event, client)
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
