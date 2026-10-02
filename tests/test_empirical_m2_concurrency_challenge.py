#!/usr/bin/env python3
"""
tests/test_empirical_m2_concurrency_challenge.py
Adversarial empirical challenge suite for Milestone 2:
1. Concurrency & Race Conditions:
   - Simultaneous concurrent arrival of file_shared and message.file_share for the same file using asyncio.gather.
   - Assert download occurs exactly once and zero race condition crashes occur.
2. Bulk Upload Stress:
   - Bulk upload of 10 files in a single message. Verify all 10 are staged, attachment tags generated, and prompt correctly formatted.
   - Mixed bulk upload (valid, oversized, failed).
3. Comment-less and Mention-only Uploads:
   - Whitespace-only, bot-mention-only (<@U12345>   ), and arbitrary user mentions. Verify fallback prompt synthesis.
4. Ingress Filtering:
   - Ensure bot messages with files are safely blocked, but human user file uploads are never dropped.
"""

import os
import sys
import shutil
import tempfile
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

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

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class TestConcurrentDualDeliveryChallenge(unittest.IsolatedAsyncioTestCase):
    """
    Challenge 1: Concurrency and Race Conditions
    Tests whether simultaneous concurrent arrival of file_shared and message.file_share
    for the exact same file results in exactly one download.
    """

    async def asyncSetUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fm_test_concurrency_")
        self.router = SessionRouter(
            default_target_cwd=self.temp_dir,
            adapter_factory=lambda cwd: MockHeadlessAdapter(cwd),
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = AsyncMock()

    async def asyncTearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_simultaneous_file_shared_and_message_downloads_once(self):
        """
        EMPIRICAL CHALLENGE:
        Slack sends both file_shared and message (subtype=file_share) concurrently.
        When both arrive simultaneously via asyncio.gather:
        1. Both coroutines must complete without unhandled exceptions.
        2. download_slack_file must be invoked EXACTLY ONCE for the file.
        3. Staged file path must be consistent and preserved.
        """
        file_id = "F_RACE_DUAL_001"
        file_bytes = b"Payload for dual delivery race test"

        self.client.files_info.return_value = {
            "ok": True,
            "file": {
                "id": file_id,
                "name": "concurrent_test.csv",
                "size": len(file_bytes),
                "url_private_download": "https://files.slack.com/concurrent_test.csv",
                "shares": {
                    "public": {
                        "C_RACE": [{"thread_ts": "1729000000.000100"}]
                    }
                }
            }
        }

        file_shared_event = {
            "type": "file_shared",
            "file_id": file_id,
            "channel_id": "C_RACE",
            "event_ts": "1729000000.000100",
        }

        message_event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_RACE",
            "ts": "1729000000.000100",
            "text": "Simultaneous arrival",
            "user": "U_CARL",
            "files": [
                {
                    "id": file_id,
                    "name": "concurrent_test.csv",
                    "url_private_download": "https://files.slack.com/concurrent_test.csv",
                    "size": len(file_bytes),
                }
            ],
            "cwd": self.temp_dir,
        }

        download_count = 0

        async def simulated_slow_download(url, token, **kwargs):
            nonlocal download_count
            download_count += 1
            # Simulate real network latency (50ms) to expose race window
            await asyncio.sleep(0.05)
            return file_bytes

        sent_prompts = []

        async def mock_send(prompt):
            sent_prompts.append(prompt)
            yield {"event_type": "finish", "response": "Acknowledged."}

        session, _ = await self.router.get_or_create(
            "C_RACE", "1729000000.000100", target_cwd=self.temp_dir
        )
        session.send_user_message = mock_send

        with patch("first_mate.gateway.ingress.download_slack_file", side_effect=simulated_slow_download):
            # Concurrent arrival of both events
            await asyncio.gather(
                self.gateway.handle_file_shared(file_shared_event, self.client),
                self.gateway.handle_message(message_event, self.client),
            )

        # ASSERTION: The file must be downloaded EXACTLY ONCE
        self.assertEqual(
            download_count,
            1,
            f"RACE CONDITION DETECTED: download_slack_file was invoked {download_count} times instead of 1 under concurrent arrival!"
        )

        # Prompt must contain the staged file attachment tag
        self.assertEqual(len(sent_prompts), 1)
        self.assertIn("concurrent_test.csv", sent_prompts[0])
        self.assertIn(TRANSFERS_DIR_NAME, sent_prompts[0])


class TestBulkUploadChallenge(unittest.IsolatedAsyncioTestCase):
    """
    Challenge 2: Bulk Upload Stress
    Tests bulk upload of 10 files in a single message and mixed status handling.
    """

    async def asyncSetUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fm_test_bulk_")
        self.router = SessionRouter(
            default_target_cwd=self.temp_dir,
            adapter_factory=lambda cwd: MockHeadlessAdapter(cwd),
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = AsyncMock()

    async def asyncTearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_bulk_upload_10_files_all_staged_and_prompt_formatted(self):
        """
        Verify bulk upload of 10 files in a single message:
        - All 10 files are staged to disk.
        - 10 distinct attachment tags are generated.
        - Fallback prompt names all 10 files.
        """
        num_files = 10
        files = [
            {
                "id": f"F_BULK_{i:02d}",
                "name": f"dataset_{i:02d}.parquet",
                "size": 1024 * (i + 1),
                "url_private_download": f"https://files.slack.com/dataset_{i:02d}.parquet",
            }
            for i in range(num_files)
        ]

        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_BULK_CHAN",
            "ts": "1729000010.000100",
            "text": "",
            "user": "U_CARL",
            "files": files,
            "cwd": self.temp_dir,
        }

        async def mock_dl(url, token, **kwargs):
            return b"PARQUET_MOCK_DATA"

        sent_prompts = []

        async def mock_send(prompt):
            sent_prompts.append(prompt)
            yield {"event_type": "finish", "response": "Datasets loaded."}

        session, _ = await self.router.get_or_create(
            "C_BULK_CHAN", "1729000010.000100", target_cwd=self.temp_dir
        )
        session.send_user_message = mock_send

        with patch("first_mate.gateway.ingress.download_slack_file", side_effect=mock_dl):
            await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(sent_prompts), 1)
        prompt = sent_prompts[0]

        # Verify 10 attachment tags
        tag_count = prompt.count("[Attached Slack File: dataset_")
        self.assertEqual(tag_count, 10, f"Expected 10 attachment tags, got {tag_count}")

        # Verify all 10 files are listed in fallback prompt
        for i in range(num_files):
            self.assertIn(f"dataset_{i:02d}.parquet", prompt)

        # Verify all 10 files physically exist on disk
        for i in range(num_files):
            staged_path = self.gateway._staged_files.get(f"F_BULK_{i:02d}")
            self.assertIsNotNone(staged_path)
            self.assertTrue(os.path.isfile(staged_path))

    async def test_bulk_upload_mixed_status_handling(self):
        """
        Verify bulk upload with mixed status:
        - 2 valid files (staged)
        - 1 oversized file >100MB (exceeded_size)
        - 1 missing download URL (failed)
        Prompt should cleanly report status of each file without crashing.
        """
        files = [
            {"id": "F_OK_1", "name": "ok1.txt", "size": 100, "url_private_download": "http://slack/1"},
            {"id": "F_OK_2", "name": "ok2.txt", "size": 200, "url_private_download": "http://slack/2"},
            {"id": "F_BIG", "name": "big.iso", "size": 200_000_000, "url_private_download": "http://slack/big"},
            {"id": "F_NO_URL", "name": "nourl.dat", "size": 50},  # No URL and no client files_info mock
        ]

        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_MIXED",
            "ts": "1729000020.000100",
            "text": "Inspect these 4 files",
            "user": "U_CARL",
            "files": files,
            "cwd": self.temp_dir,
        }

        self.client.files_info.return_value = {"ok": False}

        async def mock_dl(url, token, **kwargs):
            return b"OK_BYTES"

        sent_prompts = []

        async def mock_send(prompt):
            sent_prompts.append(prompt)
            yield {"event_type": "finish", "response": "Done."}

        session, _ = await self.router.get_or_create(
            "C_MIXED", "1729000020.000100", target_cwd=self.temp_dir
        )
        session.send_user_message = mock_send

        with patch("first_mate.gateway.ingress.download_slack_file", side_effect=mock_dl):
            await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(sent_prompts), 1)
        prompt = sent_prompts[0]

        # Verify status messages in prompt
        self.assertIn("[Attached Slack File: ok1.txt saved to ", prompt)
        self.assertIn("[Attached Slack File: ok2.txt saved to ", prompt)
        self.assertIn("[Attached Slack File: big.iso exceeded size limit of 100MB; skipped]", prompt)
        self.assertIn("[Attached Slack File: nourl.dat download failed]", prompt)
        self.assertIn("Inspect these 4 files", prompt)


class TestCommentlessAndMentionUploadChallenge(unittest.IsolatedAsyncioTestCase):
    """
    Challenge 3: Comment-less, Whitespace-only, and Mention-only Uploads
    Tests fallback prompt synthesis across various empty-comment permutations.
    """

    async def asyncSetUp(self):
        self._orig_bot_user_id = app.BOT_USER_ID
        app.BOT_USER_ID = "U_AGY_BOT"
        self.temp_dir = tempfile.mkdtemp(prefix="fm_test_commentless_")
        self.router = SessionRouter(
            default_target_cwd=self.temp_dir,
            adapter_factory=lambda cwd: MockHeadlessAdapter(cwd),
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = AsyncMock()
        self.client.auth_test = AsyncMock(return_value={"user_id": "U_AGY_BOT"})

    async def asyncTearDown(self):
        app.BOT_USER_ID = self._orig_bot_user_id
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_whitespace_only_in_dm_synthesizes_fallback_prompt(self):
        """Verify upload with whitespace-only text in DM triggers clean fallback prompt."""
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "D_TEST_DM",
            "channel_type": "im",
            "ts": "1729000030.000100",
            "text": "   \n\t  \r\n   ",
            "user": "U_CARL",
            "files": [
                {
                    "id": "F_WS",
                    "name": "whitespace_test.py",
                    "url_private_download": "http://slack/ws",
                    "size": 50,
                }
            ],
            "cwd": self.temp_dir,
        }

        # 1. Ingress filter must allow it
        allowed = await app.should_process_ingress_event(event, self.client)
        self.assertTrue(allowed, "Ingress filter incorrectly dropped DM whitespace-only file upload!")

        # 2. IngressGateway synthesizes fallback prompt
        sent_prompts = []

        async def mock_send(prompt):
            sent_prompts.append(prompt)
            yield {"event_type": "finish", "response": "ok"}

        session, _ = await self.router.get_or_create(
            "D_TEST_DM", "1729000030.000100", target_cwd=self.temp_dir
        )
        session.send_user_message = mock_send

        with patch("first_mate.gateway.ingress.download_slack_file", return_value=b"data"):
            await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(sent_prompts), 1)
        prompt = sent_prompts[0]
        self.assertIn("I've uploaded whitespace_test.py to the workspace. Please inspect or process this file.", prompt)
        self.assertIn("[Attached Slack File: whitespace_test.py saved to ", prompt)

    async def test_bot_mention_only_upload_in_channel_synthesizes_fallback_prompt(self):
        """Verify upload with only <@U_AGY_BOT> in public channel triggers clean fallback prompt."""
        event = {
            "type": "app_mention",
            "subtype": "file_share",
            "channel": "C_PUBLIC",
            "ts": "1729000031.000100",
            "text": "<@U_AGY_BOT>   ",
            "user": "U_CARL",
            "files": [
                {
                    "id": "F_MENTION",
                    "name": "analysis.xlsx",
                    "url_private_download": "http://slack/xlsx",
                    "size": 100,
                }
            ],
            "cwd": self.temp_dir,
        }

        allowed = await app.should_process_ingress_event(event, self.client)
        self.assertTrue(allowed, "Ingress filter dropped public channel file upload with bot mention!")

        sent_prompts = []

        async def mock_send(prompt):
            sent_prompts.append(prompt)
            yield {"event_type": "finish", "response": "ok"}

        session, _ = await self.router.get_or_create(
            "C_PUBLIC", "1729000031.000100", target_cwd=self.temp_dir
        )
        session.send_user_message = mock_send

        with patch("first_mate.gateway.ingress.download_slack_file", return_value=b"data"):
            await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(sent_prompts), 1)
        prompt = sent_prompts[0]
        self.assertIn("I've uploaded analysis.xlsx to the workspace. Please inspect or process this file.", prompt)
        self.assertIn("[Attached Slack File: analysis.xlsx saved to ", prompt)


class TestIngressFilteringChallenge(unittest.IsolatedAsyncioTestCase):
    """
    Challenge 4: Ingress Filtering & Bot Isolation
    Ensures bot messages with files are safely blocked, but human user file uploads are never dropped.
    """

    async def asyncSetUp(self):
        self._orig_bot_user_id = app.BOT_USER_ID
        app.BOT_USER_ID = "U_AGY_BOT"
        self.client = AsyncMock()
        self.client.auth_test = AsyncMock(return_value={"user_id": "U_AGY_BOT"})

    async def asyncTearDown(self):
        app.BOT_USER_ID = self._orig_bot_user_id

    async def test_bot_messages_with_files_are_safely_blocked(self):
        """Verify that events originating from bots containing files are strictly rejected."""
        bot_cases = [
            {"subtype": "bot_message", "text": "", "files": [{"id": "F1"}]},
            {"bot_id": "B_HERMES", "text": "File from Hermes", "files": [{"id": "F2"}]},
            {"bot_profile": {"name": "slackbot"}, "text": "", "files": [{"id": "F3"}]},
            {"user": "U_AGY_BOT", "text": "Self echo", "files": [{"id": "F4"}]},
            {"user": "hermes-assistant", "text": "Butler upload", "files": [{"id": "F5"}]},
            {"user": "github-bot", "text": "CI artifact", "files": [{"id": "F6"}]},
        ]

        for case in bot_cases:
            event = {
                "type": "message",
                "channel": "D_TEST",
                "channel_type": "im",
                "ts": "1729000040.000100",
                **case,
            }
            allowed = await app.should_process_ingress_event(event, self.client)
            self.assertFalse(
                allowed,
                f"Security breach: Bot message with files was NOT filtered! Case: {case}"
            )

    async def test_human_messages_with_files_are_strictly_allowed(self):
        """Verify human uploads in DMs and mentioned threads are preserved."""
        human_cases = [
            {"channel": "D_CARL", "channel_type": "im", "text": "", "user": "U_CARL"},
            {"channel": "D_CARL", "channel_type": "im", "text": "inspect this", "user": "U_CARL"},
            {"channel": "C_DEV", "text": "<@U_AGY_BOT> please review", "user": "U_CARL"},
        ]

        for case in human_cases:
            event = {
                "type": "message",
                "subtype": "file_share",
                "ts": "1729000041.000100",
                "files": [{"id": "F_HUMAN", "name": "code.py"}],
                **case,
            }
            allowed = await app.should_process_ingress_event(event, self.client)
            self.assertTrue(
                allowed,
                f"False rejection: Human user file upload was dropped! Case: {case}"
            )


if __name__ == "__main__":
    unittest.main()
