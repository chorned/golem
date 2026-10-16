#!/usr/bin/env python3
"""
tests/test_nose_gmail_sniffer.py
Unit tests for Gmail inbox sniffer in NoseSentinel (golem/nose.py)
and ShoulderTapEventBus alert types (golem/bus.py).
"""

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from golem.bus import ShoulderTapAlert, ShoulderTapEventBus, get_default_event_bus
from golem.nose import (
    EmailSniffer,
    NoseSentinel,
    FleetSnapshot,
    resolve_email_query,
    extract_urls_from_text,
    DEFAULT_NOSE_EMAIL_QUERY,
)


class TestEmailSnifferUnit(unittest.IsolatedAsyncioTestCase):
    """Test suite for EmailSniffer and its helper methods."""

    def setUp(self) -> None:
        self.bus = ShoulderTapEventBus()

    def tearDown(self) -> None:
        self.bus.clear()

    def test_resolve_email_query_defaults(self) -> None:
        """Verify query resolution precedence: param > NOSE_EMAIL_QUERY > NOSE_EMAIL_SENDERS > default."""
        # 1. Default
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(resolve_email_query(), DEFAULT_NOSE_EMAIL_QUERY)

        # 2. NOSE_EMAIL_QUERY env var
        with patch.dict(os.environ, {"NOSE_EMAIL_QUERY": "is:unread label:urgent"}):
            self.assertEqual(resolve_email_query(), "is:unread label:urgent")

        # 3. NOSE_EMAIL_SENDERS single
        with patch.dict(os.environ, {"NOSE_EMAIL_SENDERS": "alice@example.com"}):
            self.assertEqual(resolve_email_query(), "is:unread from:alice@example.com")

        # 4. NOSE_EMAIL_SENDERS multiple
        with patch.dict(os.environ, {"NOSE_EMAIL_SENDERS": "alice@example.com, bob@example.com"}):
            self.assertEqual(resolve_email_query(), "is:unread (from:alice@example.com OR from:bob@example.com)")

        # 5. Explicit query param overrides env vars
        with patch.dict(os.environ, {"NOSE_EMAIL_QUERY": "is:unread", "NOSE_EMAIL_SENDERS": "test@test.com"}):
            self.assertEqual(resolve_email_query(query="from:boss@corp.com"), "from:boss@corp.com")

    def test_extract_urls_from_text(self) -> None:
        """Verify URL regex extraction and trailing punctuation stripping."""
        text = (
            "Hey check out this Instagram reel:\n"
            "https://www.instagram.com/reel/DbtKto1ublf/?psln=abc,\n"
            "and also https://reddit.com/r/dnb/comments/xyz. "
            "Also see http://example.org/path?q=1&b=2."
        )
        urls = extract_urls_from_text(text)
        expected = [
            "https://www.instagram.com/reel/DbtKto1ublf/?psln=abc",
            "https://reddit.com/r/dnb/comments/xyz",
            "http://example.org/path?q=1&b=2",
        ]
        self.assertEqual(urls, expected)

    def test_graceful_fallback_test_mode(self) -> None:
        """Verify graceful fallback when running under GOLEM_TEST_MODE=1."""
        with patch.dict(os.environ, {"GOLEM_TEST_MODE": "1"}):
            sniffer = EmailSniffer(bus=self.bus)
            self.assertFalse(sniffer.is_available())
            telemetry = sniffer.get_telemetry()
            self.assertFalse(telemetry["enabled"])
            self.assertEqual(telemetry["processed_count"], 0)

    def test_graceful_fallback_missing_token(self) -> None:
        """Verify graceful fallback when google_token.json is missing."""
        with patch.dict(os.environ, {"GOLEM_TEST_MODE": "0", "FIRSTMATE_TEST_MODE": "0"}):
            sniffer = EmailSniffer(
                bus=self.bus,
                token_path="/tmp/nonexistent_google_token_12345.json",
                enabled=None,
            )
            self.assertFalse(sniffer.is_available())

    async def test_startup_calibration_records_existing_unread(self) -> None:
        """Verify startup calibration records all existing unread message IDs without processing."""
        sniffer = EmailSniffer(bus=self.bus, enabled=True)
        # Mock search_messages returning pre-existing emails
        sniffer.search_messages = AsyncMock(return_value=[
            {"id": "msg_pre_1", "subject": "Old unread 1"},
            {"id": "msg_pre_2", "subject": "Old unread 2"},
        ])
        sniffer.get_message = AsyncMock()
        sniffer.mark_as_read = AsyncMock()

        received_alerts: list[ShoulderTapAlert] = []
        self.bus.subscribe(lambda a: received_alerts.append(a), alert_types=["email_received"])

        # First sniff: should calibrate and return 0 alerts
        alerts = await sniffer.sniff_new_emails()
        self.assertEqual(len(alerts), 0)
        self.assertEqual(len(received_alerts), 0)
        self.assertTrue(sniffer.is_calibrated)
        self.assertIn("msg_pre_1", sniffer.seen_message_ids)
        self.assertIn("msg_pre_2", sniffer.seen_message_ids)
        self.assertEqual(sniffer.processed_count, 0)
        sniffer.get_message.assert_not_called()
        sniffer.mark_as_read.assert_not_called()

    async def test_sniff_new_email_processing_flow(self) -> None:
        """Verify new incoming email is fetched, URLs extracted, marked as read, and alert emitted."""
        sniffer = EmailSniffer(bus=self.bus, enabled=True)
        sniffer.is_calibrated = True
        sniffer.seen_message_ids = {"old_msg"}

        sniffer.search_messages = AsyncMock(return_value=[
            {"id": "old_msg", "subject": "Old"},
            {"id": "new_msg_101", "from": "Carl Horned <k.horned@gmail.com>", "subject": "Cool Reel", "snippet": "Check this reel"},
        ])
        sniffer.get_message = AsyncMock(return_value={
            "id": "new_msg_101",
            "from": "Carl Horned <k.horned@gmail.com>",
            "subject": "Cool Reel",
            "date": "Thu, 15 Oct 2026 12:00:00 -0500",
            "body": "Here is the reel: https://www.instagram.com/reel/DbtKto1ublf/ and discord: https://discord.gg/test",
            "snippet": "Here is the reel",
        })
        sniffer.mark_as_read = AsyncMock(return_value=True)

        received_alerts: list[ShoulderTapAlert] = []
        self.bus.subscribe(lambda a: received_alerts.append(a), alert_types=["email_received"])

        alerts = await sniffer.sniff_new_emails()

        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.alert_type, "email_received")
        self.assertEqual(alert.source, "nose")
        self.assertTrue(alert.is_email_received)
        self.assertIn("Carl Horned <k.horned@gmail.com>", alert.summary)
        self.assertEqual(alert.data["email_id"], "new_msg_101")
        self.assertEqual(alert.data["from"], "Carl Horned <k.horned@gmail.com>")
        self.assertEqual(alert.data["subject"], "Cool Reel")
        self.assertEqual(alert.data["urls"], [
            "https://www.instagram.com/reel/DbtKto1ublf/",
            "https://discord.gg/test",
        ])

        # Verify bus dispatch
        self.assertEqual(len(received_alerts), 1)
        self.assertEqual(received_alerts[0].data["email_id"], "new_msg_101")

        # Verify mark_as_read called
        sniffer.mark_as_read.assert_awaited_once_with("new_msg_101")
        self.assertEqual(sniffer.processed_count, 1)
        self.assertIn("new_msg_101", sniffer.seen_message_ids)

        # On subsequent poll, same message is not processed again
        alerts2 = await sniffer.sniff_new_emails()
        self.assertEqual(len(alerts2), 0)
        self.assertEqual(sniffer.processed_count, 1)


class TestNoseSentinelEmailIntegration(unittest.IsolatedAsyncioTestCase):
    """Test suite for NoseSentinel email sniffing integration."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path="/tmp/nonexistent_bot_err_test.log",
            email_enabled=True,
        )

    async def asyncTearDown(self) -> None:
        await self.sentinel.stop()
        self.bus.clear()

    async def test_telemetry_digest_and_snapshot_include_email(self) -> None:
        """Verify get_telemetry_digest and get_telemetry_snapshot include email telemetry."""
        digest = await self.sentinel.get_telemetry_digest()
        self.assertIn("email", digest)
        self.assertTrue(digest["email"]["enabled"])
        self.assertEqual(digest["email"]["processed_count"], 0)

        # Verify snapshot alias
        snapshot = await self.sentinel.get_telemetry_snapshot()
        self.assertIn("email", snapshot)
        self.assertEqual(snapshot["email"], digest["email"])

    async def test_tick_quiescent_emits_and_returns_email_alert(self) -> None:
        """Verify that when bot.err and fleet are quiescent, incoming email emits alert and tick returns it."""
        mock_sniffer = self.sentinel.email_sniffer
        mock_sniffer.is_calibrated = True

        mock_sniffer.search_messages = AsyncMock(return_value=[
            {"id": "email_xyz", "from": "Carl <k.horned@gmail.com>", "subject": "Test Subgenre"}
        ])
        mock_sniffer.get_message = AsyncMock(return_value={
            "id": "email_xyz",
            "from": "Carl <k.horned@gmail.com>",
            "subject": "Test Subgenre",
            "date": "Thu, 15 Oct 2026 12:30:00 -0500",
            "body": "Listen to this: https://instagram.com/reel/123",
            "snippet": "Listen to this",
        })
        mock_sniffer.mark_as_read = AsyncMock(return_value=True)

        received_alerts: list[ShoulderTapAlert] = []
        self.bus.subscribe(lambda a: received_alerts.append(a), alert_types=["email_received"])

        with patch.object(self.sentinel, "sniff_bot_err", return_value=([], "")):
            with patch.object(self.sentinel, "sniff_fleet", new_callable=AsyncMock) as mock_fleet:
                mock_fleet.return_value = FleetSnapshot()

                result = await self.sentinel.tick()

                self.assertIsNotNone(result)
                self.assertEqual(result.alert_type, "email_received")
                self.assertEqual(result.data["email_id"], "email_xyz")
                self.assertEqual(len(received_alerts), 1)
                self.assertEqual(received_alerts[0].data["urls"], ["https://instagram.com/reel/123"])


if __name__ == "__main__":
    unittest.main()
