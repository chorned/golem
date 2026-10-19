#!/usr/bin/env python3
"""
tests/test_on_demand_notify.py
Comprehensive unit test suite for Golem's On-Demand Messaging Engine (golem/notify.py):
- Slack messaging with session registry tracking for thread continuity
- iMessage messaging via BlueBubbles with history persistence for thread continuity
- Concurrent dual-platform messaging (Slack + iMessage)
- Error handling & validation

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.notify import (
    DEFAULT_IMESSAGE_CHAT_GUID,
    DEFAULT_SLACK_CHANNEL,
    send_imessage,
    send_on_demand_message,
    send_slack_message,
)


class TestOnDemandNotify(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.registry_path = os.path.join(self.temp_dir.name, "sessions_registry.json")

    def tearDown(self):
        self.temp_dir.cleanup()

    async def test_send_slack_message_success_and_registry(self):
        """Verify Slack message is dispatched and registered in sessions_registry.json."""
        mock_resp = {"ok": True, "ts": "1740000000.123456", "channel": DEFAULT_SLACK_CHANNEL}
        mock_client = MagicMock()
        mock_client.chat_postMessage = AsyncMock(return_value=mock_resp)

        with patch("slack_sdk.web.async_client.AsyncWebClient", return_value=mock_client):
            with patch.dict(os.environ, {
                "SLACK_BOT_TOKEN": "xoxb-test-token",
                "FIRSTMATE_SESSIONS_REGISTRY_PATH": self.registry_path,
            }):
                res = await send_slack_message("Needs attention: server high memory", allow_live_in_test=True)

                self.assertTrue(res["ok"])
                self.assertEqual(res["platform"], "slack")
                self.assertEqual(res["ts"], "1740000000.123456")
                self.assertEqual(res["channel"], DEFAULT_SLACK_CHANNEL)

                # Verify session was registered in registry.json for thread continuation
                self.assertTrue(os.path.isfile(self.registry_path))
                with open(self.registry_path, "r", encoding="utf-8") as f:
                    reg = json.load(f)
                expected_key = f"{DEFAULT_SLACK_CHANNEL}:1740000000.123456"
                self.assertIn(expected_key, reg)
                self.assertTrue(reg[expected_key].get("is_on_demand"))

    async def test_send_slack_message_thread_continuation(self):
        """Verify Slack message respects custom channel and thread_ts."""
        mock_resp = {"ok": True, "ts": "1740000010.654321", "channel": "C_CUSTOM"}
        mock_client = MagicMock()
        mock_client.chat_postMessage = AsyncMock(return_value=mock_resp)

        with patch("slack_sdk.web.async_client.AsyncWebClient", return_value=mock_client):
            with patch.dict(os.environ, {
                "SLACK_BOT_TOKEN": "xoxb-test-token",
                "FIRSTMATE_SESSIONS_REGISTRY_PATH": self.registry_path,
            }):
                res = await send_slack_message(
                    text="Follow up on previous query",
                    channel="C_CUSTOM",
                    thread_ts="1740000000.123456",
                    allow_live_in_test=True,
                )

                self.assertTrue(res["ok"])
                mock_client.chat_postMessage.assert_awaited_once_with(
                    channel="C_CUSTOM",
                    text="Follow up on previous query",
                    thread_ts="1740000000.123456",
                )

    async def test_send_slack_message_test_mode_safeguard(self):
        """Verify test mode prevents live Slack dispatch and returns safe mock response."""
        with patch.dict(os.environ, {"GOLEM_TEST_MODE": "1", "SLACK_BOT_TOKEN": "xoxb-real-token"}):
            with patch("slack_sdk.web.async_client.AsyncWebClient") as mock_client:
                res = await send_slack_message("Needs attention: server high memory")

                self.assertTrue(res["ok"])
                self.assertTrue(res.get("mock"))
                self.assertEqual(res["platform"], "slack")
                self.assertEqual(res["ts"], "123.456")
                self.assertEqual(res["channel"], DEFAULT_SLACK_CHANNEL)
                mock_client.assert_not_called()

    async def test_send_slack_message_validation_and_errors(self):
        """Verify empty text or missing token fails cleanly."""
        res_empty = await send_slack_message("")
        self.assertFalse(res_empty["ok"])
        self.assertIn("empty", res_empty["error"])

        with patch.dict(os.environ, {"SLACK_BOT_TOKEN": "", "GOLEM_TEST_MODE": "0", "FIRSTMATE_TEST_MODE": "0"}, clear=True):
            res_no_tok = await send_slack_message("hello", token="", allow_live_in_test=True)
            self.assertFalse(res_no_tok["ok"])
            self.assertIn("Missing SLACK_BOT_TOKEN", res_no_tok["error"])

    async def test_send_imessage_success_and_thread_continuity(self):
        """Verify iMessage is dispatched via BlueBubblesClient with guid persistence."""
        mock_transport = MagicMock()
        mock_client = MagicMock()
        mock_client.chat_postMessage = AsyncMock(return_value={
            "ok": True,
            "ts": "BB-GUID-12345",
            "guid": "BB-GUID-12345",
            "channel": DEFAULT_IMESSAGE_CHAT_GUID,
        })
        mock_transport.get_client = MagicMock(return_value=mock_client)
        mock_transport._session = None

        with patch("bluebubbles_transport.BlueBubblesTransport", return_value=mock_transport):
            with patch("bluebubbles_transport.resolve_bluebubbles_url", return_value="http://127.0.0.1:1234"):
                with patch("bluebubbles_transport.resolve_bluebubbles_password", return_value="secret"):
                    res = await send_imessage(
                        text="Urgent: fleet node unreachable",
                        chat_guid=DEFAULT_IMESSAGE_CHAT_GUID,
                        allow_live_in_test=True,
                    )

                    self.assertTrue(res["ok"])
                    self.assertEqual(res["platform"], "imessage")
                    self.assertEqual(res["guid"], "BB-GUID-12345")
                    self.assertEqual(res["chat_guid"], DEFAULT_IMESSAGE_CHAT_GUID)

                    mock_client.chat_postMessage.assert_awaited_once_with(
                        channel=DEFAULT_IMESSAGE_CHAT_GUID,
                        text="Urgent: fleet node unreachable",
                        reply_to_guid=None,
                        new_thread=True,
                    )

    async def test_send_imessage_test_mode_safeguard(self):
        """Verify test mode prevents live BlueBubbles dispatch and returns safe mock response."""
        with patch.dict(os.environ, {"GOLEM_TEST_MODE": "1"}):
            with patch("bluebubbles_transport.BlueBubblesTransport") as mock_transport:
                res = await send_imessage("Urgent: fleet node unreachable")

                self.assertTrue(res["ok"])
                self.assertTrue(res.get("mock"))
                self.assertEqual(res["platform"], "imessage")
                self.assertEqual(res["guid"], "mock-imessage-guid-123.456")
                self.assertEqual(res["chat_guid"], DEFAULT_IMESSAGE_CHAT_GUID)
                mock_transport.assert_not_called()

    async def test_send_on_demand_message_dual_concurrent(self):
        """Verify send_on_demand_message dispatches to both platforms concurrently."""
        with patch("golem.notify.send_slack_message", new_callable=AsyncMock) as mock_slack:
            with patch("golem.notify.send_imessage", new_callable=AsyncMock) as mock_imessage:
                mock_slack.return_value = {"ok": True, "platform": "slack", "ts": "100.1"}
                mock_imessage.return_value = {"ok": True, "platform": "imessage", "guid": "GUID.1"}

                res = await send_on_demand_message("System update completed", target="both")

                self.assertTrue(res["ok"])
                self.assertTrue(res["slack"]["ok"])
                self.assertTrue(res["imessage"]["ok"])
                mock_slack.assert_awaited_once()
                mock_imessage.assert_awaited_once()

    async def test_send_on_demand_message_target_filtering(self):
        """Verify target='slack' only messages Slack, target='imessage' only messages iMessage."""
        with patch("golem.notify.send_slack_message", new_callable=AsyncMock) as mock_slack:
            with patch("golem.notify.send_imessage", new_callable=AsyncMock) as mock_imessage:
                mock_slack.return_value = {"ok": True, "platform": "slack"}
                mock_imessage.return_value = {"ok": True, "platform": "imessage"}

                # Slack only
                res_s = await send_on_demand_message("Slack test", target="slack")
                self.assertTrue(res_s["ok"])
                self.assertIn("slack", res_s)
                self.assertNotIn("imessage", res_s)

                mock_slack.reset_mock()
                mock_imessage.reset_mock()

                # iMessage only
                res_i = await send_on_demand_message("iMessage test", target="imessage")
                self.assertTrue(res_i["ok"])
                self.assertIn("imessage", res_i)
                self.assertNotIn("slack", res_i)


if __name__ == "__main__":
    unittest.main()
