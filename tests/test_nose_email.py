#!/usr/bin/env python3
"""
tests/test_nose_email.py
Unit tests covering:
1. NoseSentinel email sniffing, startup calibration (ignoring pre-existing unread emails),
   sender filtering, and marking emails as read.
2. ShoulderTapAlert(alert_type="email_received") emission on event bus.
3. Reactive handler processing the alert, formatting the analysis + suggested action message,
   and calling send_slack_message with session continuity registration.
"""

import os
import sys
import json
import time
import asyncio
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.bus import ShoulderTapAlert, ShoulderTapEventBus
from golem.nose import (
    NoseSentinel,
    EmailSniffer,
    resolve_email_query,
    extract_urls_from_text,
    DEFAULT_NOSE_EMAIL_QUERY,
)
from golem.ingress import IngressGateway
from golem.router import SessionRouter
from golem.notify import DEFAULT_SLACK_CHANNEL
from golem.mouth import synthesize_email_briefing, derive_actionable_followup


class MockAgentDriver:
    """Mock agent driver for SessionRouter."""
    def __init__(self) -> None:
        self.conversation_id = "test-driver-conv"
        self.received_prompts = []

    async def stream_turn(self, prompt: str):
        self.received_prompts.append(prompt)
        from golem.protocol import FinishEvent, TextDeltaEvent
        yield TextDeltaEvent(text_delta="Confirmed calendar event.")
        yield FinishEvent(status="SUCCESS", response="Confirmed calendar event.")

    async def send_turn(self, prompt: str):
        async for ev in self.stream_turn(prompt):
            yield ev

    async def start(self):
        return True


class MockSlackClient:
    """Mock Slack client tracking posted messages and thread replies."""
    def __init__(self) -> None:
        self.posted_messages = []

    async def chat_postMessage(self, channel, text, thread_ts=None, **kwargs):
        msg_ts = kwargs.get("ts", "999.001")
        self.posted_messages.append({
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": msg_ts,
        })
        return {"ok": True, "ts": msg_ts, "channel": channel}

    async def reactions_add(self, name, channel, timestamp):
        return {"ok": True}

    async def reactions_remove(self, name, channel, timestamp):
        return {"ok": True}

    async def conversations_replies(self, channel, ts, limit=20):
        replies = []
        for msg in self.posted_messages:
            if msg.get("channel") == channel and (msg.get("ts") == ts or msg.get("thread_ts") == ts):
                replies.append({
                    "user": "U_GOLEM",
                    "bot_id": "B_GOLEM",
                    "text": msg.get("text", ""),
                    "ts": msg.get("ts", ts),
                })
        return {"ok": True, "messages": replies}


class TestNoseSentinelEmailSniffing(unittest.IsolatedAsyncioTestCase):
    """
    Test suite for NoseSentinel and EmailSniffer:
    - Startup calibration (ignoring pre-existing unread emails)
    - Sender filtering and query resolution
    - Marking emails as read
    """

    async def asyncSetUp(self):
        os.environ["GOLEM_TEST_MODE"] = "1"
        os.environ["FIRSTMATE_TEST_MODE"] = "1"
        self.bus = ShoulderTapEventBus()

    async def asyncTearDown(self):
        self.bus.clear()

    async def test_startup_calibration_ignores_preexisting_emails(self):
        """Verify startup calibration records all existing unread emails and ignores them on subsequent ticks."""
        sniffer = EmailSniffer(
            query="is:unread from:k.horned@gmail.com",
            enabled=True,
            bus=self.bus,
        )

        # 1. Mock search_messages returning 2 pre-existing unread emails on boot
        preexisting_msgs = [
            {"id": "msg_pre_001", "from": "Carl <k.horned@gmail.com>", "subject": "Old Email 1"},
            {"id": "msg_pre_002", "from": "Carl <k.horned@gmail.com>", "subject": "Old Email 2"},
        ]
        sniffer.search_messages = AsyncMock(return_value=preexisting_msgs)

        # Execute startup calibration
        calibrated_ids = await sniffer.calibrate_startup()
        self.assertTrue(sniffer.is_calibrated)
        self.assertEqual(calibrated_ids, {"msg_pre_001", "msg_pre_002"})
        self.assertIn("msg_pre_001", sniffer.seen_message_ids)
        self.assertIn("msg_pre_002", sniffer.seen_message_ids)

        # 2. Run sniff_new_emails when Gmail still returns the pre-existing emails
        alerts = await sniffer.sniff_new_emails()
        # Pre-existing emails must be ignored
        self.assertEqual(len(alerts), 0)
        self.assertEqual(sniffer.processed_count, 0)

    async def test_sender_filtering_and_query_configuration(self):
        """Verify query resolution accurately applies sender filtering and custom queries."""
        # Configured via explicit sender parameter
        sentinel_sender = NoseSentinel(
            bus=self.bus,
            email_senders="partner@company.com",
            email_enabled=True,
        )
        self.assertEqual(sentinel_sender.email_sniffer.query, "is:unread from:partner@company.com")

        # Configured via multiple senders
        sentinel_multi = NoseSentinel(
            bus=self.bus,
            email_senders="a@test.com, b@test.com",
            email_enabled=True,
        )
        self.assertEqual(sentinel_multi.email_sniffer.query, "is:unread (from:a@test.com OR from:b@test.com)")

        # Configured via custom query override
        sentinel_query = NoseSentinel(
            bus=self.bus,
            email_query="label:vip is:unread",
            email_enabled=True,
        )
        self.assertEqual(sentinel_query.email_sniffer.query, "label:vip is:unread")

    async def test_sniffing_new_email_and_marking_as_read(self):
        """Verify discovering a new email fetches full content, extracts URLs, and marks as read."""
        sniffer = EmailSniffer(
            query="is:unread from:k.horned@gmail.com",
            enabled=True,
            bus=self.bus,
        )
        sniffer.is_calibrated = True
        sniffer.seen_message_ids = {"msg_old_1"}

        # Mock search finding 1 old email and 1 brand new email
        sniffer.search_messages = AsyncMock(return_value=[
            {"id": "msg_old_1"},
            {"id": "msg_new_2", "from": "Carl <k.horned@gmail.com>", "subject": "Concert Reel"},
        ])

        # Mock fetching message details
        sniffer.get_message = AsyncMock(return_value={
            "id": "msg_new_2",
            "from": "Carl <k.horned@gmail.com>",
            "subject": "Concert Reel",
            "body": "Check this reel: https://www.instagram.com/reel/DbtKto1ublf/ and buy tickets",
            "snippet": "Check this reel: https://www.instagram.com/reel/DbtKto1ublf/",
            "date": "Thu, 15 Oct 2026 13:00:00 -0500",
        })

        # Mock marking email as read
        marked_read_ids = []
        async def _mock_mark(msg_id):
            marked_read_ids.append(msg_id)
            return True
        sniffer.mark_as_read = AsyncMock(side_effect=_mock_mark)

        alerts = await sniffer.sniff_new_emails()

        # 1. Alert generated for new email
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.alert_type, "email_received")
        self.assertEqual(alert.data["email_id"], "msg_new_2")
        self.assertIn("https://www.instagram.com/reel/DbtKto1ublf/", alert.data["urls"])

        # 2. Email was marked as read
        self.assertEqual(marked_read_ids, ["msg_new_2"])
        self.assertEqual(sniffer.processed_count, 1)
        self.assertIn("msg_new_2", sniffer.seen_message_ids)

        # 3. Telemetry reflects processed count
        telemetry = sniffer.get_telemetry()
        self.assertEqual(telemetry["processed_count"], 1)
        self.assertTrue(telemetry["enabled"])


class TestShoulderTapAlertEmission(unittest.IsolatedAsyncioTestCase):
    """
    Test suite for ShoulderTapAlert(alert_type="email_received") emission on ShoulderTapEventBus.
    """

    async def asyncSetUp(self):
        os.environ["GOLEM_TEST_MODE"] = "1"
        os.environ["FIRSTMATE_TEST_MODE"] = "1"
        self.bus = ShoulderTapEventBus()

    async def asyncTearDown(self):
        self.bus.clear()

    async def test_email_received_alert_emission_and_properties(self):
        """Verify NoseSentinel publishes alert_type='email_received' to event bus subscribers."""
        received_alerts = []

        def _subscriber(alert: ShoulderTapAlert):
            received_alerts.append(alert)

        self.bus.subscribe(_subscriber, alert_types=["email_received"])

        sentinel = NoseSentinel(
            bus=self.bus,
            email_enabled=True,
        )
        sentinel.email_sniffer.is_calibrated = True

        # Mock Gmail discovering a new unread email
        sentinel.email_sniffer.search_messages = AsyncMock(return_value=[
            {"id": "msg_alert_007", "from": "Carl", "subject": "URGENT Event"}
        ])
        sentinel.email_sniffer.get_message = AsyncMock(return_value={
            "id": "msg_alert_007",
            "from": "Carl <k.horned@gmail.com>",
            "subject": "URGENT Event",
            "body": "Look at https://reddit.com/r/technology/comments/xyz",
            "snippet": "Look at https://reddit.com/r/technology/comments/xyz",
            "date": "Thu, 15 Oct 2026 13:05:00 -0500",
        })
        sentinel.email_sniffer.mark_as_read = AsyncMock(return_value=True)

        alerts = await sentinel.sniff_emails()
        self.assertEqual(len(alerts), 1)

        # Wait briefly for async bus task dispatch
        await asyncio.sleep(0.05)

        # Verify bus subscriber received the alert
        self.assertEqual(len(received_alerts), 1)
        rec = received_alerts[0]
        self.assertEqual(rec.alert_type, "email_received")
        self.assertTrue(rec.is_email_received)
        self.assertEqual(rec.source, "nose")
        self.assertEqual(rec.data["email_id"], "msg_alert_007")
        self.assertEqual(rec.data["subject"], "URGENT Event")
        self.assertEqual(rec.data["urls"], ["https://reddit.com/r/technology/comments/xyz"])


class TestReactiveMouthEmailHandler(unittest.IsolatedAsyncioTestCase):
    """
    Test suite for IngressGateway reactive handler:
    - Processing email_received alert
    - Formatting analysis + suggested action message
    - Calling send_slack_message with session continuity registration
    - Thread reply continuity when Carl says 'Yes'
    """

    async def asyncSetUp(self):
        os.environ["GOLEM_TEST_MODE"] = "1"
        os.environ["FIRSTMATE_TEST_MODE"] = "1"
        self.bus = ShoulderTapEventBus()
        self.driver = MockAgentDriver()
        self.router = SessionRouter(
            driver_factory=lambda cwd, extra_args=None: self.driver,
            bus=self.bus,
        )
        self.gateway = IngressGateway(
            router=self.router,
            sensory_bus=self.bus,
            bot_user_id="U_GOLEM",
        )
        self.client = MockSlackClient()

    async def asyncTearDown(self):
        self.bus.clear()

    @patch("golem.mouth.inspect_and_summarize_urls")
    async def test_reactive_handler_formats_analysis_and_suggested_action(self, mock_inspect):
        """Verify handler extracts content, formats analysis, and includes actionable follow-up."""
        mock_inspect.return_value = [{
            "url": "https://www.instagram.com/reel/DbtKto1ublf/",
            "platform": "Instagram",
            "title": "Red Rocks Live Tour",
            "description": "Tickets on sale Friday at 10am MST",
            "author": "thewarondrugs",
        }]

        alert = ShoulderTapAlert(
            alert_type="email_received",
            source="nose",
            summary="New email from Carl: Tour Announcement",
            data={
                "email_id": "msg_format_test",
                "from": "Carl Horned <k.horned@gmail.com>",
                "subject": "Red Rocks 2026 Tour",
                "body": "Here is the reel: https://www.instagram.com/reel/DbtKto1ublf/",
                "snippet": "Here is the reel...",
                "urls": ["https://www.instagram.com/reel/DbtKto1ublf/"],
                "date": "Thu, 15 Oct 2026 13:10:00 -0500",
            },
        )

        await self.gateway._handle_email_received_alert(alert, client=self.client)

        # 1. Message sent to Slack
        self.assertEqual(len(self.client.posted_messages), 1)
        msg = self.client.posted_messages[0]
        self.assertEqual(msg["channel"], DEFAULT_SLACK_CHANNEL)

        # 2. Contains analysis of what the post is about
        self.assertIn("📬 *Shared via Email from Carl Horned:* *Red Rocks 2026 Tour*", msg["text"])
        self.assertIn("*Red Rocks Live Tour*", msg["text"])
        self.assertIn("Tickets on sale Friday at 10am MST", msg["text"])

        # 3. Contains direct link
        self.assertIn("https://www.instagram.com/reel/DbtKto1ublf/", msg["text"])

        # 4. Contains actionable follow-up question
        self.assertIn("Want me to create a calendar event for tomorrow to buy tickets?", msg["text"])

    @patch("golem.notify.send_slack_message")
    @patch("golem.mouth.inspect_and_summarize_urls")
    async def test_reactive_handler_calls_send_slack_message_and_records_registry(
        self, mock_inspect, mock_send_slack
    ):
        """Verify handler uses send_slack_message when no explicit client is passed and registers session."""
        mock_inspect.return_value = []
        mock_send_slack.return_value = {
            "ok": True,
            "ts": "888.123",
            "channel": DEFAULT_SLACK_CHANNEL,
            "thread_ts": "888.123",
        }

        alert = ShoulderTapAlert(
            alert_type="email_received",
            source="nose",
            summary="New email from Carl",
            data={
                "email_id": "msg_send_slack_test",
                "from": "Carl",
                "subject": "Article to read",
                "body": "Check out this newsletter article on AI agents",
                "snippet": "Check out this newsletter",
                "urls": ["https://example.com/newsletter/123"],
            },
        )

        # Call handler with client=None (standard background bus invocation)
        await self.gateway._handle_email_received_alert(alert, client=None)

        # Verify send_slack_message was called
        mock_send_slack.assert_called_once()
        call_kwargs = mock_send_slack.call_args[1]
        self.assertEqual(call_kwargs["channel"], DEFAULT_SLACK_CHANNEL)
        self.assertIn("Article to read", call_kwargs["text"])
        self.assertIn("email_context", call_kwargs)
        self.assertEqual(call_kwargs["email_context"]["email_id"], "msg_send_slack_test")
        self.assertIn("reading list", call_kwargs["email_context"]["suggested_action"])

        # Verify session was warmed in router under channel:ts
        session_key = f"{DEFAULT_SLACK_CHANNEL}:888.123"
        self.assertIn(session_key, self.router.sessions)
        session = self.router.sessions[session_key]
        self.assertIsNotNone(session.email_context)
        self.assertEqual(session.email_context["subject"], "Article to read")

    @patch("golem.notify.send_slack_message")
    @patch("golem.mouth.inspect_and_summarize_urls")
    async def test_end_to_end_nose_to_ingress_warm_reply(self, mock_inspect, mock_send_slack):
        """
        Full end-to-end integration test:
        1. NoseSentinel emits email_received alert over ShoulderTapEventBus.
        2. IngressGateway reactive handler receives alert via subscription.
        3. Message posted to Carl in Slack and session is warmed.
        4. Carl replies 'Yes' in that thread.
        5. Session hydrates prompt with original email context + suggested action.
        """
        mock_send_slack.return_value = {
            "ok": True,
            "ts": "777.001",
            "channel": DEFAULT_SLACK_CHANNEL,
            "thread_ts": "777.001",
        }
        mock_inspect.return_value = [{
            "url": "https://www.instagram.com/reel/concert123/",
            "platform": "Instagram",
            "title": "Summer Nights Tour",
            "description": "Presale tickets tomorrow morning",
            "author": "band",
        }]

        # Set up sentinel sharing the same event bus as IngressGateway
        sentinel = NoseSentinel(
            bus=self.bus,
            email_enabled=True,
        )
        sentinel.email_sniffer.is_calibrated = True

        # Mock discovery of incoming email
        sentinel.email_sniffer.search_messages = AsyncMock(return_value=[
            {"id": "msg_e2e_001", "from": "Carl", "subject": "Tour Announced"}
        ])
        sentinel.email_sniffer.get_message = AsyncMock(return_value={
            "id": "msg_e2e_001",
            "from": "Carl <k.horned@gmail.com>",
            "subject": "Tour Announced",
            "body": "Take a look: https://www.instagram.com/reel/concert123/",
            "snippet": "Take a look...",
            "date": "Thu, 15 Oct 2026 13:15:00 -0500",
        })
        sentinel.email_sniffer.mark_as_read = AsyncMock(return_value=True)

        # 1. Trigger email sniff from NoseSentinel
        alerts = await sentinel.sniff_emails()
        self.assertEqual(len(alerts), 1)

        # Let the event bus dispatch to IngressGateway subscriber
        await asyncio.sleep(0.08)

        # 2. Check that the router session has been pre-warmed
        warmed_sessions = [s for s in self.router.sessions.values() if s.email_context]
        self.assertTrue(len(warmed_sessions) > 0)
        session = warmed_sessions[0]
        self.assertEqual(session.email_context["email_id"], "msg_e2e_001")
        self.assertIn("tickets", session.email_context["suggested_action"].lower())

        # 3. Carl replies "Yes" to that thread
        root_ts = session.thread_ts
        reply_event = {
            "channel": session.channel,
            "thread_ts": root_ts,
            "ts": f"{float(root_ts) + 1.0:.6f}",
            "text": "Yes, please do that",
            "user": "UAM3FCV5L",
        }
        await self.gateway.handle_message(reply_event, client=self.client)

        # 4. Agent adapter must receive the prompt containing the email context and suggested action
        self.assertTrue(len(self.driver.received_prompts) > 0)
        last_prompt = self.driver.received_prompts[-1]
        self.assertIn("Tour Announced", last_prompt)
        self.assertIn("Yes, please do that", last_prompt)
        self.assertIn("Suggested Action", last_prompt)


if __name__ == "__main__":
    unittest.main()
