#!/usr/bin/env python3
"""
tests/test_ingress_email_handler.py
Unit tests for the reactive Mouth email handler in IngressGateway (golem/ingress.py),
URL inspection and briefing synthesis in golem/mouth.py, and session pre-warming.
"""

import os
import sys
import json
import time
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.bus import ShoulderTapAlert, ShoulderTapEventBus
from golem.ingress import IngressGateway
from golem.router import SessionRouter
from golem.notify import DEFAULT_SLACK_CHANNEL
from golem.mouth import (
    inspect_single_url,
    inspect_and_summarize_urls,
    derive_actionable_followup,
    synthesize_email_briefing,
)


class MockSlackClient:
    def __init__(self) -> None:
        self.posted_messages = []
        self.reactions_added = []
        self.reactions_removed = []

    async def chat_postMessage(self, channel, text, thread_ts=None, **kwargs):
        msg_ts = kwargs.get("ts", "123.456")
        self.posted_messages.append({
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": msg_ts,
        })
        return {"ok": True, "ts": msg_ts, "channel": channel}

    async def reactions_add(self, name, channel, timestamp):
        self.reactions_added.append({"name": name, "channel": channel, "timestamp": timestamp})
        return {"ok": True}

    async def reactions_remove(self, name, channel, timestamp):
        self.reactions_removed.append({"name": name, "channel": channel, "timestamp": timestamp})
        return {"ok": True}

    async def conversations_replies(self, channel, ts, limit=20):
        # By default return the root briefing message posted earlier
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


class MockDriver:
    """Mock agent driver capturing prompts received."""
    def __init__(self) -> None:
        self.conversation_id = "test-conv-001"
        self.prompts_received = []

    async def stream_turn(self, prompt: str):
        self.prompts_received.append(prompt)
        from golem.protocol import FinishEvent, TextDeltaEvent
        yield TextDeltaEvent(text_delta="Calendar event created.")
        yield FinishEvent(status="SUCCESS", response="Calendar event created.")

    async def send_turn(self, prompt: str):
        async for ev in self.stream_turn(prompt):
            yield ev

    async def start(self):
        return True


class TestIngressEmailHandler(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        os.environ["GOLEM_TEST_MODE"] = "1"
        os.environ["FIRSTMATE_TEST_MODE"] = "1"
        self.bus = ShoulderTapEventBus()
        self.driver = MockDriver()
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

    def test_bus_subscription_registered(self):
        """Verify IngressGateway subscribes to email_received alerts on ShoulderTapEventBus."""
        found = False
        for cb, alert_types in self.bus._subscribers:
            if alert_types and "email_received" in alert_types:
                found = True
                break
        self.assertTrue(found, "IngressGateway must subscribe to 'email_received' alert types on the bus")

    def test_derive_actionable_followup(self):
        """Verify context-aware inference of suggested follow-up questions."""
        # 1. Concert / ticket context
        action1 = derive_actionable_followup("Check out this band playing at Red Rocks with tickets on sale Friday")
        self.assertIn("calendar event", action1)
        self.assertIn("tickets", action1)

        # 2. Reading / article context
        action2 = derive_actionable_followup("Here is a fascinating newsletter article on distributed systems")
        self.assertIn("reading list", action2)

        # 3. Video / reel context
        action3 = derive_actionable_followup("Check out this video clip from youtube")
        self.assertIn("reminder", action3)

    @patch("golem.mouth._extract_media_url_metadata")
    async def test_inspect_and_summarize_urls(self, mock_media):
        """Verify URL inspection parses social media and extracts clean metadata."""
        mock_media.return_value = {
            "title": "The War On Drugs Live",
            "description": "Tickets drop tomorrow at 10am MST",
            "author": "thewarondrugs",
            "duration": "1:30",
            "tags": ["rock", "live"],
        }
        urls = ["https://www.instagram.com/reel/DbtKto1ublf/"]
        summaries = await inspect_and_summarize_urls(urls)
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["platform"], "Instagram")
        self.assertEqual(summaries[0]["title"], "The War On Drugs Live")
        self.assertEqual(summaries[0]["author"], "thewarondrugs")

    async def test_synthesize_email_briefing_fallback_structure(self):
        """Verify synthesized briefing adheres to Carl's pattern: analysis, URL, actionable follow-up."""
        sender = "Carl Horned <k.horned@gmail.com>"
        subject = "Concert announcement"
        body = "Here is the reel for the upcoming tour"
        urls = ["https://www.instagram.com/reel/DbtKto1ublf/"]
        inspected = [{
            "platform": "Instagram",
            "title": "Red Rocks Tour",
            "description": "Presale starts Friday at 10am",
            "author": "artist",
        }]

        briefing, action = await synthesize_email_briefing(
            sender=sender,
            subject=subject,
            body=body,
            urls=urls,
            inspected_urls=inspected,
        )

        self.assertIn("📬 *Shared via Email from Carl Horned:*", briefing)
        self.assertIn("*Red Rocks Tour*", briefing)
        self.assertIn("Presale starts Friday", briefing)
        self.assertIn("https://www.instagram.com/reel/DbtKto1ublf/", briefing)
        self.assertIn("Want me to create a calendar event for tomorrow to buy tickets?", briefing)
        self.assertIn("tickets", action)

    @patch("golem.mouth.inspect_and_summarize_urls")
    async def test_handle_email_received_alert_full_flow(self, mock_inspect):
        """Verify alert dispatch delivers Slack message, records registry, and warms SessionHandle."""
        mock_inspect.return_value = [{
            "url": "https://www.instagram.com/reel/DbtKto1ublf/",
            "platform": "Instagram",
            "title": "Live at Red Rocks",
            "description": "Tickets on sale Friday at 10am MST",
            "author": "thewarondrugs",
        }]

        alert = ShoulderTapAlert(
            alert_type="email_received",
            source="nose",
            summary="New email from Carl: Live Tour",
            data={
                "email_id": "msg_test_001",
                "from": "Carl <k.horned@gmail.com>",
                "subject": "Live Tour Announcement",
                "body": "Check this reel: https://www.instagram.com/reel/DbtKto1ublf/",
                "snippet": "Check this reel: https://www.instagram.com/reel/DbtKto1ublf/",
                "urls": ["https://www.instagram.com/reel/DbtKto1ublf/"],
                "date": "Thu, 15 Oct 2026 12:00:00 -0500",
            },
        )

        # Dispatch alert through the sensory alert pipeline
        await self.gateway.process_sensory_alert(alert, client=self.client)

        # 1. Verify message posted to Slack
        self.assertEqual(len(self.client.posted_messages), 1)
        posted = self.client.posted_messages[0]
        self.assertEqual(posted["channel"], DEFAULT_SLACK_CHANNEL)
        self.assertIn("📬 *Shared via Email from Carl:*", posted["text"])
        self.assertIn("Live at Red Rocks", posted["text"])
        self.assertIn("https://www.instagram.com/reel/DbtKto1ublf/", posted["text"])
        self.assertIn("Want me to create a calendar event for tomorrow to buy tickets?", posted["text"])

        # 2. Verify SessionHandle is warm in SessionRouter
        session_key = f"{DEFAULT_SLACK_CHANNEL}:123.456"
        self.assertIn(session_key, self.router.sessions)
        session = self.router.sessions[session_key]
        self.assertIsNotNone(session.email_context)
        self.assertEqual(session.email_context["email_id"], "msg_test_001")
        self.assertEqual(session.email_context["subject"], "Live Tour Announcement")
        self.assertIn("tickets", session.email_context["suggested_action"].lower())

    @patch("golem.mouth.inspect_and_summarize_urls")
    async def test_warm_session_user_affirmation_continuity(self, mock_inspect):
        """Verify that when Carl replies 'Yes' in the thread, the warm session hydrates the prompt with email context."""
        mock_inspect.return_value = [{
            "url": "https://www.instagram.com/reel/123/",
            "platform": "Instagram",
            "title": "Summer Festival",
            "description": "Tickets drop tomorrow",
            "author": "fest",
        }]

        # 1. Trigger email alert to pre-warm the session
        alert = ShoulderTapAlert(
            alert_type="email_received",
            source="nose",
            data={
                "email_id": "msg_test_002",
                "from": "Carl <k.horned@gmail.com>",
                "subject": "Festival Tickets",
                "body": "Look: https://www.instagram.com/reel/123/",
                "snippet": "Look: https://www.instagram.com/reel/123/",
                "urls": ["https://www.instagram.com/reel/123/"],
            },
        )
        await self.gateway.process_sensory_alert(alert, client=self.client)
        root_ts = "123.456"

        # 2. Carl replies 'Yes' in the Slack thread
        reply_event = {
            "channel": DEFAULT_SLACK_CHANNEL,
            "thread_ts": root_ts,
            "ts": "123.789",
            "text": "Yes please",
            "user": "UAM3FCV5L",
        }
        await self.gateway.handle_message(reply_event, client=self.client)

        # 3. Check what prompt was received by the agent adapter
        self.assertTrue(len(self.driver.prompts_received) > 0)
        received_prompt = self.driver.prompts_received[-1]

        # The prompt MUST contain the email context and suggested action so the agent knows what 'Yes' refers to
        self.assertIn("Festival Tickets", received_prompt)
        self.assertIn("Yes please", received_prompt)
        self.assertIn("Suggested Action", received_prompt)

    async def test_email_deduplication(self):
        """Verify duplicate email alerts with the same email_id are dropped."""
        alert = ShoulderTapAlert(
            alert_type="email_received",
            source="nose",
            data={
                "email_id": "msg_dup_123",
                "from": "Carl",
                "subject": "Test Duplicate",
                "body": "Hello",
                "urls": [],
            },
        )

        await self.gateway.process_sensory_alert(alert, client=self.client)
        self.assertEqual(len(self.client.posted_messages), 1)

        # Dispatch again with same email_id
        await self.gateway.process_sensory_alert(alert, client=self.client)
        # Count must remain 1
        self.assertEqual(len(self.client.posted_messages), 1)

    @patch("golem.notify.send_slack_message", new_callable=AsyncMock)
    async def test_error_isolation(self, mock_send_slack):
        """Verify exceptions during Slack dispatch or URL inspection do not crash the handler."""
        mock_send_slack.return_value = {"ok": True, "ts": "mock.fallback.ts", "channel": "C_TEST"}

        alert = ShoulderTapAlert(
            alert_type="email_received",
            source="nose",
            data={
                "email_id": "msg_err_001",
                "from": "Carl",
                "subject": "Error Test",
                "body": "Crash test",
                "urls": ["http://invalid-url-domain-12345.xyz/test"],
            },
        )

        broken_client = MagicMock()
        broken_client.chat_postMessage.side_effect = Exception("Slack API down")

        # Must not raise unhandled exception
        try:
            await self.gateway.process_sensory_alert(alert, client=broken_client)
        except Exception as e:
            self.fail(f"process_sensory_alert raised exception: {e}")

        # Assert fallback was called safely without live dispatch
        mock_send_slack.assert_awaited_once()
        call_kwargs = mock_send_slack.await_args.kwargs
        self.assertIn("Shared via Email", call_kwargs.get("text", ""))
        self.assertIn("Error Test", call_kwargs.get("text", ""))


if __name__ == "__main__":
    unittest.main()
