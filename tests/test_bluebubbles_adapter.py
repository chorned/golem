"""
Unit tests for BlueBubbles iMessage Transport Adapter and unified Chief of Staff workflow.
Tests webhook authorization, sender whitelist, message routing, outbound chunking,
preflight alignment ("Grill-Me") over iMessage, and executive command execution.
"""

import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

import app
from bluebubbles_transport import (
    BlueBubblesClient,
    BlueBubblesTransport,
    chunk_text,
    MAX_IMESSAGE_CHUNK,
    sanitize_for_imessage,
    SLACK_EMOJI_MAP,
)


class TestBlueBubblesTransportUnit(unittest.TestCase):
    """Unit tests for standalone helper functions and client shims."""

    def test_chunk_text_small(self):
        msg = "Short message under 3500 chars"
        chunks = chunk_text(msg, max_chars=3500)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], msg)

    def test_chunk_text_large_break_lines(self):
        # Create message with multiple paragraphs exceeding limit
        para = "A" * 1500
        long_msg = f"{para}\n\n{para}\n\n{para}"
        self.assertTrue(len(long_msg) > 3500)

        chunks = chunk_text(long_msg, max_chars=3500)
        self.assertTrue(len(chunks) >= 2)
        for c in chunks:
            self.assertTrue(len(c) <= 3500)
        rejoined = "\n\n".join(chunks)
        self.assertEqual(len(rejoined.replace("\n", "")), len(long_msg.replace("\n", "")))

    def test_client_conversations_replies(self):
        transport = MagicMock()
        client = BlueBubblesClient(transport, "iMessage;-;k.horned@gmail.com")
        client.record_incoming("k.horned@gmail.com", "First message", "100.001")
        self.assertEqual(len(client.history), 1)

        async def _test():
            res = await client.conversations_replies("iMessage;-;k.horned@gmail.com")
            self.assertEqual(len(res["messages"]), 1)
            self.assertEqual(res["messages"][0]["text"], "First message")

            # Check .conversations property shim
            res2 = await client.conversations.replies("iMessage;-;k.horned@gmail.com")
            self.assertEqual(len(res2["messages"]), 1)

        asyncio.run(_test())

    def test_sanitize_for_imessage_links(self):
        # Redundant URL/label pair: <http://localhost:54028|http://localhost:54028> -> http://localhost:54028
        raw = "Sandbox server listening at <http://localhost:54028|http://localhost:54028>"
        sanitized = sanitize_for_imessage(raw)
        self.assertEqual(sanitized, "Sandbox server listening at http://localhost:54028")

        # Distinct label: <http://example.com|Example Page> -> Example Page (http://example.com)
        raw2 = "Check out <http://example.com|Example Page> now."
        sanitized2 = sanitize_for_imessage(raw2)
        self.assertEqual(sanitized2, "Check out Example Page (http://example.com) now.")

        # Bare link with brackets: <https://github.com/test/repo> -> https://github.com/test/repo
        raw3 = "Repository: <https://github.com/test/repo>"
        sanitized3 = sanitize_for_imessage(raw3)
        self.assertEqual(sanitized3, "Repository: https://github.com/test/repo")

    def test_sanitize_for_imessage_emojis(self):
        raw = ":white_check_mark: Completed task :thumbsup: and verified :eyes: without :warning: or :x:"
        sanitized = sanitize_for_imessage(raw)
        self.assertEqual(sanitized, "✅ Completed task 👍 and verified 👀 without ⚠️ or ❌")

    def test_sanitize_for_imessage_mentions(self):
        raw = "Alert sent to <#C01234|general> by <@U98765|carl>"
        sanitized = sanitize_for_imessage(raw)
        self.assertEqual(sanitized, "Alert sent to #general by @carl")

    def test_client_chat_post_message_sanitizes(self):
        transport = MagicMock()
        transport.send_imessage = AsyncMock(return_value={"status": 200})
        client = BlueBubblesClient(transport, "iMessage;-;k.horned@gmail.com")

        raw_text = ":white_check_mark: Server is up at <http://localhost:54028|http://localhost:54028>!"
        async def _test():
            res = await client.chat_postMessage(channel="iMessage;-;k.horned@gmail.com", text=raw_text)
            self.assertTrue(res["ok"])
            transport.send_imessage.assert_called_once()
            called_text = transport.send_imessage.call_args[0][1]
            self.assertIn("✅", called_text)
            self.assertIn("http://localhost:54028", called_text)
            self.assertNotIn(":white_check_mark:", called_text)
            self.assertNotIn("<http://localhost:54028", called_text)

        asyncio.run(_test())


class TestBlueBubblesWebhookServer(AioHTTPTestCase):
    """Integration tests for the aiohttp webhook endpoint."""

    async def get_application(self):
        self.dispatched_messages = []

        async def mock_handler(chat_guid, text, sender, payload):
            self.dispatched_messages.append({
                "chat_guid": chat_guid,
                "text": text,
                "sender": sender,
                "payload": payload
            })

        self.transport = BlueBubblesTransport(
            server_url="http://127.0.0.1:1234",
            password="test_secret_password",
            webhook_path="/bluebubbles-webhook",
            allowed_handles="k.horned@gmail.com,carl@example.com",
            message_handler=mock_handler
        )

        app_instance = web.Application()
        app_instance.router.add_get("/health", lambda _: web.Response(text="ok"))
        app_instance.router.add_post(self.transport.webhook_path, self.transport._handle_webhook)
        return app_instance

    @unittest_run_loop
    async def test_health_check(self):
        resp = await self.client.get("/health")
        self.assertEqual(resp.status, 200)
        self.assertEqual(await resp.text(), "ok")

    @unittest_run_loop
    async def test_webhook_unauthorized_missing_password(self):
        payload = {"type": "new-message", "data": {"text": "hello"}}
        resp = await self.client.post("/bluebubbles-webhook", json=payload)
        self.assertEqual(resp.status, 401)
        res_json = await resp.json()
        self.assertEqual(res_json.get("error"), "unauthorized")

    @unittest_run_loop
    async def test_webhook_unauthorized_wrong_password(self):
        payload = {"type": "new-message", "data": {"text": "hello"}}
        resp = await self.client.post("/bluebubbles-webhook?password=wrong_pass", json=payload)
        self.assertEqual(resp.status, 401)

    @unittest_run_loop
    async def test_webhook_whitelist_rejection(self):
        payload = {
            "type": "new-message",
            "data": {
                "text": "Unauthorized ping",
                "handle": {"address": "stranger@unknown.com"},
                "chats": [{"guid": "iMessage;-;stranger@unknown.com"}]
            }
        }
        resp = await self.client.post("/bluebubbles-webhook?password=test_secret_password", json=payload)
        self.assertEqual(resp.status, 403)
        res_json = await resp.json()
        self.assertEqual(res_json.get("reason"), "unauthorized_sender")
        self.assertEqual(len(self.dispatched_messages), 0)

    @unittest_run_loop
    async def test_webhook_whitelist_allowed(self):
        payload = {
            "type": "new-message",
            "data": {
                "text": "Hello AGY from authorized handle",
                "handle": {"address": "k.horned@gmail.com"},
                "chats": [{"guid": "iMessage;-;k.horned@gmail.com"}]
            }
        }
        resp = await self.client.post("/bluebubbles-webhook?password=test_secret_password", json=payload)
        self.assertEqual(resp.status, 200)
        res_json = await resp.json()
        self.assertEqual(res_json.get("status"), "ok")

        # Give async background task a brief tick to run
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.dispatched_messages), 1)
        dispatched = self.dispatched_messages[0]
        self.assertEqual(dispatched["sender"], "k.horned@gmail.com")
        self.assertEqual(dispatched["chat_guid"], "iMessage;-;k.horned@gmail.com")
        self.assertEqual(dispatched["text"], "Hello AGY from authorized handle")

    @unittest_run_loop
    async def test_webhook_ignore_from_me(self):
        payload = {
            "type": "new-message",
            "data": {
                "text": "Message sent by the server host",
                "isFromMe": True,
                "handle": {"address": "k.horned@gmail.com"},
                "chats": [{"guid": "iMessage;-;k.horned@gmail.com"}]
            }
        }
        resp = await self.client.post("/bluebubbles-webhook?password=test_secret_password", json=payload)
        self.assertEqual(resp.status, 200)
        res_json = await resp.json()
        self.assertEqual(res_json.get("reason"), "is_from_me")
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.dispatched_messages), 0)

    @unittest_run_loop
    async def test_webhook_chat_guid_prefers_chats_over_message_guid(self):
        """Verify chat_guid is extracted from chats[0]['guid'] rather than message data['guid']."""
        payload = {
            "type": "new-message",
            "data": {
                "guid": "EA4CA9CA-4886-480A-B21E-ACF2FAA68DB8",  # Incoming message UUID
                "text": "!bearings",
                "handle": {"address": "k.horned@gmail.com"},
                "chats": [
                    {
                        "guid": "iMessage;-;k.horned@gmail.com",
                        "chatIdentifier": "k.horned@gmail.com"
                    }
                ]
            }
        }
        resp = await self.client.post("/bluebubbles-webhook?password=test_secret_password", json=payload)
        self.assertEqual(resp.status, 200)
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.dispatched_messages), 1)
        dispatched = self.dispatched_messages[0]
        self.assertEqual(dispatched["chat_guid"], "iMessage;-;k.horned@gmail.com")
        self.assertNotEqual(dispatched["chat_guid"], "EA4CA9CA-4886-480A-B21E-ACF2FAA68DB8")

    @unittest_run_loop
    async def test_webhook_chat_guid_fallback_to_handle(self):
        """Verify chat_guid properly falls back to direct handle GUID when chats list is omitted."""
        payload = {
            "type": "new-message",
            "data": {
                "guid": "B44CA9CA-4886-480A-B21E-ACF2FAA68DB9",
                "text": "Fallback test",
                "handle": {"address": "k.horned@gmail.com"}
            }
        }
        resp = await self.client.post("/bluebubbles-webhook?password=test_secret_password", json=payload)
        self.assertEqual(resp.status, 200)
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.dispatched_messages), 1)
        dispatched = self.dispatched_messages[0]
        self.assertEqual(dispatched["chat_guid"], "iMessage;-;k.horned@gmail.com")


class TestBlueBubblesOutboundAndBridge(unittest.IsolatedAsyncioTestCase):
    """Tests for outbound iMessage delivery, attachment sending, and end-to-end Chief of Staff alignment."""

    async def asyncSetUp(self):
        self.transport = BlueBubblesTransport(
            server_url="http://127.0.0.1:1234",
            password="test_secret_password",
            allowed_handles="k.horned@gmail.com"
        )

    async def asyncTearDown(self):
        await self.transport.stop()

    async def test_send_imessage_outbound_chunking(self):
        """Verify long message is split into chunks and posted sequentially."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json.return_value = {"status": 200, "message": "Success"}

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post.return_value.__aenter__.return_value = mock_resp
        self.transport._session = mock_session

        long_text = "Word " * 900  # ~4500 characters
        res = await self.transport.send_imessage("iMessage;-;k.horned@gmail.com", long_text)
        self.assertEqual(res["status"], 200)
        # Should have called post twice due to chunking
        self.assertEqual(mock_session.post.call_count, 2)
        call_payload = mock_session.post.call_args_list[0][1]["json"]
        self.assertEqual(call_payload.get("method"), "apple-script")

    async def test_send_imessage_outbound_error_logging(self):
        """Verify outbound non-200 responses log status and err_body without crashing."""
        mock_resp = AsyncMock()
        mock_resp.status = 400
        mock_resp.text = AsyncMock(return_value='{"error": "Chat not found"}')

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post.return_value.__aenter__.return_value = mock_resp
        self.transport._session = mock_session

        with patch("bluebubbles_transport.logger.error") as mock_err:
            res = await self.transport.send_imessage("bad-chat-guid", "hello")
            self.assertEqual(res.get("status"), 400)
            mock_err.assert_called_once()
            self.assertIn("BlueBubbles send failed [400]", mock_err.call_args[0][0])

    async def test_send_imessage_attachment(self):
        """Verify outbound attachment calls /api/v1/message/attachment with multipart form."""
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as tf:
            tf.write("Attachment sample content")
            tmp_path = tf.name

        try:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json.return_value = {"status": 200, "message": "Success"}

            mock_session = MagicMock()
            mock_session.closed = False
            mock_session.post.return_value.__aenter__.return_value = mock_resp
            self.transport._session = mock_session

            res = await self.transport.send_imessage_attachment("iMessage;-;k.horned@gmail.com", tmp_path)
            self.assertEqual(res["status"], 200)
            mock_session.post.assert_called_once()
            call_url = mock_session.post.call_args[0][0]
            self.assertIn("/api/v1/message/attachment", call_url)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    async def test_imessage_dispatch_and_agent_start(self):
        """Verify incoming iMessage request creates workspace, starts agy agent, and prompts."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        sender = "k.horned@gmail.com"
        client = self.transport.get_client(chat_guid)
        app.sessions.clear()

        event = {
            "channel": chat_guid,
            "user": sender,
            "text": "build a quick html preview prototype",
            "ts": "2000.001",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        mock_ws_json = json.dumps({
            "result": {
                "workspace": {"workspace_id": "ws-bb-1"},
                "root_pane": {"pane_id": "p-bb-1"}
            }
        })

        cli_calls = []

        async def fake_run_cli(args):
            cli_calls.append(args)
            if args[0] == "workspace" and args[1] == "create":
                return 0, mock_ws_json, ""
            return 0, "", ""

        with patch.object(app.herdr_client, "run_cli", side_effect=fake_run_cli), \
             patch("asyncio.create_task"):

            await app.process_user_prompt(event, client)

            session_key = f"{chat_guid}:{chat_guid}"
            self.assertIn(session_key, app.sessions)
            session = app.sessions[session_key]
            self.assertEqual(session.status, "starting")
            self.assertTrue(session.is_imessage)

            # Verified agent start launched directly with prompt arguments
            start_cmds = [c for c in cli_calls if c[0] == "agent" and c[1] == "start"]
            self.assertEqual(len(start_cmds), 1)
            self.assertEqual(start_cmds[0][2], session.agent_name)
            self.assertIn("--prompt-interactive", start_cmds[0])
            self.assertTrue(any("build a quick html preview prototype" in arg for arg in start_cmds[0]))


    async def test_imessage_approval_relay_with_thumbsup(self):
        """Verify Carl texting 👍 relays 'y enter' to blocked agent PTY stdin."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        sender = "k.horned@gmail.com"
        client = self.transport.get_client(chat_guid)
        session_key = f"{chat_guid}:{chat_guid}"

        app.sessions.clear()
        session = app.GatewaySession(
            session_key=session_key,
            agent_name="fm-bb-test",
            target_cwd="/tmp/sandbox",
            channel=chat_guid,
            thread_ts=chat_guid,
            is_imessage=True
        )
        session.status = "blocked"
        app.sessions[session_key] = session

        event_thumbsup = {
            "channel": chat_guid,
            "user": sender,
            "text": "👍",
            "ts": "2000.002",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        cli_calls = []

        async def fake_run_cli(args):
            cli_calls.append(args)
            return 0, "", ""

        with patch.object(app.herdr_client, "run_cli", side_effect=fake_run_cli), \
             patch.object(self.transport, "send_imessage", new_callable=AsyncMock, return_value={"status": 200}) as mock_send:

            await app.process_user_prompt(event_thumbsup, client)

            # Verified 'y enter' sent to agent
            send_keys_cmds = [c for c in cli_calls if c[0] == "agent" and c[1] == "send-keys"]
            self.assertEqual(len(send_keys_cmds), 1)
            self.assertEqual(send_keys_cmds[0], ["agent", "send-keys", "fm-bb-test", "y", "enter"])
            self.assertEqual(session.status, "working")

            mock_send.assert_called_once()
            _, text = mock_send.call_args[0]
            self.assertIn("Approval relayed", text)

    async def test_imessage_executive_command_bearings(self):
        """Verify '!bearings' command works directly over iMessage."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        client = self.transport.get_client(chat_guid)

        event = {
            "channel": chat_guid,
            "user": "k.horned@gmail.com",
            "text": "!bearings",
            "ts": "2100.001",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        with patch("app.handle_bearings_request", new_callable=AsyncMock) as mock_bearings:
            await app.process_user_prompt(event, client)
            mock_bearings.assert_called_once()
            called_args = mock_bearings.call_args[0]
            self.assertEqual(called_args[0], chat_guid)
            self.assertEqual(called_args[1], chat_guid)

    async def test_imessage_rejection_relay(self):
        """Verify Carl texting 'abort' relays 'n enter' to blocked agent PTY stdin."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        sender = "k.horned@gmail.com"
        client = self.transport.get_client(chat_guid)
        session_key = f"{chat_guid}:{chat_guid}"

        app.sessions.clear()
        session = app.GatewaySession(
            session_key=session_key,
            agent_name="fm-bb-test2",
            target_cwd="/tmp/sandbox",
            channel=chat_guid,
            thread_ts=chat_guid,
            is_imessage=True
        )
        session.status = "blocked"
        app.sessions[session_key] = session

        event_abort = {
            "channel": chat_guid,
            "user": sender,
            "text": "abort",
            "ts": "2000.003",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        cli_calls = []

        async def fake_run_cli(args):
            cli_calls.append(args)
            return 0, "", ""

        with patch.object(app.herdr_client, "run_cli", side_effect=fake_run_cli), \
             patch.object(self.transport, "send_imessage", new_callable=AsyncMock, return_value={"status": 200}) as mock_send:

            await app.process_user_prompt(event_abort, client)

            # Verified 'n enter' sent to agent
            send_keys_cmds = [c for c in cli_calls if c[0] == "agent" and c[1] == "send-keys"]
            self.assertEqual(len(send_keys_cmds), 1)
            self.assertEqual(send_keys_cmds[0], ["agent", "send-keys", "fm-bb-test2", "n", "enter"])
            self.assertEqual(session.status, "idle")

            mock_send.assert_called_once()
            _, text = mock_send.call_args[0]
            self.assertIn("Cancellation relayed", text)

    async def test_imessage_status_inquiry(self):
        """Verify status questions over iMessage query live pane scrollback."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        sender = "k.horned@gmail.com"
        client = self.transport.get_client(chat_guid)
        session_key = f"{chat_guid}:{chat_guid}"

        app.sessions.clear()
        session = app.GatewaySession(
            session_key=session_key,
            agent_name="fm-bb-active",
            target_cwd="/tmp/sandbox",
            channel=chat_guid,
            thread_ts=chat_guid,
            is_imessage=True
        )
        session.status = "working"
        app.sessions[session_key] = session

        event_status = {
            "channel": chat_guid,
            "user": sender,
            "text": "what is it doing? is it stuck?",
            "ts": "2000.004",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        mock_scrollback = "Compiling assets\nServing HTTP on http://127.0.0.1:8080\nReady for input"

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock, return_value=(0, mock_scrollback, "")), \
             patch.object(self.transport, "send_imessage", new_callable=AsyncMock, return_value={"status": 200}) as mock_send:

            await app.process_user_prompt(event_status, client)

            mock_send.assert_called_once()
            _, text = mock_send.call_args[0]
            self.assertIn("Status Update", text)
            self.assertIn("http://127.0.0.1:8080", text)


    async def test_imessage_executive_command_learn(self):
        """Verify '!learn' command bypasses alignment gate over iMessage."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        client = self.transport.get_client(chat_guid)

        event = {
            "channel": chat_guid,
            "user": "k.horned@gmail.com",
            "text": "!learn save node quirk",
            "ts": "2600.001",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        with patch("app.handle_learn_request", new_callable=AsyncMock) as mock_learn:
            await app.process_user_prompt(event, client)
            mock_learn.assert_called_once()
            called_args = mock_learn.call_args[0]
            self.assertEqual(called_args[0], chat_guid)
            self.assertEqual(called_args[3], "save node quirk")

