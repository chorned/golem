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

    async def test_imessage_preflight_alignment_and_go_approval(self):
        """Verify incoming iMessage request triggers Pre-Flight Alignment, and 'go' triggers execution."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        sender = "k.horned@gmail.com"
        session_key = f"{chat_guid}:{chat_guid}"

        app.pending_alignment.clear()
        app.pending_alignment_by_msg_ts.clear()

        # Step 1: User sends initial request over iMessage
        event_init = {
            "channel": chat_guid,
            "user": sender,
            "text": "Check disk space and Docker containers on hermes",
            "ts": "2000.001",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        mock_plan = {
            "intent_summary": "inspect storage and containers on hermes",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "df -h / && docker ps",
            "assumptions": "read-only audit of hermes host",
            "choice": "shallow vs deep inspection",
            "recommendation": "inspect root disk and containers",
            "estimated_duration_sec": 15
        }

        client = self.transport.get_client(chat_guid)

        with patch.object(app.fleet, "execute_task", new_callable=AsyncMock) as mock_exec, \
             patch("app.generate_preflight_plan", new_callable=AsyncMock, return_value=mock_plan), \
             patch.object(self.transport, "send_imessage", new_callable=AsyncMock, return_value={"status": 200}) as mock_send:

            await app.process_user_prompt(event_init, client)

            # 1. Backstage task not executed yet
            mock_exec.assert_not_called()

            # 2. Alignment briefing sent over iMessage
            mock_send.assert_called_once()
            called_guid, called_msg = mock_send.call_args[0]
            self.assertEqual(called_guid, chat_guid)
            self.assertIn("On it, Carl.", called_msg)
            self.assertIn("The best agent for this is *Hermes Butler*", called_msg)
            self.assertIn("df -h / && docker ps", called_msg)
            self.assertIn("Hit me with `go` (or 👍)", called_msg)

            # 3. State latched
            self.assertIn(session_key, app.pending_alignment)

        # Step 2: User approves with 'go'
        event_go = {
            "channel": chat_guid,
            "user": sender,
            "text": "go",
            "ts": "2000.002",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        mock_task_res = {
            "task_id": "t_bb_go",
            "exit_code": 0,
            "stdout": "Filesystem Size Used Avail Use% Mounted on\n/dev/disk3s1 460G 20G 440G 5% /",
            "stderr": "",
            "output": "Filesystem Size Used Avail Use% Mounted on\n/dev/disk3s1 460G 20G 440G 5% /",
            "blocked": False
        }

        with patch.object(app.fleet, "execute_task", new_callable=AsyncMock, return_value=mock_task_res) as mock_exec_go, \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value="*Briefing:* Root disk is 5% used and Docker is healthy."), \
             patch.object(self.transport, "send_imessage", new_callable=AsyncMock, return_value={"status": 200}) as mock_send_final:

            await app.process_user_prompt(event_go, client)

            # 1. Plan popped
            self.assertNotIn(session_key, app.pending_alignment)

            # 2. Dispatched planned payload (NOT English text)
            mock_exec_go.assert_called_once()
            called_instruction = mock_exec_go.call_args[1].get("instruction") or mock_exec_go.call_args[0][1]
            self.assertEqual(called_instruction, "df -h / && docker ps")

            # 3. Final executive briefing sent to iMessage
            mock_send_final.assert_called_once()
            _, final_text = mock_send_final.call_args[0]
            self.assertIn("Root disk is 5% used and Docker is healthy.", final_text)

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
            called_kw = mock_bearings.call_args[1]
            self.assertEqual(called_kw["channel"], chat_guid)
            self.assertEqual(called_kw["client"], client)

    async def test_imessage_tweak_alignment(self):
        """Verify sending a tweak regenerates alignment plan and sends updated briefing."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        session_key = f"{chat_guid}:{chat_guid}"
        client = self.transport.get_client(chat_guid)

        mock_plan = {
            "intent_summary": "audit root disk",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "df -h /",
            "assumptions": "read-only audit",
            "choice": "root only vs all",
            "recommendation": "check root",
            "estimated_duration_sec": 15
        }
        app.pending_alignment[session_key] = {
            "plan": mock_plan,
            "target_info": {
                "workspace_label": "homelab-ops",
                "target_machine": "local",
                "role": "Hermes Butler",
                "pane_id": "w2:p1"
            },
            "channel": chat_guid,
            "thread_ts": chat_guid,
            "alignment_msg_ts": "2200.002",
            "original_prompt": "Check disk"
        }

        mock_updated_plan = {
            "intent_summary": "audit root disk and ZFS pools",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "df -h / && zpool list && zfs list",
            "assumptions": "read-only audit",
            "choice": "root only vs all",
            "recommendation": "check root and ZFS",
            "estimated_duration_sec": 15
        }

        event_tweak = {
            "channel": chat_guid,
            "user": "k.horned@gmail.com",
            "text": "include ZFS as well",
            "ts": "2200.003",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        with patch.object(app.fleet, "execute_task", new_callable=AsyncMock) as mock_exec, \
             patch("app.generate_preflight_plan", new_callable=AsyncMock, return_value=mock_updated_plan), \
             patch.object(self.transport, "send_imessage", new_callable=AsyncMock, return_value={"status": 200}) as mock_send:

            await app.process_user_prompt(event_tweak, client)

            # Execution not run
            mock_exec.assert_not_called()

            # Updated plan stored
            self.assertIn(session_key, app.pending_alignment)
            self.assertEqual(app.pending_alignment[session_key]["plan"]["dispatched_payload"], "df -h / && zpool list && zfs list")

            # Updated briefing sent
            mock_send.assert_called_once()
            _, briefing_text = mock_send.call_args[0]
            self.assertIn("df -h / && zpool list && zfs list", briefing_text)

    async def test_imessage_conversational_turn(self):
        """Verify conversational question over iMessage returns direct answer without alignment card."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        client = self.transport.get_client(chat_guid)
        session_key = f"{chat_guid}:{chat_guid}"
        app.pending_alignment.clear()

        event = {
            "channel": chat_guid,
            "user": "k.horned@gmail.com",
            "text": "Well it was working in the previous prompt right? What happened?",
            "ts": "2300.001",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        with patch.object(app.fleet, "execute_task", new_callable=AsyncMock) as mock_exec, \
             patch.object(self.transport, "send_imessage", new_callable=AsyncMock, return_value={"status": 200}) as mock_send:

            await app.process_user_prompt(event, client)

            mock_exec.assert_not_called()
            self.assertNotIn(session_key, app.pending_alignment)
            mock_send.assert_called_once()
            called_guid, reply_text = mock_send.call_args[0]
            self.assertEqual(called_guid, chat_guid)
            self.assertNotIn("On it, Carl.", reply_text)
            self.assertNotIn("Here is what I'm running:", reply_text)
            self.assertTrue(len(reply_text) > 10)

    async def test_imessage_first_person_alignment_on_hermes(self):
        """Verify local tasks on Hermes over iMessage format first-person alignment card."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        client = self.transport.get_client(chat_guid)
        session_key = f"{chat_guid}:{chat_guid}"
        app.pending_alignment.clear()

        event = {
            "channel": chat_guid,
            "user": "k.horned@gmail.com",
            "text": "Run python test suite and generate coverage",
            "ts": "2400.001",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        with patch.object(app.fleet, "execute_task", new_callable=AsyncMock) as mock_exec, \
             patch.object(self.transport, "send_imessage", new_callable=AsyncMock, return_value={"status": 200}) as mock_send:

            await app.process_user_prompt(event, client)

            mock_exec.assert_not_called()
            self.assertIn(session_key, app.pending_alignment)
            mock_send.assert_called_once()
            _, briefing = mock_send.call_args[0]
            self.assertIn("On it, Carl.", briefing)
            self.assertIn("I'll take care of this directly on *hermes* (`localhost`).", briefing)
            self.assertIn("*Here is what I'm running:*", briefing)
            self.assertNotIn("The best agent for this is", briefing)
            self.assertIn("⤷ *_Recommendation (Default if you just say `go`):_*", briefing)
            self.assertIn("Hit me with `go` (or 👍)", briefing)

    async def test_imessage_approval_with_thumbsup_text(self):
        """Verify Carl texting '👍' approves pending plan and dispatches task."""
        chat_guid = "iMessage;-;k.horned@gmail.com"
        client = self.transport.get_client(chat_guid)
        session_key = f"{chat_guid}:{chat_guid}"

        mock_plan = {
            "intent_summary": "inspect git status",
            "target_node": "hermes",
            "delegated_worker": "Local AGY",
            "domain": "local development",
            "dispatched_payload": "git status -s",
            "assumptions": "clean repo",
            "choice": "quick vs full",
            "recommendation": "quick status",
            "estimated_duration_sec": 5,
            "is_self": True
        }
        app.pending_alignment[session_key] = {
            "plan": mock_plan,
            "target_info": {"workspace_label": "fleet-control", "target_machine": "local", "role": "Chief of Staff"},
            "channel": chat_guid,
            "thread_ts": chat_guid,
            "alignment_msg_ts": "2500.002",
            "original_prompt": "check git"
        }

        event = {
            "channel": chat_guid,
            "user": "k.horned@gmail.com",
            "text": "👍",
            "ts": "2500.003",
            "thread_ts": chat_guid,
            "is_imessage": True
        }

        mock_task_res = {
            "task_id": "t_bb_thumbsup",
            "exit_code": 0,
            "stdout": "Clean tree",
            "stderr": "",
            "output": "Clean tree",
            "blocked": False
        }

        with patch.object(app.fleet, "execute_task", new_callable=AsyncMock, return_value=mock_task_res) as mock_exec, \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value="*Briefing:* Repository is clean."), \
             patch.object(self.transport, "send_imessage", new_callable=AsyncMock, return_value={"status": 200}) as mock_send:

            await app.process_user_prompt(event, client)

            self.assertNotIn(session_key, app.pending_alignment)
            mock_exec.assert_called_once()
            called_cmd = mock_exec.call_args[1].get("instruction") or mock_exec.call_args[0][1]
            self.assertEqual(called_cmd, "git status -s")

            mock_send.assert_called_once()
            _, briefing = mock_send.call_args[0]
            self.assertIn("Repository is clean.", briefing)

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
            called_kw = mock_learn.call_args[1]
            self.assertEqual(called_kw["channel"], chat_guid)
            self.assertEqual(called_kw["user_notes"], "save node quirk")
            self.assertEqual(called_kw["client"], client)

