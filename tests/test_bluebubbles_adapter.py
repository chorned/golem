"""
Unit tests for BlueBubbles iMessage Transport Adapter and unified Chief of Staff workflow.
Tests webhook authorization, sender whitelist, message routing, outbound chunking,
preflight alignment ("Grill-Me") over iMessage, and executive command execution.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import json
import os
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import quote

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
        with tempfile.TemporaryDirectory() as tmp_dir:
            client = BlueBubblesClient(transport, "iMessage;-;k.horned@gmail.com", history_dir=tmp_dir)
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

    def test_sanitize_for_imessage_clean_native_typography(self):
        """Verify asterisks, backticks, and horizontal rules are cleanly normalized without affecting code blocks."""
        raw = (
            "Here is the summary:\n"
            "---\n"
            "*1. Overview*\n"
            "This is *bold*, **strong**, and _italic_ on `hermes`.\n"
            "> Note: Always verify.\n"
            "• Top bullet\n"
            "  - Sub bullet level 1\n"
            "    * Sub bullet level 2\n"
            "```bash\n"
            "echo 'code *block* `preserved`'\n"
            "```\n"
            "Burn is ~$0.26/day and math: 2 * 3 = 6."
        )
        sanitized = sanitize_for_imessage(raw)

        self.assertNotIn("---", sanitized)
        self.assertIn("1. Overview", sanitized)
        self.assertIn("This is bold, strong, and italic on hermes.", sanitized)
        self.assertIn("│ Note: Always verify.", sanitized)
        self.assertIn("• Top bullet", sanitized)
        self.assertIn("  ◦ Sub bullet level 1", sanitized)
        self.assertIn("    ▪ Sub bullet level 2", sanitized)
        self.assertIn("```bash\necho 'code *block* `preserved`'\n```", sanitized)
        self.assertIn("~$0.26/day", sanitized)
        self.assertIn("2 * 3 = 6", sanitized)

    def test_sanitize_for_imessage_carl_economic_briefing(self):
        """Verify Carl's exact economic breakdown briefing converts to clean native typography."""
        raw = (
            "Here is the breakdown of how our economy is doing month-to-date compared to the same window last month.\n\n"
            "On `hermes`, we track two distinct economic pipelines: our *AI / Cloud compute economy* (live) and our *household financial ledger* (SimpleFIN ingestion).\n\n"
            "---\n\n"
            "*1. AI & Cloud Compute Economy (Oct 1–7 vs. Sep 1–7)*\n\n"
            "Our active operational burn across Google Cloud, Gemini APIs, and Hermes runtime sessions:\n\n"
            "• *GCP & Gemini API Spend:* *$2.03* vs. *$0.26* last month (*+$1.77*, a ~7× surge driven by multi-turn agent work).\n"
            "• *GCP Credits Remaining:* *$37.97* of our $40 credit pool remaining (100% of current spend is covered by promotional credits).\n"
            "• *Daily Burn Rate:* Currently *~$0.26/day* (up from ~$0.04/day).\n"
            "• *Token Throughput:*\n"
            "  - *Hermes API Calls:* *49* vs. *12* (+308%)\n"
            "  - *Input Tokens:* *167.4k* vs. *74.0k* (+126%)\n"
            "  - *Output Tokens:* *55.6k* vs. *1.4k* (+3,790%, reflecting deeper reasoning steps)\n"
            "  - *Total Tokens:* *223.0k* vs. *75.4k* (+196%)\n"
            "• *Antigravity Workload:* *350 sessions / 60.1k steps* this month vs. *1 session / 228 steps* early September, reflecting the full transition to AGY as our primary operational hand.\n\n"
            "---\n\n"
            "*2. Household Financial Ledger (`financials.db` / FinDom)*\n\n"
            "• *Data Status:* The automated SimpleFIN ingestion cron (`simplefin_ingest.py` backing `financials.db` and the FinDom UI) paused during cleanup in August. As a result, September and October bank transactions have not synced yet.\n"
            "• *Historical Baseline (Last Active Run — Aug 1–5 vs. Jul 1–5):*\n"
            "  - *Spend / Outflows:* $6,341.26 vs. $18,790.45\n"
            "  - *Income / Inflows:* $3,258.30 vs. $15,937.23\n"
            "  - *Net Cash Flow:* -$3,082.96 vs. -$2,853.22\n\n"
            "---\n\n"
            "*Bottom Line*\n"
            "* Operational AI spending is surging as workload shifts into AGY, but it remains fully cushioned by active GCP credits with $37.97 left in the buffer.\n"
            "* If you meant our personal bank ledger, the sync has been idle since August. Say the word and I can trigger `simplefin_ingest.py` backstage to pull and reconcile September and October transactions into SQLite."
        )

        sanitized = sanitize_for_imessage(raw)

        # 1. Zero raw horizontal rules
        self.assertNotIn("---", sanitized)
        # 2. Zero raw asterisks or backticks outside of potential math
        self.assertNotIn("*AI", sanitized)
        self.assertNotIn("*GCP", sanitized)
        self.assertNotIn("*$2.03*", sanitized)
        self.assertNotIn("`hermes`", sanitized)
        self.assertNotIn("`financials.db`", sanitized)
        self.assertNotIn("`simplefin_ingest.py`", sanitized)
        # 3. Clean headers
        self.assertIn("1. AI & Cloud Compute Economy (Oct 1–7 vs. Sep 1–7)", sanitized)
        self.assertIn("2. Household Financial Ledger (financials.db / FinDom)", sanitized)
        self.assertIn("Bottom Line", sanitized)
        # 4. Clean bullet hierarchies
        self.assertIn("• GCP & Gemini API Spend: $2.03 vs. $0.26 last month (+$1.77, a ~7× surge driven by multi-turn agent work).", sanitized)
        self.assertIn("  ◦ Hermes API Calls: 49 vs. 12 (+308%)", sanitized)
        self.assertIn("  ◦ Spend / Outflows: $6,341.26 vs. $18,790.45", sanitized)
        self.assertIn("• Operational AI spending is surging", sanitized)
        self.assertIn("• If you meant our personal bank ledger", sanitized)


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
        mock_ready_agent = json.dumps({
            "result": {
                "agent": {
                    "interactive_ready": True,
                    "agent_status": "idle"
                }
            }
        })

        cli_calls = []

        async def fake_run_cli(args, timeout=5.0):
            cli_calls.append(args)
            if args[0] == "workspace" and args[1] == "create":
                return 0, mock_ws_json, ""
            elif args[0] == "agent" and args[1] == "get":
                return 0, mock_ready_agent, ""
            return 0, "", ""

        with patch.object(app.herdr_client, "run_cli", side_effect=fake_run_cli), \
             patch("asyncio.create_task"):

            await app.process_user_prompt(event, client)

            session_key = f"{chat_guid}:{chat_guid}"
            self.assertIn(session_key, app.sessions)
            session = app.sessions[session_key]
            self.assertEqual(session.status, "starting")
            self.assertTrue(session.is_imessage)

            # Verified agent start launched cleanly without prompt arguments
            start_cmds = [c for c in cli_calls if c[0] == "agent" and c[1] == "start"]
            self.assertEqual(len(start_cmds), 1)
            self.assertEqual(start_cmds[0][2], session.agent_name)
            self.assertNotIn("--prompt-interactive", start_cmds[0])

            # Verified prompt dispatched via agent prompt after handshake
            prompt_cmds = [c for c in cli_calls if c[0] == "agent" and c[1] == "prompt"]
            self.assertEqual(len(prompt_cmds), 1)
            self.assertEqual(prompt_cmds[0][2], session.agent_name)
            self.assertTrue(any("build a quick html preview prototype" in arg for arg in prompt_cmds[0]))


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


class TestBlueBubblesPrivateAPILifecycle(unittest.IsolatedAsyncioTestCase):
    """
    Comprehensive verification for BlueBubbles Private API (IMCore) conversational UX:
    1. Read receipts: POST /api/v1/chat/:chatGuid/read
    2. Typing indicators: POST/DELETE /api/v1/chat/:chatGuid/typing
    3. Native Tapbacks: POST /api/v1/message/react (emphasize, like, question)
    4. Inline Thread Replies: selectedMessageGuid routing
    5. In-place Edits: POST /api/v1/message/:guid/edit with backwardsCompatibilityText
    6. Focus Bypass & Bubble Effects: effectId and forceNotify
    7. Client Shims: chat_update, reactions_add, assistant_threads_setStatus
    8. Webhook Ingress: immediate read mark, emphasize tapback, typing indicator
    9. Fail-safe timeout & AppleScript fallback
    """

    async def asyncSetUp(self):
        self.transport = BlueBubblesTransport(
            server_url="http://127.0.0.1:1234",
            password="test_secret_password",
            allowed_handles="k.horned@gmail.com"
        )
        self.transport._private_api_supported = True
        self.transport._last_private_api_check = time.time()
        self.chat_guid = "iMessage;-;k.horned@gmail.com"
        self.client = self.transport.get_client(self.chat_guid)

    async def asyncTearDown(self):
        await self.transport.stop()

    async def test_set_chat_read(self):
        """Verify set_chat_read calls POST /api/v1/chat/:chatGuid/read."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json.return_value = {"status": 200, "message": "Success"}

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post.return_value.__aenter__.return_value = mock_resp
        self.transport._session = mock_session

        res = await self.transport.set_chat_read(self.chat_guid)
        self.assertIsNone(res)

        mock_session.post.assert_called_once()
        call_url = mock_session.post.call_args[0][0]
        self.assertIn(f"/api/v1/chat/{quote(self.chat_guid, safe='')}/read", call_url)

    async def test_set_typing_indicator_start_and_stop(self):
        """Verify set_typing_indicator starts via POST and stops via DELETE."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json.return_value = {"status": 200, "message": "Success"}

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post.return_value.__aenter__.return_value = mock_resp
        mock_session.delete.return_value.__aenter__.return_value = mock_resp
        self.transport._session = mock_session

        # Start typing
        res_start = await self.transport.set_typing_indicator(self.chat_guid, True)
        self.assertIsNone(res_start)
        mock_session.post.assert_called_once()
        self.assertIn(f"/api/v1/chat/{quote(self.chat_guid, safe='')}/typing", mock_session.post.call_args[0][0])

        # Stop typing
        res_stop = await self.transport.set_typing_indicator(self.chat_guid, False)
        self.assertIsNone(res_stop)
        mock_session.delete.assert_called_once()
        self.assertIn(f"/api/v1/chat/{quote(self.chat_guid, safe='')}/typing", mock_session.delete.call_args[0][0])

    async def test_send_reaction_spec(self):
        """Verify send_reaction dispatches POST /api/v1/message/react with proper schema."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json.return_value = {"status": 200, "message": "Success"}

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post.return_value.__aenter__.return_value = mock_resp
        self.transport._session = mock_session

        res = await self.transport.send_reaction(self.chat_guid, "msg-guid-123", "emphasize", part_index=0)
        self.assertIsNone(res)

        mock_session.post.assert_called_once()
        called_url, called_kwargs = mock_session.post.call_args[0][0], mock_session.post.call_args[1]
        self.assertIn("/api/v1/message/react", called_url)
        payload = called_kwargs.get("json", {})
        self.assertEqual(payload.get("chatGuid"), self.chat_guid)
        self.assertEqual(payload.get("selectedMessageGuid"), "msg-guid-123")
        self.assertEqual(payload.get("reaction"), "emphasize")
        self.assertEqual(payload.get("reactionType"), "emphasize")
        self.assertEqual(payload.get("partIndex"), 0)

    async def test_edit_message_spec(self):
        """Verify edit_message sends both new and backwards compatibility keys."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json.return_value = {"status": 200, "message": "Success"}

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post.return_value.__aenter__.return_value = mock_resp
        self.transport._session = mock_session

        res = await self.transport.edit_message("msg-guid-456", "Updated text", backwards_text="Legacy text")
        self.assertIsNone(res)

        mock_session.post.assert_called_once()
        called_url, called_kwargs = mock_session.post.call_args[0][0], mock_session.post.call_args[1]
        self.assertIn("/api/v1/message/msg-guid-456/edit", called_url)
        payload = called_kwargs.get("json", {})
        self.assertEqual(payload.get("editedMessage"), "Updated text")
        self.assertEqual(payload.get("newText"), "Updated text")
        self.assertEqual(payload.get("backwardsCompatibilityMessage"), "Legacy text")
        self.assertEqual(payload.get("backwardsCompatibilityText"), "Legacy text")
        self.assertEqual(payload.get("partIndex"), 0)

    async def test_send_message_inline_thread_reply_and_effects(self):
        """Verify send_message returns dispatched GUID, sets selectedMessageGuid, and supports effects."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json.return_value = {
            "status": 200,
            "data": {"guid": "dispatched-guid-999"}
        }

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post.return_value.__aenter__.return_value = mock_resp
        self.transport._session = mock_session

        guid = await self.transport.send_message(
            self.chat_guid,
            "Urgent alert reply",
            reply_to_guid="parent-bubble-001",
            effect_id="slam",
            force_notify=True
        )
        self.assertEqual(guid, "dispatched-guid-999")

        mock_session.post.assert_called_once()
        called_kwargs = mock_session.post.call_args[1]
        payload = called_kwargs.get("json", {})
        self.assertEqual(payload.get("chatGuid"), self.chat_guid)
        self.assertEqual(payload.get("message"), "Urgent alert reply")
        self.assertEqual(payload.get("selectedMessageGuid"), "parent-bubble-001")
        self.assertEqual(payload.get("partIndex"), 0)
        self.assertEqual(payload.get("effectId"), "slam")
        self.assertTrue(payload.get("forceNotify"))

    async def test_client_chat_update(self):
        """Verify BlueBubblesClient.chat_update invokes transport.edit_message."""
        with patch.object(self.transport, "edit_message", new_callable=AsyncMock) as mock_edit:
            mock_edit.return_value = None
            res = await self.client.chat_update(
                channel=self.chat_guid,
                ts="msg-guid-111",
                text="In-place edited content"
            )
            self.assertTrue(res.get("ok"))
            mock_edit.assert_called_once_with(
                message_guid="msg-guid-111",
                new_text="In-place edited content",
                backwards_text="In-place edited content",
                part_index=0
            )

    async def test_client_reactions_add_slack_mapping(self):
        """Verify BlueBubblesClient.reactions_add maps Slack emoji names to native tapbacks."""
        with patch.object(self.transport, "send_reaction", new_callable=AsyncMock) as mock_react:
            mock_react.return_value = None

            # Slack :gear: -> BlueBubbles emphasize
            await self.client.reactions_add(name="gear", channel=self.chat_guid, timestamp="msg-001")
            mock_react.assert_called_with(chat_guid=self.chat_guid, message_guid="msg-001", reaction_type="emphasize")

            # Slack :white_check_mark: -> BlueBubbles like
            await self.client.reactions_add(name="white_check_mark", channel=self.chat_guid, timestamp="msg-001")
            mock_react.assert_called_with(chat_guid=self.chat_guid, message_guid="msg-001", reaction_type="like")

            # Slack :x: -> BlueBubbles question
            await self.client.reactions_add(name="x", channel=self.chat_guid, timestamp="msg-001")
            mock_react.assert_called_with(chat_guid=self.chat_guid, message_guid="msg-001", reaction_type="question")

            # Slack :warning: -> BlueBubbles question
            await self.client.reactions_add(name="warning", channel=self.chat_guid, timestamp="msg-001")
            mock_react.assert_called_with(chat_guid=self.chat_guid, message_guid="msg-001", reaction_type="question")

    async def test_client_assistant_threads_setStatus(self):
        """Verify BlueBubblesClient.assistant_threads_setStatus manages typing indicator."""
        with patch.object(self.transport, "set_typing_indicator", new_callable=AsyncMock) as mock_typing:
            mock_typing.return_value = None

            # Set status -> typing True
            res = await self.client.assistant_threads_setStatus(
                channel=self.chat_guid,
                thread_ts=self.chat_guid,
                status="Thinking..."
            )
            self.assertTrue(res.get("ok"))
            mock_typing.assert_called_with(chat_guid=self.chat_guid, active=True)

            # Clear status -> typing False
            res2 = await self.client.assistant_threads_setStatus(
                channel=self.chat_guid,
                thread_ts=self.chat_guid,
                status=""
            )
            self.assertTrue(res2.get("ok"))
            mock_typing.assert_called_with(chat_guid=self.chat_guid, active=False)

    async def test_webhook_ingress_lifecycle_triggers(self):
        """Verify inbound message triggers read mark, emphasize tapback, and typing indicator."""
        with patch.object(self.transport, "set_chat_read", new_callable=AsyncMock) as mock_read, \
             patch.object(self.transport, "send_reaction", new_callable=AsyncMock) as mock_react, \
             patch.object(self.transport, "set_typing_indicator", new_callable=AsyncMock) as mock_typing:

            mock_read.return_value = None
            mock_react.return_value = None
            mock_typing.return_value = None

            mock_request = AsyncMock()
            mock_request.query = {"password": "test_secret_password"}
            mock_request.headers = {}
            mock_request.json.return_value = {
                "type": "new-message",
                "data": {
                    "guid": "inbound-msg-guid-888",
                    "text": "Hello Golem",
                    "handle": {"address": "k.horned@gmail.com"},
                    "chats": [{"guid": self.chat_guid}],
                    "isFromMe": False
                }
            }

            resp = await self.transport._handle_webhook(mock_request)
            self.assertEqual(resp.status, 200)

            await asyncio.sleep(0.05)

            mock_read.assert_called_once_with(self.chat_guid)
            mock_react.assert_called_once_with(self.chat_guid, "inbound-msg-guid-888", "emphasize")
            mock_typing.assert_called_once_with(self.chat_guid, True)

    async def test_private_api_failure_fallback_applescript(self):
        """Verify fallback to apple-script method when Private API is unavailable."""
        mock_resp_fail = AsyncMock()
        mock_resp_fail.status = 500
        mock_resp_fail.text.return_value = "Helper disconnected"

        mock_resp_success = AsyncMock()
        mock_resp_success.status = 200
        mock_resp_success.json.return_value = {"status": 200, "message": "Success"}

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post.return_value.__aenter__.side_effect = [mock_resp_fail, mock_resp_success]
        self.transport._session = mock_session

        res = await self.transport.send_imessage(self.chat_guid, "Fallback text message")
        self.assertEqual(res.get("status"), 200)
        self.assertEqual(mock_session.post.call_count, 2)
        second_call_payload = mock_session.post.call_args_list[1][1].get("json", {})
        self.assertEqual(second_call_payload.get("method"), "apple-script")

    async def test_inbound_imessage_attachment_extraction_and_staging(self):
        """Verify inbound iMessage attachments are extracted into event['files'] and staged."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_img = os.path.join(tmp_dir, "incoming_photo.jpg")
            with open(test_img, "wb") as f:
                f.write(b"\xff\xd8\xff\xe0test_jpeg_bytes")

            raw_payload = {
                "type": "new-message",
                "data": {
                    "guid": "msg-guid-img-001",
                    "text": "(attachment)",
                    "handle": {"address": "carl@example.com"},
                    "chats": [{"guid": "iMessage;-;carl@example.com"}],
                    "attachments": [
                        {
                            "guid": "att-guid-001",
                            "transferName": "incoming_photo.jpg",
                            "mimeType": "image/jpeg",
                            "totalBytes": len(b"\xff\xd8\xff\xe0test_jpeg_bytes"),
                            "filePath": test_img,
                        }
                    ],
                }
            }

            captured_events = []

            async def fake_handle_message(event, client):
                captured_events.append(event)

            with patch("app.USE_HEADLESS_GATEWAY", True), \
                 patch("app.bb_transport", self.transport), \
                 patch.object(app.ingress_gateway, "handle_message", side_effect=fake_handle_message):
                await app.handle_bluebubbles_inbound_message(
                    chat_guid="iMessage;-;carl@example.com",
                    text="(attachment)",
                    sender="carl@example.com",
                    raw_payload=raw_payload,
                )

            self.assertEqual(len(captured_events), 1)
            event = captured_events[0]
            self.assertTrue(event["is_imessage"])
            self.assertEqual(len(event["files"]), 1)
            file_meta = event["files"][0]
            self.assertEqual(file_meta["name"], "incoming_photo.jpg")
            self.assertEqual(file_meta["local_path"], test_img)
            self.assertTrue(file_meta["is_imessage"])

            # Verify staging via IngressGateway
            staged = await app.ingress_gateway.stage_inbound_files(
                event["files"],
                target_cwd=tmp_dir,
                client=self.transport.get_client("iMessage;-;carl@example.com")
            )
            self.assertEqual(len(staged), 1)
            self.assertEqual(staged[0]["status"], "staged")
            self.assertTrue(os.path.exists(staged[0]["path"]))
            with open(staged[0]["path"], "rb") as sf:
                self.assertEqual(sf.read(), b"\xff\xd8\xff\xe0test_jpeg_bytes")

    async def test_bluebubbles_client_files_upload_v2_and_mimetype(self):
        """Verify BlueBubblesClient.files_upload_v2 correctly uploads image attachments with Private API."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_img = os.path.join(tmp_dir, "generated_art.png")
            with open(test_img, "wb") as f:
                f.write(b"\x89PNG\r\n\x1a\nfake_png_data")

            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json.return_value = {"status": 200, "message": "Success", "data": {"guid": "att-send-123"}}

            mock_session = MagicMock()
            mock_session.closed = False
            mock_session.post.return_value.__aenter__.return_value = mock_resp
            self.transport._session = mock_session
            self.transport._private_api_supported = True

            client = self.transport.get_client(self.chat_guid)
            res = await client.files_upload_v2(
                channel=self.chat_guid,
                file=test_img,
                filename="generated_art.png",
                title="Generated Image",
            )
            self.assertTrue(res.get("ok"))
            mock_session.post.assert_called_once()
            call_url = mock_session.post.call_args[0][0]
            self.assertIn("/api/v1/message/attachment", call_url)

    async def test_bluebubbles_transport_download_attachment(self):
        """Verify download_attachment fetches binary attachment data via REST endpoint."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b"downloaded_bytes_12345"

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.get.return_value.__aenter__.return_value = mock_resp
        self.transport._session = mock_session

        data = await self.transport.download_attachment("att-guid-999")
        self.assertEqual(data, b"downloaded_bytes_12345")
        mock_session.get.assert_called_once()
        call_url = mock_session.get.call_args[0][0]
        self.assertIn("/api/v1/attachment/att-guid-999/download", call_url)

    def test_bluebubbles_client_history_persistence(self):
        """Verify BlueBubblesClient persists chat history to disk and reloads on new client instance."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            chat_guid = "iMessage;-;test.user@example.com"
            mock_transport = MagicMock()
            client1 = BlueBubblesClient(mock_transport, chat_guid, history_dir=tmp_dir)
            client1.record_incoming("test.user@example.com", "Can you generate a graph of the finances?", "ts-001", "guid-001")

            async def _post():
                await client1.chat_postMessage(channel=chat_guid, text="Here is a visual breakdown:\nAug $3,198 vs Oct $2,677")

            asyncio.run(_post())
            self.assertEqual(len(client1.history), 2)

            # Create fresh client for the same chat_guid and verify history reloads from disk
            client2 = BlueBubblesClient(mock_transport, chat_guid, history_dir=tmp_dir)
            self.assertEqual(len(client2.history), 2)
            self.assertEqual(client2.history[0]["text"], "Can you generate a graph of the finances?")
            self.assertEqual(client2.history[1]["text"], "Here is a visual breakdown:\nAug $3,198 vs Oct $2,677")
            self.assertEqual(client2.history[1]["bot_id"], "B_AGY")

    async def test_bluebubbles_client_conversations_replies_server_fallback(self):
        """Verify conversations_replies falls back to transport.fetch_chat_messages when history is sparse."""
        chat_guid = "iMessage;-;test.user@example.com"
        mock_transport = MagicMock()
        mock_transport.fetch_chat_messages = AsyncMock(return_value=[
            {"text": "Turn 1 request", "ts": "g-1", "guid": "g-1", "user": "test.user@example.com"},
            {"text": "Turn 1 response", "ts": "g-2", "guid": "g-2", "bot_id": "B_AGY"},
        ])
        with tempfile.TemporaryDirectory() as tmp_dir:
            client = BlueBubblesClient(mock_transport, chat_guid, history_dir=tmp_dir)
            self.assertEqual(len(client.history), 0)

            res = await client.conversations_replies(chat_guid)
            self.assertEqual(len(res["messages"]), 2)
            self.assertEqual(res["messages"][0]["text"], "Turn 1 request")
            self.assertEqual(res["messages"][1]["text"], "Turn 1 response")
            mock_transport.fetch_chat_messages.assert_called_once_with(chat_guid, limit=25)

    async def test_ingress_imessage_thread_context_rehydration(self):
        """Verify IngressGateway rehydrates iMessage chat context into prompt_to_send."""
        from golem.gateway.ingress import IngressGateway

        mock_router = MagicMock()
        gateway = IngressGateway(router=mock_router)

        chat_guid = "iMessage;-;k.horned@gmail.com"
        mock_client = MagicMock()
        mock_client.conversations_replies = AsyncMock(return_value={
            "ok": True,
            "messages": [
                {"ts": "100.1", "user": "k.horned@gmail.com", "text": "Can you generate a graph of the finances?"},
                {"ts": "100.2", "bot_id": "B_AGY", "text": "Here is a breakdown:\nAug $3198 vs Oct $2677"},
            ]
        })

        ctx = await gateway._fetch_thread_context(
            mock_client, channel=chat_guid, thread_ts=chat_guid, exclude_ts="100.3", is_imessage=True
        )

        self.assertIn("[Previous iMessage Conversation Context:", ctx)
        self.assertIn("User: Can you generate a graph of the finances?", ctx)
        self.assertIn("Hermes: Here is a breakdown:\nAug $3198 vs Oct $2677", ctx)

    def test_bluebubbles_client_clear_history(self):
        """Verify client.clear_history wipes memory and disk persistence."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            chat_guid = "iMessage;-;test.user@example.com"
            mock_transport = MagicMock()
            client = BlueBubblesClient(mock_transport, chat_guid, history_dir=tmp_dir)
            client.record_incoming("test.user@example.com", "Hello world", "ts-1", "guid-1")
            self.assertEqual(len(client.history), 1)

            client.clear_history()
            self.assertEqual(len(client.history), 0)

            # Reload client from disk to verify persistence was also cleared
            client2 = BlueBubblesClient(mock_transport, chat_guid, history_dir=tmp_dir)
            self.assertEqual(len(client2.history), 0)

    async def test_bluebubbles_client_thread_originator_guid_filtering(self):
        """Verify conversations_replies filters by thread_originator_guid when given an explicit thread GUID."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            chat_guid = "iMessage;-;test.user@example.com"
            mock_transport = MagicMock()
            client = BlueBubblesClient(mock_transport, chat_guid, history_dir=tmp_dir)

            # Record a main chat message
            client.record_incoming("test.user@example.com", "Main chat 1", "ts-main-1", "guid-main-1")

            # Record a root thread message and a reply
            client.record_incoming("test.user@example.com", "Thread root question", "ts-t1", "guid-parent-1")
            client.record_incoming(
                "test.user@example.com",
                "Inline reply to thread",
                "ts-t2",
                "guid-reply-1",
                thread_originator_guid="guid-parent-1",
            )

            # Query without thread ts (or with channel) -> returns all messages
            all_res = await client.conversations_replies(chat_guid)
            self.assertEqual(len(all_res["messages"]), 3)

            # Query with specific thread ts -> returns only messages in that thread
            thread_res = await client.conversations_replies(chat_guid, ts="guid-parent-1")
            self.assertEqual(len(thread_res["messages"]), 2)
            self.assertEqual(thread_res["messages"][0]["text"], "Thread root question")
            self.assertEqual(thread_res["messages"][1]["text"], "Inline reply to thread")

    async def test_ingress_new_command_resets_session(self):
        """Verify /new command tears down router session, clears iMessage history, and posts reset confirmation."""
        from golem.gateway.ingress import IngressGateway

        mock_router = MagicMock()
        mock_router.teardown = AsyncMock()
        gateway = IngressGateway(router=mock_router)

        chat_guid = "iMessage;-;test.user@example.com"
        mock_client = MagicMock()
        mock_client.clear_history = MagicMock()
        mock_client.chat_postMessage = AsyncMock()

        event = {
            "channel": chat_guid,
            "user": "test.user@example.com",
            "text": "/new",
            "ts": "ts-new-001",
            "thread_ts": chat_guid,
            "is_imessage": True,
        }

        await gateway.handle_message(event, mock_client)

        mock_router.teardown.assert_called_once_with(chat_guid, chat_guid, reason="reset")
        mock_client.clear_history.assert_called_once()
        mock_client.chat_postMessage.assert_called_once()
        sent_text = mock_client.chat_postMessage.call_args[1].get("text") or mock_client.chat_postMessage.call_args[0][1]
        self.assertIn("New Session", sent_text)

    def test_bluebubbles_client_resolve_thread_root(self):
        """Verify resolve_thread_root traverses parent chain to root message."""
        mock_transport = MagicMock()
        client = BlueBubblesClient(mock_transport, "iMessage;-;test@example.com")
        client.record_incoming("test@example.com", "Root message", "ts-root", "guid-root")
        client.record_incoming("test@example.com", "Reply 1", "ts-r1", "guid-r1", thread_originator_guid="guid-root")
        client.record_incoming("test@example.com", "Reply 2", "ts-r2", "guid-r2", thread_originator_guid="guid-r1")

        self.assertEqual(client.resolve_thread_root("guid-root"), "guid-root")
        self.assertEqual(client.resolve_thread_root("guid-r1"), "guid-root")
        self.assertEqual(client.resolve_thread_root("guid-r2"), "guid-root")

    async def test_model_a_top_level_vs_thread_reply_context(self):
        """Verify Model A skips thread context for top-level messages and rehydrates for inline thread replies."""
        from golem.gateway.ingress import IngressGateway

        mock_router = MagicMock()
        mock_session = MagicMock()
        mock_session.is_busy = False
        mock_session.is_executing = False
        mock_session.runner_pane = None
        mock_session.adapter = MagicMock()
        mock_session.adapter.run_turn = AsyncMock(return_value={"status": "SUCCESS", "output_tail": "Done"})
        mock_router.get_or_create = AsyncMock(return_value=(mock_session, True))

        gateway = IngressGateway(router=mock_router)
        gateway._fetch_thread_context = AsyncMock(return_value="[Previous iMessage Conversation Context:\nUser: Old context\n]")
        gateway._handle_mouth_turn = AsyncMock(return_value="Briefing response")

        mock_client = MagicMock()
        mock_client.chat_postMessage = AsyncMock()

        # 1. Top-level message (thread_ts == ts) -> is_in_thread is False -> _fetch_thread_context NOT called
        top_event = {
            "channel": "iMessage;-;test@example.com",
            "user": "test@example.com",
            "text": "Start a brand new topic",
            "ts": "guid-top-001",
            "thread_ts": "guid-top-001",
            "is_imessage": True,
        }
        await gateway.handle_message(top_event, mock_client)
        gateway._fetch_thread_context.assert_not_called()

        # 2. Inline thread reply (thread_ts != ts) -> is_in_thread is True -> _fetch_thread_context IS called
        reply_event = {
            "channel": "iMessage;-;test@example.com",
            "user": "test@example.com",
            "text": "Follow up on this topic",
            "ts": "guid-reply-002",
            "thread_ts": "guid-top-001",
            "is_imessage": True,
        }
        await gateway.handle_message(reply_event, mock_client)
        gateway._fetch_thread_context.assert_called_once()



