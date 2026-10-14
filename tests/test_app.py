#!/usr/bin/env python3
"""
tests/test_app.py
Comprehensive unit tests for First Mate Thin Ingress Gateway:
1. Target Directory Resolution:
   - Specific project names route to ~/projects/<name>.
   - Generic spikes, demos, and prototypes route to ~/projects/sandbox/{task_id}.
2. Server Command Scoping:
   - Scopes http.server commands with --directory <sandbox_cwd>.
   - Never allows processes to spawn from root /.
3. Chat Formatting Normalizer for BlueBubbles:
   - Converts Slack links and shortcodes to clean URLs and native Unicode emojis.
4. Thin Dispatch to Herdr Runtime:
   - Automatically provisions Herdr workspace and starts Antigravity CLI (agy).
   - Zero keyword regex gates for task execution.
5. Interactive Approval Relay:
   - Relays 'y enter' on user approval (👍 / "approve" / "go").
   - Relays 'n enter' on user rejection ("abort" / "no" / "cancel").
6. Grounded Status Inquiries:
   - Reads live terminal scrollback via herdr agent read and extracts running URLs.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app
from app import (
    resolve_target_directory,
    scope_server_commands,
    normalize_for_bluebubbles,
    is_status_inquiry,
    is_approval_reply,
    is_rejection_reply,
    process_user_prompt,
    handle_reaction_added,
    GatewaySession,
    sessions,
    HOST_DIR,
    DEFAULT_TARGET_REPO
)
from golem.gateway import SessionState


class TestThinIngressGateway(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        sessions.clear()
        self.env_patcher = patch.dict(os.environ, {"DUAL_PANE_WORKSPACE": "0"})
        self.env_patcher.start()

    def tearDown(self):
        sessions.clear()
        self.env_patcher.stop()

    def test_target_directory_resolution_for_named_projects(self):
        """Verify prompt mentioning a specific project in ~/projects resolves to that directory."""
        with patch("os.path.isdir", return_value=True), patch("os.listdir", return_value=["AGY"]):
            cwd = resolve_target_directory("refactor the auth middleware in AGY")
            self.assertTrue(cwd.endswith("/projects/AGY"))

    def test_target_directory_resolution_for_generic_spikes(self):
        """Verify generic spikes, demos, or unspecified projects route to sandbox."""
        cwd = resolve_target_directory("create a simple test page and host it on python server", task_id="demo-123")
        self.assertIn("/projects/sandbox/demo-123", cwd)
        self.assertTrue(os.path.isdir(cwd))

    def test_server_command_scoping_enforces_sandbox_directory(self):
        """Verify python http.server commands are scoped to the designated sandbox directory."""
        sandbox_path = "/Users/hermes/projects/sandbox/task-99"
        raw_cmd = "python3 -m http.server 8080"
        scoped = scope_server_commands(raw_cmd, sandbox_path)
        self.assertEqual(scoped, f"python3 -m http.server 8080 --directory {sandbox_path}")

        # When directory already specified, do not duplicate
        already_scoped = f"python3 -m http.server 8080 --directory {sandbox_path}"
        self.assertEqual(scope_server_commands(already_scoped, sandbox_path), already_scoped)

    def test_server_command_scoping_never_spawns_from_root(self):
        """Verify sandbox scoping falls back to safe sandbox directory if / is passed."""
        raw_cmd = "python3 -m http.server 8080"
        scoped = scope_server_commands(raw_cmd, "/")
        self.assertNotIn("--directory /", scoped.replace(os.path.expanduser("~/projects/sandbox"), "").replace("/Users/hermes/projects/sandbox", ""))
        self.assertIn("sandbox", scoped)

    def test_normalize_for_bluebubbles_links_and_emojis(self):
        """Verify outbound text for BlueBubbles converts Slack links and emoji shortcodes cleanly."""
        text = "Check out <http://localhost:8080|Preview> and <https://example.com>. Status: :white_check_mark: :thumbsup: :eyes: :warning: :x:"
        normalized = normalize_for_bluebubbles(text)

        self.assertIn("Preview (http://localhost:8080)", normalized)
        self.assertIn("https://example.com", normalized)
        self.assertNotIn("<http", normalized)
        self.assertIn("✅", normalized)
        self.assertIn("👍", normalized)
        self.assertIn("👀", normalized)
        self.assertIn("⚠️", normalized)
        self.assertIn("❌", normalized)
        self.assertNotIn(":white_check_mark:", normalized)

    async def test_dispatch_user_request_spawns_herdr_workspace_and_prompts_agy(self):
        """Verify new user request creates Herdr workspace, starts Antigravity CLI, and prompts agent."""
        client = AsyncMock()
        client.reactions_add = AsyncMock()
        client.reactions_remove = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "100.001"})

        event = {
            "channel": "C12345",
            "ts": "100.001",
            "thread_ts": "100.001",
            "text": "build a quick prototype counter in sandbox",
            "is_imessage": False
        }

        mock_ws_output = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w9"},
                "root_pane": {"pane_id": "w9:p1"}
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

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock) as mock_cli:
            mock_cli.side_effect = [
                (0, mock_ws_output, ""),  # workspace create
                (0, "started", ""),       # agent start --kind agy --pane w9:p1
                (0, mock_ready_agent, ""),# agent get (readiness check)
                (0, "prompted", ""),      # agent prompt fm-100001 <prompt>
            ]

            await process_user_prompt(event, client)

            # Verification:
            # 1. Herdr workspace create was called
            self.assertEqual(mock_cli.call_args_list[0][0][0][:2], ["workspace", "create"])
            # 2. Antigravity CLI started cleanly with zero trailing prompt arguments
            self.assertEqual(mock_cli.call_args_list[1][0][0], ["agent", "start", "fm-100001", "--kind", "agy", "--pane", "w9:p1"])
            # 3. Prompt dispatched via herdr agent prompt after readiness
            self.assertEqual(mock_cli.call_args_list[3][0][0][:3], ["agent", "prompt", "fm-100001"])

            # Session registered
            session_key = "C12345:100.001"
            self.assertIn(session_key, sessions)
            self.assertEqual(sessions[session_key].status, "starting")
            if sessions[session_key].monitor_task and not sessions[session_key].monitor_task.done():
                sessions[session_key].monitor_task.cancel()

    async def test_followup_prompt_uses_agent_prompt(self):
        """Verify subsequent prompts in an existing session use herdr agent prompt."""
        client = AsyncMock()
        client.reactions_add = AsyncMock()
        session_key = "C12345:100.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-100001",
            target_cwd="/Users/hermes/projects/sandbox/100001",
            channel="C12345",
            thread_ts="100.001"
        )
        sessions[session_key] = session

        event = {
            "channel": "C12345",
            "ts": "100.002",
            "thread_ts": "100.001",
            "text": "add a reset button to the counter",
            "is_imessage": False
        }

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = (0, "prompted", "")
            await process_user_prompt(event, client)

            self.assertEqual(mock_cli.call_args_list[0][0][0][:3], ["agent", "prompt", "fm-100001"])
            self.assertEqual(session.status, "working")

    async def test_interactive_approval_relay_sends_y_enter_to_herdr_pane(self):
        """Verify user approving a blocked agent relays 'y enter' directly to Herdr pane stdin."""
        session_key = "C12345:200.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/sandbox/test",
            channel="C12345",
            thread_ts="200.001"
        )
        session.status = "blocked"
        sessions[session_key] = session

        client = AsyncMock()
        client.chat_postMessage = AsyncMock()

        # Carl replies "go" or "approve"
        event = {
            "channel": "C12345",
            "ts": "200.002",
            "thread_ts": "200.001",
            "text": "approve",
            "is_imessage": False
        }

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = (0, "ok", "")

            await process_user_prompt(event, client)

            # Verification: executed herdr agent send-keys fm-test-agent y enter
            mock_cli.assert_called_once_with(["agent", "send-keys", "fm-test-agent", "y", "enter"])
            self.assertEqual(session.status, "working")

    async def test_interactive_rejection_relay_sends_n_enter_to_herdr_pane(self):
        """Verify user rejecting a blocked agent relays 'n enter' directly to Herdr pane stdin."""
        session_key = "C12345:300.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/sandbox/test",
            channel="C12345",
            thread_ts="300.001"
        )
        session.status = "blocked"
        sessions[session_key] = session

        client = AsyncMock()
        client.chat_postMessage = AsyncMock()

        # Carl replies "abort" or "cancel"
        event = {
            "channel": "C12345",
            "ts": "300.002",
            "thread_ts": "300.001",
            "text": "abort",
            "is_imessage": False
        }

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = (0, "ok", "")

            await process_user_prompt(event, client)

            # Verification: executed herdr agent send-keys fm-test-agent n enter
            mock_cli.assert_called_once_with(["agent", "send-keys", "fm-test-agent", "n", "enter"])
            self.assertEqual(session.status, "idle")

    async def test_reaction_approval_relays_y_enter_to_herdr_pane(self):
        """Verify Slack reaction 👍 on approval card relays 'y enter' to Herdr pane stdin."""
        session_key = "C12345:400.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/sandbox/test",
            channel="C12345",
            thread_ts="400.001"
        )
        session.status = "blocked"
        session.approval_msg_ts = "400.002"
        sessions[session_key] = session

        client = AsyncMock()
        reaction_event = {
            "reaction": "thumbsup",
            "item": {"channel": "C12345", "ts": "400.002"}
        }

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = (0, "ok", "")

            await handle_reaction_added(reaction_event, client)

            mock_cli.assert_called_once_with(["agent", "send-keys", "fm-test-agent", "y", "enter"])
            self.assertEqual(session.status, "working")

    async def test_live_status_inquiry_reads_herdr_scrollback_without_ungrounded_promises(self):
        """Verify 'what's the URL?' reads herdr agent scrollback and extracts running ports immediately."""
        session_key = "C12345:500.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/sandbox/test",
            channel="C12345",
            thread_ts="500.001"
        )
        session.status = "working"
        sessions[session_key] = session

        client = AsyncMock()
        client.chat_postMessage = AsyncMock()

        event = {
            "channel": "C12345",
            "ts": "500.002",
            "thread_ts": "500.001",
            "text": "what's the URL?",
            "is_imessage": False
        }

        mock_scrollback = "Serving HTTP on 0.0.0.0 port 51723 ...\nAvailable at http://127.0.0.1:51723"

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = (0, mock_scrollback, "")

            await process_user_prompt(event, client)

            mock_cli.assert_called_once_with([
                "agent", "read", "fm-test-agent", "--lines", "30", "--source", "recent-unwrapped"
            ])

            client.chat_postMessage.assert_called_once()
            reply = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("http://127.0.0.1:51723", reply)
            self.assertNotIn("retrieving it now", reply.lower())

    async def test_ignores_channel_message_without_mention(self):
        """Channel message without @AGY is ignored and does not react."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        client.reactions_add = AsyncMock()
        app.BOT_USER_ID = "U_AGY"

        event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "600.001",
            "text": "can anyone check disk space?",
        }

        should_process = await app.should_process_ingress_event(event, client)
        self.assertFalse(should_process)

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(event, client)
            mock_prompt.assert_not_called()
            mock_ingress.assert_not_called()
            client.reactions_add.assert_not_called()

    async def test_ignores_message_targeting_other_bots(self):
        """Message containing <@U_HERMES> in a channel or thread is ignored and does not react."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        client.reactions_add = AsyncMock()
        app.BOT_USER_ID = "U_AGY"

        # 1. In a channel
        channel_event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "600.002",
            "text": "<@U_HERMES> can you check disk space?",
        }
        self.assertFalse(await app.should_process_ingress_event(channel_event, client))

        # 2. In an active thread
        session_key = "C_GENERAL:600.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/AGY",
            channel="C_GENERAL",
            thread_ts="600.001"
        )
        sessions[session_key] = session

        thread_event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "600.003",
            "thread_ts": "600.001",
            "text": "<@U_HERMES> what is the status?",
        }
        self.assertFalse(await app.should_process_ingress_event(thread_event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(channel_event, client)
            await app.handle_message_events(thread_event, client)
            mock_prompt.assert_not_called()
            mock_ingress.assert_not_called()
            client.reactions_add.assert_not_called()

    async def test_handles_direct_messages(self):
        """1:1 DM is processed normally."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        app.BOT_USER_ID = "U_AGY"

        event = {
            "channel": "D12345",
            "channel_type": "im",
            "ts": "600.004",
            "text": "check the logs from the last hour",
        }
        self.assertTrue(await app.should_process_ingress_event(event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(event, client)
            self.assertTrue(mock_prompt.called or mock_ingress.called)

    async def test_handles_explicit_agy_mentions(self):
        """Message with <@U_AGY> is processed."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        app.BOT_USER_ID = "U_AGY"

        event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "600.006",
            "text": "<@U_AGY> let's take a look at our cronjobs",
        }
        self.assertTrue(await app.should_process_ingress_event(event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(event, client)
            self.assertTrue(mock_prompt.called or mock_ingress.called)

    async def test_handles_active_thread_followup_without_mention(self):
        """Once a thread is active, follow-up messages without @AGY mention must be processed."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        app.BOT_USER_ID = "U_AGY"

        session_key = "C_GENERAL:700.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/AGY",
            channel="C_GENERAL",
            thread_ts="700.001"
        )
        sessions[session_key] = session

        event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "700.002",
            "thread_ts": "700.001",
            "text": "can you also check memory usage?",
        }
        self.assertTrue(await app.should_process_ingress_event(event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(event, client)
            self.assertTrue(mock_prompt.called or mock_ingress.called)

    async def test_handles_active_thread_in_session_router_without_mention(self):
        """Active threads tracked via session_router must be processed on follow-up turns without mention."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        app.BOT_USER_ID = "U_AGY"

        mock_handle = MagicMock()
        mock_handle.thread_ts = "800.001"
        mock_handle.state = SessionState.IDLE
        app.session_router.sessions["C_GENERAL:800.001"] = mock_handle

        try:
            event = {
                "channel": "C_GENERAL",
                "channel_type": "channel",
                "ts": "800.002",
                "thread_ts": "800.001",
                "text": "please proceed with the refactor",
            }
            self.assertTrue(await app.should_process_ingress_event(event, client))

            with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
                 patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
                await app.handle_message_events(event, client)
                self.assertTrue(mock_prompt.called or mock_ingress.called)
        finally:
            app.session_router.sessions.pop("C_GENERAL:800.001", None)

    async def test_ignores_inactive_thread_message_without_mention(self):
        """Message in an inactive thread without @AGY is ignored and does not react."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        client.reactions_add = AsyncMock()
        app.BOT_USER_ID = "U_AGY"

        event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "900.002",
            "thread_ts": "900.001",  # Inactive thread, not in sessions/router
            "text": "is anyone working on this ticket?",
        }
        self.assertFalse(await app.should_process_ingress_event(event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(event, client)
            mock_prompt.assert_not_called()
            mock_ingress.assert_not_called()
            client.reactions_add.assert_not_called()

    async def test_ignores_empty_or_standalone_mention_without_reaction(self):
        """Empty messages or standalone @AGY mentions without prompt text are ignored without reactions."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        client.reactions_add = AsyncMock()
        app.BOT_USER_ID = "U_AGY"

        for text in ("", "   ", "<@U_AGY>", "<@U_AGY>   "):
            event = {
                "channel": "C_GENERAL",
                "channel_type": "channel",
                "ts": "950.001",
                "text": text,
            }
            self.assertFalse(await app.should_process_ingress_event(event, client))

            with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
                 patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
                await app.handle_message_events(event, client)
                mock_prompt.assert_not_called()
                mock_ingress.assert_not_called()
                client.reactions_add.assert_not_called()

    async def test_app_mention_targeting_other_bot_ignored_without_reaction(self):
        """Slack app_mention event targeting another bot is ignored without reaction."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        client.reactions_add = AsyncMock()
        app.BOT_USER_ID = "U_AGY"

        event = {
            "type": "app_mention",
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "960.001",
            "text": "<@U_HERMES> please reboot homelab",
        }
        self.assertFalse(await app.should_process_ingress_event(event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_app_mention(event, client)
            mock_prompt.assert_not_called()
            mock_ingress.assert_not_called()
            client.reactions_add.assert_not_called()

    async def test_handles_active_thread_followup_mentioning_human_teammate(self):
        """Active thread follow-up that mentions a teammate (e.g. <@U_ALICE>) is processed."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        app.BOT_USER_ID = "U_AGY"

        session_key = "C_GENERAL:750.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/AGY",
            channel="C_GENERAL",
            thread_ts="750.001"
        )
        sessions[session_key] = session

        event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "750.002",
            "thread_ts": "750.001",
            "text": "<@U_ALICE> I have approved the PR, please deploy it now",
        }
        self.assertTrue(await app.should_process_ingress_event(event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(event, client)
            self.assertTrue(mock_prompt.called or mock_ingress.called)

    async def test_handles_explicit_mention_with_display_label(self):
        """Mentions with Slack label formatting like <@U_AGY|agy> are processed."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        app.BOT_USER_ID = "U_AGY"

        event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "760.001",
            "text": "<@U_AGY|agy> please check system health",
        }
        self.assertTrue(await app.should_process_ingress_event(event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(event, client)
            self.assertTrue(mock_prompt.called or mock_ingress.called)

    async def test_handles_message_mentioning_both_hermes_and_agy(self):
        """When both Hermes and AGY are mentioned, AGY is addressed and processes the turn."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        app.BOT_USER_ID = "U_AGY"

        event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "770.001",
            "text": "<@U_HERMES> and <@U_AGY> please coordinate the deployment",
        }
        self.assertTrue(await app.should_process_ingress_event(event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(event, client)
            self.assertTrue(mock_prompt.called or mock_ingress.called)

    async def test_ignores_message_sent_by_other_bot_in_active_thread(self):
        """Messages sent by other bots in an active thread are ignored without reactions."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        client.reactions_add = AsyncMock()
        app.BOT_USER_ID = "U_AGY"

        session_key = "C_GENERAL:780.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/AGY",
            channel="C_GENERAL",
            thread_ts="780.001"
        )
        sessions[session_key] = session

        for bot_event in (
            {"channel": "C_GENERAL", "ts": "780.002", "thread_ts": "780.001", "user": "U_HERMES", "text": "Homelab report ready"},
            {"channel": "C_GENERAL", "ts": "780.003", "thread_ts": "780.001", "user": "U_CI_BOT", "text": "Build passed"},
            {"channel": "C_GENERAL", "ts": "780.004", "thread_ts": "780.001", "bot_id": "B9999", "text": "Webhook payload"},
            {"channel": "C_GENERAL", "ts": "780.005", "thread_ts": "780.001", "bot_profile": {"name": "app"}, "text": "App alert"},
            {"channel": "C_GENERAL", "ts": "780.006", "thread_ts": "780.001", "user": "U_AGY", "text": "Self message echo"},
        ):
            self.assertFalse(await app.should_process_ingress_event(bot_event, client))

            with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
                 patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
                await app.handle_message_events(bot_event, client)
                mock_prompt.assert_not_called()
                mock_ingress.assert_not_called()
                client.reactions_add.assert_not_called()

    async def test_ignores_plain_text_hermes_mention_in_active_thread(self):
        """Plain text @hermes message in an active thread is ignored without reaction."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        client.reactions_add = AsyncMock()
        app.BOT_USER_ID = "U_AGY"

        session_key = "C_GENERAL:790.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/AGY",
            channel="C_GENERAL",
            thread_ts="790.001"
        )
        sessions[session_key] = session

        event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "790.002",
            "thread_ts": "790.001",
            "text": "@hermes please reboot the server",
        }
        self.assertFalse(await app.should_process_ingress_event(event, client))

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
             patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
            await app.handle_message_events(event, client)
            mock_prompt.assert_not_called()
            mock_ingress.assert_not_called()
            client.reactions_add.assert_not_called()

    async def test_ignores_non_conversational_subtypes_without_reaction(self):
        """Non-conversational message subtypes (message_replied, message_changed, etc.) are ignored."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        client.reactions_add = AsyncMock()
        app.BOT_USER_ID = "U_AGY"

        for subtype in ("message_replied", "message_changed", "channel_leave", "pinned_item"):
            event = {
                "channel": "C_GENERAL",
                "channel_type": "channel",
                "ts": "795.001",
                "subtype": subtype,
                "text": "subtype event text",
            }
            self.assertFalse(await app.should_process_ingress_event(event, client))

            with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
                 patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
                await app.handle_message_events(event, client)
                mock_prompt.assert_not_called()
                mock_ingress.assert_not_called()
                client.reactions_add.assert_not_called()

    async def test_active_thread_with_failed_prior_turn_still_accepts_followup(self):
        """A thread whose previous turn failed is still active and accepts follow-up turns."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        app.BOT_USER_ID = "U_AGY"

        session_key = "C_GENERAL:810.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-agent",
            target_cwd="/Users/hermes/projects/AGY",
            channel="C_GENERAL",
            thread_ts="810.001"
        )
        session.status = "failed"
        sessions[session_key] = session

        event = {
            "channel": "C_GENERAL",
            "channel_type": "channel",
            "ts": "810.002",
            "thread_ts": "810.001",
            "text": "try again with python3 instead",
        }
        self.assertTrue(await app.should_process_ingress_event(event, client))

    async def test_closed_session_thread_requires_new_mention(self):
        """A thread whose session was closed ignores messages without @AGY mention, but accepts with mention."""
        client = AsyncMock()
        client.auth_test = AsyncMock(return_value={"user_id": "U_AGY"})
        client.reactions_add = AsyncMock()
        app.BOT_USER_ID = "U_AGY"

        mock_handle = MagicMock()
        mock_handle.thread_ts = "820.001"
        mock_handle.state = SessionState.CLOSED
        mock_handle.channel = "C_GENERAL"
        app.session_router.sessions["C_GENERAL:820.001"] = mock_handle

        try:
            # 1. Without mention -> ignored
            ev_no_mention = {
                "channel": "C_GENERAL",
                "channel_type": "channel",
                "ts": "820.002",
                "thread_ts": "820.001",
                "text": "can you hear me?",
            }
            self.assertFalse(await app.should_process_ingress_event(ev_no_mention, client))

            with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_prompt, \
                 patch.object(app.ingress_gateway, "handle_message", new_callable=AsyncMock) as mock_ingress:
                await app.handle_message_events(ev_no_mention, client)
                mock_prompt.assert_not_called()
                mock_ingress.assert_not_called()
                client.reactions_add.assert_not_called()

            # 2. With mention -> processed
            ev_with_mention = {
                "channel": "C_GENERAL",
                "channel_type": "channel",
                "ts": "820.003",
                "thread_ts": "820.001",
                "text": "<@U_AGY> wake up and restart session",
            }
            self.assertTrue(await app.should_process_ingress_event(ev_with_mention, client))
        finally:
            app.session_router.sessions.pop("C_GENERAL:820.001", None)

    async def test_text_delivered_immediately_when_background_task_active(self):
        """Simulate agy outputting an authorization URL while a background process task remains running.
        Assert the message is dispatched to Slack within 2 seconds without waiting for a 'result' event.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-oauth-app-test",
            "init": {"tools": ["run_command"]},
        })

        oauth_url = "http://127.0.0.1:53682/auth?state=xyz123"
        stream_lines = [
            # 1. Tool start
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "run_command",
                    "step_index": 1,
                    "tool_info": {
                        "parameters": {"CommandLine": "rclone config reconnect gdrive:"}
                    },
                }
            }),
            # 2. Tool completed as a background task
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "run_command",
                    "step_index": 1,
                    "tool_info": {
                        "output": "Tool is running as a background task with task id: 555b9570/task-91\nTask logs are available at: file:///task-91.log",
                        "status": "RUNNING",
                    },
                }
            }),
            # 3. Model speaks conversational text containing authorization URL
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 2,
                    "text_delta": f"Please visit {oauth_url} to authorize Google Drive access.",
                }
            }),
        ]

        class BackgroundProcessMock:
            def __init__(self):
                self.stdin = MagicMock()
                self.stdin.write = MagicMock()
                self.stdin.drain = AsyncMock()
                self.stdin.close = MagicMock()
                self.stdin.wait_closed = AsyncMock()
                self.stdin.is_closing = MagicMock(return_value=False)
                self.stderr = MagicMock()
                self.stderr.read = AsyncMock(return_value=b"")
                self.returncode = None
                self._lines = [init_json] + stream_lines

            async def readline(self):
                if self._lines:
                    line = self._lines.pop(0)
                    return line.encode("utf-8") + b"\n"
                await asyncio.sleep(10.0)
                return b""

            async def wait(self):
                return 0

            def terminate(self):
                self.returncode = 0

            def kill(self):
                self.returncode = -9

        mock_proc = BackgroundProcessMock()
        mock_proc.stdout = MagicMock()
        mock_proc.stdout.readline = AsyncMock(side_effect=mock_proc.readline)

        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "200.001"})
        client.reactions_add = AsyncMock(return_value={"ok": True})
        client.reactions_remove = AsyncMock(return_value={"ok": True})

        from golem.gateway.router import SessionRouter
        from golem.gateway.ingress import IngressGateway

        router = SessionRouter(default_target_cwd="/Users/hermes/projects/golem")
        gateway = IngressGateway(router=router)

        event = {
            "channel": "C_OAUTH_APP",
            "ts": "100.999",
            "thread_ts": "100.999",
            "text": "ok, can you help me re-auth google drive?",
        }

        t_start = asyncio.get_event_loop().time()

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            await asyncio.wait_for(gateway.handle_message(event, client), timeout=2.5)

        t_elapsed = asyncio.get_event_loop().time() - t_start

        self.assertLess(t_elapsed, 2.0, f"Turn took {t_elapsed:.2f}s, expected < 2.0s")
        self.assertTrue(client.chat_postMessage.called)
        posted_texts = [
            call.kwargs.get("text") or (call.args[1] if len(call.args) > 1 else "")
            for call in client.chat_postMessage.call_args_list
        ]
        combined_text = " ".join(posted_texts)
        self.assertIn(oauth_url, combined_text)
        self.assertEqual(len(client.chat_postMessage.call_args_list), 1)

        await router.teardown("C_OAUTH_APP", "100.999")

    async def test_slash_command_ack_and_dispatch(self):
        """Verify slash command immediately acks within milliseconds and dispatches to handler."""
        ack_called = False

        async def mock_ack():
            nonlocal ack_called
            ack_called = True

        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True})

        command = {
            "command": "/update",
            "text": "",
            "channel_id": "C_SLASH_TEST",
            "user_id": "U_USER1",
        }

        with patch("app.handle_update_request", new_callable=AsyncMock) as mock_handle_update:
            await app.handle_slash_command(mock_ack, {}, client, command)
            self.assertTrue(ack_called, "Slash command handler MUST call ack() immediately")
            mock_handle_update.assert_awaited_once_with(channel="C_SLASH_TEST", thread_ts=None, client=client)

    async def test_wildcard_slash_command_acks_immediately(self):
        """Verify wildcard slash command listener acks unregistered slash commands immediately to prevent timeouts."""
        ack_called = False

        async def mock_ack():
            nonlocal ack_called
            ack_called = True

        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True})

        command = {
            "command": "/customcommand",
            "text": "test arg",
            "channel_id": "C_WILDCARD_TEST",
            "user_id": "U_USER2",
        }

        with patch("app.ingress_gateway", None):
            with patch("app.process_user_prompt", new_callable=AsyncMock) as mock_prompt:
                await app.handle_wildcard_slash_command(mock_ack, {}, client, command)
                self.assertTrue(ack_called, "Wildcard slash command listener MUST call ack() immediately")
                mock_prompt.assert_awaited_once()

    async def test_handle_update_request_aborts_when_busy(self):
        """Verify handle_update_request aborts update if active running tasks exist."""
        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True})

        # Add a running session
        running_session = MagicMock()
        running_session.status = "running"
        running_session.is_busy = True
        app.sessions["C_BUSY:100.1"] = running_session

        await app.handle_update_request("C_BUSY", thread_ts="100.1", client=client)

        client.chat_postMessage.assert_awaited_once()
        posted_text = client.chat_postMessage.call_args.kwargs.get("text", "")
        self.assertIn("Update Aborted", posted_text)

    async def test_ingress_gateway_intercepts_update(self):
        """Verify IngressGateway intercepts /update and !update cleanly without invoking Mouth LLM."""
        from golem.gateway.router import SessionRouter
        from golem.gateway.ingress import IngressGateway

        router = SessionRouter(default_target_cwd="/Users/hermes/projects/golem")
        gateway = IngressGateway(router=router)

        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True})
        client.reactions_add = AsyncMock(return_value={"ok": True})

        event = {
            "channel": "C_UPDATE_INGRESS",
            "ts": "100.555",
            "thread_ts": "100.555",
            "text": "/update",
        }

        with patch.object(gateway, "_post_response", new_callable=AsyncMock) as mock_post, \
             patch("asyncio.create_task") as mock_task:
            await gateway.handle_message(event, client)
            mock_post.assert_awaited_once()
            posted_text = mock_post.call_args[0][3]
            self.assertIn("Update:", posted_text)
            mock_task.assert_called_once()


if __name__ == "__main__":
    unittest.main()

