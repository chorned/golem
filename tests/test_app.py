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
"""

import os
import sys
import json
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


class TestThinIngressGateway(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        sessions.clear()

    def tearDown(self):
        sessions.clear()

    def test_target_directory_resolution_for_named_projects(self):
        """Verify prompt mentioning a specific project in ~/projects resolves to that directory."""
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
        self.assertNotIn("--directory /", scoped.replace("/Users/hermes/projects/sandbox", ""))
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


if __name__ == "__main__":
    unittest.main()
