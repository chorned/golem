#!/usr/bin/env python3
"""
tests/test_telemetry_observer.py
Comprehensive unit tests for First Mate conversational milestone telemetry,
process filtering, scrollback noise suppression, and zero-token status intercepts.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import time
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app
from app import GatewaySession, sessions, process_user_prompt
import golem  # noqa: F401
from telemetry_observer import (
    SessionTelemetryObserver,
    resolve_machine_name,
    resolve_machine_label,
    format_runner_label,
    extract_listening_ports,
    format_terminal_tail,
    strip_terminal_sequences,
    sanitize_scrollback,
    sanitize_scrollback_command,
    is_plumbing_command,
    extract_pending_command,
    summarize_task_intent,
    format_startup_message,
    format_approval_blocks,
    format_completion_message,
    format_failure_message,
    build_status_card_blocks
)


class TestConversationalTelemetry(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        sessions.clear()

    def tearDown(self):
        sessions.clear()

    def test_resolve_machine_name(self):
        """Verify concise host name resolution for conversational messages."""
        self.assertEqual(resolve_machine_name("desktop-pc"), "desktop-pc")
        self.assertEqual(resolve_machine_name("compute-grid"), "desktop-pc")
        self.assertEqual(resolve_machine_name("oci-machine"), "oci-machine")
        self.assertEqual(resolve_machine_name("cloud-ops"), "oci-machine")
        self.assertEqual(resolve_machine_name("personal-laptop"), "personal-laptop")
        self.assertEqual(resolve_machine_name("dev-grid"), "personal-laptop")
        self.assertEqual(resolve_machine_name("hermes"), "hermes")
        self.assertEqual(resolve_machine_name("localhost"), "hermes")
        self.assertEqual(resolve_machine_name(""), "hermes")
        self.assertEqual(resolve_machine_name(None), "hermes")

    def test_is_plumbing_command(self):
        """Verify internal infrastructure binaries and shims are blacklisted as plumbing."""
        self.assertTrue(is_plumbing_command("node /Users/hermes/.herdr/mcp/herdr-mesh.js"))
        self.assertTrue(is_plumbing_command("herdr agent list"))
        self.assertTrue(is_plumbing_command("agy -p 'prompt'"))
        self.assertTrue(is_plumbing_command("opencode --internal"))
        self.assertTrue(is_plumbing_command("/bin/zsh"))
        self.assertTrue(is_plumbing_command("cat << 'EOF' > file.txt"))
        self.assertTrue(is_plumbing_command("git status"))
        self.assertTrue(is_plumbing_command("launchctl list"))

        # Real operational commands should NOT be plumbing
        self.assertFalse(is_plumbing_command("python3 -m http.server 8090"))
        self.assertFalse(is_plumbing_command("npm run build"))
        self.assertFalse(is_plumbing_command("curl -i http://localhost:8090"))
        self.assertFalse(is_plumbing_command("pytest tests/"))

    def test_sanitize_scrollback(self):
        """Verify scrollback sanitizer strips prompts, git tags, version headers, and chrome."""
        raw = (
            "\x1b[32mAntigravity CLI\x1b[0m\n"
            "Gemini 3.8 Flash\n"
            "? for shortcuts\n"
            "hermes@Hermes-Mac-mini % python3 -m http.server 8090\n"
            "Serving HTTP on 0.0.0.0 port 8090 ...\n"
            "~/projects/sandbox/fm-12345:main\n"
            "Ready for connections.\n"
        )
        cleaned = sanitize_scrollback(raw)
        lines = cleaned.splitlines()
        self.assertNotIn("Antigravity CLI", cleaned)
        self.assertNotIn("? for shortcuts", cleaned)
        self.assertNotIn("hermes@Hermes-Mac-mini", cleaned)
        self.assertNotIn(":main", cleaned)
        self.assertIn("Serving HTTP on 0.0.0.0 port 8090 ...", cleaned)
        self.assertIn("Ready for connections.", cleaned)

    def test_extract_pending_command(self):
        """Verify command requiring approval is extracted cleanly from Ask Mode output."""
        sample_1 = "● Run command python3 -m http.server 8090\nAllow this action? [y/N]"
        self.assertEqual(extract_pending_command(sample_1), "python3 -m http.server 8090")

        sample_2 = "Do you want to run: npm run build? [y/n]"
        self.assertEqual(extract_pending_command(sample_2), "npm run build")

        sample_3 = "Serving HTTP on 0.0.0.0 port 8090\nAllow this action?"
        self.assertEqual(extract_pending_command(sample_3), "python3 -m http.server 8090")

        sample_plumbing = "node /Users/hermes/.herdr/mcp/herdr-mesh.js\nAllow this action?"
        self.assertEqual(extract_pending_command(sample_plumbing), "the requested command")

    def test_summarize_task_intent(self):
        """Verify extraction of user intent for Chief of Staff startup notice."""
        p1 = "@AGY Can you have OpenCode create a simple test page and host it in a simple python server?"
        self.assertEqual(summarize_task_intent(p1), "create a simple test page and host it in a simple python server")

        p2 = "Please build a React dashboard"
        self.assertEqual(summarize_task_intent(p2), "build a React dashboard")

        p3 = "Can you run tests?"
        self.assertEqual(summarize_task_intent(p3), "run tests")

    def test_format_startup_message(self):
        """Verify startup delegation message matches Chief of Staff conversational standard."""
        prompt = "@AGY Can you have OpenCode create a simple test page and host it in a simple python server?"
        msg = format_startup_message("OpenCode", "hermes", prompt)
        self.assertEqual(
            msg,
            "On it. Handing this to OpenCode on `hermes` to create a simple test page and host it in a simple python server."
        )

    def test_format_approval_blocks(self):
        """Verify conversational Block Kit approval buttons."""
        text, blocks = format_approval_blocks("OpenCode", "python3 -m http.server 8090", "C_TEST:123")
        self.assertEqual(text, "*OpenCode wants to run `python3 -m http.server 8090`. Good to go?*")
        self.assertEqual(len(blocks), 2)
        self.assertEqual(blocks[0]["type"], "section")
        self.assertEqual(blocks[1]["type"], "actions")
        buttons = blocks[1]["elements"]
        self.assertEqual(len(buttons), 2)
        self.assertEqual(buttons[0]["text"]["text"], "Approve")
        self.assertEqual(buttons[0]["action_id"], "approve_action")
        self.assertEqual(buttons[1]["text"]["text"], "Reject")
        self.assertEqual(buttons[1]["action_id"], "reject_action")

    def test_format_completion_message(self):
        """Verify completion announcement features clean operational artifacts."""
        msg_server = format_completion_message(
            machine="hermes",
            urls=["http://localhost:8090"]
        )
        self.assertEqual(msg_server, "All set! The test server is running at http://localhost:8090 on `hermes`.")

        msg_created = format_completion_message(
            machine="hermes",
            urls=["http://localhost:8090"],
            files=["index.html"]
        )
        self.assertEqual(
            msg_created,
            "All set! The test server is running at http://localhost:8090 on `hermes` (created `index.html`)."
        )

        msg_files_only = format_completion_message(
            machine="hermes",
            files=["index.html", "style.css"]
        )
        self.assertEqual(msg_files_only, "All set! Created `index.html, style.css` on `hermes`.")

    def test_format_failure_message(self):
        """Verify conversational failure message format."""
        msg = format_failure_message("port 8090 is already in use")
        self.assertEqual(msg, "Hit an issue running the server: `port 8090 is already in use`.")

    async def test_observer_detects_subagent_delegation(self):
        """Verify observer detects child pane in workspace inspect and posts delegation notice."""
        session = GatewaySession(
            session_key="C_TEST:600.001",
            agent_name="fm-test-delegation",
            target_cwd="/Users/hermes/projects/sandbox/test-delegation",
            channel="C_TEST",
            thread_ts="600.001",
            workspace_id="w_delegation"
        )
        session.status = "working"
        session.last_prompt = "build prototype"

        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "msg_ts_001"})

        herdr_client = MagicMock()
        
        ws_inspect_turn1 = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w_delegation"},
                "panes": [{"pane_id": "w_delegation:p1", "agent": "agy", "machine": "hermes"}]
            }
        })
        ws_inspect_turn2 = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w_delegation"},
                "panes": [
                    {"pane_id": "w_delegation:p1", "agent": "agy", "machine": "hermes"},
                    {"pane_id": "w_delegation:p2", "agent": "opencode", "machine": "hermes", "name": "opencode-worker"}
                ]
            }
        })

        turn = [1]
        async def mock_run_cli(args, timeout=5.0):
            if args[:2] == ["workspace", "inspect"]:
                out = ws_inspect_turn1 if turn[0] == 1 else ws_inspect_turn2
                return 0, out, ""
            elif args[:2] == ["agent", "inspect"]:
                return 0, json.dumps({"foreground_process": {"cmdline": "python3 -m http.server 8090"}}), ""
            elif args[:2] == ["agent", "list"]:
                return 0, json.dumps({"result": {"agents": []}}), ""
            elif args[:2] == ["agent", "read"]:
                return 0, "Serving HTTP on 0.0.0.0 port 8090 ...\n", ""
            return 0, "", ""

        herdr_client.run_cli = AsyncMock(side_effect=mock_run_cli)

        observer = SessionTelemetryObserver(
            session=session,
            client=client,
            herdr_client=herdr_client
        )
        # Initialize startup milestone (muted ingress: no canned message)
        await observer.initialize_dashboard()
        self.assertIn("startup", observer.milestones_posted)
        self.assertEqual(client.chat_postMessage.call_count, 0)

        # Poll Turn 1: agy alone
        await observer.poll_and_update()
        self.assertEqual(client.chat_postMessage.call_count, 0)

        # Poll Turn 2: OpenCode appears -> delegation notice posted
        turn[0] = 2
        await observer.poll_and_update()
        self.assertIn("delegation_opencode", observer.milestones_posted)
        self.assertEqual(client.chat_postMessage.call_count, 1)
        delegation_msg = client.chat_postMessage.call_args_list[0][1]["text"]
        self.assertIn("Handing this to OpenCode on `hermes`", delegation_msg)

    async def test_status_query_intercept_extracts_url(self):
        """Verify on-demand 'what's the URL?' returns clean operational truth in <300ms without LLM."""
        session_key = "C_TEST:800.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-url-query",
            target_cwd="/tmp/url-query",
            channel="C_TEST",
            thread_ts="800.001",
            workspace_id="w_url_query"
        )
        session.status = "working"
        sessions[session_key] = session

        client = AsyncMock()
        client.chat_postMessage = AsyncMock()

        event = {
            "channel": "C_TEST",
            "ts": "800.002",
            "thread_ts": "800.001",
            "text": "what's the URL?",
            "is_imessage": False
        }

        mock_terminal_scrollback = (
            "Serving HTTP on 0.0.0.0 port 8090 (http://0.0.0.0:8090/) ...\n"
            "Available at http://localhost:8090"
        )

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = (0, mock_terminal_scrollback, "")

            start_t = time.time()
            await process_user_prompt(event, client)
            elapsed = time.time() - start_t

            self.assertLess(elapsed, 0.3)
            cli_calls = [c.args[0] for c in mock_cli.call_args_list]
            self.assertTrue(any(c[:2] == ["agent", "read"] for c in cli_calls))
            self.assertFalse(any(c[:2] == ["agent", "prompt"] for c in cli_calls))

            client.chat_postMessage.assert_called_once()
            reply = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("8090", reply)

    async def test_settle_complete_does_not_emit_chat_postmessage(self):
        """Verify settle_complete does NOT call chat_postMessage, leaving app.py as sole completion emitter."""
        session = GatewaySession(
            session_key="C_TEST:900.001",
            agent_name="fm-test-settle",
            target_cwd="/tmp/settle",
            channel="C_TEST",
            thread_ts="900.001",
            workspace_id="w_settle"
        )
        client = AsyncMock()
        client.chat_postMessage = AsyncMock()
        herdr_client = MagicMock()

        observer = SessionTelemetryObserver(
            session=session,
            client=client,
            herdr_client=herdr_client
        )
        await observer.settle_complete(artifacts={"urls": ["http://localhost:8080"]}, terminal_output="Done")

        # Must mark complete and stop event
        self.assertTrue(observer.stop_event.is_set())
        self.assertIn("complete", observer.milestones_posted)
        # Must NOT call chat_postMessage (no double completion notice)
        client.chat_postMessage.assert_not_called()

    async def test_opencode_prompt_at_dispatch_does_not_prematurely_announce_delegation(self):
        """Verify prompt mentioning OpenCode does not prematurely emit delegation notice on dispatch."""
        session = GatewaySession(
            session_key="C_TEST:950.001",
            agent_name="fm-test-dispatch-opencode",
            target_cwd="/tmp/dispatch",
            channel="C_TEST",
            thread_ts="950.001",
            workspace_id="w_dispatch"
        )
        session.last_prompt = "@AGY Can you have OpenCode create a simple test page and host it?"

        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "msg_001"})
        herdr_client = MagicMock()

        ws_inspect_turn1 = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w_dispatch"},
                "panes": [{"pane_id": "w_dispatch:p1", "agent": "agy", "machine": "hermes"}]
            }
        })
        ws_inspect_turn2 = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w_dispatch"},
                "panes": [
                    {"pane_id": "w_dispatch:p1", "agent": "agy", "machine": "hermes"},
                    {"pane_id": "w_dispatch:p2", "agent": "opencode", "machine": "hermes", "name": "opencode-worker"}
                ]
            }
        })

        turn = [1]
        async def mock_run_cli(args, timeout=5.0):
            if args[:2] == ["workspace", "inspect"]:
                out = ws_inspect_turn1 if turn[0] == 1 else ws_inspect_turn2
                return 0, out, ""
            elif args[:2] == ["agent", "inspect"]:
                return 0, json.dumps({"foreground_process": {"cmdline": "python3 -m http.server 8090"}}), ""
            elif args[:2] == ["agent", "list"]:
                return 0, json.dumps({"result": {"agents": []}}), ""
            elif args[:2] == ["agent", "read"]:
                return 0, "Serving HTTP on 0.0.0.0 port 8090 ...\n", ""
            return 0, "", ""

        herdr_client.run_cli = AsyncMock(side_effect=mock_run_cli)

        observer = SessionTelemetryObserver(
            session=session,
            client=client,
            herdr_client=herdr_client
        )

        # 1. Dispatch / startup: MUST NOT announce delegation or emit canned startup text
        await observer.initialize_dashboard()
        self.assertEqual(client.chat_postMessage.call_count, 0)

        # 2. Turn 1 (only agy pane in Herdr): NO delegation notice
        await observer.poll_and_update()
        self.assertEqual(client.chat_postMessage.call_count, 0)

        # 3. Turn 2 (child pane appears in Herdr): SOLE emitter of delegation notice
        turn[0] = 2
        await observer.poll_and_update()
        self.assertEqual(client.chat_postMessage.call_count, 1)
        delegation_msg = client.chat_postMessage.call_args_list[0][1]["text"]
        self.assertIn("Handing this to OpenCode on `hermes` to execute.", delegation_msg)


if __name__ == "__main__":
    unittest.main()
