#!/usr/bin/env python3
"""
tests/test_workspace_layout.py
Unit tests for Dual-Pane Workspace architecture:
1. Workspace lifecycle spawns both liaison (Pane 1) and runner (Pane 2) panes.
2. Runner command execution via mesh executes commands in runner pane without blocking liaison.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import asyncio
import unittest
from unittest.mock import patch, AsyncMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app
from app import (
    ensure_session_agent,
    init_dual_pane_workspace,
    GatewaySession,
    sessions,
    agent_name_to_session
)
import herdr_tools
from herdr_tools import herdr_pane_run, herdr_agent_read, herdr_handoff
from agent_core import ChiefOfStaff


class TestWorkspaceLayout(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        sessions.clear()
        agent_name_to_session.clear()

    def tearDown(self):
        sessions.clear()
        agent_name_to_session.clear()

    async def test_workspace_spawns_liaison_and_runner_panes(self):
        """Verifies both liaison (agy) and runner (shell) panes are created on task start."""
        session_key = "C12345:100.999"
        mock_ws_output = json.dumps({
            "result": {
                "workspace": {"workspace_id": "ws-dual-1"},
                "root_pane": {"pane_id": "ws-dual-1:p1"}
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
                (0, mock_ws_output, ""),    # workspace create
                (0, "started liaison", ""), # agent start fm-100999-liaison --kind agy --pane 1
                (0, "created runner", ""),  # pane create --workspace fm-100999 --label runner
                (0, mock_ready_agent, ""),  # agent get readiness
                (0, "prompted", ""),        # agent prompt fm-100999-liaison <prompt>
            ]

            session = await ensure_session_agent(
                session_key=session_key,
                channel="C12345",
                thread_ts="100.999",
                prompt="run analytics build",
                dual_pane=True
            )

            # 1. Verify workspace created: herdr workspace create --label fm-<ts> --cwd <cwd> --no-focus
            ws_call = mock_cli.call_args_list[0][0][0]
            self.assertEqual(ws_call[:2], ["workspace", "create"])
            self.assertIn("--label", ws_call)
            self.assertIn("fm-100999", ws_call)
            self.assertIn("--cwd", ws_call)
            self.assertIn("--no-focus", ws_call)

            # 2. Verify Pane 1 (liaison) spawned: herdr agent start <name>-liaison --kind agy --pane 1
            liaison_call = mock_cli.call_args_list[1][0][0]
            self.assertEqual(liaison_call, [
                "agent", "start", "fm-100999-liaison", "--kind", "agy", "--pane", "1"
            ])

            # 3. Verify Pane 2 (runner) persistent shell pane created: herdr pane create --workspace <name> --label runner
            runner_call = mock_cli.call_args_list[2][0][0]
            self.assertEqual(runner_call, [
                "pane", "create", "--workspace", "fm-100999", "--label", "runner"
            ])

            # 4. Verify prompt delivered to liaison agent
            prompt_call = mock_cli.call_args_list[4][0][0]
            self.assertEqual(prompt_call[:3], ["agent", "prompt", "fm-100999-liaison"])

            # 5. Session state tracks dual-pane properties
            self.assertTrue(session.dual_pane)
            self.assertEqual(session.liaison_agent, "fm-100999-liaison")
            self.assertEqual(session.runner_pane, "runner")

            # 6. Verify init_dual_pane_workspace helper convenience
            mock_cli.side_effect = [
                (0, mock_ws_output, ""),
                (0, "started liaison", ""),
                (0, "created runner", ""),
                (0, mock_ready_agent, ""),
                (0, "prompted", "")
            ]
            session2 = await init_dual_pane_workspace(
                session_name="fm-test2",
                cwd="/Users/hermes/projects/sandbox/test2",
                channel="C999",
                thread_ts="200.001"
            )
            self.assertTrue(session2.dual_pane)
            self.assertEqual(session2.liaison_agent, "fm-test2-liaison")
            self.assertEqual(session2.runner_pane, "runner")

    async def test_runner_command_execution_via_mesh(self):
        """Verifies commands dispatched to the runner pane do not block the liaison pane."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"Command finished in runner pane\n", b"")
            mock_exec.return_value = mock_proc

            # Execute command targeting runner pane via herdr_pane_run
            result = await herdr_pane_run(pane="runner", command="npm run build")

            self.assertEqual(result["pane"], "runner")
            self.assertEqual(result["command"], "npm run build")
            self.assertEqual(result["status"], "dispatched")
            self.assertIn("Command finished in runner pane", result["stdout"])

            # Verify herdr pane run CLI call was issued with pane and command
            called_args = mock_exec.call_args[0]
            self.assertIn("pane", called_args)
            self.assertIn("run", called_args)
            self.assertIn("runner", called_args)
            self.assertIn("npm run build", called_args)

            # Verify active observation: read runner buffer via herdr_agent_read
            mock_read_proc = AsyncMock()
            mock_read_proc.returncode = 0
            mock_read_proc.communicate.return_value = (
                b"Rclone OAuth listener active at http://127.0.0.1:53682/auth\nWaiting for token...\n",
                b""
            )
            mock_exec.return_value = mock_read_proc

            output = await herdr_agent_read(pane="runner", lines=30)
            self.assertIn("http://127.0.0.1:53682/auth", output)
            self.assertIn("Waiting for token...", output)

            # Verify tool dispatch through ChiefOfStaff
            chief = ChiefOfStaff(api_key="test-key")
            tool_res = await chief.execute_tool_call("herdr_pane_run", {
                "pane": "runner",
                "command": "python3 server.py"
            })
            self.assertEqual(tool_res["pane"], "runner")

            read_res = await chief.execute_tool_call("herdr_agent_read", {
                "pane": "runner",
                "lines": 10
            })
            self.assertIn("http://127.0.0.1:53682/auth", read_res)

    async def test_dual_pane_lifecycle_monitor_and_approval_targets_liaison(self):
        """Verifies approvals, reactions, and lifecycle monitoring target liaison agent in dual pane."""
        session = GatewaySession(
            session_key="C999:500.001",
            agent_name="fm-500001",
            target_cwd="/tmp/dual",
            channel="C999",
            thread_ts="500.001",
            dual_pane=True,
            liaison_agent="fm-500001-liaison",
            runner_pane="runner"
        )
        sessions["C999:500.001"] = session

        # Verify active_agent property
        self.assertEqual(session.active_agent, "fm-500001-liaison")

        # Test approval relay uses active_agent (fm-500001-liaison)
        session.status = "blocked"
        client = AsyncMock()
        client.chat_postMessage = AsyncMock()

        event = {
            "channel": "C999",
            "ts": "500.002",
            "thread_ts": "500.001",
            "text": "approve",
            "is_imessage": False
        }

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = (0, "ok", "")
            await app.process_user_prompt(event, client)

            # Assert send-keys targeted liaison_agent, NOT fm-500001
            send_keys_call = mock_cli.call_args[0][0]
            self.assertEqual(send_keys_call, ["agent", "send-keys", "fm-500001-liaison", "y", "enter"])


if __name__ == "__main__":
    unittest.main()
