#!/usr/bin/env python3
"""
tests/test_agent_loop.py
Comprehensive unit tests for First Mate Agent-First Chief of Staff loop:
- Turn 1: Plan Pitching without premature tool dispatch
- Turn 2: Approval ("go" / 👍) triggering spawn_agent with git branch isolation
- Turn 3: In-flight inspection ("what's the URL?") triggering read_agent
- Turn 4: Undo/Rollback ("abort" / "undo") triggering stop_agent with rollback
"""

import os
import sys
import json
import asyncio
import unittest
from unittest.mock import patch, AsyncMock, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app
import agent_core
from agent_core import ChiefOfStaff, build_system_prompt, format_for_slack
from herdr_tools import SPAWNED_AGENTS



class TestAgentLoop(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        SPAWNED_AGENTS.clear()
        self.chief = ChiefOfStaff(api_key="mock_api_key", model="gemini-2.5-flash")

    def tearDown(self):
        SPAWNED_AGENTS.clear()

    def test_build_system_prompt_fleet_topology_and_rules(self):
        """Verify dynamic system prompt provides Chief of Staff identity, fleet topology, and zero-PR rules."""
        prompt = build_system_prompt()
        self.assertIn("Chief of Staff", prompt)
        self.assertIn("hermes", prompt)
        self.assertIn("desktop-pc", prompt)
        self.assertIn("oci-machine", prompt)
        self.assertIn("personal-laptop", prompt)
        self.assertIn("ALWAYS PITCH THE PLAN FIRST", prompt)
        self.assertIn("DISPATCH ON APPROVAL", prompt)
        self.assertIn("IN-FLIGHT STATUS", prompt)
        self.assertIn("UNDO & ROLLBACK", prompt)
        self.assertIn("NO UNNECESSARY PRs", prompt)

    def test_format_for_slack_converts_bold_outside_code(self):
        """Verify double asterisks convert to single asterisks in Slack mrkdwn outside code blocks."""
        input_text = "This is **bold** text and `**not bold**` in code block."
        formatted = format_for_slack(input_text)
        self.assertEqual(formatted, "This is *bold* text and `**not bold**` in code block.")

    async def test_turn_1_plan_pitch_does_not_spawn_agent(self):
        """Turn 1: Verify user task request returns a pitched plan asking for approval without calling spawn_agent."""
        thread_history = []
        user_msg = "Create a hello-world FastAPI service in a sandbox"

        # Mock Gemini API returning plan pitch text (no function calls)
        mock_api_response = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "text": "I will dispatch OpenCode to `/Users/hermes/projects/sandbox/fastapi-demo` to build the service on an isolated branch. Ready when you are—say `go` or 👍 to run it."
                    }]
                }
            }]
        }

        with patch("aiohttp.ClientSession.post") as mock_post, \
             patch("agent_core.ChiefOfStaff.execute_tool_call", new_callable=AsyncMock) as mock_tool:

            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=mock_api_response)
            mock_post.return_value.__aenter__.return_value = mock_resp

            reply, updated_history = await self.chief.process_turn(thread_history, user_msg)

            # Verification:
            # 1. Tool call was NOT executed
            mock_tool.assert_not_called()
            # 2. Plan was pitched
            self.assertIn("OpenCode", reply)
            self.assertIn("sandbox", reply)
            self.assertIn("say `go` or 👍", reply)
            # 3. Thread history recorded user and model turns
            self.assertEqual(len(updated_history), 2)
            self.assertEqual(updated_history[0]["role"], "user")
            self.assertEqual(updated_history[1]["role"], "model")

    async def test_turn_2_approval_spawns_agent_with_branch_isolation(self):
        """Turn 2: Verify approval prompt triggers spawn_agent tool call and returns confirmation."""
        thread_history = [
            {"role": "user", "parts": [{"text": "Create a hello-world FastAPI service in a sandbox"}]},
            {"role": "model", "parts": [{"text": "I will dispatch OpenCode to `/Users/hermes/projects/sandbox/fastapi-demo`. Ready when you are—say `go` or 👍 to run it."}]}
        ]
        user_msg = "go"

        # Step 1: Gemini decides to call spawn_agent
        api_step_1 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "functionCall": {
                            "name": "spawn_agent",
                            "args": {
                                "node": "hermes",
                                "cwd": "/Users/hermes/projects/sandbox/fastapi-demo",
                                "worker": "opencode-dev",
                                "instructions": "Build hello world FastAPI service",
                                "isolate_branch": True
                            }
                        }
                    }]
                }
            }]
        }

        # Step 2: Gemini generates final text based on tool result
        api_step_2 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "text": "Dispatched OpenCode to `/Users/hermes/projects/sandbox/fastapi-demo` on branch `task-1234-opencode` (pane `w1:p1`). Monitoring execution."
                    }]
                }
            }]
        }

        with patch("aiohttp.ClientSession.post") as mock_post, \
             patch("agent_core.ChiefOfStaff.execute_tool_call", new_callable=AsyncMock) as mock_tool:

            mock_tool.return_value = {
                "pane_id": "w1:p1",
                "node": "hermes",
                "cwd": "/Users/hermes/projects/sandbox/fastapi-demo",
                "branch": "task-1234-opencode"
            }

            mock_resp_1 = AsyncMock()
            mock_resp_1.status = 200
            mock_resp_1.json = AsyncMock(return_value=api_step_1)

            mock_resp_2 = AsyncMock()
            mock_resp_2.status = 200
            mock_resp_2.json = AsyncMock(return_value=api_step_2)

            mock_post.return_value.__aenter__.side_effect = [mock_resp_1, mock_resp_2]

            reply, updated_history = await self.chief.process_turn(thread_history, user_msg)

            # Verification:
            # 1. spawn_agent was executed with expected args
            mock_tool.assert_called_once_with(
                "spawn_agent",
                {
                    "node": "hermes",
                    "cwd": "/Users/hermes/projects/sandbox/fastapi-demo",
                    "worker": "opencode-dev",
                    "instructions": "Build hello world FastAPI service",
                    "isolate_branch": True
                }
            )
            # 2. Confirmation message returned
            self.assertIn("Dispatched OpenCode", reply)
            self.assertIn("task-1234-opencode", reply)
            # 3. Thread history contains user, model (functionCall), function (functionResponse), model (text)
            self.assertEqual(len(updated_history), 6)
            self.assertEqual(updated_history[2]["role"], "user")
            self.assertEqual(updated_history[3]["role"], "model")
            self.assertEqual(updated_history[4]["role"], "function")
            self.assertEqual(updated_history[5]["role"], "model")

    async def test_turn_3_status_or_url_query_reads_agent_buffer(self):
        """Turn 3: Verify follow-up 'what's the URL?' triggers read_agent and grounds reply."""
        thread_history = [
            {"role": "user", "parts": [{"text": "go"}]},
            {"role": "model", "parts": [{"text": "Dispatched OpenCode to `/Users/hermes/projects/sandbox/fastapi-demo`."}]}
        ]
        user_msg = "what is the URL and is it running?"

        api_step_1 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "functionCall": {
                            "name": "read_agent",
                            "args": {
                                "pane_id": "w1:p1"
                            }
                        }
                    }]
                }
            }]
        }

        api_step_2 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "text": "The service is actively running at `http://localhost:8000`. The server responded to health checks cleanly."
                    }]
                }
            }]
        }

        with patch("aiohttp.ClientSession.post") as mock_post, \
             patch("agent_core.ChiefOfStaff.execute_tool_call", new_callable=AsyncMock) as mock_tool:

            mock_tool.return_value = "Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)\nHealthcheck: 200 OK"

            mock_resp_1 = AsyncMock()
            mock_resp_1.status = 200
            mock_resp_1.json = AsyncMock(return_value=api_step_1)

            mock_resp_2 = AsyncMock()
            mock_resp_2.status = 200
            mock_resp_2.json = AsyncMock(return_value=api_step_2)

            mock_post.return_value.__aenter__.side_effect = [mock_resp_1, mock_resp_2]

            reply, updated_history = await self.chief.process_turn(thread_history, user_msg)

            # Verification:
            mock_tool.assert_called_once_with("read_agent", {"pane_id": "w1:p1"})
            self.assertIn("http://localhost:8000", reply)
            self.assertEqual(len(updated_history), 6)

    async def test_turn_4_undo_triggers_stop_agent_with_rollback(self):
        """Turn 4: Verify 'undo' or 'abort' triggers stop_agent with rollback=True."""
        thread_history = [
            {"role": "user", "parts": [{"text": "what is the URL?"}]},
            {"role": "model", "parts": [{"text": "The service is running at http://localhost:8000"}]}
        ]
        user_msg = "undo and scrap the changes"

        api_step_1 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "functionCall": {
                            "name": "stop_agent",
                            "args": {
                                "pane_id": "w1:p1",
                                "rollback": True
                            }
                        }
                    }]
                }
            }]
        }

        api_step_2 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "text": "Stopped agent pane `w1:p1`, wiped uncommitted changes, and deleted task branch. Baseline restored."
                    }]
                }
            }]
        }

        with patch("aiohttp.ClientSession.post") as mock_post, \
             patch("agent_core.ChiefOfStaff.execute_tool_call", new_callable=AsyncMock) as mock_tool:

            mock_tool.return_value = "Stopped agent pane `w1:p1`. Rolled back working directory `/Users/hermes/projects/sandbox/fastapi-demo` to `main` and deleted branch `task-1234-opencode`."

            mock_resp_1 = AsyncMock()
            mock_resp_1.status = 200
            mock_resp_1.json = AsyncMock(return_value=api_step_1)

            mock_resp_2 = AsyncMock()
            mock_resp_2.status = 200
            mock_resp_2.json = AsyncMock(return_value=api_step_2)

            mock_post.return_value.__aenter__.side_effect = [mock_resp_1, mock_resp_2]

            reply, updated_history = await self.chief.process_turn(thread_history, user_msg)

            # Verification:
            mock_tool.assert_called_once_with("stop_agent", {"pane_id": "w1:p1", "rollback": True})
            self.assertIn("Stopped agent pane", reply)
            self.assertIn("deleted task branch", reply)

    async def test_conversational_approval_lets_go_triggers_herdr_spawn(self):
        """Verify conversational approval 'Let's go' triggers herdr_spawn without re-pitching the plan."""
        thread_history = [
            {"role": "user", "parts": [{"text": "Create a simple test page and host it in a python server"}]},
            {"role": "model", "parts": [{"text": "I will dispatch OpenCode to `/Users/hermes/projects/sandbox/test-server` to create `index.html` and run `python3 -m http.server 54028 --directory /Users/hermes/projects/sandbox/test-server`. Ready when you are—say `go` or 👍 to run it."}]}
        ]
        user_msg = "Let's go"

        api_step_1 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "functionCall": {
                            "name": "herdr_spawn",
                            "args": {
                                "node": "hermes",
                                "cwd": "/Users/hermes/projects/sandbox/test-server",
                                "worker": "opencode-dev",
                                "prompt": "Create index.html and run python3 -m http.server 54028 --directory /Users/hermes/projects/sandbox/test-server"
                            }
                        }
                    }]
                }
            }]
        }

        api_step_2 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "text": "Dispatched OpenCode to `/Users/hermes/projects/sandbox/test-server` on branch `task-4567-opencode` (pane `w4:p2`). The server will be hosted on port 54028."
                    }]
                }
            }]
        }

        with patch("aiohttp.ClientSession.post") as mock_post, \
             patch("agent_core.ChiefOfStaff.execute_tool_call", new_callable=AsyncMock) as mock_tool:

            mock_tool.return_value = {
                "pane_id": "w4:p2",
                "node": "hermes",
                "cwd": "/Users/hermes/projects/sandbox/test-server",
                "branch": "task-4567-opencode"
            }

            mock_resp_1 = AsyncMock()
            mock_resp_1.status = 200
            mock_resp_1.json = AsyncMock(return_value=api_step_1)

            mock_resp_2 = AsyncMock()
            mock_resp_2.status = 200
            mock_resp_2.json = AsyncMock(return_value=api_step_2)

            mock_post.return_value.__aenter__.side_effect = [mock_resp_1, mock_resp_2]

            reply, updated_history = await self.chief.process_turn(thread_history, user_msg)

            # Verified herdr_spawn executed
            mock_tool.assert_called_once()
            call_name, call_args = mock_tool.call_args[0]
            self.assertEqual(call_name, "herdr_spawn")
            self.assertEqual(call_args["cwd"], "/Users/hermes/projects/sandbox/test-server")
            self.assertIn("Dispatched OpenCode", reply)
            self.assertNotIn("Ready when you are", reply)

    async def test_tool_driven_inspection_url_returns_live_url_sentence_1(self):
        """Verify 'Can you give me the URL?' calls inspection tools and returns the live URL in sentence 1."""
        thread_history = [
            {"role": "user", "parts": [{"text": "Let's go"}]},
            {"role": "model", "parts": [{"text": "Dispatched OpenCode to `/Users/hermes/projects/sandbox/test-server`."}]}
        ]
        user_msg = "Can you give me the URL?"

        # Tool call 1: run_command to check listening ports
        api_step_1 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "functionCall": {
                            "name": "run_command",
                            "args": {
                                "cmd": "lsof -iTCP -sTCP:LISTEN -n -P",
                                "cwd": "/Users/hermes/projects/sandbox/test-server"
                            }
                        }
                    }]
                }
            }]
        }

        # Response 2: Direct answer with URL in sentence 1
        api_step_2 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "text": "The test server is live at http://localhost:54028. OpenCode successfully created index.html and started the background server."
                    }]
                }
            }]
        }

        with patch("aiohttp.ClientSession.post") as mock_post, \
             patch("agent_core.ChiefOfStaff.execute_tool_call", new_callable=AsyncMock) as mock_tool:

            mock_tool.return_value = "Python 12345 hermes 3u IPv4 0x123 0t0 TCP *:54028 (LISTEN)"

            mock_resp_1 = AsyncMock()
            mock_resp_1.status = 200
            mock_resp_1.json = AsyncMock(return_value=api_step_1)

            mock_resp_2 = AsyncMock()
            mock_resp_2.status = 200
            mock_resp_2.json = AsyncMock(return_value=api_step_2)

            mock_post.return_value.__aenter__.side_effect = [mock_resp_1, mock_resp_2]

            reply, updated_history = await self.chief.process_turn(thread_history, user_msg)

            # Verification:
            mock_tool.assert_called_once_with(
                "run_command",
                {
                    "cmd": "lsof -iTCP -sTCP:LISTEN -n -P",
                    "cwd": "/Users/hermes/projects/sandbox/test-server"
                }
            )
            # URL is returned directly in sentence 1
            first_sentence = reply.split(".")[0]
            self.assertIn("http://localhost:54028", first_sentence)
            self.assertNotIn("I'll retrieve that for you now", reply)

    async def test_roleplay_trap_intercepts_empty_promises(self):
        """Verify roleplay trap intercepts 'I'll retrieve that for you now', runs tools, and demands facts."""
        SPAWNED_AGENTS["w1:p1"] = {
            "pane_id": "w1:p1",
            "node": "hermes",
            "cwd": "/Users/hermes/projects/sandbox/spike-1",
            "branch": "task-1",
            "worker": "opencode-dev"
        }

        thread_history = []
        user_msg = "What is the URL?"

        # Step 1: Model attempts to reply with empty promise roleplay text
        api_step_1 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "text": "I'll retrieve that for you now. Let me check the running ports."
                    }]
                }
            }]
        }

        # Step 2: After interception injected real inspection data, model outputs factual answer
        api_step_2 = {
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "text": "The server is active at http://localhost:8080."
                    }]
                }
            }]
        }

        with patch("aiohttp.ClientSession.post") as mock_post, \
             patch("agent_core.ChiefOfStaff.execute_tool_call", new_callable=AsyncMock) as mock_tool:

            mock_tool.side_effect = [
                "Serving HTTP on 0.0.0.0 port 8080 (http://0.0.0.0:8080/) ...", # herdr_read
                "Python 1111 hermes 4u TCP *:8080 (LISTEN)" # run_command
            ]

            mock_resp_1 = AsyncMock()
            mock_resp_1.status = 200
            mock_resp_1.json = AsyncMock(return_value=api_step_1)

            mock_resp_2 = AsyncMock()
            mock_resp_2.status = 200
            mock_resp_2.json = AsyncMock(return_value=api_step_2)

            mock_post.return_value.__aenter__.side_effect = [mock_resp_1, mock_resp_2]

            reply, updated_history = await self.chief.process_turn(thread_history, user_msg)

            # Verification: Both herdr_read and run_command were called
            self.assertEqual(mock_tool.call_count, 2)
            self.assertIn("http://localhost:8080", reply)
            self.assertNotIn("I'll retrieve that for you now", reply)


class TestIngressLifecycleStateMachine(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for the robust ingress lifecycle state machine:
    - Boot Splash Guard: Never declares 'done' when agent is idle at the startup splash banner
    - Full execution cycle: WORKING -> BLOCKED -> WORKING -> DONE with clean output extraction
    """

    def setUp(self):
        app.sessions.clear()
        app.agent_name_to_session.clear()

    async def test_boot_splash_rendering_does_not_post_task_completed(self):
        """Simulate CLI boot splash rendering without execution -> assert relay DOES NOT post 'Task Completed'."""
        client = AsyncMock()
        client.chat_postMessage = AsyncMock()
        session = app.GatewaySession(
            session_key="C123:T100",
            agent_name="fm-test-boot",
            target_cwd="/Users/hermes/projects/sandbox/fm-test-boot",
            channel="C123",
            thread_ts="T100"
        )
        app.sessions[session.session_key] = session

        splash_screen = (
            "hermes@hermes fm-test-boot % agy\n"
            "^[_Gi=31;OK^[\\^[[?62;22c\n\n"
            "      ▄▀▀▄        Antigravity CLI 1.2.12\n"
            "     ▀▀▀▀▀▀       k.horned@gmail.com (Google AI Ultra)\n"
            "    ▀▀▀▀▀▀▀▀      Gemini 3.8 Flash (High)\n"
            "   ▄▀▀    ▀▀▄     ~/projects/sandbox/fm-test-boot\n"
            "  ▄▀▀      ▀▀▄\n\n"
            "────────────────────────────────────────────────────────────────────────────────\n"
            ">\n"
            "────────────────────────────────────────────────────────────────────────────────\n"
            "? for shortcuts                                          Gemini 3.8 Flash · high\n"
        )

        async def mock_run_cli(args, timeout=5.0):
            if args[0] == "agent" and args[1] == "get":
                return 0, json.dumps({"result": {"agent": {"agent_status": "idle"}}}), ""
            elif args[0] == "agent" and args[1] == "read":
                return 0, splash_screen, ""
            return 0, "", ""

        with patch.object(app.herdr_client, "run_cli", side_effect=mock_run_cli):
            task = asyncio.create_task(app.monitor_agent_lifecycle(session, client))
            await asyncio.sleep(3.5)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Verification:
        # 1. State invariant preserved: session NEVER transitioned to 'done'
        self.assertNotEqual(session.status, "done")
        self.assertEqual(session.status, "booting")
        self.assertFalse(session.has_worked)
        # 2. No premature completion message was posted to Slack/iMessage
        client.chat_postMessage.assert_not_called()

    async def test_executing_commands_transitions_working_blocked_working_done_with_clean_output(self):
        """Simulate CLI executing commands (WORKING -> BLOCKED -> WORKING -> DONE) -> assert clean output extraction."""
        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "msg-123"})
        client.reactions_add = AsyncMock()
        client.reactions_remove = AsyncMock()

        session = app.GatewaySession(
            session_key="C123:T200",
            agent_name="fm-test-exec",
            target_cwd="/Users/hermes/projects/sandbox/fm-test-exec",
            channel="C123",
            thread_ts="T200"
        )
        app.sessions[session.session_key] = session

        tick = 0

        output_working = "▸ Thought for 2s\n  Creating test page in sandbox...\n● Edit(index.html)\nGenerating..."
        output_blocked = "Do you want to run `python3 -m http.server 8080 --directory /Users/hermes/projects/sandbox/fm-test-exec`?\n[y/n] approval required"
        output_done = (
            "● Run(python3 -m http.server 8080 --directory /Users/hermes/projects/sandbox/fm-test-exec)\n"
            "Serving HTTP on 0.0.0.0 port 8080 (http://127.0.0.1:8080/) ...\n"
            "Created simple test page and launched server at http://127.0.0.1:8080.\n"
            ">\n? for shortcuts"
        )

        async def mock_run_cli(args, timeout=5.0):
            nonlocal tick
            sub = args[1] if len(args) > 1 else ""
            if sub == "get":
                if tick == 0:
                    return 0, json.dumps({"result": {"agent": {"agent_status": "working"}}}), ""
                elif tick == 1:
                    return 0, json.dumps({"result": {"agent": {"agent_status": "blocked"}}}), ""
                elif tick == 2:
                    return 0, json.dumps({"result": {"agent": {"agent_status": "working"}}}), ""
                else:
                    return 0, json.dumps({"result": {"agent": {"agent_status": "idle"}}}), ""
            elif sub == "read":
                if tick == 0:
                    return 0, output_working, ""
                elif tick == 1:
                    return 0, output_blocked, ""
                elif tick == 2:
                    return 0, output_working, ""
                else:
                    return 0, output_done, ""
            return 0, "", ""

        with patch.object(app.herdr_client, "run_cli", side_effect=mock_run_cli):
            task = asyncio.create_task(app.monitor_agent_lifecycle(session, client, trigger_ts="T200"))

            # Tick 0: Working
            await asyncio.sleep(1.6)
            self.assertTrue(session.has_worked)
            self.assertEqual(session.status, "working")

            # Tick 1: Blocked (Ask Mode)
            tick = 1
            await asyncio.sleep(1.6)
            self.assertEqual(session.status, "blocked")
            # Verify approval card was posted
            client.chat_postMessage.assert_called()
            self.assertIn("Approval required", client.chat_postMessage.call_args_list[0][1]["text"])

            # User approves: transition back to working
            tick = 2
            session.status = "working"
            await asyncio.sleep(1.6)
            self.assertEqual(session.status, "working")

            # Tick 3: Done
            tick = 3
            await asyncio.sleep(3.2)
            self.assertEqual(session.status, "done")

            # Verify final completion briefing was posted with clean output and live URL banner
            final_call = client.chat_postMessage.call_args_list[-1]
            final_text = final_call[1]["text"]
            self.assertIn("*Task Completed via Herdr", final_text)
            self.assertIn("*Live Endpoint:* http://127.0.0.1:8080", final_text)
            self.assertIn("Created simple test page", final_text)
            self.assertNotIn("Antigravity CLI 1.2.12", final_text)
            self.assertNotIn("? for shortcuts", final_text)

    async def test_agent_start_failure_fails_fast_and_updates_ui(self):
        """Simulate Herdr returning invalid_agent_argument -> verify session marks failed, reacji swaps to :x:, and no infinite loop occurs."""
        client = AsyncMock()
        client.reactions_add = AsyncMock()
        client.reactions_remove = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "msg-err"})

        event = {
            "channel": "C_TEST",
            "ts": "100.999",
            "thread_ts": "100.999",
            "text": "run complex multi-line task\nwith special chars",
            "is_imessage": False
        }

        mock_ws_output = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w_failed"},
                "root_pane": {"pane_id": "w_failed:p1"}
            }
        })
        error_json = json.dumps({
            "error": {
                "code": "invalid_agent_argument",
                "message": "agent arguments cannot be encoded safely for the target shell"
            },
            "id": "cli:agent:start"
        })

        async def mock_run_cli(args, timeout=5.0):
            if args[:2] == ["workspace", "create"]:
                return 0, mock_ws_output, ""
            elif args[:2] == ["agent", "start"]:
                return 1, error_json, "agent arguments cannot be encoded safely for the target shell"
            elif args[:2] == ["workspace", "close"]:
                return 0, json.dumps({"result": {"type": "ok"}}), ""
            return 0, "", ""

        with patch.object(app.herdr_client, "run_cli", side_effect=mock_run_cli):
            await app.process_user_prompt(event, client)

        session_key = "C_TEST:100.999"
        self.assertIn(session_key, app.sessions)
        session = app.sessions[session_key]

        # 1. State invariant: session marked as failed
        self.assertEqual(session.status, "failed")
        self.assertEqual(session.state, "failed")
        self.assertFalse(session.has_worked)

        # 2. Reacji updated to :x:
        reacji_calls = [c[1]["name"] for c in client.reactions_add.call_args_list]
        self.assertIn("x", reacji_calls)

        # 3. Diagnostic error posted to thread
        client.chat_postMessage.assert_called()
        posted_text = client.chat_postMessage.call_args_list[0][1]["text"]
        self.assertIn("Failed to spawn Antigravity agent", posted_text)
        self.assertIn("agent arguments cannot be encoded safely", posted_text)

        # 4. No infinite loop / monitor task running
        self.assertTrue(session.monitor_task is None or session.monitor_task.done())

    async def test_readiness_handshake_delivers_prompt(self):
        """Simulate clean start -> wait for ready prompt -> verify herdr agent prompt dispatches only after TUI initialization."""
        client = AsyncMock()
        client.reactions_add = AsyncMock()
        client.reactions_remove = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "msg-ok"})

        event = {
            "channel": "C_TEST",
            "ts": "200.999",
            "thread_ts": "200.999",
            "text": "build a dashboard",
            "is_imessage": False
        }

        mock_ws_output = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w_handshake"},
                "root_pane": {"pane_id": "w_handshake:p1"}
            }
        })

        cli_call_log = []
        poll_count = 0

        async def mock_run_cli(args, timeout=5.0):
            nonlocal poll_count
            cli_call_log.append(list(args))
            sub = args[1] if len(args) > 1 else ""

            if args[0] == "workspace" and sub == "create":
                return 0, mock_ws_output, ""
            elif args[0] == "agent" and sub == "start":
                return 0, json.dumps({"result": {"type": "ok"}}), ""
            elif args[0] == "agent" and sub == "get":
                poll_count += 1
                if poll_count < 2:
                    return 0, json.dumps({"result": {"agent": {"agent_status": "starting", "interactive_ready": False}}}), ""
                else:
                    return 0, json.dumps({"result": {"agent": {"agent_status": "idle", "interactive_ready": True}}}), ""
            elif args[0] == "agent" and sub == "read":
                if poll_count < 2:
                    return 0, "signing in...\n", ""
                else:
                    return 0, "Antigravity CLI\n>\n? for shortcuts", ""
            elif args[0] == "agent" and sub == "prompt":
                return 0, json.dumps({"result": {"type": "ok"}}), ""
            return 0, "", ""

        with patch.object(app.herdr_client, "run_cli", side_effect=mock_run_cli):
            await app.process_user_prompt(event, client)

        session_key = "C_TEST:200.999"
        self.assertIn(session_key, app.sessions)
        session = app.sessions[session_key]

        # 1. Verify agent start was clean (zero trailing prompt args)
        start_calls = [c for c in cli_call_log if c[:2] == ["agent", "start"]]
        self.assertEqual(len(start_calls), 1)
        self.assertEqual(start_calls[0], ["agent", "start", session.agent_name, "--kind", "agy", "--pane", "w_handshake:p1"])

        # 2. Verify agent prompt was dispatched AFTER readiness check
        prompt_calls = [c for c in cli_call_log if c[:2] == ["agent", "prompt"]]
        self.assertEqual(len(prompt_calls), 1)
        self.assertEqual(prompt_calls[0][2], session.agent_name)
        self.assertIn("build a dashboard", prompt_calls[0][3])

        # Verify order: start -> get (not ready) -> get (ready) -> prompt
        start_idx = next(i for i, c in enumerate(cli_call_log) if c[:2] == ["agent", "start"])
        prompt_idx = next(i for i, c in enumerate(cli_call_log) if c[:2] == ["agent", "prompt"])
        self.assertGreater(prompt_idx, start_idx)

        # Clean up session monitor task
        if session.monitor_task and not session.monitor_task.done():
            session.monitor_task.cancel()

    async def test_lifecycle_watchdog_times_out_stuck_booting_agent(self):
        """Verify monitor_agent_lifecycle enforces strict timeout if agent remains booting > 30s."""
        client = AsyncMock()
        client.reactions_add = AsyncMock()
        client.reactions_remove = AsyncMock()
        client.chat_postMessage = AsyncMock()

        session = app.GatewaySession(
            session_key="C_TEST:300.999",
            agent_name="fm-test-timeout",
            target_cwd="/tmp/timeout",
            channel="C_TEST",
            thread_ts="300.999",
            workspace_id="w_timeout"
        )
        session.status = "booting"
        session.has_worked = False

        async def mock_run_cli(args, timeout=5.0):
            if args[:2] == ["agent", "get"]:
                return 0, json.dumps({"result": {"agent": {"agent_status": "starting"}}}), ""
            elif args[:2] == ["agent", "read"]:
                return 0, "signing in...\n", ""
            elif args[:2] == ["workspace", "close"]:
                return 0, "ok", ""
            return 0, "", ""

        # Mock time.time to simulate passage of 35 seconds
        current_mock_time = [1000.0]
        def mock_time():
            return current_mock_time[0]

        with patch.object(app.herdr_client, "run_cli", side_effect=mock_run_cli), \
             patch("time.time", side_effect=mock_time):
            async def advance_time():
                await asyncio.sleep(0.05)
                current_mock_time[0] += 35.0

            adv_task = asyncio.create_task(advance_time())
            monitor_task = asyncio.create_task(app.monitor_agent_lifecycle(session, client, trigger_ts="300.999"))
            await asyncio.gather(adv_task, monitor_task)

        # Verification: session marked as failed, timeout message sent, reacji :x:
        self.assertEqual(session.status, "failed")
        reacji_calls = [c[1]["name"] for c in client.reactions_add.call_args_list]
        self.assertIn("x", reacji_calls)
        client.chat_postMessage.assert_called()
        self.assertIn("timed out during initialization (>30s)", client.chat_postMessage.call_args_list[0][1]["text"])


if __name__ == "__main__":
    unittest.main()

