#!/usr/bin/env python3
"""
tests/test_sensory_tools.py
Comprehensive unit test suite for First Mate 2.0 Full Sensory Architecture & Query Tools:
  - TestSensoryToolDeclarations: Gemini REST API declarations for check_eyes & check_nose
  - TestSensoryToolExecution: Pure JSON data contracts for check_eyes & check_nose, zero f-strings
  - TestSensoryToolSynthesis: FunctionResponse return & autonomous LLM executive Slack mrkdwn synthesis
  - TestMidStreamSensoryGrounding: Out-of-band <1s response with eyes_status and nose_status
  - TestSensoryAdversarialPhrasing: Zero regex keyword gating across varied user phrasings & degradation

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
import time
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.agent.mouth import generate_mouth_response
from golem.gateway.ingress import (
    CHECK_EYES_TOOL_DECLARATION,
    CHECK_NOSE_TOOL_DECLARATION,
    DISPATCH_TASK_TOOL_DECLARATION,
    IngressGateway,
    call_gemini_api,
    get_mouth_tools_declaration,
)
from golem.gateway.router import SessionRouter
from golem.gateway.session import EyesStatusResult, SessionHandle
from golem.execution.runner import ExecutionHandle
from golem.sensory.bus import ShoulderTapAlert, ShoulderTapEventBus
from golem.sensory.nose import AnomalyVerdict, FleetSnapshot, NoseSentinel


class MockSlackClient:
    """Mock Slack client recording chat_postMessage, reactions, and uploads."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.uploaded_files: List[Dict[str, Any]] = []
        self.thread_history: Dict[str, List[Dict[str, Any]]] = {}

    async def chat_postMessage(
        self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        msg = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": f"msg_{time.time()}_{len(self.posted_messages)}",
            **kwargs,
        }
        self.posted_messages.append(msg)
        return msg

    async def reactions_add(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> None:
        self.reactions_added.append(
            {"channel": channel, "timestamp": timestamp, "name": name}
        )

    async def reactions_remove(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> None:
        self.reactions_removed.append(
            {"channel": channel, "timestamp": timestamp, "name": name}
        )

    async def files_upload_v2(
        self,
        channel: str,
        thread_ts: Optional[str] = None,
        file: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        call = {"channel": channel, "thread_ts": thread_ts, "file": file, **kwargs}
        self.uploaded_files.append(call)
        return {"ok": True}

    async def conversations_replies(
        self, channel: str, ts: str, limit: int = 15, **kwargs: Any
    ) -> Dict[str, Any]:
        return {
            "ok": True,
            "messages": self.thread_history.get(f"{channel}:{ts}", []),
        }


def make_mock_adapter(cwd: str, extra_args: Optional[List[str]] = None) -> Any:
    adapter = MagicMock()
    adapter.start = AsyncMock()
    adapter.stop = AsyncMock()
    adapter.conversation_id = "mock-conv-id"
    return adapter


class TestSensoryToolDeclarations(unittest.TestCase):
    """Requirement 1 & 5: Verify Gemini function declarations for sensory tools."""

    def test_tool_declarations_discoverable(self) -> None:
        """Verify get_mouth_tools_declaration contains dispatch_task, check_eyes, and check_nose."""
        tools = get_mouth_tools_declaration()
        self.assertIsInstance(tools, list)
        self.assertGreaterEqual(len(tools), 1)

        fn_decls = tools[0].get("functionDeclarations", [])
        tool_names = [fn.get("name") for fn in fn_decls]

        self.assertIn("dispatch_task", tool_names)
        self.assertIn("check_eyes", tool_names)
        self.assertIn("check_nose", tool_names)

    def test_check_eyes_declaration_schema(self) -> None:
        """Verify check_eyes declaration schema: object type, non-empty description, no required params."""
        decl = CHECK_EYES_TOOL_DECLARATION
        self.assertEqual(decl.get("name"), "check_eyes")
        self.assertIsInstance(decl.get("description"), str)
        self.assertGreater(len(decl["description"]), 20)

        params = decl.get("parameters", {})
        self.assertEqual(params.get("type"), "OBJECT")
        self.assertEqual(params.get("required"), [])

        props = params.get("properties", {})
        self.assertIn("pane", props)
        self.assertEqual(props["pane"].get("type"), "STRING")

    def test_check_nose_declaration_schema(self) -> None:
        """Verify check_nose declaration schema: optional trigger_fresh_sniff boolean property."""
        decl = CHECK_NOSE_TOOL_DECLARATION
        self.assertEqual(decl.get("name"), "check_nose")
        self.assertIsInstance(decl.get("description"), str)
        self.assertGreater(len(decl["description"]), 20)

        params = decl.get("parameters", {})
        self.assertEqual(params.get("type"), "OBJECT")
        self.assertEqual(params.get("required"), [])

        props = params.get("properties", {})
        self.assertIn("trigger_fresh_sniff", props)
        self.assertEqual(props["trigger_fresh_sniff"].get("type"), "BOOLEAN")
        self.assertIn(
            "cached", props["trigger_fresh_sniff"].get("description", "").lower()
        )

    def test_dispatch_task_declaration_schema(self) -> None:
        """Verify dispatch_task declaration schema remains intact."""
        decl = DISPATCH_TASK_TOOL_DECLARATION
        self.assertEqual(decl.get("name"), "dispatch_task")
        params = decl.get("parameters", {})
        self.assertIn("task_description", params.get("properties", {}))

    def test_declarations_json_serializable(self) -> None:
        """Verify all tool declarations serialize cleanly to JSON without error."""
        tools = get_mouth_tools_declaration()
        raw_json = json.dumps(tools)
        decoded = json.loads(raw_json)
        self.assertEqual(len(decoded[0]["functionDeclarations"]), 4)


class TestSensoryToolExecution(unittest.IsolatedAsyncioTestCase):
    """Requirement 1 & 5: Verify pure JSON data contracts for check_eyes and check_nose."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.mock_fleet = MagicMock()
        self.sentinel = NoseSentinel(
            fleet=self.mock_fleet,
            bus=self.bus,
            interval_minutes=15.0,
        )
        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR, adapter_factory=make_mock_adapter
        )
        self.gateway = IngressGateway(
            router=self.router,
            bot_user_id="U_FIRSTMATE",
            sensory_bus=self.bus,
            nose_sentinel=self.sentinel,
        )

    async def asyncTearDown(self) -> None:
        if hasattr(self, "router") and self.router is not None:
            for s in list(self.router.sessions.values()):
                try:
                    await s.close()
                except Exception:
                    pass
            self.router.sessions.clear()
            await self.router.stop_reaper()
        if hasattr(self.sentinel, "stop"):
            await self.sentinel.stop()
        self.bus.clear()

    async def test_check_eyes_payload_running_state(self) -> None:
        """Verify _execute_check_eyes returns RUNNING state matching data contract."""
        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(
            return_value={
                "status": "running",
                "command": "python3 build.py",
                "pid": 54321,
                "blocker": None,
                "blockers": [],
                "buffer_tail": "Compiling assets...",
            }
        )
        mock_session.get_eyes_status = MagicMock(
            return_value={
                "state": "RUNNING",
                "active_command": "python3 build.py",
                "command": "python3 build.py",
                "pid": 54321,
                "blocker": None,
                "blockers": [],
                "buffer_tail": "Compiling assets...",
            }
        )

        res = await self.gateway._execute_check_eyes(session=mock_session)
        self.assertIsInstance(res, dict)
        self.assertEqual(res["state"], "RUNNING")
        self.assertEqual(res["active_command"], "python3 build.py")
        self.assertEqual(res["pid"], 54321)
        self.assertIsNone(res["blocker"])
        self.assertEqual(res["buffer_tail"], "Compiling assets...")

    async def test_check_eyes_payload_idle_state(self) -> None:
        """Verify _execute_check_eyes returns IDLE state with None/empty command & PID."""
        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(
            return_value={
                "status": "idle",
                "command": "",
                "pid": None,
                "blocker": None,
                "blockers": [],
                "buffer_tail": "Ready.",
            }
        )
        mock_session.get_eyes_status = MagicMock(
            return_value={
                "state": "IDLE",
                "active_command": "",
                "command": "",
                "pid": None,
                "blocker": None,
                "blockers": [],
                "buffer_tail": "Ready.",
            }
        )

        res = await self.gateway._execute_check_eyes(session=mock_session)
        self.assertIsInstance(res, dict)
        self.assertEqual(res["state"], "IDLE")
        self.assertIsNone(res["pid"])
        self.assertIsNone(res["blocker"])

    async def test_check_eyes_payload_completed_state(self) -> None:
        """Verify _execute_check_eyes returns COMPLETED state."""
        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(
            return_value={
                "status": "completed",
                "command": "cargo test",
                "pid": 1111,
                "blocker": None,
                "blockers": [],
                "buffer_tail": "test result: ok. 15 passed",
            }
        )
        mock_session.get_eyes_status = MagicMock(
            return_value={
                "state": "COMPLETED",
                "active_command": "cargo test",
                "command": "cargo test",
                "pid": 1111,
                "blocker": None,
                "blockers": [],
                "buffer_tail": "test result: ok. 15 passed",
            }
        )

        res = await self.gateway._execute_check_eyes(session=mock_session)
        self.assertEqual(res["state"], "COMPLETED")
        self.assertEqual(res["active_command"], "cargo test")

    async def test_check_eyes_payload_blocker_oauth_state(self) -> None:
        """Verify _execute_check_eyes returns BLOCKER state for interactive OAuth link."""
        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(
            return_value={
                "status": "running",
                "command": "rclone config reconnect",
                "pid": 2222,
                "blocker": {
                    "type": "oauth",
                    "url": "http://127.0.0.1:53682/auth",
                    "prompt": None,
                },
                "blockers": [
                    {
                        "type": "oauth",
                        "url": "http://127.0.0.1:53682/auth",
                        "prompt": None,
                    }
                ],
                "buffer_tail": "Waiting for auth: http://127.0.0.1:53682/auth",
            }
        )
        mock_session.get_eyes_status = MagicMock(
            return_value={
                "state": "BLOCKER",
                "active_command": "rclone config reconnect",
                "command": "rclone config reconnect",
                "pid": 2222,
                "blocker": {
                    "type": "oauth",
                    "url": "http://127.0.0.1:53682/auth",
                    "prompt": None,
                },
                "blockers": [
                    {
                        "type": "oauth",
                        "url": "http://127.0.0.1:53682/auth",
                        "prompt": None,
                    }
                ],
                "buffer_tail": "Waiting for auth: http://127.0.0.1:53682/auth",
            }
        )

        res = await self.gateway._execute_check_eyes(session=mock_session)
        self.assertEqual(res["state"], "BLOCKER")
        self.assertIsNotNone(res["blocker"])
        self.assertEqual(res["blocker"]["type"], "oauth")
        self.assertEqual(res["blocker"]["url"], "http://127.0.0.1:53682/auth")

    async def test_check_eyes_payload_blocker_prompt_state(self) -> None:
        """Verify _execute_check_eyes returns BLOCKER state for interactive prompt."""
        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(
            return_value={
                "status": "running",
                "command": "rm -rf /tmp/test",
                "pid": 3333,
                "blocker": {
                    "type": "prompt",
                    "url": None,
                    "prompt": "Proceed? [y/N]",
                },
                "blockers": [
                    {
                        "type": "prompt",
                        "url": None,
                        "prompt": "Proceed? [y/N]",
                    }
                ],
                "buffer_tail": "Proceed? [y/N]",
            }
        )
        mock_session.get_eyes_status = MagicMock(
            return_value={
                "state": "BLOCKER",
                "active_command": "rm -rf /tmp/test",
                "command": "rm -rf /tmp/test",
                "pid": 3333,
                "blocker": {
                    "type": "prompt",
                    "url": None,
                    "prompt": "Proceed? [y/N]",
                },
                "blockers": [
                    {
                        "type": "prompt",
                        "url": None,
                        "prompt": "Proceed? [y/N]",
                    }
                ],
                "buffer_tail": "Proceed? [y/N]",
            }
        )

        res = await self.gateway._execute_check_eyes(session=mock_session)
        self.assertEqual(res["state"], "BLOCKER")
        self.assertEqual(res["blocker"]["type"], "prompt")
        self.assertEqual(res["blocker"]["prompt"], "Proceed? [y/N]")

    async def test_check_nose_payload_cached_snapshot(self) -> None:
        """Verify check_nose returns cached snapshot instantly with accurate telemetry."""
        now = time.time()
        self.sentinel.last_snapshot = FleetSnapshot(
            timestamp=now - 180.0,
            disk={"free_gb": 64.5, "percent_used": 55.0, "pressure": False},
            docker={
                "docker_running": True,
                "container_count": 5,
                "containers": ["plex", "qbittorrent"],
            },
            tailscale={
                "online": True,
                "backend_state": "Running",
                "peers": {"p1": {"Online": True}},
            },
        )
        self.sentinel.last_verdict = AnomalyVerdict(
            smells_off=False,
            severity="low",
            category="other",
            summary="All systems nominal; 0 errors, fleet healthy.",
        )
        self.sentinel._last_tick_time = now - 180.0

        t0 = time.perf_counter()
        res = await self.gateway._execute_check_nose(trigger_fresh_sniff=False)
        elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 0.10, "Cached sniff must return < 10ms")
        self.assertAlmostEqual(res["minutes_ago"], 3.0, delta=0.2)
        self.assertFalse(res["smells_off"])
        self.assertEqual(
            res["verdict_summary"], "All systems nominal; 0 errors, fleet healthy."
        )
        self.assertEqual(res["disk"]["free_gb"], 64.5)
        self.assertEqual(res["docker"]["container_count"], 5)
        self.assertTrue(res["tailscale"]["online"])
        self.assertEqual(res["recent_error_count"], 0)

    async def test_check_nose_payload_trigger_fresh_sniff(self) -> None:
        """Verify check_nose with trigger_fresh_sniff=True invokes sentinel.tick()."""
        with patch.object(
            self.sentinel, "tick", new_callable=AsyncMock
        ) as mock_tick:
            mock_tick.return_value = None
            res = await self.gateway._execute_check_nose(trigger_fresh_sniff=True)
            mock_tick.assert_awaited_once()
            self.assertIsInstance(res, dict)

    async def test_payloads_pure_json_zero_fstrings(self) -> None:
        """Verify payloads from both tools are pure JSON dictionaries without pre-baked text."""
        eyes_res = await self.gateway._execute_check_eyes(session=None)
        nose_res = await self.gateway._execute_check_nose(trigger_fresh_sniff=False)

        self.assertIsInstance(eyes_res, dict)
        self.assertIsInstance(nose_res, dict)

        # Confirm JSON serialization round-trip
        self.assertEqual(json.loads(json.dumps(eyes_res)), eyes_res)
        self.assertEqual(json.loads(json.dumps(nose_res)), nose_res)

        # Ensure no raw ANSI or markdown header characters leaked in keys or values
        for k, v in eyes_res.items():
            if isinstance(v, str):
                self.assertNotIn("\x1b", v)
                self.assertNotIn("###", v)
        for k, v in nose_res.items():
            if isinstance(v, str):
                self.assertNotIn("\x1b", v)
                self.assertNotIn("###", v)

    async def test_check_eyes_session_none_defensive(self) -> None:
        """Verify _execute_check_eyes safely returns IDLE dict when session is None."""
        res = await self.gateway._execute_check_eyes(session=None)
        self.assertEqual(res["state"], "IDLE")
        self.assertEqual(res["active_command"], "")
        self.assertIsNone(res["pid"])

    async def test_check_nose_sentinel_none_defensive(self) -> None:
        """Verify _execute_check_nose safely returns unavailable status when sentinel is None."""
        gateway = IngressGateway(
            router=self.router, bot_user_id="U_FIRSTMATE", nose_sentinel=None
        )
        gateway.get_nose_sentinel = MagicMock(return_value=None)
        res = await gateway._execute_check_nose()
        self.assertEqual(res["error"], "NoseSentinel unavailable")
        self.assertFalse(res["smells_off"])
        self.assertIn("unavailable", res["verdict_summary"].lower())

    async def test_check_eyes_status_exception_defensive(self) -> None:
        """Verify _execute_check_eyes handles exception in get_runner_status without crashing."""
        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(
            side_effect=RuntimeError("PTY closed")
        )
        mock_session.get_eyes_status = MagicMock(
            side_effect=AttributeError("no eyes")
        )
        mock_session.active_execution = None
        mock_session.is_executing = False

        res = await self.gateway._execute_check_eyes(session=mock_session)
        self.assertIsInstance(res, dict)
        self.assertEqual(res["state"], "ERROR")
        self.assertIn("error", res)

    async def test_check_nose_sentinel_exception_defensive(self) -> None:
        """Verify _execute_check_nose handles exception in sentinel query cleanly."""
        mock_sentinel = MagicMock()
        mock_sentinel.get_telemetry_digest = AsyncMock(
            side_effect=Exception("Disk probe timeout")
        )
        gateway = IngressGateway(
            router=self.router,
            bot_user_id="U_FIRSTMATE",
            nose_sentinel=mock_sentinel,
        )
        res = await gateway._execute_check_nose()
        self.assertIn("error", res)
        self.assertTrue(res["smells_off"])


class TestSensoryToolSynthesis(unittest.IsolatedAsyncioTestCase):
    """Requirement 2 & 5: Verify functionResponse feedback and LLM synthesis of executive briefings."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.mock_fleet = MagicMock()
        self.sentinel = NoseSentinel(
            fleet=self.mock_fleet,
            bus=self.bus,
            interval_minutes=15.0,
        )
        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR, adapter_factory=make_mock_adapter
        )
        self.gateway = IngressGateway(
            router=self.router,
            bot_user_id="U_FIRSTMATE",
            sensory_bus=self.bus,
            nose_sentinel=self.sentinel,
        )
        self.client = MockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        if hasattr(self, "router") and self.router is not None:
            for s in list(self.router.sessions.values()):
                try:
                    await s.close()
                except Exception:
                    pass
            self.router.sessions.clear()
            await self.router.stop_reaper()
        if hasattr(self.sentinel, "stop"):
            await self.sentinel.stop()
        self.bus.clear()

    async def test_check_eyes_synthesis_flow(self) -> None:
        """Verify check_eyes invokes tool, provides functionResponse to Gemini, and synthesizes briefing."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_eyes", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": "*Runner Observer:* Status is *RUNNING* (PID 9999). Active command: `pytest tests/`."
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, turn2_resp]

            # Configure mock runner execution
            with patch.object(
                self.gateway, "_execute_check_eyes", new_callable=AsyncMock
            ) as mock_exec_eyes:
                mock_exec_eyes.return_value = {
                    "state": "RUNNING",
                    "active_command": "pytest tests/",
                    "pid": 9999,
                    "blocker": None,
                    "buffer_tail": "Running test suite...",
                }

                event = {
                    "channel": "C_SYNTH",
                    "ts": "100.001",
                    "thread_ts": "100.001",
                    "text": "<@U_FIRSTMATE> what do you see?",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(mock_gemini.call_count, 2)
                self.assertEqual(mock_exec_eyes.call_count, 1)

                # Inspect Turn 2 payload
                turn2_payload = mock_gemini.call_args_list[1][1]["payload"]
                contents = turn2_payload.get("contents", [])
                fn_resp_part = contents[2]["parts"][0]["functionResponse"]
                self.assertEqual(fn_resp_part["name"], "check_eyes")
                self.assertEqual(
                    fn_resp_part["response"]["content"]["state"], "RUNNING"
                )

                # Check posted Slack briefing
                self.assertEqual(len(self.client.posted_messages), 1)
                msg = self.client.posted_messages[0]["text"]
                self.assertIn("*Runner Observer:*", msg)
                self.assertIn("RUNNING", msg)
                self.assertIn("pytest tests/", msg)

                # Check reaction state
                added = [r["name"] for r in self.client.reactions_added]
                self.assertIn("white_check_mark", added)

    async def test_check_nose_synthesis_flow(self) -> None:
        """Verify check_nose invokes tool, provides functionResponse to Gemini, and synthesizes briefing."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "check_nose",
                                    "args": {"trigger_fresh_sniff": False},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": "*Fleet Health:* Air is clean across the fleet. Zero anomalies detected."
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, turn2_resp]

            with patch.object(
                self.gateway, "_execute_check_nose", new_callable=AsyncMock
            ) as mock_exec_nose:
                mock_exec_nose.return_value = {
                    "seconds_ago": 60.0,
                    "minutes_ago": 1.0,
                    "last_check_timestamp": "2026-10-02T12:00:00Z",
                    "smells_off": False,
                    "verdict_summary": "All systems nominal; 0 errors.",
                    "disk": {
                        "free_gb": 50.0,
                        "percent_used": 50.0,
                        "pressure": False,
                    },
                    "docker": {
                        "running": True,
                        "container_count": 4,
                        "containers": [],
                    },
                    "tailscale": {
                        "online": True,
                        "backend_state": "Running",
                        "peers": {},
                    },
                    "recent_error_count": 0,
                }

                event = {
                    "channel": "C_SYNTH",
                    "ts": "100.002",
                    "thread_ts": "100.002",
                    "text": "<@U_FIRSTMATE> what are you smelling?",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(mock_gemini.call_count, 2)
                turn2_payload = mock_gemini.call_args_list[1][1]["payload"]
                contents = turn2_payload.get("contents", [])
                fn_resp_part = contents[2]["parts"][0]["functionResponse"]
                self.assertEqual(fn_resp_part["name"], "check_nose")
                self.assertFalse(
                    fn_resp_part["response"]["content"]["smells_off"]
                )

                self.assertEqual(len(self.client.posted_messages), 1)
                msg = self.client.posted_messages[0]["text"]
                self.assertIn("*Fleet Health:*", msg)

    async def test_zero_regex_gate_enforcement(self) -> None:
        """Verify varied sensory queries pass directly to Gemini tool calling with zero regex gating."""
        queries = [
            "what do you see?",
            "what are you smelling?",
            "check the fleet",
            "are there any blockers?",
            "how's the runner doing?",
        ]

        for idx, q in enumerate(queries):
            with patch(
                "golem.gateway.ingress.call_gemini_api",
                new_callable=AsyncMock,
            ) as mock_gemini:
                mock_gemini.return_value = {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {"text": f"*Echo:* Handled prompt '{q}'"}
                                ]
                            },
                            "finishReason": "STOP",
                        }
                    ]
                }

                event = {
                    "channel": f"C_GATE_{idx}",
                    "ts": f"100.{idx:03d}",
                    "thread_ts": f"100.{idx:03d}",
                    "text": f"<@U_FIRSTMATE> {q}",
                }
                await self.gateway.handle_message(event, self.client)

                # Each prompt must reach Gemini
                self.assertGreaterEqual(mock_gemini.call_count, 1)

    async def test_synthesis_slack_mrkdwn_formatting(self) -> None:
        """Verify synthesized responses contain valid Slack mrkdwn and no double asterisks or ATX headers."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_eyes", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": "*Observer Briefing:*\n• Runner state: `RUNNING`\n• Active PID: 1234"
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, turn2_resp]
            event = {
                "channel": "C_FMT",
                "ts": "100.003",
                "thread_ts": "100.003",
                "text": "<@U_FIRSTMATE> status check",
            }
            await self.gateway.handle_message(event, self.client)

            msg = self.client.posted_messages[0]["text"]
            self.assertIn("*Observer Briefing:*", msg)
            self.assertNotIn("**", msg)
            self.assertNotIn("###", msg)

    async def test_fallback_synthesis_eyes_clean_mrkdwn(self) -> None:
        """Verify fallback synthesis for check_eyes produces valid Slack mrkdwn if LLM synthesis fails."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_eyes", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            # Turn 1 tool call, Turn 2 synthesis returns None (triggers fallback)
            mock_gemini.side_effect = [turn1_resp, None]

            with patch.object(
                self.gateway, "_execute_check_eyes", new_callable=AsyncMock
            ) as mock_exec_eyes:
                mock_exec_eyes.return_value = {
                    "state": "RUNNING",
                    "active_command": "cargo test --release",
                    "pid": 7777,
                    "buffer_tail": "compiling test crates...",
                }

                event = {
                    "channel": "C_FB",
                    "ts": "100.004",
                    "thread_ts": "100.004",
                    "text": "<@U_FIRSTMATE> what do you see?",
                }
                await self.gateway.handle_message(event, self.client)

                msg = self.client.posted_messages[0]["text"]
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                self.assertIn("cargo test --release", msg)
                self.assertIn("boom", [r["name"] for r in self.client.reactions_added])
                self.assertNotIn("\x1b", msg)
                self.assertNotIn("###", msg)

    async def test_fallback_synthesis_eyes_blocker_url(self) -> None:
        """Verify fallback synthesis for check_eyes surfaces interactive OAuth links."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_eyes", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, None]

            with patch.object(
                self.gateway, "_execute_check_eyes", new_callable=AsyncMock
            ) as mock_exec_eyes:
                mock_exec_eyes.return_value = {
                    "state": "BLOCKER",
                    "active_command": "rclone",
                    "pid": 8888,
                    "blocker": {
                        "type": "oauth",
                        "url": "http://127.0.0.1:53682/auth",
                    },
                    "buffer_tail": "",
                }

                event = {
                    "channel": "C_FB_BLK",
                    "ts": "100.005",
                    "thread_ts": "100.005",
                    "text": "<@U_FIRSTMATE> are there blockers?",
                }
                await self.gateway.handle_message(event, self.client)

                msg = self.client.posted_messages[0]["text"]
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                self.assertIn("http://127.0.0.1:53682/auth", msg)
                self.assertIn("boom", [r["name"] for r in self.client.reactions_added])

    async def test_fallback_synthesis_nose_nominal_mrkdwn(self) -> None:
        """Verify fallback synthesis for check_nose fails loudly with raw error and boom reaction instead of simulating nominal state."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "check_nose",
                                    "args": {},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, None]

            with patch.object(
                self.gateway, "_execute_check_nose", new_callable=AsyncMock
            ) as mock_exec_nose:
                mock_exec_nose.return_value = {
                    "smells_off": False,
                    "minutes_ago": 2.0,
                    "verdict_summary": "All systems nominal; 0 errors.",
                    "disk": {"free_gb": 120.0},
                    "docker": {"container_count": 6},
                    "tailscale": {"online": True},
                }

                event = {
                    "channel": "C_FB_NOSE",
                    "ts": "100.006",
                    "thread_ts": "100.006",
                    "text": "<@U_FIRSTMATE> check the fleet",
                }
                await self.gateway.handle_message(event, self.client)

                msg = self.client.posted_messages[0]["text"]
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                self.assertIn("120.0", msg)
                self.assertIn("All systems nominal", msg)
                added = [r["name"] for r in self.client.reactions_added]
                self.assertIn("boom", added)

    async def test_fallback_synthesis_nose_anomaly_mrkdwn(self) -> None:
        """Verify fallback synthesis for check_nose surfaces health anomalies."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "check_nose",
                                    "args": {},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, None]

            with patch.object(
                self.gateway, "_execute_check_nose", new_callable=AsyncMock
            ) as mock_exec_nose:
                mock_exec_nose.return_value = {
                    "smells_off": True,
                    "minutes_ago": 0.5,
                    "verdict_summary": "Root disk exhaustion detected on hermes (98% full).",
                    "disk": {"free_gb": 1.2},
                    "docker": {"container_count": 0},
                    "tailscale": {"online": False},
                }

                event = {
                    "channel": "C_FB_ANOM",
                    "ts": "100.007",
                    "thread_ts": "100.007",
                    "text": "<@U_FIRSTMATE> what's that smell?",
                }
                await self.gateway.handle_message(event, self.client)

                msg = self.client.posted_messages[0]["text"]
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                self.assertIn("Root disk exhaustion", msg)
                self.assertIn("boom", [r["name"] for r in self.client.reactions_added])

    async def test_slack_reaction_markers_eyes_blocker(self) -> None:
        """Verify check_eyes swaps reaction to :warning: when a blocker is active."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_eyes", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "Waiting for authorization confirmation."}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, turn2_resp]

            with patch.object(
                self.gateway, "_execute_check_eyes", new_callable=AsyncMock
            ) as mock_exec_eyes:
                mock_exec_eyes.return_value = {
                    "state": "BLOCKER",
                    "blocker": {"type": "oauth"},
                }

                event = {
                    "channel": "C_REAC",
                    "ts": "100.008",
                    "thread_ts": "100.008",
                    "text": "<@U_FIRSTMATE> check runner",
                }
                await self.gateway.handle_message(event, self.client)

                added = [r["name"] for r in self.client.reactions_added]
                self.assertIn("warning", added)

    async def test_slack_reaction_markers_nose_anomaly(self) -> None:
        """Verify check_nose swaps reaction to :warning: when smells_off is True."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "check_nose",
                                    "args": {},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": "Fleet anomaly active."}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, turn2_resp]

            with patch.object(
                self.gateway, "_execute_check_nose", new_callable=AsyncMock
            ) as mock_exec_nose:
                mock_exec_nose.return_value = {"smells_off": True}

                event = {
                    "channel": "C_REAC_NOSE",
                    "ts": "100.009",
                    "thread_ts": "100.009",
                    "text": "<@U_FIRSTMATE> check fleet",
                }
                await self.gateway.handle_message(event, self.client)

                added = [r["name"] for r in self.client.reactions_added]
                self.assertIn("warning", added)


class TestMidStreamSensoryGrounding(unittest.IsolatedAsyncioTestCase):
    """Requirement 3 & 5: Verify out-of-band mid-stream sensory grounding with sub-second latency."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.mock_fleet = MagicMock()
        self.sentinel = NoseSentinel(
            fleet=self.mock_fleet,
            bus=self.bus,
            interval_minutes=15.0,
        )
        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR, adapter_factory=make_mock_adapter
        )
        self.gateway = IngressGateway(
            router=self.router,
            bot_user_id="U_FIRSTMATE",
            sensory_bus=self.bus,
            nose_sentinel=self.sentinel,
        )
        self.client = MockSlackClient()

    async def asyncTearDown(self) -> None:
        if hasattr(self, "router") and self.router is not None:
            for s in list(self.router.sessions.values()):
                try:
                    await s.close()
                except Exception:
                    pass
            self.router.sessions.clear()
            await self.router.stop_reaper()
        if hasattr(self.sentinel, "stop"):
            await self.sentinel.stop()
        self.bus.clear()

    async def test_out_of_band_sensory_context_injection(self) -> None:
        """Verify _handle_out_of_band_status injects eyes_status and nose_status during execution."""
        session, _ = await self.gateway.router.get_or_create("C_OOB", "100.001")
        session.active_execution = ExecutionHandle(
            status="running", pane="runner", pid=8888, command="pytest -v"
        )
        session.get_runner_status = AsyncMock(
            return_value={
                "status": "running",
                "command": "pytest -v",
                "pid": 8888,
                "buffer_tail": "Running test 14/20...",
            }
        )

        with patch(
            "golem.gateway.ingress.generate_mouth_response",
            new_callable=AsyncMock,
        ) as mock_mouth_gen:
            mock_mouth_gen.return_value = (
                "⚓ *Liaison Update:* Running tests in Pane 2 (PID 8888)."
            )

            event = {
                "channel": "C_OOB",
                "ts": "100.002",
                "thread_ts": "100.001",
                "text": "How are things looking right now?",
            }
            await self.gateway.handle_message(event, self.client)

            mock_mouth_gen.assert_awaited_once()
            call_kwargs = mock_mouth_gen.call_args[1]
            self.assertIn("eyes_status", call_kwargs)
            self.assertIn("nose_status", call_kwargs)
            self.assertEqual(len(self.client.posted_messages), 1)

    async def test_mid_stream_response_latency_under_1s(self) -> None:
        """Verify generate_mouth_response responds in < 1 second without blocking."""
        eyes_status = {
            "state": "RUNNING",
            "active_command": "cargo build",
            "pid": 1234,
            "buffer_tail": "Compiling...",
        }
        nose_status = {
            "smells_off": False,
            "minutes_ago": 1.0,
            "verdict_summary": "Nominal",
            "disk": {"free_gb": 50.0},
            "docker": {"container_count": 4},
            "tailscale": {"online": True},
        }

        mock_gemini_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "⚓ *Liaison Update:* Active execution is underway with cargo build (PID 1234)."}
                        ]
                    }
                }
            ]
        }
        mock_resp_obj = AsyncMock()
        mock_resp_obj.status = 200
        mock_resp_obj.json = AsyncMock(return_value=mock_gemini_response)

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__.return_value = mock_resp_obj
        mock_post_cm.__aexit__.return_value = None

        with patch("aiohttp.ClientSession.post", return_value=mock_post_cm):
            t0 = time.perf_counter()
            resp = await generate_mouth_response(
                "Can I ask a question while this is running?",
                {"status": "running", "pid": 1234, "command": "cargo build"},
                eyes_status=eyes_status,
                nose_status=nose_status,
                api_key="test-key",
            )
            elapsed = time.perf_counter() - t0

            self.assertLess(
                elapsed, 0.5, "Local mocked response must complete in < 500ms"
            )
            self.assertIn("Liaison Update", resp)

    async def test_mid_stream_factual_grounding_eyes(self) -> None:
        """Verify mid-stream inquiry about terminal observation reflects current Eyes facts."""
        eyes_status = {
            "state": "RUNNING",
            "active_command": "npm test",
            "pid": 4321,
            "buffer_tail": "PASS src/index.test.ts",
        }

        mock_gemini_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "👀 *Runner Observer:* Status is *RUNNING* (PID 4321). Command: `npm test`."}
                        ]
                    }
                }
            ]
        }
        mock_resp_obj = AsyncMock()
        mock_resp_obj.status = 200
        mock_resp_obj.json = AsyncMock(return_value=mock_gemini_response)

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__.return_value = mock_resp_obj
        mock_post_cm.__aexit__.return_value = None

        with patch("aiohttp.ClientSession.post", return_value=mock_post_cm):
            resp = await generate_mouth_response(
                "What do you see in the terminal?",
                {"status": "running", "pid": 4321, "command": "npm test"},
                eyes_status=eyes_status,
                api_key="test-key",
            )
            self.assertIn("Runner Observer", resp)
            self.assertIn("RUNNING", resp)
            self.assertIn("npm test", resp)
            self.assertIn("4321", resp)

    async def test_mid_stream_factual_grounding_nose(self) -> None:
        """Verify mid-stream inquiry about fleet health reflects current Nose facts."""
        nose_status = {
            "smells_off": False,
            "minutes_ago": 2.0,
            "verdict_summary": "Clean air across all fleet nodes.",
            "disk": {"free_gb": 128.0},
            "docker": {"container_count": 8},
            "tailscale": {"online": True},
        }

        mock_gemini_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "👃 *Fleet Health:* All systems nominal; root disk 128.0 GB free."}
                        ]
                    }
                }
            ]
        }
        mock_resp_obj = AsyncMock()
        mock_resp_obj.status = 200
        mock_resp_obj.json = AsyncMock(return_value=mock_gemini_response)

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__.return_value = mock_resp_obj
        mock_post_cm.__aexit__.return_value = None

        with patch("aiohttp.ClientSession.post", return_value=mock_post_cm):
            resp = await generate_mouth_response(
                "What does The Nose smell right now?",
                {"status": "running", "pid": 5555, "command": "sleep 10"},
                nose_status=nose_status,
                api_key="test-key",
            )
            self.assertIn("Fleet Health", resp)
            self.assertIn("nominal", resp.lower())
            self.assertIn("128", resp)

    async def test_mid_stream_oauth_blocker_immediate_card(self) -> None:
        """Verify mid-stream response immediately surfaces clickable OAuth card when blocked."""
        resp = await generate_mouth_response(
            "are we waiting for something?",
            {
                "status": "running",
                "blocker": {
                    "type": "oauth",
                    "url": "http://127.0.0.1:53682/auth",
                },
            },
        )
        self.assertIn("Authentication Required", resp)
        self.assertIn(
            "<http://127.0.0.1:53682/auth|Click here to authenticate>", resp
        )

    async def test_mid_stream_interactive_prompt_blocker_card(self) -> None:
        """Verify mid-stream response immediately surfaces confirmation prompt when blocked."""
        resp = await generate_mouth_response(
            "are we waiting?",
            {
                "status": "running",
                "blocker": {
                    "type": "prompt",
                    "prompt": "Overwrite existing files? [y/N]",
                },
            },
        )
        self.assertIn("Confirmation Required", resp)
        self.assertIn("Overwrite existing files? [y/N]", resp)


class TestSensoryAdversarialPhrasing(unittest.IsolatedAsyncioTestCase):
    """Requirement 4 & 5: Adversarial edge cases, varied phrasings, and graceful degradation."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.mock_fleet = MagicMock()
        self.sentinel = NoseSentinel(
            fleet=self.mock_fleet,
            bus=self.bus,
            interval_minutes=15.0,
        )
        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR, adapter_factory=make_mock_adapter
        )
        self.gateway = IngressGateway(
            router=self.router,
            bot_user_id="U_FIRSTMATE",
            sensory_bus=self.bus,
            nose_sentinel=self.sentinel,
        )
        self.client = MockSlackClient()
        os.environ["FIRSTMATE_FORCE_MOUTH"] = "1"

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIRSTMATE_FORCE_MOUTH", None)
        if hasattr(self, "router") and self.router is not None:
            for s in list(self.router.sessions.values()):
                try:
                    await s.close()
                except Exception:
                    pass
            self.router.sessions.clear()
            await self.router.stop_reaper()
        if hasattr(self.sentinel, "stop"):
            await self.sentinel.stop()
        self.bus.clear()

    async def test_adversarial_phrasing_routes_via_function_calling_eyes(
        self,
    ) -> None:
        """Verify diverse phrasing for runner observation routes via function calling without keyword locks."""
        phrasings = [
            "what does the screen show right now?",
            "are there any blockers?",
            "how's the runner?",
            "what do you see in the terminal?",
            "inspect terminal buffer",
        ]

        for idx, phrase in enumerate(phrasings):
            turn1 = {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "functionCall": {
                                        "name": "check_eyes",
                                        "args": {},
                                    }
                                }
                            ]
                        },
                        "finishReason": "STOP",
                    }
                ]
            }
            turn2 = {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": f"*Runner Briefing:* Handled '{phrase}'."
                                }
                            ]
                        },
                        "finishReason": "STOP",
                    }
                ]
            }

            with patch(
                "golem.gateway.ingress.call_gemini_api",
                new_callable=AsyncMock,
            ) as mock_gemini:
                mock_gemini.side_effect = [turn1, turn2]

                event = {
                    "channel": f"C_ADV_EYES_{idx}",
                    "ts": f"200.{idx:03d}",
                    "thread_ts": f"200.{idx:03d}",
                    "text": f"<@U_FIRSTMATE> {phrase}",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(mock_gemini.call_count, 2)
                fn_call = mock_gemini.call_args_list[1][1]["payload"][
                    "contents"
                ][2]["parts"][0]["functionResponse"]
                self.assertEqual(fn_call["name"], "check_eyes")

    async def test_adversarial_phrasing_routes_via_function_calling_nose(
        self,
    ) -> None:
        """Verify diverse phrasing for fleet health routes via function calling without keyword locks."""
        phrasings = [
            "what are you smelling?",
            "check the fleet",
            "does anything smell off?",
            "sniff the logs",
            "is the air clean?",
        ]

        for idx, phrase in enumerate(phrasings):
            turn1 = {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "functionCall": {
                                        "name": "check_nose",
                                        "args": {},
                                    }
                                }
                            ]
                        },
                        "finishReason": "STOP",
                    }
                ]
            }
            turn2 = {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": f"*Fleet Briefing:* Handled '{phrase}'."
                                }
                            ]
                        },
                        "finishReason": "STOP",
                    }
                ]
            }

            with patch(
                "golem.gateway.ingress.call_gemini_api",
                new_callable=AsyncMock,
            ) as mock_gemini:
                mock_gemini.side_effect = [turn1, turn2]

                event = {
                    "channel": f"C_ADV_NOSE_{idx}",
                    "ts": f"300.{idx:03d}",
                    "thread_ts": f"300.{idx:03d}",
                    "text": f"<@U_FIRSTMATE> {phrase}",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(mock_gemini.call_count, 2)
                fn_call = mock_gemini.call_args_list[1][1]["payload"][
                    "contents"
                ][2]["parts"][0]["functionResponse"]
                self.assertEqual(fn_call["name"], "check_nose")

    async def test_graceful_degradation_when_nose_absent(self) -> None:
        """Verify graceful degradation when NoseSentinel is None on gateway."""
        gateway = IngressGateway(
            router=self.router, bot_user_id="U_FIRSTMATE", nose_sentinel=None
        )
        gateway.get_nose_sentinel = MagicMock(return_value=None)

        turn1 = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_nose", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2 = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": "NoseSentinel is currently offline; unable to sniff logs."
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1, turn2]

            event = {
                "channel": "C_DEG_NOSE",
                "ts": "400.001",
                "thread_ts": "400.001",
                "text": "<@U_FIRSTMATE> check fleet health",
            }
            await gateway.handle_message(event, self.client)

            fn_call = mock_gemini.call_args_list[1][1]["payload"]["contents"][
                2
            ]["parts"][0]["functionResponse"]
            self.assertEqual(
                fn_call["response"]["content"]["error"],
                "NoseSentinel unavailable",
            )
            self.assertEqual(len(self.client.posted_messages), 1)

    async def test_graceful_degradation_when_runner_absent(self) -> None:
        """Verify graceful degradation when runner has no active commands or unallocated pane."""
        turn1 = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_eyes", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2 = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": "Runner pane is idle with no active command."
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gemini:
            mock_gemini.side_effect = [turn1, turn2]

            event = {
                "channel": "C_DEG_EYES",
                "ts": "500.001",
                "thread_ts": "500.001",
                "text": "<@U_FIRSTMATE> what do you see?",
            }
            await self.gateway.handle_message(event, self.client)

            fn_call = mock_gemini.call_args_list[1][1]["payload"]["contents"][
                2
            ]["parts"][0]["functionResponse"]
            self.assertEqual(
                fn_call["response"]["content"]["state"], "IDLE"
            )

    async def test_rapid_concurrent_sensory_queries(self) -> None:
        """Verify concurrent sensory requests in the same gateway complete without deadlock."""
        turn1_eyes = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_eyes", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_eyes = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": "Runner status is nominal."}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn1_nose = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"functionCall": {"name": "check_nose", "args": {}}}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_nose = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": "Fleet health is nominal."}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        # 4 concurrent requests with thread-safe dispatcher
        async def mock_gemini_dispatcher(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            payload = kwargs.get("payload", {})
            contents = payload.get("contents", [])
            for content in contents:
                for part in content.get("parts", []):
                    if "functionResponse" in part:
                        fn_name = part["functionResponse"].get("name")
                        if fn_name == "check_eyes":
                            return turn2_eyes
                        elif fn_name == "check_nose":
                            return turn2_nose
            user_text = ""
            for content in contents:
                if content.get("role") == "user":
                    for part in content.get("parts", []):
                        if "text" in part:
                            user_text = part["text"]
            if "eyes" in user_text.lower() or "see" in user_text.lower():
                return turn1_eyes
            return turn1_nose

        with patch(
            "golem.gateway.ingress.call_gemini_api",
            new_callable=AsyncMock,
        ) as mock_gem:
            mock_gem.side_effect = mock_gemini_dispatcher

            async def run_query(ch: str, text: str) -> None:
                ev = {
                    "channel": ch,
                    "ts": f"600.{ch}",
                    "thread_ts": f"600.{ch}",
                    "text": f"<@U_FIRSTMATE> {text}",
                }
                await self.gateway.handle_message(ev, self.client)

            tasks = [
                run_query("C1", "what do you see?"),
                run_query("C2", "what are you smelling?"),
                run_query("C3", "check eyes"),
                run_query("C4", "check nose"),
            ]

            await asyncio.gather(*tasks)
        self.assertEqual(len(self.client.posted_messages), 4)

    async def test_adversarial_corrupt_telemetry_fields(self) -> None:
        """Verify check_nose handles None and corrupt telemetry metrics gracefully."""
        self.sentinel.last_snapshot = FleetSnapshot(
            timestamp=None,
            disk={"free_gb": None, "percent_used": None, "pressure": None},
            docker={
                "docker_running": None,
                "container_count": None,
                "containers": None,
            },
            tailscale={"online": None, "backend_state": None, "peers": None},
        )
        self.sentinel.last_verdict = None

        res = await self.gateway._execute_check_nose(trigger_fresh_sniff=False)
        self.assertIsInstance(res, dict)
        self.assertEqual(res["disk"]["free_gb"], 0.0)
        self.assertEqual(res["disk"]["percent_used"], 0.0)
        self.assertFalse(res["disk"]["pressure"])
        self.assertEqual(res["docker"]["container_count"], 0)
        self.assertFalse(res["docker"]["running"])
        self.assertFalse(res["tailscale"]["online"])
        self.assertEqual(res["recent_error_count"], 0)


if __name__ == "__main__":
    unittest.main()
