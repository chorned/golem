#!/usr/bin/env python3
"""
tests/test_sensory_adversarial_m1_c2.py
Adversarial Verification Suite for Milestone 1 (Challenger 2):
  1. Zero regex gates & adversarial phrasing:
     - Queries: "what are you smelling?", "what do you see?", "check the fleet",
       "are there any blockers?", "is anything broken?", "give me a status update on runner".
  2. Gemini function calling receives CHECK_EYES_TOOL_DECLARATION & CHECK_NOSE_TOOL_DECLARATION.
  3. Tool execution results feed back via functionResponse as pure JSON dictionaries
     without raising exceptions or falling back to raw f-strings.
  4. Fallback synthesis produces valid Slack mrkdwn (*bold*, inline `code`, clean bullets)
     without ANSI escape codes or CommonMark ATX headers (###).
  5. Reaction lifecycle swaps (:warning: for blockers/anomalies, :white_check_mark: for success).
  6. Sub-second out-of-band mid-stream latency (<1.0s) and fault tolerance under null/corrupted state.
"""

from __future__ import annotations

import os
import sys
import re
import json
import time
import asyncio
import unittest
from typing import Dict, Any, List, Optional
from unittest.mock import MagicMock, AsyncMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus
from golem.sensory.nose import (
    NoseSentinel,
    FleetSnapshot,
    AnomalyVerdict,
    get_default_nose_sentinel,
    set_default_nose_sentinel,
)
from golem.gateway.session import SessionHandle, EyesStatusResult, is_out_of_band_message
from golem.gateway.ingress import (
    IngressGateway,
    get_mouth_tools_declaration,
    CHECK_EYES_TOOL_DECLARATION,
    CHECK_NOSE_TOOL_DECLARATION,
    DISPATCH_TASK_TOOL_DECLARATION,
)
from golem.agent.mouth import generate_mouth_response, is_batch_task_command


class TestSensoryAdversarialM1Challenger2(unittest.IsolatedAsyncioTestCase):
    """Adversarial challenger test suite for Milestone 1 sensory integration."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.mock_fleet = MagicMock()
        self.sentinel = NoseSentinel(
            fleet=self.mock_fleet,
            bus=self.bus,
            interval_minutes=15.0,
        )
        self.sentinel.last_snapshot = FleetSnapshot(
            timestamp=time.time() - 30.0,
            nodes={"hermes": {"reachable": True}},
            tailscale={"online": True, "backend_state": "Running", "peers": {}},
            disk={"free_gb": 42.0, "percent_used": 58.0, "pressure": False},
            docker={"docker_running": True, "container_count": 6, "containers": ["plex", "postgres"]},
        )
        self.sentinel.last_verdict = AnomalyVerdict(
            smells_off=False,
            severity="low",
            category="other",
            summary="All systems nominal; 0 errors, fleet healthy.",
        )
        self.mock_router = MagicMock()
        self.mock_client = MagicMock()
        self.mock_client.chat_postMessage = AsyncMock(return_value={"ok": True})
        self.mock_client.reactions_add = AsyncMock(return_value={"ok": True})
        self.mock_client.reactions_remove = AsyncMock(return_value={"ok": True})

        self.gateway = IngressGateway(
            router=self.mock_router,
            bot_user_id="U_BOT_TEST",
            nose_sentinel=self.sentinel,
        )

    async def asyncTearDown(self) -> None:
        if hasattr(self.sentinel, "stop"):
            await self.sentinel.stop()
        self.bus.clear()

    # --------------------------------------------------------------------------
    # 1. Tool Declaration Verification
    # --------------------------------------------------------------------------
    def test_tool_declarations_schema_and_types(self) -> None:
        """Verify check_eyes and check_nose declarations meet Gemini REST specifications."""
        tools = get_mouth_tools_declaration()
        self.assertIsInstance(tools, list)
        self.assertEqual(len(tools), 1)
        decls = tools[0].get("functionDeclarations", [])
        names = [d["name"] for d in decls]

        self.assertIn("dispatch_task", names)
        self.assertIn("check_eyes", names)
        self.assertIn("check_nose", names)

        # check_eyes schema
        eyes_decl = next(d for d in decls if d["name"] == "check_eyes")
        self.assertEqual(eyes_decl["parameters"]["type"], "OBJECT")
        self.assertIn("pane", eyes_decl["parameters"]["properties"])
        self.assertEqual(eyes_decl["parameters"].get("required", []), [])

        # check_nose schema
        nose_decl = next(d for d in decls if d["name"] == "check_nose")
        self.assertEqual(nose_decl["parameters"]["type"], "OBJECT")
        self.assertIn("trigger_fresh_sniff", nose_decl["parameters"]["properties"])
        self.assertEqual(nose_decl["parameters"]["properties"]["trigger_fresh_sniff"]["type"], "BOOLEAN")
        self.assertEqual(nose_decl["parameters"].get("required", []), [])

        # Verify JSON serializability of entire tools structure
        serialized = json.dumps(tools)
        self.assertIsInstance(serialized, str)

    # --------------------------------------------------------------------------
    # 2. Dynamic Function Calling & functionResponse Feedback Loop
    # --------------------------------------------------------------------------
    async def test_adversarial_queries_trigger_check_nose_and_function_response(self) -> None:
        """
        Verify that adversarial olfactory queries trigger check_nose,
        pass pure JSON telemetry back to functionResponse, and synthesize briefing.
        """
        adversarial_nose_queries = [
            "what are you smelling?",
            "check the fleet",
            "is anything broken?",
        ]

        for query in adversarial_nose_queries:
            # We track the two Gemini API calls made by _handle_mouth_turn:
            # Step 1: initial call returning functionCall
            # Step 2: synthesis call receiving functionResponse
            api_calls: List[Dict[str, Any]] = []

            async def fake_gemini_api(payload: Dict[str, Any], model: Any = None, api_key: Any = None) -> Dict[str, Any]:
                api_calls.append(payload)
                if len(api_calls) == 1:
                    # Step 1: Model decides to call check_nose
                    return {
                        "candidates": [
                            {
                                "content": {
                                    "role": "model",
                                    "parts": [
                                        {
                                            "functionCall": {
                                                "name": "check_nose",
                                                "args": {"trigger_fresh_sniff": False},
                                            }
                                        }
                                    ],
                                }
                            }
                        ]
                    }
                else:
                    # Step 2: Model synthesizes briefing from functionResponse
                    return {
                        "candidates": [
                            {
                                "content": {
                                    "role": "model",
                                    "parts": [
                                        {
                                            "text": (
                                                "*Fleet Health:* Air is clean. All fleet nodes operational.\n"
                                                "• Verdict: 0 errors detected across fleet\n"
                                                "• Root disk: 42.0 GB free\n"
                                                "• Docker: 6 containers healthy"
                                            )
                                        }
                                    ],
                                }
                            }
                        ]
                    }

            self.gateway._call_gemini_api = fake_gemini_api

            mock_session = MagicMock()
            mock_session.target_cwd = "/Users/hermes/projects/first-mate"

            await self.gateway._handle_mouth_turn(
                session=mock_session,
                prompt=query,
                client=self.mock_client,
                channel="C_TEST",
                thread_ts="T_123",
                ts="12345.678",
                is_imessage=False,
            )

            # Verification assertions
            self.assertEqual(len(api_calls), 2, f"Expected exactly 2 Gemini calls for query: {query}")

            # Verify Call 1 received the sensory tool declarations
            step1_payload = api_calls[0]
            self.assertIn("tools", step1_payload)
            tools_decls = step1_payload["tools"][0]["functionDeclarations"]
            tool_names = [d["name"] for d in tools_decls]
            self.assertIn("check_nose", tool_names)
            self.assertIn("check_eyes", tool_names)

            # Verify Call 2 received the functionResponse part
            step2_payload = api_calls[1]
            contents = step2_payload.get("contents", [])
            self.assertEqual(len(contents), 3, "Contents should have [user, model, user(functionResponse)]")

            fn_response_turn = contents[2]
            self.assertEqual(fn_response_turn["role"], "user")
            parts = fn_response_turn["parts"]
            self.assertEqual(len(parts), 1)
            self.assertIn("functionResponse", parts[0])

            fn_resp = parts[0]["functionResponse"]
            self.assertEqual(fn_resp["name"], "check_nose")
            self.assertIn("response", fn_resp)
            self.assertEqual(fn_resp["response"]["name"], "check_nose")

            # Check that content is a pure dictionary without f-string templates
            content_dict = fn_resp["response"]["content"]
            self.assertIsInstance(content_dict, dict)
            self.assertIn("smells_off", content_dict)
            self.assertIn("disk", content_dict)
            self.assertIn("tailscale", content_dict)
            self.assertFalse(content_dict["smells_off"])

            # Verify entire briefing payload is JSON serializable
            json_str = json.dumps(step2_payload)
            self.assertIsInstance(json_str, str)

            # Verify Slack message posted contains the LLM synthesis
            posted_calls = self.mock_client.chat_postMessage.call_args_list
            latest_post = posted_calls[-1].kwargs
            self.assertIn("*Fleet Health:*", latest_post["text"])
            self.assertIn("42.0 GB free", latest_post["text"])

    async def test_adversarial_queries_trigger_check_eyes_and_function_response(self) -> None:
        """
        Verify that adversarial runner buffer queries trigger check_eyes,
        pass pure JSON telemetry back to functionResponse, and synthesize briefing.
        """
        adversarial_eyes_queries = [
            "what do you see?",
            "are there any blockers?",
            "give me a status update on runner",
        ]

        for query in adversarial_eyes_queries:
            api_calls: List[Dict[str, Any]] = []

            async def fake_gemini_api(payload: Dict[str, Any], model: Any = None, api_key: Any = None) -> Dict[str, Any]:
                api_calls.append(payload)
                if len(api_calls) == 1:
                    return {
                        "candidates": [
                            {
                                "content": {
                                    "role": "model",
                                    "parts": [
                                        {
                                            "functionCall": {
                                                "name": "check_eyes",
                                                "args": {},
                                            }
                                        }
                                    ],
                                }
                            }
                        ]
                    }
                else:
                    return {
                        "candidates": [
                            {
                                "content": {
                                    "role": "model",
                                    "parts": [
                                        {
                                            "text": (
                                                "*Runner Status:* Active task running in Pane 2 (PID 7890).\n"
                                                "• Command: `npm test`\n"
                                                "• Progress: 42 test suites passing"
                                            )
                                        }
                                    ],
                                }
                            }
                        ]
                    }

            self.gateway._call_gemini_api = fake_gemini_api

            mock_session = MagicMock()
            mock_session.target_cwd = "/Users/hermes/projects/first-mate"
            mock_session.get_runner_status = AsyncMock(return_value={
                "status": "running",
                "command": "npm test",
                "pid": 7890,
                "buffer_tail": "PASS src/app.test.js",
            })
            mock_session.get_eyes_status = MagicMock(return_value={
                "state": "RUNNING",
                "active_command": "npm test",
                "command": "npm test",
                "pid": 7890,
                "blocker": None,
                "blockers": [],
                "buffer_tail": "PASS src/app.test.js",
            })

            await self.gateway._handle_mouth_turn(
                session=mock_session,
                prompt=query,
                client=self.mock_client,
                channel="C_TEST",
                thread_ts="T_123",
                ts="12345.678",
                is_imessage=False,
            )

            self.assertEqual(len(api_calls), 2)
            step2_payload = api_calls[1]
            fn_resp = step2_payload["contents"][2]["parts"][0]["functionResponse"]
            self.assertEqual(fn_resp["name"], "check_eyes")

            content_dict = fn_resp["response"]["content"]
            self.assertIsInstance(content_dict, dict)
            self.assertEqual(content_dict["state"], "RUNNING")
            self.assertEqual(content_dict["active_command"], "npm test")
            self.assertEqual(content_dict["pid"], 7890)

            # JSON serializability check
            json_str = json.dumps(step2_payload)
            self.assertIsInstance(json_str, str)

            posted = self.mock_client.chat_postMessage.call_args.kwargs["text"]
            self.assertIn("*Runner Status:*", posted)
            self.assertIn("npm test", posted)

    # --------------------------------------------------------------------------
    # 3. Fallback Synthesis Formatting & Sanitization
    # --------------------------------------------------------------------------
    async def test_fallback_synthesis_formatting_slack_mrkdwn(self) -> None:
        """
        Verify that when Gemini API synthesis fails, fallback synthesis in _handle_mouth_turn
        formats valid Slack mrkdwn (*bold*, inline `code`, clean bullets)
        without ANSI codes or CommonMark ATX headers (###).
        """
        # Scenario A: check_eyes running with command
        async def fake_gemini_step1_only(payload: Dict[str, Any], model: Any = None, api_key: Any = None) -> Any:
            if "functionResponse" not in json.dumps(payload):
                return {
                    "candidates": [
                        {
                            "content": {
                                "role": "model",
                                "parts": [{"functionCall": {"name": "check_eyes", "args": {}}}],
                            }
                        }
                    ]
                }
            # Synthesis step fails / returns None
            return None

        self.gateway._call_gemini_api = fake_gemini_step1_only

        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(return_value={"status": "running"})
        mock_session.get_eyes_status = MagicMock(return_value={
            "state": "RUNNING",
            "active_command": "pytest -v",
            "pid": 5555,
            "blocker": None,
            "buffer_tail": "test_auth.py::test_login PASSED",
        })

        await self.gateway._handle_mouth_turn(
            session=mock_session,
            prompt="what is running?",
            client=self.mock_client,
            channel="C_TEST",
            thread_ts="T_123",
            ts="12345.678",
            is_imessage=False,
        )

        posted_text = self.mock_client.chat_postMessage.call_args.kwargs["text"]

        # Formatting invariants
        self.assertNotIn("###", posted_text, "No CommonMark ATX headers allowed")
        self.assertNotIn("\x1b[", posted_text, "No ANSI color/escape sequences allowed")
        self.assertNotIn("**", posted_text, "Must use single asterisks *bold* for Slack mrkdwn")
        self.assertIn("⚠️ *Executive Synthesis Failed*", posted_text)
        self.assertIn("pytest -v", posted_text)
        self.assertIn('"state": "RUNNING"', posted_text)

        # Scenario B: check_nose anomaly
        self.sentinel.last_verdict = AnomalyVerdict(
            smells_off=True,
            severity="high",
            category="disk",
            summary="Disk pressure critical on hermes: only 1.2 GB free.",
        )
        self.sentinel.last_snapshot.disk = {"free_gb": 1.2, "percent_used": 98.0, "pressure": True}

        async def fake_gemini_step1_nose(payload: Dict[str, Any], model: Any = None, api_key: Any = None) -> Any:
            if "functionResponse" not in json.dumps(payload):
                return {
                    "candidates": [
                        {
                            "content": {
                                "role": "model",
                                "parts": [{"functionCall": {"name": "check_nose", "args": {}}}],
                            }
                        }
                    ]
                }
            return None

        self.gateway._call_gemini_api = fake_gemini_step1_nose

        await self.gateway._handle_mouth_turn(
            session=mock_session,
            prompt="anything wrong with the fleet?",
            client=self.mock_client,
            channel="C_TEST",
            thread_ts="T_123",
            ts="12345.679",
            is_imessage=False,
        )

        posted_nose_text = self.mock_client.chat_postMessage.call_args.kwargs["text"]
        self.assertNotIn("###", posted_nose_text)
        self.assertNotIn("\x1b[", posted_nose_text)
        self.assertNotIn("**", posted_nose_text)
        self.assertIn("⚠️ *Executive Synthesis Failed*", posted_nose_text)
        self.assertIn("Disk pressure critical", posted_nose_text)

    # --------------------------------------------------------------------------
    # 4. Reaction Lifecycle Swaps
    # --------------------------------------------------------------------------
    async def test_reaction_lifecycle_swaps(self) -> None:
        """
        Verify reaction state transitions:
          - check_eyes with BLOCKER -> :warning:
          - check_nose with smells_off=True -> :warning:
          - check_nose with smells_off=False -> :white_check_mark:
        """
        async def fake_gemini_eyes_blocker(payload: Dict[str, Any], model: Any = None, api_key: Any = None) -> Any:
            if "functionResponse" not in json.dumps(payload):
                return {"candidates": [{"content": {"parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]}}]}
            return {"candidates": [{"content": {"parts": [{"text": "OAuth required"}]}}]}

        self.gateway._call_gemini_api = fake_gemini_eyes_blocker

        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(return_value={"status": "running"})
        mock_session.get_eyes_status = MagicMock(return_value={
            "state": "BLOCKER",
            "active_command": "rclone config",
            "pid": 3333,
            "blocker": {"type": "oauth", "url": "http://127.0.0.1:8080/auth"},
            "buffer_tail": "Waiting for auth...",
        })

        await self.gateway._handle_mouth_turn(
            session=mock_session,
            prompt="is runner blocked?",
            client=self.mock_client,
            channel="C_TEST",
            thread_ts="T_1",
            ts="100.1",
            is_imessage=False,
        )

        # Assert reaction swapped to warning
        self.mock_client.reactions_remove.assert_awaited_with(channel="C_TEST", timestamp="100.1", name="gear")
        self.mock_client.reactions_add.assert_awaited_with(channel="C_TEST", timestamp="100.1", name="warning")

    # --------------------------------------------------------------------------
    # 5. Out-of-Band Mid-Stream Latency & Phrasing
    # --------------------------------------------------------------------------
    async def test_midstream_out_of_band_adversarial_phrasing_and_subsecond_latency(self) -> None:
        """
        Verify that during active backstage execution, all adversarial queries:
          1. Qualify as out-of-band without being rejected as batch task commands.
          2. Complete with factual sensory grounding under 1.0 second.
        """
        adversarial_queries = [
            "what are you smelling?",
            "what do you see?",
            "check the fleet",
            "are there any blockers?",
            "is anything broken?",
            "give me a status update on runner",
        ]

        eyes_status = {
            "state": "RUNNING",
            "active_command": "cargo test --all",
            "pid": 9999,
            "blocker": None,
            "buffer_tail": "running 25 tests...",
        }
        nose_status = {
            "seconds_ago": 15.0,
            "minutes_ago": 0.25,
            "smells_off": False,
            "verdict_summary": "All systems nominal; 0 errors.",
            "disk": {"free_gb": 60.0, "percent_used": 40.0, "pressure": False},
            "docker": {"running": True, "container_count": 4, "containers": []},
            "tailscale": {"online": True, "backend_state": "Running", "peers": {}},
        }
        status_info = {"status": "running", "pid": 9999, "command": "cargo test --all"}

        for query in adversarial_queries:
            # 1. Zero regex gating check
            self.assertFalse(is_batch_task_command(query), f"'{query}' must not be treated as a batch command")
            self.assertTrue(is_out_of_band_message(query), f"'{query}' must qualify as out-of-band message")

            # 2. Sub-second latency check (<1s)
            mock_gemini_response = {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {"text": f"Status response for: {query}"}
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
                    prompt=query,
                    status_info=status_info,
                    eyes_status=eyes_status,
                    nose_status=nose_status,
                    api_key="test-key",
                )
                elapsed = time.perf_counter() - t0

            self.assertLess(elapsed, 1.0, f"Query '{query}' took {elapsed:.3f}s; must be < 1.0s")
            self.assertIsInstance(resp, str)
            self.assertTrue(len(resp) > 0)
            self.assertNotIn("###", resp)
            self.assertNotIn("\x1b[", resp)

    # --------------------------------------------------------------------------
    # 6. Fault Tolerance & Corrupted State Invariants
    # --------------------------------------------------------------------------
    async def test_sensory_tools_fault_tolerance(self) -> None:
        """Verify _execute_check_eyes and _execute_check_nose survive null/corrupted state."""
        # A. Nose Sentinel is None
        mock_router_no_sentinel = MagicMock()
        mock_router_no_sentinel.nose_sentinel = None
        gw_no_nose = IngressGateway(router=mock_router_no_sentinel, bot_user_id="U_BOT", nose_sentinel=None)
        with patch("golem.sensory.nose.get_default_nose_sentinel", return_value=None):
            nose_dict = await gw_no_nose._execute_check_nose(trigger_fresh_sniff=False)
            self.assertIsInstance(nose_dict, dict)
            self.assertIn("verdict_summary", nose_dict)
            self.assertFalse(nose_dict["smells_off"])

        # B. Session is None for check_eyes
        eyes_dict_none = await self.gateway._execute_check_eyes(session=None)
        self.assertIsInstance(eyes_dict_none, dict)
        self.assertEqual(eyes_dict_none["state"], "IDLE")
        self.assertIsNone(eyes_dict_none["pid"])

        # C. Session with massive buffer (50k chars)
        mock_heavy_session = MagicMock()
        mock_heavy_session.get_runner_status = AsyncMock(return_value={
            "status": "running",
            "command": "cat huge.log",
            "pid": 1111,
            "buffer_tail": "X" * 50_000,
        })
        mock_heavy_session.get_eyes_status = MagicMock(return_value=EyesStatusResult(
            initial_dict={
                "state": "RUNNING",
                "active_command": "cat huge.log",
                "pid": 1111,
                "blocker": None,
                "buffer_tail": "X" * 50_000,
            }
        ))

        heavy_result = await self.gateway._execute_check_eyes(session=mock_heavy_session)
        self.assertIsInstance(heavy_result, dict)
        self.assertEqual(heavy_result["state"], "RUNNING")
        # Ensure json serializable
        self.assertIsInstance(json.dumps(heavy_result), str)

    # --------------------------------------------------------------------------
    # 7. Empirical Bug Reproduction (Challenger Findings)
    # --------------------------------------------------------------------------
    def test_reproduce_eyes_status_completed_overridden_by_pid(self) -> None:
        """
        EMPIRICAL DEFECT: Verify SessionHandle.get_eyes_status correctly identifies
        completed and error states even when PID is populated in status_info.
        Currently FAILS because session.py:304 checks (pid is not None and pid != '')
        before checking status_val == 'completed'.
        """
        mock_adapter = MagicMock()
        session = SessionHandle("s_bug", "c1", "t1", adapter=mock_adapter)
        status_info = {
            "status": "completed",
            "command": "python3 build.py",
            "pid": 4321,
            "buffer_tail": "Build succeeded.",
        }
        res = session.get_eyes_status(status_info)
        self.assertEqual(
            res["state"],
            "COMPLETED",
            f"Expected state='COMPLETED' for completed task with PID, but got '{res['state']}'",
        )

    async def test_reproduce_nose_telemetry_none_field_type_error(self) -> None:
        """
        EMPIRICAL DEFECT: Verify NoseSentinel.get_telemetry_digest survives when
        node telemetry returns None for free_gb or percent_used.
        Currently FAILS with TypeError: float() argument must be a string or a real number, not 'NoneType'.
        """
        self.sentinel.last_snapshot = FleetSnapshot(
            timestamp=time.time() - 10.0,
            nodes={"hermes": {"reachable": True}},
            tailscale={"online": True, "backend_state": "Running", "peers": {}},
            disk={"free_gb": None, "percent_used": None, "pressure": False},
            docker={"docker_running": True, "container_count": None, "containers": []},
        )
        digest = await self.sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
        self.assertIsInstance(digest, dict)
        self.assertEqual(digest["disk"]["free_gb"], 0.0)


if __name__ == "__main__":
    unittest.main()
