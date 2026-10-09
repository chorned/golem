#!/usr/bin/env python3
"""
tests/test_adversarial_m1_it2_stress.py

Adversarial Stress Test & Empirical Challenge Harness for Milestone 1 Iteration 2.
Executed by teamwork_preview_challenger_m1_it2_2.

Evaluates:
  1. Reaction transitions under edge cases (disconnects, timeouts, non-string messages).
  2. Verification that reaction states always transition cleanly and never orphan :gear:.
  3. Non-string message contents (None, int, dict, list, bytes).
  4. Missing session IDs, channel, thread_ts, and ts.
  5. Concurrent /status and /health requests under simulated socket disconnection.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import time
import asyncio
import aiohttp
import unittest
from typing import Optional, List, Dict, Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    AgentEvent,
)
from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle
from golem.gateway.ingress import (
    IngressGateway,
    call_gemini_api,
    get_mouth_tools_declaration,
    BackstageExecutionAccumulator,
    add_reaction,
    remove_reaction,
    swap_reaction,
    strip_bot_mentions,
)


class FlakySlackClient:
    """Mock Slack client capable of simulating socket disconnects and reaction flakiness."""

    def __init__(
        self,
        disconnect_reactions: bool = False,
        disconnect_messages: bool = False,
        rate_limit_reactions: bool = False,
    ) -> None:
        self.lock = asyncio.Lock()
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.thread_history: Dict[str, List[Dict[str, Any]]] = {}
        self.disconnect_reactions = disconnect_reactions
        self.disconnect_messages = disconnect_messages
        self.rate_limit_reactions = rate_limit_reactions

    async def chat_postMessage(
        self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        if self.disconnect_messages:
            raise ConnectionResetError("Simulated socket disconnection on chat_postMessage")
        async with self.lock:
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
        if self.disconnect_reactions:
            raise ConnectionResetError("Simulated socket disconnection on reactions_add")
        if self.rate_limit_reactions:
            err = Exception("rate_limited")
            err.response = {"error": "ratelimited"}
            raise err
        async with self.lock:
            self.reactions_added.append(
                {"channel": channel, "timestamp": timestamp, "name": name}
            )

    async def reactions_remove(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> None:
        if self.disconnect_reactions:
            raise ConnectionResetError("Simulated socket disconnection on reactions_remove")
        async with self.lock:
            self.reactions_removed.append(
                {"channel": channel, "timestamp": timestamp, "name": name}
            )

    async def conversations_replies(
        self, channel: str, ts: str, limit: int = 20, **kwargs: Any
    ) -> Dict[str, Any]:
        async with self.lock:
            key = f"{channel}:{ts}"
            messages = list(self.thread_history.get(key, []))
            return {"ok": True, "messages": messages}

    def get_net_reactions(self, timestamp: str) -> List[str]:
        """Compute the currently active reactions for a given timestamp."""
        added = [r["name"] for r in self.reactions_added if r["timestamp"] == timestamp]
        removed = [r["name"] for r in self.reactions_removed if r["timestamp"] == timestamp]
        active = list(added)
        for r in removed:
            if r in active:
                active.remove(r)
        return active


class MockAdapter:
    """Mock adapter for turn execution."""

    def __init__(self) -> None:
        self.turns_received: List[str] = []

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_received.append(prompt)
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text_delta=f"Output for: {prompt}")
        yield FinishEvent(response=f"Output for: {prompt}", status="SUCCESS")


class TestAdversarialM1It2Stress(unittest.IsolatedAsyncioTestCase):
    """Stress test suite for Milestone 1 Iteration 2."""

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR,
            adapter_factory=lambda cwd: MockAdapter(),
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = FlakySlackClient()
        os.environ["GEMINI_API_KEY"] = "mock-key-it2"

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()
        os.environ.pop("GEMINI_API_KEY", None)

    # =========================================================================
    # 1. Non-String Message Contents (DEFECT PROBE)
    # =========================================================================
    async def test_non_string_text_handled_gracefully(self) -> None:
        """
        Verify that non-string text (None, int, dict, list, bytes)
        does not raise TypeError in strip_bot_mentions and is handled cleanly in handle_message.
        """
        non_string_cases = [
            ("none_text", None),
            ("int_text", 12345),
            ("dict_text", {"content": "hello"}),
            ("list_text", ["item1", "item2"]),
            ("bytes_text", b"binary message"),
        ]

        for label, val in non_string_cases:
            with self.subTest(label=label):
                self.assertEqual(strip_bot_mentions(val), "")
                ts = f"9000.{hash(label) % 10000:04d}"
                evt = {
                    "channel": "C_NON_STR",
                    "ts": ts,
                    "thread_ts": ts,
                    "text": val,
                }
                # Must not raise TypeError
                await self.gateway.handle_message(evt, self.client)

    async def test_file_share_with_none_text_handled_without_crash(self) -> None:
        """
        When a user uploads a file without accompanying text in Slack (text=None),
        verify that handle_message does not crash with TypeError, stages the file,
        and cleanly transitions reaction.
        """
        evt = {
            "channel": "C_FILES",
            "ts": "9100.0001",
            "thread_ts": "9100.0001",
            "text": None,  # Slack file upload with no user caption
            "subtype": "file_share",
            "files": [{"id": "F_123", "name": "report.pdf", "url_private_download": "http://slack/f123"}],
        }
        with patch.object(self.gateway, "stage_inbound_files", new_callable=AsyncMock) as mock_stage:
            mock_stage.return_value = [{"name": "report.pdf", "status": "staged", "path": "/tmp/report.pdf"}]
            await self.gateway.handle_message(evt, self.client)

        active = self.client.get_net_reactions("9100.0001")
        self.assertNotIn("gear", active)
        self.assertIn("white_check_mark", active)

    # =========================================================================
    # 2. Orphan :gear: Reaction Prevention Under Exceptions
    # =========================================================================
    async def test_router_get_or_create_exception_swaps_gear_to_boom(self) -> None:
        """
        When router.get_or_create raises an exception after line 1106,
        verify that handle_message catches it, removes :gear:, and swaps to :boom:.
        """
        self.router.get_or_create = AsyncMock(
            side_effect=RuntimeError("Workspace allocation failed: disk quota exceeded")
        )

        ts = "9200.0001"
        evt = {
            "channel": "C_ORPHAN",
            "ts": ts,
            "thread_ts": ts,
            "text": "<@U_GOLEM> status report",
        }

        with self.assertRaises(RuntimeError):
            await self.gateway.handle_message(evt, self.client)

        active_reactions = self.client.get_net_reactions(ts)
        self.assertNotIn("gear", active_reactions, "Remediated: :gear: must never be orphaned upon router exception")
        self.assertIn("boom", active_reactions, "Reaction must be swapped to :boom: on router exception")
        self.assertIn("gear", [r["name"] for r in self.client.reactions_removed])

    async def test_command_teardown_exception_swaps_gear_to_boom(self) -> None:
        """
        When /wrapup teardown raises an exception,
        verify that handle_message catches it, removes :gear:, and swaps to :boom:.
        """
        self.router.teardown = AsyncMock(side_effect=RuntimeError("Teardown lock failure"))

        ts = "9200.0002"
        evt = {
            "channel": "C_ORPHAN",
            "ts": ts,
            "thread_ts": ts,
            "text": "/wrapup",
        }

        with self.assertRaises(RuntimeError):
            await self.gateway.handle_message(evt, self.client)

        active_reactions = self.client.get_net_reactions(ts)
        self.assertNotIn("gear", active_reactions, "Remediated: :gear: must never be orphaned upon /wrapup teardown exception")
        self.assertIn("boom", active_reactions, "Reaction must be swapped to :boom: on teardown exception")
        self.assertIn("gear", [r["name"] for r in self.client.reactions_removed])

    async def test_chat_postmessage_socket_disconnect_swaps_gear_to_boom(self) -> None:
        """
        In _handle_mouth_turn, if chat_postMessage raises ConnectionResetError,
        verify that handle_message catches it, removes :gear:, and swaps to :boom:.
        """
        flaky_client = FlakySlackClient(disconnect_messages=True)

        ts = "9200.0003"
        evt = {
            "channel": "C_ORPHAN",
            "ts": ts,
            "thread_ts": ts,
            "text": "<@U_GOLEM> What is the plan?",
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.return_value = {
                "candidates": [{"content": {"parts": [{"text": "Executive plan ready."}]}}]
            }

            with self.assertRaises(ConnectionResetError):
                await self.gateway.handle_message(evt, flaky_client)

        active = flaky_client.get_net_reactions(ts)
        self.assertNotIn("gear", active, "Remediated: :gear: must never be orphaned when postMessage disconnected")
        self.assertIn("boom", active, "Reaction must be swapped to :boom: on socket disconnect")
        self.assertIn("gear", [r["name"] for r in flaky_client.reactions_removed])

    # =========================================================================
    # 3. Clean Reaction Transitions When No Exceptions Occur
    # =========================================================================
    async def test_reaction_transitions_across_all_command_types(self) -> None:
        """
        Verify that every command type cleanly transitions away from :gear:
        and leaves the correct terminal reaction under normal execution.
        """
        scenarios = [
            ("/wrapup", "white_check_mark"),
            ("/archive", "white_check_mark"),
            ("/bearings", "white_check_mark"),
            ("/ahoy", "white_check_mark"),
            ("/learn notes", "white_check_mark"),
        ]

        for idx, (cmd, expected_react) in enumerate(scenarios):
            with self.subTest(cmd=cmd):
                ts = f"1100.{idx:04d}"
                evt = {
                    "channel": "C_REACT",
                    "ts": ts,
                    "thread_ts": ts,
                    "text": cmd,
                }
                await self.gateway.handle_message(evt, self.client)

                active = self.client.get_net_reactions(ts)
                self.assertNotIn("gear", active, f":gear: remained active for {cmd}")
                self.assertIn(expected_react, active, f"Expected {expected_react} for {cmd}, got {active}")

    async def test_sensory_health_reaction_transitions(self) -> None:
        """Verify /health reaction transitions under clean, anomaly, and exception states."""
        cases = [
            ("nominal", {"smells_off": False, "minutes_ago": 1.0, "verdict_summary": "Nominal", "disk": {}, "docker": {}, "tailscale": {}}, "white_check_mark"),
            ("anomaly", {"smells_off": True, "minutes_ago": 1.0, "verdict_summary": "High load", "disk": {}, "docker": {}, "tailscale": {}}, "warning"),
            ("error_dict", {"error": "Sentinel socket failed", "smells_off": True, "minutes_ago": 1.0, "verdict_summary": "Error", "disk": {}, "docker": {}, "tailscale": {}}, "warning"),
        ]

        for idx, (label, tool_ret, expected_react) in enumerate(cases):
            with self.subTest(label=label):
                ts = f"1200.{idx:04d}"
                evt = {
                    "channel": "C_HEALTH_R",
                    "ts": ts,
                    "thread_ts": ts,
                    "text": "/health",
                }
                with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
                    mock_nose.return_value = tool_ret
                    await self.gateway.handle_message(evt, self.client)

                active = self.client.get_net_reactions(ts)
                self.assertNotIn("gear", active, f":gear: remained active for {label}")
                self.assertIn(expected_react, active, f"Expected {expected_react} for {label}, got {active}")

    async def test_sensory_status_reaction_transitions(self) -> None:
        """Verify /status reaction transitions under IDLE, RUNNING, BLOCKER, and ERROR states."""
        cases = [
            ("idle", {"state": "IDLE", "buffer_tail": ""}, "white_check_mark"),
            ("running", {"state": "RUNNING", "active_command": "cargo test", "pid": 1234, "buffer_tail": "ok"}, "white_check_mark"),
            ("blocker_url", {"state": "BLOCKER", "blocker": {"url": "http://auth"}}, "warning"),
            ("blocker_prompt", {"state": "BLOCKER", "blocker": {"type": "prompt", "prompt": "confirm?"}}, "warning"),
            ("error_state", {"state": "ERROR", "error": "PTY broken", "buffer_tail": "died"}, "warning"),
        ]

        for idx, (label, tool_ret, expected_react) in enumerate(cases):
            with self.subTest(label=label):
                ts = f"1300.{idx:04d}"
                evt = {
                    "channel": "C_STATUS_R",
                    "ts": ts,
                    "thread_ts": ts,
                    "text": "/status",
                }
                with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
                    mock_eyes.return_value = tool_ret
                    await self.gateway.handle_message(evt, self.client)

                active = self.client.get_net_reactions(ts)
                self.assertNotIn("gear", active, f":gear: remained active for {label}")
                self.assertIn(expected_react, active, f"Expected {expected_react} for {label}, got {active}")

    async def test_mouth_turn_reaction_transitions(self) -> None:
        """Verify Mouth turns transition from :gear: to white_check_mark, boom, or x."""
        # 1. Pure conversational turn -> white_check_mark
        ts1 = "1400.0001"
        evt1 = {"channel": "C_MOUTH", "ts": ts1, "thread_ts": ts1, "text": "<@U_GOLEM> What's the plan?"}
        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.return_value = {
                "candidates": [{"content": {"parts": [{"text": "Standing by."}]}}]
            }
            await self.gateway.handle_message(evt1, self.client)

        active1 = self.client.get_net_reactions(ts1)
        self.assertNotIn("gear", active1)
        self.assertIn("white_check_mark", active1)

        # 2. Tool call with Step 2 failure -> boom
        ts2 = "1400.0002"
        evt2 = {"channel": "C_MOUTH", "ts": ts2, "thread_ts": ts2, "text": "<@U_GOLEM> run check"}
        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [
                {"candidates": [{"content": {"parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]}}]},
                None,  # Step 2 fails
            ]
            with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
                mock_eyes.return_value = {"state": "IDLE"}
                await self.gateway.handle_message(evt2, self.client)

        active2 = self.client.get_net_reactions(ts2)
        self.assertNotIn("gear", active2)
        self.assertIn("boom", active2)

        # 3. Tool call with Step 2 error payload -> boom
        ts3 = "1400.0003"
        evt3 = {"channel": "C_MOUTH", "ts": ts3, "thread_ts": ts3, "text": "<@U_GOLEM> run check"}
        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [
                {"candidates": [{"content": {"parts": [{"functionCall": {"name": "check_nose", "args": {}}}]}}]},
                {"_error": True, "status": 503, "raw_error": "Service Unavailable"},
            ]
            with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
                mock_nose.return_value = {"smells_off": False}
                await self.gateway.handle_message(evt3, self.client)

        active3 = self.client.get_net_reactions(ts3)
        self.assertNotIn("gear", active3)
        self.assertIn("boom", active3)

    # =========================================================================
    # 4. Flaky Network & Socket Disconnections
    # =========================================================================
    async def test_socket_disconnect_on_reactions_add_and_remove(self) -> None:
        """
        Verify that when Slack API disconnects during reaction calls,
        add_reaction, remove_reaction, and swap_reaction handle it without crashing.
        """
        flaky_client = FlakySlackClient(disconnect_reactions=True)

        await add_reaction(flaky_client, "C_TEST", "100.1", "gear")
        await remove_reaction(flaky_client, "C_TEST", "100.1", "gear")
        await swap_reaction(flaky_client, "C_TEST", "100.1", "gear", "white_check_mark")

        evt = {"channel": "C_TEST", "ts": "1500.0001", "thread_ts": "1500.0001", "text": "/bearings"}
        await self.gateway.handle_message(evt, flaky_client)

    async def test_concurrent_status_and_health_under_socket_disconnection(self) -> None:
        """
        Stress test: 20 concurrent /status and 20 concurrent /health requests
        running against a FlakySlackClient with simulated socket disconnections.
        """
        flaky_client = FlakySlackClient(disconnect_reactions=True)

        async def run_single(idx: int, cmd: str) -> None:
            ts = f"2000.{idx:04d}"
            evt = {
                "channel": f"C_CONCUR_{idx % 5}",
                "ts": ts,
                "thread_ts": ts,
                "text": cmd,
            }
            await self.gateway.handle_message(evt, flaky_client)

        tasks = []
        for i in range(20):
            tasks.append(asyncio.create_task(run_single(i, "/status")))
            tasks.append(asyncio.create_task(run_single(i + 20, "/health")))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, res in enumerate(results):
            self.assertNotIsInstance(
                res, Exception, f"Concurrent task {i} raised unexpected exception: {res}"
            )

        self.assertEqual(len(flaky_client.posted_messages), 40)


if __name__ == "__main__":
    unittest.main()
