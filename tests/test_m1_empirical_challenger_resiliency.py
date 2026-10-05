#!/usr/bin/env python3
"""
tests/test_m1_empirical_challenger_resiliency.py

Adversarial Stress Test & Empirical Challenge Harness for Milestone 1 (R1).
Executed by teamwork_preview_challenger_m1_2.

Evaluates:
  1. Concurrency stress across incoming messages and multiple threads.
  2. Zero occurrence of hardcoded fallbacks across ALL scenarios on Step 2 failure.
  3. Proper setting of reaction :boom: or :x: on simulated exceptions.
  4. System stability under adversarial load, malformed payloads, and edge cases.
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
)


class AdversarialMockSlackClient:
    """Thread-safe mock Slack client for adversarial stress testing."""

    def __init__(self, simulate_reaction_flakiness: bool = False) -> None:
        self.lock = asyncio.Lock()
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.thread_history: Dict[str, List[Dict[str, Any]]] = {}
        self.simulate_reaction_flakiness = simulate_reaction_flakiness

    async def chat_postMessage(
        self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, Any]:
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
        if self.simulate_reaction_flakiness and name == "already_reacted":
            raise Exception("already_reacted")
        async with self.lock:
            self.reactions_added.append(
                {"channel": channel, "timestamp": timestamp, "name": name}
            )

    async def reactions_remove(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> None:
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


class AdversarialMockAdapter:
    """Mock adapter simulating variable execution latency and events."""

    def __init__(self, latency: float = 0.001) -> None:
        self.latency = latency
        self.turns_received: List[str] = []
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        async with self.lock:
            self.turns_received.append(prompt)
        if self.latency > 0:
            await asyncio.sleep(self.latency)
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text_delta=f"Processed: {prompt[:30]}")
        yield FinishEvent(response=f"Done: {prompt[:30]}", status="SUCCESS")


class TestMilestone1AdversarialChallenge(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge suite testing M1 invariants under adversarial load."""

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR,
            adapter_factory=lambda cwd: AdversarialMockAdapter(),
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = AdversarialMockSlackClient()
        os.environ["GEMINI_API_KEY"] = "mock-key-challenger-m1"

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()
        os.environ.pop("GEMINI_API_KEY", None)

    # =========================================================================
    # CHALLENGE 1: High Concurrency Stress Across Messages & Threads
    # =========================================================================
    async def test_concurrency_50_messages_same_thread(self) -> None:
        """Stress-test 50 concurrent incoming messages hitting the SAME session thread."""
        thread_ts = "1000.0001"
        channel = "C_CONCUR_SAME"

        self.client.thread_history[f"{channel}:{thread_ts}"] = [
            {"ts": thread_ts, "user": "U_USER", "text": "Initial conversation seed"}
        ]

        async def send_single_msg(idx: int) -> None:
            evt = {
                "channel": channel,
                "ts": f"1000.{idx:04d}",
                "thread_ts": thread_ts,
                "text": f"<@U_GOLEM> Concurrent ping {idx}",
            }
            await self.gateway.handle_message(evt, self.client)

        tasks = [asyncio.create_task(send_single_msg(i)) for i in range(1, 51)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, res in enumerate(results):
            self.assertNotIsInstance(res, Exception, f"Task {i} failed with exception: {res}")

        self.assertEqual(len(self.client.posted_messages), 50)

    async def test_concurrency_30_different_threads_simultaneously(self) -> None:
        """Stress-test 30 concurrent messages across 30 DIFFERENT channels and threads."""
        async def send_thread_msg(idx: int) -> None:
            ch = f"C_CHAN_{idx}"
            th = f"2000.{idx:04d}"
            evt = {
                "channel": ch,
                "ts": th,
                "thread_ts": th,
                "text": f"<@U_GOLEM> /health",
            }
            await self.gateway.handle_message(evt, self.client)

        tasks = [asyncio.create_task(send_thread_msg(i)) for i in range(30)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, res in enumerate(results):
            self.assertNotIsInstance(res, Exception, f"Thread {i} failed: {res}")

        self.assertEqual(len(self.client.posted_messages), 30)

    async def test_concurrency_mixed_commands_and_chatter(self) -> None:
        """Stress-test concurrent interleaved slash commands, sensory checks, and chat."""
        commands = [
            "/status", "!status", "/eyes", "!eyes",
            "/health", "!health", "/nose", "!nose",
            "/bearings", "/ahoy", "hello there", "what is up"
        ]

        async def send_cmd(idx: int, cmd: str) -> None:
            ch = "C_MIXED"
            ts = f"3000.{idx:04d}"
            evt = {
                "channel": ch,
                "ts": ts,
                "thread_ts": "3000.0000",
                "text": f"<@U_GOLEM> {cmd}",
            }
            await self.gateway.handle_message(evt, self.client)

        tasks = [asyncio.create_task(send_cmd(i, cmd)) for i, cmd in enumerate(commands)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, res in enumerate(results):
            self.assertNotIsInstance(res, Exception, f"Mixed command {i} failed: {res}")

        self.assertEqual(len(self.client.posted_messages), len(commands))

    # =========================================================================
    # CHALLENGE 2: Hardcoded Fallback & Fail-Loud Invariant Verification
    # =========================================================================
    async def test_step2_failure_on_eyes_idle_fails_loudly_with_boom(self) -> None:
        """Verify check_eyes when IDLE fails loudly with :boom: upon Step 2 synthesis error."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        step2_error = {
            "_error": True,
            "status": 500,
            "code": 500,
            "error_type": "HTTP_500",
            "raw_error": "Gemini 500 error",
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, step2_error]

            with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
                mock_eyes.return_value = {
                    "state": "IDLE",
                    "active_command": "",
                    "pid": None,
                    "buffer_tail": "",
                    "blocker": None,
                }

                evt = {
                    "channel": "C_TEST",
                    "ts": "4000.0000",
                    "thread_ts": "4000.0000",
                    "text": "<@U_GOLEM> check eyes",
                }
                await self.gateway.handle_message(evt, self.client)

                self.assertTrue(len(self.client.posted_messages) > 0)
                msg = self.client.posted_messages[0]["text"]
                added_reactions = [r["name"] for r in self.client.reactions_added]

                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                self.assertIn("boom", added_reactions)

    async def test_step2_failure_on_eyes_running_command_reveals_residual_fallback(self) -> None:
        """
        Adversarial Test: Check whether check_eyes with an active RUNNING command
        fails loudly OR uses residual hardcoded fallback when Step 2 synthesis fails.
        """
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        step2_error = {
            "_error": True,
            "status": 500,
            "code": 500,
            "error_type": "HTTP_500",
            "raw_error": "Internal Google Gemini Model Error",
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, step2_error]

            with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
                mock_eyes.return_value = {
                    "state": "RUNNING",
                    "active_command": "cargo test --all",
                    "pid": 12345,
                    "buffer_tail": "compiling test binaries...",
                    "blocker": None,
                }

                evt = {
                    "channel": "C_TEST",
                    "ts": "4000.0001",
                    "thread_ts": "4000.0001",
                    "text": "<@U_GOLEM> check eyes status",
                }
                await self.gateway.handle_message(evt, self.client)

                msg = self.client.posted_messages[0]["text"]
                added_reactions = [r["name"] for r in self.client.reactions_added]

                self.assertNotIn("👀 *Runner Observer:* Status is *RUNNING*", msg)
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                self.assertIn("cargo test --all", msg)
                self.assertIn("boom", added_reactions)
                self.assertNotIn("white_check_mark", added_reactions)

    async def test_step2_failure_on_dispatch_task_reveals_residual_fallback(self) -> None:
        """
        Adversarial Test: Check whether dispatch_task on success fails loudly
        OR uses residual '✅ *Task Complete:*' fallback when Step 2 synthesis fails.
        """
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "run test suite"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        step2_error = {
            "_error": True,
            "status": 500,
            "code": 500,
            "error_type": "HTTP_500",
            "raw_error": "Synthesis error",
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, step2_error]

            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_task:
                mock_task.return_value = {
                    "status": "SUCCESS",
                    "output": "All 10 tests passed in 0.5s",
                }

                evt = {
                    "channel": "C_TEST",
                    "ts": "4000.0003",
                    "thread_ts": "4000.0003",
                    "text": "<@U_GOLEM> run test suite",
                }
                await self.gateway.handle_message(evt, self.client)

                msg = self.client.posted_messages[0]["text"]
                added_reactions = [r["name"] for r in self.client.reactions_added]

                self.assertNotIn("✅ *Task Complete:*", msg)
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                self.assertIn("All 10 tests passed in 0.5s", msg)
                self.assertIn("boom", added_reactions)
                self.assertNotIn("white_check_mark", added_reactions)

    async def test_step2_failure_unhandled_typeerror_on_non_serializable_tool_result(self) -> None:
        """
        Adversarial Test: Verify that json.dumps(tool_result) without default=str crashes
        with TypeError when tool_result contains a set or non-serializable object.
        """
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "check_nose", "args": {}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, None]
            with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
                mock_nose.return_value = {
                    "smells_off": False,
                    "minutes_ago": 1.0,
                    "non_serializable": set([1, 2, 3]),  # set is not JSON serializable!
                }
                evt = {
                    "channel": "C_NON_JSON",
                    "ts": "4000.0099",
                    "thread_ts": "4000.0099",
                    "text": "<@U_GOLEM> check nose",
                }
                await self.gateway.handle_message(evt, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                msg = self.client.posted_messages[0]["text"]
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                self.assertIn("{1, 2, 3}", msg)
                added = [r["name"] for r in self.client.reactions_added]
                self.assertIn("boom", added)

    async def test_health_query_failure_falsely_reports_nominal_air_is_clean(self) -> None:
        """
        Adversarial Test: Verify that when NoseSentinel raises an unhandled exception,
        /health command falsely reports 'Air is clean; all systems nominal' and white_check_mark.
        """
        mock_sentinel = MagicMock()
        mock_sentinel.get_telemetry_digest.side_effect = RuntimeError("Nose daemon crashed")
        self.gateway.nose_sentinel = mock_sentinel

        evt = {
            "channel": "C_SENTINEL_ERR",
            "ts": "7000.0001",
            "thread_ts": "7000.0001",
            "text": "/health",
        }
        await self.gateway.handle_message(evt, self.client)

        msg = self.client.posted_messages[0]["text"]
        added_reactions = [r["name"] for r in self.client.reactions_added]

        self.assertNotIn("Air is clean; all systems nominal", msg)
        self.assertIn("Nose daemon crashed", msg)
        self.assertIn("warning", added_reactions)
        self.assertNotIn("white_check_mark", added_reactions)

    async def test_status_query_failure_falsely_reports_runner_is_idle(self) -> None:
        """
        Adversarial Test: Verify that when runner status query raises an exception,
        /status command falsely reports 'Runner is currently IDLE. Buffer is clear.' and white_check_mark.
        """
        session, _ = await self.router.get_or_create("C_RUNNER_ERR", "8000.0001")
        session.get_runner_status = AsyncMock(side_effect=RuntimeError("Runner PTY connection severed"))

        evt = {
            "channel": "C_RUNNER_ERR",
            "ts": "8000.0001",
            "thread_ts": "8000.0001",
            "text": "/status",
        }
        await self.gateway.handle_message(evt, self.client)

        msg = self.client.posted_messages[0]["text"]
        added_reactions = [r["name"] for r in self.client.reactions_added]

        self.assertNotIn("Runner is currently *IDLE*. Buffer is clear.", msg)
        self.assertIn("Runner PTY connection severed", msg)
        self.assertIn("warning", added_reactions)
        self.assertNotIn("white_check_mark", added_reactions)

    # =========================================================================
    # CHALLENGE 3: Reaction Swapping on Exceptions & Failures
    # =========================================================================
    async def test_reaction_boom_on_step2_exception(self) -> None:
        """Verify reaction is swapped to 'boom' when Step 2 synthesis throws an exception."""
        turn1_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, aiohttp.ClientError("DNS resolution failed")]

            with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
                mock_eyes.return_value = {
                    "state": "IDLE",
                    "active_command": "",
                    "pid": None,
                    "buffer_tail": "",
                    "blocker": None,
                }

                evt = {
                    "channel": "C_EXC",
                    "ts": "5000.0001",
                    "thread_ts": "5000.0001",
                    "text": "<@U_GOLEM> check eyes",
                }
                await self.gateway.handle_message(evt, self.client)

                added = [r["name"] for r in self.client.reactions_added]
                self.assertIn("boom", added)
                self.assertIn("⚠️ *Executive Synthesis Failed*", self.client.posted_messages[0]["text"])

    async def test_reaction_x_on_adapter_failure(self) -> None:
        """Verify reaction is swapped to 'x' when adapter fails with ERROR."""
        class FailingAdapter:
            async def start(self) -> None: pass
            async def stop(self) -> None: pass
            async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
                yield FinishEvent(response="Fatal compilation failure", status="ERROR", error="Compiler crash")

        self.router.adapter_factory = lambda cwd: FailingAdapter()

        with patch.dict(os.environ, {"FIRSTMATE_TEST_MODE": "1"}):
            evt = {
                "channel": "C_FAIL",
                "ts": "5000.0002",
                "thread_ts": "5000.0002",
                "text": "run broken build",
            }
            await self.gateway.handle_message(evt, self.client)

            added = [r["name"] for r in self.client.reactions_added]
            self.assertIn("x", added)
            self.assertIn("❌ *Error:* Compiler crash", self.client.posted_messages[0]["text"])

    # =========================================================================
    # CHALLENGE 4: Thread Context Safety Across Multi-turn Interactions
    # =========================================================================
    async def test_multi_turn_thread_context_rehydration_deep(self) -> None:
        """Verify that a 5-turn deep thread correctly preserves thread history without mutating commands."""
        channel = "C_DEEP"
        thread_ts = "6000.0001"

        history = [
            {"ts": "6000.0001", "user": "U_CARL", "text": "Turn 1: Plan architecture"},
            {"ts": "6000.0002", "user": "U_GOLEM", "text": "Turn 1 reply: Plan approved"},
            {"ts": "6000.0003", "user": "U_CARL", "text": "Turn 2: Run tests"},
            {"ts": "6000.0004", "user": "U_GOLEM", "text": "Turn 2 reply: 10 tests passed"},
        ]
        self.client.thread_history[f"{channel}:{thread_ts}"] = history

        evt = {
            "channel": channel,
            "ts": "6000.0005",
            "thread_ts": thread_ts,
            "text": "<@U_GOLEM> /status",
        }
        await self.gateway.handle_message(evt, self.client)

        self.assertEqual(len(self.client.posted_messages), 1)
        self.assertIn("👀 *Runner Observer:*", self.client.posted_messages[0]["text"])


if __name__ == "__main__":
    unittest.main()
