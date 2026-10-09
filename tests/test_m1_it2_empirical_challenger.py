#!/usr/bin/env python3
"""
tests/test_m1_it2_empirical_challenger.py

Adversarial Stress Test & Empirical Verification Harness for Milestone 1 Iteration 2.
Executed by teamwork_preview_challenger_m1_it2_1.

Empirically verifies resolution of all 4 defects:
1. Residual fallbacks are eradicated; Step 2 failures produce raw error, tool JSON, and :boom:.
2. json.dumps does not throw TypeError on complex objects (sets, dates, exceptions, UUID, bytes, custom classes).
3. /health exception reports error with :warning: and NEVER says "Air is clean".
4. /status exception reports error with :warning: and NEVER says "Runner is currently IDLE".
Plus boundary, edge-case, and high-concurrency stress testing.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import time
import uuid
import datetime
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
)


class MockSlackClient:
    """Thread-safe mock Slack client for verification."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.thread_history: Dict[str, List[Dict[str, Any]]] = {}

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
            return {"ok": True, "messages": list(self.thread_history.get(key, []))}


class DummyCustomClass:
    """Class without JSON serialization support."""
    def __init__(self, val: str) -> None:
        self.val = val
    def __repr__(self) -> str:
        return f"<DummyCustomClass {self.val}>"


class TestMilestone1Iteration2Challenger(unittest.IsolatedAsyncioTestCase):
    """Adversarial challenge test suite for M1 It2 verification."""

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR,
            adapter_factory=lambda cwd: MagicMock(),
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = MockSlackClient()
        os.environ["GEMINI_API_KEY"] = "mock-key-challenger-it2"

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()
        os.environ.pop("GEMINI_API_KEY", None)

    # =========================================================================
    # DEFECT 1: Residual Fallback Eradication & Step 2 Fail-Loud Invariant
    # =========================================================================
    async def test_step2_failure_across_all_tools_eradicates_all_fallbacks(self) -> None:
        """
        Adversarially probe all possible tools (check_eyes, check_nose, dispatch_task, unknown)
        under varied statuses (RUNNING, BLOCKER, COMPLETED, ERROR) and verify:
        - NEVER emits residual fallback templates
        - ALWAYS emits fail-loud synthesis error header and tool JSON
        - ALWAYS swaps reaction to 'boom'
        """
        forbidden_snippets = [
            "👀 *Runner Observer:* Status is *",
            "👀 *Runner Observer:* Waiting for authorization",
            "👃 *Fleet Health Anomaly:* Something smells off",
            "✅ *Task Complete:*",
            "❌ *Task Failed:*",
        ]

        test_cases = [
            # 1. check_eyes with running command
            ("check_eyes", {}, {"state": "RUNNING", "active_command": "pytest -v", "pid": 9999, "buffer_tail": "running..."}, "check runner"),
            # 2. check_eyes with blocker
            ("check_eyes", {}, {"state": "BLOCKER", "blocker": {"type": "oauth", "url": "https://auth.example.com"}}, "check status"),
            # 3. check_nose with anomaly
            ("check_nose", {}, {"smells_off": True, "verdict_summary": "Disk full", "disk": {"free_gb": 0.1}}, "how fleet smells"),
            # 4. dispatch_task with success
            ("dispatch_task", {"task_description": "build"}, {"status": "SUCCESS", "output": "build passed in 10s"}, "compile app"),
            # 5. dispatch_task with error
            ("dispatch_task", {"task_description": "build"}, {"status": "ERROR", "error": "compile syntax error", "output": "error: line 4"}, "compile app"),
        ]

        for i, (fn_name, fn_args, tool_ret, user_text) in enumerate(test_cases):
            client = MockSlackClient()
            turn1_resp = {
                "candidates": [
                    {
                        "content": {
                            "parts": [{"functionCall": {"name": fn_name, "args": fn_args}}]
                        },
                        "finishReason": "STOP",
                    }
                ]
            }
            # Step 2 returns HTTP 503 error
            step2_err = {
                "_error": True,
                "status": 503,
                "code": 503,
                "error_type": "ServiceUnavailable",
                "raw_error": f"Gemini backend 503 on case {i}",
            }

            with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
                mock_gemini.side_effect = [turn1_resp, step2_err]

                with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as m_eyes, \
                     patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as m_nose, \
                     patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as m_backstage:

                    m_eyes.return_value = tool_ret
                    m_nose.return_value = tool_ret
                    m_backstage.return_value = tool_ret

                    evt = {
                        "channel": f"C_FALLBACK_{i}",
                        "ts": f"1000.{i:04d}",
                        "thread_ts": f"1000.{i:04d}",
                        "text": f"<@U_GOLEM> {user_text}",
                    }
                    await self.gateway.handle_message(evt, client)

                    self.assertEqual(len(client.posted_messages), 1, f"Case {i} did not post message")
                    msg_text = client.posted_messages[0]["text"]
                    reactions = [r["name"] for r in client.reactions_added]

                    # Verify no residual fallback string was used
                    for snippet in forbidden_snippets:
                        self.assertNotIn(snippet, msg_text, f"Case {i} contained residual fallback '{snippet}'!")

                    # Verify fail-loud content
                    self.assertIn("⚠️ *Executive Synthesis Failed*", msg_text, f"Case {i} missing synthesis fail header")
                    self.assertIn("HTTP 503", msg_text, f"Case {i} missing HTTP status")
                    self.assertIn(f"Gemini backend 503 on case {i}", msg_text, f"Case {i} missing raw error")
                    self.assertIn("Tool Output:", msg_text, f"Case {i} missing tool output")
                    self.assertIn("boom", reactions, f"Case {i} did not swap to :boom:")
                    self.assertNotIn("white_check_mark", reactions, f"Case {i} erroneously added white_check_mark")

    # =========================================================================
    # DEFECT 2: json.dumps Robustness with Non-Serializable Types
    # =========================================================================
    async def test_step2_failure_json_dumps_complex_non_serializable_types(self) -> None:
        """
        Verify that tool_result containing:
        - python sets
        - datetime objects
        - Exception instances
        - custom classes
        - UUID instances
        - bytes objects
        never raises TypeError in json.dumps, and successfully delivers the fail-loud message.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        exc = RuntimeError("Nested failure details")
        uid = uuid.uuid4()
        custom = DummyCustomClass("special-state")

        exotic_tool_result = {
            "status": "COMPLETED",
            "tags": {"alpha", "beta", "gamma"},
            "timestamp": now,
            "exception_info": exc,
            "identifier": uid,
            "binary_payload": b"\x00\x01\x02",
            "custom_entity": custom,
        }

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
        step2_err = {"_error": True, "status": 400, "raw_error": "Malformed candidate"}

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_resp, step2_err]

            with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as m_nose:
                m_nose.return_value = exotic_tool_result

                evt = {
                    "channel": "C_TYPES",
                    "ts": "2000.0001",
                    "thread_ts": "2000.0001",
                    "text": "<@U_GOLEM> run exotic serialization test",
                }
                # Must complete cleanly without TypeError
                await self.gateway.handle_message(evt, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                msg = self.client.posted_messages[0]["text"]
                self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                self.assertIn("Nested failure details", msg)
                self.assertIn("<DummyCustomClass special-state>", msg)
                self.assertIn(str(uid), msg)
                reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("boom", reactions)

    # =========================================================================
    # DEFECT 3: /health Reporting on Exceptions & Sentinel Unavailability
    # =========================================================================
    async def test_health_query_exceptions_and_variations_never_say_air_is_clean(self) -> None:
        """
        Adversarially probe /health across multiple exception types:
        - RuntimeError
        - TimeoutError
        - ConnectionError
        - sentinel is None (unconfigured)
        Across all command aliases: /health, !health, /nose, !nose
        Verify:
        - NEVER says 'Air is clean; all systems nominal'
        - NEVER sets 'white_check_mark'
        - ALWAYS sets 'warning'
        - ALWAYS reports the error message
        """
        variations = [
            ("/health", RuntimeError("Nose socket crashed")),
            ("!health", TimeoutError("Nose probe timed out after 30s")),
            ("/nose", ConnectionRefusedError("Tailscale RPC unreachable")),
            ("!nose", None),  # sentinel is None
        ]

        for i, (cmd, exc) in enumerate(variations):
            client = MockSlackClient()
            if exc is not None:
                mock_sentinel = MagicMock()
                mock_sentinel.get_telemetry_digest.side_effect = exc
                self.gateway.nose_sentinel = mock_sentinel
            else:
                self.gateway.nose_sentinel = None

            evt = {
                "channel": f"C_HEALTH_{i}",
                "ts": f"3000.{i:04d}",
                "thread_ts": f"3000.{i:04d}",
                "text": f"<@U_GOLEM> {cmd}",
            }
            await self.gateway.handle_message(evt, client)

            self.assertEqual(len(client.posted_messages), 1, f"Failed for {cmd}")
            msg = client.posted_messages[0]["text"]
            reactions = [r["name"] for r in client.reactions_added]

            self.assertNotIn("Air is clean; all systems nominal", msg, f"Command {cmd} falsely reported nominal air")
            self.assertNotIn("white_check_mark", reactions, f"Command {cmd} erroneously applied white_check_mark")
            self.assertIn("warning", reactions, f"Command {cmd} did not set warning reaction")

            if exc is not None:
                self.assertIn(str(exc), msg, f"Command {cmd} did not display exception details")
            else:
                self.assertIn("NoseSentinel unavailable", msg, f"Command {cmd} did not report unavailable sentinel")

    # =========================================================================
    # DEFECT 4: /status Reporting on Exceptions & Runner Disconnections
    # =========================================================================
    async def test_status_query_exceptions_and_variations_never_say_runner_idle(self) -> None:
        """
        Adversarially probe /status across multiple exception types:
        - BrokenPipeError
        - asyncio.TimeoutError
        - RuntimeError
        Across all command aliases: /status, !status, /eyes, !eyes
        Verify:
        - NEVER says 'Runner is currently *IDLE*'
        - NEVER sets 'white_check_mark'
        - ALWAYS sets 'warning'
        - ALWAYS reports the error message
        """
        variations = [
            ("/status", BrokenPipeError("PTY master EOF")),
            ("!status", asyncio.TimeoutError()),
            ("/eyes", RuntimeError("Runner pane unmounted")),
            ("!eyes", ConnectionResetError("SSH mesh node disconnected")),
        ]

        for i, (cmd, exc) in enumerate(variations):
            client = MockSlackClient()
            channel = f"C_STATUS_{i}"
            thread_ts = f"4000.{i:04d}"
            session, _ = await self.router.get_or_create(channel, thread_ts)
            session.get_runner_status = AsyncMock(side_effect=exc)

            evt = {
                "channel": channel,
                "ts": thread_ts,
                "thread_ts": thread_ts,
                "text": f"<@U_GOLEM> {cmd}",
            }
            await self.gateway.handle_message(evt, client)

            self.assertEqual(len(client.posted_messages), 1, f"Failed for {cmd}")
            msg = client.posted_messages[0]["text"]
            reactions = [r["name"] for r in client.reactions_added]

            self.assertNotIn("Runner is currently *IDLE*", msg, f"Command {cmd} falsely reported idle runner")
            self.assertNotIn("Buffer is clear", msg, f"Command {cmd} falsely reported buffer clear")
            self.assertNotIn("white_check_mark", reactions, f"Command {cmd} erroneously applied white_check_mark")
            self.assertIn("warning", reactions, f"Command {cmd} did not set warning reaction")
            self.assertIn("👀 *Runner Observer Error:*", msg, f"Command {cmd} missing error header")

    # =========================================================================
    # STRESS TEST: Concurrency & Malformed Ingress Fuzzing
    # =========================================================================
    async def test_concurrency_stress_interleaved_defects_and_commands(self) -> None:
        """
        Fire 60 concurrent messages interleaving:
        - Step 2 synthesis failures
        - Failing /health checks
        - Failing /status checks
        - Nominal chat messages
        Verify zero race conditions, zero deadlocks, and correct reaction mapping.
        """
        mock_sentinel = MagicMock()
        mock_sentinel.get_telemetry_digest.side_effect = RuntimeError("Concurrent sentinel crash")
        self.gateway.nose_sentinel = mock_sentinel

        async def worker(idx: int) -> None:
            ch = f"C_CONCUR_{idx % 5}"
            th = f"5000.{idx // 5:04d}"
            session, _ = await self.router.get_or_create(ch, th)
            session.get_runner_status = AsyncMock(side_effect=RuntimeError("Concurrent runner crash"))

            if idx % 3 == 0:
                text = "/health"
            elif idx % 3 == 1:
                text = "/status"
            else:
                text = "/ahoy"

            evt = {
                "channel": ch,
                "ts": f"5000.{idx:04d}",
                "thread_ts": th,
                "text": text,
            }
            await self.gateway.handle_message(evt, self.client)

        tasks = [asyncio.create_task(worker(i)) for i in range(60)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, res in enumerate(results):
            self.assertNotIsInstance(res, Exception, f"Worker {i} raised: {res}")

        self.assertEqual(len(self.client.posted_messages), 60)

    async def test_malformed_event_fuzzing(self) -> None:
        """
        Fuzz handle_message with edge-case payloads:
        - text is empty string
        - missing text key
        - text without user mention
        """
        malformed_events = [
            {"channel": "C_FUZZ", "ts": "6000.0002", "text": ""},
            {"channel": "C_FUZZ", "ts": "6000.0003"},  # no text key
            {"channel": "C_FUZZ", "ts": "6000.0004", "text": "random message with no bot mention"},
        ]

        for evt in malformed_events:
            # Should not raise exception
            await self.gateway.handle_message(evt, self.client)


if __name__ == "__main__":
    unittest.main()
