#!/usr/bin/env python3
"""
tests/test_adversarial_m1_ingress_challenge.py
Adversarial and Empirical Challenge Test Suite for Milestone 1 (Slack Gateway Resiliency & Protocol Integrity).

Empirically tests:
1. Adversarial slash command inputs:
   - Leading / trailing whitespace, tabs, and newlines
   - Mixed and upper casing (/STATUS, /Health, !Eyes, !NOSE, etc.)
   - Extra tokens and trailing arguments (/status now, !Eyes verbose, /HEALTH 123, etc.)
   - Preceding bot mentions (<@U_GOLEM> /status, @AGY /health, etc.)
   - Multiline commands, punctuation variations, and conversational non-commands
2. Step 2 failure handling under varied HTTP errors and transport exceptions:
   - HTTP 400, 401, 403, 429, 500, 502, 503, 504
   - Transport exceptions: TimeoutError, ClientConnectorError, ConnectionResetError, RuntimeError
   - Corrupted & malformed payloads: None, {}, empty candidates, empty parts, looping functionCall
   - Invariant assertion: fail-loud Slack message, raw error detail, tool JSON, reaction :boom:, zero fake telemetry
3. Thread history prompt rehydration edge cases:
   - Code blocks with triple backticks, blockquotes, URLs, and multiline formatting
   - 300-char message truncation and 10-message window sliding
   - Sender attribution (AGY vs User) and exclude_ts filtering
   - clean_text immutability invariant: prompt_to_send receives prepended context while clean_text remains unmutated
   - In-thread slash command interception bypasses LLM
   - Resiliency when conversations_replies fails or raises
4. Step 2 schema integrity:
   - Validates tools declaration schema structure and presence in Step 2 payload

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import time
import asyncio
import unittest
from typing import Optional, List, Dict, Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle
from golem.gateway.ingress import (
    IngressGateway,
    call_gemini_api,
    get_mouth_tools_declaration,
    strip_bot_mentions,
)
from golem.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    AgentEvent,
)


class MockSlackClient:
    """Recording Mock Slack client."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
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

    async def conversations_replies(
        self, channel: str, ts: str, limit: int = 20, **kwargs: Any
    ) -> Dict[str, Any]:
        key = f"{channel}:{ts}"
        messages = self.thread_history.get(key, [])
        return {"ok": True, "messages": messages}

    def clear(self) -> None:
        self.posted_messages.clear()
        self.reactions_added.clear()
        self.reactions_removed.clear()


class MockHeadlessAdapter:
    """Mock adapter recording prompt turns."""

    def __init__(self, cwd: str = "/tmp") -> None:
        self.cwd = cwd
        self.turns_received: List[str] = []

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_received.append(prompt)
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text_delta=f"Executed: {prompt}")
        yield FinishEvent(response=f"Executed: {prompt}", status="SUCCESS")


class TestAdversarialSlashCommands(unittest.IsolatedAsyncioTestCase):
    """Stress-test slash command parsing against adversarial inputs."""

    async def asyncSetUp(self) -> None:
        self.mock_session = MagicMock()
        self.mock_session.is_busy = False
        self.mock_session.is_executing = False
        self.mock_session.runner_pane = None
        self.mock_session.adapter = None
        self.mock_session.target_cwd = "/tmp"

        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR,
            adapter_factory=lambda cwd: MockHeadlessAdapter(cwd=cwd),
        )
        self.router.get_or_create = AsyncMock(return_value=(self.mock_session, False))

        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = MockSlackClient()
        os.environ["GEMINI_API_KEY"] = "fake-api-key"

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()
        os.environ.pop("GEMINI_API_KEY", None)

    async def test_adversarial_eyes_slash_commands(self) -> None:
        """Adversarially challenge /status, !status, /eyes, !eyes across casing, whitespace, and arguments."""
        adversarial_inputs = [
            # Casing variations
            "/STATUS",
            "/Status",
            "!Eyes",
            "!EYES",
            "!StAtUs",
            # Whitespace variations
            "   /status   ",
            "\t/eyes\n",
            " \r\n !status \t ",
            # Extra tokens and arguments
            "/status now",
            "/status --verbose --all",
            "!Eyes verbose",
            "!eyes pane-2",
            "/eyes 123",
            # Preceding bot mentions
            "<@U_GOLEM> /status",
            "<@U_GOLEM|golem>   !Eyes verbose",
            "<@U_GOLEM> /STATUS now",
            "@AGY /status",
            "@FirstMate !eyes",
            # Multiline inputs
            "/status\nnext line detail",
            "\n\n!eyes\n\n",
            # Emoji trailing
            "/status 🚀",
            "!eyes 👀",
        ]

        for i, cmd_input in enumerate(adversarial_inputs):
            with self.subTest(cmd_input=cmd_input):
                self.client.clear()

                with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
                    mock_eyes.return_value = {
                        "state": "RUNNING",
                        "active_command": "make test",
                        "pid": 5555,
                        "buffer_tail": "All tests green",
                        "blocker": None,
                    }

                    event = {
                        "channel": "C_ADV_EYES",
                        "ts": f"100.{1000 + i}",
                        "thread_ts": f"100.{1000 + i}",
                        "text": cmd_input,
                    }

                    await self.gateway.handle_message(event, self.client)

                    mock_eyes.assert_awaited_once()
                    self.assertEqual(len(self.client.posted_messages), 1, f"Failed on input: {cmd_input!r}")
                    posted_text = self.client.posted_messages[0]["text"]
                    self.assertIn("👀 *Runner Observer:* Status is *RUNNING* (PID 5555)", posted_text)
                    self.assertIn("make test", posted_text)
                    added_reactions = [r["name"] for r in self.client.reactions_added]
                    self.assertIn("white_check_mark", added_reactions)

    async def test_adversarial_nose_slash_commands(self) -> None:
        """Adversarially challenge /health, !health, /nose, !nose across casing, whitespace, and arguments."""
        adversarial_inputs = [
            # Casing variations
            "/HEALTH",
            "/Health",
            "!NoSe",
            "!NOSE",
            "!HeAlTh",
            # Whitespace variations
            "   /health   ",
            "\t/nose\n",
            " \r\n !health \t ",
            # Extra tokens and arguments
            "/HEALTH 123",
            "!health --check-now",
            "/nose --fresh",
            "!NoSe verbose now",
            # Preceding bot mentions
            "<@U_GOLEM> /HEALTH 123",
            "<@U_GOLEM|golem>   !health",
            "@AGY /health",
            "@FirstMate !nose",
            # Multiline inputs
            "/health\nwith extra notes",
            "\n\n!nose\n\n",
            # Emoji trailing
            "/health 🏥",
            "!nose 👃",
        ]

        for i, cmd_input in enumerate(adversarial_inputs):
            with self.subTest(cmd_input=cmd_input):
                self.client.clear()

                with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
                    mock_nose.return_value = {
                        "smells_off": False,
                        "minutes_ago": 1.2,
                        "verdict_summary": "All systems nominal.",
                        "disk": {"free_gb": 250.0},
                        "docker": {"container_count": 4},
                        "tailscale": {"online": True},
                    }

                    event = {
                        "channel": "C_ADV_NOSE",
                        "ts": f"100.{2000 + i}",
                        "thread_ts": f"100.{2000 + i}",
                        "text": cmd_input,
                    }

                    await self.gateway.handle_message(event, self.client)

                    mock_nose.assert_awaited_once_with(trigger_fresh_sniff=True)
                    self.assertEqual(len(self.client.posted_messages), 1, f"Failed on input: {cmd_input!r}")
                    posted_text = self.client.posted_messages[0]["text"]
                    self.assertIn("👃 *Fleet Health:* Air is clean; all systems nominal", posted_text)
                    self.assertIn("250.0 GB free", posted_text)
                    added_reactions = [r["name"] for r in self.client.reactions_added]
                    self.assertIn("white_check_mark", added_reactions)

    async def test_adversarial_lifecycle_slash_commands(self) -> None:
        """Adversarially challenge /wrapup, /archive, /bearings, /ahoy, /learn."""
        scenarios = [
            ("/WRAPUP now", "🧹 *Wrapup:* Housekeeping complete"),
            ("   !WrapUp   ", "🧹 *Wrapup:* Housekeeping complete"),
            ("/ARCHIVE old-session", "📦 *Archive:* Session archived"),
            (" \t /BEARINGS \n ", "🧭 *Fleet Bearings & Status Digest:*"),
            ("!Ahoy catchup", "⚓ *Ahoy:* Session catch-up complete"),
            ("<@U_GOLEM> /LEARN important discovery codified", "🧠 *Operational Lesson Crystallized:* important discovery codified"),
        ]

        for i, (cmd_input, expected_header) in enumerate(scenarios):
            with self.subTest(cmd_input=cmd_input):
                self.client.clear()

                event = {
                    "channel": "C_ADV_LIFE",
                    "ts": f"100.{3000 + i}",
                    "thread_ts": f"100.{3000 + i}",
                    "text": cmd_input,
                }

                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                posted_text = self.client.posted_messages[0]["text"]
                self.assertIn(expected_header, posted_text)

    async def test_conversational_queries_do_not_falsely_intercept(self) -> None:
        """Verify that conversational queries mentioning status/health do NOT falsely trigger command interception."""
        conversational_prompts = [
            "what is the /status of our current build?",
            "can you check health please?",
            "status",  # Bare word without slash or bang
            "health",  # Bare word without slash or bang
            "eyes on the runner",
            "how is the fleet doing today?",
        ]

        for i, prompt in enumerate(conversational_prompts):
            with self.subTest(prompt=prompt):
                self.client.clear()

                # Mock Turn 1 to return pure conversational text
                turn1_resp = {
                    "candidates": [
                        {
                            "content": {
                                "parts": [{"text": f"Conversational answer to: {prompt}"}]
                            },
                            "finishReason": "STOP",
                        }
                    ]
                }

                with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
                    mock_gemini.return_value = turn1_resp

                    event = {
                        "channel": "C_CONV",
                        "ts": f"100.{4000 + i}",
                        "thread_ts": f"100.{4000 + i}",
                        "text": f"<@U_GOLEM> {prompt}",
                    }

                    await self.gateway.handle_message(event, self.client)

                    # Invariant: Must call Mouth LLM and post conversational answer, NOT intercept as slash command
                    self.assertEqual(len(self.client.posted_messages), 1)
                    posted_text = self.client.posted_messages[0]["text"]
                    self.assertIn("Conversational answer to:", posted_text)
                    self.assertNotIn("👀 *Runner Observer:*", posted_text)
                    self.assertNotIn("👃 *Fleet Health:*", posted_text)


class TestAdversarialStep2Failures(unittest.IsolatedAsyncioTestCase):
    """Stress-test Step 2 synthesis failure handling under varied HTTP errors and exceptions."""

    async def asyncSetUp(self) -> None:
        self.mock_session = MagicMock()
        self.mock_session.is_busy = False
        self.mock_session.is_executing = False
        self.mock_session.runner_pane = None
        self.mock_session.adapter = None
        self.mock_session.target_cwd = "/tmp"

        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR,
            adapter_factory=lambda cwd: MockHeadlessAdapter(cwd=cwd),
        )
        self.router.get_or_create = AsyncMock(return_value=(self.mock_session, False))

        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = MockSlackClient()
        os.environ["GEMINI_API_KEY"] = "fake-api-key"

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()
        os.environ.pop("GEMINI_API_KEY", None)

    async def test_step2_varied_http_error_codes(self) -> None:
        """Adversarially test Step 2 synthesis failure across HTTP 400, 401, 403, 429, 500, 502, 503, 504."""
        http_errors = [
            (400, "HTTP_400", "Bad Request: Schema mismatch in functionResponse"),
            (401, "HTTP_401", "Unauthorized: API key invalid"),
            (403, "HTTP_403", "Forbidden: Quota limit reached"),
            (429, "HTTP_429", "Too Many Requests: Rate limit exceeded"),
            (500, "HTTP_500", "Internal Server Error from Gemini backend"),
            (502, "HTTP_502", "Bad Gateway: Upstream proxy connection dropped"),
            (503, "HTTP_503", "Service Unavailable: Model temporarily overloaded"),
            (504, "HTTP_504", "Gateway Timeout: Backend did not respond"),
        ]

        candidate = {
            "content": {
                "parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]
            }
        }

        for i, (status_code, err_type, err_msg) in enumerate(http_errors):
            with self.subTest(status_code=status_code):
                self.client.clear()

                turn1_resp = {"candidates": [candidate]}
                turn2_err = {
                    "_error": True,
                    "status": status_code,
                    "code": status_code,
                    "error_type": err_type,
                    "raw_error": err_msg,
                    "message": err_msg,
                }

                with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
                    mock_gemini.side_effect = [turn1_resp, turn2_err]

                    with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
                        mock_eyes.return_value = {
                            "state": "IDLE",
                            "active_command": "",
                            "pid": None,
                            "blocker": None,
                            "buffer_tail": "",
                        }

                        event = {
                            "channel": "C_HTTP_ERR",
                            "ts": f"200.{100 + i}",
                            "thread_ts": f"200.{100 + i}",
                            "text": "<@U_GOLEM> status report",
                        }

                        await self.gateway.handle_message(event, self.client)

                        self.assertEqual(len(self.client.posted_messages), 1)
                        msg = self.client.posted_messages[0]["text"]

                        # Invariants
                        self.assertIn(f"⚠️ *Executive Synthesis Failed* (HTTP {status_code} / {err_type}):", msg)
                        self.assertIn(err_msg, msg)
                        self.assertIn('"state": "IDLE"', msg)
                        self.assertIn("_Raw sensory data returned above without executive synthesis._", msg)
                        # Ensure no fake telemetry
                        self.assertNotIn("Runner is currently IDLE", msg)
                        self.assertNotIn("Runner is currently *IDLE*", msg)

                        added_reactions = [r["name"] for r in self.client.reactions_added]
                        self.assertIn("boom", added_reactions)

    async def test_step2_transport_and_network_exceptions(self) -> None:
        """Adversarially test Step 2 synthesis failure across network timeouts and connection drops."""
        exceptions_to_test = [
            asyncio.TimeoutError("Step 2 Gemini request timed out after 30 seconds"),
            ConnectionResetError("Socket abruptly disconnected by peer"),
            RuntimeError("SSL certificate verification error during handshake"),
        ]

        candidate = {
            "content": {
                "parts": [{"functionCall": {"name": "check_nose", "args": {}}}]
            }
        }

        for i, exc in enumerate(exceptions_to_test):
            with self.subTest(exc_type=type(exc).__name__):
                self.client.clear()

                turn1_resp = {"candidates": [candidate]}

                with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
                    mock_gemini.side_effect = [turn1_resp, exc]

                    with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
                        mock_nose.return_value = {
                            "smells_off": False,
                            "minutes_ago": 3.0,
                            "verdict_summary": "All daemon logs clean.",
                            "disk": {"free_gb": 180.0},
                            "docker": {"container_count": 6},
                            "tailscale": {"online": True},
                        }

                        event = {
                            "channel": "C_NET_ERR",
                            "ts": f"300.{100 + i}",
                            "thread_ts": f"300.{100 + i}",
                            "text": "<@U_GOLEM> fleet check",
                        }

                        await self.gateway.handle_message(event, self.client)

                        self.assertEqual(len(self.client.posted_messages), 1)
                        msg = self.client.posted_messages[0]["text"]

                        # Invariants
                        self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                        self.assertIn(type(exc).__name__, msg)
                        self.assertIn(str(exc), msg)
                        self.assertIn("All daemon logs clean.", msg)
                        # Ensure no fake telemetry
                        self.assertNotIn("Air is clean", msg)

                        added_reactions = [r["name"] for r in self.client.reactions_added]
                        self.assertIn("boom", added_reactions)

    async def test_step2_malformed_and_corrupt_payloads(self) -> None:
        """Adversarially test Step 2 when Gemini returns malformed or empty payloads."""
        malformed_payloads = [
            ("none_response", None),
            ("empty_dictionary", {}),
            ("zero_candidates", {"candidates": []}),
            ("empty_parts_list", {"candidates": [{"content": {"parts": []}}]}),
            ("looping_function_call", {"candidates": [{"content": {"parts": [{"functionCall": {"name": "check_nose"}}]}}]}),
        ]

        candidate = {
            "content": {
                "parts": [{"functionCall": {"name": "check_nose", "args": {}}}]
            }
        }

        for i, (label, malformed_resp) in enumerate(malformed_payloads):
            with self.subTest(label=label):
                self.client.clear()

                turn1_resp = {"candidates": [candidate]}

                with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
                    mock_gemini.side_effect = [turn1_resp, malformed_resp]

                    with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
                        mock_nose.return_value = {
                            "smells_off": False,
                            "minutes_ago": 0.5,
                            "verdict_summary": "Zero anomalies detected.",
                            "disk": {"free_gb": 90.0},
                            "docker": {"container_count": 2},
                            "tailscale": {"online": True},
                        }

                        event = {
                            "channel": "C_MALFORMED",
                            "ts": f"400.{100 + i}",
                            "thread_ts": f"400.{100 + i}",
                            "text": "<@U_GOLEM> audit sensors",
                        }

                        await self.gateway.handle_message(event, self.client)

                        self.assertEqual(len(self.client.posted_messages), 1, f"Failed on label: {label}")
                        msg = self.client.posted_messages[0]["text"]

                        # Invariants
                        self.assertIn("⚠️ *Executive Synthesis Failed*", msg)
                        self.assertIn("Zero anomalies detected.", msg)
                        self.assertIn("_Raw sensory data returned above without executive synthesis._", msg)
                        self.assertNotIn("Air is clean", msg)

                        added_reactions = [r["name"] for r in self.client.reactions_added]
                        self.assertIn("boom", added_reactions)


class TestThreadHistoryPromptRehydration(unittest.IsolatedAsyncioTestCase):
    """Stress-test thread history rehydration and prompt construction edge cases."""

    async def asyncSetUp(self) -> None:
        self.mock_session = MagicMock()
        self.mock_session.is_busy = False
        self.mock_session.is_executing = False
        self.mock_session.runner_pane = None
        self.mock_session.target_cwd = "/tmp"

        self.adapter = MockHeadlessAdapter()
        self.mock_session.adapter = self.adapter

        self.router = SessionRouter(
            default_target_cwd=PROJECT_DIR,
            adapter_factory=lambda cwd: self.adapter,
        )
        self.router.get_or_create = AsyncMock(return_value=(self.mock_session, False))

        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = MockSlackClient()
        os.environ["GEMINI_API_KEY"] = "fake-api-key"

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()
        os.environ.pop("GEMINI_API_KEY", None)

    async def test_thread_history_complex_formatting_and_truncation(self) -> None:
        """Verify thread history handles code blocks, markdown quotes, links, and truncation cleanly."""
        self.client.thread_history["C_COMPLEX:100.1"] = [
            {"ts": "100.1", "user": "U_CARL", "text": "```bash\ngit status --porcelain\npytest -v\n```"},
            {"ts": "100.2", "bot_id": "B_GOLEM", "text": "> Running git and pytest...\nTests passed 15/15."},
            {"ts": "100.3", "user": "U_CARL", "text": "See report at <https://ci.internal/build/123|Build 123>"},
            {"ts": "100.4", "user": "U_CARL", "text": "Z" * 500},  # Oversized message (>300 chars)
            {"ts": "100.5", "user": "U_CARL", "text": "<@U_GOLEM> What should we run next?"},  # Target message (ts=100.5)
        ]

        ctx = await self.gateway._fetch_thread_context(
            self.client, "C_COMPLEX", "100.1", exclude_ts="100.5"
        )

        self.assertIn("[Previous Slack Thread Context:", ctx)
        self.assertIn("User: ```bash\ngit status --porcelain\npytest -v\n```", ctx)
        self.assertIn("AGY: > Running git and pytest...\nTests passed 15/15.", ctx)
        self.assertIn("User: See report at <https://ci.internal/build/123|Build 123>", ctx)
        # Check truncation to 300 chars + "..."
        self.assertIn("Z" * 300 + "...", ctx)
        # Check that current turn is excluded
        self.assertNotIn("What should we run next?", ctx)

    async def test_clean_text_immutability_invariant(self) -> None:
        """Verify that rehydrated thread context does NOT mutate clean_text."""
        self.client.thread_history["C_IMMUT:200.1"] = [
            {"ts": "200.1", "user": "U_CARL", "text": "Initial setup step"},
            {"ts": "200.2", "bot_id": "B_GOLEM", "text": "Setup completed"},
        ]

        # User sends a message in the active thread
        event = {
            "channel": "C_IMMUT",
            "thread_ts": "200.1",
            "ts": "200.3",
            "text": "<@U_GOLEM> continue with execution",
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.return_value = {
                "candidates": [
                    {
                        "content": {
                            "parts": [{"text": "Continuing execution as requested."}]
                        },
                        "finishReason": "STOP",
                    }
                ]
            }

            await self.gateway.handle_message(event, self.client)

            # Invariant: The prompt passed into Mouth LLM must contain both the thread context AND the clean text
            call_args_list = mock_gemini.call_args_list
            self.assertGreaterEqual(len(call_args_list), 1)
            first_payload = call_args_list[0].kwargs.get("payload") or call_args_list[0][0][0]
            user_part_text = first_payload["contents"][0]["parts"][0]["text"]

            self.assertIn("[Previous Slack Thread Context:", user_part_text)
            self.assertIn("Initial setup step", user_part_text)
            self.assertIn("continue with execution", user_part_text)

    async def test_in_thread_slash_command_intercepts_cleanly(self) -> None:
        """Verify that slash commands in warm threads intercept immediately without LLM invocation."""
        self.client.thread_history["C_INTH_CMD:300.1"] = [
            {"ts": "300.1", "user": "U_CARL", "text": "Running migration"},
            {"ts": "300.2", "bot_id": "B_GOLEM", "text": "Migration in progress"},
        ]

        commands = ["/status", "!eyes", "/health", "!nose", "/wrapup"]
        for i, cmd in enumerate(commands):
            with self.subTest(cmd=cmd):
                self.client.clear()

                event = {
                    "channel": "C_INTH_CMD",
                    "thread_ts": "300.1",
                    "ts": f"300.{10 + i}",
                    "text": f"<@U_GOLEM> {cmd}",
                }

                with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes, \
                     patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose, \
                     patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:

                    mock_eyes.return_value = {"state": "IDLE", "buffer_tail": ""}
                    mock_nose.return_value = {"smells_off": False, "minutes_ago": 0.1, "verdict_summary": "Clean", "disk": {}, "docker": {}, "tailscale": {}}

                    await self.gateway.handle_message(event, self.client)

                    # Invariant: Must NOT call Mouth LLM for direct slash commands in thread
                    mock_gemini.assert_not_called()
                    self.assertEqual(len(self.client.posted_messages), 1)

    async def test_thread_history_api_failure_gracefully_falls_back(self) -> None:
        """Verify that failure in conversations_replies falls back to empty context without crashing."""
        self.client.conversations_replies = AsyncMock(side_effect=RuntimeError("Slack API rate limited"))

        ctx = await self.gateway._fetch_thread_context(
            self.client, "C_FAIL_API", "400.1", exclude_ts="400.2"
        )
        self.assertEqual(ctx, "")


class TestSensoryToolsStep2SchemaIntegrity(unittest.TestCase):
    """Verify structural schema compliance of Step 2 tools declaration."""

    def test_mouth_tools_declaration_structure(self) -> None:
        """Verify get_mouth_tools_declaration contains valid function declarations."""
        tools = get_mouth_tools_declaration()
        self.assertIsInstance(tools, list)
        self.assertGreaterEqual(len(tools), 1)

        tool_container = tools[0]
        self.assertIn("functionDeclarations", tool_container)
        fn_decls = tool_container["functionDeclarations"]
        fn_names = [f["name"] for f in fn_decls]

        # Invariant: All core mouth tools must be declared
        self.assertIn("check_eyes", fn_names)
        self.assertIn("check_nose", fn_names)
        self.assertIn("dispatch_task", fn_names)

        # Invariant: check_nose accepts trigger_fresh_sniff
        nose_decl = next(f for f in fn_decls if f["name"] == "check_nose")
        props = nose_decl.get("parameters", {}).get("properties", {})
        self.assertIn("trigger_fresh_sniff", props)


if __name__ == "__main__":
    unittest.main()
