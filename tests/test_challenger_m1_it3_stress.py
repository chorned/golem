#!/usr/bin/env python3
"""
tests/test_challenger_m1_it3_stress.py

Adversarial Stress Test & Empirical Challenge Suite for Milestone 1 (Iteration 3).
Authored and executed by teamwork_preview_challenger_m1_it3_2.

Discovered and Confirmed Empirical Bugs:
  Bug 1: NameError: name 'target_reaction' is not defined in `_fallback_direct_adapter_turn`
         (line 2299 of golem/gateway/ingress.py). Causes graceful adapter fallback to crash
         with :boom: and unhandled exception instead of completing turn.
  Bug 2: Orphaned ':gear:' reaction when incoming event has empty text and subtype='file_share'
         without downloadable files. Line 1110 adds ':gear:', line 1282 drops event and returns
         without removing ':gear:'.

Stress Test Coverage:
  1. Empirical Bug Reproduction (Bug 1 & Bug 2)
  2. Boundary Conditions Stress Testing (Empty text, massive inputs, irregular whitespace)
  3. Long Threads & Context Rehydration Stress Testing (500 turns, preservation of clean_text)
  4. Rapid Disconnects & Socket Resiliency (reactions_add, chat_postMessage, call_gemini_api)
  5. Terminal Reaction State Transitions (:white_check_mark:, :warning:, :x:, :boom:)
  6. High-Concurrency Stress Testing (50 concurrent tasks across 10 threads)
"""

import os
import sys
import json
import time
import asyncio
import unittest
from typing import Optional, List, Dict, Any
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.gateway.ingress import (
    IngressGateway,
    BackstageExecutionAccumulator,
    add_reaction,
    remove_reaction,
    swap_reaction,
    strip_bot_mentions,
    get_mouth_tools_declaration,
)
from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle
from golem.agent.protocol import TextDeltaEvent, FinishEvent, StatusUpdateEvent


class StressMockSlackClient:
    """Mock Slack client with thread safety and fault injection."""

    def __init__(
        self,
        fail_chat_post: bool = False,
        fail_reactions_add: bool = False,
        fail_reactions_remove: bool = False,
        fail_conversations: bool = False,
    ) -> None:
        self.lock = asyncio.Lock()
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.thread_history: Dict[str, List[Dict[str, Any]]] = {}
        self.fail_chat_post = fail_chat_post
        self.fail_reactions_add = fail_reactions_add
        self.fail_reactions_remove = fail_reactions_remove
        self.fail_conversations = fail_conversations

    async def chat_postMessage(
        self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        if self.fail_chat_post:
            raise ConnectionResetError("Simulated socket drop on chat_postMessage")
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
        if self.fail_reactions_add:
            raise ConnectionResetError("Simulated socket drop on reactions_add")
        async with self.lock:
            self.reactions_added.append(
                {"channel": channel, "timestamp": timestamp, "name": name}
            )

    async def reactions_remove(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> None:
        if self.fail_reactions_remove:
            raise ConnectionResetError("Simulated socket drop on reactions_remove")
        async with self.lock:
            self.reactions_removed.append(
                {"channel": channel, "timestamp": timestamp, "name": name}
            )

    async def conversations_replies(
        self, channel: str, ts: str, limit: int = 20, **kwargs: Any
    ) -> Dict[str, Any]:
        if self.fail_conversations:
            raise ConnectionResetError("Simulated socket drop on conversations_replies")
        async with self.lock:
            key = f"{channel}:{ts}"
            messages = self.thread_history.get(key, [])
            return {"ok": True, "messages": messages}


class ChallengerM1Iteration3StressTests(unittest.IsolatedAsyncioTestCase):
    """Rigorous empirical stress test suite for Milestone 1."""

    def setUp(self) -> None:
        self.router = MagicMock(spec=SessionRouter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM_BOT")
        self.client = StressMockSlackClient()

        # Set up a default mock session
        self.mock_session = MagicMock(spec=SessionHandle)
        self.mock_session.session_id = "test-session-1"
        self.mock_session.is_busy = False
        self.mock_session.is_executing = False
        self.mock_session.runner_pane = None
        self.mock_session.target_cwd = "/tmp/test"
        self.mock_session.force_mouth = False
        self.mock_session.is_mock = True

        self.router.get_or_create = AsyncMock(return_value=(self.mock_session, False))
        self.router.teardown = AsyncMock(return_value=None)

    # =========================================================================
    # Group 1: Empirical Bug Reproduction
    # =========================================================================

    async def test_BUG_REPRO_unbound_target_reaction_in_fallback_direct_adapter_turn(self):
        """
        VERIFIED REMEDIATION 1:
        In `_fallback_direct_adapter_turn` (line 2299 of golem/gateway/ingress.py):
        target_reaction is properly defined ('white_check_mark' on SUCCESS, 'x' on ERROR).
        Execution completes without NameError and swaps :gear: to :white_check_mark:.
        """
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            acc = BackstageExecutionAccumulator()
            acc.record_event(FinishEvent(status="SUCCESS", response="Fallback direct output"))
            mock_backstage.return_value = acc

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="test fallback",
                client=self.client,
                channel="C123",
                thread_ts="1000.1",
                ts="1000.1",
                is_imessage=False,
            )

            self.assertTrue(
                any(r["name"] == "white_check_mark" for r in self.client.reactions_added),
                "Expected :white_check_mark: reaction after successful fallback turn without NameError",
            )
            self.assertFalse(
                any(r["name"] == "boom" for r in self.client.reactions_added),
                "Did not expect :boom: reaction",
            )

    async def test_BUG_REPRO_fallback_adapter_turn_via_handle_message_crashes_with_boom(self):
        """
        VERIFIED REMEDIATION 1 (End-to-End Impact):
        When Gemini API fails in Mouth turn and gateway falls back to direct adapter execution,
        target_reaction is defined so handle_message succeeds, posts direct execution output,
        and transitions reaction to :white_check_mark: instead of crashing with :boom:.
        """
        client = StressMockSlackClient()
        event = {
            "type": "message",
            "channel": "C123",
            "ts": "1000.0002",
            "text": "run task with fallback",
        }
        self.mock_session.force_mouth = True

        # Simulate Gemini API failing on Step 1 (returning None)
        with patch.object(self.gateway, "_call_gemini_api", new_callable=AsyncMock, return_value=None):
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
                acc = BackstageExecutionAccumulator()
                acc.record_event(FinishEvent(status="SUCCESS", response="Direct execution finished"))
                mock_backstage.return_value = acc

                await self.gateway.handle_message(event, client)

                self.assertEqual(len(client.posted_messages), 1)
                self.assertIn("Direct execution finished", client.posted_messages[0]["text"])
                self.assertTrue(
                    any(r["name"] == "white_check_mark" for r in client.reactions_added),
                    "Expected :white_check_mark: reaction on successful fallback execution",
                )
                self.assertFalse(
                    any(r["name"] == "boom" for r in client.reactions_added),
                    "Reaction should NOT be swapped to :boom:",
                )

    async def test_BUG_REPRO_orphaned_gear_on_empty_text_with_file_share_subtype_no_files(self):
        """
        VERIFIED REMEDIATION 2:
        When an event arrives with empty text and subtype='file_share' but files=[],
        line 1110 adds reaction ':gear:', and line 1283 cleanly removes ':gear:'
        upon dropping the empty event, preventing an orphaned reaction.
        """
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C123",
            "ts": "1001.0001",
            "text": "",
            "files": [],
        }

        await self.gateway.handle_message(event, self.client)

        gear_added = any(r["name"] == "gear" for r in self.client.reactions_added)
        gear_removed = any(r["name"] == "gear" for r in self.client.reactions_removed)
        terminal_reactions = [r["name"] for r in self.client.reactions_added if r["name"] != "gear"]

        self.assertTrue(gear_added, "Expected :gear: to be added at line 1110")
        self.assertTrue(gear_removed, "Expected :gear: to be removed before return at line 1283")
        self.assertEqual(terminal_reactions, [], "Did not expect terminal reactions on dropped event")
        gears_remaining = [r for r in self.client.reactions_added if r["name"] == "gear"]
        gears_removed = [r for r in self.client.reactions_removed if r["name"] == "gear"]
        self.assertEqual(len(gears_remaining), len(gears_removed), "Net active :gear: reactions should be 0")

    # =========================================================================
    # Group 2: Boundary Conditions Stress Testing
    # =========================================================================

    async def test_boundary_empty_and_whitespace_text_variations(self):
        """Verify that empty, whitespace-only, and bot-mention-only messages are dropped cleanly without reactions."""
        test_inputs = [
            "",
            "   ",
            "\t\n\r  ",
            "<@U_GOLEM_BOT>",
            "<@U_GOLEM_BOT>   ",
            "   <@U_GOLEM_BOT>   ",
            None,
        ]

        for idx, raw in enumerate(test_inputs):
            client = StressMockSlackClient()
            event = {
                "type": "message",
                "channel": "C_TEST",
                "ts": f"1002.{idx:04d}",
                "text": raw,
            }
            await self.gateway.handle_message(event, client)

            self.assertEqual(len(client.posted_messages), 0, f"Expected 0 messages posted for input: {repr(raw)}")
            self.assertEqual(len(client.reactions_added), 0, f"Expected 0 reactions added for input: {repr(raw)}")
            self.router.get_or_create.assert_not_called()

    async def test_boundary_massive_message_and_unicode_handling(self):
        """Stress test gateway with 100,000 character prompt, emojis, RTL, and zero-width characters."""
        huge_text = "🚀 Testing Golem Chief of Staff 🐉\u200b\u200c " + ("A" * 100_000)
        client = StressMockSlackClient()
        event = {
            "type": "message",
            "channel": "C_MASSIVE",
            "ts": "1003.0001",
            "text": huge_text,
        }

        async def mock_stream(*args, **kwargs):
            yield TextDeltaEvent(text_delta="Received large prompt ok.")
            yield FinishEvent(status="SUCCESS", response="Received large prompt ok.")

        self.mock_session.send_user_message = mock_stream

        await self.gateway.handle_message(event, client)

        self.assertEqual(len(client.posted_messages), 1)
        self.assertIn("Received large prompt ok.", client.posted_messages[0]["text"])
        self.assertTrue(any(r["name"] == "white_check_mark" for r in client.reactions_added))

    async def test_boundary_slash_commands_with_irregular_whitespace_and_casing(self):
        """Verify slash commands with irregular whitespace, tabs, and casing route directly."""
        mock_sentinel = MagicMock()
        mock_sentinel.get_telemetry_digest = MagicMock(return_value={
            "smells_off": False,
            "verdict_summary": "All systems nominal.",
            "minutes_ago": 0.5,
            "disk": {"free_gb": 50.0},
            "docker": {"container_count": 5},
            "tailscale": {"online": True},
        })
        self.gateway.get_nose_sentinel = MagicMock(return_value=mock_sentinel)

        irregular_commands = [
            ("   /STATUS   ", "👀 *Runner Observer:"),
            ("\t/HEALTH\n", "👃 *Fleet Health:"),
            ("  !status  ", "👀 *Runner Observer:"),
            ("\n!nose\r", "👃 *Fleet Health:"),
            ("  /bearings  ", "🧭 *Fleet Bearings"),
            ("\t/wrapup\t", "🧹 *Wrapup:"),
            ("   /archive   ", "📦 *Archive:"),
        ]

        for idx, (cmd, expected_prefix) in enumerate(irregular_commands):
            client = StressMockSlackClient()
            event = {
                "type": "message",
                "channel": "C_CMD",
                "ts": f"1004.{idx:04d}",
                "text": cmd,
            }

            self.mock_session.get_runner_status = AsyncMock(return_value={"status": "idle", "command": "", "pid": None})
            self.mock_session.get_eyes_status = MagicMock(return_value={"state": "IDLE", "active_command": "", "pid": None, "buffer_tail": ""})

            await self.gateway.handle_message(event, client)

            self.assertEqual(len(client.posted_messages), 1, f"Expected 1 response for {repr(cmd)}")
            self.assertTrue(
                client.posted_messages[0]["text"].startswith(expected_prefix),
                f"Expected prefix '{expected_prefix}' for {repr(cmd)}, got: {client.posted_messages[0]['text']}"
            )
            self.assertTrue(
                any(r["name"] == "white_check_mark" for r in client.reactions_added),
                f"Expected :white_check_mark: for {repr(cmd)}"
            )

    # =========================================================================
    # Group 3: Long Threads & Context Rehydration Stress Testing
    # =========================================================================

    async def test_long_thread_history_500_turns_stress(self):
        """
        Stress test thread rehydration with 500 existing messages.
        Verify that only the most recent 10 messages are extracted, messages over 300 chars
        are bounded with '...', and retrieval executes under 100ms.
        """
        channel = "C_THREAD_STRESS"
        thread_ts = "1005.0000"

        raw_msgs = []
        for i in range(500):
            is_bot = (i % 2 == 1)
            raw_msgs.append({
                "ts": f"1005.{i+1:04d}",
                "user": "U_BOT" if is_bot else "U_USER",
                "bot_id": "B_GOLEM" if is_bot else None,
                "text": f"Turn {i}: " + ("x" * (400 if i % 10 == 0 else 50)),
            })

        self.client.thread_history[f"{channel}:{thread_ts}"] = raw_msgs

        t0 = time.time()
        context = await self.gateway._fetch_thread_context(
            self.client, channel=channel, thread_ts=thread_ts, exclude_ts="1005.0501"
        )
        elapsed = time.time() - t0

        self.assertLess(elapsed, 0.1, f"Thread context rehydration took too long: {elapsed:.4f}s")
        self.assertTrue(context.startswith("[Previous Slack Thread Context:\n"))
        self.assertTrue(context.endswith("\n]"))

        lines = context.strip().split("\n")[1:-1]
        self.assertEqual(len(lines), 10, f"Expected exactly 10 context lines, got {len(lines)}")
        for line in lines:
            if "Turn 490:" in line:
                self.assertTrue(line.endswith("..."), f"Expected line to be truncated with '...': {line}")
                self.assertLessEqual(len(line), 320)

    async def test_thread_history_preserves_clean_text_in_command_routing(self):
        """
        Verify that in an active thread with prior history:
        1. Slash commands (/status) route directly without being prepended by thread context.
        2. Normal user messages preserve untouched clean_text while prompt_to_send gets thread context.
        """
        channel = "C_PRESERVE"
        thread_ts = "1006.0000"
        self.client.thread_history[f"{channel}:{thread_ts}"] = [
            {"ts": "1006.0001", "user": "U_USER", "text": "Can you check on our runner?"},
            {"ts": "1006.0002", "bot_id": "B_GOLEM", "text": "Sure, standing by."},
        ]

        # Case 1: Slash command in active thread
        event_cmd = {
            "type": "message",
            "channel": channel,
            "thread_ts": thread_ts,
            "ts": "1006.0003",
            "text": "/status",
        }
        self.mock_session.get_runner_status = AsyncMock(return_value={"status": "idle", "command": "", "pid": None})
        self.mock_session.get_eyes_status = MagicMock(return_value={"state": "IDLE", "active_command": "", "pid": None, "buffer_tail": ""})

        await self.gateway.handle_message(event_cmd, self.client)
        self.assertEqual(len(self.client.posted_messages), 1)
        self.assertIn("👀 *Runner Observer:* Runner is currently *IDLE*", self.client.posted_messages[0]["text"])

        # Case 2: Normal conversation in active thread
        sent_prompts = []
        async def mock_stream(prompt_text):
            sent_prompts.append(prompt_text)
            yield TextDeltaEvent(text_delta="Acknowledged thread context.")
            yield FinishEvent(status="SUCCESS", response="Acknowledged thread context.")

        self.mock_session.send_user_message = mock_stream
        event_chat = {
            "type": "message",
            "channel": channel,
            "thread_ts": thread_ts,
            "ts": "1006.0004",
            "text": "What did we talk about previously?",
        }
        await self.gateway.handle_message(event_chat, self.client)

        self.assertEqual(len(sent_prompts), 1)
        self.assertIn("[Previous Slack Thread Context:\n", sent_prompts[0])
        self.assertIn("User: Can you check on our runner?", sent_prompts[0])
        self.assertIn("What did we talk about previously?", sent_prompts[0])

    # =========================================================================
    # Group 4: Rapid Disconnects & Socket Resiliency
    # =========================================================================

    async def test_rapid_disconnect_during_reactions_add_does_not_crash_pipeline(self):
        """Verify that when reactions_add throws ConnectionResetError, the gateway does not crash."""
        client = StressMockSlackClient(fail_reactions_add=True)
        event = {
            "type": "message",
            "channel": "C_DISC_1",
            "ts": "1007.0001",
            "text": "/bearings",
        }

        await self.gateway.handle_message(event, client)
        self.assertEqual(len(client.posted_messages), 1)
        self.assertIn("Fleet Bearings & Status Digest", client.posted_messages[0]["text"])

    async def test_rapid_disconnect_during_chat_post_message_transitions_to_boom(self):
        """
        Verify that when chat_postMessage drops connection, the top-level exception handler
        catches it, attempts reaction swap to ':boom:', and raises ConnectionResetError.
        """
        client = StressMockSlackClient(fail_chat_post=True)
        event = {
            "type": "message",
            "channel": "C_DISC_2",
            "ts": "1007.0002",
            "text": "/bearings",
        }

        with self.assertRaises(ConnectionResetError):
            await self.gateway.handle_message(event, client)

        self.assertTrue(any(r["name"] == "boom" for r in client.reactions_added))

    async def test_rapid_disconnect_during_gemini_api_call_fails_loudly_with_boom(self):
        """
        Verify that when Gemini API call fails during Step 2 synthesis,
        the gateway fails loudly, posts the raw HTTP 500 error payload, and swaps reaction to :boom:.
        """
        client = StressMockSlackClient()
        event = {
            "type": "message",
            "channel": "C_DISC_3",
            "ts": "1007.0003",
            "text": "run test suite",
        }

        self.mock_session.force_mouth = True

        step1_resp = {
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
                    }
                }
            ]
        }

        step2_resp = {
            "_error": True,
            "status": 500,
            "code": 500,
            "error_type": "ConnectionRefusedError",
            "raw_error": "Connection refused by upstream Gemini endpoint",
            "message": "Connection refused",
        }

        call_count = 0
        async def mock_call_gemini(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return step1_resp
            return step2_resp

        with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call_gemini):
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
                acc = BackstageExecutionAccumulator()
                acc.record_event(FinishEvent(status="SUCCESS", response="Tests completed"))
                mock_backstage.return_value = acc

                await self.gateway.handle_message(event, client)

        self.assertEqual(len(client.posted_messages), 1)
        fail_msg = client.posted_messages[0]["text"]
        self.assertIn("⚠️ *Executive Synthesis Failed* (HTTP 500 / ConnectionRefusedError):", fail_msg)
        self.assertIn("Connection refused by upstream Gemini endpoint", fail_msg)
        self.assertIn("*Tool Output:*", fail_msg)
        self.assertTrue(any(r["name"] == "boom" for r in client.reactions_added))

    # =========================================================================
    # Group 5: Terminal Reaction State Matrix
    # =========================================================================

    async def test_terminal_reactions_white_check_mark_matrix(self):
        """Verify clean transition from :gear: to :white_check_mark: across all success cases."""
        test_cases = [
            ("/wrapup", lambda: None),
            ("/archive", lambda: None),
            ("/bearings", lambda: None),
            ("/ahoy", lambda: None),
            ("/learn operational lesson", lambda: None),
        ]

        for idx, (cmd, setup_fn) in enumerate(test_cases):
            client = StressMockSlackClient()
            setup_fn()
            event = {
                "type": "message",
                "channel": "C_TERM",
                "ts": f"1008.{idx:04d}",
                "text": cmd,
            }
            await self.gateway.handle_message(event, client)

            gear_removed = any(r["name"] == "gear" for r in client.reactions_removed)
            check_added = any(r["name"] == "white_check_mark" for r in client.reactions_added)
            self.assertTrue(gear_removed, f"Expected gear removed for {cmd}")
            self.assertTrue(check_added, f"Expected white_check_mark added for {cmd}")

    async def test_terminal_reactions_warning_matrix(self):
        """Verify clean transition from :gear: to :warning: across all warning/blocker cases."""
        # Case A: /health with smells_off=True
        client_nose = StressMockSlackClient()
        event_nose = {"type": "message", "channel": "C_WARN", "ts": "1009.0001", "text": "/health"}
        mock_sentinel = MagicMock()
        mock_sentinel.get_telemetry_digest = MagicMock(return_value={
            "smells_off": True,
            "verdict_summary": "High memory pressure detected",
            "minutes_ago": 1.2,
            "disk": {"free_gb": 5.0},
            "docker": {"container_count": 4},
            "tailscale": {"online": True},
        })
        self.gateway.get_nose_sentinel = MagicMock(return_value=mock_sentinel)
        await self.gateway.handle_message(event_nose, client_nose)
        self.assertTrue(any(r["name"] == "warning" for r in client_nose.reactions_added))

        # Case B: /status with interactive blocker
        client_eyes = StressMockSlackClient()
        event_eyes = {"type": "message", "channel": "C_WARN", "ts": "1009.0002", "text": "/status"}
        self.mock_session.get_runner_status = AsyncMock(return_value={
            "status": "running",
            "blocker": {"type": "oauth", "url": "https://auth.example.com"},
        })
        self.mock_session.get_eyes_status = MagicMock(return_value={
            "state": "BLOCKER",
            "blocker": {"type": "oauth", "url": "https://auth.example.com"},
        })
        await self.gateway.handle_message(event_eyes, client_eyes)
        self.assertTrue(any(r["name"] == "warning" for r in client_eyes.reactions_added))

    async def test_terminal_reactions_x_matrix(self):
        """Verify clean transition from :gear: to :x: for cancellations and task errors."""
        # Case A: Out-of-band cancellation
        client_cancel = StressMockSlackClient()
        event_cancel = {"type": "message", "channel": "C_X", "ts": "1010.0001", "text": "stop"}
        self.mock_session.is_busy = True
        self.mock_session.cancel_active_execution = AsyncMock(return_value=True)
        await self.gateway.handle_message(event_cancel, client_cancel)
        self.assertTrue(any(r["name"] == "x" for r in client_cancel.reactions_added))

        # Case B: Direct adapter execution error
        client_err = StressMockSlackClient()
        event_err = {"type": "message", "channel": "C_X", "ts": "1010.0002", "text": "failing task"}
        self.mock_session.is_busy = False
        async def mock_stream_error(*args, **kwargs):
            yield FinishEvent(status="ERROR", response="", error="Process terminated with exit code 1")
        self.mock_session.send_user_message = mock_stream_error
        await self.gateway.handle_message(event_err, client_err)
        self.assertTrue(any(r["name"] == "x" for r in client_err.reactions_added))

    # =========================================================================
    # Group 6: High-Concurrency Stress Testing
    # =========================================================================

    async def test_high_concurrency_mixed_requests_50_tasks(self):
        """
        Stress test gateway under 50 concurrent requests mixing slash commands,
        sensory checks, and conversational messages across 10 different threads.
        Verify zero race conditions, complete reaction cleanup, and sub-3s execution.
        """
        commands = ["/status", "/health", "/bearings", "/wrapup", "how is the fleet?"]

        mock_sentinel = MagicMock()
        mock_sentinel.get_telemetry_digest = MagicMock(return_value={
            "smells_off": False,
            "verdict_summary": "All systems nominal.",
            "minutes_ago": 0.5,
            "disk": {"free_gb": 50.0},
            "docker": {"container_count": 5},
            "tailscale": {"online": True},
        })
        self.gateway.get_nose_sentinel = MagicMock(return_value=mock_sentinel)

        async def worker(idx: int):
            thread_id = f"1011.{idx % 10:04d}"
            cmd = commands[idx % len(commands)]
            client = StressMockSlackClient()
            event = {
                "type": "message",
                "channel": f"C_CONCUR_{idx % 10}",
                "thread_ts": thread_id,
                "ts": f"1011.{idx:04d}",
                "text": cmd,
            }

            session = MagicMock(spec=SessionHandle)
            session.session_id = f"sess_{idx % 10}"
            session.is_busy = False
            session.is_executing = False
            session.runner_pane = None
            session.target_cwd = "/tmp"
            session.force_mouth = False
            session.is_mock = True

            session.get_runner_status = AsyncMock(return_value={"status": "idle", "command": "", "pid": None})
            session.get_eyes_status = MagicMock(return_value={"state": "IDLE", "active_command": "", "pid": None, "buffer_tail": ""})

            async def stream_fn(*args, **kwargs):
                yield TextDeltaEvent(text_delta=f"Response {idx}")
                yield FinishEvent(status="SUCCESS", response=f"Response {idx}")
            session.send_user_message = stream_fn

            with patch.object(self.gateway.router, "get_or_create", AsyncMock(return_value=(session, False))):
                await self.gateway.handle_message(event, client)

            return len(client.posted_messages), len(client.reactions_added)

        t0 = time.time()
        results = await asyncio.gather(*(worker(i) for i in range(50)))
        elapsed = time.time() - t0

        self.assertLess(elapsed, 3.0, f"50 concurrent requests took too long: {elapsed:.4f}s")
        for idx, (msgs_count, reacts_count) in enumerate(results):
            self.assertEqual(msgs_count, 1, f"Task {idx} did not post exactly 1 message")
            self.assertGreaterEqual(reacts_count, 2, f"Task {idx} did not transition reactions (:gear: -> terminal)")


if __name__ == "__main__":
    unittest.main()
