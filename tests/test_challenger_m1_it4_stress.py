#!/usr/bin/env python3
"""
tests/test_challenger_m1_it4_stress.py

Adversarial Stress Test Suite for Milestone 1 (Iteration 4).
Authored and executed by teamwork_preview_challenger_m1_it4_2.

Adversarial Objectives:
1. Stress test reaction transitions under edge cases, race conditions, dropped events, and network failures.
2. Formally verify zero orphaned ':gear:' reactions across all ingress paths.
3. Stress test direct execution fallback under simulated failure modes:
   - Syntax errors / exception events in adapter stream
   - Empty outputs / blank returns
   - Subprocess execution timeouts (>3.0s yield window)
   - Synchronous exceptions in stream initiation
   - Simulated socket drops and Slack API connection errors

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
from golem.agent.protocol import (
    TextDeltaEvent,
    FinishEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
)


class MockSlackClient:
    """Thread-safe mock Slack client with fault injection capabilities."""

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
            raise ConnectionResetError("Simulated socket reset on chat_postMessage")
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
            raise ConnectionResetError("Simulated socket reset on reactions_add")
        async with self.lock:
            self.reactions_added.append(
                {"channel": channel, "timestamp": timestamp, "name": name}
            )

    async def reactions_remove(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> None:
        if self.fail_reactions_remove:
            raise ConnectionResetError("Simulated socket reset on reactions_remove")
        async with self.lock:
            self.reactions_removed.append(
                {"channel": channel, "timestamp": timestamp, "name": name}
            )

    async def conversations_replies(
        self, channel: str, ts: str, limit: int = 20, **kwargs: Any
    ) -> Dict[str, Any]:
        if self.fail_conversations:
            raise ConnectionResetError("Simulated socket reset on conversations_replies")
        async with self.lock:
            key = f"{channel}:{ts}"
            messages = self.thread_history.get(key, [])
            return {"ok": True, "messages": messages}

    def active_gear_count(self, timestamp: Optional[str] = None) -> int:
        """Count net active :gear: reactions (added minus removed)."""
        if timestamp:
            added = sum(1 for r in self.reactions_added if r["name"] == "gear" and r["timestamp"] == timestamp)
            removed = sum(1 for r in self.reactions_removed if r["name"] == "gear" and r["timestamp"] == timestamp)
        else:
            added = sum(1 for r in self.reactions_added if r["name"] == "gear")
            removed = sum(1 for r in self.reactions_removed if r["name"] == "gear")
        return added - removed


class ChallengerM1Iteration4StressTests(unittest.IsolatedAsyncioTestCase):
    """Rigorous empirical stress test suite for Milestone 1 Iteration 4."""

    def setUp(self) -> None:
        self.router = MagicMock(spec=SessionRouter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM_BOT")
        self.client = MockSlackClient()

        self.mock_session = MagicMock(spec=SessionHandle)
        self.mock_session.session_id = "test-session-m1-it4"
        self.mock_session.is_busy = False
        self.mock_session.is_executing = False
        self.mock_session.runner_pane = None
        self.mock_session.target_cwd = "/tmp/test"
        self.mock_session.force_mouth = False
        self.mock_session.is_mock = True

        self.router.get_or_create = AsyncMock(return_value=(self.mock_session, False))
        self.router.teardown = AsyncMock(return_value=None)

    # =========================================================================
    # Group 1: Reaction Transitions & Dropped Events Edge Cases
    # =========================================================================

    async def test_dropped_empty_text_no_files_leaves_zero_reactions(self):
        """Events with completely empty text and no files are dropped before any reaction is added."""
        event = {
            "type": "message",
            "channel": "C123",
            "ts": "1000.0001",
            "text": "",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(len(self.client.reactions_added), 0)
        self.assertEqual(len(self.client.reactions_removed), 0)
        self.assertEqual(len(self.client.posted_messages), 0)
        self.assertEqual(self.client.active_gear_count("1000.0001"), 0)

    async def test_dropped_whitespace_and_bot_mentions_leaves_zero_reactions(self):
        """Events with whitespace or only bot mentions are dropped before any reaction is added."""
        cases = [
            "   \t\n  ",
            "<@U_GOLEM_BOT>",
            "<@U_GOLEM_BOT>    ",
            "@golem   ",
            "@AGY",
        ]
        for i, text in enumerate(cases):
            ts = f"1000.00{i+10}"
            event = {
                "type": "message",
                "channel": "C123",
                "ts": ts,
                "text": text,
            }
            await self.gateway.handle_message(event, self.client)
            self.assertEqual(self.client.active_gear_count(ts), 0)
        self.assertEqual(len(self.client.reactions_added), 0)

    async def test_duplicate_ts_dropped_without_reaction_leak(self):
        """Duplicate events dropped by deduplication set never receive reactions."""
        ts = "1000.0099"
        event = {
            "type": "message",
            "channel": "C123",
            "ts": ts,
            "text": "/wrapup",
        }
        # First processing
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(self.client.active_gear_count(ts), 0)
        added_count_1 = len(self.client.reactions_added)

        # Duplicate event with same ts
        await self.gateway.handle_message(event, self.client)
        added_count_2 = len(self.client.reactions_added)
        self.assertEqual(added_count_1, added_count_2, "Duplicate message should not add any additional reactions")
        self.assertEqual(self.client.active_gear_count(ts), 0)

    async def test_dropped_file_share_with_empty_files_cleans_up_gear(self):
        """Events with subtype='file_share' and files=[] add :gear: and cleanly remove it when dropped."""
        ts = "1001.0001"
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C123",
            "ts": ts,
            "text": "",
            "files": [],
        }
        await self.gateway.handle_message(event, self.client)
        self.assertTrue(any(r["name"] == "gear" for r in self.client.reactions_added))
        self.assertTrue(any(r["name"] == "gear" for r in self.client.reactions_removed))
        self.assertEqual(self.client.active_gear_count(ts), 0)

    async def test_imessage_bypasses_all_reactions_across_all_paths(self):
        """iMessage events must never invoke reactions_add or reactions_remove under any circumstance."""
        ts = "1002.0001"
        event = {
            "type": "message",
            "channel": "chat123",
            "ts": ts,
            "text": "/wrapup",
            "is_imessage": True,
        }
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(len(self.client.reactions_added), 0)
        self.assertEqual(len(self.client.reactions_removed), 0)
        self.assertEqual(len(self.client.posted_messages), 1)

    async def test_empty_or_none_ts_bypasses_reactions(self):
        """Events with missing ts or empty ts never invoke reactions."""
        event = {
            "type": "message",
            "channel": "C123",
            "ts": "",
            "text": "/bearings",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(len(self.client.reactions_added), 0)
        self.assertEqual(len(self.client.reactions_removed), 0)

    async def test_slash_commands_transition_reactions_cleanly(self):
        """All slash commands (/wrapup, /archive, /bearings, /ahoy, /learn) cleanly transition :gear: -> :white_check_mark:."""
        commands = ["/wrapup", "/archive", "/bearings", "/ahoy", "/learn note"]
        for i, cmd in enumerate(commands):
            ts = f"1003.00{i}"
            event = {
                "type": "message",
                "channel": "C123",
                "ts": ts,
                "text": cmd,
            }
            await self.gateway.handle_message(event, self.client)
            self.assertEqual(self.client.active_gear_count(ts), 0)
            added_terminal = [r["name"] for r in self.client.reactions_added if r["timestamp"] == ts and r["name"] != "gear"]
            self.assertEqual(added_terminal, ["white_check_mark"])

    async def test_sensory_health_command_transitions(self):
        """Test /health transitions: clean -> :white_check_mark:, anomaly -> :warning:, error -> :warning:."""
        # 1. Clean health
        with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
            mock_nose.return_value = {
                "smells_off": False,
                "verdict_summary": "All clear",
                "minutes_ago": 1.0,
                "disk": {"free_gb": 50},
                "docker": {"container_count": 2},
                "tailscale": {"online": True},
            }
            ts = "1004.0001"
            await self.gateway.handle_message({"type": "message", "channel": "C123", "ts": ts, "text": "/health"}, self.client)
            self.assertEqual(self.client.active_gear_count(ts), 0)
            self.assertTrue(any(r["timestamp"] == ts and r["name"] == "white_check_mark" for r in self.client.reactions_added))

        # 2. Anomaly health
        with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
            mock_nose.return_value = {
                "smells_off": True,
                "verdict_summary": "Disk pressure",
                "minutes_ago": 2.0,
                "disk": {"free_gb": 1},
                "docker": {"container_count": 0},
                "tailscale": {"online": False},
            }
            ts = "1004.0002"
            await self.gateway.handle_message({"type": "message", "channel": "C123", "ts": ts, "text": "/health"}, self.client)
            self.assertEqual(self.client.active_gear_count(ts), 0)
            self.assertTrue(any(r["timestamp"] == ts and r["name"] == "warning" for r in self.client.reactions_added))

        # 3. Health query error
        with patch.object(self.gateway, "_execute_check_nose", new_callable=AsyncMock) as mock_nose:
            mock_nose.return_value = {"error": "Sentinel offline", "verdict_summary": "Offline"}
            ts = "1004.0003"
            await self.gateway.handle_message({"type": "message", "channel": "C123", "ts": ts, "text": "/health"}, self.client)
            self.assertEqual(self.client.active_gear_count(ts), 0)
            self.assertTrue(any(r["timestamp"] == ts and r["name"] == "warning" for r in self.client.reactions_added))

    async def test_sensory_eyes_command_transitions(self):
        """Test /status transitions: idle -> :white_check_mark:, blocker -> :warning:, error -> :warning:."""
        # 1. Idle status
        with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
            mock_eyes.return_value = {"state": "IDLE", "active_command": "", "blocker": None}
            ts = "1005.0001"
            await self.gateway.handle_message({"type": "message", "channel": "C123", "ts": ts, "text": "/status"}, self.client)
            self.assertEqual(self.client.active_gear_count(ts), 0)
            self.assertTrue(any(r["timestamp"] == ts and r["name"] == "white_check_mark" for r in self.client.reactions_added))

        # 2. Blocker status
        with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
            mock_eyes.return_value = {
                "state": "RUNNING",
                "active_command": "rclone config",
                "blocker": {"type": "oauth", "url": "http://127.0.0.1:53682/auth"},
            }
            ts = "1005.0002"
            await self.gateway.handle_message({"type": "message", "channel": "C123", "ts": ts, "text": "/status"}, self.client)
            self.assertEqual(self.client.active_gear_count(ts), 0)
            self.assertTrue(any(r["timestamp"] == ts and r["name"] == "warning" for r in self.client.reactions_added))

        # 3. Error status
        with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
            mock_eyes.return_value = {"state": "ERROR", "error": "Runner connection lost"}
            ts = "1005.0003"
            await self.gateway.handle_message({"type": "message", "channel": "C123", "ts": ts, "text": "/status"}, self.client)
            self.assertEqual(self.client.active_gear_count(ts), 0)
            self.assertTrue(any(r["timestamp"] == ts and r["name"] == "warning" for r in self.client.reactions_added))

    async def test_out_of_band_cancellation_reaction_transition(self):
        """When session is busy and cancellation message arrives, reaction transitions to :x:."""
        self.mock_session.is_busy = True
        self.mock_session.cancel_execution = AsyncMock()
        ts = "1006.0001"
        event = {"type": "message", "channel": "C123", "ts": ts, "text": "cancel that command"}
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(self.client.active_gear_count(ts), 0)
        self.assertTrue(any(r["timestamp"] == ts and r["name"] == "x" for r in self.client.reactions_added))

    async def test_unhandled_exception_transitions_reaction_to_boom(self):
        """Any unhandled exception in handle_message swaps :gear: to :boom: with zero orphaned :gear:."""
        ts = "1007.0001"
        event = {"type": "message", "channel": "C123", "ts": ts, "text": "/wrapup"}
        # Force exception during teardown
        self.router.teardown.side_effect = RuntimeError("Fatal router crash")
        with self.assertRaises(RuntimeError):
            await self.gateway.handle_message(event, self.client)
        self.assertEqual(self.client.active_gear_count(ts), 0)
        self.assertTrue(any(r["timestamp"] == ts and r["name"] == "boom" for r in self.client.reactions_added))

    # =========================================================================
    # Group 2: Direct Execution Fallback Under Simulated Failure Modes
    # =========================================================================

    async def test_fallback_direct_execution_with_syntax_error(self):
        """
        Simulated Failure Mode: Syntax Error during adapter stream.
        Direct execution fallback must capture status='ERROR', swap :gear: to :x:,
        and not raise an unhandled exception or leave :gear: orphaned.
        """
        ts = "1008.0001"
        self.mock_session.force_mouth = True

        async def _failing_stream(prompt: str) -> AsyncGenerator[Any, None]:
            yield StatusUpdateEvent(status="running")
            # Simulate syntax error during code execution in backstage
            raise SyntaxError("invalid syntax in backstage worker script")

        self.mock_session.send_user_message = _failing_stream

        # Force Gemini API to fail so gateway invokes _fallback_direct_adapter_turn
        with patch.object(self.gateway, "_call_gemini_api", new_callable=AsyncMock, return_value=None):
            event = {"type": "message", "channel": "C123", "ts": ts, "text": "run bad script"}
            await self.gateway.handle_message(event, self.client)

            self.assertEqual(self.client.active_gear_count(ts), 0)
            self.assertTrue(
                any(r["timestamp"] == ts and r["name"] == "x" for r in self.client.reactions_added),
                "Expected :x: reaction on syntax error during direct execution fallback",
            )
            self.assertFalse(
                any(r["timestamp"] == ts and r["name"] == "boom" for r in self.client.reactions_added),
                "Did not expect :boom: for handled adapter execution error",
            )

    async def test_fallback_direct_execution_with_empty_output(self):
        """
        Simulated Failure Mode: Empty output / zero deltas from adapter.
        Direct execution fallback completes cleanly, swaps :gear: to :white_check_mark:,
        and does not raise or orphan :gear:.
        """
        ts = "1009.0001"
        self.mock_session.force_mouth = True

        async def _empty_stream(prompt: str) -> AsyncGenerator[Any, None]:
            yield FinishEvent(status="SUCCESS", response="")

        self.mock_session.send_user_message = _empty_stream

        with patch.object(self.gateway, "_call_gemini_api", new_callable=AsyncMock, return_value=None):
            event = {"type": "message", "channel": "C123", "ts": ts, "text": "silent command"}
            await self.gateway.handle_message(event, self.client)

            self.assertEqual(self.client.active_gear_count(ts), 0)
            self.assertTrue(
                any(r["timestamp"] == ts and r["name"] == "white_check_mark" for r in self.client.reactions_added),
                "Expected :white_check_mark: reaction on empty successful fallback output",
            )

    async def test_fallback_direct_execution_with_yield_timeout(self):
        """
        Simulated Failure Mode: Long-running task exceeding 3.0s yield window.
        Direct execution fallback acknowledges yield, completes successfully,
        and swaps :gear: to :white_check_mark: with zero orphaned :gear:.
        """
        ts = "1010.0001"
        self.mock_session.force_mouth = True

        async def _slow_stream(prompt: str) -> AsyncGenerator[Any, None]:
            # Simulate task yielding after delay
            await asyncio.sleep(0.3)
            yield TextDeltaEvent(text_delta="Finished after long run")
            yield FinishEvent(status="SUCCESS", response="Finished after long run")

        self.mock_session.send_user_message = _slow_stream

        # Use yield_timeout=0.1 to trigger yield window quickly without slow wall-clock delay
        with patch.object(self.gateway, "_call_gemini_api", new_callable=AsyncMock, return_value=None):
            orig_exec = self.gateway._execute_backstage_task
            async def _fast_yield_exec(*args: Any, **kwargs: Any) -> Any:
                kwargs["yield_timeout"] = 0.1
                return await orig_exec(*args, **kwargs)

            with patch.object(self.gateway, "_execute_backstage_task", side_effect=_fast_yield_exec):
                event = {"type": "message", "channel": "C123", "ts": ts, "text": "run slow command"}
                await self.gateway.handle_message(event, self.client)

                self.assertEqual(self.client.active_gear_count(ts), 0)
                # Verify yield acknowledgment was posted
                self.assertTrue(
                    any("Dispatched backstage task" in msg["text"] for msg in self.client.posted_messages),
                    "Expected yield acknowledgment posted to Slack",
                )
                self.assertTrue(
                    any(r["timestamp"] == ts and r["name"] == "white_check_mark" for r in self.client.reactions_added),
                    "Expected final :white_check_mark: reaction",
                )

    async def test_fallback_direct_execution_synchronous_stream_exception(self):
        """
        Simulated Failure Mode: Synchronous exception in session.send_user_message.
        Exception bubbles up to handle_message, which cleanly swaps :gear: to :boom:.
        """
        ts = "1011.0001"
        self.mock_session.force_mouth = True

        def _bad_stream_init(prompt: str) -> Any:
            raise RuntimeError("Fatal PTY allocation error")

        self.mock_session.send_user_message = _bad_stream_init

        with patch.object(self.gateway, "_call_gemini_api", new_callable=AsyncMock, return_value=None):
            event = {"type": "message", "channel": "C123", "ts": ts, "text": "run command"}
            with self.assertRaises(RuntimeError):
                await self.gateway.handle_message(event, self.client)

            self.assertEqual(self.client.active_gear_count(ts), 0)
            self.assertTrue(
                any(r["timestamp"] == ts and r["name"] == "boom" for r in self.client.reactions_added),
                "Expected :boom: reaction on synchronous stream crash",
            )

    async def test_reaction_helpers_gracefully_survive_slack_api_network_failures(self):
        """Simulated Network Failure: Socket drops on reactions_add and reactions_remove do not crash gateway."""
        faulty_client = MockSlackClient(fail_reactions_add=True, fail_reactions_remove=True)
        ts = "1012.0001"
        event = {"type": "message", "channel": "C123", "ts": ts, "text": "/wrapup"}

        # Should execute cleanly without raising ConnectionResetError
        await self.gateway.handle_message(event, faulty_client)
        self.assertEqual(len(faulty_client.posted_messages), 1)

    # =========================================================================
    # Group 3: High-Concurrency & Stress Invariant Verification
    # =========================================================================

    async def test_high_concurrency_zero_orphaned_gear_invariant(self):
        """
        High-concurrency stress test: 50 concurrent messages across 10 threads
        with mixed slash commands, status checks, dropped empty events, and mouth turns.
        Assert that zero messages across the entire workload have orphaned :gear: reactions.
        """
        event_templates = [
            {"text": "/wrapup"},
            {"text": "/archive"},
            {"text": "/bearings"},
            {"text": "/ahoy"},
            {"text": "/learn note"},
            {"text": ""},  # dropped empty
            {"text": "<@U_GOLEM_BOT>"},  # dropped bot mention
            {"text": "Hello, how are you?", "mouth": True},
        ]

        tasks = []
        timestamps = []
        for i in range(50):
            ts = f"2000.{i:05d}"
            thread_ts = f"2000.{i % 10:05d}"
            tmpl = event_templates[i % len(event_templates)]
            timestamps.append(ts)

            evt = {
                "type": "message",
                "channel": "C_CONCURRENT",
                "ts": ts,
                "thread_ts": thread_ts,
                "text": tmpl["text"],
            }
            if tmpl.get("mouth"):
                # Mock Gemini response for mouth turns
                mock_resp = {
                    "candidates": [
                        {"content": {"parts": [{"text": "Conversational reply."}]}}
                    ]
                }
            else:
                mock_resp = None

            async def _run_event(e: Dict[str, Any], resp: Optional[Dict[str, Any]]) -> None:
                if resp:
                    with patch.object(self.gateway, "_call_gemini_api", new_callable=AsyncMock, return_value=resp):
                        await self.gateway.handle_message(e, self.client)
                else:
                    await self.gateway.handle_message(e, self.client)

            tasks.append(_run_event(evt, mock_resp))

        # Run all 50 concurrently
        await asyncio.gather(*tasks)

        # Invariant check: every timestamp must have exactly 0 active :gear: reactions
        for ts in timestamps:
            active_gears = self.client.active_gear_count(ts)
            self.assertEqual(
                active_gears,
                0,
                f"Orphaned :gear: detected for timestamp {ts} (net active: {active_gears})",
            )


if __name__ == "__main__":
    unittest.main()
