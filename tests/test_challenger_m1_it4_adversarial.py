#!/usr/bin/env python3
"""
tests/test_challenger_m1_it4_adversarial.py
Milestone 1 Iteration 4 Empirical Challenger Test Suite:
  1. Verification of Direct Execution Fallback:
     - Correct reaction mapping (:white_check_mark: on SUCCESS, :x: on ERROR).
     - Elimination of NameError ('target_reaction').
     - Verification that normal fallback completion never crashes to :boom:.
     - Handling of diverse accumulator shapes (BackstageExecutionAccumulator, Dict).
     - Stripping of file directives in fallback mode.
     - iMessage and None-timestamp boundary handling.
  2. Verification of Dropped Empty File Events:
     - Subtype 'file_share' with files: [] and empty/whitespace text cleanly removes :gear:.
     - Net active reaction count is exactly 0 (no orphaned reactions).
     - iMessage boundary handling (zero reaction removals attempted).
     - Concurrency storm test (50 concurrent empty file share events).
  3. End-to-end regression verification with Gemini API failure modes.
"""

import os
import sys
import time
import asyncio
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Optional, List, Dict, Any

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus
from golem.gateway.ingress import (
    IngressGateway,
    BackstageExecutionAccumulator,
    call_gemini_api,
)
from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle
from golem.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
)


class ChallengerMockSlackClient:
    """Mock Slack client tracking all message posts, reactions, and calls thread-safely."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []

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
        return {"ok": True, "messages": []}


class TestDirectFallbackExecutionAdversarial(unittest.IsolatedAsyncioTestCase):
    """Stress-test direct execution fallback under various accumulator shapes and error modes."""

    def setUp(self) -> None:
        self.router = MagicMock(spec=SessionRouter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM_BOT")
        self.client = ChallengerMockSlackClient()

        self.mock_session = MagicMock(spec=SessionHandle)
        self.mock_session.session_id = "test-session-fallback"
        self.mock_session.is_busy = False
        self.mock_session.is_executing = False
        self.mock_session.runner_pane = None
        self.mock_session.target_cwd = "/tmp/test"
        self.mock_session.force_mouth = True
        self.mock_session.is_mock = True

        self.router.get_or_create = AsyncMock(return_value=(self.mock_session, False))

    async def test_fallback_success_accumulator_reaction_white_check_mark(self) -> None:
        """Verify BackstageExecutionAccumulator with SUCCESS status transitions :gear: -> :white_check_mark:."""
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            acc = BackstageExecutionAccumulator()
            acc.record_event(TextDeltaEvent(delta="Building project... Done."))
            acc.record_event(FinishEvent(status="SUCCESS", response="Build succeeded"))
            mock_backstage.return_value = acc

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="run build",
                client=self.client,
                channel="C_TEST",
                thread_ts="100.1",
                ts="100.1",
                is_imessage=False,
            )

            # Assert message posted
            self.assertEqual(len(self.client.posted_messages), 1)
            self.assertIn("Build succeeded", self.client.posted_messages[0]["text"])

            # Assert reactions: gear removed, white_check_mark added, NO boom, NO x
            added = [r["name"] for r in self.client.reactions_added]
            removed = [r["name"] for r in self.client.reactions_removed]
            self.assertIn("white_check_mark", added)
            self.assertIn("gear", removed)
            self.assertNotIn("boom", added)
            self.assertNotIn("x", added)

    async def test_fallback_error_accumulator_reaction_x(self) -> None:
        """Verify BackstageExecutionAccumulator with ERROR status transitions :gear: -> :x:, NEVER :boom:."""
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            acc = BackstageExecutionAccumulator()
            acc.record_event(TextDeltaEvent(delta="Error: compile failure"))
            acc.record_event(FinishEvent(status="ERROR", response="Compilation failed with exit code 1"))
            mock_backstage.return_value = acc

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="run build",
                client=self.client,
                channel="C_TEST",
                thread_ts="100.2",
                ts="100.2",
                is_imessage=False,
            )

            self.assertEqual(len(self.client.posted_messages), 1)
            self.assertIn("Compilation failed", self.client.posted_messages[0]["text"])

            added = [r["name"] for r in self.client.reactions_added]
            removed = [r["name"] for r in self.client.reactions_removed]
            self.assertIn("x", added)
            self.assertIn("gear", removed)
            self.assertNotIn("boom", added)
            self.assertNotIn("white_check_mark", added)

    async def test_fallback_dict_accumulator_success_reaction(self) -> None:
        """Verify dict accumulator with status='SUCCESS' transitions :gear: -> :white_check_mark:."""
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            mock_backstage.return_value = {"status": "SUCCESS", "output": "Dict success output"}

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="dict task",
                client=self.client,
                channel="C_TEST",
                thread_ts="100.3",
                ts="100.3",
                is_imessage=False,
            )

            self.assertEqual(len(self.client.posted_messages), 1)
            self.assertIn("Dict success output", self.client.posted_messages[0]["text"])

            added = [r["name"] for r in self.client.reactions_added]
            self.assertIn("white_check_mark", added)
            self.assertNotIn("boom", added)

    async def test_fallback_dict_accumulator_error_reaction(self) -> None:
        """Verify dict accumulator with status='ERROR' transitions :gear: -> :x:."""
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            mock_backstage.return_value = {"status": "ERROR", "output": "Dict error output"}

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="dict task err",
                client=self.client,
                channel="C_TEST",
                thread_ts="100.4",
                ts="100.4",
                is_imessage=False,
            )

            self.assertEqual(len(self.client.posted_messages), 1)
            self.assertIn("Dict error output", self.client.posted_messages[0]["text"])

            added = [r["name"] for r in self.client.reactions_added]
            self.assertIn("x", added)
            self.assertNotIn("boom", added)

    async def test_fallback_default_status_when_status_missing(self) -> None:
        """Verify dict accumulator lacking 'status' defaults to SUCCESS -> :white_check_mark:."""
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            mock_backstage.return_value = {"output": "No status field present"}

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="unspecified status",
                client=self.client,
                channel="C_TEST",
                thread_ts="100.5",
                ts="100.5",
                is_imessage=False,
            )

            added = [r["name"] for r in self.client.reactions_added]
            self.assertIn("white_check_mark", added)
            self.assertNotIn("boom", added)

    async def test_fallback_strips_file_directives_cleanly(self) -> None:
        """Verify file transfer directives are stripped from posted message in fallback mode."""
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            mock_backstage.return_value = {
                "status": "SUCCESS",
                "output": "Here is the report:\n[SEND_FILE: /tmp/report.pdf]\nDone.",
            }

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="report request",
                client=self.client,
                channel="C_TEST",
                thread_ts="100.6",
                ts="100.6",
                is_imessage=False,
            )

            self.assertEqual(len(self.client.posted_messages), 1)
            posted_text = self.client.posted_messages[0]["text"]
            self.assertNotIn("[SEND_FILE:", posted_text)
            self.assertIn("Here is the report:", posted_text)

    async def test_fallback_imessage_no_reactions(self) -> None:
        """Verify that in iMessage mode, no reactions are added or removed."""
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            mock_backstage.return_value = {"status": "SUCCESS", "output": "iMessage reply"}

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="imessage task",
                client=self.client,
                channel="imessage_chat",
                thread_ts="100.7",
                ts="100.7",
                is_imessage=True,
            )

            self.assertEqual(len(self.client.posted_messages), 1)
            self.assertEqual(len(self.client.reactions_added), 0)
            self.assertEqual(len(self.client.reactions_removed), 0)

    async def test_fallback_none_ts_no_reactions(self) -> None:
        """Verify that when ts is None or empty, no reactions are manipulated."""
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            mock_backstage.return_value = {"status": "SUCCESS", "output": "No TS"}

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="no ts task",
                client=self.client,
                channel="C_TEST",
                thread_ts="",
                ts="",
                is_imessage=False,
            )

            self.assertEqual(len(self.client.posted_messages), 1)
            self.assertEqual(len(self.client.reactions_added), 0)

    async def test_fallback_empty_output_still_swaps_reaction(self) -> None:
        """Verify that when output is empty string, no message is posted but reaction still swaps."""
        with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
            mock_backstage.return_value = {"status": "SUCCESS", "output": ""}

            await self.gateway._fallback_direct_adapter_turn(
                session=self.mock_session,
                prompt="silent task",
                client=self.client,
                channel="C_TEST",
                thread_ts="100.8",
                ts="100.8",
                is_imessage=False,
            )

            self.assertEqual(len(self.client.posted_messages), 0)
            added = [r["name"] for r in self.client.reactions_added]
            self.assertIn("white_check_mark", added)

    async def test_e2e_mouth_gemini_500_triggers_fallback_and_white_check_mark(self) -> None:
        """End-to-end handle_message: Gemini Step 1 returns 500 error payload -> direct fallback -> :white_check_mark:."""
        event = {
            "type": "message",
            "channel": "C_TEST",
            "ts": "100.9",
            "text": "run e2e test",
        }
        with patch.object(
            self.gateway,
            "_call_gemini_api",
            new_callable=AsyncMock,
            return_value={"_error": True, "status": 500, "message": "Internal error"},
        ):
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
                acc = BackstageExecutionAccumulator()
                acc.record_event(FinishEvent(status="SUCCESS", response="Direct fallback succeeded"))
                mock_backstage.return_value = acc

                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                self.assertIn("Direct fallback succeeded", self.client.posted_messages[0]["text"])

                added = [r["name"] for r in self.client.reactions_added]
                self.assertIn("white_check_mark", added)
                self.assertNotIn("boom", added)

    async def test_e2e_mouth_direct_fallback_task_error_swaps_to_x_not_boom(self) -> None:
        """End-to-end handle_message: Gemini Step 1 fails, backstage returns ERROR -> :x:, NEVER :boom:."""
        event = {
            "type": "message",
            "channel": "C_TEST",
            "ts": "100.10",
            "text": "run failing task",
        }
        with patch.object(self.gateway, "_call_gemini_api", new_callable=AsyncMock, return_value=None):
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_backstage:
                acc = BackstageExecutionAccumulator()
                acc.record_event(FinishEvent(status="ERROR", response="Command exited with 127"))
                mock_backstage.return_value = acc

                await self.gateway.handle_message(event, self.client)

                self.assertEqual(len(self.client.posted_messages), 1)
                self.assertIn("Command exited with 127", self.client.posted_messages[0]["text"])

                added = [r["name"] for r in self.client.reactions_added]
                self.assertIn("x", added)
                self.assertNotIn("boom", added)


class TestDroppedEmptyFileEventsAdversarial(unittest.IsolatedAsyncioTestCase):
    """Stress-test dropped file events to verify absolute elimination of orphaned reactions."""

    def setUp(self) -> None:
        self.router = MagicMock(spec=SessionRouter)
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM_BOT")
        self.client = ChallengerMockSlackClient()

        self.mock_session = MagicMock(spec=SessionHandle)
        self.mock_session.session_id = "test-session-files"
        self.mock_session.is_busy = False
        self.mock_session.is_executing = False
        self.mock_session.runner_pane = None
        self.mock_session.target_cwd = "/tmp/test"
        self.mock_session.force_mouth = False
        self.mock_session.is_mock = True

        self.router.get_or_create = AsyncMock(return_value=(self.mock_session, False))

    async def test_empty_text_empty_files_file_share_removes_gear(self) -> None:
        """Verify subtype='file_share' with empty text and files: [] adds and immediately removes :gear:."""
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_FILES",
            "ts": "200.1",
            "text": "",
            "files": [],
        }

        await self.gateway.handle_message(event, self.client)

        gear_added = [r for r in self.client.reactions_added if r["name"] == "gear"]
        gear_removed = [r for r in self.client.reactions_removed if r["name"] == "gear"]

        self.assertEqual(len(gear_added), 1, "Expected :gear: to be added upon initial message receipt")
        self.assertEqual(len(gear_removed), 1, "Expected :gear: to be removed upon event drop")
        self.assertEqual(gear_added[0]["timestamp"], "200.1")
        self.assertEqual(gear_removed[0]["timestamp"], "200.1")
        self.assertEqual(len(self.client.posted_messages), 0, "No message should be posted")

    async def test_whitespace_text_empty_files_file_share_removes_gear(self) -> None:
        """Verify subtype='file_share' with whitespace-only text (' \t\n ') and files: [] removes :gear:."""
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_FILES",
            "ts": "200.2",
            "text": "   \n\t  \r  ",
            "files": [],
        }

        await self.gateway.handle_message(event, self.client)

        gear_added = [r for r in self.client.reactions_added if r["name"] == "gear"]
        gear_removed = [r for r in self.client.reactions_removed if r["name"] == "gear"]

        self.assertEqual(len(gear_added), 1)
        self.assertEqual(len(gear_removed), 1)
        self.assertEqual(len(self.client.posted_messages), 0)

    async def test_none_files_field_file_share_removes_gear(self) -> None:
        """Verify subtype='file_share' with files: None and empty text removes :gear:."""
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_FILES",
            "ts": "200.3",
            "text": "",
            "files": None,
        }

        await self.gateway.handle_message(event, self.client)

        gear_added = [r for r in self.client.reactions_added if r["name"] == "gear"]
        gear_removed = [r for r in self.client.reactions_removed if r["name"] == "gear"]

        self.assertEqual(len(gear_added), 1)
        self.assertEqual(len(gear_removed), 1)

    async def test_empty_files_imessage_no_reactions(self) -> None:
        """Verify in iMessage mode, empty file_share drops silently without attempting reaction remove."""
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "imessage_test",
            "ts": "200.4",
            "text": "",
            "files": [],
            "is_imessage": True,
        }

        await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(self.client.reactions_added), 0)
        self.assertEqual(len(self.client.reactions_removed), 0)
        self.assertEqual(len(self.client.posted_messages), 0)

    async def test_empty_files_none_ts_no_crash(self) -> None:
        """Verify empty file_share with missing ts drops cleanly without crashing."""
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_FILES",
            "ts": None,
            "text": "",
            "files": [],
        }

        await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(self.client.reactions_added), 0)
        self.assertEqual(len(self.client.reactions_removed), 0)

    async def test_file_share_with_text_and_no_files_not_dropped(self) -> None:
        """Verify subtype='file_share' with text ('hello') and files: [] is NOT dropped."""
        event = {
            "type": "message",
            "subtype": "file_share",
            "channel": "C_FILES",
            "ts": "200.5",
            "text": "Hello bot",
            "files": [],
        }
        self.mock_session.force_mouth = True

        with patch.object(self.gateway, "_handle_mouth_turn", new_callable=AsyncMock) as mock_mouth:
            await self.gateway.handle_message(event, self.client)
            self.assertEqual(mock_mouth.call_count, 1, "Should proceed to handle_mouth_turn")

    async def test_concurrent_burst_50_empty_file_share_events_zero_orphaned_gear(self) -> None:
        """Stress-test: 50 concurrent empty file share events processed simultaneously."""
        events = [
            {
                "type": "message",
                "subtype": "file_share",
                "channel": f"C_BURST_{i % 5}",
                "ts": f"300.{i:04d}",
                "text": "",
                "files": [],
            }
            for i in range(50)
        ]

        tasks = [self.gateway.handle_message(ev, self.client) for ev in events]
        await asyncio.gather(*tasks)

        self.assertEqual(len(self.client.reactions_added), 50, "All 50 events should have added :gear:")
        self.assertEqual(len(self.client.reactions_removed), 50, "All 50 events should have removed :gear:")
        self.assertEqual(len(self.client.posted_messages), 0, "Zero messages should be posted")

        # Verify that for every ts, added == removed
        added_ts = {r["timestamp"] for r in self.client.reactions_added if r["name"] == "gear"}
        removed_ts = {r["timestamp"] for r in self.client.reactions_removed if r["name"] == "gear"}
        self.assertEqual(added_ts, removed_ts, "All added :gear: timestamps must match removed timestamps")


if __name__ == "__main__":
    unittest.main()
