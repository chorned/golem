#!/usr/bin/env python3
"""
tests/test_gateway_m3.py
Comprehensive test suite for Milestone 3: Ingress Gateway & Thread Session State.
Validates:
  - SessionHandle state transitions, turn streaming, lock serialization, and timeout
  - SessionRouter warm retrieval, concurrency locking, teardown, and background reaper
  - Ingress non-verbal reaction protocol, deduplication, error suppression, and reaction swaps
  - TC-01: Conversational inquiry direct response (0 worker panes, 0 canned announcements, 0 footers)
  - TC-02: Multi-turn thread continuity without re-initialization
  - Explicit slash commands: /wrapup, /archive, /bearings, /ahoy, /learn
"""

import os
import sys
import time
import asyncio
import unittest
from typing import Optional, List, Dict, Any, AsyncGenerator

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
    AgentEvent,
)
from golem.gateway.session import SessionHandle, SessionState
from golem.gateway.router import SessionRouter
from golem.gateway.ingress import (
    IngressGateway,
    add_reaction,
    remove_reaction,
    swap_reaction,
    strip_bot_mentions,
    strip_flushed_chunks,
    process_slack_message,
)


class MockHeadlessAdapter:
    """Mock adapter simulating HeadlessAgentAdapter for high-speed deterministic testing."""

    def __init__(self, cwd: str = "/Users/hermes/projects/AGY", canned_events: Optional[List[AgentEvent]] = None) -> None:
        self.cwd = cwd
        self.started = False
        self.stopped = False
        self.turns_received: List[str] = []
        self.canned_events = canned_events
        self.custom_generator = None

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_received.append(prompt)
        if self.custom_generator:
            async for ev in self.custom_generator(prompt):
                yield ev
            return

        if self.canned_events is not None:
            for ev in self.canned_events:
                yield ev
            return

        # Default echo turn
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text_delta=f"*Response to:* {prompt}")
        yield FinishEvent(response=f"*Response to:* {prompt}", status="SUCCESS")


class MockSlackClient:
    """Mock client recording chat_postMessage, reactions_add, and reactions_remove calls."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.reaction_error_to_raise: Optional[str] = None
        self.thread_history: Dict[str, List[Dict[str, Any]]] = {}

    async def conversations_replies(self, channel: str, ts: str, limit: int = 15) -> Dict[str, Any]:
        return {"ok": True, "messages": self.thread_history.get(f"{channel}:{ts}", [])}

    async def reactions_add(self, channel: str, timestamp: str, name: str) -> None:
        if self.reaction_error_to_raise:
            exc = Exception(self.reaction_error_to_raise)
            exc.response = {"error": self.reaction_error_to_raise}
            raise exc
        self.reactions_added.append({"channel": channel, "timestamp": timestamp, "name": name})

    async def reactions_remove(self, channel: str, timestamp: str, name: str) -> None:
        if self.reaction_error_to_raise:
            exc = Exception(self.reaction_error_to_raise)
            exc.response = {"error": self.reaction_error_to_raise}
            raise exc
        self.reactions_removed.append({"channel": channel, "timestamp": timestamp, "name": name})

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None) -> Dict[str, Any]:
        self.posted_messages.append({"channel": channel, "text": text, "thread_ts": thread_ts})
        return {"ok": True, "ts": f"{time.time()}"}


class TestSessionHandle(unittest.IsolatedAsyncioTestCase):
    """Unit tests for SessionHandle state machine, timeouts, and turn dispatching."""

    async def test_session_initialization_and_touch(self) -> None:
        adapter = MockHeadlessAdapter()
        session = SessionHandle(
            session_id="C123:T456",
            channel="C123",
            thread_ts="T456",
            adapter=adapter,
            target_cwd="/test/path",
            timeout_seconds=14400.0,
        )
        self.assertEqual(session.session_id, "C123:T456")
        self.assertEqual(session.channel, "C123")
        self.assertEqual(session.thread_ts, "T456")
        self.assertEqual(session.state, SessionState.INITIALIZING)
        self.assertFalse(session.is_expired())

        # Verify touch and last_activity_at alias
        t0 = session.last_active_ts
        await asyncio.sleep(0.01)
        session.touch()
        self.assertGreater(session.last_active_ts, t0)
        self.assertEqual(session.last_activity_at, session.last_active_ts)

        session.last_activity_at = 1000.0
        self.assertEqual(session.last_active_ts, 1000.0)

    async def test_is_expired(self) -> None:
        adapter = MockHeadlessAdapter()
        session = SessionHandle(
            session_id="C1:T1",
            channel="C1",
            thread_ts="T1",
            adapter=adapter,
            timeout_seconds=0.05,
        )
        self.assertFalse(session.is_expired())
        await asyncio.sleep(0.06)
        self.assertTrue(session.is_expired())

    async def test_send_user_message_success_flow(self) -> None:
        events = [
            StatusUpdateEvent(status="working"),
            TextDeltaEvent(text_delta="Hello "),
            TextDeltaEvent(text_delta="Carl!"),
            FinishEvent(response="Hello Carl!", status="SUCCESS"),
        ]
        adapter = MockHeadlessAdapter(canned_events=events)
        session = SessionHandle(
            session_id="C1:T1",
            channel="C1",
            thread_ts="T1",
            adapter=adapter,
        )

        received_events = []
        async for ev in session.send_user_message("Greetings"):
            received_events.append(ev)

        self.assertEqual(len(received_events), 4)
        self.assertEqual(session.state, SessionState.IDLE)
        self.assertEqual(session.last_prompt, "Greetings")
        self.assertEqual(session.last_response, "Hello Carl!")
        self.assertEqual(session.current_turn, 1)

    async def test_send_user_message_awaiting_input(self) -> None:
        events = [
            StatusUpdateEvent(status="working"),
            AwaitingInputEvent(prompt="Proceed with deletion? [y/N]", choices=["y", "n"]),
        ]
        adapter = MockHeadlessAdapter(canned_events=events)
        session = SessionHandle("C1:T1", "C1", "T1", adapter)

        received = []
        async for ev in session.send_user_message("Delete files"):
            received.append(ev)

        self.assertEqual(len(received), 2)
        self.assertEqual(session.state, SessionState.AWAITING_INPUT)

    async def test_send_user_message_finish_error_status(self) -> None:
        events = [
            FinishEvent(response="", status="ERROR", error="Subprocess crash"),
        ]
        adapter = MockHeadlessAdapter(canned_events=events)
        session = SessionHandle("C1:T1", "C1", "T1", adapter)

        received = []
        async for ev in session.send_user_message("Run task"):
            received.append(ev)

        self.assertEqual(session.state, SessionState.FAILED)

    async def test_send_user_message_exception_handling(self) -> None:
        async def failing_generator(prompt: str):
            yield TextDeltaEvent(text_delta="Starting...")
            raise RuntimeError("Fatal connection failure")

        adapter = MockHeadlessAdapter()
        adapter.custom_generator = failing_generator
        session = SessionHandle("C1:T1", "C1", "T1", adapter)

        with self.assertRaises(RuntimeError):
            async for _ in session.send_user_message("Crash"):
                pass

        self.assertEqual(session.state, SessionState.FAILED)

    async def test_send_user_message_closed_raises_error(self) -> None:
        adapter = MockHeadlessAdapter()
        session = SessionHandle("C1:T1", "C1", "T1", adapter)
        await session.close()
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertTrue(adapter.stopped)

        with self.assertRaises(RuntimeError):
            async for _ in session.send_user_message("Hello"):
                pass

    async def test_concurrent_turns_serialized_by_lock(self) -> None:
        order = []

        async def slow_turn(prompt: str):
            order.append(f"start:{prompt}")
            await asyncio.sleep(0.05)
            order.append(f"end:{prompt}")
            yield FinishEvent(response=prompt, status="SUCCESS")

        adapter = MockHeadlessAdapter()
        adapter.custom_generator = slow_turn
        session = SessionHandle("C1:T1", "C1", "T1", adapter)

        async def run_turn(prompt: str):
            async for _ in session.send_user_message(prompt):
                pass

        await asyncio.gather(run_turn("turn1"), run_turn("turn2"))
        self.assertEqual(order, ["start:turn1", "end:turn1", "start:turn2", "end:turn2"])


class TestSessionRouter(unittest.IsolatedAsyncioTestCase):
    """Unit tests for SessionRouter registry, concurrency locking, and reaper watchdog."""

    def setUp(self) -> None:
        self.created_adapters: List[MockHeadlessAdapter] = []

        def factory(cwd: str) -> MockHeadlessAdapter:
            adapter = MockHeadlessAdapter(cwd=cwd)
            self.created_adapters.append(adapter)
            return adapter

        self.router = SessionRouter(
            default_target_cwd="/Users/hermes/projects/AGY",
            inactivity_timeout=14400.0,
            reaper_interval=0.05,
            adapter_factory=factory,
        )

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    async def test_build_session_key(self) -> None:
        key = SessionRouter.build_session_key("C999", "123456.789")
        self.assertEqual(key, "C999:123456.789")

    async def test_get_or_create_new_and_warm(self) -> None:
        # First call: creates new session
        session1, is_new1 = await self.router.get_or_create("C1", "100.1")
        self.assertTrue(is_new1)
        self.assertEqual(session1.session_id, "C1:100.1")
        self.assertEqual(session1.state, SessionState.IDLE)
        self.assertEqual(len(self.created_adapters), 1)
        self.assertTrue(self.created_adapters[0].started)

        # Second call: returns warm session
        session2, is_new2 = await self.router.get_or_create("C1", "100.1")
        self.assertFalse(is_new2)
        self.assertIs(session1, session2)
        self.assertEqual(len(self.created_adapters), 1)

    async def test_get_or_create_concurrency_lock(self) -> None:
        # Multiple concurrent requests for the exact same channel:thread_ts
        results = await asyncio.gather(
            self.router.get_or_create("C1", "200.1"),
            self.router.get_or_create("C1", "200.1"),
            self.router.get_or_create("C1", "200.1"),
        )
        sessions = [r[0] for r in results]
        is_new_flags = [r[1] for r in results]

        # Only one is_new should be True
        self.assertEqual(is_new_flags.count(True), 1)
        self.assertEqual(is_new_flags.count(False), 2)
        # All returned handles are identical
        self.assertIs(sessions[0], sessions[1])
        self.assertIs(sessions[1], sessions[2])
        # Only 1 adapter instantiated
        self.assertEqual(len(self.created_adapters), 1)

    async def test_teardown(self) -> None:
        session, is_new = await self.router.get_or_create("C1", "300.1")
        self.assertTrue(is_new)
        self.assertIn("C1:300.1", self.router.sessions)

        success = await self.router.teardown("C1", "300.1", reason="explicit_test")
        self.assertTrue(success)
        self.assertNotIn("C1:300.1", self.router.sessions)
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertTrue(self.created_adapters[0].stopped)

        # Teardown of nonexistent session returns False
        success_second = await self.router.teardown("C1", "300.1")
        self.assertFalse(success_second)

    async def test_reap_expired_sessions(self) -> None:
        session1, _ = await self.router.get_or_create("C1", "400.1")
        session2, _ = await self.router.get_or_create("C1", "400.2")

        # Set session1 inactive > 14400s
        session1.last_active_ts = time.time() - 15000.0

        pruned = await self.router.reap_expired_sessions()
        self.assertEqual(pruned, ["C1:400.1"])
        self.assertNotIn("C1:400.1", self.router.sessions)
        self.assertIn("C1:400.2", self.router.sessions)
        self.assertEqual(session1.state, SessionState.CLOSED)
        self.assertEqual(session2.state, SessionState.IDLE)

    async def test_reaper_background_loop_and_stop(self) -> None:
        self.router.inactivity_timeout = 0.04
        self.router.reaper_interval = 0.02
        self.router.start_reaper()

        session, _ = await self.router.get_or_create("C1", "500.1")
        self.assertIn("C1:500.1", self.router.sessions)

        # Wait for timeout and reaper sweep
        await asyncio.sleep(0.08)
        self.assertNotIn("C1:500.1", self.router.sessions)
        self.assertEqual(session.state, SessionState.CLOSED)

        await self.router.stop_reaper()
        self.assertFalse(self.router._is_running)


class TestIngressGateway(unittest.IsolatedAsyncioTestCase):
    """Unit tests for IngressGateway reaction protocol, deduplication, command interception, and delivery."""

    def setUp(self) -> None:
        self.created_adapters: List[MockHeadlessAdapter] = []

        def factory(cwd: str) -> MockHeadlessAdapter:
            adapter = MockHeadlessAdapter(cwd=cwd)
            self.created_adapters.append(adapter)
            return adapter

        self.router = SessionRouter(
            default_target_cwd="/Users/hermes/projects/AGY",
            adapter_factory=factory,
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = MockSlackClient()

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    async def test_non_verbal_ingress_ack_gear(self) -> None:
        event = {
            "channel": "C_TEST",
            "ts": "100.001",
            "text": "<@U_AGY_BOT> Hello First Mate!",
        }
        await self.gateway.handle_message(event, self.client)

        # Assert gear reaction added immediately
        gear_reactions = [r for r in self.client.reactions_added if r["name"] == "gear"]
        self.assertTrue(len(gear_reactions) >= 1)
        self.assertEqual(gear_reactions[0]["channel"], "C_TEST")
        self.assertEqual(gear_reactions[0]["timestamp"], "100.001")

        # Assert reaction swapped to white_check_mark upon success
        checkmark_reactions = [r for r in self.client.reactions_added if r["name"] == "white_check_mark"]
        self.assertTrue(len(checkmark_reactions) >= 1)

    async def test_reaction_error_suppression_already_reacted(self) -> None:
        self.client.reaction_error_to_raise = "already_reacted"
        event = {
            "channel": "C_TEST",
            "ts": "100.002",
            "text": "Ping",
        }
        # Should execute cleanly without raising SlackApiError/Exception
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(len(self.client.posted_messages), 1)

    async def test_deduplication_drops_duplicate_ts(self) -> None:
        event = {
            "channel": "C_TEST",
            "ts": "100.003",
            "text": "Execute once",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(len(self.client.posted_messages), 1)
        self.assertEqual(len(self.created_adapters[0].turns_received), 1)

        # Send duplicate event
        await self.gateway.handle_message(event, self.client)
        # No additional turns or messages
        self.assertEqual(len(self.client.posted_messages), 1)
        self.assertEqual(len(self.created_adapters[0].turns_received), 1)

    async def test_awaiting_input_swaps_gear_to_warning(self) -> None:
        events = [
            AwaitingInputEvent(prompt="Deploy to production? [y/N]"),
        ]
        adapter = MockHeadlessAdapter(canned_events=events)
        self.router.adapter_factory = lambda cwd: adapter

        event = {
            "channel": "C_TEST",
            "ts": "100.004",
            "text": "Deploy prod",
        }
        await self.gateway.handle_message(event, self.client)

        warning_reactions = [r for r in self.client.reactions_added if r["name"] == "warning"]
        self.assertTrue(len(warning_reactions) >= 1)
        self.assertIn("Deploy to production? [y/N]", self.client.posted_messages[-1]["text"])

    async def test_execution_error_swaps_gear_to_x(self) -> None:
        events = [
            FinishEvent(response="", status="ERROR", error="Disk full on host"),
        ]
        adapter = MockHeadlessAdapter(canned_events=events)
        self.router.adapter_factory = lambda cwd: adapter

        event = {
            "channel": "C_TEST",
            "ts": "100.005",
            "text": "Create large backup",
        }
        await self.gateway.handle_message(event, self.client)

        x_reactions = [r for r in self.client.reactions_added if r["name"] == "x"]
        self.assertTrue(len(x_reactions) >= 1)
        self.assertIn("Disk full on host", self.client.posted_messages[-1]["text"])

    async def test_imessage_mode_skips_reactions_and_thread_ts(self) -> None:
        event = {
            "channel": "+15551234567",
            "ts": "100.006",
            "text": "Check status",
            "is_imessage": True,
        }
        await self.gateway.handle_message(event, self.client)

        # No reactions added in iMessage mode
        self.assertEqual(len(self.client.reactions_added), 0)
        # Message posted with thread_ts = None
        self.assertEqual(len(self.client.posted_messages), 1)
        self.assertIsNone(self.client.posted_messages[0]["thread_ts"])

    async def test_strip_bot_mentions(self) -> None:
        raw = "<@U_AGY_BOT> @AGY Please run tests"
        cleaned = strip_bot_mentions(raw, "U_AGY_BOT")
        self.assertEqual(cleaned, "Please run tests")

        # Test Slack mentions with pipe labels
        raw_pipe = "<@U_AGY_BOT|firstmate> How's our fleet doing?"
        self.assertEqual(strip_bot_mentions(raw_pipe, "U_AGY_BOT"), "How's our fleet doing?")
        raw_labeled_cmd = "<@U_AGY_BOT|agy> /wrapup"
        self.assertEqual(strip_bot_mentions(raw_labeled_cmd, "U_AGY_BOT"), "/wrapup")
        raw_generic = "<@U12345|some_user> Status report"
        self.assertEqual(strip_bot_mentions(raw_generic), "Status report")

        # Test Slack mentions with empty pipe labels
        self.assertEqual(strip_bot_mentions("<@U_AGY_BOT|> status", "U_AGY_BOT"), "status")
        self.assertEqual(strip_bot_mentions("<@U_AGY_BOT|>", "U_AGY_BOT"), "")
        self.assertEqual(strip_bot_mentions("<@U_OTHER|> status", "U_AGY_BOT"), "status")

    async def test_empty_mention_only_event_silently_dropped(self) -> None:
        """Verify handle_message silently drops an event containing only '<@U_AGY_BOT|>' with zero replies and zero sessions created."""
        event = {
            "channel": "C_TEST",
            "ts": "100.007",
            "text": "<@U_AGY_BOT|>",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(len(self.client.posted_messages), 0)
        self.assertEqual(len(self.client.reactions_added), 0)
        self.assertEqual(len(self.router.sessions), 0)

    async def test_eager_flush_interim_text_before_tool_execution(self) -> None:
        """
        Verify that an initial conversational acknowledgment produced before a tool call
        is eagerly flushed to Slack as an independent message, rather than buffered until
        the end of the turn alongside the investigation results.
        """
        events = [
            TextDeltaEvent(text_delta="On it—let me pull the latest schedules across the system."),
            ToolStartEvent(tool_name="run_command", parameters={"CommandLine": "crontab -l"}),
            ToolEndEvent(tool_name="run_command", output="0 6 * * * rss_movie_fetcher.py"),
            TextDeltaEvent(text_delta="Here is the high-level map of our active scheduled jobs:\n• rss-feed-forwarder (Daily @ 06:00 AM)"),
            FinishEvent(
                response="On it—let me pull the latest schedules across the system.\n\nHere is the high-level map of our active scheduled jobs:\n• rss-feed-forwarder (Daily @ 06:00 AM)",
                status="SUCCESS",
            ),
        ]
        adapter = MockHeadlessAdapter(canned_events=events)
        self.router.adapter_factory = lambda cwd: adapter

        event = {
            "channel": "C_CRON",
            "ts": "100.008",
            "thread_ts": "100.008",
            "text": "Let’s chat about the different cronjobs we have set up right now",
        }
        await self.gateway.handle_message(event, self.client)

        # Must have posted 2 separate messages in Slack:
        # Message 1: The quick acknowledgment before the tool call
        # Message 2: The investigation results after the tool call
        self.assertEqual(len(self.client.posted_messages), 2)
        msg1 = self.client.posted_messages[0]
        msg2 = self.client.posted_messages[1]

        self.assertEqual(msg1["text"], "On it—let me pull the latest schedules across the system.")
        self.assertEqual(msg1["thread_ts"], "100.008")

        self.assertIn("Here is the high-level map of our active scheduled jobs", msg2["text"])
        self.assertNotIn("On it—let me pull", msg2["text"])
        self.assertEqual(msg2["thread_ts"], "100.008")

        # Reaction life cycle: gear added on ingress, swapped to checkmark at finish
        gear_reactions = [r for r in self.client.reactions_added if r["name"] == "gear"]
        checkmark_reactions = [r for r in self.client.reactions_added if r["name"] == "white_check_mark"]
        self.assertTrue(len(gear_reactions) >= 1)
        self.assertTrue(len(checkmark_reactions) >= 1)

    async def test_eager_flush_multiple_tools(self) -> None:
        """Verify multiple tools with interim commentary flush each chunk in sequence."""
        events = [
            TextDeltaEvent(text_delta="Checking cron jobs..."),
            ToolStartEvent(tool_name="bash", parameters={"cmd": "crontab -l"}),
            ToolEndEvent(tool_name="bash", output="none"),
            TextDeltaEvent(text_delta="Checking launchd plists..."),
            ToolStartEvent(tool_name="bash", parameters={"cmd": "launchctl list"}),
            ToolEndEvent(tool_name="bash", output="com.user.backup"),
            TextDeltaEvent(text_delta="Found 1 launchd job: com.user.backup"),
            FinishEvent(
                response="Checking cron jobs...\nChecking launchd plists...\nFound 1 launchd job: com.user.backup",
                status="SUCCESS",
            ),
        ]
        adapter = MockHeadlessAdapter(canned_events=events)
        self.router.adapter_factory = lambda cwd: adapter

        event = {
            "channel": "C_MULTI",
            "ts": "100.009",
            "text": "Check all schedulers",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(self.client.posted_messages), 3)
        self.assertEqual(self.client.posted_messages[0]["text"], "Checking cron jobs...")
        self.assertEqual(self.client.posted_messages[1]["text"], "Checking launchd plists...")
        self.assertEqual(self.client.posted_messages[2]["text"], "Found 1 launchd job: com.user.backup")

    async def test_eager_flush_multiple_tools_interleaved(self) -> None:
        """Verify multiple tools with interleaved commentary and results do not duplicate text."""
        events = [
            TextDeltaEvent(text_delta="Starting schedule investigation..."),
            ToolStartEvent(tool_name="bash", parameters={"cmd": "crontab -l"}),
            ToolEndEvent(tool_name="bash", output="0 6 * * * rss_movie_fetcher.py"),
            TextDeltaEvent(text_delta="Crontab examined. Next, checking launchd services..."),
            ToolStartEvent(tool_name="bash", parameters={"cmd": "launchctl list"}),
            ToolEndEvent(tool_name="bash", output="com.user.macbackup"),
            TextDeltaEvent(text_delta="Launchd examined. Here is the unified schedule breakdown:\n1. rss-feed-forwarder\n2. com.user.macbackup"),
            FinishEvent(
                response=(
                    "Starting schedule investigation...\n\n"
                    "Crontab examined. Next, checking launchd services...\n\n"
                    "Launchd examined. Here is the unified schedule breakdown:\n"
                    "1. rss-feed-forwarder\n2. com.user.macbackup"
                ),
                status="SUCCESS",
            ),
        ]
        adapter = MockHeadlessAdapter(canned_events=events)
        self.router.adapter_factory = lambda cwd: adapter

        event = {
            "channel": "C_INTERLEAVED",
            "ts": "100.010",
            "text": "Audit all schedules",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(self.client.posted_messages), 3)
        self.assertEqual(self.client.posted_messages[0]["text"], "Starting schedule investigation...")
        self.assertEqual(self.client.posted_messages[1]["text"], "Crontab examined. Next, checking launchd services...")
        self.assertIn("Launchd examined. Here is the unified schedule breakdown:", self.client.posted_messages[2]["text"])
        self.assertNotIn("Starting schedule investigation...", self.client.posted_messages[2]["text"])
        self.assertNotIn("Crontab examined. Next, checking launchd services...", self.client.posted_messages[2]["text"])

    def test_strip_flushed_chunks_utility(self) -> None:
        """Unit test strip_flushed_chunks for all edge cases."""
        self.assertEqual(strip_flushed_chunks("", ["abc"]), "")
        self.assertEqual(strip_flushed_chunks("Hello world", []), "Hello world")
        self.assertEqual(
            strip_flushed_chunks("Part 1\n\nPart 2", ["Part 1"]),
            "Part 2"
        )
        self.assertEqual(
            strip_flushed_chunks("Part 1\nPart 2\nPart 3", ["Part 1", "Part 2"]),
            "Part 3"
        )
        # Already fully flushed
        self.assertEqual(
            strip_flushed_chunks("Part 1", ["Part 1"]),
            ""
        )
        # Response only contains final part
        self.assertEqual(
            strip_flushed_chunks("Part 2 only", ["Part 1"]),
            "Part 2 only"
        )
        # Interleaved multi-tool chunks
        self.assertEqual(
            strip_flushed_chunks("Step 1\nResult 1\nStep 2\nResult 2", ["Step 1", "Step 2"]),
            "Result 1\nResult 2"
        )
        # Markdown wrapped chunks
        self.assertEqual(
            strip_flushed_chunks("*Part 1*\n\nPart 2", ["Part 1"]),
            "Part 2"
        )
        self.assertEqual(
            strip_flushed_chunks("> Part 1\n\nPart 2", ["Part 1"]),
            "Part 2"
        )
        self.assertEqual(
            strip_flushed_chunks("_Part 1_\n\nPart 2", ["Part 1"]),
            "Part 2"
        )



class TestAcceptanceCriteriaTC01AndTC02(unittest.IsolatedAsyncioTestCase):
    """
    Verification of Acceptance Criteria:
      TC-01: Conversational inquiry direct response with 0 worker panes, 0 footers, 0 canned announcements.
      TC-02: Multi-turn thread continuity without re-initialization.
    """

    def setUp(self) -> None:
        self.created_adapters: List[MockHeadlessAdapter] = []

        def factory(cwd: str) -> MockHeadlessAdapter:
            adapter = MockHeadlessAdapter(cwd=cwd)
            self.created_adapters.append(adapter)
            return adapter

        self.router = SessionRouter(
            default_target_cwd="/Users/hermes/projects/AGY",
            adapter_factory=factory,
        )
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = MockSlackClient()

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    async def test_tc01_conversational_inquiry_direct_response(self) -> None:
        """
        TC-01: "How's our fleet doing?" replies directly in 1 turn using Slack *bold* headers,
        0 worker subagents/panes, 0 'Task Completed' footers, and 0 canned announcements.
        """
        fleet_response = (
            "🧭 *Fleet Status Overview:*\n"
            "• *hermes* (localhost): 🟢 Healthy\n"
            "• *desktop-pc* (Windows CUDA): 🟢 Idle\n"
            "• *oci-machine* (Production VPS): 🟢 Online"
        )
        adapter = MockHeadlessAdapter(canned_events=[
            TextDeltaEvent(text_delta=fleet_response),
            FinishEvent(response=fleet_response, status="SUCCESS"),
        ])
        self.router.adapter_factory = lambda cwd: adapter

        event = {
            "channel": "C_OPS",
            "ts": "200.001",
            "text": "How's our fleet doing?",
        }
        await self.gateway.handle_message(event, self.client)

        # 1. Exactly 1 response delivered
        self.assertEqual(len(self.client.posted_messages), 1)
        delivered_text = self.client.posted_messages[0]["text"]

        # 2. Contains Slack *bold* headers
        self.assertIn("*Fleet Status Overview:*", delivered_text)

        # 3. ZERO canned announcements ("I will dispatch OpenCode...", "On it...", etc.)
        self.assertNotIn("I will dispatch", delivered_text)
        self.assertNotIn("Starting worker", delivered_text)

        # 4. ZERO "Task Completed" or "Task Completed via Herdr" footers
        self.assertNotIn("Task Completed", delivered_text)
        self.assertNotIn("via Herdr", delivered_text)

        # 5. Non-verbal reaction protocol: gear -> white_check_mark
        self.assertEqual(self.client.reactions_added[0]["name"], "gear")
        self.assertEqual(self.client.reactions_added[1]["name"], "white_check_mark")

    async def test_tc02_multi_turn_continuity(self) -> None:
        """
        TC-02: Thread context persists across turns (Turn 1 followed by Turn 2 in same thread)
        without re-inspecting or echoing stale conversational fragments.
        """
        # Turn 1
        event1 = {
            "channel": "C_DEV",
            "ts": "300.001",
            "thread_ts": "300.001",
            "text": "Show cron overview",
        }
        await self.gateway.handle_message(event1, self.client)

        self.assertEqual(len(self.created_adapters), 1)
        adapter_instance = self.created_adapters[0]
        self.assertEqual(len(adapter_instance.turns_received), 1)
        self.assertEqual(adapter_instance.turns_received[0], "Show cron overview")

        # Turn 2 in the same thread
        event2 = {
            "channel": "C_DEV",
            "ts": "300.002",
            "thread_ts": "300.001",
            "text": "Which one runs at 3 AM?",
        }
        await self.gateway.handle_message(event2, self.client)

        # Verifies the EXACT same warm adapter instance was reused (no second adapter created)
        self.assertEqual(len(self.created_adapters), 1)
        self.assertEqual(len(adapter_instance.turns_received), 2)
        self.assertEqual(adapter_instance.turns_received[1], "Which one runs at 3 AM?")

        # Turn 3 in a DIFFERENT thread
        event3 = {
            "channel": "C_DEV",
            "ts": "400.001",
            "thread_ts": "400.001",
            "text": "New separate topic",
        }
        await self.gateway.handle_message(event3, self.client)
        # Separate thread creates a second adapter
        self.assertEqual(len(self.created_adapters), 2)


class TestExplicitCommands(unittest.IsolatedAsyncioTestCase):
    """Unit tests for /wrapup, /archive, /bearings, /ahoy, and /learn commands."""

    def setUp(self) -> None:
        self.router = SessionRouter(adapter_factory=lambda cwd: MockHeadlessAdapter(cwd=cwd))
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_AGY_BOT")
        self.client = MockSlackClient()

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    async def test_wrapup_command(self) -> None:
        # Pre-seed session
        session, _ = await self.router.get_or_create("C1", "100.1")
        self.assertIn("C1:100.1", self.router.sessions)

        event = {
            "channel": "C1",
            "ts": "100.2",
            "thread_ts": "100.1",
            "text": "/wrapup",
        }
        await self.gateway.handle_message(event, self.client)

        # Session torn down
        self.assertNotIn("C1:100.1", self.router.sessions)
        self.assertEqual(session.state, SessionState.CLOSED)
        # Housekeeping message delivered
        self.assertIn("🧹 *Wrapup:* Housekeeping complete.", self.client.posted_messages[-1]["text"])

    async def test_archive_command(self) -> None:
        session, _ = await self.router.get_or_create("C1", "200.1")
        event = {
            "channel": "C1",
            "ts": "200.2",
            "thread_ts": "200.1",
            "text": "!archive",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertNotIn("C1:200.1", self.router.sessions)
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertIn("📦 *Archive:* Session archived and torn down.", self.client.posted_messages[-1]["text"])

    async def test_bearings_command(self) -> None:
        event = {
            "channel": "C1",
            "ts": "300.1",
            "text": "/bearings",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertIn("🧭 *Fleet Bearings & Status Digest:*", self.client.posted_messages[-1]["text"])

    async def test_ahoy_command(self) -> None:
        event = {
            "channel": "C1",
            "ts": "400.1",
            "text": "/ahoy",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertIn("⚓ *Ahoy:* Session catch-up complete.", self.client.posted_messages[-1]["text"])

    async def test_learn_command(self) -> None:
        event = {
            "channel": "C1",
            "ts": "500.1",
            "text": "/learn Always pin poetry dependencies strictly",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertIn("🧠 *Operational Lesson Crystallized:*", self.client.posted_messages[-1]["text"])
        self.assertIn("Always pin poetry dependencies strictly", self.client.posted_messages[-1]["text"])

    async def test_wrapup_nonexistent_session(self) -> None:
        event = {
            "channel": "C1",
            "ts": "600.1",
            "text": "/wrapup",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertIn("🧹 *Wrapup:* Housekeeping complete.", self.client.posted_messages[-1]["text"])

    async def test_swap_reaction_ignorable_remove_error(self) -> None:
        client = MockSlackClient()
        # Mock reactions_remove raising no_reaction
        async def mock_remove(channel, timestamp, name):
            exc = Exception("no_reaction")
            exc.response = {"error": "no_reaction"}
            raise exc
        client.reactions_remove = mock_remove

        await swap_reaction(client, "C1", "100.1", "gear", "white_check_mark")
        # Should have added white_check_mark despite remove error
        self.assertEqual(len(client.reactions_added), 1)
        self.assertEqual(client.reactions_added[0]["name"], "white_check_mark")

    async def test_status_query_not_intercepted_by_keyword_regex(self) -> None:
        """Verify queries like 'What is our fleet status?' route to session without legacy regex blocking."""
        adapter = MockHeadlessAdapter(canned_events=[
            FinishEvent(response="Status: all systems operational.", status="SUCCESS")
        ])
        self.router.adapter_factory = lambda cwd: adapter

        event = {
            "channel": "C1",
            "ts": "700.1",
            "text": "What is our fleet status?",
        }
        await self.gateway.handle_message(event, self.client)
        self.assertEqual(len(adapter.turns_received), 1)
        self.assertEqual(adapter.turns_received[0], "What is our fleet status?")
        self.assertIn("Status: all systems operational.", self.client.posted_messages[-1]["text"])

    async def test_long_message_chunking_boundary_aware(self) -> None:
        """Verify long message (>3800 chars) is delivered in multiple boundary-aware chunks."""
        # 3 paragraphs of 2000 chars each = 6000 chars
        paragraph1 = "Paragraph 1: " + ("A" * 1980)
        paragraph2 = "Paragraph 2: " + ("B" * 1980)
        long_response = f"{paragraph1}\n\n{paragraph2}"

        adapter = MockHeadlessAdapter(canned_events=[
            FinishEvent(response=long_response, status="SUCCESS")
        ])
        self.router.adapter_factory = lambda cwd: adapter

        event = {
            "channel": "C1",
            "ts": "800.1",
            "text": "Generate long report",
        }
        await self.gateway.handle_message(event, self.client)
        # Should have split into at least 2 messages
        self.assertGreaterEqual(len(self.client.posted_messages), 2)
        for msg in self.client.posted_messages:
            self.assertLessEqual(len(msg["text"]), 3800)

    async def test_process_slack_message_convenience_function(self) -> None:
        event = {
            "channel": "C_FN",
            "ts": "900.1",
            "text": "Ping via function",
        }
        await process_slack_message(event, self.client, router=self.router)
        self.assertGreaterEqual(len(self.client.posted_messages), 1)

    async def test_thread_context_rehydration_on_resumed_session(self) -> None:
        """Verify that when resuming an existing thread, prior Slack messages are rehydrated into prompt context."""
        adapter = MockHeadlessAdapter()
        self.router.adapter_factory = lambda cwd: adapter

        self.client.thread_history["C_RESUME:100.1"] = [
            {"ts": "100.1", "user": "U_CARL", "text": "on OCI we have a skill for audiobooks"},
            {"ts": "100.2", "bot_id": "B_AGY", "text": "I'm checking the /Volumes/audiobooks_inbox mount."},
        ]

        event = {
            "channel": "C_RESUME",
            "thread_ts": "100.1",
            "ts": "100.3",
            "text": "where did we end up with this?",
        }
        await self.gateway.handle_message(event, self.client)

        self.assertEqual(len(adapter.turns_received), 1)
        received_prompt = adapter.turns_received[0]
        self.assertIn("Previous Slack Thread Context", received_prompt)
        self.assertIn("audiobooks", received_prompt)
        self.assertIn("where did we end up with this?", received_prompt)

    async def test_thread_context_rehydrated_on_warm_second_turn(self) -> None:
        """Verify that second turn in an active thread rehydrates Slack thread history."""
        adapter = MockHeadlessAdapter()
        self.router.adapter_factory = lambda cwd: adapter

        self.client.thread_history["C_WARM:200.1"] = [
            {"ts": "200.1", "user": "U_CARL", "text": "Initial thread question"},
        ]

        # First turn (resumed)
        event1 = {
            "channel": "C_WARM",
            "thread_ts": "200.1",
            "ts": "200.2",
            "text": "Turn 1",
        }
        await self.gateway.handle_message(event1, self.client)
        self.assertIn("Previous Slack Thread Context", adapter.turns_received[0])

        # Second turn (warm)
        event2 = {
            "channel": "C_WARM",
            "thread_ts": "200.1",
            "ts": "200.3",
            "text": "Turn 2 follow-up",
        }
        await self.gateway.handle_message(event2, self.client)
        self.assertEqual(len(adapter.turns_received), 2)
        # Turn 2 must rehydrate thread context for continuous conversational history
        self.assertIn("Previous Slack Thread Context", adapter.turns_received[1])
        self.assertIn("Initial thread question", adapter.turns_received[1])
        self.assertIn("Turn 2 follow-up", adapter.turns_received[1])


if __name__ == "__main__":
    unittest.main()
