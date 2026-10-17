#!/usr/bin/env python3
"""
tests/e2e/test_tier2_boundaries.py
E2E Tier 2 Boundary & Corner Case Test Suite for First Mate 2.0.

Covers Boundary Categories from TEST_INFRA.md and PROJECT.md:
1. Gateway Ingress & Session Boundaries:
   - Empty user messages, whitespace-only messages, standalone bot mentions (<@U_BOT>),
     empty-label bot mentions (<@U_BOT|>), multiple bot mentions.
   - Massive user inputs (100,000+ characters), special unicode/emoji characters, binary control characters.
   - Rapid sequential and concurrent message dispatches to the same thread session.
   - Inactivity reaper timeout boundaries: exact ttl expiration, session resurrection prevention on closed sessions.

2. Headless Agent Adapter Boundaries:
   - Malformed NDJSON streaming lines: non-dict JSON ([], "hello", 123, null, true),
     raw invalid JSON, empty lines.
   - Event payloads with null subfields (step_update=None, tool_info=None, parameters=None, result=None, error=None).
   - Massive stderr burst streaming (128KB+ stderr burst drained in <1s without deadlocking child process).
   - Rapid generator termination (aclose()) while subprocess is actively writing to stdout/stderr.

3. Execution Harness & Runner Boundaries:
   - Commands right at yield_timeout boundary.
   - Interactive blocker detection boundaries: malformed loopback URLs, URLs wrapped in brackets
     [http://127.0.0.1:8080/auth], URLs with trailing punctuation, ANSI-wrapped URLs, sudo/git credential prompts.
   - Teardown idempotency: calling teardown() on already closed or nonexistent workspaces.

4. Transport & Wire Normalization Boundaries:
   - Message boundary chunking right at 3,800 chars; unbroken words exceeding 3,800 chars;
     unclosed code fences exceeding 3,800 chars.
   - Formatting corner cases: triple asterisks (***text***), nested markdown quotes, mixed CRLF/LF line endings.

Opaque-box, requirement-driven test design derived from ORIGINAL_REQUEST.md, PROJECT.md, and TEST_INFRA.md.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import sys
import json
import time
import shutil
import asyncio
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

# ---------------------------------------------------------------------------
# Imports from golem modules
# ---------------------------------------------------------------------------
from golem.gateway.ingress import (
    IngressGateway,
    strip_bot_mentions,
    add_reaction,
    remove_reaction,
    swap_reaction,
)
from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle, SessionState

from golem.agent.adapter import HeadlessAgentAdapter
from golem.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
)

from golem.execution.runner import RunnerExecutor, ExecutionHandle
from golem.execution.buffer import RunnerBufferObserver
from golem.execution.harness import DualPaneHarness

from golem.transport.normalizer import (
    sanitize_for_slack,
    transpile_markdown_to_mrkdwn,
    strip_ansi,
    strip_tui_debris,
    strip_box_drawing,
    strip_prompt_echoes,
)
from golem.transport.chunker import split_message_boundary_aware


# ===========================================================================
# 1. Gateway Ingress & Session Boundaries
# ===========================================================================
class TestTier2GatewayIngressAndSessionBoundaries(unittest.IsolatedAsyncioTestCase):
    """
    Tier 2 boundary tests for Gateway Ingress & Session Router.
    Validates empty/whitespace inputs, standalone/empty-label bot mentions,
    massive user payloads (100k+ chars), unicode/control bytes, concurrent turn serialization,
    and exact TTL inactivity reaper pruning with resurrection prevention.
    """

    def setUp(self):
        self.router = SessionRouter(
            inactivity_timeout=10.0,
            adapter_factory=lambda cwd: MagicMock()
        )
        self.client = MagicMock()
        self.client.reactions_add = AsyncMock()
        self.client.reactions_remove = AsyncMock()
        self.client.chat_postMessage = AsyncMock()

    async def asyncTearDown(self):
        await self.router.stop_reaper()

    def test_strip_bot_mentions_combinations(self):
        """Gateway: Validates bot mention stripping across empty label, multiple mentions, and prefixes."""
        # Empty string and whitespace-only
        self.assertEqual(strip_bot_mentions(""), "")
        self.assertEqual(strip_bot_mentions("   \n\t  "), "")

        # Standalone bot mention with and without bot_user_id
        self.assertEqual(strip_bot_mentions("<@U_BOT>", "U_BOT"), "")
        self.assertEqual(strip_bot_mentions("<@U12345678>"), "")

        # Empty-label bot mention (<@U_BOT|>)
        self.assertEqual(strip_bot_mentions("<@U_BOT|>", "U_BOT"), "")
        self.assertEqual(strip_bot_mentions("<@U999|>"), "")

        # Multiple bot mentions mixed together
        multi = "<@U_BOT> <@U_COWORKER|bot> <@U_THIRD>   "
        self.assertEqual(strip_bot_mentions(multi, "U_BOT"), "")

        # Mentions embedded with actual prompt text
        prompt = "<@U_BOT> How is the fleet doing? <@U_OTHER|label>"
        self.assertEqual(strip_bot_mentions(prompt, "U_BOT"), "How is the fleet doing?")

        # Conversational @AGY / @FirstMate prefixes
        self.assertEqual(strip_bot_mentions("@AGY run status check"), "run status check")
        self.assertEqual(strip_bot_mentions("@FirstMate check logs"), "check logs")

    async def test_empty_and_whitespace_only_messages_ignored(self):
        """Gateway: Empty and whitespace-only messages produce zero reactions, sessions, or egress posts."""
        gw = IngressGateway(router=self.router, bot_user_id="U_BOT")

        empty_events = [
            {"text": "", "channel": "C_TEST", "ts": "100.1"},
            {"text": "   ", "channel": "C_TEST", "ts": "100.2"},
            {"text": "\t\n\r  \n", "channel": "C_TEST", "ts": "100.3"},
        ]

        for ev in empty_events:
            await gw.handle_message(ev, self.client)

        self.assertEqual(self.client.reactions_add.await_count, 0)
        self.assertEqual(self.client.chat_postMessage.await_count, 0)
        self.assertEqual(len(self.router.sessions), 0)

    async def test_standalone_and_empty_label_bot_mentions_ignored(self):
        """Gateway: Standalone bot mentions (<@U_BOT>, <@U_BOT|>) produce zero reactions or sessions."""
        gw = IngressGateway(router=self.router, bot_user_id="U_BOT")

        mention_events = [
            {"text": "<@U_BOT>", "channel": "C_TEST", "ts": "200.1"},
            {"text": "  <@U_BOT>  ", "channel": "C_TEST", "ts": "200.2"},
            {"text": "<@U_BOT|>", "channel": "C_TEST", "ts": "200.3"},
            {"text": "<@U_BOT> <@U_OTHER|bot>", "channel": "C_TEST", "ts": "200.4"},
            {"text": "@AGY   ", "channel": "C_TEST", "ts": "200.5"},
        ]

        for ev in mention_events:
            await gw.handle_message(ev, self.client)

        self.assertEqual(self.client.reactions_add.await_count, 0)
        self.assertEqual(self.client.chat_postMessage.await_count, 0)
        self.assertEqual(len(self.router.sessions), 0)

    async def test_massive_user_input_100k_chars(self):
        """Gateway: Massive input (100,000+ characters) processes without ReDoS or crash and reaches session."""
        mock_adapter = MagicMock()
        captured_prompts = []

        async def mock_stream_turn(prompt):
            captured_prompts.append(prompt)
            yield TextDeltaEvent(text_delta=f"Received massive payload of length {len(prompt)}")
            yield FinishEvent(response=f"Received massive payload of length {len(prompt)}", status="SUCCESS")

        mock_adapter.send_turn = mock_stream_turn
        mock_adapter.start = AsyncMock()

        custom_router = SessionRouter(adapter_factory=lambda cwd: mock_adapter)
        gw = IngressGateway(router=custom_router, bot_user_id="U_BOT")

        # 100,000 characters payload prefixed with bot mention
        large_body = "DATA_" * 20000  # exactly 100,000 characters
        event = {
            "text": f"<@U_BOT> {large_body}",
            "channel": "C_MASSIVE",
            "ts": "300.1"
        }

        t0 = time.time()
        await gw.handle_message(event, self.client)
        elapsed = time.time() - t0

        # Processing must be fast (< 1.0s) confirming no ReDoS vulnerability
        self.assertLess(elapsed, 1.0, f"Massive payload processing took too long: {elapsed}s")
        self.assertEqual(len(captured_prompts), 1)
        self.assertEqual(len(captured_prompts[0]), 100000)

        # Non-verbal reaction ack (:gear:) was added, then swapped to :white_check_mark:
        self.client.reactions_add.assert_any_await(channel="C_MASSIVE", timestamp="300.1", name="gear")
        self.client.reactions_add.assert_any_await(channel="C_MASSIVE", timestamp="300.1", name="white_check_mark")
        # Response was posted back to thread
        self.assertGreaterEqual(self.client.chat_postMessage.await_count, 1)
        self.assertIn("100000", self.client.chat_postMessage.call_args[1]["text"])

    async def test_special_unicode_and_binary_control_characters(self):
        """Gateway: Emoji, RTL text, unicode mathematical symbols, and binary control characters."""
        mock_adapter = MagicMock()
        captured = []

        async def mock_stream_turn(prompt):
            captured.append(prompt)
            yield FinishEvent(response=f"Echo: {prompt}", status="SUCCESS")

        mock_adapter.send_turn = mock_stream_turn
        mock_adapter.start = AsyncMock()

        custom_router = SessionRouter(adapter_factory=lambda cwd: mock_adapter)
        gw = IngressGateway(router=custom_router, bot_user_id="U_BOT")

        # Complex unicode: emojis with ZWJ sequences, math symbols, CJK, binary control chars
        special_text = (
            "🚀✨👨‍👩‍👧‍👦 𝔘𝔫𝔦𝔠𝔬𝔡𝔢 測試 ñoño ⚡️ "
            "\x00\x01\x02\x07\x08\x0b\x0c\x0e\x0f\x1b\x7f"
        )
        event = {
            "text": f"<@U_BOT> {special_text}",
            "channel": "C_SPECIAL",
            "ts": "400.1"
        }

        await gw.handle_message(event, self.client)

        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0], special_text)
        self.client.chat_postMessage.assert_awaited()
        # Control characters like \x1b must be stripped from egress by normalizer
        sent_text = self.client.chat_postMessage.call_args[1]["text"]
        self.assertNotIn("\x1b", sent_text)

    async def test_rapid_sequential_dispatches_to_same_session(self):
        """Gateway: Rapid sequential dispatches increment turn counter and maintain session state."""
        mock_adapter = MagicMock()

        async def mock_stream_turn(prompt):
            yield TextDeltaEvent(text_delta=f"Answer to {prompt}")
            yield FinishEvent(response=f"Answer to {prompt}", status="SUCCESS")

        mock_adapter.send_turn = mock_stream_turn
        session = SessionHandle("C_SEQ:T_SEQ", "C_SEQ", "T_SEQ", mock_adapter)

        for i in range(1, 6):
            events = []
            async for ev in session.send_user_message(f"Turn {i}"):
                events.append(ev)
            self.assertEqual(session.current_turn, i)
            self.assertEqual(session.state, SessionState.IDLE)
            self.assertEqual(session.last_response, f"Answer to Turn {i}")

    async def test_concurrent_message_dispatches_serialized(self):
        """Gateway: Concurrent dispatches to the same SessionHandle are strictly serialized by session lock."""
        mock_adapter = MagicMock()
        execution_order = []

        async def mock_stream_turn(prompt):
            execution_order.append(f"start_{prompt}")
            await asyncio.sleep(0.05)
            yield TextDeltaEvent(text_delta=f"processing_{prompt}")
            await asyncio.sleep(0.05)
            execution_order.append(f"end_{prompt}")
            yield FinishEvent(response=f"completed_{prompt}", status="SUCCESS")

        mock_adapter.send_turn = mock_stream_turn
        session = SessionHandle("C_CONC:T_CONC", "C_CONC", "T_CONC", mock_adapter)

        async def dispatch_turn(prompt):
            events = []
            async for ev in session.send_user_message(prompt):
                events.append(ev)
            return events

        # Concurrently launch two turns
        res1, res2 = await asyncio.gather(
            dispatch_turn("turn_A"),
            dispatch_turn("turn_B")
        )

        # Must not interleave execution (lock must ensure one turn finishes before the other starts)
        valid_orders = [
            ["start_turn_A", "end_turn_A", "start_turn_B", "end_turn_B"],
            ["start_turn_B", "end_turn_B", "start_turn_A", "end_turn_A"],
        ]
        self.assertIn(execution_order, valid_orders)
        self.assertEqual(session.current_turn, 2)
        self.assertEqual(session.state, SessionState.IDLE)

    async def test_inactivity_reaper_exact_ttl_expiration(self):
        """Gateway: Reaper prunes sessions strictly past timeout_seconds while retaining active sessions."""
        ttl = 10.0
        router = SessionRouter(inactivity_timeout=ttl, adapter_factory=lambda cwd: MagicMock())

        s_active, _ = await router.get_or_create("C_REAP", "T_ACTIVE")
        s_expired, _ = await router.get_or_create("C_REAP", "T_EXPIRED")

        now = time.time()
        # Active session: 1 second before expiration boundary
        s_active.last_active_ts = now - (ttl - 1.0)
        # Expired session: 0.5 seconds past expiration boundary
        s_expired.last_active_ts = now - (ttl + 0.5)

        pruned = await router.reap_expired_sessions()

        self.assertIn("C_REAP:T_EXPIRED", pruned)
        self.assertNotIn("C_REAP:T_ACTIVE", pruned)
        self.assertIn("C_REAP:T_ACTIVE", router.sessions)
        self.assertNotIn("C_REAP:T_EXPIRED", router.sessions)
        self.assertEqual(s_expired.state, SessionState.CLOSED)
        self.assertEqual(s_active.state, SessionState.IDLE)

    async def test_closed_session_resurrection_prevention(self):
        """Gateway: Closed sessions cannot accept messages; router creates fresh session rather than resurrecting."""
        router = SessionRouter(adapter_factory=lambda cwd: MagicMock())
        session, is_new = await router.get_or_create("C_RES", "T_RES")
        self.assertTrue(is_new)

        # Explicit teardown / close
        await session.close(reason="explicit_teardown")
        self.assertEqual(session.state, SessionState.CLOSED)

        # 1. Closed session rejects direct message dispatch with RuntimeError
        with self.assertRaises(RuntimeError) as ctx:
            async for _ in session.send_user_message("Try send on closed"):
                pass
        self.assertIn("is CLOSED; cannot send message", str(ctx.exception))

        # 2. Subsequent router.get_or_create must return a NEW session, not resurrect the closed instance
        new_session, is_new2 = await router.get_or_create("C_RES", "T_RES")
        self.assertTrue(is_new2)
        self.assertIsNot(new_session, session)
        self.assertEqual(new_session.state, SessionState.IDLE)
        self.assertFalse(new_session.is_expired())


# ===========================================================================
# 2. Headless Agent Adapter Boundaries
# ===========================================================================
class TestTier2HeadlessAgentAdapterBoundaries(unittest.IsolatedAsyncioTestCase):
    """
    Tier 2 boundary tests for HeadlessAgentAdapter.
    Validates malformed NDJSON stream tolerance, null subfield payloads,
    massive 128KB+ stderr pipe burst draining, and rapid generator aclose() termination.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fm_test_adapter_")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_malformed_ndjson_non_dict_json_primitives(self):
        """Adapter: Non-dict valid JSON ([], 'hello', 123, null, true) in stdout stream are ignored without crash."""
        adapter = HeadlessAgentAdapter(cwd=self.temp_dir)
        adapter.conversation_id = "test-conv-malformed"

        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.stdin = AsyncMock()

        reader = asyncio.StreamReader()
        mock_proc.stdout = reader
        mock_proc.stderr = AsyncMock()
        adapter.process = mock_proc

        # Feed non-dict JSON lines followed by a valid finish event
        stream_lines = [
            b"[]\n",
            b"\"hello world\"\n",
            b"12345\n",
            b"null\n",
            b"true\n",
            b"false\n",
            b"{\"event\": \"result\", \"result\": {\"status\": \"SUCCESS\", \"response\": \"Clean recovery\"}}\n"
        ]
        for l in stream_lines:
            reader.feed_data(l)
        reader.feed_eof()

        events = []
        async for ev in adapter.send_turn("test malformed primitives"):
            events.append(ev)

        # Expected: status_update (working) followed by finish (SUCCESS)
        event_types = [ev.event_type for ev in events]
        self.assertIn("status_update", event_types)
        self.assertIn("finish", event_types)
        finish_ev = events[-1]
        self.assertIsInstance(finish_ev, FinishEvent)
        self.assertEqual(finish_ev.status, "SUCCESS")
        self.assertEqual(finish_ev.response, "Clean recovery")

    async def test_malformed_ndjson_raw_invalid_json_and_empty_lines(self):
        """Adapter: Raw invalid JSON, truncated lines, and empty lines in stdout stream are safely skipped."""
        adapter = HeadlessAgentAdapter(cwd=self.temp_dir)
        adapter.conversation_id = "test-conv-raw-junk"

        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.stdin = AsyncMock()

        reader = asyncio.StreamReader()
        mock_proc.stdout = reader
        mock_proc.stderr = AsyncMock()
        adapter.process = mock_proc

        stream_lines = [
            b"\n",
            b"   \n",
            b"not a json object\n",
            b"{truncated_json: true\n",
            b"{\"event\": \"step_update\", \"step_update\": {\"step_type\": \"agent_response\", \"text_delta\": \"Valid delta\"}}\n",
            b"{\"event\": \"result\", \"result\": {\"status\": \"SUCCESS\", \"response\": \"Final output\"}}\n"
        ]
        for l in stream_lines:
            reader.feed_data(l)
        reader.feed_eof()

        events = []
        async for ev in adapter.send_turn("test raw junk"):
            events.append(ev)

        deltas = [ev for ev in events if isinstance(ev, TextDeltaEvent)]
        self.assertEqual(len(deltas), 1)
        self.assertEqual(deltas[0].text_delta, "Valid delta")

        finishes = [ev for ev in events if isinstance(ev, FinishEvent)]
        self.assertEqual(len(finishes), 1)
        self.assertEqual(finishes[0].response, "Final output")

    def test_agent_event_protocol_deserialization_edge_cases(self):
        """Adapter Protocol: AgentEvent deserialization handles invalid types and unknown event types."""
        # Non-dict payload to from_dict raises ValueError
        with self.assertRaises(ValueError):
            AgentEvent.from_dict(["not", "a", "dict"])

        with self.assertRaises(ValueError):
            AgentEvent.from_dict("string payload")

        # Non-string to from_ndjson raises ValueError
        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson(12345)

        # from_ndjson with invalid JSON raises JSONDecodeError
        with self.assertRaises(json.JSONDecodeError):
            AgentEvent.from_ndjson("bad json")

        # from_ndjson with non-dict JSON raises ValueError
        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson("[]")

        # from_dict with unknown event type falls back to base AgentEvent
        event = AgentEvent.from_dict({"event_type": "unknown_future_event", "custom_field": 42})
        self.assertIsInstance(event, AgentEvent)
        self.assertEqual(event.event_type, "unknown_future_event")

    async def test_event_payloads_with_null_subfields(self):
        """Adapter: Events with null subfields (step_update=None, tool_info=None, etc.) process without error."""
        adapter = HeadlessAgentAdapter(cwd=self.temp_dir)
        adapter.conversation_id = "test-conv-nulls"

        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.stdin = AsyncMock()

        reader = asyncio.StreamReader()
        mock_proc.stdout = reader
        mock_proc.stderr = AsyncMock()
        adapter.process = mock_proc

        null_events = [
            b"{\"event\": \"step_update\", \"step_update\": null}\n",
            b"{\"event\": \"step_update\", \"step_update\": {\"step_type\": \"tool\", \"state\": \"ACTIVE\", \"tool_name\": \"bash\", \"tool_info\": null}}\n",
            b"{\"event\": \"step_update\", \"step_update\": {\"step_type\": \"tool\", \"state\": \"DONE\", \"tool_name\": \"bash\", \"tool_info\": {\"parameters\": null, \"output\": null, \"error\": null}}}\n",
            b"{\"event\": \"step_update\", \"step_update\": {\"step_type\": \"status\", \"status\": null, \"detail\": null}}\n",
            b"{\"event\": \"awaiting_input\", \"awaiting_input\": null}\n",
            b"{\"event\": \"result\", \"result\": null}\n",
        ]
        for l in null_events:
            reader.feed_data(l)
        reader.feed_eof()

        events = []
        async for ev in adapter.send_turn("test null fields"):
            events.append(ev)

        # Verify all events yielded without unhandled AttributeError
        event_types = [ev.event_type for ev in events]
        self.assertIn("tool_start", event_types)
        self.assertIn("tool_end", event_types)
        self.assertIn("awaiting_input", event_types)
        self.assertIn("finish", event_types)

        tool_start = next(e for e in events if isinstance(e, ToolStartEvent))
        self.assertEqual(tool_start.parameters, {})

        tool_end = next(e for e in events if isinstance(e, ToolEndEvent))
        self.assertIsNone(tool_end.output)
        self.assertFalse(tool_end.is_error)

        finish = next(e for e in events if isinstance(e, FinishEvent))
        self.assertEqual(finish.status, "SUCCESS")

    async def test_massive_stderr_burst_streaming_no_deadlock(self):
        """Adapter: 150KB+ stderr burst (>64KB pipe buffer) drained in <1s concurrently without deadlock."""
        # Create a mock subprocess script that emits init, then on turn writes 150KB stderr before result
        worker_script = os.path.join(self.temp_dir, "stderr_burst_worker.py")
        with open(worker_script, "w") as f:
            f.write(
                "import sys, json\n"
                "sys.stdout.write(json.dumps({'event': 'init', 'conversation_id': 'conv-burst'}) + '\\n')\n"
                "sys.stdout.flush()\n"
                "line = sys.stdin.readline()\n"
                "# Burst 160,000 bytes into stderr (exceeding standard 64KB POSIX pipe buffer)\n"
                "sys.stderr.write('E' * 160000 + '\\n')\n"
                "sys.stderr.flush()\n"
                "sys.stdout.write(json.dumps({'event': 'result', 'result': {'status': 'SUCCESS', 'response': 'burst drained successfully'}}) + '\\n')\n"
                "sys.stdout.flush()\n"
            )
        os.chmod(worker_script, 0o755)

        wrapper_sh = os.path.join(self.temp_dir, "run_burst.sh")
        with open(wrapper_sh, "w") as f:
            f.write(f"#!/bin/bash\nexec {sys.executable} {worker_script} \"$@\"\n")
        os.chmod(wrapper_sh, 0o755)

        adapter = HeadlessAgentAdapter(
            cwd=self.temp_dir,
            agy_bin=wrapper_sh,
        )

        t0 = time.time()
        await adapter.start()

        events = []
        async for ev in adapter.send_turn("trigger burst"):
            events.append(ev)

        elapsed = time.time() - t0
        await adapter.stop()

        # Must drain within 1.0s without deadlocking child process on write()
        self.assertLess(elapsed, 1.0, f"Stderr burst drained too slowly ({elapsed}s), possible deadlock")
        self.assertEqual(len(events), 2)  # status_update + finish
        finish_ev = events[-1]
        self.assertEqual(finish_ev.status, "SUCCESS")
        self.assertEqual(finish_ev.response, "burst drained successfully")

    async def test_rapid_generator_termination_aclose_mid_stream(self):
        """Adapter: Terminating turn generator via aclose() releases lock and permits subsequent turn."""
        worker_script = os.path.join(self.temp_dir, "streaming_worker.py")
        with open(worker_script, "w") as f:
            f.write(
                "import sys, json, time\n"
                "sys.stdout.write(json.dumps({'event': 'init', 'conversation_id': 'conv-stream'}) + '\\n')\n"
                "sys.stdout.flush()\n"
                "while True:\n"
                "    line = sys.stdin.readline()\n"
                "    if not line: break\n"
                "    for i in range(30):\n"
                "        delta = {'event': 'step_update', 'step_update': {'step_type': 'agent_response', 'state': 'ACTIVE', 'step_index': i, 'text_delta': f'chunk_{i} '}}\n"
                "        sys.stdout.write(json.dumps(delta) + '\\n')\n"
                "        sys.stdout.flush()\n"
                "        sys.stderr.write(f'stderr_chunk_{i}\\n')\n"
                "        sys.stderr.flush()\n"
                "        time.sleep(0.01)\n"
                "    res = {'event': 'result', 'result': {'status': 'SUCCESS', 'response': 'finished normally'}}\n"
                "    sys.stdout.write(json.dumps(res) + '\\n')\n"
                "    sys.stdout.flush()\n"
            )
        os.chmod(worker_script, 0o755)

        wrapper_sh = os.path.join(self.temp_dir, "run_streaming.sh")
        with open(wrapper_sh, "w") as f:
            f.write(f"#!/bin/bash\nexec {sys.executable} {worker_script} \"$@\"\n")
        os.chmod(wrapper_sh, 0o755)

        adapter = HeadlessAgentAdapter(
            cwd=self.temp_dir,
            agy_bin=wrapper_sh,
        )
        await adapter.start()

        # Turn 1: consume only 2 tokens then abruptly terminate generator
        gen1 = adapter.send_turn("first prompt")
        count = 0
        async for _ in gen1:
            count += 1
            if count == 2:
                await gen1.aclose()
                break

        # Turn 2: should run completely to finish without deadlock from orphaned lock
        t2_events = []
        async for ev in adapter.send_turn("second prompt"):
            t2_events.append(ev)

        await adapter.stop()

        self.assertGreater(len(t2_events), 1)
        finish_ev = t2_events[-1]
        self.assertIsInstance(finish_ev, FinishEvent)
        self.assertEqual(finish_ev.status, "SUCCESS")
        self.assertEqual(finish_ev.response, "finished normally")


# ===========================================================================
# 3. Execution Harness & Runner Boundaries
# ===========================================================================
class TestTier2ExecutionHarnessAndRunnerBoundaries(unittest.IsolatedAsyncioTestCase):
    """
    Tier 2 boundary tests for Execution Harness, Runner, and Buffer Observer.
    Validates commands right at yield_timeout boundary, interactive blocker extraction,
    and DualPaneHarness teardown idempotency.
    """

    def setUp(self):
        self.executor = RunnerExecutor(herdr_bin="/usr/local/bin/herdr")
        self.observer = RunnerBufferObserver()

    async def asyncTearDown(self):
        if hasattr(self, "executor") and self.executor is not None:
            await self.executor.async_close()

    async def test_runner_command_completed_right_at_yield_timeout_boundary(self):
        """Runner: Command completing right at yield_timeout boundary returns status='completed'."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.pid = 33333
            mock_proc.communicate.return_value = (b"Command finished on boundary\n", b"")
            mock_exec.return_value = mock_proc

            with patch.object(self.executor, "_get_pane_foreground_pid", return_value=None):
                handle = await self.executor.run_command(
                    pane="runner",
                    command="quick_task.sh",
                    yield_timeout=0.1
                )

                self.assertEqual(handle.status, "completed")
                self.assertEqual(handle.returncode, 0)
                self.assertEqual(handle.stdout, "Command finished on boundary")

    async def test_runner_command_exceeded_yield_timeout_boundary(self):
        """Runner: Command exceeding yield_timeout boundary yields status='running' and foreground PID."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 44444
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 44444}}).encode(),
                b""
            )

            mock_exec.side_effect = [mock_proc, mock_info_proc]

            handle = await self.executor.run_command(
                pane="runner",
                command="long_daemon.sh",
                yield_timeout=0.05
            )

            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 44444)
            self.assertIn("Liaison loop remains active", handle.message)

    async def test_runner_command_near_zero_yield_timeout(self):
        """Runner: Extremely small yield_timeout (0.001s) yields running handle immediately without blocking."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 55555
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 55555}}).encode(),
                b""
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            t0 = time.time()
            handle = await self.executor.run_command(
                pane="runner",
                command="heavy_job",
                yield_timeout=0.001
            )
            elapsed = time.time() - t0

            self.assertLess(elapsed, 0.2)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 55555)

    def test_interactive_blocker_detection_bracketed_urls(self):
        """Buffer Observer: URLs enclosed in brackets [http://127.0.0.1:8080/auth] are extracted cleanly."""
        bracketed_cases = [
            ("Visit [http://127.0.0.1:8080/auth] to authorize", "http://127.0.0.1:8080/auth"),
            ("Please open [https://localhost:3000/callback]", "https://localhost:3000/callback"),
            ("Authorize: [http://0.0.0.0:53682/auth?state=xyz]", "http://0.0.0.0:53682/auth?state=xyz"),
        ]
        for text, expected_url in bracketed_cases:
            blocker = self.observer.extract_interactive_blockers(text)
            self.assertIsNotNone(blocker, f"Failed to detect blocker in: {text}")
            self.assertEqual(blocker["type"], "oauth")
            self.assertEqual(blocker["url"], expected_url)

    def test_interactive_blocker_detection_trailing_punctuation(self):
        """Buffer Observer: Trailing punctuation (. , ; ) > ] \") is stripped from loopback OAuth URLs."""
        punct_cases = [
            ("Visit http://127.0.0.1:8080/auth. Done.", "http://127.0.0.1:8080/auth"),
            ("Open http://127.0.0.1:8080/auth, then return.", "http://127.0.0.1:8080/auth"),
            ("Endpoint is http://127.0.0.1:8080/auth; waiting", "http://127.0.0.1:8080/auth"),
            ("(see http://127.0.0.1:8080/auth)", "http://127.0.0.1:8080/auth"),
            ("<http://127.0.0.1:8080/auth>", "http://127.0.0.1:8080/auth"),
            ("\"http://127.0.0.1:8080/auth\"", "http://127.0.0.1:8080/auth"),
        ]
        for text, expected_url in punct_cases:
            blocker = self.observer.extract_interactive_blockers(text)
            self.assertIsNotNone(blocker, f"Failed on punctuation case: {text}")
            self.assertEqual(blocker["type"], "oauth")
            self.assertEqual(blocker["url"], expected_url)

    def test_interactive_blocker_detection_ansi_wrapped_urls(self):
        """Buffer Observer: ANSI color codes wrapping loopback URLs are stripped before extraction."""
        ansi_text = "\x1b[32;1mOpen auth link: \x1b[34;4mhttp://127.0.0.1:53682/auth\x1b[0m\n"
        blocker = self.observer.extract_interactive_blockers(ansi_text)

        self.assertIsNotNone(blocker)
        self.assertEqual(blocker["type"], "oauth")
        self.assertEqual(blocker["url"], "http://127.0.0.1:53682/auth")
        self.assertNotIn("\x1b", blocker["content"])

    def test_interactive_blocker_detection_malformed_loopback_and_external_urls(self):
        """Buffer Observer: Malformed loopback URLs without path or external domains do not trigger OAuth."""
        neg_cases = [
            "Just mentioning http://127.0.0.1 without port or path",
            "Refer to external service https://github.com/login/oauth/authorize",
            "Documentation at https://google.com/auth",
            "Generic message with no links or prompts at all.",
        ]
        for text in neg_cases:
            blocker = self.observer.extract_interactive_blockers(text)
            self.assertIsNone(blocker, f"False positive on: {text}")

    def test_interactive_blocker_detection_sudo_git_ssh_prompts(self):
        """Buffer Observer: sudo, git credentials, ssh passphrase, and [y/N] prompts extracted as 'prompt'."""
        prompt_cases = [
            ("[sudo] password for hermes: ", "[sudo] password for hermes:"),
            ("Password for 'https://github.com': ", "Password for 'https://github.com':"),
            ("Enter passphrase for key '/Users/hermes/.ssh/id_ed25519': ", "Enter passphrase for key '/Users/hermes/.ssh/id_ed25519':"),
            ("Do you want to continue? [y/N] ", "Do you want to continue? [y/N]"),
            ("Apply changes? (y/n): ", "Apply changes? (y/n):"),
        ]
        for raw_prompt, expected_substr in prompt_cases:
            blocker = self.observer.extract_interactive_blockers(raw_prompt)
            self.assertIsNotNone(blocker, f"Failed to detect prompt in: {raw_prompt}")
            self.assertEqual(blocker["type"], "prompt")
            self.assertIsNone(blocker["url"])
            self.assertIn(expected_substr.strip(), blocker["content"])

    async def test_dual_pane_harness_teardown_idempotency_already_closed(self):
        """Harness: Calling teardown() multiple times on a workspace is idempotent and avoids redundant CLI calls."""
        client = AsyncMock()
        client.run_cli.return_value = (0, "{}", "")
        harness = DualPaneHarness(herdr_client=client)

        harness._workspaces["sess_test"] = {
            "workspace_id": "ws_test_99",
            "status": "active"
        }

        # First call closes workspace
        await harness.teardown("sess_test")
        self.assertEqual(client.run_cli.await_count, 1)
        client.run_cli.assert_awaited_with(["workspace", "close", "ws_test_99"])
        self.assertEqual(harness._workspaces["sess_test"]["status"], "closed")

        # Second call must be idempotent no-op (status already closed)
        await harness.teardown("sess_test")
        self.assertEqual(client.run_cli.await_count, 1)

    async def test_dual_pane_harness_teardown_nonexistent_workspace(self):
        """Harness: Calling teardown() on nonexistent workspace ID completes cleanly without unhandled error."""
        client = AsyncMock()
        client.run_cli.return_value = (0, "{}", "")
        harness = DualPaneHarness(herdr_client=client)

        await harness.teardown("nonexistent_session_12345")
        self.assertEqual(client.run_cli.await_count, 1)
        client.run_cli.assert_awaited_with(["workspace", "close", "nonexistent_session_12345"])


# ===========================================================================
# 4. Transport & Wire Normalization Boundaries
# ===========================================================================
class TestTier2TransportAndWireNormalizationBoundaries(unittest.TestCase):
    """
    Tier 2 boundary tests for Transport Layer (Normalizer & Chunker).
    Validates exact 3,800 char boundary splits, unbroken words >3,800 chars,
    unclosed code block fences >3,800 chars, triple asterisks, nested quotes,
    and mixed CRLF/LF line endings.
    """

    def test_chunker_exact_3800_char_boundary(self):
        """Chunker: Exactly 3,800 characters returns 1 chunk; 3,801 characters returns 2 chunks <= 3,800."""
        text_3800 = "x" * 3800
        chunks_3800 = split_message_boundary_aware(text_3800, max_chars=3800)
        self.assertEqual(len(chunks_3800), 1)
        self.assertEqual(len(chunks_3800[0]), 3800)

        # 3,801 characters: boundary exceeded by exactly 1 character
        text_3801 = "x" * 3801
        chunks_3801 = split_message_boundary_aware(text_3801, max_chars=3800)
        self.assertEqual(len(chunks_3801), 2)
        for idx, c in enumerate(chunks_3801):
            self.assertLessEqual(len(c), 3800, f"Chunk {idx} exceeds max_chars: {len(c)}")
        self.assertEqual(chunks_3801[0] + chunks_3801[1], text_3801)

    def test_chunker_multi_chunk_exact_multiples(self):
        """Chunker: Exactly 7,600 characters (2 x 3,800) cleanly splits into chunks <= 3,800 without data loss."""
        part1 = "A" * 3800
        part2 = "B" * 3800
        combined = part1 + part2
        chunks = split_message_boundary_aware(combined, max_chars=3800)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(len(chunks[0]), 3800)
        self.assertEqual(len(chunks[1]), 3800)
        self.assertEqual(chunks[0], part1)
        self.assertEqual(chunks[1], part2)

    def test_chunker_unbroken_words_exceeding_3800_chars(self):
        """Chunker: Contiguous unbroken token of 10,000 chars slices safely without infinite loop or loss."""
        unbroken = "Z" * 10000
        chunks = split_message_boundary_aware(unbroken, max_chars=3800)

        self.assertGreater(len(chunks), 1)
        for idx, c in enumerate(chunks):
            self.assertLessEqual(len(c), 3800, f"Chunk {idx} exceeds limit: {len(c)}")

        # Complete reconstruction without lost characters
        self.assertEqual("".join(chunks), unbroken)

    def test_chunker_unclosed_code_fence_exceeding_3800_chars(self):
        """Chunker: 6,000+ chars inside unclosed code block safely closes chunk 1 and reopens chunk 2 with ```lang."""
        code_lines = [f"item_{i:04d} = compute_metric({i})" for i in range(250)]
        code_body = "\n".join(code_lines)  # ~7,700 chars
        unclosed_text = f"```python\n{code_body}"  # CommonMark: unclosed extends to EOF

        chunks = split_message_boundary_aware(unclosed_text, max_chars=3800)
        self.assertGreater(len(chunks), 1)

        for idx, c in enumerate(chunks):
            self.assertLessEqual(len(c), 3800, f"Chunk {idx} exceeds max_chars: {len(c)}")

        # Chunk 1 must be cleanly closed with ```
        first_chunk = chunks[0]
        self.assertTrue(first_chunk.rstrip().endswith("```"), "Chunk 1 did not close with code fence")

        # Chunk 2 must reopen with ```python
        second_chunk = chunks[1]
        self.assertTrue(second_chunk.startswith("```python"), "Chunk 2 did not reopen with ```python")

        # Invariant: intermediate split chunks must have balanced fences; final chunk retains unclosed EOF fence
        for idx, c in enumerate(chunks[:-1]):
            self.assertEqual(c.count("```") % 2, 0, f"Chunk {idx} has unbalanced backtick fences")
        self.assertEqual(chunks[-1].count("```") % 2, 1, "Final chunk of unclosed block has odd fence count")

    def test_chunker_unclosed_tilde_fence_exceeding_3800_chars(self):
        """Chunker: Unclosed tilde code block (~~~bash) preserves tilde fence delimiter across chunks."""
        code_lines = [f"echo 'line {i}'" for i in range(300)]
        unclosed_tilde = f"~~~bash\n" + "\n".join(code_lines)

        chunks = split_message_boundary_aware(unclosed_tilde, max_chars=3800)
        self.assertGreater(len(chunks), 1)

        first_chunk = chunks[0]
        self.assertTrue(first_chunk.rstrip().endswith("~~~"), "Chunk 1 did not close with ~~~")

        second_chunk = chunks[1]
        self.assertTrue(second_chunk.startswith("~~~bash"), "Chunk 2 did not reopen with ~~~bash")

    def test_formatting_triple_asterisks_bold_italic(self):
        """Normalizer: Triple asterisks (***text***) convert to Slack-native *_text_* with zero ** or *** in egress."""
        raw_text = (
            "Status: ***CRITICAL ALERT*** triggered on node.\n"
            "Here is ***important notice*** for Carl.\n"
            "### ***Heading with Bold Italic***\n"
        )
        cleaned = sanitize_for_slack(raw_text)

        # Zero double or triple asterisks outside code blocks
        self.assertNotIn("***", cleaned)
        self.assertNotIn("**", cleaned)
        self.assertIn("*_CRITICAL ALERT_*", cleaned)
        self.assertIn("*_important notice_*", cleaned)
        self.assertIn("*_Heading with Bold Italic_*", cleaned)

    def test_formatting_nested_markdown_quotes(self):
        """Normalizer: Multi-level nested blockquotes (> > >) are preserved; inner headers are transpiled."""
        nested_quotes = (
            "> Top-level quotation\n"
            "> > Second-level nested quotation\n"
            "> > > Third-level deeply nested quotation\n"
            ">\n"
            "> ### Nested Header Inside Quote\n"
            "> Substantive quote text.\n"
        )
        cleaned = sanitize_for_slack(nested_quotes)

        # Blockquote structure preserved
        self.assertIn("> Top-level quotation", cleaned)
        self.assertIn("> > Second-level nested quotation", cleaned)
        self.assertIn("> > > Third-level deeply nested quotation", cleaned)
        # Heading inside blockquote converted to Slack bold header
        self.assertIn("> *Nested Header Inside Quote*", cleaned)
        self.assertNotIn("###", cleaned)

    def test_formatting_mixed_crlf_and_lf_line_endings(self):
        """Normalizer: Mixed Windows CRLF (\\r\\n) and Unix LF (\\n) line endings normalize cleanly."""
        mixed_text = (
            "Line 1 with CRLF\r\n"
            "Line 2 with LF\n"
            "Line 3 with CRLF\r\n\r\n"
            "### Heading with CRLF\r\n"
            "```python\r\n"
            "def foo():\r\n"
            "    return True\r\n"
            "```\r\n"
            "Trailing note.\r\n"
        )
        cleaned = sanitize_for_slack(mixed_text)

        # Zero carriage returns in output
        self.assertNotIn("\r", cleaned)
        self.assertIn("Line 1 with CRLF", cleaned)
        self.assertIn("Line 2 with LF", cleaned)
        self.assertIn("*Heading with CRLF*", cleaned)
        self.assertIn("def foo():\n    return True", cleaned)


if __name__ == "__main__":
    unittest.main()
