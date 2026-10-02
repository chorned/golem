#!/usr/bin/env python3
"""
tests/test_m6_adversarial_empirical.py
Milestone 6 Empirical Adversarial Challenge Suite.

Adversarially stress-tests:
1. TextDeltaEvent instantiation permutations, extra kwargs, property aliases,
   and serialization/deserialization round-trips.
2. Root compatibility shims: canonical identity (assertIs), re-exports,
   and function forwarding for normalizer, herdr_fleet, agent_core, slack_transport, and app.
3. Acceptance criteria stress:
   - TC-01: Rapid conversational inquiries (0 worker panes, bold headers, 0 canned announcements).
   - TC-02: Rapid 5-turn multi-turn thread continuity (same session, monotonically increasing ts, 0 stale echo).
   - TC-03: Interactive blocker resilience (dual-pane separation, yield timeout, buffer scraping for OAuth/prompts).
   - TC-04: Isolated worker delegation (herdr_handoff, sandbox worktree, scratchpad sanitization).
   - TC-05: Formatting & chrome sanitization (extreme ANSI, VT100 boxes, spinners, CRLF, CommonMark -> Slack mrkdwn).
"""

from __future__ import annotations

import os
import sys
import json
import time
import asyncio
import unittest
from typing import Dict, Any, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

# Core imports
from first_mate.agent.protocol import (
    AgentEvent,
    AgentEventType,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
)
from first_mate.agent.adapter import HeadlessAgentAdapter
import first_mate.transport.normalizer as canonical_norm
from first_mate.transport.normalizer import (
    sanitize_for_slack,
    transpile_markdown_to_mrkdwn,
    strip_terminal_chrome,
    strip_ansi,
    strip_tui_debris,
    strip_box_drawing,
    strip_prompt_echoes,
)
from first_mate.transport.chunker import split_message_boundary_aware
from first_mate.transport.slack_client import SlackTransport, SlackClient
import first_mate.transport as transport_pkg
from first_mate.gateway import SessionRouter, SessionHandle, SessionState
from first_mate.gateway.ingress import IngressGateway, strip_bot_mentions
from first_mate.execution.runner import RunnerExecutor, ExecutionHandle
from first_mate.execution.buffer import (
    RunnerBufferObserver,
    OAUTH_CALLBACK_PATTERN,
    INTERACTIVE_PROMPT_PATTERNS,
)
from first_mate.execution.harness import DualPaneHarness
from first_mate.execution.delegation import WorkerDelegator, sanitize_worker_output
import first_mate.execution.mesh as canonical_mesh
import first_mate.agent as canonical_agent
from herdr_client import HerdrClient

# Root shims
import normalizer
import herdr_fleet
import agent_core
import app
import herdr_tools

MOCK_AGY_BIN = os.path.join(PROJECT_DIR, "tests", "mock_agy_proc.py")


# ===========================================================================
# 1. TextDeltaEvent Permutations & Wire Round-Trips
# ===========================================================================

class TestTextDeltaEventAdversarialPermutations(unittest.TestCase):
    """Exhaustive stress-testing of TextDeltaEvent arguments and serialization."""

    def test_single_positional_argument(self):
        """Verify single positional string maps to text and text_delta."""
        e = TextDeltaEvent("hello positional")
        self.assertEqual(e.text, "hello positional")
        self.assertEqual(e.text_delta, "hello positional")
        self.assertEqual(e.event_type, "text_delta")

    def test_empty_positional_argument(self):
        """Verify empty positional string."""
        e = TextDeltaEvent("")
        self.assertEqual(e.text, "")
        self.assertEqual(e.text_delta, "")

    def test_zero_arguments_default(self):
        """Verify parameterless construction defaults to empty string."""
        e = TextDeltaEvent()
        self.assertEqual(e.text, "")
        self.assertEqual(e.text_delta, "")
        self.assertEqual(e.event_type, "text_delta")

    def test_keyword_text_only(self):
        """Verify text='val' properly sets text and text_delta."""
        e = TextDeltaEvent(text="val_from_text")
        self.assertEqual(e.text, "val_from_text")
        self.assertEqual(e.text_delta, "val_from_text")

    def test_keyword_text_delta_only(self):
        """Verify text_delta='val' properly sets text and text_delta."""
        e = TextDeltaEvent(text_delta="val_from_delta")
        self.assertEqual(e.text, "val_from_delta")
        self.assertEqual(e.text_delta, "val_from_delta")

    def test_both_keywords_empty_strings(self):
        """Verify text='' and text_delta='' both empty."""
        e = TextDeltaEvent(text="", text_delta="")
        self.assertEqual(e.text, "")
        self.assertEqual(e.text_delta, "")

    def test_keyword_empty_text_nonempty_delta(self):
        """Verify text='' with text_delta='active' uses active delta."""
        e = TextDeltaEvent(text="", text_delta="active")
        self.assertEqual(e.text, "active")
        self.assertEqual(e.text_delta, "active")

    def test_keyword_nonempty_text_empty_delta(self):
        """Verify text='active' with text_delta='' uses active text."""
        e = TextDeltaEvent(text="active", text_delta="")
        self.assertEqual(e.text, "active")
        self.assertEqual(e.text_delta, "active")

    def test_property_setter_cross_synchronization(self):
        """Verify setting .text updates .text_delta and vice versa."""
        e = TextDeltaEvent(text="initial")
        e.text = "updated_text"
        self.assertEqual(e.text_delta, "updated_text")
        self.assertEqual(e.text, "updated_text")

        e.text_delta = "updated_delta"
        self.assertEqual(e.text, "updated_delta")
        self.assertEqual(e.text_delta, "updated_delta")

    def test_extra_base_kwargs_and_unknown_kwargs(self):
        """Verify known base fields are captured while unknown kwargs do not crash."""
        ts = 1700000000.5
        e = TextDeltaEvent(
            text="payload",
            conversation_id="conv-999",
            step_index=7,
            timestamp=ts,
            rogue_argument="should_not_crash",
            another_extra={"nested": True}
        )
        self.assertEqual(e.text, "payload")
        self.assertEqual(e.conversation_id, "conv-999")
        self.assertEqual(e.step_index, 7)
        self.assertEqual(e.timestamp, ts)

    def test_serialization_to_dict_and_ndjson(self):
        """Verify to_dict and to_ndjson serialization format."""
        e = TextDeltaEvent(text="hello roundtrip", conversation_id="c1", step_index=2)
        d = e.to_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["event_type"], "text_delta")
        self.assertEqual(d["text_delta"], "hello roundtrip")
        self.assertEqual(d["conversation_id"], "c1")
        self.assertEqual(d["step_index"], 2)

        ndjson_line = e.to_ndjson()
        self.assertTrue(ndjson_line.endswith("\n"))
        parsed = json.loads(ndjson_line.strip())
        self.assertEqual(parsed["text_delta"], "hello roundtrip")

    def test_deserialization_round_trip(self):
        """Verify round-trip deserialization produces identical TextDeltaEvent."""
        complex_strings = [
            "Simple text",
            "Multi-line\nText\nWith\nNewlines",
            "Unicode: 🚀 • ⚡️ • 漢字 • عربي • ñ",
            "Code snippet: ```python\ndef f(x):\n    return x * 2\n```",
            "Quotes: \"double\" and 'single' and `backticks`",
            "",
            "   leading and trailing whitespace   ",
            "Very long string " + ("a" * 10000),
        ]
        for s in complex_strings:
            orig = TextDeltaEvent(text=s, conversation_id="conv-1", step_index=10)
            wire_ndjson = orig.to_ndjson()

            # Deserialization via AgentEvent.from_ndjson
            deserialized = AgentEvent.from_ndjson(wire_ndjson)
            self.assertIsInstance(deserialized, TextDeltaEvent)
            self.assertEqual(deserialized.text, s)
            self.assertEqual(deserialized.text_delta, s)
            self.assertEqual(deserialized.conversation_id, "conv-1")
            self.assertEqual(deserialized.step_index, 10)

            # Deserialization via TextDeltaEvent.from_dict
            from_d = TextDeltaEvent.from_dict(orig.to_dict())
            self.assertIsInstance(from_d, TextDeltaEvent)
            self.assertEqual(from_d.text, s)
            self.assertEqual(from_d.text_delta, s)

    def test_deserialization_from_dict_with_only_text_key(self):
        """Verify from_dict correctly maps 'text' when 'text_delta' is missing."""
        data = {
            "event_type": "text_delta",
            "text": "only text key provided",
            "conversation_id": "c-test"
        }
        res = AgentEvent.from_dict(data)
        self.assertIsInstance(res, TextDeltaEvent)
        self.assertEqual(res.text, "only text key provided")
        self.assertEqual(res.text_delta, "only text key provided")
        self.assertEqual(res.conversation_id, "c-test")

    def test_deserialization_malformed_input_rejection(self):
        """Verify AgentEvent from_dict and from_ndjson reject invalid payloads."""
        with self.assertRaises(ValueError):
            AgentEvent.from_dict("not-a-dict")
        with self.assertRaises(ValueError):
            AgentEvent.from_dict([1, 2, 3])
        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson(12345)
        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson("not valid json {")
        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson('"json scalar, not object"')


# ===========================================================================
# 2. Root Compatibility Shims & Canonical Identity (assertIs)
# ===========================================================================

class TestRootCompatibilityShimsCanonicalIdentity(unittest.TestCase):
    """Verify root compatibility shims preserve exact object identity and function forwarding."""

    def test_normalizer_shim_canonical_identity(self):
        """Verify normalizer.py re-exports canonical functions and patterns via assertIs."""
        normalizer_symbols = [
            "sanitize_for_slack",
            "transpile_markdown_to_mrkdwn",
            "transpile_markdown_to_slack",
            "strip_terminal_chrome",
            "strip_ansi",
            "strip_tui_debris",
            "strip_box_drawing",
            "strip_prompt_echoes",
            "protect_code_blocks",
            "restore_code_blocks",
            "ANSI_ESCAPE_PATTERN",
            "BOX_DRAWING_PATTERN",
            "BRAILLE_SPINNER_PATTERN",
            "ELLIPSIS_CLOSE_PAREN_PATTERN",
            "EXPAND_PAREN_PATTERN",
            "STANDALONE_NUMBER_ELLIPSIS_PATTERN",
            "PROMPT_MARKER_PATTERN",
            "ASCII_SPINNER_PATTERN",
            "TOOL_CRUMB_PREFIXES",
            "NAV_BAR_PATTERN",
            "TIP_PATTERN",
        ]
        for sym in normalizer_symbols:
            self.assertTrue(hasattr(normalizer, sym), f"normalizer missing {sym}")
            self.assertIs(
                getattr(normalizer, sym),
                getattr(canonical_norm, sym),
                f"normalizer.{sym} is not identical (assertIs) to canonical_norm.{sym}"
            )

    def test_herdr_fleet_shim_canonical_identity(self):
        """Verify herdr_fleet.py re-exports canonical mesh symbols with assertIs."""
        self.assertIs(
            herdr_fleet.TOPOLOGY_SPECS,
            canonical_mesh.TOPOLOGY_SPECS,
            "herdr_fleet.TOPOLOGY_SPECS must be identical to canonical_mesh.TOPOLOGY_SPECS"
        )
        self.assertIs(
            herdr_fleet.NODE_ALIASES,
            canonical_mesh.NODE_ALIASES,
            "herdr_fleet.NODE_ALIASES must be identical to canonical_mesh.NODE_ALIASES"
        )
        self.assertIs(
            herdr_fleet.wrap_windows_command,
            canonical_mesh.wrap_windows_command,
            "herdr_fleet.wrap_windows_command must be identical to canonical_mesh.wrap_windows_command"
        )

    def test_agent_core_shim_canonical_identity(self):
        """Verify agent_core.py re-exports canonical agent protocol symbols with assertIs."""
        agent_symbols = [
            "AgentEvent",
            "TextDeltaEvent",
            "StatusUpdateEvent",
            "ToolStartEvent",
            "ToolEndEvent",
            "AwaitingInputEvent",
            "FinishEvent",
            "HeadlessAgentAdapter",
            "DEFAULT_AGY_BIN",
            "CALIBRATION_PROMPT",
            "SLACK_MRKDWN_RULES",
            "CHOICE_FORMATTING_RULES",
            "PACING_CALIBRATION_RULES",
            "TRIAGE_SUPPRESSION_RULES",
            "get_calibration_rules",
            "build_calibrated_system_prompt",
        ]
        for sym in agent_symbols:
            self.assertTrue(hasattr(agent_core, sym), f"agent_core missing {sym}")
            self.assertIs(
                getattr(agent_core, sym),
                getattr(canonical_agent, sym),
                f"agent_core.{sym} is not identical (assertIs) to canonical_agent.{sym}"
            )

    def test_transport_package_exports_and_identity(self):
        """Verify first_mate.transport exports SlackTransport and canonical chunker/normalizer."""
        self.assertIs(
            transport_pkg.SlackTransport,
            SlackTransport,
            "transport_pkg.SlackTransport must be identical to SlackTransport"
        )
        self.assertIs(
            transport_pkg.SlackClient,
            SlackClient,
            "transport_pkg.SlackClient must be identical to SlackClient"
        )
        self.assertIs(
            transport_pkg.split_message_boundary_aware,
            split_message_boundary_aware,
            "transport_pkg.split_message_boundary_aware must be identical"
        )

    def test_app_chunker_and_normalizer_forwarding(self):
        """Verify app module forwards split_message_boundary_aware and sanitize_for_slack cleanly."""
        p1 = "Paragraph 1 is long enough to exceed boundary."
        p2 = "Paragraph 2 is also long enough to exceed boundary."
        p3 = "Paragraph 3 is also long enough to exceed boundary."
        text = f"{p1}\n\n{p2}\n\n{p3}"
        chunks = app.split_message_boundary_aware(text, max_chars=len(p1) + 5)
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0], p1)
        self.assertEqual(chunks[1], p2)
        self.assertEqual(chunks[2], p3)

        dirty = "### Heading\n\x1b[31mRed text\x1b[0m with **bold** and [link](https://example.com)"
        clean = app.sanitize_for_slack(dirty)
        self.assertNotIn("###", clean)
        self.assertNotIn("\x1b", clean)
        self.assertNotIn("**", clean)
        self.assertIn("*Heading*", clean)
        self.assertIn("*bold*", clean)
        self.assertIn("<https://example.com|link>", clean)


# ===========================================================================
# 3. Acceptance Criteria Stress Testing (TC-01 .. TC-05)
# ===========================================================================

class AcceptanceStressMockClient:
    """Tracking client for conversational, reaction, and pane assertions."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, Any]] = []
        self.reactions_removed: List[Dict[str, Any]] = []
        self.worker_panes_spawned: List[str] = []

    async def reactions_add(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        self.reactions_added.append({"channel": channel, "timestamp": timestamp, "name": name})
        return {"ok": True}

    async def reactions_remove(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        self.reactions_removed.append({"channel": channel, "timestamp": timestamp, "name": name})
        return {"ok": True}

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None) -> Dict[str, Any]:
        msg = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": f"{time.time():.6f}",
        }
        self.posted_messages.append(msg)
        return {"ok": True, "ts": msg["ts"]}


class TestTC01ConversationalInquiryStress(unittest.IsolatedAsyncioTestCase):
    """TC-01: Conversational inquiries direct response under rapid burst."""

    async def test_rapid_conversational_inquiry_burst(self):
        """
        Simulate multiple rapid conversational fleet inquiries:
        - Must answer in a single turn.
        - Must use single-asterisk *bold* headers.
        - Must spawn 0 subagent worker panes.
        - Must contain 0 'Task Completed' footers.
        - Must contain 0 canned gateway announcements ('Starting agent...', 'Dispatching...').
        """
        client = AcceptanceStressMockClient()
        router = SessionRouter(
            adapter_factory=lambda cwd: HeadlessAgentAdapter(agy_bin=MOCK_AGY_BIN, cwd=cwd)
        )

        inquiries = [
            "How's our fleet doing?",
            "What's the status of our fleet?",
            "How are our machines running today?",
            "Can you give me a fleet overview?",
        ]

        expected_response = "*Fleet Status Summary*\n• *hermes*: Online (root disk 45GB free)\n• *desktop-pc*: Online (GPU idle)\n• *personal-laptop*: Standby\n• *oci-machine*: Online (VPS active)"

        def make_turn_fn(resp):
            async def _turn(*args, **kwargs):
                yield StatusUpdateEvent(status="working")
                yield TextDeltaEvent(text=resp)
                yield FinishEvent(response=resp)
            return _turn

        try:
            for i, inquiry in enumerate(inquiries):
                thread_ts = f"171000000{i}.000100"
                session = await router.get_or_create_session("C_FLEET", thread_ts)

                with patch.object(session.adapter, "send_turn", side_effect=make_turn_fn(expected_response)):
                    # Ingress non-verbal ack
                    await client.reactions_add("C_FLEET", thread_ts, "gear")

                    # Accumulate response
                    accumulated_text = ""
                    async for event in session.send_user_message(inquiry):
                        if isinstance(event, TextDeltaEvent):
                            accumulated_text += event.text

                    # Egress sanitization & posting
                    clean_output = sanitize_for_slack(accumulated_text)
                    await client.chat_postMessage("C_FLEET", clean_output, thread_ts=thread_ts)

                    # TC-01 Assertions:
                    # 1. Zero worker panes spawned
                    self.assertEqual(len(client.worker_panes_spawned), 0)

                    # 2. Bold headers using single asterisks
                    self.assertIn("*Fleet Status Summary*", clean_output)
                    self.assertNotIn("**Fleet Status Summary**", clean_output)
                    self.assertNotIn("### Fleet Status Summary", clean_output)

                    # 3. Zero 'Task Completed' footers
                    self.assertNotIn("Task Completed", clean_output)
                    self.assertNotIn("task completed", clean_output.lower())

                    # 4. Zero canned announcements
                    canned_phrases = ["starting agent", "dispatching worker", "spawning pane", "please wait while i"]
                    for phrase in canned_phrases:
                        self.assertNotIn(phrase, clean_output.lower())
        finally:
            await router.stop_reaper()


class TestTC02MultiTurnContinuityStress(unittest.IsolatedAsyncioTestCase):
    """TC-02: Rapid multi-turn conversation session continuity."""

    async def test_five_turn_burst_continuity_and_stale_suppression(self):
        """
        Simulate a rapid 5-turn conversation in the same thread:
        - Thread context persists across turns in the same SessionHandle.
        - Monotonically increasing last_active_ts.
        - Zero echoing of previous turns' prompts or stale history fragments.
        """
        router = SessionRouter(
            adapter_factory=lambda cwd: HeadlessAgentAdapter(agy_bin=MOCK_AGY_BIN, cwd=cwd)
        )
        channel = "C_CONVERSATION"
        thread_ts = "1720000000.000100"

        turns = [
            ("Show fleet cron jobs", "*Fleet Scheduled Jobs*\n1. Daily backup at 03:00\n2. ZFS scrub on Sunday\n3. Log rotation at midnight"),
            ("Which one runs at 3 AM?", "The *Daily backup* runs at 03:00 UTC on `hermes` targeting local storage pools."),
            ("What does it backup?", "It backs up `/Users/hermes/projects` and critical container configs to the ZFS dataset."),
            ("Can we change the frequency?", "Yes, we can adjust the crontab or launchd plist on `hermes` to run every 12 hours."),
            ("Sounds good, thank you.", "You got it, Carl. Standing by whenever you need further adjustments."),
        ]

        def make_turn_fn(resp):
            async def _turn(*args, **kwargs):
                yield StatusUpdateEvent(status="working")
                yield TextDeltaEvent(text=resp)
                yield FinishEvent(response=resp)
            return _turn

        try:
            session = await router.get_or_create_session(channel, thread_ts)
            self.assertIsNotNone(session)
            session_id = session.session_id

            previous_ts = session.last_active_ts

            for turn_idx, (user_prompt, assistant_response) in enumerate(turns):
                # Verify session remains the same object
                current_session = await router.get_or_create_session(channel, thread_ts)
                self.assertIs(current_session, session)
                self.assertEqual(current_session.session_id, session_id)

                with patch.object(session.adapter, "send_turn", side_effect=make_turn_fn(assistant_response)):
                    output_tokens = []
                    async for event in session.send_user_message(user_prompt):
                        if isinstance(event, TextDeltaEvent):
                            output_tokens.append(event.text)

                    combined_resp = "".join(output_tokens)
                    clean_resp = sanitize_for_slack(combined_resp)

                    # 1. Monotonic activity update
                    self.assertGreaterEqual(session.last_active_ts, previous_ts)
                    previous_ts = session.last_active_ts

                    # 2. Response contains assistant response
                    self.assertIn(assistant_response.splitlines()[0], clean_resp)

                    # 3. Response does NOT echo the user prompt text back
                    self.assertNotIn(f"[User Message] {user_prompt}", clean_resp)
                    self.assertNotIn(f"User: {user_prompt}", clean_resp)

                    # 4. Response does not echo stale previous turn prompts
                    if turn_idx > 0:
                        for prev_prompt, _ in turns[:turn_idx]:
                            self.assertNotIn(prev_prompt, clean_resp)
        finally:
            await router.stop_reaper()


class TestTC03InteractiveBlockerResilienceStress(unittest.IsolatedAsyncioTestCase):
    """TC-03: Dual-pane execution yield and interactive blocker detection."""

    async def test_dual_pane_yield_and_oauth_url_extraction(self):
        """
        Simulate an interactive command (e.g. rclone config / OAuth flow):
        - DualPaneHarness allocates Pane 1 (liaison) and Pane 2 (runner).
        - RunnerExecutor yields ExecutionHandle with status='running' within 0.2s when command exceeds timeout.
        - Liaison loop remains responsive and does not block.
        - RunnerBufferObserver scrapes buffer and surfaces OAuth URL and [y/N] prompt.
        """
        mock_client = MagicMock(spec=HerdrClient)
        mock_ws_output = json.dumps({
            "result": {
                "workspace": {"workspace_id": "ws-m4-1"},
                "root_pane": {"pane_id": "ws-m4-1:p1"}
            }
        })
        mock_runner_output = json.dumps({
            "result": {
                "pane": {"pane_id": "ws-m4-1:p2"}
            }
        })
        mock_client.run_cli = AsyncMock(side_effect=[
            (0, mock_ws_output, ""),       # workspace create
            (0, "started liaison", ""),   # agent start fm-interactive-liaison
            (0, mock_runner_output, ""),  # pane create --label runner
        ])

        harness = DualPaneHarness(herdr_client=mock_client)
        session_id = "fm-interactive"

        liaison_agent, runner_pane = await harness.provision_dual_pane(session_id, "/Users/hermes/projects")
        self.assertEqual(liaison_agent, "fm-interactive-liaison")
        self.assertEqual(runner_pane, "runner")

        # Test RunnerExecutor yield on long command
        executor = RunnerExecutor(herdr_client=mock_client)

        def mock_create_subprocess_exec(*args, **kwargs):
            proc = MagicMock()
            cmd_tuple = tuple(str(a) for a in args)
            if "process-info" in cmd_tuple:
                proc.returncode = 0
                proc.pid = 99887
                proc.communicate = AsyncMock(return_value=(b'{"result": {"pid": 99887}}', b""))
            else:
                proc.returncode = 0
                proc.pid = 99887
                async def slow_communicate():
                    await asyncio.sleep(2.0)
                    return b"stdout", b"stderr"
                proc.communicate = slow_communicate
            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=mock_create_subprocess_exec):
            start_t = time.perf_counter()
            handle = await executor.run_command(runner_pane, "rclone config reconnect", yield_timeout=0.1)
            duration = time.perf_counter() - start_t

            # Verify command yielded promptly
            self.assertLess(duration, 0.5)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pane, runner_pane)
            self.assertEqual(handle.command, "rclone config reconnect")
            self.assertEqual(handle.pid, 99887)

        # Test RunnerBufferObserver extracting OAuth URL and prompt blockers
        observer = RunnerBufferObserver(herdr_client=mock_client)

        oauth_buffer = """
2026/09/30 01:23:45 NOTICE: If your browser doesn't open automatically go to the following link:
http://127.0.0.1:53682/auth?state=xyz987token
Log in and authorize rclone for access
"""
        with patch.object(observer, "read_buffer", new_callable=AsyncMock, return_value=oauth_buffer):
            extracted_oauth = observer.extract_interactive_blockers(oauth_buffer)
            self.assertIsNotNone(extracted_oauth)
            self.assertEqual(extracted_oauth.get("type"), "oauth")
            self.assertEqual(extracted_oauth.get("url"), "http://127.0.0.1:53682/auth?state=xyz987token")

            # Verify Slack transpilation produces valid clickable link
            surfaced_msg = f"Rclone is waiting for authorization at <{extracted_oauth['url']}|Authorize Rclone>."
            clean_slack_msg = sanitize_for_slack(surfaced_msg)
            self.assertIn("<http://127.0.0.1:53682/auth?state=xyz987token|Authorize Rclone>", clean_slack_msg)

        prompt_buffer = """
Waiting for code...
Configure drive as team drive? [y/N]
>
"""
        with patch.object(observer, "read_buffer", new_callable=AsyncMock, return_value=prompt_buffer):
            extracted_prompt = observer.extract_interactive_blockers(prompt_buffer)
            self.assertIsNotNone(extracted_prompt)
            self.assertEqual(extracted_prompt.get("type"), "prompt")
            self.assertIn("[y/N]", extracted_prompt.get("content", ""))


class TestTC04WorkerDelegationStress(unittest.IsolatedAsyncioTestCase):
    """TC-04: Isolated worker delegation via herdr_handoff."""

    async def test_worker_sandbox_isolation_and_scratchpad_suppression(self):
        """
        Verify complex coding tasks delegated to OpenCode:
        - Run in isolated branch / worktree sandbox.
        - Worker output with internal scratchpads, git diffs, and ANSI escape sequences
          is sanitized before returning to the user.
        """
        mock_client = MagicMock(spec=HerdrClient)
        # Mock worktree create, pane create, agent start, pane wait, pane read
        mock_client.run_cli = AsyncMock(side_effect=[
            (0, "worktree created", ""),  # worktree create
            (0, json.dumps({"result": {"pane": {"pane_id": "wt:p1"}}}), ""),  # pane create
            (0, "agent started", ""),     # agent start
            (0, "agent done", ""),        # agent wait
            (0, (
                "<scratchpad>\n"
                "Planning changes to auth.py...\n"
                "</scratchpad>\n"
                "[scratchpad] calculating test coverage\n"
                "╭──────────────────────────────────────────────╮\n"
                "│ OpenCode Task Completed: 1 file modified     │\n"
                "╰──────────────────────────────────────────────╯\n"
                "⠋ Running test suite...\n"
                "auth.py...)\n"
                ">\n"
                "\x1b[32mAll 42 tests passed.\x1b[0m\n"
                "*Summary Verdict*: Token verification routine successfully implemented in `auth.py`."
            ), ""),  # pane read
        ])

        delegator = WorkerDelegator(herdr_client=mock_client)

        result = await delegator.handoff(
            subagent="opencode",
            task="Implement verify_token in auth.py",
            cwd="/Users/hermes/projects/first-mate",
            isolate_branch=True
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["subagent"], "opencode")
        clean_summary = result["output"]

        # 1. No scratchpad blocks or lines
        self.assertNotIn("<scratchpad>", clean_summary)
        self.assertNotIn("Planning changes to auth.py", clean_summary)
        self.assertNotIn("[scratchpad]", clean_summary)

        # 2. No VT100 box characters
        for glyph in ["╭", "╮", "│", "╰", "╯", "─"]:
            self.assertNotIn(glyph, clean_summary)

        # 3. No ANSI codes
        self.assertNotIn("\x1b", clean_summary)

        # 4. No spinner or crumb
        self.assertNotIn("⠋", clean_summary)
        self.assertNotIn("Running test suite...", clean_summary)

        # 5. Core summary preserved with Slack mrkdwn
        self.assertIn("*Summary Verdict*", clean_summary)
        self.assertIn("Token verification routine successfully implemented", clean_summary)
        self.assertIn("All 42 tests passed.", clean_summary)


class TestTC05FormattingAndChromeSanitizationStress(unittest.TestCase):
    """TC-05: Formatting, ANSI escape codes, VT100 boxes, and Markdown transpilation."""

    def test_extreme_dirty_stream_sanitization(self):
        """
        Feed an extreme combination of dirty terminal and markdown elements:
        - 24-bit TrueColor and standard ANSI sequences.
        - Isolated ESC bytes (\x1b, \x1b\x7f).
        - VT100 box-drawing glyphs.
        - Braille spinners and ASCII spinners.
        - Terminal widget collapse markers (... (expand)).
        - CRLF and LF mixed line endings.
        - ATX markdown headers (###, ##, #).
        - CommonMark bold (**text**).
        - CommonMark markdown links ([label](url)).
        - Preserved code fences with internal untouched syntax.
        """
        dirty_input = (
            "### Section 1: Fleet Diagnostics\r\n"
            "\x1b[38;2;255;100;0mConnecting to fleet...\x1b[0m\x1b\x7f\r\n"
            "╭─── Fleet Node Status ───╮\r\n"
            "│ ⠋ Polling nodes... (expand) │\r\n"
            "╰─────────────────────────╯\r\n"
            "**Node Status**: All nodes are **online**.\r\n"
            "For full logs, view [Grafana Dashboard](https://metrics.internal.homelab/fleet).\r\n"
            "\r\n"
            "```python\r\n"
            "# Code blocks must preserve formatting and not be broken by CRLF\r\n"
            "def check_health():\r\n"
            "    headers = {'Authorization': 'Bearer test'}\r\n"
            "    return True\r\n"
            "```\r\n"
            "### Section 2: Completed.\r\n"
        )

        clean = sanitize_for_slack(dirty_input)

        # 1. Zero ATX headers
        self.assertNotIn("###", clean)
        self.assertNotIn("##", clean)
        self.assertIn("*Section 1: Fleet Diagnostics*", clean)
        self.assertIn("*Section 2: Completed.*", clean)

        # 2. Zero ANSI escape sequences and bare ESC bytes
        self.assertNotIn("\x1b", clean)
        self.assertNotIn("[38;2;", clean)

        # 3. Zero VT100 box-drawing glyphs
        for box_char in "╭─╮│╰╯":
            self.assertNotIn(box_char, clean)

        # 4. Zero spinners or widget collapse debris
        self.assertNotIn("⠋", clean)
        self.assertNotIn("... (expand)", clean)

        # 5. Zero double asterisks
        self.assertNotIn("**", clean)
        self.assertIn("*Node Status*", clean)
        self.assertIn("*online*", clean)

        # 6. Links converted to Slack mrkdwn <url|label>
        self.assertIn("<https://metrics.internal.homelab/fleet|Grafana Dashboard>", clean)
        self.assertNotIn("[Grafana Dashboard](https://metrics.internal.homelab/fleet)", clean)

        # 7. Code block preserved with clean line endings (no \r)
        self.assertNotIn("\r", clean)
        self.assertIn("```python\n# Code blocks must preserve formatting", clean)
        self.assertIn("def check_health():\n    headers = {'Authorization': 'Bearer test'}\n    return True\n```", clean)


if __name__ == "__main__":
    unittest.main()
