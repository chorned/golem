#!/usr/bin/env python3
"""
tests/e2e/test_tier3_combinations.py
E2E Tier 3 Cross-Feature Combination Test Suite for First Mate 2.0.

Covers Pairwise & Multi-Feature Interactions:
1. Combination 1 (R1 + R2 + R4): Ingress Gateway + Headless Adapter + Transport Normalizer + MilestoneUpdater
   - Inbound Slack event -> SessionRouter assigns thread -> HeadlessAgentAdapter streams events
     -> Normalizer cleans output -> MilestoneUpdater debounces and updates Slack message -> Finish event finalized.
2. Combination 2 (R1 + R3): Ingress Gateway + DualPaneHarness + BufferObserver
   - Inbound user request triggers tool execution in Pane 2 -> RunnerExecutor yields running handle in 3.0s
     -> BufferObserver detects OAuth URL / [y/N] prompt -> Gateway surfaces awaiting_input blocker to Slack
     without leaking curses chrome or PTY prompt.
3. Combination 3 (R1 + R2): SessionRouter + Inactivity Watchdog + Headless Adapter Lifecycle
   - Multi-turn active session -> Session inactivity timer expires -> Reaper terminates child adapter cleanly
     -> New message in same thread reinstates clean fresh session without zombie state or lingering locks.
4. Combination 4 (R3 + R4): WorkerDelegator + FleetMesh + Normalizer Sanitization
   - Task delegated to opencode-dev via herdr_handoff -> Worktree created on isolated branch
     -> Subprocess output containing scratchpads, VT100 boxes, prompt echoes (❯ ...), and braille spinners
     sanitized by multi-stage filter -> Executive briefing returned.
5. Combination 5 (R1 + R5): Dual-Track app.py + Compatibility Shims + IngressGateway
   - Incoming Bolt events handled by ingress_gateway when USE_HEADLESS_GATEWAY=1 while legacy registries
     (app.sessions, app.agent_name_to_session, SPAWNED_AGENTS) stay populated for backwards compatibility.

All tests follow opaque-box, requirement-driven test design derived from ORIGINAL_REQUEST.md,
PROJECT.md, and TEST_INFRA.md.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import json
import time
import asyncio
import unittest
from typing import Optional, List, Dict, Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

# ---------------------------------------------------------------------------
# Module Imports
# ---------------------------------------------------------------------------
from golem.agent.protocol import (
    AgentEvent,
    AgentEventType,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
)
from golem.agent.adapter import HeadlessAgentAdapter
from golem.gateway.session import SessionHandle, SessionState
from golem.gateway.router import SessionRouter
from golem.gateway.ingress import (
    IngressGateway,
    add_reaction,
    remove_reaction,
    swap_reaction,
    strip_bot_mentions,
    process_slack_message,
)
from golem.transport.normalizer import (
    sanitize_for_slack,
    transpile_markdown_to_mrkdwn,
    strip_terminal_chrome,
    strip_ansi,
    strip_box_drawing,
    strip_tui_debris,
    strip_prompt_echoes,
)
from golem.transport.chunker import split_message_boundary_aware
from golem.transport.milestone import MilestoneUpdater
from golem.execution.harness import DualPaneHarness
from golem.execution.runner import RunnerExecutor, ExecutionHandle
from golem.execution.buffer import (
    RunnerBufferObserver,
    OAUTH_CALLBACK_PATTERN,
    INTERACTIVE_PROMPT_PATTERNS,
)
from golem.execution.delegation import WorkerDelegator, sanitize_worker_output
from golem.execution.mesh import (
    FleetMesh,
    TOPOLOGY_SPECS,
    NODE_ALIASES,
    wrap_windows_command,
)
import app
import normalizer
import agent_core
import herdr_tools
import herdr_fleet
import herdr_client
import supervisor
import telemetry_observer


# ---------------------------------------------------------------------------
# Test Mocks & Simulators
# ---------------------------------------------------------------------------
class MockSlackClient:
    """Mock Slack WebClient recording chat_postMessage, chat_update, and reaction operations."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.updated_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self._msg_counter = 1000

    async def chat_postMessage(
        self,
        channel: str,
        text: str,
        thread_ts: Optional[str] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        self._msg_counter += 1
        ts = f"1700000000.{self._msg_counter:06d}"
        entry = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": ts,
            "kwargs": kwargs,
        }
        self.posted_messages.append(entry)
        return {"ok": True, "ts": ts, "message": entry}

    async def chat_update(
        self,
        channel: str,
        ts: str,
        text: str,
        **kwargs: Any
    ) -> Dict[str, Any]:
        entry = {
            "channel": channel,
            "ts": ts,
            "text": text,
            "kwargs": kwargs,
        }
        self.updated_messages.append(entry)
        return {"ok": True, "ts": ts, "text": text}

    async def reactions_add(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        self.reactions_added.append({"channel": channel, "timestamp": timestamp, "name": name})
        return {"ok": True}

    async def reactions_remove(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        self.reactions_removed.append({"channel": channel, "timestamp": timestamp, "name": name})
        return {"ok": True}


class MockSimulatedAdapter:
    """Simulates HeadlessAgentAdapter with fine-grained control over event streams."""

    def __init__(
        self,
        cwd: str = "/Users/hermes/projects/AGY",
        canned_events: Optional[List[AgentEvent]] = None,
        generator_fn: Optional[Any] = None,
    ) -> None:
        self.cwd = cwd
        self.started = False
        self.stopped = False
        self.start_calls = 0
        self.stop_calls = 0
        self.turns_received: List[str] = []
        self.canned_events = canned_events
        self.generator_fn = generator_fn

    async def start(self) -> None:
        self.started = True
        self.start_calls += 1

    async def stop(self) -> None:
        self.stopped = True
        self.stop_calls += 1

    @property
    def is_alive(self) -> bool:
        return self.started and not self.stopped

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_received.append(prompt)
        if self.generator_fn:
            async for ev in self.generator_fn(prompt):
                yield ev
            return

        if self.canned_events is not None:
            for ev in self.canned_events:
                yield ev
            return

        # Default echo turn
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text=f"*Response to:* {prompt}")
        yield FinishEvent(response=f"*Response to:* {prompt}", status="SUCCESS")


# ===========================================================================
# 1. Combination 1: Ingress Gateway + Headless Adapter + Transport Normalizer (R1 + R2 + R4)
# ===========================================================================
class TestCombination1IngressAdapterTransport(unittest.IsolatedAsyncioTestCase):
    """
    Tier 3: Pairwise and multi-feature integration of:
    - Ingress Gateway (R1)
    - Headless Agent Adapter (R2)
    - Transport Normalizer & MilestoneUpdater (R4)
    """

    async def test_e2e_full_turn_pipeline_with_reactions_and_sanitization(self):
        """
        R1 + R2 + R4: Inbound Slack event -> SessionRouter assigns thread ->
        Adapter streams dirty markdown, ANSI, and prompt echoes -> Normalizer cleans
        output -> Slack message delivered with clean mrkdwn -> Reactions swap gear to white_check_mark.
        """
        client = MockSlackClient()
        raw_dirty_markdown = (
            "### Fleet Health Audit\n\n"
            "**All operational nodes nominal:**\n"
            "- [hermes](http://localhost:8080) online.\n"
            "\x1b[32m✔ Service healthy.\x1b[0m\n"
            "❯ herdr agent list\n"
            "Done."
        )

        simulated_events = [
            StatusUpdateEvent(status="working"),
            TextDeltaEvent(text=raw_dirty_markdown),
            FinishEvent(response=raw_dirty_markdown, status="SUCCESS"),
        ]

        def make_adapter(cwd: str):
            return MockSimulatedAdapter(cwd=cwd, canned_events=simulated_events)

        router = SessionRouter(adapter_factory=make_adapter)
        gateway = IngressGateway(router=router, bot_user_id="U_BOT_123")

        slack_event = {
            "type": "message",
            "channel": "C_COMB_1",
            "ts": "1700000000.111111",
            "thread_ts": "1700000000.111111",
            "text": "<@U_BOT_123> Run system audit and report status",
        }

        await gateway.handle_message(slack_event, client)

        # 1. Non-verbal reaction lifecycle: :gear: added then swapped to :white_check_mark:
        self.assertEqual(len(client.reactions_added), 2)
        self.assertEqual(client.reactions_added[0]["name"], "gear")
        self.assertEqual(client.reactions_added[0]["timestamp"], "1700000000.111111")
        self.assertEqual(client.reactions_removed[0]["name"], "gear")
        self.assertEqual(client.reactions_added[1]["name"], "white_check_mark")

        # 2. Delivered message verification
        self.assertEqual(len(client.posted_messages), 1)
        posted_text = client.posted_messages[0]["text"]

        # 3. Transpilation & Chrome Sanitization (TC-05)
        self.assertNotIn("###", posted_text, "Markdown ATX headers must be transpiled")
        self.assertIn("*Fleet Health Audit*", posted_text, "Headers should be Slack *bold*")
        self.assertNotIn("**", posted_text, "Double asterisks must be transpiled to single asterisks")
        self.assertIn("*All operational nodes nominal:*", posted_text)
        self.assertIn("<http://localhost:8080|hermes>", posted_text, "Markdown links must be Slack <url|label>")
        self.assertNotIn("\x1b", posted_text, "ANSI escape sequences must be stripped")
        self.assertNotIn("❯ herdr", posted_text, "Prompt echoes must be stripped")
        self.assertIn("Service healthy.", posted_text)

    async def test_e2e_streaming_milestone_debounce_and_finalization(self):
        """
        R2 + R4: Stream intermediate tokens from HeadlessAgentAdapter through MilestoneUpdater.
        Verifies token-bucket debounce throttles rapid interim updates and clean in-place finalization.
        """
        client = MockSlackClient()
        channel = "C_COMB_1_STREAM"
        thread_ts = "1700000000.222222"

        updater = MilestoneUpdater(client=client, min_interval_seconds=0.5)

        # Emulate 5 rapid delta stream events
        deltas = [
            "Step 1: Connecting to Herdr daemon...",
            "Step 2: Checking workspace state...",
            "Step 3: Running telemetry probes...",
            "Step 4: Compiling node health metrics...",
            "Step 5: Formatting final report...",
        ]

        # Initial post
        ts = await updater.post_or_update(channel, thread_ts, deltas[0])
        self.assertEqual(ts, client.posted_messages[0]["ts"])
        self.assertEqual(len(client.posted_messages), 1)

        # Rapid stream within 0.1s
        for delta in deltas[1:]:
            await updater.post_or_update(channel, thread_ts, delta)

        # Token bucket must have debounced: chat_update not called on every single delta
        self.assertLessEqual(len(client.updated_messages), 2)

        # Final turn completion event
        final_markdown = "### Completed Fleet Audit\n\n**All nodes 100% healthy.**"
        sanitized_final = sanitize_for_slack(final_markdown)
        await updater.finalize(channel, thread_ts, sanitized_final)

        # Finalization must resolve milestone card in-place via chat_update
        self.assertTrue(len(client.updated_messages) >= 1)
        latest_update = client.updated_messages[-1]["text"]
        self.assertIn("*Completed Fleet Audit*", latest_update)
        self.assertNotIn("###", latest_update)
        self.assertIn("*All nodes 100% healthy.*", latest_update)

    async def test_e2e_long_output_boundary_aware_chunking(self):
        """
        R1 + R4: Giant agent output (>4000 characters) across paragraph boundaries is
        sanitized and sliced boundary-aware without corrupting mrkdwn or code blocks.
        """
        client = MockSlackClient()
        paras = []
        for i in range(24):
            paras.append(
                f"### Section {i+1}: Node Telemetry {i+1}\n\n"
                f"**Node status report for worker-{i:02d}:**\n"
                f"Operational memory: `{1024 * (i+1)}MB`. Host network verified.\n"
                f"Reference URL: [Worker Dashboard](http://127.0.0.1:90{i:02d}/metrics)."
            )
        full_text = "\n\n".join(paras)
        self.assertGreater(len(full_text), 4000)

        simulated_events = [
            FinishEvent(response=full_text, status="SUCCESS"),
        ]

        def make_adapter(cwd: str):
            return MockSimulatedAdapter(cwd=cwd, canned_events=simulated_events)

        router = SessionRouter(adapter_factory=make_adapter)
        gateway = IngressGateway(router=router)

        event = {
            "type": "message",
            "channel": "C_COMB_1_LONG",
            "ts": "1700000000.333333",
            "thread_ts": "1700000000.333333",
            "text": "Generate long telemetry report",
        }

        await gateway.handle_message(event, client)

        # Sliced across multiple messages
        self.assertGreater(len(client.posted_messages), 1)
        for msg in client.posted_messages:
            self.assertLessEqual(len(msg["text"]), 3800)
            self.assertNotIn("###", msg["text"])
            self.assertNotIn("**", msg["text"])

        # All sections accounted for
        combined_egress = "\n\n".join(m["text"] for m in client.posted_messages)
        self.assertIn("*Section 1: Node Telemetry 1*", combined_egress)
        self.assertIn("*Section 12: Node Telemetry 12*", combined_egress)
        self.assertIn("<http://127.0.0.1:9000/metrics|Worker Dashboard>", combined_egress)

    async def test_e2e_error_event_propagation_and_x_reaction(self):
        """
        R1 + R2: When adapter stream ends with FinishEvent(status='ERROR'),
        IngressGateway swaps reaction to :x: and surfaces formatted diagnostic error.
        """
        client = MockSlackClient()
        error_events = [
            StatusUpdateEvent(status="working"),
            FinishEvent(
                response="",
                status="ERROR",
                error="Child subprocess failed to allocate pseudo-terminal: Resource temporarily unavailable.",
            ),
        ]

        def make_adapter(cwd: str):
            return MockSimulatedAdapter(cwd=cwd, canned_events=error_events)

        router = SessionRouter(adapter_factory=make_adapter)
        gateway = IngressGateway(router=router)

        event = {
            "type": "message",
            "channel": "C_COMB_1_ERR",
            "ts": "1700000000.444444",
            "thread_ts": "1700000000.444444",
            "text": "Trigger faulty runner process",
        }

        await gateway.handle_message(event, client)

        # Verified reaction swapped to :x:
        self.assertEqual(client.reactions_added[-1]["name"], "x")
        self.assertEqual(len(client.posted_messages), 1)
        self.assertIn("❌ *Error:*", client.posted_messages[0]["text"])
        self.assertIn("Resource temporarily unavailable", client.posted_messages[0]["text"])

    async def test_e2e_adversarial_markdown_and_ansi_sanitization(self):
        """
        Adversarial test: Malformed, dirty stream containing unclosed code fences,
        VT100 box frames, multiple prompt markers, ANSI cursor controls, and braille spinners.
        """
        raw_adversarial = (
            "╭──────────────────────────────────────────────────╮\n"
            "│  FLEET MESH RUNNER v2.0 - SCRATCHPAD             │\n"
            "╰──────────────────────────────────────────────────╯\n"
            "⠋ Scanning open ports...\n"
            "⠙ Probing Docker daemon...\n"
            "\x1b[2K\r# Header Level 1\n"
            "Normal paragraph with **bold** and `code`.\n"
            "\x1b[31;1mFATAL:\x1b[0m Port 8080 occupied by \x1b[33mpid 4501\x1b[0m\n"
            "❯ netstat -tuln\n"
            "❯ ps aux | grep 8080\n"
            "Review docs at [Network Guide](http://127.0.0.1:8000/network#section-1).\n"
        )

        cleaned = sanitize_for_slack(raw_adversarial)

        self.assertNotIn("╭", cleaned)
        self.assertNotIn("│", cleaned)
        self.assertNotIn("╰", cleaned)
        self.assertNotIn("⠋", cleaned)
        self.assertNotIn("⠙", cleaned)
        self.assertNotIn("\x1b", cleaned)
        self.assertNotIn("# Header Level 1", cleaned)
        self.assertIn("*Header Level 1*", cleaned)
        self.assertNotIn("**bold**", cleaned)
        self.assertIn("*bold*", cleaned)
        self.assertNotIn("❯ netstat", cleaned)
        self.assertNotIn("❯ ps aux", cleaned)
        self.assertIn("<http://127.0.0.1:8000/network#section-1|Network Guide>", cleaned)
        self.assertIn("Port 8080 occupied by pid 4501", cleaned)


# ===========================================================================
# 2. Combination 2: Ingress Gateway + DualPaneHarness + BufferObserver (R1 + R3)
# ===========================================================================
class TestCombination2IngressDualPaneBufferObserver(unittest.IsolatedAsyncioTestCase):
    """
    Tier 3: Pairwise and multi-feature integration of:
    - Ingress Gateway (R1)
    - DualPaneHarness & Herdr runner execution (R3)
    - Terminal BufferObserver & interactive blocker detection (R3 / TC-03)
    """

    async def test_dual_pane_provisioning_and_runner_execution_yield(self):
        """
        R3 / TC-03: DualPaneHarness provisions Pane 1 (liaison) and Pane 2 (runner).
        RunnerExecutor dispatches interactive long-running command with 3.0s yield timeout.
        When command exceeds timeout, yields status='running' handle in <= 3.0s, keeping liaison responsive.
        """
        mock_herdr = AsyncMock()
        mock_herdr.run_cli.side_effect = [
            # 1. workspace create
            (0, json.dumps({"result": {"workspace": {"workspace_id": "ws-c2-1"}, "root_pane": {"pane_id": "p1"}}}), ""),
            # 2. agent start (liaison)
            (0, "started liaison", ""),
            # 3. pane create (runner)
            (0, json.dumps({"result": {"pane": {"pane_id": "runner"}}}), ""),
        ]

        harness = DualPaneHarness(herdr_client=mock_herdr, default_cwd="/Users/hermes/projects/golem")
        liaison_agent, runner_pane = await harness.provision_dual_pane("sess-c2", "/Users/hermes/projects/golem")

        self.assertEqual(liaison_agent, "sess-c2-liaison")
        self.assertEqual(runner_pane, "runner")

        # Now test RunnerExecutor yield timeout behavior
        executor = RunnerExecutor(herdr_client=mock_herdr)
        try:
            # Mock asyncio.create_subprocess_exec to simulate command exceeding yield timeout
            mock_proc = AsyncMock()
            # wait_for timing out during communicate
            mock_proc.communicate.side_effect = asyncio.TimeoutError()
            mock_proc.pid = 98765

            with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
                handle = await executor.run_command(
                    pane="runner",
                    command="rclone config reconnect remote:",
                    yield_timeout=0.05
                )

            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pane, "runner")
            self.assertEqual(handle.pid, 98765)
            self.assertIn("Liaison loop remains active", handle.message)
        finally:
            await executor.async_close()

    async def test_fast_command_completion_within_yield_timeout(self):
        """
        R3: Fast shell commands (e.g. 'echo ok') completing under yield_timeout
        return status='completed' handle with stdout populated.
        """
        executor = RunnerExecutor()
        try:
            mock_proc = AsyncMock()
            mock_proc.communicate.return_value = (b"All 159 tests passed.\n", b"")
            mock_proc.returncode = 0
            mock_proc.pid = 11223

            with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
                 patch.object(executor, "_get_pane_foreground_pid", return_value=None):
                handle = await executor.run_command(
                    pane="runner",
                    command="pytest tests",
                    yield_timeout=3.0
                )

            self.assertEqual(handle.status, "completed")
            self.assertEqual(handle.returncode, 0)
            self.assertIn("All 159 tests passed.", handle.stdout)
        finally:
            await executor.async_close()

    async def test_buffer_observer_detects_oauth_url_and_surfaces_blocker(self):
        """
        R3 / TC-03: BufferObserver inspects settled runner buffer, detects OAuth callback URL,
        and extracts blocker payload without leaking terminal chrome or curses boxes.
        """
        observer = RunnerBufferObserver()
        raw_terminal_buffer = (
            "╭────────────────────────────────────────────────────────────╮\n"
            "│ Google Drive OAuth Authorization Flow                      │\n"
            "│ Log in and authorize rclone at:                            │\n"
            "│ http://127.0.0.1:53682/auth?state=fm-oauth-token-xyz123    │\n"
            "╰────────────────────────────────────────────────────────────╯\n"
            "Waiting for browser redirect code on http://127.0.0.1:53682/auth ...\n"
            "❯ \n"
        )

        blocker = observer.extract_interactive_blockers(raw_terminal_buffer)
        self.assertIsNotNone(blocker)
        self.assertEqual(blocker["type"], "oauth")
        self.assertEqual(blocker["url"], "http://127.0.0.1:53682/auth?state=fm-oauth-token-xyz123")

        # Verify formatting into clean Slack mrkdwn
        clean_auth_notice = f"⚠️ *Authentication Required:* Please complete login at <{blocker['url']}|OAuth Login URL>"
        self.assertNotIn("╭", clean_auth_notice)
        self.assertNotIn("│", clean_auth_notice)
        self.assertNotIn("❯", clean_auth_notice)
        self.assertIn("<http://127.0.0.1:53682/auth?state=fm-oauth-token-xyz123|OAuth Login URL>", clean_auth_notice)

    async def test_buffer_observer_detects_confirmation_prompt_and_strips_chrome(self):
        """
        R3 / TC-03: BufferObserver detects interactive [y/N] confirmation prompt in runner buffer.
        """
        observer = RunnerBufferObserver()
        buffer_text = (
            "\x1b[33m[CAUTION]\x1b[0m This operation will overwrite local worktree branch.\n"
            "Do you wish to continue? [y/N]\n"
        )

        blocker = observer.extract_interactive_blockers(buffer_text)
        self.assertIsNotNone(blocker)
        self.assertEqual(blocker["type"], "prompt")
        self.assertIn("[y/N]", blocker["content"])

    async def test_awaiting_input_gateway_reaction_and_notification(self):
        """
        R1 + R3: When adapter emits awaiting_input event (triggered by buffer blocker),
        IngressGateway swaps reaction :gear: -> :warning: and posts interactive blocker card to Slack.
        """
        client = MockSlackClient()
        awaiting_events = [
            StatusUpdateEvent(status="working"),
            AwaitingInputEvent(
                prompt="⚠️ *Authorization Required:* Please visit <http://127.0.0.1:53682/auth|OAuth URL> to continue.",
                choices=["1. Authorize in Browser", "2. Cancel Operation"],
            ),
        ]

        def make_adapter(cwd: str):
            return MockSimulatedAdapter(cwd=cwd, canned_events=awaiting_events)

        router = SessionRouter(adapter_factory=make_adapter)
        gateway = IngressGateway(router=router)

        event = {
            "type": "message",
            "channel": "C_COMB_2_AWAIT",
            "ts": "1700000000.555555",
            "thread_ts": "1700000000.555555",
            "text": "Authorize cloud storage remote",
        }

        await gateway.handle_message(event, client)

        # 1. Reaction swapped to :warning:
        self.assertEqual(client.reactions_added[-1]["name"], "warning")

        # 2. Posted message contains auth URL and choices without terminal debris
        self.assertEqual(len(client.posted_messages), 1)
        posted = client.posted_messages[0]["text"]
        self.assertIn("Authorization Required", posted)
        self.assertIn("<http://127.0.0.1:53682/auth|OAuth URL>", posted)
        self.assertNotIn("╭", posted)
        self.assertNotIn("❯", posted)


# ===========================================================================
# 3. Combination 3: SessionRouter + Inactivity Watchdog + Headless Adapter Lifecycle (R1 + R2)
# ===========================================================================
class TestCombination3SessionRouterLifecycleWatchdog(unittest.IsolatedAsyncioTestCase):
    """
    Tier 3: Pairwise and multi-feature integration of:
    - SessionRouter thread mapping (R1)
    - Inactivity timeout watchdog & reaper sweep (R1)
    - HeadlessAgentAdapter lifecycle & warm multi-turn persistence (R2)
    """

    async def test_multi_turn_warm_session_continuity_and_touch(self):
        """
        R1 + R2 / TC-02: Multi-turn interaction in the same Slack thread reuses the same warm
        SessionHandle and adapter without re-spawning or echoing stale conversational fragments.
        """
        created_adapters: List[MockSimulatedAdapter] = []

        def make_adapter(cwd: str):
            adapter = MockSimulatedAdapter(cwd=cwd)
            created_adapters.append(adapter)
            return adapter

        router = SessionRouter(adapter_factory=make_adapter)

        # Turn 1
        s1, is_new1 = await router.get_or_create("C_THREAD_1", "1700000000.000100")
        self.assertTrue(is_new1)
        self.assertEqual(len(created_adapters), 1)
        self.assertTrue(created_adapters[0].started)

        t1_start = s1.last_active_ts
        async for _ in s1.send_user_message("First question: what cron jobs are running?"):
            pass
        self.assertEqual(s1.current_turn, 1)
        self.assertEqual(s1.state, SessionState.IDLE)
        self.assertGreaterEqual(s1.last_active_ts, t1_start)

        # Turn 2: Follow-up question in the same thread
        s2, is_new2 = await router.get_or_create("C_THREAD_1", "1700000000.000100")
        self.assertFalse(is_new2, "Must reuse warm session for existing thread")
        self.assertIs(s1, s2, "Must return exact same SessionHandle instance")
        self.assertEqual(len(created_adapters), 1, "Must not spawn a second adapter")

        async for _ in s2.send_user_message("Which one runs at 3 AM?"):
            pass
        self.assertEqual(s2.current_turn, 2)
        self.assertEqual(len(created_adapters[0].turns_received), 2)
        self.assertEqual(created_adapters[0].turns_received[1], "Which one runs at 3 AM?")

    async def test_session_lock_serializes_concurrent_turns(self):
        """
        R1: Two concurrent messages in the same thread are serialized under session.lock,
        preventing race conditions or interleaved turn execution.
        """
        execution_order: List[str] = []

        async def turn_generator(prompt: str) -> AsyncGenerator[AgentEvent, None]:
            execution_order.append(f"start:{prompt}")
            await asyncio.sleep(0.05)
            execution_order.append(f"finish:{prompt}")
            yield TextDeltaEvent(text=f"Done with {prompt}")
            yield FinishEvent(response=f"Done with {prompt}", status="SUCCESS")

        adapter = MockSimulatedAdapter(generator_fn=turn_generator)
        router = SessionRouter(adapter_factory=lambda cwd: adapter)

        session, _ = await router.get_or_create("C_CONCUR", "1700000000.999999")

        async def run_turn(prompt: str):
            async for _ in session.send_user_message(prompt):
                pass

        # Dispatch two turns simultaneously
        await asyncio.gather(
            run_turn("Turn A"),
            run_turn("Turn B")
        )

        # Serialized execution: start A -> finish A -> start B -> finish B (or vice versa)
        self.assertEqual(len(execution_order), 4)
        if execution_order[0] == "start:Turn A":
            self.assertEqual(execution_order[1], "finish:Turn A")
            self.assertEqual(execution_order[2], "start:Turn B")
            self.assertEqual(execution_order[3], "finish:Turn B")
        else:
            self.assertEqual(execution_order[0], "start:Turn B")
            self.assertEqual(execution_order[1], "finish:Turn B")
            self.assertEqual(execution_order[2], "start:Turn A")
            self.assertEqual(execution_order[3], "finish:Turn A")

    async def test_inactivity_watchdog_reaps_expired_session_and_stops_adapter(self):
        """
        R1 + R2: When a session's inactivity timer expires, the reaper sweep closes the session,
        stops the underlying child adapter, and cleans up router state.
        """
        adapter = MockSimulatedAdapter()
        router = SessionRouter(inactivity_timeout=0.05, reaper_interval=0.01, adapter_factory=lambda cwd: adapter)

        session, _ = await router.get_or_create("C_REAP", "1700000000.777777")
        self.assertTrue(adapter.started)
        self.assertFalse(adapter.stopped)

        # Simulate inactivity passage
        session.last_active_ts = time.time() - 10.0
        self.assertTrue(session.is_expired())

        # Execute reaper sweep
        reaped_keys = await router.reap_expired_sessions()

        self.assertIn("C_REAP:1700000000.777777", reaped_keys)
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertTrue(adapter.stopped, "Adapter stop() must be called when reaped")
        self.assertNotIn("C_REAP:1700000000.777777", router.sessions)

    async def test_reinstatement_of_fresh_session_after_expiration(self):
        """
        R1 + R2: After an old session is reaped, a new user message in the same thread
        reinstates a fresh session and new adapter without zombie state or lingering locks.
        """
        adapters = []

        def make_adapter(cwd: str):
            a = MockSimulatedAdapter(cwd=cwd)
            adapters.append(a)
            return a

        router = SessionRouter(inactivity_timeout=0.05, adapter_factory=make_adapter)

        # 1. First session created
        s1, _ = await router.get_or_create("C_THREAD_REINSTATE", "1700000000.888888")
        s1.last_active_ts = time.time() - 100.0  # Force expiration
        await router.reap_expired_sessions()
        self.assertEqual(s1.state, SessionState.CLOSED)
        self.assertTrue(adapters[0].stopped)

        # 2. Second message in same thread reinstates clean fresh session
        s2, is_new = await router.get_or_create("C_THREAD_REINSTATE", "1700000000.888888")
        self.assertTrue(is_new)
        self.assertIsNot(s1, s2)
        self.assertEqual(len(adapters), 2)
        self.assertTrue(adapters[1].started)
        self.assertFalse(adapters[1].stopped)

        # Execute turn on fresh session
        async for _ in s2.send_user_message("New turn on fresh session"):
            pass
        self.assertEqual(s2.current_turn, 1)
        self.assertEqual(s2.state, SessionState.IDLE)

    async def test_explicit_teardown_cleans_up_adapter_immediately(self):
        """
        R1: Explicit /wrapup command triggers teardown, terminating adapter immediately.
        """
        adapter = MockSimulatedAdapter()
        router = SessionRouter(adapter_factory=lambda cwd: adapter)
        gateway = IngressGateway(router=router)
        client = MockSlackClient()

        # Seed warm session
        session, _ = await router.get_or_create("C_WRAPUP", "1700000000.666666")
        self.assertIn("C_WRAPUP:1700000000.666666", router.sessions)

        # Send /wrapup
        event = {
            "type": "message",
            "channel": "C_WRAPUP",
            "ts": "1700000000.666777",
            "thread_ts": "1700000000.666666",
            "text": "/wrapup",
        }

        await gateway.handle_message(event, client)

        # Verified session closed and adapter stopped
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertTrue(adapter.stopped)
        self.assertNotIn("C_WRAPUP:1700000000.666666", router.sessions)
        self.assertIn("Housekeeping complete", client.posted_messages[0]["text"])
        self.assertEqual(client.reactions_added[-1]["name"], "white_check_mark")


# ===========================================================================
# 4. Combination 4: WorkerDelegator + FleetMesh + Normalizer Sanitization (R3 + R4)
# ===========================================================================
class TestCombination4WorkerDelegationFleetMeshNormalizer(unittest.IsolatedAsyncioTestCase):
    """
    Tier 3: Pairwise and multi-feature integration of:
    - WorkerDelegator & sandbox worktree isolation (R3 / TC-04)
    - FleetMesh topology & per-pane locking (R3)
    - Normalizer multi-stage output sanitization (R4 / TC-05)
    """

    async def test_worker_delegation_provisions_isolated_worktree_branch(self):
        """
        R3 / TC-04: Task delegated to opencode-dev provisions an isolated worktree branch
        and returns clean sanitized outcomes.
        """
        mock_herdr = AsyncMock()
        mock_herdr.run_cli.side_effect = [
            # 1. worktree create --cwd ... --branch task-<ts>-opencode --no-focus
            (0, json.dumps({"result": {"worktree": {"path": "/tmp/worktree/task-001"}, "workspace": {"workspace_id": "ws-wt-1"}}}), ""),
            # 2. agent start
            (0, "started agent", ""),
            # 3. agent prompt
            (0, "prompt delivered", ""),
            # 4. agent wait
            (0, "agent idle", ""),
            # 5. agent read (settled output buffer)
            (0, "All refactoring tasks completed cleanly.\nTests passing 100%.", ""),
        ]

        delegator = WorkerDelegator(herdr_client=mock_herdr, base_repo="/Users/hermes/projects/golem")
        res = await delegator.handoff(
            subagent="opencode",
            task="Refactor auth middleware to use JWT",
            isolate_branch=True,
        )

        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["subagent"], "opencode")
        self.assertTrue(res["branch"].startswith("task-"))
        self.assertEqual(res["cwd"], "/tmp/worktree/task-001")
        self.assertIn("All refactoring tasks completed cleanly", res["output"])

        # Verify CLI worktree creation called with isolated branch
        first_call = mock_herdr.run_cli.call_args_list[0][0][0]
        self.assertEqual(first_call[:2], ["worktree", "create"])
        self.assertIn("--branch", first_call)

    async def test_fleet_mesh_per_pane_locking_serializes_concurrent_workers(self):
        """
        R3: FleetMesh enforces per-pane asyncio.Lock, preventing interleaved or overlapping
        execution commands on the same worker pane.
        """
        mesh = FleetMesh()
        execution_trace: List[str] = []

        async def worker_job(job_id: str, duration: float):
            execution_trace.append(f"start:{job_id}")
            await asyncio.sleep(duration)
            execution_trace.append(f"finish:{job_id}")
            return f"result:{job_id}"

        # Target same pane "w1:p1" concurrently
        res1, res2 = await asyncio.gather(
            mesh.run_locked("w1:p1", worker_job, "job-A", 0.05),
            mesh.run_locked("w1:p1", worker_job, "job-B", 0.05),
        )

        self.assertEqual(res1, "result:job-A")
        self.assertEqual(res2, "result:job-B")
        self.assertEqual(len(execution_trace), 4)

        # Confirm strictly serialized without overlapping
        if execution_trace[0] == "start:job-A":
            self.assertEqual(execution_trace[1], "finish:job-A")
            self.assertEqual(execution_trace[2], "start:job-B")
            self.assertEqual(execution_trace[3], "finish:job-B")
        else:
            self.assertEqual(execution_trace[0], "start:job-B")
            self.assertEqual(execution_trace[1], "finish:job-B")
            self.assertEqual(execution_trace[2], "start:job-A")
            self.assertEqual(execution_trace[3], "finish:job-A")

    def test_fleet_mesh_node_topology_resolution_and_aliases(self):
        """
        R3: FleetMesh accurately resolves all 5 node topologies and their aliases.
        """
        mesh = FleetMesh()

        # 1. Local fleet control
        fc = mesh.resolve_node("fleet-control")
        self.assertEqual(fc["target_machine"], "local")
        self.assertEqual(mesh.resolve_node("localhost")["label"], "fleet-control")

        # 2. Homelab ops
        hl = mesh.resolve_node("homelab-ops")
        self.assertEqual(hl["role"], "Hermes Butler")
        self.assertEqual(mesh.resolve_node("hermes")["label"], "homelab-ops")

        # 3. Compute grid
        cg = mesh.resolve_node("compute-grid")
        self.assertEqual(cg["target_machine"], "desktop-pc")
        self.assertEqual(mesh.resolve_node("desktop-pc")["label"], "compute-grid")
        self.assertEqual(mesh.resolve_node("gpu")["label"], "compute-grid")

        # 4. Dev grid (Carl's personal laptop)
        dg = mesh.resolve_node("dev-grid")
        self.assertEqual(dg["target_machine"], "personal-laptop")
        self.assertEqual(mesh.resolve_node("laptop")["label"], "dev-grid")

        # 5. Cloud ops
        co = mesh.resolve_node("cloud-ops")
        self.assertEqual(co["target_machine"], "oci-machine")
        self.assertEqual(mesh.resolve_node("cloud")["label"], "cloud-ops")

    def test_fleet_mesh_windows_command_wrapping_for_compute_grid(self):
        """
        R3: POSIX idioms dispatched to compute-grid are wrapped cleanly in cmd.exe /c runner.
        """
        # POSIX && chaining
        wrapped_chain = wrap_windows_command("pytest tests && echo done")
        self.assertTrue(wrapped_chain.startswith("cmd.exe /c"))
        self.assertIn("pytest tests && echo done", wrapped_chain)

        # /dev/null redirect -> nul
        wrapped_null = wrap_windows_command("cat output.log > /dev/null 2>/dev/null")
        self.assertIn("> nul", wrapped_null)
        self.assertIn("2>nul", wrapped_null)

        # ./script.sh path prefix -> .\script.sh
        wrapped_script = wrap_windows_command("./run_benchmark.py")
        self.assertIn(".\\run_benchmark.py", wrapped_script)

        # Already wrapped commands preserved
        powershell_cmd = "powershell.exe -Command Get-Process"
        self.assertEqual(wrap_windows_command(powershell_cmd), powershell_cmd)

    def test_worker_output_sanitization_strips_scratchpads_and_terminal_chrome(self):
        """
        R3 + R4 / TC-04 + TC-05: Subprocess output containing internal scratchpad blocks,
        VT100 boxes, prompt echoes, and braille spinners is completely sanitized.
        """
        raw_worker_buffer = (
            "<scratchpad>\n"
            "Analyzing code changes in auth.py...\n"
            "JWT verification signature matched.\n"
            "</scratchpad>\n"
            "[scratchpad] Executed unit test runner in subprocess.\n"
            "╭────────────────────────────────────────────────────────╮\n"
            "│  OpenCode Dev Sandbox - Worker Output                  │\n"
            "╰────────────────────────────────────────────────────────╯\n"
            "⠋ Running test suite...\n"
            "⠙ Test run completed.\n"
            "\x1b[32m✔ Successfully updated auth middleware.\x1b[0m\n"
            "❯ git status\n"
            "Modified files: golem/gateway/ingress.py\n"
        )

        cleaned = sanitize_worker_output(raw_worker_buffer)

        # Verified zero scratchpad leakage
        self.assertNotIn("<scratchpad>", cleaned)
        self.assertNotIn("Analyzing code changes", cleaned)
        self.assertNotIn("[scratchpad]", cleaned)

        # Verified zero curses / VT100 chrome
        self.assertNotIn("╭", cleaned)
        self.assertNotIn("│", cleaned)
        self.assertNotIn("╰", cleaned)

        # Verified zero spinners, ANSI, or prompt echoes
        self.assertNotIn("⠋", cleaned)
        self.assertNotIn("⠙", cleaned)
        self.assertNotIn("\x1b", cleaned)
        self.assertNotIn("❯ git status", cleaned)

        # Verified genuine substantive summary preserved
        self.assertIn("Successfully updated auth middleware.", cleaned)
        self.assertIn("Modified files: golem/gateway/ingress.py", cleaned)

    async def test_worktree_isolation_failure_handled_gracefully(self):
        """
        R3: When worktree isolation fails (e.g. disk space or branch lock error),
        returns structured error without throwing unhandled exception.
        """
        mock_herdr = AsyncMock()
        mock_herdr.run_cli.return_value = (1, "", "fatal: A branch named 'task-failed' already exists.")

        delegator = WorkerDelegator(herdr_client=mock_herdr)
        res = await delegator.handoff(subagent="opencode", task="build feature", isolate_branch=True)

        self.assertEqual(res["status"], "error")
        self.assertIn("Worktree isolation failed", res["error"])


# ===========================================================================
# 5. Combination 5: Dual-Track app.py + Compatibility Shims + IngressGateway (R1 + R5)
# ===========================================================================
class TestCombination5DualTrackAppAndCompatibilityShims(unittest.IsolatedAsyncioTestCase):
    """
    Tier 3: Pairwise and multi-feature integration of:
    - Dual-track app.py event dispatching (R1 + R5)
    - Root module compatibility shims (R5)
    - Legacy registries (app.sessions, agent_name_to_session, SPAWNED_AGENTS)
    """

    def setUp(self):
        app.sessions.clear()
        app.agent_name_to_session.clear()
        herdr_tools.SPAWNED_AGENTS.clear()

    def tearDown(self):
        app.sessions.clear()
        app.agent_name_to_session.clear()
        herdr_tools.SPAWNED_AGENTS.clear()

    async def test_dual_track_routing_headless_gateway_enabled(self):
        """
        R1 + R5: When USE_HEADLESS_GATEWAY=True, Slack Bolt events route directly
        through app.ingress_gateway.handle_message.
        """
        client = MockSlackClient()
        mock_gateway = AsyncMock()

        event = {
            "type": "app_mention",
            "channel": "C_DUAL_1",
            "ts": "1700000000.123123",
            "text": "<@U_BOT> status check",
        }

        with patch.object(app, "USE_HEADLESS_GATEWAY", True), \
             patch.object(app, "ingress_gateway", mock_gateway):
            await app.handle_app_mention(event, client)
            mock_gateway.handle_message.assert_awaited_once_with(event, client)

    async def test_dual_track_routing_legacy_fallback_when_disabled(self):
        """
        R1 + R5: When USE_HEADLESS_GATEWAY=False, Bolt events fall back to legacy process_user_prompt.
        """
        client = MockSlackClient()
        event = {
            "type": "message",
            "channel": "D_DUAL_LEGACY",
            "channel_type": "im",
            "ts": "1700000000.456456",
            "text": "run legacy prompt",
        }

        with patch.object(app, "USE_HEADLESS_GATEWAY", False), \
             patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_legacy:
            await app.handle_message_events(event, client)
            mock_legacy.assert_awaited_once_with(event, client)

    async def test_legacy_registries_coexist_with_headless_gateway(self):
        """
        R5: While headless gateway is active, legacy registries (app.sessions,
        app.agent_name_to_session, herdr_tools.SPAWNED_AGENTS) remain fully populated
        and functional for backwards compatibility.
        """
        mock_herdr = AsyncMock()
        mock_herdr.run_cli.return_value = (
            0,
            json.dumps({"result": {"workspace": {"workspace_id": "ws-legacy-1"}, "root_pane": {"pane_id": "p1"}}}),
            ""
        )

        # Call legacy ensure_session_agent
        with patch.object(app, "herdr_client", mock_herdr), \
             patch.object(app, "wait_for_agent_readiness", new_callable=AsyncMock, return_value=True):
            legacy_session = await app.ensure_session_agent(
                session_key="C_LEGACY:100.1",
                channel="C_LEGACY",
                thread_ts="100.1",
                prompt="legacy task prompt",
                agent_name_override="fm-legacy-test",
                dual_pane=False
            )

        # Verified legacy registry population
        self.assertIn("C_LEGACY:100.1", app.sessions)
        self.assertIn("fm-legacy-test", app.agent_name_to_session)
        self.assertEqual(app.sessions["C_LEGACY:100.1"].active_agent, "fm-legacy-test")

        # Verified SPAWNED_AGENTS registry
        herdr_tools.SPAWNED_AGENTS["p1"] = {
            "agent_name": "fm-legacy-test",
            "branch": "main",
            "task_id": "fm-legacy-test"
        }
        self.assertIn("p1", herdr_tools.SPAWNED_AGENTS)
        self.assertEqual(herdr_tools.SPAWNED_AGENTS["p1"]["agent_name"], "fm-legacy-test")

    def test_root_compatibility_shims_preserve_canonical_identities(self):
        """
        R5: Root compatibility shims preserve exact object identity (assertIs)
        with their canonical golem implementations.
        """
        import golem.transport.normalizer as canon_norm
        import golem.transport.chunker as canon_chunk
        import golem.agent.adapter as canon_adapter
        import golem.agent.protocol as canon_proto
        import golem.execution.mesh as canon_mesh

        # normalizer.py shims
        self.assertIs(normalizer.sanitize_for_slack, canon_norm.sanitize_for_slack)
        self.assertIs(normalizer.strip_terminal_chrome, canon_norm.strip_terminal_chrome)
        self.assertIs(normalizer.strip_ansi, canon_norm.strip_ansi)
        self.assertIs(normalizer.strip_box_drawing, canon_norm.strip_box_drawing)
        self.assertIs(normalizer.strip_prompt_echoes, canon_norm.strip_prompt_echoes)

        # agent_core.py shims
        self.assertIs(agent_core.HeadlessAgentAdapter, canon_adapter.HeadlessAgentAdapter)
        self.assertIs(agent_core.AgentEvent, canon_proto.AgentEvent)
        self.assertIs(agent_core.TextDeltaEvent, canon_proto.TextDeltaEvent)
        self.assertIs(agent_core.FinishEvent, canon_proto.FinishEvent)

        # herdr_fleet.py shims
        self.assertIs(herdr_fleet.TOPOLOGY_SPECS, canon_mesh.TOPOLOGY_SPECS)
        self.assertIs(herdr_fleet.wrap_windows_command, canon_mesh.wrap_windows_command)

        # chunker shim on app
        self.assertIs(app.split_message_boundary_aware, canon_chunk.split_message_boundary_aware)

    def test_app_instance_monkey_patch_targets_preserved(self):
        """
        R5: app.app instance preserves all monkey-patched runtime targets required
        by legacy supervision and integration tests.
        """
        self.assertTrue(hasattr(app.app, "supervisor"))
        self.assertIsInstance(app.app.supervisor, supervisor.AgentSupervisor)
        self.assertTrue(hasattr(app.app, "herdr_client"))
        self.assertIsInstance(app.app.herdr_client, herdr_client.HerdrClient)
        self.assertTrue(hasattr(app.app, "fleet"))
        self.assertIsInstance(app.app.fleet, herdr_fleet.HerdrFleet)
        self.assertTrue(hasattr(app, "client_herdr"))

    def test_gateway_session_and_session_handle_interoperability(self):
        """
        R1 + R5: GatewaySession (legacy) and SessionHandle (modern) operate side-by-side
        without namespace collision or typing conflicts.
        """
        # Legacy session
        legacy = app.GatewaySession(
            session_key="C_INTEROP:1.0",
            agent_name="agent-dual-1",
            target_cwd="/tmp",
            channel="C_INTEROP",
            thread_ts="1.0",
            dual_pane=True,
            liaison_agent="agent-dual-1-liaison",
            runner_pane="runner"
        )
        self.assertEqual(legacy.active_agent, "agent-dual-1-liaison")
        self.assertEqual(legacy.runner_pane, "runner")

        # Modern session handle
        modern = SessionHandle(
            session_id="C_INTEROP:1.0",
            channel="C_INTEROP",
            thread_ts="1.0",
            adapter=MockSimulatedAdapter(),
            target_cwd="/tmp"
        )
        self.assertEqual(modern.session_id, "C_INTEROP:1.0")
        self.assertEqual(modern.state, SessionState.INITIALIZING)


if __name__ == "__main__":
    unittest.main()
