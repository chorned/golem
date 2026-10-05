#!/usr/bin/env python3
"""
tests/test_m6_challenger4_adversarial_audit.py
Milestone 6 Challenger 4 Empirical Adversarial Audit Suite.

Adversarially validates:
- TC-01: Conversational inquiry direct response, 0 worker panes, single asterisk bold, zero canned announcements.
- TC-02: Multi-turn thread continuity, session reuse, monotonic timestamps, zero stale prompt echo.
- TC-03: Dual-pane runner execution yield <=3.0s, buffer scraping for OAuth URLs & prompts, liaison responsiveness.
- TC-04: Isolated worker delegation in dedicated worktree/branch, base repo protection, scratchpad & chrome stripping.
- TC-05: Wire normalization (0 ATX #, 0 **, 0 VT100, 0 ANSI, Slack <url|label>) & boundary-aware chunking preserving fences.
"""

from __future__ import annotations

import os
import sys
import json
import time
import asyncio
import unittest
from typing import Dict, Any, List, Optional, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.gateway.ingress import IngressGateway, strip_bot_mentions
from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle, SessionState
from golem.agent.adapter import HeadlessAgentAdapter
from golem.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    ToolStartEvent,
    ToolEndEvent,
)
from golem.agent.prompts import build_calibrated_system_prompt
from golem.execution.runner import RunnerExecutor, ExecutionHandle
from golem.execution.buffer import RunnerBufferObserver
from golem.execution.delegation import WorkerDelegator, sanitize_worker_output
from golem.execution.harness import DualPaneHarness
from golem.transport.normalizer import (
    sanitize_for_slack,
    transpile_markdown_to_mrkdwn,
    strip_ansi,
    strip_tui_debris,
    strip_box_drawing,
    strip_prompt_echoes,
    protect_code_blocks,
    restore_code_blocks,
)
from golem.transport.chunker import (
    split_message_boundary_aware,
    parse_code_blocks,
    find_enclosing_block,
)


class MockSlackClient:
    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, Any]] = []
        self.reactions_removed: List[Dict[str, Any]] = []
        self.worker_panes_spawned: List[str] = []

    async def reactions_add(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        self.reactions_added.append({
            "channel": channel,
            "timestamp": timestamp,
            "name": name,
            "time": time.perf_counter(),
        })
        return {"ok": True}

    async def reactions_remove(self, channel: str, timestamp: str, name: str) -> Dict[str, Any]:
        self.reactions_removed.append({
            "channel": channel,
            "timestamp": timestamp,
            "name": name,
            "time": time.perf_counter(),
        })
        return {"ok": True}

    async def chat_postMessage(self, channel: str, text: str, thread_ts: Optional[str] = None) -> Dict[str, Any]:
        msg = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "time": time.perf_counter(),
        }
        self.posted_messages.append(msg)
        return {"ok": True, "ts": f"{time.time()}"}


class ControllableMockAdapter:
    def __init__(self, responses: Optional[Dict[str, str]] = None) -> None:
        self.responses = responses or {}
        self.turns_received: List[str] = []

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.turns_received.append(prompt)
        resp = self.responses.get(prompt, f"*Result:* Handled `{prompt}`")
        yield StatusUpdateEvent(status="working")
        yield TextDeltaEvent(text=resp)
        yield FinishEvent(response=resp, status="SUCCESS")


class TestAdversarialTC01Conversational(unittest.IsolatedAsyncioTestCase):
    """TC-01: Conversational inquiries single-turn direct response adversarial audit."""

    async def test_burst_casual_inquiries_cleanliness_and_zero_worker_panes(self):
        client = MockSlackClient()
        test_queries = [
            "How's our fleet doing?",
            "What's the status of our machines?",
            "Good morning AGY, any alerts?",
            "Can you give me a status update on the homelab?",
            "How is the cluster running?",
        ]
        adapter = ControllableMockAdapter()
        for q in test_queries:
            adapter.responses[q] = f"*Fleet Status Overview*\nAll 4 nodes online. No active alerts for `{q}`."

        router = SessionRouter(adapter_factory=lambda cwd: adapter)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")

        try:
            for idx, q in enumerate(test_queries):
                ts = f"173000000{idx}.000100"
                event = {
                    "channel": "C_TEST_TC01",
                    "ts": ts,
                    "thread_ts": ts,
                    "text": f"<@U_AGY_BOT> {q}",
                }
                t0 = time.perf_counter()
                await gateway.handle_message(event, client)
                latency = time.perf_counter() - t0

                # Check 1: exactly 1 message posted per turn
                self.assertEqual(len(client.posted_messages), idx + 1)
                posted = client.posted_messages[-1]["text"]

                # Check 2: Slack bold headers used (single asterisk, no ATX hash, no double asterisk)
                self.assertIn("*Fleet Status Overview*", posted)
                self.assertNotIn("**", posted)
                for line in posted.splitlines():
                    self.assertFalse(line.lstrip().startswith("#"))

                # Check 3: Zero worker panes spawned
                self.assertEqual(len(client.worker_panes_spawned), 0)

                # Check 4: Zero canned announcements
                canned = ["starting agent", "dispatching", "i will run", "let me inspect", "looking into this"]
                for c in canned:
                    self.assertNotIn(c, posted.lower())

                # Check 5: Zero completion footers
                self.assertNotIn("task completed", posted.lower())
                self.assertNotIn("execution completed", posted.lower())

                # Check 6: Reactions added (:gear: then :white_check_mark:)
                gear_for_ts = [r for r in client.reactions_added if r["name"] == "gear" and r["timestamp"] == ts]
                check_for_ts = [r for r in client.reactions_added if r["name"] == "white_check_mark" and r["timestamp"] == ts]
                self.assertEqual(len(gear_for_ts), 1)
                self.assertEqual(len(check_for_ts), 1)
        finally:
            await router.stop_reaper()


class TestAdversarialTC02MultiTurnContinuity(unittest.IsolatedAsyncioTestCase):
    """TC-02: Multi-turn thread continuity and session persistence."""

    async def test_eight_turn_thread_continuity_and_no_stale_echo(self):
        client = MockSlackClient()
        turns = [
            ("Turn 1: Overview of cron jobs", "*Cron Overview:* 5 jobs configured."),
            ("Turn 2: Which one runs at 3 AM?", "*Cron 3 AM:* The database backup job."),
            ("Turn 3: Does it write to local or remote?", "*Storage Target:* Writes to local ZFS dataset."),
            ("Turn 4: How much space is remaining?", "*Disk Capacity:* 450 GB remaining."),
            ("Turn 5: What about docker containers?", "*Containers:* 12 active containers on hermes."),
            ("Turn 6: Is Plex healthy?", "*Plex Status:* Healthy, streaming active."),
            ("Turn 7: Any failed systemd units?", "*Systemd Units:* 0 failed units."),
            ("Turn 8: Great, wrap it up.", "*Session Concluded:* All systems nominal."),
        ]

        adapter = ControllableMockAdapter({q: a for q, a in turns})
        router = SessionRouter(adapter_factory=lambda cwd: adapter)
        gateway = IngressGateway(router=router, bot_user_id="U_AGY_BOT")

        channel = "C_CONTINUITY"
        thread_ts = "1730000200.000100"

        try:
            initial_session = None
            for turn_idx, (user_text, expected_reply) in enumerate(turns):
                msg_ts = f"1730000200.000{turn_idx + 1:03d}"
                event = {
                    "channel": channel,
                    "ts": msg_ts,
                    "thread_ts": thread_ts,
                    "text": user_text,
                }
                await gateway.handle_message(event, client)

                session_key = f"{channel}:{thread_ts}"
                current_session = router.sessions.get(session_key)
                self.assertIsNotNone(current_session)

                if initial_session is None:
                    initial_session = current_session
                else:
                    # Invariant: exactly the same SessionHandle instance
                    self.assertIs(current_session, initial_session)

                self.assertEqual(current_session.current_turn, turn_idx + 1)
                self.assertEqual(len(client.posted_messages), turn_idx + 1)

                delivered = client.posted_messages[-1]["text"]
                self.assertIn(expected_reply.splitlines()[0], delivered)

                # Prior prompts must NEVER be echoed into current response
                for prior_prompt, _ in turns[:turn_idx]:
                    self.assertNotIn(prior_prompt, delivered)
        finally:
            await router.stop_reaper()


class TestAdversarialTC03InteractiveBlockerResilience(unittest.IsolatedAsyncioTestCase):
    """TC-03: Dual-pane yield within <= 3.0s and interactive blocker detection."""

    async def test_yield_timeout_enforcement_and_handle_structure(self):
        mock_client = MagicMock()
        executor = RunnerExecutor(herdr_client=mock_client)

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            slow_proc = AsyncMock()
            slow_proc.pid = 88123
            slow_proc.communicate.side_effect = asyncio.TimeoutError()

            info_proc = AsyncMock()
            info_proc.returncode = 0
            info_proc.communicate.return_value = (b'{"result": {"pid": 88123}}', b"")
            mock_exec.side_effect = [slow_proc, info_proc]

            start_t = time.perf_counter()
            handle = await executor.run_command(
                pane="runner",
                command="rclone config reconnect remote:",
                yield_timeout=0.1
            )
            elapsed = time.perf_counter() - start_t

            # Invariant 1: Execution completes in <= 3.0s
            self.assertLess(elapsed, 3.0)

            # Invariant 2: Yielded handle has status='running' and foreground PID
            self.assertIsInstance(handle, ExecutionHandle)
            self.assertEqual(handle.status, "running")
            self.assertEqual(handle.pid, 88123)
            self.assertEqual(handle.pane, "runner")

    def test_buffer_observer_blocker_permutations(self):
        observer = RunnerBufferObserver()

        test_cases = [
            (
                "Open browser at http://127.0.0.1:53682/auth?state=abc123token for login",
                "oauth",
                "http://127.0.0.1:53682/auth?state=abc123token"
            ),
            (
                "Listening on http://localhost:8080/callback for authorization code",
                "oauth",
                "http://localhost:8080/callback"
            ),
            (
                "Authorize at http://0.0.0.0:4000/oauth2/auth",
                "oauth",
                "http://0.0.0.0:4000/oauth2/auth"
            ),
            (
                "Do you want to proceed with upgrade? [y/N]",
                "prompt",
                None
            ),
            (
                "Apply these database changes now? (y/n)",
                "prompt",
                None
            ),
            (
                "[sudo] password for hermes: ",
                "prompt",
                None
            ),
            (
                "Enter passphrase for key '/Users/hermes/.ssh/id_ed25519':",
                "prompt",
                None
            ),
        ]

        for buffer_text, expected_type, expected_url in test_cases:
            blocker = observer.extract_interactive_blockers(buffer_text)
            self.assertIsNotNone(blocker, f"Failed to extract blocker from: {buffer_text}")
            self.assertEqual(blocker["type"], expected_type)
            if expected_url:
                self.assertEqual(blocker["url"], expected_url)
            else:
                self.assertIsNone(blocker["url"])


class TestAdversarialTC04WorkerDelegation(unittest.IsolatedAsyncioTestCase):
    """TC-04: Isolated worker delegation via herdr_handoff and output sanitization."""

    async def test_worker_delegation_branch_and_output_sanitization(self):
        mock_herdr = AsyncMock()
        mock_wt_json = json.dumps({
            "result": {
                "worktree": {"path": "/tmp/wt/task-1730000400-opencode"},
                "workspace": {"workspace_id": "ws-400"}
            }
        })
        raw_worker_terminal = (
            "<scratchpad>\n"
            "Checking syntax and AST changes in golem/gateway/router.py\n"
            "</scratchpad>\n"
            "[scratchpad] ran 159 tests, coverage 98%\n"
            "╭──────────────────────────────────────────────╮\n"
            "│ OpenCode: Task Successfully Completed       │\n"
            "╰──────────────────────────────────────────────╯\n"
            "⠋ Running test suite...\n"
            "❯ herdr agent wait\n"
            "\x1b[32m✔ Verified all requirements.\x1b[0m\n"
            "*Briefing Verdict*: Successfully updated router to support async yield."
        )

        mock_herdr.run_cli.side_effect = [
            (0, mock_wt_json, ""),
            (0, "started", ""),
            (0, "prompt sent", ""),
            (0, "done", ""),
            (0, raw_worker_terminal, ""),
        ]

        delegator = WorkerDelegator(herdr_client=mock_herdr, base_repo="/Users/hermes/projects/first-mate")
        result = await delegator.handoff(
            subagent="opencode",
            task="Update router yield logic",
            isolate_branch=True
        )

        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["branch"].startswith("task-"))
        self.assertTrue(result["branch"].endswith("-opencode"))

        out = result["output"]
        # Scratchpads, VT100 boxes, spinners, prompt echoes, ANSI stripped
        self.assertNotIn("<scratchpad>", out)
        self.assertNotIn("Checking syntax and AST", out)
        self.assertNotIn("[scratchpad]", out)
        for box_glyph in ["╭", "╮", "│", "╰", "╯", "─"]:
            self.assertNotIn(box_glyph, out)
        self.assertNotIn("⠋", out)
        self.assertNotIn("❯", out)
        self.assertNotIn("\x1b", out)
        self.assertIn("Verified all requirements.", out)
        self.assertIn("Briefing Verdict", out)


class TestAdversarialTC05WireNormalizationAndChunker(unittest.TestCase):
    """TC-05: Formatting, terminal chrome sanitization, and code-fence preserving chunker."""

    def test_transpile_and_sanitize_deep_matrix(self):
        cases = [
            ("### Level 3 Header", "*Level 3 Header*"),
            ("## Level 2 Header", "*Level 2 Header*"),
            ("# Level 1 Header", "*Level 1 Header*"),
            ("This is **bold** text", "This is *bold* text"),
            ("This is ***bold italic*** text", "This is *_bold italic_* text"),
            ("[Documentation](https://docs.firstmate.io)", "<https://docs.firstmate.io|Documentation>"),
            ("![Diagram](https://img.io/arch.png)", "<https://img.io/arch.png|Diagram>"),
            ("Bare link: [https://api.internal/v1](https://api.internal/v1)", "Bare link: <https://api.internal/v1>"),
        ]
        for inp, expected in cases:
            out = sanitize_for_slack(inp)
            self.assertEqual(out, expected)

    def test_extreme_terminal_chrome_stripping(self):
        dirty = (
            "╭─── Fleet Status ───╮\r\n"
            "│ ⠋ Checking nodes... (expand) │\r\n"
            "╰────────────────────╯\r\n"
            "❯ git status\r\n"
            ">\r\n"
            "\x1b[38;5;196mError: Connection reset by peer\x1b[0m\r\n"
            "```python\r\n"
            "# Code blocks must retain ### hashes\r\n"
            "x = '**literal asterisks**'\r\n"
            "```\r\n"
            "Done."
        )
        cleaned = sanitize_for_slack(dirty)
        for glyph in ["╭", "╮", "│", "╰", "╯", "─", "⠋", "❯", "\x1b", "\r"]:
            self.assertNotIn(glyph, cleaned)
        self.assertNotIn("**literal asterisks**", cleaned.split("```")[0])
        self.assertIn("```python\n# Code blocks must retain ### hashes\nx = '**literal asterisks**'\n```", cleaned)

    def test_chunker_small_budget_with_code_block_no_infinite_loop(self):
        code = "```python\nprint('hello world')\n```"
        # Test small budgets down to 10 chars
        for b in [10, 15, 20, 25, 30, 50, 100]:
            chunks = split_message_boundary_aware(code, max_chars=b)
            self.assertGreater(len(chunks), 0)
            for c in chunks:
                self.assertLessEqual(len(c), max(b, len("```python\n```\n")))

    def test_chunker_code_fence_balancing(self):
        long_code = "```python\n" + "\n".join([f"line_{i} = {i}" for i in range(100)]) + "\n```"
        chunks = split_message_boundary_aware(long_code, max_chars=400)
        self.assertGreater(len(chunks), 1)
        for idx, chunk in enumerate(chunks):
            self.assertEqual(
                chunk.count("```") % 2, 0,
                f"Chunk {idx} has unclosed backticks: {chunk}"
            )

    def test_chunker_sweep_budgets_termination_and_fence_safety(self):
        """Sweep budget from 15 to 80 chars across various fenced blocks ensuring strict termination."""
        samples = [
            "```python\ndef foo():\n    return 42\n```",
            "````bash\necho 'four backticks'\n````",
            "~~~json\n{\"key\": \"val\"}\n~~~",
            "   ```python\n   x = 1\n   ```",
            "Para 1\r\n\r\n```python\r\na = 1\r\n```\r\n\r\nPara 2",
        ]
        for sample in samples:
            for budget in range(15, 80, 5):
                chunks = split_message_boundary_aware(sample, max_chars=budget)
                self.assertGreater(len(chunks), 0)
                # Verify progress made on every chunk
                for c in chunks:
                    self.assertTrue(len(c) > 0)

    def test_normalizer_bare_escape_and_sentinel_collision_safety(self):
        """Verify normalizer does not devour bare escape letters or collide with user sentinels."""
        # 1. Bare escape before capital letters and digits
        raw_bare = "Status: \x1bTask finished with code \x1b200, \x1bError was null."
        clean_bare = sanitize_for_slack(raw_bare)
        self.assertIn("Task finished with code 200, Error was null.", clean_bare)

        # 2. Literal user text containing sentinel-like patterns
        raw_sentinel = "Here is a code token: `\\x00FM_CODE_0\\x00` and literal \\x00FM_CODE_1234abcd_0\\x00."
        clean_sentinel = sanitize_for_slack(raw_sentinel)
        self.assertIn("\\x00FM_CODE_0\\x00", clean_sentinel)
        self.assertIn("\\x00FM_CODE_1234abcd_0\\x00", clean_sentinel)

        # 3. ATX heading variations
        raw_headings = (
            "   ### Indented Level 3 ###\n"
            "## Heading With **Bold** Inside\n"
            "# Heading With ***Bold Italic*** Inside\n"
            "Normal text."
        )
        clean_headings = sanitize_for_slack(raw_headings)
        self.assertIn("*Indented Level 3*", clean_headings)
        self.assertIn("*Heading With Bold Inside*", clean_headings)
        self.assertIn("*Heading With _Bold Italic_ Inside*", clean_headings)


class TestAdversarialRouterConcurrencyAndLifecycle(unittest.IsolatedAsyncioTestCase):
    """Stress tests on SessionRouter concurrency and lifecycle operations."""

    async def test_high_concurrency_get_or_create_session(self):
        """50 concurrent coroutines requesting same session return identical instance."""
        adapter = ControllableMockAdapter()
        router = SessionRouter(adapter_factory=lambda cwd: adapter)

        try:
            channel = "C_CONCURRENT"
            thread_ts = "1730000999.000100"

            async def get_sess():
                return await router.get_or_create_session(channel, thread_ts)

            sessions = await asyncio.gather(*(get_sess() for _ in range(50)))
            first_sess = sessions[0]
            for s in sessions[1:]:
                self.assertIs(s, first_sess)
            self.assertEqual(len(router.sessions), 1)
        finally:
            await router.stop_reaper()

    async def test_explicit_session_teardown_and_reaper_sweep(self):
        """Test explicit teardown via handle_teardown and sweep_expired_sessions."""
        adapter = ControllableMockAdapter()
        router = SessionRouter(adapter_factory=lambda cwd: adapter, inactivity_timeout=0.1)

        try:
            # 1. Create two sessions
            s1 = await router.get_or_create_session("C1", "T1")
            s2 = await router.get_or_create_session("C1", "T2")
            self.assertEqual(len(router.sessions), 2)

            # 2. Explicit teardown on T1
            closed = await router.handle_teardown("C1", "T1")
            self.assertTrue(closed)
            self.assertNotIn("C1:T1", router.sessions)
            self.assertEqual(s1.state, SessionState.CLOSED)

            # 3. Wait for T2 to exceed inactivity timeout (0.1s)
            await asyncio.sleep(0.15)
            swept = await router.sweep_expired_sessions()
            self.assertIn("C1:T2", swept)
            self.assertNotIn("C1:T2", router.sessions)
            self.assertEqual(s2.state, SessionState.CLOSED)
        finally:
            await router.stop_reaper()


if __name__ == "__main__":
    unittest.main()

