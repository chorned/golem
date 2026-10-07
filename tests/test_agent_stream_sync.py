#!/usr/bin/env python3
"""
tests/test_agent_stream_sync.py
Comprehensive test suite for HeadlessAgentAdapter stream synchronization,
pipe hygiene, thinking latency tolerance, and multi-turn state isolation.

Verifies:
- TC-SYNC-01: Pre-tool conversational acknowledgment with thinking latency (pacing calibration).
- TC-SYNC-02: Synchronous long-running tool calls (>1.5s) are not prematurely terminated.
- TC-SYNC-03: Total pipe draining eliminates phase shifts after early consumer breaks.
- TC-SYNC-04: False-positive background substring immunity in synchronous tool outputs.
- TC-SYNC-05: Multi-turn alternating fast and slow continuity with zero cross-talk.
- TC-SYNC-06: Pre-turn pipe buffer purge stress test (50+ stale NDJSON lines discarded).
"""

from __future__ import annotations

import os
import json
import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch

from golem.agent.adapter import HeadlessAgentAdapter
from golem.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    ThoughtDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    FinishEvent,
)


class MockDelayProcess:
    """Mock process allowing simulated async delays between specific lines."""

    def __init__(self, init_line: str, stdout_lines: list[Any], returncode: int = 0) -> None:
        self.init_line = init_line
        self.stdout_lines = list(stdout_lines)
        self.returncode = returncode
        self._init_sent = False

        self.stdin = MagicMock()
        self.stdin.write = MagicMock()
        self.stdin.drain = AsyncMock()
        self.stdin.close = MagicMock()
        self.stdin.wait_closed = AsyncMock()
        self.stdin.is_closing = MagicMock(return_value=False)

        self.stderr = MagicMock()
        self.stderr.read = AsyncMock(return_value=b"")

        self.stdout = MagicMock()
        self.stdout.readline = AsyncMock(side_effect=self._mock_readline)

    async def _mock_readline(self) -> bytes:
        if not self._init_sent:
            self._init_sent = True
            return self.init_line.encode("utf-8") + b"\n"

        if self.stdout_lines:
            item = self.stdout_lines.pop(0)
            if isinstance(item, tuple):
                delay, line = item
                await asyncio.sleep(delay)
                return line.encode("utf-8") + b"\n"
            return item.encode("utf-8") + b"\n"
        return b""

    async def wait(self) -> int:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = 0

    def kill(self) -> None:
        self.returncode = -9


class MultiTurnMockProcess:
    """Mock process supporting sequential turns based on prompt inspection."""

    def __init__(self, init_line: str) -> None:
        self.init_line = init_line
        self._init_sent = False
        self.returncode: Optional[int] = None

        self.stdin = MagicMock()
        self.stdin.write = MagicMock(side_effect=self._on_stdin_write)
        self.stdin.drain = AsyncMock()
        self.stdin.close = MagicMock()
        self.stdin.wait_closed = AsyncMock()
        self.stdin.is_closing = MagicMock(return_value=False)

        self.stderr = MagicMock()
        self.stderr.read = AsyncMock(return_value=b"")

        self.stdout = MagicMock()
        self.stdout.readline = AsyncMock(side_effect=self._mock_readline)
        self._turn_queue: asyncio.Queue[list[Any]] = asyncio.Queue()
        self._current_lines: list[Any] = []

    def _on_stdin_write(self, data: bytes) -> None:
        try:
            req = json.loads(data.decode("utf-8"))
            prompt = req.get("message", {}).get("content", "")
            if "ping" in prompt.lower():
                self._turn_queue.put_nowait([
                    json.dumps({
                        "event": "step_update",
                        "step_update": {
                            "step_type": "agent_response",
                            "state": "ACTIVE",
                            "step_index": 1,
                            "text_delta": "pong",
                        },
                    }),
                    json.dumps({
                        "event": "result",
                        "result": {"status": "SUCCESS", "response": "pong"},
                    }),
                ])
            elif "investigate" in prompt.lower():
                self._turn_queue.put_nowait([
                    json.dumps({
                        "event": "step_update",
                        "step_update": {
                            "step_type": "agent_response",
                            "state": "ACTIVE",
                            "step_index": 1,
                            "text_delta": "Checking fleet status...",
                        },
                    }),
                    (0.3, json.dumps({
                        "event": "step_update",
                        "step_update": {
                            "step_type": "tool",
                            "state": "ACTIVE",
                            "tool_name": "herdr_pane_run",
                            "step_index": 2,
                            "tool_info": {"parameters": {"command": "tailscale status"}},
                        },
                    })),
                    json.dumps({
                        "event": "step_update",
                        "step_update": {
                            "step_type": "tool",
                            "state": "DONE",
                            "tool_name": "herdr_pane_run",
                            "step_index": 2,
                            "tool_info": {"output": "All nodes active", "status": "DONE"},
                        },
                    }),
                    json.dumps({
                        "event": "step_update",
                        "step_update": {
                            "step_type": "agent_response",
                            "state": "ACTIVE",
                            "step_index": 3,
                            "text_delta": " All nodes verified online.",
                        },
                    }),
                    json.dumps({
                        "event": "result",
                        "result": {
                            "status": "SUCCESS",
                            "response": "Checking fleet status... All nodes verified online.",
                        },
                    }),
                ])
            else:
                self._turn_queue.put_nowait([
                    json.dumps({
                        "event": "step_update",
                        "step_update": {
                            "step_type": "agent_response",
                            "state": "ACTIVE",
                            "step_index": 1,
                            "text_delta": "The exit code was 0.",
                        },
                    }),
                    json.dumps({
                        "event": "result",
                        "result": {"status": "SUCCESS", "response": "The exit code was 0."},
                    }),
                ])
        except Exception:
            pass

    async def _mock_readline(self) -> bytes:
        if not self._init_sent:
            self._init_sent = True
            return self.init_line.encode("utf-8") + b"\n"

        if not self._current_lines:
            lines = await self._turn_queue.get()
            self._current_lines = list(lines)

        if self._current_lines:
            item = self._current_lines.pop(0)
            if isinstance(item, tuple):
                delay, line = item
                await asyncio.sleep(delay)
                return line.encode("utf-8") + b"\n"
            return item.encode("utf-8") + b"\n"
        return b""

    async def wait(self) -> int:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = 0

    def kill(self) -> None:
        self.returncode = -9


class TestAgentStreamSync(unittest.IsolatedAsyncioTestCase):
    """Unit test suite for mouth stream reliability and pipe hygiene."""

    async def test_pre_tool_conversational_acknowledgment_with_thinking_latency(self) -> None:
        """
        TC-SYNC-01: Verify agent emitting preamble text delta before deciding to invoke a tool
        is not cut prematurely after 1.5s while formulating the tool call.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-sync-01",
            "init": {"tools": ["herdr_pane_run"]},
        })
        stream = [
            # 1. Preamble text delta (per pacing calibration rules)
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Sure! Give me a second to gather the fleet details.",
                },
            }),
            # 2. 2.0s thinking delay before tool invocation (exceeds old 1.5s timeout)
            (2.0, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "herdr_pane_run",
                    "step_index": 2,
                    "tool_info": {
                        "parameters": {"pane": "runner", "command": "tailscale status"}
                    },
                },
            })),
            # 3. Tool execution finishes
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "herdr_pane_run",
                    "step_index": 2,
                    "tool_info": {
                        "output": "100.80.59.45 desktop-pc active\n100.113.173.33 oci active",
                        "status": "DONE",
                    },
                },
            }),
            # 4. Final briefing
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 3,
                    "text_delta": " All nodes are currently active and responsive.",
                },
            }),
            # 5. Authoritative turn completion
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": (
                        "Sure! Give me a second to gather the fleet details."
                        " All nodes are currently active and responsive."
                    ),
                },
            }),
        ]

        mock_proc = MockDelayProcess(init_json, stream)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            events: list[AgentEvent] = []
            async for ev in adapter.send_turn("How are our fleet machines doing?"):
                events.append(ev)

            # Assertions
            tool_starts = [e for e in events if isinstance(e, ToolStartEvent)]
            tool_ends = [e for e in events if isinstance(e, ToolEndEvent)]
            finishes = [e for e in events if isinstance(e, FinishEvent)]

            self.assertEqual(len(tool_starts), 1, "ToolStartEvent should have been received despite 2.0s delay")
            self.assertEqual(len(tool_ends), 1, "ToolEndEvent should have been received")
            self.assertEqual(tool_starts[0].tool_name, "herdr_pane_run")
            self.assertEqual(len(finishes), 1, "Turn should have exactly 1 FinishEvent")
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertIn("All nodes are currently active", finishes[0].response)

    async def test_synchronous_long_running_tool_not_prematurely_cut(self) -> None:
        """
        TC-SYNC-02: Verify long-running synchronous tool (>2.0s) executing while in_tool_call
        is True is never aborted by stream idle timeouts.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-sync-02",
            "init": {"tools": ["run_command"]},
        })
        stream = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Running long database migration...",
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "run_command",
                    "step_index": 2,
                    "tool_info": {
                        "parameters": {"CommandLine": "alembic upgrade head"}
                    },
                },
            }),
            # Tool takes 2.5s to run in foreground
            (2.5, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "run_command",
                    "step_index": 2,
                    "tool_info": {
                        "output": "Migration 0042_add_index applied successfully.",
                        "status": "DONE",
                    },
                },
            })),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 3,
                    "text_delta": " Migration applied cleanly.",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Running long database migration... Migration applied cleanly.",
                },
            }),
        ]

        mock_proc = MockDelayProcess(init_json, stream)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            events: list[AgentEvent] = []
            async for ev in adapter.send_turn("Run the migration"):
                events.append(ev)

            tool_ends = [e for e in events if isinstance(e, ToolEndEvent)]
            finishes = [e for e in events if isinstance(e, FinishEvent)]

            self.assertEqual(len(tool_ends), 1, "ToolEndEvent must not be skipped by premature cut")
            self.assertIn("Migration 0042_add_index", tool_ends[0].output)
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")

    async def test_total_pipe_draining_eliminates_phase_shift_after_early_break(self) -> None:
        """
        TC-SYNC-03: Real subprocess test with mock_agy_proc.py.
        Breaking after 1 delta in Turn 1 must NOT cause Turn 2 to consume Turn 1's leftover result.
        """
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/first-mate",
            agy_bin=mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            # Turn 1: consume 1 delta then break early
            async for ev in adapter.send_turn("Prompt 1"):
                if isinstance(ev, TextDeltaEvent):
                    break

            # Turn 2: run full turn
            t2_events: list[AgentEvent] = []
            async for ev in adapter.send_turn("Prompt 2"):
                t2_events.append(ev)

            finishes = [e for e in t2_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            # Must be Prompt 2 response, zero phase shift
            self.assertEqual(finishes[0].response, "Echo: Prompt 2")
        finally:
            await adapter.stop()

    async def test_false_positive_background_substring_immunity(self) -> None:
        """
        TC-SYNC-04: Tool outputs containing '/task-', 'task id:', or branch names like
        'task-1234-fix' must NOT trigger false positive background task flags or cut turns.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-sync-04",
            "init": {"tools": ["run_command"]},
        })
        stream = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "run_command",
                    "step_index": 1,
                    "tool_info": {
                        "parameters": {"CommandLine": "git branch"}
                    },
                },
            }),
            # Substrings that historically tripped naive substring filters
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "run_command",
                    "step_index": 1,
                    "tool_info": {
                        "output": (
                            "* task-456-bugfix\n"
                            "  origin/task-789-sync\n"
                            "Task id: 99 logged in file /task-data/audit.log\n"
                        ),
                        "status": "DONE",
                    },
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 2,
                    "text_delta": "Current branch is task-456-bugfix.",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Current branch is task-456-bugfix.",
                },
            }),
        ]

        mock_proc = MockDelayProcess(init_json, stream)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            events: list[AgentEvent] = []
            async for ev in adapter.send_turn("Check branches"):
                events.append(ev)

            # Verify no spurious background state was retained
            self.assertFalse(adapter.has_active_background_tasks)
            self.assertEqual(len(adapter.active_background_tasks), 0)

            finishes = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertEqual(finishes[0].response, "Current branch is task-456-bugfix.")

    async def test_multi_turn_alternating_fast_and_slow_continuity(self) -> None:
        """
        TC-SYNC-05: Multi-turn sequence alternating between instant responses,
        delayed multi-step investigations, and fast follow-ups without cross-turn leakage.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-sync-05",
            "init": {"tools": ["herdr_pane_run"]},
        })
        mock_proc = MultiTurnMockProcess(init_json)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc

            # Turn 1: Fast direct reply
            t1_events = [e async for e in adapter.send_turn("ping")]
            f1 = [e for e in t1_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(f1), 1)
            self.assertEqual(f1[0].response, "pong")

            # Turn 2: Multi-step investigation with pre-tool acknowledgment and tool delay
            t2_events = [e async for e in adapter.send_turn("investigate cluster")]
            f2 = [e for e in t2_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(f2), 1)
            self.assertIn("All nodes verified online", f2[0].response)

            # Turn 3: Follow-up question
            t3_events = [e async for e in adapter.send_turn("what was the exit code?")]
            f3 = [e for e in t3_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(f3), 1)
            self.assertEqual(f3[0].response, "The exit code was 0.")

    async def test_pre_turn_pipe_buffer_purge_stress(self) -> None:
        """
        TC-SYNC-06: Purge 50 stale NDJSON lines (including old result and error events)
        pre-loaded into adapter.process.stdout._buffer prior to send_turn().
        Asserts adapter purges buffer cleanly and yields only the fresh turn response.
        """
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/first-mate",
            agy_bin=mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            # Start process to initialize pipes
            await adapter.start()
            self.assertTrue(adapter.is_alive)
            self.assertIsNotNone(adapter.process)
            self.assertIsNotNone(adapter.process.stdout)

            # Forcefully inject 50 stale NDJSON frames into reader._buffer
            for i in range(50):
                stale_payload = {
                    "event": "result",
                    "result": {
                        "status": "SUCCESS",
                        "response": f"Stale contaminated message {i}",
                    },
                }
                adapter.process.stdout._buffer.extend(
                    (json.dumps(stale_payload) + "\n").encode("utf-8")
                )

            # Verify buffer has data
            self.assertGreater(len(adapter.process.stdout._buffer), 0)

            # Send turn: _drain_stdout() must purge stale buffer before executing turn
            events: list[AgentEvent] = []
            async for ev in adapter.send_turn("Fresh inquiry"):
                events.append(ev)

            finishes = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertEqual(
                finishes[0].response,
                "Echo: Fresh inquiry",
                "Turn response must not match any of the 50 stale frames",
            )
        finally:
            await adapter.stop()


class TestThoughtDeltaAndWebUIFormatting(unittest.TestCase):
    """Test ThoughtDeltaEvent protocol, accumulator, and WebUI ANSI formatting."""

    def test_thought_delta_event_protocol(self) -> None:
        """ThoughtDeltaEvent serializes and deserializes cleanly through AgentEvent NDJSON."""
        ev = ThoughtDeltaEvent(thought="Analyzing repository structure...")
        self.assertEqual(ev.event_type, "thought_delta")
        self.assertEqual(ev.thought, "Analyzing repository structure...")
        self.assertEqual(ev.text, "Analyzing repository structure...")

        # Test from_dict
        d = ev.to_dict()
        parsed = AgentEvent.from_dict(d)
        self.assertIsInstance(parsed, ThoughtDeltaEvent)
        self.assertEqual(parsed.thought, "Analyzing repository structure...")

        # Test from_ndjson
        line = ev.to_ndjson()
        parsed_nd = AgentEvent.from_ndjson(line)
        self.assertIsInstance(parsed_nd, ThoughtDeltaEvent)
        self.assertEqual(parsed_nd.thought, "Analyzing repository structure...")

    def test_backstage_execution_accumulator_thoughts(self) -> None:
        """Accumulator records thoughts into self.thoughts while keeping text_deltas isolated."""
        from golem.gateway.ingress import BackstageExecutionAccumulator

        acc = BackstageExecutionAccumulator()
        acc.record_event(ThoughtDeltaEvent(thought="Deciding on next step"))
        acc.record_event(TextDeltaEvent(text="Hello Carl"))

        self.assertEqual(acc.thoughts, ["Deciding on next step"])
        self.assertEqual(acc.text_deltas, ["Hello Carl"])
        # Public output must only contain text deltas, never raw thoughts
        self.assertEqual(acc.get_full_output(), "Hello Carl")

    def test_format_hand_event_for_webui_thought(self) -> None:
        """_format_hand_event_for_webui formats thoughts with ANSI purple marker and indent."""
        from golem.gateway.ingress import _format_hand_event_for_webui

        ev = ThoughtDeltaEvent(thought="First thought line\nSecond thought line")
        formatted = _format_hand_event_for_webui(ev)
        self.assertIsNotNone(formatted)
        self.assertIn("[THOUGHT]", formatted)
        self.assertIn("First thought line", formatted)
        self.assertIn("Second thought line", formatted)

    def test_format_hand_event_for_webui_tools(self) -> None:
        """_format_hand_event_for_webui formats tool start and tool end with command and output."""
        from golem.gateway.ingress import _format_hand_event_for_webui

        start_ev = ToolStartEvent(
            tool_name="run_command",
            parameters={"CommandLine": "pytest tests/test_agent.py", "Cwd": "/workspace/first-mate"},
        )
        formatted_start = _format_hand_event_for_webui(start_ev)
        self.assertIsNotNone(formatted_start)
        self.assertIn("[TOOL START]", formatted_start)
        self.assertIn("run_command", formatted_start)
        self.assertIn("pytest tests/test_agent.py", formatted_start)

        end_ev = ToolEndEvent(
            tool_name="run_command",
            output="3 passed in 0.12s",
            duration_seconds=0.12,
            is_error=False,
        )
        formatted_end = _format_hand_event_for_webui(end_ev)
        self.assertIsNotNone(formatted_end)
        self.assertIn("[TOOL END]", formatted_end)
        self.assertIn("run_command", formatted_end)
        self.assertIn("3 passed in 0.12s", formatted_end)


if __name__ == "__main__":
    unittest.main()
