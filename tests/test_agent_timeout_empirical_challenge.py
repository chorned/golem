#!/usr/bin/env python3
"""
tests/test_agent_timeout_empirical_challenge.py
Challenger 2 Empirical Verification Suite for Milestone 1 (Mouth Stream Reliability).

Focus:
1. Thinking delays of 3.0s, 5.0s, and 8.0s between conversational preamble and tool start.
   Asserts turns are never cut prematurely regardless of thinking latency.
2. Long-running synchronous tool calls (4.0s) executing in foreground.
   Asserts turns remain active, tool output is completely captured, and finish event succeeds.
3. Tricky substrings in tool outputs ('task id: 123', 'refs/heads/task-xyz', etc.).
   Asserts immunity against false-positive background task flags and early stream-idle timeouts.
4. Cross-turn background task persistence combined with Turn 2 preamble thinking delay.
   Asserts that an active background task does not prematurely cut Turn 2 before its first tool call.
5. Multi-step synchronous execution exceeding cumulative 7.0s turn duration.
"""

from __future__ import annotations

import os
import json
import time
import asyncio
import unittest
from typing import Any, List
from unittest.mock import MagicMock, AsyncMock, patch

from first_mate.agent.adapter import HeadlessAgentAdapter
from first_mate.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    FinishEvent,
)


class MockDelayProcess:
    """Mock process allowing simulated async delays between specific lines."""

    def __init__(self, init_line: str, stdout_lines: List[Any], returncode: int = 0) -> None:
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


class TestAgentTimeoutEmpiricalChallenge(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge suite for preamble protection and timeout handling."""

    async def test_thinking_delay_3s_between_preamble_and_tool_start(self) -> None:
        """
        Challenge 1A: Thinking delay of 3.0s between preamble text delta and tool start.
        Asserts turn is NOT cut prematurely after 1.5s or 3.0s.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-think-3s",
            "init": {"tools": ["herdr_pane_run"]},
        })
        stream = [
            # Preamble text delta
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Checking fleet status across nodes...",
                },
            }),
            # 3.0s thinking delay before tool call formulation
            (3.0, json.dumps({
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
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "herdr_pane_run",
                    "step_index": 2,
                    "tool_info": {
                        "output": "100.80.59.45 desktop-pc active",
                        "status": "DONE",
                    },
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 3,
                    "text_delta": " Fleet is online.",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Checking fleet status across nodes... Fleet is online.",
                },
            }),
        ]

        mock_proc = MockDelayProcess(init_json, stream)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            events: List[AgentEvent] = []
            start_t = time.time()
            async for ev in adapter.send_turn("Status update please"):
                events.append(ev)
            elapsed = time.time() - start_t

            self.assertGreaterEqual(elapsed, 3.0, "Turn should have waited through the full 3.0s delay")
            tool_starts = [e for e in events if isinstance(e, ToolStartEvent)]
            tool_ends = [e for e in events if isinstance(e, ToolEndEvent)]
            finishes = [e for e in events if isinstance(e, FinishEvent)]

            self.assertEqual(len(tool_starts), 1, "ToolStartEvent must be yielded after 3s thinking delay")
            self.assertEqual(len(tool_ends), 1, "ToolEndEvent must be yielded")
            self.assertEqual(len(finishes), 1, "Exactly one FinishEvent expected")
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertEqual(
                finishes[0].response,
                "Checking fleet status across nodes... Fleet is online.",
            )

    async def test_thinking_delay_5s_between_preamble_and_tool_start(self) -> None:
        """
        Challenge 1B: Thinking delay of 5.0s between preamble text delta and tool start.
        Asserts turn survives deep LLM reasoning latency without premature cut.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-think-5s",
            "init": {"tools": ["herdr_pane_run"]},
        })
        stream = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Analyzing complex multi-node topology...",
                },
            }),
            # 5.0s thinking delay
            (5.0, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "herdr_pane_run",
                    "step_index": 2,
                    "tool_info": {
                        "parameters": {"pane": "runner", "command": "docker ps"}
                    },
                },
            })),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "herdr_pane_run",
                    "step_index": 2,
                    "tool_info": {
                        "output": "CONTAINER ID IMAGE STATUS\n112233 postgres Up 2 hours",
                        "status": "DONE",
                    },
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 3,
                    "text_delta": " Postgres container verified running.",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Analyzing complex multi-node topology... Postgres container verified running.",
                },
            }),
        ]

        mock_proc = MockDelayProcess(init_json, stream)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            events: List[AgentEvent] = []
            start_t = time.time()
            async for ev in adapter.send_turn("Check postgres"):
                events.append(ev)
            elapsed = time.time() - start_t

            self.assertGreaterEqual(elapsed, 5.0, "Turn should have waited through the full 5.0s delay")
            tool_starts = [e for e in events if isinstance(e, ToolStartEvent)]
            tool_ends = [e for e in events if isinstance(e, ToolEndEvent)]
            finishes = [e for e in events if isinstance(e, FinishEvent)]

            self.assertEqual(len(tool_starts), 1, "ToolStartEvent must be received after 5.0s delay")
            self.assertEqual(len(tool_ends), 1)
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertIn("Postgres container verified running", finishes[0].response)

    async def test_thinking_delay_8s_between_preamble_and_tool_start(self) -> None:
        """
        Challenge 1C: Thinking delay of 8.0s between preamble text delta and tool start.
        Stress-tests extreme reasoning latency before the first tool call is emitted.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-think-8s",
            "init": {"tools": ["herdr_pane_run"]},
        })
        stream = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Synthesizing deep diagnostic plan...",
                },
            }),
            # 8.0s thinking delay
            (8.0, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "herdr_pane_run",
                    "step_index": 2,
                    "tool_info": {
                        "parameters": {"pane": "runner", "command": "zpool status"}
                    },
                },
            })),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "herdr_pane_run",
                    "step_index": 2,
                    "tool_info": {
                        "output": "pool: tank\nstate: ONLINE\nerrors: No known data errors",
                        "status": "DONE",
                    },
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 3,
                    "text_delta": " ZFS storage pool is healthy.",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Synthesizing deep diagnostic plan... ZFS storage pool is healthy.",
                },
            }),
        ]

        mock_proc = MockDelayProcess(init_json, stream)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            events: List[AgentEvent] = []
            start_t = time.time()
            async for ev in adapter.send_turn("Check ZFS storage"):
                events.append(ev)
            elapsed = time.time() - start_t

            self.assertGreaterEqual(elapsed, 8.0, "Turn should have waited through the full 8.0s delay")
            tool_starts = [e for e in events if isinstance(e, ToolStartEvent)]
            tool_ends = [e for e in events if isinstance(e, ToolEndEvent)]
            finishes = [e for e in events if isinstance(e, FinishEvent)]

            self.assertEqual(len(tool_starts), 1, "ToolStartEvent must be received after 8.0s delay")
            self.assertEqual(len(tool_ends), 1)
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertIn("ZFS storage pool is healthy", finishes[0].response)

    async def test_long_running_synchronous_tool_4s_stays_active(self) -> None:
        """
        Challenge 2: Synchronous tool running for 4.0s (longer than old 1.5s timeout).
        Asserts turn stays active and finishes with full response and captured tool output.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-sync-tool-4s",
            "init": {"tools": ["run_command"]},
        })
        stream = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Executing 4-second synchronous build job...",
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
                        "parameters": {"CommandLine": "make release"}
                    },
                },
            }),
            # 4.0s tool execution in foreground
            (4.0, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "run_command",
                    "step_index": 2,
                    "tool_info": {
                        "output": "Compiling target... Done in 3.98s. Binary ready at dist/bin.",
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
                    "text_delta": " Release build ready at dist/bin.",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": (
                        "Executing 4-second synchronous build job... "
                        "Release build ready at dist/bin."
                    ),
                },
            }),
        ]

        mock_proc = MockDelayProcess(init_json, stream)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            events: List[AgentEvent] = []
            start_t = time.time()
            async for ev in adapter.send_turn("Run build"):
                events.append(ev)
            elapsed = time.time() - start_t

            self.assertGreaterEqual(elapsed, 4.0, "Turn should stay alive through full 4.0s tool execution")
            tool_ends = [e for e in events if isinstance(e, ToolEndEvent)]
            finishes = [e for e in events if isinstance(e, FinishEvent)]

            self.assertEqual(len(tool_ends), 1, "ToolEndEvent must not be skipped")
            self.assertIn("Done in 3.98s", tool_ends[0].output)
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertIn("Release build ready at dist/bin", finishes[0].response)

    async def test_tricky_substrings_in_tool_output_immunity(self) -> None:
        """
        Challenge 3: Tool outputs containing tricky strings ('task id: 123',
        'refs/heads/task-xyz', 'Running background task check', 'task id: 888').
        Asserts these NEVER trip has_background_task or shorten timeouts.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-tricky-strings",
            "init": {"tools": ["run_command"]},
        })
        tricky_tool_output = (
            "Examining git references and audit logs:\n"
            "* commit a1b2c3d (refs/heads/task-xyz)\n"
            "* commit e5f6g7h (origin/refs/heads/task-456)\n"
            "Log trace: task id: 123 completed successfully\n"
            "Log trace: task id: 888 scheduled for tomorrow\n"
            "Worker status: Running background task check on port 8080\n"
            "File path: /var/log/task-runners/audit.log\n"
            "Summary: 0 errors found."
        )

        stream = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "run_command",
                    "step_index": 1,
                    "tool_info": {
                        "parameters": {"CommandLine": "git log --all"}
                    },
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "run_command",
                    "step_index": 1,
                    "tool_info": {
                        "output": tricky_tool_output,
                        "status": "DONE",
                    },
                },
            }),
            # Pacing delay after tool completion: agent takes 1.5s to formulate response
            (1.5, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 2,
                    "text_delta": "Audit complete: git references and task logs verified clean.",
                },
            })),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Audit complete: git references and task logs verified clean.",
                },
            }),
        ]

        mock_proc = MockDelayProcess(init_json, stream)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            events: List[AgentEvent] = []
            async for ev in adapter.send_turn("Audit task references"):
                events.append(ev)

            # Assert adapter state was NOT contaminated by tricky substrings
            self.assertFalse(adapter.has_active_background_tasks, "Spurious background flag must NOT be set")
            self.assertEqual(len(adapter.active_background_tasks), 0, "No background task IDs should be registered")

            finishes = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertEqual(
                finishes[0].response,
                "Audit complete: git references and task logs verified clean.",
            )

    async def test_cross_turn_background_persistence_with_preamble_thinking_delay(self) -> None:
        """
        Challenge 4 (Adversarial Edge Case):
        Turn 1 spawns an authentic background task ('bg-task-999').
        Turn 2 starts with background task still active, emits preamble text,
        then experiences a 3.0s thinking delay before Turn 2's tool call begins.
        Asserts Turn 2 is NOT prematurely terminated by bg_idle_timeout during thinking!
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-cross-turn-bg",
            "init": {"tools": ["run_command", "herdr_pane_run"]},
        })

        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        # Turn 1: Spawns real background task
        t1_stream = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "run_command",
                    "step_index": 1,
                    "tool_info": {"parameters": {"CommandLine": "sleep 100", "IsDaemon": True}},
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "run_command",
                    "step_index": 1,
                    "tool_info": {
                        "output": "background task with task id: bg-task-999",
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
                    "text_delta": "Daemon spawned in background.",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Daemon spawned in background.",
                },
            }),
        ]

        mock_proc_t1 = MockDelayProcess(init_json, t1_stream)
        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc_t1
            t1_events = [e async for e in adapter.send_turn("Start daemon")]
            self.assertEqual(len([e for e in t1_events if isinstance(e, FinishEvent)]), 1)
            # Verify Turn 1 correctly noted background task
            self.assertTrue(adapter.has_active_background_tasks)
            self.assertIn("bg-task-999", adapter.active_background_tasks)

        # Turn 2: Background task bg-task-999 is STILL ACTIVE in adapter state.
        # Now agent emits preamble, thinks for 3.0s, then invokes tool.
        t2_stream = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Checking on the active background daemon...",
                },
            }),
            # 3.0s thinking delay before tool call
            (3.0, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "herdr_pane_run",
                    "step_index": 2,
                    "tool_info": {"parameters": {"command": "ps aux | grep sleep"}},
                },
            })),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "herdr_pane_run",
                    "step_index": 2,
                    "tool_info": {"output": "hermes 9999 sleep 100", "status": "DONE"},
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 3,
                    "text_delta": " Daemon process is alive with PID 9999.",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Checking on the active background daemon... Daemon process is alive with PID 9999.",
                },
            }),
        ]

        mock_proc_t2 = MockDelayProcess(init_json, t2_stream)
        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc_t2
            # Simulate adapter.process being the alive mock
            adapter.process = mock_proc_t2

            start_t = time.time()
            t2_events = [e async for e in adapter.send_turn("How is the daemon doing?")]
            elapsed = time.time() - start_t

            self.assertGreaterEqual(elapsed, 3.0, "Turn 2 should have survived the 3.0s thinking delay")
            tool_starts = [e for e in t2_events if isinstance(e, ToolStartEvent)]
            tool_ends = [e for e in t2_events if isinstance(e, ToolEndEvent)]
            finishes = [e for e in t2_events if isinstance(e, FinishEvent)]

            self.assertEqual(len(tool_starts), 1, "Turn 2 must NOT have aborted before its tool started")
            self.assertEqual(len(tool_ends), 1)
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertIn("Daemon process is alive with PID 9999", finishes[0].response)

    async def test_multi_step_synchronous_tools_cumulative_duration_7s(self) -> None:
        """
        Challenge 5: Multi-step turn with two 2.5s synchronous tools and a 2.0s thinking delay.
        Total turn duration > 7.0s.
        Asserts turn never cuts prematurely across multiple synchronous stages.
        """
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "c-multi-step-7s",
            "init": {"tools": ["run_command"]},
        })
        stream = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Step 1: Running unit tests...",
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "run_command",
                    "step_index": 2,
                    "tool_info": {"parameters": {"CommandLine": "pytest tests/unit"}},
                },
            }),
            # Tool 1 takes 2.5s
            (2.5, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "run_command",
                    "step_index": 2,
                    "tool_info": {"output": "42 passed in 2.45s", "status": "DONE"},
                },
            })),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 3,
                    "text_delta": " Unit tests passed. Step 2: Running integration tests...",
                },
            }),
            # 2.0s thinking delay before tool 2
            (2.0, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "run_command",
                    "step_index": 4,
                    "tool_info": {"parameters": {"CommandLine": "pytest tests/integration"}},
                },
            })),
            # Tool 2 takes 2.5s
            (2.5, json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "run_command",
                    "step_index": 4,
                    "tool_info": {"output": "18 passed in 2.48s", "status": "DONE"},
                },
            })),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 5,
                    "text_delta": " All test suites green.",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": (
                        "Step 1: Running unit tests... Unit tests passed. "
                        "Step 2: Running integration tests... All test suites green."
                    ),
                },
            }),
        ]

        mock_proc = MockDelayProcess(init_json, stream)
        adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            events: List[AgentEvent] = []
            start_t = time.time()
            async for ev in adapter.send_turn("Run full test verification"):
                events.append(ev)
            elapsed = time.time() - start_t

            self.assertGreaterEqual(elapsed, 7.0, "Total turn execution should exceed 7.0s")
            tool_starts = [e for e in events if isinstance(e, ToolStartEvent)]
            tool_ends = [e for e in events if isinstance(e, ToolEndEvent)]
            finishes = [e for e in events if isinstance(e, FinishEvent)]

            self.assertEqual(len(tool_starts), 2, "Both tool start events must be yielded")
            self.assertEqual(len(tool_ends), 2, "Both tool end events must be yielded")
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertIn("All test suites green", finishes[0].response)


if __name__ == "__main__":
    unittest.main()
