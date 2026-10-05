#!/usr/bin/env python3
"""
tests/test_agent_m2_stress.py
Empirical stress-testing and adversarial harness for Milestone 2:
HeadlessAgentAdapter and AgentEvent protocol.

Covers:
1. High-throughput mock NDJSON streams (10,000+ events) with rapid deltas and complex tool events.
2. Resource metrics: wall-clock timing, throughput (events/sec), peak memory consumption (tracemalloc).
3. Non-UTF-8 byte injection into stream stdout.
4. Real subprocess lifecycle: SIGKILL mid-turn and non-zero exit code (42).
5. Post-crash multi-turn recovery.
6. Adversarial injection of non-dict JSON roots ([1, 2, 3], "string", 42, null) and null subfields.
7. Early consumer exit / generator interruption deadlock verification.
8. Protocol deserializer robustness under malformed payloads.
"""

import sys
import os
import json
import time
import signal
import asyncio
import tracemalloc
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from golem.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
)
from golem.agent.adapter import HeadlessAgentAdapter


class FakeStressStreamProcess:
    """Mock process capable of streaming large pre-generated event queues."""

    def __init__(self, init_line: str, lines: list[bytes], returncode: int = 0):
        self.init_line = init_line if isinstance(init_line, bytes) else init_line.encode("utf-8") + b"\n"
        self.lines = list(lines)
        self.returncode = None
        self._target_returncode = returncode
        self.stdin_written = []
        self._is_closed = False

        self.stdin = MagicMock()
        self.stdin.write = MagicMock(side_effect=self._mock_write)
        self.stdin.drain = AsyncMock()
        self.stdin.close = MagicMock(side_effect=self._mock_close)
        self.stdin.wait_closed = AsyncMock()
        self.stdin.is_closing = MagicMock(side_effect=lambda: self._is_closed)

        self.stdout = MagicMock()
        self.stdout.readline = AsyncMock(side_effect=self._mock_readline)

        self.stderr = MagicMock()
        self.stderr.read = AsyncMock(return_value=b"")

        self._read_first = False

    def _mock_write(self, data: bytes):
        if self._is_closed:
            raise BrokenPipeError("Pipe closed")
        self.stdin_written.append(data)

    def _mock_close(self):
        self._is_closed = True

    async def _mock_readline(self) -> bytes:
        if not self._read_first:
            self._read_first = True
            return self.init_line
        if self.lines:
            return self.lines.pop(0)
        self.returncode = self._target_returncode
        return b""

    async def wait(self) -> int:
        self.returncode = self._target_returncode
        return self._target_returncode

    def terminate(self):
        self.returncode = 0

    def kill(self):
        self.returncode = -9


class TestAgentM2Stress(IsolatedAsyncioTestCase):
    """Empirical stress-testing suite for Milestone 2 adapter and protocol."""

    async def test_high_throughput_10k_stream(self):
        """
        Stress test 1: Generate high-throughput stream of 12,000+ events
        (10,000 text_deltas + 1,000 tool start/end pairs + status updates + result).
        Measure: execution time, events/sec, memory delta, and peak memory.
        """
        tracemalloc.start()

        total_deltas = 10000
        total_tools = 1000
        stream_bytes: list[bytes] = []

        # Initial status
        stream_bytes.append(
            json.dumps({
                "event": "step_update",
                "step_update": {"step_type": "status", "status": "working", "detail": "Starting batch"},
            }).encode("utf-8") + b"\n"
        )

        # Interleave deltas and tool events
        for i in range(total_tools):
            for j in range(10):
                idx = i * 10 + j
                stream_bytes.append(
                    json.dumps({
                        "event": "step_update",
                        "step_update": {
                            "step_type": "agent_response",
                            "state": "ACTIVE",
                            "step_index": idx,
                            "text_delta": f"token_{idx} ",
                        },
                    }).encode("utf-8") + b"\n"
                )

            stream_bytes.append(
                json.dumps({
                    "event": "step_update",
                    "step_update": {
                        "step_type": "tool",
                        "state": "ACTIVE",
                        "step_index": i,
                        "tool_name": "herdr_pane_run",
                        "tool_call_id": f"call_{i}",
                        "tool_info": {"parameters": {"cmd": f"echo {i}"}},
                    },
                }).encode("utf-8") + b"\n"
            )
            stream_bytes.append(
                json.dumps({
                    "event": "step_update",
                    "step_update": {
                        "step_type": "tool",
                        "state": "DONE",
                        "step_index": i,
                        "tool_name": "herdr_pane_run",
                        "tool_call_id": f"call_{i}",
                        "duration_seconds": 0.001,
                        "tool_info": {"output": f"result_{i}"},
                    },
                }).encode("utf-8") + b"\n"
            )

        stream_bytes.append(
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Batch finished successfully.",
                    "duration_seconds": 2.5,
                    "usage": {"input_tokens": 1000, "output_tokens": 10000},
                },
            }).encode("utf-8") + b"\n"
        )

        init_json = json.dumps({"event": "init", "conversation_id": "c-stress-10k", "init": {"tools": []}})
        mock_proc = FakeStressStreamProcess(init_line=init_json, lines=stream_bytes)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

            t_start = time.perf_counter()
            received_events: list[AgentEvent] = []
            async for ev in adapter.send_turn("Run 10k stress"):
                received_events.append(ev)
            t_end = time.perf_counter()

            elapsed = t_end - t_start
            _, mem_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            deltas = [e for e in received_events if isinstance(e, TextDeltaEvent)]
            tool_starts = [e for e in received_events if isinstance(e, ToolStartEvent)]
            tool_ends = [e for e in received_events if isinstance(e, ToolEndEvent)]
            finish_events = [e for e in received_events if isinstance(e, FinishEvent)]

            self.assertEqual(len(deltas), total_deltas)
            self.assertEqual(len(tool_starts), total_tools)
            self.assertEqual(len(tool_ends), total_tools)
            self.assertEqual(len(finish_events), 1)
            self.assertEqual(finish_events[0].status, "SUCCESS")

            events_per_sec = len(received_events) / elapsed if elapsed > 0 else 0
            peak_mb = mem_peak / (1024 * 1024)

            print(f"\n[STRESS 1: 10K+ EVENTS] Processed {len(received_events)} events in {elapsed:.3f}s ({events_per_sec:.0f} ev/s). Peak mem: {peak_mb:.2f} MB")
            self.assertGreater(events_per_sec, 2000, "Throughput too low for headless IPC event loop")
            await adapter.stop()

    async def test_non_utf8_byte_injection(self):
        """
        Stress test 2: Inject non-UTF-8 bytes (raw invalid bytes, broken multi-byte sequences).
        Verify adapter decodes with errors='replace' without crashing.
        """
        init_json = json.dumps({"event": "init", "conversation_id": "c-utf8", "init": {}})
        bad_byte_lines = [
            b"\xff\xfe\xfd\n",  # Pure invalid UTF-8 bytes
            b"\x80\x81\x82\x83\n",
            b"\xc0\xaf\n",  # Overlong encoding
            b'{"event": "step_update", "step_update": {"step_type": "agent_response", "text_delta": "Valid \xff\xfe token"}}\n',
            json.dumps({
                "event": "result",
                "result": {"status": "SUCCESS", "response": "Recovered."},
            }).encode("utf-8") + b"\n",
        ]

        mock_proc = FakeStressStreamProcess(init_line=init_json, lines=bad_byte_lines)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

            events = []
            async for ev in adapter.send_turn("Test bad utf8"):
                events.append(ev)

            finish = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finish), 1)
            self.assertEqual(finish[0].status, "SUCCESS")
            print("\n[STRESS 2: NON-UTF8 BYTES] Handled non-UTF8 bytes cleanly without unhandled exception")
            await adapter.stop()

    async def test_real_child_process_sigkill_mid_turn(self):
        """
        Stress test 3: Real subprocess lifecycle.
        Spawn a real Python child process, then SIGKILL it mid-turn.
        Verify:
        - Generator yields FinishEvent(status="ERROR")
        - Generator does NOT hang
        - adapter.is_alive is False
        - A subsequent turn cleanly re-spawns and succeeds!
        """
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/first-mate",
            agy_bin=mock_bin,
            env={"MOCK_AGY_MODE": "crash_after_first_delta"},
        )

        t_start = time.perf_counter()
        events_t1: list[AgentEvent] = []

        async def run_turn_and_kill():
            turn_gen = adapter.send_turn("Do work")
            async for ev in turn_gen:
                events_t1.append(ev)
                if isinstance(ev, TextDeltaEvent):
                    self.assertIsNotNone(adapter.process)
                    os.kill(adapter.process.pid, signal.SIGKILL)

        try:
            await asyncio.wait_for(run_turn_and_kill(), timeout=5.0)
        except asyncio.TimeoutError:
            self.fail("adapter.send_turn hung indefinitely after child process SIGKILL!")

        t_elapsed = time.perf_counter() - t_start
        print(f"\n[STRESS 3: REAL SIGKILL] Handled mid-turn SIGKILL in {t_elapsed:.3f}s")

        self.assertTrue(any(isinstance(e, TextDeltaEvent) for e in events_t1))
        finish_events = [e for e in events_t1 if isinstance(e, FinishEvent)]
        self.assertEqual(len(finish_events), 1)
        finish = finish_events[0]
        self.assertEqual(finish.status, "ERROR")
        self.assertIn("Process terminated unexpectedly", finish.error)
        self.assertFalse(adapter.is_alive)

        # Post-crash recovery on Turn 2
        adapter.env = {"MOCK_AGY_MODE": "normal"}

        events_t2: list[AgentEvent] = []
        async for ev in adapter.send_turn("Turn 2 prompt"):
            events_t2.append(ev)

        t2_finish = [e for e in events_t2 if isinstance(e, FinishEvent)]
        self.assertEqual(len(t2_finish), 1)
        self.assertEqual(t2_finish[0].status, "SUCCESS")
        self.assertEqual(t2_finish[0].response, "Echo: Turn 2 prompt")
        print("[STRESS 3: RECOVERY] Subsequent turn cleanly re-spawned and completed turn!")
        await adapter.stop()

    async def test_real_child_process_nonzero_exit_mid_turn(self):
        """
        Stress test 4: Real subprocess exiting with non-zero exit code (42) and stderr output.
        Verify FinishEvent captures stderr and exit code 42 without crashing.
        """
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/first-mate",
            agy_bin=mock_bin,
            env={"MOCK_AGY_MODE": "exit_nonzero_mid_turn"},
        )

        events: list[AgentEvent] = []
        async for ev in adapter.send_turn("Trigger crash"):
            events.append(ev)

        finish_events = [e for e in events if isinstance(e, FinishEvent)]
        self.assertEqual(len(finish_events), 1)
        finish = finish_events[0]
        self.assertEqual(finish.status, "ERROR")
        self.assertIn("42", finish.error)
        self.assertIn("Fatal error: simulated crash in mock agy", finish.error)
        print(f"\n[STRESS 4: EXIT 42] Captured exit code and stderr correctly: {finish.error}")
        await adapter.stop()

    async def test_adversarial_non_dict_json_root_vulnerability(self):
        """
        Adversarial test 5: Inject valid JSON that is not a dictionary (e.g. lists, strings, numbers, null).
        Verify that adapter safely skips non-dict lines without crashing and completes the turn.
        """
        init_json = json.dumps({"event": "init", "conversation_id": "c-nondict", "init": {}})
        nondict_lines = [
            b"[1, 2, 3]\n",
            b'"just a string"\n',
            b"42\n",
            b"null\n",
            b"true\n",
            json.dumps({
                "event": "result",
                "result": {"status": "SUCCESS", "response": "After non-dict"},
            }).encode("utf-8") + b"\n",
        ]

        mock_proc = FakeStressStreamProcess(init_line=init_json, lines=nondict_lines)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

            events = []
            async for ev in adapter.send_turn("Test non-dict"):
                events.append(ev)

            await adapter.stop()
            finish = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finish), 1)
            self.assertEqual(finish[0].status, "SUCCESS")
            self.assertEqual(finish[0].response, "After non-dict")

    async def test_adversarial_null_subfields_vulnerability(self):
        """
        Adversarial test 6: Inject JSON dicts where subfields are null (e.g. step_update: null, result: null).
        Verify that adapter safely coalesces null subfields without AttributeError and completes turn.
        """
        init_json = json.dumps({"event": "init", "conversation_id": "c-nullsub", "init": {}})
        null_subfield_lines = [
            b'{"event": "step_update", "step_update": null}\n',
            json.dumps({
                "event": "result",
                "result": {"status": "SUCCESS", "response": "Done"},
            }).encode("utf-8") + b"\n",
        ]

        mock_proc = FakeStressStreamProcess(init_line=init_json, lines=null_subfield_lines)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

            events = []
            async for ev in adapter.send_turn("Test null subfield"):
                events.append(ev)

            await adapter.stop()
            finish = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finish), 1)
            self.assertEqual(finish[0].status, "SUCCESS")
            self.assertEqual(finish[0].response, "Done")

    async def test_adversarial_early_break_causes_permanent_deadlock(self):
        """
        Adversarial test 7: Breaking early out of async generator send_turn.
        Verify that subsequent turns do NOT deadlock and complete cleanly.
        """
        init_json = json.dumps({"event": "init", "conversation_id": "c-deadlock", "init": {}})
        lines = [
            json.dumps({"event": "step_update", "step_update": {"step_type": "agent_response", "text_delta": "T1"}}).encode() + b"\n",
            json.dumps({"event": "result", "result": {"status": "SUCCESS", "response": "T1"}}).encode() + b"\n",
            json.dumps({"event": "result", "result": {"status": "SUCCESS", "response": "T2"}}).encode() + b"\n",
        ]
        mock_proc = FakeStressStreamProcess(init_line=init_json, lines=lines)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/first-mate")

            gen = adapter.send_turn("P1")
            async for ev in gen:
                # Early break on the first event
                break

            # Confirm lock was held immediately after early break before Turn 2 starts
            self.assertTrue(adapter._lock.locked(), "Lock should be held after early break until next turn cleans it up")

            # Turn 2 should preempt/close the lingering generator and complete without deadlock
            t2_events = []
            async def turn2():
                async for ev in adapter.send_turn("P2"):
                    t2_events.append(ev)

            await asyncio.wait_for(turn2(), timeout=2.0)
            self.assertTrue(any(isinstance(e, FinishEvent) for e in t2_events))
            await adapter.stop()

    def test_protocol_from_ndjson_non_dict_crash(self):
        """
        Adversarial test 8: AgentEvent.from_ndjson raises ValueError when input is non-dict JSON.
        """
        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson("[1, 2, 3]\n")

        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson('"raw string"\n')

        with self.assertRaises(ValueError):
            AgentEvent.from_dict(None)  # type: ignore


if __name__ == "__main__":
    import unittest
    unittest.main()
