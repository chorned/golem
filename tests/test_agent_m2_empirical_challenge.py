#!/usr/bin/env python3
"""
tests/test_agent_m2_empirical_challenge.py
Challenger 3 empirical verification suite for Milestone 2:
Headless Agent Adapter & Event Protocol (Gate 2).

Verifies:
1. Early generator break: start turn, consume 1 delta, break, and immediately start turn 2
   to verify zero lock deadlocks.
2. Stderr exhaustion: simulate child process writing 128KB of stderr during streaming turn;
   verify no blocking or deadlocks occur.
3. Fuzz test AgentEvent.from_ndjson() with 1,000 malformed, non-dict, and truncated payloads.
4. Stream-json pipe synchronization and unhashable event_type behavior.
"""

import os
import sys
import json
import time
import random
import string
import asyncio
import tempfile
import unittest
from typing import List

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


class TestAgentM2EmpiricalChallenge(unittest.IsolatedAsyncioTestCase):
    """Empirical verification suite executed by Challenger 3."""

    async def test_early_generator_break_zero_lock_deadlock(self):
        """
        Mission Requirement 2:
        Test early generator break: start a turn, consume 1 delta, break,
        and immediately start turn 2 to verify zero lock deadlocks.
        """
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/first-mate",
            agy_bin=mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            # Turn 1: consume exactly 1 delta and break immediately
            t1_deltas = 0
            async for ev in adapter.send_turn("Turn 1 prompt"):
                if isinstance(ev, TextDeltaEvent):
                    t1_deltas += 1
                    break

            self.assertEqual(t1_deltas, 1, "Must consume exactly 1 delta before breaking")

            # Immediately start Turn 2 with zero pause
            t2_events: List[AgentEvent] = []

            async def run_turn2():
                async for ev in adapter.send_turn("Turn 2 prompt"):
                    t2_events.append(ev)

            # Turn 2 must not deadlock or timeout (2.0s budget)
            await asyncio.wait_for(run_turn2(), timeout=2.0)

            finishes = [e for e in t2_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1, "Turn 2 must yield exactly 1 FinishEvent")
            self.assertEqual(finishes[0].status, "SUCCESS")

            # Verify lock is completely free
            self.assertFalse(adapter._lock.locked(), "Adapter lock must not remain held after turn 2")

            # Consecutive early breaks stress: break 3 times in a row
            for i in range(3):
                async for _ in adapter.send_turn(f"Rapid break {i}"):
                    break

            # Subsequent full turn completes without deadlocks
            t4_events: List[AgentEvent] = []
            async def run_turn4():
                async for ev in adapter.send_turn("Turn 4 prompt"):
                    t4_events.append(ev)

            await asyncio.wait_for(run_turn4(), timeout=2.0)
            self.assertTrue(any(isinstance(e, FinishEvent) for e in t4_events))
            self.assertFalse(adapter._lock.locked())

        finally:
            await adapter.stop()

    async def test_stderr_exhaustion_128kb_no_deadlock(self):
        """
        Mission Requirement 3:
        Test stderr exhaustion: simulate a child process writing 128KB of stderr
        during a streaming turn; verify no blocking or deadlocks occur.
        """
        # Create dedicated child script that writes 128KB (131,072 bytes) of stderr
        # concurrently with stdout event emission.
        script = """#!/usr/bin/env python3
import sys, json

# 1. Emit init event
sys.stdout.write(json.dumps({'event': 'init', 'conversation_id': 'c-stderr-128k', 'init': {'tools': []}}) + '\\n')
sys.stdout.flush()

while True:
    line = sys.stdin.readline()
    if not line:
        break

    # 2. Write 128KB of stderr (1,024 lines * 128 bytes = 131,072 bytes)
    for i in range(1024):
        msg = f"stderr log line {i:04d}: test warning data "
        msg = msg.ljust(127) + "\\n"
        sys.stderr.write(msg)
        sys.stderr.flush()

    # 3. Stream text delta to stdout
    delta = {
        'event': 'step_update',
        'step_update': {
            'step_type': 'agent_response',
            'state': 'ACTIVE',
            'step_index': 1,
            'text_delta': 'Processed after 128KB stderr write.'
        }
    }
    sys.stdout.write(json.dumps(delta) + '\\n')
    sys.stdout.flush()

    # 4. Finish event
    res = {
        'event': 'result',
        'result': {
            'status': 'SUCCESS',
            'response': 'Completed 128KB stderr turn.',
            'duration_seconds': 0.1
        }
    }
    sys.stdout.write(json.dumps(res) + '\\n')
    sys.stdout.flush()
"""
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".py") as f:
            f.write(script)
            tmp_path = f.name
        os.chmod(tmp_path, 0o755)

        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/first-mate",
            agy_bin=tmp_path,
        )

        try:
            t_start = time.perf_counter()
            events: List[AgentEvent] = []

            async def run_turn():
                async for ev in adapter.send_turn("Trigger 128KB stderr"):
                    events.append(ev)

            # Standard pipe buffer is 64KB. If stderr is not drained, child blocks on flush.
            # Draining must complete turn well under 5.0 seconds.
            await asyncio.wait_for(run_turn(), timeout=5.0)
            elapsed = time.perf_counter() - t_start

            # Verify received events
            deltas = [e for e in events if isinstance(e, TextDeltaEvent)]
            finishes = [e for e in events if isinstance(e, FinishEvent)]

            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0].text_delta, "Processed after 128KB stderr write.")
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            self.assertEqual(finishes[0].response, "Completed 128KB stderr turn.")

            # Verify stderr lines captured in bounded deque
            self.assertEqual(len(adapter._stderr_lines), 100, "Deque maxlen=100 must be full")
            self.assertTrue(adapter._stderr_lines[-1].startswith("stderr log line 1023"))

            print(f"\\n[EMPIRICAL VERIFIED] 128KB stderr written and drained in {elapsed:.3f}s with zero deadlocks.")

        finally:
            await adapter.stop()
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_fuzz_agent_event_from_ndjson_1000_payloads(self):
        """
        Mission Requirement 4:
        Fuzz test AgentEvent.from_ndjson() with 1,000 malformed, non-dict,
        and truncated payloads.
        """
        random.seed(20260930)
        payloads: List[str] = []

        # 1. Non-dict valid JSON primitives and arrays (350 payloads)
        non_dicts = [
            "123\\n", "0\\n", "-42\\n", "3.14159\\n", "true\\n", "false\\n", "null\\n",
            '"hello world"\\n', '""\\n', '"event_type"\\n', '"finish"\\n',
            "[]\\n", "[1, 2, 3]\\n", '["a", "b"]\\n', '[{"event_type": "finish"}]\\n',
            "[[1, 2], [3, 4]]\\n", "[null, true, false]\\n",
            "99999\\n", "1e-5\\n"
        ]
        for _ in range(350):
            payloads.append(random.choice(non_dicts))

        # 2. Truncated valid JSON payloads (350 payloads)
        valid_templates = [
            json.dumps({"event_type": "finish", "response": "OK", "status": "SUCCESS", "usage": {"tokens": 100}}),
            json.dumps({"event_type": "text_delta", "text_delta": "partial response token"}),
            json.dumps({"event_type": "tool_start", "tool_name": "bash", "parameters": {"cmd": "ls -la"}}),
            json.dumps({"event_type": "tool_end", "tool_name": "bash", "output": "file1.txt", "duration_seconds": 0.05}),
            json.dumps({"event_type": "awaiting_input", "prompt": "Continue? [y/N]", "choices": ["y", "n"]}),
            json.dumps({"event_type": "status_update", "status": "working", "detail": "Executing step 3"}),
        ]
        for _ in range(350):
            template = random.choice(valid_templates)
            cut_idx = random.randint(1, len(template) - 1)
            payloads.append(template[:cut_idx] + "\\n")

        # 3. Malformed syntax and random garbage (300 payloads)
        syntax_errors = [
            "{", "}", "{{", "}}", '{"event_type":}', '{"event_type": "finish",}',
            "NaN", "Infinity", "-Infinity", "undefined", "None", "True", "False",
            "<root><event>test</event></root>", "DROP TABLE events;",
        ]
        for _ in range(300):
            base = random.choice(syntax_errors)
            garbage = "".join(random.choices(string.printable, k=random.randint(0, 30)))
            payloads.append(base + garbage + "\\n")

        self.assertEqual(len(payloads), 1000)

        value_errors_caught = 0
        unexpected_exceptions = []

        for idx, payload in enumerate(payloads):
            try:
                AgentEvent.from_ndjson(payload)
                # If a valid payload somehow deserialized, verify it is an AgentEvent
            except (ValueError, json.JSONDecodeError):
                value_errors_caught += 1
            except Exception as e:
                unexpected_exceptions.append((idx, payload, type(e).__name__, str(e)))

        self.assertEqual(
            unexpected_exceptions,
            [],
            f"Fuzz test encountered unexpected exceptions: {unexpected_exceptions[:5]}",
        )
        self.assertGreaterEqual(
            value_errors_caught,
            990,
            f"Expected at least 990 payloads to raise ValueError, got {value_errors_caught}",
        )
        print(f"\n[EMPIRICAL VERIFIED] Fuzzed 1,000 payloads: {value_errors_caught} safely raised ValueError/JSONDecodeError; 0 crashes.")

    def test_adversarial_unhashable_event_type_behavior(self):
        """
        Adversarial Finding:
        When a valid JSON dict has an unhashable event_type (list or dict),
        AgentEvent.from_dict / from_ndjson currently raises TypeError because
        subclass_map.get() evaluates unhashable types as keys.
        """
        # Hashable non-string event_types fall back to AgentEvent safely:
        self.assertIsInstance(AgentEvent.from_ndjson('{"event_type": 123}\n'), AgentEvent)
        self.assertIsInstance(AgentEvent.from_ndjson('{"event_type": null}\n'), AgentEvent)

        # Unhashable list or dict triggers TypeError
        with self.assertRaises(TypeError):
            AgentEvent.from_ndjson('{"event_type": []}\n')

        with self.assertRaises(TypeError):
            AgentEvent.from_ndjson('{"event_type": {}}\n')

    async def test_pipe_stream_state_after_early_break(self):
        """
        Adversarial Finding:
        Early consumer break releases self._lock (zero deadlocks), but in a persistent
        duplex stream-json pipe, any unread events generated by Turn 1 remain in the OS
        stdout pipe buffer and are consumed on Turn 2 unless drained or reset.
        """
        mock_bin = os.path.abspath("tests/mock_agy_proc.py")
        adapter = HeadlessAgentAdapter(
            cwd="/Users/hermes/projects/first-mate",
            agy_bin=mock_bin,
            env={"MOCK_AGY_MODE": "normal"},
        )
        try:
            # Turn 1: consume 1 delta then break
            async for ev in adapter.send_turn("Prompt 1"):
                if isinstance(ev, TextDeltaEvent):
                    break

            # Turn 2: run full turn
            t2_events = []
            async for ev in adapter.send_turn("Prompt 2"):
                t2_events.append(ev)

            # Turn 2 completes with SUCCESS without deadlocking
            finishes = [e for e in t2_events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finishes), 1)
            self.assertEqual(finishes[0].status, "SUCCESS")
            # With _drain_stdout(), unconsumed output from Prompt 1 is purged,
            # ensuring finishes[0].response is 'Echo: Prompt 2' without phase shift.
            self.assertEqual(finishes[0].response, "Echo: Prompt 2")
        finally:
            await adapter.stop()


if __name__ == "__main__":
    unittest.main()

