#!/usr/bin/env python3
"""
tests/test_agent_m2.py
Comprehensive unit test suite for Milestone 2: Headless Agent Adapter & Event Protocol.

Covers:
1. Strongly typed AgentEvent hierarchy & NDJSON serialization/deserialization for all 7 event types.
2. Polymorphic deserialization and alias tolerance.
3. System prompt calibrations and rule extensions.
4. HeadlessAgentAdapter subprocess lifecycle and stream-json protocol.
5. Turn streaming: deltas, tool starts/ends, awaiting input, and finish events.
6. Error handling: abrupt process exit, broken pipe, malformed NDJSON, and error events.
7. Multi-turn conversation persistence over a warm subprocess.
8. Backwards-compatibility shim exports from agent_core.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import json
import time
import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

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
from golem.agent.adapter import HeadlessAgentAdapter, DEFAULT_AGY_BIN
from golem.agent.calibration import (
    CALIBRATION_PROMPT,
    SLACK_MRKDWN_RULES,
    CHOICE_FORMATTING_RULES,
    PACING_CALIBRATION_RULES,
    TRIAGE_SUPPRESSION_RULES,
    get_calibration_rules,
    build_calibrated_system_prompt,
)
import agent_core


class TestAgentEventProtocol(IsolatedAsyncioTestCase):
    """Test strongly typed AgentEvent hierarchy and NDJSON serialization/deserialization."""

    def test_base_agent_event_serialization(self):
        """Test base AgentEvent serialization and round-trip deserialization."""
        ts = time.time()
        event = AgentEvent(
            event_type="base",
            timestamp=ts,
            conversation_id="conv-101",
            step_index=1,
        )
        d = event.to_dict()
        self.assertEqual(d["event_type"], "base")
        self.assertEqual(d["conversation_id"], "conv-101")
        self.assertEqual(d["step_index"], 1)

        ndjson = event.to_ndjson()
        self.assertTrue(ndjson.endswith("\n"))

        deserialized = AgentEvent.from_ndjson(ndjson)
        self.assertIsInstance(deserialized, AgentEvent)
        self.assertEqual(deserialized.conversation_id, "conv-101")
        self.assertEqual(deserialized.step_index, 1)

    def test_text_delta_event(self):
        """Test TextDeltaEvent serialization, alias text property, and deserialization."""
        event = TextDeltaEvent(
            text_delta="Hello *world*!",
            conversation_id="conv-102",
            step_index=2,
        )
        self.assertEqual(event.event_type, "text_delta")
        self.assertEqual(event.text, "Hello *world*!")
        event.text = "Updated text"
        self.assertEqual(event.text_delta, "Updated text")

        ndjson = event.to_ndjson()
        parsed = AgentEvent.from_ndjson(ndjson)
        self.assertIsInstance(parsed, TextDeltaEvent)
        self.assertEqual(parsed.text_delta, "Updated text")

        # Tolerant alias: dict with 'text' instead of 'text_delta'
        from_alias = AgentEvent.from_dict({"event_type": "text_delta", "text": "Aliased"})
        self.assertIsInstance(from_alias, TextDeltaEvent)
        self.assertEqual(from_alias.text_delta, "Aliased")

    def test_status_update_event(self):
        """Test StatusUpdateEvent serialization and round-trip."""
        event = StatusUpdateEvent(
            status="working",
            detail="Compiling code",
            conversation_id="conv-103",
            step_index=3,
        )
        ndjson = event.to_ndjson()
        parsed = AgentEvent.from_ndjson(ndjson)
        self.assertIsInstance(parsed, StatusUpdateEvent)
        self.assertEqual(parsed.status, "working")
        self.assertEqual(parsed.detail, "Compiling code")
        self.assertEqual(parsed.conversation_id, "conv-103")

    def test_tool_start_event(self):
        """Test ToolStartEvent serialization and round-trip."""
        params = {"pane": "runner", "command": "python3 -m unittest"}
        event = ToolStartEvent(
            tool_name="herdr_pane_run",
            tool_call_id="call-555",
            parameters=params,
            conversation_id="conv-104",
            step_index=4,
        )
        ndjson = event.to_ndjson()
        parsed = AgentEvent.from_ndjson(ndjson)
        self.assertIsInstance(parsed, ToolStartEvent)
        self.assertEqual(parsed.tool_name, "herdr_pane_run")
        self.assertEqual(parsed.tool_call_id, "call-555")
        self.assertEqual(parsed.parameters, params)

    def test_tool_end_event(self):
        """Test ToolEndEvent serialization and round-trip with duration and error flag."""
        event = ToolEndEvent(
            tool_name="herdr_pane_run",
            tool_call_id="call-555",
            output={"exit_code": 0, "stdout": "OK"},
            duration_seconds=1.45,
            is_error=False,
            conversation_id="conv-104",
            step_index=5,
        )
        ndjson = event.to_ndjson()
        parsed = AgentEvent.from_ndjson(ndjson)
        self.assertIsInstance(parsed, ToolEndEvent)
        self.assertEqual(parsed.tool_name, "herdr_pane_run")
        self.assertEqual(parsed.duration_seconds, 1.45)
        self.assertFalse(parsed.is_error)
        self.assertEqual(parsed.output, {"exit_code": 0, "stdout": "OK"})

    def test_awaiting_input_event(self):
        """Test AwaitingInputEvent serialization and round-trip."""
        event = AwaitingInputEvent(
            prompt="Do you approve deploying to prod?",
            choices=["1. Yes", "2. No"],
            tool_name="ask_permission",
            conversation_id="conv-105",
            step_index=6,
        )
        ndjson = event.to_ndjson()
        parsed = AgentEvent.from_ndjson(ndjson)
        self.assertIsInstance(parsed, AwaitingInputEvent)
        self.assertEqual(parsed.prompt, "Do you approve deploying to prod?")
        self.assertEqual(parsed.choices, ["1. Yes", "2. No"])
        self.assertEqual(parsed.tool_name, "ask_permission")

    def test_finish_event_success_and_error(self):
        """Test FinishEvent for both success and error outcomes."""
        success_event = FinishEvent(
            response="Done! Everything passes.",
            status="SUCCESS",
            duration_seconds=3.12,
            usage={"input_tokens": 150, "output_tokens": 42},
            conversation_id="conv-106",
            step_index=7,
        )
        parsed_success = AgentEvent.from_ndjson(success_event.to_ndjson())
        self.assertIsInstance(parsed_success, FinishEvent)
        self.assertEqual(parsed_success.status, "SUCCESS")
        self.assertEqual(parsed_success.response, "Done! Everything passes.")
        self.assertEqual(parsed_success.usage, {"input_tokens": 150, "output_tokens": 42})
        self.assertIsNone(parsed_success.error)

        error_event = FinishEvent(
            response="",
            status="ERROR",
            error="Connection timed out",
            conversation_id="conv-106",
        )
        parsed_error = AgentEvent.from_ndjson(error_event.to_ndjson())
        self.assertIsInstance(parsed_error, FinishEvent)
        self.assertEqual(parsed_error.status, "ERROR")
        self.assertEqual(parsed_error.error, "Connection timed out")

    def test_all_seven_event_types_polymorphic_deserialization(self):
        """Verify AgentEvent.from_dict polymorphic dispatch handles all 7 distinct types."""
        samples = [
            ({"event_type": "base"}, AgentEvent),
            ({"event_type": "text_delta", "text_delta": "delta"}, TextDeltaEvent),
            ({"event_type": "status_update", "status": "idle"}, StatusUpdateEvent),
            ({"event_type": "tool_start", "tool_name": "t1"}, ToolStartEvent),
            ({"event_type": "tool_end", "tool_name": "t1", "output": "out"}, ToolEndEvent),
            ({"event_type": "awaiting_input", "prompt": "q"}, AwaitingInputEvent),
            ({"event_type": "finish", "response": "r", "status": "SUCCESS"}, FinishEvent),
        ]
        for data, expected_cls in samples:
            inst = AgentEvent.from_dict(data)
            self.assertIsInstance(inst, expected_cls)
            self.assertEqual(inst.event_type, data["event_type"])

    def test_from_dict_and_ndjson_invalid_payload_raises_value_error(self):
        """Verify non-dict payload raises ValueError."""
        with self.assertRaises(ValueError):
            AgentEvent.from_dict([1, 2, 3])  # type: ignore
        with self.assertRaises(ValueError):
            AgentEvent.from_dict("string")  # type: ignore
        with self.assertRaises(ValueError):
            AgentEvent.from_dict(None)  # type: ignore
        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson("[1, 2, 3]\n")
        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson("42\n")
        with self.assertRaises(ValueError):
            AgentEvent.from_ndjson('"raw string"\n')


class TestPromptCalibrations(IsolatedAsyncioTestCase):
    """Test system prompt calibration texts and builders."""

    def test_calibration_rules_contain_all_required_constraints(self):
        """Verify calibration text covers mrkdwn, numbered choices, pacing, and triage suppression."""
        rules = get_calibration_rules()
        # 1. Slack mrkdwn
        self.assertIn("Section headers: Use *Bold Title*", rules)
        self.assertIn("Bold text: Use *single asterisks*", rules)
        self.assertIn("never **double asterisks**", rules)
        self.assertIn("Links: Use <URL|Label>", rules)

        # 2. Numbered choices
        self.assertIn("No Interactive Terminal Menus", rules)
        self.assertIn("numbered markdown list", rules)

        # 3. Conversational pacing
        self.assertIn("Broad Overview First", rules)
        self.assertIn("Natural Voice & Clean Responses", rules)
        self.assertIn("Direct Handling for Greetings & Banter", rules)
        self.assertIn("Fleet Status Queries", rules)

        # 4. Suppression of unsolicited incident triage
        self.assertIn("Suppression of Unsolicited Incident Triage", rules)
        self.assertIn("Never autonomously scrape system logs", rules)

    def test_build_calibrated_system_prompt_defaults(self):
        """Verify build_calibrated_system_prompt builds complete prompt with topology and calibration."""
        prompt = build_calibrated_system_prompt()
        self.assertIn("First Mate", prompt)
        self.assertIn("hermes", prompt)
        self.assertIn("desktop-pc", prompt)
        self.assertIn("oci-machine", prompt)
        self.assertIn("personal-laptop", prompt)
        self.assertIn(CALIBRATION_PROMPT, prompt)

    def test_build_calibrated_system_prompt_custom_base(self):
        """Verify build_calibrated_system_prompt appends calibration to custom base prompt."""
        base = "You are CustomBot."
        prompt = build_calibrated_system_prompt(base_prompt=base, extra_instructions="Be concise.")
        self.assertTrue(prompt.startswith("You are CustomBot."))
        self.assertIn(CALIBRATION_PROMPT, prompt)
        self.assertIn("Be concise.", prompt)
        self.assertIn("--- Extra Instructions ---", prompt)

    def test_build_calibrated_system_prompt_whitespace_extra_skipped(self):
        """Verify whitespace-only extra_instructions does not append an empty section."""
        prompt = build_calibrated_system_prompt(extra_instructions="   \n\t ")
        self.assertNotIn("--- Extra Instructions ---", prompt)


class FakeStreamProcess:
    """Mock asyncio subprocess with simulated stdin/stdout pipes."""

    def __init__(self, init_line: str, stdout_lines: list[str], returncode: int = 0):
        self.init_line = init_line
        self.stdout_lines = list(stdout_lines)
        self.returncode: Optional[int] = None
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
            return self.init_line.encode("utf-8") + b"\n"
        if self.stdout_lines:
            line = self.stdout_lines.pop(0)
            return line.encode("utf-8") + b"\n"
        # Simulate EOF and process exit
        self.returncode = self._target_returncode
        return b""

    async def wait(self) -> int:
        self.returncode = self._target_returncode
        return self._target_returncode

    def terminate(self):
        self.returncode = 0

    def kill(self):
        self.returncode = -9


class TestHeadlessAgentAdapter(IsolatedAsyncioTestCase):
    """Test HeadlessAgentAdapter lifecycle, turn execution, and streaming."""

    async def test_start_and_init_capture(self):
        """Verify startup command, arguments, and capture of conversation_id and available tools."""
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "conv-test-999",
            "init": {
                "cwd": "/Users/hermes/projects/AGY",
                "tools": ["herdr_pane_run", "herdr_agent_read", "run_command"],
            },
        })
        mock_proc = FakeStreamProcess(init_line=init_json, stdout_lines=[])

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(
                cwd="/Users/hermes/projects/AGY",
                extra_args=["--extra-flag"],
            )
            await adapter.start()

            # Verify subprocess call
            mock_exec.assert_called_once()
            args = mock_exec.call_args[0]
            self.assertEqual(args[0], DEFAULT_AGY_BIN)
            self.assertIn("--input-format", args)
            self.assertIn("stream-json", args)
            self.assertIn("--output-format", args)
            self.assertIn("--dangerously-skip-permissions", args)
            self.assertIn("--extra-flag", args)

            # Verify attributes captured from init
            self.assertEqual(adapter.conversation_id, "conv-test-999")
            self.assertEqual(len(adapter.available_tools), 3)
            self.assertIn("herdr_pane_run", adapter.available_tools)
            self.assertTrue(adapter.is_alive)

            await adapter.stop()

    async def test_send_turn_successful_streaming(self):
        """Verify user turn writing, delta emission, tool execution events, and finish result."""
        init_json = json.dumps({
            "event": "init",
            "conversation_id": "conv-stream-1",
            "init": {"tools": ["herdr_pane_run"]},
        })
        stream_lines = [
            # 1. Delta
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Checking fleet status...",
                },
            }),
            # 2. Tool call start
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "step_index": 2,
                    "tool_name": "herdr_pane_run",
                    "tool_call_id": "c-1",
                    "tool_info": {"parameters": {"command": "tailscale status"}},
                },
            }),
            # 3. Tool call end
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "step_index": 2,
                    "tool_name": "herdr_pane_run",
                    "tool_call_id": "c-1",
                    "duration_seconds": 0.8,
                    "tool_info": {"output": "100.80.59.45 desktop-pc"},
                },
            }),
            # 4. Final delta
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 3,
                    "text_delta": " Fleet is 100% online.",
                },
            }),
            # 5. Result
            json.dumps({
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": "Checking fleet status... Fleet is 100% online.",
                    "duration_seconds": 1.2,
                    "usage": {"input_tokens": 80, "output_tokens": 30},
                },
            }),
        ]
        mock_proc = FakeStreamProcess(init_line=init_json, stdout_lines=stream_lines)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/AGY")

            events = []
            async for ev in adapter.send_turn("How's our fleet doing?"):
                events.append(ev)

            # Check that stdin received the NDJSON user event
            self.assertTrue(len(mock_proc.stdin_written) > 0)
            user_msg = json.loads(mock_proc.stdin_written[0].decode("utf-8").strip())
            self.assertEqual(user_msg["event"], "user")
            self.assertEqual(user_msg["message"]["content"], "How's our fleet doing?")

            # Inspect emitted event sequence
            event_types = [e.event_type for e in events]
            self.assertEqual(
                event_types,
                ["status_update", "text_delta", "tool_start", "tool_end", "text_delta", "finish"],
            )

            # Check details
            self.assertEqual(events[1].text_delta, "Checking fleet status...")
            self.assertEqual(events[2].tool_name, "herdr_pane_run")
            self.assertEqual(events[3].duration_seconds, 0.8)
            self.assertEqual(events[4].text_delta, " Fleet is 100% online.")

            finish_ev = events[5]
            self.assertEqual(finish_ev.status, "SUCCESS")
            self.assertEqual(finish_ev.response, "Checking fleet status... Fleet is 100% online.")
            self.assertEqual(finish_ev.usage["input_tokens"], 80)

            await adapter.stop()

    async def test_awaiting_input_event_emission(self):
        """Verify ask_question / ask_permission tool calls emit AwaitingInputEvent."""
        init_json = json.dumps({"event": "init", "conversation_id": "c-ask", "init": {}})
        stream_lines = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "step_index": 1,
                    "tool_name": "ask_permission",
                    "tool_info": {
                        "parameters": {
                            "question": "Deploy database migrations?",
                            "choices": ["1. Approve", "2. Abort"],
                        },
                        "output": None,
                    },
                },
            }),
            json.dumps({
                "event": "result",
                "result": {"status": "SUCCESS", "response": "Waiting for your answer."},
            }),
        ]
        mock_proc = FakeStreamProcess(init_line=init_json, stdout_lines=stream_lines)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/AGY")

            events = [e async for e in adapter.send_turn("Run migration")]
            input_events = [e for e in events if isinstance(e, AwaitingInputEvent)]
            self.assertEqual(len(input_events), 1)
            self.assertEqual(input_events[0].prompt, "Deploy database migrations?")
            self.assertEqual(input_events[0].choices, ["1. Approve", "2. Abort"])
            self.assertEqual(input_events[0].tool_name, "ask_permission")
            await adapter.stop()

    async def test_abrupt_process_crash_handling(self):
        """Verify sudden process death / stdout EOF produces FinishEvent(status='ERROR')."""
        init_json = json.dumps({"event": "init", "conversation_id": "c-crash", "init": {}})
        # Subprocess produces one delta then dies abruptly (stdout EOF without result)
        stream_lines = [
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Processing...",
                },
            }),
        ]
        mock_proc = FakeStreamProcess(init_line=init_json, stdout_lines=stream_lines, returncode=137)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/AGY")

            events = [e async for e in adapter.send_turn("Crash test")]
            last_ev = events[-1]
            self.assertIsInstance(last_ev, FinishEvent)
            self.assertEqual(last_ev.status, "ERROR")
            self.assertIn("Process terminated unexpectedly", last_ev.error)
            await adapter.stop()

    async def test_broken_pipe_error_handling(self):
        """Verify BrokenPipeError on stdin write yields FinishEvent(status='ERROR') cleanly."""
        init_json = json.dumps({"event": "init", "conversation_id": "c-pipe", "init": {}})
        mock_proc = FakeStreamProcess(init_line=init_json, stdout_lines=[])
        mock_proc.stdin.write = MagicMock(side_effect=BrokenPipeError("Pipe broken"))

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/AGY")

            events = [e async for e in adapter.send_turn("Hello")]
            self.assertEqual(len(events), 1)
            self.assertIsInstance(events[0], FinishEvent)
            self.assertEqual(events[0].status, "ERROR")
            self.assertIn("Broken pipe", events[0].error)
            await adapter.stop()

    async def test_malformed_ndjson_skipped_gracefully(self):
        """Verify corrupt/malformed NDJSON lines are safely skipped without interrupting stream."""
        init_json = json.dumps({"event": "init", "conversation_id": "c-malformed", "init": {}})
        stream_lines = [
            "NOT_JSON_AT_ALL {{{",
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Valid token",
                },
            }),
            "",  # empty line
            json.dumps({
                "event": "result",
                "result": {"status": "SUCCESS", "response": "Valid token"},
            }),
        ]
        mock_proc = FakeStreamProcess(init_line=init_json, stdout_lines=stream_lines)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/AGY")

            events = [e async for e in adapter.send_turn("Test malformed")]
            text_deltas = [e.text_delta for e in events if isinstance(e, TextDeltaEvent)]
            self.assertEqual(text_deltas, ["Valid token"])
            finish_events = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finish_events), 1)
            self.assertEqual(finish_events[0].status, "SUCCESS")
            await adapter.stop()

    async def test_non_dict_and_null_subfields_gracefully_handled(self):
        """Verify non-dict JSON lines and null subfields are safely tolerated."""
        init_json = json.dumps({"event": "init", "conversation_id": "c-nulls", "init": None})
        stream_lines = [
            "[1, 2, 3]",
            '"hello"',
            "42",
            "null",
            json.dumps({"event": "step_update", "step_update": None}),
            json.dumps({"event": "step_update", "step_update": {"step_type": "tool", "tool_info": None}}),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "ACTIVE",
                    "tool_name": "ask_permission",
                    "tool_info": {"parameters": None},
                },
            }),
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "tool",
                    "state": "DONE",
                    "tool_name": "ask_permission",
                    "tool_info": {"parameters": None},
                },
            }),
            json.dumps({"event": "awaiting_input", "awaiting_input": None}),
            json.dumps({"event": "result", "result": None}),
        ]
        mock_proc = FakeStreamProcess(init_line=init_json, stdout_lines=stream_lines)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/AGY")

            events = [e async for e in adapter.send_turn("Test nulls")]
            finish_events = [e for e in events if isinstance(e, FinishEvent)]
            self.assertEqual(len(finish_events), 1)
            self.assertEqual(finish_events[0].status, "SUCCESS")
            await adapter.stop()

    async def test_multi_turn_sequence_over_warm_subprocess(self):
        """Verify multiple turns run against the same persistent subprocess."""
        init_json = json.dumps({"event": "init", "conversation_id": "c-multi", "init": {}})
        # Turn 1 lines + Turn 2 lines
        stream_lines = [
            # Turn 1
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Turn 1 answer",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {"status": "SUCCESS", "response": "Turn 1 answer"},
            }),
            # Turn 2
            json.dumps({
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 2,
                    "text_delta": "Turn 2 follow-up answer",
                },
            }),
            json.dumps({
                "event": "result",
                "result": {"status": "SUCCESS", "response": "Turn 2 follow-up answer"},
            }),
        ]
        mock_proc = FakeStreamProcess(init_line=init_json, stdout_lines=stream_lines)

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_proc
            adapter = HeadlessAgentAdapter(cwd="/Users/hermes/projects/AGY")

            # Turn 1
            t1_events = [e async for e in adapter.send_turn("Turn 1 prompt")]
            t1_finish = [e for e in t1_events if isinstance(e, FinishEvent)][0]
            self.assertEqual(t1_finish.response, "Turn 1 answer")

            # Subprocess should still be alive
            self.assertTrue(adapter.is_alive)
            self.assertEqual(mock_exec.call_count, 1)

            # Turn 2
            t2_events = [e async for e in adapter.send_turn("Turn 2 prompt")]
            t2_finish = [e for e in t2_events if isinstance(e, FinishEvent)][0]
            self.assertEqual(t2_finish.response, "Turn 2 follow-up answer")

            # Subprocess was not respawned
            self.assertEqual(mock_exec.call_count, 1)

            # Verify both prompts written to stdin
            self.assertEqual(len(mock_proc.stdin_written), 2)
            p1 = json.loads(mock_proc.stdin_written[0].decode("utf-8").strip())
            p2 = json.loads(mock_proc.stdin_written[1].decode("utf-8").strip())
            self.assertEqual(p1["message"]["content"], "Turn 1 prompt")
            self.assertEqual(p2["message"]["content"], "Turn 2 prompt")

            await adapter.stop()


class TestAgentCoreShim(IsolatedAsyncioTestCase):
    """Verify backwards-compatibility shim in agent_core re-exports M2 components cleanly."""

    def test_agent_core_reexports(self):
        """Confirm all M2 classes and functions are accessible via agent_core."""
        self.assertTrue(hasattr(agent_core, "AgentEvent"))
        self.assertTrue(hasattr(agent_core, "TextDeltaEvent"))
        self.assertTrue(hasattr(agent_core, "StatusUpdateEvent"))
        self.assertTrue(hasattr(agent_core, "ToolStartEvent"))
        self.assertTrue(hasattr(agent_core, "ToolEndEvent"))
        self.assertTrue(hasattr(agent_core, "AwaitingInputEvent"))
        self.assertTrue(hasattr(agent_core, "FinishEvent"))
        self.assertTrue(hasattr(agent_core, "HeadlessAgentAdapter"))
        self.assertTrue(hasattr(agent_core, "CALIBRATION_PROMPT"))
        self.assertTrue(hasattr(agent_core, "build_calibrated_system_prompt"))

    def test_agent_core_legacy_remains_intact(self):
        """Confirm legacy ChiefOfStaff, build_system_prompt, and format_for_slack are intact."""
        self.assertTrue(hasattr(agent_core, "ChiefOfStaff"))
        self.assertTrue(hasattr(agent_core, "build_system_prompt"))
        self.assertTrue(hasattr(agent_core, "format_for_slack"))
        prompt = agent_core.build_system_prompt()
        self.assertIn("Chief of Staff", prompt)
        formatted = agent_core.format_for_slack("**test**")
        self.assertEqual(formatted, "*test*")

    def test_module_shims_events_and_prompts(self):
        """Verify golem.agent.events and golem.agent.prompts shims match PROJECT.md."""
        import golem.agent.events as events_mod
        import golem.agent.prompts as prompts_mod
        self.assertTrue(hasattr(events_mod, "AgentEvent"))
        self.assertTrue(hasattr(events_mod, "TextDeltaEvent"))
        self.assertTrue(hasattr(events_mod, "StatusUpdateEvent"))
        self.assertTrue(hasattr(events_mod, "ToolStartEvent"))
        self.assertTrue(hasattr(events_mod, "ToolEndEvent"))
        self.assertTrue(hasattr(events_mod, "AwaitingInputEvent"))
        self.assertTrue(hasattr(events_mod, "FinishEvent"))
        self.assertTrue(hasattr(prompts_mod, "CALIBRATION_PROMPT"))
        self.assertTrue(hasattr(prompts_mod, "build_calibrated_system_prompt"))
        self.assertTrue(hasattr(prompts_mod, "get_calibration_rules"))
