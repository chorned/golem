#!/usr/bin/env python3
"""
tests/test_agent_drivers_adversarial.py
Adversarial stress harness and empirical challenge suite for Milestone 2:
CLI Portability Driver Architecture (BaseAgentDriver, AntigravityDriver,
ClaudeCodeDriver, OpenCodeDriver, create_driver, config, and router integration).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import json
import time
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncGenerator, List, Dict, Optional, Any

from golem.config import (
    get_active_engine,
    get_engine_model,
    get_mouth_model,
    get_eyes_model,
    get_nose_model,
    get_config_dict,
)
from golem.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    FinishEvent,
)
from golem.agent.drivers.base import BaseAgentDriver
from golem.agent.drivers.antigravity import AntigravityDriver
from golem.agent.drivers.claude_code import ClaudeCodeDriver
from golem.agent.drivers.opencode import OpenCodeDriver
from golem.agent.drivers import (
    DRIVER_REGISTRY,
    get_driver_class,
    register_driver,
    create_driver,
)
from golem.agent.adapter import HeadlessAgentAdapter
from golem.gateway.session import SessionHandle, SessionState
from golem.gateway.router import SessionRouter


class DummyConcreteDriver(BaseAgentDriver):
    """Fully concrete driver for shim & contract testing."""

    def __init__(self, cwd: str, model: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(cwd=cwd, model=model, **kwargs)
        self._alive = False
        self.start_called = 0
        self.stop_called = 0
        self.prompt_history: List[str] = []

    @property
    def engine_name(self) -> str:
        return "dummy_concrete"

    @property
    def is_alive(self) -> bool:
        return self._alive

    async def start_session(self) -> None:
        self.start_called += 1
        self._alive = True
        self.conversation_id = f"conv-{self.start_called}"

    async def send_prompt(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        self.prompt_history.append(prompt)
        yield StatusUpdateEvent(status="working", detail="working on prompt")
        yield TextDeltaEvent(text=f"Ack: {prompt}")
        yield FinishEvent(status="SUCCESS", response=f"Ack: {prompt}")

    async def terminate_session(self) -> None:
        self.stop_called += 1
        self._alive = False


class TestBaseAgentDriverAdversarialContract(unittest.TestCase):
    """Adversarial challenge to BaseAgentDriver ABC enforcement and shims."""

    def test_abc_missing_engine_name_fails_instantiation(self) -> None:
        class IncompleteDriver(BaseAgentDriver):
            @property
            def is_alive(self) -> bool:
                return False
            async def start_session(self) -> None: pass
            async def send_prompt(self, p: str): yield FinishEvent()
            async def terminate_session(self) -> None: pass

        with self.assertRaises(TypeError) as ctx:
            IncompleteDriver(cwd="/tmp")
        self.assertIn("engine_name", str(ctx.exception))

    def test_abc_missing_is_alive_fails_instantiation(self) -> None:
        class IncompleteDriver(BaseAgentDriver):
            @property
            def engine_name(self) -> str: return "inc"
            async def start_session(self) -> None: pass
            async def send_prompt(self, p: str): yield FinishEvent()
            async def terminate_session(self) -> None: pass

        with self.assertRaises(TypeError) as ctx:
            IncompleteDriver(cwd="/tmp")
        self.assertIn("is_alive", str(ctx.exception))

    def test_abc_missing_start_session_fails_instantiation(self) -> None:
        class IncompleteDriver(BaseAgentDriver):
            @property
            def engine_name(self) -> str: return "inc"
            @property
            def is_alive(self) -> bool: return False
            async def send_prompt(self, p: str): yield FinishEvent()
            async def terminate_session(self) -> None: pass

        with self.assertRaises(TypeError) as ctx:
            IncompleteDriver(cwd="/tmp")
        self.assertIn("start_session", str(ctx.exception))

    def test_abc_missing_send_prompt_fails_instantiation(self) -> None:
        class IncompleteDriver(BaseAgentDriver):
            @property
            def engine_name(self) -> str: return "inc"
            @property
            def is_alive(self) -> bool: return False
            async def start_session(self) -> None: pass
            async def terminate_session(self) -> None: pass

        with self.assertRaises(TypeError) as ctx:
            IncompleteDriver(cwd="/tmp")
        self.assertIn("send_prompt", str(ctx.exception))

    def test_abc_missing_terminate_session_fails_instantiation(self) -> None:
        class IncompleteDriver(BaseAgentDriver):
            @property
            def engine_name(self) -> str: return "inc"
            @property
            def is_alive(self) -> bool: return False
            async def start_session(self) -> None: pass
            async def send_prompt(self, p: str): yield FinishEvent()

        with self.assertRaises(TypeError) as ctx:
            IncompleteDriver(cwd="/tmp")
        self.assertIn("terminate_session", str(ctx.exception))

    def test_defensive_copies_on_init(self) -> None:
        external_args = ["--arg1", "--arg2"]
        external_env = {"K1": "V1", "K2": "V2"}
        driver = DummyConcreteDriver(cwd="/tmp", extra_args=external_args, env=external_env)

        # Mutate external collections
        external_args.append("--arg3")
        external_env["K3"] = "V3"

        self.assertEqual(driver.extra_args, ["--arg1", "--arg2"])
        self.assertEqual(driver.env, {"K1": "V1", "K2": "V2"})

    def test_repr_safety_under_various_conditions(self) -> None:
        driver = DummyConcreteDriver(cwd="/var/log", model=None)
        r = repr(driver)
        self.assertIn("DummyConcreteDriver", r)
        self.assertIn("dummy_concrete", r)
        self.assertIn("alive=False", r)

        driver.model = "my-test-model"
        self.assertIn("model=my-test-model", repr(driver))


class TestBaseAgentDriverShimsAdversarial(unittest.IsolatedAsyncioTestCase):
    """Test compatibility shims under normal and error conditions."""

    async def test_shims_faithfully_wrap_methods(self) -> None:
        driver = DummyConcreteDriver(cwd="/tmp")

        # start shim
        await driver.start()
        self.assertEqual(driver.start_called, 1)
        self.assertTrue(driver.is_alive)

        # send_turn shim
        events = [ev async for ev in driver.send_turn("turn test")]
        self.assertEqual(len(events), 3)
        self.assertEqual(driver.prompt_history[-1], "turn test")

        # stream_turn shim
        events2 = [ev async for ev in driver.stream_turn("stream test")]
        self.assertEqual(len(events2), 3)
        self.assertEqual(driver.prompt_history[-1], "stream test")

        # stop shim
        await driver.stop()
        self.assertEqual(driver.stop_called, 1)
        self.assertFalse(driver.is_alive)

    async def test_exception_propagation_through_shims(self) -> None:
        class FaultyDriver(DummyConcreteDriver):
            async def start_session(self) -> None:
                raise ConnectionError("Start failed")
            async def terminate_session(self) -> None:
                raise RuntimeError("Stop failed")
            async def send_prompt(self, p: str):
                yield StatusUpdateEvent(status="working")
                raise ValueError("Stream failed")

        driver = FaultyDriver(cwd="/tmp")

        with self.assertRaises(ConnectionError):
            await driver.start()

        with self.assertRaises(RuntimeError):
            await driver.stop()

        gen = driver.send_turn("test")
        ev1 = await gen.__anext__()
        self.assertIsInstance(ev1, StatusUpdateEvent)
        with self.assertRaises(ValueError):
            await gen.__anext__()


class TestAntigravityDriverCommandLineAdversarial(unittest.TestCase):
    """Stress test AntigravityDriver command-line construction."""

    def test_command_line_model_injection(self) -> None:
        # 1. Normal model
        driver = AntigravityDriver(cwd="/tmp", model="gemini-3.8-flash-high")
        cmd = driver._build_cmd()
        self.assertIn("--model", cmd)
        idx = cmd.index("--model")
        self.assertEqual(cmd[idx + 1], "gemini-3.8-flash-high")

        # 2. Empty model string -> no --model flag
        driver_empty = AntigravityDriver(cwd="/tmp", model="")
        cmd_empty = driver_empty._build_cmd()
        self.assertNotIn("--model", cmd_empty)

        # 3. Model already in extra_args -> no duplication
        driver_dup = AntigravityDriver(
            cwd="/tmp",
            model="gemini-3.8-flash-high",
            extra_args=["--verbose", "--model", "custom-model", "--json"],
        )
        cmd_dup = driver_dup._build_cmd()
        self.assertEqual(cmd_dup.count("--model"), 1)
        idx2 = cmd_dup.index("--model")
        self.assertEqual(cmd_dup[idx2 + 1], "custom-model")

    def test_dynamic_model_fallback_on_init(self) -> None:
        with patch.dict(os.environ, {"ANTIGRAVITY_MODEL": "gemini-test-fallback"}):
            with patch("golem.config.get_engine_model", return_value="gemini-test-config"):
                driver = AntigravityDriver(cwd="/tmp", model=None)
                self.assertEqual(driver.model, "gemini-test-config")

        # When config raises or missing
        with patch.dict(os.environ, {"ANTIGRAVITY_MODEL": "gemini-test-env-fallback"}):
            with patch("golem.config.get_engine_model", side_effect=ImportError("No config")):
                driver2 = AntigravityDriver(cwd="/tmp", model=None)
                self.assertEqual(driver2.model, "gemini-test-env-fallback")

    def test_extra_args_special_characters_and_ordering(self) -> None:
        args = ["--prompt", "hello world; rm -rf /", "--config", '{"key": "value"}']
        driver = AntigravityDriver(cwd="/tmp", model="gemini-3.8", extra_args=args)
        cmd = driver._build_cmd()
        # Verify base args come first
        self.assertEqual(cmd[1:5], ["--input-format", "stream-json", "--output-format", "stream-json"])
        # Verify extra args come at the end
        self.assertEqual(cmd[-4:], args)


class TestAntigravityDriverLifecycleAdversarial(unittest.IsolatedAsyncioTestCase):
    """Stress test AntigravityDriver subprocess lifecycle, rapid recycling, and pipe errors."""

    async def test_start_session_when_already_alive_is_noop(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")
        mock_proc = MagicMock()
        mock_proc.returncode = None
        driver.process = mock_proc

        self.assertTrue(driver.is_alive)
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            await driver.start_session()
            mock_exec.assert_not_called()

    async def test_premature_exit_on_start_raises_and_cleans_up(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        # stdout.readline returns empty b"" (EOF)
        mock_proc.stdout.readline = AsyncMock(return_value=b"")
        mock_proc.stderr.read = AsyncMock(return_value=b"FATAL: binary corrupted")
        mock_proc.wait = AsyncMock(return_value=1)
        mock_proc.stdin.is_closing.return_value = False

        with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc)):
            with self.assertRaises(RuntimeError) as ctx:
                await driver.start_session()
            self.assertIn("exited prematurely", str(ctx.exception))
            self.assertIn("FATAL: binary corrupted", str(ctx.exception))
            self.assertIsNone(driver.process)
            self.assertFalse(driver.is_alive)

    async def test_timeout_on_start_raises_and_cleans_up(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")
        mock_proc = MagicMock()
        mock_proc.returncode = None

        async def slow_readline():
            await asyncio.sleep(10.0)
            return b""

        mock_proc.stdout.readline = slow_readline
        mock_proc.wait = AsyncMock(return_value=0)
        mock_proc.stdin.is_closing.return_value = False

        with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc)):
            with patch.dict(os.environ, {"AGY_INIT_TIMEOUT": "0.05"}):
                with self.assertRaises(RuntimeError) as ctx:
                    await driver.start_session()
                self.assertIn("Timed out waiting for init event", str(ctx.exception))
                self.assertIsNone(driver.process)

    async def test_idempotent_termination(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")
        # Multiple terminations on dead driver do not fail
        await driver.terminate_session()
        await driver.terminate_session()
        await driver.terminate_session()
        self.assertFalse(driver.is_alive)

    async def test_broken_pipe_on_stdin_write_yields_error_event(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")
        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.stdin.write.side_effect = BrokenPipeError("Broken pipe")
        mock_proc.wait = AsyncMock(return_value=1)
        mock_proc.stdin.is_closing.return_value = False
        driver.process = mock_proc

        events: List[AgentEvent] = []
        async for ev in driver.send_prompt("test broken pipe"):
            events.append(ev)

        self.assertEqual(len(events), 1)
        finish_ev = events[0]
        self.assertIsInstance(finish_ev, FinishEvent)
        self.assertEqual(finish_ev.status, "ERROR")
        self.assertIn("Broken pipe", str(finish_ev.error))
        self.assertIsNone(driver.process)

    async def test_rapid_start_terminate_recycle(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")

        for i in range(5):
            mock_proc = MagicMock()
            mock_proc.returncode = None
            init_json = json.dumps({"event": "init", "conversation_id": f"conv-{i}", "init": {"tools": ["bash"]}}).encode() + b"\n"
            mock_proc.stdout.readline = AsyncMock(return_value=init_json)
            mock_proc.wait = AsyncMock(return_value=0)
            mock_proc.stdin.is_closing.return_value = False

            with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc)):
                await driver.start_session()
                self.assertTrue(driver.is_alive)
                self.assertEqual(driver.conversation_id, f"conv-{i}")
                await driver.terminate_session()
                self.assertFalse(driver.is_alive)
                self.assertIsNone(driver.process)


    async def test_stderr_drain_ring_buffer_under_flood(self) -> None:
        """Simulate high stderr volume (>150 lines); verify deque maintains maxlen without leak."""
        driver = AntigravityDriver(cwd="/tmp")
        mock_proc = MagicMock()
        mock_proc.returncode = None
        driver.process = mock_proc

        # Create async stream reader for stderr that yields 150 lines
        lines = [f"Warning line {i}\n".encode() for i in range(150)] + [b""]
        mock_proc.stderr.readline = AsyncMock(side_effect=lines)

        await driver._drain_stderr()
        self.assertEqual(len(driver._stderr_lines), 100)
        self.assertEqual(driver._stderr_lines[-1], "Warning line 149")
        self.assertEqual(driver._stderr_lines[0], "Warning line 50")


class TestClaudeCodeAndOpenCodeDriversAdversarial(unittest.IsolatedAsyncioTestCase):
    """Stress test ClaudeCodeDriver and OpenCodeDriver stubs."""

    async def test_claude_driver_auto_start_and_event_types(self) -> None:
        driver = ClaudeCodeDriver(cwd="/tmp", model="claude-3-7-sonnet-latest")
        self.assertFalse(driver.is_alive)

        events: List[AgentEvent] = []
        async for ev in driver.send_prompt("Adversarial prompt"):
            events.append(ev)

        self.assertTrue(driver.is_alive)
        self.assertEqual(len(events), 3)

        # Event 1: StatusUpdateEvent
        self.assertIsInstance(events[0], StatusUpdateEvent)
        self.assertEqual(events[0].event_type, "status_update")
        self.assertEqual(events[0].status, "working")

        # Event 2: TextDeltaEvent
        self.assertIsInstance(events[1], TextDeltaEvent)
        self.assertEqual(events[1].event_type, "text_delta")
        self.assertIn("Acknowledged task: Adversarial prompt", events[1].text)
        self.assertIn("Acknowledged task: Adversarial prompt", events[1].text_delta)

        # Event 3: FinishEvent
        self.assertIsInstance(events[2], FinishEvent)
        self.assertEqual(events[2].event_type, "finish")
        self.assertEqual(events[2].status, "SUCCESS")

        await driver.terminate_session()
        self.assertFalse(driver.is_alive)

    async def test_opencode_driver_auto_start_and_event_types(self) -> None:
        driver = OpenCodeDriver(cwd="/tmp", model="deepseek/deepseek-chat")
        self.assertFalse(driver.is_alive)

        events: List[AgentEvent] = []
        async for ev in driver.send_prompt("Refactor codebase"):
            events.append(ev)

        self.assertTrue(driver.is_alive)
        self.assertEqual(len(events), 3)
        self.assertIsInstance(events[0], StatusUpdateEvent)
        self.assertIsInstance(events[1], TextDeltaEvent)
        self.assertIsInstance(events[2], FinishEvent)
        self.assertIn("Refactor codebase", events[2].response)

        await driver.terminate_session()
        self.assertFalse(driver.is_alive)

    async def test_conversation_id_persists_across_multiple_turns(self) -> None:
        driver = ClaudeCodeDriver(cwd="/tmp")
        await driver.start_session()
        conv1 = driver.conversation_id
        self.assertIsNotNone(conv1)

        async for _ in driver.send_prompt("Turn 1"): pass
        self.assertEqual(driver.conversation_id, conv1)

        async for _ in driver.send_prompt("Turn 2"): pass
        self.assertEqual(driver.conversation_id, conv1)

        await driver.terminate_session()


class TestDriverRegistryAndFactoryAdversarial(unittest.TestCase):
    """Stress test driver registration, alias resolution, and invalid engine errors."""

    def test_register_driver_validation(self) -> None:
        # Non-class
        with self.assertRaises(TypeError):
            register_driver("invalid", "not_a_class")  # type: ignore

        # Class not subclassing BaseAgentDriver
        class NotADriver:
            pass

        with self.assertRaises(TypeError):
            register_driver("invalid", NotADriver)  # type: ignore

        # Valid registration
        register_driver("test_custom", DummyConcreteDriver)
        self.assertEqual(get_driver_class("test_custom"), DummyConcreteDriver)
        self.assertEqual(get_driver_class("TEST_CUSTOM"), DummyConcreteDriver)

    def test_get_driver_class_unsupported_raises_informative_value_error(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            get_driver_class("unsupported_engine_xyz")
        err = str(ctx.exception)
        self.assertIn("Unknown execution engine 'unsupported_engine_xyz'", err)
        self.assertIn("Available engines", err)
        self.assertIn("antigravity", err)
        self.assertIn("claude_code", err)
        self.assertIn("opencode", err)

    def test_create_driver_with_whitespace_and_case_normalization(self) -> None:
        d1 = create_driver(engine="  Antigravity  ", cwd="/tmp", model="gemini-3.8")
        self.assertIsInstance(d1, AntigravityDriver)

        d2 = create_driver(engine="CLAUDE_CODE", cwd="/tmp")
        self.assertIsInstance(d2, ClaudeCodeDriver)

        d3 = create_driver(engine="OpenCode", cwd="/tmp")
        self.assertIsInstance(d3, OpenCodeDriver)

        d4 = create_driver(engine="AGY", cwd="/tmp")
        self.assertIsInstance(d4, AntigravityDriver)

        d5 = create_driver(engine="Claude", cwd="/tmp")
        self.assertIsInstance(d5, ClaudeCodeDriver)


class TestSessionRouterAndHandleIntegrationAdversarial(unittest.IsolatedAsyncioTestCase):
    """Stress test dynamic router engine resolution and SessionHandle sync."""

    async def test_session_handle_bidirectional_driver_adapter_sync(self) -> None:
        d1 = DummyConcreteDriver(cwd="/tmp", model="m1")
        session = SessionHandle(
            session_id="S1:T1",
            channel="S1",
            thread_ts="T1",
            driver=d1,
        )
        self.assertIs(session.driver, d1)
        self.assertIs(session.adapter, d1)

        d2 = DummyConcreteDriver(cwd="/tmp", model="m2")
        session.adapter = d2
        self.assertIs(session.driver, d2)
        self.assertIs(session.adapter, d2)

    async def test_router_dynamic_engine_switching(self) -> None:
        router = SessionRouter(default_target_cwd="/tmp")

        # 1. Default -> antigravity
        with patch.dict(os.environ, {"GOLEM_ENGINE": "antigravity"}):
            self.assertEqual(router.engine, "antigravity")

        # 2. Switch to claude_code
        with patch.dict(os.environ, {"GOLEM_ENGINE": "claude_code", "CLAUDE_CODE_MODEL": "claude-3-7-sonnet-latest"}):
            self.assertEqual(router.engine, "claude_code")
            s_claude, is_new = await router.get_or_create("C_CLAUDE", "T_CLAUDE")
            self.assertTrue(is_new)
            self.assertIsInstance(s_claude.driver, ClaudeCodeDriver)
            await router.teardown("C_CLAUDE", "T_CLAUDE")

        # 3. Switch to opencode
        with patch.dict(os.environ, {"GOLEM_ENGINE": "opencode", "OPENCODE_MODEL": "deepseek/deepseek-chat"}):
            self.assertEqual(router.engine, "opencode")
            s_open, is_new = await router.get_or_create("C_OPEN", "T_OPEN")
            self.assertTrue(is_new)
            self.assertIsInstance(s_open.driver, OpenCodeDriver)
            await router.teardown("C_OPEN", "T_OPEN")

        await router.stop_reaper()

    async def test_session_teardown_closes_underlying_driver(self) -> None:
        router = SessionRouter(default_target_cwd="/tmp")
        mock_driver = DummyConcreteDriver(cwd="/tmp")
        router.driver_factory = lambda cwd, **kwargs: mock_driver

        session, _ = await router.get_or_create("C_TEARDOWN", "T_TEARDOWN")
        # get_or_create automatically calls start() on the driver
        self.assertTrue(mock_driver._alive)
        self.assertEqual(mock_driver.start_called, 1)

        await router.teardown("C_TEARDOWN", "T_TEARDOWN")
        self.assertFalse(mock_driver.is_alive)
        self.assertEqual(mock_driver.stop_called, 1)

        await router.stop_reaper()


class TestAntigravityNDJSONParsingAdversarial(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress testing of NDJSON stream parser in AntigravityDriver."""

    async def test_corrupted_json_lines_skipped_gracefully(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")
        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.stdin.drain = AsyncMock()
        driver.process = mock_proc

        stream_lines = [
            b'{"event": "step_update", "step_update": {"step_type": "agent_response", "text_delta": "Part 1 "}}\n',
            b'NOT VALID JSON AT ALL\n',
            b'{"malformed_json: missing_brace\n',
            b'   \n',  # blank line
            b'{"event": "step_update", "step_update": {"step_type": "agent_response", "text_delta": "Part 2"}}\n',
            b'{"event": "result", "result": {"status": "SUCCESS", "response": "Part 1 Part 2"}}\n',
        ]
        mock_proc.stdout.readline = AsyncMock(side_effect=stream_lines)

        events: List[AgentEvent] = []
        async for ev in driver.send_prompt("test malformed"):
            events.append(ev)

        # Expected: StatusUpdateEvent, TextDelta(Part 1), TextDelta(Part 2), FinishEvent
        delta_events = [e for e in events if isinstance(e, TextDeltaEvent)]
        self.assertEqual(len(delta_events), 2)
        self.assertEqual(delta_events[0].text_delta, "Part 1 ")
        self.assertEqual(delta_events[1].text_delta, "Part 2")

        finish_ev = events[-1]
        self.assertIsInstance(finish_ev, FinishEvent)
        self.assertEqual(finish_ev.status, "SUCCESS")
        self.assertEqual(finish_ev.response, "Part 1 Part 2")

    async def test_ask_question_tool_yields_awaiting_input(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")
        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.stdin.drain = AsyncMock()
        driver.process = mock_proc

        stream_lines = [
            b'{"event": "step_update", "step_update": {"step_type": "tool", "tool_name": "ask_question", "state": "COMPLETED", "tool_info": {"parameters": {"question": "Delete file?", "choices": ["Yes", "No"]}}}}\n',
            b'{"event": "result", "result": {"status": "SUCCESS", "response": "Awaiting response"}}\n',
        ]
        mock_proc.stdout.readline = AsyncMock(side_effect=stream_lines)

        events: List[AgentEvent] = []
        async for ev in driver.send_prompt("test question"):
            events.append(ev)

        ai_events = [e for e in events if getattr(e, "event_type", None) == "awaiting_input"]
        self.assertEqual(len(ai_events), 1)
        self.assertEqual(ai_events[0].prompt, "Delete file?")
        self.assertEqual(ai_events[0].choices, ["Yes", "No"])

    async def test_error_event_in_stream_yields_error_finish_event(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")
        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.stdin.drain = AsyncMock()
        driver.process = mock_proc

        stream_lines = [
            b'{"event": "error", "error": {"message": "API Quota Exceeded"}}\n',
        ]
        mock_proc.stdout.readline = AsyncMock(side_effect=stream_lines)

        events: List[AgentEvent] = []
        async for ev in driver.send_prompt("test error"):
            events.append(ev)

        self.assertGreaterEqual(len(events), 2)
        finish_ev = events[-1]
        self.assertIsInstance(finish_ev, FinishEvent)
        self.assertEqual(finish_ev.status, "ERROR")
        self.assertIn("API Quota Exceeded", finish_ev.error)

    async def test_early_generator_break_releases_lock_for_next_turn(self) -> None:
        driver = AntigravityDriver(cwd="/tmp")
        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.stdin.drain = AsyncMock()
        driver.process = mock_proc

        stream_lines_turn1 = [
            b'{"event": "step_update", "step_update": {"step_type": "agent_response", "text_delta": "Token 1"}}\n',
            b'{"event": "step_update", "step_update": {"step_type": "agent_response", "text_delta": "Token 2"}}\n',
            b'{"event": "result", "result": {"status": "SUCCESS"}}\n',
        ]
        mock_proc.stdout.readline = AsyncMock(side_effect=stream_lines_turn1)

        # Break early on first event
        async for ev in driver.send_prompt("break early"):
            if isinstance(ev, TextDeltaEvent):
                break

        # Next turn can acquire lock and execute cleanly without deadlock
        stream_lines_turn2 = [
            b'{"event": "step_update", "step_update": {"step_type": "agent_response", "text_delta": "Next turn token"}}\n',
            b'{"event": "result", "result": {"status": "SUCCESS", "response": "Finished"}}\n',
        ]
        mock_proc.stdout.readline = AsyncMock(side_effect=stream_lines_turn2)

        events_turn2 = [e async for e in driver.send_prompt("turn 2")]
        self.assertFalse(driver._lock.locked())
        self.assertEqual(events_turn2[-1].status, "SUCCESS")


class TestHeadlessAgentAdapterCompatibilityAdversarial(unittest.TestCase):
    """Verify HeadlessAgentAdapter backward compatibility subclassing and aliases."""

    def test_adapter_subclassing_and_type_checks(self) -> None:
        self.assertTrue(issubclass(HeadlessAgentAdapter, AntigravityDriver))
        self.assertTrue(issubclass(HeadlessAgentAdapter, BaseAgentDriver))

        adapter = HeadlessAgentAdapter(cwd="/tmp", agy_bin="/bin/echo")
        self.assertIsInstance(adapter, AntigravityDriver)
        self.assertIsInstance(adapter, BaseAgentDriver)
        self.assertEqual(adapter.engine_name, "antigravity")
        self.assertEqual(adapter.agy_bin, "/bin/echo")

    def test_adapter_shims_exist(self) -> None:
        adapter = HeadlessAgentAdapter(cwd="/tmp")
        self.assertTrue(hasattr(adapter, "start"))
        self.assertTrue(hasattr(adapter, "stop"))
        self.assertTrue(hasattr(adapter, "send_turn"))
        self.assertTrue(hasattr(adapter, "stream_turn"))


class TestConfigResilienceAdversarial(unittest.TestCase):
    """Verify config helper resilience and structure."""

    def test_config_dict_schema(self) -> None:
        cfg = get_config_dict()
        self.assertIsInstance(cfg, dict)
        self.assertIn("active_engine", cfg)
        self.assertIn("engine_model", cfg)
        self.assertIn("mouth_model", cfg)
        self.assertIn("eyes_model", cfg)
        self.assertIn("nose_model", cfg)

    def test_reload_config_safe(self) -> None:
        from golem.config import reload_config
        # Should execute safely without exceptions
        reload_config()


class TestSessionHandleFaultHandlingAdversarial(unittest.IsolatedAsyncioTestCase):
    """Verify SessionHandle state transitions and lock safety when driver throws."""

    async def test_session_state_transitions_to_failed_on_driver_exception(self) -> None:
        mock_driver = MagicMock()
        mock_driver.is_alive = True
        mock_driver.conversation_id = "test-conv"

        async def faulty_stream(prompt):
            yield StatusUpdateEvent(status="working")
            raise ConnectionResetError("Connection lost to CLI process")

        mock_driver.send_turn = faulty_stream

        session = SessionHandle(
            session_id="FAIL:1",
            channel="C1",
            thread_ts="T1",
            driver=mock_driver,
        )
        session.state = SessionState.IDLE

        with self.assertRaises(ConnectionResetError):
            async for _ in session.send_user_message("hello"):
                pass

        self.assertEqual(session.state, SessionState.FAILED)
        self.assertFalse(session.lock.locked())

        await session.close()
        self.assertEqual(session.state, SessionState.CLOSED)


class TestZeroHardcodedModelsAudit(unittest.TestCase):
    """Static audit asserting zero hardcoded model strings in Python source."""

    def test_zero_hardcoded_model_strings_in_repo(self) -> None:
        pattern = re.compile(r'gemini-[a-zA-Z0-9.-]+|claude-[a-zA-Z0-9.-]+')
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        target_dirs = [os.path.join(root_dir, "golem")]
        target_files = [os.path.join(root_dir, "agent_core.py")]

        violations: List[str] = []

        for d in target_dirs:
            for dirpath, _, filenames in os.walk(d):
                for f in filenames:
                    if f.endswith(".py"):
                        path = os.path.join(dirpath, f)
                        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                            for lineno, line in enumerate(fh, 1):
                                if pattern.search(line):
                                    violations.append(f"{path}:{lineno} -> {line.strip()}")

        for path in target_files:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                    for lineno, line in enumerate(fh, 1):
                        if pattern.search(line):
                            violations.append(f"{path}:{lineno} -> {line.strip()}")

        self.assertEqual(
            len(violations),
            0,
            f"Found hardcoded model names in source code:\n" + "\n".join(violations),
        )


if __name__ == "__main__":
    unittest.main()
