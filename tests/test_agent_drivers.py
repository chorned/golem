#!/usr/bin/env python3
"""
tests/test_agent_drivers.py
Comprehensive test suite for Phase 2: CLI Portability Driver Architecture.
Tests cover:
  1. BaseAgentDriver ABC interface contracts, required properties, and compatibility shims.
  2. first_mate/config.py declarative config, model lookup per engine, and zero-hardcoding.
  3. Driver registry and create_driver factory.
  4. AntigravityDriver model flag generation, subclassing, and event streaming.
  5. ClaudeCodeDriver and OpenCodeDriver pluggable stubs and typed event protocol.
  6. SessionHandle.driver property alias, turn execution, and lifecycle termination.
  7. SessionRouter dynamic driver resolution, FIRST_MATE_ENGINE dispatch, and legacy adapter_factory preservation.
"""

import os
import time
import inspect
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncGenerator, List, Dict, Optional, Any

from first_mate.config import (
    get_active_engine,
    get_engine_model,
    get_mouth_model,
    get_eyes_model,
    get_nose_model,
)
from first_mate.agent.protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
)
from first_mate.agent.drivers.base import BaseAgentDriver
from first_mate.agent.drivers.antigravity import AntigravityDriver
from first_mate.agent.drivers.claude_code import ClaudeCodeDriver
from first_mate.agent.drivers.opencode import OpenCodeDriver
from first_mate.agent.drivers import (
    DRIVER_REGISTRY,
    get_driver_class,
    create_driver,
)
from first_mate.agent.adapter import HeadlessAgentAdapter
from first_mate.gateway.session import SessionHandle, SessionState
from first_mate.gateway.router import SessionRouter


class DummyConcreteDriver(BaseAgentDriver):
    """Minimal concrete driver implementation for testing ABC contract."""

    def __init__(self, cwd: str, model: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(cwd=cwd, model=model, **kwargs)
        self._alive = False
        self.started = False
        self.terminated = False

    @property
    def engine_name(self) -> str:
        return "dummy_engine"

    @property
    def is_alive(self) -> bool:
        return self._alive

    async def start_session(self) -> None:
        self.started = True
        self._alive = True
        self.conversation_id = "dummy-conv-1"

    async def send_prompt(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        yield StatusUpdateEvent(status="working", detail=f"Processing {prompt}")
        yield TextDeltaEvent(text_delta=f"Dummy response to: {prompt}")
        yield FinishEvent(status="SUCCESS", response=f"Dummy response to: {prompt}")

    async def terminate_session(self) -> None:
        self.terminated = True
        self._alive = False


class TestBaseAgentDriverABC(unittest.TestCase):
    """Test BaseAgentDriver abstract base class enforcement and compatibility shims."""

    def test_abc_cannot_be_instantiated_directly(self) -> None:
        with self.assertRaises(TypeError):
            BaseAgentDriver(cwd="/tmp")  # type: ignore

    def test_concrete_subclass_instantiation_and_defaults(self) -> None:
        driver = DummyConcreteDriver(cwd="/tmp")
        self.assertEqual(driver.cwd, "/tmp")
        self.assertEqual(driver.engine_name, "dummy_engine")
        self.assertFalse(driver.is_alive)
        self.assertIsNone(driver.model)
        self.assertEqual(driver.extra_args, [])
        self.assertIsNone(driver.env)
        self.assertIsNone(driver.conversation_id)
        self.assertEqual(driver.available_tools, [])

    def test_extra_args_defensive_copy(self) -> None:
        raw_args = ["--flag-1", "--flag-2"]
        driver = DummyConcreteDriver(cwd="/tmp", extra_args=raw_args)
        raw_args.append("--flag-3")
        self.assertEqual(driver.extra_args, ["--flag-1", "--flag-2"])

    def test_compatibility_shims_delegation(self) -> None:
        async def run_test():
            driver = DummyConcreteDriver(cwd="/tmp", model="test-model")

            # 1. start() alias calls start_session()
            await driver.start()
            self.assertTrue(driver.started)
            self.assertTrue(driver.is_alive)

            # 2. send_turn() alias calls send_prompt()
            events_turn: List[AgentEvent] = []
            async for ev in driver.send_turn("hello turn"):
                events_turn.append(ev)
            self.assertEqual(len(events_turn), 3)
            self.assertIsInstance(events_turn[0], StatusUpdateEvent)
            self.assertIsInstance(events_turn[1], TextDeltaEvent)
            self.assertIsInstance(events_turn[2], FinishEvent)

            # 3. stream_turn() alias calls send_prompt()
            events_stream: List[AgentEvent] = []
            async for ev in driver.stream_turn("hello stream"):
                events_stream.append(ev)
            self.assertEqual(len(events_stream), 3)

            # 4. stop() alias calls terminate_session()
            await driver.stop()
            self.assertTrue(driver.terminated)
            self.assertFalse(driver.is_alive)

        asyncio.run(run_test())


class TestConfigModule(unittest.TestCase):
    """Test first_mate/config.py declarative environment resolution and zero-hardcoding."""

    def test_get_active_engine_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(get_active_engine(), "antigravity")

    def test_get_active_engine_custom_and_normalized(self) -> None:
        with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "  Claude_Code  "}):
            self.assertEqual(get_active_engine(), "claude_code")

        with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "OPENCODE"}):
            self.assertEqual(get_active_engine(), "opencode")

    def test_get_engine_model_mappings(self) -> None:
        env = {
            "ANTIGRAVITY_MODEL": "gemini-3.8-flash-high",
            "CLAUDE_CODE_MODEL": "claude-3-7-sonnet-latest",
            "OPENCODE_MODEL": "deepseek/deepseek-chat",
        }
        with patch.dict(os.environ, env):
            self.assertEqual(get_engine_model("antigravity"), "gemini-3.8-flash-high")
            self.assertEqual(get_engine_model("agy"), "gemini-3.8-flash-high")
            self.assertEqual(get_engine_model("claude_code"), "claude-3-7-sonnet-latest")
            self.assertEqual(get_engine_model("claude"), "claude-3-7-sonnet-latest")
            self.assertEqual(get_engine_model("opencode"), "deepseek/deepseek-chat")

    def test_get_engine_model_default_engine_lookup(self) -> None:
        env = {
            "FIRST_MATE_ENGINE": "claude_code",
            "CLAUDE_CODE_MODEL": "claude-3-7-sonnet-latest",
        }
        with patch.dict(os.environ, env):
            self.assertEqual(get_engine_model(), "claude-3-7-sonnet-latest")

    def test_get_engine_model_unknown(self) -> None:
        self.assertEqual(get_engine_model("nonexistent_engine"), "")

    def test_sensory_model_getters(self) -> None:
        env = {
            "MOUTH_MODEL": "gemini-3.8-flash",
            "EYES_MODEL": "gemini-flash-lite-latest",
            "NOSE_MODEL": "gemini-flash-lite-latest",
        }
        with patch.dict(os.environ, env):
            self.assertEqual(get_mouth_model(), "gemini-3.8-flash")
            self.assertEqual(get_eyes_model(), "gemini-flash-lite-latest")
            self.assertEqual(get_nose_model(), "gemini-flash-lite-latest")

    def test_mouth_model_fallback_to_gemini_model(self) -> None:
        with patch.dict(os.environ, {"GEMINI_MODEL": "legacy-gemini-model"}, clear=True):
            self.assertEqual(get_mouth_model(), "legacy-gemini-model")

    def test_disk_fallback_when_environ_cleared(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(get_mouth_model(), "gemini-3.8-flash")
            self.assertEqual(get_eyes_model(), "gemini-flash-lite-latest")
            self.assertEqual(get_nose_model(), "gemini-flash-lite-latest")
            self.assertEqual(get_engine_model("antigravity"), "gemini-3.8-flash-high")
            self.assertEqual(get_active_engine(), "antigravity")

    def test_zero_hardcoding_when_env_file_missing(self) -> None:
        with patch("first_mate.config._get_file_env", return_value={}):
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(get_mouth_model(), "")
                self.assertEqual(get_eyes_model(), "")
                self.assertEqual(get_nose_model(), "")
                self.assertEqual(get_engine_model("antigravity"), "")
                self.assertEqual(get_active_engine(), "antigravity")


class TestDriverRegistryAndFactory(unittest.TestCase):
    """Test driver registry and create_driver factory function."""

    def test_registry_contains_all_engines(self) -> None:
        self.assertIn("antigravity", DRIVER_REGISTRY)
        self.assertIn("agy", DRIVER_REGISTRY)
        self.assertIn("claude_code", DRIVER_REGISTRY)
        self.assertIn("claude", DRIVER_REGISTRY)
        self.assertIn("opencode", DRIVER_REGISTRY)
        self.assertEqual(DRIVER_REGISTRY["antigravity"], AntigravityDriver)
        self.assertEqual(DRIVER_REGISTRY["claude_code"], ClaudeCodeDriver)
        self.assertEqual(DRIVER_REGISTRY["opencode"], OpenCodeDriver)

    def test_get_driver_class_resolution(self) -> None:
        self.assertEqual(get_driver_class("antigravity"), AntigravityDriver)
        self.assertEqual(get_driver_class("claude_code"), ClaudeCodeDriver)
        self.assertEqual(get_driver_class("opencode"), OpenCodeDriver)
        with self.assertRaises(ValueError):
            get_driver_class("invalid_engine_name")

    def test_create_driver_default(self) -> None:
        with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "antigravity", "ANTIGRAVITY_MODEL": "gemini-3.8-flash-high"}):
            driver = create_driver(cwd="/tmp")
            self.assertIsInstance(driver, AntigravityDriver)
            self.assertEqual(driver.engine_name, "antigravity")
            self.assertEqual(driver.model, "gemini-3.8-flash-high")

    def test_create_driver_explicit_engine_and_model_override(self) -> None:
        driver = create_driver(engine="claude_code", cwd="/tmp", model="custom-claude-model")
        self.assertIsInstance(driver, ClaudeCodeDriver)
        self.assertEqual(driver.engine_name, "claude_code")
        self.assertEqual(driver.model, "custom-claude-model")

    def test_create_driver_unknown_engine_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            create_driver(engine="unsupported_engine", cwd="/tmp")


class TestAntigravityDriver(unittest.TestCase):
    """Test AntigravityDriver CLI argument generation and backward-compatible alias."""

    def test_inheritance_and_properties(self) -> None:
        self.assertTrue(issubclass(AntigravityDriver, BaseAgentDriver))
        driver = AntigravityDriver(cwd="/tmp", model="gemini-3.8-flash-high")
        self.assertEqual(driver.engine_name, "antigravity")
        self.assertEqual(driver.model, "gemini-3.8-flash-high")

    def test_headless_agent_adapter_backward_compatibility(self) -> None:
        self.assertTrue(issubclass(HeadlessAgentAdapter, BaseAgentDriver))
        adapter = HeadlessAgentAdapter(cwd="/tmp")
        self.assertEqual(adapter.engine_name, "antigravity")

    def test_model_flag_injection_in_command_line(self) -> None:
        driver = AntigravityDriver(cwd="/tmp", model="gemini-3.8-flash-high")
        cmd = driver._build_cmd()
        self.assertIn("--model", cmd)
        model_idx = cmd.index("--model")
        self.assertEqual(cmd[model_idx + 1], "gemini-3.8-flash-high")

    def test_model_flag_not_duplicated_if_in_extra_args(self) -> None:
        driver = AntigravityDriver(
            cwd="/tmp",
            model="gemini-3.8-flash-high",
            extra_args=["--model", "already-specified-model"],
        )
        cmd = driver._build_cmd()
        self.assertEqual(cmd.count("--model"), 1)
        model_idx = cmd.index("--model")
        self.assertEqual(cmd[model_idx + 1], "already-specified-model")

    def test_no_model_flag_when_model_is_none_or_empty(self) -> None:
        driver = AntigravityDriver(cwd="/tmp", model="")
        cmd = driver._build_cmd()
        self.assertNotIn("--model", cmd)


class TestClaudeCodeDriver(unittest.IsolatedAsyncioTestCase):
    """Test ClaudeCodeDriver stub lifecycle, model resolution, and typed event streaming."""

    async def test_lifecycle_and_model_resolution(self) -> None:
        with patch.dict(os.environ, {"CLAUDE_CODE_MODEL": "claude-3-7-sonnet-latest"}):
            driver = ClaudeCodeDriver(cwd="/tmp")
            self.assertEqual(driver.engine_name, "claude_code")
            self.assertEqual(driver.model, "claude-3-7-sonnet-latest")
            self.assertFalse(driver.is_alive)

            await driver.start_session()
            self.assertTrue(driver.is_alive)
            self.assertIsNotNone(driver.conversation_id)

            await driver.terminate_session()
            self.assertFalse(driver.is_alive)

    async def test_event_streaming(self) -> None:
        driver = ClaudeCodeDriver(cwd="/tmp", model="claude-3-7-sonnet-latest")
        await driver.start_session()

        events: List[AgentEvent] = []
        async for ev in driver.send_prompt("Build the parser"):
            events.append(ev)

        self.assertGreaterEqual(len(events), 3)
        self.assertIsInstance(events[0], StatusUpdateEvent)
        self.assertIsInstance(events[1], TextDeltaEvent)
        finish_ev = events[-1]
        self.assertIsInstance(finish_ev, FinishEvent)
        self.assertEqual(finish_ev.status, "SUCCESS")
        await driver.terminate_session()


class TestOpenCodeDriver(unittest.IsolatedAsyncioTestCase):
    """Test OpenCodeDriver stub lifecycle, model resolution, and typed event streaming."""

    async def test_lifecycle_and_model_resolution(self) -> None:
        with patch.dict(os.environ, {"OPENCODE_MODEL": "deepseek/deepseek-chat"}):
            driver = OpenCodeDriver(cwd="/tmp")
            self.assertEqual(driver.engine_name, "opencode")
            self.assertEqual(driver.model, "deepseek/deepseek-chat")
            self.assertFalse(driver.is_alive)

            await driver.start_session()
            self.assertTrue(driver.is_alive)
            self.assertIsNotNone(driver.conversation_id)

            await driver.terminate_session()
            self.assertFalse(driver.is_alive)

    async def test_event_streaming(self) -> None:
        driver = OpenCodeDriver(cwd="/tmp", model="deepseek/deepseek-chat")
        await driver.start_session()

        events: List[AgentEvent] = []
        async for ev in driver.send_prompt("Run static analysis"):
            events.append(ev)

        self.assertGreaterEqual(len(events), 3)
        self.assertIsInstance(events[0], StatusUpdateEvent)
        self.assertIsInstance(events[1], TextDeltaEvent)
        finish_ev = events[-1]
        self.assertIsInstance(finish_ev, FinishEvent)
        self.assertEqual(finish_ev.status, "SUCCESS")
        await driver.terminate_session()


class TestSessionHandleDriverIntegration(unittest.IsolatedAsyncioTestCase):
    """Test SessionHandle integration with BaseAgentDriver and driver property alias."""

    async def test_driver_property_alias_and_setter(self) -> None:
        mock_driver_1 = DummyConcreteDriver(cwd="/tmp")
        session = SessionHandle(
            session_id="C_DRV:T_DRV",
            channel="C_DRV",
            thread_ts="T_DRV",
            adapter=mock_driver_1,
        )
        self.assertIs(session.driver, mock_driver_1)
        self.assertIs(session.adapter, mock_driver_1)

        mock_driver_2 = DummyConcreteDriver(cwd="/tmp")
        session.driver = mock_driver_2
        self.assertIs(session.driver, mock_driver_2)
        self.assertIs(session.adapter, mock_driver_2)

    async def test_driver_kwarg_in_session_handle_init(self) -> None:
        mock_driver = DummyConcreteDriver(cwd="/tmp")
        session = SessionHandle(
            session_id="C_DRV2:T_DRV2",
            channel="C_DRV2",
            thread_ts="T_DRV2",
            driver=mock_driver,
        )
        self.assertIs(session.driver, mock_driver)
        self.assertIs(session.adapter, mock_driver)

    async def test_turn_execution_with_base_agent_driver(self) -> None:
        driver = DummyConcreteDriver(cwd="/tmp")
        await driver.start_session()
        session = SessionHandle(
            session_id="C_TURN:T_TURN",
            channel="C_TURN",
            thread_ts="T_TURN",
            driver=driver,
        )
        session.state = SessionState.IDLE

        events: List[AgentEvent] = []
        async for ev in session.send_user_message("Hello from Captain Carl"):
            events.append(ev)

        self.assertEqual(len(events), 3)
        self.assertEqual(session.state, SessionState.IDLE)
        self.assertIn("Dummy response to: Hello from Captain Carl", session.last_response)

        await session.close()
        self.assertTrue(driver.terminated)


class TestSessionRouterDriverResolution(unittest.IsolatedAsyncioTestCase):
    """Test SessionRouter dynamic engine resolution, driver_factory, and legacy adapter_factory."""

    async def asyncSetUp(self) -> None:
        self.router: Optional[SessionRouter] = None

    async def asyncTearDown(self) -> None:
        if self.router:
            await self.router.stop_reaper()

    async def test_router_default_engine_antigravity(self) -> None:
        with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "antigravity"}):
            self.router = SessionRouter(default_target_cwd="/tmp")
            self.assertEqual(self.router.engine, "antigravity")

    async def test_router_dynamic_engine_resolution_claude_code(self) -> None:
        with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "claude_code", "CLAUDE_CODE_MODEL": "claude-3-7-sonnet-latest"}):
            self.router = SessionRouter(default_target_cwd="/tmp")
            self.assertEqual(self.router.engine, "claude_code")
            session, is_new = await self.router.get_or_create("C_CLAUDE", "T_CLAUDE")
            self.assertTrue(is_new)
            self.assertIsInstance(session.driver, ClaudeCodeDriver)
            self.assertEqual(session.driver.engine_name, "claude_code")
            await self.router.teardown("C_CLAUDE", "T_CLAUDE")

    async def test_router_dynamic_engine_resolution_opencode(self) -> None:
        with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "opencode", "OPENCODE_MODEL": "deepseek/deepseek-chat"}):
            self.router = SessionRouter(default_target_cwd="/tmp")
            self.assertEqual(self.router.engine, "opencode")
            session, is_new = await self.router.get_or_create("C_OPEN", "T_OPEN")
            self.assertTrue(is_new)
            self.assertIsInstance(session.driver, OpenCodeDriver)
            self.assertEqual(session.driver.engine_name, "opencode")
            await self.router.teardown("C_OPEN", "T_OPEN")

    async def test_router_explicit_engine_overrides_environment(self) -> None:
        with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "antigravity"}):
            self.router = SessionRouter(default_target_cwd="/tmp", engine="opencode")
            self.assertEqual(self.router.engine, "opencode")
            session, _ = await self.router.get_or_create("C_OVR", "T_OVR")
            self.assertIsInstance(session.driver, OpenCodeDriver)
            await self.router.teardown("C_OVR", "T_OVR")

    async def test_router_driver_factory_precedence(self) -> None:
        custom_driver = DummyConcreteDriver(cwd="/tmp")
        self.router = SessionRouter(
            default_target_cwd="/tmp",
            driver_factory=lambda cwd, **kwargs: custom_driver,
        )
        session, _ = await self.router.get_or_create("C_DF", "T_DF")
        self.assertIs(session.driver, custom_driver)
        await self.router.teardown("C_DF", "T_DF")

    async def test_router_legacy_adapter_factory_preserved(self) -> None:
        legacy_mock = MagicMock()
        legacy_mock.start = AsyncMock()
        legacy_mock.stop = AsyncMock()
        self.router = SessionRouter(
            default_target_cwd="/tmp",
            adapter_factory=lambda cwd: legacy_mock,
        )
        session, _ = await self.router.get_or_create("C_LEGACY", "T_LEGACY")
        self.assertIs(session.driver, legacy_mock)
        self.assertIs(session.adapter, legacy_mock)
        await self.router.teardown("C_LEGACY", "T_LEGACY")

    async def test_router_driver_factory_priority_over_adapter_factory(self) -> None:
        driver_mock = DummyConcreteDriver(cwd="/tmp")
        adapter_mock = MagicMock()
        self.router = SessionRouter(
            default_target_cwd="/tmp",
            driver_factory=lambda cwd, **kwargs: driver_mock,
            adapter_factory=lambda cwd: adapter_mock,
        )
        session, _ = await self.router.get_or_create("C_PRIO", "T_PRIO")
        self.assertIs(session.driver, driver_mock)
        await self.router.teardown("C_PRIO", "T_PRIO")


if __name__ == "__main__":
    unittest.main()
