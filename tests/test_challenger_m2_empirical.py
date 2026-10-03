#!/usr/bin/env python3
"""
tests/test_challenger_m2_empirical.py
Empirical Challenger Test Suite for Milestone 2:
1. Zero-hardcoding and disk-backed fallback when os.environ is cleared.
2. Environment override precedence and whitespace normalization.
3. AST-based static string literal audit for model names in code.
4. SessionRouter dynamic engine switching under concurrent requests.
5. Adapter/Driver factory priority and SessionHandle driver/adapter dual-aliasing.
"""

from __future__ import annotations

import os
import ast
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock
from typing import List, Dict, Any, Set

import first_mate.config as fm_config
from first_mate.config import (
    get_active_engine,
    get_engine_model,
    get_mouth_model,
    get_eyes_model,
    get_nose_model,
    get_config_dict,
    reload_config,
)
from first_mate.agent.drivers.base import BaseAgentDriver
from first_mate.agent.drivers.antigravity import AntigravityDriver
from first_mate.agent.drivers.claude_code import ClaudeCodeDriver
from first_mate.agent.drivers.opencode import OpenCodeDriver
from first_mate.gateway.session import SessionHandle, SessionState
from first_mate.gateway.router import SessionRouter


class DummyTestDriver(BaseAgentDriver):
    def __init__(self, cwd: str, model: str | None = None, **kwargs: Any) -> None:
        super().__init__(cwd=cwd, model=model, **kwargs)
        self._alive = False

    @property
    def engine_name(self) -> str:
        return "dummy"

    @property
    def is_alive(self) -> bool:
        return self._alive

    async def start_session(self) -> None:
        self._alive = True

    async def send_prompt(self, prompt: str):
        yield MagicMock()

    async def terminate_session(self) -> None:
        self._alive = False


class TestChallengerZeroHardcodingAndFallback(unittest.TestCase):
    """Empirical challenge: Wiped environ fallback & zero hardcoding."""

    def test_wiped_environ_disk_fallback(self) -> None:
        """When os.environ is completely wiped, values resolve from disk .env."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(get_mouth_model(), "gemini-3.8-flash")
            self.assertEqual(get_eyes_model(), "gemini-flash-lite-latest")
            self.assertEqual(get_nose_model(), "gemini-flash-lite-latest")
            self.assertEqual(get_active_engine(), "antigravity")
            self.assertEqual(get_engine_model("antigravity"), "gemini-3.8-flash-high")
            self.assertEqual(get_engine_model("claude_code"), "claude-3-7-sonnet-latest")
            self.assertEqual(get_engine_model("opencode"), "deepseek/deepseek-chat")

    def test_missing_env_file_zero_hardcoded_model_fallback(self) -> None:
        """When os.environ is wiped AND .env is missing/empty, NO hardcoded model strings are returned."""
        with patch("first_mate.config._get_file_env", return_value={}):
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(get_mouth_model(), "")
                self.assertEqual(get_eyes_model(), "")
                self.assertEqual(get_nose_model(), "")
                self.assertEqual(get_engine_model("antigravity"), "")
                self.assertEqual(get_engine_model("claude_code"), "")
                self.assertEqual(get_engine_model("opencode"), "")
                # Engine defaults to antigravity safely
                self.assertEqual(get_active_engine(), "antigravity")

    def test_convenience_getter_function_audit(self) -> None:
        """Verify whether functions claimed in worker handoff actually exist in first_mate.config."""
        expected_getters = [
            "get_mouth_model",
            "get_eyes_model",
            "get_nose_model",
            "get_active_engine",
            "get_engine_model",
        ]
        for name in expected_getters:
            self.assertTrue(hasattr(fm_config, name), f"Missing expected getter {name}")

        claimed_but_missing = [
            "get_engine",
            "get_antigravity_model",
            "get_claude_code_model",
            "get_opencode_model",
            "get_model_for_engine",
        ]
        missing_found = [n for n in claimed_but_missing if not hasattr(fm_config, n)]
        # Record this finding: all 5 claimed convenience functions are missing from config.py
        self.assertEqual(len(missing_found), 5)


class TestChallengerEnvironmentPrecedence(unittest.TestCase):
    """Empirical challenge: Environment variable override precedence."""

    def test_environment_overrides_disk_env(self) -> None:
        custom_env = {
            "MOUTH_MODEL": "custom-mouth-model-v2",
            "EYES_MODEL": "custom-eyes-model-v2",
            "NOSE_MODEL": "custom-nose-model-v2",
            "FIRST_MATE_ENGINE": "claude_code",
            "CLAUDE_CODE_MODEL": "claude-custom-opus",
            "ANTIGRAVITY_MODEL": "custom-agy-model",
            "OPENCODE_MODEL": "custom-deepseek-v3",
        }
        with patch.dict(os.environ, custom_env):
            self.assertEqual(get_mouth_model(), "custom-mouth-model-v2")
            self.assertEqual(get_eyes_model(), "custom-eyes-model-v2")
            self.assertEqual(get_nose_model(), "custom-nose-model-v2")
            self.assertEqual(get_active_engine(), "claude_code")
            self.assertEqual(get_engine_model("claude_code"), "claude-custom-opus")
            self.assertEqual(get_engine_model("antigravity"), "custom-agy-model")
            self.assertEqual(get_engine_model("opencode"), "custom-deepseek-v3")
            # Default lookup with active engine
            self.assertEqual(get_engine_model(), "claude-custom-opus")

    def test_whitespace_stripping_and_case_normalization(self) -> None:
        with patch.dict(os.environ, {"MOUTH_MODEL": "   gemini-custom-padded   \n"}):
            self.assertEqual(get_mouth_model(), "gemini-custom-padded")

        with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "   CLAUDE_CODE   "}):
            self.assertEqual(get_active_engine(), "claude_code")

        with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "agy"}):
            self.assertEqual(get_engine_model("agy"), get_engine_model("antigravity"))

    def test_legacy_gemini_model_fallback(self) -> None:
        with patch.dict(os.environ, {"GEMINI_MODEL": "legacy-gemini-pro"}, clear=True):
            self.assertEqual(get_mouth_model(), "legacy-gemini-pro")


class TestChallengerASTStaticCodeAudit(unittest.TestCase):
    """AST-based empirical challenge: Parse Python ASTs to find hardcoded model strings in code."""

    def test_no_hardcoded_model_literals_in_ast(self) -> None:
        forbidden_substrings = [
            "gemini-2.5-flash",
            "gemini-3.8-flash",
            "gemini-flash-lite",
            "claude-3-7-sonnet",
            "deepseek-chat",
        ]

        project_root = Path(__file__).resolve().parent.parent
        targets = list((project_root / "first_mate").rglob("*.py"))
        targets.append(project_root / "agent_core.py")

        violations: List[str] = []

        for target_path in targets:
            content = target_path.read_text(encoding="utf-8")
            tree = ast.parse(content, filename=str(target_path))

            # Find all string literals (ast.Constant where value is str)
            # Skip docstrings by checking if parent is Module, FunctionDef, or ClassDef as first statement
            docstrings: Set[int] = set()
            for node in ast.walk(tree):
                if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    if node.body and isinstance(node.body[0], ast.Expr):
                        if isinstance(node.body[0].value, ast.Constant) and isinstance(node.body[0].value.value, str):
                            docstrings.add(id(node.body[0].value))

            for node in ast.walk(tree):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    if id(node) in docstrings:
                        continue  # Docstrings are documentation, not execution logic
                    str_val = node.value.strip().lower()
                    for forbidden in forbidden_substrings:
                        if forbidden in str_val:
                            violations.append(f"{target_path.relative_to(project_root)}:{getattr(node, 'lineno', '?')} -> '{node.value}' contains '{forbidden}'")

        self.assertEqual(violations, [], f"Found hardcoded model strings in code AST:\n" + "\n".join(violations))


class TestChallengerRouterDynamismAndConcurrency(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: SessionRouter dynamic engine switching under concurrency."""

    async def asyncSetUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.registry_file = os.path.join(self.tmp_dir.name, "registry.json")

    async def asyncTearDown(self) -> None:
        self.tmp_dir.cleanup()

    async def test_dynamic_engine_switching(self) -> None:
        """Verify new sessions reflect changes to FIRST_MATE_ENGINE in the environment."""
        router = SessionRouter(
            default_target_cwd="/tmp",
            registry_path=self.registry_file,
        )

        try:
            with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "antigravity"}):
                session1, is_new1 = await router.get_or_create("C1", "T1")
                self.assertTrue(is_new1)
                self.assertIsInstance(session1.driver, AntigravityDriver)
                self.assertEqual(session1.driver.engine_name, "antigravity")

            with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "claude_code"}):
                session2, is_new2 = await router.get_or_create("C2", "T2")
                self.assertTrue(is_new2)
                self.assertIsInstance(session2.driver, ClaudeCodeDriver)
                self.assertEqual(session2.driver.engine_name, "claude_code")

            with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "opencode"}):
                session3, is_new3 = await router.get_or_create("C3", "T3")
                self.assertTrue(is_new3)
                self.assertIsInstance(session3.driver, OpenCodeDriver)
                self.assertEqual(session3.driver.engine_name, "opencode")

            # Existing sessions remain unaffected
            self.assertIsInstance(session1.driver, AntigravityDriver)
            self.assertIsInstance(session2.driver, ClaudeCodeDriver)
        finally:
            await router.stop_reaper()

    async def test_concurrent_requests_same_thread_deduplication(self) -> None:
        """Verify that 25 concurrent get_or_create calls for the same thread produce exactly 1 session."""
        router = SessionRouter(
            default_target_cwd="/tmp",
            registry_path=self.registry_file,
        )

        try:
            async def call_router():
                sess, is_new = await router.get_or_create("C_CONC", "T_CONC")
                return sess, is_new

            results = await asyncio.gather(*(call_router() for _ in range(25)))

            sessions = [r[0] for r in results]
            is_new_flags = [r[1] for r in results]

            # Exactly one request should see is_new=True
            self.assertEqual(is_new_flags.count(True), 1)
            self.assertEqual(is_new_flags.count(False), 24)

            # All 25 returned sessions must be the exact same instance
            first_sess = sessions[0]
            for s in sessions[1:]:
                self.assertIs(s, first_sess)
        finally:
            await router.stop_reaper()

    async def test_concurrent_requests_distinct_threads_under_engine_churn(self) -> None:
        """Stress-test concurrent requests across different threads while engine changes dynamically."""
        router = SessionRouter(
            default_target_cwd="/tmp",
            registry_path=self.registry_file,
        )

        try:
            engines = ["antigravity", "claude_code", "opencode"]

            async def worker(idx: int):
                chosen_engine = engines[idx % len(engines)]
                with patch.dict(os.environ, {"FIRST_MATE_ENGINE": chosen_engine}):
                    sess, is_new = await router.get_or_create(f"C_{idx}", f"T_{idx}")
                    return sess, is_new, chosen_engine

            tasks = [worker(i) for i in range(30)]
            results = await asyncio.gather(*tasks)

            self.assertEqual(len(results), 30)
            self.assertEqual(len(router.sessions), 30)
            for sess, is_new, engine in results:
                self.assertTrue(is_new)
                self.assertIsNotNone(sess.driver)
        finally:
            await router.stop_reaper()

    async def test_adapter_factory_priority_never_bypassed(self) -> None:
        """Ensure legacy adapter_factory is never bypassed by create_driver."""
        mock_adapter = MagicMock()
        mock_adapter.start = AsyncMock()
        mock_adapter.stop = AsyncMock()

        router = SessionRouter(
            default_target_cwd="/tmp",
            adapter_factory=lambda cwd: mock_adapter,
            registry_path=self.registry_file,
        )

        try:
            with patch.dict(os.environ, {"FIRST_MATE_ENGINE": "antigravity"}):
                sess, is_new = await router.get_or_create("C_AF", "T_AF")
                self.assertIs(sess.driver, mock_adapter)
                self.assertIs(sess.adapter, mock_adapter)
        finally:
            await router.stop_reaper()

    async def test_session_handle_driver_adapter_mirroring(self) -> None:
        """Verify two-way mirroring between session.driver and session.adapter."""
        driver1 = DummyTestDriver(cwd="/tmp")
        sess = SessionHandle(
            session_id="C:T",
            channel="C",
            thread_ts="T",
            driver=driver1,
        )
        self.assertIs(sess.driver, driver1)
        self.assertIs(sess.adapter, driver1)

        # Mutate via driver
        driver2 = DummyTestDriver(cwd="/tmp")
        sess.driver = driver2
        self.assertIs(sess.driver, driver2)
        self.assertIs(sess.adapter, driver2)

        # Mutate via adapter
        driver3 = DummyTestDriver(cwd="/tmp")
        sess.adapter = driver3
        self.assertIs(sess.driver, driver3)
        self.assertIs(sess.adapter, driver3)


if __name__ == "__main__":
    unittest.main()
