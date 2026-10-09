#!/usr/bin/env python3
"""
tests/test_adversarial_m1_collisions.py
Challenger 1 Adversarial Test Suite for Milestone 1:
Oppositional Analysis: Dual Import & Collision Stress Testing.

Mandatory Oppositional Test Areas:
1. Simultaneous Dual Imports across all submodules in various orders.
2. Cross-namespace monkeypatching and mutation reflection.
3. Complex nested unittest.mock.patch and mock decorators.
4. Pickle serialization and deserialization across both namespaces.
5. importlib.reload() on golem and golem modules without sys.modules corruption.
6. Deep dynamic imports (__import__, importlib.import_module, clean subprocesses).

Lineage Invariant:
"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."
"""

import os
import sys
import pickle
import asyncio
import importlib
import subprocess
import unittest
from unittest.mock import patch, MagicMock, AsyncMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import golem
import golem.gateway
import golem.gateway.router
import golem.gateway.session
import golem.gateway.ingress
import golem.agent
import golem.agent.protocol
import golem.agent.mouth
import golem.agent.drivers.antigravity
import golem.execution
import golem.execution.runner
import golem.execution.harness
import golem.sensory
import golem.sensory.bus
import golem.sensory.eyes
import golem.sensory.nose
import golem.transport
import golem.transport.normalizer
import golem.transport.chunker


ALL_SUBMODULES = [
    "config",
    "gateway",
    "gateway.router",
    "gateway.session",
    "gateway.ingress",
    "agent",
    "agent.adapter",
    "agent.calibration",
    "agent.events",
    "agent.mouth",
    "agent.prompts",
    "agent.protocol",
    "agent.drivers",
    "agent.drivers.base",
    "agent.drivers.antigravity",
    "agent.drivers.claude_code",
    "agent.drivers.opencode",
    "execution",
    "execution.buffer",
    "execution.delegation",
    "execution.harness",
    "execution.mesh",
    "execution.runner",
    "sensory",
    "sensory.bus",
    "sensory.eyes",
    "sensory.nose",
    "transport",
    "transport.chunker",
    "transport.file_transfer",
    "transport.milestone",
    "transport.normalizer",
    "transport.slack_client",
]


class TestArea1DualSimultaneousImports(unittest.TestCase):
    """Area 1: Adversarial dual import collisions and order dependency."""

    def test_all_33_submodules_identical_in_sys_modules(self):
        """Verify sys.modules holds the identical module object for both namespaces."""
        for sub in ALL_SUBMODULES:
            fm_mod = importlib.import_module(f"golem.{sub}")
            g_mod = importlib.import_module(f"golem.{sub}")
            self.assertIs(
                fm_mod,
                g_mod,
                f"Submodule identity failure for {sub}: {fm_mod} is not {g_mod}",
            )
            self.assertIs(
                sys.modules[f"golem.{sub}"],
                sys.modules[f"golem.{sub}"],
                f"sys.modules desynchronization for {sub}",
            )

    def test_order_dependency_clean_subprocesses(self):
        """Verify that importing golem first vs golem first yields exact object identity."""
        code_fm_first = """
import sys, importlib
fm = importlib.import_module('golem.router')
g = importlib.import_module('golem.gateway.router')
assert fm is g
assert sys.modules['golem.router'] is sys.modules['golem.gateway.router']
"""
        res = subprocess.run([sys.executable, "-c", code_fm_first], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0, f"fm_first failed: {res.stderr}")

        code_g_first = """
import sys, importlib
g = importlib.import_module('golem.gateway.router')
fm = importlib.import_module('golem.router')
assert fm is g
assert sys.modules['golem.router'] is sys.modules['golem.gateway.router']
"""
        res2 = subprocess.run([sys.executable, "-c", code_g_first], capture_output=True, text=True)
        self.assertEqual(res2.returncode, 0, f"g_first failed: {res2.stderr}")

    def test_isinstance_and_issubclass_cross_compatibility(self):
        """Verify isinstance and issubclass work bidirectionally across namespaces."""
        from golem.gateway.router import SessionRouter as FM_Router
        from golem.gateway.router import SessionRouter as G_Router
        from golem.agent.protocol import TextDeltaEvent as FM_Event
        from golem.agent.protocol import TextDeltaEvent as G_Event, AgentEvent as G_BaseEvent

        self.assertTrue(issubclass(FM_Router, G_Router))
        self.assertTrue(issubclass(G_Router, FM_Router))
        self.assertTrue(issubclass(FM_Event, G_BaseEvent))

        fm_evt = FM_Event(text_delta="test")
        g_evt = G_Event(text_delta="test_g")

        self.assertIsInstance(fm_evt, G_Event)
        self.assertIsInstance(fm_evt, G_BaseEvent)
        self.assertIsInstance(g_evt, FM_Event)


class TestArea2CrossNamespaceMonkeypatching(unittest.TestCase):
    """Area 2: Adversarial monkeypatching across namespaces."""

    def test_submodule_attribute_mutation_bidirectional(self):
        """Mutating golem.<mod> updates golem.<mod> and vice versa."""
        test_key = "__adversarial_probe_1__"
        test_val = "probe_val_42"

        golem.gateway.router.__dict__[test_key] = test_val
        self.assertEqual(getattr(golem.gateway.router, test_key, None), test_val)
        del golem.gateway.router.__dict__[test_key]

        test_key2 = "__adversarial_probe_2__"
        golem.gateway.router.__dict__[test_key2] = "probe_val_84"
        self.assertEqual(getattr(golem.gateway.router, test_key2, None), "probe_val_84")
        del golem.gateway.router.__dict__[test_key2]

    def test_function_replacement_bidirectional(self):
        """Replacing a function on one namespace modifies callers in the other."""
        orig_fn = golem.transport.normalizer.sanitize_for_slack
        try:
            golem.transport.normalizer.sanitize_for_slack = lambda t: f"ADVERSARIAL:{t}"
            res = golem.transport.normalizer.sanitize_for_slack("abc")
            self.assertEqual(res, "ADVERSARIAL:abc")

            golem.transport.normalizer.sanitize_for_slack = lambda t: f"FM_REPLACED:{t}"
            res2 = golem.transport.normalizer.sanitize_for_slack("xyz")
            self.assertEqual(res2, "FM_REPLACED:xyz")
        finally:
            golem.transport.normalizer.sanitize_for_slack = orig_fn

    def test_class_method_mutation_bidirectional(self):
        """Replacing a class method mutates instances created in both namespaces."""
        orig_touch = golem.gateway.session.SessionHandle.touch
        try:
            golem.gateway.session.SessionHandle.touch = lambda self: "touched_adversarially"
            handle = golem.gateway.session.SessionHandle(
                session_id="s1", channel="C1", thread_ts="t1"
            )
            self.assertEqual(handle.touch(), "touched_adversarially")
        finally:
            golem.gateway.session.SessionHandle.touch = orig_touch


class TestArea3ComplexMockPatchAndDecorators(unittest.TestCase):
    """Area 3: unittest.mock.patch with complex nested attributes, decorators, and restoration."""

    @patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock)
    def test_decorator_flat_patches_gateway_caller(self, mock_gemini):
        """@patch('golem...') intercepts golem.gateway.ingress callers."""
        mock_gemini.return_value = {"text": "patched_fm"}
        res = asyncio.run(golem.gateway.ingress.call_gemini_api())
        self.assertEqual(res, {"text": "patched_fm"})
        mock_gemini.assert_called_once()

    @patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock)
    def test_decorator_gateway_patches_flat_caller(self, mock_gemini):
        """@patch('golem...') intercepts golem.gateway.ingress callers."""
        mock_gemini.return_value = {"text": "patched_golem"}
        res = asyncio.run(golem.gateway.ingress.call_gemini_api())
        self.assertEqual(res, {"text": "patched_golem"})
        mock_gemini.assert_called_once()

    @patch("golem.transport.normalizer.sanitize_for_slack")
    @patch("golem.gateway.session.SessionHandle.is_expired")
    def test_stacked_mixed_decorators(self, mock_expired, mock_sanitize):
        """Stacked decorators mixing golem and golem targets apply cleanly."""
        mock_sanitize.return_value = "STACKED_CLEAN"
        mock_expired.return_value = True

        handle = golem.gateway.session.SessionHandle("s1", "c1", "t1")
        self.assertTrue(handle.is_expired())
        self.assertEqual(
            golem.transport.normalizer.sanitize_for_slack("raw"), "STACKED_CLEAN"
        )

    def test_patch_object_nested_attribute_restoration(self):
        """patch.object restores original across both namespaces without lingering mocks."""
        orig_run = golem.execution.runner.RunnerExecutor.run_command
        mock_run = AsyncMock(return_value=MagicMock(status="completed", stdout="adversarial"))

        from golem.execution.runner import RunnerExecutor as FM_Runner
        from golem.execution.runner import RunnerExecutor as G_Runner

        with patch.object(FM_Runner, "run_command", new=mock_run):
            inst = G_Runner()
            self.assertIs(inst.run_command, mock_run)

        # After context exit, ensure exact restoration
        self.assertIs(FM_Runner.run_command, orig_run)
        self.assertIs(G_Runner.run_command, orig_run)


class AdversarialExecutionError(RuntimeError):
    def __init__(self, msg: str, code: int = 500) -> None:
        super().__init__(msg)
        self.code = code


class TestArea4PickleSerializationAndExceptions(unittest.TestCase):
    """Area 4: Pickle serialization/deserialization across namespaces and legacy payloads."""

    def test_pickle_roundtrip_events(self):
        """Serialize via golem, deserialize via golem, and verify equality."""
        from golem.agent.protocol import (
            TextDeltaEvent,
            StatusUpdateEvent,
            FinishEvent,
        )
        from golem.agent.protocol import (
            TextDeltaEvent as FM_TextDeltaEvent,
            StatusUpdateEvent as FM_StatusUpdateEvent,
            FinishEvent as FM_FinishEvent,
        )

        events = [
            TextDeltaEvent(text_delta="stream payload"),
            StatusUpdateEvent(status="working", detail="running test"),
            FinishEvent(response="completed successfully", status="SUCCESS"),
        ]

        for ev in events:
            raw = pickle.dumps(ev)
            loaded = pickle.loads(raw)
            self.assertEqual(type(loaded).__name__, type(ev).__name__)
            self.assertIsInstance(loaded, type(ev))
            if hasattr(ev, "text_delta"):
                self.assertEqual(loaded.text_delta, ev.text_delta)
                self.assertIsInstance(loaded, FM_TextDeltaEvent)
            elif hasattr(ev, "detail"):
                self.assertEqual(loaded.detail, ev.detail)
                self.assertIsInstance(loaded, FM_StatusUpdateEvent)
            elif hasattr(ev, "response"):
                self.assertEqual(loaded.response, ev.response)
                self.assertIsInstance(loaded, FM_FinishEvent)

    def test_pickle_session_state_enum(self):
        """Serialize SessionState enum and verify identity preservation."""
        from golem.gateway.session import SessionState as G_State
        from golem.gateway.session import SessionState as FM_State

        for state in G_State:
            dumped = pickle.dumps(state)
            loaded = pickle.loads(dumped)
            self.assertIs(loaded, state)
            self.assertIs(loaded, getattr(FM_State, state.name))

    def test_pickle_legacy_serialized_payload_resolution(self):
        """Legacy pickle payloads referring to 'golem.agent.protocol' deserialize in golem."""
        from golem.agent.protocol import TextDeltaEvent

        ev = TextDeltaEvent(text_delta="legacy_stream_test")
        dumped = pickle.dumps(ev)

        loaded = pickle.loads(dumped)
        self.assertIsInstance(loaded, TextDeltaEvent)
        self.assertEqual(loaded.text_delta, "legacy_stream_test")

    def test_exception_serialization_and_cross_catchability(self):
        """Custom exception serialized across namespaces catches cleanly."""
        golem.execution.runner.AdversarialExecutionError = AdversarialExecutionError
        err = AdversarialExecutionError("adversarial execution failure", code=503)
        err_bytes = pickle.dumps(err)

        loaded_err = pickle.loads(err_bytes)
        self.assertEqual(loaded_err.code, 503)

        with self.assertRaises(golem.execution.runner.AdversarialExecutionError):
            raise loaded_err


class TestArea5ImportlibReloadRobustness(unittest.TestCase):
    """Area 5: importlib.reload() without sys.modules or sys.meta_path corruption."""

    def test_importlib_reload_flattened_and_subpackages(self):
        """Reloading top-level and submodules preserves sys.modules integrity."""
        importlib.reload(golem)
        self.assertTrue(hasattr(golem, "ingress"))
        self.assertTrue(hasattr(golem, "gateway"))

    def test_submodule_reload_maintains_identity(self):
        """Reloading submodules preserves exact sys.modules identity across namespaces."""
        reloaded_g = importlib.reload(golem.gateway.router)
        reloaded_fm = importlib.reload(golem.gateway.router)

        self.assertIs(sys.modules["golem.gateway.router"], sys.modules["golem.gateway.router"])
        self.assertIs(reloaded_g, sys.modules["golem.gateway.router"])

    def test_consecutive_reloads_stress(self):
        """Consecutive reloads do not destabilize module identity."""
        for _ in range(5):
            importlib.reload(golem.gateway.session)
            importlib.reload(golem.gateway.session)
            self.assertIs(
                sys.modules["golem.gateway.session"],
                sys.modules["golem.gateway.session"],
            )


class TestArea6DeepDynamicImports(unittest.TestCase):
    """Area 6: Deep dynamic imports and error handling."""

    def test_dunder_import_deep_submodule(self):
        """__import__('golem.gateway.router') resolves and populates sys.modules."""
        mod = __import__("golem.gateway.router")
        self.assertEqual(mod.__name__, "golem")
        self.assertIn("golem.gateway.router", sys.modules)
        self.assertIs(
            sys.modules["golem.gateway.router"],
            sys.modules["golem.gateway.router"],
        )

    def test_dunder_import_with_fromlist(self):
        """__import__('golem.gateway.router', fromlist=['SessionRouter']) returns leaf module."""
        mod = __import__("golem.gateway.router", fromlist=["SessionRouter"])
        self.assertIs(mod, sys.modules["golem.gateway.router"])
        self.assertIs(mod, sys.modules["golem.gateway.router"])
        self.assertTrue(hasattr(mod, "SessionRouter"))

    def test_importlib_import_module_4_level_deep(self):
        """importlib.import_module on deeply nested submodule."""
        mod = importlib.import_module("golem.agent.drivers.antigravity")
        self.assertIn(mod.__name__, ["golem.agent.drivers.antigravity", "golem.drivers.antigravity"])
        self.assertTrue(hasattr(mod, "AntigravityDriver"))
        self.assertIs(
            sys.modules["golem.agent.drivers.antigravity"],
            sys.modules["golem.agent.drivers.antigravity"],
        )

    def test_nonexistent_submodule_raises_module_not_found(self):
        """Importing non-existent submodule raises ModuleNotFoundError cleanly."""
        with self.assertRaises(ModuleNotFoundError):
            importlib.import_module("golem.gateway.nonexistent_submodule_xyz")

        with self.assertRaises(ModuleNotFoundError):
            __import__("golem.nonexistent_submodule_abc")


class TestBoundaryObservations(unittest.TestCase):
    """Boundary conditions and architectural characteristics observed during challenge."""

    def test_top_level_package_is_not_aliased_by_design(self):
        """Verify top-level package and subpackage exports."""
        self.assertIs(golem.gateway.ingress, golem.ingress)
        self.assertIs(golem.gateway.router, golem.router)
        self.assertIs(golem.execution.runner, golem.runner)
        self.assertIs(golem.sensory.bus, golem.bus)
        self.assertIs(golem.transport.chunker, golem.chunker)


if __name__ == "__main__":
    unittest.main()
