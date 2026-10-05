#!/usr/bin/env python3
"""
tests/test_golem_namespace_and_shims.py
Comprehensive test suite verifying Milestone 1:
Package Namespace Migration & Lineage Invariant.

Verifies:
1. Direct import of `golem` and all 5 core subpackages + config.
2. Exact object identity (assertIs) between `first_mate.*` and `golem.*` symbols and modules.
3. Bidirectional unittest.mock.patch fidelity across `first_mate.*` and `golem.*`.
4. Lineage invariant enforcement across modules, docstrings, and manifests.
5. Dual-key environment variable resolution in `golem.config` and `app`.
6. Launchd plist configuration for `com.user.golem.plist`.
7. PEP 621 packaging manifests (pyproject.toml, setup.py).

Lineage Invariant:
"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."
"""

import os
import sys
try:
    import tomllib
except ModuleNotFoundError:
    import toml
    class _TomlWrapper:
        @staticmethod
        def load(f):
            content = f.read()
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            return toml.loads(content)
        @staticmethod
        def loads(s):
            return toml.loads(s)
    tomllib = _TomlWrapper()
import plistlib
import unittest
from unittest.mock import patch, MagicMock, AsyncMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import golem
import golem.config
import golem.gateway
import golem.agent
import golem.execution
import golem.sensory
import golem.transport

import first_mate
import first_mate.config
import first_mate.gateway
import first_mate.agent
import first_mate.execution
import first_mate.sensory
import first_mate.transport

EXPECTED_LINEAGE = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."


class TestGolemNamespaceImports(unittest.TestCase):
    """Verifies that golem package and all subpackages can be imported directly."""

    def test_golem_top_level_package(self):
        """Verifies golem metadata and top-level subpackage exports."""
        self.assertEqual(golem.__version__, "2.1.0")
        self.assertEqual(golem.LINEAGE, EXPECTED_LINEAGE)
        self.assertIn(EXPECTED_LINEAGE, golem.__doc__)
        self.assertTrue(hasattr(golem, "gateway"))
        self.assertTrue(hasattr(golem, "agent"))
        self.assertTrue(hasattr(golem, "execution"))
        self.assertTrue(hasattr(golem, "sensory"))
        self.assertTrue(hasattr(golem, "transport"))
        self.assertTrue(hasattr(golem, "config"))

    def test_all_34_submodules_importable(self):
        """Verifies each canonical module in golem is importable."""
        modules = [
            "golem.config",
            "golem.agent",
            "golem.agent.adapter",
            "golem.agent.calibration",
            "golem.agent.events",
            "golem.agent.mouth",
            "golem.agent.prompts",
            "golem.agent.protocol",
            "golem.agent.drivers",
            "golem.agent.drivers.base",
            "golem.agent.drivers.antigravity",
            "golem.agent.drivers.claude_code",
            "golem.agent.drivers.opencode",
            "golem.execution",
            "golem.execution.buffer",
            "golem.execution.delegation",
            "golem.execution.harness",
            "golem.execution.mesh",
            "golem.execution.runner",
            "golem.gateway",
            "golem.gateway.ingress",
            "golem.gateway.router",
            "golem.gateway.session",
            "golem.sensory",
            "golem.sensory.bus",
            "golem.sensory.eyes",
            "golem.sensory.nose",
            "golem.transport",
            "golem.transport.chunker",
            "golem.transport.file_transfer",
            "golem.transport.milestone",
            "golem.transport.normalizer",
            "golem.transport.slack_client",
        ]
        import importlib
        for mod_name in modules:
            mod = importlib.import_module(mod_name)
            self.assertIsNotNone(mod, f"Failed to import {mod_name}")


class TestDualLayerShimIdentity(unittest.TestCase):
    """Verifies exact object identity (assertIs) between first_mate.* and golem.*."""

    def test_module_identity_in_sys_modules(self):
        """Verifies sys.modules entries for first_mate.* map to golem.* modules."""
        submodules = [
            "gateway",
            "gateway.router",
            "gateway.session",
            "gateway.ingress",
            "agent",
            "agent.adapter",
            "agent.mouth",
            "agent.protocol",
            "agent.drivers",
            "agent.drivers.base",
            "agent.drivers.antigravity",
            "agent.drivers.claude_code",
            "agent.drivers.opencode",
            "execution",
            "execution.harness",
            "execution.runner",
            "execution.buffer",
            "execution.delegation",
            "execution.mesh",
            "sensory",
            "sensory.bus",
            "sensory.eyes",
            "sensory.nose",
            "transport",
            "transport.chunker",
            "transport.normalizer",
            "transport.slack_client",
            "transport.milestone",
            "transport.file_transfer",
            "config",
        ]
        import importlib
        for sub in submodules:
            fm_mod = importlib.import_module(f"first_mate.{sub}")
            g_mod = importlib.import_module(f"golem.{sub}")
            self.assertIs(
                fm_mod,
                g_mod,
                f"Module identity failed for first_mate.{sub} vs golem.{sub}"
            )
            self.assertIs(
                sys.modules[f"first_mate.{sub}"],
                sys.modules[f"golem.{sub}"],
                f"sys.modules identity failed for {sub}"
            )

    def test_class_and_function_symbol_identity(self):
        """Verifies key classes and functions share exact assertIs identity."""
        from first_mate.gateway.router import SessionRouter as FM_SR
        from golem.gateway.router import SessionRouter as G_SR
        self.assertIs(FM_SR, G_SR)

        from first_mate.gateway.session import SessionHandle as FM_SH, SessionState as FM_SS
        from golem.gateway.session import SessionHandle as G_SH, SessionState as G_SS
        self.assertIs(FM_SH, G_SH)
        self.assertIs(FM_SS, G_SS)

        from first_mate.gateway.ingress import IngressGateway as FM_IG
        from golem.gateway.ingress import IngressGateway as G_IG
        self.assertIs(FM_IG, G_IG)

        from first_mate.agent.adapter import HeadlessAgentAdapter as FM_HAA
        from golem.agent.adapter import HeadlessAgentAdapter as G_HAA
        self.assertIs(FM_HAA, G_HAA)

        from first_mate.agent.protocol import AgentEvent as FM_AE, TextDeltaEvent as FM_TDE
        from golem.agent.protocol import AgentEvent as G_AE, TextDeltaEvent as G_TDE
        self.assertIs(FM_AE, G_AE)
        self.assertIs(FM_TDE, G_TDE)

        from first_mate.agent.drivers.antigravity import AntigravityDriver as FM_AGD
        from golem.agent.drivers.antigravity import AntigravityDriver as G_AGD
        self.assertIs(FM_AGD, G_AGD)

        from first_mate.execution.harness import DualPaneHarness as FM_DPH
        from golem.execution.harness import DualPaneHarness as G_DPH
        self.assertIs(FM_DPH, G_DPH)

        from first_mate.execution.runner import RunnerExecutor as FM_RE, ExecutionHandle as FM_EH
        from golem.execution.runner import RunnerExecutor as G_RE, ExecutionHandle as G_EH
        self.assertIs(FM_RE, G_RE)
        self.assertIs(FM_EH, G_EH)

        from first_mate.sensory.bus import ShoulderTapEventBus as FM_EB
        from golem.sensory.bus import ShoulderTapEventBus as G_EB
        self.assertIs(FM_EB, G_EB)

        from first_mate.sensory.eyes import EyesTaskObserver as FM_EO
        from golem.sensory.eyes import EyesTaskObserver as G_EO
        self.assertIs(FM_EO, G_EO)

        from first_mate.sensory.nose import NoseSentinel as FM_NS
        from golem.sensory.nose import NoseSentinel as G_NS
        self.assertIs(FM_NS, G_NS)

        from first_mate.transport.normalizer import sanitize_for_slack as FM_SFS
        from golem.transport.normalizer import sanitize_for_slack as G_SFS
        self.assertIs(FM_SFS, G_SFS)

        from first_mate.transport.chunker import split_message_boundary_aware as FM_SMBA
        from golem.transport.chunker import split_message_boundary_aware as G_SMBA
        self.assertIs(FM_SMBA, G_SMBA)


class TestBidirectionalMockPatchFidelity(unittest.TestCase):
    """Verifies that unittest.mock.patch operates bidirectionally across namespaces."""

    def test_patch_first_mate_intercepts_golem_execution(self):
        """Verifies patching first_mate.gateway.ingress.call_gemini_api intercepts golem caller."""
        import golem.gateway.ingress as gmi

        mock_coro = AsyncMock(return_value={"text": "patched_via_first_mate"})
        with patch("first_mate.gateway.ingress.call_gemini_api", new=mock_coro):
            self.assertIs(gmi.call_gemini_api, mock_coro)

    def test_patch_golem_intercepts_first_mate_execution(self):
        """Verifies patching golem.gateway.ingress.call_gemini_api intercepts first_mate caller."""
        import first_mate.gateway.ingress as fmi

        mock_coro = AsyncMock(return_value={"text": "patched_via_golem"})
        with patch("golem.gateway.ingress.call_gemini_api", new=mock_coro):
            self.assertIs(fmi.call_gemini_api, mock_coro)

    def test_patch_runner_executor_bidirectional(self):
        """Verifies RunnerExecutor.run_command patch intercepts both namespaces."""
        from first_mate.execution.runner import RunnerExecutor as FM_Runner
        from golem.execution.runner import RunnerExecutor as G_Runner

        mock_run = AsyncMock(return_value=MagicMock(status="completed", stdout="mock_out"))
        with patch.object(FM_Runner, "run_command", new=mock_run):
            g_inst = G_Runner()
            self.assertIs(g_inst.run_command, mock_run)

        mock_run2 = AsyncMock(return_value=MagicMock(status="completed", stdout="mock_out_2"))
        with patch.object(G_Runner, "run_command", new=mock_run2):
            fm_inst = FM_Runner()
            self.assertIs(fm_inst.run_command, mock_run2)


class TestLineageInvariantEnforcement(unittest.TestCase):
    """Verifies the Lineage Invariant is present across packages and docstrings."""

    def test_package_constants(self):
        """Verifies LINEAGE constant on golem and first_mate packages."""
        self.assertEqual(golem.LINEAGE, EXPECTED_LINEAGE)
        self.assertEqual(first_mate.LINEAGE, EXPECTED_LINEAGE)

    def test_all_modules_contain_lineage_docstring(self):
        """Verifies every .py file in golem/ and first_mate/ contains the Lineage Invariant."""
        import glob
        all_py = glob.glob(os.path.join(PROJECT_DIR, "golem", "**", "*.py"), recursive=True) + \
                 glob.glob(os.path.join(PROJECT_DIR, "first_mate", "**", "*.py"), recursive=True)
        self.assertGreaterEqual(len(all_py), 30)
        for filepath in all_py:
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
            self.assertIn(
                EXPECTED_LINEAGE,
                content,
                f"Missing lineage invariant in {os.path.relpath(filepath, PROJECT_DIR)}"
            )


class TestDualKeyEnvironmentVariableResolution(unittest.TestCase):
    """Verifies dual-key environment variable resolution in golem.config."""

    def setUp(self):
        self.orig_env = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.orig_env)

    def test_active_engine_dual_key(self):
        """Verifies GOLEM_DEFAULT_HAND_WORKER -> GOLEM_ENGINE -> FIRST_MATE_ENGINE."""
        os.environ.pop("GOLEM_DEFAULT_HAND_WORKER", None)
        os.environ.pop("GOLEM_ENGINE", None)
        os.environ.pop("FIRST_MATE_ENGINE", None)

        os.environ["FIRST_MATE_ENGINE"] = "claude_code"
        self.assertEqual(golem.config.get_active_engine(), "claude_code")

        os.environ["GOLEM_ENGINE"] = "opencode"
        self.assertEqual(golem.config.get_active_engine(), "opencode")

        os.environ["GOLEM_DEFAULT_HAND_WORKER"] = "antigravity"
        self.assertEqual(golem.config.get_active_engine(), "antigravity")

    def test_default_hand_worker(self):
        """Verifies get_default_hand_worker defaults to 'agy'."""
        os.environ.pop("GOLEM_DEFAULT_HAND_WORKER", None)
        os.environ.pop("GOLEM_ENGINE", None)
        os.environ.pop("FIRST_MATE_ENGINE", None)
        self.assertIn(golem.config.get_default_hand_worker(), ["agy", "antigravity"])

    def test_model_resolution_dual_key(self):
        """Verifies model resolution handles GOLEM_* and bare/FIRST_MATE keys."""
        os.environ.pop("GOLEM_MOUTH_MODEL", None)
        os.environ.pop("MOUTH_MODEL", None)
        os.environ.pop("GEMINI_MODEL", None)

        os.environ["MOUTH_MODEL"] = "gemini-2.0-flash"
        self.assertEqual(golem.config.get_mouth_model(), "gemini-2.0-flash")

        os.environ["GOLEM_MOUTH_MODEL"] = "gemini-3.8-flash"
        self.assertEqual(golem.config.get_mouth_model(), "gemini-3.8-flash")


class TestPackagingAndLaunchdService(unittest.TestCase):
    """Verifies pyproject.toml, setup.py, and launchd plist manifests."""

    def test_pyproject_toml_pep621(self):
        """Verifies pyproject.toml PEP 621 structure, scripts, and lineage description."""
        pyproject_path = os.path.join(PROJECT_DIR, "pyproject.toml")
        self.assertTrue(os.path.isfile(pyproject_path))
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)
        project = data.get("project", {})
        self.assertEqual(project.get("name"), "golem")
        self.assertEqual(project.get("version"), "2.1.0")
        self.assertEqual(project.get("description"), EXPECTED_LINEAGE)
        scripts = project.get("scripts", {})
        self.assertEqual(scripts.get("golem"), "golem.cli:main")
        self.assertEqual(scripts.get("first-mate"), "golem.cli:main")

    def test_setup_py_exists_and_parses(self):
        """Verifies setup.py exists and contains expected lineage description."""
        setup_path = os.path.join(PROJECT_DIR, "setup.py")
        self.assertTrue(os.path.isfile(setup_path))
        with open(setup_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(EXPECTED_LINEAGE, content)
        self.assertIn('golem=golem.cli:main', content)
        self.assertIn('first-mate=golem.cli:main', content)

    def test_com_user_golem_plist(self):
        """Verifies com.user.golem.plist exists, parses, and has valid configuration."""
        plist_path = os.path.join(PROJECT_DIR, "com.user.golem.plist")
        self.assertTrue(os.path.isfile(plist_path))
        with open(plist_path, "rb") as f:
            data = plistlib.load(f)
        self.assertEqual(data.get("Label"), "com.user.golem")
        self.assertTrue(data.get("RunAtLoad"))
        self.assertTrue(data.get("KeepAlive"))
        env = data.get("EnvironmentVariables", {})
        self.assertEqual(env.get("DAEMON_LABEL"), "com.user.golem")
        self.assertEqual(env.get("GOLEM_DAEMON_LABEL"), "com.user.golem")
        self.assertEqual(env.get("PYTHONPATH"), "/Users/hermes/projects/first-mate")


class TestMilestone1Remediations(unittest.TestCase):
    """Verifies remediations for Milestone 1 oppositional analysis findings."""

    def test_first_mate_dynamic_attribute_lookup(self):
        """Verifies dynamically attached attributes on golem resolve via first_mate.__getattr__."""
        golem.DYNAMIC_REMEDIATION_KEY = "verified_123"
        try:
            self.assertEqual(first_mate.DYNAMIC_REMEDIATION_KEY, "verified_123")
        finally:
            if hasattr(golem, "DYNAMIC_REMEDIATION_KEY"):
                delattr(golem, "DYNAMIC_REMEDIATION_KEY")

        with self.assertRaises(AttributeError):
            _ = first_mate.NON_EXISTENT_DYNAMIC_ATTR_XYZ

    def test_synchronous_cli_entrypoints(self):
        """Verifies golem.cli.main and app.cli are synchronous functions."""
        import inspect
        import golem.cli
        import app

        self.assertTrue(hasattr(golem.cli, "main"))
        self.assertFalse(inspect.iscoroutinefunction(golem.cli.main))
        self.assertTrue(hasattr(app, "cli"))
        self.assertFalse(inspect.iscoroutinefunction(app.cli))

    def test_whitespace_environment_variable_stripping(self):
        """Verifies whitespace env vars are stripped and fall back properly."""
        with patch.object(golem.config, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {
                "GOLEM_DEFAULT_HAND_WORKER": "   ",
                "FIRST_MATE_ENGINE": "agy_fallback"
            }, clear=True):
                self.assertEqual(golem.config.get_default_hand_worker(), "agy_fallback")

            with patch.dict(os.environ, {
                "GOLEM_ANTIGRAVITY_MODEL": "   ",
                "ANTIGRAVITY_MODEL": "gemini-flash"
            }, clear=True):
                self.assertEqual(golem.config.get_engine_model("agy"), "gemini-flash")

            with patch.dict(os.environ, {
                "GOLEM_MOUTH_MODEL": "   ",
                "MOUTH_MODEL": "gemini-flash"
            }, clear=True):
                self.assertEqual(golem.config.get_mouth_model(), "gemini-flash")


if __name__ == "__main__":
    unittest.main()

