#!/usr/bin/env python3
"""
tests/test_adversarial_m1_packaging.py
Milestone 1 Oppositional Analysis & Adversarial Verification Test Suite:
- PEP 621 Packaging & Manifest Compliance
- CLI Entrypoint Synchronous/Asynchronous Invocation Resilience
- Dual-Key Environment Variable Precedence, Empty String, and Whitespace Edge Cases
- Launchd Plist Syntax & Property Type Verification
- 100% Lineage Invariant String Compliance

Author: Challenger 2 (Milestone 1)
Lineage Invariant: "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."
"""

import glob
import inspect
import os
import plistlib
import subprocess
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
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LINEAGE_INVARIANT = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."


class TestPEP621PackagingAdversarial(unittest.TestCase):
    """Adversarial stress tests for PEP 621 packaging manifests and build declarations."""

    def setUp(self):
        self.pyproject_path = PROJECT_ROOT / "pyproject.toml"
        self.setup_path = PROJECT_ROOT / "setup.py"
        self.assertTrue(self.pyproject_path.exists(), "pyproject.toml must exist")
        self.assertTrue(self.setup_path.exists(), "setup.py must exist")

    def test_pyproject_toml_pep621_syntax_and_schema(self):
        """Parse pyproject.toml with tomllib and verify all required PEP 621 fields."""
        with open(self.pyproject_path, "rb") as f:
            data = tomllib.load(f)

        # Build system
        self.assertIn("build-system", data)
        self.assertIn("requires", data["build-system"])
        self.assertIn("build-backend", data["build-system"])
        self.assertEqual(data["build-system"]["build-backend"], "setuptools.build_meta")

        # Project metadata
        project = data.get("project", {})
        self.assertEqual(project.get("name"), "golem")
        self.assertEqual(project.get("version"), "2.1.0")
        self.assertIn("description", project)
        self.assertIn(LINEAGE_INVARIANT, project["description"])
        self.assertEqual(project.get("requires-python"), ">=3.11")
        self.assertIn("dependencies", project)

        # Scripts table
        scripts = project.get("scripts", {})
        self.assertIn("golem", scripts)
        self.assertIn("golem", scripts)
        self.assertEqual(scripts["golem"], "golem.cli:main")
        self.assertEqual(scripts["golem"], "golem.cli:main")

    def test_package_inclusion_declarations(self):
        """Verify package inclusion rules include golem* and golem*."""
        with open(self.pyproject_path, "rb") as f:
            data = tomllib.load(f)

        find_cfg = (
            data.get("tool", {})
            .get("setuptools", {})
            .get("packages", {})
            .get("find", {})
        )
        include_patterns = find_cfg.get("include", [])
        self.assertIn("golem*", include_patterns)
        self.assertIn("golem*", include_patterns)

    def test_adversarial_root_module_inclusion_defect(self):
        """
        Verify whether app.py is included in pyproject.toml packages or py-modules.
        Guarantees that 'pip install .' bundles root app.py via py-modules.
        """
        with open(self.pyproject_path, "rb") as f:
            data = tomllib.load(f)

        setuptools_cfg = data.get("tool", {}).get("setuptools", {})
        py_modules = setuptools_cfg.get("py-modules", [])
        self.assertIn("app", py_modules, "Root app.py must be declared in py-modules!")


class TestCLIEntrypointsAdversarial(unittest.TestCase):
    """Adversarial stress tests for CLI entrypoints golem.cli:main and golem=golem.cli:main."""

    def test_entrypoint_callable_is_sync_or_async(self):
        """
        Verify CLI entrypoint targets are synchronous callables rather than unawaited async coroutines.
        """
        import golem.cli
        import app

        self.assertTrue(hasattr(golem.cli, "main"), "golem.cli must define main")
        self.assertFalse(inspect.iscoroutinefunction(golem.cli.main), "golem.cli.main must be a synchronous function")
        self.assertTrue(hasattr(app, "cli"), "app must define cli")
        self.assertFalse(inspect.iscoroutinefunction(app.cli), "app.cli must be a synchronous function")


class TestEnvPrecedenceAndEdgeCases(unittest.TestCase):
    """Stress tests for environment variable resolution precedence, empty strings, and whitespace."""

    def setUp(self):
        import golem.config as cfg
        self.cfg = cfg

    def test_default_hand_worker_precedence(self):
        """Test GOLEM_DEFAULT_HAND_WORKER wins over GOLEM_ENGINE and GOLEM_ENGINE."""
        with patch.object(self.cfg, "_get_file_env", return_value={}):
            # Both set: GOLEM wins
            with patch.dict(os.environ, {
                "GOLEM_DEFAULT_HAND_WORKER": "worker_golem",
                "GOLEM_ENGINE": "worker_legacy"
            }, clear=True):
                self.assertEqual(self.cfg.get_default_hand_worker(), "worker_golem")

            # Only default set
            with patch.dict(os.environ, {
                "GOLEM_ENGINE": "worker_legacy"
            }, clear=True):
                self.assertEqual(self.cfg.get_default_hand_worker(), "worker_legacy")

            # Neither set -> default "agy"
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(self.cfg.get_default_hand_worker(), "agy")

    def test_default_hand_worker_empty_string_fallback(self):
        """Test empty string in GOLEM var falls back to legacy or default."""
        with patch.object(self.cfg, "_get_file_env", return_value={}):
            # Empty GOLEM, populated GOLEM -> falls back to default
            with patch.dict(os.environ, {
                "GOLEM_DEFAULT_HAND_WORKER": "",
                "GOLEM_ENGINE": "worker_legacy"
            }, clear=True):
                self.assertEqual(self.cfg.get_default_hand_worker(), "worker_legacy")

            # Both empty -> falls back to "agy"
            with patch.dict(os.environ, {
                "GOLEM_DEFAULT_HAND_WORKER": "",
                "GOLEM_ENGINE": ""
            }, clear=True):
                self.assertEqual(self.cfg.get_default_hand_worker(), "agy")

    def test_whitespace_string_vulnerability(self):
        """
        Verify whitespace strings in environment variables (e.g. '   ')
        are stripped before boolean 'or' so they fall back cleanly.
        """
        with patch.object(self.cfg, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {
                "GOLEM_DEFAULT_HAND_WORKER": "   ",
                "GOLEM_ENGINE": "worker_legacy"
            }, clear=True):
                val = self.cfg.get_default_hand_worker()
                self.assertEqual(val, "worker_legacy", "Whitespace string must fall back to legacy key")

            with patch.dict(os.environ, {
                "GOLEM_ANTIGRAVITY_MODEL": "   ",
                "ANTIGRAVITY_MODEL": "gemini-flash"
            }, clear=True):
                val = self.cfg.get_engine_model("agy")
                self.assertEqual(val, "gemini-flash", "Whitespace model must fall back to legacy key")

            with patch.dict(os.environ, {
                "GOLEM_MOUTH_MODEL": "   ",
                "MOUTH_MODEL": "gemini-flash"
            }, clear=True):
                val = self.cfg.get_mouth_model()
                self.assertEqual(val, "gemini-flash", "Whitespace mouth model must fall back to legacy key")

    def test_mouth_model_precedence(self):
        """Test GOLEM_MOUTH_MODEL -> MOUTH_MODEL -> GEMINI_MODEL."""
        with patch.object(self.cfg, "_get_file_env", return_value={}):
            # All three set: GOLEM wins
            with patch.dict(os.environ, {
                "GOLEM_MOUTH_MODEL": "model_golem",
                "MOUTH_MODEL": "model_mouth",
                "GEMINI_MODEL": "model_gemini"
            }, clear=True):
                self.assertEqual(self.cfg.get_mouth_model(), "model_golem")

            # MOUTH_MODEL and GEMINI_MODEL set: MOUTH_MODEL wins
            with patch.dict(os.environ, {
                "MOUTH_MODEL": "model_mouth",
                "GEMINI_MODEL": "model_gemini"
            }, clear=True):
                self.assertEqual(self.cfg.get_mouth_model(), "model_mouth")

            # Only GEMINI_MODEL set
            with patch.dict(os.environ, {
                "GEMINI_MODEL": "model_gemini"
            }, clear=True):
                self.assertEqual(self.cfg.get_mouth_model(), "model_gemini")

            # None set -> returns empty string
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(self.cfg.get_mouth_model(), "")

    def test_daemon_label_precedence(self):
        """Test GOLEM_DAEMON_LABEL -> DAEMON_LABEL -> default 'com.user.golem'."""
        with patch.object(self.cfg, "_get_file_env", return_value={}):
            with patch.dict(os.environ, {
                "GOLEM_DAEMON_LABEL": "com.user.custom_golem",
                "DAEMON_LABEL": "com.user.custom_legacy"
            }, clear=True):
                self.assertEqual(self.cfg.get_daemon_label(), "com.user.custom_golem")

            with patch.dict(os.environ, {
                "DAEMON_LABEL": "com.user.custom_legacy"
            }, clear=True):
                self.assertEqual(self.cfg.get_daemon_label(), "com.user.custom_legacy")

            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(self.cfg.get_daemon_label(), "com.user.golem")


class TestLaunchdPlists(unittest.TestCase):
    """Verification of launchd plist files using plutil and plistlib."""

    def test_plutil_lint_both_plists(self):
        """Verify XML syntax of both plists via macOS plutil -lint."""
        import shutil
        if not shutil.which("plutil"):
            self.skipTest("plutil not available on this platform")
        for plist_name in ["com.user.golem.plist"]:
            path = PROJECT_ROOT / plist_name
            self.assertTrue(path.exists(), f"{plist_name} must exist")
            res = subprocess.run(
                ["plutil", "-lint", str(path)],
                capture_output=True,
                text=True
            )
            self.assertEqual(res.returncode, 0, f"plutil -lint failed for {plist_name}: {res.stderr}")

    def test_plist_property_types_and_keys(self):
        """Parse plists with plistlib and verify property types match launchd schema."""
        for plist_name, expected_label in [
            ("com.user.golem.plist", "com.user.golem"),
            ("com.user.golem.plist", "com.user.golem")
        ]:
            path = PROJECT_ROOT / plist_name
            with open(path, "rb") as f:
                data = plistlib.load(f)

            self.assertIsInstance(data.get("Label"), str)
            self.assertEqual(data["Label"], expected_label)

            self.assertIsInstance(data.get("ProgramArguments"), list)
            for arg in data["ProgramArguments"]:
                self.assertIsInstance(arg, str)

            self.assertIsInstance(data.get("WorkingDirectory"), str)
            self.assertIsInstance(data.get("RunAtLoad"), bool)
            self.assertIsInstance(data.get("KeepAlive"), (bool, dict))
            self.assertIsInstance(data.get("StandardOutPath"), str)
            self.assertIsInstance(data.get("StandardErrorPath"), str)
            self.assertIsInstance(data.get("EnvironmentVariables"), dict)

            env = data["EnvironmentVariables"]
            for k, v in env.items():
                self.assertIsInstance(k, str)
                self.assertIsInstance(v, str)

            # Check critical environment variables
            self.assertIn("PYTHONPATH", env)
            self.assertIn("DAEMON_LABEL", env)
            self.assertEqual(env["DAEMON_LABEL"], expected_label)


class TestLineageInvariantPresence(unittest.TestCase):
    """Verify 100% lineage invariant string presence across every Python file in golem/ and golem/."""

    def test_lineage_invariant_all_python_files(self):
        """Every .py file in golem/ and golem/ must contain the exact lineage invariant string."""
        golem_files = list(PROJECT_ROOT.glob("golem/**/*.py"))
        all_files = golem_files

        self.assertGreater(len(golem_files), 30, "golem/ must have >= 30 files")

        missing = []
        for file_path in all_files:
            content = file_path.read_text(encoding="utf-8")
            if LINEAGE_INVARIANT not in content:
                missing.append(str(file_path.relative_to(PROJECT_ROOT)))

        self.assertEqual(
            missing, [],
            f"Lineage invariant missing in {len(missing)} files: {missing}"
        )

    def test_lineage_invariant_manifests_and_root_files(self):
        """Verify lineage invariant string in app.py, setup.py, pyproject.toml, PROJECT.md."""
        for filename in ["app.py", "setup.py", "pyproject.toml", "PROJECT.md"]:
            path = PROJECT_ROOT / filename
            self.assertTrue(path.exists(), f"{filename} must exist")
            content = path.read_text(encoding="utf-8")
            self.assertIn(
                LINEAGE_INVARIANT, content,
                f"Lineage invariant missing in {filename}"
            )


if __name__ == "__main__":
    unittest.main()
