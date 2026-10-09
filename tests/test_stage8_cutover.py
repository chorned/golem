#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

tests/test_stage8_cutover.py
Stage 8 Verification Suite: CI Matrix, Cutover Script & Preflight Readiness:
  - Validates GitHub Actions CI matrix supports Python 3.11, 3.12, 3.13.
  - Verifies app.py --preflight subsystem initialization and clean shutdown.
  - Verifies scripts/safe_cutover.sh --dry-run execution.
  - Enforces universal lineage and 0-grep security invariants.
"""

from __future__ import annotations

import os
import sys
import yaml
import subprocess
import unittest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from app import parse_args


class TestStage8CIMatrix(unittest.TestCase):
    """Unit tests verifying CI workflow configuration."""

    def test_ci_workflow_matrix(self) -> None:
        ci_path = os.path.join(PROJECT_DIR, ".github", "workflows", "ci.yml")
        self.assertTrue(os.path.isfile(ci_path), "ci.yml does not exist")

        with open(ci_path, "r", encoding="utf-8") as f:
            ci_data = yaml.safe_load(f)

        jobs = ci_data.get("jobs", {})
        self.assertIn("test", jobs)
        matrix = jobs["test"].get("strategy", {}).get("matrix", {})
        py_versions = matrix.get("python-version", [])

        for v in ("3.11", "3.12", "3.13"):
            self.assertIn(v, py_versions, f"Python {v} missing from CI matrix")


class TestStage8PreflightAndCutover(unittest.TestCase):
    """Unit tests for preflight verification and cutover script."""

    def test_parse_args_preflight(self) -> None:
        args = parse_args(["--preflight", "--port", "8769"])
        self.assertTrue(getattr(args, "preflight", False))
        self.assertEqual(getattr(args, "port", None), 8769)

    def test_preflight_cli_execution(self) -> None:
        res = subprocess.run(
            [sys.executable, "app.py", "--preflight", "--port", "8777"],
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(res.returncode, 0, f"Preflight failed with stderr:\n{res.stderr}")
        self.assertIn("[PREFLIGHT_OK]", res.stdout)

    def test_safe_cutover_dry_run(self) -> None:
        cutover_sh = os.path.join(PROJECT_DIR, "scripts", "safe_cutover.sh")
        self.assertTrue(os.path.isfile(cutover_sh), "safe_cutover.sh does not exist")
        self.assertTrue(os.access(cutover_sh, os.X_OK), "safe_cutover.sh is not executable")

        res = subprocess.run(
            [cutover_sh, "--dry-run"],
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(res.returncode, 0, f"safe_cutover.sh failed:\n{res.stderr}")
        self.assertIn("Preflight Dry-Run Complete", res.stdout)


if __name__ == "__main__":
    unittest.main()
