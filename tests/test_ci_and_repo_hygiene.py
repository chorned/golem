#!/usr/bin/env python3
"""
tests/test_ci_and_repo_hygiene.py
Unit tests for Milestone 4: CI/CD Release Pipeline & Repository Hygiene.

Lineage Invariant:
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import subprocess
import unittest
import yaml

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LINEAGE_INVARIANT = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."


class TestGithubActionsCIWorkflow(unittest.TestCase):
    """Verifies GitHub Actions CI workflow existence, schema, matrix, and test steps."""

    def setUp(self):
        self.workflow_path = os.path.join(PROJECT_ROOT, ".github", "workflows", "ci.yml")

    def test_workflow_file_exists(self):
        """Workflow file .github/workflows/ci.yml must exist."""
        self.assertTrue(os.path.isfile(self.workflow_path), f"Missing CI workflow file: {self.workflow_path}")

    def test_workflow_lineage_invariant(self):
        """Workflow header must codify the exact lineage invariant."""
        with open(self.workflow_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(LINEAGE_INVARIANT, content, "Workflow must embed lineage invariant")

    def test_workflow_yaml_syntax_valid(self):
        """Workflow must parse cleanly as standard YAML."""
        with open(self.workflow_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        self.assertIsInstance(data, dict, "Workflow YAML root must be a mapping dictionary")
        self.assertIn("name", data)
        self.assertIn("jobs", data)

    def test_workflow_triggers_on_main(self):
        """Workflow must trigger on push and pull_request targeting main branch."""
        with open(self.workflow_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        triggers = data.get("on") or data.get(True)
        self.assertIsNotNone(triggers, "Workflow triggers must be defined")

        # Handlers for push and pull_request
        self.assertTrue("push" in triggers or "push" in data, "Workflow must trigger on push")
        self.assertTrue("pull_request" in triggers or "pull_request" in data, "Workflow must trigger on pull_request")

        with open(self.workflow_path, "r", encoding="utf-8") as f:
            raw_text = f.read()
        self.assertIn("main", raw_text, "Workflow triggers must target main branch")

    def test_workflow_python_matrix(self):
        """Matrix must include Python versions 3.11, 3.12, and 3.13."""
        with open(self.workflow_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        jobs = data.get("jobs", {})
        self.assertIn("test", jobs, "Jobs must declare a 'test' job")
        test_job = jobs["test"]

        strategy = test_job.get("strategy", {})
        matrix = strategy.get("matrix", {})
        python_versions = [str(v) for v in matrix.get("python-version", [])]

        self.assertIn("3.11", python_versions, "Matrix must include Python 3.11")
        self.assertIn("3.12", python_versions, "Matrix must include Python 3.12")
        self.assertIn("3.13", python_versions, "Matrix must include Python 3.13")
        self.assertEqual(test_job.get("runs-on"), "ubuntu-latest", "Test job must run on ubuntu-latest")

    def test_workflow_step_invocations(self):
        """Workflow steps must invoke dependency install, tests, and package build."""
        with open(self.workflow_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Checkout and setup-python
        self.assertIn("actions/checkout@v4", content, "Workflow must use actions/checkout@v4")
        self.assertIn("actions/setup-python@v5", content, "Workflow must use actions/setup-python@v5")

        # Dependency installation
        self.assertIn("pip install", content, "Workflow must install pip dependencies")

        # Unit test execution
        self.assertIn("python -m unittest discover tests", content, "Workflow must invoke unit test discovery")

        # E2E test execution
        self.assertIn("tests/e2e", content, "Workflow must execute E2E test suite")

        # Packaging build verification
        self.assertIn("python -m build", content, "Workflow must verify packaging via build")

        # Security and secret check
        self.assertTrue("security" in content.lower() or "secret" in content.lower() or "bandit" in content.lower(),
                        "Workflow must include a security/secret scanning step")


class TestOpenSourceLicense(unittest.TestCase):
    """Verifies LICENSE file exists, adheres to Apache-2.0, and cites lineage invariant."""

    def setUp(self):
        self.license_path = os.path.join(PROJECT_ROOT, "LICENSE")

    def test_license_file_exists(self):
        """LICENSE file must exist at project root."""
        self.assertTrue(os.path.isfile(self.license_path), f"Missing LICENSE file: {self.license_path}")

    def test_license_contains_apache_terms(self):
        """LICENSE must contain standard Apache License Version 2.0 terms."""
        with open(self.license_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("Apache License", content)
        self.assertIn("Version 2.0, January 2004", content)
        self.assertIn("TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION", content)
        self.assertIn("1. Definitions.", content)
        self.assertIn("2. Grant of Copyright License.", content)
        self.assertIn("3. Grant of Patent License.", content)
        self.assertIn("4. Redistribution.", content)
        self.assertIn("5. Submission of Contributions.", content)
        self.assertIn("6. Trademarks.", content)
        self.assertIn("7. Disclaimer of Warranty.", content)
        self.assertIn("8. Limitation of Liability.", content)
        self.assertIn("9. Accepting Warranty or Additional Liability.", content)

    def test_license_contains_lineage_and_copyright(self):
        """LICENSE appendix/header must contain copyright and lineage invariant."""
        with open(self.license_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("Copyright 2026 Golem Authors", content)
        self.assertIn(LINEAGE_INVARIANT, content)

    def test_pyproject_license_metadata_harmonized(self):
        """pyproject.toml license must be Apache-2.0 matching LICENSE file."""
        pyproject_path = os.path.join(PROJECT_ROOT, "pyproject.toml")
        with open(pyproject_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn('license = { text = "Apache-2.0" }', content)
        self.assertIn('"License :: OSI Approved :: Apache Software License"', content)


class TestContributingGuidelines(unittest.TestCase):
    """Verifies CONTRIBUTING.md contains onboarding, testing, dual-layer shim, and lineage."""

    def setUp(self):
        self.contrib_path = os.path.join(PROJECT_ROOT, "CONTRIBUTING.md")

    def test_contributing_file_exists(self):
        """CONTRIBUTING.md must exist at project root."""
        self.assertTrue(os.path.isfile(self.contrib_path), f"Missing CONTRIBUTING.md: {self.contrib_path}")

    def test_contributing_lineage_invariant(self):
        """CONTRIBUTING.md must prominently embed the lineage invariant."""
        with open(self.contrib_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(LINEAGE_INVARIANT, content)

    def test_contributing_onboarding_and_setup(self):
        """CONTRIBUTING.md must provide virtualenv and dependency installation instructions."""
        with open(self.contrib_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn(".venv", content)
        self.assertIn("python3 -m venv", content)
        self.assertIn("pip install", content)

    def test_contributing_testing_instructions(self):
        """CONTRIBUTING.md must provide unit and E2E test execution instructions."""
        with open(self.contrib_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("python3 -m unittest discover tests", content)
        self.assertIn("tests/e2e", content)

    def test_contributing_dual_layer_shim_explanation(self):
        """CONTRIBUTING.md must document maintenance of the dual-layer compatibility shim."""
        with open(self.contrib_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("Module Flattening", content)
        self.assertIn("Object Identity", content)
        self.assertIn("Compatibility Facades", content)
        self.assertIn("golem", content)
        self.assertIn("assertIs", content)

    def test_contributing_pr_guidelines(self):
        """CONTRIBUTING.md must outline pull request guidelines and zero secrets policy."""
        with open(self.contrib_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("Pull Request", content)
        self.assertIn("Secrets", content)


class TestIssueAndPullRequestTemplates(unittest.TestCase):
    """Verifies bug report, feature request, and PR templates exist with lineage invariant."""

    def test_bug_report_template_exists_and_valid(self):
        """Bug report template must exist with valid frontmatter and lineage invariant."""
        path = os.path.join(PROJECT_ROOT, ".github", "ISSUE_TEMPLATE", "bug_report.md")
        self.assertTrue(os.path.isfile(path), f"Missing bug report template: {path}")

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("name: Bug Report", content)
        self.assertIn("labels: [\"bug\"]", content)
        self.assertIn(LINEAGE_INVARIANT, content)
        self.assertIn("Steps to Reproduce", content)

    def test_feature_request_template_exists_and_valid(self):
        """Feature request template must exist with valid frontmatter and lineage invariant."""
        path = os.path.join(PROJECT_ROOT, ".github", "ISSUE_TEMPLATE", "feature_request.md")
        self.assertTrue(os.path.isfile(path), f"Missing feature request template: {path}")

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("name: Feature Request", content)
        self.assertIn("labels: [\"enhancement\"]", content)
        self.assertIn(LINEAGE_INVARIANT, content)
        self.assertIn("Proposed Solution", content)

    def test_pull_request_template_exists_and_valid(self):
        """Pull request template must exist with checklist and lineage invariant."""
        path = os.path.join(PROJECT_ROOT, ".github", "pull_request_template.md")
        self.assertTrue(os.path.isfile(path), f"Missing pull request template: {path}")

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn(LINEAGE_INVARIANT, content)
        self.assertIn("Dual-Layer Shim Invariant", content)
        self.assertIn("Tests", content)
        self.assertIn("Zero Secrets", content)


class TestRepositoryGitignore(unittest.TestCase):
    """Verifies .gitignore comprehensively ignores caches, virtualenvs, build artifacts."""

    def setUp(self):
        self.gitignore_path = os.path.join(PROJECT_ROOT, ".gitignore")

    def test_gitignore_file_exists(self):
        """The .gitignore file must exist."""
        self.assertTrue(os.path.isfile(self.gitignore_path))

    def test_gitignore_lineage_invariant(self):
        """The .gitignore file header must embed the lineage invariant."""
        with open(self.gitignore_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(LINEAGE_INVARIANT, content)

    def test_gitignore_required_patterns_present(self):
        """Must cover virtualenvs, bytecode, build dirs, coverage, caches, and logs."""
        with open(self.gitignore_path, "r", encoding="utf-8") as f:
            content = f.read()

        required_patterns = [
            ".venv/",
            "__pycache__/",
            "*.py[cod]",
            "*.egg-info/",
            "dist/",
            "build/",
            ".coverage",
            "htmlcov/",
            ".pytest_cache/",
            ".DS_Store",
            ".logs/*.log",
            "!.logs/M*-*.log",
            ".env",
            ".env.*",
            "!.env.example",
            "!.env.template",
        ]
        for pattern in required_patterns:
            self.assertIn(pattern, content, f"Missing required .gitignore pattern: {pattern}")

    def test_gitignore_multi_environment_secrets_ignored(self):
        """Multi-deployment environment files like .env.production and .env.staging must be ignored."""
        with open(self.gitignore_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn(".env.*", content, ".gitignore must contain .env.*")

        try:
            for env_file in [".env.production", ".env.staging", ".env.test", ".env.backup"]:
                res = subprocess.run(
                    ["git", "check-ignore", "-q", env_file],
                    cwd=PROJECT_ROOT,
                    capture_output=True,
                )
                self.assertEqual(
                    res.returncode,
                    0,
                    f"Expected {env_file} to be ignored by .gitignore",
                )

            res_example = subprocess.run(
                ["git", "check-ignore", "-q", ".env.example"],
                cwd=PROJECT_ROOT,
                capture_output=True,
            )
            self.assertNotEqual(
                res_example.returncode,
                0,
                ".env.example must be preserved and NOT ignored by .gitignore",
            )
        except (FileNotFoundError, subprocess.SubprocessError):
            pass


class TestRepositorySecretScanning(unittest.TestCase):
    """Verifies that repository secret scanner correctly detects leaked tokens and passes repo."""

    # High-confidence credential patterns
    PATTERNS = {
        "slack_token": re.compile(r"xox[baprs]-[0-9A-Za-z-]{20,}"),
        "slack_app_token": re.compile(r"xapp-[0-9A-Za-z-]{20,}"),
        "gemini_api_key": re.compile(r"(?:AQ\.[0-9A-Za-z-_]{40,}|AIza[0-9A-Za-z-_]{35})"),
        "github_token": re.compile(r"(?:gh[pousr]_[0-9a-zA-Z]{36}|github_pat_[0-9a-zA-Z_]{22,})"),
        "aws_access_key": re.compile(r"AKIA[0-9A-Z]{16}"),
        "private_key": re.compile(r"-----BEGIN (?:[A-Z0-9_-]+ )?PRIVATE KEY(?: BLOCK)?-----"),
        "openai_key": re.compile(r"sk-[A-Za-z0-9_-]{32,}"),
    }

    def test_synthetic_secret_scanner_detection(self):
        """Positive control: Scanner must detect synthetic leaked credentials."""
        # Synthetic samples assembled at runtime to prevent static push-protection scanners from flagging test fixtures
        samples = [
            ("".join(["xox", "b-123456789012-1234567890123-abcdefghijklmnopqrstuvwx"]), "slack_token"),
            ("".join(["xapp", "-1-foo-12345678901234-61fe46512b42725494d1707faa9ad8b75f04514b4c3d12d421801be3dc743484"]), "slack_app_token"),
            ("".join(["AQ", ".Ab8RN6IcfYmGqO0Wfgi9ByI4iGT3rY1FDXQWmxD5tnn6y08FiQ"]), "gemini_api_key"),
            ("".join(["AIza", "SyDx1234567890abcdefghijklmnopqrstuv"]), "gemini_api_key"),
            ("".join(["ghp", "_1234567890abcdefghijklmnopqrstuvwxyz"]), "github_token"),
            ("".join(["AKIA", "IOSFODNN7EXAMPL9"]), "aws_access_key"),
            ("".join(["-----BEGIN ", "RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA\n-----END RSA PRIVATE KEY-----"]), "private_key"),
            ("".join(["-----BEGIN ", "ENCRYPTED PRIVATE KEY-----\nMIIFDjBABgkqhkiG9w0BBQ0wMzAbBgkqhkiG9w0BBQwwDgQI\n-----END ENCRYPTED PRIVATE KEY-----"]), "private_key"),
            ("".join(["-----BEGIN ", "OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABlwAAAAdzc2gtcn\n-----END OPENSSH PRIVATE KEY-----"]), "private_key"),
            ("".join(["-----BEGIN ", "PGP PRIVATE KEY BLOCK-----\nVersion: GnuPG v2\n-----END PGP PRIVATE KEY BLOCK-----"]), "private_key"),
            ("".join(["sk-", "live_1234567890abcdefghijklmnopqrstuvwxyz123456"]), "openai_key"),
        ]
        for sample, expected_type in samples:
            pat = self.PATTERNS[expected_type]
            self.assertTrue(pat.search(sample), f"Expected pattern for {expected_type} to match {sample}")

    def test_zero_unmasked_secrets_in_tracked_source_files(self):
        """All production source code, configs, workflows, and docs must have zero unmasked secrets."""
        tracked_files = []
        try:
            res = subprocess.run(
                ["git", "ls-files"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )
            tracked_files = [line.strip() for line in res.stdout.splitlines() if line.strip()]
        except Exception:
            pass

        files_to_scan = set()
        for rel_path in tracked_files:
            files_to_scan.add(os.path.join(PROJECT_ROOT, rel_path))

        # Explicitly ensure both launchd plist definitions and key root configs are audited
        mandatory_targets = [
            
            "com.user.golem.plist",
            "app.py",
            "setup.py",
            "agent_core.py",
            "herdr_fleet.py",
            "herdr_tools.py",
            "CONTRIBUTING.md",
            "LICENSE",
            "PROJECT.md",
            "TASK_QUEUE.md",
            "pyproject.toml",
            ".gitignore",
        ]
        for name in mandatory_targets:
            full_path = os.path.join(PROJECT_ROOT, name)
            if os.path.isfile(full_path):
                files_to_scan.add(full_path)

        scan_dirs = [
            os.path.join(PROJECT_ROOT, "golem"),
            
            os.path.join(PROJECT_ROOT, ".github"),
        ]
        for d in scan_dirs:
            if os.path.isdir(d):
                for root, _, files in os.walk(d):
                    for fname in files:
                        files_to_scan.add(os.path.join(root, fname))

        # Deliberate test fixtures testing security scanning rules
        test_fixture_skips = {
            os.path.join(PROJECT_ROOT, "tests", "test_ci_and_repo_hygiene.py"),
            os.path.join(PROJECT_ROOT, "tests", "test_empirical_m2_download_security.py"),
        }

        found_leaks = []
        for fpath in sorted(files_to_scan):
            if fpath in test_fixture_skips:
                continue
            if not os.path.isfile(fpath):
                continue
            if fpath.endswith((".pyc", ".pyo", ".so", ".png", ".jpg", ".ico", ".gz", ".whl")):
                continue
            with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                for line_idx, line in enumerate(f, start=1):
                    for secret_name, pattern in self.PATTERNS.items():
                        match = pattern.search(line)
                        if match:
                            val = match.group(0)
                            # Skip harmless placeholders or doc examples
                            if "..." in val or "your_" in val.lower() or "example" in val.lower():
                                continue
                            found_leaks.append((os.path.relpath(fpath, PROJECT_ROOT), line_idx, secret_name, val))

        self.assertEqual(
            found_leaks,
            [],
            f"Found unmasked secrets in tracked production files: {found_leaks}"
        )

    def test_env_example_contains_zero_actual_secrets(self):
        """The .env.example template must only contain placeholder tokens, no live secrets."""
        env_example_path = os.path.join(PROJECT_ROOT, ".env.example")
        if not os.path.exists(env_example_path):
            self.skipTest(".env.example does not exist")

        with open(env_example_path, "r", encoding="utf-8") as f:
            content = f.read()

        for secret_name, pattern in self.PATTERNS.items():
            match = pattern.search(content)
            self.assertIsNone(
                match,
                f"Found unmasked secret in .env.example: {secret_name} -> {match.group(0) if match else ''}"
            )


if __name__ == "__main__":
    unittest.main()
