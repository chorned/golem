#!/usr/bin/env python3
"""
tests/test_documentation_and_manuals.py
Comprehensive validation tests for Milestone 5: Flagship Documentation & Operating Manuals.

Lineage Invariant:
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import unittest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LINEAGE_INVARIANT = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."


class TestLineageInvariantPresence(unittest.TestCase):
    """Verifies that the exact lineage invariant is codified across all documentation assets."""

    def test_lineage_invariant_in_readme(self):
        """README.md must prominently embed the exact lineage invariant."""
        readme_path = os.path.join(PROJECT_ROOT, "README.md")
        self.assertTrue(os.path.isfile(readme_path), f"Missing README.md: {readme_path}")
        with open(readme_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(LINEAGE_INVARIANT, content, "README.md must embed exact lineage invariant")

    def test_lineage_invariant_in_agents_md(self):
        """AGENTS.md must prominently embed the exact lineage invariant."""
        agents_path = os.path.join(PROJECT_ROOT, "AGENTS.md")
        self.assertTrue(os.path.isfile(agents_path), f"Missing AGENTS.md: {agents_path}")
        with open(agents_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(LINEAGE_INVARIANT, content, "AGENTS.md must embed exact lineage invariant")

    def test_lineage_invariant_in_project_md(self):
        """PROJECT.md must embed the exact lineage invariant."""
        project_path = os.path.join(PROJECT_ROOT, "PROJECT.md")
        self.assertTrue(os.path.isfile(project_path), f"Missing PROJECT.md: {project_path}")
        with open(project_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(LINEAGE_INVARIANT, content, "PROJECT.md must embed exact lineage invariant")

    def test_lineage_invariant_in_contributing_md(self):
        """CONTRIBUTING.md must embed the exact lineage invariant."""
        contrib_path = os.path.join(PROJECT_ROOT, "CONTRIBUTING.md")
        self.assertTrue(os.path.isfile(contrib_path), f"Missing CONTRIBUTING.md: {contrib_path}")
        with open(contrib_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(LINEAGE_INVARIANT, content, "CONTRIBUTING.md must embed exact lineage invariant")

    def test_lineage_invariant_in_license(self):
        """LICENSE must embed the exact lineage invariant."""
        license_path = os.path.join(PROJECT_ROOT, "LICENSE")
        self.assertTrue(os.path.isfile(license_path), f"Missing LICENSE: {license_path}")
        with open(license_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(LINEAGE_INVARIANT, content, "LICENSE must embed exact lineage invariant")

    def test_lineage_invariant_in_test_file_header(self):
        """The test module docstring must embed the exact lineage invariant."""
        self.assertIn(LINEAGE_INVARIANT, __doc__, "Test docstring must embed exact lineage invariant")


class TestReadmeFlagshipContent(unittest.TestCase):
    """Verifies that README.md satisfies all architectural, topological, and quickstart requirements."""

    def setUp(self):
        self.readme_path = os.path.join(PROJECT_ROOT, "README.md")
        with open(self.readme_path, "r", encoding="utf-8") as f:
            self.content = f.read()

    def test_readme_file_exists(self):
        """README.md file must exist at project root."""
        self.assertTrue(os.path.isfile(self.readme_path))

    def test_readme_badges_present(self):
        """README.md must feature CI/CD, Python, License, and Lineage badges."""
        self.assertIn("actions/workflows/ci.yml/badge.svg", self.content, "Missing CI/CD badge")
        self.assertIn("badge/python-3.11", self.content, "Missing Python version badge")
        self.assertIn("License-Apache--2.0", self.content, "Missing Apache-2.0 License badge")
        self.assertIn("badge/lineage-", self.content, "Missing Lineage badge")

    def test_architectural_flex_failure_modes(self):
        """README.md must deeply articulate all five naive PTY scraping failure modes."""
        # 1. Visual buffer scraping desynchronization
        self.assertTrue(
            "visual buffer scraping desynchronization" in self.content.lower() or
            "visual buffer scraping" in self.content.lower(),
            "Must explain visual buffer scraping desynchronization failure mode"
        )
        # 2. ANSI color & VT100 control code corruption
        self.assertTrue(
            "ansi" in self.content.lower() and "vt100" in self.content.lower() and "corruption" in self.content.lower(),
            "Must explain ANSI color and VT100 control code corruption"
        )
        # 3. Terminal screen line wrapping artifacts
        self.assertTrue(
            "line wrapping" in self.content.lower() or "column" in self.content.lower(),
            "Must explain terminal screen line wrapping artifacts"
        )
        # 4. Blocking subshell race conditions
        self.assertTrue(
            "blocking subshell" in self.content.lower() or "race condition" in self.content.lower(),
            "Must explain blocking subshell race conditions and deadlocks"
        )
        # 5. Catastrophic LLM token starvation
        self.assertTrue(
            "token starvation" in self.content.lower(),
            "Must explain catastrophic LLM token starvation from re-ingesting raw screen frames"
        )

    def test_architectural_flex_decoupled_solution(self):
        """README.md must articulate Golem's decoupled tripartite solution."""
        # Front-of-House Mouth (Pane 1)
        self.assertIn("Front-of-House Mouth", self.content, "Must document Front-of-House Mouth")
        self.assertIn("Pane 1", self.content, "Must document Mouth in Pane 1")
        self.assertIn("dispatch_task", self.content, "Must document dispatch_task tool call")

        # Backstage Hands (Pane 2..N)
        self.assertIn("Backstage Hands", self.content, "Must document Backstage Hands")
        self.assertIn("Pane 2", self.content, "Must document Hands runner in Pane 2")
        self.assertIn("yield", self.content.lower(), "Must document non-blocking yield timeout")
        self.assertIn("ExecutionHandle", self.content, "Must document ExecutionHandle")

        # 24/7 Sensory Plane
        self.assertIn("Sensory Plane", self.content, "Must document Sensory Plane")
        self.assertIn("The Eyes", self.content, "Must document The Eyes observer")
        self.assertIn("SHA-256", self.content, "Must document buffer change SHA-256 hashing")
        self.assertIn("The Nose", self.content, "Must document The Nose sentinel")
        self.assertIn("ShoulderTapEventBus", self.content, "Must document ShoulderTapEventBus")

    def test_ascii_topology_diagrams_present(self):
        """README.md must include rich, clean ASCII diagrams for architecture and fleet topology."""
        # Box drawing characters indicating rich ASCII diagrams
        self.assertIn("┌", self.content, "Must include structured ASCII box art")
        self.assertIn("└", self.content, "Must include structured ASCII box art")
        self.assertIn("│", self.content, "Must include structured ASCII box art")

        # Architectural topology diagram content
        self.assertIn("GOLEM TRIPARTITE SENSORY ARCHITECTURE", self.content)
        self.assertIn("GOLEM FLEET MACHINE TOPOLOGY", self.content)

        # Fleet nodes in diagrams
        self.assertIn("fleet-control", self.content)
        self.assertIn("homelab-ops", self.content)
        self.assertIn("compute-grid", self.content)
        self.assertIn("dev-grid", self.content)
        self.assertIn("cloud-ops", self.content)

    def test_ascii_box_drawing_alignment(self):
        """All fenced ASCII diagrams in README.md must maintain strict uniform line length and aligned borders."""
        blocks = re.findall(r"```(?:\w+)?\n(.*?)```", self.content, re.DOTALL)
        diagram_blocks = [b for b in blocks if "┌" in b and "└" in b]

        self.assertGreaterEqual(
            len(diagram_blocks), 4, "Must contain at least 4 ASCII diagrams with box drawing characters"
        )

        box_chars = set("│┌┐└┘├┤┬┴┼")
        for idx, block in enumerate(diagram_blocks):
            lines = [l for l in block.split("\n") if l]
            self.assertTrue(len(lines) > 0, f"Diagram {idx} must contain lines")
            line_lens = [len(l) for l in lines]
            min_len = min(line_lens)
            max_len = max(line_lens)
            self.assertEqual(
                min_len,
                max_len,
                f"Diagram {idx} lines must have uniform character length: min={min_len}, max={max_len}. Lines: {line_lens}"
            )

            # Assert outer border characters align consistently on the same column indices across all rows
            for l_num, line in enumerate(lines):
                self.assertIn(
                    line[0],
                    box_chars,
                    f"Diagram {idx} line {l_num} must start with box character at col 0: got {repr(line[0])}"
                )
                self.assertIn(
                    line[-1],
                    box_chars,
                    f"Diagram {idx} line {l_num} must end with box character at col {max_len - 1}: got {repr(line[-1])}"
                )

    def test_env_example_harmonization_with_readme(self):
        """Verify that .env.example contains configuration variables referenced in README.md."""
        env_example_path = os.path.join(PROJECT_ROOT, ".env.example")
        self.assertTrue(os.path.isfile(env_example_path), f"Missing .env.example: {env_example_path}")
        with open(env_example_path, "r", encoding="utf-8") as f:
            env_content = f.read()

        # Check critical environment variables documented in README.md
        self.assertIn("GOLEM_DEFAULT_HAND_WORKER=agy", env_content)
        self.assertIn("GOLEM_ENABLE_WEBUI=0", env_content)
        self.assertIn("FIRST_MATE_ENGINE=antigravity", env_content)
        self.assertIn("MOUTH_MODEL=", env_content)
        self.assertIn("EYES_MODEL=", env_content)
        self.assertIn("NOSE_MODEL=", env_content)

    def test_real_time_webui_documentation(self):
        """README.md must document port 8765 WebUI, 4-pane layout, and SSE event streaming."""
        self.assertIn("8765", self.content, "Must document default WebUI port 8765")
        self.assertIn("--webUI", self.content, "Must document --webUI CLI option")
        self.assertIn("GOLEM_ENABLE_WEBUI", self.content, "Must document GOLEM_ENABLE_WEBUI env variable")
        self.assertIn("/events", self.content, "Must document /events SSE endpoint")
        self.assertIn("/api/status", self.content, "Must document /api/status endpoint")

        # 4-pane layout details
        self.assertTrue("eye" in self.content.lower() and "pane" in self.content.lower())
        self.assertTrue("nose" in self.content.lower() and "pane" in self.content.lower())
        self.assertTrue("mouth" in self.content.lower() and "pane" in self.content.lower())
        self.assertTrue("hand" in self.content.lower() and "grid" in self.content.lower())

    def test_quickstart_instructions(self):
        """README.md must detail prerequisites, editable pip install, dual CLI, and launchd."""
        self.assertIn("Python 3.11", self.content, "Must declare Python 3.11+ prerequisite")
        self.assertIn("Herdr", self.content, "Must declare Herdr prerequisite")
        self.assertIn("pip install -e .", self.content, "Must give editable install command")
        self.assertIn("pip install -e \".[dev]\"", self.content, "Must give dev install command")
        self.assertIn("golem", self.content, "Must document golem CLI")
        self.assertIn("first-mate", self.content, "Must document first-mate CLI alias")
        self.assertIn(".env.example", self.content, "Must reference .env.example")
        self.assertIn("com.user.golem.plist", self.content, "Must document launchd plist")
        self.assertIn("launchctl kickstart", self.content, "Must document launchctl kickstart command")

    def test_dual_layer_compatibility_shim_documentation(self):
        """README.md must explain sys.meta_path alias finder and 100% object identity."""
        self.assertIn("_FirstMateAliasFinder", self.content)
        self.assertIn("sys.meta_path", self.content)
        self.assertIn("assert first_mate", self.content)
        self.assertIn("is golem", self.content)
        self.assertIn("mock", self.content.lower())


class TestAgentsOperatingManual(unittest.TestCase):
    """Verifies that AGENTS.md codifies the Golem Chief of Staff operating contract."""

    def setUp(self):
        self.agents_path = os.path.join(PROJECT_ROOT, "AGENTS.md")
        with open(self.agents_path, "r", encoding="utf-8") as f:
            self.content = f.read()

    def test_agents_md_exists(self):
        """AGENTS.md must exist at project root."""
        self.assertTrue(os.path.isfile(self.agents_path))

    def test_agents_md_lineage_invariant(self):
        """AGENTS.md must embed the exact lineage invariant in its header."""
        self.assertIn(LINEAGE_INVARIANT, self.content)

    def test_firstmate_golem_pattern(self):
        """AGENTS.md must define One Captain (Carl) and One Liaison (Golem Chief of Staff)."""
        self.assertIn("One Captain, One Liaison", self.content)
        self.assertIn("Carl", self.content)
        self.assertIn("Chief of Staff", self.content)
        self.assertIn("backstage", self.content.lower())

    def test_sensory_architecture_roles(self):
        """AGENTS.md must codify Mouth, Hands, Eyes, Nose, and Shoulder Tap Bus."""
        self.assertIn("The Mouth", self.content)
        self.assertIn("The Hands", self.content)
        self.assertIn("The Eyes", self.content)
        self.assertIn("The Nose", self.content)
        self.assertIn("ShoulderTapEventBus", self.content)
        self.assertIn("check_eyes", self.content)
        self.assertIn("check_nose", self.content)

    def test_fleet_machine_topology_table(self):
        """AGENTS.md must contain the full fleet machine topology table."""
        self.assertIn("fleet-control", self.content)
        self.assertIn("homelab-ops", self.content)
        self.assertIn("compute-grid", self.content)
        self.assertIn("dev-grid", self.content)
        self.assertIn("cloud-ops", self.content)
        self.assertIn("100.80.59.45", self.content)
        self.assertIn("100.96.38.5", self.content)
        self.assertIn("100.113.173.33", self.content)

    def test_reacji_state_lifecycle(self):
        """AGENTS.md must codify the reacji state markers."""
        self.assertIn(":eyes:", self.content)
        self.assertIn(":gear:", self.content)
        self.assertIn(":warning:", self.content)
        self.assertIn(":white_check_mark:", self.content)
        self.assertIn(":x:", self.content)

    def test_executive_command_suite(self):
        """AGENTS.md must define the executive slash commands."""
        self.assertIn("/bearings", self.content)
        self.assertIn("/ahoy", self.content)
        self.assertIn("/wrapup", self.content)
        self.assertIn("/update", self.content)
        self.assertIn("/learn", self.content)

    def test_operational_guardrails(self):
        """AGENTS.md must enforce non-blocking subshell, /Volumes/, and media guardrails."""
        self.assertIn("ExecutionHandle", self.content)
        self.assertIn("/Volumes/*", self.content)
        self.assertIn("-nostdin", self.content)
        self.assertIn("-frames:v 1", self.content)
        self.assertIn("5-Second Rule", self.content)


class TestMarkdownSyntaxAndLinkSanity(unittest.TestCase):
    """Verifies markdown syntax cleanliness, code fence balance, and link sanity."""

    def test_code_fences_are_balanced(self):
        """All code fences (```) in core markdown documents must be balanced (even count)."""
        docs_to_check = [
            os.path.join(PROJECT_ROOT, "README.md"),
            os.path.join(PROJECT_ROOT, "AGENTS.md"),
            os.path.join(PROJECT_ROOT, "CONTRIBUTING.md"),
            os.path.join(PROJECT_ROOT, "PROJECT.md"),
            os.path.join(PROJECT_ROOT, "TASK_QUEUE.md"),
        ]
        for doc_path in docs_to_check:
            if not os.path.isfile(doc_path):
                continue
            with open(doc_path, "r", encoding="utf-8") as f:
                content = f.read()

            fence_matches = re.findall(r"^```", content, flags=re.MULTILINE)
            self.assertEqual(
                len(fence_matches) % 2,
                0,
                f"Unbalanced code fences in {os.path.basename(doc_path)}: found {len(fence_matches)} fences"
            )

    def test_readme_relative_file_links_resolve(self):
        """Relative file links in README.md must point to existing local files."""
        readme_path = os.path.join(PROJECT_ROOT, "README.md")
        with open(readme_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Match markdown links: [text](target)
        link_pattern = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
        matches = link_pattern.findall(content)

        for text, target in matches:
            # Skip external URLs
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            # Skip pure anchor links within document
            if target.startswith("#"):
                continue

            # Strip any internal anchor from file target (e.g., path#section)
            clean_target = target.split("#")[0]
            if not clean_target:
                continue

            target_full_path = os.path.normpath(os.path.join(PROJECT_ROOT, clean_target))
            self.assertTrue(
                os.path.exists(target_full_path),
                f"Broken relative link in README.md: '{target}' (resolved to {target_full_path})"
            )

    def test_readme_internal_anchor_links_resolve(self):
        """Internal anchor links in README.md must resolve to valid section headings."""
        readme_path = os.path.join(PROJECT_ROOT, "README.md")
        with open(readme_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Extract headings (# Heading, ## Heading, etc.)
        headings = re.findall(r"^#+\s+(.+)$", content, flags=re.MULTILINE)
        normalized_anchors = set()
        for h in headings:
            # GitHub anchor slugification rules:
            # 1. Lowercase
            # 2. Strip characters other than alphanumeric, space, hyphens
            # 3. Replace spaces with hyphens
            slug = re.sub(r"[^\w\s-]", "", h.lower()).strip()
            slug = re.sub(r"\s+", "-", slug)
            normalized_anchors.add(slug)

            # Also allow double hyphen for dropped '&'
            slug_with_dropped_symbols = re.sub(r"&", "", h.lower())
            slug_with_dropped_symbols = re.sub(r"[^\w\s-]", "", slug_with_dropped_symbols).strip()
            slug_with_dropped_symbols = re.sub(r"\s+", "-", slug_with_dropped_symbols)
            normalized_anchors.add(slug_with_dropped_symbols)

        # Find internal anchor links
        link_pattern = re.compile(r"\[([^\]]+)\]\(#([^)]+)\)")
        for text, anchor in link_pattern.findall(content):
            self.assertTrue(
                anchor in normalized_anchors or anchor.replace("--", "-") in normalized_anchors,
                f"Broken anchor link in README.md: #{anchor} not found in headings: {normalized_anchors}"
            )


if __name__ == "__main__":
    unittest.main()
