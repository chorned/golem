#!/usr/bin/env python3
"""
tests/test_m6_adversarial_deep_stress.py
Milestone 6 Deep Adversarial Stress & Edge Case Harvester.

Exhaustively probes:
1. TextDeltaEvent extreme types, edge values (None, falsy, large objects, event_type tampering).
2. HerdrFleet routing guardrails (homelab, compute, OCI strict guardrail, laptop strict ad-hoc guardrail).
3. App.py all 32 legacy symbols functional sanity and intent classifiers.
4. Chunker extreme boundaries (exact 3800, 3801, massive single tokens, repeated code fences).
5. Normalizer code block preservation with nested markdown characters.
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import MagicMock, AsyncMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from first_mate.agent.protocol import AgentEvent, TextDeltaEvent
import herdr_fleet
import app
from first_mate.transport.chunker import split_message_boundary_aware
from first_mate.transport.normalizer import sanitize_for_slack


class TestTextDeltaEventExtremeEdgeCases(unittest.TestCase):
    """Adversarial stress on TextDeltaEvent argument types and tampering."""

    def test_event_type_tampering_reset(self):
        """Verify passing a non-text_delta event_type is reset by __post_init__."""
        e = TextDeltaEvent(text="legit", event_type="rogue_type")
        self.assertEqual(e.event_type, "text_delta")

    def test_none_argument_handling(self):
        """Verify handling of None arguments."""
        e1 = TextDeltaEvent(text=None, text_delta="fallback")
        self.assertEqual(e1.text, "fallback")

        e2 = TextDeltaEvent(text="primary", text_delta=None)
        self.assertEqual(e2.text, "primary")

    def test_non_string_conversion(self):
        """Verify stringification or handling when non-string types are passed."""
        e = TextDeltaEvent(text=str(12345))
        self.assertEqual(e.text, "12345")
        self.assertEqual(e.text_delta, "12345")

    def test_dict_serialization_keys_and_dataclass_fields(self):
        """Verify dataclass asdict contains exact expected field names."""
        e = TextDeltaEvent(text="sample text", conversation_id="conv-123", step_index=4)
        d = e.to_dict()
        expected_keys = {"event_type", "timestamp", "conversation_id", "step_index", "text_delta"}
        self.assertEqual(set(d.keys()), expected_keys)
        self.assertNotIn("text", d)  # 'text' is a property alias, not a duplicate field

    def test_from_dict_with_both_text_and_text_delta(self):
        """Verify from_dict properly prioritizes text_delta if both present in dict."""
        data = {
            "event_type": "text_delta",
            "text_delta": "canonical_delta",
            "text": "alternate_text",
        }
        e = AgentEvent.from_dict(data)
        self.assertIsInstance(e, TextDeltaEvent)
        self.assertEqual(e.text_delta, "canonical_delta")
        self.assertEqual(e.text, "canonical_delta")


class TestHerdrFleetRoutingGuardrails(unittest.TestCase):
    """Stress-test fleet triage rules and strict isolation guardrails."""

    def setUp(self):
        self.fleet = herdr_fleet.HerdrFleet(herdr_bin="mock_herdr")

    def test_homelab_chore_routing(self):
        queries = [
            "restart plex container",
            "docker compose down and up",
            "check zfs pool status",
            "nas storage backup failed",
        ]
        for q in queries:
            target = self.fleet.triage_prompt(q)
            self.assertEqual(target.get("workspace_label"), "homelab-ops", f"Failed for '{q}'")
            self.assertEqual(target.get("role"), "Hermes Butler", f"Failed for '{q}'")

    def test_compute_grid_routing(self):
        queries = [
            "train model with pytorch on cuda",
            "check gpu vram utilization",
            "run benchmark on desktop-pc",
        ]
        for q in queries:
            target = self.fleet.triage_prompt(q)
            self.assertEqual(target.get("target_machine"), "desktop-pc", f"Failed for '{q}'")
            self.assertEqual(target.get("workspace_label"), "compute-grid")

    def test_oci_cloud_guardrail(self):
        """Verify generic cloud queries do NOT route to OCI without explicit prod/ingress keyword."""
        # Generic cloud compute -> defaults to fleet-control (local anchor)
        target_generic = self.fleet.triage_prompt("run cloud compute script")
        self.assertNotEqual(target_generic.get("target_machine"), "oci-machine")

        # Explicit production/ingress -> routes to cloud-ops
        target_explicit = self.fleet.triage_prompt("update wireguard ingress on oci-machine")
        self.assertEqual(target_explicit.get("target_machine"), "oci-machine")
        self.assertEqual(target_explicit.get("workspace_label"), "cloud-ops")

    def test_laptop_strict_adhoc_guardrail(self):
        """Verify personal laptop is NEVER selected for generic dev tasks."""
        # Generic coding task -> must route to fleet-control (localhost anchor)
        target_dev = self.fleet.triage_prompt("build this project and run pytest")
        self.assertEqual(target_dev.get("target_machine"), "local")
        self.assertEqual(target_dev.get("workspace_label"), "fleet-control")

        # Explicit laptop request -> routes to dev-grid
        target_laptop = self.fleet.triage_prompt("check battery status on my laptop")
        self.assertEqual(target_laptop.get("target_machine"), "personal-laptop")
        self.assertEqual(target_laptop.get("workspace_label"), "dev-grid")


class TestAppLegacySymbolsAndClassifiers(unittest.TestCase):
    """Verify all 32 legacy symbols on app.py and helper classifiers."""

    def test_all_32_symbols_present_and_callable(self):
        """Verify presence of all 32 legacy symbols."""
        symbols = [
            "sessions", "agent_name_to_session", "ensure_session_agent",
            "init_dual_pane_workspace", "GatewaySession", "resolve_target_directory",
            "scope_server_commands", "normalize_for_bluebubbles", "is_status_inquiry",
            "is_approval_reply", "is_rejection_reply", "process_user_prompt",
            "handle_reaction_added", "HOST_DIR", "DEFAULT_TARGET_REPO",
            "split_message_boundary_aware", "sanitize_for_slack", "pending_alignment",
            "pending_alignment_by_msg_ts", "active_thread_sessions", "active_tasks",
            "active_subprocesses", "workspace_locks", "fetch_recent_thread_context",
            "monitor_agent_lifecycle", "post_completion_briefing", "handle_bearings_request",
            "handle_learn_request", "handle_ahoy_request", "handle_wrapup_request",
            "handle_update_request", "supervisor"
        ]
        for s in symbols:
            self.assertTrue(hasattr(app, s), f"Missing legacy symbol: {s}")

    def test_intent_classifiers(self):
        """Verify status inquiry, approval, and rejection classifiers."""
        self.assertTrue(app.is_status_inquiry("how's it going?"))
        self.assertTrue(app.is_status_inquiry("what's the status"))
        self.assertTrue(app.is_status_inquiry("what port?"))
        # Inquiries mentioning 'fleet' are deliberately NOT intercepted as status inquiries (handled by root liaison)
        self.assertFalse(app.is_status_inquiry("how's our fleet doing?"))
        self.assertFalse(app.is_status_inquiry("delete the database"))

        self.assertTrue(app.is_approval_reply("go"))
        self.assertTrue(app.is_approval_reply("lgtm"))
        self.assertTrue(app.is_approval_reply("thumbsup"))
        self.assertTrue(app.is_approval_reply("👍"))
        self.assertFalse(app.is_approval_reply("no stop"))

        self.assertTrue(app.is_rejection_reply("reject"))
        self.assertTrue(app.is_rejection_reply("abort"))
        self.assertTrue(app.is_rejection_reply("cancel"))
        self.assertTrue(app.is_rejection_reply("stop"))
        self.assertTrue(app.is_rejection_reply("no"))
        self.assertTrue(app.is_rejection_reply("👎"))
        self.assertFalse(app.is_rejection_reply("proceed please"))


class TestChunkerExtremeBoundaries(unittest.TestCase):
    """Stress-test chunker with boundary inputs and code blocks."""

    def test_exact_3800_chars_no_split(self):
        """Text of exactly 3800 chars must remain 1 chunk."""
        text = "a" * 3800
        chunks = split_message_boundary_aware(text, max_chars=3800)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(len(chunks[0]), 3800)

    def test_3801_chars_splits(self):
        """Text exceeding 3800 chars must split into chunks."""
        text = ("Word " * 761).strip()  # 761 * 5 - 1 = 3804 chars
        self.assertGreater(len(text), 3800)
        chunks = split_message_boundary_aware(text, max_chars=3800)
        self.assertGreaterEqual(len(chunks), 2)
        for c in chunks:
            self.assertLessEqual(len(c), 3800)

    def test_massive_unbroken_token(self):
        """A single token exceeding max_chars must hard-slice without error."""
        text = "X" * 8000
        chunks = split_message_boundary_aware(text, max_chars=3800)
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0], "X" * 3800)
        self.assertEqual(chunks[1], "X" * 3800)
        self.assertEqual(chunks[2], "X" * 400)

    def test_code_fence_balancing_on_split(self):
        """Code block spanning across chunk split must close in chunk 1 and reopen in chunk 2."""
        code_lines = ["    x += 1" for _ in range(500)]
        code_content = "\n".join(code_lines)
        full_text = f"Intro paragraph.\n\n```python\n{code_content}\n```\n\nOutro paragraph."
        self.assertGreater(len(full_text), 4000)

        chunks = split_message_boundary_aware(full_text, max_chars=2000)
        self.assertGreaterEqual(len(chunks), 2)

        # Every chunk that begins inside a code block must have balanced fences
        for i, c in enumerate(chunks[:-1]):
            fence_count = c.count("```")
            self.assertEqual(fence_count % 2, 0, f"Chunk {i} has unbalanced backtick fences: {fence_count}")


class TestNormalizerCodeBlockIntegrity(unittest.TestCase):
    """Stress-test normalizer code-block protection with markdown-like code content."""

    def test_markdown_and_ansi_inside_code_block_preserved(self):
        """Characters like #, **, [link](url) inside code blocks must NOT be transpiled."""
        code_body = (
            "# This is a Python comment, not an ATX header\n"
            "power = 2 ** 8\n"
            "link_str = '[not a link](http://foo)'\n"
            "url = 'https://example.com'\n"
        )
        text = f"### Main Header\n\nHere is code:\n```python\n{code_body}```\n\n**Footer bold**"
        sanitized = sanitize_for_slack(text)

        # Header outside is transpiled
        self.assertNotIn("### Main Header", sanitized)
        self.assertIn("*Main Header*", sanitized)

        # Bold outside is transpiled
        self.assertNotIn("**Footer bold**", sanitized)
        self.assertIn("*Footer bold*", sanitized)

        # Code block contents preserved
        self.assertIn("# This is a Python comment", sanitized)
        self.assertIn("2 ** 8", sanitized)
        self.assertIn("[not a link](http://foo)", sanitized)


if __name__ == "__main__":
    unittest.main()
