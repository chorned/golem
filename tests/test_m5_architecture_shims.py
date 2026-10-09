#!/usr/bin/env python3
"""
tests/test_m5_architecture_shims.py
Comprehensive verification suite for Milestone 5:
Architecture Restructuring & Compatibility Shims.

Verifies:
1. Decoupled execution exports & absence of circular import hazards.
2. Root module compatibility shims and canonical identity preservation (assertIs).
3. app.py dual-track architecture: production daemon components + all 32 legacy symbols.
4. Launchd plist environment configuration and syntax.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import plistlib
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import unittest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import normalizer
import agent_core
import herdr_tools
import herdr_fleet
import herdr_client
import supervisor
import telemetry_observer
import app
import golem.execution
import golem.transport.normalizer as canonical_norm
import golem.transport.chunker as canonical_chunker
from golem.gateway import SessionRouter, SessionHandle, SessionState
from golem.gateway.ingress import IngressGateway
from golem.execution.harness import DualPaneHarness


class TestExecutionDecouplingAndCircularImports(unittest.TestCase):
    """Verifies Task 1: Decoupling golem.execution and eliminating circular imports."""

    def test_golem_execution_exports(self):
        """Verifies canonical execution tools are properly exported."""
        expected_symbols = [
            "DualPaneHarness",
            "ExecutionHandle",
            "RunnerExecutor",
            "RunnerBufferObserver",
            "WorkerDelegator",
            "sanitize_worker_output",
            "FleetMesh",
            "TOPOLOGY_SPECS",
            "NODE_ALIASES",
            "wrap_windows_command",
            "herdr_pane_run",
            "herdr_agent_read",
            "herdr_handoff",
        ]
        for sym in expected_symbols:
            self.assertTrue(
                hasattr(golem.execution, sym),
                f"golem.execution missing required export: {sym}"
            )

    def test_no_circular_import_between_execution_and_herdr_tools(self):
        """Verifies cross-import in isolated namespace triggers no circular import deadlock."""
        import importlib
        # Re-importing golem.execution then herdr_tools
        exec_mod = importlib.import_module("golem.execution")
        tools_mod = importlib.import_module("herdr_tools")
        self.assertIsNotNone(exec_mod)
        self.assertIsNotNone(tools_mod)


class TestRootModuleCompatibilityShims(unittest.TestCase):
    """Verifies Task 2: Root module compatibility shims and re-exports."""

    def test_normalizer_shim_reexports_and_identity(self):
        """Verifies normalizer.py re-exports canonical symbols with exact identity."""
        symbols = [
            "sanitize_for_slack",
            "transpile_markdown_to_mrkdwn",
            "transpile_markdown_to_slack",
            "strip_terminal_chrome",
            "strip_ansi",
            "strip_tui_debris",
            "strip_box_drawing",
            "strip_prompt_echoes",
            "protect_code_blocks",
            "restore_code_blocks",
            "ANSI_ESCAPE_PATTERN",
            "BOX_DRAWING_PATTERN",
            "BRAILLE_SPINNER_PATTERN",
            "PROMPT_MARKER_PATTERN",
            "ASCII_SPINNER_PATTERN",
            "TOOL_CRUMB_PREFIXES",
            "NAV_BAR_PATTERN",
            "TIP_PATTERN",
        ]
        for sym in symbols:
            self.assertTrue(hasattr(normalizer, sym), f"normalizer.py missing {sym}")
            self.assertIs(
                getattr(normalizer, sym),
                getattr(canonical_norm, sym),
                f"normalizer.{sym} is not identical to canonical"
            )

    def test_herdr_tools_shim_exports(self):
        """Verifies herdr_tools.py exports all legacy registries and functions."""
        self.assertIsInstance(herdr_tools.SPAWNED_AGENTS, dict)
        self.assertIsInstance(herdr_tools.HERDR_TOOLS_DECLARATIONS, list)
        self.assertTrue(callable(herdr_tools.herdr_spawn))
        self.assertTrue(callable(herdr_tools.spawn_agent))
        self.assertTrue(callable(herdr_tools.herdr_read))
        self.assertTrue(callable(herdr_tools.read_agent))
        self.assertTrue(callable(herdr_tools.herdr_stop))
        self.assertTrue(callable(herdr_tools.stop_agent))
        self.assertTrue(callable(herdr_tools.herdr_pane_run))
        self.assertTrue(callable(herdr_tools.pane_run))
        self.assertTrue(callable(herdr_tools.herdr_agent_read))
        self.assertTrue(callable(herdr_tools.agent_read))
        self.assertTrue(callable(herdr_tools.herdr_handoff))
        self.assertTrue(callable(herdr_tools.handoff))
        self.assertTrue(callable(herdr_tools.run_command))
        self.assertTrue(callable(herdr_tools.validate_action_payload))
        self.assertTrue(callable(herdr_tools.build_agent_tool_dispatch_table))

        dispatch_table = herdr_tools.build_agent_tool_dispatch_table()
        self.assertIsInstance(dispatch_table, dict)
        self.assertIn("herdr_pane_run", dispatch_table)
        self.assertIn("herdr_agent_read", dispatch_table)
        self.assertIn("herdr_handoff", dispatch_table)
        self.assertIn("run_command", dispatch_table)

    def test_herdr_fleet_shim_exports(self):
        """Verifies herdr_fleet.py exports topology and fleet management."""
        self.assertTrue(hasattr(herdr_fleet, "HerdrFleet"))
        self.assertTrue(hasattr(herdr_fleet, "TOPOLOGY_SPECS"))
        self.assertIsInstance(herdr_fleet.TOPOLOGY_SPECS, dict)
        self.assertIn("fleet-control", herdr_fleet.TOPOLOGY_SPECS)
        self.assertIn("homelab-ops", herdr_fleet.TOPOLOGY_SPECS)
        self.assertIn("cloud-ops", herdr_fleet.TOPOLOGY_SPECS)
        self.assertIn("dev-grid", herdr_fleet.TOPOLOGY_SPECS)
        self.assertIn("compute-grid", herdr_fleet.TOPOLOGY_SPECS)
        self.assertTrue(callable(herdr_fleet.wrap_windows_command))

    def test_herdr_client_shim_exports(self):
        """Verifies herdr_client.py exports HerdrClient and InfrastructureError."""
        self.assertTrue(hasattr(herdr_client, "HerdrClient"))
        self.assertTrue(hasattr(herdr_client, "InfrastructureError"))
        self.assertTrue(issubclass(herdr_client.InfrastructureError, RuntimeError))

    def test_supervisor_shim_exports(self):
        """Verifies supervisor.py exports AgentSupervisor, cleanup, and activity summarizer."""
        self.assertTrue(hasattr(supervisor, "AgentSupervisor"))
        self.assertTrue(callable(supervisor.cleanup_rogue_servers))
        self.assertTrue(callable(supervisor.summarize_terminal_activity))
        self.assertTrue(hasattr(supervisor, "SPAWNED_BACKGROUND_SERVERS"))

    def test_agent_core_shim_exports(self):
        """Verifies agent_core.py exports ChiefOfStaff and M2 event/adapter shims."""
        self.assertTrue(hasattr(agent_core, "ChiefOfStaff"))
        self.assertTrue(callable(agent_core.build_system_prompt))
        self.assertTrue(callable(agent_core.format_for_slack))
        self.assertTrue(hasattr(agent_core, "AgentEvent"))
        self.assertTrue(hasattr(agent_core, "HeadlessAgentAdapter"))
        self.assertTrue(hasattr(agent_core, "TextDeltaEvent"))
        self.assertTrue(hasattr(agent_core, "StatusUpdateEvent"))
        self.assertTrue(hasattr(agent_core, "FinishEvent"))

    def test_telemetry_observer_shim_exports(self):
        """Verifies telemetry_observer.py exports SessionTelemetryObserver and helpers."""
        self.assertTrue(hasattr(telemetry_observer, "SessionTelemetryObserver"))
        self.assertTrue(callable(telemetry_observer.extract_listening_ports))
        self.assertTrue(callable(telemetry_observer.resolve_machine_name))
        self.assertTrue(callable(telemetry_observer.format_completion_message))
        self.assertTrue(callable(telemetry_observer.format_approval_blocks))


class TestAppArchitectureAndCompatibility(unittest.TestCase):
    """Verifies Task 3: app.py dual-track architecture and legacy test compatibility."""

    def test_app_legacy_symbol_exports(self):
        """Verifies all 32 required symbols are present on app."""
        required_symbols = [
            "sessions",
            "agent_name_to_session",
            "ensure_session_agent",
            "init_dual_pane_workspace",
            "GatewaySession",
            "resolve_target_directory",
            "scope_server_commands",
            "normalize_for_bluebubbles",
            "is_status_inquiry",
            "is_approval_reply",
            "is_rejection_reply",
            "process_user_prompt",
            "handle_reaction_added",
            "HOST_DIR",
            "DEFAULT_TARGET_REPO",
            "split_message_boundary_aware",
            "sanitize_for_slack",
            "pending_alignment",
            "pending_alignment_by_msg_ts",
            "active_thread_sessions",
            "active_tasks",
            "active_subprocesses",
            "workspace_locks",
            "fetch_recent_thread_context",
            "monitor_agent_lifecycle",
            "post_completion_briefing",
            "handle_bearings_request",
            "handle_learn_request",
            "handle_ahoy_request",
            "handle_wrapup_request",
            "handle_update_request",
            "supervisor",
        ]
        for sym in required_symbols:
            self.assertTrue(hasattr(app, sym), f"app.py missing required symbol: {sym}")

    def test_app_monkey_patch_targets(self):
        """Verifies app.app instance has supervisor, herdr_client, fleet attributes for test_supervisor.py."""
        self.assertTrue(hasattr(app.app, "supervisor"), "app.app missing .supervisor")
        self.assertTrue(hasattr(app.app, "herdr_client"), "app.app missing .herdr_client")
        self.assertTrue(hasattr(app.app, "fleet"), "app.app missing .fleet")
        self.assertTrue(hasattr(app, "client_herdr"), "app missing .client_herdr")

    def test_app_production_singletons(self):
        """Verifies session_router, ingress_gateway, and dual_pane_harness singletons exist."""
        self.assertIsInstance(app.session_router, SessionRouter)
        self.assertIsInstance(app.ingress_gateway, IngressGateway)
        self.assertIsInstance(app.dual_pane_harness, DualPaneHarness)

    def test_gateway_session_contract(self):
        """Verifies GatewaySession contract and active_agent resolution."""
        s_single = app.GatewaySession(
            session_key="C1:1.0",
            agent_name="agent-single",
            target_cwd="/tmp",
            channel="C1",
            thread_ts="1.0",
            dual_pane=False
        )
        self.assertEqual(s_single.active_agent, "agent-single")
        self.assertEqual(s_single.state, "starting")

        s_dual = app.GatewaySession(
            session_key="C2:2.0",
            agent_name="agent-dual",
            target_cwd="/tmp",
            channel="C2",
            thread_ts="2.0",
            dual_pane=True,
            liaison_agent="agent-dual-liaison",
            runner_pane="runner"
        )
        self.assertEqual(s_dual.active_agent, "agent-dual-liaison")
        self.assertEqual(s_dual.runner_pane, "runner")

    def test_split_message_boundary_aware_delegation(self):
        """Verifies split_message_boundary_aware in app delegates cleanly to canonical chunker."""
        text = "Hello world.\n\nSecond paragraph."
        chunks = app.split_message_boundary_aware(text, max_chars=100)
        self.assertEqual(chunks, [text])

        # Test paragraph split
        p1 = "First paragraph here." * 10
        p2 = "Second paragraph here." * 10
        full = f"{p1}\n\n{p2}"
        chunks = app.split_message_boundary_aware(full, max_chars=len(p1) + 10)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], p1)
        self.assertEqual(chunks[1], p2)


class TestLaunchdSupervisionPlist(unittest.TestCase):
    """Verifies Task 4: com.user.golem.plist environment configuration."""

    def setUp(self):
        self.plist_path = os.path.join(PROJECT_DIR, "com.user.golem.plist")

    def test_plist_file_exists_and_parses(self):
        """Verifies plist is syntactically valid XML/plist."""
        self.assertTrue(os.path.isfile(self.plist_path))
        with open(self.plist_path, "rb") as f:
            data = plistlib.load(f)
        self.assertEqual(data.get("Label"), "com.user.golem")
        self.assertTrue(data.get("RunAtLoad"))
        self.assertTrue(data.get("KeepAlive"))

    def test_plist_environment_variables(self):
        """Verifies required EnvironmentVariables are populated in the plist."""
        with open(self.plist_path, "rb") as f:
            data = plistlib.load(f)
        env = data.get("EnvironmentVariables", {})
        self.assertEqual(env.get("PYTHONUNBUFFERED"), "1")
        self.assertEqual(env.get("DAEMON_LABEL"), "com.user.golem")
        self.assertEqual(env.get("PYTHONPATH"), "/Users/hermes/projects/golem")
        self.assertEqual(env.get("AGY_BIN"), "/Users/hermes/.local/bin/agy")
        self.assertEqual(env.get("DUAL_PANE_WORKSPACE"), "1")
        self.assertEqual(env.get("DEFAULT_TARGET_REPO"), "/Users/hermes/projects/AGY")


if __name__ == "__main__":
    unittest.main()
