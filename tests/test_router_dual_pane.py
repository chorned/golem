#!/usr/bin/env python3
"""
tests/test_router_dual_pane.py
Comprehensive unit tests verifying DualPaneHarness wiring into SessionRouter and SessionHandle,
validating TC-01 dual-pane provisioning, multi-turn persistence, lifecycle teardown, reaper cleanup,
and backward compatibility when harness=None.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import json
import unittest
from typing import Optional, List, Dict, Any
from unittest.mock import AsyncMock, MagicMock, patch

from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle, SessionState
from golem.execution.harness import DualPaneHarness
from herdr_client import HerdrClient
import app


class MockAdapter:
    """Mock agent adapter for testing."""

    def __init__(self, cwd: str = "/tmp") -> None:
        self.cwd = cwd
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_turn(self, prompt: str):
        yield {"event_type": "text_delta", "content": f"Echo: {prompt}"}
        yield {"event_type": "finish", "status": "SUCCESS", "response": f"Echo: {prompt}"}


class TestSessionRouterDualPane(unittest.IsolatedAsyncioTestCase):
    """Test suite for SessionRouter dual-pane integration and lifecycle management."""

    async def asyncSetUp(self) -> None:
        self.mock_harness = AsyncMock(spec=DualPaneHarness)
        self.mock_harness.provision_dual_pane.return_value = ("C100:200.1-liaison", "runner")
        self.mock_harness.teardown.return_value = None
        self.adapter = MockAdapter()
        self.router = SessionRouter(
            default_target_cwd="/Users/hermes/projects/AGY",
            inactivity_timeout=3600.0,
            adapter_factory=lambda cwd: self.adapter,
            harness=self.mock_harness,
        )

    async def asyncTearDown(self) -> None:
        await self.router.stop_reaper()

    async def test_tc01_dual_pane_provisioning(self) -> None:
        """
        TC-01: Verify SessionRouter.get_or_create() provisions both Pane 1
        ({session_id}-liaison) and Pane 2 (runner) via DualPaneHarness.
        """
        session, is_new = await self.router.get_or_create("C100", "200.1")

        self.assertTrue(is_new)
        self.mock_harness.provision_dual_pane.assert_called_once_with(
            session_id="C100:200.1",
            cwd="/Users/hermes/projects/AGY",
        )
        self.assertIs(session.harness, self.mock_harness)
        self.assertEqual(session.workspace_id, "C100:200.1")
        self.assertEqual(session.liaison_pane, "C100:200.1-liaison")
        self.assertEqual(session.runner_pane, "runner")
        self.assertEqual(session.state, SessionState.IDLE)
        self.assertTrue(self.adapter.started)

    async def test_multi_turn_session_persistence(self) -> None:
        """
        Verify multi-turn session persistence: subsequent get_or_create()
        returns the existing handle without re-provisioning.
        """
        session_turn1, is_new1 = await self.router.get_or_create("C100", "200.1")
        self.assertTrue(is_new1)
        self.assertEqual(self.mock_harness.provision_dual_pane.call_count, 1)

        session_turn2, is_new2 = await self.router.get_or_create("C100", "200.1")
        self.assertFalse(is_new2)
        self.assertIs(session_turn1, session_turn2)
        # Verify harness was not called again
        self.assertEqual(self.mock_harness.provision_dual_pane.call_count, 1)

    async def test_explicit_teardown_cleans_up_harness(self) -> None:
        """Verify explicit router.teardown() cleanly tears down the harness workspace."""
        session, _ = await self.router.get_or_create("C100", "200.1")
        self.assertIn("C100:200.1", self.router.sessions)

        success = await self.router.teardown("C100", "200.1", reason="wrapup")
        self.assertTrue(success)
        self.assertNotIn("C100:200.1", self.router.sessions)
        self.assertEqual(session.state, SessionState.CLOSED)
        self.mock_harness.teardown.assert_called_once_with("C100:200.1")
        self.assertTrue(self.adapter.stopped)

    async def test_session_handle_close_cleans_up_harness(self) -> None:
        """Verify calling session.close() directly tears down harness workspace."""
        session, _ = await self.router.get_or_create("C100", "200.1")

        await session.close(reason="user_closed")
        self.assertEqual(session.state, SessionState.CLOSED)
        self.mock_harness.teardown.assert_called_once_with("C100:200.1")
        self.assertTrue(self.adapter.stopped)

    async def test_reaper_cleans_up_expired_sessions_with_harness(self) -> None:
        """Verify reap_expired_sessions() invokes harness.teardown() for expired sessions."""
        short_router = SessionRouter(
            inactivity_timeout=0.05,
            adapter_factory=lambda cwd: MockAdapter(cwd),
            harness=self.mock_harness,
        )

        s1, _ = await short_router.get_or_create("C1", "1.0")
        s2, _ = await short_router.get_or_create("C2", "2.0")
        self.assertEqual(len(short_router.sessions), 2)

        # Allow sessions to expire
        await asyncio.sleep(0.06)

        reaped = await short_router.reap_expired_sessions()
        self.assertEqual(sorted(reaped), ["C1:1.0", "C2:2.0"])
        self.assertEqual(len(short_router.sessions), 0)
        self.assertEqual(s1.state, SessionState.CLOSED)
        self.assertEqual(s2.state, SessionState.CLOSED)

        self.mock_harness.teardown.assert_any_call("C1:1.0")
        self.mock_harness.teardown.assert_any_call("C2:2.0")
        self.assertEqual(self.mock_harness.teardown.call_count, 2)

    async def test_stop_reaper_cleans_up_active_sessions_with_harness(self) -> None:
        """Verify stop_reaper() cleans up all active sessions via harness."""
        s1, _ = await self.router.get_or_create("C1", "1.0")
        s2, _ = await self.router.get_or_create("C2", "2.0")
        self.assertEqual(len(self.router.sessions), 2)

        await self.router.stop_reaper()
        self.assertEqual(len(self.router.sessions), 0)
        self.assertEqual(s1.state, SessionState.CLOSED)
        self.assertEqual(s2.state, SessionState.CLOSED)

        self.mock_harness.teardown.assert_any_call("C1:1.0")
        self.mock_harness.teardown.assert_any_call("C2:2.0")

    async def test_backward_compatibility_when_harness_is_none(self) -> None:
        """
        Verify backward compatibility: when harness is None, SessionRouter
        gracefully runs without dual-pane provisioning, preserving compatibility.
        """
        compat_router = SessionRouter(
            default_target_cwd="/tmp",
            adapter_factory=lambda cwd: MockAdapter(cwd),
            harness=None,
        )
        self.assertIsNone(compat_router.harness)

        session, is_new = await compat_router.get_or_create("C-NO-HARNESS", "999.0")
        self.assertTrue(is_new)
        self.assertIsNone(session.harness)
        self.assertIsNone(session.workspace_id)
        self.assertIsNone(session.liaison_pane)
        self.assertIsNone(session.runner_pane)
        self.assertEqual(session.state, SessionState.IDLE)

        # Teardown should succeed without errors
        success = await compat_router.teardown("C-NO-HARNESS", "999.0")
        self.assertTrue(success)
        self.assertEqual(session.state, SessionState.CLOSED)

    async def test_custom_target_cwd_propagated_to_harness(self) -> None:
        """Verify custom target_cwd passed to get_or_create is forwarded to harness."""
        session, _ = await self.router.get_or_create(
            "C100", "200.1", target_cwd="/custom/project/repo"
        )
        self.mock_harness.provision_dual_pane.assert_called_once_with(
            session_id="C100:200.1",
            cwd="/custom/project/repo",
        )
        self.assertEqual(session.target_cwd, "/custom/project/repo")

    async def test_failed_session_replacement_tears_down_old_harness(self) -> None:
        """Verify that replacing a failed session cleanly closes and tears down the old harness."""
        s1, _ = await self.router.get_or_create("C100", "200.1")
        s1.state = SessionState.FAILED

        # New turn in same thread triggers replacement
        s2, is_new = await self.router.get_or_create("C100", "200.1")
        self.assertTrue(is_new)
        self.assertIsNot(s1, s2)
        self.assertEqual(s1.state, SessionState.CLOSED)
        self.mock_harness.teardown.assert_called_with("C100:200.1")
        self.assertEqual(self.mock_harness.provision_dual_pane.call_count, 2)

    async def test_real_dual_pane_harness_integration(self) -> None:
        """
        Verify end-to-end integration between SessionRouter and real DualPaneHarness
        with mocked HerdrClient CLI responses.
        """
        mock_herdr = AsyncMock(spec=HerdrClient)
        mock_ws_output = json.dumps({
            "result": {
                "workspace": {"workspace_id": "ws-real-123"},
                "root_pane": {"pane_id": "ws-real-123:p1"}
            }
        })
        mock_runner_output = json.dumps({
            "result": {
                "pane": {"pane_id": "ws-real-123:p2"}
            }
        })

        mock_herdr.run_cli.side_effect = [
            (0, mock_ws_output, ""),       # workspace create
            (0, "started liaison", ""),   # agent start
            (0, mock_runner_output, ""),  # pane create --label runner
            (0, "closed workspace", ""),  # workspace close
        ]

        real_harness = DualPaneHarness(herdr_client=mock_herdr, default_cwd="/tmp")
        router = SessionRouter(
            default_target_cwd="/tmp",
            adapter_factory=lambda cwd: MockAdapter(cwd),
            harness=real_harness,
        )

        session, is_new = await router.get_or_create("C-REAL", "1.0")
        self.assertTrue(is_new)
        self.assertEqual(session.liaison_pane, "C-REAL:1.0-liaison")
        self.assertEqual(session.runner_pane, "runner")
        self.assertEqual(session.workspace_id, "C-REAL:1.0")

        # Verify active workspace in harness
        ws_info = real_harness.get_workspace("C-REAL:1.0")
        self.assertIsNotNone(ws_info)
        self.assertEqual(ws_info["status"], "active")

        # Now teardown
        success = await router.teardown("C-REAL", "1.0")
        self.assertTrue(success)
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertEqual(ws_info["status"], "closed")

    def test_app_dual_pane_harness_wired_to_session_router(self) -> None:
        """Verify app.py wires dual_pane_harness into app.session_router."""
        self.assertIsInstance(app.dual_pane_harness, DualPaneHarness)
        self.assertIsInstance(app.session_router, SessionRouter)
        self.assertIs(app.session_router.harness, app.dual_pane_harness)
        self.assertIs(app.ingress_gateway.router, app.session_router)

    async def test_disk_backed_registry_persists_across_router_restarts(self) -> None:
        """Verify session registry mappings persist to disk and survive router restarts."""
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            reg_file = os.path.join(tmpdir, "sessions_registry.json")
            router1 = SessionRouter(registry_path=reg_file, harness=None)
            router1.record_session(
                session_key="C_PERSIST:1.0",
                conversation_id="conv-999",
                target_cwd="/test/dir",
                channel="C_PERSIST",
                thread_ts="1.0",
            )
            self.assertTrue(os.path.exists(reg_file))
            self.assertTrue(router1.has_thread("C_PERSIST", "1.0"))

            # Instantiate second router instance reading the same file
            router2 = SessionRouter(registry_path=reg_file, harness=None)
            self.assertTrue(router2.has_thread("C_PERSIST", "1.0"))
            rec = router2.get_recorded_session("C_PERSIST:1.0")
            self.assertIsNotNone(rec)
            self.assertEqual(rec["conversation_id"], "conv-999")
            self.assertEqual(rec["target_cwd"], "/test/dir")

    async def test_get_or_create_resumes_saved_conversation_id(self) -> None:
        """Verify get_or_create passes --conversation <id> when session is recorded in registry."""
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            reg_file = os.path.join(tmpdir, "sessions_registry.json")
            created_adapters = []

            def mock_factory(cwd: str, extra_args: Optional[List[str]] = None) -> Any:
                adapter = MockAdapter(cwd)
                adapter.extra_args = extra_args or []
                adapter.conversation_id = "resumed-conv"
                created_adapters.append(adapter)
                return adapter

            router = SessionRouter(
                registry_path=reg_file,
                adapter_factory=mock_factory,
                harness=None,
            )
            router.record_session(
                session_key="C_RESUME:2.0",
                conversation_id="saved-conv-456",
                target_cwd="/test/dir",
                channel="C_RESUME",
                thread_ts="2.0",
            )

            session, is_new = await router.get_or_create("C_RESUME", "2.0")
            self.assertTrue(is_new)
            self.assertEqual(len(created_adapters), 1)
            self.assertEqual(created_adapters[0].extra_args, ["--conversation", "saved-conv-456"])


if __name__ == "__main__":
    unittest.main()
