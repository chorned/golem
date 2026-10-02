#!/usr/bin/env python3
"""
tests/test_tc01_empirical_challenge.py
Adversarial empirical challenge test suite for TC-01 (Dual-Pane Workspace Provisioning & Lifecycle).
Validates:
1. Exact CLI invocation contract: Pane 1 ({session_id}-liaison) and Pane 2 (runner).
2. Metadata association on SessionHandle (harness, workspace_id, liaison_pane, runner_pane).
3. Multi-turn session persistence across sequential turns.
4. Concurrency & race condition resistance under high parallel load.
5. Fault injection & error recovery during provisioning and teardown.
6. Special character robustness in session identifiers.
7. Idempotency of teardown and close.
8. Backward compatibility when harness is None.
"""

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock

from first_mate.gateway.router import SessionRouter
from first_mate.gateway.session import SessionHandle, SessionState
from first_mate.execution.harness import DualPaneHarness
from herdr_client import HerdrClient


class MockAdapter:
    """Mock agent adapter for test harness."""

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


class TestTC01EmpiricalChallenge(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge suite for TC-01 dual-pane provisioning and lifecycle."""

    async def asyncSetUp(self) -> None:
        self.mock_harness = AsyncMock(spec=DualPaneHarness)
        self.mock_harness.provision_dual_pane.side_effect = lambda session_id, cwd: (
            f"{session_id}-liaison",
            "runner",
        )
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

    async def test_tc01_dual_pane_provisioning_exact_contract(self) -> None:
        """TC-01: SessionRouter.get_or_create provisions Pane 1 and Pane 2 with exact metadata."""
        session, is_new = await self.router.get_or_create("C_SLACK_1", "1700000000.000100")

        self.assertTrue(is_new)
        expected_session_id = "C_SLACK_1:1700000000.000100"
        self.mock_harness.provision_dual_pane.assert_called_once_with(
            session_id=expected_session_id,
            cwd="/Users/hermes/projects/AGY",
        )
        self.assertIs(session.harness, self.mock_harness)
        self.assertEqual(session.workspace_id, expected_session_id)
        self.assertEqual(session.liaison_pane, f"{expected_session_id}-liaison")
        self.assertEqual(session.runner_pane, "runner")
        self.assertEqual(session.state, SessionState.IDLE)
        self.assertTrue(self.adapter.started)

    async def test_tc01_multi_turn_persistence_invariant(self) -> None:
        """TC-01: Multi-turn queries on same thread reuse handle without re-provisioning."""
        channel, thread_ts = "C_PERSIST", "1700000001.000001"
        first_session, is_new_first = await self.router.get_or_create(channel, thread_ts)
        self.assertTrue(is_new_first)
        self.assertEqual(self.mock_harness.provision_dual_pane.call_count, 1)

        # Execute 10 sequential turns
        for turn_idx in range(2, 12):
            session, is_new = await self.router.get_or_create(channel, thread_ts)
            self.assertFalse(is_new, f"Turn {turn_idx} should not report is_new=True")
            self.assertIs(session, first_session, f"Turn {turn_idx} must return the exact same handle")
            self.assertEqual(
                session.workspace_id,
                f"{channel}:{thread_ts}",
                f"Workspace ID mutated on turn {turn_idx}",
            )
            self.assertEqual(
                session.liaison_pane,
                f"{channel}:{thread_ts}-liaison",
                f"Liaison pane mutated on turn {turn_idx}",
            )
            self.assertEqual(
                session.runner_pane,
                "runner",
                f"Runner pane mutated on turn {turn_idx}",
            )

        # Harness provision_dual_pane must NOT have been called again
        self.assertEqual(self.mock_harness.provision_dual_pane.call_count, 1)

    async def test_tc01_concurrent_get_or_create_race_condition(self) -> None:
        """TC-01: 25 concurrent calls for the same session must provision exactly once."""
        channel, thread_ts = "C_RACE", "1700000002.999999"
        num_tasks = 25

        async def worker():
            return await self.router.get_or_create(channel, thread_ts)

        results = await asyncio.gather(*(worker() for _ in range(num_tasks)))

        new_count = sum(1 for _, is_new in results if is_new)
        reused_count = sum(1 for _, is_new in results if not is_new)
        first_handle = results[0][0]

        self.assertEqual(new_count, 1, "Exactly one task should create the new session")
        self.assertEqual(reused_count, num_tasks - 1, "All other tasks should receive reused session")
        for idx, (handle, _) in enumerate(results):
            self.assertIs(handle, first_handle, f"Task {idx} received a different session handle")

        # Crucial: dual pane harness provision must only be called once
        self.assertEqual(self.mock_harness.provision_dual_pane.call_count, 1)

    async def test_tc01_concurrent_distinct_threads_isolation(self) -> None:
        """TC-01: Concurrent calls across distinct threads provision isolated dual-pane workspaces."""
        num_threads = 15

        async def worker(idx: int):
            return await self.router.get_or_create(f"C_CHAN_{idx}", f"TS_{idx}.0")

        results = await asyncio.gather(*(worker(i) for i in range(num_threads)))

        sessions = [handle for handle, is_new in results]
        # All must be distinct
        self.assertEqual(len(set(sessions)), num_threads)
        self.assertEqual(len(self.router.sessions), num_threads)

        for idx, handle in enumerate(sessions):
            expected_id = f"C_CHAN_{idx}:TS_{idx}.0"
            self.assertEqual(handle.workspace_id, expected_id)
            self.assertEqual(handle.liaison_pane, f"{expected_id}-liaison")
            self.assertEqual(handle.runner_pane, "runner")

        self.assertEqual(self.mock_harness.provision_dual_pane.call_count, num_threads)

    async def test_tc01_fault_injection_provisioning_failure(self) -> None:
        """TC-01: Provisioning exception does not leak broken session; retry succeeds."""
        failing_harness = AsyncMock(spec=DualPaneHarness)
        failing_harness.provision_dual_pane.side_effect = RuntimeError("Herdr daemon connection refused")

        router = SessionRouter(
            default_target_cwd="/tmp",
            adapter_factory=lambda cwd: MockAdapter(cwd),
            harness=failing_harness,
        )

        with self.assertRaises(RuntimeError) as ctx:
            await router.get_or_create("C_FAIL", "100.1")
        self.assertIn("Herdr daemon connection refused", str(ctx.exception))

        # Crucial: session table must be empty, not holding a partial broken handle
        self.assertNotIn("C_FAIL:100.1", router.sessions)

        # Now fix harness and retry
        failing_harness.provision_dual_pane.side_effect = None
        failing_harness.provision_dual_pane.return_value = ("C_FAIL:100.1-liaison", "runner")

        session, is_new = await router.get_or_create("C_FAIL", "100.1")
        self.assertTrue(is_new)
        self.assertEqual(session.liaison_pane, "C_FAIL:100.1-liaison")
        self.assertEqual(session.runner_pane, "runner")

    async def test_tc01_teardown_fault_tolerance(self) -> None:
        """TC-01: Harness teardown failure does not crash router or prevent session close."""
        self.mock_harness.teardown.side_effect = RuntimeError("Failed to close Herdr workspace")

        session, _ = await self.router.get_or_create("C_FAULT_TEARDOWN", "100.2")

        # Teardown should catch exception gracefully and return True
        success = await self.router.teardown("C_FAULT_TEARDOWN", "100.2")
        self.assertTrue(success)
        self.assertEqual(session.state, SessionState.CLOSED)
        self.assertNotIn("C_FAULT_TEARDOWN:100.2", self.router.sessions)

    async def test_tc01_session_close_idempotency(self) -> None:
        """TC-01: session.close() is strictly idempotent and only calls harness.teardown once."""
        session, _ = await self.router.get_or_create("C_IDEMPOTENT", "100.3")

        await session.close(reason="first_close")
        self.assertEqual(self.mock_harness.teardown.call_count, 1)
        self.assertIsNone(session.harness)
        self.assertTrue(session._harness_torn_down)

        # Second and third close
        await session.close(reason="second_close")
        await session.close(reason="third_close")
        self.assertEqual(self.mock_harness.teardown.call_count, 1)

    async def test_tc01_special_characters_in_identifiers(self) -> None:
        """TC-01: Handles colons, hyphens, and dots in channel and thread identifiers."""
        complex_channel = "C#channel-alpha:dept"
        complex_thread = "1700000000.123:456-beta"
        expected_session_id = f"{complex_channel}:{complex_thread}"

        session, is_new = await self.router.get_or_create(complex_channel, complex_thread)
        self.assertTrue(is_new)
        self.assertEqual(session.session_id, expected_session_id)
        self.assertEqual(session.workspace_id, expected_session_id)
        self.assertEqual(session.liaison_pane, f"{expected_session_id}-liaison")
        self.assertEqual(session.runner_pane, "runner")

        success = await self.router.teardown(complex_channel, complex_thread)
        self.assertTrue(success)
        self.mock_harness.teardown.assert_called_once_with(expected_session_id)

    async def test_tc01_backward_compatibility_when_harness_none(self) -> None:
        """TC-01: SessionRouter runs cleanly without harness when harness is None."""
        router = SessionRouter(
            default_target_cwd="/tmp",
            adapter_factory=lambda cwd: MockAdapter(cwd),
            harness=None,
        )

        session, is_new = await router.get_or_create("C_NOHARNESS", "1.1")
        self.assertTrue(is_new)
        self.assertIsNone(session.harness)
        self.assertIsNone(session.workspace_id)
        self.assertIsNone(session.liaison_pane)
        self.assertIsNone(session.runner_pane)

        # Multi-turn persistence works without harness
        session2, is_new2 = await router.get_or_create("C_NOHARNESS", "1.1")
        self.assertFalse(is_new2)
        self.assertIs(session, session2)

        # Teardown works cleanly without harness
        success = await router.teardown("C_NOHARNESS", "1.1")
        self.assertTrue(success)
        self.assertEqual(session.state, SessionState.CLOSED)

    async def test_tc01_real_dual_pane_harness_cli_call_semantics(self) -> None:
        """
        TC-01: Verifies real DualPaneHarness produces exact Herdr CLI commands:
        1. workspace create --label <session_id> --cwd <cwd> --no-focus
        2. agent start <session_id>-liaison --kind agy --pane 1
        3. pane create --workspace <session_id> --label runner
        4. workspace close <session_id>
        """
        mock_herdr = AsyncMock(spec=HerdrClient)
        executed_commands = []

        async def record_cli(cmd):
            executed_commands.append(list(cmd))
            if cmd[:2] == ["workspace", "create"]:
                return (
                    0,
                    json.dumps({
                        "result": {
                            "workspace": {"workspace_id": "ws-emp-1"},
                            "root_pane": {"pane_id": "1"},
                        }
                    }),
                    "",
                )
            elif cmd[:2] == ["agent", "start"]:
                return (0, "agy started", "")
            elif cmd[:2] == ["pane", "create"]:
                return (
                    0,
                    json.dumps({
                        "result": {
                            "pane": {"pane_id": "runner-p2"},
                        }
                    }),
                    "",
                )
            elif cmd[:2] == ["workspace", "close"]:
                return (0, "workspace closed", "")
            return (0, "", "")

        mock_herdr.run_cli.side_effect = record_cli

        real_harness = DualPaneHarness(herdr_client=mock_herdr, default_cwd="/tmp")
        router = SessionRouter(
            default_target_cwd="/tmp",
            adapter_factory=lambda cwd: MockAdapter(cwd),
            harness=real_harness,
        )

        session_key = "C_EMP:T_EMP"
        session, is_new = await router.get_or_create("C_EMP", "T_EMP", target_cwd="/test/repo")
        self.assertTrue(is_new)
        self.assertEqual(session.liaison_pane, f"{session_key}-liaison")
        self.assertEqual(session.runner_pane, "runner")

        # Verify CLI commands issued during provisioning
        self.assertEqual(len(executed_commands), 3)
        self.assertEqual(
            executed_commands[0],
            ["workspace", "create", "--label", session_key, "--cwd", "/test/repo", "--no-focus"],
        )
        self.assertEqual(
            executed_commands[1],
            ["agent", "start", f"{session_key}-liaison", "--kind", "agy", "--pane", "1"],
        )
        self.assertEqual(
            executed_commands[2],
            ["pane", "create", "--workspace", session_key, "--label", "runner"],
        )

        # Now teardown
        await router.teardown("C_EMP", "T_EMP")
        self.assertEqual(len(executed_commands), 4)
        self.assertEqual(executed_commands[3], ["workspace", "close", "ws-emp-1"])


if __name__ == "__main__":
    unittest.main()
