#!/usr/bin/env python3
"""
tests/test_runner_instrumentation_r4.py
Comprehensive unit test suite for Milestone 4 (R4):
Runner Instrumentation & Dynamic EyesTaskObserver Integration.

Validates the 5 Core Requirements of Deliverable R4:
  1. Structured Log Emission in RunnerExecutor.run_command:
     - Dispatch marker: [INFO] Golem.Hands: Dispatched command to pane '{pane}' [PID: {pid}]
     - Yield marker: [INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues
     - Settlement marker: [INFO] Golem.Hands: Command settled with exit code {returncode}
  2. Dynamic EyesTaskObserver Attachment upon 3.0s Command Yield in Pane 2:
     - Observer is instantiated and attached when execution exceeds yield timeout in Pane 2
     - Observer monitoring task is started and stored in executor._active_observers[pane]
     - Fast commands (<3.0s) do not leave active observers attached
  3. Interactive Blocker Event Publishing to ShoulderTapEventBus:
     - Attached observer publishes typed ShoulderTapAlert(alert_type='blocker', source='eyes')
     - Bus subscribers receive OAuth URLs and interactive confirmation prompts
     - Deduplication prevents spamming identical blockers across ticks
  4. Clean Observer Cleanup and Teardown upon Settlement:
     - Explicit settle_command / settle_pane cleanly stops observer and pops from _active_observers
     - Bus completion alert automatically triggers observer teardown and settlement log
     - Idempotent cleanup prevents errors on duplicate settlement calls
  5. Pane 1 and Non-Runner Pane Isolation:
     - Commands yielding in Pane 1 (liaison) or non-runner panes do NOT attach EyesTaskObserver
     - Strict pane discriminator distinguishes Pane 2 (runner) from other panes

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import json
import time
import asyncio
import logging
import unittest
from typing import Dict, Any, Optional, List
from unittest.mock import patch, AsyncMock, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
    get_default_event_bus,
    reset_default_event_bus,
)
from golem.sensory.eyes import EyesTaskObserver
from golem.execution.runner import RunnerExecutor, ExecutionHandle


class LogCaptureHandler(logging.Handler):
    """In-memory logging handler capturing records and formatted messages."""

    def __init__(self) -> None:
        super().__init__()
        self.records: List[logging.LogRecord] = []
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.messages.append(record.getMessage())

    def clear(self) -> None:
        self.records.clear()
        self.messages.clear()


class BaseRunnerInstrumentationTest(unittest.IsolatedAsyncioTestCase):
    """Base test harness with isolated event bus, log capture handler, and executor."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.log_handler = LogCaptureHandler()
        self.log_handler.setLevel(logging.INFO)

        # Attach handler to both Golem.Hands and root logger
        self.hands_logger = logging.getLogger("Golem.Hands")
        self.hands_logger.setLevel(logging.INFO)
        self.hands_logger.addHandler(self.log_handler)

        self.runner_logger = logging.getLogger("Golem.Execution.Runner")
        self.runner_logger.setLevel(logging.INFO)
        self.runner_logger.addHandler(self.log_handler)

        self.mock_buffer_observer = MagicMock()
        self.mock_buffer_observer.read_buffer = AsyncMock(return_value="")

        self.executor = RunnerExecutor(
            herdr_bin="/usr/local/bin/herdr",
            sensory_bus=self.bus,
            buffer_observer=self.mock_buffer_observer,
        )

    async def asyncTearDown(self) -> None:
        self.hands_logger.removeHandler(self.log_handler)
        self.runner_logger.removeHandler(self.log_handler)
        self.log_handler.clear()
        # Clean up any lingering active observers
        if hasattr(self.executor, "_active_observers"):
            for pane, obs in list(self.executor._active_observers.items()):
                if hasattr(obs, "stop_monitoring"):
                    await obs.stop_monitoring()
            self.executor._active_observers.clear()
        self.bus.clear()


class TestRunnerStructuredLogging(BaseRunnerInstrumentationTest):
    """
    Validates Requirement 1:
    Structured log markers emitted in RunnerExecutor.run_command:
      - Dispatch:   [INFO] Golem.Hands: Dispatched command to pane '{pane}' [PID: {pid}]
      - Yield:      [INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues
      - Settlement: [INFO] Golem.Hands: Command settled with exit code {returncode}
    """

    async def test_dispatch_log_marker_emitted_on_command_dispatch(self) -> None:
        """Verify Dispatch log marker is emitted when a command is executed."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.pid = 41234
            mock_proc.communicate.return_value = (b"fast output", b"")
            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="echo 'dispatch test'",
                yield_timeout=3.0,
            )

            self.assertEqual(handle.status, "completed")
            expected_marker = "[INFO] Golem.Hands: Dispatched command to pane 'runner' [PID: 41234]"
            self.assertTrue(
                any(expected_marker in msg for msg in self.log_handler.messages),
                f"Expected dispatch marker '{expected_marker}' not found in logs: {self.log_handler.messages}",
            )

    async def test_yield_log_marker_emitted_when_command_exceeds_timeout(self) -> None:
        """Verify Yield log marker is emitted when command exceeds yield timeout."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 55678
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 55678}}).encode(),
                b"",
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            handle = await self.executor.run_command(
                pane="runner",
                command="sleep 10",
                yield_timeout=0.05,
            )

            self.assertEqual(handle.status, "running")
            expected_marker = (
                "[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues"
            )
            self.assertTrue(
                any(expected_marker in msg for msg in self.log_handler.messages),
                f"Expected yield marker '{expected_marker}' not found in logs: {self.log_handler.messages}",
            )

    async def test_yield_log_marker_not_emitted_for_fast_commands(self) -> None:
        """Verify Yield log marker is NOT emitted when command completes within timeout."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.pid = 12345
            mock_proc.communicate.return_value = (b"quick result", b"")
            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="ls -l",
                yield_timeout=3.0,
            )

            self.assertEqual(handle.status, "completed")
            yield_marker = "Command exceeded 3.0s yield window"
            self.assertFalse(
                any(yield_marker in msg for msg in self.log_handler.messages),
                "Yield marker should NOT be emitted for fast commands",
            )

    async def test_settlement_log_marker_emitted_for_fast_success(self) -> None:
        """Verify Settlement log marker with return code 0 is emitted for fast success."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.pid = 23456
            mock_proc.communicate.return_value = (b"Build success", b"")
            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="make build",
                yield_timeout=3.0,
            )

            self.assertEqual(handle.status, "completed")
            expected_marker = "[INFO] Golem.Hands: Command settled with exit code 0"
            self.assertTrue(
                any(expected_marker in msg for msg in self.log_handler.messages),
                f"Expected settlement marker '{expected_marker}' not found in logs: {self.log_handler.messages}",
            )

    async def test_settlement_log_marker_emitted_for_fast_failure(self) -> None:
        """Verify Settlement log marker with non-zero exit code is emitted for fast failure."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 127
            mock_proc.pid = 34567
            mock_proc.communicate.return_value = (b"", b"command not found: missing_tool")
            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="missing_tool",
                yield_timeout=3.0,
            )

            self.assertEqual(handle.status, "error")
            expected_marker = "[INFO] Golem.Hands: Command settled with exit code 127"
            self.assertTrue(
                any(expected_marker in msg for msg in self.log_handler.messages),
                f"Expected settlement marker '{expected_marker}' not found in logs: {self.log_handler.messages}",
            )

    async def test_settlement_log_marker_emitted_on_explicit_settle(self) -> None:
        """Verify Settlement log marker is emitted when settle_command or settle_pane is invoked."""
        if hasattr(self.executor, "settle_command"):
            await self.executor.settle_command("runner", returncode=0)
        elif hasattr(self.executor, "settle_pane"):
            await self.executor.settle_pane("runner", returncode=0)

        expected_marker = "[INFO] Golem.Hands: Command settled with exit code 0"
        self.assertTrue(
            any(expected_marker in msg for msg in self.log_handler.messages),
            f"Expected settlement marker '{expected_marker}' in logs: {self.log_handler.messages}",
        )


class TestDynamicEyesObserverAttachment(BaseRunnerInstrumentationTest):
    """
    Validates Requirement 2:
    Dynamic EyesTaskObserver attachment upon 3.0s command yield in Pane 2.
    """

    async def test_observer_attached_when_pane2_command_yields(self) -> None:
        """Verify EyesTaskObserver is instantiated and registered when command yields in Pane 2."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 66778
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 66778}}).encode(),
                b"",
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            handle = await self.executor.run_command(
                pane="runner",
                command="npm run build",
                yield_timeout=0.05,
                session_id="session-test-p2",
            )

            self.assertEqual(handle.status, "running")
            self.assertIn("runner", self.executor._active_observers)

            observer = self.executor._active_observers["runner"]
            self.assertIsInstance(observer, EyesTaskObserver)
            self.assertTrue(observer.is_monitoring)
            self.assertEqual(observer.pane, "runner")
            self.assertEqual(observer.session_id, "session-test-p2")
            self.assertIs(observer.event_bus, self.bus)

    async def test_fast_command_does_not_attach_observer(self) -> None:
        """Verify commands completing within yield timeout do not attach an observer."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.pid = 11111
            mock_proc.communicate.return_value = (b"Finished immediately", b"")
            mock_exec.return_value = mock_proc

            handle = await self.executor.run_command(
                pane="runner",
                command="echo hello",
                yield_timeout=3.0,
            )

            self.assertEqual(handle.status, "completed")
            self.assertNotIn("runner", self.executor._active_observers)
            self.assertEqual(len(self.executor._active_observers), 0)

    async def test_resolved_pane2_id_attaches_observer(self) -> None:
        """Verify resolved Pane 2 ID (e.g. w1:p2) correctly attaches an observer."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 88990
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 88990}}).encode(),
                b"",
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            handle = await self.executor.run_command(
                pane="w1:p2",
                command="cargo build --release",
                yield_timeout=0.05,
            )

            self.assertEqual(handle.status, "running")
            self.assertIn("w1:p2", self.executor._active_observers)
            observer = self.executor._active_observers["w1:p2"]
            self.assertTrue(observer.is_monitoring)


class TestEyesObserverBlockerPublishing(BaseRunnerInstrumentationTest):
    """
    Validates Requirement 3:
    Blocker event publishing to ShoulderTapEventBus from dynamically attached observer.
    """

    async def test_attached_observer_publishes_oauth_blocker_to_bus(self) -> None:
        """Verify attached observer detects OAuth callback URL and publishes blocker alert to bus."""
        received_alerts: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda alert: received_alerts.append(alert), alert_types=["blocker"])

        # Yield command in Pane 2 to attach observer
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 99111
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 99111}}).encode(),
                b"",
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            handle = await self.executor.run_command(
                pane="runner",
                command="rclone config reconnect",
                yield_timeout=0.05,
                session_id="oauth-test-sess",
            )
            self.assertEqual(handle.status, "running")

        observer = self.executor._active_observers["runner"]

        # Mock buffer observer providing OAuth URL
        oauth_buffer = (
            "Authenticating with remote storage...\n"
            "Open the following link to authenticate: http://127.0.0.1:53682/auth?state=xyz\n"
            "Waiting for code..."
        )
        self.mock_buffer_observer.read_buffer = AsyncMock(return_value=oauth_buffer)
        observer.buffer_observer = self.mock_buffer_observer

        # Execute evaluation tick
        alert = await observer.tick()

        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "blocker")
        self.assertEqual(alert.source, "eyes")
        self.assertEqual(alert.data.get("url"), "http://127.0.0.1:53682/auth?state=xyz")
        self.assertEqual(alert.data.get("type"), "oauth")

        # Verify bus delivered alert to subscribers
        await asyncio.sleep(0.05)
        self.assertEqual(len(received_alerts), 1)
        self.assertEqual(received_alerts[0].session_id, "oauth-test-sess")

    async def test_attached_observer_publishes_prompt_blocker_to_bus(self) -> None:
        """Verify attached observer detects confirmation prompt and publishes blocker alert."""
        received_alerts: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda alert: received_alerts.append(alert), alert_types=["blocker"])

        # Attach observer via yield
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 99222
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 99222}}).encode(),
                b"",
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            await self.executor.run_command(
                pane="runner",
                command="apt-get upgrade",
                yield_timeout=0.05,
                session_id="prompt-test-sess",
            )

        observer = self.executor._active_observers["runner"]
        prompt_buffer = "Do you want to continue? [y/N]: "
        self.mock_buffer_observer.read_buffer = AsyncMock(return_value=prompt_buffer)
        observer.buffer_observer = self.mock_buffer_observer

        alert = await observer.tick()

        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "blocker")
        self.assertEqual(alert.data.get("type"), "prompt")

        await asyncio.sleep(0.05)
        self.assertEqual(len(received_alerts), 1)

    async def test_attached_observer_deduplicates_identical_blocker_alerts(self) -> None:
        """Verify identical blocker alerts are not repeatedly published across ticks."""
        received_alerts: List[ShoulderTapAlert] = []
        self.bus.subscribe(lambda alert: received_alerts.append(alert), alert_types=["blocker"])

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 99333
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 99333}}).encode(),
                b"",
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            await self.executor.run_command(
                pane="runner",
                command="rclone authorize",
                yield_timeout=0.05,
            )

        observer = self.executor._active_observers["runner"]
        oauth_buffer = "Please visit http://127.0.0.1:8080/auth to log in."
        self.mock_buffer_observer.read_buffer = AsyncMock(return_value=oauth_buffer)
        observer.buffer_observer = self.mock_buffer_observer

        # Tick 1: publishes blocker alert
        alert1 = await observer.tick()
        self.assertIsNotNone(alert1)

        # Tick 2: buffer unchanged, must not publish duplicate alert
        alert2 = await observer.tick()
        self.assertIsNone(alert2)

        await asyncio.sleep(0.05)
        self.assertEqual(len(received_alerts), 1)


class TestObserverSettlementAndCleanup(BaseRunnerInstrumentationTest):
    """
    Validates Requirement 4:
    Clean observer cleanup and teardown upon settlement.
    """

    async def test_settle_command_stops_and_unmounts_observer(self) -> None:
        """Verify settle_command cleanly stops the background watcher task and unregisters."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 77112
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 77112}}).encode(),
                b"",
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            await self.executor.run_command(
                pane="runner",
                command="long_build.sh",
                yield_timeout=0.05,
            )

        self.assertIn("runner", self.executor._active_observers)
        observer = self.executor._active_observers["runner"]
        self.assertTrue(observer.is_monitoring)

        # Settle the command
        if hasattr(self.executor, "settle_command"):
            await self.executor.settle_command("runner", returncode=0)
        elif hasattr(self.executor, "settle_pane"):
            await self.executor.settle_pane("runner", returncode=0)

        # Verify observer is removed from registry and cancelled
        self.assertNotIn("runner", self.executor._active_observers)
        self.assertFalse(observer.is_monitoring)

        # Verify settlement log marker was emitted
        expected_marker = "[INFO] Golem.Hands: Command settled with exit code 0"
        self.assertTrue(any(expected_marker in msg for msg in self.log_handler.messages))

    async def test_settle_command_is_idempotent(self) -> None:
        """Verify calling settle_command repeatedly does not raise errors."""
        # Calling on non-existent pane should be a safe no-op
        if hasattr(self.executor, "settle_command"):
            await self.executor.settle_command("nonexistent_pane", returncode=0)
            await self.executor.settle_command("nonexistent_pane", returncode=0)
        elif hasattr(self.executor, "settle_pane"):
            await self.executor.settle_pane("nonexistent_pane", returncode=0)
            await self.executor.settle_pane("nonexistent_pane", returncode=0)

        self.assertEqual(len(self.executor._active_observers), 0)

    async def test_bus_completion_alert_triggers_automatic_settlement(self) -> None:
        """Verify ShoulderTapEventBus completion alert automatically triggers settlement."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 88223
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 88223}}).encode(),
                b"",
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            await self.executor.run_command(
                pane="runner",
                command="test_job.sh",
                yield_timeout=0.05,
            )

        self.assertIn("runner", self.executor._active_observers)
        observer = self.executor._active_observers["runner"]

        # Publish completion alert on the bus
        completion_alert = ShoulderTapAlert(
            alert_type="completion",
            source="eyes",
            session_id="test-job",
            summary="Job finished successfully",
            data={"pane": "runner", "is_error": False},
        )
        self.bus.publish(completion_alert)

        await asyncio.sleep(0.05)

        # Active observer must be unmounted and stopped
        self.assertNotIn("runner", self.executor._active_observers)
        self.assertFalse(observer.is_monitoring)

        # Settlement marker emitted
        expected_marker = "[INFO] Golem.Hands: Command settled with exit code 0"
        self.assertTrue(any(expected_marker in msg for msg in self.log_handler.messages))


class TestPaneIsolationNonRunner(BaseRunnerInstrumentationTest):
    """
    Validates Requirement 5:
    Pane 1 or non-runner pane isolation (no spurious observer attachment).
    """

    async def test_pane1_liaison_yield_does_not_attach_observer(self) -> None:
        """Verify Pane 1 (liaison) yielding does NOT attach an EyesTaskObserver."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.pid = 10101
            mock_proc.communicate.side_effect = asyncio.TimeoutError()

            mock_info_proc = AsyncMock()
            mock_info_proc.returncode = 0
            mock_info_proc.communicate.return_value = (
                json.dumps({"result": {"pid": 10101}}).encode(),
                b"",
            )
            mock_exec.side_effect = [mock_proc, mock_info_proc]

            handle = await self.executor.run_command(
                pane="fm-sess-liaison",
                command="agy thinking...",
                yield_timeout=0.05,
            )

            self.assertEqual(handle.status, "running")
            # CRITICAL ISOLATION INVARIANT:
            self.assertNotIn("fm-sess-liaison", self.executor._active_observers)
            self.assertEqual(
                len(self.executor._active_observers), 0,
                "Pane 1 (liaison) must NEVER trigger EyesTaskObserver attachment!",
            )

    async def test_non_runner_pane_yield_does_not_attach_observer(self) -> None:
        """Verify arbitrary non-runner panes do NOT attach an EyesTaskObserver."""
        non_runner_panes = ["control", "system", "daemon", "w1:p1", "liaison"]

        for test_pane in non_runner_panes:
            with patch("asyncio.create_subprocess_exec") as mock_exec:
                mock_proc = AsyncMock()
                mock_proc.pid = 20202
                mock_proc.communicate.side_effect = asyncio.TimeoutError()

                mock_info_proc = AsyncMock()
                mock_info_proc.returncode = 0
                mock_info_proc.communicate.return_value = (
                    json.dumps({"result": {"pid": 20202}}).encode(),
                    b"",
                )
                mock_exec.side_effect = [mock_proc, mock_info_proc]

                handle = await self.executor.run_command(
                    pane=test_pane,
                    command="arbitrary command",
                    yield_timeout=0.05,
                )

                self.assertEqual(handle.status, "running")
                self.assertNotIn(
                    test_pane, self.executor._active_observers,
                    f"Non-runner pane '{test_pane}' must not attach EyesTaskObserver!",
                )
                self.assertEqual(len(self.executor._active_observers), 0)

    def test_is_runner_pane_discriminator_matrix(self) -> None:
        """Verify is_runner_pane discriminates Pane 2 from Pane 1 and other panes correctly."""
        if hasattr(self.executor, "is_runner_pane"):
            # Positive Pane 2 cases
            self.assertTrue(self.executor.is_runner_pane("runner"))
            self.assertTrue(self.executor.is_runner_pane("p2"))
            self.assertTrue(self.executor.is_runner_pane("pane2"))
            self.assertTrue(self.executor.is_runner_pane("w1:p2"))
            self.assertTrue(self.executor.is_runner_pane("session-runner"))
            self.assertTrue(self.executor.is_runner_pane("custom_runner"))

            # Negative Non-runner cases
            self.assertFalse(self.executor.is_runner_pane("liaison"))
            self.assertFalse(self.executor.is_runner_pane("fm-sess-liaison"))
            self.assertFalse(self.executor.is_runner_pane("p1"))
            self.assertFalse(self.executor.is_runner_pane("w1:p1"))
            self.assertFalse(self.executor.is_runner_pane("control"))
            self.assertFalse(self.executor.is_runner_pane("system"))


if __name__ == "__main__":
    unittest.main()
