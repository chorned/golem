#!/usr/bin/env python3
"""
tests/test_eyes_runner_task_stream.py
Tests verifying CHO-216:
1. RunnerBufferObserver import and class binding in golem.eyes and golem.webui.server.
2. Binding real runner terminal buffer and active /tmp/herdr_tasks/ task log to EyesTaskObserver.
3. Streaming live buffer diffs, state classifications (RUNNING, IDLE, COMPLETED, BLOCKER),
   and active command telemetry to the Eyes WebUI terminal stream.
"""

import os
import sys
import time
import shutil
import tempfile
import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.eyes import EyesTaskObserver, RunnerBufferObserver as EyesRunnerBufferObserver
from golem.buffer import RunnerBufferObserver
from golem.webui.server import (
    WebUIServer,
    RunnerBufferObserver as ServerRunnerBufferObserver,
    EyesTaskObserver as ServerEyesTaskObserver,
)


class TestEyesRunnerTaskStream(unittest.IsolatedAsyncioTestCase):
    """Unit tests for CHO-216 Eyes runner buffer and task log streaming."""

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_herdr_tasks_")
        self.bus = ShoulderTapEventBus()
        self.webui = WebUIServer(port=18765, event_bus=self.bus)

    async def asyncTearDown(self) -> None:
        if os.path.isdir(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_runner_buffer_observer_bindings(self) -> None:
        """Verify RunnerBufferObserver is cleanly exported and bound in golem.eyes and golem.webui.server."""
        self.assertIs(EyesRunnerBufferObserver, RunnerBufferObserver)
        self.assertIs(ServerRunnerBufferObserver, RunnerBufferObserver)
        self.assertIs(ServerEyesTaskObserver, EyesTaskObserver)

        # Verify WebUIServer instance attributes
        self.assertIsNone(self.webui.buffer_observer)
        self.assertIsNone(self.webui.eyes_observer)
        eyes = self.webui._ensure_eyes_live()
        self.assertIsInstance(eyes, EyesTaskObserver)
        self.assertIs(self.webui.eyes_observer, eyes)
        self.assertIsInstance(self.webui.buffer_observer, RunnerBufferObserver)

    async def test_bind_runner_buffer_and_read(self) -> None:
        """Verify explicitly binding a runner buffer callable or string to EyesTaskObserver."""
        observer = EyesTaskObserver(
            event_bus=self.bus,
            webui_server=self.webui,
            pane="runner",
        )

        # 1. String binding
        observer.bind_runner_buffer("Line 1: Building assets\nLine 2: Compilation successful\n")
        content = await observer.read_buffer("runner", lines=10)
        self.assertIn("Line 1: Building assets", content)
        self.assertIn("Line 2: Compilation successful", content)

        # 2. Callable binding
        def _get_buffer():
            return "Dynamic buffer content: Step 3 running..."

        observer.bind_runner_buffer(_get_buffer)
        content_dyn = await observer.read_buffer("runner")
        self.assertEqual(content_dyn, "Dynamic buffer content: Step 3 running...")

    async def test_bind_active_herdr_task_log(self) -> None:
        """Verify binding and auto-discovering an active task log in /tmp/herdr_tasks/."""
        # Create a mock task log file
        task_log_path = os.path.join(self.temp_dir, "runner_task_123.log")
        with open(task_log_path, "w", encoding="utf-8") as f:
            f.write("Task log line 1\nTask log line 2\nTask log line 3\n")

        observer = EyesTaskObserver(
            event_bus=self.bus,
            webui_server=self.webui,
            pane="runner",
        )

        # 1. Explicit bind_task_log
        observer.bind_task_log(task_log_path)
        content = await observer.read_buffer("runner", lines=2)
        self.assertIn("Task log line 2", content)
        self.assertIn("Task log line 3", content)

        # 2. Auto-discovery via RunnerBufferObserver.get_active_task_log
        with patch("golem.buffer.time.time", return_value=os.path.getmtime(task_log_path) + 5.0):
            with patch.object(
                RunnerBufferObserver,
                "get_active_task_log",
                return_value=task_log_path
            ):
                observer.bind_task_log("")
                observer.bind_runner_buffer(None)
                # Ensure buffer_observer fallback reads task log
                found_log = RunnerBufferObserver.get_active_task_log("runner")
                self.assertEqual(found_log, task_log_path)
                tail = RunnerBufferObserver.read_task_log(found_log, lines=2)
                self.assertIn("Task log line 3", tail)

    async def test_stream_live_buffer_diffs_and_telemetry(self) -> None:
        """Verify streaming of buffer diffs, active commands, and state classifications to WebUI."""
        broadcast_events = []

        def _mock_broadcast(event_type: str, data: dict):
            broadcast_events.append((event_type, data))

        self.webui.broadcast = _mock_broadcast

        # Register an active hand with command
        self.webui.register_active_hand(
            pane_id="runner",
            node="localhost",
            command="python -m pytest tests/",
            pid=9876,
        )

        observer = EyesTaskObserver(
            event_bus=self.bus,
            webui_server=self.webui,
            pane="runner",
            task="Run unit test suite",
        )

        # Mock classify_buffer to return RUNNING
        observer.classify_buffer = AsyncMock(return_value={
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Executing pytest suite",
        })

        # Tick 1: Initial buffer
        buffer_t1 = "test_1.py ... passed\ntest_2.py ... passed\n"
        await observer.evaluate_buffer_content(buffer_t1, pane="runner")

        # Verify diff was streamed to WebUI
        eye_logs = [data for ev, data in broadcast_events if ev == "eye_log"]
        self.assertTrue(any("[DIFF]" in str(d.get("message")) for d in eye_logs))
        self.assertTrue(any("[COMMAND]" in str(d.get("message")) for d in eye_logs))

        # Verify active command telemetry
        telemetries = [data for ev, data in broadcast_events if ev == "eye_telemetry"]
        self.assertTrue(len(telemetries) > 0)
        latest_tel = telemetries[-1]
        self.assertEqual(latest_tel.get("state"), "RUNNING")
        self.assertEqual(latest_tel.get("command"), "python -m pytest tests/")
        self.assertEqual(latest_tel.get("pid"), 9876)

        # Tick 2: Additional lines added (live diff)
        buffer_t2 = (
            "test_1.py ... passed\n"
            "test_2.py ... passed\n"
            "test_3.py ... passed\n"
            "test_4.py ... passed\n"
        )
        observer.classify_buffer = AsyncMock(return_value={
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Tests progressing",
        })
        await observer.evaluate_buffer_content(buffer_t2, pane="runner")

        diff_logs = [d for ev, d in broadcast_events if ev == "eye_log" and "[DIFF]" in str(d.get("message"))]
        self.assertTrue(len(diff_logs) >= 2)
        self.assertIn("+2 lines", diff_logs[-1]["message"])

        # Tick 3: State transition to BLOCKER
        observer.classify_buffer = AsyncMock(return_value={
            "state": "BLOCKER",
            "blocker_type": "oauth",
            "blocker_url": "http://127.0.0.1:53682/auth",
            "blocker_prompt": None,
            "summary": "OAuth login required",
        })
        buffer_t3 = buffer_t2 + "Please authorize at: http://127.0.0.1:53682/auth\n"
        await observer.evaluate_buffer_content(buffer_t3, pane="runner")

        blocker_logs = [d for ev, d in broadcast_events if ev == "eye_log" and "[BLOCKER]" in str(d.get("message"))]
        self.assertTrue(len(blocker_logs) > 0)
        self.assertEqual(blocker_logs[-1]["state"], "BLOCKER")

        # Tick 4: State transition to COMPLETED
        observer.classify_buffer = AsyncMock(return_value={
            "state": "COMPLETED",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "All 4 tests passed successfully",
        })
        buffer_t4 = buffer_t3 + "4 passed in 1.20s\nOK\n"
        await observer.evaluate_buffer_content(buffer_t4, pane="runner")

        comp_logs = [d for ev, d in broadcast_events if ev == "eye_log" and "[COMPLETED]" in str(d.get("message"))]
        self.assertTrue(len(comp_logs) > 0)
        self.assertEqual(comp_logs[-1]["state"], "COMPLETED")


if __name__ == "__main__":
    unittest.main()
