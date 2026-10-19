#!/usr/bin/env python3
"""
tests/test_sensory_backstage_deadlock_and_stalled_hands.py
Comprehensive unit test suite verifying:
1. RunnerBufferObserver.get_active_task_log wildcard isolation (no unrelated fallback for specific panes).
2. SessionHandle.get_runner_status and check_eyes exposing active backstage hand command, PID, and WebUI buffer tail.
3. IngressGateway._execute_backstage_task non-deadlocking re-entrant dispatch guard.
4. EyesTaskObserver avoiding exponential buffer self-duplication on agy-* hands.
5. NoseSentinel stalled hand detection via probe_stalled_hands and tick().
6. IngressGateway._handle_health_anomaly_alert fallback to send_slack_message when client is None.
"""

import os
import sys
import time
import asyncio
import tempfile
import shutil
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.buffer import RunnerBufferObserver
from golem.session import SessionHandle
from golem.ingress import IngressGateway, BackstageExecutionAccumulator
from golem.eyes import EyesTaskObserver
from golem.nose import NoseSentinel, StatusDelta, AnomalyVerdict
from golem.webui.server import WebUIServer


class TestBufferTaskLogIsolation(unittest.TestCase):
    """Bug 3: Test that get_active_task_log does not return unrelated task logs for specific panes."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_buffer_tasks_")

    def tearDown(self) -> None:
        if os.path.isdir(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_specific_pane_does_not_return_unrelated_task_log(self) -> None:
        """Specific pane 'agy-126489' must NOT match unrelated task log candidates."""
        unrelated_log = os.path.join(self.temp_dir, "runner_task_other_999.log")
        with open(unrelated_log, "w", encoding="utf-8") as f:
            f.write("Unrelated task output\n")

        real_scandir = os.scandir
        with patch("golem.buffer.os.path.isdir", side_effect=lambda d: d in ("/tmp/herdr_tasks", self.temp_dir)):
            with patch("golem.buffer.os.scandir", side_effect=lambda d: real_scandir(self.temp_dir)):
                # 1. Specific pane with no match returns None
                res = RunnerBufferObserver.get_active_task_log(pane="agy-126489")
                self.assertIsNone(res)

                # 2. 'runner' or empty pane falls back to candidates
                res_runner = RunnerBufferObserver.get_active_task_log(pane="runner")
                self.assertEqual(res_runner, unrelated_log)

                res_empty = RunnerBufferObserver.get_active_task_log(pane=None)
                self.assertEqual(res_empty, unrelated_log)

    def test_specific_pane_matches_correct_log(self) -> None:
        """Specific pane returns matching log file."""
        matched_log = os.path.join(self.temp_dir, "agy-126489_run.log")
        with open(matched_log, "w", encoding="utf-8") as f:
            f.write("Matched task output\n")

        real_scandir = os.scandir
        with patch("golem.buffer.os.path.isdir", side_effect=lambda d: d in ("/tmp/herdr_tasks", self.temp_dir)):
            with patch("golem.buffer.os.scandir", side_effect=lambda d: real_scandir(self.temp_dir)):
                res = RunnerBufferObserver.get_active_task_log(pane="agy-126489")
                self.assertEqual(res, matched_log)


class TestBackstageHandSensoryExposure(unittest.IsolatedAsyncioTestCase):
    """Bug 1a: Test get_runner_status and check_eyes exposing active backstage hand status."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.webui = WebUIServer(port=18871, event_bus=self.bus)
        self.session = SessionHandle(session_id="test_sess_backstage", channel="C123", thread_ts="1234.5678", webui_server=self.webui)
        self.gateway = IngressGateway(bus=self.bus, webui_server=self.webui)

    async def test_get_runner_status_reads_active_backstage_hand(self) -> None:
        """When _active_backstage_hand is set on session, get_runner_status returns running status and WebUI buffer tail."""
        pane = "agy-4299"
        self.webui.register_active_hand(
            pane_id=pane,
            node="localhost",
            command="python -m pytest tests/",
            pid=7711,
            worker="agy",
        )
        self.webui.update_hand_buffer(
            pane_id=pane,
            delta="Line 1: init\nLine 2: running tests...\nLine 3: 5 passed\n",
        )

        self.session._active_backstage_hand = {
            "pane_id": pane,
            "command": "python -m pytest tests/",
            "pid": 7711,
            "worker": "agy",
            "node": "localhost",
            "started_at": time.time(),
        }

        # Mock buffer_observer to return an idle shell prompt to simulate Herdr PTY idle state
        mock_buf = MagicMock()
        mock_buf.read_buffer = AsyncMock(return_value="hermes@Hermes AGY %\n")
        self.session.buffer_observer = mock_buf

        # 1. Verify get_runner_status returns live hand info
        status_info = await self.session.get_runner_status()
        self.assertEqual(status_info["status"], "running")
        self.assertEqual(status_info["command"], "python -m pytest tests/")
        self.assertEqual(status_info["pid"], 7711)
        self.assertIn("Line 3: 5 passed", status_info["buffer_tail"])

        # 2. Verify is_executing property
        self.assertTrue(self.session.is_executing)

        # 3. Verify gateway._execute_check_eyes returns RUNNING state
        eyes_res = await self.gateway._execute_check_eyes(session=self.session)
        self.assertEqual(eyes_res["state"], "RUNNING")
        self.assertEqual(eyes_res["active_command"], "python -m pytest tests/")
        self.assertEqual(eyes_res["pid"], 7711)
        self.assertIn("Line 3: 5 passed", eyes_res["buffer_tail"])


class TestReentrantDispatchGuard(unittest.IsolatedAsyncioTestCase):
    """Bug 1b: Test non-deadlocking re-entrant dispatch guard in _execute_backstage_task."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.webui = WebUIServer(port=18872, event_bus=self.bus)
        self.session = SessionHandle(session_id="test_sess_reentrant", channel="C123", thread_ts="1234.5678")
        self.gateway = IngressGateway(bus=self.bus, webui_server=self.webui)

    async def test_reentrant_dispatch_returns_active_progress_immediately(self) -> None:
        """When session.lock is held by an active backstage hand, re-entrant dispatch returns immediately."""
        pane = "agy-reentrant"
        self.webui.register_active_hand(
            pane_id=pane,
            node="localhost",
            command="long running task",
            pid=9988,
            worker="agy",
        )
        self.webui.update_hand_buffer(
            pane_id=pane,
            delta="Step 1 complete\nStep 2 working...\n",
        )

        self.session._active_backstage_hand = {
            "pane_id": pane,
            "command": "long running task",
            "pid": 9988,
            "worker": "agy",
            "node": "localhost",
            "started_at": time.time() - 30.0,
        }

        # Simulate lock being held by the first turn
        await self.session.lock.acquire()
        try:
            # Second turn calls _execute_backstage_task
            result = await self.gateway._execute_backstage_task(
                session=self.session,
                task_desc="long running task",
                pane_id=pane,
            )

            self.assertIsInstance(result, BackstageExecutionAccumulator)
            self.assertEqual(result.turn_status, "SUCCESS")
            self.assertIn(f"[BACKSTAGE TASK ALREADY RUNNING on {pane}", result.final_response)
            self.assertIn("long running task", result.final_response)
            self.assertIn("Step 2 working...", result.final_response)
        finally:
            self.session.lock.release()


class TestEyesExponentialBufferDuplication(unittest.IsolatedAsyncioTestCase):
    """Bug 2: Test that EyesTaskObserver does not exponentially duplicate active hand buffers."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.webui = WebUIServer(port=18873, event_bus=self.bus)

    async def test_agy_hand_buffer_not_duplicated_on_ticks(self) -> None:
        """Evaluating buffer for an agy-* pane does not append clean_buffer back to webui hand."""
        pane = "agy-duplicate-check"
        self.webui.register_active_hand(
            pane_id=pane,
            node="localhost",
            command="compile",
            pid=1234,
            worker="agy",
        )
        initial_log = "Compiling crate...\n"
        self.webui.update_hand_buffer(pane, initial_log)

        observer = EyesTaskObserver(
            event_bus=self.bus,
            webui_server=self.webui,
            pane=pane,
        )

        # Mock classification
        observer.classify_buffer = AsyncMock(return_value={
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Compiling",
        })

        # Tick 1: read from webui
        buf1 = await observer.read_buffer(pane)
        self.assertIn("Compiling crate...", buf1)
        await observer.evaluate_buffer_content(buf1, pane=pane)

        # Hand buffer should still contain only one copy of initial_log
        hand_buf = self.webui._active_hands[pane]["buffer_tail"]
        self.assertEqual(hand_buf.count("Compiling crate..."), 1)

        # Tick 2
        buf2 = await observer.read_buffer(pane)
        await observer.evaluate_buffer_content(buf2, pane=pane)

        # Hand buffer must still contain only one copy (not 2x or 4x!)
        hand_buf2 = self.webui._active_hands[pane]["buffer_tail"]
        self.assertEqual(hand_buf2.count("Compiling crate..."), 1)


class TestNoseStalledHandDetection(unittest.IsolatedAsyncioTestCase):
    """Bug 5: Test NoseSentinel stalled backstage hand detection."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.webui = WebUIServer(port=18874, event_bus=self.bus)
        self.nose = NoseSentinel(event_bus=self.bus, webui_server=self.webui)

    def test_probe_stalled_hands_detects_hung_hand(self) -> None:
        """A RUNNING hand waiting on heartbeats for >= stall threshold produces a non-benign StatusDelta."""
        pane = "agy-stalled-test"
        now = time.time()
        self.webui.register_active_hand(
            pane_id=pane,
            node="localhost",
            command="pytest tests/long_suite.py",
            pid=5555,
            worker="agy",
        )
        # Set started_at to 1200s ago and buffer tail containing heartbeat +1200s
        self.webui._active_hands[pane]["started_at"] = now - 1200.0
        self.webui._active_hands[pane]["last_real_activity_at"] = now - 1200.0
        self.webui._active_hands[pane]["buffer_tail"] = (
            "🚀 [01:00:00] Spawned agy on localhost (PID 5555): pytest tests/long_suite.py\n"
            "⏳ [HEARTBEAT +1200s] Worker active — waiting on model/tool stream...\n"
        )

        with patch.dict(os.environ, {"GOLEM_HAND_STALL_SECONDS": "900"}):
            deltas = self.nose.probe_stalled_hands(now=now)

            self.assertEqual(len(deltas), 1)
            delta = deltas[0]
            self.assertEqual(delta.category, "hand_stalled")
            self.assertEqual(delta.component, f"hand:{pane}")
            self.assertFalse(delta.benign)
            self.assertIn("STALLED", delta.current)
            self.assertIn("has been stuck waiting on model/tool stream", delta.description)

            # Subsequent probe should be deduplicated (emit once per episode)
            deltas_dup = self.nose.probe_stalled_hands(now=now + 10.0)
            self.assertEqual(len(deltas_dup), 0)

            # Settle hand to COMPLETED
            self.webui._active_hands[pane]["status"] = "COMPLETED"
            self.nose.probe_stalled_hands(now=now + 20.0)
            self.assertNotIn(pane, self.nose._alerted_stalled_hands)

    def test_probe_stalled_hands_default_180s_threshold(self) -> None:
        """Default stall threshold is 180s (3 minutes) of zero real activity."""
        pane = "agy-stalled-default"
        now = 2000.0
        self.webui.register_active_hand(
            pane_id=pane,
            node="localhost",
            command="pytest tests/suite.py",
            started_at=now - 200.0,
            last_real_activity_at=now - 200.0,
        )
        self.webui._active_hands[pane]["buffer_tail"] = "⏳ [HEARTBEAT +200s] Worker active — waiting on model/tool stream...\n"

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GOLEM_HAND_STALL_SECONDS", None)
            deltas = self.nose.probe_stalled_hands(now=now)

            self.assertEqual(len(deltas), 1)
            delta = deltas[0]
            self.assertEqual(delta.category, "hand_stalled")
            self.assertIn("STALLED (200s)", delta.current)

    def test_probe_stalled_hands_respects_recent_real_activity(self) -> None:
        """When hand has been running for 500s but had real activity 30s ago, it is NOT stalled."""
        pane = "agy-active-test"
        now = 2000.0
        self.webui.register_active_hand(
            pane_id=pane,
            node="localhost",
            command="compilation",
            started_at=now - 500.0,
            last_real_activity_at=now - 30.0,
        )
        self.webui._active_hands[pane]["buffer_tail"] = "Building object file 42/50\n"

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GOLEM_HAND_STALL_SECONDS", None)
            deltas = self.nose.probe_stalled_hands(now=now)
            self.assertEqual(len(deltas), 0)

    async def test_nose_tick_emits_anomaly_alert_for_stalled_hand(self) -> None:
        """NoseSentinel.tick() breaks quiescence when stalled hand delta is detected."""
        pane = "agy-hung-tick"
        now = time.time()
        self.webui.register_active_hand(
            pane_id=pane,
            node="localhost",
            command="complex task",
            pid=6666,
            worker="agy",
        )
        self.webui._active_hands[pane]["started_at"] = now - 1000.0
        self.webui._active_hands[pane]["last_real_activity_at"] = now - 1000.0
        self.webui._active_hands[pane]["buffer_tail"] = "⏳ [HEARTBEAT +1000s] Worker active — waiting on model/tool stream...\n"

        published_alerts = []
        self.bus.subscribe(lambda a: published_alerts.append(a))

        self.nose.sniff_bot_err = MagicMock(return_value=([], ""))
        self.nose.sniff_fleet = AsyncMock(return_value=MagicMock(nodes={}, disk={}, docker={}))
        self.nose.compute_status_deltas = MagicMock(return_value=[])

        # Mock evaluate_anomaly
        mock_verdict = AnomalyVerdict(
            smells_off=True,
            severity="warning",
            category="hand_stalled",
            summary=f"Backstage hand {pane} stalled",
            affected_nodes=["localhost"],
            recommended_action="Inspect worker",
        )
        self.nose.evaluate_anomaly = AsyncMock(return_value=mock_verdict)

        with patch.dict(os.environ, {"GOLEM_HAND_STALL_SECONDS": "900"}):
            alert = await self.nose.tick()

            self.assertIsNotNone(alert)
            self.assertEqual(alert.alert_type, "health_anomaly")
            self.assertIn("stalled", alert.summary.lower())


class TestWebUIHandRealActivity(unittest.TestCase):
    """Test WebUIServer tracks last_real_activity_at separately from heartbeat lines."""

    def test_update_hand_buffer_ignores_heartbeat_for_last_real_activity(self) -> None:
        pane = "agy-real-act"
        webui = WebUIServer(port=18876)
        t0 = 1000.0
        webui.register_active_hand(
            pane_id=pane,
            node="localhost",
            command="test",
            started_at=t0,
            last_activity_at=t0,
            last_real_activity_at=t0,
        )

        hand = webui._active_hands[pane]
        self.assertEqual(hand["last_real_activity_at"], t0)

        # Heartbeat update
        with patch("golem.webui.server.time.time", return_value=1050.0):
            webui.update_hand_buffer(pane, delta="⏳ [HEARTBEAT +50s] Worker active...\n")

        # last_activity_at should update, but last_real_activity_at must NOT
        self.assertEqual(hand["last_activity_at"], 1050.0)
        self.assertEqual(hand["last_real_activity_at"], t0)

        # Real activity update
        with patch("golem.webui.server.time.time", return_value=1060.0):
            webui.update_hand_buffer(pane, delta="Running step 1 of 5\n")

        self.assertEqual(hand["last_activity_at"], 1060.0)
        self.assertEqual(hand["last_real_activity_at"], 1060.0)


class TestInstantHeartbeatStallAlert(unittest.IsolatedAsyncioTestCase):
    """Test instant stall alert emitted from heartbeat worker in IngressGateway._execute_backstage_task."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.webui = WebUIServer(port=18875, event_bus=self.bus)
        self.nose = NoseSentinel(event_bus=self.bus, webui_server=self.webui)
        self.session = SessionHandle(session_id="test_sess_hb_stall", channel="C123", thread_ts="1234.5678")
        self.gateway = IngressGateway(bus=self.bus, webui_server=self.webui)
        self.gateway.nose_sentinel = self.nose

    async def test_heartbeat_worker_fires_instant_alert_on_stall(self) -> None:
        """Heartbeat worker fires health_anomaly alert when stream activity is silent past stall threshold."""
        pane = "agy-hb-stall"
        published_alerts = []
        self.bus.subscribe(lambda a: published_alerts.append(a), alert_types=["health_anomaly"])

        # Stream generator that hangs (simulating model/stream silence)
        async def hanging_stream():
            yield {"event_type": "status", "data": "starting"}
            await asyncio.sleep(5.0)

        with patch.object(self.gateway, "_stream_backstage_events", return_value=hanging_stream()):
            with patch.dict(os.environ, {
                "GOLEM_HAND_STALL_SECONDS": "0.1",
                "GOLEM_HEARTBEAT_INTERVAL": "0.05",
            }):
                task = asyncio.create_task(
                    self.gateway._execute_backstage_task(
                        session=self.session,
                        task_desc="stalling task",
                        pane_id=pane,
                        channel="C123",
                        thread_ts="1234.5678",
                        yield_timeout=0.01,
                    )
                )

                # Wait slightly for heartbeat worker to detect stall
                await asyncio.sleep(0.25)
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

        # Verify alert was published
        self.assertTrue(len(published_alerts) >= 1)
        alert = published_alerts[0]
        self.assertEqual(alert.alert_type, "health_anomaly")
        self.assertEqual(alert.data.get("category"), "hand_stalled")
        self.assertEqual(alert.data.get("pane_id"), pane)
        # Verify nose sentinel was marked to avoid duplicate alert on next tick
        self.assertIn(pane, self.nose._alerted_stalled_hands)


class TestHealthAnomalySlackFallback(unittest.IsolatedAsyncioTestCase):
    """Bug 6: Test IngressGateway._handle_health_anomaly_alert fallback to send_slack_message."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.gateway = IngressGateway(bus=self.bus)

    async def test_handle_health_anomaly_calls_send_slack_message_when_client_none(self) -> None:
        """When client is None, _handle_health_anomaly_alert resolves channel and sends via send_slack_message."""
        alert = ShoulderTapAlert(
            alert_type="health_anomaly",
            source="nose",
            summary="Stalled backstage hand agy-123 detected.",
            data={"channel": "C0MOCK123"},
        )

        with patch("golem.notify.send_slack_message", new_callable=AsyncMock) as mock_send:
            await self.gateway._handle_health_anomaly_alert(alert, client=None)
            mock_send.assert_awaited_once()
            call_kwargs = mock_send.call_args.kwargs
            self.assertIn("Stalled backstage hand", call_kwargs["text"])
            self.assertEqual(call_kwargs["channel"], "C0MOCK123")


if __name__ == "__main__":
    unittest.main()

