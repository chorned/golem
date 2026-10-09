#!/usr/bin/env python3
"""
tests/test_sensory_bus.py
Unit tests for ShoulderTapEventBus and ShoulderTapAlert.
Validates:
  - ShoulderTapAlert data class attributes, defaults, and serialization
  - Publish-subscribe delivery to single and multiple subscribers
  - Alert type filtering (blocker, completion, health_anomaly)
  - Session-scoped queues for waking active liaison sessions
  - Queue overflow handling
  - Error isolation: exceptions in one subscriber callback do not affect others
  - Non-blocking asynchronous dispatch

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import time
import asyncio
import unittest
from typing import Optional, List, Dict, Any

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
    get_default_event_bus,
    peek_default_event_bus,
    reset_default_event_bus,
)
from golem.gateway.session import SessionHandle
from golem.gateway.router import SessionRouter


class TestShoulderTapAlert(unittest.TestCase):
    """Unit tests for ShoulderTapAlert dataclass."""

    def test_alert_initialization_defaults(self) -> None:
        """Verify default values when initializing ShoulderTapAlert."""
        t_before = time.time()
        alert = ShoulderTapAlert(
            alert_type="blocker",
            source="eyes",
        )
        t_after = time.time()

        self.assertEqual(alert.alert_type, "blocker")
        self.assertEqual(alert.source, "eyes")
        self.assertIsNone(alert.session_id)
        self.assertIsNone(alert.channel)
        self.assertIsNone(alert.thread_ts)
        self.assertEqual(alert.summary, "")
        self.assertEqual(alert.data, {})
        self.assertTrue(t_before <= alert.timestamp <= t_after)
        self.assertTrue(alert.is_blocker)
        self.assertFalse(alert.is_completion)
        self.assertFalse(alert.is_health_anomaly)

    def test_alert_explicit_values(self) -> None:
        """Verify all explicit fields are stored correctly."""
        alert = ShoulderTapAlert(
            alert_type="completion",
            source="runner",
            session_id="C_SLACK:100.200",
            channel="C_SLACK",
            thread_ts="100.200",
            timestamp=123456789.0,
            summary="Command finished with exit code 0",
            data={"pid": 4321, "exit_code": 0, "duration": 4.5},
        )
        self.assertEqual(alert.alert_type, "completion")
        self.assertEqual(alert.source, "runner")
        self.assertEqual(alert.session_id, "C_SLACK:100.200")
        self.assertEqual(alert.channel, "C_SLACK")
        self.assertEqual(alert.thread_ts, "100.200")
        self.assertEqual(alert.timestamp, 123456789.0)
        self.assertEqual(alert.summary, "Command finished with exit code 0")
        self.assertEqual(alert.data["pid"], 4321)
        self.assertTrue(alert.is_completion)
        self.assertFalse(alert.is_blocker)

    def test_alert_allowed_types(self) -> None:
        """Verify all standard alert types are valid."""
        valid_types = ["blocker", "completion", "health_anomaly"]
        for atype in valid_types:
            with self.subTest(atype=atype):
                alert = ShoulderTapAlert(alert_type=atype, source="test")
                self.assertEqual(alert.alert_type, atype)

    def test_alert_to_dict_and_from_dict_roundtrip(self) -> None:
        """Verify serialization and deserialization preserve all alert attributes."""
        alert = ShoulderTapAlert(
            alert_type="health_anomaly",
            source="nose",
            session_id="C_HEALTH:1.0",
            channel="C_HEALTH",
            thread_ts="1.0",
            timestamp=1700000000.0,
            summary="Disk space low on hermes",
            data={"node": "hermes", "metric": "disk", "usage_pct": 94},
        )
        d = alert.to_dict()
        self.assertEqual(d["alert_type"], "health_anomaly")
        self.assertEqual(d["source"], "nose")
        self.assertEqual(d["summary"], "Disk space low on hermes")

        reconstructed = ShoulderTapAlert.from_dict(d)
        self.assertEqual(reconstructed.alert_type, alert.alert_type)
        self.assertEqual(reconstructed.source, alert.source)
        self.assertEqual(reconstructed.data, alert.data)
        self.assertTrue(reconstructed.is_health_anomaly)


class TestShoulderTapEventBusPublishSubscribe(unittest.IsolatedAsyncioTestCase):
    """Unit tests for ShoulderTapEventBus publish and subscribe mechanisms."""

    async def asyncSetUp(self) -> None:
        reset_default_event_bus()
        self.bus = ShoulderTapEventBus()

    async def asyncTearDown(self) -> None:
        self.bus.clear()
        reset_default_event_bus()

    async def test_single_async_subscriber_receives_alert(self) -> None:
        """Verify a single registered async subscriber receives published alerts."""
        received: List[ShoulderTapAlert] = []

        async def handler(alert: ShoulderTapAlert) -> None:
            received.append(alert)

        self.bus.subscribe(handler)

        alert = ShoulderTapAlert(alert_type="blocker", source="eyes", summary="OAuth URL detected")
        self.bus.publish(alert)

        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].summary, "OAuth URL detected")

    async def test_multiple_subscribers_receive_broadcast(self) -> None:
        """Verify multiple subscribers receive identical broadcast alerts."""
        received_1: List[ShoulderTapAlert] = []
        received_2: List[ShoulderTapAlert] = []

        async def handler_1(alert: ShoulderTapAlert) -> None:
            received_1.append(alert)

        async def handler_2(alert: ShoulderTapAlert) -> None:
            received_2.append(alert)

        self.bus.subscribe(handler_1)
        self.bus.subscribe(handler_2)

        alert = ShoulderTapAlert(alert_type="completion", source="runner", summary="Done")
        self.bus.publish(alert)

        await asyncio.sleep(0.05)
        self.assertEqual(len(received_1), 1)
        self.assertEqual(len(received_2), 1)
        self.assertEqual(received_1[0].summary, "Done")
        self.assertEqual(received_2[0].summary, "Done")

    async def test_sync_callback_supported(self) -> None:
        """Verify synchronous callbacks are invoked without breaking the bus."""
        received: List[ShoulderTapAlert] = []

        def sync_handler(alert: ShoulderTapAlert) -> None:
            received.append(alert)

        self.bus.subscribe(sync_handler)
        alert = ShoulderTapAlert(alert_type="blocker", source="eyes")
        self.bus.publish(alert)

        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 1)

    async def test_unsubscribe_removes_subscriber(self) -> None:
        """Verify unsubscribing stops delivery of subsequent alerts."""
        received: List[ShoulderTapAlert] = []

        async def handler(alert: ShoulderTapAlert) -> None:
            received.append(alert)

        unsub = self.bus.subscribe(handler)
        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes", summary="Alert 1"))
        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 1)

        # Unsubscribe callable
        unsub()
        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes", summary="Alert 2"))
        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 1)

    async def test_unsubscribe_method_removes_subscriber(self) -> None:
        """Verify bus.unsubscribe(handler) removes subscriber directly."""
        received: List[ShoulderTapAlert] = []

        def handler(alert: ShoulderTapAlert) -> None:
            received.append(alert)

        self.bus.subscribe(handler)
        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes", summary="Alert 1"))
        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 1)

        self.bus.unsubscribe(handler)
        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes", summary="Alert 2"))
        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 1)

    async def test_publish_without_subscribers_is_noop(self) -> None:
        """Verify publishing an alert when zero subscribers exist does not raise."""
        alert = ShoulderTapAlert(alert_type="completion", source="runner")
        try:
            self.bus.publish(alert)
        except Exception as e:
            self.fail(f"publish() raised an unexpected exception: {e}")


class TestShoulderTapEventBusFiltering(unittest.IsolatedAsyncioTestCase):
    """Unit tests for subscription alert_type filtering."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()

    async def test_filter_single_alert_type(self) -> None:
        """Verify subscriber filtered to 'blocker' ignores other types."""
        blockers_received: List[ShoulderTapAlert] = []

        async def blocker_handler(alert: ShoulderTapAlert) -> None:
            blockers_received.append(alert)

        self.bus.subscribe(blocker_handler, alert_types=["blocker"])

        self.bus.publish(ShoulderTapAlert(alert_type="completion", source="runner", summary="Task finished"))
        self.bus.publish(ShoulderTapAlert(alert_type="health_anomaly", source="nose", summary="Disk full"))
        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes", summary="Need OAuth"))

        await asyncio.sleep(0.05)
        self.assertEqual(len(blockers_received), 1)
        self.assertEqual(blockers_received[0].summary, "Need OAuth")

    async def test_filter_multiple_alert_types(self) -> None:
        """Verify subscriber with multiple alert types receives only those specified."""
        received: List[ShoulderTapAlert] = []

        async def handler(alert: ShoulderTapAlert) -> None:
            received.append(alert)

        self.bus.subscribe(handler, alert_types=["blocker", "completion"])

        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes", summary="B1"))
        self.bus.publish(ShoulderTapAlert(alert_type="health_anomaly", source="nose", summary="N1"))
        self.bus.publish(ShoulderTapAlert(alert_type="completion", source="runner", summary="C1"))

        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 2)
        summaries = [a.summary for a in received]
        self.assertIn("B1", summaries)
        self.assertIn("C1", summaries)
        self.assertNotIn("N1", summaries)

    async def test_wildcard_subscriber_receives_all_types(self) -> None:
        """Verify alert_types=None receives all alert types."""
        received: List[ShoulderTapAlert] = []

        async def handler(alert: ShoulderTapAlert) -> None:
            received.append(alert)

        self.bus.subscribe(handler, alert_types=None)

        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes"))
        self.bus.publish(ShoulderTapAlert(alert_type="completion", source="runner"))
        self.bus.publish(ShoulderTapAlert(alert_type="health_anomaly", source="nose"))

        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 3)


class TestShoulderTapEventBusSessionQueues(unittest.IsolatedAsyncioTestCase):
    """Unit tests for thread/session-scoped event delivery queues."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()

    async def test_get_session_queue_creates_and_returns_queue(self) -> None:
        """Verify get_session_queue returns an asyncio.Queue instance."""
        queue = self.bus.get_session_queue("C1:100.1")
        self.assertIsInstance(queue, asyncio.Queue)

    async def test_get_session_queue_is_idempotent(self) -> None:
        """Verify repeated calls for the same session_id return the same queue instance."""
        queue_1 = self.bus.get_session_queue("C1:100.1")
        queue_2 = self.bus.get_session_queue("C1:100.1")
        self.assertIs(queue_1, queue_2)

    async def test_matching_session_id_alert_routed_to_queue(self) -> None:
        """Verify published alert with session_id is routed to that session's queue."""
        queue = self.bus.get_session_queue("C1:100.1")

        alert = ShoulderTapAlert(
            alert_type="blocker",
            source="eyes",
            session_id="C1:100.1",
            summary="OAuth link found",
        )
        self.bus.publish(alert)

        await asyncio.sleep(0.05)
        self.assertFalse(queue.empty())
        item = queue.get_nowait()
        self.assertEqual(item.summary, "OAuth link found")

    async def test_mismatched_session_id_not_routed_to_queue(self) -> None:
        """Verify alerts for other sessions do not enter this session's queue."""
        queue_a = self.bus.get_session_queue("SESSION_A")
        queue_b = self.bus.get_session_queue("SESSION_B")

        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes", session_id="SESSION_A", summary="For A"))
        await asyncio.sleep(0.05)

        self.assertFalse(queue_a.empty())
        self.assertTrue(queue_b.empty())

    async def test_none_session_id_not_routed_to_session_queue(self) -> None:
        """Verify alerts without a session_id are not pushed into specific session queues."""
        queue = self.bus.get_session_queue("SESSION_A")
        self.bus.publish(ShoulderTapAlert(alert_type="health_anomaly", source="nose", session_id=None, summary="Fleet alert"))

        await asyncio.sleep(0.05)
        self.assertTrue(queue.empty())

    async def test_remove_session_queue(self) -> None:
        """Verify removing a session queue evicts it cleanly."""
        queue = self.bus.get_session_queue("SESSION_A")
        self.bus.remove_session_queue("SESSION_A")
        # Next call produces a brand new queue
        queue_new = self.bus.get_session_queue("SESSION_A")
        self.assertIsNot(queue, queue_new)

    async def test_queue_overflow_handling(self) -> None:
        """Verify oldest alerts are dropped on queue overflow without raising."""
        bus = ShoulderTapEventBus(queue_maxsize=2)
        q = bus.get_session_queue("overflow_sess")

        a1 = ShoulderTapAlert(alert_type="blocker", source="eyes", session_id="overflow_sess", summary="1")
        a2 = ShoulderTapAlert(alert_type="blocker", source="eyes", session_id="overflow_sess", summary="2")
        a3 = ShoulderTapAlert(alert_type="blocker", source="eyes", session_id="overflow_sess", summary="3")

        bus.publish(a1)
        bus.publish(a2)
        bus.publish(a3)

        item1 = await q.get()
        item2 = await q.get()
        self.assertEqual(item1.summary, "2")
        self.assertEqual(item2.summary, "3")

    async def test_has_session_queue(self) -> None:
        """Verify has_session_queue accurately reflects presence of queue."""
        self.assertFalse(self.bus.has_session_queue("SESSION_Z"))
        self.bus.get_session_queue("SESSION_Z")
        self.assertTrue(self.bus.has_session_queue("SESSION_Z"))
        self.bus.remove_session_queue("SESSION_Z")
        self.assertFalse(self.bus.has_session_queue("SESSION_Z"))

    async def test_peek_default_event_bus(self) -> None:
        """Verify peek_default_event_bus returns None when uninstantiated and bus instance when created."""
        reset_default_event_bus()
        self.assertIsNone(peek_default_event_bus())
        bus = get_default_event_bus()
        self.assertIs(peek_default_event_bus(), bus)
        reset_default_event_bus()
        self.assertIsNone(peek_default_event_bus())

    async def test_session_handle_close_removes_queue_from_bus(self) -> None:
        """Verify closing a SessionHandle evicts its queue from ShoulderTapEventBus."""
        sess_id = "C_CLEANUP:1.0"
        self.bus.get_session_queue(sess_id)
        self.assertTrue(self.bus.has_session_queue(sess_id))

        sess = SessionHandle(session_id=sess_id, channel="C_CLEANUP", thread_ts="1.0", adapter=None, bus=self.bus)
        await sess.close()
        self.assertFalse(self.bus.has_session_queue(sess_id))

    async def test_session_router_teardown_removes_queue_from_bus(self) -> None:
        """Verify router.teardown and router.teardown_session evict queues from ShoulderTapEventBus."""
        router = SessionRouter(default_target_cwd="/tmp", adapter_factory=lambda cwd: None, bus=self.bus)
        sess = await router.get_or_create_session(channel="C_TEARDOWN", thread_ts="2.0")
        self.bus.get_session_queue(sess.session_id)
        self.assertTrue(self.bus.has_session_queue(sess.session_id))

        await router.teardown("C_TEARDOWN", "2.0")
        self.assertFalse(self.bus.has_session_queue(sess.session_id))

        # Also test teardown_session directly
        sess2 = await router.get_or_create_session(channel="C_TEARDOWN2", thread_ts="3.0")
        self.bus.get_session_queue(sess2.session_id)
        self.assertTrue(self.bus.has_session_queue(sess2.session_id))
        await router.teardown_session(sess2.session_id)
        self.assertFalse(self.bus.has_session_queue(sess2.session_id))


class TestShoulderTapEventBusResilience(unittest.IsolatedAsyncioTestCase):
    """Unit tests for bus resilience under subscriber errors and delays."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()

    async def test_failing_subscriber_does_not_affect_other_subscribers(self) -> None:
        """Verify an exception raised by one subscriber does not prevent others from receiving alerts."""
        received: List[ShoulderTapAlert] = []

        async def bad_subscriber(alert: ShoulderTapAlert) -> None:
            raise RuntimeError("Subscriber crashed unexpectedly")

        async def good_subscriber(alert: ShoulderTapAlert) -> None:
            received.append(alert)

        self.bus.subscribe(bad_subscriber)
        self.bus.subscribe(good_subscriber)

        alert = ShoulderTapAlert(alert_type="blocker", source="eyes", summary="Critical alert")
        self.bus.publish(alert)

        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].summary, "Critical alert")

    async def test_failing_subscriber_does_not_affect_session_queue(self) -> None:
        """Verify a failing subscriber does not prevent session queue delivery."""
        queue = self.bus.get_session_queue("SESSION_X")

        async def failing_subscriber(alert: ShoulderTapAlert) -> None:
            raise ValueError("Failure in callback")

        self.bus.subscribe(failing_subscriber)

        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes", session_id="SESSION_X", summary="Queued alert"))
        await asyncio.sleep(0.05)

        self.assertFalse(queue.empty())
        item = queue.get_nowait()
        self.assertEqual(item.summary, "Queued alert")

    async def test_slow_subscriber_does_not_block_publish(self) -> None:
        """Verify publish() returns immediately even if subscriber performs slow async work."""
        async def slow_subscriber(alert: ShoulderTapAlert) -> None:
            await asyncio.sleep(1.0)

        self.bus.subscribe(slow_subscriber)

        t_start = time.time()
        self.bus.publish(ShoulderTapAlert(alert_type="blocker", source="eyes"))
        elapsed = time.time() - t_start

        self.assertLess(elapsed, 0.25)


if __name__ == "__main__":
    unittest.main()
