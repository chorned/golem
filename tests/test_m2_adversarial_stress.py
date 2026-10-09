#!/usr/bin/env python3
"""
tests/test_m2_adversarial_stress.py
Adversarial Stress Test Suite for Milestone 2: Live Backstage SSE Stream Wiring & Event Bridge.

Author: teamwork_preview_challenger_m2_2

Adversarial Challenge Matrix:
1. Concurrency & High-Frequency Stream Flooding:
   - Rapid bursts of 50 concurrent SSE connections on /events and /api/stream.
   - Abrupt client disconnects (TCP RST via SO_LINGER and mid-stream client cancellation).
   - Slow consumer queue overflow (QueueFull oldest-drop semantics).
   - Clean port release and zero EADDRINUSE leak across rapid stop/restart cycles.
2. Multi-Threaded Alert Publishing over ShoulderTapEventBus:
   - High-velocity alert publishing across 15 concurrent OS threads.
   - Cross-loop thread-safety verification (loop.call_soon_threadsafe) to WebUI SSE queues.
   - Session queue overflow handling and zero queue corruption under heavy multi-threaded contention.
   - Zero event loop starvation on the main loop.
3. Dual Sync/Awaitable Polymorphic API Under Concurrency:
   - register_active_hand, update_hand_buffer, unregister_active_hand, broadcast behavior.
   - Verification of ZERO RuntimeWarnings when discarded without awaiting.
   - Compatibility with asyncio.gather, asyncio.wait_for, and repeated awaits.
   - Simultaneous concurrent mutations across threads and coroutines on shared panes.
4. Hand Registration & Buffer Edge Conditions:
   - Hand registration with pid=None, pid=0, pid=-1, string PID.
   - Unannounced pane IDs with auto-registration on update_hand_buffer.
   - High-frequency buffer delta flooding (1,000 rapid deltas) capped at 4,096 chars.
   - Huge single delta buffer truncation (500,000 chars) capped at 4,096 chars.
   - Idempotent unregistration of unknown or already-settled panes.
5. IngressGateway Backstage Task Stress & Resiliency:
   - Abrupt cancellation of backstage task while streaming deltas, verifying guaranteed settlement and cleanup.
   - High-velocity TextDeltaEvent streaming through _execute_backstage_task.
   - Edge-case payloads in _broadcast_mouth_briefing (empty, unicode, model=None).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import time
import socket
import struct
import asyncio
import warnings
import threading
import unittest
from typing import Dict, Any, Optional, List, Tuple
from unittest.mock import patch, MagicMock

import aiohttp
from aiohttp import web, ClientSession

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.agent.protocol import (
    TextDeltaEvent,
    StatusUpdateEvent,
    FinishEvent,
    AgentEvent,
)
from golem.gateway.router import SessionRouter
from golem.gateway.session import SessionHandle
from golem.gateway.ingress import IngressGateway
from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
    get_default_event_bus,
    reset_default_event_bus,
)
from golem.webui.server import (
    WebUIServer,
    _SyncAndAwaitableNone,
    format_sse_event,
)


def _get_free_port() -> int:
    """Acquire a temporary available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestRapidSSEConnectionsAndAbruptDisconnects(unittest.IsolatedAsyncioTestCase):
    """Stress-test SSE endpoints with high client concurrency and violent disconnects."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    async def test_01_rapid_burst_connections_both_endpoints(self) -> None:
        """Connect 40 concurrent clients across /events and /api/stream; verify handshakes and immediate hand state."""
        # Pre-register a hand so initial state is populated
        self.server.register_active_hand("pre_hand_01", command="pytest", pid=1001)

        endpoints = ["/events", "/api/stream"]
        connected_count = 0
        received_hand_status = 0

        async with aiohttp.ClientSession() as session:
            async def connect_client(cid: int, ep: str) -> None:
                nonlocal connected_count, received_hand_status
                try:
                    async with session.get(f"http://127.0.0.1:{self.port}{ep}") as resp:
                        self.assertEqual(resp.status, 200)
                        # Read initial handshake event
                        line1 = await asyncio.wait_for(resp.content.readline(), timeout=3.0)
                        if b"event: connected" in line1:
                            connected_count += 1
                        # Read hands_status event
                        line2 = await asyncio.wait_for(resp.content.readline(), timeout=3.0)
                        # Read until hands_status or timeout
                        for _ in range(5):
                            line = await asyncio.wait_for(resp.content.readline(), timeout=1.0)
                            if b"event: hands_status" in line:
                                received_hand_status += 1
                                break
                except Exception:
                    pass

            tasks = [
                asyncio.create_task(connect_client(i, endpoints[i % 2]))
                for i in range(40)
            ]
            await asyncio.gather(*tasks)

        self.assertEqual(connected_count, 40)
        self.assertEqual(received_hand_status, 40)
        # Verify subscribers purged on client exit
        self.server.broadcast("heartbeat", {"ping": "flush"})
        await asyncio.sleep(0.05)
        self.assertEqual(self.server.subscriber_count, 0)

    async def test_02_abrupt_client_disconnects_mid_stream(self) -> None:
        """30 clients connect; 15 disconnect abruptly mid-broadcast without reading EOF while 15 remain."""
        received_events: Dict[int, int] = {}
        abrupt_disconnects = 0

        async with aiohttp.ClientSession() as session:
            async def worker(cid: int) -> None:
                nonlocal abrupt_disconnects
                received_events[cid] = 0
                try:
                    async with session.get(f"http://127.0.0.1:{self.port}/api/stream") as resp:
                        while True:
                            line = await resp.content.readline()
                            if not line:
                                break
                            if b"event:" in line:
                                received_events[cid] += 1
                                # Disconnect abruptly after 3 events if odd client
                                if cid % 2 == 1 and received_events[cid] >= 3:
                                    abrupt_disconnects += 1
                                    return
                except Exception:
                    pass

            client_tasks = [asyncio.create_task(worker(i)) for i in range(30)]
            for _ in range(60):
                if self.server.subscriber_count == 30:
                    break
                await asyncio.sleep(0.05)
            self.assertEqual(self.server.subscriber_count, 30)

            # Broadcast 20 high-velocity events
            for k in range(20):
                self.server.broadcast("hand_buffer", {"pane_id": "p1", "delta": f"chunk_{k}\n"})
                await asyncio.sleep(0.01)

            # Wait for disconnects to take effect
            await asyncio.sleep(0.1)

            # Cancel surviving clients
            for t in client_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*client_tasks, return_exceptions=True)

            self.assertEqual(abrupt_disconnects, 15)
            # Flush server subscriber tracking
            self.server.broadcast("heartbeat", {})
            await asyncio.sleep(0.05)
            self.assertEqual(self.server.subscriber_count, 0)

    async def test_03_raw_tcp_rst_abort(self) -> None:
        """Simulate violent network termination via raw socket SO_LINGER (TCP RST)."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.port))
        s.sendall(b"GET /events HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n\r\n")

        # Give server time to register connection
        await asyncio.sleep(0.05)
        self.assertEqual(self.server.subscriber_count, 1)

        # Force immediate hard reset (RST packet instead of clean FIN)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        s.close()

        # Server broadcasts into severed connection
        for i in range(5):
            self.server.broadcast("test_rst", {"msg": f"rst_probe_{i}"})
            await asyncio.sleep(0.01)

        await asyncio.sleep(0.05)
        # Server should drop subscriber cleanly without crashing
        self.assertEqual(self.server.subscriber_count, 0)
        self.assertTrue(self.server.is_running)

    async def test_04_slow_consumer_queue_overflow_drop_oldest(self) -> None:
        """When client queue reaches maxsize (1000), oldest message is dropped and server does not block."""
        # Manually register a mock queue without a reader
        slow_queue: asyncio.Queue = asyncio.Queue(maxsize=10)
        self.server._subscribers.add(slow_queue)

        # Broadcast 25 events (more than 10 capacity)
        for i in range(25):
            self.server.broadcast("overflow_test", {"seq": i})

        # Queue should be capped at maxsize without blocking broadcast
        self.assertEqual(slow_queue.qsize(), 10)

        # Read what remains in queue; newest items should be present
        items = []
        while not slow_queue.empty():
            items.append(slow_queue.get_nowait())

        # The last item should contain seq: 24
        self.assertIn('"seq": 24', items[-1])
        self.server._subscribers.discard(slow_queue)

    async def test_05_clean_server_restart_zero_port_leak(self) -> None:
        """Stop and restart WebUIServer on the exact same port; verify zero EADDRINUSE collisions."""
        self.assertTrue(self.server.is_running)
        port = self.port
        await self.server.stop()
        self.assertFalse(self.server.is_running)

        # Re-initialize on same port
        new_server = WebUIServer(host="127.0.0.1", port=port)
        await new_server.start()
        self.assertTrue(new_server.is_running)
        self.assertEqual(new_server.subscriber_count, 0)
        await new_server.stop()


class TestMultiThreadedEventBusPublishing(unittest.IsolatedAsyncioTestCase):
    """Stress-test ShoulderTapEventBus with high-velocity multi-threaded publishing and WebUI bridging."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.bus = ShoulderTapEventBus(queue_maxsize=100)
        self.server = WebUIServer(host="127.0.0.1", port=self.port, event_bus=self.bus)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()
        self.bus.clear()

    async def test_01_concurrent_multithreaded_publish_to_webui_bridge(self) -> None:
        """15 concurrent OS threads publish 450 alerts; verify thread-safe WebUI bridging and zero starvation."""
        num_threads = 15
        alerts_per_thread = 30
        total_alerts = num_threads * alerts_per_thread

        def publisher_thread(tid: int) -> None:
            for j in range(alerts_per_thread):
                kind = ["eye_log", "nose_alert", "mouth_briefing"][j % 3]
                src = ["eyes", "nose", "mouth"][j % 3]
                alert = ShoulderTapAlert(
                    source=src,
                    alert_type=kind,
                    summary=f"thread_{tid}_alert_{j}",
                    data={"tid": tid, "seq": j, "smells_off": False},
                )
                self.bus.publish(alert)

        # Run publishers in parallel threads
        threads = [threading.Thread(target=publisher_thread, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Allow server loop to drain scheduled call_soon_threadsafe calls
        await asyncio.sleep(0.3)

        # Verify recent logs ring buffer received alerts from all organs
        self.assertGreater(len(self.server._recent_logs["eyes"]), 0)
        self.assertGreater(len(self.server._recent_logs["nose"]), 0)
        self.assertGreater(len(self.server._recent_logs["mouth"]), 0)
        # Ring buffers must respect max_entries limit
        self.assertLessEqual(len(self.server._recent_logs["eyes"]), 50)
        self.assertLessEqual(len(self.server._recent_logs["nose"]), 50)
        self.assertLessEqual(len(self.server._recent_logs["mouth"]), 50)

    async def test_02_session_queue_multithreaded_overflow_protection(self) -> None:
        """20 threads concurrently flooding a session queue beyond maxsize drops oldest without corruption."""
        q = self.bus.get_session_queue("stress_session_01")
        self.assertEqual(q.maxsize, 100)

        num_threads = 20
        alerts_per_thread = 50

        def flood_thread(tid: int) -> None:
            for i in range(alerts_per_thread):
                self.bus.publish(ShoulderTapAlert(
                    session_id="stress_session_01",
                    summary=f"t{tid}_{i}",
                ))

        threads = [threading.Thread(target=flood_thread, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Queue size must be bounded by queue_maxsize
        self.assertLessEqual(q.qsize(), 100)
        self.assertGreater(q.qsize(), 0)

        # Clean drain
        drained = 0
        while not q.empty():
            item = q.get_nowait()
            self.assertTrue(item.summary.startswith("t"))
            drained += 1
        self.assertEqual(drained, q.maxsize)

    async def test_03_mixed_async_and_sync_bus_subscribers(self) -> None:
        """Simultaneous async coroutines and sync callbacks subscribed to bus receive events without collision."""
        sync_received: List[str] = []
        async_received: List[str] = []

        def sync_callback(alert: ShoulderTapAlert) -> None:
            sync_received.append(alert.summary)

        async def async_callback(alert: ShoulderTapAlert) -> None:
            await asyncio.sleep(0.001)
            async_received.append(alert.summary)

        unsub_sync = self.bus.subscribe(sync_callback, alert_types=["blocker"])
        unsub_async = self.bus.subscribe(async_callback, alert_types=["blocker"])

        for i in range(10):
            self.bus.publish(ShoulderTapAlert(
                alert_type="blocker",
                summary=f"blocker_{i}",
                source="eyes",
            ))

        for _ in range(50):
            if len(async_received) >= 10:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(len(sync_received), 10)
        self.assertEqual(len(async_received), 10)

        unsub_sync()
        unsub_async()


class TestDualSyncAwaitablePolymorphicBehavior(unittest.IsolatedAsyncioTestCase):
    """Stress-test _SyncAndAwaitableNone polymorphic behavior under concurrent callers."""

    async def asyncSetUp(self) -> None:
        self.server = WebUIServer(port=_get_free_port())

    def test_01_sync_discard_zero_runtime_warnings(self) -> None:
        """Calling hand methods and broadcast synchronously emits zero RuntimeWarning when discarded."""
        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")

            # Sync calls without awaiting
            r1 = self.server.register_active_hand("pane_sync", command="ls", pid=101)
            r2 = self.server.update_hand_buffer("pane_sync", "delta output\n")
            r3 = self.server.broadcast("test_event", {"a": 1})
            r4 = self.server.unregister_active_hand("pane_sync")

            # Force garbage collection
            del r1, r2, r3, r4

            runtime_warnings = [w for w in captured_warnings if issubclass(w.category, RuntimeWarning)]
            self.assertEqual(len(runtime_warnings), 0, f"Unexpected RuntimeWarnings: {runtime_warnings}")

    async def test_02_await_all_hand_methods(self) -> None:
        """Awaiting each hand method executes cleanly and evaluates to None."""
        res1 = await self.server.register_active_hand("pane_async", command="pwd", pid=202)
        self.assertIsNone(res1)

        res2 = await self.server.update_hand_buffer("pane_async", "working dir\n")
        self.assertIsNone(res2)

        res3 = await self.server.broadcast("test_event", {"b": 2})
        self.assertIsNone(res3)

        res4 = await self.server.unregister_active_hand("pane_async")
        self.assertIsNone(res4)

    async def test_03_gather_and_wait_for_compatibility(self) -> None:
        """Passing _SyncAndAwaitableNone instances to asyncio.gather and wait_for succeeds."""
        # Test asyncio.gather
        results = await asyncio.gather(
            self.server.register_active_hand("g1", command="c1"),
            self.server.register_active_hand("g2", command="c2"),
            self.server.update_hand_buffer("g1", "d1"),
            self.server.update_hand_buffer("g2", "d2"),
            self.server.broadcast("b1", {}),
        )
        self.assertEqual(results, [None, None, None, None, None])

        # Test asyncio.wait_for
        wait_res = await asyncio.wait_for(
            self.server.unregister_active_hand("g1"),
            timeout=1.0,
        )
        self.assertIsNone(wait_res)

    async def test_04_concurrent_dual_callers_same_panes(self) -> None:
        """Simultaneous threads calling sync methods and async tasks awaiting methods on shared panes."""
        shared_panes = [f"shared_pane_{k}" for k in range(5)]

        def sync_worker(wid: int) -> None:
            for pane in shared_panes:
                self.server.register_active_hand(pane, pid=wid)
                self.server.update_hand_buffer(pane, f"sync_worker_{wid}\n")
                self.server.broadcast("sync_hand_ping", {"pane": pane})

        async def async_worker(aid: int) -> None:
            for pane in shared_panes:
                await self.server.register_active_hand(pane, pid=100 + aid)
                await self.server.update_hand_buffer(pane, f"async_worker_{aid}\n")
                await self.server.broadcast("async_hand_ping", {"pane": pane})

        # Launch 5 sync threads and 5 async tasks simultaneously
        threads = [threading.Thread(target=sync_worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()

        async_tasks = [asyncio.create_task(async_worker(j)) for j in range(5)]
        await asyncio.gather(*async_tasks)

        for t in threads:
            t.join()

        # All 5 panes must remain in registry with populated buffer tails
        hands = self.server.active_hands
        self.assertEqual(len(hands), 5)
        for pane in shared_panes:
            self.assertIn(pane, hands)
            self.assertGreater(len(hands[pane]["buffer_tail"]), 0)


class TestHandRegistrationAndBufferEdgeCases(unittest.IsolatedAsyncioTestCase):
    """Stress-test edge cases in hand registration, PID variations, and buffer tails."""

    async def asyncSetUp(self) -> None:
        self.server = WebUIServer(port=_get_free_port())

    def test_01_register_hand_pid_none_and_edge_values(self) -> None:
        """Registering with pid=None, pid=0, pid=-1, or string pid formats cleanly without error."""
        cases = [
            ("pane_none", None, "PID: N/A"),
            ("pane_zero", 0, "PID: 0"),
            ("pane_neg", -1, "PID: -1"),
            ("pane_str", "9876", "PID: 9876"),
        ]

        for pane, pid_val, expected_str in cases:
            self.server.register_active_hand(pane, command="edge_cmd", pid=pid_val)
            hand = self.server.active_hands[pane]
            self.assertEqual(hand["pid"], pid_val)
            self.assertIn(expected_str, hand["buffer_tail"])

    def test_02_unannounced_pane_id_auto_registration(self) -> None:
        """Calling update_hand_buffer on an unknown pane automatically registers it with defaults."""
        self.server.update_hand_buffer("ghost_pane_404", "sudden output\n")
        hands = self.server.active_hands
        self.assertIn("ghost_pane_404", hands)
        ghost = hands["ghost_pane_404"]
        self.assertEqual(ghost["node"], "localhost")
        self.assertEqual(ghost["command"], "Task")
        self.assertIsNone(ghost["pid"])
        self.assertIn("sudden output\n", ghost["buffer_tail"])

    def test_03_high_frequency_buffer_deltas_capping(self) -> None:
        """Streaming 1,000 tiny deltas rapidly stays strictly capped at 4,096 characters."""
        self.server.register_active_hand("rapid_pane", command="rapid")
        for i in range(1000):
            self.server.update_hand_buffer("rapid_pane", f"{i:04d}\n")

        tail = self.server.active_hands["rapid_pane"]["buffer_tail"]
        self.assertLessEqual(len(tail), 4096)
        # Tail must contain the latest chunks
        self.assertTrue(tail.endswith("0999\n"))

    def test_04_huge_single_buffer_delta(self) -> None:
        """A single 500,000-character delta is truncated to exactly 4,096 characters containing the end."""
        self.server.register_active_hand("huge_pane", command="cat bigfile")
        huge_text = "A" * 499990 + "FINAL_TAIL"
        self.server.update_hand_buffer("huge_pane", huge_text)

        tail = self.server.active_hands["huge_pane"]["buffer_tail"]
        self.assertEqual(len(tail), 4096)
        self.assertTrue(tail.endswith("FINAL_TAIL"))

    def test_05_unregister_nonexistent_and_double_unregister(self) -> None:
        """Unregistering a nonexistent pane or double unregistering is completely idempotent and safe."""
        ret1 = self.server.unregister_active_hand("nonexistent_pane_999")
        self.assertIsInstance(ret1, _SyncAndAwaitableNone)

        self.server.register_active_hand("target_pane")
        self.assertIn("target_pane", self.server.active_hands)
        self.server.unregister_active_hand("target_pane")
        self.assertNotIn("target_pane", self.server.active_hands)

        # Second unregister of same pane
        ret2 = self.server.unregister_active_hand("target_pane")
        self.assertIsInstance(ret2, _SyncAndAwaitableNone)
        self.assertNotIn("target_pane", self.server.active_hands)


class TestIngressGatewayBackstageTaskResilience(unittest.IsolatedAsyncioTestCase):
    """Stress-test IngressGateway backstage task lifecycle, cancellation, and error handling."""

    async def asyncSetUp(self) -> None:
        self.router = SessionRouter(registry_path="/tmp/test_golem_m2_adv_stress_reg.json")
        self.webui = WebUIServer(port=_get_free_port())
        self.gateway = IngressGateway(router=self.router, webui_server=self.webui)
        self.session = await self.router.get_or_create(channel="C_ADV", thread_ts="T_ADV")
        if isinstance(self.session, tuple):
            self.session = self.session[0]

    async def test_01_backstage_task_abrupt_cancellation_settles_and_unregisters(self) -> None:
        """Simulate task cancellation mid-stream: verify finally block executes hand_settle and unregisters hand."""
        async def endless_stream(session, desc):
            yield TextDeltaEvent(text_delta="Starting long build...\n")
            await asyncio.sleep(10.0)
            yield FinishEvent(status="SUCCESS", response="Done")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=endless_stream):
            task = asyncio.create_task(
                self.gateway._execute_backstage_task(
                    task_desc="long build",
                    session=self.session,
                    channel="C_ADV",
                    thread_ts="T_ADV",
                )
            )

            # Let task start and register hand
            await asyncio.sleep(0.05)
            pane = getattr(self.session, "runner_pane", None) or getattr(self.session, "session_id", None) or "runner"
            self.assertIn(pane, self.webui.active_hands)

            # Abruptly cancel task
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

            # Hand must be unregistered from active hands
            self.assertNotIn(pane, self.webui.active_hands)

    async def test_02_backstage_task_with_rapid_deltas_and_bursts(self) -> None:
        """Stream 100 rapid TextDeltaEvents through _execute_backstage_task and verify buffer consistency."""
        async def rapid_stream(session, desc):
            for i in range(100):
                yield TextDeltaEvent(text_delta=f"log_chunk_{i}\n")
            yield FinishEvent(status="SUCCESS", response="")

        with patch.object(self.gateway, "_stream_backstage_events", side_effect=rapid_stream):
            acc = await self.gateway._execute_backstage_task(
                task_desc="rapid deltas",
                session=self.session,
                channel="C_ADV",
                thread_ts="T_ADV",
            )
            self.assertEqual(acc.turn_status, "SUCCESS")
            self.assertEqual(len(acc.text_deltas), 100)
            full_out = acc.get_full_output()
            self.assertIn("log_chunk_0", full_out)
            self.assertIn("log_chunk_99", full_out)

    async def test_03_mouth_briefing_broadcast_edge_cases(self) -> None:
        """Test _broadcast_mouth_briefing with empty, unicode, huge text, and None model."""
        # Empty briefing -> should be ignored safely
        await self.gateway._broadcast_mouth_briefing("")
        await self.gateway._broadcast_mouth_briefing("   ")

        # Multibyte Unicode briefing
        unicode_briefing = "⚡ Chief of Staff: 任務完成！All systems nominal. 🚀"
        await self.gateway._broadcast_mouth_briefing(unicode_briefing, model="gemini-3.8-flash")

        # Huge briefing text
        huge_briefing = "Summary: " + ("x" * 20000)
        await self.gateway._broadcast_mouth_briefing(huge_briefing, model=None)


if __name__ == "__main__":
    unittest.main()
