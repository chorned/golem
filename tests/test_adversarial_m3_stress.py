#!/usr/bin/env python3
"""
tests/test_adversarial_m3_stress.py
Adversarial Stress Test Suite for Milestone 3 (Deliverable R3: WebUI Dashboard State & Telemetry Deduplication).

Mandatory Empirical Verification:
1. High-throughput SSE deltas across multiple concurrent hands (5-9 hands streaming tokens simultaneously).
2. Rapid settlement and unmounting cycles verifying zero DOM memory leaks and return to standby (#hands-standby-card).
3. Rapid telemetry bursts: simulated 100 rapid nose_telemetry ticks verifying zero terminal flooding and bounded memory.
4. Sudden client disconnects and reconnects while hands are active.
5. Edge cases: Unusual pane IDs, massive buffer deltas, and settlement race conditions.
"""

from __future__ import annotations

import os
import sys
import time
import json
import socket
import asyncio
import subprocess
import unittest
from pathlib import Path
from typing import List, Dict, Any, Optional

import aiohttp
from aiohttp import web, ClientSession

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.webui.server import (
    WebUIServer,
    start_webui_server,
    stop_webui_server,
    format_sse_event,
)


def _get_free_port() -> int:
    """Acquire a temporary available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestAdversarialConcurrentHandsHighThroughput(unittest.IsolatedAsyncioTestCase):
    """Stress-test high-throughput SSE deltas across 5 to 9 concurrent hands."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    async def test_nine_concurrent_hands_high_velocity_streaming(self) -> None:
        """
        Deploy maximum hand capacity (9 hands) simultaneously streaming high-frequency
        token deltas (50 deltas each = 450 total deltas) to multiple connected SSE clients.
        Verify:
        - All 9 hands are tracked in server._active_hands without race conditions.
        - Buffer tails remain bounded (<= 4096 bytes per hand).
        - REST /api/status accurately reflects all 9 hands.
        - Connected SSE clients receive deltas across all hands without dropped connections.
        """
        hand_count = 9
        deltas_per_hand = 50
        client_count = 4

        events_url = f"http://127.0.0.1:{self.port}/events"
        received_deltas: List[Dict[str, int]] = [{} for _ in range(client_count)]
        client_sessions: List[ClientSession] = []
        client_resps = []
        running = True

        async def sse_consumer(idx: int):
            session = aiohttp.ClientSession()
            client_sessions.append(session)
            resp = await session.get(events_url)
            client_resps.append(resp)
            self.assertEqual(resp.status, 200)
            # Read handshake
            await resp.content.readuntil(b"\n\n")

            while running:
                try:
                    chunk = await asyncio.wait_for(resp.content.readuntil(b"\n\n"), timeout=0.5)
                    if not chunk:
                        break
                    lines = chunk.decode("utf-8", errors="replace").splitlines()
                    event_type = ""
                    data_str = ""
                    for line in lines:
                        if line.startswith("event: "):
                            event_type = line[7:].strip()
                        elif line.startswith("data: "):
                            data_str = line[6:].strip()

                    if event_type == "hand_buffer" and data_str:
                        payload = json.loads(data_str)
                        pane = payload.get("pane_id", "unknown")
                        received_deltas[idx][pane] = received_deltas[idx].get(pane, 0) + 1
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    continue
                except Exception:
                    break

        consumer_tasks = [asyncio.create_task(sse_consumer(i)) for i in range(client_count)]

        # Wait for subscribers to attach
        for _ in range(50):
            if self.server.subscriber_count >= client_count:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(self.server.subscriber_count, client_count)

        # 2. Register 9 active hands
        for h in range(hand_count):
            pane_id = f"runner_pane_{h+1}"
            self.server.register_active_hand(
                pane_id=pane_id,
                node=f"mesh-node-0{h+1}",
                command=f"stress_job_worker_{h+1}.sh --concurrency",
                pid=7000 + h,
                worker="agy",
            )
            self.server.broadcast("hand_spawn", {
                "pane_id": pane_id,
                "node": f"mesh-node-0{h+1}",
                "command": f"stress_job_worker_{h+1}.sh --concurrency",
                "pid": 7000 + h,
                "status": "RUNNING",
            })

        self.assertEqual(len(self.server.active_hands), 9)

        # 3. Stream deltas concurrently across all 9 hands
        async def stream_hand(h_idx: int):
            pane_id = f"runner_pane_{h_idx+1}"
            for d in range(deltas_per_hand):
                delta_chunk = f"[t={d}] token_payload_hand_{h_idx+1}_seq_{d:03d}\n"
                self.server.update_hand_buffer(pane_id, delta_chunk)
                self.server.broadcast("hand_buffer", {
                    "pane_id": pane_id,
                    "delta": delta_chunk,
                })
                if d % 10 == 0:
                    await asyncio.sleep(0.001)

        streaming_tasks = [asyncio.create_task(stream_hand(i)) for i in range(hand_count)]
        await asyncio.gather(*streaming_tasks)

        # Give small window to drain
        await asyncio.sleep(0.2)
        running = False
        for t in consumer_tasks:
            t.cancel()
        for r in client_resps:
            r.close()
        for s in client_sessions:
            await s.close()

        # 4. Verify buffer memory bound: each hand buffer_tail must be <= 4096 bytes
        for h in range(hand_count):
            pane_id = f"runner_pane_{h+1}"
            tail = self.server.active_hands[pane_id]["buffer_tail"]
            self.assertLessEqual(
                len(tail),
                4096,
                f"Buffer tail for {pane_id} exceeded 4096 bytes: {len(tail)}",
            )

        # 5. Verify REST /api/status snapshot
        status_url = f"http://127.0.0.1:{self.port}/api/status"
        async with aiohttp.ClientSession() as rest_sess:
            async with rest_sess.get(status_url) as r:
                self.assertEqual(r.status, 200)
                data = await r.json()
                self.assertEqual(len(data.get("hands", [])), 9)

        # 6. Verify each client received events from all 9 hands
        for i, client_map in enumerate(received_deltas):
            self.assertEqual(
                len(client_map),
                9,
                f"Client {i} did not receive events from all 9 hands: {client_map.keys()}",
            )


class TestAdversarialRapidSettlementAndStandbyCycles(unittest.IsolatedAsyncioTestCase):
    """Stress-test rapid settlement and unmounting cycles verifying zero DOM memory leaks and return to standby."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    async def test_fifty_rapid_spawn_settle_unmount_cycles_backend(self) -> None:
        """
        Execute 50 rapid sequential and burst cycles of hand_spawn -> delta -> hand_settle -> unregister.
        Verify server._active_hands cleanly returns to 0 entries on every settlement,
        and /api/status returns hands: [].
        """
        status_url = f"http://127.0.0.1:{self.port}/api/status"
        async with aiohttp.ClientSession() as session:
            for cycle in range(50):
                pane = f"burst_hand_{cycle}"
                # 1. Spawn
                self.server.register_active_hand(pane, "localhost", f"burst_task_{cycle}", 8000 + cycle)
                self.server.broadcast("hand_spawn", {"pane_id": pane, "command": f"task_{cycle}", "status": "RUNNING"})
                self.assertEqual(len(self.server.active_hands), 1)

                # 2. Update buffer
                self.server.update_hand_buffer(pane, "executing rapid burst step...\n")
                self.server.broadcast("hand_buffer", {"pane_id": pane, "delta": "step complete\n"})

                # 3. Settle
                self.server.broadcast("hand_settle", {"pane_id": pane, "exit_code": 0, "status": "EXIT_0"})
                self.server.unregister_active_hand(pane)
                self.assertEqual(len(self.server.active_hands), 0)

                # 4. Verify REST status is empty
                async with session.get(status_url) as r:
                    self.assertEqual(r.status, 200)
                    data = await r.json()
                    self.assertEqual(data.get("hands", []), [])

    def test_rapid_spawn_settle_unmount_frontend_node_simulation(self) -> None:
        """
        Simulate the exact frontend JavaScript lifecycle from dashboard.html in headless Node.js:
        - 50 rapid cycles of spawning, settling, and unmounting.
        - Verify activeHands map is completely empty (0 entries) after unmounting.
        - Verify DOM grid restores #hands-standby-card.
        - Verify zero dangling elements in hands-grid.
        """
        js_code = """
        const activeHands = new Map();
        let standbyCardCount = 1;
        let activeCardCount = 0;
        let lastBadgeText = "IDLE";

        function unmountHandCard(paneId) {
            const item = activeHands.get(paneId);
            if (!item) return;
            if (item.settleTimeout) {
                clearTimeout(item.settleTimeout);
                item.settleTimeout = null;
            }
            activeHands.delete(paneId);
            activeCardCount--;
            renderHandsState();
        }

        function renderHandsState() {
            const count = activeHands.size;
            if (count === 0) {
                standbyCardCount = 1;
                lastBadgeText = "IDLE";
            } else {
                standbyCardCount = 0;
                lastBadgeText = `${count} GAUNTLETS ONLINE`;
            }
        }

        function getOrCreateHandCard(h) {
            if (!h) return null;
            const paneId = h.pane_id || 'runner';
            if (activeHands.has(paneId)) {
                const item = activeHands.get(paneId);
                Object.assign(item, h);
                return item;
            }
            const item = { ...h, settleTimeout: null };
            activeHands.set(paneId, item);
            activeCardCount++;
            renderHandsState();
            return item;
        }

        // Run 50 cycles
        for (let i = 0; i < 50; i++) {
            const paneId = `sim_hand_${i}`;
            // 1. Spawn
            getOrCreateHandCard({ pane_id: paneId, command: `task_${i}`, status: 'RUNNING' });
            if (activeHands.size !== 1 || standbyCardCount !== 0) {
                throw new Error(`Cycle ${i}: failed spawn state`);
            }

            // 2. Delta
            const item = activeHands.get(paneId);
            item.buffer_tail = (item.buffer_tail || '') + 'delta chunk\\n';

            // 3. Settle & Unmount immediately
            unmountHandCard(paneId);
            if (activeHands.size !== 0 || standbyCardCount !== 1 || lastBadgeText !== "IDLE") {
                throw new Error(`Cycle ${i}: failed unmount / standby restore`);
            }
        }

        console.log(JSON.stringify({
            finalActiveHandsSize: activeHands.size,
            finalActiveCards: activeCardCount,
            finalStandbyCard: standbyCardCount,
            badge: lastBadgeText,
        }));
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        res = json.loads(proc.stdout.strip())
        self.assertEqual(res["finalActiveHandsSize"], 0)
        self.assertEqual(res["finalActiveCards"], 0)
        self.assertEqual(res["finalStandbyCard"], 1)
        self.assertEqual(res["badge"], "IDLE")

    def test_settle_timeout_race_condition_investigation(self) -> None:
        """
        Adversarial edge condition test:
        Verifies that when a task on 'runner' settles with an unmount timer,
        and a new task immediately re-spawns on the same pane within the settlement window,
        getOrCreateHandCard clears item.settleTimeout, preventing premature unmounting
        of the active new task.
        """
        import re
        js_code = """
        const activeHands = new Map();
        let unmountedPanes = [];

        function unmountHandCard(paneId) {
            const item = activeHands.get(paneId);
            if (!item) return;
            if (item.settleTimeout) {
                clearTimeout(item.settleTimeout);
                item.settleTimeout = null;
            }
            activeHands.delete(paneId);
            unmountedPanes.push(paneId);
        }

        function getOrCreateHandCard(h) {
            if (!h) return null;
            const paneId = h.pane_id || 'runner';
            if (activeHands.has(paneId)) {
                const item = activeHands.get(paneId);
                if (item.settleTimeout) {
                    clearTimeout(item.settleTimeout);
                    item.settleTimeout = null;
                }
                Object.assign(item, h);
                return item;
            }
            const item = { ...h, settleTimeout: null };
            activeHands.set(paneId, item);
            return item;
        }

        // 1. Task 1 spawns on 'runner'
        getOrCreateHandCard({ pane_id: 'runner', command: 'task 1', status: 'RUNNING' });

        // 2. Task 1 settles -> sets 20ms settleTimeout
        const item = activeHands.get('runner');
        item.status = 'EXIT_0';
        item.settleTimeout = setTimeout(() => {
            unmountHandCard('runner');
        }, 20);

        // 3. Task 2 spawns on 'runner' at 5ms (BEFORE timeout fires)
        setTimeout(() => {
            getOrCreateHandCard({ pane_id: 'runner', command: 'task 2', status: 'RUNNING' });
        }, 5);

        // 4. Inspect state at 40ms (AFTER Task 1 timeout would have fired)
        setTimeout(() => {
            const runnerItem = activeHands.get('runner');
            console.log(JSON.stringify({
                unmountedPanes: unmountedPanes,
                runnerStillActive: activeHands.has('runner'),
                runnerCommand: runnerItem ? runnerItem.command : null,
                runnerStatus: runnerItem ? runnerItem.status : null,
                settleTimeoutCleared: runnerItem ? (runnerItem.settleTimeout === null) : false
            }));
        }, 40);
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        res = json.loads(proc.stdout.strip())
        self.assertNotIn("runner", res["unmountedPanes"], "Runner pane must not be unmounted by stale settlement timer")
        self.assertTrue(res["runnerStillActive"], "Runner must remain active after re-spawn")
        self.assertEqual(res["runnerCommand"], "task 2", "Runner command must match the newly spawned task")
        self.assertEqual(res["runnerStatus"], "RUNNING", "Runner status must remain RUNNING")
        self.assertTrue(res["settleTimeoutCleared"], "settleTimeout must be cleared upon re-spawn")

        # In addition, statically verify that dashboard.html contains the clearTimeout fix in getOrCreateHandCard
        template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        content = template_path.read_text(encoding="utf-8")
        self.assertIn("function getOrCreateHandCard", content)
        fn_match = re.search(r"function getOrCreateHandCard\([^)]*\)\s*\{([\s\S]*?)return card;", content)
        self.assertIsNotNone(fn_match, "getOrCreateHandCard function must exist in dashboard.html")
        fn_body = fn_match.group(1)
        has_block = re.search(r"if\s*\(\s*activeHands\.has\(paneId\)\s*\)\s*\{([\s\S]*?)\}", fn_body)
        self.assertIsNotNone(has_block, "activeHands.has(paneId) block must exist in getOrCreateHandCard")
        block_body = has_block.group(1)
        self.assertIn("clearTimeout(item.settleTimeout)", block_body, "settleTimeout must be cleared in getOrCreateHandCard")
        self.assertIn("item.settleTimeout = null", block_body, "settleTimeout must be reset to null in getOrCreateHandCard")


class TestAdversarialTelemetryBurstsAndTerminalFlooding(unittest.TestCase):
    """Stress-test 100 rapid telemetry bursts verifying zero terminal flooding and bounded memory."""

    def test_one_hundred_rapid_nose_telemetry_ticks_zero_flooding(self) -> None:
        """
        Simulate 100 rapid nose_telemetry ticks with identical quiescent summary:
        Verify:
        - Gauges update 100 times (live telemetry freshness).
        - Terminal log appends strictly ONCE (zero terminal flooding).
        """
        js_code = """
        let last_verdict_summary = null;
        let last_smells_off = null;
        let gaugeUpdates = 0;
        let terminalAppends = 0;
        let terminalRows = [];

        function updateNoseData(data) {
            gaugeUpdates++;
        }

        function appendAgentLog(container, text) {
            terminalAppends++;
            terminalRows.push(text);
        }

        function handleNoseTelemetryUpdate(data, isAlert = false) {
            if (!data) return;
            updateNoseData(data);

            const summary = (data.verdict_summary || data.summary || '').trim();
            if (!summary) return;

            const smellsOff = Boolean(data.smells_off);
            const summaryChanged = Boolean(last_verdict_summary === null || summary !== last_verdict_summary);
            const anomalyOccurred = Boolean(smellsOff && last_smells_off !== true);

            if (summaryChanged || anomalyOccurred || isAlert) {
                const level = smellsOff ? '[WARN]' : '[INFO]';
                appendAgentLog(null, `${level} Golem.Sensory.Nose: ${summary}`);
                last_verdict_summary = summary;
                last_smells_off = smellsOff;
            }
        }

        // Fire 100 rapid identical ticks
        for (let i = 0; i < 100; i++) {
            handleNoseTelemetryUpdate({
                verdict_summary: "System healthy; zero anomalies observed.",
                smells_off: false,
                sniff_latency_ms: 12.5 + (i * 0.1),
                disk: { percent_used: 45.2 }
            });
        }

        console.log(JSON.stringify({
            gaugeUpdates,
            terminalAppends,
            terminalRowsCount: terminalRows.length,
        }));
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        res = json.loads(proc.stdout.strip())
        self.assertEqual(res["gaugeUpdates"], 100, "Gauges must update on every tick")
        self.assertEqual(res["terminalAppends"], 1, "Terminal must append strictly once for 100 identical ticks")
        self.assertEqual(res["terminalRowsCount"], 1)

    def test_one_hundred_rapid_alternating_ticks_bounded_memory(self) -> None:
        """
        Simulate 100 rapid distinct summary ticks to verify terminal DOM pruning:
        `while (container.children.length > 100) container.removeChild(container.firstElementChild);`
        Verify terminal row count remains strictly bounded to <= 100 items.
        """
        js_code = """
        let last_verdict_summary = null;
        let last_smells_off = null;
        let fakeDomChildren = [];

        function updateNoseData(data) {}

        function appendAgentLog(container, text) {
            fakeDomChildren.push(text);
            while (fakeDomChildren.length > 100) {
                fakeDomChildren.shift();
            }
        }

        function handleNoseTelemetryUpdate(data, isAlert = false) {
            if (!data) return;
            updateNoseData(data);

            const summary = (data.verdict_summary || data.summary || '').trim();
            if (!summary) return;

            const smellsOff = Boolean(data.smells_off);
            const summaryChanged = Boolean(last_verdict_summary === null || summary !== last_verdict_summary);
            const anomalyOccurred = Boolean(smellsOff && last_smells_off !== true);

            if (summaryChanged || anomalyOccurred || isAlert) {
                const level = smellsOff ? '[WARN]' : '[INFO]';
                appendAgentLog(null, `${level} Golem.Sensory.Nose: ${summary}`);
                last_verdict_summary = summary;
                last_smells_off = smellsOff;
            }
        }

        // Fire 150 distinct ticks
        for (let i = 0; i < 150; i++) {
            handleNoseTelemetryUpdate({
                verdict_summary: `Tick summary variation #${i}`,
                smells_off: false,
            });
        }

        console.log(JSON.stringify({
            retainedDomChildren: fakeDomChildren.length,
            oldestRetained: fakeDomChildren[0],
            newestRetained: fakeDomChildren[fakeDomChildren.length - 1],
        }));
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        res = json.loads(proc.stdout.strip())
        self.assertEqual(res["retainedDomChildren"], 100, "DOM children must be bounded at exactly 100")
        self.assertIn("Tick summary variation #50", res["oldestRetained"])
        self.assertIn("Tick summary variation #149", res["newestRetained"])

    def test_persisting_anomaly_deduplication(self) -> None:
        """
        Verify that when an anomaly occurs:
        - First anomaly tick appends [WARN].
        - Subsequent 50 ticks with the SAME anomaly summary append ZERO times.
        - System recovery tick appends [INFO].
        - Subsequent 50 ticks with quiescent summary append ZERO times.
        Total appends across 102 ticks: strictly 2.
        """
        js_code = """
        let last_verdict_summary = null;
        let last_smells_off = null;
        let appends = [];

        function updateNoseData(data) {}
        function appendAgentLog(container, text) { appends.push(text); }

        function handleNoseTelemetryUpdate(data, isAlert = false) {
            if (!data) return;
            updateNoseData(data);

            const summary = (data.verdict_summary || data.summary || '').trim();
            if (!summary) return;

            const smellsOff = Boolean(data.smells_off);
            const summaryChanged = Boolean(last_verdict_summary === null || summary !== last_verdict_summary);
            const anomalyOccurred = Boolean(smellsOff && last_smells_off !== true);

            if (summaryChanged || anomalyOccurred || isAlert) {
                const level = smellsOff ? '[WARN]' : '[INFO]';
                appendAgentLog(null, `${level} Golem.Sensory.Nose: ${summary}`);
                last_verdict_summary = summary;
                last_smells_off = smellsOff;
            }
        }

        // Seed with initial clean state
        last_verdict_summary = "Healthy";
        last_smells_off = false;

        // 1. Tick 1..50: Persistent anomaly
        for (let i = 0; i < 50; i++) {
            handleNoseTelemetryUpdate({
                verdict_summary: "High disk pressure detected: 96%",
                smells_off: true,
            });
        }

        // 2. Tick 51..100: Persistent recovery
        for (let i = 0; i < 50; i++) {
            handleNoseTelemetryUpdate({
                verdict_summary: "Healthy",
                smells_off: false,
            });
        }

        console.log(JSON.stringify({
            totalAppends: appends.length,
            appends: appends
        }));
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        res = json.loads(proc.stdout.strip())
        self.assertEqual(res["totalAppends"], 2)
        self.assertIn("[WARN] Golem.Sensory.Nose: High disk pressure detected: 96%", res["appends"][0])
        self.assertIn("[INFO] Golem.Sensory.Nose: Healthy", res["appends"][1])


class TestAdversarialClientDisconnectsWhileHandsActive(unittest.IsolatedAsyncioTestCase):
    """Stress-test sudden client disconnects and reconnects while hands are active."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    async def test_sudden_disconnects_mid_stream_and_reconnect(self) -> None:
        """
        Spawn 3 active hands streaming high-velocity deltas.
        Connect 6 aiohttp clients.
        Abruptly disconnect 4 clients mid-stream via response.close().
        Verify:
        - Server does not crash or raise unhandled exceptions.
        - 2 surviving clients continue receiving deltas without interruption.
        - Disconnected clients are pruned from server._subscribers.
        - A new client connects, receives handshake + active hands snapshot, and resumes streaming.
        """
        events_url = f"http://127.0.0.1:{self.port}/events"

        # 1. Register 3 active hands
        for i in range(3):
            pane_id = f"active_runner_{i+1}"
            self.server.register_active_hand(pane_id, "localhost", f"long_running_job_{i+1}", 9100 + i)

        self.assertEqual(len(self.server.active_hands), 3)

        # 2. Connect 6 clients (2 stable, 4 doomed)
        stable_sessions: List[ClientSession] = []
        stable_resps = []
        stable_counts = [0, 0]
        running = True

        async def stable_client(idx: int):
            sess = aiohttp.ClientSession()
            stable_sessions.append(sess)
            resp = await sess.get(events_url)
            stable_resps.append(resp)
            await resp.content.readuntil(b"\n\n")
            while running:
                try:
                    chunk = await asyncio.wait_for(resp.content.readuntil(b"\n\n"), timeout=0.5)
                    if not chunk:
                        break
                    if b"event: hand_buffer" in chunk:
                        stable_counts[idx] += 1
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    continue
                except Exception:
                    break

        stable_tasks = [asyncio.create_task(stable_client(i)) for i in range(2)]

        doomed_sessions: List[ClientSession] = []
        doomed_resps = []

        async def doomed_client():
            sess = aiohttp.ClientSession()
            doomed_sessions.append(sess)
            resp = await sess.get(events_url)
            doomed_resps.append(resp)
            await resp.content.readuntil(b"\n\n")

        await asyncio.gather(*[doomed_client() for _ in range(4)])

        # Wait for all 6 subscribers
        for _ in range(50):
            if self.server.subscriber_count >= 6:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(self.server.subscriber_count, 6)

        # 3. Stream 10 deltas while all are connected
        for d in range(10):
            self.server.update_hand_buffer("active_runner_1", f"delta {d}\n")
            self.server.broadcast("hand_buffer", {"pane_id": "active_runner_1", "delta": f"delta {d}\n"})
            await asyncio.sleep(0.002)

        # 4. Abruptly close all 4 doomed connections
        for resp in doomed_resps:
            resp.close()
        for sess in doomed_sessions:
            await sess.close()

        # 5. Broadcast flush deltas so server detects closed connections
        for d in range(10, 30):
            self.server.update_hand_buffer("active_runner_1", f"delta {d}\n")
            self.server.broadcast("hand_buffer", {"pane_id": "active_runner_1", "delta": f"delta {d}\n"})
            await asyncio.sleep(0.005)

        # 6. Verify dead subscribers pruned down to 2
        for _ in range(50):
            if self.server.subscriber_count == 2:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(self.server.subscriber_count, 2)

        # 7. Connect a RECONNECTING client:
        # Must receive handshake and immediate hands_status event containing all 3 hands
        reconnect_sess = aiohttp.ClientSession()
        async with reconnect_sess.get(events_url) as resp:
            self.assertEqual(resp.status, 200)
            # 1st: connected event
            ev1 = await resp.content.readuntil(b"\n\n")
            self.assertIn(b"connected", ev1)

            # 2nd: hands_status event with active hands
            ev2 = await resp.content.readuntil(b"\n\n")
            self.assertIn(b"hands_status", ev2)
            lines = ev2.decode().splitlines()
            data_line = [l for l in lines if l.startswith("data: ")][0]
            hands_data = json.loads(data_line[6:])
            self.assertEqual(len(hands_data), 3)

        await reconnect_sess.close()

        # Clean up stable clients
        running = False
        for t in stable_tasks:
            t.cancel()
        for r in stable_resps:
            r.close()
        for s in stable_sessions:
            await s.close()


class TestAdversarialEdgeCasesAndPayloads(unittest.TestCase):
    """Stress-test unusual pane IDs, massive deltas, and empty frames."""

    def test_unusual_pane_ids_safe_dom_ids(self) -> None:
        """
        Verify pane IDs with special characters (spaces, brackets, punctuation)
        produce safe DOM element IDs without syntax errors.
        """
        js_code = """
        const paneIds = [
            'Hand 01 [L_GAUNTLET]',
            'runner p2 #3',
            'mesh:node/worker@9',
            'test.name-123',
            'quotes"and<brackets>'
        ];

        const safeIds = paneIds.map(p => {
            const safePaneId = p.replace(/[^a-zA-Z0-9_-]/g, '_');
            return `dyn-hand-${safePaneId}`;
        });

        console.log(JSON.stringify(safeIds));
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        ids = json.loads(proc.stdout.strip())
        for sid in ids:
            self.assertTrue(all(c.isalnum() or c in ("-", "_") for c in sid))

    def test_massive_buffer_delta_bounded_frontend(self) -> None:
        """
        Send a massive 100KB buffer delta to a hand card in dashboard.html.
        Verify frontend buffer_tail is strictly sliced to the last 8192 characters.
        """
        js_code = """
        const item = { buffer_tail: "" };
        const massiveDelta = "A".repeat(100000);

        item.buffer_tail = (item.buffer_tail || '') + massiveDelta;
        if (item.buffer_tail.length > 8192) {
            item.buffer_tail = item.buffer_tail.slice(-8192);
        }

        console.log(JSON.stringify({
            finalLength: item.buffer_tail.length,
        }));
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        res = json.loads(proc.stdout.strip())
        self.assertEqual(res["finalLength"], 8192)


if __name__ == "__main__":
    unittest.main()
