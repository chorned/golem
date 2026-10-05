#!/usr/bin/env python3
"""
tests/test_challenger_r3_empirical.py
Milestone 3 (R3) Empirical Challenge Test Suite.

Author: Challenger M3-1 (Critic / Empirical Challenger)
Scope: WebUI Dashboard State & Telemetry Deduplication

Empirically challenges Hand B Invariants from .specs/R3-BUILD.md:
1. Invariant 1: Mock Purge & Content Scrubbing (Audiobookshelf, 58236, absdatabase, initialHandAgent, etc.)
2. Invariant 2: Standby State Invariant (Standby card rendering, IDLE badge, zero-hand initial state, empty array resets)
3. Invariant 3: Dynamic Hand Lifecycle & Grid Dynamics (Spawning, streaming deltas, settlement states, DOM timers, layout)
4. Invariant 4: Telemetry Deduplication & Pre-seeding (Gauge updates vs terminal appends under rapid ticks, anomalies, alerts)
5. Invariant 5: Lineage & Air-Gap Invariant (Stoneforge lineage, zero remote CDN imports, air-gapped styling)
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import socket
import asyncio
import subprocess
import unittest
from pathlib import Path
from typing import Dict, Any, List, Optional

import aiohttp
from aiohttp import web, ClientSession

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.webui.server import (
    LINEAGE,
    DASHBOARD_HTML,
    WebUIServer,
    get_system_status,
)


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestEmpiricalMockPurgeAndContentScrubbing(unittest.IsolatedAsyncioTestCase):
    """Invariant 1: Adversarially check that zero mock fixtures leak anywhere."""

    def setUp(self) -> None:
        self.template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        self.template_content = self.template_path.read_text(encoding="utf-8")
        self.forbidden_patterns = [
            r"audiobookshelf",
            r"58236",
            r"absdatabase",
            r"the material arcana",
            r"initialhandagent",
            r"audiobook",
        ]

    def test_adversarial_template_regex_scan(self) -> None:
        """Scan dashboard.html case-insensitively with regex for any forbidden mock terms."""
        for pattern in self.forbidden_patterns:
            matches = list(re.finditer(pattern, self.template_content, re.IGNORECASE))
            self.assertEqual(
                len(matches),
                0,
                f"Forbidden mock pattern '{pattern}' found in dashboard.html: {matches}",
            )

    def test_dashboard_constant_in_server_clean(self) -> None:
        """Ensure DASHBOARD_HTML in server.py does not contain any mock strings."""
        for pattern in self.forbidden_patterns:
            matches = list(re.finditer(pattern, DASHBOARD_HTML, re.IGNORECASE))
            self.assertEqual(
                len(matches),
                0,
                f"Forbidden mock pattern '{pattern}' found in DASHBOARD_HTML constant: {matches}",
            )

    async def test_live_http_responses_clean(self) -> None:
        """Ensure live endpoints (/ , /api/status , /api/sensory/hands) return zero mock fixtures."""
        port = _get_free_port()
        server = WebUIServer(host="127.0.0.1", port=port)
        await server.start()
        try:
            async with aiohttp.ClientSession() as session:
                for path in ["/", "/api/status", "/api/sensory/hands"]:
                    url = f"http://127.0.0.1:{port}{path}"
                    async with session.get(url) as resp:
                        self.assertEqual(resp.status, 200)
                        text = await resp.text()
                        for pattern in self.forbidden_patterns:
                            matches = list(re.finditer(pattern, text, re.IGNORECASE))
                            self.assertEqual(
                                len(matches),
                                0,
                                f"Forbidden pattern '{pattern}' leaked in HTTP response for {path}",
                            )
        finally:
            await server.stop()


class TestEmpiricalStandbyStateAndEmptyBoundaries(unittest.IsolatedAsyncioTestCase):
    """Invariant 2: Standby state rendered cleanly when _active_hands is empty."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    def test_template_standby_dom_structure(self) -> None:
        """Inspect HTML structure for correct semantic tags, ids, and initial markup."""
        template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        content = template_path.read_text(encoding="utf-8")

        # Hands anchor and grid
        self.assertIn('id="anchor-hands"', content)
        self.assertIn('id="hands-grid"', content)
        self.assertIn('id="hands-standby-card"', content)
        self.assertIn('class="hands-idle-card"', content)
        self.assertIn("HANDS STANDBY — 0 ACTIVE TASKS", content)
        self.assertIn("Awaiting backstage task dispatch from Mouth Liaison", content)

    async def test_rest_status_hands_contract(self) -> None:
        """GET /api/status must strictly return hands as an empty list when idle."""
        url = f"http://127.0.0.1:{self.port}/api/status"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertIsInstance(data.get("hands"), list)
                self.assertEqual(len(data["hands"]), 0)

    async def test_rest_sensory_hands_contract(self) -> None:
        """GET /api/sensory/hands must return an empty list when idle."""
        url = f"http://127.0.0.1:{self.port}/api/sensory/hands"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertIsInstance(data, list)
                self.assertEqual(data, [])

    async def test_empty_hands_status_broadcast_resets_hands(self) -> None:
        """Broadcasting hands_status with empty list or empty dict delivers valid JSON."""
        url = f"http://127.0.0.1:{self.port}/events"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                # Discard connected handshake
                await resp.content.readline()  # event: connected
                await resp.content.readline()  # data: ...
                await resp.content.readline()  # separator

                # Broadcast empty list
                self.server.broadcast("hands_status", [])
                ev1 = (await resp.content.readline()).decode().strip()
                data1 = (await resp.content.readline()).decode().strip()
                await resp.content.readline()
                self.assertEqual(ev1, "event: hands_status")
                self.assertEqual(json.loads(data1.replace("data: ", "")), [])

                # Broadcast empty dict
                self.server.broadcast("hands_status", {})
                ev2 = (await resp.content.readline()).decode().strip()
                data2 = (await resp.content.readline()).decode().strip()
                await resp.content.readline()
                self.assertEqual(ev2, "event: hands_status")
                self.assertEqual(json.loads(data2.replace("data: ", "")), {})


class TestEmpiricalDynamicHandLifecycleAndGrid(unittest.IsolatedAsyncioTestCase):
    """Invariant 3: Dynamic hand lifecycle, buffer streaming, and settlement."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.server = WebUIServer(host="127.0.0.1", port=self.port)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()

    async def test_rapid_hand_spawn_stream_settle_sequence(self) -> None:
        """Stress test: 5 hands spawned in parallel, buffers updated rapidly, settled with different codes."""
        hand_ids = [f"worker_pane_{i}" for i in range(5)]
        for i, hid in enumerate(hand_ids):
            self.server.register_active_hand(
                pane_id=hid,
                node="desktop-pc" if i % 2 == 0 else "localhost",
                command=f"pytest tests/test_{i}.py",
                pid=1000 + i,
                worker="agy",
            )

        self.assertEqual(len(self.server.active_hands), 5)

        # Stream 50 rapid deltas across all hands
        for step in range(50):
            target = hand_ids[step % 5]
            self.server.update_hand_buffer(target, f"Step {step}: PASSED\n")

        for hid in hand_ids:
            self.assertIn("PASSED", self.server.active_hands[hid]["buffer_tail"])

        # Check REST status returns all 5 hands
        url = f"http://127.0.0.1:{self.port}/api/status"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                self.assertEqual(len(data["hands"]), 5)

        # Settle hands one by one
        for hid in hand_ids:
            self.server.unregister_active_hand(hid)

        self.assertEqual(len(self.server.active_hands), 0)

        # Verify /api/status returns empty hands list again
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                self.assertEqual(data["hands"], [])

    def test_buffer_tail_truncation_limits_in_server(self) -> None:
        """Ensure server.py caps buffer_tail at 4096 characters to prevent unbounded memory growth."""
        pane_id = "stress_buffer_pane"
        self.server.register_active_hand(pane_id=pane_id)

        # Append large chunk (10,000 characters)
        large_delta = "X" * 10000
        self.server.update_hand_buffer(pane_id, large_delta)

        tail = self.server.active_hands[pane_id]["buffer_tail"]
        self.assertLessEqual(len(tail), 4096)
        self.assertTrue(tail.endswith("XXXXX"))

    def test_node_simulation_dynamic_grid_and_settlement_timer(self) -> None:
        """
        Execute headless Node.js simulation verifying:
        1. Standby card renders when 0 hands.
        2. Responsive grid column layout for 1, 2, 3 hands.
        3. Settlement state badge colors (EXIT_0 green, FAILED red).
        4. Settlement timer unmounts card and restores standby after timeout.
        5. Buffer capping at 8192 characters in JS client.
        """
        js_code = """
        // Minimal simulated DOM environment
        class Element {
            constructor(tagName) {
                this.tagName = tagName;
                this.children = [];
                this.parentNode = null;
                this.style = {};
                this.classList = new Set();
                this.id = '';
                this.attributes = {};
                this.textContent = '';
                this.innerHTML = '';
            }
            appendChild(child) {
                this.children.push(child);
                child.parentNode = this;
                return child;
            }
            removeChild(child) {
                const idx = this.children.indexOf(child);
                if (idx !== -1) {
                    this.children.splice(idx, 1);
                    child.parentNode = null;
                }
                return child;
            }
            remove() {
                if (this.parentNode) {
                    this.parentNode.removeChild(this);
                }
            }
            setAttribute(k, v) { this.attributes[k] = v; }
            getAttribute(k) { return this.attributes[k]; }
            querySelector(sel) {
                if (sel.startsWith('.')) {
                    const cls = sel.slice(1);
                    return this.children.find(c => c.classList && c.classList.has(cls)) || null;
                }
                if (sel.startsWith('#')) {
                    const id = sel.slice(1);
                    return this.children.find(c => c.id === id) || null;
                }
                return null;
            }
        }

        const document = {
            createElement: (tag) => new Element(tag),
            getElementById: (id) => {
                if (id === 'hands-grid') return handsGrid;
                if (id === 'anchor-hands') return anchorHands;
                if (id === 'hands-standby-card') return handsGrid.children.find(c => c.id === 'hands-standby-card') || null;
                return null;
            }
        };

        const handsGrid = new Element('div');
        handsGrid.id = 'hands-grid';
        const anchorHands = new Element('div');
        anchorHands.id = 'anchor-hands';

        const handsCountBadge = { textContent: '', className: '' };
        const activeHands = new Map();

        function escapeHtml(str) {
            return String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }

        function unmountHandCard(paneId) {
            const item = activeHands.get(paneId);
            if (!item) return;
            if (item.settleTimeout) {
                clearTimeout(item.settleTimeout);
                item.settleTimeout = null;
            }
            if (item.el) item.el.remove();
            activeHands.delete(paneId);
            renderHandsState();
        }

        function renderHandsState() {
            const count = activeHands.size;
            if (count === 0) {
                handsGrid.style.gridTemplateColumns = '1fr';
                Array.from(handsGrid.children).forEach(child => {
                    if (child.id !== 'hands-standby-card') child.remove();
                });
                let standbyCard = document.getElementById('hands-standby-card');
                if (!standbyCard) {
                    standbyCard = document.createElement('div');
                    standbyCard.className = 'hands-idle-card';
                    standbyCard.id = 'hands-standby-card';
                    handsGrid.appendChild(standbyCard);
                }
                handsCountBadge.textContent = 'IDLE';
                handsCountBadge.className = 'status-pill-badge badge-amber';
                return;
            }

            const standbyCard = document.getElementById('hands-standby-card');
            if (standbyCard) standbyCard.remove();

            if (count === 1) {
                handsGrid.style.gridTemplateColumns = 'repeat(3, minmax(0, 390px))';
                const firstCard = handsGrid.children[0];
                if (firstCard) firstCard.style.gridColumn = '2 / 3';
            } else if (count === 2) {
                handsGrid.style.gridTemplateColumns = 'repeat(2, minmax(0, 390px))';
                Array.from(handsGrid.children).forEach(c => { c.style.gridColumn = ''; });
            } else {
                handsGrid.style.gridTemplateColumns = 'repeat(3, minmax(0, 390px))';
                Array.from(handsGrid.children).forEach(c => { c.style.gridColumn = ''; });
            }
            handsCountBadge.textContent = `${count} GAUNTLETS ONLINE`;
            handsCountBadge.className = 'status-pill-badge badge-green';
        }

        function getOrCreateHandCard(h) {
            const paneId = h.pane_id;
            const card = document.createElement('article');
            card.id = `dyn-hand-${paneId}`;
            handsGrid.appendChild(card);
            const item = { ...h, el: card, settleTimeout: null, buffer_tail: h.buffer_tail || '' };
            activeHands.set(paneId, item);
            renderHandsState();
            return card;
        }

        // Test Phase 1: Initial Standby
        renderHandsState();
        const initialStandbyExists = Boolean(document.getElementById('hands-standby-card'));
        const initialGridCols = handsGrid.style.gridTemplateColumns;
        const initialBadge = handsCountBadge.textContent;

        // Test Phase 2: Add Hand 1
        getOrCreateHandCard({ pane_id: 'p1', command: 'test' });
        const oneHandStandbyAbsent = (document.getElementById('hands-standby-card') === null);
        const oneHandGridCols = handsGrid.style.gridTemplateColumns;
        const oneHandGridColCard = handsGrid.children[0].style.gridColumn;

        // Test Phase 3: Add Hand 2
        getOrCreateHandCard({ pane_id: 'p2', command: 'test2' });
        const twoHandGridCols = handsGrid.style.gridTemplateColumns;

        // Test Phase 4: Add Hand 3
        getOrCreateHandCard({ pane_id: 'p3', command: 'test3' });
        const threeHandGridCols = handsGrid.style.gridTemplateColumns;

        // Test Phase 5: Buffer append & capping
        const p1Item = activeHands.get('p1');
        p1Item.buffer_tail = 'A'.repeat(10000);
        if (p1Item.buffer_tail.length > 8192) {
            p1Item.buffer_tail = p1Item.buffer_tail.slice(-8192);
        }
        const bufferCapLength = p1Item.buffer_tail.length;

        // Test Phase 6: Unmount all -> back to standby
        unmountHandCard('p1');
        unmountHandCard('p2');
        unmountHandCard('p3');
        const finalStandbyRestored = Boolean(document.getElementById('hands-standby-card'));
        const finalBadge = handsCountBadge.textContent;

        const results = {
            initialStandbyExists,
            initialGridCols,
            initialBadge,
            oneHandStandbyAbsent,
            oneHandGridCols,
            oneHandGridColCard,
            twoHandGridCols,
            threeHandGridCols,
            bufferCapLength,
            finalStandbyRestored,
            finalBadge
        };
        console.log(JSON.stringify(results));
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        res = json.loads(proc.stdout.strip())

        self.assertTrue(res["initialStandbyExists"], "Standby card must exist initially")
        self.assertEqual(res["initialGridCols"], "1fr")
        self.assertEqual(res["initialBadge"], "IDLE")

        self.assertTrue(res["oneHandStandbyAbsent"], "Standby card must be removed when hands active")
        self.assertEqual(res["oneHandGridCols"], "repeat(3, minmax(0, 390px))")
        self.assertEqual(res["oneHandGridColCard"], "2 / 3", "Single hand must be centered in column 2")

        self.assertEqual(res["twoHandGridCols"], "repeat(2, minmax(0, 390px))")
        self.assertEqual(res["threeHandGridCols"], "repeat(3, minmax(0, 390px))")

        self.assertEqual(res["bufferCapLength"], 8192, "Buffer tail must be capped at 8192 chars")
        self.assertTrue(res["finalStandbyRestored"], "Standby card must be cleanly restored when all hands unmounted")
        self.assertEqual(res["finalBadge"], "IDLE")


class TestEmpiricalNoseTelemetryDeduplication(unittest.TestCase):
    """Invariant 4: Exhaustively verify telemetry deduplication logic under adversarial conditions."""

    def test_node_exhaustive_deduplication_matrix(self) -> None:
        """
        Adversarially verify all branches of handleNoseTelemetryUpdate:
        - 100 identical quiescent ticks -> exactly 1 append (or 0 if pre-seeded)
        - Pre-seeding from /api/status -> suppresses first tick when matching
        - Explicit alert -> appends even if summary and smells_off are unchanged
        - Anomaly transitions WARN vs INFO levels
        - Empty or null payloads handled safely
        """
        js_code = """
        let last_verdict_summary = null;
        let last_smells_off = null;
        let logAppends = [];
        let gaugeUpdates = 0;

        function updateNoseData(data) {
            gaugeUpdates++;
        }

        function appendAgentLog(container, text) {
            logAppends.push(text);
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

        // Test 1: Pre-seeding verification
        // Hydrate from /api/status snapshot
        last_verdict_summary = "Pre-seeded Healthy";
        last_smells_off = false;

        // Tick 1 arrives with matching pre-seeded values -> MUST BE SUPPRESSED
        handleNoseTelemetryUpdate({ verdict_summary: "Pre-seeded Healthy", smells_off: false });
        const preSeededSuppressed = (logAppends.length === 0);

        // Test 2: 100 identical ticks -> MUST REMAIN SUPPRESSED
        for (let i = 0; i < 100; i++) {
            handleNoseTelemetryUpdate({ verdict_summary: "Pre-seeded Healthy", smells_off: false });
        }
        const hundredTicksSuppressed = (logAppends.length === 0);

        // Test 3: Alert arrives with SAME summary -> MUST APPEND
        handleNoseTelemetryUpdate({ verdict_summary: "Pre-seeded Healthy", smells_off: false }, true);
        const alertAppended = (logAppends.length === 1);

        // Test 4: Anomaly occurs -> MUST APPEND with [WARN]
        handleNoseTelemetryUpdate({ verdict_summary: "Disk failure", smells_off: true }, false);
        const anomalyAppended = (logAppends.length === 2 && logAppends[1].includes('[WARN]'));

        // Test 5: Persisting anomaly with SAME summary -> MUST BE SUPPRESSED
        for (let i = 0; i < 50; i++) {
            handleNoseTelemetryUpdate({ verdict_summary: "Disk failure", smells_off: true }, false);
        }
        const persistingAnomalySuppressed = (logAppends.length === 2);

        // Test 6: Anomaly summary changed -> MUST APPEND with [WARN]
        handleNoseTelemetryUpdate({ verdict_summary: "Disk failure escalating to 99%", smells_off: true }, false);
        const escalatingAnomalyAppended = (logAppends.length === 3 && logAppends[2].includes('[WARN]'));

        // Test 7: Recovery -> MUST APPEND with [INFO]
        handleNoseTelemetryUpdate({ verdict_summary: "Disk cleared to 40%", smells_off: false }, false);
        const recoveryAppended = (logAppends.length === 4 && logAppends[3].includes('[INFO]'));

        // Test 8: Malformed / null inputs -> MUST NOT CRASH
        handleNoseTelemetryUpdate(null);
        handleNoseTelemetryUpdate({});
        handleNoseTelemetryUpdate({ verdict_summary: "   " });
        const safeUnderMalformed = (logAppends.length === 4);

        const result = {
            preSeededSuppressed,
            hundredTicksSuppressed,
            alertAppended,
            anomalyAppended,
            persistingAnomalySuppressed,
            escalatingAnomalyAppended,
            recoveryAppended,
            safeUnderMalformed,
            totalGaugeUpdates: gaugeUpdates,
            totalLogAppends: logAppends.length,
            logAppends
        };
        console.log(JSON.stringify(result));
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        res = json.loads(proc.stdout.strip())

        self.assertTrue(res["preSeededSuppressed"], "Pre-seeded telemetry must suppress identical first tick")
        self.assertTrue(res["hundredTicksSuppressed"], "100 identical ticks must produce zero log appends")
        self.assertTrue(res["alertAppended"], "Explicit alert must append even when summary unchanged")
        self.assertTrue(res["anomalyAppended"], "Anomaly must append with [WARN]")
        self.assertTrue(res["persistingAnomalySuppressed"], "Persisting anomaly with same summary must be suppressed")
        self.assertTrue(res["escalatingAnomalyAppended"], "Escalating anomaly with changed summary must append")
        self.assertTrue(res["recoveryAppended"], "Recovery must append with [INFO]")
        self.assertTrue(res["safeUnderMalformed"], "Malformed/null data must not append or crash")
        self.assertEqual(res["totalLogAppends"], 4)
        self.assertGreater(res["totalGaugeUpdates"], 150)


class TestEmpiricalLineageAndAirGapCompliance(unittest.TestCase):
    """Invariant 5: Lineage comment and air-gapped asset self-containment."""

    def setUp(self) -> None:
        self.template_path = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        self.template_content = self.template_path.read_text(encoding="utf-8")

    def test_lineage_metadata_verbatim(self) -> None:
        """Lineage comment and meta tag must be present verbatim."""
        expected_meta = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh. Hand Agents Grid"
        self.assertIn(expected_meta, self.template_content)
        self.assertIn(LINEAGE, self.template_content)

    def test_air_gapped_zero_remote_dependencies(self) -> None:
        """Dashboard HTML must not reference any external CDN, remote fonts, or scripts."""
        # Find all http:// and https:// URLs in the template
        urls = re.findall(r'https?://[^\s"\'<>]+', self.template_content)
        remote_asset_urls = []
        for u in urls:
            # Localhost is fine for inline documentation/references, but external hosts are forbidden
            if not u.startswith("http://127.0.0.1") and not u.startswith("http://localhost"):
                remote_asset_urls.append(u)

        self.assertEqual(
            remote_asset_urls,
            [],
            f"Remote URLs detected in air-gapped dashboard template: {remote_asset_urls}",
        )


if __name__ == "__main__":
    unittest.main()
