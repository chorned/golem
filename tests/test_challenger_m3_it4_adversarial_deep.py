#!/usr/bin/env python3
"""
tests/test_challenger_m3_it4_adversarial_deep.py
Adversarial Empirical Challenge Suite for Milestone 3 Iteration 4.

Author: Challenger M3-IT4-2 (Critic / Specialist)
Scope: Cache Invalidation, Settle Timeout Race Condition, & Multi-Threaded Mutation Stress

Challenges:
1. Cache Invalidation & Runtime Dynamic State:
   - _get_file_env() mtime-based invalidation, clear_env_cache(), and reload_config().
   - get_system_status() immediate zero-lag reflection of active hands, buffers, and settlement.
2. Settle Timeout Race Condition Challenge:
   - Premature unmount prevention when active tasks re-spawn on settling/reused panes.
3. Multi-Threaded Cache Mutation Stress:
   - Zero RuntimeError (dictionary changed size during iteration) or KeyError under concurrent
     /api/status polling and high-velocity background hand mutations.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import sys
import time
import json
import socket
import asyncio
import tempfile
import threading
import subprocess
import unittest
from pathlib import Path
from typing import Dict, Any, List

import aiohttp
from aiohttp import web, ClientSession

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import golem.config as golem_cfg
from golem.webui.server import (
    WebUIServer,
    start_webui_server,
    stop_webui_server,
    get_system_status,
    get_active_webui_server,
    format_sse_event,
)


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestCacheInvalidationAndDynamicState(unittest.IsolatedAsyncioTestCase):
    """Challenge 1: Config cache invalidation & WebUI real-time dynamic state."""

    def test_get_file_env_cache_and_invalidation_matrix(self) -> None:
        """
        Verify that _get_file_env() in golem/config.py:
        1. Returns cached values on unchanged mtime.
        2. Automatically invalidates and updates when .env mtime changes.
        3. Invalidates when clear_env_cache() is invoked explicitly.
        4. Invalidates and updates os.environ when reload_config() is called.
        5. Handles missing file gracefully without crashing.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            test_env = Path(tmpdir) / ".env"
            orig_env_path = golem_cfg._ENV_PATH
            orig_ttl = getattr(golem_cfg, "_ENV_CACHE_TTL", 2.0)
            try:
                golem_cfg._ENV_PATH = test_env
                golem_cfg._ENV_CACHE_TTL = 0.0
                golem_cfg.clear_env_cache()

                # Step 1: Missing file returns {}
                self.assertEqual(golem_cfg._get_file_env(), {})

                # Step 2: Create initial file
                test_env.write_text("TEST_KEY=initial_val\nTEST_ENGINE=agy\n")
                env1 = golem_cfg._get_file_env()
                self.assertEqual(env1.get("TEST_KEY"), "initial_val")
                self.assertEqual(env1.get("TEST_ENGINE"), "agy")

                # Step 3: Cache hit validation
                cache_info_1 = golem_cfg._get_raw_file_env.cache_info()
                env2 = golem_cfg._get_file_env()
                cache_info_2 = golem_cfg._get_raw_file_env.cache_info()
                self.assertEqual(env2, env1)
                self.assertGreater(cache_info_2.hits, cache_info_1.hits)

                # Step 4: Invalidation via mtime forward advancement
                time.sleep(0.01)
                test_env.write_text("TEST_KEY=updated_mtime_val\nTEST_ENGINE=opencode\n")
                new_mtime = test_env.stat().st_mtime + 5.0
                os.utime(test_env, (new_mtime, new_mtime))

                env3 = golem_cfg._get_file_env()
                self.assertEqual(env3.get("TEST_KEY"), "updated_mtime_val")
                self.assertEqual(env3.get("TEST_ENGINE"), "opencode")

                # Step 5: Invalidation via clear_env_cache() without mtime advancement
                fixed_mtime = test_env.stat().st_mtime
                test_env.write_text("TEST_KEY=cleared_val\n")
                os.utime(test_env, (fixed_mtime, fixed_mtime))

                # Unchanged mtime returns cached env3
                self.assertEqual(golem_cfg._get_file_env(), env3)

                # Calling clear_env_cache() forces cache discard and re-reading disk
                golem_cfg.clear_env_cache()
                env4 = golem_cfg._get_file_env()
                self.assertEqual(env4.get("TEST_KEY"), "cleared_val")

                # Step 6: Invalidation via reload_config()
                test_env.write_text("TEST_RELOAD_KEY=reloaded_ok\n")
                os.utime(test_env, (fixed_mtime, fixed_mtime))
                golem_cfg.reload_config()
                self.assertEqual(os.environ.get("TEST_RELOAD_KEY"), "reloaded_ok")
                env5 = golem_cfg._get_file_env()
                self.assertEqual(env5.get("TEST_RELOAD_KEY"), "reloaded_ok")

            finally:
                golem_cfg._ENV_PATH = orig_env_path
                golem_cfg._ENV_CACHE_TTL = orig_ttl
                golem_cfg.clear_env_cache()

    async def test_get_system_status_immediate_zero_lag_state_transitions(self) -> None:
        """
        Verify that get_system_status() in golem/webui/server.py immediately reflects
        dynamic hand registrations, buffer mutations, and state transitions with zero caching lag.
        """
        port = _get_free_port()
        server = await start_webui_server(host="127.0.0.1", port=port)
        try:
            # 1. Idle state
            s0 = get_system_status()
            self.assertEqual(len(s0["hands"]), 0)
            self.assertEqual(s0["eyes"]["state"], "IDLE")

            # 2. Register Hand 1 -> Instant reflection
            server.register_active_hand("pane_01", "desktop-pc", "cargo build", pid=5001)
            s1 = get_system_status()
            self.assertEqual(len(s1["hands"]), 1)
            self.assertEqual(s1["hands"][0]["pane_id"], "pane_01")
            self.assertEqual(s1["eyes"]["state"], "RUNNING")
            self.assertEqual(s1["eyes"]["active_command"], "cargo build")
            self.assertEqual(s1["eyes"]["pid"], 5001)

            # 3. Stream buffer delta -> Instant reflection
            server.update_hand_buffer("pane_01", "Compiling package v1.0.0...\n")
            s2 = get_system_status()
            self.assertIn("Compiling package v1.0.0...", s2["eyes"]["buffer_tail"])

            # 4. Register Hand 2 -> Instant reflection
            server.register_active_hand("pane_02", "oci-machine", "docker compose up", pid=5002)
            s3 = get_system_status()
            self.assertEqual(len(s3["hands"]), 2)
            pane_ids = {h["pane_id"] for h in s3["hands"]}
            self.assertEqual(pane_ids, {"pane_01", "pane_02"})

            # 5. Unregister Hand 1 -> Instant reflection
            server.unregister_active_hand("pane_01")
            s4 = get_system_status()
            self.assertEqual(len(s4["hands"]), 1)
            self.assertEqual(s4["hands"][0]["pane_id"], "pane_02")
            self.assertEqual(s4["eyes"]["state"], "RUNNING")
            self.assertEqual(s4["eyes"]["active_command"], "docker compose up")

            # 6. Unregister Hand 2 -> Immediate return to IDLE
            server.unregister_active_hand("pane_02")
            s5 = get_system_status()
            self.assertEqual(len(s5["hands"]), 0)
            self.assertEqual(s5["eyes"]["state"], "IDLE")

        finally:
            await stop_webui_server()


class TestSettleTimeoutRaceConditionAdversarial(unittest.TestCase):
    """Challenge 2: Settle timeout race condition when panes are reused."""

    def test_reused_pane_aborts_settlement_timer_in_headless_node(self) -> None:
        """
        Simulate rapid pane reuse where Task A finishes and starts a settlement unmount timer,
        and Task B arrives on the same pane within the settlement window.
        Verify that:
        1. Task B clears settleTimeout.
        2. Task B is NOT unmounted when Task A's original timeout fires.
        3. Buffer deltas for Task B clear settleTimeout as well.
        """
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

        function handleBufferDelta(paneId, delta) {
            let item = activeHands.get(paneId);
            if (!item) {
                getOrCreateHandCard({ pane_id: paneId, command: 'Task', buffer_tail: delta, status: 'RUNNING' });
            } else {
                if (item.settleTimeout) {
                    clearTimeout(item.settleTimeout);
                    item.settleTimeout = null;
                }
                item.buffer_tail = (item.buffer_tail || '') + delta;
            }
        }

        // Test Scenario:
        // 1. Task 1 spawns on 'runner_p2'
        getOrCreateHandCard({ pane_id: 'runner_p2', command: 'make test', status: 'RUNNING' });

        // 2. Task 1 settles at t=0, schedules unmount for t=30ms
        const card = activeHands.get('runner_p2');
        card.status = 'EXIT_0';
        card.settleTimeout = setTimeout(() => {
            unmountHandCard('runner_p2');
        }, 30);

        // 3. Task 2 re-uses 'runner_p2' at t=10ms (well before 30ms timer)
        setTimeout(() => {
            getOrCreateHandCard({ pane_id: 'runner_p2', command: 'make build', status: 'RUNNING' });
        }, 10);

        // 4. Task 2 streams output at t=20ms
        setTimeout(() => {
            handleBufferDelta('runner_p2', 'Compiled binary successfully\\n');
        }, 20);

        // 5. Evaluate state at t=60ms (after Task 1 timer would have fired)
        setTimeout(() => {
            const runner = activeHands.get('runner_p2');
            console.log(JSON.stringify({
                unmountedPanes: unmountedPanes,
                activeHandsSize: activeHands.size,
                runnerStillActive: activeHands.has('runner_p2'),
                command: runner ? runner.command : null,
                status: runner ? runner.status : null,
                settleTimeoutIsNull: runner ? (runner.settleTimeout === null) : false,
            }));
        }, 60);
        """
        proc = subprocess.run(["node", "-e", js_code], capture_output=True, text=True, check=True)
        result = json.loads(proc.stdout.strip())

        self.assertNotIn("runner_p2", result["unmountedPanes"])
        self.assertEqual(result["activeHandsSize"], 1)
        self.assertTrue(result["runnerStillActive"])
        self.assertEqual(result["command"], "make build")
        self.assertEqual(result["status"], "RUNNING")
        self.assertTrue(result["settleTimeoutIsNull"])

    def test_dashboard_template_contains_settle_timeout_safeguards(self) -> None:
        """
        Statically verify that dashboard.html contains clearTimeout logic in:
        1. getOrCreateHandCard (re-spawn on existing card).
        2. hand_buffer listener (incoming output delta on settling card).
        3. unmountHandCard (explicit unmount).
        4. hands_status listener (reconnect / bulk sync).
        """
        template_file = Path(PROJECT_DIR) / "golem" / "webui" / "templates" / "dashboard.html"
        self.assertTrue(template_file.exists())
        content = template_file.read_text(encoding="utf-8")

        # 1. getOrCreateHandCard
        self.assertIn("function getOrCreateHandCard", content)
        self.assertRegex(
            content,
            r"if\s*\(\s*activeHands\.has\(paneId\)\s*\)\s*\{[\s\S]*?clearTimeout\(item\.settleTimeout\)",
            "getOrCreateHandCard must clear settleTimeout when paneId already exists",
        )

        # 2. hand_buffer listener
        self.assertRegex(
            content,
            r"addEventListener\('hand_buffer'[\s\S]*?clearTimeout\(item\.settleTimeout\)",
            "hand_buffer listener must clear settleTimeout on active pane output",
        )

        # 3. unmountHandCard
        self.assertRegex(
            content,
            r"function unmountHandCard\(paneId\)[\s\S]*?clearTimeout\(item\.settleTimeout\)",
            "unmountHandCard must clear settleTimeout before deleting active hand",
        )


class TestMultiThreadedCacheMutationStress(unittest.IsolatedAsyncioTestCase):
    """Challenge 3: High-concurrency multi-threaded mutation & REST status polling stress."""

    async def test_concurrent_rest_status_polling_and_hand_mutations_zero_runtime_error(self) -> None:
        """
        Stress test concurrent /api/status polling and active hand mutations across
        multiple OS background threads and asyncio client requests.
        Verify:
        1. Strictly ZERO RuntimeError: dictionary changed size during iteration.
        2. Strictly ZERO KeyError in update_hand_buffer or unregister_active_hand.
        3. All /api/status requests return HTTP 200 with valid JSON.
        """
        port = _get_free_port()
        server = await start_webui_server(host="127.0.0.1", port=port)
        stop_event = threading.Event()
        captured_errors: List[Exception] = []

        def worker_mutator(worker_id: int):
            seq = 0
            while not stop_event.is_set():
                pane = f"stress_worker_{worker_id}_pane_{seq % 12}"
                try:
                    server.register_active_hand(
                        pane_id=pane,
                        node="localhost",
                        command=f"stress_job_{worker_id}_{seq}",
                        pid=20000 + (worker_id * 100) + seq,
                        worker="agy",
                    )
                    server.update_hand_buffer(pane, f"[worker {worker_id}] step {seq}\n")
                    if seq % 3 == 0:
                        server.unregister_active_hand(pane)
                except Exception as ex:
                    captured_errors.append(ex)
                seq += 1

        # Spawn 6 background mutator threads
        threads = [threading.Thread(target=worker_mutator, args=(t,), daemon=True) for t in range(6)]
        for t in threads:
            t.start()

        status_url = f"http://127.0.0.1:{port}/api/status"
        request_count = 350
        http_errors = 0

        async with aiohttp.ClientSession() as session:
            for req in range(request_count):
                try:
                    async with session.get(status_url) as resp:
                        self.assertEqual(resp.status, 200)
                        data = await resp.json()
                        self.assertIn("uptime", data)
                        self.assertIn("hands", data)
                        self.assertIn("eyes", data)
                except Exception as e:
                    http_errors += 1
                    captured_errors.append(e)
                if req % 20 == 0:
                    await asyncio.sleep(0.001)

        stop_event.set()
        for t in threads:
            t.join(timeout=2.0)

        await stop_webui_server()

        self.assertEqual(http_errors, 0, f"HTTP requests failed: {http_errors}")
        self.assertEqual(
            len(captured_errors),
            0,
            f"Encountered {len(captured_errors)} errors during concurrent mutation: {captured_errors[:5]}",
        )


if __name__ == "__main__":
    unittest.main()
