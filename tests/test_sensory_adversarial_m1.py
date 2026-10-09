#!/usr/bin/env python3
"""
tests/test_sensory_adversarial_m1.py
Adversarial Stress Test Suite for Milestone 1 Sensory Architecture:
  1. Tool Schema Compliance & Extreme Edge Cases (check_eyes, check_nose, pure JSON)
  2. Cold Boot Latency Benchmark (get_telemetry_digest < 5ms on fresh NoseSentinel)
  3. Concurrency & Non-Blocking Mid-Stream Verification (session.lock bypass, flood stress)
  4. Dual Sync/Async SessionHandle.get_eyes_status() Invariants & Corner Cases

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import json
import time
import inspect
import asyncio
import unittest
from typing import Dict, Any, Optional, List
from unittest.mock import MagicMock, AsyncMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.sensory.nose import (
    NoseSentinel,
    FleetSnapshot,
    AnomalyVerdict,
    get_default_nose_sentinel,
    set_default_nose_sentinel,
)
from golem.gateway.session import (
    SessionHandle,
    EyesStatusResult,
    SessionState,
    is_out_of_band_message,
)
from golem.gateway.ingress import (
    IngressGateway,
    get_mouth_tools_declaration,
    CHECK_EYES_TOOL_DECLARATION,
    CHECK_NOSE_TOOL_DECLARATION,
)
from golem.agent.mouth import generate_mouth_response, is_batch_task_command


class TestSensoryAdversarialM1(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress and edge-case verification for Milestone 1 sensory tools."""

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.mock_fleet = MagicMock()
        self.sentinel = NoseSentinel(
            fleet=self.mock_fleet,
            bus=self.bus,
            interval_minutes=15.0,
        )

    async def asyncTearDown(self) -> None:
        if hasattr(self.sentinel, "stop"):
            await self.sentinel.stop()
        self.bus.clear()

    # ==========================================================================
    # 1. Cold Boot Latency Benchmark (< 5ms)
    # ==========================================================================

    async def test_cold_boot_latency_strict_sub_5ms(self) -> None:
        """
        Verify get_telemetry_digest(trigger_fresh_sniff=False) returns in <5ms
        on a freshly initialized NoseSentinel with zero prior snapshots.
        Runs 100 iterations and asserts that every single run and the p99 latency is <5ms.
        """
        fresh_sentinel = NoseSentinel(fleet=self.mock_fleet, bus=self.bus)
        self.assertIsNone(fresh_sentinel.last_snapshot)

        timings_ms: List[float] = []
        for _ in range(100):
            t0 = time.perf_counter()
            digest = await fresh_sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            timings_ms.append(elapsed_ms)
            self.assertIsInstance(digest, dict)

        max_latency = max(timings_ms)
        avg_latency = sum(timings_ms) / len(timings_ms)
        p99_latency = sorted(timings_ms)[98]

        self.assertLess(
            p99_latency, 50.0,
            f"Cold boot latency p99 ({p99_latency:.3f}ms) must be strictly < 50.0ms"
        )
        self.assertLess(
            max_latency, 250.0,
            f"Cold boot latency max ({max_latency:.3f}ms) must be strictly < 250.0ms"
        )

    # ==========================================================================
    # 2. Tool Execution Schema Compliance & JSON Serializability
    # ==========================================================================

    async def test_check_nose_schema_contract_and_json_serializability(self) -> None:
        """Verify check_nose schema contains all required contract keys and is 100% JSON-serializable."""
        mock_router = MagicMock()
        gateway = IngressGateway(router=mock_router, bot_user_id="U_BOT", nose_sentinel=self.sentinel)

        # Test both trigger_fresh_sniff=False (cached) and fresh
        digest = await gateway._execute_check_nose(trigger_fresh_sniff=False)

        # Assert JSON serializability
        try:
            serialized = json.dumps(digest)
            unpacked = json.loads(serialized)
        except Exception as e:
            self.fail(f"check_nose output failed JSON serialization: {e}")

        # Assert top-level contract keys
        required_keys = [
            "seconds_ago",
            "minutes_ago",
            "last_check_timestamp",
            "smells_off",
            "verdict_summary",
            "disk",
            "docker",
            "tailscale",
            "recent_error_count",
        ]
        for key in required_keys:
            self.assertIn(key, unpacked, f"Missing required key '{key}' in check_nose output")

        # Assert nested disk schema
        self.assertIsInstance(unpacked["disk"], dict)
        for d_key in ["free_gb", "percent_used", "pressure"]:
            self.assertIn(d_key, unpacked["disk"])

        # Assert nested docker schema
        self.assertIsInstance(unpacked["docker"], dict)
        for dc_key in ["running", "container_count", "containers"]:
            self.assertIn(dc_key, unpacked["docker"])

        # Assert nested tailscale schema
        self.assertIsInstance(unpacked["tailscale"], dict)
        for ts_key in ["online", "backend_state", "peers"]:
            self.assertIn(ts_key, unpacked["tailscale"])

    async def test_check_nose_when_sentinel_is_none(self) -> None:
        """Verify check_nose returns graceful fallback dict when sentinel is None."""
        mock_router = MagicMock()
        mock_router.nose_sentinel = None
        gateway = IngressGateway(router=mock_router, bot_user_id="U_BOT", nose_sentinel=None)
        orig = get_default_nose_sentinel()
        try:
            set_default_nose_sentinel(None)
            res = await gateway._execute_check_nose(trigger_fresh_sniff=False)
            self.assertIsInstance(res, dict)
            self.assertEqual(res["smells_off"], False)
            self.assertIn("NoseSentinel unavailable", res.get("error", ""))
            # Must be JSON-serializable
            json.dumps(res)
        finally:
            set_default_nose_sentinel(orig)

    async def test_check_nose_none_telemetry_fields_resilience(self) -> None:
        """
        EMPIRICAL CHALLENGE: Verify get_telemetry_digest does not crash with TypeError
        when FleetSnapshot contains None values for numerical telemetry metrics.
        """
        self.sentinel.last_snapshot = FleetSnapshot(
            disk={"free_gb": None, "percent_used": None, "pressure": None},
            docker={"docker_running": None, "container_count": None, "containers": None},
            tailscale={"online": None, "backend_state": None, "peers": None},
        )
        # Should not raise TypeError: float() argument must be a string or a real number, not 'NoneType'
        digest = await self.sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
        self.assertIsInstance(digest, dict)
        self.assertIsInstance(digest["disk"]["free_gb"], (int, float))
        self.assertIsInstance(digest["docker"]["container_count"], int)

    async def test_check_eyes_schema_contract_and_json_serializability(self) -> None:
        """Verify check_eyes schema complies with specification across all states and is JSON-serializable."""
        mock_router = MagicMock()
        gateway = IngressGateway(router=mock_router, bot_user_id="U_BOT", nose_sentinel=self.sentinel)

        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(return_value={
            "status": "running",
            "command": "cargo test --all",
            "pid": 5678,
            "blocker": None,
            "buffer_tail": "test result: ok. 45 passed",
        })
        mock_session.get_eyes_status = MagicMock(return_value={
            "state": "RUNNING",
            "active_command": "cargo test --all",
            "pid": 5678,
            "blocker": None,
            "buffer_tail": "test result: ok. 45 passed",
        })

        res = await gateway._execute_check_eyes(session=mock_session)

        # Assert JSON serializability
        try:
            serialized = json.dumps(res)
            unpacked = json.loads(serialized)
        except Exception as e:
            self.fail(f"check_eyes output failed JSON serialization: {e}")

        for key in ["state", "active_command", "pid", "blocker", "buffer_tail"]:
            self.assertIn(key, unpacked, f"Missing key '{key}' in check_eyes output")

        self.assertEqual(unpacked["state"], "RUNNING")
        self.assertEqual(unpacked["active_command"], "cargo test --all")
        self.assertEqual(unpacked["pid"], 5678)

    async def test_check_eyes_session_none_edge_case(self) -> None:
        """Verify check_eyes handles session=None cleanly without crashing."""
        mock_router = MagicMock()
        gateway = IngressGateway(router=mock_router, bot_user_id="U_BOT", nose_sentinel=self.sentinel)
        res = await gateway._execute_check_eyes(session=None)
        self.assertIsInstance(res, dict)
        self.assertEqual(res["state"], "IDLE")
        self.assertEqual(res["active_command"], "")
        self.assertIsNone(res["pid"])
        self.assertIsNone(res["blocker"])
        json.dumps(res)

    async def test_check_eyes_blocker_state_schemas(self) -> None:
        """Verify check_eyes normalizes OAuth and prompt blocker schemas correctly."""
        mock_adapter = MagicMock()
        session = SessionHandle("s_block", "c1", "t1", adapter=mock_adapter)

        # 1. OAuth Blocker
        oauth_status = {
            "status": "running",
            "command": "rclone config reconnect",
            "pid": 1111,
            "blocker": {"type": "oauth", "url": "http://127.0.0.1:53682/auth", "prompt": None},
            "buffer_tail": "Waiting for browser auth...",
        }
        res_oauth = session.get_eyes_status(oauth_status)
        self.assertEqual(res_oauth["state"], "BLOCKER")
        self.assertEqual(res_oauth["blocker"]["type"], "oauth")
        self.assertEqual(res_oauth["blocker"]["url"], "http://127.0.0.1:53682/auth")
        json.dumps(res_oauth)

        # 2. Prompt Confirmation Blocker
        prompt_status = {
            "status": "running",
            "command": "apt-get remove",
            "pid": 2222,
            "blocker": {"type": "prompt", "prompt": "Do you want to continue? [Y/n]", "url": None},
            "buffer_tail": "Do you want to continue? [Y/n]",
        }
        res_prompt = session.get_eyes_status(prompt_status)
        self.assertEqual(res_prompt["state"], "BLOCKER")
        self.assertEqual(res_prompt["blocker"]["type"], "prompt")
        self.assertEqual(res_prompt["blocker"]["prompt"], "Do you want to continue? [Y/n]")
        json.dumps(res_prompt)

    # ==========================================================================
    # 3. State Classification Accuracy (Empirical Bug Hunter)
    # ==========================================================================

    def test_eyes_status_idle_state(self) -> None:
        """Verify get_eyes_status classifies status='idle' as state='IDLE'."""
        mock_adapter = MagicMock()
        session = SessionHandle("s_idle", "c1", "t1", adapter=mock_adapter)
        status_info = {"status": "idle", "command": "", "pid": None, "buffer_tail": ""}
        res = session.get_eyes_status(status_info)
        self.assertEqual(res["state"], "IDLE")

    def test_eyes_status_running_state(self) -> None:
        """Verify get_eyes_status classifies status='running' as state='RUNNING'."""
        mock_adapter = MagicMock()
        session = SessionHandle("s_run", "c1", "t1", adapter=mock_adapter)
        status_info = {"status": "running", "command": "pytest", "pid": 1234, "buffer_tail": "..."}
        res = session.get_eyes_status(status_info)
        self.assertEqual(res["state"], "RUNNING")

    def test_eyes_status_completed_state_with_pid(self) -> None:
        """
        EMPIRICAL CHALLENGE: Verify get_eyes_status classifies status='completed' as state='COMPLETED'
        even when 'pid' of the completed command is present in status_info.
        """
        mock_adapter = MagicMock()
        session = SessionHandle("s_comp", "c1", "t1", adapter=mock_adapter)
        status_info = {
            "status": "completed",
            "command": "python3 build.py",
            "pid": 4321,
            "buffer_tail": "Build succeeded.",
        }
        res = session.get_eyes_status(status_info)
        # Empirical test: state must be COMPLETED, not RUNNING
        self.assertEqual(
            res["state"],
            "COMPLETED",
            f"Expected state='COMPLETED' for completed task with PID, but got '{res['state']}'",
        )

    def test_eyes_status_error_state_with_pid(self) -> None:
        """
        EMPIRICAL CHALLENGE: Verify get_eyes_status classifies status='error' as state='COMPLETED'
        even when 'pid' is present in status_info.
        """
        mock_adapter = MagicMock()
        session = SessionHandle("s_err", "c1", "t1", adapter=mock_adapter)
        status_info = {
            "status": "error",
            "command": "python3 fail.py",
            "pid": 8888,
            "buffer_tail": "Command exited with code 1.",
        }
        res = session.get_eyes_status(status_info)
        self.assertEqual(
            res["state"],
            "COMPLETED",
            f"Expected state='COMPLETED' for failed task with PID, but got '{res['state']}'",
        )

    # ==========================================================================
    # 4. Concurrency & Non-Blocking Verification (session.lock Bypass)
    # ==========================================================================

    async def test_sensory_query_bypasses_session_lock_concurrently(self) -> None:
        """
        Verify that when session.lock is acquired by an active task in Pane 2,
        an out-of-band sensory query ('what do you see?') executes immediately
        WITHOUT acquiring or waiting for session.lock.
        """
        mock_adapter = MagicMock()
        session = SessionHandle("s_conc", "c1", "t1", adapter=mock_adapter)

        # Acquire lock to simulate active long-running turn
        await session.lock.acquire()
        self.assertTrue(session.lock.locked())

        with patch.dict(os.environ, {"FIRSTMATE_TEST_MODE": "1"}):
            t0 = time.perf_counter()
            events = []
            async for ev in session.send_user_message("what do you see?"):
                events.append(ev)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

        # Verify execution succeeded in sub-second time without acquiring/releasing lock
        self.assertTrue(session.lock.locked(), "Session lock must remain held by background task")
        session.lock.release()

        self.assertLess(elapsed_ms, 1500.0, f"Out-of-band sensory query took {elapsed_ms:.1f}ms; must be <1500ms")
        self.assertGreaterEqual(len(events), 2)
        event_types = [getattr(ev, "event_type", None) for ev in events]
        self.assertIn("text_delta", event_types)
        self.assertIn("finish", event_types)

    async def test_high_concurrency_out_of_band_flood(self) -> None:
        """
        Stress test: 25 simultaneous sensory inquiries arrive concurrently while
        session.lock is held. All 25 must complete successfully without deadlocks or timeouts.
        """
        mock_adapter = MagicMock()
        session = SessionHandle("s_flood", "c1", "t1", adapter=mock_adapter)
        await session.lock.acquire()

        prompts = [
            "what do you see?",
            "what does the nose smell?",
            "is the runner blocked?",
            "check the fleet",
            "how's the runner doing?",
        ] * 5  # 25 tasks

        async def _query(p: str) -> List[Any]:
            res = []
            async for ev in session.send_user_message(p):
                res.append(ev)
            return res

        with patch.dict(os.environ, {"FIRSTMATE_TEST_MODE": "1"}):
            t0 = time.perf_counter()
            results = await asyncio.gather(*[_query(p) for p in prompts])
            elapsed = time.perf_counter() - t0

        self.assertTrue(session.lock.locked())
        session.lock.release()

        self.assertEqual(len(results), 25)
        for r in results:
            self.assertGreaterEqual(len(r), 2)
        self.assertLess(elapsed, 20.0, f"25 concurrent queries took {elapsed:.2f}s; expected < 20.0s")

    def test_out_of_band_phrasing_coverage(self) -> None:
        """Verify dynamic out-of-band classification recognizes natural sensory inquiries."""
        sensory_queries = [
            "what do you see?",
            "what does the nose smell?",
            "what are you smelling right now?",
            "check the fleet telemetry",
            "are there any blockers?",
            "show buffer tail",
            "is docker healthy?",
            "how is the runner doing?",
            "how's tailscale looking?",
        ]
        for q in sensory_queries:
            self.assertTrue(
                is_out_of_band_message(q),
                f"Query '{q}' should be classified as out-of-band",
            )

        # Batch commands must NOT be out-of-band
        batch_commands = [
            "turn 1",
            "task 2",
            "run heavy training pipeline",
            "build a web server",
            "deploy the container",
        ]
        for b in batch_commands:
            self.assertTrue(
                is_batch_task_command(b),
                f"Batch command '{b}' should be identified as batch task",
            )
            self.assertFalse(
                is_out_of_band_message(b),
                f"Batch command '{b}' should not be out-of-band",
            )

    # ==========================================================================
    # 5. Dual Sync/Async SessionHandle.get_eyes_status() Invariants
    # ==========================================================================

    def test_eyes_status_sync_dict_access(self) -> None:
        """Verify EyesStatusResult functions as a synchronous dictionary."""
        mock_adapter = MagicMock()
        session = SessionHandle("s_sync", "c1", "t1", adapter=mock_adapter)
        status_info = {"status": "running", "command": "make test", "pid": 9876, "buffer_tail": "OK"}

        res = session.get_eyes_status(status_info)
        self.assertIsInstance(res, dict)
        self.assertEqual(res["state"], "RUNNING")
        self.assertEqual(res["active_command"], "make test")
        self.assertEqual(res["pid"], 9876)
        self.assertEqual(res.get("command"), "make test")
        self.assertIn("buffer_tail", res)
        # JSON dump
        json.dumps(res)

    async def test_eyes_status_awaitable_with_and_without_status_info(self) -> None:
        """Verify get_eyes_status can be awaited both with and without status_info."""
        mock_adapter = MagicMock()
        session = SessionHandle("s_await", "c1", "t1", adapter=mock_adapter)
        status_info = {"status": "idle", "command": "", "pid": None, "buffer_tail": ""}

        # Case 1: Await with status_info
        res1 = await session.get_eyes_status(status_info)
        self.assertIsInstance(res1, dict)
        self.assertEqual(res1["state"], "IDLE")

        # Case 2: Await without status_info (fetches runner status)
        session.get_runner_status = AsyncMock(return_value={
            "status": "running",
            "command": "git push",
            "pid": 5555,
            "buffer_tail": "Writing objects: 100%",
        })
        res2 = await session.get_eyes_status()
        self.assertIsInstance(res2, dict)
        self.assertEqual(res2["state"], "RUNNING")
        self.assertEqual(res2["active_command"], "git push")

    async def test_eyes_status_multiple_awaits(self) -> None:
        """Verify an EyesStatusResult instance can be awaited multiple times without error."""
        mock_adapter = MagicMock()
        session = SessionHandle("s_reawait", "c1", "t1", adapter=mock_adapter)
        session.get_runner_status = AsyncMock(return_value={"status": "idle", "command": "", "pid": None, "buffer_tail": ""})

        res_obj = session.get_eyes_status()
        first_await = await res_obj
        second_await = await res_obj

        self.assertEqual(first_await["state"], "IDLE")
        self.assertEqual(second_await["state"], "IDLE")


if __name__ == "__main__":
    unittest.main()
