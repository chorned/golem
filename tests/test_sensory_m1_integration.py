#!/usr/bin/env python3
"""
tests/test_sensory_m1_integration.py
Comprehensive integration test suite for First Mate 2.0 Milestone 1 Sensory Architecture:
  - Requirement 1: NoseSentinel telemetry digest, caching, cold boot, and singleton accessors.
  - Requirement 2: SessionHandle.get_eyes_status contract and dual sync/async compatibility.
  - Requirement 3: IngressGateway sensory tool declarations, pure JSON outputs, and Mouth functionResponse branching.
  - Requirement 4: Mouth sensory grounding, sub-second latency, and cross-sensory prompt awareness.
"""

from __future__ import annotations

import os
import sys
import time
import inspect
import asyncio
import unittest
from typing import Dict, Any, Optional
from unittest.mock import MagicMock, AsyncMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import ShoulderTapEventBus, get_default_event_bus
from golem.sensory.nose import (
    NoseSentinel,
    FleetSnapshot,
    AnomalyVerdict,
    get_default_nose_sentinel,
    set_default_nose_sentinel,
)
from golem.gateway.session import SessionHandle, EyesStatusResult
from golem.gateway.ingress import (
    IngressGateway,
    get_mouth_tools_declaration,
    CHECK_EYES_TOOL_DECLARATION,
    CHECK_NOSE_TOOL_DECLARATION,
)
from golem.agent.mouth import generate_mouth_response


class TestMilestone1SensoryArchitecture(unittest.IsolatedAsyncioTestCase):
    """Integration test suite covering all Milestone 1 sensory capabilities."""

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

    # --------------------------------------------------------------------------
    # R1: Nose Sentinel Telemetry & Singletons
    # --------------------------------------------------------------------------
    async def test_nose_singleton_accessors(self) -> None:
        """Verify get_default_nose_sentinel and set_default_nose_sentinel."""
        orig = get_default_nose_sentinel()
        try:
            set_default_nose_sentinel(self.sentinel)
            self.assertIs(get_default_nose_sentinel(), self.sentinel)
            set_default_nose_sentinel(None)
            self.assertIsNone(get_default_nose_sentinel())
        finally:
            set_default_nose_sentinel(orig)

    async def test_get_telemetry_digest_cold_boot_immediate(self) -> None:
        """Verify get_telemetry_digest returns immediately (<50ms) on cold boot when trigger_fresh_sniff=False."""
        self.assertIsNone(self.sentinel.last_snapshot)
        t0 = time.perf_counter()
        digest = await self.sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
        elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 0.05, "Cold boot telemetry digest must return immediately without blocking")
        self.assertIn("seconds_ago", digest)
        self.assertIn("minutes_ago", digest)
        self.assertIn("last_check_timestamp", digest)
        self.assertIn("smells_off", digest)
        self.assertIn("verdict_summary", digest)
        self.assertIn("disk", digest)
        self.assertIn("docker", digest)
        self.assertIn("tailscale", digest)
        self.assertIn("recent_error_count", digest)

    async def test_get_telemetry_digest_cached_snapshot(self) -> None:
        """Verify get_telemetry_digest returns cached metrics without re-probing."""
        # Populate snapshot
        self.sentinel.last_snapshot = FleetSnapshot(
            timestamp=time.time() - 120.0,
            nodes={"hermes": {"reachable": True}},
            tailscale={"online": True, "backend_state": "Running", "peers": {}},
            disk={"free_gb": 45.5, "percent_used": 60.0, "pressure": False},
            docker={"docker_running": True, "container_count": 8, "containers": ["plex"]},
        )
        self.sentinel.last_verdict = AnomalyVerdict(
            smells_off=False,
            severity="low",
            category="other",
            summary="All systems nominal; 0 errors, fleet healthy.",
        )
        self.sentinel._last_tick_time = time.time() - 120.0

        t0 = time.perf_counter()
        digest = await self.sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
        elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 0.01)
        self.assertAlmostEqual(digest["minutes_ago"], 2.0, delta=0.2)
        self.assertFalse(digest["smells_off"])
        self.assertEqual(digest["disk"]["free_gb"], 45.5)
        self.assertEqual(digest["docker"]["container_count"], 8)
        self.assertTrue(digest["tailscale"]["online"])

    async def test_get_telemetry_digest_trigger_fresh_sniff(self) -> None:
        """Verify trigger_fresh_sniff=True runs a full tick cycle."""
        with patch.object(self.sentinel, "tick", new_callable=AsyncMock) as mock_tick:
            mock_tick.return_value = None
            await self.sentinel.get_telemetry_digest(trigger_fresh_sniff=True)
            mock_tick.assert_awaited_once()

    async def test_get_telemetry_digest_defensive_against_none_fields(self) -> None:
        """Verify get_telemetry_digest handles None fields in snapshot without TypeError."""
        self.sentinel.last_snapshot = FleetSnapshot(
            timestamp=None,
            disk={"free_gb": None, "percent_used": None, "pressure": None},
            docker={"docker_running": None, "container_count": None, "containers": None},
            tailscale={"online": None, "backend_state": None, "peers": None},
        )
        digest = await self.sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
        self.assertIsInstance(digest, dict)
        self.assertEqual(digest["disk"]["free_gb"], 0.0)
        self.assertEqual(digest["disk"]["percent_used"], 0.0)
        self.assertEqual(digest["docker"]["container_count"], 0)
        self.assertFalse(digest["docker"]["running"])
        self.assertFalse(digest["tailscale"]["online"])

    # --------------------------------------------------------------------------
    # R2: SessionHandle.get_eyes_status
    # --------------------------------------------------------------------------
    def test_session_get_eyes_status_sync(self) -> None:
        """Verify get_eyes_status returns an EyesStatusResult dictionary synchronously when given status_info."""
        mock_adapter = MagicMock()
        session = SessionHandle(
            session_id="S_TEST_EYES",
            channel="C1",
            thread_ts="T1",
            adapter=mock_adapter,
        )
        status_info = {
            "status": "running",
            "command": "python3 build.py",
            "pid": 4321,
            "blocker": {"type": "oauth", "url": "http://127.0.0.1:8080/auth", "prompt": None},
            "buffer_tail": "Waiting for auth...",
        }
        res = session.get_eyes_status(status_info)
        self.assertIsInstance(res, dict)
        self.assertEqual(res["state"], "BLOCKER")
        self.assertEqual(res["active_command"], "python3 build.py")
        self.assertEqual(res["pid"], 4321)
        self.assertIsNotNone(res["blocker"])
        self.assertEqual(res["blocker"]["url"], "http://127.0.0.1:8080/auth")

    def test_session_get_eyes_status_completed_with_retained_pid(self) -> None:
        """Verify terminal status (completed) takes precedence over retained PID."""
        mock_adapter = MagicMock()
        session = SessionHandle("S_COMPLETED", "C1", "T1", adapter=mock_adapter)
        status_info = {
            "status": "completed",
            "command": "python3 build.py",
            "pid": 5555,
            "buffer_tail": "Success",
        }
        res = session.get_eyes_status(status_info)
        self.assertEqual(res["state"], "COMPLETED")
        self.assertEqual(res["pid"], 5555)

    def test_session_get_eyes_status_error_with_retained_pid(self) -> None:
        """Verify terminal status (error) takes precedence over retained PID."""
        mock_adapter = MagicMock()
        session = SessionHandle("S_ERROR", "C1", "T1", adapter=mock_adapter)
        status_info = {
            "status": "error",
            "command": "python3 build.py",
            "pid": 6666,
            "buffer_tail": "Failed",
        }
        res = session.get_eyes_status(status_info)
        self.assertEqual(res["state"], "COMPLETED")
        self.assertEqual(res["pid"], 6666)

    async def test_session_get_eyes_status_awaitable(self) -> None:
        """Verify get_eyes_status can be awaited asynchronously."""
        mock_adapter = MagicMock()
        session = SessionHandle(
            session_id="S_TEST_ASYNC",
            channel="C1",
            thread_ts="T1",
            adapter=mock_adapter,
        )
        session.get_runner_status = AsyncMock(return_value={
            "status": "running",
            "command": "pytest",
            "pid": 9999,
            "buffer_tail": "Running tests...",
        })
        res = await session.get_eyes_status()
        self.assertIsInstance(res, dict)
        self.assertEqual(res["state"], "RUNNING")
        self.assertEqual(res["active_command"], "pytest")
        self.assertEqual(res["pid"], 9999)

    # --------------------------------------------------------------------------
    # R3: Ingress Sensory Tools & Execution
    # --------------------------------------------------------------------------
    def test_mouth_tools_declaration_includes_sensory_tools(self) -> None:
        """Verify get_mouth_tools_declaration exposes check_eyes and check_nose."""
        tools = get_mouth_tools_declaration()
        self.assertIsInstance(tools, list)
        self.assertGreaterEqual(len(tools), 1)
        fn_decls = tools[0].get("functionDeclarations", [])
        tool_names = [fn["name"] for fn in fn_decls]
        self.assertIn("dispatch_task", tool_names)
        self.assertIn("check_eyes", tool_names)
        self.assertIn("check_nose", tool_names)

    async def test_ingress_execute_check_eyes_pure_json(self) -> None:
        """Verify _execute_check_eyes returns a pure JSON dictionary."""
        mock_router = MagicMock()
        gateway = IngressGateway(router=mock_router, bot_user_id="U_BOT", nose_sentinel=self.sentinel)

        mock_session = MagicMock()
        mock_session.get_runner_status = AsyncMock(return_value={
            "status": "idle",
            "command": "",
            "pid": None,
            "buffer_tail": "Ready.",
        })
        mock_session.get_eyes_status = MagicMock(return_value={
            "state": "IDLE",
            "active_command": "",
            "pid": None,
            "blocker": None,
            "buffer_tail": "Ready.",
        })

        res = await gateway._execute_check_eyes(session=mock_session)
        self.assertIsInstance(res, dict)
        self.assertEqual(res["state"], "IDLE")
        self.assertEqual(res["buffer_tail"], "Ready.")

    async def test_ingress_execute_check_nose_pure_json(self) -> None:
        """Verify _execute_check_nose returns a pure JSON dictionary."""
        mock_router = MagicMock()
        gateway = IngressGateway(router=mock_router, bot_user_id="U_BOT", nose_sentinel=self.sentinel)

        res = await gateway._execute_check_nose(trigger_fresh_sniff=False)
        self.assertIsInstance(res, dict)
        self.assertIn("seconds_ago", res)
        self.assertIn("smells_off", res)
        self.assertIn("disk", res)

    # --------------------------------------------------------------------------
    # R4: Mouth Sensory Grounding & Sub-Second Latency
    # --------------------------------------------------------------------------
    async def test_mouth_sensory_grounding_subsecond_latency(self) -> None:
        """Verify generate_mouth_response responds under 1 second with factual grounding."""
        eyes_status = {
            "state": "RUNNING",
            "active_command": "cargo build --release",
            "pid": 5432,
            "buffer_tail": "Compiling crates...",
        }
        nose_status = {
            "seconds_ago": 60.0,
            "minutes_ago": 1.0,
            "smells_off": False,
            "verdict_summary": "All systems nominal; 0 errors, fleet healthy.",
            "disk": {"free_gb": 50.0, "percent_used": 50.0, "pressure": False},
            "docker": {"running": True, "container_count": 5, "containers": []},
            "tailscale": {"online": True, "backend_state": "Running", "peers": {}},
        }
        status_info = {"status": "running", "pid": 5432, "command": "cargo build --release"}

        mock_gemini_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "👃 *Fleet Health:* Air is clean; all systems nominal. Root disk has 50.0 GB free."}
                        ]
                    }
                }
            ]
        }
        mock_resp_obj = AsyncMock()
        mock_resp_obj.status = 200
        mock_resp_obj.json = AsyncMock(return_value=mock_gemini_response)

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__.return_value = mock_resp_obj
        mock_post_cm.__aexit__.return_value = None

        with patch("aiohttp.ClientSession.post", return_value=mock_post_cm):
            t0 = time.perf_counter()
            resp = await generate_mouth_response(
                "What does The Nose smell right now?",
                status_info,
                eyes_status=eyes_status,
                nose_status=nose_status,
                api_key="mock_key",
            )
            elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 1.0, "Mouth response must complete under 1 second")
        self.assertIn("nominal", resp.lower())
        self.assertIn("50", resp)


if __name__ == "__main__":
    unittest.main()
