#!/usr/bin/env python3
"""
tests/test_sensory_stream_e2e.py
End-to-End WebSocket and SSE Event Delivery Verification Suite for CHO-218 (Parent: CHO-214).

Verifies:
1. End-to-end SSE delivery across all three sensory terminals:
   - Mouth (mouth_log, mouth_briefing, mouth_telemetry)
   - Eyes (eye_log, eye_telemetry)
   - Nose (nose_log, nose_telemetry, nose_alert)
2. End-to-end WebSocket delivery across all three sensory terminals via /ws.
3. Concurrent stream subscriber resilience (zero unhandled exceptions, zero loop starvation).
4. Linear Guardrail: Zero GraphQL API calls or state mutations from Hands.
"""

from __future__ import annotations

import os
import sys
import json
import time
import socket
import asyncio
import unittest
from typing import Dict, Any, List, Tuple
from unittest.mock import MagicMock, AsyncMock, patch

import aiohttp
from aiohttp import web, ClientSession

from golem.bus import ShoulderTapAlert, ShoulderTapEventBus
from golem.webui.server import (
    WebUIServer,
    start_webui_server,
    stop_webui_server,
    webui_broadcast,
    get_system_status,
)


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def read_sse_until(resp, target_event: str, target_content: Optional[str] = None, timeout: float = 5.0) -> Tuple[str, str]:
    """Read SSE chunks until target_event is matched and target_content (if specified) is found."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        chunk = await asyncio.wait_for(resp.content.readuntil(b"\n\n"), timeout=timeout)
        text = chunk.decode("utf-8")
        if f"event: {target_event}" in text:
            if target_content is None or target_content in text:
                return target_event, text
    raise TimeoutError(f"Timed out waiting for SSE event: {target_event} with content: {target_content}")


async def receive_ws_until(ws, target_event: str, timeout: float = 5.0) -> Dict[str, Any]:
    """Receive WebSocket JSON frames until target_event is matched, filtering out keepalives."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        frame = await ws.receive_json(timeout=timeout)
        if frame.get("event") == target_event:
            return frame
    raise TimeoutError(f"Timed out waiting for WS event: {target_event}")


class TestSensoryStreamE2E(unittest.IsolatedAsyncioTestCase):
    """End-to-end tests for sensory terminal streaming via SSE and WebSocket."""

    async def asyncSetUp(self) -> None:
        self.port = _get_free_port()
        self.bus = ShoulderTapEventBus()
        self.server = await start_webui_server(host="127.0.0.1", port=self.port, event_bus=self.bus)
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.session = ClientSession()

    async def asyncTearDown(self) -> None:
        await self.session.close()
        self.bus.clear()
        await stop_webui_server()

    async def test_sse_e2e_all_three_sensory_terminals(self) -> None:
        """
        Connect to /events via SSE and verify real-time streaming to:
        - Mouth terminal (mouth_log, mouth_briefing)
        - Eye terminal (eye_log, eye_telemetry)
        - Nose terminal (nose_log, nose_telemetry)
        """
        async with self.session.get(f"{self.base_url}/events") as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn("text/event-stream", resp.headers.get("Content-Type", ""))

            # 1. Verify initial handshake
            _, text = await read_sse_until(resp, "connected")
            self.assertIn("connected", text)

            # 2. Test Mouth Terminal Event Delivery
            self.server.append_organ_log("mouth", "Chief of Staff operational update.")
            _, text = await read_sse_until(resp, "mouth_log")
            self.assertIn("Chief of Staff operational update.", text)

            # 3. Test Eye Terminal Event Delivery
            self.server.append_organ_log("eyes", "[DIFF] +15 lines in runner buffer [STATE] RUNNING")
            _, text = await read_sse_until(resp, "eye_log")
            self.assertIn("[DIFF] +15 lines", text)
            self.assertIn("RUNNING", text)

            # 4. Test Nose Terminal Event Delivery
            receipt = "[SNIFF] Fleet: 4/4 nodes online | Disk: 120.0GB free (40.0%) | Docker: running (4 containers) | bot.err: offset=5000b (+0b delta, 0 errs) | Linear: 0 assigned (0 actionable)"
            self.server.append_organ_log("nose", receipt)
            _, text = await read_sse_until(resp, "nose_log")
            self.assertIn("[SNIFF] Fleet: 4/4 nodes online", text)

    async def test_websocket_e2e_all_three_sensory_terminals(self) -> None:
        """
        Connect to /ws via WebSocket and verify real-time JSON frames delivered across:
        - Handshake
        - Mouth terminal
        - Eye terminal
        - Nose terminal
        """
        ws_url = f"ws://127.0.0.1:{self.port}/ws"
        async with self.session.ws_connect(ws_url) as ws:
            # 1. Initial handshake
            msg = await receive_ws_until(ws, "connected")
            self.assertEqual(msg.get("data", {}).get("status"), "connected")

            # 2. Ping/Pong check
            await ws.send_str("ping")
            pong = await ws.receive_str(timeout=5.0)
            self.assertEqual(pong, "pong")

            # 3. Stream Mouth Event
            self.server.record_mouth_event("Executive synthesis generated by Mouth", model="gemini-2.5-pro", turns=3)
            mouth_frame = await receive_ws_until(ws, "mouth_briefing")
            self.assertIn("Executive synthesis", mouth_frame.get("data", {}).get("briefing", ""))

            # 4. Stream Eye Event
            self.server.broadcast("eye_log", {"message": "[STATE] RUNNING (pytest active)", "state": "RUNNING"})
            eye_frame = await receive_ws_until(ws, "eye_log")
            self.assertEqual(eye_frame.get("data", {}).get("state"), "RUNNING")

            # 5. Stream Nose Sniff Event
            receipt_data = {
                "fleet": {"total": 4, "online": 4},
                "disk": {"free_gb": 85.2, "percent_used": 62.1},
                "docker": {"running": True, "container_count": 2},
                "bot_err": {"offset": 2048, "delta_bytes": 0, "error_count": 0},
                "linear": {"assigned_count": 1, "actionable_count": 1, "ticket_ids": ["CHO-218"]},
            }
            sniff_summary = "[SNIFF] Fleet: 4/4 nodes online | Disk: 85.2GB free (62.1%) | Docker: running (2 containers) | bot.err: offset=2048b (+0b delta, 0 errs) | Linear: 1 assigned (1 actionable: CHO-218)"
            self.server.broadcast("nose_log", {"message": sniff_summary, "receipt": receipt_data})
            nose_frame = await receive_ws_until(ws, "nose_log")
            self.assertIn("CHO-218", nose_frame.get("data", {}).get("message", ""))

    async def test_bus_alert_end_to_end_delivery(self) -> None:
        """
        Verify decoupled ShoulderTapAlert publication propagates over ShoulderTapEventBus
        and arrives cleanly at connected SSE client for all three organs.
        """
        async with self.session.get(f"{self.base_url}/events") as resp:
            # Consume handshake
            await read_sse_until(resp, "connected")

            # 1. Publish Mouth Thought
            self.bus.publish(ShoulderTapAlert(
                alert_type="mouth_thought",
                source="mouth",
                summary="Analyzing fleet telemetry across nodes",
                data={"thought": "Analyzing fleet telemetry across nodes"},
            ))
            _, text = await read_sse_until(resp, "mouth_log", target_content="Analyzing fleet telemetry")
            self.assertIn("Analyzing fleet telemetry across nodes", text)

            # 2. Publish Eye Buffer Diff
            self.bus.publish(ShoulderTapAlert(
                alert_type="runner_buffer_diff",
                source="eyes",
                summary="[DIFF] +4 lines in runner terminal",
                data={"pane_id": "runner", "diff_lines": 4, "state": "RUNNING"},
            ))
            _, text = await read_sse_until(resp, "eye_log", target_content="[DIFF] +4 lines")
            self.assertIn("[DIFF] +4 lines", text)

            # 3. Publish Nose Sniff Alert
            receipt_summary = "[SNIFF] Fleet: 3/4 nodes online | Disk: 90GB free | Docker: running | bot.err: offset=1000b | Linear: 0 assigned"
            self.bus.publish(ShoulderTapAlert(
                alert_type="nose_sniff",
                source="nose",
                summary=receipt_summary,
                data={"receipt": True},
            ))
            _, text = await read_sse_until(resp, "nose_log", target_content="[SNIFF] Fleet: 3/4")
            self.assertIn("[SNIFF] Fleet: 3/4 nodes online", text)

    async def test_zero_unhandled_exceptions_under_high_velocity_mix(self) -> None:
        """
        Stress test: simultaneous SSE + WebSocket subscribers receiving rapid interleaved
        events from mouth, eyes, and nose.
        Verify zero unhandled exceptions, zero crashes, zero connection deadlocks.
        """
        ws_url = f"ws://127.0.0.1:{self.port}/ws"
        sse_url = f"{self.base_url}/events"

        async def sse_consumer():
            count = 0
            async with self.session.get(sse_url) as resp:
                while count < 30:
                    chunk = await resp.content.readuntil(b"\n\n")
                    if chunk:
                        count += 1
            return count

        async def ws_consumer():
            count = 0
            async with self.session.ws_connect(ws_url) as ws:
                while count < 30:
                    msg = await ws.receive(timeout=5.0)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        count += 1
            return count

        sse_task = asyncio.create_task(sse_consumer())
        ws_task = asyncio.create_task(ws_consumer())

        # Wait briefly for subscribers to connect
        await asyncio.sleep(0.05)

        # Emit 35 mixed sensory events in rapid succession
        for i in range(35):
            organ = ["mouth", "eyes", "nose"][i % 3]
            self.server.append_organ_log(organ, f"Rapid burst event #{i} for {organ}")
            await asyncio.sleep(0.005)

        sse_count, ws_count = await asyncio.gather(sse_task, ws_task)
        self.assertGreaterEqual(sse_count, 30)
        self.assertGreaterEqual(ws_count, 30)

    async def test_linear_guardrail_compliance(self) -> None:
        """
        Verify Linear Guardrail:
        Confirm that across all sensory event pipelines and WebUI broadcasts,
        no Linear GraphQL mutations or ticket transition methods are ever called.
        """
        from golem.linear_bridge import LinearBridge
        with patch.object(LinearBridge, "update_issue_state", MagicMock()) as mock_update:
            with patch.object(LinearBridge, "execute_query", MagicMock()) as mock_query:
                # Trigger system status and organ updates
                status = get_system_status()
                self.assertIn("nose", status)
                self.assertIn("eyes", status)
                self.assertIn("mouth", status)

                # Emit sensory events
                self.server.append_organ_log("mouth", "Guardrail check briefing")
                self.server.append_organ_log("eyes", "Guardrail check buffer")
                self.server.append_organ_log("nose", "Guardrail check sniff")

                # Verify no mutation was called
                mock_update.assert_not_called()
                mock_query.assert_not_called()


if __name__ == "__main__":
    unittest.main()
