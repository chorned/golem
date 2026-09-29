#!/usr/bin/env python3
"""
tests/test_telemetry_observer.py
Comprehensive unit tests for First Mate out-of-band PTY telemetry,
real-time delegation observation, state hashing, rate limiting, and status query intercepts.
"""

import os
import sys
import json
import time
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app
from app import GatewaySession, sessions, process_user_prompt
from telemetry_observer import (
    SessionTelemetryObserver,
    resolve_machine_label,
    format_runner_label,
    extract_listening_ports,
    format_terminal_tail,
    strip_terminal_sequences,
    build_status_card_blocks
)


class TestTelemetryObserver(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        sessions.clear()

    def tearDown(self):
        sessions.clear()

    def test_resolve_machine_label(self):
        """Verify machine label formatting maps fleet nodes to friendly hardware profiles."""
        self.assertEqual(resolve_machine_label("desktop-pc"), "desktop-pc (Windows CUDA)")
        self.assertEqual(resolve_machine_label("compute-grid"), "desktop-pc (Windows CUDA)")
        self.assertEqual(resolve_machine_label("oci-machine"), "oci-machine (Cloud Operations)")
        self.assertEqual(resolve_machine_label("cloud-ops"), "oci-machine (Cloud Operations)")
        self.assertEqual(resolve_machine_label("personal-laptop"), "personal-laptop (Active Workstation)")
        self.assertEqual(resolve_machine_label("dev-grid"), "personal-laptop (Active Workstation)")
        self.assertEqual(resolve_machine_label("hermes"), "hermes (Local Mac mini)")
        self.assertEqual(resolve_machine_label("localhost"), "hermes (Local Mac mini)")
        self.assertEqual(resolve_machine_label(""), "hermes (Local Mac mini)")
        self.assertEqual(resolve_machine_label(None), "hermes (Local Mac mini)")

    def test_format_runner_label(self):
        """Verify delegation hierarchy formatting produces clean directional labels."""
        self.assertEqual(format_runner_label("agy"), "agy")
        self.assertEqual(format_runner_label("agy", []), "agy")
        self.assertEqual(format_runner_label("agy", ["opencode"]), "agy ➔ opencode")
        self.assertEqual(format_runner_label("agy", ["opencode", "opencode"]), "agy ➔ opencode")
        self.assertEqual(format_runner_label("agy", ["agy"]), "agy")
        self.assertEqual(format_runner_label("agy", ["opencode", "worker"]), "agy ➔ opencode ➔ worker")

    def test_extract_listening_ports(self):
        """Verify port extraction from various server announcement formats."""
        text = "Serving HTTP on 0.0.0.0 port 8090 ...\nAvailable at http://localhost:8090\nProxy at 127.0.0.1:51723"
        ports = extract_listening_ports(text)
        self.assertIn("8090", ports)
        self.assertIn("51723", ports)

    def test_format_terminal_tail(self):
        """Verify terminal tail strips ANSI, control codes, splash headers, and bounds lines."""
        raw = (
            "\x1b[32mAntigravity CLI\x1b[0m\n"
            "? for shortcuts\n"
            "───────────────────\n"
            "● run python3 -m http.server 8090\n"
            "Serving HTTP on 0.0.0.0 port 8090 ...\n"
            "Ready for connections.\n"
        )
        tail = format_terminal_tail(raw, max_lines=2)
        lines = tail.splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0], "Serving HTTP on 0.0.0.0 port 8090 ...")
        self.assertEqual(lines[1], "Ready for connections.")

    def test_build_status_card_blocks(self):
        """Verify Slack Block Kit generation contains required operations headers and fields."""
        blocks = build_status_card_blocks(
            workspace_id="w-123",
            machine_label="hermes (Local Mac mini)",
            runner_label="agy ➔ opencode",
            status_phase="WORKING",
            elapsed_seconds=12,
            current_action="python3 -m http.server 8090",
            terminal_tail="Serving HTTP on port 8090",
            artifacts={"urls": ["http://localhost:8090"], "files": ["index.html"]}
        )
        self.assertTrue(any(b.get("type") == "header" and "w-123" in b.get("text", {}).get("text", "") for b in blocks))
        
        # Check section fields
        field_section = next(b for b in blocks if b.get("type") == "section" and "fields" in b)
        field_texts = [f["text"] for f in field_section["fields"]]
        self.assertTrue(any("hermes (Local Mac mini)" in t for t in field_texts))
        self.assertTrue(any("agy ➔ opencode" in t for t in field_texts))
        self.assertTrue(any("WORKING" in t for t in field_texts))
        self.assertTrue(any("12s" in t for t in field_texts))

    async def test_observer_detects_subagent_delegation(self):
        """Verify runner label updates from agy to agy ➔ opencode when child pane appears in workspace inspect."""
        session = GatewaySession(
            session_key="C_TEST:600.001",
            agent_name="fm-test-delegation",
            target_cwd="/Users/hermes/projects/sandbox/test-delegation",
            channel="C_TEST",
            thread_ts="600.001",
            workspace_id="w_delegation"
        )
        session.status = "working"

        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "card_msg_ts_001"})
        client.chat_update = AsyncMock()

        herdr_client = MagicMock()
        
        # Turn 1: Single root pane running agy
        ws_inspect_turn1 = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w_delegation"},
                "panes": [
                    {"pane_id": "w_delegation:p1", "agent": "agy", "machine": "hermes"}
                ]
            }
        })

        # Turn 2: Subagent opencode pane spawned via herdr-mesh
        ws_inspect_turn2 = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w_delegation"},
                "panes": [
                    {"pane_id": "w_delegation:p1", "agent": "agy", "machine": "hermes"},
                    {"pane_id": "w_delegation:p2", "agent": "opencode", "machine": "hermes", "name": "opencode-worker"}
                ]
            }
        })

        turn = [1]
        async def mock_run_cli(args, timeout=5.0):
            if args[:2] == ["workspace", "inspect"]:
                out = ws_inspect_turn1 if turn[0] == 1 else ws_inspect_turn2
                return 0, out, ""
            elif args[:2] == ["agent", "inspect"]:
                return 0, json.dumps({"foreground_process": {"cmdline": "node worker.js"}}), ""
            elif args[:2] == ["agent", "list"]:
                return 0, json.dumps({"result": {"agents": []}}), ""
            elif args[:2] == ["agent", "read"]:
                return 0, "Worker active\nExecuting subtask...\n", ""
            return 0, "", ""

        herdr_client.run_cli = AsyncMock(side_effect=mock_run_cli)

        observer = SessionTelemetryObserver(
            session=session,
            client=client,
            herdr_client=herdr_client,
            poll_interval=0.1,
            min_update_interval=0.0  # Allow immediate updates in test
        )
        observer.card_msg_ts = "card_msg_ts_001"

        # Poll Turn 1: should be root agy
        await observer.poll_and_update()
        self.assertEqual(observer.last_runner_label, "agy")

        # Advance to Turn 2: delegation occurs
        turn[0] = 2
        await observer.poll_and_update()
        self.assertEqual(observer.last_runner_label, "agy ➔ opencode")

        # Verify chat_update received updated runner label in Block Kit fields
        self.assertGreaterEqual(client.chat_update.call_count, 1)
        last_call_blocks = client.chat_update.call_args[1]["blocks"]
        field_section = next(b for b in last_call_blocks if b.get("type") == "section" and "fields" in b)
        runner_field = next(f["text"] for f in field_section["fields"] if "*Active Runner:*" in f["text"])
        self.assertIn("agy ➔ opencode", runner_field)

    async def test_observer_rate_limits_and_hashes_state(self):
        """Verify identical PTY state does not trigger redundant chat_update calls and throttles updates."""
        session = GatewaySession(
            session_key="C_TEST:700.001",
            agent_name="fm-test-throttle",
            target_cwd="/tmp/throttle",
            channel="C_TEST",
            thread_ts="700.001",
            workspace_id="w_throttle"
        )
        session.status = "working"

        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "card_msg_ts_002"})
        client.chat_update = AsyncMock()

        herdr_client = MagicMock()
        ws_inspect = json.dumps({
            "result": {
                "workspace": {"workspace_id": "w_throttle"},
                "panes": [{"pane_id": "w_throttle:p1", "agent": "agy", "machine": "hermes"}]
            }
        })

        read_output = ["Compiling assets...\n"]
        async def mock_run_cli(args, timeout=5.0):
            if args[:2] == ["workspace", "inspect"]:
                return 0, ws_inspect, ""
            elif args[:2] == ["agent", "inspect"]:
                return 0, json.dumps({"foreground_process": {"cmdline": "build.sh"}}), ""
            elif args[:2] == ["agent", "list"]:
                return 0, json.dumps({"result": {"agents": []}}), ""
            elif args[:2] == ["agent", "read"]:
                return 0, read_output[0], ""
            return 0, "", ""

        herdr_client.run_cli = AsyncMock(side_effect=mock_run_cli)

        observer = SessionTelemetryObserver(
            session=session,
            client=client,
            herdr_client=herdr_client,
            poll_interval=0.1,
            min_update_interval=1.5
        )
        observer.card_msg_ts = "card_msg_ts_002"

        # 1. First poll fires initial update
        await observer.poll_and_update()
        self.assertEqual(client.chat_update.call_count, 1)

        # 2. Second poll with IDENTICAL state does NOT fire chat_update (hash match)
        await observer.poll_and_update()
        self.assertEqual(client.chat_update.call_count, 1)

        # 3. Third poll with CHANGED state, but within 1.5s interval -> throttled!
        read_output[0] = "Compiling assets...\nOptimization pass 2\n"
        await observer.poll_and_update()
        self.assertEqual(client.chat_update.call_count, 1)

        # 4. Advance time past 1.5s interval -> update fires!
        observer.last_update_time = time.time() - 2.0
        await observer.poll_and_update()
        self.assertEqual(client.chat_update.call_count, 2)

    async def test_status_query_intercept_extracts_url(self):
        """Verify on-demand 'what's the URL?' queries return the extracted endpoint directly from terminal buffer without LLM reasoning."""
        session_key = "C_TEST:800.001"
        session = GatewaySession(
            session_key=session_key,
            agent_name="fm-test-url-query",
            target_cwd="/tmp/url-query",
            channel="C_TEST",
            thread_ts="800.001",
            workspace_id="w_url_query"
        )
        session.status = "working"
        sessions[session_key] = session

        client = AsyncMock()
        client.chat_postMessage = AsyncMock()

        event = {
            "channel": "C_TEST",
            "ts": "800.002",
            "thread_ts": "800.001",
            "text": "what's the URL?",
            "is_imessage": False
        }

        mock_terminal_scrollback = (
            "Serving HTTP on 0.0.0.0 port 8090 (http://0.0.0.0:8090/) ...\n"
            "Available at http://localhost:8090"
        )

        with patch.object(app.herdr_client, "run_cli", new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = (0, mock_terminal_scrollback, "")

            start_t = time.time()
            await process_user_prompt(event, client)
            elapsed = time.time() - start_t

            # Verify executed in well under 300ms
            self.assertLess(elapsed, 0.3)

            # Verify called agent read and NOT agent prompt
            cli_calls = [c.args[0] for c in mock_cli.call_args_list]
            self.assertTrue(any(c[:2] == ["agent", "read"] for c in cli_calls))
            self.assertFalse(any(c[:2] == ["agent", "prompt"] for c in cli_calls))

            # Verify URL returned directly in chat response
            client.chat_postMessage.assert_called_once()
            reply = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("8090", reply)

    async def test_observer_freezes_timer_on_blocked_state(self):
        """Verify Herdr transition to BLOCKED (Ask Mode) pauses timer and sets phase to APPROVAL REQUIRED."""
        session = GatewaySession(
            session_key="C_TEST:900.001",
            agent_name="fm-test-blocked",
            target_cwd="/tmp/blocked",
            channel="C_TEST",
            thread_ts="900.001",
            workspace_id="w_blocked"
        )
        session.status = "working"

        client = AsyncMock()
        client.chat_update = AsyncMock()
        herdr_client = MagicMock()
        herdr_client.run_cli = AsyncMock(return_value=(0, "{}", ""))

        observer = SessionTelemetryObserver(
            session=session,
            client=client,
            herdr_client=herdr_client,
            min_update_interval=0.0
        )
        observer.card_msg_ts = "card_msg_ts_003"
        observer.start_time = time.time() - 10.0  # 10s elapsed

        # Transition to blocked
        session.status = "blocked"
        await observer.poll_and_update()

        self.assertIsNotNone(observer.paused_elapsed)
        self.assertGreaterEqual(observer.paused_elapsed, 10)

        # Inspect chat_update block
        call_blocks = client.chat_update.call_args[1]["blocks"]
        field_section = next(b for b in call_blocks if b.get("type") == "section" and "fields" in b)
        phase_field = next(f["text"] for f in field_section["fields"] if "*Status Phase:*" in f["text"])
        self.assertIn("APPROVAL REQUIRED", phase_field)

    async def test_observer_settle_complete_and_failed(self):
        """Verify settle_complete and settle_failed update Slack status card with completion details."""
        session = GatewaySession(
            session_key="C_TEST:1000.001",
            agent_name="fm-test-settle",
            target_cwd="/tmp/settle",
            channel="C_TEST",
            thread_ts="1000.001",
            workspace_id="w_settle"
        )
        client = AsyncMock()
        client.chat_update = AsyncMock()
        herdr_client = MagicMock()

        observer = SessionTelemetryObserver(session=session, client=client, herdr_client=herdr_client)
        observer.card_msg_ts = "card_msg_ts_004"

        # Complete
        await observer.settle_complete(
            artifacts={"urls": ["http://localhost:9000"], "files": ["app.py"]},
            terminal_output="Done"
        )
        self.assertTrue(observer.stop_event.is_set())
        complete_call_text = client.chat_update.call_args[1]["text"]
        self.assertIn("✅ FIRST MATE OPS", complete_call_text)

        # Failed
        observer.stop_event.clear()
        await observer.settle_failed(error_detail="Segmentation fault in worker")
        self.assertTrue(observer.stop_event.is_set())
        failed_call_text = client.chat_update.call_args[1]["text"]
        self.assertIn("❌ FIRST MATE OPS", failed_call_text)


if __name__ == "__main__":
    unittest.main()
