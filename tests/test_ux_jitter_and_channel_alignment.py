#!/usr/bin/env python3
"""
Regression tests for UX smoothness and 1:1 Slack / iMessage channel alignment:
1. Turn-scoped progress bubble coalescing & chronological final briefing placement.
2. Read-only hop cap enforcement, single-shot check_eyes, and re-entrant spin-loop prevention.
3. Hand preflight capabilities/precedent injection & Eyes false-positive OAuth suppression.
4. Tool-aware stall thresholds, collided pane key deduplication, and channel-aligned Nose alerts.
5. Local file:// URI sanitization across both Slack and iMessage normalizers.
"""

import asyncio
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from golem.bluebubbles_transport import sanitize_for_imessage
from golem.bus import ShoulderTapAlert
from golem.eyes import _validate_classification
from golem.harness import get_preflight_node_invariants
from golem.ingress import (
    BackstageExecutionAccumulator,
    IngressGateway,
    MAX_CONSECUTIVE_READ_ONLY_HOPS,
    READ_ONLY_HOP_TOOLS,
)
from golem.normalizer import sanitize_for_slack, transpile_markdown_to_mrkdwn
from golem.nose import NoseSentinel


class TestFileUriSanitization(unittest.TestCase):
    """Change 5: Ensure file:// URIs never render as broken <file://...|label> links in Slack or iMessage."""

    def test_slack_normalizer_converts_file_markdown_links_to_inline_code(self):
        raw = (
            "Updated [sync_worker.py](file:///home/user/project/sync_worker.py#L10-L25) "
            "and verified <file:///tmp/output.json|output.json>."
        )
        mrkdwn = transpile_markdown_to_mrkdwn(raw)
        self.assertNotIn("file://", mrkdwn)
        self.assertIn("`sync_worker.py`", mrkdwn)
        self.assertIn("`output.json`", mrkdwn)

        sanitized = sanitize_for_slack(raw)
        self.assertNotIn("file://", sanitized)
        self.assertIn("`sync_worker.py`", sanitized)
        self.assertIn("`output.json`", sanitized)

    def test_imessage_sanitizer_strips_file_links_cleanly(self):
        raw = (
            " Executed `/tmp/run_job.py` and saved <file:///tmp/report.txt|report.txt> "
            "plus [summary.md](file:///tmp/summary.md)."
        )
        clean = sanitize_for_imessage(raw)
        self.assertNotIn("file://", clean)
        self.assertNotIn("<file://", clean)
        self.assertIn("report.txt", clean)
        self.assertIn("summary.md", clean)


class TestPreflightAndEyesAuthSuppression(unittest.TestCase):
    """Change 3: Hand preflight injects capabilities + cited precedent, and Eyes suppresses false-positive OAuth."""

    def test_preflight_injects_capabilities_and_precedent_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wiki_dir = Path(tmpdir)
            (wiki_dir / "context" / "rules").mkdir(parents=True)
            (wiki_dir / "runbooks" / "skills" / "api_sync").mkdir(parents=True)

            (wiki_dir / "context" / "rules" / "hermes.md").write_text(
                "Host rule: use local virtualenv.", encoding="utf-8"
            )
            (wiki_dir / "context" / "capabilities.md").write_text(
                "Autonomous Browser Auth: Saved session profile at ~/.config/app/profile.",
                encoding="utf-8",
            )
            (wiki_dir / "runbooks" / "skills" / "api_sync" / "SKILL.md").write_text(
                "Step 1: Refresh session cookies via headless browser before calling API.",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"GOLEM_WIKI_DIR": str(wiki_dir)}), patch("golem.wiki.DEFAULT_WIKI_DIR", wiki_dir):
                combined = get_preflight_node_invariants(
                    "hermes",
                    precedent_source="runbooks/skills/api_sync/SKILL.md",
                )

            self.assertIn("Host rule: use local virtualenv.", combined)
            self.assertIn("Autonomous Browser Auth: Saved session profile", combined)
            self.assertIn("Step 1: Refresh session cookies via headless browser", combined)

    def test_eyes_suppresses_false_positive_oauth_for_autonomous_agy_login_navigation(self):
        agy_buffer = (
            "🚀 [16:23:19] Spawned agy on hermes (PID 12345): Refresh session cookies\n"
            "▸ [THOUGHT] Navigating to https://www.example.com/login to refresh session cookies...\n"
            "● [TOOL START] run_command\n"
            "⏳ [HEARTBEAT +15s] Worker active — executing tool (run_command)...\n"
        )
        raw_classification = {
            "state": "BLOCKER",
            "blocker_type": "oauth",
            "blocker_url": "https://www.example.com/login",
            "blocker_prompt": "Log in to your account",
            "summary": "Waiting for login",
        }
        validated = _validate_classification(agy_buffer, raw_classification)
        self.assertEqual(validated["state"], "RUNNING")
        self.assertIsNone(validated["blocker_type"])
        self.assertIsNone(validated["blocker_url"])

    def test_eyes_preserves_genuine_oauth_callback_blocker(self):
        oauth_buffer = (
            "Please visit this URL to authorize the application:\n"
            "https://accounts.google.com/o/oauth2/v2/auth?client_id=123&redirect_uri=http://localhost:8080/callback&response_type=code\n"
            "Enter the authorization code: "
        )
        raw_classification = {
            "state": "BLOCKER",
            "blocker_type": "oauth",
            "blocker_url": "https://accounts.google.com/o/oauth2/v2/auth?client_id=123&redirect_uri=http://localhost:8080/callback&response_type=code",
            "blocker_prompt": "Enter the authorization code:",
            "summary": "Waiting for OAuth code",
        }
        validated = _validate_classification(oauth_buffer, raw_classification)
        self.assertEqual(validated["state"], "BLOCKER")
        self.assertEqual(validated["blocker_type"], "oauth")


class TestNoseToolStallAndChannelRouting(unittest.IsolatedAsyncioTestCase):
    """Change 4: Tool-aware stall thresholds, collided pane deduplication, and channel-aligned Nose alerts."""

    async def test_probe_stalled_hands_uses_600s_threshold_when_tool_in_flight(self):
        now = time.time()
        webui = MagicMock()
        # Hand has been running a tool for 240s (>180s idle threshold, but <600s tool threshold)
        webui.get_executing_hands.return_value = {
            "agy-1001": {
                "pane_id": "agy-1001",
                "worker": "agy",
                "node": "hermes",
                "status": "RUNNING",
                "started_at": now - 250.0,
                "last_real_activity_at": now - 240.0,
                "buffer_tail": (
                    "▸ [THOUGHT] Running verification script\n"
                    "● [TOOL START] run_command\n"
                    "⏳ [HEARTBEAT +240s] Worker active — executing tool (run_command)...\n"
                ),
                "channel": "iMessage;-;+15551234567",
            }
        }
        sentinel = NoseSentinel(webui_server=webui)
        env_without_override = {k: v for k, v in os.environ.items() if k != "GOLEM_HAND_STALL_SECONDS"}
        with patch.dict(os.environ, env_without_override, clear=True):
            stalled = sentinel.probe_stalled_hands()
        self.assertEqual(len(stalled), 0)

    async def test_probe_stalled_hands_deduplicates_collided_pane_keys(self):
        now = time.time()
        webui = MagicMock()
        webui.get_executing_hands.return_value = {
            "agy-695629@hermes#2": {
                "pane_id": "agy-695629@hermes#2",
                "worker": "agy",
                "node": "hermes",
                "status": "RUNNING",
                "started_at": now - 700.0,
                "last_real_activity_at": now - 650.0,
                "buffer_tail": "⏳ [HEARTBEAT +650s] Worker active — waiting on model/tool stream...\n",
                "channel": "iMessage;-;+15551234567",
            }
        }
        sentinel = NoseSentinel(webui_server=webui)
        # Simulate ingress heartbeat already having alerted on base pane id 'agy-695629'
        sentinel._alerted_stalled_hands.add("agy-695629")
        stalled = sentinel.probe_stalled_hands()
        self.assertEqual(len(stalled), 0)

    async def test_handle_health_anomaly_alert_routes_imessage_and_slack_appropriately(self):
        router = MagicMock()
        gateway = IngressGateway(router=router)

        imessage_alert = ShoulderTapAlert(
            alert_type="health_anomaly",
            source="nose",
            channel="iMessage;-;+15551234567",
            thread_ts="im-thread-1",
            summary="Backstage hand stalled on hermes.",
        )
        with patch("golem.notify.send_imessage", new_callable=AsyncMock) as mock_imsg, \
             patch("golem.notify.send_slack_message", new_callable=AsyncMock) as mock_slack:
            await gateway._handle_health_anomaly_alert(imessage_alert, client=None)
            mock_imsg.assert_awaited_once()
            mock_slack.assert_not_awaited()


class TestTurnProgressCoalescingAndSpinLoopPrevention(unittest.IsolatedAsyncioTestCase):
    """Changes 1 & 2: Single progress bubble per turn, chronological final briefing, and spin-loop guards."""

    async def test_multi_hop_turn_preserves_interim_ts_across_fast_second_hop(self):
        router = MagicMock()
        gateway = IngressGateway(router=router)
        session = MagicMock()
        session.session_id = "sess-1"
        session.target_cwd = "/tmp"
        session._active_backstage_hand = None
        client = MagicMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "999.001"})
        client.chat_update = AsyncMock(return_value={"ok": True, "ts": "111.222"})

        # Hop 1 dispatches a task that yielded interim_message_ts="111.222".
        # Hop 2 dispatches a fast verification task (<3s) where interim_message_ts=None.
        acc1 = BackstageExecutionAccumulator()
        acc1.turn_status = "SUCCESS"
        acc1.final_response = "Step 1 done"
        acc1.interim_message_ts = "111.222"

        acc2 = BackstageExecutionAccumulator()
        acc2.turn_status = "SUCCESS"
        acc2.final_response = "Step 2 verified"
        acc2.interim_message_ts = None

        call_responses = [
            # Initial call -> Hop 1 dispatch_task
            {
                "candidates": [{
                    "content": {
                        "parts": [{"functionCall": {"name": "dispatch_task", "args": {"task_description": "Run step 1"}}}]
                    }
                }]
            },
            # Chain Hop 2 -> dispatch_task (verification)
            {
                "candidates": [{
                    "content": {
                        "parts": [{"functionCall": {"name": "dispatch_task", "args": {"task_description": "Verify step 1"}}}]
                    }
                }]
            },
            # Chain Hop 3 -> terminal synthesis text
            {
                "candidates": [{
                    "content": {
                        "parts": [{"text": "*Complete:* Both steps verified cleanly."}]
                    }
                }]
            },
        ]

        with patch.object(gateway, "_call_gemini_api", AsyncMock(side_effect=call_responses)), \
             patch.object(gateway, "_execute_backstage_task", AsyncMock(side_effect=[acc1, acc2])):
            await gateway._run_mouth_turn(
                session=session,
                prompt="Run and verify",
                client=client,
                channel="C123",
                thread_ts="100.000",
                ts="100.001",
                is_imessage=False,
            )

        # Even though Hop 2 had interim_message_ts=None, Hop 1's interim_message_ts ("111.222")
        # must be updated in-place by the final briefing!
        client.chat_update.assert_awaited_once_with(
            channel="C123",
            ts="111.222",
            text="*Complete:* Both steps verified cleanly.",
        )

    async def test_intervening_messages_settle_interim_and_post_final_briefing_at_bottom(self):
        router = MagicMock()
        gateway = IngressGateway(router=router)
        session = MagicMock()
        session.session_id = "sess-2"
        session.target_cwd = "/tmp"
        session._active_backstage_hand = None
        client = MagicMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "999.002"})
        client.chat_update = AsyncMock(return_value={"ok": True, "ts": "111.333"})

        async def fake_execute_backstage(*args, **kwargs):
            # Simulate yielding "On it..." at seq=1, followed by an intervening message at seq=2
            await gateway._post_response(client, "iMessage;-;+1555000", "im-1", "⚙️ *On it:* Working...", True)
            session._turn_interim_ts = "111.333"
            session._turn_interim_seq = gateway._get_channel_outbound_seq("iMessage;-;+1555000")
            # Simulate an intervening mid-stream message posted to the channel before completion
            await gateway._post_response(client, "iMessage;-;+1555000", "im-1", "Still working on it...", True)
            acc = BackstageExecutionAccumulator()
            acc.turn_status = "SUCCESS"
            acc.final_response = "All items added"
            acc.interim_message_ts = "111.333"
            return acc

        call_responses = [
            {
                "candidates": [{
                    "content": {
                        "parts": [{"functionCall": {"name": "dispatch_task", "args": {"task_description": "Add items"}}}]
                    }
                }]
            },
            {
                "candidates": [{
                    "content": {
                        "parts": [{"text": "*Final Briefing:* All items added."}]
                    }
                }]
            },
        ]

        with patch.object(gateway, "_call_gemini_api", AsyncMock(side_effect=call_responses)), \
             patch.object(gateway, "_execute_backstage_task", side_effect=fake_execute_backstage):
            await gateway._run_mouth_turn(
                session=session,
                prompt="Add items",
                client=client,
                channel="iMessage;-;+1555000",
                thread_ts="im-1",
                ts="im-1",
                is_imessage=True,
            )

        # 1. Interim bubble "111.333" is settled in-place
        client.chat_update.assert_awaited_once()
        update_call = client.chat_update.await_args.kwargs
        self.assertEqual(update_call["ts"], "111.333")
        self.assertIn("Completed", update_call["text"])

        # 2. Final briefing is posted chronologically as a new message at the bottom
        last_post_kwargs = client.chat_postMessage.await_args_list[-1].kwargs
        self.assertEqual(last_post_kwargs["text"], "*Final Briefing:* All items added.")

    async def test_check_eyes_running_stops_chaining_to_prevent_spin_loop(self):
        router = MagicMock()
        gateway = IngressGateway(router=router)
        session = MagicMock()
        session.session_id = "sess-spin"
        session.target_cwd = "/tmp"
        client = MagicMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "200.1"})

        # Model calls check_eyes on Hop 1. Because state == "RUNNING", _run_mouth_turn
        # must NOT chain into Hop 2..10; it must go straight to Step 2 synthesis.
        initial_resp = {
            "candidates": [{
                "content": {
                    "parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]
                }
            }]
        }
        with patch.object(gateway, "_call_gemini_api", AsyncMock(return_value=initial_resp)) as mock_gemini, \
             patch.object(
                 gateway,
                 "_execute_check_eyes",
                 AsyncMock(return_value={"state": "RUNNING", "command": "sync_cart.py", "pid": 4321, "buffer_tail": " Working..."}),
             ) as mock_eyes, \
             patch.object(
                 gateway,
                 "_execute_step2_synthesis",
                 AsyncMock(return_value=("*Status:* Worker `sync_cart.py` is actively running.", {})),
             ) as mock_step2:
            await gateway._run_mouth_turn(
                session=session,
                prompt="continue",
                client=client,
                channel="C123",
                thread_ts="100.0",
                ts="100.2",
                is_imessage=False,
            )

        self.assertEqual(mock_eyes.await_count, 1)
        # _call_gemini_api called only once (initial Hop 1), never chained into Hop 2..10
        self.assertEqual(mock_gemini.await_count, 1)
        mock_step2.assert_awaited_once()
        self.assertNotIn("budget_note", mock_step2.await_args.kwargs)

    async def test_consecutive_read_only_hops_strips_read_only_tools(self):
        router = MagicMock()
        gateway = IngressGateway(router=router)
        session = MagicMock()
        session.session_id = "sess-ro"
        session.target_cwd = "/tmp"
        client = MagicMock()
        client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "300.1"})

        # 3 consecutive read_project_file hops, then pure text on hop 4
        responses = [
            {"candidates": [{"content": {"parts": [{"functionCall": {"name": "read_project_file", "args": {"path": "/tmp/a"}}}]}}]},
            {"candidates": [{"content": {"parts": [{"functionCall": {"name": "read_project_file", "args": {"path": "/tmp/b"}}}]}}]},
            {"candidates": [{"content": {"parts": [{"functionCall": {"name": "read_project_file", "args": {"path": "/tmp/c"}}}]}}]},
            {"candidates": [{"content": {"parts": [{"text": "Done reading files."}]}}]},
        ]
        captured_payloads = []

        async def capture_gemini(payload, model, api_key):
            captured_payloads.append(payload)
            return responses[len(captured_payloads) - 1]

        with patch.object(gateway, "_call_gemini_api", side_effect=capture_gemini), \
             patch.object(gateway, "_execute_tool_call", AsyncMock(return_value={"status": "SUCCESS", "content": "ok"})):
            await gateway._run_mouth_turn(
                session=session,
                prompt="Inspect files",
                client=client,
                channel="C123",
                thread_ts="100.0",
                ts="100.3",
                is_imessage=False,
            )

        self.assertEqual(len(captured_payloads), 4)
        # After MAX_CONSECUTIVE_READ_ONLY_HOPS (3), the 4th call's tools must not contain read-only tools
        hop4_decls = [
            d["name"]
            for group in captured_payloads[3]["tools"]
            for d in group.get("functionDeclarations", [])
        ]
        for ro_tool in READ_ONLY_HOP_TOOLS:
            self.assertNotIn(ro_tool, hop4_decls)
        self.assertIn("dispatch_task", hop4_decls)

    async def test_steering_notes_drained_upon_hand_completion(self):
        from golem.protocol import FinishEvent
        router = MagicMock()
        gateway = IngressGateway(router=router)
        session = MagicMock()
        session.session_id = "sess-steer"
        session.target_cwd = "/tmp"
        session.lock = asyncio.Lock()
        session._active_backstage_hand = None
        session.pending_steering_notes = []

        async def fake_stream(sess, task_desc):
            # Simulate user sending a steering note while the Hand is mid-flight
            sess.pending_steering_notes.append("You can do this yourself, check the Wiki")
            yield FinishEvent(status="SUCCESS", response="Initial inspection complete.")

        with patch.object(gateway, "_stream_backstage_events", side_effect=fake_stream):
            acc = await gateway._execute_backstage_task(
                task_desc="Inspect cart",
                target_node="hermes",
                target_cwd="/tmp",
                session=session,
                yield_timeout=3.0,
                worker="agy",
            )

        self.assertEqual(session.pending_steering_notes, [])
        self.assertEqual(getattr(acc, "steering_notes_drained", []), ["You can do this yourself, check the Wiki"])
        self.assertIn("[MID-STREAM OPERATOR STEERING RECEIVED WHILE HAND WAS RUNNING]:", acc.final_response)
        self.assertIn("You can do this yourself, check the Wiki", acc.final_response)

    def test_cohesive_delegation_prompt_rules(self):
        from golem.mouth import BASE_CHIEF_OF_STAFF_INSTRUCTION, DISPATCH_TASK_TOOL_DECLARATION
        self.assertIn("COHESIVE HAND DELEGATION", BASE_CHIEF_OF_STAFF_INSTRUCTION)
        self.assertIn("SINGLE-SNAPSHOT SENSORY PROBES", BASE_CHIEF_OF_STAFF_INSTRUCTION)
        self.assertIn("crontab -l", DISPATCH_TASK_TOOL_DECLARATION["description"])


if __name__ == "__main__":
    unittest.main()
