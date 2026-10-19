#!/usr/bin/env python3
"""
tests/test_nose_linear_tickets.py
Unit and integration tests for Linear assigned-ticket watchdog in The Nose,
Shoulder Tap Event Bus alerting, and Mouth liaison prompt synthesis.
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
import time

from golem.linear_bridge import LinearBridge
from golem.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.nose import NoseSentinel
from golem.mouth import synthesize_assigned_tickets_prompt
from golem.ingress import IngressGateway


class TestLinearBridgeAssignedIssues(unittest.IsolatedAsyncioTestCase):
    """Test LinearBridge.get_assigned_issues and viewer resolution."""

    async def test_get_viewer_caching(self) -> None:
        bridge = LinearBridge(api_key="mock-api-key")
        mock_viewer = {
            "id": "hermes-id-123",
            "name": "Hermes",
            "displayName": "hermes.horned",
            "email": "hermes.horned@gmail.com",
        }
        with patch.object(bridge, "execute_query", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = {"viewer": mock_viewer}
            viewer = await bridge.get_viewer()
            self.assertEqual(viewer["id"], "hermes-id-123")
            self.assertEqual(bridge._viewer_id_cache, "hermes-id-123")

            # Second call uses cache without query
            mock_exec.reset_mock()
            viewer2 = await bridge.get_viewer()
            self.assertEqual(viewer2["id"], "hermes-id-123")
            mock_exec.assert_not_called()

    async def test_get_assigned_issues_filters_done_and_carl(self) -> None:
        bridge = LinearBridge(api_key="mock-api-key")
        bridge._viewer_id_cache = "hermes-id-123"

        raw_nodes = [
            {
                "id": "issue-1",
                "identifier": "CHO-197",
                "title": "TikTok-2-HEB Skill",
                "url": "https://linear.app/chorned/issue/CHO-197",
                "updatedAt": "2026-10-17T01:47:47Z",
                "state": {"id": "s1", "name": "Todo", "type": "unstarted"},
                "project": {"id": "p1", "name": "Golem"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": {
                    "id": "hermes-id-123",
                    "name": "Hermes",
                    "displayName": "hermes.horned",
                    "email": "hermes.horned@gmail.com",
                },
            },
            {
                "id": "issue-2",
                "identifier": "CHO-99",
                "title": "Fix release update notes",
                "url": "https://linear.app/chorned/issue/CHO-99",
                "updatedAt": "2026-09-09T01:30:15Z",
                "state": {"id": "s2", "name": "Canceled", "type": "canceled"},
                "project": {"id": "p2", "name": "FrugaLLM"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": {
                    "id": "hermes-id-123",
                    "name": "Hermes",
                    "displayName": "hermes.horned",
                    "email": "hermes.horned@gmail.com",
                },
            },
            {
                "id": "issue-3",
                "identifier": "CHO-88",
                "title": "Old done task",
                "url": "https://linear.app/chorned/issue/CHO-88",
                "updatedAt": "2026-09-01T00:00:00Z",
                "state": {"id": "s3", "name": "Done", "type": "completed"},
                "project": {"id": "p2", "name": "FrugaLLM"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": {
                    "id": "hermes-id-123",
                    "name": "Hermes",
                    "displayName": "hermes.horned",
                    "email": "hermes.horned@gmail.com",
                },
            },
            {
                "id": "issue-4",
                "identifier": "CHO-50",
                "title": "Task for Carl",
                "url": "https://linear.app/chorned/issue/CHO-50",
                "updatedAt": "2026-10-10T00:00:00Z",
                "state": {"id": "s4", "name": "In Progress", "type": "started"},
                "project": {"id": "p1", "name": "Golem"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": {
                    "id": "carl-id-456",
                    "name": "Carl Horned",
                    "displayName": "k.horned",
                    "email": "k.horned@gmail.com",
                },
            },
            {
                "id": "issue-5",
                "identifier": "CHO-40",
                "title": "Unassigned task",
                "url": "https://linear.app/chorned/issue/CHO-40",
                "updatedAt": "2026-10-10T00:00:00Z",
                "state": {"id": "s1", "name": "Todo", "type": "unstarted"},
                "project": {"id": "p1", "name": "Golem"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": None,
            },
        ]

        with patch.object(bridge, "execute_query", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = {"issues": {"nodes": raw_nodes}}

            # non_done_only = True
            non_done = await bridge.get_assigned_issues(non_done_only=True)
            self.assertEqual(len(non_done), 2)
            keys = [i["identifier"] for i in non_done]
            self.assertIn("CHO-197", keys)
            self.assertIn("CHO-99", keys)
            self.assertNotIn("CHO-88", keys)  # Done filtered out
            self.assertNotIn("CHO-50", keys)  # Carl filtered out
            self.assertNotIn("CHO-40", keys)  # Unassigned filtered out

            # non_done_only = False
            all_hermes = await bridge.get_assigned_issues(non_done_only=False)
            self.assertEqual(len(all_hermes), 3)
            all_keys = [i["identifier"] for i in all_hermes]
            self.assertIn("CHO-88", all_keys)

    async def test_get_assigned_issues_empty_when_no_api_key(self) -> None:
        bridge = LinearBridge(api_key="")
        issues = await bridge.get_assigned_issues()
        self.assertEqual(issues, [])

    async def test_get_assigned_issues_includes_unassigned_in_progress_golem_plans(self) -> None:
        bridge = LinearBridge(api_key="mock-api-key")
        bridge._viewer_id_cache = "hermes-id-123"

        raw_nodes = [
            # 1. Unassigned top-level In Progress Golem plan issue -> SHOULD BE INCLUDED
            {
                "id": "issue-214",
                "identifier": "CHO-214",
                "title": "Unassigned Golem Plan Deliverable",
                "url": "https://linear.app/chorned/issue/CHO-214",
                "updatedAt": "2026-10-17T02:00:00Z",
                "state": {"id": "s4", "name": "In Progress", "type": "started"},
                "project": {"id": "p1", "name": "Golem_Brain"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": None,
                "parent": None,
            },
            # 2. Unassigned Todo issue -> FILTERED OUT
            {
                "id": "issue-40",
                "identifier": "CHO-40",
                "title": "Unassigned Todo Task",
                "url": "https://linear.app/chorned/issue/CHO-40",
                "updatedAt": "2026-10-10T00:00:00Z",
                "state": {"id": "s1", "name": "Todo", "type": "unstarted"},
                "project": {"id": "p1", "name": "Golem"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": None,
                "parent": None,
            },
            # 3. Unassigned [Hand] breadcrumb -> FILTERED OUT
            {
                "id": "issue-215",
                "identifier": "CHO-215",
                "title": "[Hand] Subagent task breadcrumb",
                "url": "https://linear.app/chorned/issue/CHO-215",
                "updatedAt": "2026-10-17T02:05:00Z",
                "state": {"id": "s4", "name": "In Progress", "type": "started"},
                "project": {"id": "p1", "name": "Golem_Brain"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": None,
                "parent": None,
            },
            # 4. Unassigned child subtask -> FILTERED OUT
            {
                "id": "issue-216",
                "identifier": "CHO-216",
                "title": "Child Subtask",
                "url": "https://linear.app/chorned/issue/CHO-216",
                "updatedAt": "2026-10-17T02:10:00Z",
                "state": {"id": "s4", "name": "In Progress", "type": "started"},
                "project": {"id": "p1", "name": "Golem_Brain"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": None,
                "parent": {"id": "issue-214", "identifier": "CHO-214"},
            },
        ]

        with patch.object(bridge, "execute_query", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = {"issues": {"nodes": raw_nodes}}
            issues = await bridge.get_assigned_issues(non_done_only=True)
            keys = [i["identifier"] for i in issues]
            self.assertIn("CHO-214", keys)
            self.assertNotIn("CHO-40", keys)
            self.assertNotIn("CHO-215", keys)
            self.assertNotIn("CHO-216", keys)
            self.assertEqual(len(issues), 1)

    async def test_get_assigned_issues_active_child_under_claimed_fixed_parent_is_returned(self) -> None:
        """Verify an active Todo or In Progress child issue under a Claimed Fixed parent is returned and not dropped."""
        bridge = LinearBridge(api_key="mock-api-key")
        bridge._viewer_id_cache = "hermes-id-123"

        raw_nodes = [
            # 1. Parent in Claimed Fixed state (settled deliverable)
            {
                "id": "issue-parent-207",
                "identifier": "CHO-207",
                "title": "Project GOAT-GOTY",
                "url": "https://linear.app/chorned/issue/CHO-207",
                "updatedAt": "2026-10-17T02:00:00Z",
                "state": {"id": "s-cf", "name": "Claimed Fixed", "type": "started"},
                "project": {"id": "p1", "name": "Golem"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": {
                    "id": "hermes-id-123",
                    "name": "Hermes",
                    "displayName": "hermes.horned",
                    "email": "hermes.horned@gmail.com",
                },
                "parent": None,
                "children": {
                    "nodes": [
                        {
                            "id": "issue-child-233",
                            "identifier": "CHO-233",
                            "title": "Issue when running Oni",
                            "state": {"id": "s-todo", "name": "Todo", "type": "unstarted"},
                        },
                        {
                            "id": "issue-child-220",
                            "identifier": "CHO-220",
                            "title": "Old Phase 1",
                            "state": {"id": "s-cf", "name": "Claimed Fixed", "type": "started"},
                        },
                    ]
                },
            },
            # 2. Active Todo child issue assigned to Hermes under settled parent CHO-207
            {
                "id": "issue-child-233",
                "identifier": "CHO-233",
                "title": "Issue when running Oni",
                "url": "https://linear.app/chorned/issue/CHO-233",
                "updatedAt": "2026-10-17T02:10:00Z",
                "state": {"id": "s-todo", "name": "Todo", "type": "unstarted"},
                "project": {"id": "p1", "name": "Golem"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": {
                    "id": "hermes-id-123",
                    "name": "Hermes",
                    "displayName": "hermes.horned",
                    "email": "hermes.horned@gmail.com",
                },
                "parent": {
                    "id": "issue-parent-207",
                    "identifier": "CHO-207",
                    "title": "Project GOAT-GOTY",
                    "state": {"id": "s-cf", "name": "Claimed Fixed", "type": "started"},
                },
            },
            # 3. Active In Progress child issue assigned to Hermes under settled parent CHO-207
            {
                "id": "issue-child-235",
                "identifier": "CHO-235",
                "title": "WASM memory leak investigation",
                "url": "https://linear.app/chorned/issue/CHO-235",
                "updatedAt": "2026-10-17T02:15:00Z",
                "state": {"id": "s-ip", "name": "In Progress", "type": "started"},
                "project": {"id": "p1", "name": "Golem"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": {
                    "id": "hermes-id-123",
                    "name": "Hermes",
                    "displayName": "hermes.horned",
                    "email": "hermes.horned@gmail.com",
                },
                "parent": {
                    "id": "issue-parent-207",
                    "identifier": "CHO-207",
                    "title": "Project GOAT-GOTY",
                    "state": {"id": "s-cf", "name": "Claimed Fixed", "type": "started"},
                },
            },
            # 4. Settled child issue under settled parent CHO-207 -> SHOULD BE EXCLUDED
            {
                "id": "issue-child-220",
                "identifier": "CHO-220",
                "title": "Old Phase 1",
                "url": "https://linear.app/chorned/issue/CHO-220",
                "updatedAt": "2026-10-17T01:00:00Z",
                "state": {"id": "s-cf", "name": "Claimed Fixed", "type": "started"},
                "project": {"id": "p1", "name": "Golem"},
                "team": {"id": "t1", "key": "CHO", "name": "Chorned"},
                "assignee": {
                    "id": "hermes-id-123",
                    "name": "Hermes",
                    "displayName": "hermes.horned",
                    "email": "hermes.horned@gmail.com",
                },
                "parent": {
                    "id": "issue-parent-207",
                    "identifier": "CHO-207",
                    "title": "Project GOAT-GOTY",
                    "state": {"id": "s-cf", "name": "Claimed Fixed", "type": "started"},
                },
            },
        ]

        with patch.object(bridge, "execute_query", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = {"issues": {"nodes": raw_nodes}}
            issues = await bridge.get_assigned_issues(non_done_only=True)
            keys = [i["identifier"] for i in issues]

            # Both active child issues must be returned
            self.assertIn("CHO-233", keys)
            self.assertIn("CHO-235", keys)
            # Settled parent and settled child must be excluded
            self.assertNotIn("CHO-207", keys)
            self.assertNotIn("CHO-220", keys)
            self.assertEqual(len(issues), 2)



class TestNoseLinearWatchdogAndAlerts(unittest.IsolatedAsyncioTestCase):
    """Test NoseSentinel integration with Linear assigned tickets and Shoulder Tap bus."""

    async def test_sniff_assigned_tickets_and_telemetry(self) -> None:
        mock_bridge = MagicMock()
        mock_issues = [
            {
                "id": "id-1",
                "identifier": "CHO-197",
                "title": "TikTok-2-HEB Skill",
                "state": "Todo",
                "project": "Golem",
                "url": "https://linear.app/chorned/issue/CHO-197",
            }
        ]
        mock_bridge.get_assigned_issues = AsyncMock(return_value=mock_issues)

        bus = ShoulderTapEventBus()
        sentinel = NoseSentinel(bus=bus, linear_bridge=mock_bridge)

        tickets = await sentinel.sniff_assigned_tickets()
        self.assertEqual(len(tickets), 1)
        self.assertEqual(tickets[0]["identifier"], "CHO-197")

        sentinel._last_assigned_tickets = tickets
        telemetry = await sentinel.get_telemetry_digest()
        self.assertIn("assigned_tickets", telemetry)
        self.assertIn("linear_assigned_tickets", telemetry)
        self.assertEqual(len(telemetry["assigned_tickets"]), 1)
        self.assertEqual(telemetry["assigned_tickets"][0]["identifier"], "CHO-197")

    async def test_tick_emits_linear_assigned_tickets_alert(self) -> None:
        mock_bridge = MagicMock()
        mock_issues = [
            {
                "id": "id-1",
                "identifier": "CHO-197",
                "title": "TikTok-2-HEB Skill",
                "state": "Todo",
                "project": "Golem",
                "url": "https://linear.app/chorned/issue/CHO-197",
            }
        ]
        mock_bridge.get_assigned_issues = AsyncMock(return_value=mock_issues)

        bus = ShoulderTapEventBus()
        received_alerts: list[ShoulderTapAlert] = []
        bus.subscribe(lambda a: received_alerts.append(a), alert_types=["linear_assigned_tickets"])

        sentinel = NoseSentinel(bus=bus, linear_bridge=mock_bridge)
        sentinel.sniff_bot_err = MagicMock(return_value=([], ""))
        sentinel.sniff_fleet = AsyncMock(return_value=None)
        sentinel.compute_status_deltas = MagicMock(return_value=[])
        sentinel.sniff_emails = AsyncMock(return_value=[])

        alert = await sentinel.tick()
        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "linear_assigned_tickets")
        self.assertTrue(alert.is_linear_assigned_tickets)
        self.assertEqual(len(received_alerts), 1)
        self.assertEqual(received_alerts[0].alert_type, "linear_assigned_tickets")
        self.assertIn("CHO-197", received_alerts[0].summary)
        self.assertEqual(received_alerts[0].data["count"], 1)

        # Subsequent tick with same ticket signature does not duplicate alert
        await sentinel.tick()
        self.assertEqual(len(received_alerts), 1)

        # Ticket status change triggers new alert
        mock_issues_updated = [
            {
                "id": "id-1",
                "identifier": "CHO-197",
                "title": "TikTok-2-HEB Skill",
                "state": "In Progress",
                "project": "Golem",
                "url": "https://linear.app/chorned/issue/CHO-197",
            }
        ]
        mock_bridge.get_assigned_issues = AsyncMock(return_value=mock_issues_updated)
        await sentinel.tick()
        self.assertEqual(len(received_alerts), 2)
        self.assertEqual(received_alerts[1].data["tickets"][0]["state"], "In Progress")


class TestMouthPromptSynthesisAndIngressHandler(unittest.IsolatedAsyncioTestCase):
    """Test Mouth prompt synthesis and IngressGateway sensory alert handling."""

    def test_synthesize_assigned_tickets_prompt(self) -> None:
        tickets = [
            {
                "identifier": "CHO-197",
                "title": "TikTok-2-HEB Skill",
                "state": "Todo",
                "project": "Golem",
                "url": "https://linear.app/chorned/issue/CHO-197",
                "subtasks": [
                    {"identifier": "CHO-198", "title": "[1/3] Setup HEB auth", "settled": False},
                    {"identifier": "CHO-199", "title": "[2/3] Parse TikTok recipe", "settled": False},
                ],
            },
            {
                "identifier": "CHO-214",
                "title": "Audiobookshelf Audit",
                "state": "In Progress",
                "project": "Golem",
                "url": "https://linear.app/chorned/issue/CHO-214",
                "subtasks": [
                    {"identifier": "CHO-215", "title": "[1/2] Inventory series", "settled": True},
                    {"identifier": "CHO-216", "title": "[2/2] Extract durations", "settled": False},
                ],
            },
        ]
        prompt = synthesize_assigned_tickets_prompt(tickets)
        self.assertIn("Linear Attention Required", prompt)
        self.assertIn("CHO-197", prompt)
        self.assertIn("TikTok-2-HEB Skill", prompt)
        self.assertIn("CHO-214", prompt)
        # Natural context-aware CTAs instead of robotic boilerplate
        self.assertNotIn("Would you like me to *start*, *continue*, *retry*, or *cancel* this ticket?", prompt)
        self.assertIn("*kick off Phase 1*", prompt)
        self.assertIn("*pick back up where we left off on CHO-216*", prompt)

    def test_synthesize_assigned_tickets_prompt_empty(self) -> None:
        self.assertEqual(synthesize_assigned_tickets_prompt([]), "")

    async def test_ingress_gateway_handles_linear_assigned_tickets_alert(self) -> None:
        bus = ShoulderTapEventBus()
        mock_router = MagicMock()
        mock_router.bus = bus
        mock_router.sessions = {}
        mock_router._registry = {}

        gw = IngressGateway(router=mock_router, bus=bus)

        mock_client = MagicMock()
        mock_client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "123.456"})

        tickets = [
            {
                "identifier": "CHO-197",
                "title": "TikTok-2-HEB Skill",
                "state": "Todo",
                "project": "Golem",
            }
        ]
        alert = ShoulderTapAlert(
            alert_type="linear_assigned_tickets",
            source="nose",
            channel="C_TEST",
            data={"tickets": tickets, "count": 1},
        )

        await gw.process_sensory_alert(alert, client=mock_client)
        mock_client.chat_postMessage.assert_called_once()
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        self.assertEqual(call_kwargs["channel"], "C_TEST")
        self.assertIn("CHO-197", call_kwargs["text"])
        self.assertNotIn("*start*, *continue*, *retry*, or *cancel*", call_kwargs["text"])
        self.assertIn("*kick off Phase 1*", call_kwargs["text"])

    async def test_one_thread_per_ticket_never_bundles_multiple_tickets(self) -> None:
        """Proactive nudges must post each parent ticket to its own 1:1 thread (or reuse existing thread)."""
        from golem.ingress import _THREAD_PLAN_CACHE

        bus = ShoulderTapEventBus()
        mock_router = MagicMock()
        mock_router.bus = bus
        mock_router.sessions = {}
        mock_router._registry = {}

        gw = IngressGateway(router=mock_router, bus=bus)

        posted_messages = []

        async def fake_post(**kwargs):
            ts_val = f"ts_{len(posted_messages) + 1}.000"
            posted_messages.append((ts_val, kwargs))
            return {"ok": True, "ts": ts_val}

        mock_client = MagicMock()
        mock_client.chat_postMessage = AsyncMock(side_effect=fake_post)

        tickets = [
            {
                "identifier": "CHO-214",
                "title": "Audiobookshelf Audit",
                "state": "In Progress",
                "subtasks": [
                    {"identifier": "CHO-215", "title": "Inventory", "settled": True},
                    {"identifier": "CHO-216", "title": "Durations", "settled": False},
                ],
            },
            {
                "identifier": "CHO-207",
                "title": "Project GOAT-GOTY",
                "state": "In Progress",
                "subtasks": [
                    {"identifier": "CHO-208", "title": "Phase 1", "settled": False},
                ],
            },
        ]
        alert = ShoulderTapAlert(
            alert_type="linear_assigned_tickets",
            source="nose",
            channel="C_TEST",
            data={"tickets": tickets, "count": 2},
        )

        try:
            await gw.process_sensory_alert(alert, client=mock_client)
            self.assertEqual(len(posted_messages), 2)
            # First message is strictly about CHO-214
            self.assertIn("CHO-214", posted_messages[0][1]["text"])
            self.assertNotIn("CHO-207", posted_messages[0][1]["text"])
            # Second message is strictly about CHO-207
            self.assertIn("CHO-207", posted_messages[1][1]["text"])
            self.assertNotIn("CHO-214", posted_messages[1][1]["text"])
            # Each thread cache entry holds ONLY its own ticket
            self.assertEqual(
                [t["identifier"] for t in _THREAD_PLAN_CACHE["C_TEST:ts_1.000"]["linear_tickets_context"]],
                ["CHO-214"],
            )
            self.assertEqual(
                [t["identifier"] for t in _THREAD_PLAN_CACHE["C_TEST:ts_2.000"]["linear_tickets_context"]],
                ["CHO-207"],
            )
            # Channel-global latest_linear_alert key must NEVER be written
            self.assertNotIn("C_TEST:latest_linear_alert", _THREAD_PLAN_CACHE)
        finally:
            _THREAD_PLAN_CACHE.pop("C_TEST:ts_1.000", None)
            _THREAD_PLAN_CACHE.pop("C_TEST:ts_2.000", None)


class TestActiveWorkDetectionForLinearTickets(unittest.IsolatedAsyncioTestCase):
    """
    Tests for Deterministic Active Work Detection in The Nose & Ingress Gateway.
    Verifies Signal A, Signal B, and Signal C ignore actively worked In Progress tickets,
    while stalled/idle tickets still trigger alerts.
    """

    def setUp(self) -> None:
        self.now = 1792200000.0  # Deterministic reference timestamp

    def test_non_in_progress_tickets_not_actively_worked(self) -> None:
        from golem.nose import is_ticket_actively_worked

        todo_ticket = {"identifier": "CHO-197", "title": "TikTok-2-HEB", "state": "Todo"}
        done_ticket = {"identifier": "CHO-197", "title": "TikTok-2-HEB", "state": {"name": "Done", "type": "completed"}}
        canceled_ticket = {"identifier": "CHO-197", "title": "TikTok-2-HEB", "state": "Canceled"}

        # Even with active eyes runner, non-In-Progress tickets return False
        mock_eyes = MagicMock(is_running=True)
        self.assertFalse(is_ticket_actively_worked(todo_ticket, eyes_observer=mock_eyes, now=self.now))
        self.assertFalse(is_ticket_actively_worked(done_ticket, eyes_observer=mock_eyes, now=self.now))
        self.assertFalse(is_ticket_actively_worked(canceled_ticket, eyes_observer=mock_eyes, now=self.now))

    def test_stalled_in_progress_ticket_returns_false_and_alerts(self) -> None:
        from golem.nose import is_ticket_actively_worked

        stalled_ticket = {
            "identifier": "CHO-197",
            "title": "TikTok-2-HEB",
            "state": "In Progress",
            "updatedAt": "2026-09-01T00:00:00Z",  # Old update
        }

        # No active sessions, no eyes observer, no runner
        self.assertFalse(is_ticket_actively_worked(stalled_ticket, session_router=None, eyes_observer=None, now=self.now))

    async def test_signal_a_in_memory_session_suppresses_alert(self) -> None:
        from golem.nose import is_ticket_actively_worked

        ticket = {
            "identifier": "CHO-197",
            "title": "TikTok-2-HEB",
            "state": "In Progress",
            "updatedAt": "2026-09-01T00:00:00Z",
        }

        # Mock session with matching linear_issue_key and is_busy=True
        mock_session = MagicMock()
        mock_session.linear_issue_key = "CHO-197"
        mock_session.active_linear_parent_key = None
        mock_session.is_busy = True
        mock_session.last_activity = self.now

        mock_router = MagicMock()
        mock_router.sessions = {"s1": mock_session}

        self.assertTrue(is_ticket_actively_worked(ticket, session_router=mock_router, now=self.now))

        # Test Sentinel tick suppression
        mock_bridge = MagicMock()
        mock_bridge.get_assigned_issues = AsyncMock(return_value=[ticket])

        bus = ShoulderTapEventBus()
        received_alerts: list[ShoulderTapAlert] = []
        bus.subscribe(lambda a: received_alerts.append(a), alert_types=["linear_assigned_tickets"])

        sentinel = NoseSentinel(bus=bus, linear_bridge=mock_bridge, session_router=mock_router)
        sentinel.sniff_bot_err = MagicMock(return_value=([], ""))
        sentinel.sniff_fleet = AsyncMock(return_value=None)
        sentinel.compute_status_deltas = MagicMock(return_value=[])
        sentinel.sniff_emails = AsyncMock(return_value=[])

        alert = await sentinel.tick()
        self.assertIsNone(alert)
        self.assertEqual(len(received_alerts), 0)

    async def test_sibling_ticket_starting_does_not_re_nudge_queued_ticket(self) -> None:
        """
        Regression test for the 7:42 AM bug:
        1. CHO-214 and CHO-207 are both alerted at 12:25 AM.
        2. At 7:40 AM, CHO-214 starts running first while CHO-207 is queued in the batch.
        3. At 7:42 AM, Nose ticks again: CHO-207 must NOT fire a duplicate alert even if
           CHO-214 becomes active or CHO-207 is waiting in queued_subtasks.
        """
        mock_bridge = MagicMock()
        t_214 = {"identifier": "CHO-214", "title": "ABS Audit", "state": "In Progress"}
        t_207 = {"identifier": "CHO-207", "title": "Project GOAT-GOTY", "state": "In Progress"}
        mock_bridge.get_assigned_issues = AsyncMock(return_value=[t_214, t_207])

        bus = ShoulderTapEventBus()
        received_alerts: list[ShoulderTapAlert] = []
        bus.subscribe(lambda a: received_alerts.append(a), alert_types=["linear_assigned_tickets"])

        mock_router = MagicMock()
        mock_router.sessions = {}
        sentinel = NoseSentinel(bus=bus, linear_bridge=mock_bridge, session_router=mock_router)
        sentinel.sniff_bot_err = MagicMock(return_value=([], ""))
        sentinel.sniff_fleet = AsyncMock(return_value=None)
        sentinel.compute_status_deltas = MagicMock(return_value=[])
        sentinel.sniff_emails = AsyncMock(return_value=[])

        # Tick 1 (12:25 AM): Both CHO-214 and CHO-207 are alerted
        alert1 = await sentinel.tick()
        self.assertIsNotNone(alert1)
        self.assertEqual(len(received_alerts), 1)

        # Tick 2 (7:42 AM): CHO-214 is now active, and CHO-207's signature was already alerted
        mock_session = MagicMock()
        mock_session.linear_issue_key = "CHO-214"
        mock_session.is_busy = True
        mock_session.last_active_ts = time.time()
        mock_router.sessions = {"s1": mock_session}

        alert2 = await sentinel.tick()
        self.assertIsNone(alert2)
        self.assertEqual(len(received_alerts), 1)

    async def test_signal_a_disk_registry_suppresses_alert(self) -> None:
        import tempfile
        import json
        from golem.nose import is_ticket_actively_worked

        ticket = {
            "identifier": "CHO-197",
            "title": "TikTok-2-HEB Skill",
            "state": "In Progress",
            "updatedAt": "2026-09-01T00:00:00Z",
        }

        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tf:
            reg_data = {
                "C123:456": {
                    "conversation_id": "test-conv",
                    "active_linear_parent_key": "CHO-197",
                    "target_cwd": "/Users/hermes/projects/AGY",
                    "updated_at": self.now - 300,  # 5 min ago
                    "is_busy": False,
                }
            }
            json.dump(reg_data, tf)
            tf_path = tf.name

        try:
            self.assertTrue(is_ticket_actively_worked(ticket, registry_path=tf_path, now=self.now))
        finally:
            import os
            if os.path.exists(tf_path):
                os.remove(tf_path)

    def test_signal_b_active_eyes_runner_suppresses_alert(self) -> None:
        from golem.nose import is_ticket_actively_worked

        ticket = {
            "identifier": "CHO-197",
            "title": "TikTok-2-HEB",
            "state": "In Progress",
        }

        mock_eyes = MagicMock()
        mock_eyes.is_running = True
        mock_eyes._last_emitted_state = "RUNNING"

        self.assertTrue(is_ticket_actively_worked(ticket, eyes_observer=mock_eyes, now=self.now))

    def test_signal_b_active_process_suppresses_alert(self) -> None:
        from golem.nose import is_ticket_actively_worked

        ticket = {
            "identifier": "CHO-197",
            "title": "TikTok-2-HEB",
            "state": "In Progress",
        }

        mock_ps = "1234 python3 -m unittest /Users/hermes/projects/TikTok-2-HEB/tests/test_core.py\n5678 /bin/zsh"
        with patch("subprocess.check_output", return_value=mock_ps):
            self.assertTrue(is_ticket_actively_worked(ticket, now=self.now))

    def test_signal_b_recent_workspace_files_suppresses_alert(self) -> None:
        import tempfile
        import os
        from datetime import datetime, timezone
        from golem.nose import is_ticket_actively_worked

        with tempfile.TemporaryDirectory() as tmp_ws:
            test_file = os.path.join(tmp_ws, "client.py")
            with open(test_file, "w") as f:
                f.write("print('hello')")
            # Set mtime to 3 minutes ago
            os.utime(test_file, (self.now - 180, self.now - 180))

            recent_iso = datetime.fromtimestamp(self.now - 200, tz=timezone.utc).isoformat()
            ticket = {
                "identifier": "CHO-197",
                "title": "Build Test Project",
                "description": f"Working in {tmp_ws}",
                "state": "In Progress",
                "updatedAt": recent_iso,
            }

            self.assertTrue(is_ticket_actively_worked(ticket, now=self.now))

    def test_signal_c_recent_grace_window(self) -> None:
        from datetime import datetime, timezone
        from golem.nose import is_ticket_actively_worked

        recent_iso = datetime.fromtimestamp(self.now - 300, tz=timezone.utc).isoformat()
        ticket = {
            "identifier": "CHO-888",
            "title": "Isolated Test Ticket",
            "state": "In Progress",
            "updatedAt": recent_iso,
        }

        # Active runner during 20m grace window
        mock_eyes = MagicMock(is_running=True)
        self.assertTrue(is_ticket_actively_worked(ticket, eyes_observer=mock_eyes, now=self.now))

        # Stale update (25 minutes ago)
        stale_iso = datetime.fromtimestamp(self.now - 1500, tz=timezone.utc).isoformat()
        stale_ticket = {
            "identifier": "CHO-888",
            "title": "Isolated Test Ticket",
            "state": "In Progress",
            "updatedAt": stale_iso,
        }
        self.assertFalse(is_ticket_actively_worked(stale_ticket, eyes_observer=None, now=self.now))

    async def test_ingress_suppresses_actively_worked_ticket(self) -> None:
        bus = ShoulderTapEventBus()
        mock_router = MagicMock()
        mock_session = MagicMock()
        mock_session.linear_issue_key = "CHO-197"
        mock_session.is_busy = True
        mock_router.sessions = {"s1": mock_session}

        gw = IngressGateway(router=mock_router, bus=bus)

        mock_client = MagicMock()
        mock_client.chat_postMessage = AsyncMock()

        tickets = [
            {
                "identifier": "CHO-197",
                "title": "TikTok-2-HEB Skill",
                "state": "In Progress",
                "project": "Golem",
            }
        ]
        alert = ShoulderTapAlert(
            alert_type="linear_assigned_tickets",
            source="nose",
            channel="C_TEST",
            data={"tickets": tickets, "count": 1},
        )

        await gw.process_sensory_alert(alert, client=mock_client)
        # Suppressed because CHO-197 is actively being worked on in mock_session
        mock_client.chat_postMessage.assert_not_called()

    def test_backlog_or_todo_ticket_bound_to_busy_session_is_actively_worked(self) -> None:
        from golem.nose import is_ticket_actively_worked

        todo_ticket = {
            "identifier": "CHO-207",
            "title": "Podcast Bot Delivery",
            "state": "Todo",
        }
        backlog_ticket = {
            "identifier": "CHO-207",
            "title": "Podcast Bot Delivery",
            "state": "Backlog",
        }

        mock_session = MagicMock()
        mock_session.linear_issue_key = "CHO-207"
        mock_session.is_busy = True
        mock_session.last_activity = self.now

        mock_router = MagicMock()
        mock_router.sessions = {"s1": mock_session}

        self.assertTrue(is_ticket_actively_worked(todo_ticket, session_router=mock_router, now=self.now))
        self.assertTrue(is_ticket_actively_worked(backlog_ticket, session_router=mock_router, now=self.now))

        # Without busy session, Todo/Backlog ticket returns False
        mock_session.is_busy = False
        mock_session.state = "idle"
        self.assertFalse(is_ticket_actively_worked(todo_ticket, session_router=mock_router, now=self.now))

    async def test_tick_suppresses_backlog_ticket_tracked_by_in_progress_plan(self) -> None:
        mock_bridge = MagicMock()
        mock_issues = [
            {
                "id": "id-207",
                "identifier": "CHO-207",
                "title": "Original Backlog Request",
                "description": "Please build the podcast pipeline",
                "state": "Backlog",
                "project": "Golem",
                "url": "https://linear.app/chorned/issue/CHO-207",
            },
            {
                "id": "id-219",
                "identifier": "CHO-219",
                "title": "Execute Plan for CHO-207",
                "description": "Tracking plan for CHO-207 subtasks",
                "state": "In Progress",
                "project": "Golem",
                "url": "https://linear.app/chorned/issue/CHO-219",
            },
        ]
        mock_bridge.get_assigned_issues = AsyncMock(return_value=mock_issues)

        bus = ShoulderTapEventBus()
        sentinel = NoseSentinel(bus=bus, linear_bridge=mock_bridge)
        sentinel.sniff_bot_err = MagicMock(return_value=([], ""))
        sentinel.sniff_fleet = AsyncMock(return_value=None)
        sentinel.compute_status_deltas = MagicMock(return_value=[])
        sentinel.sniff_emails = AsyncMock(return_value=[])

        # CHO-219 is stalled, but CHO-207 should be suppressed because CHO-219 is In Progress and references CHO-207
        alert = await sentinel.tick()
        self.assertIsNotNone(alert)
        self.assertEqual(alert.data["count"], 1)
        self.assertEqual(alert.data["tickets"][0]["identifier"], "CHO-219")


if __name__ == "__main__":
    unittest.main()

