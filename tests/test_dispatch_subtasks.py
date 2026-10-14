"""
Unit and integration tests for dispatch_subtasks, WebUI multi-agent identity collision fix,
out-of-band grounding from _active_hands, and Audiobookshelf read-only subtask scoping.
"""

import asyncio
import json
import socket
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from golem.ingress import (
    IngressGateway,
    DISPATCH_SUBTASKS_TOOL_DECLARATION,
    get_mouth_tools_declaration,
    execute_tool,
)
from golem.mouth import (
    MOUTH_TOOLS,
    BASE_CHIEF_OF_STAFF_INSTRUCTION,
    generate_mouth_response,
)
from golem.webui.server import WebUIServer
from golem.session import SessionHandle, SessionState


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestWebUIIdentityCollisionFix(unittest.TestCase):
    """Task 1: Verify WebUI identity collision fix in WebUIServer."""

    def setUp(self):
        self.port = _get_free_port()
        self.webui = WebUIServer(host="127.0.0.1", port=self.port)

    def tearDown(self):
        if self.webui.is_running:
            asyncio.run(self.webui.stop())

    def test_distinct_pane_keys_on_same_node(self):
        """Workers on the same node with distinct pane_ids generate distinct registry keys."""
        k1 = self.webui._allocate_hand_key(pane_id="hand_CHO-155", node="oci-machine")
        self.webui._active_hands[k1] = {
            "pane_id": "hand_CHO-155",
            "node": "oci-machine",
            "herdr_pane": "hand_CHO-155",
            "status": "RUNNING",
        }

        k2 = self.webui._allocate_hand_key(pane_id="hand_CHO-156", node="oci-machine")
        self.webui._active_hands[k2] = {
            "pane_id": "hand_CHO-156",
            "node": "oci-machine",
            "herdr_pane": "hand_CHO-156",
            "status": "RUNNING",
        }

        self.assertNotEqual(k1, k2)
        self.assertEqual(k1, "hand_CHO-155")
        self.assertEqual(k2, "hand_CHO-156")
        self.assertEqual(len(self.webui._active_hands), 2)

    def test_card_retention_after_settle_until_parent_resolution(self):
        """Settled cards are retained in _active_hands until clear_settled_hands is called."""
        self.webui.register_active_hand(
            pane_id="hand_CHO-155",
            node="oci-machine",
            command="curl -s http://localhost:13378/api/libraries",
        )
        self.webui.register_active_hand(
            pane_id="hand_CHO-156",
            node="oci-machine",
            command="docker exec abs sqlite3 /config/abs.db",
        )

        self.assertEqual(len(self.webui._active_hands), 2)
        self.assertEqual(self.webui._active_hands["hand_CHO-155"]["status"], "RUNNING")
        self.assertEqual(self.webui._active_hands["hand_CHO-156"]["status"], "RUNNING")

        # Settle first hand
        self.webui.settle_active_hand("hand_CHO-155", exit_code=0, status="EXIT_0")
        self.assertIn("hand_CHO-155", self.webui._active_hands)
        self.assertEqual(self.webui._active_hands["hand_CHO-155"]["status"], "EXIT_0")

        # Settle second hand
        self.webui.settle_active_hand("hand_CHO-156", exit_code=0, status="EXIT_0")
        self.assertIn("hand_CHO-156", self.webui._active_hands)
        self.assertEqual(self.webui._active_hands["hand_CHO-156"]["status"], "EXIT_0")

        # Both cards retained side-by-side
        self.assertEqual(len(self.webui._active_hands), 2)

        # Clear settled hands upon parent task resolution
        cleared = self.webui.clear_settled_hands(parent_key="CHO-154")
        self.assertEqual(len(cleared), 2)
        self.assertEqual(len(self.webui._active_hands), 0)


class TestDispatchSubtasksTool(unittest.IsolatedAsyncioTestCase):
    """Task 2: Verify dispatch_subtasks schema, registration, parallel & sequential dispatch."""

    def setUp(self):
        self.port = _get_free_port()
        self.webui = WebUIServer(host="127.0.0.1", port=self.port)
        self.router = MagicMock()
        self.gateway = IngressGateway(router=self.router, webui_server=self.webui)

    def tearDown(self):
        if self.webui.is_running:
            asyncio.run(self.webui.stop())

    def test_dispatch_subtasks_tool_declaration_schema(self):
        """Verify tool schema matches requirements."""
        self.assertEqual(DISPATCH_SUBTASKS_TOOL_DECLARATION["name"], "dispatch_subtasks")
        props = DISPATCH_SUBTASKS_TOOL_DECLARATION["parameters"]["properties"]
        self.assertIn("parent_issue_key", props)
        self.assertIn("execution_mode", props)
        self.assertEqual(props["execution_mode"]["enum"], ["parallel", "sequential"])
        self.assertIn("subtasks", props)
        subtask_props = props["subtasks"]["items"]["properties"]
        self.assertIn("issue_key", subtask_props)
        self.assertIn("target_node", subtask_props)
        self.assertIn("role_name", subtask_props)
        self.assertIn("read_only", subtask_props)
        self.assertIn("task_brief", subtask_props)

        # In get_mouth_tools_declaration()
        mouth_decls = get_mouth_tools_declaration()
        all_funcs = []
        for d in mouth_decls:
            all_funcs.extend([f["name"] for f in d.get("functionDeclarations", [])])
        self.assertIn("dispatch_subtasks", all_funcs)

        # In MOUTH_TOOLS
        mouth_tool_names = [t["name"] for t in MOUTH_TOOLS]
        self.assertIn("dispatch_subtasks", mouth_tool_names)

    async def test_parallel_subtasks_dispatch_and_webui_cards(self):
        """Parallel execution spawns hands concurrently and populates distinct WebUI cards."""
        subtasks = [
            {
                "issue_key": "CHO-155",
                "target_node": "oci-machine",
                "role_name": "Series Inventory",
                "read_only": True,
                "task_brief": "Query local ABS REST API (curl -s http://localhost:13378/api/libraries/items)",
            },
            {
                "issue_key": "CHO-156",
                "target_node": "oci-machine",
                "role_name": "Episode Durations",
                "read_only": True,
                "task_brief": "Query ABS episode metadata and media durations",
            },
        ]

        # Dispatch via gateway._execute_backstage_task in parallel mode
        with patch("golem.linear_bridge.LinearBridge.update_issue_state", new_callable=AsyncMock) as mock_update, \
             patch("golem.linear_bridge.LinearBridge.settle_subtask_and_evaluate_parent", new_callable=AsyncMock) as mock_settle:

            acc = await self.gateway._execute_backstage_task(
                parent_issue_key="CHO-154",
                execution_mode="parallel",
                subtasks=subtasks,
            )

            # Both cards should exist and be retained in _active_hands
            self.assertEqual(len(self.webui._active_hands), 2)
            self.assertIn("hand_CHO-155", self.webui._active_hands)
            self.assertIn("hand_CHO-156", self.webui._active_hands)
            self.assertEqual(self.webui._active_hands["hand_CHO-155"]["status"], "EXIT_0")
            self.assertEqual(self.webui._active_hands["hand_CHO-156"]["status"], "EXIT_0")

            # Check linear bridge updates
            self.assertEqual(mock_update.call_count, 2)
            self.assertEqual(mock_settle.call_count, 2)

            self.assertEqual(acc.turn_status, "SUCCESS")
            full_out = acc.get_full_output()
            self.assertIn("CHO-155", full_out)
            self.assertIn("CHO-156", full_out)

    async def test_sequential_subtasks_dispatch(self):
        """Sequential execution runs subtasks one by one, keeping finished cards retained."""
        subtasks = [
            {
                "issue_key": "CHO-155",
                "target_node": "oci-machine",
                "role_name": "Series Inventory",
                "read_only": True,
                "task_brief": "Task 1 sequential",
            },
            {
                "issue_key": "CHO-156",
                "target_node": "oci-machine",
                "role_name": "Episode Durations",
                "read_only": True,
                "task_brief": "Task 2 sequential",
            },
        ]

        with patch("golem.linear_bridge.LinearBridge.update_issue_state", new_callable=AsyncMock), \
             patch("golem.linear_bridge.LinearBridge.settle_subtask_and_evaluate_parent", new_callable=AsyncMock):

            acc = await self.gateway._execute_backstage_task(
                parent_issue_key="CHO-154",
                execution_mode="sequential",
                subtasks=subtasks,
            )

            self.assertEqual(len(self.webui._active_hands), 2)
            self.assertEqual(self.webui._active_hands["hand_CHO-155"]["status"], "EXIT_0")
            self.assertEqual(self.webui._active_hands["hand_CHO-156"]["status"], "EXIT_0")
            self.assertEqual(acc.turn_status, "SUCCESS")

    async def test_execute_tool_dispatch_subtasks(self):
        """Verify execute_tool routes dispatch_subtasks properly."""
        subtasks = [
            {
                "issue_key": "CHO-155",
                "target_node": "oci-machine",
                "role_name": "Series Inventory",
                "read_only": True,
                "task_brief": "Query ABS items",
            }
        ]

        with patch("golem.linear_bridge.LinearBridge.update_issue_state", new_callable=AsyncMock), \
             patch("golem.linear_bridge.LinearBridge.settle_subtask_and_evaluate_parent", new_callable=AsyncMock):

            res = await self.gateway._execute_tool_call(
                tool_name="dispatch_subtasks",
                tool_args={
                    "parent_issue_key": "CHO-154",
                    "execution_mode": "parallel",
                    "subtasks": subtasks,
                },
            )
            self.assertIn("status", res)
            self.assertEqual(res["status"], "SUCCESS")


class TestOutOfBandGrounding(unittest.IsolatedAsyncioTestCase):
    """Task 3: Ground the Out-of-Band Reactor from webui_server._active_hands."""

    async def test_out_of_band_empty_registry_grounding(self):
        """When no workers are active, registry state is explicitly [Active Workers: 0 (None running)]."""
        port = _get_free_port()
        webui = WebUIServer(host="127.0.0.1", port=port)
        session = SessionHandle(session_id="test_session", channel="C_TEST", thread_ts="T_123")
        session.harness = MagicMock()
        session.harness.webui_server = webui

        # Empty active hands
        with patch("golem.session.generate_mouth_response", new_callable=AsyncMock) as mock_mouth:
            mock_mouth.return_value = "No workers running right now."
            events = []
            async for ev in session._stream_out_of_band_impl("What is the status?"):
                events.append(ev)

            # Check that generate_mouth_response was called with worker_registry_state
            mock_mouth.assert_called_once()
            _, kwargs = mock_mouth.call_args
            state_header = kwargs.get("worker_registry_state", "")
            self.assertIn("[CURRENT WORKER REGISTRY STATE]", state_header)
            self.assertIn("Active Workers: 0 (None running)", state_header)

    async def test_out_of_band_active_registry_grounding(self):
        """When workers are active, registry state lists each worker, node, and status."""
        port = _get_free_port()
        webui = WebUIServer(host="127.0.0.1", port=port)
        webui.register_active_hand(
            pane_id="hand_CHO-155",
            node="oci-machine",
            command="curl -s http://localhost:13378/api/libraries",
        )
        webui.register_active_hand(
            pane_id="hand_CHO-156",
            node="oci-machine",
            command="docker exec abs sqlite3 /config/abs.db",
        )

        session = SessionHandle(session_id="test_session", channel="C_TEST", thread_ts="T_123")
        session.harness = MagicMock()
        session.harness.webui_server = webui

        with patch("golem.session.generate_mouth_response", new_callable=AsyncMock) as mock_mouth:
            mock_mouth.return_value = "Hand 1 and Hand 2 are currently active on oci-machine."
            events = []
            async for ev in session._stream_out_of_band_impl("What is the status?"):
                events.append(ev)

            mock_mouth.assert_called_once()
            _, kwargs = mock_mouth.call_args
            state_header = kwargs.get("worker_registry_state", "")
            self.assertIn("[CURRENT WORKER REGISTRY STATE]", state_header)
            self.assertIn("Active Workers: 2", state_header)
            self.assertIn("hand_CHO-155: RUNNING (Node: oci-machine)", state_header)
            self.assertIn("hand_CHO-156: RUNNING (Node: oci-machine)", state_header)


class TestAudiobookshelfReadOnlyScope(unittest.TestCase):
    """Task 4: Clarify Audiobookshelf subtask scopes in mouth instruction."""

    def test_abs_instruction_includes_read_only_rules(self):
        """Instruction explicitly instructs read-only scoping for Hand 1 and Hand 2 on oci-machine."""
        self.assertIn("CHO-155", BASE_CHIEF_OF_STAFF_INSTRUCTION)
        self.assertIn("CHO-156", BASE_CHIEF_OF_STAFF_INSTRUCTION)
        self.assertIn("read_only: true", BASE_CHIEF_OF_STAFF_INSTRUCTION)
        self.assertIn("http://localhost:13378/api/libraries", BASE_CHIEF_OF_STAFF_INSTRUCTION)
        self.assertIn("oci-machine", BASE_CHIEF_OF_STAFF_INSTRUCTION)


if __name__ == "__main__":
    unittest.main()
