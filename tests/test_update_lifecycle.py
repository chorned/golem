# tests/test_update_lifecycle.py
import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from app import handle_update_request
from golem.herdr_fleet import refresh_node_herdr


class TestUpdateLifecycle(unittest.IsolatedAsyncioTestCase):

    async def test_update_gracefully_closes_active_tasks(self):
        mock_say = AsyncMock(return_value={"ts": "123"})
        with patch("app.webui_server") as mock_webui, \
             patch("asyncio.create_subprocess_exec") as mock_exec, \
             patch("herdr_fleet.HerdrFleet.refresh_fleet_herdr", AsyncMock(return_value=[{"node": "hermes", "status": "ACTIVE"}])), \
             patch("herdr_fleet.HerdrFleet.update_fleet_agy", AsyncMock(return_value=[{"node": "hermes", "status": "UP_TO_DATE"}])):
            mock_webui._active_hands = ["task-1", "task-2"]
            mock_proc = MagicMock()
            mock_proc.returncode = 0
            mock_proc.communicate = AsyncMock(return_value=(b"OK", b""))
            mock_exec.return_value = mock_proc
            await handle_update_request(mock_say, "C123", "123.456")
            self.assertTrue(mock_say.called)
            self.assertEqual(len(mock_webui._active_hands), 0)
            self.assertIn("Gracefully closed 2 open task(s)", mock_say.call_args_list[0][0][0])

    async def test_refresh_node_herdr_detects_local_socket(self):
        with patch("os.path.exists", return_value=True):
            res = await refresh_node_herdr("hermes", "localhost")
            self.assertEqual(res["status"], "ACTIVE")

    async def test_full_update_lifecycle_flow(self):
        mock_say = AsyncMock(return_value={"ts": "123"})
        with patch("app.webui_server") as mock_webui, \
             patch("asyncio.create_subprocess_exec") as mock_exec, \
             patch("herdr_fleet.HerdrFleet.refresh_fleet_herdr", AsyncMock(return_value=[{"node": "hermes", "status": "ACTIVE"}])), \
             patch("herdr_fleet.HerdrFleet.update_fleet_agy", AsyncMock(return_value=[{"node": "hermes", "status": "UP_TO_DATE"}])):

            mock_webui._active_hands = []

            # Mock safe_update.sh execution exit code 0
            mock_proc = MagicMock()
            mock_proc.returncode = 0
            mock_proc.communicate = AsyncMock(return_value=(b"OK", b""))
            mock_exec.return_value = mock_proc

            await handle_update_request(mock_say, "C123", "123.456")

            self.assertGreaterEqual(mock_say.call_count, 2)
            last_msg = mock_say.call_args_list[-1][0][0]
            self.assertIn("Full-Stack Update & Refresh Complete", last_msg)

    async def test_should_process_ingress_event_allows_bang_commands(self):
        from app import should_process_ingress_event
        mock_client = AsyncMock()
        mock_client.auth_test = AsyncMock(return_value={"user_id": "U_BOT_123"})
        event = {
            "type": "message",
            "channel": "C_CHANNEL_PUBLIC",
            "channel_type": "channel",
            "text": "!update",
            "ts": "12345.6789",
            "thread_ts": None,
        }
        res = await should_process_ingress_event(event, mock_client)
        self.assertTrue(res)

    async def test_update_proceeds_when_tasks_are_settled(self):
        mock_say = AsyncMock(return_value={"ts": "123"})
        with patch("app.webui_server") as mock_webui, \
             patch("asyncio.create_subprocess_exec") as mock_exec, \
             patch("herdr_fleet.HerdrFleet.refresh_fleet_herdr", AsyncMock(return_value=[{"node": "hermes", "status": "ACTIVE"}])), \
             patch("herdr_fleet.HerdrFleet.update_fleet_agy", AsyncMock(return_value=[{"node": "hermes", "status": "UP_TO_DATE"}])):

            # Settled hands simulate the exact Carl situation (hand_CHO-164, hand_CHO-165 in EXIT_0)
            mock_webui._active_hands = {
                "hand_CHO-164": {"pane_id": "hand_CHO-164", "status": "EXIT_0", "exit_code": 0},
                "hand_CHO-165": {"pane_id": "hand_CHO-165", "status": "EXIT_0", "exit_code": 0},
            }
            def _clear():
                mock_webui._active_hands.clear()
            mock_webui.clear_settled_hands = MagicMock(side_effect=_clear)
            mock_webui.get_executing_hands = MagicMock(return_value={})

            mock_proc = MagicMock()
            mock_proc.returncode = 0
            mock_proc.communicate = AsyncMock(return_value=(b"OK", b""))
            mock_exec.return_value = mock_proc

            await handle_update_request(mock_say, "C123", "123.456")

            self.assertTrue(mock_webui.clear_settled_hands.called)
            self.assertGreaterEqual(mock_say.call_count, 2)
            last_msg = mock_say.call_args_list[-1][0][0]
            self.assertIn("Full-Stack Update & Refresh Complete", last_msg)


if __name__ == "__main__":
    unittest.main()
