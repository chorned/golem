"""
tests/test_mouth_webui_stream.py
Verification suite for CHO-215 (Parent: CHO-214):
1. Fix NameError in server.py / mouth handler.
2. Wire and stream real-time inbound user prompts, multi-hop Gemini function/tool calls
   and responses, and token usage telemetry directly to the Mouth WebUI terminal stream.
3. Verify no syntax or runtime errors remain in the Mouth stream pipeline.
"""

import os
import time
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

from golem.webui.server import WebUIServer, get_active_webui_server, get_system_status
from golem.mouth import generate_mouth_response
from golem.ingress import IngressGateway, _is_mock_object


class TestMouthWebUIStreamPipeline(unittest.IsolatedAsyncioTestCase):
    """Test suite ensuring real-time Mouth terminal streaming and NameError resolution."""

    async def asyncSetUp(self) -> None:
        self.server = WebUIServer(port=8912)
        # Manually register server as active for testing
        import golem.webui.server as ws
        ws._GLOBAL_WEBUI_SERVER = self.server

    async def asyncTearDown(self) -> None:
        if self.server.is_running:
            await self.server.stop()
        import golem.webui.server as ws
        if ws._GLOBAL_WEBUI_SERVER is self.server:
            ws._GLOBAL_WEBUI_SERVER = None

    def test_no_name_errors_in_server_and_mouth(self) -> None:
        """Verify no NameError or undefined global names when initializing and querying status."""
        status = get_system_status()
        self.assertIn("mouth", status)
        self.assertIn("logs", status["mouth"])
        self.assertIsInstance(status["mouth"]["logs"], list)

    def test_append_organ_log_mouth_streams_and_buffers(self) -> None:
        """Verify append_organ_log('mouth', ...) stores line in recent logs and broadcasts mouth_log."""
        subscriber_q = asyncio.Queue()
        self.server._subscribers.add(subscriber_q)

        test_log = "[HOP 1/4] Reasoning with gemini-3.8-flash..."
        self.server.append_organ_log("mouth", test_log)

        recent = self.server._recent_logs["mouth"]
        self.assertTrue(any(test_log in line for line in recent))

        # Check broadcasted SSE events
        received_events = []
        while not subscriber_q.empty():
            received_events.append(subscriber_q.get_nowait())

        # Should broadcast mouth_log and mouth_briefing for backward compatibility
        event_names = [e for e in received_events if "event:" in e]
        self.assertTrue(any("event: mouth_log" in e for e in event_names))
        self.assertTrue(any("event: mouth_briefing" in e for e in event_names))

    async def test_generate_mouth_response_streams_inbound_and_tokens(self) -> None:
        """Verify generate_mouth_response streams [INBOUND] and [TOKENS] to WebUI without NameError."""
        status_info = {"status": "running", "command": "pytest"}

        mock_gemini_resp = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "Understood! I will check the mesh."}
                        ]
                    }
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 120,
                "candidatesTokenCount": 45,
                "thoughtsTokenCount": 12,
            }
        }

        with patch("golem.mouth.get_gemini_api_key", return_value="mock-key"), \
             patch("aiohttp.ClientSession.post") as mock_post:
            mock_ctx = AsyncMock()
            mock_ctx.status = 200
            mock_ctx.json = AsyncMock(return_value=mock_gemini_resp)
            mock_post.return_value.__aenter__.return_value = mock_ctx

            reply = await generate_mouth_response(
                prompt="How is the compute node looking?",
                status_info=status_info,
            )

            self.assertIn("Understood!", reply)

            # Check that Mouth terminal received [INBOUND], [TOKENS], and [REPLY]
            mouth_logs = self.server._recent_logs["mouth"]
            self.assertTrue(any("[INBOUND]" in l and "compute node" in l for l in mouth_logs))
            self.assertTrue(any("[TOKENS]" in l and "prompt=120" in l for l in mouth_logs))
            self.assertTrue(any("[REPLY]" in l and "Understood" in l for l in mouth_logs))

    async def test_handle_mouth_turn_streams_prompts_and_hops(self) -> None:
        """Verify _handle_mouth_turn streams [PROMPT], [TURN START], [HOP], [TOKENS], and [SYNTHESIS]."""
        gateway = IngressGateway()
        gateway.webui = self.server

        client = AsyncMock()
        client.chat_postMessage = AsyncMock(return_value={"ts": "123.456"})

        hop1_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "check_nose",
                                    "args": {"trigger_fresh_sniff": False}
                                }
                            }
                        ]
                    }
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 250,
                "candidatesTokenCount": 30,
                "thoughtsTokenCount": 5,
            }
        }

        synthesis_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "✨ *All clear:* Homelab and fleet sentinels report green."}
                        ]
                    }
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 350,
                "candidatesTokenCount": 20,
                "thoughtsTokenCount": 0,
            }
        }

        call_count = 0
        async def mock_call_gemini(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return hop1_response
            return synthesis_response

        with patch.object(gateway, "_call_gemini_api", side_effect=mock_call_gemini), \
             patch("golem.gateway.ingress.get_mouth_model", return_value="gemini-3.8-flash"), \
             patch("golem.gateway.ingress.get_gemini_api_key", return_value="mock-key"), \
             patch.object(gateway, "_execute_check_nose", return_value={"smells_off": False, "summary": "Nominal"}):

            await gateway._handle_mouth_turn(
                session=None,
                prompt="Are the fleet nodes nominal?",
                client=client,
                channel="C_TEST",
                thread_ts="123456.789",
                ts="123456.789",
                is_imessage=False,
            )

        mouth_logs = self.server._recent_logs["mouth"]
        self.assertTrue(any("[TURN START]" in l for l in mouth_logs))
        self.assertTrue(any("[PROMPT]" in l and "Are the fleet nodes nominal?" in l for l in mouth_logs))
        self.assertTrue(any("[HOP 1" in l and "check_nose" in l for l in mouth_logs))
        self.assertTrue(any("[SYNTHESIS]" in l for l in mouth_logs))


if __name__ == "__main__":
    unittest.main()
