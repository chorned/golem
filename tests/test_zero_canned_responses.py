#!/usr/bin/env python3
"""
tests/test_zero_canned_responses.py
Strict architectural regression suite verifying that ZERO canned responses exist.
Every subsystem must either produce dynamic LLM generation, live-probed telemetry,
or fail loudly with diagnostic error markers and appropriate error reactions.
"""

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.agent.mouth import generate_mouth_response
from golem.gateway.ingress import IngressGateway
from golem.gateway.session import SessionHandle


CANNED_STRINGS = [
    "Air is clean; all systems nominal",
    "Runner is currently *IDLE*",
    "Runner is currently IDLE",
    "All systems operating nominally",
    "All background Herdr agents settled",
    "Liaison Update: Active execution is underway in Pane 2",
    "Status Digest: Runner pane is currently idle",
]


class TestZeroCannedResponses(unittest.IsolatedAsyncioTestCase):
    """Verifies that no canned responses can ever be returned under any condition."""

    def _assert_no_canned_strings(self, response: str) -> None:
        for canned in CANNED_STRINGS:
            self.assertNotIn(
                canned,
                response,
                f"Deceptive canned string '{canned}' found in response: '{response}'"
            )

    async def test_mouth_no_api_key_fails_loudly(self) -> None:
        """When GEMINI_API_KEY is missing, fail loudly with diagnostic error."""
        status_info = {"status": "running", "pid": 1111}
        with patch("golem.agent.mouth.get_gemini_api_key", return_value=""):
            resp = await generate_mouth_response(
                prompt="How is the fleet smelling?",
                status_info=status_info,
                api_key=None,
            )
            self._assert_no_canned_strings(resp)
            self.assertIn("Mouth Generation Error", resp)
            self.assertIn("GEMINI_API_KEY is not configured", resp)

    async def test_mouth_api_http_error_fails_loudly(self) -> None:
        """When Gemini API returns HTTP 500, report status code and error body."""
        status_info = {"status": "idle"}
        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_resp.text = AsyncMock(return_value="Rate limit exceeded")

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__.return_value = mock_resp
        mock_post_cm.__aexit__.return_value = None

        with patch("aiohttp.ClientSession.post", return_value=mock_post_cm):
            resp = await generate_mouth_response(
                prompt="Status check",
                status_info=status_info,
                api_key="valid-key",
            )
            self._assert_no_canned_strings(resp)
            self.assertIn("Mouth Generation Error", resp)
            self.assertIn("HTTP 500", resp)
            self.assertIn("Rate limit exceeded", resp)

    async def test_mouth_network_connection_error_fails_loudly(self) -> None:
        """When network connection fails, report exception type and details."""
        status_info = {"status": "running", "pid": 2222}

        with patch("aiohttp.ClientSession.post", side_effect=ConnectionResetError("Connection reset by peer")):
            resp = await generate_mouth_response(
                prompt="What do The Eyes see?",
                status_info=status_info,
                api_key="valid-key",
            )
            self._assert_no_canned_strings(resp)
            self.assertIn("Mouth Generation Error", resp)
            self.assertIn("ConnectionResetError", resp)

    async def test_mouth_empty_candidates_fails_loudly(self) -> None:
        """When Gemini returns 200 with empty candidates, fail loudly."""
        status_info = {"status": "running"}
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"candidates": []})

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__.return_value = mock_resp
        mock_post_cm.__aexit__.return_value = None

        with patch("aiohttp.ClientSession.post", return_value=mock_post_cm):
            resp = await generate_mouth_response(
                prompt="Is everything okay?",
                status_info=status_info,
                api_key="valid-key",
            )
            self._assert_no_canned_strings(resp)
            self.assertIn("Mouth Generation Error", resp)
            self.assertIn("no candidates", resp)

    async def test_mouth_empty_text_parts_fails_loudly(self) -> None:
        """When Gemini returns candidates with empty text, fail loudly with finishReason."""
        status_info = {"status": "running"}
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "candidates": [{
                "finishReason": "SAFETY",
                "content": {"parts": [{"text": ""}]}
            }]
        })

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__.return_value = mock_resp
        mock_post_cm.__aexit__.return_value = None

        with patch("aiohttp.ClientSession.post", return_value=mock_post_cm):
            resp = await generate_mouth_response(
                prompt="Check the runner",
                status_info=status_info,
                api_key="valid-key",
            )
            self._assert_no_canned_strings(resp)
            self.assertIn("Mouth Generation Error", resp)
            self.assertIn("finishReason=SAFETY", resp)

    async def test_ingress_bearings_probes_fleet_live(self) -> None:
        """Verify /bearings performs live probing of all nodes and reports results."""
        mock_router = MagicMock()
        gateway = IngressGateway(router=mock_router)

        mock_fleet = MagicMock()
        mock_fleet.probe_node = AsyncMock(side_effect=[
            {"node": "hermes", "role": "localhost", "status": "🟢 Online (120GB free)"},
            {"node": "desktop-pc", "role": "Compute Grid", "status": "🟢 Online (RTX 4090)"},
            {"node": "personal-laptop", "role": "Personal Laptop", "status": "🔴 Offline (Unreachable)"},
            {"node": "oci-machine", "role": "Production VPS", "status": "🟢 Online (Uptime 40d)"},
        ])
        gateway.fleet = mock_fleet

        client = AsyncMock()
        event = {
            "channel": "C_TEST",
            "text": "/bearings",
            "ts": "1234.5678",
            "thread_ts": "1234.5678",
        }

        await gateway.handle_message(event, client)

        # Verify probe_node called for all 4 nodes
        self.assertEqual(mock_fleet.probe_node.call_count, 4)
        client.chat_postMessage.assert_called_once()
        sent_text = client.chat_postMessage.call_args[1]["text"]
        self.assertIn("Fleet Bearings & Status Digest", sent_text)
        self.assertIn("🔴 Offline (Unreachable)", sent_text)
        self.assertIn("RTX 4090", sent_text)

    async def test_ingress_ahoy_queries_active_hands_live(self) -> None:
        """Verify /ahoy reports actual active hands instead of a static message."""
        mock_router = MagicMock()
        gateway = IngressGateway(router=mock_router)

        mock_webui = MagicMock()
        mock_webui._active_hands = {
            "pane_2": {
                "pane_id": "pane_2",
                "target_node": "desktop-pc",
                "task_description": "Fine-tuning LoRA weights",
            }
        }
        gateway.webui_server = mock_webui

        client = AsyncMock()
        event = {
            "channel": "C_TEST",
            "text": "/ahoy",
            "ts": "1234.5678",
            "thread_ts": "1234.5678",
        }

        await gateway.handle_message(event, client)

        client.chat_postMessage.assert_called_once()
        sent_text = client.chat_postMessage.call_args[1]["text"]
        self.assertIn("Session active with 1 executing hand(s)", sent_text)
        self.assertIn("Fine-tuning LoRA weights", sent_text)
        self.assertNotIn("All background Herdr agents settled", sent_text)

    async def test_ingress_empty_candidate_fails_loudly(self) -> None:
        """Verify Step 1 pure conversational turn with empty text fails loudly."""
        mock_router = MagicMock()
        gateway = IngressGateway(router=mock_router)

        # Mock session
        mock_session = AsyncMock(spec=SessionHandle)
        mock_session.is_busy = False
        mock_session.is_executing = False
        mock_session.runner_pane = None
        mock_router.get_or_create = AsyncMock(return_value=(mock_session, False))

        # Mock Gemini Step 1 returning empty candidate text
        mock_candidate = {
            "content": {
                "parts": [{"text": ""}]
            }
        }
        gateway._call_gemini_api = AsyncMock(return_value={"candidates": [mock_candidate]})

        client = AsyncMock()
        event = {
            "channel": "C_TEST",
            "text": "Hello there",
            "ts": "1234.5678",
            "thread_ts": "1234.5678",
        }

        with patch.dict(os.environ, {"FIRSTMATE_FORCE_MOUTH": "1", "GEMINI_API_KEY": "valid-key"}):
            await gateway.handle_message(event, client)

        client.chat_postMessage.assert_called_once()
        sent_text = client.chat_postMessage.call_args[1]["text"]
        self.assertIn("Mouth Response Error", sent_text)
        self.assertIn("returned empty candidate text", sent_text)
        self.assertNotIn("All systems operating nominally", sent_text)


if __name__ == "__main__":
    unittest.main()
