#!/usr/bin/env python3
"""
tests/test_mouth_llm.py
Unit tests for the dynamic Mouth LLM reactor and out-of-band conversational engine.
Verifies that the Liaison Mouth in Slack behaves like a real human:
always responsive, able to converse mid-stream without hardcoded regex gates.
"""

import os
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Dict, Any

from first_mate.agent.mouth import (
    generate_mouth_response,
    get_mouth_model,
    MOUTH_MODEL_DEFAULT,
    is_cancellation_message,
    is_status_inquiry,
    is_conversational_checkin,
    is_conversational_inquiry,
    is_out_of_band_message,
)
from first_mate.agent.protocol import TextDeltaEvent, FinishEvent


class TestMouthLLM(unittest.IsolatedAsyncioTestCase):
    """Test suite for Mouth LLM dynamic reaction and out-of-band intent handling."""

    def test_natural_midstream_inquiries_recognized(self) -> None:
        """Verify natural human mid-stream questions and inquiries qualify as out-of-band without regex gates."""
        inquiries = [
            "Can I ask you anything mid stream like this?",
            "Can I ask a question?",
            "Could you explain what step we are on?",
            "Would it be possible to check the logs?",
            "How long do you think this will take?",
            "What's happening right now?",
            "Are you still running?",
            "Is everything going okay?",
            "Tell me about the current progress",
            "Hey, take your time",
            "Thanks!",
            "Thank you so much",
            "Cool, sounds good",
            "Ping",
            "status",
            "cancel that execution",
            # Phrases that previously failed under brittle regex gates:
            "I was wondering if the podcast scraper is almost done",
            "Let me know if you run into any issues",
            "Just checking in on things",
            "FYI I need this in 10 minutes",
            "Looks like it is taking a while",
            "Carl here, let me know the status",
        ]
        for msg in inquiries:
            with self.subTest(msg=msg):
                self.assertTrue(
                    is_out_of_band_message(msg),
                    f"Expected '{msg}' to be recognized as out-of-band message",
                )

    def test_non_conversational_batch_tasks_not_out_of_band(self) -> None:
        """Verify mock turn markers preserve lock queuing in serialization tests."""
        batch_tasks = [
            "Turn A",
            "Turn B",
            "Turn 1",
            "Turn 2",
            "turn1",
            "turn_A",
            "Question 1",
            "Question 2",
            "Message-0",
            "Task Alpha",
            "Task Beta",
        ]
        for task in batch_tasks:
            with self.subTest(task=task):
                self.assertFalse(
                    is_out_of_band_message(task),
                    f"Expected '{task}' not to be classified as out-of-band message",
                )

    def test_out_of_band_always_true_for_natural_inquiries_when_session_executing(self) -> None:
        """Verify that when session.is_executing is True, natural inquiries are processed out-of-band by Mouth LLM."""
        mock_session = MagicMock()
        mock_session.is_executing = True
        prompts = [
            "Can I ask you anything mid stream like this?",
            "I was wondering if the podcast scraper is almost done",
            "Let me know if you run into any issues",
            "Just checking in on things",
            "Looks like it is taking a while",
            "status",
            "ping",
        ]
        for p in prompts:
            with self.subTest(prompt=p):
                self.assertTrue(
                    is_out_of_band_message(p, session=mock_session),
                    f"Expected '{p}' to be out-of-band when session.is_executing is True",
                )

    async def test_generate_mouth_response_with_gemini_api(self) -> None:
        """Verify generate_mouth_response calls Gemini API when available and returns LLM response."""
        mock_session = MagicMock()
        mock_session.adapter = MagicMock()
        type(mock_session.adapter).__name__ = "HeadlessAgentAdapter"
        mock_session.is_executing = True
        mock_session.last_prompt = "Generate podcast dossiers for Joe Rogan"

        status_info: Dict[str, Any] = {
            "status": "running",
            "pid": 4321,
            "command": "playwright test",
            "buffer_tail": "Processing episode #2100... 50% complete",
        }

        mock_gemini_response = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "Of course! I'm currently running the podcast automation in the background, but I'm right here. What's on your mind?"}
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
            resp = await generate_mouth_response(
                prompt="Can I ask you anything mid stream like this?",
                status_info=status_info,
                session=mock_session,
                api_key="test_api_key",
            )
            self.assertIn("Of course!", resp)
            self.assertIn("podcast automation", resp)

    async def test_generate_mouth_response_oauth_blocker(self) -> None:
        """Verify interactive OAuth blocker returns clickable authentication link."""
        status_info: Dict[str, Any] = {
            "status": "running",
            "blocker": {
                "type": "oauth",
                "url": "http://127.0.0.1:8080/auth?code=abc",
            }
        }
        resp = await generate_mouth_response(
            prompt="status",
            status_info=status_info,
        )
        self.assertIn("Authentication Required", resp)
        self.assertIn("<http://127.0.0.1:8080/auth?code=abc|Click here to authenticate>", resp)

    async def test_generate_mouth_response_prompt_blocker(self) -> None:
        """Verify interactive confirmation prompt returns blocker prompt card."""
        status_info: Dict[str, Any] = {
            "status": "running",
            "blocker": {
                "type": "prompt",
                "content": "Overwrite existing files? (y/n)",
            }
        }
        resp = await generate_mouth_response(
            prompt="status",
            status_info=status_info,
        )
        self.assertIn("Confirmation Required", resp)
        self.assertIn("Overwrite existing files? (y/n)", resp)

    async def test_generate_mouth_response_fallback_midstream_inquiry(self) -> None:
        """Verify offline/test fallback handles mid-stream question naturally."""
        mock_session = MagicMock()
        mock_session.is_executing = True
        mock_session.active_execution = MagicMock()
        mock_session.active_execution.pid = 9988

        status_info: Dict[str, Any] = {
            "status": "running",
            "pid": 9988,
            "buffer_tail": "Scraping items...",
        }
        resp = await generate_mouth_response(
            prompt="Can I ask you anything mid stream like this?",
            status_info=status_info,
            session=mock_session,
            api_key=None,  # No API key triggers calibrated fallback
        )
        self.assertIn("Liaison Update", resp)
        self.assertIn("PID 9988", resp)
        self.assertIn("What's on your mind?", resp)

    def test_mouth_model_resolution_priority(self) -> None:
        """Verify get_mouth_model prioritizes MOUTH_MODEL over GEMINI_MODEL."""
        with patch.dict(os.environ, {"MOUTH_MODEL": "gemini-custom-mouth", "GEMINI_MODEL": "gemini-fallback"}, clear=False):
            self.assertEqual(get_mouth_model(), "gemini-custom-mouth")

        with patch.dict(os.environ, {"GEMINI_MODEL": "gemini-fallback"}, clear=True):
            self.assertEqual(get_mouth_model(), "gemini-fallback")

        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(get_mouth_model(), "gemini-3.8-flash")


if __name__ == "__main__":
    unittest.main()
