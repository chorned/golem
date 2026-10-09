#!/usr/bin/env python3
"""
tests/test_mouth_synthesis_empty_candidate.py
Verifies that Step 2 synthesis handles empty candidate text or tool-call attempts
gracefully (as seen with gemini-3.8-flash) without dumping raw JSON sensory error data.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.gateway.ingress import IngressGateway
from golem.gateway.router import SessionRouter


class MockSlackClient:
    def __init__(self) -> None:
        self.posted_messages = []
        self.reactions_added = []
        self.reactions_removed = []

    async def chat_postMessage(self, channel, text, thread_ts=None, **kwargs):
        self.posted_messages.append({"channel": channel, "text": text, "thread_ts": thread_ts})
        return {"ok": True, "ts": "123.456"}

    async def reactions_add(self, name, channel, timestamp):
        self.reactions_added.append({"name": name, "channel": channel, "timestamp": timestamp})
        return {"ok": True}

    async def reactions_remove(self, name, channel, timestamp):
        self.reactions_removed.append({"name": name, "channel": channel, "timestamp": timestamp})
        return {"ok": True}


class TestMouthSynthesisEmptyCandidate(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = MockSlackClient()

    async def test_empty_candidate_recovers_via_direct_fallback_synthesis(self):
        """When Step 2 functionResponse returns empty candidate text, direct fallback produces briefing."""
        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "inspect gemini spark"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        # Step 2: Empty candidate text (gemini-3.8-flash quirk)
        turn2_empty = {
            "candidates": [
                {
                    "content": {"parts": []},
                    "finishReason": "STOP",
                }
            ]
        }
        # Step 2 fallback: Direct text synthesis returns clean briefing
        fallback_briefing = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "✨ *Gemini Spark:* Successfully inspected active tabs on `hermes`."}
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_dispatch, turn2_empty, fallback_briefing]
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_task:
                mock_task.return_value = {
                    "status": "SUCCESS",
                    "duration_seconds": 5.0,
                    "output_tail": "Inspection finished with exit code 0.",
                }

                event = {
                    "channel": "C_TEST",
                    "ts": "100.001",
                    "thread_ts": "100.001",
                    "text": "<@U_GOLEM> inspect gemini spark",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertTrue(len(self.client.posted_messages) >= 1)
                posted = self.client.posted_messages[-1]["text"]
                self.assertIn("Gemini Spark", posted)
                self.assertNotIn("⚠️ *Executive Synthesis Failed*", posted)
                self.assertNotIn("Raw sensory data returned above", posted)

    async def test_empty_candidate_fails_loudly_if_fallback_fails(self):
        """If direct fallback also returns empty text, synthesis fails loudly with raw sensory output."""
        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "inspect tabs"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_empty = {
            "candidates": [
                {
                    "content": {"parts": []},
                    "finishReason": "STOP",
                }
            ]
        }
        fallback_empty = {
            "candidates": [
                {
                    "content": {"parts": []},
                    "finishReason": "STOP",
                }
            ]
        }

        runner_output = "Runner inspected Chrome tabs and verified 3 active tabs on Hermes."

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_dispatch, turn2_empty, fallback_empty]
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_task:
                mock_task.return_value = {
                    "status": "SUCCESS",
                    "duration_seconds": 4.2,
                    "output_tail": runner_output,
                }

                event = {
                    "channel": "C_TEST",
                    "ts": "100.002",
                    "thread_ts": "100.002",
                    "text": "<@U_GOLEM> inspect tabs",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertTrue(len(self.client.posted_messages) >= 1)
                posted = self.client.posted_messages[-1]["text"]
                self.assertIn("Runner inspected Chrome tabs", posted)
                self.assertIn("⚠️ *Executive Synthesis Failed*", posted)
                self.assertIn("Raw sensory data returned above", posted)


if __name__ == "__main__":
    unittest.main()
