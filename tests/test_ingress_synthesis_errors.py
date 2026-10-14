#!/usr/bin/env python3
"""
tests/test_ingress_synthesis_errors.py
Unit tests for fail-loud architecture, unique error reference generation,
Step 2 synthesis integrity, and JSON payload truncation in Golem IngressGateway.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import re
import os
import sys
import json
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.ingress import IngressGateway, generate_error_reference
from golem.router import SessionRouter


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


class TestIngressSynthesisErrors(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = MockSlackClient()

    def test_generate_error_reference_format_and_uniqueness(self) -> None:
        """Verify generate_error_reference produces uppercase 8-hex error codes with uniqueness."""
        pattern = re.compile(r"^ERR-[0-9A-F]{8}$")
        for _ in range(50):
            ref = generate_error_reference()
            self.assertRegex(ref, pattern)

        # Custom prefix check
        custom_ref = generate_error_reference("CUSTOM")
        self.assertRegex(custom_ref, re.compile(r"^CUSTOM-[0-9A-F]{8}$"))

        # Uniqueness across 1,000 calls
        refs = {generate_error_reference() for _ in range(1000)}
        self.assertEqual(len(refs), 1000)

    async def test_empty_candidate_fails_loudly_with_unique_error_reference(self) -> None:
        """Verify Step 2 with empty candidate text fails loudly without falling back silently to output_tail."""
        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "build artifact"},
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

        runner_output = "Runner finished build with 42 warnings."

        with patch("golem.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_dispatch, turn2_empty, fallback_empty]
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_task:
                mock_task.return_value = {
                    "status": "SUCCESS",
                    "duration_seconds": 2.5,
                    "output_tail": runner_output,
                }

                event = {
                    "channel": "C_TEST",
                    "ts": "200.001",
                    "thread_ts": "200.001",
                    "text": "<@U_GOLEM> build artifact",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertTrue(len(self.client.posted_messages) >= 1)
                posted = self.client.posted_messages[-1]["text"]

                # 1. Must fail loudly with exact header
                self.assertIn("⚠️ *Executive Synthesis Failed* (HTTP 500 / SynthesisError):", posted)
                # 2. Must contain unique error reference
                self.assertRegex(posted, r"• \*Error Reference:\* `ERR-[0-9A-F]{8}`")
                # 3. Must contain diagnostic details and raw sensory notice
                self.assertIn("Synthesis failed with empty candidate text", posted)
                self.assertIn("Raw sensory data returned above without executive synthesis.", posted)
                # 4. Reaction must be boom
                added_reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("boom", added_reactions)
                self.assertNotIn("white_check_mark", added_reactions)

    async def test_two_consecutive_synthesis_failures_have_distinct_error_references(self) -> None:
        """Verify two distinct synthesis failures generate different error reference identifiers."""
        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "check_eyes",
                                    "args": {},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_empty = {"candidates": [{"content": {"parts": []}}]}

        with patch("golem.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            # Per message: initial call, chain call, chain retry, Step 2, fallback.
            mock_gemini.side_effect = [turn1_dispatch] + [turn2_empty] * 4 + [turn1_dispatch] + [turn2_empty] * 4
            with patch.object(self.gateway, "_execute_check_eyes", new_callable=AsyncMock) as mock_eyes:
                mock_eyes.return_value = {"state": "IDLE", "buffer_tail": "idle"}

                event1 = {"channel": "C_TEST", "ts": "300.001", "thread_ts": "300.001", "text": "<@U_GOLEM> check eyes 1"}
                await self.gateway.handle_message(event1, self.client)

                event2 = {"channel": "C_TEST", "ts": "300.002", "thread_ts": "300.002", "text": "<@U_GOLEM> check eyes 2"}
                await self.gateway.handle_message(event2, self.client)

                self.assertTrue(len(self.client.posted_messages) >= 2)
                msg1 = self.client.posted_messages[-2]["text"]
                msg2 = self.client.posted_messages[-1]["text"]

                m1 = re.search(r"ERR-[0-9A-F]{8}", msg1)
                m2 = re.search(r"ERR-[0-9A-F]{8}", msg2)

                self.assertIsNotNone(m1)
                self.assertIsNotNone(m2)
                self.assertNotEqual(m1.group(0), m2.group(0))

    async def test_tool_json_truncation_under_slack_limit(self) -> None:
        """Verify that massive tool output (>50k characters) is truncated to <= 2500 chars and stays under 3,800 chars total."""
        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {"task_description": "massive output test"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_empty = {"candidates": [{"content": {"parts": []}}]}

        massive_output = "X" * 60000

        with patch("golem.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_dispatch, turn2_empty]
            with patch.object(self.gateway, "_execute_backstage_task", new_callable=AsyncMock) as mock_task:
                mock_task.return_value = {
                    "status": "SUCCESS",
                    "duration_seconds": 1.0,
                    "output_tail": massive_output,
                }

                event = {
                    "channel": "C_TEST",
                    "ts": "400.001",
                    "thread_ts": "400.001",
                    "text": "<@U_GOLEM> test massive output",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertTrue(len(self.client.posted_messages) >= 1)
                posted = self.client.posted_messages[-1]["text"]

                # Total message length must strictly satisfy Slack boundary <= 3,800
                self.assertLess(len(posted), 3800)
                # Truncation marker must be present
                self.assertIn("... [truncated]", posted)
                self.assertIn("• *Error Reference:* `ERR-", posted)

    async def test_tool_execution_exception_handled_cleanly_with_error_reference(self) -> None:
        """Verify an exception during tool execution is caught, assigned an error reference, and reported without crashing."""
        turn1_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "check_eyes",
                                    "args": {"pane": "p99"},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        # Step 2 synthesis fails on empty candidates
        turn2_empty = {"candidates": [{"content": {"parts": []}}]}

        with patch("golem.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_dispatch] + [turn2_empty] * 4
            with patch.object(self.gateway, "_execute_check_eyes", side_effect=RuntimeError("PTY coordinate unmapped")):
                event = {
                    "channel": "C_TEST",
                    "ts": "500.001",
                    "thread_ts": "500.001",
                    "text": "<@U_GOLEM> check eyes broken",
                }
                await self.gateway.handle_message(event, self.client)

                self.assertTrue(len(self.client.posted_messages) >= 1)
                posted = self.client.posted_messages[-1]["text"]

                self.assertIn("⚠️ *Executive Synthesis Failed*", posted)
                self.assertIn("• *Error Reference:* `ERR-", posted)
                self.assertIn("PTY coordinate unmapped", posted)
                self.assertIn("RuntimeError", posted)

    async def test_handle_message_unhandled_exception_attaches_error_reference(self) -> None:
        """Verify unhandled exceptions escaping handle_message include an error reference and set reaction boom."""
        with patch.dict(os.environ, {"GOLEM_FORCE_MOUTH": "1"}):
            with patch.object(self.gateway, "_handle_mouth_turn", side_effect=ValueError("Catastrophic test failure")):
                event = {
                    "channel": "C_TEST",
                    "ts": "600.001",
                    "thread_ts": "600.001",
                    "text": "<@U_GOLEM> crash test",
                }
                with self.assertRaises(ValueError):
                    await self.gateway.handle_message(event, self.client)

                self.assertTrue(len(self.client.posted_messages) >= 1)
                err_msg = self.client.posted_messages[-1]["text"]

                self.assertIn("💥 *System Error:*", err_msg)
                self.assertRegex(err_msg, r"• \*Error Reference:\* `ERR-[0-9A-F]{8}`")
                self.assertIn("Catastrophic test failure", err_msg)
                added_reactions = [r["name"] for r in self.client.reactions_added]
                self.assertIn("boom", added_reactions)

    async def test_step2_asymmetric_multicall_filtering(self) -> None:
        """Verify that when candidate contains parallel functionCalls, only the executed call is paired in model_parts."""
        candidate_with_parallel_calls = {
            "content": {
                "parts": [
                    {"text": "I will check both eyes and dispatch a task."},
                    {"functionCall": {"name": "check_eyes", "args": {}}},
                    {"functionCall": {"name": "dispatch_task", "args": {"task_description": "secondary"}}},
                ]
            }
        }

        captured_payload = None

        async def mock_call_gemini(payload, model=None, api_key=None):
            nonlocal captured_payload
            captured_payload = payload
            return {
                "candidates": [
                    {
                        "content": {
                            "parts": [{"text": "Synthesis succeeded after filtered model_parts."}]
                        }
                    }
                ]
            }

        with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call_gemini):
            briefing_text, error_info = await self.gateway._execute_step2_synthesis(
                sys_prompt="System prompt",
                prompt="Check eyes",
                candidate=candidate_with_parallel_calls,
                fn_call={"name": "check_eyes", "args": {}},
                fn_name="check_eyes",
                tool_result={"state": "IDLE"},
                model="gemini-3.8-flash",
                api_key="fake-key",
            )

            self.assertEqual(briefing_text, "Synthesis succeeded after filtered model_parts.")
            self.assertIsNone(error_info)
            self.assertIsNotNone(captured_payload)

            # Check model turn (contents[1])
            model_turn = captured_payload["contents"][1]
            self.assertEqual(model_turn["role"], "model")
            model_parts = model_turn["parts"]

            # Must contain the thought text
            self.assertEqual(model_parts[0], {"text": "I will check both eyes and dispatch a task."})
            # Must contain ONLY the executed functionCall ('check_eyes')
            self.assertEqual(len(model_parts), 2)
            self.assertEqual(model_parts[1]["functionCall"]["name"], "check_eyes")

            # Check user functionResponse turn (contents[2])
            user_turn = captured_payload["contents"][2]
            self.assertEqual(user_turn["role"], "user")
            self.assertEqual(len(user_turn["parts"]), 1)
            self.assertEqual(user_turn["parts"][0]["functionResponse"]["name"], "check_eyes")


if __name__ == "__main__":
    unittest.main()
