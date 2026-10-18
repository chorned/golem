#!/usr/bin/env python3
"""
Unit tests for CHO-272 Token Economics, Byte-Stable Prefix Contract,
3-Tier Model Routing, Tool Compaction, and Timeout Resilience.
"""

import asyncio
import os
import sys
import unittest
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.config import (
    get_architect_model,
    get_workhorse_model,
    get_sentinel_model,
    get_mouth_model,
)
from golem.mouth import (
    calculate_token_cost,
    call_gemini_api,
    build_mouth_system_instruction,
    get_mouth_tools_declaration,
    synthesize_linear_ticket_nudge,
    synthesize_email_briefing,
)
from golem.ingress import (
    compact_tool_output,
    is_high_complexity_turn,
    route_mouth_turn_model,
    IngressGateway,
)


class TestTokenEconomics(unittest.TestCase):
    """Pillar 4: Token Economics & Turn Cost Telemetry."""

    def test_calculate_token_cost_architect(self):
        """Verify cost calculation for Tier 1 Architect (gemini-4-argon)."""
        # 10,000 prompt tokens (8,000 cached, 2,000 uncached), 500 output tokens
        # uncached: 2000 * 2.50 / 1M = 0.005
        # cached: 8000 * 0.625 / 1M = 0.005
        # output: 500 * 10.00 / 1M = 0.005
        # total = 0.015
        cost = calculate_token_cost(
            model="gemini-4-argon",
            prompt_tokens=10000,
            cached_tokens=8000,
            candidates_tokens=500,
        )
        self.assertAlmostEqual(cost, 0.015, places=4)

    def test_calculate_token_cost_workhorse(self):
        """Verify cost calculation for Tier 2 Workhorse (gemini-3.8-flash)."""
        # 10,000 prompt tokens (8,000 cached, 2,000 uncached), 1,000 output tokens
        # uncached: 2000 * 0.10 / 1M = 0.0002
        # cached: 8000 * 0.025 / 1M = 0.0002
        # output: 1000 * 0.40 / 1M = 0.0004
        # total = 0.0008
        cost = calculate_token_cost(
            model="gemini-3.8-flash",
            prompt_tokens=10000,
            cached_tokens=8000,
            candidates_tokens=1000,
        )
        self.assertAlmostEqual(cost, 0.0008, places=4)

    def test_calculate_token_cost_sentinel(self):
        """Verify cost calculation for Tier 3 Sentinel (gemini-3.5-flash-lite)."""
        # 10,000 prompt tokens (0 cached, 10,000 uncached), 1,000 output tokens
        # uncached: 10000 * 0.075 / 1M = 0.00075
        # cached: 0
        # output: 1000 * 0.30 / 1M = 0.0003
        # total = 0.00105
        cost = calculate_token_cost(
            model="gemini-3.5-flash-lite",
            prompt_tokens=10000,
            cached_tokens=0,
            candidates_tokens=1000,
        )
        self.assertAlmostEqual(cost, 0.00105, places=5)

    def test_cache_hit_rate_computation(self):
        """Verify cache hit rate computation logic."""
        prompt_tokens = 50000
        cached_tokens = 40000
        hit_rate = (cached_tokens / prompt_tokens * 100.0) if prompt_tokens > 0 else 0.0
        self.assertEqual(hit_rate, 80.0)

        zero_prompt_hit_rate = (0 / 0 * 100.0) if 0 > 0 else 0.0
        self.assertEqual(zero_prompt_hit_rate, 0.0)


class TestPrefixContract(unittest.IsolatedAsyncioTestCase):
    """Pillar 1: 100% Byte-Stable Prefix Contract."""

    def test_deterministic_tool_sorting(self):
        """Verify get_mouth_tools_declaration() sorts functionDeclarations alphabetically by name."""
        decl = get_mouth_tools_declaration()
        self.assertIsInstance(decl, list)
        self.assertGreaterEqual(len(decl), 1)

        fn_decls = decl[0].get("functionDeclarations", [])
        self.assertGreater(len(fn_decls), 5)
        names = [f["name"] for f in fn_decls]
        sorted_names = sorted(names)
        self.assertEqual(names, sorted_names, "functionDeclarations must be deterministically sorted by name")

    def test_static_system_instruction(self):
        """Verify build_mouth_system_instruction() is 100% byte-static across invocations."""
        from golem.mouth import BASE_CHIEF_OF_STAFF_INSTRUCTION
        import re
        inst1 = build_mouth_system_instruction()
        inst2 = build_mouth_system_instruction()
        self.assertEqual(inst1, inst2)
        self.assertEqual(re.findall(r"CHO-\d+", BASE_CHIEF_OF_STAFF_INSTRUCTION), [])

    async def test_execute_step2_synthesis_preserves_prefix_and_tools(self):
        """Verify _execute_step2_synthesis preserves sys_prompt and tools byte-for-byte and appends directive to contents."""
        gateway = IngressGateway()
        captured_payloads: List[Dict[str, Any]] = []

        async def fake_call_gemini_api(payload, model=None, api_key=None, timeout_seconds=60.0):
            captured_payloads.append(payload)
            return {
                "candidates": [
                    {
                        "content": {
                            "role": "model",
                            "parts": [{"text": "Executive briefing: all systems nominal."}],
                        }
                    }
                ]
            }

        gateway._call_gemini_api = fake_call_gemini_api

        sys_prompt = "You are AGY Chief of Staff. Deterministic prefix."
        user_prompt = "What is the status of the fleet?"
        tools = get_mouth_tools_declaration()
        active_model = "gemini-3.8-flash"

        briefing, err = await gateway._execute_step2_synthesis(
            sys_prompt=sys_prompt,
            prompt=user_prompt,
            candidate={"content": {"parts": [{"functionCall": {"name": "check_nose", "args": {}}}]}},
            fn_call={"name": "check_nose", "args": {}},
            fn_name="check_nose",
            tool_result={"smells_off": False, "verdict": "NOMINAL"},
            model=active_model,
            api_key="test-api-key",
            tools=tools,
        )

        self.assertEqual(briefing, "Executive briefing: all systems nominal.")
        self.assertIsNone(err)
        self.assertEqual(len(captured_payloads), 1)

        payload = captured_payloads[0]

        # 1. System instruction MUST be 100% byte-identical to Step 1 (NO mutation)
        self.assertEqual(
            payload["systemInstruction"]["parts"][0]["text"],
            sys_prompt,
            "System instruction was mutated in Step 2; must remain byte-identical for KV cache prefix!",
        )

        # 2. Tools declaration MUST be identical and present (Refinement 1)
        self.assertEqual(payload["tools"], tools, "Tools declaration must remain present and identical in Step 2")

        # 3. Contents MUST contain the synthesis directive in the final user turn part
        contents = payload["contents"]
        self.assertEqual(contents[-1]["role"], "user")
        user_parts = contents[-1]["parts"]
        text_parts = [p["text"] for p in user_parts if "text" in p]
        self.assertTrue(any("EXECUTION COMPLETE" in t for t in text_parts))
        self.assertTrue(any("CRITICAL INSTRUCTION FOR SYNTHESIS" in t for t in text_parts))


class TestModelAffinityAndRouting(unittest.IsolatedAsyncioTestCase):
    """Pillar 2: 3-Tier Model Router & Model Affinity."""

    def test_tier_model_resolution(self):
        """Verify Tier 1, Tier 2, Tier 3 resolution defaults and overrides."""
        self.assertEqual(get_architect_model(), "gemini-4-argon")
        self.assertEqual(get_workhorse_model(), "gemini-3.8-flash")
        self.assertEqual(get_sentinel_model(), "gemini-3.5-flash-lite")

        with patch.dict(os.environ, {"GOLEM_MODEL_ARCHITECT": "custom-architect-model"}):
            self.assertEqual(get_architect_model(), "custom-architect-model")

        with patch.dict(os.environ, {"GOLEM_MODEL_WORKHORSE": "custom-workhorse-model"}):
            self.assertEqual(get_workhorse_model(), "custom-workhorse-model")

        with patch.dict(os.environ, {"GOLEM_MODEL_SENTINEL": "custom-sentinel-model"}):
            self.assertEqual(get_sentinel_model(), "custom-sentinel-model")

    def test_route_mouth_turn_model(self):
        """Verify routing between Tier 1 Architect and Tier 2 Workhorse."""
        # Routine check-ins and fleet inquiries route to Tier 2 Workhorse
        self.assertEqual(route_mouth_turn_model("Hello, how's our fleet doing today?"), get_workhorse_model())
        self.assertEqual(route_mouth_turn_model("Check docker status on hermes"), get_workhorse_model())

        # High-complexity architectural tasks route to Tier 1 Architect
        self.assertEqual(route_mouth_turn_model("Please design a multi-phase architecture for the database migration"), get_architect_model())
        self.assertEqual(route_mouth_turn_model("Refactor the ingress gateway state machine to eliminate race conditions"), get_architect_model())

        # Explicit test patch / override is respected
        self.assertEqual(route_mouth_turn_model("Hello", explicit_model="test-mock-model"), "test-mock-model")

    async def test_linear_ticket_nudge_routes_to_workhorse(self):
        """Verify synthesize_linear_ticket_nudge routes to get_workhorse_model()."""
        ticket = {"identifier": "ENG-100", "title": "Implement caching", "state": {"name": "Todo"}}
        with patch.dict(os.environ, {"FIRSTMATE_TEST_MODE": "0", "GOLEM_TEST_MODE": "0"}):
            with patch("golem.mouth.get_gemini_api_key", return_value="fake-key"):
                with patch("golem.mouth.call_gemini_api", new_callable=AsyncMock) as mock_call:
                    mock_call.return_value = {
                        "candidates": [{"content": {"parts": [{"text": "Ready to kick off Phase 1?"}]}}]
                    }
                    res = await synthesize_linear_ticket_nudge(ticket)
                    self.assertEqual(res, "Ready to kick off Phase 1?")
                    mock_call.assert_called_once()
                    _, kwargs = mock_call.call_args
                    self.assertEqual(kwargs.get("model"), get_workhorse_model())
                    self.assertEqual(kwargs.get("timeout_seconds"), 60.0)

    async def test_email_briefing_routes_to_workhorse(self):
        """Verify synthesize_email_briefing routes to get_workhorse_model()."""
        with patch.dict(os.environ, {"FIRSTMATE_TEST_MODE": "0", "GOLEM_TEST_MODE": "0"}):
            with patch("golem.mouth.get_gemini_api_key", return_value="fake-key"):
                with patch("golem.mouth.call_gemini_api", new_callable=AsyncMock) as mock_call:
                    mock_call.return_value = {
                        "candidates": [{"content": {"parts": [{"text": "Here is the shared event briefing."}]}}]
                    }
                    briefing, cta = await synthesize_email_briefing(
                        sender="Carl <carl@example.com>",
                        subject="Upcoming Concert",
                        body="Check out these tickets",
                    )
                    self.assertEqual(briefing, "Here is the shared event briefing.")
                    mock_call.assert_called_once()
                    _, kwargs = mock_call.call_args
                    self.assertEqual(kwargs.get("model"), get_workhorse_model())
                    self.assertEqual(kwargs.get("timeout_seconds"), 60.0)

    async def test_two_strike_auto_escalation_in_mouth_turn(self):
        """Verify that 2 consecutive failures on Tier 2 Workhorse auto-escalates to Tier 1 Architect."""
        gateway = IngressGateway()
        called_models: List[str] = []

        async def fake_call_api(payload, model=None, api_key=None, timeout_seconds=60.0):
            called_models.append(model)
            if len(called_models) < 3:
                # First 2 attempts fail (error / empty)
                return {"_error": True, "code": 500, "message": "Simulated error"}
            # 3rd attempt (on escalated Tier 1 Architect) succeeds
            return {
                "candidates": [
                    {
                        "content": {
                            "role": "model",
                            "parts": [{"text": "Resolved via escalated Architect model."}],
                        }
                    }
                ]
            }

        gateway._call_gemini_api = fake_call_api
        gateway._post_response = AsyncMock()
        gateway._broadcast_mouth_briefing = AsyncMock()

        session = MagicMock()
        client = MagicMock()

        with patch("golem.ingress.get_gemini_api_key", return_value="fake-key"):
            await gateway._run_mouth_turn(
                session=session,
                prompt="Check the status of our fleet",  # Routine turn -> starts on Workhorse
                client=client,
                channel="C123",
                thread_ts="T123",
                ts="TS123",
                is_imessage=False,
            )

        # First attempt on gemini-3.8-flash (fails)
        self.assertEqual(called_models[0], "gemini-3.8-flash")
        # Strike 1 retry on gemini-3.8-flash (fails)
        self.assertEqual(called_models[1], "gemini-3.8-flash")
        # Strike 2 auto-escalates to gemini-4-argon (succeeds)
        self.assertEqual(called_models[2], "gemini-4-argon")


class TestToolOutputCompaction(unittest.TestCase):
    """Pillar 3: Pre-Append Tool Output Compaction."""

    def test_compact_read_project_file_large(self):
        """Verify read_project_file caps files > 12,000 chars to 100 head + summary + 50 tail lines."""
        lines = [f"line {i}: content payload with lots of filler text" for i in range(300)]
        big_content = "\n".join(lines)
        self.assertGreater(len(big_content), 12000)

        raw_result = {"status": "SUCCESS", "type": "file", "content": big_content}
        compacted = compact_tool_output("read_project_file", raw_result)

        self.assertTrue(compacted.get("compacted"))
        c_lines = compacted["content"].splitlines()
        # Head (100) + summary line(s) + tail (50)
        self.assertIn("line 0: content payload", c_lines[0])
        self.assertIn("line 299: content payload", c_lines[-1])
        self.assertTrue(any("truncated for context hygiene" in line for line in c_lines))
        self.assertLess(len(c_lines), 160)

    def test_compact_read_project_file_small(self):
        """Verify small files remain untouched."""
        small_result = {"status": "SUCCESS", "type": "file", "content": "short file content"}
        compacted = compact_tool_output("read_project_file", small_result)
        self.assertEqual(compacted, small_result)

    def test_compact_dispatch_task_and_eyes(self):
        """Verify buffer tails in dispatch_task and check_eyes are capped to 100 lines."""
        many_lines = "\n".join([f"log row {i}" for i in range(250)])
        result = {"status": "SUCCESS", "output": many_lines}
        compacted = compact_tool_output("dispatch_task", result)
        out_lines = compacted["output"].splitlines()
        self.assertEqual(len(out_lines), 100)
        self.assertEqual(out_lines[-1], "log row 249")
        self.assertTrue(compacted.get("output_truncated"))

        eyes_result = {"state": "IDLE", "buffer_tail": many_lines}
        compacted_eyes = compact_tool_output("check_eyes", eyes_result)
        eye_lines = compacted_eyes["buffer_tail"].splitlines()
        self.assertEqual(len(eye_lines), 100)

    def test_compact_plan_task_with_subtasks(self):
        """Verify plan_task_with_subtasks prunes verbose internal schemas and keeps key, title, state, url."""
        raw_plan = {
            "status": "SUCCESS",
            "parent": {
                "id": "uuid-12345",
                "identifier": "ENG-101",
                "title": "Migrate database",
                "url": "https://linear.app/issue/ENG-101",
                "internal_state_object": {"__typename": "State", "id": "s1", "name": "In Progress"},
            },
            "subtasks": [
                {
                    "id": "uuid-sub-1",
                    "identifier": "ENG-102",
                    "title": "[1/2] Export dump",
                    "url": "https://linear.app/issue/ENG-102",
                    "internal_schema_cruft": {"nodes": [1, 2, 3]},
                }
            ],
            "runtime_telemetry": {"heavy": "diagnostics", "nested": {}},
        }
        compacted = compact_tool_output("plan_task_with_subtasks", raw_plan)
        self.assertNotIn("runtime_telemetry", compacted)
        self.assertIn("parent", compacted)
        self.assertEqual(compacted["parent"]["identifier"], "ENG-101")
        self.assertEqual(compacted["parent"]["url"], "https://linear.app/issue/ENG-101")
        self.assertEqual(len(compacted["subtasks"]), 1)
        self.assertEqual(compacted["subtasks"][0]["identifier"], "ENG-102")


class TestTimeoutAndErrorFormatting(unittest.IsolatedAsyncioTestCase):
    """Immediate Fix: 60s Timeout & TimeoutError String Formatting."""

    def test_default_timeout_is_60s(self):
        """Verify call_gemini_api defaults to 60.0s timeout."""
        import inspect
        sig = inspect.signature(call_gemini_api)
        self.assertEqual(sig.parameters["timeout_seconds"].default, 60.0)

    async def test_timeout_error_formatting(self):
        """Verify TimeoutError exception produces clean diagnostic instead of empty string."""
        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.side_effect = asyncio.TimeoutError()
            res = await call_gemini_api(
                payload={"contents": [{"role": "user", "parts": [{"text": "Hi"}]}]},
                model="gemini-4-argon",
                api_key="fake-key",
                timeout_seconds=60.0,
            )
            self.assertIsNotNone(res)
            self.assertTrue(res.get("_error"))
            self.assertEqual(res.get("code"), 500)
            self.assertIn("timed out after 60.0s", res.get("raw_error", ""))
            self.assertIn("timed out after 60.0s", res.get("message", ""))
            self.assertNotEqual(res.get("raw_error"), "", "raw_error must not be blank on TimeoutError!")


if __name__ == "__main__":
    unittest.main()
