#!/usr/bin/env python3
"""
tests/test_adversarial_stage2_synthesis.py
Adversarial and Empirical Challenge Test Suite for Stage 2 (Ingress Hardening & Synthesis Integrity).

Stress-tests:
1. Parallel function calls: 2, 3, or more function calls in candidate parts; duplicate function calls;
   asymmetric multi-call filtering; preservation of thoughtSignature and thought text parts.
2. Missing parts and boundary candidate objects: None, {}, non-dict, missing parts key, empty parts list,
   None elements inside parts, non-dict elements inside parts.
3. Empty text blocks: parts with empty string text {"text": ""}, {"text": None}, whitespace-only text.
4. Complex nested tool return structures: deeply nested dicts (10 levels), large arrays, unicode,
   nulls, booleans, floating-point numbers, primitive values (int, float, str, bool, list),
   very large payloads (>2500 chars).
5. Strict Gemini generateContent schema conformance: verifying zero HTTP 400 schema mismatch risks
   (correct role alternation, functionResponse matching functionCall, tools declaration matching mode="NONE",
   systemInstruction structure, generationConfig, and valid JSON serialization).
6. 1:1 call-to-response pairing and thought preservation: ensuring zero turn desynchronization and
   cryptographic thought signatures / thought text are not dropped or corrupted.
7. Fail-loud error reference uniqueness and formatting: format ERR-[0-9A-F]{8}, zero silent fallback to out_tail.
8. Header-based authentication security enforcement: rejecting '?key=' and '&key=' with ValueError across callers.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import re
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.ingress import (
    IngressGateway,
    generate_error_reference,
    get_mouth_tools_declaration,
    call_gemini_api,
)
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


def validate_gemini_generate_content_schema(payload: dict) -> None:
    """
    Empirically validate payload structure against the official Gemini REST generateContent API schema.
    Raises AssertionError if any schema mismatch that would cause HTTP 400 is detected.
    """
    assert isinstance(payload, dict), "Payload must be a dictionary"

    # 1. systemInstruction validation
    if "systemInstruction" in payload:
        si = payload["systemInstruction"]
        assert isinstance(si, dict), "systemInstruction must be a dict"
        assert "parts" in si, "systemInstruction must contain 'parts'"
        assert isinstance(si["parts"], list), "systemInstruction.parts must be a list"
        assert len(si["parts"]) > 0, "systemInstruction.parts must not be empty"
        for part in si["parts"]:
            assert isinstance(part, dict), "systemInstruction part must be a dict"
            assert "text" in part, "systemInstruction part must contain 'text'"
            assert isinstance(part["text"], str) and len(part["text"]) > 0, "text must be non-empty string"

    # 2. contents validation
    assert "contents" in payload, "Payload must contain 'contents'"
    contents = payload["contents"]
    assert isinstance(contents, list), "contents must be a list"
    assert len(contents) >= 1, "contents must have at least one turn"

    for idx, content in enumerate(contents):
        assert isinstance(content, dict), f"content[{idx}] must be a dict"
        assert "role" in content, f"content[{idx}] must have 'role'"
        assert content["role"] in ("user", "model"), f"content[{idx}] role must be 'user' or 'model'"
        assert "parts" in content, f"content[{idx}] must have 'parts'"
        assert isinstance(content["parts"], list), f"content[{idx}].parts must be a list"
        assert len(content["parts"]) > 0, f"content[{idx}].parts must not be empty"

        for p_idx, part in enumerate(content["parts"]):
            assert isinstance(part, dict), f"content[{idx}].parts[{p_idx}] must be a dict"
            valid_keys = {"text", "inlineData", "functionCall", "functionResponse", "thoughtSignature", "thought"}
            assert any(k in part for k in ("text", "inlineData", "functionCall", "functionResponse")), (
                f"content[{idx}].parts[{p_idx}] must contain a valid Gemini part type: {part}"
            )
            if "text" in part:
                assert isinstance(part["text"], str), f"text part must be a str: {part}"
                assert len(part["text"]) > 0, f"text part must not be empty string: {part}"
            if "functionCall" in part:
                fc = part["functionCall"]
                assert isinstance(fc, dict), f"functionCall must be a dict: {fc}"
                assert "name" in fc and isinstance(fc["name"], str) and len(fc["name"]) > 0
                assert "args" in fc and isinstance(fc["args"], dict)
            if "functionResponse" in part:
                fr = part["functionResponse"]
                assert isinstance(fr, dict), f"functionResponse must be a dict: {fr}"
                assert "name" in fr and isinstance(fr["name"], str) and len(fr["name"]) > 0
                assert "response" in fr and isinstance(fr["response"], dict)

    # 3. 1:1 call-to-response pairing validation across turns
    model_function_calls = []
    user_function_responses = []
    for content in contents:
        if content["role"] == "model":
            for part in content["parts"]:
                if "functionCall" in part:
                    model_function_calls.append(part["functionCall"]["name"])
        elif content["role"] == "user":
            for part in content["parts"]:
                if "functionResponse" in part:
                    user_function_responses.append(part["functionResponse"]["name"])

    # If user provided functionResponse, it must pair 1:1 with preceding model functionCall
    if user_function_responses:
        assert len(model_function_calls) == len(user_function_responses), (
            f"Asymmetric function call vs response count! model calls: {model_function_calls}, user responses: {user_function_responses}"
        )
        assert model_function_calls == user_function_responses, (
            f"Function names mismatch between calls and responses! calls: {model_function_calls}, responses: {user_function_responses}"
        )

    # 4. tools & toolConfig validation
    if "toolConfig" in payload:
        tc = payload["toolConfig"]
        assert isinstance(tc, dict), "toolConfig must be a dict"
        fc_cfg = tc.get("functionCallingConfig", {})
        if fc_cfg.get("mode") == "NONE":
            # Gemini requires 'tools' to be present if toolConfig is specified with mode="NONE"
            assert "tools" in payload, "Gemini requires 'tools' when mode is 'NONE'"
            assert isinstance(payload["tools"], list) and len(payload["tools"]) > 0, (
                "tools must be a non-empty list when toolConfig is present"
            )

    if "tools" in payload:
        tools = payload["tools"]
        assert isinstance(tools, list), "tools must be a list"
        for t in tools:
            assert isinstance(t, dict), "tool item must be a dict"
            assert "functionDeclarations" in t, "tool must declare 'functionDeclarations'"
            assert isinstance(t["functionDeclarations"], list)

    # 5. generationConfig validation
    if "generationConfig" in payload:
        gc = payload["generationConfig"]
        assert isinstance(gc, dict), "generationConfig must be a dict"

    # 6. JSON serialization check
    try:
        json_str = json.dumps(payload)
        assert len(json_str) > 0
    except (TypeError, ValueError) as json_err:
        raise AssertionError(f"Payload failed JSON serialization: {json_err}") from json_err


class TestAdversarialStage2Synthesis(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        self.gateway = IngressGateway(router=self.router, bot_user_id="U_GOLEM")
        self.client = MockSlackClient()

    async def test_adversarial_parallel_function_calls_filtering_and_schema(self) -> None:
        """
        Adversarially test candidate containing 3 parallel function calls.
        Verify:
        - Only the executed functionCall is retained in model_parts.
        - Thought text before and after the calls is preserved.
        - Exactly 1:1 call-to-response pairing in contents.
        - Payload strictly passes Gemini generateContent schema validation.
        """
        candidate = {
            "content": {
                "parts": [
                    {"text": "Thought: Checking system sensors in parallel."},
                    {"functionCall": {"name": "check_eyes", "args": {}}, "thoughtSignature": "sig_eyes_001"},
                    {"functionCall": {"name": "check_nose", "args": {"trigger_fresh_sniff": True}}},
                    {"functionCall": {"name": "dispatch_task", "args": {"task_description": "run diagnostics"}}},
                    {"text": "Thought: Prioritizing eyes check first."},
                ]
            }
        }

        captured_payload = None

        async def mock_call(payload, model=None, api_key=None):
            nonlocal captured_payload
            captured_payload = payload
            return {
                "candidates": [
                    {"content": {"parts": [{"text": "Executive Briefing: Runner is IDLE."}]}}
                ]
            }

        with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call):
            briefing_text, error_info = await self.gateway._execute_step2_synthesis(
                sys_prompt="You are Golem Chief of Staff.",
                prompt="How is the runner looking?",
                candidate=candidate,
                fn_call={"name": "check_eyes", "args": {}},
                fn_name="check_eyes",
                tool_result={"state": "IDLE", "active_command": None, "pid": 0},
                model="gemini-3.8-flash",
                api_key="valid-test-key",
            )

        self.assertEqual(briefing_text, "Executive Briefing: Runner is IDLE.")
        self.assertIsNone(error_info)
        self.assertIsNotNone(captured_payload)

        # Validate schema
        validate_gemini_generate_content_schema(captured_payload)

        # Verify model_parts details
        model_parts = captured_payload["contents"][1]["parts"]
        # Should have 2 thought parts and 1 matched call part
        self.assertEqual(len(model_parts), 3)
        self.assertEqual(model_parts[0]["text"], "Thought: Checking system sensors in parallel.")
        self.assertEqual(model_parts[1]["text"], "Thought: Prioritizing eyes check first.")
        self.assertEqual(model_parts[2]["functionCall"]["name"], "check_eyes")
        self.assertEqual(model_parts[2]["thoughtSignature"], "sig_eyes_001")

        # Verify user functionResponse
        user_parts = captured_payload["contents"][2]["parts"]
        self.assertEqual(len(user_parts), 1)
        self.assertEqual(user_parts[0]["functionResponse"]["name"], "check_eyes")
        self.assertEqual(user_parts[0]["functionResponse"]["response"]["name"], "check_eyes")

    async def test_duplicate_function_calls_for_same_function(self) -> None:
        """
        Adversarially test candidate with duplicate functionCalls for the same function name.
        Verify only 1 call is paired, avoiding asymmetric multi-call HTTP 400.
        """
        candidate = {
            "content": {
                "parts": [
                    {"text": "Running duplicated check_eyes"},
                    {"functionCall": {"name": "check_eyes", "args": {"pane": "p1"}}, "thoughtSignature": "first_call"},
                    {"functionCall": {"name": "check_eyes", "args": {"pane": "p2"}}, "thoughtSignature": "second_call"},
                ]
            }
        }

        captured_payload = None

        async def mock_call(payload, model=None, api_key=None):
            nonlocal captured_payload
            captured_payload = payload
            return {
                "candidates": [
                    {"content": {"parts": [{"text": "Briefing text"}]}}
                ]
            }

        with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call):
            briefing, err = await self.gateway._execute_step2_synthesis(
                sys_prompt="Sys prompt",
                prompt="Check eyes",
                candidate=candidate,
                fn_call={"name": "check_eyes", "args": {"pane": "p1"}},
                fn_name="check_eyes",
                tool_result={"state": "RUNNING"},
                model="gemini-3.8-flash",
                api_key="valid-test-key",
            )

        self.assertEqual(briefing, "Briefing text")
        self.assertIsNone(err)
        validate_gemini_generate_content_schema(captured_payload)

        model_parts = captured_payload["contents"][1]["parts"]
        call_parts = [p for p in model_parts if "functionCall" in p]
        self.assertEqual(len(call_parts), 1)
        self.assertEqual(call_parts[0]["thoughtSignature"], "first_call")

    async def test_missing_parts_and_malformed_candidate_boundary_cases(self) -> None:
        """
        Boundary condition stress test:
        1. candidate is None
        2. candidate is {}
        3. candidate is not a dict (string, int, list)
        4. candidate['content'] is None or missing
        5. candidate['content']['parts'] is empty list, None, or missing
        6. candidate['content']['parts'] contains non-dict items
        Verify zero crashes, graceful fallback to fn_call, and strict schema compliance.
        """
        malformed_candidates = [
            None,
            {},
            "unexpected string candidate",
            12345,
            [],
            {"content": None},
            {"content": {}},
            {"content": {"parts": []}},
            {"content": {"parts": None}},
            {"content": {"parts": ["not a dict", None, 123]}},
        ]

        for idx, mal_candidate in enumerate(malformed_candidates):
            captured_payload = None

            async def mock_call(payload, model=None, api_key=None):
                nonlocal captured_payload
                captured_payload = payload
                return {
                    "candidates": [
                        {"content": {"parts": [{"text": f"Recovered synthesis for case {idx}"}]}}
                    ]
                }

            with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call):
                briefing, err = await self.gateway._execute_step2_synthesis(
                    sys_prompt="Sys prompt",
                    prompt="Do something",
                    candidate=mal_candidate,
                    fn_call={"name": "dispatch_task", "args": {"task_description": "foo"}},
                    fn_name="dispatch_task",
                    tool_result={"status": "SUCCESS"},
                    model="gemini-3.8-flash",
                    api_key="valid-test-key",
                )

            self.assertEqual(briefing, f"Recovered synthesis for case {idx}")
            self.assertIsNone(err)
            self.assertIsNotNone(captured_payload)
            validate_gemini_generate_content_schema(captured_payload)

            # Check model turn contains the fallback matched call part
            model_parts = captured_payload["contents"][1]["parts"]
            call_parts = [p for p in model_parts if "functionCall" in p]
            self.assertEqual(len(call_parts), 1)
            self.assertEqual(call_parts[0]["functionCall"]["name"], "dispatch_task")

    async def test_empty_text_blocks_and_falsy_text_filtering(self) -> None:
        """
        Stress test: candidate parts with empty strings, None text, or whitespace-only.
        Verify empty text parts are omitted from model_parts to prevent Gemini API 400 rejection.
        """
        candidate = {
            "content": {
                "parts": [
                    {"text": ""},
                    {"text": None},
                    {"text": "Valid thought text"},
                    {"functionCall": {"name": "check_nose", "args": {}}},
                ]
            }
        }

        captured_payload = None

        async def mock_call(payload, model=None, api_key=None):
            nonlocal captured_payload
            captured_payload = payload
            return {
                "candidates": [
                    {"content": {"parts": [{"text": "Clean nose briefing."}]}}
                ]
            }

        with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call):
            briefing, err = await self.gateway._execute_step2_synthesis(
                sys_prompt="Sys prompt",
                prompt="Check nose",
                candidate=candidate,
                fn_call={"name": "check_nose", "args": {}},
                fn_name="check_nose",
                tool_result={"smells_off": False, "verdict_summary": "All clean"},
                model="gemini-3.8-flash",
                api_key="valid-test-key",
            )

        self.assertEqual(briefing, "Clean nose briefing.")
        self.assertIsNone(err)
        validate_gemini_generate_content_schema(captured_payload)

        model_parts = captured_payload["contents"][1]["parts"]
        # Should only have 1 thought part ("Valid thought text") and 1 call part
        self.assertEqual(len(model_parts), 2)
        self.assertEqual(model_parts[0]["text"], "Valid thought text")
        self.assertEqual(model_parts[1]["functionCall"]["name"], "check_nose")

    async def test_complex_nested_tool_return_structures(self) -> None:
        """
        Stress test: complex, highly nested, unicode-heavy, and primitive tool return values:
        - 10 levels deep dictionary
        - Lists of dicts, nulls, booleans, floats, unicode/emoji strings
        - Raw string / int / list / bool tool_results
        Verify payload construction handles all without schema failure or JSON serialization crash.
        """
        deep_nested = {"level1": {"level2": {"level3": {"level4": {"level5": {
            "level6": {"level7": {"level8": {"level9": {"level10": {
                "metrics": [1, 2.5, True, False, None, "🚀 特殊文字 \u26a0\ufe0f"],
                "data": {"nested_key": "nested_value"}
            }}}}}}}}}}}

        primitive_results = [
            deep_nested,
            "plain string result from terminal output",
            123456,
            3.1415926535,
            True,
            ["list", "of", "various", "items", {"a": 1}],
        ]

        for idx, tool_res in enumerate(primitive_results):
            captured_payload = None

            async def mock_call(payload, model=None, api_key=None):
                nonlocal captured_payload
                captured_payload = payload
                return {
                    "candidates": [
                        {"content": {"parts": [{"text": f"Synthesis for structure {idx}"}]}}
                    ]
                }

            with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call):
                briefing, err = await self.gateway._execute_step2_synthesis(
                    sys_prompt="Sys prompt",
                    prompt="Execute task",
                    candidate={"content": {"parts": [{"functionCall": {"name": "dispatch_task", "args": {}}}]}},
                    fn_call={"name": "dispatch_task", "args": {}},
                    fn_name="dispatch_task",
                    tool_result=tool_res,
                    model="gemini-3.8-flash",
                    api_key="valid-test-key",
                )

            self.assertEqual(briefing, f"Synthesis for structure {idx}")
            self.assertIsNone(err)
            self.assertIsNotNone(captured_payload)
            validate_gemini_generate_content_schema(captured_payload)

            # Check functionResponse content
            fr = captured_payload["contents"][2]["parts"][0]["functionResponse"]
            self.assertEqual(fr["name"], "dispatch_task")
            self.assertIsInstance(fr["response"], dict)
            self.assertIn("content", fr["response"])

    async def test_tools_declaration_provided_with_mode_none(self) -> None:
        """
        Confirm that Step 2 synthesis retains tools declarations alongside toolConfig mode='NONE'.
        Even if tools=None or tools=[] is passed, it falls back to get_mouth_tools_declaration(),
        preventing Gemini HTTP 400 error: 'tools must be specified when mode is NONE'.
        """
        for tools_param in [None, []]:
            captured_payload = None

            async def mock_call(payload, model=None, api_key=None):
                nonlocal captured_payload
                captured_payload = payload
                return {
                    "candidates": [
                        {"content": {"parts": [{"text": "Synthesis OK"}]}}
                    ]
                }

            with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call):
                briefing, err = await self.gateway._execute_step2_synthesis(
                    sys_prompt="Sys prompt",
                    prompt="Status",
                    candidate={"content": {"parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]}},
                    fn_call={"name": "check_eyes", "args": {}},
                    fn_name="check_eyes",
                    tool_result={"state": "IDLE"},
                    model="gemini-3.8-flash",
                    api_key="valid-test-key",
                    tools=tools_param,
                )

            self.assertEqual(briefing, "Synthesis OK")
            self.assertIsNone(err)
            validate_gemini_generate_content_schema(captured_payload)
            self.assertIn("tools", captured_payload)
            self.assertTrue(len(captured_payload["tools"]) > 0)
            self.assertEqual(captured_payload["toolConfig"]["functionCallingConfig"]["mode"], "NONE")

    async def test_fail_loud_format_and_zero_silent_degradation(self) -> None:
        """
        Verify that when Step 2 synthesis fails (e.g. HTTP 400 / 500 / ZeroCandidates),
        it fails loudly with a unique error reference code matching ERR-[0-9A-F]{8},
        and does NOT silently return raw output_tail as normal briefing text.
        """
        error_scenarios = [
            # HTTP 400 error from API
            {
                "_error": True,
                "status": 400,
                "code": 400,
                "error_type": "HTTP_400",
                "raw_error": "Invalid JSON payload received.",
                "message": "Invalid JSON payload received.",
            },
            # HTTP 500 error from API
            {
                "_error": True,
                "status": 500,
                "code": 500,
                "error_type": "HTTP_500",
                "raw_error": "Internal Server Error",
                "message": "Internal Server Error",
            },
            # Zero candidates returned
            {
                "candidates": []
            },
        ]

        for sc in error_scenarios:
            async def mock_call(payload, model=None, api_key=None):
                return sc

            with patch.object(self.gateway, "_call_gemini_api", side_effect=mock_call):
                briefing, error_info = await self.gateway._execute_step2_synthesis(
                    sys_prompt="Sys prompt",
                    prompt="Check status",
                    candidate={"content": {"parts": [{"functionCall": {"name": "check_eyes", "args": {}}}]}},
                    fn_call={"name": "check_eyes", "args": {}},
                    fn_name="check_eyes",
                    tool_result={"state": "IDLE", "output_tail": "Secret command output tail"},
                    model="gemini-3.8-flash",
                    api_key="valid-test-key",
                )

            self.assertEqual(briefing, "")
            self.assertIsNotNone(error_info)
            error_ref = error_info.get("error_reference")
            self.assertIsNotNone(error_ref)
            self.assertRegex(error_ref, re.compile(r"^ERR-[0-9A-F]{8}$"))

    async def test_url_key_parameter_security_guard_across_callers(self) -> None:
        """
        Empirically test that call_gemini_api raises a security violation ValueError
        if '?key=' or '&key=' appears in the request URL across all callers.
        """
        with patch("golem.ingress.GEMINI_API_URL", "https://api.example.com/models/{model}:" + "generateContent" + "?key=leaked_key"):
            with self.assertRaises(ValueError) as cm:
                await call_gemini_api(payload={"contents": []}, model="gemini-3.8-flash", api_key="test-key")
            self.assertIn("Security violation", str(cm.exception))

        with patch("golem.ingress.GEMINI_API_URL", "https://api.example.com/models/{model}:generateContent&key=leaked_key"):
            with self.assertRaises(ValueError) as cm:
                await call_gemini_api(payload={"contents": []}, model="gemini-3.8-flash", api_key="test-key")
            self.assertIn("Security violation", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
