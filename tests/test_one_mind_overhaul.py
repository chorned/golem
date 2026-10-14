#!/usr/bin/env python3
"""
tests/test_one_mind_overhaul.py
Integration test suite for the Golem 'One Mind' Surgical Architecture Overhaul.

Verifies the 6 key architectural pillars:
1. Zero `CHO-\\d+` strings in `BASE_CHIEF_OF_STAFF_INSTRUCTION` and `MOUTH_TOOLS`, plus full wiki peripheral vision.
2. `call_gemini_api` automatic failover to `gemini-3.8-flash`.
3. 0ms Linear plan cache & auto-binding of subtask index / omitted parent key.
4. Single-turn chainable `dispatch_task` -> verification probe -> briefing.
5. Mid-stream message routing during active execution invoking `cancel_active_task`.
6. Continuous iMessage threading (`thread_ts == chat_guid`) and 2,500-char context window.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import json
import os
import re
import sys
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.ingress import (
    CHAINABLE_TOOLS,
    IngressGateway,
    _THREAD_PLAN_CACHE,
    _THREAD_REF,
    execute_tool,
)
from golem.mouth import (
    BASE_CHIEF_OF_STAFF_INSTRUCTION,
    MOUTH_TOOLS,
    call_gemini_api,
    get_mouth_fallback_models,
    build_mouth_system_instruction,
)
from golem.router import SessionRouter
from golem.execution.runner import ExecutionHandle


class MockSlackClient:
    """Mock Slack client recording chat_postMessage, reactions, and replies."""

    def __init__(self) -> None:
        self.posted_messages: List[Dict[str, Any]] = []
        self.reactions_added: List[Dict[str, str]] = []
        self.reactions_removed: List[Dict[str, str]] = []
        self.thread_history: Dict[str, List[Dict[str, Any]]] = {}

    async def chat_postMessage(
        self, channel: str, text: str, thread_ts: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        msg = {
            "ok": True,
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "ts": f"msg_{len(self.posted_messages) + 1}.000",
            **kwargs,
        }
        self.posted_messages.append(msg)
        return msg

    async def reactions_add(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> Dict[str, Any]:
        rec = {"name": name, "channel": channel, "timestamp": timestamp}
        self.reactions_added.append(rec)
        return {"ok": True}

    async def reactions_remove(
        self, channel: str, timestamp: str, name: str, **kwargs: Any
    ) -> Dict[str, Any]:
        rec = {"name": name, "channel": channel, "timestamp": timestamp}
        self.reactions_removed.append(rec)
        return {"ok": True}

    def conversations_replies(
        self, channel: str, ts: str, limit: int = 20, **kwargs: Any
    ) -> Dict[str, Any]:
        return {"ok": True, "messages": self.thread_history.get(f"{channel}:{ts}", [])}


# ---------------------------------------------------------------------------
# Pillar 1: Zero CHO Strings & Wiki Peripheral Vision
# ---------------------------------------------------------------------------
class TestZeroChoAndPeripheralVision(unittest.TestCase):
    """Scenario 1: Zero CHO-\\d+ strings and full wiki peripheral vision."""

    def test_zero_cho_strings_in_base_instruction(self):
        """Verify BASE_CHIEF_OF_STAFF_INSTRUCTION has zero hardcoded CHO issue keys."""
        matches = re.findall(r"CHO-\d+", BASE_CHIEF_OF_STAFF_INSTRUCTION)
        self.assertEqual(matches, [], f"Found hardcoded CHO keys in BASE_CHIEF_OF_STAFF_INSTRUCTION: {matches}")

    def test_zero_cho_strings_in_mouth_tools(self):
        """Verify MOUTH_TOOLS schema declarations have zero hardcoded CHO issue keys."""
        tools_json = json.dumps(MOUTH_TOOLS)
        matches = re.findall(r"CHO-\d+", tools_json)
        self.assertEqual(matches, [], f"Found hardcoded CHO keys in MOUTH_TOOLS: {matches}")

    def test_zero_cho_strings_in_system_instruction_with_peripheral_vision(self):
        """Verify full system instruction contains zero CHO keys and includes peripheral vision."""
        full_instruction = build_mouth_system_instruction()
        matches = re.findall(r"CHO-\d+", full_instruction)
        self.assertEqual(matches, [], f"Found hardcoded CHO keys in build_mouth_system_instruction: {matches}")
        from golem.memory.peripheral import compile_peripheral_vision
        peripheral = compile_peripheral_vision()
        self.assertIn(BASE_CHIEF_OF_STAFF_INSTRUCTION, full_instruction)
        if peripheral:
            self.assertIn(peripheral, full_instruction)


# ---------------------------------------------------------------------------
# Pillar 2: Automatic Failover to gemini-3.8-flash
# ---------------------------------------------------------------------------
class TestCallGeminiApiFailover(unittest.IsolatedAsyncioTestCase):
    """Scenario 2: call_gemini_api automatic failover to gemini-3.8-flash."""

    async def test_automatic_failover_to_gemini_38_flash_on_quota_error(self):
        """Verify call_gemini_api catches 429 quota exhaustion and fails over to gemini-3.8-flash."""
        with patch.dict(os.environ, {"MOUTH_MODEL": "gemini-4-argon"}):
            fallbacks = get_mouth_fallback_models()
            self.assertIn("gemini-3.8-flash", fallbacks)

            calls: List[str] = []

            class MockResponse:
                def __init__(self, status: int, data: Dict[str, Any], text_content: str = ""):
                    self.status = status
                    self._data = data
                    self._text = text_content

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass

                async def json(self):
                    return self._data

                async def text(self):
                    return self._text

            def mock_post(url: str, **kwargs: Any):
                calls.append(url)
                if "gemini-3.8-flash" in url:
                    # Fallback succeeds
                    return MockResponse(
                        200,
                        {
                            "candidates": [
                                {
                                    "content": {
                                        "parts": [{"text": "Briefing synthesized via gemini-3.8-flash fallback."}]
                                    }
                                }
                            ],
                            "usageMetadata": {"promptTokenCount": 50, "candidatesTokenCount": 15},
                        },
                    )
                else:
                    # Primary model exhausted
                    return MockResponse(
                        429,
                        {"error": {"code": 429, "message": "RESOURCE_EXHAUSTED", "status": "RESOURCE_EXHAUSTED"}},
                        text_content="RESOURCE_EXHAUSTED: rate limit exceeded for primary model",
                    )

            with patch("aiohttp.ClientSession.post", side_effect=mock_post):
                resp = await call_gemini_api(
                    payload={"contents": [{"parts": [{"text": "Hello"}]}]},
                    api_key="test-api-key",
                )

                self.assertIsNotNone(resp)
                self.assertNotIn("_error", resp)
                candidates = resp.get("candidates", [])
                self.assertTrue(len(candidates) > 0)
                text = candidates[0]["content"]["parts"][0]["text"]
                self.assertIn("gemini-3.8-flash fallback", text)

                # Verify that at least 2 attempts were made and the fallback targeted gemini-3.8-flash
                self.assertGreaterEqual(len(calls), 2)
                self.assertIn("gemini-3.8-flash", calls[-1])


# ---------------------------------------------------------------------------
# Pillar 3: 0ms Linear Plan Cache & Auto-Binding
# ---------------------------------------------------------------------------
class TestZeroMsLinearPlanCache(unittest.IsolatedAsyncioTestCase):
    """Scenario 3: 0ms Linear plan cache & auto-binding of subtask index / omitted parent key."""

    async def test_linear_plan_cache_auto_binds_parent_and_subtask_index(self):
        """Verify 0ms plan cache auto-binds omitted parent_issue_key and resolves subtask_index."""
        router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        gateway = IngressGateway(router=router, bot_user_id="U_GOLEM")

        session, _ = await router.get_or_create("C_TEST", "100.1")
        cached_plan = {
            "parent": {
                "identifier": "ENG-500",
                "title": "Authentication Architecture Overhaul",
                "url": "https://linear.app/issue/ENG-500",
            },
            "subtasks": [
                {"identifier": "ENG-501", "title": "Implement JWT validation"},
                {"identifier": "ENG-502", "title": "Add OAuth2 refresh loop"},
                {"identifier": "ENG-503", "title": "Migrate session storage to Redis"},
            ],
            "comments": [],
        }
        session.linear_plan_cache = cached_plan

        executed_kwargs: Dict[str, Any] = {}

        async def fake_execute_backstage_task(**kwargs: Any):
            nonlocal executed_kwargs
            executed_kwargs = kwargs
            return {"status": "SUCCESS", "message": "All subtasks executed"}

        gateway._execute_backstage_task = fake_execute_backstage_task

        # Call dispatch_subtasks with omitted parent_issue_key and subtask_index: 2
        tool_args = {
            "execution_mode": "parallel",
            "subtasks": [
                {"subtask_index": 2, "task_description": "Implement OAuth2 refresh"},
                {"subtask_index": 3, "task_description": "Migrate session storage"},
            ],
        }

        res = await gateway._execute_tool_call(
            "dispatch_subtasks",
            tool_args,
            session=session,
            client=MagicMock(),
            channel="C_TEST",
            thread_ts="100.1",
        )

        self.assertEqual(res.get("status"), "SUCCESS")
        # Verify parent_issue_key was auto-bound from in-memory cache
        self.assertEqual(executed_kwargs.get("parent_issue_key"), "ENG-500")
        # Verify subtasks had their issue_key auto-bound from subtask_index
        subs = executed_kwargs.get("subtasks", [])
        self.assertEqual(len(subs), 2)
        self.assertEqual(subs[0]["issue_key"], "ENG-502")
        self.assertEqual(subs[1]["issue_key"], "ENG-503")


# ---------------------------------------------------------------------------
# Pillar 4: Single-Turn Chainable dispatch_task -> Verification Probe -> Briefing
# ---------------------------------------------------------------------------
class TestSingleTurnChainableDispatchTask(unittest.IsolatedAsyncioTestCase):
    """Scenario 4: Single-turn chainable dispatch_task -> verification probe -> briefing."""

    def test_dispatch_task_is_in_chainable_tools(self):
        """Verify dispatch_task is declared as chainable in CHAINABLE_TOOLS."""
        self.assertIn("dispatch_task", CHAINABLE_TOOLS)

    async def test_dispatch_task_chains_to_verification_probe_and_briefing(self):
        """Verify single-turn dispatch_task chains to a verification probe and then synthesizes executive briefing."""
        router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        gateway = IngressGateway(router=router, bot_user_id="U_GOLEM")
        client = MockSlackClient()
        session, _ = await router.get_or_create("C_CHAIN", "200.1")
        session.force_mouth = True

        executed_tools: List[tuple] = []

        async def fake_execute_backstage_task(*args: Any, **kwargs: Any):
            task_desc = args[0] if args else kwargs.get("task_desc") or kwargs.get("task_description") or ""
            executed_tools.append(("dispatch_task", {"task_description": task_desc}))
            if len(executed_tools) == 1:
                return {"status": "SUCCESS", "exit_code": 0, "output": "Tests passed: 15/15"}
            else:
                return {"status": "SUCCESS", "exit_code": 0, "output": "Working tree clean, no untracked files"}

        gateway._execute_backstage_task = fake_execute_backstage_task

        gemini_calls = 0

        async def fake_call_gemini_api(payload: Dict[str, Any], **kwargs: Any):
            nonlocal gemini_calls
            gemini_calls += 1
            if gemini_calls == 1:
                # Turn 1: Dispatch test suite
                return {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "functionCall": {
                                            "name": "dispatch_task",
                                            "args": {"task_description": "pytest tests/unit/"},
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            elif gemini_calls == 2:
                # Hop 2: Dispatch verification probe (git status)
                return {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "functionCall": {
                                            "name": "dispatch_task",
                                            "args": {"task_description": "git status --porcelain"},
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            else:
                # Hop 3: Synthesize Executive Briefing
                return {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "text": "*Verification Complete:* All 15 tests passed and git tree is pristine."
                                    }
                                ]
                            }
                        }
                    ]
                }

        gateway._call_gemini_api = fake_call_gemini_api

        await gateway._run_mouth_turn(
            session=session,
            prompt="Run test suite and verify clean git status",
            client=client,
            channel="C_CHAIN",
            thread_ts="200.1",
            ts="200.1",
            is_imessage=False,
        )

        # Verify tool chaining occurred across 2 distinct dispatch_task calls
        self.assertEqual(len(executed_tools), 2)
        self.assertEqual(executed_tools[0][0], "dispatch_task")
        self.assertEqual(executed_tools[0][1].get("task_description"), "pytest tests/unit/")
        self.assertEqual(executed_tools[1][0], "dispatch_task")
        self.assertEqual(executed_tools[1][1].get("task_description"), "git status --porcelain")

        # Verify executive briefing posted to client
        self.assertTrue(len(client.posted_messages) >= 1)
        briefing = client.posted_messages[-1]["text"]
        self.assertIn("Verification Complete", briefing)
        self.assertIn("All 15 tests passed", briefing)


# ---------------------------------------------------------------------------
# Pillar 5: Mid-Stream Message Routing and Active Cancellation
# ---------------------------------------------------------------------------
class TestMidStreamMessageRoutingAndCancellation(unittest.IsolatedAsyncioTestCase):
    """Scenario 5: Mid-stream message routing during active execution invoking cancel_active_task."""

    async def test_mid_stream_natural_language_invokes_cancel_active_task(self):
        """Verify mid-stream natural language message routes through Mouth and invokes cancel_active_task."""
        router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        gateway = IngressGateway(router=router, bot_user_id="U_GOLEM")
        client = MockSlackClient()

        session, _ = await router.get_or_create("C_MID", "300.1")
        session.active_execution = ExecutionHandle(
            status="running", pane="runner", pid=9999, command="npm run build"
        )
        session.cancel_execution = AsyncMock()
        session.get_runner_status = AsyncMock(
            return_value={"status": "running", "command": "npm run build", "pid": 9999, "buffer_tail": "Compiling..."}
        )

        gemini_calls = 0

        async def fake_call_gemini_api(payload: Dict[str, Any], **kwargs: Any):
            nonlocal gemini_calls
            gemini_calls += 1
            if gemini_calls == 1:
                # Mouth decides to invoke cancel_active_task tool
                return {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "functionCall": {
                                            "name": "cancel_active_task",
                                            "args": {},
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            else:
                # Step 2 synthesis
                return {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {"text": "🛑 *Execution Cancelled:* Stopped npm build in runner pane."}
                                ]
                            }
                        }
                    ]
                }

        gateway._call_gemini_api = fake_call_gemini_api

        event = {
            "channel": "C_MID",
            "ts": "300.2",
            "thread_ts": "300.1",
            "text": "Could you please abort the running build right now?",
        }

        session.force_mouth = True
        await gateway.handle_message(event, client)

        session.cancel_execution.assert_awaited_once()
        self.assertTrue(len(client.posted_messages) >= 1)
        briefing = client.posted_messages[-1]["text"]
        self.assertIn("Execution Cancelled", briefing)

    async def test_mid_stream_slash_command_cancels_immediately(self):
        """Verify /cancel command immediately executes out-of-band cancellation without LLM turn."""
        router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        gateway = IngressGateway(router=router, bot_user_id="U_GOLEM")
        client = MockSlackClient()

        session, _ = await router.get_or_create("C_MID2", "300.3")
        session.active_execution = ExecutionHandle(
            status="running", pane="runner", pid=1111, command="cargo test"
        )
        session.cancel_execution = AsyncMock()

        event = {
            "channel": "C_MID2",
            "ts": "300.4",
            "thread_ts": "300.3",
            "text": "/cancel",
        }

        await gateway.handle_message(event, client)

        session.cancel_execution.assert_awaited_once()
        self.assertTrue(len(client.posted_messages) >= 1)
        self.assertIn("Execution Cancelled", client.posted_messages[-1]["text"])


# ---------------------------------------------------------------------------
# Pillar 6: Continuous iMessage Threading & 2,500-Char Context Window
# ---------------------------------------------------------------------------
class TestContinuousIMessageThreadingAndContextWindow(unittest.IsolatedAsyncioTestCase):
    """Scenario 6: Continuous iMessage threading (thread_ts == chat_guid) and 2,500-char context window."""

    async def test_continuous_imessage_threading_uses_chat_guid(self):
        """Verify consecutive iMessage messages share thread_ts == chat_guid and the same session."""
        router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        gateway = IngressGateway(router=router, bot_user_id="U_GOLEM")
        client = MockSlackClient()

        chat_guid = "iMessage;-;carl@example.com"
        handled_sessions: List[tuple] = []

        async def fake_handle_mouth_turn(session, prompt, client, channel, thread_ts, ts, is_imessage):
            handled_sessions.append((session, thread_ts, is_imessage))

        gateway._handle_mouth_turn = fake_handle_mouth_turn

        with patch.dict(os.environ, {"GOLEM_FORCE_MOUTH": "1"}):
            # Message 1
            event1 = {
                "channel": chat_guid,
                "ts": "1720000001.0001",
                "text": "What is the fleet status?",
                "is_imessage": True,
            }
            await gateway.handle_message(event1, client)

            # Message 2
            event2 = {
                "channel": chat_guid,
                "ts": "1720000002.0002",
                "text": "Can you check Docker containers?",
                "is_imessage": True,
            }
            await gateway.handle_message(event2, client)

        self.assertEqual(len(handled_sessions), 2)
        sess1, th1, im1 = handled_sessions[0]
        sess2, th2, im2 = handled_sessions[1]

        # Both messages share the identical session object and thread_ts == chat_guid
        self.assertIs(sess1, sess2)
        self.assertEqual(th1, chat_guid)
        self.assertEqual(th2, chat_guid)
        self.assertTrue(im1)
        self.assertTrue(im2)

    async def test_imessage_fetch_thread_context_with_2500_char_capping(self):
        """Verify _fetch_thread_context uses Hermes sender for bot on iMessage and caps at 2500 chars."""
        router = SessionRouter(driver_factory=lambda cwd: AsyncMock())
        gateway = IngressGateway(router=router, bot_user_id="B_BOT")

        chat_guid = "iMessage;-;+15551234567"
        long_body = "X" * 3200

        fake_history = {
            "messages": [
                {"ts": "1.0", "user": "U_CARL", "text": "Deploy the new API"},
                {"ts": "2.0", "bot_id": "B_BOT", "text": long_body},
                {"ts": "3.0", "user": "U_CARL", "text": "Thanks!"},
            ]
        }

        mock_client = MagicMock()
        mock_client.conversations_replies = MagicMock(return_value=fake_history)

        context = await gateway._fetch_thread_context(
            mock_client,
            channel=chat_guid,
            thread_ts=chat_guid,
            exclude_ts="3.0",
            is_imessage=True,
        )

        lines = [l for l in context.strip().split("\n") if l.strip()]
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], "[Previous iMessage Conversation Context:")
        self.assertEqual(lines[1], "User: Deploy the new API")

        # Bot line should use "Hermes" sender for iMessage
        self.assertTrue(lines[2].startswith("Hermes: "))
        self.assertEqual(lines[3], "]")
        bot_content = lines[2][len("Hermes: "):]

        # Should be truncated to 2500 chars + "..." (2503 total)
        self.assertEqual(len(bot_content), 2503)
        self.assertTrue(bot_content.endswith("..."))
        self.assertEqual(bot_content[:2500], "X" * 2500)


if __name__ == "__main__":
    unittest.main()
