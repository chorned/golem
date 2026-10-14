# tests/test_react_loop.py
import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from golem.ingress import (
    run_conversational_turn,
    execute_tool,
    MAX_HOPS,
    sanitize_model_turn_for_history,
)


class TestReactLoop(unittest.IsolatedAsyncioTestCase):

    async def test_react_loop_chains_read_and_dispatch(self):
        # Turn 1: Model calls read_project_file
        resp_hop_1 = {
            "candidates": [{
                "content": {
                    "parts": [{"functionCall": {"name": "read_project_file", "args": {"path": "test.json"}}}]
                }
            }]
        }
        # Turn 2: Model sees file and calls dispatch_task
        resp_hop_2 = {
            "candidates": [{
                "content": {
                    "parts": [{"functionCall": {"name": "dispatch_task", "args": {"command": "python3 sync.py"}}}]
                }
            }]
        }
        # Turn 3: Model summarizes outcome in text
        resp_hop_3 = {
            "candidates": [{
                "content": {
                    "parts": [{"text": "Sync process dispatched successfully in Pane 2."}]
                }
            }]
        }

        with patch("golem.ingress.call_gemini_api", AsyncMock(side_effect=[resp_hop_1, resp_hop_2, resp_hop_3])), \
             patch("golem.ingress.execute_tool", AsyncMock(side_effect=[{"status": "SUCCESS", "content": "{}"}, {"status": "RUNNING", "task_id": "123"}])):

            reply = await run_conversational_turn(
                session_id="test",
                user_prompt="check test.json and sync",
                conversation_history=[],
                tools=[],
                system_instruction="You are Golem."
            )

            self.assertIn("Sync process dispatched successfully", reply)

    async def test_react_loop_delivers_direct_text_when_no_tools_called(self):
        resp = {
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello Carl, all systems are operational."}]
                }
            }]
        }
        with patch("golem.ingress.call_gemini_api", AsyncMock(return_value=resp)):
            reply = await run_conversational_turn(
                session_id="test",
                user_prompt="status check",
                conversation_history=[],
                tools=[],
                system_instruction="You are Golem."
            )
            self.assertEqual(reply, "Hello Carl, all systems are operational.")

    async def test_react_loop_max_hops_exhaustion(self):
        # Model endlessly calls read_project_file
        hop_resp = {
            "candidates": [{
                "content": {
                    "parts": [{"functionCall": {"name": "read_project_file", "args": {"path": "loop.txt"}}}]
                }
            }]
        }
        # Final pass forced mode="NONE"
        final_resp = {
            "candidates": [{
                "content": {
                    "parts": [{"text": "Terminating after max hops."}]
                }
            }]
        }

        with patch("golem.ingress.call_gemini_api", AsyncMock(side_effect=[hop_resp, hop_resp, final_resp])), \
             patch("golem.ingress.execute_tool", AsyncMock(return_value={"status": "SUCCESS", "content": "..."})):

            reply = await run_conversational_turn(
                session_id="test",
                user_prompt="infinite loop test",
                conversation_history=[],
                tools=[],
                system_instruction="You are Golem.",
                max_hops=2
            )
            self.assertEqual(reply, "Terminating after max hops.")

    async def test_react_loop_normalizes_command_for_dispatch_task(self):
        resp_hop_1 = {
            "candidates": [{
                "content": {
                    "parts": [{"functionCall": {"name": "dispatch_task", "args": {"command": "ls -la"}}}]
                }
            }]
        }
        resp_hop_2 = {
            "candidates": [{
                "content": {
                    "parts": [{"text": "Listed directory contents."}]
                }
            }]
        }

        mock_execute = AsyncMock(return_value={"status": "COMPLETED", "output_tail": "total 0"})

        with patch("golem.ingress.call_gemini_api", AsyncMock(side_effect=[resp_hop_1, resp_hop_2])), \
             patch("golem.ingress.execute_tool", mock_execute):

            reply = await run_conversational_turn(
                session_id="test",
                user_prompt="run ls",
                conversation_history=[],
                tools=[],
                system_instruction="You are Golem."
            )

            self.assertIn("Listed directory contents.", reply)
            mock_execute.assert_called_once()
            call_args = mock_execute.call_args[0]
            self.assertEqual(call_args[0], "dispatch_task")
            self.assertEqual(call_args[1]["command"], "ls -la")
            self.assertEqual(call_args[1]["task_description"], "ls -la")

    async def test_execute_tool_dispatch_task_invokes_backstage(self):
        with patch("golem.ingress._execute_backstage_task", AsyncMock(return_value={"status": "RUNNING", "task_id": "t-1"})) as mock_backstage:
            res = await execute_tool("dispatch_task", {"command": "python3 run.py", "target_node": "hermes"})
            self.assertEqual(res["status"], "RUNNING")
            mock_backstage.assert_called_once()
            kwargs = mock_backstage.call_args[1]
            self.assertEqual(kwargs["command"], "python3 run.py")
            self.assertEqual(kwargs["target_node"], "hermes")

    def test_sanitize_model_turn_preserves_thought_signature(self):
        raw_content = {
            "role": "model",
            "parts": [{
                "functionCall": {"name": "read_project_file", "args": {"path": "test.json"}},
                "thought_signature": "valid_token_xyz"
            }]
        }
        sanitized = sanitize_model_turn_for_history(raw_content)
        part = sanitized["parts"][0]
        self.assertEqual(part["thought_signature"], "valid_token_xyz")
        self.assertEqual(part["thoughtSignature"], "valid_token_xyz")
        self.assertNotIn("thought_signature", part["functionCall"])
        self.assertNotIn("thoughtSignature", part["functionCall"])

    def test_sanitize_model_turn_injects_fallback_if_signature_missing(self):
        raw_content = {
            "role": "model",
            "parts": [{
                "functionCall": {"name": "read_project_file", "args": {"path": "test.json"}}
            }]
        }
        sanitized = sanitize_model_turn_for_history(raw_content)
        part = sanitized["parts"][0]
        self.assertEqual(part["thought_signature"], "skip_thought_signature_validator")
        self.assertEqual(part["thoughtSignature"], "skip_thought_signature_validator")
        self.assertNotIn("thought_signature", part["functionCall"])
        self.assertNotIn("thoughtSignature", part["functionCall"])

    def test_sanitize_model_turn_strips_misplaced_signatures_from_function_call(self):
        raw_content = {
            "role": "model",
            "parts": [{
                "functionCall": {
                    "name": "plan_task_with_subtasks",
                    "args": {"title": "Test"},
                    "thought_signature": "misplaced_sig_123",
                    "thoughtSignature": "misplaced_sig_123",
                }
            }]
        }
        sanitized = sanitize_model_turn_for_history(raw_content)
        part = sanitized["parts"][0]
        self.assertEqual(part["thought_signature"], "misplaced_sig_123")
        self.assertEqual(part["thoughtSignature"], "misplaced_sig_123")
        self.assertNotIn("thought_signature", part["functionCall"])
        self.assertNotIn("thoughtSignature", part["functionCall"])

    async def test_react_loop_preserves_thought_signature_in_multi_hop_history(self):
        # Turn 1: Model emits functionCall with thought_signature
        resp_hop_1 = {
            "candidates": [{
                "content": {
                    "parts": [{
                        "functionCall": {"name": "read_project_file", "args": {"path": "test.json"}},
                        "thought_signature": "token_abc_123"
                    }]
                }
            }]
        }
        # Turn 2: Model finishes with text
        resp_hop_2 = {
            "candidates": [{
                "content": {
                    "parts": [{"text": "File read complete."}]
                }
            }]
        }

        captured_contents = []

        async def mock_call_api(*args, **kwargs):
            captured_contents.append(list(kwargs.get("contents", [])))
            if len(captured_contents) == 1:
                return resp_hop_1
            return resp_hop_2

        with patch("golem.ingress.call_gemini_api", side_effect=mock_call_api), \
             patch("golem.ingress.execute_tool", AsyncMock(return_value={"status": "SUCCESS", "content": "{}"})):

            reply = await run_conversational_turn(
                session_id="test",
                user_prompt="read test.json",
                conversation_history=[],
                tools=[],
                system_instruction="You are Golem."
            )

            self.assertEqual(reply, "File read complete.")
            self.assertEqual(len(captured_contents), 2)
            hop2_history = captured_contents[1]
            model_turn = hop2_history[1]
            self.assertEqual(model_turn["role"], "model")
            self.assertEqual(model_turn["parts"][0]["thought_signature"], "token_abc_123")

    async def test_step2_synthesis_guarantees_thought_signature_in_briefing_contents(self):
        """
        Verify that _execute_step2_synthesis guarantees thought_signature and thoughtSignature
        are present on the functionCall part even when the model candidate lacked signatures,
        preventing HTTP 400 INVALID_ARGUMENT errors.
        """
        from golem.ingress import IngressGateway

        gateway = IngressGateway()
        captured_payloads = []

        async def mock_call_api(**kwargs):
            captured_payloads.append(kwargs.get("payload", {}))
            return {
                "candidates": [{
                    "content": {
                        "parts": [{"text": "*Plan Created:* Created parent issue CHO-151 with 2 subtasks."}]
                    }
                }]
            }

        unsigned_candidate = {
            "content": {
                "parts": [
                    {
                        "functionCall": {
                            "name": "plan_task_with_subtasks",
                            "args": {
                                "parent_title": "Audit Audiobookshelf podcasts",
                                "subtasks": ["List podcasts", "Calculate durations"]
                            }
                        }
                    }
                ]
            }
        }

        with patch.object(gateway, "_call_gemini_api", side_effect=mock_call_api):
            briefing_text, error_info = await gateway._execute_step2_synthesis(
                sys_prompt="You are Golem.",
                prompt="Audit podcasts on Audiobookshelf",
                candidate=unsigned_candidate,
                fn_call={"name": "plan_task_with_subtasks", "args": {}},
                fn_name="plan_task_with_subtasks",
                tool_result={"status": "PLAN_CREATED", "parent_key": "CHO-151", "subtasks": []},
                model="gemini-3.8-flash",
                api_key="mock-key",
            )

            self.assertTrue(bool(briefing_text))
            self.assertIsNone(error_info)
            self.assertEqual(len(captured_payloads), 1)

            contents = captured_payloads[0].get("contents", [])
            self.assertEqual(len(contents), 3)

            # Turn 2 (position 2 in 1-based indexing) is the model turn
            model_turn = contents[1]
            self.assertEqual(model_turn["role"], "model")
            fc_part = model_turn["parts"][0]
            self.assertIn("functionCall", fc_part)
            self.assertEqual(fc_part["functionCall"]["name"], "plan_task_with_subtasks")

            # Must contain both snake_case and camelCase thought signature tokens on Part, and NOT inside functionCall
            self.assertEqual(fc_part["thought_signature"], "skip_thought_signature_validator")
            self.assertEqual(fc_part["thoughtSignature"], "skip_thought_signature_validator")
            self.assertNotIn("thought_signature", fc_part["functionCall"])
            self.assertNotIn("thoughtSignature", fc_part["functionCall"])

    async def test_ingress_plan_task_with_subtasks_roundtrip_step2_thought_signature(self):
        """
        Verify end-to-end handle_message preserves thought signatures on plan_task_with_subtasks,
        preventing the 'Function call is missing a thought_signature' 400 failure.
        """
        from golem.ingress import IngressGateway

        gateway = IngressGateway()
        client = MagicMock()
        client.post_chat_message = AsyncMock()
        client.add_reaction = AsyncMock()

        mock_plan = {
            "parent": {"id": "p-151", "identifier": "CHO-151", "url": "https://linear.app/issue/CHO-151", "title": "Audiobookshelf"},
            "subtasks": [
                {"id": "c-152", "identifier": "CHO-152", "title": "[1/2] List podcasts", "url": "https://linear.app/issue/CHO-152"},
                {"id": "c-153", "identifier": "CHO-153", "title": "[2/2] Calculate episode lengths", "url": "https://linear.app/issue/CHO-153"},
            ]
        }

        turn1_plan = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "plan_task_with_subtasks",
                                    "args": {
                                        "parent_title": "Audit podcasts",
                                        "subtasks": ["List podcasts", "Calculate durations"]
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }

        turn2_briefing = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "✅ *Plan Created in Linear:* Created `CHO-151` with 2 subtasks."}
                        ]
                    }
                }
            ]
        }

        with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini, \
             patch("golem.linear_bridge.LinearBridge.create_plan_with_subtasks", new_callable=AsyncMock) as mock_create, \
             patch("golem.ingress.set_assistant_status", new_callable=AsyncMock):

            mock_gemini.side_effect = [turn1_plan, turn2_briefing]
            mock_create.return_value = mock_plan

            event = {
                "channel": "C_SLACK",
                "ts": "100.999",
                "thread_ts": "100.999",
                "text": "Can you validate which podcasts are on our audiobookshelf?",
            }
            await gateway.handle_message(event, client)

            self.assertEqual(mock_gemini.call_count, 2)
            step2_payload = mock_gemini.call_args_list[1][1]["payload"]
            step2_contents = step2_payload["contents"]

            model_turn = step2_contents[1]
            self.assertEqual(model_turn["role"], "model")
            fc_part = model_turn["parts"][0]
            self.assertEqual(fc_part["thought_signature"], "skip_thought_signature_validator")
            self.assertEqual(fc_part["thoughtSignature"], "skip_thought_signature_validator")
            self.assertNotIn("thought_signature", fc_part["functionCall"])
            self.assertNotIn("thoughtSignature", fc_part["functionCall"])


if __name__ == "__main__":
    unittest.main()

