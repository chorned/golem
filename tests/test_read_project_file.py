# tests/test_read_project_file.py
import unittest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

from golem.ingress import _handle_read_project_file, _execute_tool_call, IngressGateway
from golem.mouth import READ_PROJECT_FILE_DECLARATION, MOUTH_TOOLS


class TestReadProjectFile(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._temp_dir.name)

    async def asyncTearDown(self):
        self._temp_dir.cleanup()

    def test_read_project_file_registered_in_mouth_tools(self):
        tool_names = [t["name"] for t in MOUTH_TOOLS]
        self.assertIn("read_project_file", tool_names)
        self.assertIn("path", READ_PROJECT_FILE_DECLARATION["parameters"]["required"])

    async def test_read_project_file_reads_content(self):
        test_file = self.tmp_path / "test_episodes.json"
        test_file.write_text('{"episodes": [{"id": 1, "title": "The Storm"}]}', encoding="utf-8")

        res = await _handle_read_project_file({"path": str(test_file)})
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["type"], "file")
        self.assertIn("The Storm", res["content"])
        self.assertEqual(res["total_lines"], 1)

    async def test_read_project_file_lists_directory_with_pattern(self):
        (self.tmp_path / "episodes_01.json").write_text("{}", encoding="utf-8")
        (self.tmp_path / "episodes_02.json").write_text("{}", encoding="utf-8")
        (self.tmp_path / "notes.txt").write_text("notes", encoding="utf-8")

        res = await _handle_read_project_file({"path": str(self.tmp_path), "pattern": "*episodes*.json"})
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["type"], "directory")
        self.assertIn("episodes_01.json", res["entries"])
        self.assertIn("episodes_02.json", res["entries"])
        self.assertNotIn("notes.txt", res["entries"])

    async def test_read_project_file_handles_missing_file(self):
        missing_file = self.tmp_path / "nonexistent.json"
        res = await _handle_read_project_file({"path": str(missing_file)})
        self.assertEqual(res["status"], "NOT_FOUND")

    async def test_read_project_file_missing_with_suggestions(self):
        (self.tmp_path / "the_unmade_god_episodes.json").write_text("{}", encoding="utf-8")
        missing = self.tmp_path / "episodes.json"
        res = await _handle_read_project_file({"path": str(missing)})
        self.assertEqual(res["status"], "NOT_FOUND")
        self.assertIn("suggested_matches", res)
        self.assertIn("the_unmade_god_episodes.json", res["suggested_matches"])

    async def test_read_project_file_empty_path(self):
        res = await _handle_read_project_file({"path": ""})
        self.assertEqual(res["status"], "ERROR")
        self.assertIn("Path argument is required", res["error"])

    async def test_read_project_file_truncation(self):
        big_file = self.tmp_path / "large.txt"
        lines = [f"line {i}" for i in range(50)]
        big_file.write_text("\n".join(lines), encoding="utf-8")

        res = await _handle_read_project_file({"path": str(big_file), "max_lines": 10})
        self.assertEqual(res["status"], "SUCCESS")
        self.assertTrue(res["truncated"])
        self.assertEqual(res["total_lines"], 50)
        self.assertEqual(len(res["content"].splitlines()), 10)

    async def test_execute_tool_call_routing(self):
        f = self.tmp_path / "test.txt"
        f.write_text("hello world", encoding="utf-8")

        res = await _execute_tool_call("read_project_file", {"path": str(f)})
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["content"], "hello world")

    async def test_ingress_gateway_handles_read_project_file_turn(self):
        doc = self.tmp_path / "registry.json"
        doc.write_text('{"projects": ["podcast-bot"]}', encoding="utf-8")

        gateway = IngressGateway()
        client = AsyncMock()
        client.reactions_added = []
        client.posted_messages = []

        async def mock_post(channel, text=None, thread_ts=None, **kwargs):
            msg = {"channel": channel, "text": text, "thread_ts": thread_ts}
            client.posted_messages.append(msg)
            return {"ok": True, "ts": "100.999"}

        client.chat_postMessage = AsyncMock(side_effect=mock_post)
        client.reactions_add = AsyncMock(side_effect=lambda **kw: client.reactions_added.append(kw))

        turn1_call = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "read_project_file",
                                    "args": {"path": str(doc)},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        turn2_synthesis = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": "Found project `podcast-bot` in registry."
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("golem.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [turn1_call, turn2_synthesis]
            event = {
                "channel": "C123",
                "ts": "100.1",
                "thread_ts": "100.1",
                "text": "can you check the registry file?",
            }
            await gateway.handle_message(event, client)

            self.assertGreaterEqual(len(client.posted_messages), 1)
            last_msg = client.posted_messages[-1]["text"]
            self.assertIn("podcast-bot", last_msg)

    async def test_ingress_gateway_chains_read_tool_into_dispatch_task(self):
        doc = self.tmp_path / "unmade_god_episodes.json"
        doc.write_text('{"episodes": [{"id": 1, "title": "The Awakening"}]}', encoding="utf-8")

        gateway = IngressGateway()
        client = AsyncMock()
        client.reactions_added = []
        client.posted_messages = []

        async def mock_post(channel, text=None, thread_ts=None, **kwargs):
            msg = {"channel": channel, "text": text, "thread_ts": thread_ts}
            client.posted_messages.append(msg)
            return {"ok": True, "ts": "100.999"}

        client.chat_postMessage = AsyncMock(side_effect=mock_post)
        client.reactions_add = AsyncMock(side_effect=lambda **kw: client.reactions_added.append(kw))

        gateway._execute_backstage_task = AsyncMock(
            return_value={
                "status": "SUCCESS",
                "output": "Transfer to oci-machine complete: 1 episode synced.",
                "duration_seconds": 1.2,
                "tools_executed_count": 1,
            }
        )

        hop1_read_call = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "read_project_file",
                                    "args": {"path": str(doc)},
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        hop2_dispatch_call = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "dispatch_task",
                                    "args": {
                                        "task_description": "Transfer unmade god episodes to OCI",
                                        "target_node": "oci-machine",
                                        "precedent_status": "NOVEL_EXPLORATION",
                                        "precedent_source": "Syncing newly produced episodes to cloud storage",
                                    },
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        step2_synthesis = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": "*Episodes verified:* Found 1 episode (`The Awakening`). *Dispatched OCI sync job:* Background runner completed transfer."
                            }
                        ]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("golem.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini:
            mock_gemini.side_effect = [hop1_read_call, hop2_dispatch_call, step2_synthesis]
            event = {
                "channel": "C123",
                "ts": "100.1",
                "thread_ts": "100.1",
                "text": "check that episodes are ready, and then get them over to OCI",
            }
            await gateway.handle_message(event, client)

            self.assertEqual(mock_gemini.call_count, 3)

            hop2_call_args = mock_gemini.call_args_list[1][1]
            hop2_payload = hop2_call_args.get("payload", {})
            self.assertEqual(hop2_payload["toolConfig"]["functionCallingConfig"]["mode"], "AUTO")
            hop2_contents = hop2_payload.get("contents", [])
            self.assertGreaterEqual(len(hop2_contents), 3)
            self.assertIn("The Awakening", str(hop2_contents))

            step2_call_args = mock_gemini.call_args_list[2][1]
            step2_payload = step2_call_args.get("payload", {})
            self.assertEqual(step2_payload["toolConfig"]["functionCallingConfig"]["mode"], "NONE")
            step2_contents = step2_payload.get("contents", [])
            self.assertGreaterEqual(len(step2_contents), 5)

            called_args = gateway._execute_backstage_task.call_args[0]
            called_kwargs = gateway._execute_backstage_task.call_args[1]
            task_desc_val = called_kwargs.get("task_desc") or (called_args[0] if called_args else None)
            target_node_val = called_kwargs.get("target_node") or (called_args[1] if len(called_args) > 1 else None)
            self.assertEqual(task_desc_val, "Transfer unmade god episodes to OCI")
            self.assertEqual(target_node_val, "oci-machine")

            self.assertGreaterEqual(len(client.posted_messages), 1)
            final_text = client.posted_messages[-1]["text"]
            self.assertIn("Episodes verified", final_text)
            self.assertIn("The Awakening", final_text)

    async def test_ingress_gateway_chains_multiple_read_tools_before_dispatch(self):
        f1 = self.tmp_path / "episodes.json"
        f1.write_text('{"count": 2}', encoding="utf-8")

        gateway = IngressGateway()
        client = AsyncMock()
        client.reactions_added = []
        client.posted_messages = []

        async def mock_post(channel, text=None, thread_ts=None, **kwargs):
            msg = {"channel": channel, "text": text, "thread_ts": thread_ts}
            client.posted_messages.append(msg)
            return {"ok": True, "ts": "100.999"}

        client.chat_postMessage = AsyncMock(side_effect=mock_post)
        client.reactions_add = AsyncMock(side_effect=lambda **kw: client.reactions_added.append(kw))

        gateway._execute_backstage_task = AsyncMock(
            return_value={"status": "SUCCESS", "output": "Synced successfully."}
        )

        hop1_read = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "read_project_file", "args": {"path": str(f1)}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        hop2_consult = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "consult_memory", "args": {"path": "runbook.md"}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        hop3_dispatch = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "dispatch_task", "args": {"task_description": "Run sync"}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        step2_synth = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": "*Done:* Read episodes, checked runbook, dispatched sync."}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("golem.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini, \
             patch.object(gateway, "_execute_consult_memory", new_callable=AsyncMock) as mock_mem:
            mock_gemini.side_effect = [hop1_read, hop2_consult, hop3_dispatch, step2_synth]
            mock_mem.return_value = {"status": "SUCCESS", "content": "# Runbook"}

            event = {"channel": "C123", "ts": "100.1", "thread_ts": "100.1", "text": "verify and run"}
            await gateway.handle_message(event, client)

            self.assertEqual(mock_gemini.call_count, 4)
            self.assertEqual(gateway._execute_backstage_task.call_count, 1)
            self.assertIn("Read episodes, checked runbook, dispatched sync", client.posted_messages[-1]["text"])

    async def test_ingress_gateway_chain_terminates_at_max_hops(self):
        f = self.tmp_path / "data.json"
        f.write_text("{}", encoding="utf-8")

        gateway = IngressGateway()
        client = AsyncMock()
        client.reactions_added = []
        client.posted_messages = []

        async def mock_post(channel, text=None, thread_ts=None, **kwargs):
            msg = {"channel": channel, "text": text, "thread_ts": thread_ts}
            client.posted_messages.append(msg)
            return {"ok": True, "ts": "100.999"}

        client.chat_postMessage = AsyncMock(side_effect=mock_post)
        client.reactions_add = AsyncMock(side_effect=lambda **kw: client.reactions_added.append(kw))

        hop1 = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "read_project_file", "args": {"path": str(f)}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        hop2 = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "consult_memory", "args": {"path": "a.md"}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        hop3 = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"functionCall": {"name": "consult_memory", "args": {"path": "b.md"}}}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        synth = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": "*Briefing:* Completed 3 inspection hops."}]
                    },
                    "finishReason": "STOP",
                }
            ]
        }

        with patch("golem.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini, \
             patch.object(gateway, "_execute_consult_memory", new_callable=AsyncMock) as mock_mem:
            mock_gemini.side_effect = [hop1, hop2, hop3, synth]
            mock_mem.return_value = {"status": "SUCCESS", "content": "Memory content"}

            event = {"channel": "C123", "ts": "100.1", "thread_ts": "100.1", "text": "inspect thoroughly"}
            await gateway.handle_message(event, client)

            self.assertEqual(mock_gemini.call_count, 4)
            self.assertIn("Completed 3 inspection hops", client.posted_messages[-1]["text"])


if __name__ == "__main__":
    unittest.main()
