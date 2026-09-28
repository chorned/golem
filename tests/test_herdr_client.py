#!/usr/bin/env python3
"""
tests/test_herdr_client.py
Comprehensive unit test suite for HerdrClient:
- Parsing `herdr api snapshot` JSON and pane allocation.
- Worker catalog loading and environment injection from .agents/workers.yaml.
- FrugaLLM proxy routing (OPENAI_BASE_URL set to local proxy http://127.0.0.1:4000/v1).
- Pane split allocation logic with --no-focus and --direction.
- dispatch_worker flow: branch checkout, env injection, agent start, agent prompt --wait, agent read.
"""

import os
import sys
import json
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from herdr_client import HerdrClient, InfrastructureError


class TestHerdrClient(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.client = HerdrClient()

    def test_workers_catalog_loading_and_frugallm_config(self):
        """Verify .agents/workers.yaml loads properly and contains FrugaLLM config."""
        cfg = self.client.load_workers_config()
        self.assertIn("frugallm", cfg)
        self.assertIn("workers", cfg)

        frugallm = self.client.get_frugallm_config()
        self.assertEqual(frugallm.get("endpoint"), "http://127.0.0.1:61721/v1")
        self.assertEqual(frugallm.get("base_url"), "http://127.0.0.1:61721/v1")
        self.assertEqual(frugallm.get("host"), "0.0.0.0:61721")
        self.assertEqual(frugallm.get("password"), "horned66")

        # Test defined workers
        workers = cfg["workers"]
        self.assertIn("opencode-dev", workers)
        self.assertIn("hermes-homelab", workers)
        self.assertIn("cuda-worker", workers)

        opencode = self.client.get_worker_config("opencode-dev")
        self.assertEqual(opencode["harness"], "opencode")
        self.assertEqual(opencode["target_node"], "hermes")
        self.assertEqual(opencode["workspace"], "dev-grid")
        # Natively routes through FrugaLLM without artificial proxies
        self.assertEqual(opencode.get("env"), {})

        cuda_worker = self.client.get_worker_config("cuda-worker")
        self.assertEqual(cuda_worker["env"]["CUDA_VISIBLE_DEVICES"], "0")

    def test_worker_not_found_raises_value_error(self):
        """Verify requesting undefined worker raises ValueError."""
        with self.assertRaises(ValueError):
            self.client.get_worker_config("non-existent-worker")

    async def test_get_snapshot_json_parsing(self):
        """Verify HerdrClient.get_snapshot executes `herdr api snapshot` and parses JSON."""
        mock_snapshot = {
            "id": "cli:api:snapshot",
            "result": {
                "workspaces": [
                    {
                        "workspace_id": "w1",
                        "label": "fleet-control",
                        "panes": [{"pane_id": "w1:p1", "label": "chief-of-staff"}]
                    },
                    {
                        "workspace_id": "w4",
                        "label": "dev-grid",
                        "panes": [{"pane_id": "w4:p1", "label": "laptop-worker"}]
                    }
                ]
            }
        }

        with patch.object(self.client, "run_cli", new_callable=AsyncMock, return_value=(0, json.dumps(mock_snapshot), "")) as mock_run:
            snapshot = await self.client.get_snapshot()
            mock_run.assert_called_once_with(["api", "snapshot"], timeout=10.0)
            self.assertIn("result", snapshot)
            self.assertEqual(len(snapshot["result"]["workspaces"]), 2)

    async def test_allocate_worker_pane_from_snapshot(self):
        """Verify allocate_worker_pane locates target workspace and splits sibling pane."""
        mock_snapshot = {
            "result": {
                "workspaces": [
                    {
                        "workspace_id": "w4",
                        "label": "dev-grid",
                        "panes": [{"pane_id": "w4:p1", "label": "laptop-worker"}]
                    }
                ]
            }
        }

        split_result = {
            "id": "cli:pane:split",
            "result": {
                "pane": {
                    "pane_id": "w4:p2",
                    "workspace_id": "w4"
                }
            }
        }

        with patch.object(self.client, "get_snapshot", new_callable=AsyncMock, return_value=mock_snapshot), \
             patch.object(self.client, "run_cli", new_callable=AsyncMock, return_value=(0, json.dumps(split_result), "")) as mock_run:

            new_pane_id = await self.client.allocate_worker_pane(
                workspace_label="dev-grid",
                direction="right",
                cwd="/Users/hermes/projects/AGY"
            )

            self.assertEqual(new_pane_id, "w4:p2")
            mock_run.assert_called_once_with([
                "pane", "split", "w4:p1",
                "--direction", "right",
                "--no-focus",
                "--cwd", "/Users/hermes/projects/AGY"
            ], timeout=10.0)

    async def test_dispatch_worker_full_flow(self):
        """
        Verify dispatch_worker coordinates branch isolation, FrugaLLM environment injection,
        agent startup, prompt submission with --wait, and output retrieval.
        """
        worker_id = "opencode-dev"
        prompt = "Refactor auth handler to use bearer tokens"
        branch = "task/1727480000_opencode_dev"

        calls = []

        async def fake_run_cli(args, timeout=30.0):
            calls.append(list(args))
            subcmd = args[0]
            if subcmd == "pane" and args[1] == "split":
                return 0, json.dumps({"result": {"pane": {"pane_id": "w4:p2"}}}), ""
            elif subcmd == "pane" and args[1] == "run":
                return 0, "ok", ""
            elif subcmd == "agent" and args[1] == "start":
                return 0, json.dumps({"status": "ready"}), ""
            elif subcmd == "agent" and args[1] == "prompt":
                return 0, "Submission successful", ""
            elif subcmd == "agent" and args[1] == "read":
                return 0, "Refactored auth handler successfully. Tests ready.", ""
            return 0, "", ""

        with patch.object(self.client, "run_cli", side_effect=fake_run_cli), \
             patch.object(self.client, "get_snapshot", new_callable=AsyncMock, return_value={}):

            result = await self.client.dispatch_worker(
                worker_id=worker_id,
                prompt=prompt,
                branch_name=branch,
                repo_path="/Users/hermes/projects/AGY"
            )

            self.assertEqual(result["worker_id"], "opencode-dev")
            self.assertEqual(result["branch"], branch)
            self.assertEqual(result["status"], "settled")
            self.assertIn("Refactored auth handler", result["output"])

            # 1. Verify pane split was called
            split_calls = [c for c in calls if c[0] == "pane" and c[1] == "split"]
            self.assertTrue(len(split_calls) >= 1)

            # 2. Verify git checkout -b branch was run in pane
            branch_calls = [c for c in calls if c[0] == "pane" and c[1] == "run" and any(f"git checkout -b {branch}" in arg for arg in c)]
            self.assertEqual(len(branch_calls), 1)

            # 3. Verify native FrugaLLM credentials injected into worker pane
            env_calls = [c for c in calls if c[0] == "pane" and c[1] == "run" and any("OPENAI_BASE_URL=" in str(arg) for arg in c)]
            self.assertEqual(len(env_calls), 1)
            self.assertIn('OPENAI_API_KEY="horned66"', env_calls[0][3])
            self.assertIn('OPENAI_API_BASE="http://127.0.0.1:61721/v1"', env_calls[0][3])

            # 4. Verify agent start called with opencode kind and unique dynamic name
            start_calls = [c for c in calls if c[0] == "agent" and c[1] == "start"]
            self.assertEqual(len(start_calls), 1)
            self.assertTrue(start_calls[0][2].startswith("opencode-"))
            self.assertEqual(start_calls[0][4], "opencode")

            # 5. Verify agent prompt and decoupled agent wait targeting the dynamic agent
            prompt_calls = [c for c in calls if c[0] == "agent" and c[1] == "prompt"]
            self.assertEqual(len(prompt_calls), 1)
            self.assertEqual(prompt_calls[0][2], start_calls[0][2])

            wait_calls = [c for c in calls if c[0] == "agent" and c[1] == "wait"]
            self.assertEqual(len(wait_calls), 1)
            self.assertEqual(wait_calls[0][2], start_calls[0][2])
            self.assertIn("--until", wait_calls[0])
            self.assertIn("done", wait_calls[0])
            self.assertIn("blocked", wait_calls[0])
            self.assertIn("--timeout", wait_calls[0])

            # 6. Verify agent read called
            read_calls = [c for c in calls if c[0] == "agent" and c[1] == "read"]
            self.assertEqual(len(read_calls), 1)
            self.assertIn("--lines", read_calls[0])

    async def test_unique_agent_naming_prevents_collisions(self):
        """Verify sequential tasks generate distinct agent names to avoid name collisions."""
        started_agents = []

        async def fake_run_cli(args, timeout=30.0):
            subcmd = args[0]
            if subcmd == "pane" and args[1] == "split":
                return 0, json.dumps({"result": {"pane": {"pane_id": "w4:p2"}}}), ""
            elif subcmd == "agent" and args[1] == "start":
                started_agents.append(args[2])
                return 0, "ok", ""
            elif subcmd == "agent" and args[1] == "prompt":
                return 0, "done", ""
            elif subcmd == "agent" and args[1] == "read":
                return 0, "output", ""
            return 0, "", ""

        with patch.object(self.client, "run_cli", side_effect=fake_run_cli), \
             patch.object(self.client, "get_snapshot", new_callable=AsyncMock, return_value={}):

            res1 = await self.client.dispatch_worker(
                worker_id="opencode-dev",
                prompt="Task 1",
                task_id="task-1001"
            )
            res2 = await self.client.dispatch_worker(
                worker_id="opencode-dev",
                prompt="Task 2",
                task_id="task-1002"
            )

            self.assertEqual(len(started_agents), 2)
            self.assertEqual(res1["agent_name"], "opencode-task-1001")
            self.assertEqual(res2["agent_name"], "opencode-task-1002")
            self.assertNotEqual(started_agents[0], started_agents[1])
            self.assertEqual(started_agents[0], "opencode-task-1001")
            self.assertEqual(started_agents[1], "opencode-task-1002")

    async def test_herdr_agent_prompt_non_zero_exit_triggers_infrastructure_error(self):
        """Verify non-zero exit code or blocked agent from agent prompt triggers immediate InfrastructureError."""
        calls = []

        async def fake_run_cli(args, timeout=30.0):
            calls.append(list(args))
            subcmd = args[0]
            if subcmd == "pane" and args[1] == "split":
                return 0, json.dumps({"result": {"pane": {"pane_id": "w4:p2"}}}), ""
            elif subcmd == "agent" and args[1] == "start":
                return 0, "ok", ""
            elif subcmd == "agent" and args[1] == "prompt":
                return 1, json.dumps({"error": {"code": "agent_blocked", "message": "Agent blocked waiting for user input"}}), ""
            return 0, "", ""

        with patch.object(self.client, "run_cli", side_effect=fake_run_cli), \
             patch.object(self.client, "get_snapshot", new_callable=AsyncMock, return_value={}):

            with self.assertRaises(InfrastructureError) as ctx:
                await self.client.dispatch_worker(
                    worker_id="opencode-dev",
                    prompt="Do work",
                    task_id="task-fail"
                )

            self.assertIn("Herdr agent dispatch failed", str(ctx.exception))
            # Verify agent wait was NEVER called because we failed fast!
            wait_calls = [c for c in calls if c[0] == "agent" and c[1] == "wait"]
            self.assertEqual(len(wait_calls), 0)

    async def test_teardown_removes_workspace_and_stops_herdr_agent(self):
        """Verify remove_worktree stops the active Herdr agent and removes the workspace."""
        cli_calls = []

        async def fake_run_cli(args, timeout=15.0):
            cli_calls.append(list(args))
            return 0, json.dumps({"result": {"status": "ok"}}), ""

        with patch.object(self.client, "run_cli", side_effect=fake_run_cli), \
             patch("asyncio.create_subprocess_exec", new_callable=AsyncMock):

            res = await self.client.remove_worktree(
                workspace_id="w9",
                branch_name="task-feature",
                worktree_path="/tmp/worktree",
                agent_name="opencode-task-feature",
                force=True
            )

            self.assertTrue(res["success"])
            # 1. Verify agent stop was called
            agent_stop = [c for c in cli_calls if c[0] == "agent" and c[1] == "stop"]
            self.assertEqual(len(agent_stop), 1)
            self.assertEqual(agent_stop[0][2], "opencode-task-feature")

            # 2. Verify worktree remove workspace was called
            ws_rm = [c for c in cli_calls if c[0] == "worktree" and c[1] == "remove"]
            self.assertEqual(len(ws_rm), 1)
            self.assertIn("w9", ws_rm[0])

    def test_snapshot_parser_extracts_agent_status_and_terminal_title(self):
        """Verify parse_snapshot_pane_info extracts agent_status and terminal_title for conversational replies."""
        mock_snapshot = {
            "id": "cli:api:snapshot",
            "result": {
                "snapshot": {
                    "panes": [
                        {
                            "pane_id": "w9:p1",
                            "workspace_id": "w9",
                            "agent_status": "blocked",
                            "terminal_title": "OC | Fixing missing slack_bolt dependency …",
                            "cwd": "/Users/hermes/.herdr/worktrees/AGY/task-feature"
                        },
                        {
                            "pane_id": "w4:p1",
                            "workspace_id": "w4",
                            "agent_status": "running",
                            "terminal_title": "opencode run refactor",
                            "cwd": "/Users/hermes/projects/AGY"
                        }
                    ],
                    "agents": [
                        {
                            "name": "opencode-task-feature",
                            "pane_id": "w9:p1",
                            "workspace_id": "w9",
                            "agent_status": "blocked"
                        }
                    ]
                }
            }
        }

        # 1. Test blocked pane extraction
        info_blocked = self.client.parse_snapshot_pane_info(mock_snapshot, pane_id="w9:p1")
        self.assertEqual(info_blocked["agent_status"], "blocked")
        self.assertEqual(info_blocked["terminal_title"], "OC | Fixing missing slack_bolt dependency …")
        self.assertEqual(info_blocked["pane_id"], "w9:p1")
        self.assertEqual(info_blocked["workspace_id"], "w9")

        # 2. Test running pane extraction
        info_running = self.client.parse_snapshot_pane_info(mock_snapshot, pane_id="w4:p1")
        self.assertEqual(info_running["agent_status"], "running")
        self.assertEqual(info_running["terminal_title"], "opencode run refactor")

        # 3. Test agent_name lookup
        info_by_agent = self.client.parse_snapshot_pane_info(mock_snapshot, agent_name="opencode-task-feature")
        self.assertEqual(info_by_agent["agent_status"], "blocked")
        self.assertEqual(info_by_agent["pane_id"], "w9:p1")

    async def test_readiness_polling_waits_for_interactive_ready_true_before_prompting(self):
        """Verify readiness polling repeatedly queries snapshot until interactive_ready is True."""
        call_count = 0

        async def fake_get_snapshot():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                # Agent is booting up; interactive_ready is False
                return {
                    "result": {
                        "snapshot": {
                            "agents": [{
                                "name": "opencode-task-ready",
                                "pane_id": "w4:p2",
                                "interactive_ready": False,
                                "agent_status": "starting"
                            }]
                        }
                    }
                }
            # Turn 3: Agent TUI ready
            return {
                "result": {
                    "snapshot": {
                        "agents": [{
                            "name": "opencode-task-ready",
                            "pane_id": "w4:p2",
                            "interactive_ready": True,
                            "agent_status": "idle"
                        }]
                    }
                }
            }

        with patch.object(self.client, "get_snapshot", side_effect=fake_get_snapshot):
            ready = await self.client.wait_for_agent_ready(
                agent_name="opencode-task-ready",
                pane_id="w4:p2",
                timeout_s=5.0,
                poll_interval_s=0.01
            )
            self.assertTrue(ready)
            self.assertEqual(call_count, 3)

    async def test_prompt_graceful_fallback_on_5000ms_idle_warning_when_agent_active(self):
        """Verify 5000ms idle warning falls back to checking snapshot and proceeding to agent wait."""
        cli_calls = []

        async def fake_run_cli(args, timeout=30.0):
            cli_calls.append(list(args))
            sub = args[0]
            if sub == "pane" and args[1] == "split":
                return 0, json.dumps({"result": {"pane": {"pane_id": "w4:p2"}}}), ""
            elif sub == "agent" and args[1] == "start":
                return 0, "ok", ""
            elif sub == "agent" and args[1] == "prompt":
                # Simulated Herdr 5000ms idle observer timeout error
                return 1, "agent prompt produced no observed working or blocked state within 5000 ms; current status is idle", ""
            elif sub == "agent" and args[1] == "wait":
                return 0, "done", ""
            elif sub == "agent" and args[1] == "read":
                return 0, "Settled output after fallback wait", ""
            return 0, "", ""

        mock_active_snapshot = {
            "result": {
                "snapshot": {
                    "panes": [{
                        "pane_id": "w4:p2",
                        "agent_status": "working",
                        "terminal_title": "opencode active turn"
                    }],
                    "agents": [{
                        "name": "opencode-task-fallback",
                        "pane_id": "w4:p2",
                        "agent_status": "working",
                        "interactive_ready": True
                    }]
                }
            }
        }

        with patch.object(self.client, "run_cli", side_effect=fake_run_cli), \
             patch.object(self.client, "get_snapshot", new_callable=AsyncMock, return_value=mock_active_snapshot):

            result = await self.client.dispatch_worker(
                worker_id="opencode-dev",
                prompt="Refactor code",
                task_id="task-fallback"
            )

            self.assertEqual(result["status"], "settled")
            self.assertEqual(result["output"], "Settled output after fallback wait")

            # Verify wait was still called despite prompt returning code 1 with 5000ms warning
            wait_calls = [c for c in cli_calls if c[0] == "agent" and c[1] == "wait"]
            self.assertEqual(len(wait_calls), 1)
            self.assertIn("--until", wait_calls[0])
            self.assertIn("done", wait_calls[0])
            self.assertIn("blocked", wait_calls[0])

    async def test_native_frugallm_credentials_exported_into_pane(self):
        """Verify OPENAI_BASE_URL, OPENAI_API_BASE, OPENAI_API_KEY, and MODEL are exported into pane."""
        cli_calls = []

        async def fake_run_cli(args, timeout=30.0):
            cli_calls.append(list(args))
            sub = args[0]
            if sub == "pane" and args[1] == "split":
                return 0, json.dumps({"result": {"pane": {"pane_id": "w4:p2"}}}), ""
            elif sub == "agent" and args[1] == "start":
                return 0, "ok", ""
            elif sub == "agent" and args[1] == "prompt":
                return 0, "done", ""
            elif sub == "agent" and args[1] == "wait":
                return 0, "done", ""
            elif sub == "agent" and args[1] == "read":
                return 0, "done", ""
            return 0, "", ""

        with patch.object(self.client, "run_cli", side_effect=fake_run_cli), \
             patch.object(self.client, "get_snapshot", new_callable=AsyncMock, return_value={}):

            await self.client.dispatch_worker(
                worker_id="opencode-dev",
                prompt="Test task",
                task_id="task-frugallm"
            )

            env_runs = [c for c in cli_calls if c[0] == "pane" and c[1] == "run" and any("OPENAI_API_KEY" in str(arg) for arg in c)]
            self.assertEqual(len(env_runs), 1)
            export_payload = env_runs[0][3]
            self.assertIn('export OPENAI_BASE_URL="http://127.0.0.1:61721/v1"', export_payload)
            self.assertIn('export OPENAI_API_BASE="http://127.0.0.1:61721/v1"', export_payload)
            self.assertIn('export OPENAI_API_KEY="horned66"', export_payload)
            self.assertIn('export MODEL="frugallm/coding"', export_payload)


if __name__ == "__main__":
    unittest.main()
