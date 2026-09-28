#!/usr/bin/env python3
"""
Test Suite for Herdr Persistent Fleet, Chief of Staff & Hermes Butler Orchestration.
Covers:
- Herdr server connection and fleet machine discovery
- Persistent workspace topology (fleet-control, homelab-ops, cloud-ops, dev-grid, compute-grid)
- Chief of Staff & Butler prompt triage and routing
- Interactive blocked state detection & Slack thread unblocking
- Rolling HUD debouncing and final executive summary delivery
"""

import os
import sys
import json
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import aiohttp
import app
import triage_engine
import herdr_fleet
from herdr_fleet import HerdrFleet, TOPOLOGY_SPECS


class TestHerdrChiefOfStaff(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.fleet = HerdrFleet()
        app.fleet = self.fleet
        app.herdr = self.fleet

    async def test_herdr_discovery_and_machines(self):
        """Verify Herdr server connectivity and registered fleet machines."""
        running = await self.fleet.ensure_server()
        self.assertTrue(running, "Herdr server must be running")

        machines = await self.fleet.get_machines()
        machine_labels = [m.get("label") for m in machines]
        self.assertIn("desktop-pc", machine_labels)
        self.assertIn("oci-machine", machine_labels)
        self.assertIn("personal-laptop", machine_labels)

    async def test_topology_workspaces_and_panes(self):
        """Verify persistent topology has all 5 required workspaces and panes."""
        topo = await self.fleet.ensure_topology()
        self.assertIn("fleet-control", topo)
        self.assertIn("homelab-ops", topo)
        self.assertIn("cloud-ops", topo)
        self.assertIn("dev-grid", topo)
        self.assertIn("compute-grid", topo)

        self.assertEqual(topo["fleet-control"]["agent_name"], "chief-of-staff")
        self.assertEqual(topo["fleet-control"]["agent_kind"], "agy")

        self.assertEqual(topo["homelab-ops"]["agent_name"], "hermes-worker")
        self.assertEqual(topo["homelab-ops"]["agent_kind"], "hermes")

        self.assertEqual(topo["cloud-ops"]["target_machine"], "oci-machine")
        self.assertEqual(topo["dev-grid"]["target_machine"], "personal-laptop")
        self.assertEqual(topo["compute-grid"]["target_machine"], "desktop-pc")

    def test_chief_of_staff_triage_routing(self):
        """Verify Chief of Staff accurately triages prompts to appropriate agents."""
        # 1. Homelab Chores -> Hermes Butler
        homelab_prompts = [
            "Check docker container status on the homelab",
            "Plex media server is buffering, please restart container",
            "Run zfs backup scrub on truenas pool",
            "Jellyfin storage volume is at 90% capacity"
        ]
        for p in homelab_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "homelab-ops", f"Failed for prompt: {p}")
            self.assertEqual(triage["role"], "Hermes Butler")

        # 2. Compute / Heavy Workloads -> Desktop PC
        compute_prompts = [
            "Train PyTorch model on CUDA GPU",
            "Run local LLM benchmark on desktop-pc",
            "Render Blender animation frames on PC"
        ]
        for p in compute_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "compute-grid", f"Failed for prompt: {p}")

        # 3. Cloud Services -> OCI Machine
        cloud_prompts = [
            "Configure terraform ingress rules on OCI cloud",
            "Check oracle cloud vps wireguard connection",
            "Update oci-machine public ip DNS records"
        ]
        for p in cloud_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "cloud-ops", f"Failed for prompt: {p}")

        # 4. Primary Dev Anchor -> fleet-control (localhost) for generic coding/repo tasks
        generic_coding_prompts = [
            "Refactor frontend react hooks and run tests",
            "Fix bug in git repo and commit changes",
            "Run python test suite and generate coverage"
        ]
        for p in generic_coding_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "fleet-control", f"Generic coding failed to route to localhost: {p}")

        # 5. Personal Laptop (dev-grid) -> STRICTLY requires explicit laptop intent
        laptop_prompts = [
            "Build new feature on laptop branch",
            "Inspect git status on my laptop",
            "Run flutter test on personal-laptop"
        ]
        for p in laptop_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "dev-grid", f"Explicit laptop task failed to route to dev-grid: {p}")

        # 6. General / Orchestration -> Chief of Staff (fleet-control)
        general_prompts = [
            "What is the status of our fleet today?",
            "Schedule a review meeting next Monday",
            "Summarize recent architecture decisions"
        ]
        for p in general_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "fleet-control", f"Failed for prompt: {p}")

    def test_target_resolution(self):
        """Verify resolve_target handles aliases, machines, and workspace labels."""
        self.assertEqual(self.fleet.resolve_target("homelab")["workspace_label"], "homelab-ops")
        self.assertEqual(self.fleet.resolve_target("hermes")["workspace_label"], "homelab-ops")
        self.assertEqual(self.fleet.resolve_target("butler")["workspace_label"], "homelab-ops")

        self.assertEqual(self.fleet.resolve_target("chief")["workspace_label"], "fleet-control")
        self.assertEqual(self.fleet.resolve_target("agy")["workspace_label"], "fleet-control")

        self.assertEqual(self.fleet.resolve_target("oci")["workspace_label"], "cloud-ops")
        self.assertEqual(self.fleet.resolve_target("oci-machine")["workspace_label"], "cloud-ops")

        self.assertEqual(self.fleet.resolve_target("laptop")["workspace_label"], "dev-grid")
        self.assertEqual(self.fleet.resolve_target("personal-laptop")["workspace_label"], "dev-grid")

        self.assertEqual(self.fleet.resolve_target("pc")["workspace_label"], "compute-grid")
        self.assertEqual(self.fleet.resolve_target("desktop-pc")["workspace_label"], "compute-grid")

    async def test_switch_to_herdr_target_in_slack(self):
        """Verify 'switch to <target>' command repoints thread to Herdr workspace."""
        client = AsyncMock()
        channel = "C_HERDR"
        thread_ts = "999.888"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {
            "host": "local",
            "ip": None,
            "workspace": "/tmp",
            "target_workspace": "fleet-control",
            "blocked_pane": None,
            "conversation_id": None
        }

        event = {
            "channel": channel,
            "ts": thread_ts,
            "text": "switch to homelab"
        }
        await app.process_user_prompt(event, client)

        session = app.sessions[session_key]
        self.assertEqual(session["target_workspace"], "homelab-ops")
        client.chat_postMessage.assert_called_once()
        reply = client.chat_postMessage.call_args[1]["text"]
        self.assertIn("Switched target to remote host *homelab-ops*", reply)

    async def test_interactive_blocked_state_and_unblock(self):
        """Verify blocked alert is posted to Slack and subsequent message pipes to pane when actively blocked."""
        client = AsyncMock()
        channel = "C_BLOCK"
        thread_ts = "123.456"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {
            "host": "local",
            "ip": None,
            "workspace": "/tmp",
            "target_workspace": "homelab-ops",
            "blocked_pane": "w2:p1",
            "conversation_id": None
        }

        # User replies in thread to unblock
        unblock_event = {
            "channel": channel,
            "ts": "123.457",
            "thread_ts": thread_ts,
            "text": "yes"
        }

        with patch.object(app.fleet, "get_pane_status", new_callable=AsyncMock, return_value=("blocked", "Are you sure? [y/N]:")), \
             patch.object(app.fleet, "send_input", new_callable=AsyncMock) as mock_send_input:
            await app.process_user_prompt(unblock_event, client)

            mock_send_input.assert_called_once_with("w2:p1", "yes")
            self.assertIsNone(app.sessions[session_key]["blocked_pane"])

            client.chat_postMessage.assert_called_once()
            call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Delivered input `yes` to agent.", call_text)

    async def test_interactive_unblock_dropped_when_pane_no_longer_blocked(self):
        """Verify stray messages are not executed as raw commands when pane has already completed or errored."""
        client = AsyncMock()
        channel = "C_BLOCK_STRAY"
        thread_ts = "123.458"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {
            "host": "local",
            "ip": None,
            "workspace": "/tmp",
            "target_workspace": "homelab-ops",
            "blocked_pane": "w2:p1",
            "conversation_id": None
        }

        stray_event = {
            "channel": channel,
            "ts": "123.459",
            "thread_ts": thread_ts,
            "text": "rm -rf /tmp/important"
        }

        with patch.object(app.fleet, "get_pane_status", new_callable=AsyncMock, return_value=("idle", None)), \
             patch.object(app.fleet, "send_input", new_callable=AsyncMock) as mock_send_input:
            await app.process_user_prompt(stray_event, client)

            # Must NOT deliver input to pane stdin
            mock_send_input.assert_not_called()
            self.assertIsNone(app.sessions[session_key]["blocked_pane"])

            client.chat_postMessage.assert_called_once()
            call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("no longer waiting for confirmation", call_text)

    def test_oci_production_guardrails(self):
        """Verify explicit guardrail preventing generic compute/testing tasks from routing to oci-machine."""
        # Generic compute and testing prompts must NEVER route to cloud-ops (oci-machine)
        generic_prompts = [
            "Run compute benchmark in the cloud",
            "Test model evaluation container in cloud",
            "Run automated testing on cloud cluster"
        ]
        for p in generic_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertNotEqual(triage["workspace_label"], "cloud-ops", f"Generic task routed to cloud-ops: {p}")

        # Explicit production, OCI, cloud ingress, or public hosting prompts MUST route to cloud-ops
        explicit_prompts = [
            "Configure terraform ingress rules on OCI cloud",
            "Check oracle cloud vps wireguard connection",
            "Update oci-machine public ip DNS records",
            "Deploy production API to public hosting"
        ]
        for p in explicit_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "cloud-ops", f"Explicit task failed to route to cloud-ops: {p}")

    def test_windows_execution_wrapping(self):
        """Verify POSIX idioms are wrapped in shell runner for desktop-pc Windows worker pane."""
        from herdr_fleet import wrap_windows_command

        # POSIX idioms wrapped in cmd.exe /c
        wrapped1 = wrap_windows_command("cd test && python script.py")
        self.assertEqual(wrapped1, 'cmd.exe /c "cd test && python script.py"')

        wrapped2 = wrap_windows_command("./run.sh 2>/dev/null && echo done")
        self.assertEqual(wrapped2, 'cmd.exe /c ".\\run.sh 2>nul && echo done"')

        # Explicit PowerShell or cmd runners left as-is
        self.assertEqual(wrap_windows_command("powershell -Command Get-Process"), "powershell -Command Get-Process")
        self.assertEqual(wrap_windows_command("cmd.exe /c echo hello"), "cmd.exe /c echo hello")

        # Commands without POSIX idioms left clean
        self.assertEqual(wrap_windows_command("python train.py"), "python train.py")

    def test_static_pane_concurrency_and_locking(self):
        """Verify per-pane mutex lock and I/O lock guarantee serialization for worker panes."""
        lock1 = self.fleet.get_pane_lock("w4:p1")
        lock2 = self.fleet.get_pane_lock("w4:p1")
        lock_other = self.fleet.get_pane_lock("w5:p1")
        self.assertIs(lock1, lock2, "Same pane must share exact same mutex lock")
        self.assertIsNot(lock1, lock_other, "Different panes must have isolated locks")

        io_lock1 = self.fleet.get_pane_io_lock("w4:p1")
        io_lock2 = self.fleet.get_pane_io_lock("w4:p1")
        self.assertIs(io_lock1, io_lock2, "Same pane must share exact same I/O lock")

    async def test_pane_status_blocked_detection(self):
        """Verify get_pane_status detects interactive confirmation prompts."""
        mock_output = "Are you sure you want to delete container? [y/N]:"
        with patch.object(self.fleet, "read_pane", new_callable=AsyncMock, return_value=mock_output), \
             patch.object(self.fleet, "run_cli", new_callable=AsyncMock, return_value=(1, "", "")):

            status, prompt = await self.fleet.get_pane_status("w2:p1", agent_name="hermes-worker")
            self.assertEqual(status, "blocked")
            self.assertIn("[y/N]", prompt)

    async def test_remote_artifact_discovery(self):
        """Verify find_remote_workspace_artifacts discovers remote files over SSH."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_payload = b'{"/home/opc/result.png": 1780000000.0, "/home/opc/data.csv": 1780000010.0}\n'
        mock_proc.communicate = AsyncMock(return_value=(mock_payload, b""))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
            res = await app.find_remote_workspace_artifacts("oci-machine", "100.113.173.33", "~")
            self.assertEqual(len(res), 2)
            self.assertIn("/home/opc/result.png", res)
            self.assertEqual(res["/home/opc/result.png"], 1780000000.0)

            # Verify SSH invoked python3 with script input
            args = mock_exec.call_args[0]
            self.assertEqual(args[0], "ssh")
            self.assertIn("python3", args)

    async def test_remote_git_diff_windows_and_posix(self):
        """Verify get_git_diff uses appropriate shell runner on Windows vs POSIX remotes."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"diff --git a/test.py b/test.py\n+new line", b""))

        # 1. Test Windows (desktop-pc)
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
            diff = await app.get_git_diff("desktop-pc", None, "~")
            self.assertIsNotNone(diff)
            self.assertIn("diff --git", diff)
            args = mock_exec.call_args[0]
            self.assertEqual(args[0], "ssh")
            # Must use cmd.exe /c runner
            cmd_arg = next(a for a in args if "cmd.exe /c" in a)
            self.assertIn("2>nul", cmd_arg)

        # 2. Test POSIX (personal-laptop)
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
            diff = await app.get_git_diff("personal-laptop", None, "~")
            self.assertIsNotNone(diff)
            args = mock_exec.call_args[0]
            self.assertEqual(args[0], "ssh")
            cmd_arg = next(a for a in args if "cd ~" in a)
            self.assertIn("2>/dev/null", cmd_arg)

    async def test_remote_artifact_exfiltration_upload(self):
        """Verify remote new artifacts are fetched to local hermes temp file and uploaded to Slack."""
        client = AsyncMock()
        before_artifacts = {"/home/opc/old.png": 1780000000.0}
        after_artifacts = {
            "/home/opc/old.png": 1780000000.0,
            "/home/opc/new_diagram.png": 1780000050.0
        }

        with patch.object(app, "find_remote_workspace_artifacts", new_callable=AsyncMock, return_value=after_artifacts), \
             patch.object(app, "fetch_remote_file", new_callable=AsyncMock, return_value="/tmp/test_downloaded.png") as mock_fetch:

            await app.upload_new_artifacts(
                active_workspace="~",
                before_artifacts=before_artifacts,
                channel="C_REMOTE_ART",
                thread_ts="111.222",
                client=client,
                host="oci-machine",
                ip="100.113.173.33"
            )

            mock_fetch.assert_called_once_with("oci-machine", "100.113.173.33", "/home/opc/new_diagram.png")
            client.files_upload_v2.assert_called_once()
            call_kw = client.files_upload_v2.call_args[1]
            self.assertEqual(call_kw["channel"], "C_REMOTE_ART")
            self.assertEqual(call_kw["thread_ts"], "111.222")
            self.assertEqual(call_kw["filename"], "new_diagram.png")
            self.assertEqual(call_kw["file"], "/tmp/test_downloaded.png")

    async def test_headless_task_execution_and_logging(self):
        """Verify fleet.execute_task runs commands headlessly and logs output without terminal chrome."""
        target_info = {
            "workspace_label": "homelab-ops",
            "pane_id": "w2:p1",
            "pane_label": "hermes-worker",
            "role": "Hermes Butler",
            "target_machine": "local"
        }
        res = await self.fleet.execute_task(
            target_info=target_info,
            instruction="echo 'container_healthy=true'",
            task_id="test_headless_123"
        )
        self.assertEqual(res["exit_code"], 0)
        self.assertIn("container_healthy=true", res["stdout"])
        self.assertFalse(res["blocked"])
        self.assertTrue(os.path.isfile(res["log_file"]))
        with open(res["log_file"], "r") as f:
            content = f.read()
            self.assertIn("container_healthy=true", content)

    async def test_headless_agy_synthesis(self):
        """Verify synthesize_with_agy invokes agy CLI brain headlessly in project context."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"*Briefing:* All 5 containers are running normally.", b""))

        target_info = {
            "workspace_label": "homelab-ops",
            "role": "Hermes Butler",
            "target_machine": "local"
        }

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
            briefing = await app.synthesize_with_agy(
                clean_prompt="check containers",
                raw_output="container1 UP\ncontainer2 UP",
                exit_code=0,
                target_info=target_info
            )
            self.assertIn("All 5 containers are running normally.", briefing)
            mock_exec.assert_called_once()
            args = mock_exec.call_args[0]
            self.assertIn("-p", args)
            self.assertIn("--output-format", args)
            self.assertEqual(mock_exec.call_args[1]["cwd"], app.PROJECT_ROOT)

    def test_prompt_dispatch_hygiene_hermes_worker(self):
        """Verify prompt dispatch hygiene sends only clean instruction and context to hermes-worker."""
        # 1. build_hermes_prompt formats clean instruction without system prompt/mode boundary
        clean_prompt = app.build_hermes_prompt("df -h")
        self.assertEqual(clean_prompt, "df -h")
        self.assertNotIn("[Slack Interface Context]", clean_prompt)
        self.assertNotIn("• Mode Boundary:", clean_prompt)

        # 2. build_hermes_prompt includes concise uploaded files note if provided
        upload_meta = [{"name": "report.csv", "path": "/path/report.csv", "size": 100}]
        files_prompt = app.build_hermes_prompt("process report", uploaded_files=upload_meta)
        self.assertIn("report.csv", files_prompt)
        self.assertIn("process report", files_prompt)
        self.assertNotIn("• Mode Boundary:", files_prompt)

    async def test_send_prompt_strips_mode_boundary_for_hermes_worker(self):
        """Verify fleet.send_prompt strips multi-paragraph system prompts and mode boundary for hermes-worker."""
        target_info = {
            "workspace_label": "homelab-ops",
            "pane_id": "w2:p1",
            "pane_label": "hermes-worker",
            "agent_name": "hermes-worker",
            "agent_kind": "hermes",
            "target_machine": "local"
        }
        raw_multi_prompt = (
            "[Slack Interface Context]\n"
            "You are AGY, an autonomous senior software engineer...\n"
            "• Mode Boundary:\n"
            "  - These Slack capabilities apply exclusively...\n\n"
            "[User Message]\n"
            "docker ps -a"
        )

        with patch.object(self.fleet, "run_cli", new_callable=AsyncMock, return_value=(0, "", "")) as mock_run_cli:
            await self.fleet.send_prompt(target_info, raw_multi_prompt)

            # Check what arguments were passed to herdr
            calls = mock_run_cli.call_args_list
            sent_texts = [
                call[0][0][3] if (len(call[0][0]) > 3 and call[0][0][1] in ("prompt", "send-text"))
                else (call[0][0][2] if (len(call[0][0]) > 2 and call[0][0][1] == "send-text") else "")
                for call in calls
            ]
            self.assertTrue(any("docker ps -a" in txt for txt in sent_texts), f"Expected 'docker ps -a' in sent texts: {sent_texts}")
            for txt in sent_texts:
                self.assertNotIn("• Mode Boundary:", txt)
                self.assertNotIn("[Slack Interface Context]", txt)

    async def test_reacji_lifecycle_and_executive_briefing(self):
        """Verify reacji state transitions (:eyes: -> :gear: -> :white_check_mark:) and final executive briefing delivery."""
        client = AsyncMock()
        target_info = {
            "workspace_label": "homelab-ops",
            "pane_id": "w2:p1",
            "pane_label": "hermes-worker",
            "agent_name": "hermes-worker",
            "agent_kind": "hermes",
            "role": "Hermes Butler",
            "target_machine": "local"
        }

        mock_task_res = {
            "task_id": "t1",
            "exit_code": 0,
            "stdout": "Filesystem Size Used\n/dev/disk3s1 460G 11G (4%)",
            "stderr": "",
            "output": "Filesystem Size Used\n/dev/disk3s1 460G 11G (4%)",
            "log_file": "/tmp/herdr_tasks/t1.log",
            "blocked": False,
            "blocked_prompt": None
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock, return_value=mock_task_res), \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value="*Briefing:* Homelab disk is healthy at 4% capacity."), \
             patch("app.find_workspace_artifacts", return_value={}), \
             patch("app.get_git_diff", new_callable=AsyncMock, return_value=None):

            await app.execute_herdr_task(
                prompt="df -h",
                target_info=target_info,
                channel="C_TEST",
                thread_ts="123.000",
                session_key="C_TEST:123.000",
                client=client,
                msg_ts="123.000"
            )

            # 1. Verify reacji transitions: gear was swapped in, then white_check_mark
            reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
            self.assertIn("gear", reacji_adds)
            self.assertIn("white_check_mark", reacji_adds)

            # 2. Verify executive briefing was posted as final message
            client.chat_postMessage.assert_called_once()
            call_kw = client.chat_postMessage.call_args[1]
            self.assertEqual(call_kw["channel"], "C_TEST")
            self.assertEqual(call_kw["thread_ts"], "123.000")
            self.assertIn("Homelab disk is healthy at 4% capacity.", call_kw["text"])

    def test_learn_thread_extraction_and_transcript_formatting(self):
        """Verify compile_thread_transcript correctly structures thread context and excludes /learn."""
        messages = [
            {"user": "U123", "text": "Restart plex docker container on homelab", "ts": "100.001"},
            {"bot_id": "B123", "text": "Checking docker container status... container stopped.", "ts": "100.002"},
            {"bot_id": "B123", "text": "docker compose restart plex executed cleanly.", "files": [{"name": "plex.log"}], "ts": "100.003"},
            {"user": "U123", "text": "/learn remember to inspect docker compose file before restart", "ts": "100.004"}
        ]

        transcript = app.compile_thread_transcript(messages, current_msg_ts="100.004")

        # 1. Original intent is captured
        self.assertIn("### Original Intent (User: U123)", transcript)
        self.assertIn("Restart plex docker container on homelab", transcript)

        # 2. Intermediary worker steps are formatted
        self.assertIn("### Step 1: Liaison / Chief of Staff (AGY)", transcript)
        self.assertIn("docker compose restart plex executed cleanly.", transcript)
        self.assertIn("[Attached files: plex.log]", transcript)

        # 3. The /learn command itself is excluded
        self.assertNotIn("/learn remember to inspect", transcript)

    async def test_mock_distillation_and_rule_update(self):
        """Verify distill_lesson executes agy CLI in json mode and codifies rules into .agents/rules/."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        envelope_json = {
            "status": "SUCCESS",
            "response": json.dumps({
                "summary": "Always use docker compose with -f /opt/plex/docker-compose.yml",
                "files_updated": [".agents/rules/homelab.md"],
                "action_type": "rule"
            })
        }
        mock_proc.communicate = AsyncMock(return_value=(json.dumps(envelope_json).encode("utf-8"), b""))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
            res = await app.distill_lesson(
                transcript="### Original Intent\nRestart Plex\n",
                user_notes="use explicit compose file path"
            )

            mock_exec.assert_called_once()
            args = mock_exec.call_args[0]
            self.assertIn("-p", args)
            self.assertIn("--output-format", args)
            self.assertIn("json", args)
            self.assertIn("--dangerously-skip-permissions", args)
            self.assertEqual(mock_exec.call_args[1]["cwd"], app.PROJECT_ROOT)

            self.assertEqual(res["action_type"], "rule")
            self.assertEqual(res["files_updated"], [".agents/rules/homelab.md"])
            self.assertIn("Always use docker compose", res["summary"])

    async def test_mock_distillation_skill_crystallization(self):
        """Verify distill_lesson produces a skill runbook when action_type is skill."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        direct_json = {
            "summary": "Plex Container Healthcheck and Restart Runbook",
            "files_updated": [".agents/skills/restart-plex/SKILL.md"],
            "action_type": "skill"
        }
        mock_proc.communicate = AsyncMock(return_value=(json.dumps(direct_json).encode("utf-8"), b""))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            res = await app.distill_lesson(
                transcript="### Original Intent\nRestart Plex\n"
            )
            self.assertEqual(res["action_type"], "skill")
            self.assertEqual(res["files_updated"], [".agents/skills/restart-plex/SKILL.md"])
            self.assertEqual(res["summary"], "Plex Container Healthcheck and Restart Runbook")

    async def test_learn_command_lifecycle_in_thread(self):
        """Verify full /learn command execution inside a Slack thread with reacji and confirmation."""
        client = AsyncMock()
        client.conversations_replies.return_value = {
            "messages": [
                {"user": "U123", "text": "Set up WireGuard VPN on OCI cloud", "ts": "200.001"},
                {"bot_id": "B123", "text": "Configured wg0 interface with endpoint 100.64.0.5", "ts": "200.002"},
                {"user": "U123", "text": "/learn add wireguard port forwarding steps", "ts": "200.003"}
            ]
        }

        mock_distill_res = {
            "summary": "WireGuard Ingress Configuration",
            "files_updated": [".agents/skills/wireguard-setup/SKILL.md"],
            "action_type": "skill"
        }

        event = {
            "channel": "C_CLOUD",
            "ts": "200.003",
            "thread_ts": "200.001",
            "text": "/learn add wireguard port forwarding steps"
        }

        with patch("app.distill_lesson", new_callable=AsyncMock, return_value=mock_distill_res) as mock_distill:
            await app.process_user_prompt(event, client)

            # 1. Verify initial reaction was brain (<200ms)
            init_reaction = client.reactions_add.call_args_list[0][1]
            self.assertEqual(init_reaction["name"], "brain")
            self.assertEqual(init_reaction["timestamp"], "200.003")

            # 2. Verify distill_lesson received compiled transcript and user notes
            mock_distill.assert_called_once()
            called_transcript, called_notes = mock_distill.call_args[1].get("transcript") or mock_distill.call_args[0][0], mock_distill.call_args[1].get("user_notes", "")
            self.assertIn("Set up WireGuard VPN", called_transcript)
            self.assertEqual(called_notes, "add wireguard port forwarding steps")

            # 3. Verify reaction swapped to white_check_mark
            reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
            self.assertIn("white_check_mark", reacji_adds)

            # 4. Verify Slack thread confirmation reply
            client.chat_postMessage.assert_called_once()
            reply_kw = client.chat_postMessage.call_args[1]
            self.assertEqual(reply_kw["channel"], "C_CLOUD")
            self.assertEqual(reply_kw["thread_ts"], "200.001")
            self.assertIn("🧠 *Learned & Codified:*", reply_kw["text"])
            self.assertIn("Added deterministic runbook for *WireGuard Ingress Configuration*", reply_kw["text"])
            self.assertIn(".agents/skills/wireguard-setup/SKILL.md", reply_kw["text"])

    async def test_learn_command_outside_thread_warning(self):
        """Verify /learn called outside a thread informs user and reacts with warning."""
        client = AsyncMock()
        event = {
            "channel": "C_GENERAL",
            "ts": "300.001",
            "text": "/learn"
        }

        await app.process_user_prompt(event, client)

        # 1. Reactions: initial brain, then swapped to warning
        reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
        self.assertIn("brain", reacji_adds)
        self.assertIn("warning", reacji_adds)

        # 2. Warning message posted
        client.chat_postMessage.assert_called_once()
        msg_kw = client.chat_postMessage.call_args[1]
        self.assertEqual(msg_kw["channel"], "C_GENERAL")
        self.assertIn("The `/learn` command must be used as a reply in a Slack thread", msg_kw["text"])

    async def test_learn_empty_thread_warning(self):
        """Verify /learn in an empty thread with no prior history notifies user."""
        client = AsyncMock()
        client.conversations_replies.return_value = {
            "messages": [
                {"user": "U123", "text": "/learn", "ts": "400.001"}
            ]
        }
        event = {
            "channel": "C_TEST",
            "ts": "400.001",
            "thread_ts": "400.001",
            "text": "/learn"
        }

        await app.process_user_prompt(event, client)

        reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
        self.assertIn("brain", reacji_adds)
        self.assertIn("warning", reacji_adds)

        client.chat_postMessage.assert_called_once()
        msg_kw = client.chat_postMessage.call_args[1]
        self.assertIn("No prior execution history found in this thread to distill", msg_kw["text"])

    async def test_learn_distillation_failure_handling(self):
        """Verify distillation failure swaps reacji to :x: and reports failure concisely."""
        client = AsyncMock()
        client.conversations_replies.return_value = {
            "messages": [
                {"user": "U123", "text": "Run query", "ts": "500.001"},
                {"bot_id": "B123", "text": "Query succeeded", "ts": "500.002"},
                {"user": "U123", "text": "/learn", "ts": "500.003"}
            ]
        }
        event = {
            "channel": "C_FAIL",
            "ts": "500.003",
            "thread_ts": "500.001",
            "text": "/learn"
        }

        with patch("app.distill_lesson", side_effect=RuntimeError("LLM distillation timeout")):
            await app.process_user_prompt(event, client)

            reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
            self.assertIn("brain", reacji_adds)
            self.assertIn("x", reacji_adds)

            client.chat_postMessage.assert_called_once()
            msg_kw = client.chat_postMessage.call_args[1]
            self.assertIn("Distillation Failed", msg_kw["text"])
            self.assertIn("LLM distillation timeout", msg_kw["text"])

    async def test_bearings_graceful_degradation_laptop_offline(self):
        """Verify /bearings reports [Asleep / Offline] for personal-laptop without failing."""
        client = AsyncMock()
        event = {
            "channel": "C_DEV",
            "ts": "600.001",
            "text": "/bearings"
        }

        mock_node_map = {
            "hermes": {"node": "hermes", "role": "localhost (Primary Dev & Homelab)", "status": "Root disk 100Gi free, 3 running", "online": True},
            "desktop-pc": {"node": "desktop-pc", "role": "Compute Grid (Windows CUDA)", "status": "GPU 0% util, 500MB / 12288MB VRAM", "online": True},
            "personal-laptop": {"node": "personal-laptop", "role": "Personal Laptop (Active Workstation)", "status": "[Asleep / Offline]", "online": False},
            "oci-machine": {"node": "oci-machine", "role": "Cloud Operations (Production VPS)", "status": "Online (up 45 days)", "online": True},
        }
        async def fake_probe(node, timeout=8.0):
            return mock_node_map.get(node, {"node": node, "status": "[Offline]", "online": False})

        with patch.object(app.herdr, "probe_node", side_effect=fake_probe) as mock_probe, \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value=None):

            await app.process_user_prompt(event, client)

            reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
            self.assertIn("eyes", reacji_adds)
            self.assertIn("gear", reacji_adds)
            self.assertIn("white_check_mark", reacji_adds)

            client.chat_postMessage.assert_called_once()
            msg_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Fleet Bearings & Status Digest", msg_text)
            self.assertIn("personal-laptop", msg_text)
            self.assertIn("[Asleep / Offline]", msg_text)
            self.assertIn("hermes", msg_text)

    async def test_update_aborts_when_pane_lock_held(self):
        """Verify /update aborts and warns when a pane lock or background task is active."""
        client = AsyncMock()
        event = {
            "channel": "C_OPS",
            "ts": "700.001",
            "text": "/update"
        }

        with patch.object(self.fleet, "has_active_locks", return_value=["w1:p1"]):
            await app.process_user_prompt(event, client)

            reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
            self.assertIn("eyes", reacji_adds)
            self.assertIn("gear", reacji_adds)
            self.assertIn("warning", reacji_adds)
            self.assertNotIn("white_check_mark", reacji_adds)

            client.chat_postMessage.assert_called_once()
            msg_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Cannot update", msg_text)
            self.assertIn("w1:p1", msg_text)

    async def test_update_triggers_rollback_on_test_failure(self):
        """Verify /update rolls back with git reset --hard ORIG_HEAD if test gate fails."""
        client = AsyncMock()
        event = {
            "channel": "C_OPS",
            "ts": "800.001",
            "text": "/update"
        }

        mock_proc_pull = AsyncMock()
        mock_proc_pull.returncode = 0
        mock_proc_pull.communicate.return_value = (b"Already up to date.\n", b"")

        mock_proc_test = AsyncMock()
        mock_proc_test.returncode = 1
        mock_proc_test.communicate.return_value = (b"", b"FAILED (failures=1)\nAssertionError: False != True")

        mock_proc_reset = AsyncMock()
        mock_proc_reset.returncode = 0
        mock_proc_reset.communicate.return_value = (b"HEAD is now at 1234567\n", b"")

        def subprocess_side_effect(*args, **kwargs):
            cmd = args[0]
            if cmd == "git" and len(args) > 1 and args[1] == "pull":
                return mock_proc_pull
            elif cmd == "git" and len(args) > 1 and args[1] == "reset":
                return mock_proc_reset
            else:
                return mock_proc_test

        with patch.object(self.fleet, "has_active_locks", return_value=[]), \
             patch.object(self.fleet, "get_active_tasks", new_callable=AsyncMock, return_value=[]), \
             patch("asyncio.create_subprocess_exec", side_effect=subprocess_side_effect) as mock_exec:

            await app.process_user_prompt(event, client)

            reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
            self.assertIn("eyes", reacji_adds)
            self.assertIn("gear", reacji_adds)
            self.assertIn("x", reacji_adds)

            reset_calls = [c for c in mock_exec.call_args_list if c[0][:4] == ("git", "reset", "--hard", "ORIG_HEAD")]
            self.assertTrue(len(reset_calls) >= 1, "Expected git reset --hard ORIG_HEAD to be called")

            client.chat_postMessage.assert_called_once()
            msg_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Update Aborted (Test Gate Failure)", msg_text)
            self.assertIn("Rolled back to previous revision", msg_text)

    async def test_wrapup_proposes_rule_diffs_without_overwriting(self):
        """Verify /wrapup proposes quirk diffs in Slack without touching disk until approved."""
        import tempfile
        client = AsyncMock()
        event_wrapup = {
            "channel": "C_WRAP",
            "ts": "900.001",
            "text": "/wrapup"
        }

        mock_quirk = {
            "node": "desktop-pc",
            "file": "test_rules_quirk.md",
            "rule_text": "Always wrap Windows shell invocations with cmd.exe /c",
            "diff": "```diff\n+ ## Observed Quirk\n```"
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_target_file = os.path.join(tmp_dir, "test_rules_quirk.md")
            mock_quirk["file"] = test_target_file

            with patch("app.scan_recent_logs_for_quirks", return_value=[mock_quirk]), \
                 patch("app.PROJECT_ROOT", tmp_dir):

                # 1. Run wrapup
                await app.process_user_prompt(event_wrapup, client)

                # Verify file was NOT modified/created
                self.assertFalse(os.path.exists(test_target_file), "Wrapup must not write to disk before approval")

                # Verify proposal is in pending_quirk_proposals
                session_key = "900.001"
                self.assertIn(session_key, app.pending_quirk_proposals)

                # Verify proposed diff posted to Slack
                msg_text = client.chat_postMessage.call_args[1]["text"]
                self.assertIn("Session Wrap-Up & Housekeeping", msg_text)
                self.assertIn("Suggested Quirk Rule", msg_text)
                self.assertIn("approve quirk", msg_text)

                # 2. Now user sends approval message in thread
                event_approve = {
                    "channel": "C_WRAP",
                    "ts": "900.002",
                    "thread_ts": "900.001",
                    "text": "approve quirk"
                }
                client.chat_postMessage.reset_mock()
                await app.process_user_prompt(event_approve, client)

                # Verify proposal is consumed
                self.assertNotIn(session_key, app.pending_quirk_proposals)

                # Verify file was written to disk with the approved rule
                self.assertTrue(os.path.exists(test_target_file), "Approval must save rule to disk")
                with open(test_target_file, "r") as f:
                    content = f.read()
                self.assertIn("Always wrap Windows shell invocations with cmd.exe /c", content)

                # Verify confirmation message
                client.chat_postMessage.assert_called_once()
                confirm_text = client.chat_postMessage.call_args[1]["text"]
                self.assertIn("Quirk Rule Approved & Saved", confirm_text)

    async def test_ahoy_reports_quiescent_and_blocked(self):
        """Verify /ahoy reports quiescent status or surfaces blocked pane decisions."""
        client = AsyncMock()

        # Case A: Fleet is quiescent
        event_ahoy_quiescent = {
            "channel": "C_AHOY",
            "ts": "1000.001",
            "text": "/ahoy"
        }
        await app.process_user_prompt(event_ahoy_quiescent, client)
        reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
        self.assertIn("white_check_mark", reacji_adds)
        client.chat_postMessage.assert_called_once()
        msg_text = client.chat_postMessage.call_args[1]["text"]
        self.assertIn("Fleet Quiescent", msg_text)

        # Case B: Fleet has a blocked pane
        client.reset_mock()
        app.sessions["C_AHOY:1000.002"] = {"blocked_pane": "w2:p1"}
        with patch.object(self.fleet, "get_pane_status", new_callable=AsyncMock, return_value=("blocked", "Plex credentials required")):
            event_ahoy_blocked = {
                "channel": "C_AHOY",
                "ts": "1000.002",
                "text": "/ahoy"
            }
            await app.process_user_prompt(event_ahoy_blocked, client)
            reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
            self.assertIn("warning", reacji_adds)
            client.chat_postMessage.assert_called_once()
            msg_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Pending Decisions & Attention Required", msg_text)
            self.assertIn("Plex credentials required", msg_text)

    async def test_preflight_alignment_generation(self):
        """Verify natural language request generates Pre-Flight Alignment briefing strictly following required layout."""
        client = AsyncMock()
        client.chat_postMessage.return_value = {"ts": "1100.002"}
        event = {
            "channel": "C_ALIGN",
            "ts": "1100.001",
            "text": "Check disk space and Docker containers on hermes"
        }

        app.pending_alignment.clear()
        app.pending_alignment_by_msg_ts.clear()

        mock_plan = {
            "intent_summary": "inspect storage and docker containers on hermes",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "df -h / && docker system df",
            "assumptions": "read-only audit of hermes host",
            "choice": "shallow vs deep inspection",
            "recommendation": "inspect root disk and containers",
            "estimated_duration_sec": 15
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock) as mock_exec_task, \
             patch("app.generate_preflight_plan", new_callable=AsyncMock, return_value=mock_plan):
            await app.process_user_prompt(event, client)

            # 1. Shell task must NOT have been executed immediately
            mock_exec_task.assert_not_called()

            # 2. Initial reaction was eyes (<200ms)
            reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
            self.assertIn("eyes", reacji_adds)

            # 3. Alignment message posted
            client.chat_postMessage.assert_called_once()
            msg_text = client.chat_postMessage.call_args[1]["text"]

            # 4. Strictly assert required layout sections
            self.assertIn("On it, Carl.", msg_text)
            self.assertIn("It sounds like you want to *", msg_text)
            self.assertIn("The best agent for this is *Hermes Butler*", msg_text)
            self.assertIn("cleanly on *hermes*", msg_text)
            self.assertIn("*Here is what I'm briefing them with:*", msg_text)
            self.assertIn("> *\"", msg_text)
            self.assertIn("*Sanity check & defaults:*", msg_text)
            self.assertIn("• *Assumption:*", msg_text)
            self.assertIn("• *Choice:*", msg_text)
            self.assertIn("⤷ *_Recommendation (Default if you just say `go`):_*", msg_text)
            self.assertIn("*Hit me with `go` (or 👍) to run with the recommendation, or let me know what to tweak.*", msg_text)

            # 5. Pending plan stored in state
            session_key = "C_ALIGN:1100.001"
            self.assertIn(session_key, app.pending_alignment)
            self.assertEqual(app.pending_alignment[session_key]["alignment_msg_ts"], "1100.002")

    async def test_alignment_approval_via_text_go(self):
        """Verify replying 'go' executes the recommended dispatched payload."""
        client = AsyncMock()
        channel = "C_GO"
        thread_ts = "1200.001"
        session_key = f"{channel}:{thread_ts}"

        mock_plan = {
            "intent_summary": "audit storage on hermes",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "df -h /",
            "assumptions": "read-only audit",
            "choice": "root only vs all",
            "recommendation": "check root filesystem",
            "estimated_duration_sec": 15
        }
        app.pending_alignment[session_key] = {
            "plan": mock_plan,
            "target_info": {
                "workspace_label": "homelab-ops",
                "target_machine": "local",
                "role": "Hermes Butler",
                "pane_id": "w2:p1"
            },
            "channel": channel,
            "thread_ts": thread_ts,
            "alignment_msg_ts": "1200.002",
            "original_prompt": "Check disk space on hermes"
        }
        app.pending_alignment_by_msg_ts["1200.002"] = session_key

        event_go = {
            "channel": channel,
            "ts": "1200.003",
            "thread_ts": thread_ts,
            "text": "go"
        }

        mock_task_res = {
            "task_id": "t_go",
            "exit_code": 0,
            "stdout": "Filesystem Size Used Avail Use% Mounted on\n/dev/disk3s1 460G 20G 440G 5% /",
            "stderr": "",
            "output": "Filesystem Size Used Avail Use% Mounted on\n/dev/disk3s1 460G 20G 440G 5% /",
            "blocked": False
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock, return_value=mock_task_res) as mock_exec, \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value="*Briefing:* Root filesystem is healthy at 5% usage."):

            await app.process_user_prompt(event_go, client)

            # 1. Plan state popped
            self.assertNotIn(session_key, app.pending_alignment)
            self.assertNotIn("1200.002", app.pending_alignment_by_msg_ts)

            # 2. execute_task received the planned dispatched_payload (NOT English text!)
            mock_exec.assert_called_once()
            called_instruction = mock_exec.call_args[1].get("instruction") or mock_exec.call_args[0][1]
            self.assertEqual(called_instruction, "df -h /")

            # 3. Executive briefing posted to Slack
            client.chat_postMessage.assert_called_once()
            msg_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Root filesystem is healthy at 5% usage.", msg_text)

            # 4. Reaction swapped to white_check_mark
            reacji_adds = [c[1]["name"] for c in client.reactions_add.call_args_list]
            self.assertIn("white_check_mark", reacji_adds)

    async def test_alignment_approval_via_emoji_reaction(self):
        """Verify reacting with 👍 (+1) to the alignment message triggers execution."""
        client = AsyncMock()
        channel = "C_REACT"
        thread_ts = "1300.001"
        align_msg_ts = "1300.002"
        session_key = f"{channel}:{thread_ts}"

        mock_plan = {
            "intent_summary": "inspect running docker containers",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "docker ps",
            "assumptions": "read-only status",
            "choice": "active vs all",
            "recommendation": "list active containers",
            "estimated_duration_sec": 10
        }
        app.pending_alignment[session_key] = {
            "plan": mock_plan,
            "target_info": {
                "workspace_label": "homelab-ops",
                "target_machine": "local",
                "role": "Hermes Butler",
                "pane_id": "w2:p1"
            },
            "channel": channel,
            "thread_ts": thread_ts,
            "alignment_msg_ts": align_msg_ts,
            "original_prompt": "list containers"
        }
        app.pending_alignment_by_msg_ts[align_msg_ts] = session_key

        event_react = {
            "type": "reaction_added",
            "user": "U_CARL",
            "reaction": "+1",
            "item": {
                "type": "message",
                "channel": channel,
                "ts": align_msg_ts
            }
        }

        mock_task_res = {
            "task_id": "t_react",
            "exit_code": 0,
            "stdout": "CONTAINER ID IMAGE COMMAND CREATED STATUS PORTS NAMES",
            "stderr": "",
            "output": "CONTAINER ID IMAGE COMMAND CREATED STATUS PORTS NAMES",
            "blocked": False
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock, return_value=mock_task_res) as mock_exec, \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value="*Briefing:* All containers healthy."):

            await app.handle_reaction_added(event_react, client)

            # 1. Plan state popped
            self.assertNotIn(session_key, app.pending_alignment)
            self.assertNotIn(align_msg_ts, app.pending_alignment_by_msg_ts)

            # 2. execute_task received planned payload
            mock_exec.assert_called_once()
            called_instruction = mock_exec.call_args[1].get("instruction") or mock_exec.call_args[0][1]
            self.assertEqual(called_instruction, "docker ps")

            # 3. Executive briefing delivered
            client.chat_postMessage.assert_called_once()
            msg_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("All containers healthy.", msg_text)

    async def test_alignment_plan_adjustments_tweak(self):
        """Verify replying with tweaks regenerates the alignment plan and posts updated briefing."""
        client = AsyncMock()
        client.chat_postMessage.return_value = {"ts": "1400.004"}
        channel = "C_TWEAK"
        thread_ts = "1400.001"
        session_key = f"{channel}:{thread_ts}"

        mock_plan = {
            "intent_summary": "audit root disk",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "df -h /",
            "assumptions": "read-only audit",
            "choice": "root only vs all",
            "recommendation": "check root",
            "estimated_duration_sec": 15
        }
        app.pending_alignment[session_key] = {
            "plan": mock_plan,
            "target_info": {
                "workspace_label": "homelab-ops",
                "target_machine": "local",
                "role": "Hermes Butler",
                "pane_id": "w2:p1"
            },
            "channel": channel,
            "thread_ts": thread_ts,
            "alignment_msg_ts": "1400.002",
            "original_prompt": "Check disk"
        }
        app.pending_alignment_by_msg_ts["1400.002"] = session_key

        event_tweak = {
            "channel": channel,
            "ts": "1400.003",
            "thread_ts": thread_ts,
            "text": "include ZFS as well"
        }

        mock_updated_plan = {
            "intent_summary": "audit root disk and ZFS pools",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "df -h / && zpool list && zfs list",
            "assumptions": "read-only audit",
            "choice": "root only vs all",
            "recommendation": "check root and ZFS",
            "estimated_duration_sec": 15
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock) as mock_exec, \
             patch("app.generate_preflight_plan", new_callable=AsyncMock, return_value=mock_updated_plan):
            await app.process_user_prompt(event_tweak, client)

            # 1. Shell execution not triggered yet
            mock_exec.assert_not_called()

            # 2. Plan was updated with user tweaks
            updated_plan = app.pending_alignment[session_key]["plan"]
            self.assertIn("zfs list", updated_plan["dispatched_payload"])

            # 3. New alignment briefing posted
            client.chat_postMessage.assert_called_once()
            msg_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("On it, Carl.", msg_text)
            self.assertIn("Hit me with `go` (or 👍)", msg_text)

    async def test_conversational_question_in_alignment_thread_preserves_latched_plan(self):
        """Verify asking a conversational question in an alignment thread returns direct answer and preserves pending plan for subsequent 'go'."""
        client = AsyncMock()
        channel = "C_CONV_ALIGN"
        thread_ts = "1401.001"
        session_key = f"{channel}:{thread_ts}"

        mock_plan = {
            "intent_summary": "audit root disk",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "df -h /",
            "assumptions": "read-only audit",
            "choice": "root only vs all",
            "recommendation": "check root",
            "estimated_duration_sec": 15
        }
        app.pending_alignment[session_key] = {
            "plan": mock_plan,
            "target_info": {
                "workspace_label": "homelab-ops",
                "target_machine": "local",
                "role": "Hermes Butler",
                "pane_id": "w2:p1"
            },
            "channel": channel,
            "thread_ts": thread_ts,
            "alignment_msg_ts": "1401.002",
            "original_prompt": "Check disk"
        }
        app.pending_alignment_by_msg_ts["1401.002"] = session_key

        event_question = {
            "channel": channel,
            "ts": "1401.003",
            "thread_ts": thread_ts,
            "text": "Why did that check run against root rather than our external storage pools?"
        }

        mock_triage_converse = {
            "turn_type": "CONVERSE",
            "conversational_reply": "The check audited root disk to assess base OS partition capacity before checking external pools.",
            "action_plan": None
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock) as mock_exec, \
             patch("app.direct_gemini_triage", new_callable=AsyncMock, return_value=mock_triage_converse):

            await app.process_user_prompt(event_question, client)

            # 1. Execution was NOT triggered
            mock_exec.assert_not_called()

            # 2. Conversational answer posted directly
            client.chat_postMessage.assert_called_once()
            reply_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("assess base OS partition capacity", reply_text)
            self.assertNotIn("On it, Carl.", reply_text)
            self.assertNotIn("Here is what I'm running:", reply_text)

            # 3. CRITICAL: Latched plan is PRESERVED in pending_alignment!
            self.assertIn(session_key, app.pending_alignment)
            self.assertEqual(app.pending_alignment[session_key]["plan"]["dispatched_payload"], "df -h /")

        # Now Carl says "go" in the same thread
        event_go = {
            "channel": channel,
            "ts": "1401.004",
            "thread_ts": thread_ts,
            "text": "go"
        }

        mock_task_res = {
            "task_id": "task_align_go",
            "exit_code": 0,
            "stdout": "Filesystem Size Used Avail Use% Mounted on\n/dev/sda1 100G 20G 80G 20% /",
            "stderr": "",
            "output": "Filesystem Size Used Avail Use% Mounted on\n/dev/sda1 100G 20G 80G 20% /",
            "blocked": False
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock, return_value=mock_task_res) as mock_exec_go, \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value="*Briefing:* Root disk has 80G available."):

            await app.process_user_prompt(event_go, client)

            # 4. Latched plan executed
            mock_exec_go.assert_called_once()
            called_cmd = mock_exec_go.call_args[1].get("instruction") or mock_exec_go.call_args[0][1]
            self.assertEqual(called_cmd, "df -h /")

            # 5. Pending alignment popped
            self.assertNotIn(session_key, app.pending_alignment)

    async def test_conversational_question_followed_by_emoji_reaction_approval(self):
        """Verify conversational question preserves plan and emoji reaction 👍 on thread executes it."""
        client = AsyncMock()
        channel = "C_REAC_CONV"
        thread_ts = "1501.001"
        session_key = f"{channel}:{thread_ts}"

        mock_plan = {
            "intent_summary": "audit root disk",
            "target_node": "hermes",
            "delegated_worker": "Hermes Butler",
            "domain": "homelab infrastructure",
            "dispatched_payload": "df -h /",
            "assumptions": "read-only audit",
            "choice": "root only vs all",
            "recommendation": "check root",
            "estimated_duration_sec": 15
        }
        app.pending_alignment[session_key] = {
            "plan": mock_plan,
            "target_info": {
                "workspace_label": "homelab-ops",
                "target_machine": "local",
                "role": "Hermes Butler",
                "pane_id": "w2:p1"
            },
            "channel": channel,
            "thread_ts": thread_ts,
            "alignment_msg_ts": "1501.002",
            "original_prompt": "Check disk"
        }
        app.pending_alignment_by_msg_ts["1501.002"] = session_key

        event_question = {
            "channel": channel,
            "ts": "1501.003",
            "thread_ts": thread_ts,
            "text": "Why check root first?"
        }

        mock_triage_converse = {
            "turn_type": "CONVERSE",
            "conversational_reply": "Root holds OS binaries and temporary buffers.",
            "action_plan": None
        }

        with patch("app.direct_gemini_triage", new_callable=AsyncMock, return_value=mock_triage_converse):
            await app.process_user_prompt(event_question, client)
            self.assertIn(session_key, app.pending_alignment)

        # Carl reacts with 👍 to a message in the thread
        reaction_event = {
            "user": "U_CARL",
            "reaction": "+1",
            "item": {
                "type": "message",
                "channel": channel,
                "ts": "1501.003",
                "thread_ts": thread_ts
            }
        }

        mock_task_res = {
            "task_id": "task_reac_conv",
            "exit_code": 0,
            "stdout": "df output",
            "stderr": "",
            "output": "df output",
            "blocked": False
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock, return_value=mock_task_res) as mock_exec, \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value="*Briefing:* Clean."):

            await app.handle_reaction_added(reaction_event, client)

            mock_exec.assert_called_once()
            called_cmd = mock_exec.call_args[1].get("instruction") or mock_exec.call_args[0][1]
            self.assertEqual(called_cmd, "df -h /")
            self.assertNotIn(session_key, app.pending_alignment)

    async def test_generate_preflight_plan_unit(self):
        """Test generate_preflight_plan parsing and fallback handling."""
        target_info = {
            "workspace_label": "homelab-ops",
            "target_machine": "local",
            "role": "Hermes Butler"
        }
        # 1. Successful direct triage parse
        mock_triage = {
            "turn_type": "ACTION",
            "action_plan": {
                "intent_summary": "check disk",
                "target_node": "hermes",
                "delegated_worker": "Hermes Butler",
                "dispatched_payload": "df -h /",
                "assumptions": "Read-only audit of mounted filesystems",
                "choice": "Root vs all",
                "recommendation": "Inspect root disk",
                "estimated_duration_sec": 5
            }
        }
        with patch("app.direct_gemini_triage", new_callable=AsyncMock, return_value=mock_triage):
            plan = await app.generate_preflight_plan("check disk", target_info)
            self.assertEqual(plan["dispatched_payload"], "df -h /")
            self.assertEqual(plan["target_node"], "hermes")

        # 2. Fallback on non-action / API failure
        mock_triage_fallback = {
            "turn_type": "CONVERSE",
            "conversational_reply": "Error from Gemini"
        }
        with patch("app.direct_gemini_triage", new_callable=AsyncMock, return_value=mock_triage_fallback):
            plan_fallback = await app.generate_preflight_plan("check disk", target_info)
            self.assertEqual(plan_fallback["dispatched_payload"], "df -h /")
            self.assertEqual(plan_fallback["target_node"], "hermes")

    async def test_bang_commands_fast_path(self):
        """Verify '!' prefix commands execute fast-path without preflight alignment."""
        client = AsyncMock()

        # Test !bearings
        with patch("app.handle_bearings_request", new_callable=AsyncMock) as mock_bearings:
            event = {"channel": "C_BANG", "ts": "1500.001", "text": "!bearings"}
            await app.process_user_prompt(event, client)
            mock_bearings.assert_called_once()

        # Test !ahoy
        with patch("app.handle_ahoy_request", new_callable=AsyncMock) as mock_ahoy:
            event = {"channel": "C_BANG", "ts": "1500.002", "text": "!ahoy"}
            await app.process_user_prompt(event, client)
            mock_ahoy.assert_called_once()

        # Test !wrapup
        with patch("app.handle_wrapup_request", new_callable=AsyncMock) as mock_wrapup:
            event = {"channel": "C_BANG", "ts": "1500.003", "text": "!wrapup"}
            await app.process_user_prompt(event, client)
            mock_wrapup.assert_called_once()

        # Test !update
        with patch("app.handle_update_request", new_callable=AsyncMock) as mock_update:
            event = {"channel": "C_BANG", "ts": "1500.004", "text": "!update"}
            await app.process_user_prompt(event, client)
            mock_update.assert_called_once()

        # Test !learn
        with patch("app.handle_learn_request", new_callable=AsyncMock) as mock_learn:
            event = {"channel": "C_BANG", "ts": "1500.005", "thread_ts": "1500.001", "text": "!learn save step"}
            await app.process_user_prompt(event, client)
            mock_learn.assert_called_once()

    async def test_oci_fleet_probe_8s_timeout(self):
        """Verify query_oci_status invokes ssh targeting oci-machine with 8.0s timeout."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (b"up 135 days\n", b"")

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec, \
             patch("asyncio.wait_for", return_value=(b"up 135 days\n", b"")) as mock_wait:

            res = await app.query_oci_status()

            self.assertTrue(res["online"])
            self.assertEqual(res["node"], "oci-machine")
            self.assertIn("up 135 days", res["status"])

            args = mock_exec.call_args[0]
            self.assertEqual(args[0], "ssh")
            self.assertIn("oci-machine", args)
            self.assertIn("ConnectTimeout=8", args)

            self.assertEqual(mock_wait.call_args[1]["timeout"], 8.0)

    async def test_reaction_error_logging(self):
        """Verify add_reacji and remove_reacji catch SlackApiError and log warning without crashing."""
        client = AsyncMock()
        from slack_sdk.errors import SlackApiError
        mock_resp = {"ok": False, "error": "missing_scope"}
        slack_err = SlackApiError(message="The request failed", response=mock_resp)

        client.reactions_add.side_effect = slack_err
        client.reactions_remove.side_effect = slack_err

        with patch("app.logger.warning") as mock_log:
            await app.add_reacji(client, "C_TEST", "100.1", "eyes")
            mock_log.assert_called_once()
            self.assertIn("missing_scope", mock_log.call_args[0][0])

        with patch("app.logger.warning") as mock_log:
            await app.remove_reacji(client, "C_TEST", "100.1", "eyes")
            mock_log.assert_called_once()
            self.assertIn("missing_scope", mock_log.call_args[0][0])

    async def test_conversational_triage_direct_reply(self):
        """Verify conversational questions ('What happened?') return direct answer without alignment card or Herdr execution."""
        app.pending_alignment.clear()
        client = AsyncMock()
        event = {
            "channel": "C_CONV",
            "ts": "5000.001",
            "thread_ts": "5000.001",
            "text": "Well it was working in the previous prompt right? What happened?"
        }

        mock_converse_res = {
            "turn_type": "CONVERSE",
            "conversational_reply": "The previous operation encountered an issue or completed its turn.",
            "action_plan": None
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock) as mock_exec, \
             patch("app.direct_gemini_triage", new_callable=AsyncMock, return_value=mock_converse_res):
            await app.process_user_prompt(event, client)

            mock_exec.assert_not_called()
            self.assertEqual(len(app.pending_alignment), 0)
            client.chat_postMessage.assert_called_once()
            call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertNotIn("On it, Carl.", call_text)
            self.assertNotIn("Here is what I'm running:", call_text)
            self.assertNotIn("The best agent for this is", call_text)
            self.assertTrue(len(call_text) > 10)

    async def test_first_person_alignment_on_hermes(self):
        """Verify local tasks on Hermes generate alignment briefing with first-person phrasing and recommendation defaults."""
        app.pending_alignment.clear()
        client = AsyncMock()
        event = {
            "channel": "C_FIRST_PERSON",
            "ts": "5000.002",
            "thread_ts": "5000.002",
            "text": "Refactor frontend react hooks and run tests"
        }

        mock_action_res = {
            "turn_type": "ACTION",
            "conversational_reply": None,
            "action_plan": {
                "is_self": True,
                "target_node": "hermes",
                "delegated_worker": "Local AGY",
                "dispatched_payload": "git status -s",
                "intent_summary": "refactor frontend react hooks and run tests",
                "assumptions": "Working directory clean",
                "choice": "Run tests with coverage vs standard",
                "recommendation": "Execute standard test run",
                "estimated_duration_sec": 15
            }
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock) as mock_exec, \
             patch("app.direct_gemini_triage", new_callable=AsyncMock, return_value=mock_action_res):
            await app.process_user_prompt(event, client)

            mock_exec.assert_not_called()
            self.assertIn("C_FIRST_PERSON:5000.002", app.pending_alignment)
            client.chat_postMessage.assert_called_once()
            call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("On it, Carl.", call_text)
            self.assertIn("I'll take care of this directly on *hermes* (`localhost`).", call_text)
            self.assertIn("*Here is what I'm running:*", call_text)
            self.assertNotIn("The best agent for this is", call_text)
            self.assertIn("⤷ *_Recommendation (Default if you just say `go`):_*", call_text)
            self.assertIn("Hit me with `go` (or 👍)", call_text)

    def test_payload_validation_bans_dummy_echo(self):
        """Verify validate_action_payload strictly bans dummy echo and placeholder patterns."""
        from app import validate_action_payload

        # Dummy echo variants must be banned and replaced with an operational command
        self.assertNotEqual(validate_action_payload("echo '[Task: check git]'"), "echo '[Task: check git]'")
        self.assertEqual(validate_action_payload("echo '[Task: check git]'"), "git status -s")
        self.assertEqual(validate_action_payload("echo 'Executing homelab chore'"), "git status -s")
        self.assertEqual(validate_action_payload('echo "placeholder"'), "git status -s")
        self.assertEqual(validate_action_payload("echo '...'"), "git status -s")
        self.assertEqual(validate_action_payload(""), "git status -s")

        # Legitimate operational commands must be preserved
        self.assertEqual(validate_action_payload("df -h / && docker ps"), "df -h / && docker ps")
        self.assertEqual(validate_action_payload("git status -s"), "git status -s")
        self.assertEqual(validate_action_payload("nvidia-smi"), "nvidia-smi")
        self.assertEqual(validate_action_payload("uptime && uname -a"), "uptime && uname -a")

    async def test_oci_probe_transient_retry(self):
        """Verify query_oci_status handles transient Tailscale DERP/WireGuard negotiation with retry."""
        mock_proc_fail = AsyncMock()
        mock_proc_fail.returncode = 255
        mock_proc_fail.communicate.return_value = (b"", b"Connection timed out")

        mock_proc_succ = AsyncMock()
        mock_proc_succ.returncode = 0
        mock_proc_succ.communicate.return_value = (b"up 45 days\n", b"")

        # First call fails, second call succeeds
        with patch("asyncio.create_subprocess_exec", side_effect=[mock_proc_fail, mock_proc_succ]), \
             patch("asyncio.wait_for", side_effect=[
                 (b"", b"Connection timed out"),
                 (b"up 45 days\n", b"")
             ]), \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:

            res = await app.query_oci_status()

            self.assertTrue(res["online"])
            self.assertEqual(res["node"], "oci-machine")
            self.assertIn("up 45 days", res["status"])
            mock_sleep.assert_called_once_with(0.5)

    async def test_approval_via_thumbsup_reaction(self):
        """Verify reacting with :+1: / 👍 dispatches the pending task."""
        channel = "C_REAC"
        thread_ts = "6000.001"
        align_ts = "6000.002"
        session_key = f"{channel}:{thread_ts}"

        mock_plan = {
            "intent_summary": "inspect git status",
            "target_node": "hermes",
            "delegated_worker": "Local AGY",
            "domain": "local development",
            "dispatched_payload": "git status -s",
            "assumptions": "clean tree",
            "choice": "short vs long",
            "recommendation": "run git status",
            "estimated_duration_sec": 5,
            "is_self": True
        }

        app.pending_alignment[session_key] = {
            "plan": mock_plan,
            "target_info": {"workspace_label": "fleet-control", "target_machine": "local", "role": "Chief of Staff"},
            "channel": channel,
            "thread_ts": thread_ts,
            "alignment_msg_ts": align_ts,
            "original_prompt": "git status"
        }
        app.pending_alignment_by_msg_ts[align_ts] = session_key

        reaction_event = {
            "user": "U_CARL",
            "reaction": "+1",
            "item": {
                "type": "message",
                "channel": channel,
                "ts": align_ts
            }
        }
        client = AsyncMock()

        mock_task_res = {
            "task_id": "task_reac_123",
            "exit_code": 0,
            "stdout": "M app.py",
            "stderr": "",
            "output": "M app.py",
            "blocked": False
        }

        with patch.object(self.fleet, "execute_task", new_callable=AsyncMock, return_value=mock_task_res) as mock_exec, \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value="*Briefing:* Status is clean."):

            await app.handle_reaction_added(reaction_event, client)

            self.assertNotIn(session_key, app.pending_alignment)
            self.assertNotIn(align_ts, app.pending_alignment_by_msg_ts)
            mock_exec.assert_called_once()
            called_cmd = mock_exec.call_args[1].get("instruction") or mock_exec.call_args[0][1]
            self.assertEqual(called_cmd, "git status -s")

    async def test_direct_gemini_triage_converse_api(self):
        """Verify direct_gemini_triage returns CONVERSE when Gemini API classifies conversational intent."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "candidates": [{
                "content": {
                    "parts": [{
                        "text": json.dumps({
                            "turn_type": "CONVERSE",
                            "conversational_reply": "I am AGY, Chief of Staff."
                        })
                    }]
                }
            }]
        })

        mock_post_ctx = MagicMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.post = MagicMock(return_value=mock_post_ctx)

        with patch("aiohttp.ClientSession", return_value=mock_session), \
             patch("triage_engine.get_gemini_api_key", return_value="fake-api-key"):
            res = await triage_engine.direct_gemini_triage("Who are you?")
            self.assertEqual(res["turn_type"], "CONVERSE")
            self.assertEqual(res["conversational_reply"], "I am AGY, Chief of Staff.")
            self.assertIsNone(res["action_plan"])

    async def test_direct_gemini_triage_action_api(self):
        """Verify direct_gemini_triage returns ACTION with formatted action_plan."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "candidates": [{
                "content": {
                    "parts": [{
                        "text": json.dumps({
                            "turn_type": "ACTION",
                            "action_plan": {
                                "intent_summary": "Restart Plex docker container",
                                "target_node": "hermes",
                                "delegated_worker": "Hermes Butler",
                                "dispatched_payload": "docker restart plex",
                                "assumptions": "Docker daemon running",
                                "choice": "Soft restart vs container rebuild",
                                "recommendation": "Soft restart plex",
                                "estimated_duration_sec": 10
                            }
                        })
                    }]
                }
            }]
        })

        mock_post_ctx = MagicMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.post = MagicMock(return_value=mock_post_ctx)

        with patch("aiohttp.ClientSession", return_value=mock_session), \
             patch("triage_engine.get_gemini_api_key", return_value="fake-api-key"):
            res = await triage_engine.direct_gemini_triage("Restart plex")
            self.assertEqual(res["turn_type"], "ACTION")
            plan = res["action_plan"]
            self.assertEqual(plan["dispatched_payload"], "docker restart plex")
            self.assertEqual(plan["recommendation"], "Soft restart plex")

    async def test_direct_gemini_triage_api_error_diagnostic(self):
        """Verify API errors in direct_gemini_triage return diagnostic error without canned phrases."""
        mock_post_ctx = MagicMock()
        mock_post_ctx.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("Connection refused by Gemini endpoint"))
        mock_post_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.post = MagicMock(return_value=mock_post_ctx)

        with patch("aiohttp.ClientSession", return_value=mock_session), \
             patch("triage_engine.get_gemini_api_key", return_value="fake-api-key"):
            res = await triage_engine.direct_gemini_triage("What happened?")
            self.assertEqual(res["turn_type"], "CONVERSE")
            reply = res["conversational_reply"]
            self.assertTrue(reply.startswith("I encountered an API error while processing that turn:"))
            self.assertIn("ClientError: Connection refused", reply)
            self.assertNotIn("I'm tracking. Let me know", reply)

    async def test_direct_gemini_triage_timeout_diagnostic(self):
        """Verify timeout in direct_gemini_triage returns diagnostic timeout message without hanging 20s."""
        async def mock_post_enter(*args, **kwargs):
            raise asyncio.TimeoutError()

        mock_post_ctx = MagicMock()
        mock_post_ctx.__aenter__ = AsyncMock(side_effect=mock_post_enter)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.post = MagicMock(return_value=mock_post_ctx)

        with patch("aiohttp.ClientSession", return_value=mock_session), \
             patch("triage_engine.get_gemini_api_key", return_value="fake-api-key"):
            res = await triage_engine.direct_gemini_triage("What is the fleet status?", timeout=2.0)
            self.assertEqual(res["turn_type"], "CONVERSE")
            reply = res["conversational_reply"]
            self.assertTrue(reply.startswith("I encountered an API error while processing that turn:"))
            self.assertIn("Timeout after 2.0s", reply)

    async def test_backstage_agy_subprocess_hardening(self):
        """Verify backstage execute_agy_task passes DEVNULL stdin, permissions bypass, and stream-json format."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.stdout.readline = AsyncMock(side_effect=[
            b'{"type": "message", "content": "Done."}\n',
            b""
        ])
        mock_proc.wait = AsyncMock(return_value=0)

        client = AsyncMock()
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
            await app.execute_agy_task(
                prompt="git status",
                host="local",
                ip=None,
                active_workspace=app.PROJECT_ROOT,
                conversation_id=None,
                channel="C_TEST",
                thread_ts="100.001",
                session_key="C_TEST:100.001",
                client=client
            )

            self.assertTrue(len(mock_exec.call_args_list) >= 1)
            args = mock_exec.call_args_list[0][0]
            kwargs = mock_exec.call_args_list[0][1]
            self.assertIn("--dangerously-skip-permissions", args)
            self.assertIn("--output-format", args)
            self.assertIn("stream-json", args)
            self.assertEqual(kwargs.get("stdin"), asyncio.subprocess.DEVNULL)
            self.assertEqual(kwargs.get("cwd"), app.PROJECT_ROOT)

    def test_fallback_intent_triage_no_parroting(self):
        """Verify build_fallback_intent_triage gives direct answers without echoing user prompts."""
        target_info = {"workspace_label": "fleet-control", "target_machine": "local", "role": "Chief of Staff"}

        queries = [
            "What happened?",
            "Why did that test fail?",
            "Who are you?",
            "Can you explain this architecture?"
        ]

        for q in queries:
            res = app.build_fallback_intent_triage(q, target_info)
            self.assertEqual(res["turn_type"], "CONVERSE")
            reply = res["conversational_reply"]
            self.assertNotIn("Regarding your question", reply)
            self.assertNotIn(f'"{q}"', reply)
            self.assertTrue(len(reply) > 15)

    async def test_reacji_silences_benign_errors(self):
        """Verify add_reacji and remove_reacji silence already_reacted, no_reaction, and message_not_found."""
        from slack_sdk.errors import SlackApiError

        client = AsyncMock()

        # Benign errors: already_reacted, no_reaction, message_not_found
        for benign in ["already_reacted", "no_reaction", "message_not_found"]:
            err_resp = {"ok": False, "error": benign}
            client.reactions_add.side_effect = SlackApiError(message=benign, response=err_resp)
            client.reactions_remove.side_effect = SlackApiError(message=benign, response=err_resp)

            with patch("app.logger.warning") as mock_warn:
                await app.add_reacji(client, "C123", "123.456", "eyes")
                mock_warn.assert_not_called()

                await app.remove_reacji(client, "C123", "123.456", "eyes")
                mock_warn.assert_not_called()

        # Real errors: must log a warning
        real_err_resp = {"ok": False, "error": "not_authed"}
        client.reactions_add.side_effect = SlackApiError(message="not_authed", response=real_err_resp)
        client.reactions_remove.side_effect = SlackApiError(message="not_authed", response=real_err_resp)

        with patch("app.logger.warning") as mock_warn:
            await app.add_reacji(client, "C123", "123.456", "eyes")
            mock_warn.assert_called_once()
            self.assertIn("Slack reaction failed: not_authed", mock_warn.call_args[0][0])

        with patch("app.logger.warning") as mock_warn:
            await app.remove_reacji(client, "C123", "123.456", "eyes")
            mock_warn.assert_called_once()
            self.assertIn("Slack reaction failed: not_authed", mock_warn.call_args[0][0])

    def test_triage_prompt_fleet_topology_injection(self):
        """Verify build_triage_prompt injects complete fleet machine topology and profiles."""
        target_info = {"workspace_label": "fleet-control", "target_machine": "local", "role": "Chief of Staff"}
        prompt = app.build_triage_prompt("Check GPU status", target_info)

        self.assertIn("desktop-pc (100.80.59.45)", prompt)
        self.assertIn("personal-laptop (100.96.38.5)", prompt)
        self.assertIn("oci-machine (100.113.173.33)", prompt)
        self.assertIn("homelab-ops: localhost (hermes)", prompt)
        self.assertIn("fleet-control: localhost (/Users/hermes/projects/AGY)", prompt)

    async def test_probe_node_desktop_pc_nvidia_smi(self):
        """Verify probe_node('desktop-pc') parses nvidia-smi metrics properly."""
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (b"NVIDIA GeForce RTX 5060 Ti, 12, 2500, 16384\n", b"")

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec, \
             patch("asyncio.wait_for", return_value=(b"NVIDIA GeForce RTX 5060 Ti, 12, 2500, 16384\n", b"")):

            res = await self.fleet.probe_node("desktop-pc")
            self.assertTrue(res["online"])
            self.assertEqual(res["node"], "desktop-pc")
            self.assertIn("RTX 5060 Ti", res["status"])
            self.assertIn("12% GPU", res["status"])
            self.assertIn("2500/16384 MiB VRAM", res["status"])

            args = mock_exec.call_args[0]
            self.assertEqual(args[0], "ssh")
            self.assertIn("desktop-pc", args)
            self.assertIn("nvidia-smi --query-gpu=name,utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits", args)

    async def test_probe_node_desktop_pc_fallback_cmd(self):
        """Verify probe_node('desktop-pc') falls back to cmd.exe /c echo ok if nvidia-smi fails."""
        mock_proc_fail = AsyncMock()
        mock_proc_fail.returncode = 1
        mock_proc_fail.communicate.return_value = (b"", b"'nvidia-smi' is not recognized")

        mock_proc_succ = AsyncMock()
        mock_proc_succ.returncode = 0
        mock_proc_succ.communicate.return_value = (b"ok\r\n", b"")

        with patch("asyncio.create_subprocess_exec", side_effect=[mock_proc_fail, mock_proc_succ]), \
             patch("asyncio.wait_for", side_effect=[
                 (b"", b"'nvidia-smi' is not recognized"),
                 (b"ok\r\n", b"")
             ]):

            res = await self.fleet.probe_node("desktop-pc")
            self.assertTrue(res["online"])
            self.assertEqual(res["node"], "desktop-pc")
            self.assertEqual(res["status"], "Online (GPU Idle / Windows OK)")

    async def test_probe_node_personal_laptop(self):
        """Verify probe_node('personal-laptop') parses macOS uptime and battery state."""
        mac_output = b"20:17  up 1 day,  5:59, 3 users, load averages: 1.24 1.88 1.98\nNow drawing from 'AC Power'\n"
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (mac_output, b"")

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec, \
             patch("asyncio.wait_for", return_value=(mac_output, b"")):

            res = await self.fleet.probe_node("personal-laptop")
            self.assertTrue(res["online"])
            self.assertEqual(res["node"], "personal-laptop")
            self.assertIn("Online", res["status"])
            self.assertIn("up 1 day,  5:59", res["status"])
            self.assertIn("AC Power", res["status"])

            args = mock_exec.call_args[0]
            self.assertEqual(args[0], "ssh")
            self.assertIn("personal-laptop", args)
            self.assertIn("uptime && pmset -g batt | head -n 1", args)

    async def test_probe_node_personal_laptop_offline(self):
        """Verify probe_node('personal-laptop') gracefully marks Sleeping / Standby when unreachable."""
        with patch("asyncio.create_subprocess_exec", side_effect=asyncio.TimeoutError):
            res = await self.fleet.probe_node("personal-laptop")
            self.assertFalse(res["online"])
            self.assertEqual(res["node"], "personal-laptop")
            self.assertEqual(res["status"], "Sleeping / Standby (Ad-hoc only)")

    async def test_probe_node_hermes_local(self):
        """Verify probe_node('hermes') queries local metrics directly without ssh."""
        df_out = b"Filesystem Size Used Avail Capacity iused ifree %iused Mounted on\n/dev/disk1s4s1 466Gi 17Gi 85Gi 18% 427k 648M 0% /\n"
        docker_out = b"container-1\ncontainer-2\ncontainer-3\n"

        mock_proc_df = AsyncMock()
        mock_proc_df.returncode = 0
        mock_proc_df.communicate.return_value = (df_out, b"")

        mock_proc_docker = AsyncMock()
        mock_proc_docker.returncode = 0
        mock_proc_docker.communicate.return_value = (docker_out, b"")

        with patch("asyncio.create_subprocess_exec", side_effect=[mock_proc_df, mock_proc_docker]) as mock_exec, \
             patch("asyncio.wait_for", side_effect=[(df_out, b""), (docker_out, b"")]):

            res = await self.fleet.probe_node("hermes")
            self.assertTrue(res["online"])
            self.assertEqual(res["node"], "hermes")
            self.assertIn("85Gi free", res["status"])
            self.assertIn("3 containers", res["status"])

            # Verify no ssh call was made
            for call in mock_exec.call_args_list:
                self.assertNotEqual(call[0][0], "ssh")

    async def test_handle_bearings_calls_probe_node_and_no_tmux_panes(self):
        """Verify handle_bearings_command calls probe_node concurrently and does not touch tmux panes."""
        client = AsyncMock()
        ack = AsyncMock()
        command = {
            "channel_id": "C_BEARINGS",
            "thread_ts": "999.001",
            "command": "/bearings"
        }

        mock_probes = {
            "hermes": {"node": "hermes", "role": "localhost", "status": "Online (62Gi free)", "online": True},
            "desktop-pc": {"node": "desktop-pc", "role": "Compute Grid", "status": "Online • RTX 5060 Ti", "online": True},
            "personal-laptop": {"node": "personal-laptop", "role": "Laptop", "status": "Online (up 1 day) • AC Power", "online": True},
            "oci-machine": {"node": "oci-machine", "role": "Cloud Ops", "status": "Online (up 100 days)", "online": True}
        }

        async def fake_probe(node, timeout=8.0):
            return mock_probes[node]

        with patch.object(app.herdr, "probe_node", side_effect=fake_probe) as mock_probe_node, \
             patch.object(app.herdr, "run_cli", new_callable=AsyncMock) as mock_run_cli, \
             patch("app.synthesize_with_agy", new_callable=AsyncMock, return_value=None):

            await app.handle_bearings_command(ack, command, client)

            ack.assert_called_once()
            self.assertEqual(mock_probe_node.call_count, 4)
            called_nodes = [c[0][0] for c in mock_probe_node.call_args_list]
            self.assertIn("hermes", called_nodes)
            self.assertIn("desktop-pc", called_nodes)
            self.assertIn("personal-laptop", called_nodes)
            self.assertIn("oci-machine", called_nodes)

            # Ensure tmux panes / herdr run_cli were NOT called
            mock_run_cli.assert_not_called()

            client.chat_postMessage.assert_called_once()
            msg = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Fleet Bearings & Status Digest", msg)
            self.assertIn("RTX 5060 Ti", msg)
            self.assertIn("AC Power", msg)


if __name__ == "__main__":
    unittest.main()


