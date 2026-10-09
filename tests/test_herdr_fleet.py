#!/usr/bin/env python3
"""
tests/test_herdr_fleet.py
Unit tests for HerdrFleet persistent topology, node probing, and machine routing.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from herdr_fleet import HerdrFleet, TOPOLOGY_SPECS, wrap_windows_command


class TestHerdrFleet(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.fleet = HerdrFleet()

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

        compute_prompts = [
            "Train PyTorch model on CUDA GPU",
            "Run local LLM benchmark on desktop-pc",
            "Render Blender animation frames on PC"
        ]
        for p in compute_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "compute-grid", f"Failed for prompt: {p}")

        cloud_prompts = [
            "Configure terraform ingress rules on OCI cloud",
            "Check oracle cloud vps wireguard connection",
            "Update oci-machine public ip DNS records"
        ]
        for p in cloud_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "cloud-ops", f"Failed for prompt: {p}")

    def test_target_resolution(self):
        """Verify target resolution for all node aliases."""
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

    def test_oci_production_guardrails(self):
        """Verify explicit guardrail preventing generic compute/testing tasks from routing to oci-machine."""
        generic_prompts = [
            "Run compute benchmark in the cloud",
            "Test model evaluation container in cloud",
            "Run automated testing on cloud cluster"
        ]
        for p in generic_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertNotEqual(triage["workspace_label"], "cloud-ops", f"Generic task routed to cloud-ops: {p}")

        explicit_prompts = [
            "Configure terraform ingress rules on OCI cloud",
            "Check oracle cloud vps wireguard connection",
            "Update oci-machine public ip DNS records",
        ]
        for p in explicit_prompts:
            triage = self.fleet.triage_prompt(p)
            self.assertEqual(triage["workspace_label"], "cloud-ops", f"Explicit task failed to route to cloud-ops: {p}")

    def test_windows_command_wrapping(self):
        """Verify POSIX commands for Windows are wrapped in cmd.exe /c."""
        wrapped = wrap_windows_command("dir && echo ok")
        self.assertEqual(wrapped, 'cmd.exe /c "dir && echo ok"')

        clean = wrap_windows_command("uptime")
        self.assertEqual(clean, "uptime")

    def test_static_pane_concurrency_and_locking(self):
        """Verify get_pane_lock returns same asyncio.Lock for same pane_id."""
        lock1 = self.fleet.get_pane_lock("w4:p1")
        lock2 = self.fleet.get_pane_lock("w4:p1")
        lock_other = self.fleet.get_pane_lock("w5:p1")
        self.assertIs(lock1, lock2)
        self.assertIsNot(lock1, lock_other)

    async def test_probe_node_hermes_local(self):
        """Verify probe_node on hermes returns disk and container metrics."""
        mock_proc_df = MagicMock()
        mock_proc_df.returncode = 0
        mock_proc_df.communicate = AsyncMock(return_value=(b"Filesystem Size Used Avail Use% Mounted on\n/dev/disk3s1 460G 20G 440G 5% /\n", b""))

        mock_proc_docker = MagicMock()
        mock_proc_docker.returncode = 0
        mock_proc_docker.communicate = AsyncMock(return_value=(b"container1\ncontainer2\n", b""))

        async def fake_exec(*args, **kwargs):
            if args[0] == "df":
                return mock_proc_df
            return mock_proc_docker

        with patch("asyncio.create_subprocess_exec", side_effect=fake_exec):
            res = await self.fleet.probe_node("hermes")
            self.assertTrue(res["online"])
            self.assertEqual(res["node"], "hermes")
            self.assertIn("root disk 440G free", res["status"])
            self.assertIn("2 containers", res["status"])

    async def test_probe_node_desktop_pc_nvidia_smi(self):
        """Verify probe_node on desktop-pc captures GPU telemetry."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"NVIDIA GeForce RTX 3080, 25, 2100, 10240\n", b""))
        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc):
            res = await self.fleet.probe_node("desktop-pc")
            self.assertTrue(res["online"])
            self.assertEqual(res["node"], "desktop-pc")
            self.assertIn("RTX 3080", res["status"])
            self.assertIn("25% GPU", res["status"])
            self.assertIn("2100/10240 MiB VRAM", res["status"])

    async def test_probe_node_personal_laptop_offline(self):
        """Verify probe_node on laptop offline returns asleep status."""
        mock_proc = MagicMock()
        mock_proc.returncode = 255
        mock_proc.communicate = AsyncMock(return_value=(b"", b"Connection timed out"))
        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc):
            res = await self.fleet.probe_node("personal-laptop")
            self.assertFalse(res["online"])
            self.assertIn("Sleeping / Standby", res["status"])

    async def test_oci_fleet_probe_8s_timeout(self):
        """Verify OCI machine probe enforces 8.0s timeout."""
        with patch("asyncio.create_subprocess_exec", side_effect=asyncio.TimeoutError):
            res = await self.fleet.probe_node("oci-machine")
            self.assertFalse(res["online"])
            self.assertIn("Unreachable / Offline", res["status"])

    async def test_oci_probe_transient_retry(self):
        """Verify OCI machine probe retries once on transient failure."""
        call_count = 0
        mock_proc_fail = MagicMock()
        mock_proc_fail.returncode = 255
        mock_proc_fail.communicate = AsyncMock(return_value=(b"", b"connection refused"))

        mock_proc_ok = MagicMock()
        mock_proc_ok.returncode = 0
        mock_proc_ok.communicate = AsyncMock(return_value=(b"up 45 days, 2 users, load average: 0.15", b""))

        async def fake_exec(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_proc_fail
            return mock_proc_ok

        with patch("asyncio.create_subprocess_exec", side_effect=fake_exec):
            res = await self.fleet.probe_node("oci-machine")
            self.assertEqual(call_count, 2)
            self.assertTrue(res["online"])
            self.assertIn("Online (up 45 days, 2 users)", res["status"])

    async def test_update_node_agy_localhost_and_remote(self):
        """Verify update_node_agy checks local and remote agy CLI binary versions."""
        mock_proc_local = MagicMock()
        mock_proc_local.returncode = 0
        mock_proc_local.communicate = AsyncMock(
            return_value=(b"Checking for updates... (current version 1.3.1)\nYou are already on the latest version.", b"")
        )

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc_local):
            res = await self.fleet.update_node_agy("hermes")
            self.assertIn("v1.3.1", res["status"])
            self.assertFalse(res["updated"])

        mock_proc_remote = MagicMock()
        mock_proc_remote.returncode = 0
        mock_proc_remote.communicate = AsyncMock(
            return_value=(b"Found new version 1.3.1. Update successful!", b"")
        )

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc_remote):
            res_remote = await self.fleet.update_node_agy("personal-laptop")
            self.assertIn("updated", res_remote["status"])
            self.assertTrue(res_remote["updated"])

    async def test_update_fleet_agy_aggregates_all_nodes(self):
        """Verify update_fleet_agy concurrently checks all fleet targets."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(
            return_value=(b"Checking for updates... (current version 1.3.1)\nYou are already on the latest version.", b"")
        )

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            fleet_res = await self.fleet.update_fleet_agy(nodes=["hermes", "desktop-pc"])
            self.assertEqual(len(fleet_res), 2)
            nodes = [r["node"] for r in fleet_res]
            self.assertIn("localhost (hermes)", nodes)
            self.assertIn("desktop-pc", nodes)


