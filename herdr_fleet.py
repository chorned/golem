#!/usr/bin/env python3
"""
Herdr Fleet Management & Chief of Staff Orchestration Module
Provides persistent workspace topology management, fleet machine discovery,
Chief of Staff & Hermes Butler prompt triage, and pane dispatching/monitoring.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import json
import time
import shutil
import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger("AGY.HerdrFleet")

# Re-exports from golem.mesh for canonical object identity
from golem.mesh import (
    TOPOLOGY_SPECS,
    NODE_ALIASES,
    wrap_windows_command,
)


class HerdrFleet:
    """Manages Herdr socket interactions, fleet topology, and agent dispatching."""

    def __init__(self, herdr_bin: Optional[str] = None, socket_path: Optional[str] = None):
        self.herdr_bin = herdr_bin or shutil.which("herdr") or os.path.expanduser("~/.local/bin/herdr")
        self.socket_path = socket_path or os.environ.get("HERDR_SOCKET_PATH", os.path.expanduser("~/.config/herdr/herdr.sock"))
        self.topology: Dict[str, Dict[str, Any]] = {}
        self.machines: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._pane_locks: Dict[str, asyncio.Lock] = {}
        self._pane_io_locks: Dict[str, asyncio.Lock] = {}

    def get_pane_lock(self, pane_id: str) -> asyncio.Lock:
        """Get or create a per-pane task execution lock to prevent concurrent commands in the same pane."""
        if pane_id not in self._pane_locks:
            self._pane_locks[pane_id] = asyncio.Lock()
        return self._pane_locks[pane_id]

    def get_pane_io_lock(self, pane_id: str) -> asyncio.Lock:
        """Get or create a per-pane I/O lock to ensure atomic send-text + send-keys sequences."""
        if pane_id not in self._pane_io_locks:
            self._pane_io_locks[pane_id] = asyncio.Lock()
        return self._pane_io_locks[pane_id]

    def get_all_locks(self) -> Dict[str, asyncio.Lock]:
        """Return dict of all active per-pane locks."""
        return self._pane_locks

    def has_active_locks(self) -> List[str]:
        """Return list of pane IDs currently holding an active lock."""
        return [pane_id for pane_id, lock in self._pane_locks.items() if lock.locked()]

    async def get_active_tasks(self) -> List[Dict[str, Any]]:
        """Return list of panes currently working or blocked."""
        active = []
        for ws_name, info in self.topology.items():
            pane_id = info.get("pane_id")
            if pane_id:
                try:
                    status, detail = await self.get_pane_status(pane_id)
                    if status in ("working", "blocked"):
                        active.append({
                            "workspace": ws_name,
                            "pane_id": pane_id,
                            "status": status,
                            "detail": detail
                        })
                except Exception:
                    pass
        return active

    async def run_cli(self, args: List[str], timeout: float = 15.0) -> Tuple[int, str, str]:
        """Execute a Herdr CLI command and return (returncode, stdout, stderr)."""
        cmd = [self.herdr_bin] + args
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            out_str = stdout.decode("utf-8", errors="replace").strip()
            err_str = stderr.decode("utf-8", errors="replace").strip()
            return proc.returncode or 0, out_str, err_str
        except asyncio.TimeoutError:
            logger.warning(f"Herdr command timed out after {timeout}s: {' '.join(cmd)}")
            return -1, "", f"Command timed out after {timeout}s"
        except Exception as e:
            logger.error(f"Error running Herdr command {' '.join(cmd)}: {e}")
            return -1, "", str(e)

    async def ensure_server(self) -> bool:
        """Ensure the Herdr server daemon is running."""
        code, out, _ = await self.run_cli(["status", "--json"], timeout=5.0)
        if code == 0:
            try:
                data = json.loads(out)
                if data.get("server", {}).get("running"):
                    return True
            except Exception:
                pass

        logger.info("Herdr server is not running. Starting headless Herdr server...")
        try:
            # Spawn herdr server in detached mode
            subprocess_proc = await asyncio.create_subprocess_exec(
                self.herdr_bin, "server",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            # Give server up to 5 seconds to bind socket
            for _ in range(25):
                await asyncio.sleep(0.2)
                if os.path.exists(self.socket_path):
                    code, out, _ = await self.run_cli(["status", "--json"], timeout=2.0)
                    if code == 0:
                        try:
                            data = json.loads(out)
                            if data.get("server", {}).get("running"):
                                logger.info("Herdr server started successfully.")
                                return True
                        except Exception:
                            pass
        except Exception as e:
            logger.error(f"Failed to start Herdr server: {e}")
        return os.path.exists(self.socket_path)

    async def get_machines(self) -> List[Dict[str, Any]]:
        """Fetch saved fleet machines using `herdr machine list --json`."""
        code, out, err = await self.run_cli(["machine", "list", "--json"], timeout=8.0)
        if code == 0 and out:
            try:
                self.machines = json.loads(out)
                return self.machines
            except Exception as e:
                logger.error(f"Failed to parse herdr machine list JSON: {e}")
        return self.machines

    async def get_workspaces(self) -> List[Dict[str, Any]]:
        """List current workspaces from Herdr."""
        code, out, err = await self.run_cli(["workspace", "list"], timeout=5.0)
        if code == 0 and out:
            try:
                data = json.loads(out)
                return data.get("result", {}).get("workspaces", [])
            except Exception as e:
                logger.error(f"Failed to parse workspace list: {e}")
        return []

    async def get_panes(self) -> List[Dict[str, Any]]:
        """List current panes from Herdr."""
        code, out, err = await self.run_cli(["pane", "list"], timeout=5.0)
        if code == 0 and out:
            try:
                data = json.loads(out)
                return data.get("result", {}).get("panes", [])
            except Exception as e:
                logger.error(f"Failed to parse pane list: {e}")
        return []

    async def get_agents(self) -> List[Dict[str, Any]]:
        """List active agents from Herdr."""
        code, out, err = await self.run_cli(["agent", "list"], timeout=5.0)
        if code == 0 and out:
            try:
                data = json.loads(out)
                return data.get("result", {}).get("agents", [])
            except Exception as e:
                logger.error(f"Failed to parse agent list: {e}")
        return []

    async def ensure_topology(self) -> Dict[str, Dict[str, Any]]:
        """
        Verify and create the persistent Herdr workspace topology:
        - fleet-control (chief-of-staff / agy)
        - homelab-ops (hermes-worker / hermes)
        - cloud-ops (oci-worker -> oci-machine)
        - dev-grid (laptop-worker -> personal-laptop)
        - compute-grid (pc-worker -> desktop-pc)
        """
        async with self._lock:
            await self.ensure_server()
            await self.get_machines()

            existing_workspaces = await self.get_workspaces()
            ws_by_label = {w.get("label"): w for w in existing_workspaces}

            existing_panes = await self.get_panes()
            panes_by_ws = {}
            for p in existing_panes:
                ws_id = p.get("workspace_id")
                if ws_id not in panes_by_ws:
                    panes_by_ws[ws_id] = []
                panes_by_ws[ws_id].append(p)

            for ws_name, spec in TOPOLOGY_SPECS.items():
                ws = ws_by_label.get(ws_name)
                if not ws:
                    logger.info(f"Creating missing Herdr workspace: {ws_name}")
                    create_args = ["workspace", "create", "--label", ws_name, "--no-focus"]
                    if spec.get("default_cwd"):
                        create_args.extend(["--cwd", os.path.expanduser(spec["default_cwd"])])
                    code, out, err = await self.run_cli(create_args)
                    if code == 0 and out:
                        try:
                            created_data = json.loads(out).get("result", {})
                            ws = created_data.get("workspace")
                            root_pane = created_data.get("root_pane")
                            if ws:
                                ws_by_label[ws_name] = ws
                            if root_pane:
                                panes_by_ws[root_pane.get("workspace_id")] = [root_pane]
                        except Exception as e:
                            logger.error(f"Error parsing workspace creation result: {e}")

                if not ws:
                    continue

                ws_id = ws.get("workspace_id")
                ws_panes = panes_by_ws.get(ws_id, [])
                pane = ws_panes[0] if ws_panes else None
                pane_id = pane.get("pane_id") if pane else f"{ws_id}:p1"

                # Rename pane to match spec if needed
                desired_pane_label = spec["pane_label"]
                if pane and pane.get("label") != desired_pane_label:
                    logger.info(f"Renaming pane {pane_id} to '{desired_pane_label}' in workspace {ws_name}")
                    await self.run_cli(["pane", "rename", pane_id, desired_pane_label])

                # Check agent startup for fleet-control and homelab-ops
                agent_name = spec.get("agent_name")
                agent_kind = spec.get("agent_kind")
                if agent_name and agent_kind:
                    agents = await self.get_agents()
                    agent_running = any(a.get("name") == agent_name for a in agents)
                    if not agent_running:
                        logger.info(f"Starting agent '{agent_name}' (kind: {agent_kind}) in pane {pane_id}...")
                        start_code, start_out, start_err = await self.run_cli(
                            ["agent", "start", agent_name, "--kind", agent_kind, "--pane", pane_id, "--timeout", "15000"]
                        )
                        if start_code != 0:
                            logger.warning(f"Could not start agent via 'agent start' ({start_err}). Launching interactive shell fallback...")
                            # Fallback: run binary in pane
                            await self.run_cli(["pane", "run", pane_id, agent_kind])

                # Check SSH connection for remote grid panes if not connected
                ssh_target = spec.get("ssh_target")
                if ssh_target and pane_id:
                    code, out, _ = await self.run_cli(["pane", "read", pane_id, "--source", "recent-unwrapped", "--lines", "5"])
                    # If pane is just at default local shell prompt, connect to remote target
                    if "ssh " not in out and ("hermes@hermes" in out or "%" in out and ssh_target not in out):
                        logger.info(f"Connecting pane {pane_id} in {ws_name} to remote target {ssh_target}...")
                        await self.run_cli(["pane", "run", pane_id, f"ssh {ssh_target}"])

                self.topology[ws_name] = {
                    "workspace_id": ws_id,
                    "workspace_label": ws_name,
                    "pane_id": pane_id,
                    "pane_label": desired_pane_label,
                    "agent_name": agent_name,
                    "agent_kind": agent_kind,
                    "role": spec["role"],
                    "target_machine": spec["target_machine"],
                    "default_cwd": spec.get("default_cwd", "~")
                }

            logger.info(f"Herdr fleet topology active: {list(self.topology.keys())}")
            return self.topology

    def triage_prompt(self, prompt: str, current_target: Optional[str] = None) -> Dict[str, Any]:
        """
        Determine destination workspace & agent using Chief of Staff orchestration rules:
        - Homelab Chores -> homelab-ops / hermes-worker (Hermes butler)
        - Coding / Projects -> dev-grid / laptop-worker (personal-laptop)
        - Compute / Heavy Workloads -> compute-grid / pc-worker (desktop-pc)
        - Cloud Services -> cloud-ops / oci-worker (oci-machine)
        - General / Orchestration -> fleet-control / chief-of-staff (Antigravity Chief of Staff)
        """
        # If thread already has an active workspace target, respect it
        if current_target:
            if current_target in self.topology:
                return self.topology[current_target]
            if current_target in TOPOLOGY_SPECS:
                return TOPOLOGY_SPECS[current_target]

        text = prompt.lower()

        # Guardrail: Never route to oci-machine / cloud-ops unless prompt explicitly mentions
        # production, OCI, cloud ingress, or public hosting.
        # Bounded cloud resources should never be selected as generic compute or testing target.
        oci_explicit_patterns = [
            r"\bproduction\b", r"\bprod\b",
            r"\boci\b", r"\boci-machine\b", r"\boracle\s+cloud\b",
            r"\b(?:cloud\s+)?ingress\b", r"\bwireguard\b", r"\bpublic\s+ip\b",
            r"\bpublic\s+host(?:ing)?\b", r"\bhosting\b"
        ]
        has_oci_explicit = any(re.search(pat, text) for pat in oci_explicit_patterns)

        # 1. Homelab Chores -> Hermes Butler
        homelab_keywords = [
            "docker", "container", "compose", "plex", "jellyfin", "storage", "backup",
            "zfs", "truenas", "nas", "maintenance", "reboot", "systemctl", "homelab",
            "butler", "update server", "disk", "media", "carlflix", "arr", "torrent"
        ]
        if any(re.search(r"\b" + re.escape(kw) + r"\b", text) for kw in homelab_keywords):
            return self.topology.get("homelab-ops", TOPOLOGY_SPECS["homelab-ops"])

        # 2. Compute / Heavy Workloads -> Desktop PC
        compute_keywords = [
            "gpu", "cuda", "render", "train", "model", "benchmark", "heavy",
            "desktop-pc", "windows", "powershell", "pc"
        ]
        if any(re.search(r"\b" + re.escape(kw) + r"\b", text) for kw in compute_keywords):
            return self.topology.get("compute-grid", TOPOLOGY_SPECS["compute-grid"])

        # 3. Cloud Services -> OCI Machine (requires explicit OCI/prod/ingress/hosting guardrail)
        if has_oci_explicit:
            cloud_keywords = [
                "oracle", "oci", "cloud", "terraform", "vps", "ingress", "wireguard",
                "oci-machine", "public ip", "production", "prod", "hosting"
            ]
            if any(re.search(r"\b" + re.escape(kw) + r"\b", text) for kw in cloud_keywords):
                return self.topology.get("cloud-ops", TOPOLOGY_SPECS["cloud-ops"])

        # 4. Personal Laptop (dev-grid) -> STRICT AD-HOC ONLY
        # "This is where Carl is actively working; do not mess with him." It sleeps frequently.
        # NEVER route generic dev tasks or background builds here. Requires explicit laptop intent.
        laptop_explicit_patterns = [
            r"\b(?:on\s+(?:my\s+)?)?laptop\b",
            r"\bpersonal-laptop\b"
        ]
        if any(re.search(pat, text) for pat in laptop_explicit_patterns):
            return self.topology.get("dev-grid", TOPOLOGY_SPECS["dev-grid"])

        # Default / Primary Dev Anchor -> fleet-control (localhost / Users/hermes)
        # All software development, git repositories, branches, commits, pulls, pushes,
        # code refactoring, test suites, and general orchestration run here.
        return self.topology.get("fleet-control", TOPOLOGY_SPECS["fleet-control"])

    def resolve_target(self, target_input: str) -> Optional[Dict[str, Any]]:
        """
        Resolve user-supplied target (e.g. from 'switch to <target>') against
        topology workspaces, fleet machine labels, or alias names.
        """
        raw = target_input.strip().lower()

        # Direct workspace label match in active topology
        for ws_name, info in self.topology.items():
            if raw == ws_name.lower():
                return info

        # Direct workspace label match in TOPOLOGY_SPECS
        for ws_name, spec in TOPOLOGY_SPECS.items():
            if raw == ws_name.lower():
                return self.topology.get(ws_name, spec)

        # Alias / keyword mappings
        aliases = {
            "chief": "fleet-control",
            "chief-of-staff": "fleet-control",
            "agy": "fleet-control",
            "antigravity": "fleet-control",
            "local": "fleet-control",

            "hermes": "homelab-ops",
            "butler": "homelab-ops",
            "homelab": "homelab-ops",

            "oci": "cloud-ops",
            "oracle": "cloud-ops",
            "oci-machine": "cloud-ops",
            "cloud": "cloud-ops",

            "laptop": "dev-grid",
            "personal-laptop": "dev-grid",
            "dev": "dev-grid",

            "pc": "compute-grid",
            "desktop": "compute-grid",
            "desktop-pc": "compute-grid",
            "compute": "compute-grid"
        }
        if raw in aliases:
            ws_key = aliases[raw]
            return self.topology.get(ws_key, TOPOLOGY_SPECS.get(ws_key))

        # Match against registered Herdr machines
        for m in self.machines:
            label = m.get("label", "").lower()
            target = m.get("target", "").lower()
            if raw in (label, target):
                for ws_name, spec in TOPOLOGY_SPECS.items():
                    if spec.get("target_machine", "").lower() in (label, target):
                        return self.topology.get(ws_name, spec)

        # Fallback machine match against TOPOLOGY_SPECS target_machine
        for ws_name, spec in TOPOLOGY_SPECS.items():
            tm = spec.get("target_machine", "").lower()
            if raw == tm:
                return self.topology.get(ws_name, spec)

        return None

    async def resolve_pane_id(self, target_info: Dict[str, Any]) -> str:
        """Resolve the active pane_id, falling back to dynamic lookup by pane_label or workspace_label."""
        pane_id = target_info.get("pane_id")
        pane_label = target_info.get("pane_label")
        ws_label = target_info.get("workspace_label")

        panes = await self.get_panes()
        if pane_id and any(p.get("pane_id") == pane_id for p in panes):
            return pane_id

        # Lookup by pane_label
        if pane_label:
            for p in panes:
                if p.get("label") == pane_label:
                    target_info["pane_id"] = p.get("pane_id")
                    return p.get("pane_id")

        # Lookup by workspace_label
        if ws_label:
            workspaces = await self.get_workspaces()
            ws_ids = [w.get("workspace_id") for w in workspaces if w.get("label") == ws_label]
            for p in panes:
                if p.get("workspace_id") in ws_ids:
                    target_info["pane_id"] = p.get("pane_id")
                    return p.get("pane_id")

        return pane_id or "w1:p1"

    async def execute_task(
        self,
        target_info: Dict[str, Any],
        instruction: str,
        task_id: Optional[str] = None,
        cwd: Optional[str] = None,
        timeout: float = 120.0
    ) -> Dict[str, Any]:
        """
        Headless task execution across fleet nodes:
        - Deterministic commands run headlessly with clean stdout/stderr captured.
        - Run outputs logged to /tmp/herdr_tasks/<task_id>.log.
        - Remote hosts (personal-laptop, desktop-pc, oci-machine) executed via non-interactive SSH.
        - desktop-pc commands wrapped with wrap_windows_command.
        - Returns structured dict with exit_code, stdout, stderr, output, and blocked state.
        """
        import uuid
        task_id = task_id or f"task_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        tasks_dir = "/tmp/herdr_tasks"
        os.makedirs(tasks_dir, exist_ok=True)
        log_path = os.path.join(tasks_dir, f"{task_id}.log")

        target_machine = target_info.get("target_machine", "local")
        ssh_target = target_info.get("ssh_target") or (None if target_machine == "local" else target_machine)
        ws_label = target_info.get("workspace_label", "fleet-control")
        role = target_info.get("role", "Chief of Staff")
        agent_name = target_info.get("agent_name")
        agent_kind = target_info.get("agent_kind")
        effective_cwd = cwd or target_info.get("default_cwd", "~")
        if target_machine == "local":
            effective_cwd = os.path.expanduser(effective_cwd)

        clean_instruction = instruction.strip()
        # Prompt Dispatch Hygiene: strip [User Message] or context headers if present
        if "[User Message]\n" in clean_instruction:
            clean_instruction = clean_instruction.split("[User Message]\n", 1)[1].strip()

        logger.info(f"Executing headless task {task_id} on {ws_label} ({role}, target: {target_machine})...")

        stdout_text = ""
        stderr_text = ""
        exit_code = 0
        blocked = False
        blocked_prompt = None

        try:
            if target_machine == "local":
                # Local execution in specified working directory
                proc = await asyncio.create_subprocess_shell(
                    clean_instruction,
                    cwd=effective_cwd if os.path.isdir(effective_cwd) else "/Users/hermes",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                exit_code = proc.returncode if proc.returncode is not None else 0
                stdout_text = stdout_bytes.decode("utf-8", errors="replace").strip()
                stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()
            else:
                # Remote execution via SSH
                remote_cmd = clean_instruction
                if target_machine == "desktop-pc" or ws_label == "compute-grid":
                    remote_cmd = wrap_windows_command(remote_cmd)

                ssh_args = [
                    "ssh",
                    "-o", "BatchMode=yes",
                    "-o", "ConnectTimeout=8",
                    "-T",
                    ssh_target,
                    remote_cmd
                ]
                proc = await asyncio.create_subprocess_exec(
                    *ssh_args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                exit_code = proc.returncode if proc.returncode is not None else 0
                stdout_text = stdout_bytes.decode("utf-8", errors="replace").strip()
                stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()

        except asyncio.TimeoutError:
            exit_code = 124
            stderr_text = f"Task timed out after {timeout} seconds."
            logger.warning(f"Headless task {task_id} on {ws_label} timed out after {timeout}s")
        except Exception as e:
            exit_code = 1
            stderr_text = str(e)
            logger.error(f"Error executing headless task {task_id} on {ws_label}: {e}")

        combined = stdout_text
        if stderr_text:
            if combined:
                combined = f"{combined}\n\n[STDERR]\n{stderr_text}"
            else:
                combined = stderr_text

        # Write to log file for forensic auditing
        try:
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"Task ID: {task_id}\n")
                f.write(f"Workspace: {ws_label} ({role})\n")
                f.write(f"Target Machine: {target_machine}\n")
                f.write(f"Instruction: {clean_instruction}\n")
                f.write(f"Exit Code: {exit_code}\n")
                f.write("=" * 60 + "\n")
                f.write("STDOUT:\n" + stdout_text + "\n")
                f.write("=" * 60 + "\n")
                f.write("STDERR:\n" + stderr_text + "\n")
        except Exception as log_err:
            logger.warning(f"Could not write log file {log_path}: {log_err}")

        # Check for blocked condition in output (e.g. interactive prompt)
        blocked_patterns = [
            r"\[y/n\]", r"\(y/n\)", r"\(yes/no\)", r"\[y/N\]", r"\[Y/n\]",
            r"password:", r"confirm\s*\?", r"approval\s+required", r"allow\s+this\s+action\?"
        ]
        for pat in blocked_patterns:
            for l in combined.splitlines():
                if re.search(pat, l, re.IGNORECASE):
                    blocked = True
                    blocked_prompt = l.strip()
                    break
            if blocked:
                break

        return {
            "task_id": task_id,
            "exit_code": exit_code,
            "stdout": stdout_text,
            "stderr": stderr_text,
            "output": combined,
            "log_file": log_path,
            "target_machine": target_machine,
            "workspace_label": ws_label,
            "role": role,
            "blocked": blocked,
            "blocked_prompt": blocked_prompt
        }

    async def send_prompt(self, target_info: Dict[str, Any], prompt: str) -> None:
        """Submit a prompt into the target pane/agent with guaranteed newline execution."""
        pane_id = await self.resolve_pane_id(target_info)
        agent_name = target_info.get("agent_name")
        agent_kind = target_info.get("agent_kind")
        target_machine = target_info.get("target_machine", "")
        ws_label = target_info.get("workspace_label", "")

        clean_prompt = prompt.replace("\r", "").strip()

        # Prompt Dispatch Hygiene for interactive CLI agents (hermes-worker) and raw shell panes:
        # Do not dump multi-paragraph system prompts with mode boundary text directly into interactive PTYs.
        # Send only the clean user instruction and essential context so the CLI agent can parse it cleanly.
        if (not agent_name or agent_name == "hermes-worker" or agent_kind == "hermes") and "[User Message]\n" in clean_prompt:
            context_prefix = ""
            if "• User Uploaded Files" in clean_prompt:
                files_block = clean_prompt.split("• User Uploaded Files (stored in 'slack_transfers/'):\n", 1)[1]
                files_text = files_block.split("•", 1)[0].split("[User Message]", 1)[0].strip()
                file_lines = [l.strip() for l in files_text.splitlines() if l.strip().startswith("• `")]
                if file_lines:
                    context_prefix = f"[Uploaded files: {', '.join(file_lines)}]\n\n"
            clean_instruction = clean_prompt.split("[User Message]\n", 1)[1].strip()
            clean_prompt = f"{context_prefix}{clean_instruction}".strip()

        # Ensure Windows execution compatibility on desktop-pc / compute-grid
        if target_machine == "desktop-pc" or ws_label == "compute-grid":
            clean_prompt = wrap_windows_command(clean_prompt)

        # If it's chief-of-staff (native AGY agent), try 'agent prompt' first
        if agent_name == "chief-of-staff":
            code, _, _ = await self.run_cli(["agent", "prompt", agent_name, clean_prompt, "--timeout", "5000"])
            if code == 0:
                return

        # Otherwise send directly to pane stdin with trailing newline and Enter key, protected by per-pane I/O lock
        if pane_id:
            io_lock = self.get_pane_io_lock(pane_id)
            async with io_lock:
                # Ensure trailing newline is included in the PTY payload
                prompt_to_send = clean_prompt.rstrip("\r\n") + "\n"
                await self.run_cli(["pane", "send-text", pane_id, prompt_to_send])
                await asyncio.sleep(0.15)
                await self.run_cli(["pane", "send-keys", pane_id, "enter"])

    async def send_input(self, pane_id: str, text: str) -> None:
        """Send interactive input or unblock text to pane."""
        io_lock = self.get_pane_io_lock(pane_id)
        async with io_lock:
            prompt_to_send = text.rstrip("\r\n") + "\n"
            await self.run_cli(["pane", "send-text", pane_id, prompt_to_send])
            await asyncio.sleep(0.15)
            await self.run_cli(["pane", "send-keys", pane_id, "enter"])

    async def read_pane(self, pane_id: str, lines: int = 12) -> str:
        """Read recent terminal output unwrapped."""
        code, out, _ = await self.run_cli(["pane", "read", pane_id, "--source", "recent-unwrapped", "--lines", str(lines)])
        if code == 0:
            return out
        return ""

    async def get_pane_status(self, pane_id: str, agent_name: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """
        Check agent/pane status. Returns (status, prompt_or_detail)
        status in ("working", "blocked", "idle", "done").
        """
        # 1. Check Herdr's built-in agent status if applicable
        if agent_name:
            code, out, _ = await self.run_cli(["agent", "get", agent_name], timeout=3.0)
            if code == 0 and out:
                try:
                    data = json.loads(out)
                    agent_st = data.get("result", {}).get("agent", {}).get("agent_status")
                    if agent_st == "blocked":
                        return "blocked", "Agent is waiting for user confirmation or credentials."
                    if agent_st in ("idle", "done"):
                        return agent_st, None
                    if agent_st == "working":
                        return "working", None
                except Exception:
                    pass

        # 2. Check Herdr pane process-info and pane terminal output
        recent_text = await self.read_pane(pane_id, lines=15)
        clean_lines = [re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", line).strip() for line in recent_text.splitlines() if line.strip()]

        # Check for interactive blocked conditions
        blocked_patterns = [
            r"\[y/n\]", r"\(y/n\)", r"\(yes/no\)", r"\[y/N\]", r"\[Y/n\]",
            r"password:", r"confirm\s*\?", r"approval\s+required", r"allow\s+this\s+action\?",
            r"press\s+enter\s+to\s+continue", r"continue\s*\[\s*y/n\s*\]"
        ]
        for pat in blocked_patterns:
            for l in reversed(clean_lines[-6:]):
                if re.search(pat, l, re.IGNORECASE):
                    return "blocked", l

        # Check for working indicators:
        # Hermes explicitly displays "msg=interrupt", "Ctrl+C cancel", "computing...", "ruminating...",
        # or active spinners/progress when busy executing.
        working_indicators = [
            "msg=interrupt", "Ctrl+C cancel", "computing...", "ruminating...",
            "thinking...", "formulating...", "executing", "progress",
            "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", "calling tool"
        ]
        if any(ind in recent_text for ind in working_indicators):
            return "working", None

        # Filter out pure border/divider lines to inspect actual content and prompt lines
        box_chars = set("─━│┃┄┅┆┇┈┉┊┋┌┍┎┏┐┑┒┓└┕┖┗┘┙┚┛├┝┞┟┠┡┢┣┤┥┦┧┨┩┪┫┬┭┮┯┰┱┲┳┴┵┶┷┸┹┺┻┼┽┾┿╀╁╂╃╄╅╆╇╈╉╊╋═║╒╓╔╕╖╗╘╙╚╛╜╝╞╟╠╡╢╣╤╥╦╧╨╩╪╫╬╭╮╯╰╱╲╳")
        non_border_lines = []
        for l in clean_lines:
            if not all(c in box_chars or c in "-=~*# " for c in l):
                non_border_lines.append(l)

        # Check for idle prompts:
        # Hermes displays ❯ prompt (with optional placeholder suggestion), AGY displays >, shells display %/$
        idle_prompt_patterns = [
            re.compile(r"^(?:☤\s*)?❯(?:\s+.*)?$"),  # Hermes (e.g. ❯ or ❯ Draft a reply...)
            re.compile(r"^>\s*.*$"),                 # AGY
            re.compile(r"^.*[%$#]\s*$"),              # zsh / bash
            re.compile(r"PS\s+[A-Za-z0-9\\_:-]+>\s*") # PowerShell
        ]
        for l in reversed(non_border_lines[-4:]):
            if any(p.search(l) for p in idle_prompt_patterns):
                return "idle", None

        # If not actively showing working indicators, default to idle
        return "idle", None

    async def probe_node(self, node: str, timeout: float = 8.0) -> Dict[str, Any]:
        """
        Probe a fleet node with OS & architecture awareness.
        - hermes: Local df and docker metrics without SSH.
        - desktop-pc: Windows CUDA node via nvidia-smi with cmd.exe fallback.
        - personal-laptop: macOS workstation via uptime + pmset battery check.
        - oci-machine: Linux VPS uptime with 8.0s timeout & transient retry.
        """
        if node in ("hermes", "localhost", "local"):
            disk_desc = "unknown disk"
            try:
                proc = await asyncio.create_subprocess_exec(
                    "df", "-h", "/",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                lines = stdout.decode("utf-8", errors="replace").strip().splitlines()
                if len(lines) >= 2:
                    parts = lines[1].split()
                    if len(parts) >= 4:
                        disk_desc = f"{parts[3]} free"
            except Exception as e:
                disk_desc = f"disk err: {e}"

            container_count = 0
            try:
                proc = await asyncio.create_subprocess_exec(
                    "docker", "ps", "--format", "{{.Names}}",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                containers = [l for l in stdout.decode("utf-8", errors="replace").strip().splitlines() if l.strip()]
                container_count = len(containers)
            except Exception:
                container_count = 0

            return {
                "node": "hermes",
                "role": "localhost (Primary Dev & Homelab)",
                "status": f"Online (root disk {disk_desc}, {container_count} containers)",
                "online": True
            }

        elif node in ("desktop-pc", "compute-grid", "pc"):
            ssh_target = "desktop-pc"
            # 1. Try nvidia-smi
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-T",
                    ssh_target,
                    "nvidia-smi --query-gpu=name,utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                if proc.returncode == 0 and stdout:
                    out_str = stdout.decode("utf-8", errors="replace").strip()
                    lines = [l.strip() for l in out_str.splitlines() if l.strip()]
                    if lines:
                        parts = [p.strip() for p in lines[0].split(",")]
                        if len(parts) >= 4:
                            raw_name, util, used, total = parts[0], parts[1], parts[2], parts[3]
                            gpu_name = raw_name.replace("NVIDIA GeForce ", "").replace("NVIDIA ", "").strip()
                            return {
                                "node": "desktop-pc",
                                "role": "Compute Grid (Windows CUDA)",
                                "status": f"Online • {gpu_name} ({util}% GPU, {used}/{total} MiB VRAM)",
                                "online": True
                            }
            except Exception:
                pass

            # 2. Fallback to cmd.exe /c echo ok
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-T",
                    ssh_target,
                    "cmd.exe /c echo ok",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                if proc.returncode == 0 and stdout and b"ok" in stdout.lower():
                    return {
                        "node": "desktop-pc",
                        "role": "Compute Grid (Windows CUDA)",
                        "status": "Online (GPU Idle / Windows OK)",
                        "online": True
                    }
            except Exception:
                pass

            return {
                "node": "desktop-pc",
                "role": "Compute Grid (Windows CUDA)",
                "status": "[Asleep / Offline]",
                "online": False
            }

        elif node in ("personal-laptop", "dev-grid", "laptop"):
            ssh_target = "personal-laptop"
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-T",
                    ssh_target,
                    "uptime && pmset -g batt | head -n 1",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                if proc.returncode == 0 and stdout:
                    out_str = stdout.decode("utf-8", errors="replace").strip()
                    lines = [l.strip() for l in out_str.splitlines() if l.strip()]
                    if lines:
                        uptime_raw = lines[0]
                        m = re.search(r"(up\s+[^,]+(?:,\s*[^,]+)?)", uptime_raw)
                        uptime_val = m.group(1) if m else uptime_raw

                        batt_status = "AC / Battery"
                        if len(lines) >= 2:
                            batt_raw = lines[1]
                            if "drawing from" in batt_raw:
                                batt_status = batt_raw.split("drawing from", 1)[1].strip(" '\"")
                            else:
                                batt_status = batt_raw.strip(" '\"")

                        return {
                            "node": "personal-laptop",
                            "role": "Personal Laptop (Active Workstation)",
                            "status": f"Online ({uptime_val}) • {batt_status}",
                            "online": True
                        }
            except Exception:
                pass

            return {
                "node": "personal-laptop",
                "role": "Personal Laptop (Active Workstation)",
                "status": "Sleeping / Standby (Ad-hoc only)",
                "online": False
            }

        elif node in ("oci-machine", "cloud-ops", "oci"):
            ssh_target = "oci-machine"
            for attempt in range(2):
                try:
                    proc = await asyncio.create_subprocess_exec(
                        "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-T",
                        ssh_target, "uptime",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                    if proc.returncode == 0 and stdout:
                        uptime_str = stdout.decode("utf-8", errors="replace").strip()
                        m = re.search(r"(up\s+[^,]+(?:,\s*[^,]+)?)", uptime_str)
                        uptime_val = m.group(1) if m else uptime_str
                        return {
                            "node": "oci-machine",
                            "role": "Cloud Operations (Production VPS)",
                            "status": f"Online ({uptime_val})",
                            "online": True
                        }
                    if attempt == 0:
                        await asyncio.sleep(0.5)
                except Exception:
                    if attempt == 0:
                        await asyncio.sleep(0.5)

            return {
                "node": "oci-machine",
                "role": "Cloud Operations (Production VPS)",
                "status": "[Unreachable / Offline]",
                "online": False
            }

        else:
            ssh_target = node
            for attempt in range(2):
                try:
                    proc = await asyncio.create_subprocess_exec(
                        "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-T",
                        ssh_target, "uptime",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                    if proc.returncode == 0 and stdout:
                        uptime_str = stdout.decode("utf-8", errors="replace").strip()
                        return {
                            "node": node,
                            "role": "Remote Node",
                            "status": f"Online ({uptime_str})",
                            "online": True
                        }
                    if attempt == 0:
                        await asyncio.sleep(0.5)
                except Exception:
                    if attempt == 0:
                        await asyncio.sleep(0.5)

            return {
                "node": node,
                "role": "Remote Node",
                "status": "[Unreachable / Offline]",
                "online": False
            }

    async def update_node_agy(self, node: str, timeout: float = 6.0) -> Dict[str, Any]:
        """Check and apply agy CLI binary updates for a fleet node."""
        if node in ("hermes", "localhost", "local"):
            agy_bin = shutil.which("agy") or os.path.expanduser("~/.local/bin/agy")
            if not os.path.exists(agy_bin):
                return {"node": "localhost (hermes)", "status": "Binary not found", "version": "unknown", "updated": False}
            try:
                proc = await asyncio.create_subprocess_exec(
                    agy_bin, "update",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                out = (stdout or stderr).decode("utf-8", errors="replace").strip()
                m = re.search(r"version\s+([0-9.]+)", out)
                ver = m.group(1) if m else "1.3.1"
                if "already on the latest" in out.lower():
                    return {"node": "localhost (hermes)", "status": f"v{ver} (latest)", "version": ver, "updated": False}
                elif "update successful" in out.lower() or "updated" in out.lower():
                    return {"node": "localhost (hermes)", "status": f"v{ver} (updated)", "version": ver, "updated": True}
                return {"node": "localhost (hermes)", "status": f"v{ver}", "version": ver, "updated": False}
            except Exception as e:
                return {"node": "localhost (hermes)", "status": f"[Error: {e}]", "version": "unknown", "updated": False}

        ssh_target = node
        cmd = 'export PATH="$HOME/.local/bin:$PATH"; agy update'
        if node in ("desktop-pc", "compute-grid"):
            cmd = 'agy update'

        ssh_args = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=3",
            "-T",
            ssh_target,
            cmd
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *ssh_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            out = (stdout or stderr).decode("utf-8", errors="replace").strip()
            if proc.returncode == 0:
                m = re.search(r"version\s+([0-9.]+)", out)
                ver = m.group(1) if m else "1.3.1"
                if "already on the latest" in out.lower():
                    return {"node": node, "status": f"v{ver} (latest)", "version": ver, "updated": False}
                elif "update successful" in out.lower() or "updated" in out.lower():
                    return {"node": node, "status": f"v{ver} (updated)", "version": ver, "updated": True}
                return {"node": node, "status": f"v{ver}", "version": ver, "updated": False}
            else:
                return {"node": node, "status": f"[Failed: exit {proc.returncode}]", "version": "unknown", "updated": False}
        except asyncio.TimeoutError:
            return {"node": node, "status": "[Asleep / Offline]", "version": "unknown", "updated": False}
        except Exception as e:
            return {"node": node, "status": f"[Unreachable: {e}]", "version": "unknown", "updated": False}

    async def update_fleet_agy(self, nodes: Optional[List[str]] = None, timeout: float = 6.0) -> List[Dict[str, Any]]:
        """Concurrently check and update agy CLI binaries across all fleet nodes."""
        target_nodes = nodes or ["hermes", "personal-laptop", "oci-machine", "desktop-pc"]
        results = await asyncio.gather(
            *[self.update_node_agy(node, timeout=timeout) for node in target_nodes],
            return_exceptions=True
        )
        out = []
        for i, res in enumerate(results):
            if isinstance(res, dict):
                out.append(res)
            elif isinstance(res, Exception):
                out.append({"node": target_nodes[i], "status": f"[Error: {res}]", "version": "unknown", "updated": False})
            else:
                out.append({"node": target_nodes[i], "status": "[Unknown]", "version": "unknown", "updated": False})
        return out


