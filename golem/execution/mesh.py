#!/usr/bin/env python3
"""
golem.execution.mesh - Fleet topology & per-pane locking integration.
Manages 5-node fleet topology:
- fleet-control: Antigravity Chief of Staff & Primary Dev Anchor (localhost)
- homelab-ops: Hermes Butler (localhost)
- compute-grid: Compute Grid / GPU workloads (desktop-pc)
- dev-grid: Carl's Active Workstation (personal-laptop - strict ad-hoc)
- cloud-ops: Cloud Operations (oci-machine)
Provides per-pane asyncio.Lock to prevent overlapping commands in the same pane,
and wrap_windows_command helper for compute-grid execution.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import re
import inspect
import asyncio
import logging
from typing import Dict, Any, Optional, List, Callable, Awaitable

logger = logging.getLogger("Golem.Execution.Mesh")

TOPOLOGY_SPECS: Dict[str, Dict[str, Any]] = {
    "fleet-control": {
        "label": "fleet-control",
        "workspace_label": "fleet-control",
        "pane_label": "chief-of-staff",
        "pane_id": "w1:p1",
        "agent_name": "chief-of-staff",
        "agent_kind": "agy",
        "role": "Antigravity Chief of Staff & Primary Dev (localhost)",
        "target_machine": "local",
        "default_cwd": "/Users/hermes"
    },
    "homelab-ops": {
        "label": "homelab-ops",
        "workspace_label": "homelab-ops",
        "pane_label": "hermes-worker",
        "pane_id": "w2:p1",
        "agent_name": "hermes-worker",
        "agent_kind": "hermes",
        "role": "Hermes Butler",
        "target_machine": "local",
        "default_cwd": "/Users/hermes"
    },
    "cloud-ops": {
        "label": "cloud-ops",
        "workspace_label": "cloud-ops",
        "pane_label": "oci-worker",
        "pane_id": "w3:p1",
        "agent_name": None,
        "agent_kind": None,
        "role": "Cloud Operations (OCI)",
        "target_machine": "oci-machine",
        "ssh_target": "oci-machine",
        "default_cwd": "~"
    },
    "dev-grid": {
        "label": "dev-grid",
        "workspace_label": "dev-grid",
        "pane_label": "laptop-worker",
        "pane_id": "w4:p1",
        "agent_name": None,
        "agent_kind": None,
        "role": "Personal Laptop (Active Workstation - Strict Ad-Hoc)",
        "target_machine": "personal-laptop",
        "ssh_target": "personal-laptop",
        "default_cwd": "~"
    },
    "compute-grid": {
        "label": "compute-grid",
        "workspace_label": "compute-grid",
        "pane_label": "pc-worker",
        "pane_id": "w5:p1",
        "agent_name": None,
        "agent_kind": None,
        "role": "Compute Grid (Desktop PC)",
        "target_machine": "desktop-pc",
        "ssh_target": "desktop-pc",
        "default_cwd": "~"
    }
}

NODE_ALIASES: Dict[str, str] = {
    "hermes": "homelab-ops",
    "homelab": "homelab-ops",
    "local": "fleet-control",
    "localhost": "fleet-control",
    "desktop-pc": "compute-grid",
    "pc": "compute-grid",
    "gpu": "compute-grid",
    "personal-laptop": "dev-grid",
    "laptop": "dev-grid",
    "oci-machine": "cloud-ops",
    "oci": "cloud-ops",
    "cloud": "cloud-ops"
}


def wrap_windows_command(cmd: str) -> str:
    """
    Ensure no hardcoded POSIX idioms (such as &&, Unix path separators, or bash-specific subshells)
    are sent to the Windows worker pane unless wrapped in an explicit PowerShell or shell runner.
    """
    stripped = cmd.strip()
    if not stripped:
        return stripped

    # If already wrapped in an explicit runner, preserve as-is
    if re.match(r"^(powershell(?:\.exe)?|cmd(?:\.exe)?|pwsh(?:\.exe)?)\b", stripped, re.IGNORECASE):
        return stripped

    posix_patterns = [
        r"&&", r"\|\|", r"2>/dev/null", r"/dev/null",
        r"\$\([^\)]+\)", r"`[^`]+`", r"(?:^|\s)\./", r"(?:^|\s)/(?:tmp|var|etc|usr|bin)\b"
    ]
    has_posix = any(re.search(pat, stripped) for pat in posix_patterns)

    if has_posix:
        # Wrap in cmd.exe /c runner which supports &&, ||, and standard Windows execution
        cmd_fixed = re.sub(r"2>/dev/null", "2>nul", stripped)
        cmd_fixed = re.sub(r"/dev/null", "nul", cmd_fixed)
        cmd_fixed = re.sub(r"(?<=[\s\"'])\./", r".\\", cmd_fixed)
        if cmd_fixed.startswith("./"):
            cmd_fixed = ".\\" + cmd_fixed[2:]
        return f'cmd.exe /c "{cmd_fixed}"'

    return stripped


class FleetMesh:
    """
    Manages fleet node topology, pane isolation, and per-pane concurrency control.
    Enforces per-pane asyncio.Lock to prevent overlapping/interleaved commands on the same pane.
    """

    def __init__(
        self,
        topology: Optional[Dict[str, Dict[str, Any]]] = None,
        herdr_client: Optional[Any] = None
    ):
        self.topology = topology or dict(TOPOLOGY_SPECS)
        self.herdr_client = herdr_client
        self._pane_locks: Dict[str, asyncio.Lock] = {}
        self._pane_io_locks: Dict[str, asyncio.Lock] = {}

    def _canonicalize_pane_id(self, pane_or_node: str) -> str:
        """Resolve node label or alias to its canonical pane_id if applicable."""
        clean = pane_or_node.strip()
        # Direct check in topology
        if clean.lower() in self.topology:
            return self.topology[clean.lower()].get("pane_id", clean)
        # Check alias
        if clean.lower() in NODE_ALIASES:
            alias_key = NODE_ALIASES[clean.lower()]
            if alias_key in self.topology:
                return self.topology[alias_key].get("pane_id", clean)
        # Check by label or pane_label in topology
        for node_info in self.topology.values():
            if node_info.get("label", "").lower() == clean.lower() or node_info.get("pane_label", "").lower() == clean.lower():
                return node_info.get("pane_id", clean)
        return clean

    def get_pane_lock(self, pane_id: str) -> asyncio.Lock:
        """Get or create an execution lock for the given pane ID."""
        canonical_id = self._canonicalize_pane_id(pane_id)
        if canonical_id not in self._pane_locks:
            self._pane_locks[canonical_id] = asyncio.Lock()
        return self._pane_locks[canonical_id]

    def get_pane_io_lock(self, pane_id: str) -> asyncio.Lock:
        """Get or create an I/O lock for atomic send-text/send-keys sequences on a pane."""
        canonical_id = self._canonicalize_pane_id(pane_id)
        if canonical_id not in self._pane_io_locks:
            self._pane_io_locks[canonical_id] = asyncio.Lock()
        return self._pane_io_locks[canonical_id]

    def get_all_locks(self) -> Dict[str, asyncio.Lock]:
        """Return dict of all active per-pane locks."""
        return self._pane_locks

    def has_active_locks(self) -> List[str]:
        """Return list of pane IDs currently holding an active lock."""
        return [pane_id for pane_id, lock in self._pane_locks.items() if lock.locked()]

    def resolve_node(self, node_identifier: str) -> Dict[str, Any]:
        """
        Resolve a node name, machine hostname, or alias to its topology configuration.
        """
        clean_name = node_identifier.strip().lower()

        # Direct topology key
        if clean_name in self.topology:
            return self.topology[clean_name]

        # Alias lookup
        if clean_name in NODE_ALIASES:
            alias_key = NODE_ALIASES[clean_name]
            if alias_key in self.topology:
                return self.topology[alias_key]

        # Match by target_machine, ssh_target, or label
        for node_info in self.topology.values():
            if (
                node_info.get("target_machine", "").lower() == clean_name
                or node_info.get("ssh_target", "").lower() == clean_name
                or node_info.get("label", "").lower() == clean_name
            ):
                return node_info

        raise KeyError(f"Fleet node '{node_identifier}' not found in topology.")

    def list_nodes(self) -> List[str]:
        """Return all primary node names in the fleet."""
        return list(self.topology.keys())

    async def run_locked(
        self,
        pane_id: str,
        coro_or_func: Callable[..., Any],
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """
        Execute an operation while holding the pane's lock.
        Guarantees that no concurrent command runs in the same pane simultaneously.
        """
        lock = self.get_pane_lock(pane_id)
        async with lock:
            if inspect.iscoroutinefunction(coro_or_func):
                return await coro_or_func(*args, **kwargs)
            res = coro_or_func(*args, **kwargs)
            if asyncio.iscoroutine(res):
                return await res
            return res
