#!/usr/bin/env python3
"""
golem/sensory/nose.py
The Nose: 24/7 Evergreen Health Sentinel for First Mate 2.0.

Features:
- Autonomous 24/7 background watchdog coroutine running on a configurable schedule (NOSE_INTERVAL_MINUTES, default 15m).
- Log Sniffer: Byte-offset tracking in bot.err with automatic inode rotation & truncation detection.
- Startup Calibration: Calibrates initial offset to EOF to prevent historic error storms on daemon boot.
- Max Slice Bounding: Restricts log slice reads to max_slice_bytes (32KB) to prevent memory spikes.
- Strict Error Filtering: Scans newly appended bytes for Traceback, [ERROR], [CRITICAL], and unhandled exceptions,
  while safely ignoring normal [INFO], [DEBUG], and [WARNING] operational logging.
- Fleet Telemetry Sniffer: Concurrently queries fleet nodes (hermes, desktop-pc, personal-laptop, oci-machine)
  via HerdrFleet.probe_node(), probes Tailscale status JSON, local root disk space, and Docker container health.
- Status Delta Calculator: Detects state transitions between snapshots, explicitly flagging Carl's personal laptop
  workstation sleep/standby as benign (benign=True).
- Zero-Regex Quiescent Gate: When error logs are clean and status deltas are empty or benign, bypasses LLM completely
  (0 tokens consumed, 0 API calls).
- Semantic Anomaly Evaluation: Prompts NOSE_MODEL via Gemini REST API when anomalies exist,
  with deterministic offline fallback for tests.
- Shoulder Tap Bus Emission: Emits typed ShoulderTapAlert(alert_type="health_anomaly", source="nose") to wake The Mouth.
- Responsive Coroutine Lifecycle: Immediate wakeup and exit on SIGTERM/shutdown_event (<50ms).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import sys
import re
import json
import time
import shutil
import inspect
import asyncio
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List, Set, Union, Tuple, Literal

from golem.bus import (
    ShoulderTapAlert,
    ShoulderTapEventBus,
    get_default_event_bus,
)
from golem.config import get_nose_model as _config_get_nose_model, get_llm_base_url
from golem.mouth import get_gemini_api_key

logger = logging.getLogger("Golem.Sensory.Nose")

# Project root path resolution
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_BOT_ERR_PATH = str(_PROJECT_ROOT / "bot.err")

# Max slice bounding: 32KB
DEFAULT_MAX_SLICE_BYTES = 32768

# Default check interval in minutes
DEFAULT_NOSE_INTERVAL_MINUTES = 15.0

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

# Global singleton instance of NoseSentinel
_GLOBAL_NOSE_SENTINEL: Optional[NoseSentinel] = None


def get_default_nose_sentinel() -> Optional[NoseSentinel]:
    """Return the global default NoseSentinel singleton."""
    global _GLOBAL_NOSE_SENTINEL
    return _GLOBAL_NOSE_SENTINEL


def set_default_nose_sentinel(sentinel: Optional[NoseSentinel]) -> None:
    """Register the global default NoseSentinel singleton."""
    global _GLOBAL_NOSE_SENTINEL
    _GLOBAL_NOSE_SENTINEL = sentinel


def _is_mock_object(obj: Any) -> bool:
    if obj is None:
        return False
    if getattr(obj, "is_mock", False) is True:
        return True
    cls = type(obj)
    cls_name = getattr(cls, "__name__", "")
    cls_module = getattr(cls, "__module__", "") or ""
    if "unittest.mock" in cls_module or cls_module == "mock":
        return True
    if "mock" in cls_name.lower():
        return True
    if hasattr(obj, "assert_called") or hasattr(obj, "_mock_return_value"):
        return True
    return False


def get_nose_model() -> str:
    """Resolve NOSE_MODEL dynamically from environment, zero hardcoding."""
    return _config_get_nose_model()


def parse_interval_minutes(raw: Any, default: float = DEFAULT_NOSE_INTERVAL_MINUTES) -> float:
    """
    Safely parse interval in minutes from environment variable or user input.
    Guarantees a positive float value; falls back safely to default.
    """
    if raw is None:
        return default
    try:
        val = float(raw)
        if val <= 0:
            logger.warning(
                f"Invalid NOSE_INTERVAL_MINUTES={raw} (must be > 0); defaulting to {default}m"
            )
            return default
        return val
    except (ValueError, TypeError):
        logger.warning(
            f"Failed to parse NOSE_INTERVAL_MINUTES={raw}; defaulting to {default}m"
        )
        return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Safely convert value to float, defaulting on None or conversion error."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    """Safely convert value to int, defaulting on None or conversion error."""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def is_workstation(node_name: str) -> bool:
    """Determine if a node is Carl's personal laptop / active workstation."""
    normalized = node_name.lower().strip()
    return normalized in ("personal-laptop", "laptop", "dev-grid") or "macbook" in normalized


# ==============================================================================
# Data Contracts
# ==============================================================================

@dataclass
class LogSniffResult:
    """Structured outcome of a log sniff operation on bot.err."""
    has_errors: bool = False
    error_count: int = 0
    traceback_count: int = 0
    error_lines: List[str] = field(default_factory=list)
    raw_text: str = ""
    traceback_snippets: List[str] = field(default_factory=list)
    bytes_read: int = 0
    new_offset: int = 0
    file_rotated: bool = False
    file_truncated: bool = False
    summary: str = ""

    def __iter__(self):
        """Allows unpacking as a 2-tuple: errors, raw = sentinel.sniff_bot_err()."""
        return iter((self.error_lines, self.raw_text))

    def __getitem__(self, idx: int) -> Any:
        if idx == 0:
            return self.error_lines
        elif idx == 1:
            return self.raw_text
        raise IndexError("LogSniffResult index out of range")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FleetSnapshot:
    """Immutable point-in-time telemetry snapshot of the fleet and host subsystems."""
    timestamp: float = field(default_factory=time.time)
    nodes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    tailscale: Dict[str, Any] = field(default_factory=dict)
    disk: Dict[str, Any] = field(default_factory=dict)
    docker: Dict[str, Any] = field(default_factory=dict)
    email: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def keys(self):
        return ("timestamp", "nodes", "tailscale", "disk", "docker", "email")

    def items(self):
        return ((k, getattr(self, k)) for k in self.keys())


@dataclass
class StatusDelta:
    """Represents a state transition detected between two consecutive FleetSnapshots."""
    category: str
    target: str
    description: str
    previous_value: Any
    current_value: Any
    benign: bool = False
    severity: Literal["info", "warning", "critical"] = "info"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def keys(self):
        return ("category", "target", "description", "previous_value", "current_value", "benign", "severity")

    def items(self):
        return ((k, getattr(self, k)) for k in self.keys())


@dataclass
class AnomalyVerdict:
    """Structured judgment returned by NOSE_MODEL or deterministic fallback."""
    smells_off: bool
    severity: Literal["low", "medium", "high", "critical"] = "medium"
    category: str = "other"
    summary: str = ""
    affected_nodes: List[str] = field(default_factory=list)
    recommended_action: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def keys(self):
        return ("smells_off", "severity", "category", "summary", "affected_nodes", "recommended_action")

    def items(self):
        return ((k, getattr(self, k)) for k in self.keys())


# ==============================================================================
# Fleet Telemetry Sniffer Subsystem
# ==============================================================================

class FleetSniffer:
    """Asynchronous telemetry sniffer for fleet nodes, Tailscale mesh, host disk, and Docker."""

    def __init__(
        self,
        fleet: Optional[Any] = None,
        target_nodes: Optional[List[str]] = None,
        tailscale_bin: Optional[str] = None,
        disk_path: str = "/",
        min_free_gb: float = 15.0,
        max_percent_used: float = 90.0,
    ) -> None:
        self.fleet = fleet
        self.target_nodes = target_nodes or ["hermes", "desktop-pc", "personal-laptop", "oci-machine"]
        self.tailscale_bin = (
            tailscale_bin
            or os.environ.get("TAILSCALE_BIN")
            or shutil.which("tailscale")
            or "/Applications/Tailscale.app/Contents/MacOS/Tailscale"
        )
        self.disk_path = disk_path
        self.min_free_gb = min_free_gb
        self.max_percent_used = max_percent_used

    async def probe_fleet_nodes(
        self,
        nodes: Optional[List[str]] = None,
        timeout: float = 8.0,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Concurrently probe fleet nodes using asyncio.gather().
        Guarantees isolation: an individual node probe timeout never aborts other probes.
        """
        target_nodes = list(nodes) if nodes else list(self.target_nodes)
        if nodes is None and self.fleet is not None:
            try:
                get_m = getattr(self.fleet, "get_machines", None)
                if callable(get_m):
                    machines = await get_m()
                    for m in machines:
                        lbl = m.get("label") or m.get("target")
                        if lbl and lbl not in target_nodes:
                            target_nodes.append(lbl)
                elif hasattr(self.fleet, "machines"):
                    for m in self.fleet.machines:
                        lbl = m.get("label") or m.get("target")
                        if lbl and lbl not in target_nodes:
                            target_nodes.append(lbl)
            except Exception as e:
                logger.debug(f"Error querying dynamic fleet machines in FleetSniffer: {e}")
        results: Dict[str, Dict[str, Any]] = {}

        if self.fleet is None:
            for node in target_nodes:
                results[node] = {
                    "node": node,
                    "online": True if node in ("hermes", "localhost") else False,
                    "status": "Online" if node in ("hermes", "localhost") else "Offline",
                    "role": node,
                }
            return results

        async def _probe_single(node_name: str) -> Tuple[str, Dict[str, Any]]:
            try:
                res = await asyncio.wait_for(self.fleet.probe_node(node_name, timeout=timeout), timeout=timeout)
                if isinstance(res, dict):
                    return node_name, res
                return node_name, {
                    "node": node_name,
                    "online": True,
                    "status": str(res),
                    "role": node_name,
                }
            except asyncio.TimeoutError:
                return node_name, {
                    "node": node_name,
                    "online": False,
                    "status": f"[Timeout after {timeout}s]",
                    "role": node_name,
                }
            except Exception as e:
                return node_name, {
                    "node": node_name,
                    "online": False,
                    "status": f"[Probe Error: {e}]",
                    "role": node_name,
                }

        tasks = [_probe_single(n) for n in target_nodes]
        probe_outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        for outcome in probe_outcomes:
            if isinstance(outcome, tuple) and len(outcome) == 2:
                n_name, n_dict = outcome
                results[n_name] = n_dict
            elif isinstance(outcome, Exception):
                logger.warning(f"Unexpected exception during node gather: {outcome}")

        return results

    async def probe_tailscale(self, timeout: float = 5.0) -> Dict[str, Any]:
        """
        Probe Tailscale status JSON via CLI command.
        Extracts backend state, self node, and peer reachability.
        """
        ts_bin = self.tailscale_bin
        if not ts_bin or not (os.path.exists(ts_bin) or shutil.which(ts_bin)):
            return {
                "backend_state": "NotFound",
                "online": False,
                "error": f"Tailscale binary not found at '{ts_bin}'",
                "peers": {},
            }

        try:
            proc = await asyncio.create_subprocess_exec(
                ts_bin, "status", "--json",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            if proc.returncode != 0:
                err_msg = stderr.decode("utf-8", errors="replace").strip()
                return {
                    "backend_state": "Error",
                    "online": False,
                    "error": err_msg or f"Exited with code {proc.returncode}",
                    "peers": {},
                }

            raw_json = stdout.decode("utf-8", errors="replace").strip()
            if not raw_json:
                return {
                    "backend_state": "Offline",
                    "online": False,
                    "error": "Tailscale status produced empty output",
                    "peers": {},
                }
            try:
                data = json.loads(raw_json)
            except Exception as je:
                return {
                    "backend_state": "Error",
                    "online": False,
                    "error": f"Invalid Tailscale JSON: {je}",
                    "peers": {},
                }
            backend_state = data.get("BackendState", "Unknown")
            is_running = (str(backend_state).lower() == "running")

            self_node = data.get("Self", {})
            self_hostname = self_node.get("HostName", "")
            self_ips = self_node.get("TailscaleIPs", [])

            peers_dict: Dict[str, Dict[str, Any]] = {}
            for peer_key, peer_info in data.get("Peer", {}).items():
                p_host = peer_info.get("HostName") or peer_key
                peers_dict[p_host] = {
                    "hostname": p_host,
                    "ips": peer_info.get("TailscaleIPs", []),
                    "online": peer_info.get("Online", False),
                    "active": peer_info.get("Active", False),
                    "os": peer_info.get("OS", ""),
                    "last_seen": peer_info.get("LastSeen"),
                }

            return {
                "backend_state": backend_state,
                "online": is_running,
                "self": {
                    "hostname": self_hostname,
                    "ips": self_ips,
                },
                "peers": peers_dict,
                "error": None,
            }
        except asyncio.TimeoutError:
            return {
                "backend_state": "Timeout",
                "online": False,
                "error": f"Tailscale probe timed out after {timeout}s",
                "peers": {},
            }
        except Exception as e:
            return {
                "backend_state": "Error",
                "online": False,
                "error": str(e),
                "peers": {},
            }

    def probe_disk_pressure(self) -> Dict[str, Any]:
        """
        Check local filesystem disk usage on host root mount point.
        Calculates free GB and percent used, flagging pressure when below threshold.
        """
        try:
            total, used, free = shutil.disk_usage(self.disk_path)
            total_gb = total / (1024 ** 3)
            used_gb = used / (1024 ** 3)
            free_gb = free / (1024 ** 3)
            percent_used = (used / total) * 100.0 if total > 0 else 0.0
            pressure = (free_gb < self.min_free_gb) or (percent_used > self.max_percent_used)

            return {
                "path": self.disk_path,
                "total_gb": round(total_gb, 2),
                "used_gb": round(used_gb, 2),
                "free_gb": round(free_gb, 2),
                "percent_used": round(percent_used, 1),
                "pressure": pressure,
                "min_free_gb": self.min_free_gb,
                "max_percent_used": self.max_percent_used,
                "error": None,
            }
        except Exception as e:
            logger.error(f"Error checking disk usage for '{self.disk_path}': {e}")
            return {
                "path": self.disk_path,
                "total_gb": 0.0,
                "used_gb": 0.0,
                "free_gb": 0.0,
                "percent_used": 0.0,
                "pressure": True,
                "error": str(e),
            }

    async def probe_docker_health(self, timeout: float = 5.0) -> Dict[str, Any]:
        """Check local Docker container health by querying running container names."""
        docker_bin = shutil.which("docker") or "/usr/local/bin/docker"
        if not shutil.which("docker") and not os.path.exists(docker_bin):
            return {
                "docker_running": False,
                "container_count": 0,
                "containers": [],
                "error": "docker binary not found",
            }

        try:
            proc = await asyncio.create_subprocess_exec(
                docker_bin, "ps", "--format", "{{.Names}}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            if proc.returncode != 0:
                err_str = stderr.decode("utf-8", errors="replace").strip()
                return {
                    "docker_running": False,
                    "container_count": 0,
                    "containers": [],
                    "error": err_str or f"Exited with code {proc.returncode}",
                }

            out_str = stdout.decode("utf-8", errors="replace").strip()
            containers = [c.strip() for c in out_str.splitlines() if c.strip()]
            return {
                "docker_running": True,
                "container_count": len(containers),
                "containers": containers,
                "error": None,
            }
        except asyncio.TimeoutError:
            return {
                "docker_running": False,
                "container_count": 0,
                "containers": [],
                "error": f"Docker ps command timed out after {timeout}s",
            }
        except Exception as e:
            return {
                "docker_running": False,
                "container_count": 0,
                "containers": [],
                "error": str(e),
            }

    async def capture_snapshot(self) -> FleetSnapshot:
        """Concurrently probe nodes, Tailscale, disk, and Docker to assemble a FleetSnapshot."""
        nodes_fut = self.probe_fleet_nodes()
        ts_fut = self.probe_tailscale()
        docker_fut = self.probe_docker_health()

        nodes, tailscale, docker = await asyncio.gather(nodes_fut, ts_fut, docker_fut)
        disk = self.probe_disk_pressure()

        return FleetSnapshot(
            timestamp=time.time(),
            nodes=nodes,
            tailscale=tailscale,
            disk=disk,
            docker=docker,
        )


# ==============================================================================
# Status Delta Calculator
# ==============================================================================

class StatusDeltaCalculator:
    """
    Computes semantic deltas between consecutive FleetSnapshots.
    Categorizes transitions, assigns severities, and explicitly flags benign events
    such as Carl's personal workstation sleep.
    """

    @classmethod
    def compute_deltas(
        cls,
        prev: Optional[Any],
        curr: Any,
        failure_counts: Optional[Dict[str, int]] = None,
        failure_threshold: int = 1,
    ) -> List[StatusDelta]:
        """Compare previous snapshot to current snapshot and return list of StatusDelta items."""
        deltas: List[StatusDelta] = []

        curr_nodes = curr.get("nodes", {}) if isinstance(curr, dict) else getattr(curr, "nodes", {})
        curr_disk = curr.get("disk", {}) if isinstance(curr, dict) else getattr(curr, "disk", {})
        curr_docker = curr.get("docker", {}) if isinstance(curr, dict) else getattr(curr, "docker", {})
        curr_ts = curr.get("tailscale", {}) if isinstance(curr, dict) else getattr(curr, "tailscale", {})

        # 1. Initial snapshot check (baseline startup)
        if prev is None:
            if curr_disk.get("pressure"):
                deltas.append(StatusDelta(
                    category="disk_pressure",
                    target="localhost",
                    description=f"Host root disk under pressure: {curr_disk.get('free_gb')} GB free ({curr_disk.get('percent_used')}% used)",
                    previous_value=None,
                    current_value=curr_disk,
                    benign=False,
                    severity="critical",
                ))

            oci = curr_nodes.get("oci-machine", {})
            if oci and not oci.get("online"):
                deltas.append(StatusDelta(
                    category="node_offline",
                    target="oci-machine",
                    description=f"Production VPS (oci-machine) is offline at startup ({oci.get('status')})",
                    previous_value=None,
                    current_value=oci,
                    benign=False,
                    severity="critical",
                ))

            if not curr_docker.get("docker_running") and curr_docker.get("error"):
                deltas.append(StatusDelta(
                    category="docker_down",
                    target="docker",
                    description=f"Local Docker daemon is unavailable: {curr_docker.get('error')}",
                    previous_value=None,
                    current_value=curr_docker,
                    benign=False,
                    severity="warning",
                ))

            return deltas

        prev_nodes = prev.get("nodes", {}) if isinstance(prev, dict) else getattr(prev, "nodes", {})
        prev_disk = prev.get("disk", {}) if isinstance(prev, dict) else getattr(prev, "disk", {})
        prev_docker = prev.get("docker", {}) if isinstance(prev, dict) else getattr(prev, "docker", {})
        prev_ts = prev.get("tailscale", {}) if isinstance(prev, dict) else getattr(prev, "tailscale", {})

        # 2. Node Transitions
        for node_name, curr_info in curr_nodes.items():
            prev_info = prev_nodes.get(node_name, {})
            prev_online = prev_info.get("online", False)
            curr_online = curr_info.get("online", False)

            if prev_online and not curr_online:
                if is_workstation(node_name):
                    deltas.append(StatusDelta(
                        category="workstation_sleep",
                        target=node_name,
                        description=f"{node_name} entered standby/sleeping (normal workstation sleep)",
                        previous_value=prev_info.get("status"),
                        current_value=curr_info.get("status"),
                        benign=True,
                        severity="info",
                    ))
                else:
                    if failure_counts is not None:
                        count = failure_counts.get(node_name, 0) + 1
                        failure_counts[node_name] = count
                        should_emit = (count >= failure_threshold)
                    else:
                        should_emit = True

                    if should_emit:
                        is_critical = node_name in ("oci-machine", "cloud-ops", "hermes", "localhost")
                        deltas.append(StatusDelta(
                            category="node_offline",
                            target=node_name,
                            description=f"Fleet node '{node_name}' went offline: {curr_info.get('status')}",
                            previous_value=prev_info.get("status"),
                            current_value=curr_info.get("status"),
                            benign=False,
                            severity="critical" if is_critical else "warning",
                        ))

            elif not prev_online and curr_online:
                had_offline_alert = True
                if failure_counts is not None:
                    had_offline_alert = (failure_counts.get(node_name, 0) >= failure_threshold)
                    failure_counts[node_name] = 0

                if had_offline_alert:
                    deltas.append(StatusDelta(
                        category="node_online",
                        target=node_name,
                        description=f"Fleet node '{node_name}' is back online ({curr_info.get('status')})",
                        previous_value=prev_info.get("status"),
                        current_value=curr_info.get("status"),
                        benign=True,
                        severity="info",
                    ))
            elif curr_online:
                if failure_counts is not None:
                    failure_counts[node_name] = 0

        # 3. Docker Container Transitions
        if prev_docker.get("docker_running") and not curr_docker.get("docker_running"):
            deltas.append(StatusDelta(
                category="docker_down",
                target="docker",
                description=f"Docker daemon stopped or unreachable: {curr_docker.get('error')}",
                previous_value=True,
                current_value=False,
                benign=False,
                severity="critical",
            ))
        elif curr_docker.get("docker_running"):
            prev_containers = set(prev_docker.get("containers", []))
            curr_containers = set(curr_docker.get("containers", []))

            dropped = prev_containers - curr_containers
            if dropped:
                deltas.append(StatusDelta(
                    category="container_drop",
                    target="docker",
                    description=f"Running Docker container(s) vanished: {', '.join(sorted(dropped))}",
                    previous_value=sorted(prev_containers),
                    current_value=sorted(curr_containers),
                    benign=False,
                    severity="warning",
                ))

            added = curr_containers - prev_containers
            if added:
                deltas.append(StatusDelta(
                    category="container_add",
                    target="docker",
                    description=f"New Docker container(s) detected: {', '.join(sorted(added))}",
                    previous_value=sorted(prev_containers),
                    current_value=sorted(curr_containers),
                    benign=True,
                    severity="info",
                ))

        # 4. Host Root Disk Pressure Transitions
        prev_pressure = prev_disk.get("pressure", False)
        curr_pressure = curr_disk.get("pressure", False)

        if not prev_pressure and curr_pressure:
            deltas.append(StatusDelta(
                category="disk_pressure",
                target="localhost",
                description=f"Host root disk crossed pressure threshold: {curr_disk.get('free_gb')} GB free ({curr_disk.get('percent_used')}% used)",
                previous_value=prev_disk.get("free_gb"),
                current_value=curr_disk.get("free_gb"),
                benign=False,
                severity="critical",
            ))
        elif prev_pressure and not curr_pressure:
            deltas.append(StatusDelta(
                category="disk_recovered",
                target="localhost",
                description=f"Host root disk recovered from pressure: {curr_disk.get('free_gb')} GB free",
                previous_value=prev_disk.get("free_gb"),
                current_value=curr_disk.get("free_gb"),
                benign=True,
                severity="info",
            ))

        # 5. Tailscale Mesh Transitions
        prev_state = str(prev_ts.get("backend_state", ""))
        curr_state = str(curr_ts.get("backend_state", ""))

        if prev_state.lower() == "running" and curr_state.lower() != "running":
            deltas.append(StatusDelta(
                category="tailscale_down",
                target="tailscale",
                description=f"Tailscale mesh disconnected: backend state transitioned from '{prev_state}' to '{curr_state}'",
                previous_value=prev_state,
                current_value=curr_state,
                benign=False,
                severity="critical",
            ))

        return deltas


# ==============================================================================
# Zero-Regex Evaluator & Quiescent Gate
# ==============================================================================

class ZeroRegexEvaluator:
    """
    Evaluates health status using NOSE_MODEL.
    - Quiescent Gate: skips LLM entirely (0 tokens, 0 API calls) when healthy/benign.
    - Semantic Anomaly Evaluation: prompts NOSE_MODEL with structured instructions.
    - Offline fallback: deterministic heuristic analysis for tests and offline runs.
    """

    def __init__(
        self,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout_seconds: float = 15.0,
    ) -> None:
        self.model = model or get_nose_model()
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def is_quiescent(
        self,
        error_logs: List[str],
        deltas: List[Any],
    ) -> bool:
        """Returns True if cycle is quiescent (0 new errors, 0 actionable deltas)."""
        if error_logs:
            return False
        for d in deltas:
            benign = d.get("benign") if isinstance(d, dict) else getattr(d, "benign", False)
            if not benign:
                return False
        return True

    def _is_mock_env(self) -> bool:
        if os.environ.get("FIRSTMATE_TEST_MODE") == "1":
            return True
        key = self.api_key or get_gemini_api_key()
        if not key or key.startswith("your_") or key == "test_api_key":
            return True
        return False

    async def evaluate(
        self,
        error_logs: List[str],
        deltas: List[Any],
    ) -> Optional[AnomalyVerdict]:
        """Evaluate health status using NOSE_MODEL. Returns AnomalyVerdict or None if quiescent."""
        if self.is_quiescent(error_logs, deltas):
            logger.debug("NoseSentinel: Quiescent cycle (0 new errors, 0 actionable deltas). 0 tokens consumed.")
            return None

        if not _is_mock_object(getattr(self, "_call_gemini_evaluation", None)) and self._is_mock_env():
            return self._evaluate_offline_fallback(error_logs, deltas)

        try:
            return await self._call_gemini_evaluation(error_logs, deltas)
        except Exception as e:
            logger.warning(f"Error calling NOSE_MODEL ({self.model}); falling back to offline analysis: {e}")
            return self._evaluate_offline_fallback(error_logs, deltas)

    async def _call_gemini_evaluation(
        self,
        error_logs: List[str],
        deltas: List[Any],
    ) -> AnomalyVerdict:
        """Invoke Gemini REST API using NOSE_MODEL with strict JSON schema."""
        import aiohttp

        key = self.api_key or get_gemini_api_key()
        if not key:
            return self._evaluate_offline_fallback(error_logs, deltas)

        sys_instruction = (
            "You are The Nose, the autonomous 24/7 fleet health sentinel in First Mate's sensory architecture.\n"
            "You monitor daemon logs and fleet telemetry, collaborate with The Mouth (Liaison) and The Eyes (Task Observer) "
            "via the Shoulder Tap Event Bus, and provide structured telemetry digests for on-demand inspection.\n"
            "Analyze the newly detected error logs and fleet status deltas, and judge whether an operational health anomaly exists ('smells_off').\n\n"
            "Strict Guidelines:\n"
            "- 'smells_off': true ONLY for genuine operational failures: unhandled daemon crash loops, "
            "production VPS downtime (oci-machine), container crashes, or host root disk exhaustion.\n"
            "- 'smells_off': false for benign/expected events: Carl's personal laptop (personal-laptop) sleeping/standby, "
            "routine test suite executions, or clean daemon restarts.\n"
            "- Output strictly valid JSON matching the schema.\n\n"
            "JSON Schema:\n"
            "{\n"
            '  "smells_off": true | false,\n'
            '  "severity": "low" | "medium" | "high" | "critical",\n'
            '  "category": "crash_loop" | "node_down" | "disk_pressure" | "container_crash" | "network" | "other",\n'
            '  "summary": "<concise 1-2 sentence executive briefing>",\n'
            '  "affected_nodes": ["<node_names>"],\n'
            '  "recommended_action": "<concrete actionable next step for Carl>"\n'
            "}"
        )

        formatted_deltas = "\n".join([
            f"- [{str(d.get('severity', 'info')).upper()}] {d.get('category')} ({d.get('target')}): {d.get('description')} (benign={d.get('benign')})"
            for d in deltas
        ]) if deltas else "None"

        error_sample = "\n".join(error_logs[-25:]) if error_logs else "None"

        user_content = (
            f"### Newly Appended Error Logs:\n```\n{error_sample}\n```\n\n"
            f"### Fleet Status Deltas:\n{formatted_deltas}\n"
        )

        payload = {
            "systemInstruction": {"parts": [{"text": sys_instruction}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": user_content}],
                }
            ],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json",
            },
        }

        base_url = get_llm_base_url("nose")
        if base_url:
            from golem.ingress import call_openai_compatible_api
            data = await call_openai_compatible_api(
                payload=payload,
                model=self.model,
                api_key=key,
                base_url=base_url,
                timeout_seconds=self.timeout_seconds,
            )
        else:
            url = GEMINI_API_URL.format(model=self.model)
            if "?key=" in url or "&key=" in url:
                raise ValueError("Security violation: Gemini API keys must not be passed via URL query parameters; use 'x-goog-api-key' header.")
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            headers = {
                "Content-Type": "application/json",
                "x-goog-api-key": key,
            }
            user_project = (
                os.environ.get("GOOGLE_CLOUD_PROJECT")
                or os.environ.get("GCP_PROJECT")
                or os.environ.get("VERTEX_PROJECT")
            )
            if user_project:
                headers["x-goog-user-project"] = user_project.strip()
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                    else:
                        logger.warning(f"NOSE_MODEL API returned HTTP {resp.status}: {await resp.text()}")
                        data = None

        if data and isinstance(data, dict):
            candidates = data.get("candidates") if isinstance(data, dict) else None
            if candidates and isinstance(candidates, list):
                cand = candidates[0] if isinstance(candidates[0], dict) else {}
                content = cand.get("content") if isinstance(cand, dict) else None
                parts = (content.get("parts") if isinstance(content, dict) else None) or []
                if not isinstance(parts, list):
                    parts = []
                text_chunks = [
                    str(p["text"]) for p in parts
                    if isinstance(p, dict) and "text" in p and p.get("text")
                ]
                raw_json = "".join(text_chunks).strip()
                raw_json = re.sub(r"^```json\s*", "", raw_json)
                raw_json = re.sub(r"\s*```$", "", raw_json)
                try:
                    parsed = json.loads(raw_json)
                    if isinstance(parsed, dict) and "smells_off" in parsed:
                        return AnomalyVerdict(
                            smells_off=bool(parsed.get("smells_off")),
                            severity=parsed.get("severity", "medium"),
                            category=parsed.get("category", "other"),
                            summary=parsed.get("summary", "Fleet anomaly detected."),
                            affected_nodes=parsed.get("affected_nodes", []),
                            recommended_action=parsed.get("recommended_action", ""),
                        )
                except Exception:
                    pass

        return self._evaluate_offline_fallback(error_logs, deltas)

    def _evaluate_offline_fallback(
        self,
        error_logs: List[str],
        deltas: List[Any],
    ) -> AnomalyVerdict:
        """Deterministic offline evaluation preserving 100% test compatibility."""
        actionable_deltas = [
            d for d in deltas
            if not (d.get("benign") if isinstance(d, dict) else getattr(d, "benign", False))
        ]

        # 1. Critical deltas
        critical_deltas = [
            d for d in actionable_deltas
            if (d.get("severity") if isinstance(d, dict) else getattr(d, "severity", "")) == "critical"
        ]
        if critical_deltas:
            d = critical_deltas[0]
            target = d.get("target") if isinstance(d, dict) else getattr(d, "target", "")
            cat = d.get("category") if isinstance(d, dict) else getattr(d, "category", "fleet_critical")
            desc = d.get("description") if isinstance(d, dict) else getattr(d, "description", "")
            nodes = [target] if target not in ("localhost", "docker", "tailscale") else ["hermes"]
            return AnomalyVerdict(
                smells_off=True,
                severity="critical",
                category=cat,
                summary=desc,
                affected_nodes=nodes,
                recommended_action=f"Investigate {target} status immediately.",
            )

        # 2. Warning / other actionable deltas
        if actionable_deltas:
            d = actionable_deltas[0]
            target = d.get("target") if isinstance(d, dict) else getattr(d, "target", "")
            cat = d.get("category") if isinstance(d, dict) else getattr(d, "category", "fleet_warning")
            desc = d.get("description") if isinstance(d, dict) else getattr(d, "description", "")
            sev = d.get("severity") if isinstance(d, dict) else getattr(d, "severity", "medium")
            nodes = [target] if target not in ("localhost", "docker", "tailscale") else ["hermes"]
            return AnomalyVerdict(
                smells_off=True,
                severity="high" if sev == "warning" else "medium",
                category=cat,
                summary=desc,
                affected_nodes=nodes,
                recommended_action=f"Check health and logs for {target}.",
            )

        # 3. Error logs
        if error_logs:
            has_traceback = any("Traceback" in l for l in error_logs)
            return AnomalyVerdict(
                smells_off=True,
                severity="high" if has_traceback else "medium",
                category="crash_loop" if has_traceback else "error_logs",
                summary=f"Detected {len(error_logs)} error line(s) in bot.err tail.",
                affected_nodes=["hermes"],
                recommended_action="Inspect bot.err for unhandled exceptions.",
            )

        # 4. Quiescent
        return AnomalyVerdict(
            smells_off=False,
            severity="low",
            category="benign",
            summary="All systems nominal or within benign parameters.",
            affected_nodes=[],
            recommended_action="",
        )


# ==============================================================================
# Gmail Inbox Sniffer Subsystem
# ==============================================================================

DEFAULT_NOSE_EMAIL_QUERY = "is:unread from:k.horned@gmail.com"


def get_google_token_path() -> Optional[Path]:
    """Find the Google token path across standard Golem / Hermes config locations."""
    env_home = os.environ.get("GOLEM_HOME", "").strip() or os.environ.get("HERMES_HOME", "").strip()
    if env_home:
        p = Path(env_home) / "google_token.json"
        if p.exists():
            return p
    for home_dir in [Path.home() / ".golem", Path.home() / ".hermes"]:
        p = home_dir / "google_token.json"
        if p.exists():
            return p
    return None


def resolve_email_query(
    query: Optional[str] = None,
    senders: Optional[str] = None,
) -> str:
    """Resolve Gmail sniff query from parameter, env vars, or default."""
    if query:
        return query.strip()
    env_query = os.environ.get("NOSE_EMAIL_QUERY", "").strip()
    if env_query:
        return env_query
    env_senders = senders or os.environ.get("NOSE_EMAIL_SENDERS", "").strip()
    if env_senders:
        sender_list = [s.strip() for s in env_senders.split(",") if s.strip()]
        if len(sender_list) == 1:
            return f"is:unread from:{sender_list[0]}"
        elif len(sender_list) > 1:
            clauses = " OR ".join(f"from:{s}" for s in sender_list)
            return f"is:unread ({clauses})"
    return DEFAULT_NOSE_EMAIL_QUERY


def extract_urls_from_text(text: str) -> List[str]:
    """Extract and clean http/https URLs from text."""
    if not text:
        return []
    raw_urls = re.findall(r'https?://[^\s<>"\'\)\]\}]+', text)
    cleaned: List[str] = []
    for u in raw_urls:
        u = re.sub(r'[\.,;:]+$', '', u)
        if u and u not in cleaned:
            cleaned.append(u)
    return cleaned


class EmailSniffer:
    """
    Asynchronous Gmail inbox sniffer for The Nose.
    Monitors Gmail for unread incoming emails, extracts metadata and URLs,
    marks emails as read, and dispatches ShoulderTapAlerts over the ShoulderTapEventBus.
    """

    def __init__(
        self,
        query: Optional[str] = None,
        senders: Optional[str] = None,
        enabled: Optional[bool] = None,
        bus: Optional[ShoulderTapEventBus] = None,
        script_path: Optional[Union[str, Path]] = None,
        python_bin: Optional[str] = None,
        token_path: Optional[Union[str, Path]] = None,
    ) -> None:
        self.query = resolve_email_query(query=query, senders=senders)
        self._force_enabled = enabled
        self.bus = bus
        self.token_path = Path(token_path) if token_path else get_google_token_path()
        self.script_path = (
            Path(script_path) if script_path else self._find_google_api_script()
        )
        self.python_bin = python_bin or self._find_python_bin()

        self.is_calibrated: bool = False
        self.seen_message_ids: Set[str] = set()
        self.last_sniff_time: Optional[float] = None
        self.processed_count: int = 0
        self.last_error: Optional[str] = None

    @classmethod
    def _find_google_api_script(cls) -> Optional[Path]:
        env_script = os.environ.get("GOLEM_GOOGLE_API_SCRIPT") or os.environ.get("HERMES_GOOGLE_API_SCRIPT")
        if env_script and os.path.exists(env_script):
            return Path(env_script)
        wiki_candidate = Path.home() / ".golem" / "wiki" / "runbooks" / "skills" / "google-workspace" / "scripts" / "google_api.py"
        if wiki_candidate.exists():
            return wiki_candidate
        project_dir = Path(__file__).resolve().parent.parent
        for base in [project_dir, Path("/Users/hermes/projects/first-mate"), Path("/Users/hermes/projects/golem")]:
            candidate = base / "skills" / "google-workspace" / "scripts" / "google_api.py"
            if candidate.exists():
                return candidate
        return None

    @classmethod
    def _find_python_bin(cls) -> str:
        project_dir = Path(__file__).resolve().parent.parent
        for base in [project_dir, Path("/Users/hermes/projects/first-mate"), Path("/Users/hermes/projects/golem")]:
            venv_py = base / ".venv" / "bin" / "python"
            if venv_py.exists():
                return str(venv_py)
        if sys.executable:
            return sys.executable
        return shutil.which("python3") or "python3"

    def is_available(self) -> bool:
        """Return True if email sniffing is configured, token exists, and not in test mode."""
        if self._force_enabled is not None:
            return self._force_enabled
        if os.environ.get("GOLEM_TEST_MODE") == "1" or os.environ.get("FIRSTMATE_TEST_MODE") == "1":
            return False
        return bool(self.token_path and self.token_path.exists() and self.script_path and self.script_path.exists())

    async def _run_google_api(self, args: List[str], timeout: float = 20.0) -> Any:
        if not self.script_path or not self.script_path.exists():
            raise FileNotFoundError(f"google_api.py not found at {self.script_path}")

        cmd = [self.python_bin, str(self.script_path), *args]
        env = os.environ.copy()
        if self.token_path:
            env["GOOGLE_WORKSPACE_CLI_CREDENTIALS_FILE"] = str(self.token_path)

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            raise TimeoutError(f"Command {' '.join(cmd)} timed out after {timeout}s")

        if proc.returncode != 0:
            err_text = stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"google_api.py failed (exit {proc.returncode}): {err_text}")

        out_text = stdout.decode("utf-8", errors="replace").strip()
        if not out_text:
            return None
        return json.loads(out_text)

    async def search_messages(self, query: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """Search Gmail messages matching query."""
        res = await self._run_google_api(["gmail", "search", query, "--max", str(max_results)])
        if isinstance(res, list):
            return res
        return []

    async def get_message(self, message_id: str) -> Dict[str, Any]:
        """Fetch full message details including headers and body text."""
        res = await self._run_google_api(["gmail", "get", message_id])
        if isinstance(res, dict):
            return res
        return {}

    async def mark_as_read(self, message_id: str) -> bool:
        """Mark email as read by removing the UNREAD label."""
        res = await self._run_google_api(["gmail", "modify", message_id, "--remove-labels", "UNREAD"])
        return bool(isinstance(res, dict) and "id" in res)

    async def calibrate_startup(self) -> Set[str]:
        """
        Record existing unread message IDs so pre-existing emails are not re-processed on boot.
        """
        if not self.is_available():
            self.is_calibrated = True
            return set()

        try:
            messages = await self.search_messages(self.query, max_results=100)
            for msg in messages:
                m_id = msg.get("id")
                if m_id:
                    self.seen_message_ids.add(m_id)
            logger.info(
                f"EmailSniffer startup calibration complete: {len(self.seen_message_ids)} existing unread email(s) recorded."
            )
        except Exception as e:
            logger.warning(f"EmailSniffer startup calibration encountered error: {e}")
            self.last_error = str(e)
        finally:
            self.is_calibrated = True

        return self.seen_message_ids

    def calibrate_startup_sync(self) -> Set[str]:
        """Synchronous startup calibration if event loop is not yet running."""
        if not self.is_available():
            self.is_calibrated = True
            return set()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            return self.seen_message_ids

        try:
            return asyncio.run(self.calibrate_startup())
        except Exception as e:
            logger.warning(f"EmailSniffer sync startup calibration failed: {e}")
            self.is_calibrated = True
            return self.seen_message_ids

    async def sniff_new_emails(self) -> List[ShoulderTapAlert]:
        """
        Poll Gmail for new unread messages.
        Extracts URLs, marks email as read, and publishes ShoulderTapAlert.
        """
        if not self.is_available():
            return []

        self.last_sniff_time = time.time()

        if not self.is_calibrated:
            await self.calibrate_startup()
            return []

        try:
            messages = await self.search_messages(self.query, max_results=20)
        except Exception as e:
            self.last_error = str(e)
            logger.warning(f"EmailSniffer search error: {e}")
            return []

        new_alerts: List[ShoulderTapAlert] = []

        for msg_meta in messages:
            msg_id = msg_meta.get("id")
            if not msg_id or msg_id in self.seen_message_ids:
                continue

            self.seen_message_ids.add(msg_id)

            try:
                full_msg = await self.get_message(msg_id)
            except Exception as e:
                logger.warning(f"EmailSniffer failed to fetch full message {msg_id}: {e}")
                full_msg = {}

            sender = full_msg.get("from") or msg_meta.get("from", "")
            subject = full_msg.get("subject") or msg_meta.get("subject", "")
            date = full_msg.get("date") or msg_meta.get("date", "")
            snippet = full_msg.get("snippet") or msg_meta.get("snippet", "")
            body = full_msg.get("body", "")

            # Extract URLs from body and snippet
            urls = extract_urls_from_text(f"{body}\n{snippet}")

            # Mark email as read / remove UNREAD label
            try:
                await self.mark_as_read(msg_id)
            except Exception as e:
                logger.warning(f"EmailSniffer failed to mark {msg_id} as read: {e}")

            self.processed_count += 1

            alert = ShoulderTapAlert(
                alert_type="email_received",
                source="nose",
                summary=f"New email from {sender}: {subject}",
                data={
                    "email_id": msg_id,
                    "from": sender,
                    "subject": subject,
                    "body": body,
                    "snippet": snippet,
                    "urls": urls,
                    "date": date,
                },
            )

            if self.bus is not None:
                try:
                    self.bus.publish(alert)
                    logger.info(f"Published email_received alert: {alert.summary}")
                except Exception as e:
                    logger.warning(f"Error publishing email alert to bus: {e}")

            new_alerts.append(alert)

        return new_alerts

    def get_telemetry(self) -> Dict[str, Any]:
        """Return structured email telemetry dictionary."""
        telemetry: Dict[str, Any] = {
            "enabled": self.is_available(),
            "last_sniff": self.last_sniff_time,
            "processed_count": self.processed_count,
            "query": self.query,
        }
        if self.last_error:
            telemetry["last_error"] = self.last_error
        return telemetry


# ==============================================================================
# Active Work Detection for Linear Assigned Tickets
# ==============================================================================

def _check_recent_files(ws: str, current_time: float, max_age_seconds: float, max_files: int = 40) -> bool:
    """Safely check if files in workspace were modified within max_age_seconds (bounded to top 2 levels, max_files)."""
    if not isinstance(ws, str) or _is_mock_object(ws) or not os.path.isdir(ws):
        return False
    count = 0
    try:
        with os.scandir(ws) as it:
            for entry in it:
                if entry.name.startswith(".") or entry.name in ("node_modules", "venv", ".venv", "__pycache__", "build", "dist", ".git"):
                    continue
                if entry.is_file():
                    count += 1
                    try:
                        if current_time - entry.stat().st_mtime <= max_age_seconds:
                            return True
                    except Exception:
                        pass
                elif entry.is_dir():
                    try:
                        with os.scandir(entry.path) as sub_it:
                            for sub_entry in sub_it:
                                if sub_entry.name.startswith(".") or sub_entry.name in ("node_modules", "venv", ".venv", "__pycache__", "build", "dist", ".git"):
                                    continue
                                if sub_entry.is_file():
                                    count += 1
                                    try:
                                        if current_time - sub_entry.stat().st_mtime <= max_age_seconds:
                                            return True
                                    except Exception:
                                        pass
                                if count >= max_files:
                                    break
                    except Exception:
                        pass
                if count >= max_files:
                    break
    except Exception:
        pass
    return False


def is_ticket_actively_worked(
    ticket: Dict[str, Any],
    session_router: Optional[Any] = None,
    eyes_observer: Optional[Any] = None,
    registry_path: Optional[str] = None,
    now: Optional[float] = None,
) -> bool:
    """
    Deterministic Active Work Detection for Linear tickets.
    Returns True if an 'In Progress' ticket is actively being worked on
    via Signal A (Active Session / Linear Plan Binding),
    Signal B (Active Runner / Subprocess / Herdr Process), or
    Signal C (Recent Activity Grace Window on In Progress).
    Returns False if the ticket is stalled/idle or not In Progress.
    """
    if not ticket or not isinstance(ticket, dict):
        return False

    state_val = ticket.get("state")
    if isinstance(state_val, dict):
        state_name = str(state_val.get("name") or "").lower()
        state_type = str(state_val.get("type") or "").lower()
    else:
        state_name = str(state_val or "").lower()
        state_type = str(ticket.get("state_type") or "").lower()

    if state_name in ("done", "canceled", "cancelled") or state_type in ("completed", "canceled"):
        return False

    is_in_progress = bool("in progress" in state_name or state_type == "started" or state_name == "started")

    current_time = float(now) if now is not None else time.time()

    ticket_key = str(ticket.get("identifier") or ticket.get("id") or "").strip()
    related_keys: Set[str] = {ticket_key} if ticket_key else set()

    # Extract parent key
    parent = ticket.get("parent")
    if isinstance(parent, dict) and parent.get("identifier"):
        related_keys.add(str(parent["identifier"]))

    # Extract children / subtask keys
    children = ticket.get("children")
    if isinstance(children, dict):
        for node in children.get("nodes", []):
            if isinstance(node, dict) and node.get("identifier"):
                related_keys.add(str(node["identifier"]))
    elif isinstance(ticket.get("subtasks"), list):
        for st in ticket["subtasks"]:
            if isinstance(st, dict) and st.get("identifier"):
                related_keys.add(str(st["identifier"]))
            elif isinstance(st, str) and st.startswith("CHO-"):
                related_keys.add(st)

    # Extract keys from title and description
    title = str(ticket.get("title") or "")
    description = str(ticket.get("description") or "")
    combined_text = f"{title} {description}"
    for k in re.findall(r"CHO-\d+", combined_text):
        related_keys.add(k)
    related_keys.discard("")

    # Extract candidate workspaces (excluding Golem's own host repo where bot.err/app.py run 24/7)
    golem_self_names = {"golem", "first-mate", "agy"}
    try:
        self_real = os.path.realpath(str(_PROJECT_ROOT))
    except Exception:
        self_real = ""

    def _is_self_repo(path_str: str) -> bool:
        if os.path.basename(path_str.rstrip("/")).lower() in golem_self_names:
            return True
        try:
            return bool(self_real and os.path.realpath(path_str) == self_real)
        except Exception:
            return False

    workspace_candidates: List[str] = []
    # 1. Direct path mentions in text
    for m in re.finditer(r"(/[a-zA-Z0-9_.-]+(?:/[a-zA-Z0-9_.-]+)+)", combined_text):
        cand = m.group(1).rstrip(",;.")
        if os.path.isdir(cand) and not _is_self_repo(cand) and cand not in workspace_candidates:
            workspace_candidates.append(cand)
    # 2. Known project directory matches
    generic_stop_words = {"skill", "build", "task", "project", "fix", "update", "create", "test", "issue", "golem", "first-mate", "agy"}
    for token in re.findall(r"[A-Za-z0-9_-]+", title):
        if len(token) > 3 and token.lower() not in generic_stop_words:
            p = f"/Users/hermes/projects/{token}"
            if os.path.isdir(p) and not _is_self_repo(p) and p not in workspace_candidates:
                workspace_candidates.append(p)
    target_cwd = ticket.get("target_cwd")
    if isinstance(target_cwd, str) and not _is_mock_object(target_cwd) and os.path.isdir(target_cwd) and not _is_self_repo(target_cwd):
        if target_cwd not in workspace_candidates:
            workspace_candidates.append(target_cwd)

    has_active_session_lock = False
    active_session_matched_key = False
    has_active_runner_or_process = False
    has_recent_activity = False

    # ------------------------------------------------------------------
    # Signal A: Active Session / Linear Plan Binding
    # ------------------------------------------------------------------
    session_records: List[Dict[str, Any]] = []

    live_sessions_map: Dict[str, Any] = {}
    if session_router is not None:
        sessions_map = getattr(session_router, "sessions", {})
        if isinstance(sessions_map, dict):
            live_sessions_map = sessions_map
            for s_key, s_obj in list(sessions_map.items()):
                state_obj = getattr(s_obj, "state", None)
                state_val_s = getattr(state_obj, "value", state_obj)
                in_flight_s = getattr(s_obj, "in_flight_subtasks", None)
                queued_s = getattr(s_obj, "queued_subtasks", None)
                has_batch_s = bool(
                    (isinstance(in_flight_s, (list, tuple, set)) and len(in_flight_s) > 0)
                    or (isinstance(queued_s, (list, tuple, set)) and len(queued_s) > 0)
                )
                is_busy_s = getattr(s_obj, "is_busy", False) or (state_val_s in ("running", "working")) or has_batch_s
                s_updated = (
                    _safe_float(getattr(s_obj, "last_dispatched_at", None), 0.0)
                    or _safe_float(getattr(s_obj, "last_activity", None), 0.0)
                    or _safe_float(getattr(s_obj, "updated_at", None), 0.0)
                    or _safe_float(getattr(s_obj, "last_active_ts", None), 0.0)
                )
                session_records.append({
                    "session_key": s_key,
                    "active_linear_parent_key": getattr(s_obj, "active_linear_parent_key", None),
                    "active_linear_parent_keys": getattr(s_obj, "active_linear_parent_keys", None),
                    "linear_issue_key": getattr(s_obj, "linear_issue_key", None),
                    "linear_issue_id": getattr(s_obj, "linear_issue_id", None),
                    "linear_plan_cache": getattr(s_obj, "linear_plan_cache", None),
                    "linear_tickets_context": getattr(s_obj, "linear_tickets_context", None),
                    "pending_subtasks": getattr(s_obj, "pending_subtasks", []),
                    "in_flight_subtasks": in_flight_s if isinstance(in_flight_s, (list, tuple, set)) else [],
                    "queued_subtasks": queued_s if isinstance(queued_s, (list, tuple, set)) else [],
                    "target_cwd": getattr(s_obj, "target_cwd", getattr(s_obj, "cwd", None)),
                    "updated_at": s_updated,
                    "is_busy": is_busy_s,
                    "_source": "router",
                })

    reg_file = (
        registry_path
        or os.environ.get("FIRSTMATE_SESSIONS_REGISTRY_PATH")
        or os.path.expanduser("~/.gemini/golem/sessions_registry.json")
    )
    if reg_file and isinstance(reg_file, str) and not _is_mock_object(reg_file) and os.path.exists(reg_file):
        try:
            with open(reg_file, "r", encoding="utf-8") as f:
                disk_data = json.load(f)
                if isinstance(disk_data, dict):
                    for d_key, d_val in disk_data.items():
                        if isinstance(d_val, dict):
                            d_entry = dict(d_val)
                            d_entry.setdefault("session_key", d_key)
                            d_entry["_source"] = "disk"
                            session_records.append(d_entry)
        except Exception as exc:
            logger.debug(f"Unable to read session registry in is_ticket_actively_worked: {exc}")

    for rec in session_records:
        rec_keys: Set[str] = set()
        for k_field in ("active_linear_parent_key", "linear_issue_key", "linear_issue_id", "linear_parent_key"):
            val = rec.get(k_field)
            if val and not _is_mock_object(val):
                rec_keys.add(str(val))
        parent_keys_list = rec.get("active_linear_parent_keys")
        if isinstance(parent_keys_list, (list, tuple, set)):
            for pk in parent_keys_list:
                if pk and not _is_mock_object(pk):
                    rec_keys.add(str(pk))
        plan = rec.get("linear_plan_cache") or rec.get("plan_cache") or rec.get("linear_plan")
        if isinstance(plan, dict):
            parent_id = plan.get("parent", {}).get("identifier") or plan.get("parent", {}).get("id")
            if parent_id and not _is_mock_object(parent_id):
                rec_keys.add(str(parent_id))
            for st in plan.get("subtasks", []):
                st_id = st.get("identifier") or st.get("id")
                if st_id and not _is_mock_object(st_id):
                    rec_keys.add(str(st_id))
        tickets_ctx = rec.get("linear_tickets_context")
        if isinstance(tickets_ctx, list):
            for t_item in tickets_ctx:
                if isinstance(t_item, dict):
                    t_id = t_item.get("identifier") or t_item.get("id")
                    if t_id and not _is_mock_object(t_id):
                        rec_keys.add(str(t_id))
                    for st in (t_item.get("subtasks") or []):
                        if isinstance(st, dict):
                            st_id = st.get("identifier") or st.get("id")
                            if st_id and not _is_mock_object(st_id):
                                rec_keys.add(str(st_id))
        for list_field in ("pending_subtasks", "in_flight_subtasks", "queued_subtasks"):
            seq_val = rec.get(list_field)
            if isinstance(seq_val, (list, tuple, set)):
                for item in seq_val:
                    if isinstance(item, dict):
                        for ik in ("issue_key", "parent_key", "identifier", "id"):
                            if item.get(ik) and not _is_mock_object(item.get(ik)):
                                rec_keys.add(str(item[ik]))
                    elif item and not _is_mock_object(item):
                        rec_keys.add(str(item))

        cwd = rec.get("target_cwd")
        if isinstance(cwd, str) and not _is_mock_object(cwd) and os.path.isdir(cwd) and not _is_self_repo(cwd) and cwd not in workspace_candidates:
            if any(k.lower() in cwd.lower() for k in related_keys) or any(t.lower() in cwd.lower() for t in re.findall(r"[A-Za-z0-9_-]+", title) if len(t) > 3 and t.lower() not in generic_stop_words):
                workspace_candidates.append(cwd)

        # Check key match or workspace match
        matches_key = bool(related_keys.intersection(rec_keys))
        matches_workspace = bool(isinstance(cwd, str) and cwd in workspace_candidates)

        if matches_key or matches_workspace:
            rec_updated = _safe_float(rec.get("updated_at") or rec.get("updatedAt"), 0.0)
            is_busy = bool(rec.get("is_busy") or rec.get("running_task") or rec.get("has_active_lock"))
            recent_session = (current_time - rec_updated <= 1800.0) if rec_updated > 0 else False

            is_active = False
            if not is_in_progress:
                # For non-in-progress tickets (Todo, Backlog), ONLY an explicitly busy session counts
                if is_busy:
                    is_active = True
                elif rec.get("_source") == "disk" and session_router is not None:
                    s_key = rec.get("session_key")
                    live_s = live_sessions_map.get(s_key) if s_key else None
                    if live_s is not None:
                        state_obj = getattr(live_s, "state", None)
                        state_val_s = getattr(state_obj, "value", state_obj)
                        if getattr(live_s, "is_busy", False) or (state_val_s == "running"):
                            is_active = True
            elif rec.get("_source") == "disk" and session_router is not None:
                s_key = rec.get("session_key")
                live_s = live_sessions_map.get(s_key) if s_key else None
                live_busy = False
                if live_s is not None:
                    state_obj = getattr(live_s, "state", None)
                    state_val_s = getattr(state_obj, "value", state_obj)
                    live_busy = getattr(live_s, "is_busy", False) or (state_val_s == "running")
                if is_busy or live_busy:
                    is_active = True
            else:
                if is_busy or recent_session:
                    is_active = True

            if is_active:
                has_active_session_lock = True
                if matches_key:
                    active_session_matched_key = True
                break

    if has_active_session_lock:
        if not is_in_progress:
            return active_session_matched_key
        return True

    if not is_in_progress:
        return False

    # ------------------------------------------------------------------
    # Signal B: Active Runner / Subprocess / Herdr Process
    # ------------------------------------------------------------------
    if eyes_observer is not None:
        if getattr(eyes_observer, "is_running", False):
            has_active_runner_or_process = True
        last_state = str(getattr(eyes_observer, "_last_emitted_state", "") or getattr(eyes_observer, "last_state", "")).upper()
        if last_state == "RUNNING":
            has_active_runner_or_process = True

    if not has_active_runner_or_process:
        try:
            current_pid = os.getpid()
            ps_out = subprocess.check_output(
                ["ps", "-eo", "pid,command"],
                text=True,
                timeout=1.0,
                stderr=subprocess.DEVNULL,
            )
            for line in ps_out.splitlines():
                line_strip = line.strip()
                if not line_strip:
                    continue
                parts = line_strip.split(None, 1)
                if len(parts) < 2:
                    continue
                pid_str, cmd = parts[0], parts[1]
                try:
                    if int(pid_str) == current_pid:
                        continue
                except Exception:
                    pass
                cmd_lower = cmd.lower()
                if "unittest" in cmd_lower and "test_nose" in cmd_lower:
                    continue
                if any(eng in cmd_lower for eng in ("agy", "herdr", "pytest", "python", "opencode")):
                    if any(k in cmd for k in related_keys):
                        has_active_runner_or_process = True
                        break
                    if any(os.path.basename(ws).lower() in cmd_lower for ws in workspace_candidates if ws):
                        has_active_runner_or_process = True
                        break
        except Exception as exc:
            logger.debug(f"ps inspection error in is_ticket_actively_worked: {exc}")

    if not has_active_runner_or_process:
        for task_dir in ("/tmp/herdr_tasks", "/tmp/golem_tasks"):
            if os.path.isdir(task_dir):
                try:
                    for entry in os.scandir(task_dir):
                        if entry.is_file():
                            mtime = entry.stat().st_mtime
                            if current_time - mtime <= 900.0:  # 15 min
                                if any(k in entry.name for k in related_keys):
                                    has_active_runner_or_process = True
                                    break
                                try:
                                    with open(entry.path, "r", encoding="utf-8", errors="ignore") as f:
                                        head = f.read(4096)
                                        if any(k in head for k in related_keys):
                                            has_active_runner_or_process = True
                                            break
                                except Exception:
                                    pass
                except Exception as exc:
                    logger.debug(f"Error scanning task dir {task_dir}: {exc}")

    if has_active_runner_or_process:
        return True

    # ------------------------------------------------------------------
    # Signal C: Recent Activity Grace Window on In Progress
    # ------------------------------------------------------------------
    ticket_updated_at_str = ticket.get("updatedAt") or ticket.get("updated_at")
    ticket_updated_ts = 0.0
    if isinstance(ticket_updated_at_str, (int, float)):
        ticket_updated_ts = float(ticket_updated_at_str)
    elif isinstance(ticket_updated_at_str, str):
        try:
            clean_str = ticket_updated_at_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_str)
            ticket_updated_ts = dt.timestamp()
        except Exception:
            ticket_updated_ts = 0.0

    # Recent activity grace window (20 min): requires recent update AND modified files
    if ticket_updated_ts > 0.0 and (current_time - ticket_updated_ts <= 1200.0):
        for ws in workspace_candidates:
            if _check_recent_files(ws, current_time, max_age_seconds=600.0):
                has_recent_activity = True
                break

    return has_recent_activity


# ==============================================================================
# The Nose Sentinel: Main Watchdog Class
# ==============================================================================

class NoseSentinel:
    """
    Autonomous 24/7 background watchdog for First Mate.
    Coordinates LogSniffer, FleetSniffer, StatusDeltaCalculator, ZeroRegexEvaluator,
    and publishes health_anomaly ShoulderTapAlerts over ShoulderTapEventBus.
    """

    def __init__(
        self,
        fleet: Optional[Any] = None,
        bus: Optional[ShoulderTapEventBus] = None,
        event_bus: Optional[ShoulderTapEventBus] = None,
        bot_err_path: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        interval_minutes: Optional[float] = None,
        interval: Optional[float] = None,
        model: Optional[str] = None,
        nose_model: Optional[str] = None,
        api_key: Optional[str] = None,
        shutdown_event: Optional[asyncio.Event] = None,
        tailscale_bin: Optional[str] = None,
        disk_path: str = "/",
        min_free_gb: float = 15.0,
        max_percent_used: float = 90.0,
        max_slice_bytes: int = DEFAULT_MAX_SLICE_BYTES,
        initial_offset: Optional[int] = None,
        startup_mode: str = "eof",
        email_sniffer: Optional[EmailSniffer] = None,
        email_query: Optional[str] = None,
        email_senders: Optional[str] = None,
        email_enabled: Optional[bool] = None,
        linear_bridge: Optional[Any] = None,
        session_router: Optional[Any] = None,
        router: Optional[Any] = None,
        eyes_observer: Optional[Any] = None,
        eyes: Optional[Any] = None,
        registry_path: Optional[str] = None,
        webui_server: Optional[Any] = None,
        server: Optional[Any] = None,
    ) -> None:
        self.fleet = fleet
        self.bus = event_bus or bus or get_default_event_bus()
        self.event_bus = self.bus
        self.linear_bridge = linear_bridge
        self.session_router = session_router or router
        self.eyes_observer = eyes_observer or eyes
        self.registry_path = registry_path
        self.webui_server = webui_server or server
        self.server = self.webui_server
        self._last_assigned_tickets: List[Dict[str, Any]] = []
        self._last_alerted_ticket_signatures: Set[str] = set()
        self._last_log_delta_bytes: int = 0
        self.last_sniff_receipt: Optional[Dict[str, Any]] = None
        self.last_sniff_receipt_str: str = ""

        # bot.err file location
        self.bot_err_path = bot_err_path or os.environ.get("FIRSTMATE_LOG_ERR") or DEFAULT_BOT_ERR_PATH
        self.max_slice_bytes = max_slice_bytes
        self.startup_mode = startup_mode

        # Interval configuration
        if interval_seconds is not None:
            self.interval_seconds = float(interval_seconds)
            self.interval_minutes = self.interval_seconds / 60.0
        elif interval_minutes is not None:
            self.interval_minutes = float(interval_minutes)
            self.interval_seconds = self.interval_minutes * 60.0
        elif interval is not None:
            self.interval_minutes = float(interval)
            self.interval_seconds = self.interval_minutes * 60.0
        else:
            raw_env = os.environ.get("NOSE_INTERVAL_MINUTES")
            self.interval_minutes = parse_interval_minutes(raw_env, default=DEFAULT_NOSE_INTERVAL_MINUTES)
            self.interval_seconds = self.interval_minutes * 60.0

        # Model configuration
        self.nose_model = model or nose_model or get_nose_model()
        self.api_key = api_key

        # Subcomponents
        self.fleet_sniffer = FleetSniffer(
            fleet=self.fleet,
            tailscale_bin=tailscale_bin,
            disk_path=disk_path,
            min_free_gb=min_free_gb,
            max_percent_used=max_percent_used,
        )
        self.delta_calculator = StatusDeltaCalculator()
        self.evaluator = ZeroRegexEvaluator(model=self.nose_model, api_key=api_key)
        self.email_sniffer = email_sniffer or EmailSniffer(
            query=email_query,
            senders=email_senders,
            enabled=email_enabled,
            bus=self.bus,
        )

        # Persistent log cursor state
        self.last_log_offset: int = 0
        self.last_log_inode: Optional[int] = None
        self._is_calibrated: bool = False

        if initial_offset is not None:
            self.last_log_offset = initial_offset
            self._is_calibrated = True
        elif os.path.exists(self.bot_err_path):
            self.calibrate_startup(mode=self.startup_mode)

        # Fleet state tracking
        self.last_snapshot: Optional[FleetSnapshot] = None
        self._last_tick_time: Optional[float] = None
        self.last_verdict: Optional[AnomalyVerdict] = None
        self._last_error_count: int = 0
        self._node_consecutive_failures: Dict[str, int] = {}

        # Lifecycle & synchronization
        self._shutdown_event = shutdown_event or asyncio.Event()
        self._task: Optional[asyncio.Task[Any]] = None
        self._is_running: bool = False
        self._sniff_lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        """True if background sentinel watchdog loop is active."""
        return self._is_running and self._task is not None and not self._task.done()

    def calibrate_startup(self, mode: Optional[str] = None, tail_bytes: int = 16384) -> int:
        """
        Calibrate initial log file offset and inode upon startup.
        Modes:
        - "eof" (default): Seek to current EOF (st_size). Prevents stale errors on reboot.
        - "tail_16k": Seek to max(0, st_size - tail_bytes) to inspect immediate boot history.
        - "zero": Seek to 0 (reads entire file from beginning, useful in unit tests).
        """
        selected_mode = mode or self.startup_mode
        if not os.path.exists(self.bot_err_path):
            self.last_log_offset = 0
            self.last_log_inode = None
            self._is_calibrated = True
            return 0

        try:
            st = os.stat(self.bot_err_path)
            self.last_log_inode = getattr(st, "st_ino", None)
            current_size = st.st_size

            if selected_mode == "eof":
                self.last_log_offset = current_size
            elif selected_mode == "tail_16k":
                self.last_log_offset = max(0, current_size - tail_bytes)
            elif selected_mode == "zero":
                self.last_log_offset = 0
            else:
                self.last_log_offset = current_size

            self._is_calibrated = True
            logger.info(
                f"NoseSentinel startup calibrated: offset={self.last_log_offset}, "
                f"inode={self.last_log_inode}, mode='{selected_mode}'"
            )
            try:
                self.email_sniffer.calibrate_startup_sync()
            except Exception as e:
                logger.debug(f"Email startup calibration warning: {e}")
            return self.last_log_offset
        except OSError as e:
            logger.warning(f"Error calibrating log startup for {self.bot_err_path}: {e}")
            self.last_log_offset = 0
            self.last_log_inode = None
            self._is_calibrated = True
            try:
                self.email_sniffer.calibrate_startup_sync()
            except Exception as e_email:
                logger.debug(f"Email startup calibration warning: {e_email}")
            return 0

    def sniff_bot_err(self) -> Tuple[List[str], str]:
        """
        Sniff newly appended lines in bot.err starting from last_log_offset.
        Returns 2-tuple: (error_lines, raw_slice_text).
        """
        if not os.path.exists(self.bot_err_path):
            self.last_log_offset = 0
            self.last_log_inode = None
            self._last_log_delta_bytes = 0
            return [], ""

        try:
            st = os.stat(self.bot_err_path)
        except OSError as e:
            logger.warning(f"Error stating log file {self.bot_err_path}: {e}")
            self._last_log_delta_bytes = 0
            return [], ""

        current_size = st.st_size
        current_inode = getattr(st, "st_ino", None)

        if not self._is_calibrated:
            self.calibrate_startup()
            self._last_log_delta_bytes = 0
            return [], ""

        # Inode rotation check
        if self.last_log_inode is not None and current_inode != self.last_log_inode:
            logger.info(
                f"Log file rotation detected (inode changed from {self.last_log_inode} to {current_inode}); resetting offset to 0"
            )
            self.last_log_offset = 0
            self.last_log_inode = current_inode

        # Truncation check
        if current_size < self.last_log_offset:
            logger.info(
                f"Log file truncation detected (size {current_size} < offset {self.last_log_offset}); resetting offset to 0"
            )
            self.last_log_offset = 0

        if self.last_log_inode is None:
            self.last_log_inode = current_inode

        # Quiescent check: no new bytes written
        if current_size == self.last_log_offset:
            self._last_log_delta_bytes = 0
            return [], ""

        bytes_to_read = current_size - self.last_log_offset
        read_start = self.last_log_offset
        self._last_log_delta_bytes = bytes_to_read

        # Max slice bounding
        if bytes_to_read > self.max_slice_bytes:
            logger.warning(
                f"Log slice size ({bytes_to_read} bytes) exceeds max_slice_bytes ({self.max_slice_bytes}); bounding read"
            )
            read_start = current_size - self.max_slice_bytes

        try:
            with open(self.bot_err_path, "rb") as f:
                f.seek(read_start)
                raw_bytes = f.read(self.max_slice_bytes)
            self.last_log_offset = current_size
        except OSError as e:
            logger.error(f"Error reading log file {self.bot_err_path}: {e}")
            self._last_log_delta_bytes = 0
            return [], ""

        log_text = raw_bytes.decode("utf-8", errors="replace")
        error_lines = self._scan_error_text(log_text)
        return error_lines, log_text

    def sniff_log(self) -> Tuple[List[str], str]:
        """Alias for sniff_bot_err()."""
        return self.sniff_bot_err()

    def _scan_error_text(self, text: str) -> List[str]:
        """
        Scan decoded text for error patterns, unhandled exceptions, and tracebacks.
        Filters out normal [INFO], [DEBUG], and [WARNING] operational lines.
        """
        if not text:
            return []

        lines = text.splitlines()
        error_lines: List[str] = []
        in_traceback = False

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Traceback signature
            if "Traceback (most recent call last):" in line:
                in_traceback = True
                error_lines.append(line)
                continue

            if in_traceback:
                # Detect start of next log record (e.g. timestamp [LEVEL])
                if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+\[", line) and "Traceback" not in line:
                    in_traceback = False
                else:
                    error_lines.append(line)
                    continue

            # Error keywords
            if "[ERROR]" in line or "[CRITICAL]" in line:
                error_lines.append(line)
            elif "Exception ignored" in line or "Unhandled exception" in line or "Fatal error:" in line:
                error_lines.append(line)

        return error_lines

    # Telemetry probe delegations
    async def probe_fleet_nodes(self, nodes: Optional[List[str]] = None, timeout: float = 8.0) -> Dict[str, Dict[str, Any]]:
        return await self.fleet_sniffer.probe_fleet_nodes(nodes=nodes, timeout=timeout)

    async def probe_tailscale(self, timeout: float = 5.0) -> Dict[str, Any]:
        return await self.fleet_sniffer.probe_tailscale(timeout=timeout)

    def probe_disk_pressure(self) -> Dict[str, Any]:
        return self.fleet_sniffer.probe_disk_pressure()

    async def probe_docker_health(self, timeout: float = 5.0) -> Dict[str, Any]:
        return await self.fleet_sniffer.probe_docker_health(timeout=timeout)

    async def capture_snapshot(self) -> FleetSnapshot:
        snap = await self.fleet_sniffer.capture_snapshot()
        snap.email = self.email_sniffer.get_telemetry()
        return snap

    async def sniff_fleet(self) -> FleetSnapshot:
        return await self.capture_snapshot()

    async def sniff_emails(self) -> List[ShoulderTapAlert]:
        """Sniff Gmail inbox for newly received unread emails."""
        return await self.email_sniffer.sniff_new_emails()

    def get_linear_bridge(self) -> Optional[Any]:
        """Resolve or instantiate LinearBridge singleton."""
        if self.linear_bridge is not None:
            return self.linear_bridge
        try:
            from golem.linear_bridge import LinearBridge
            self.linear_bridge = LinearBridge()
            return self.linear_bridge
        except Exception as exc:
            logger.debug(f"Unable to initialize LinearBridge in NoseSentinel: {exc}")
            return None

    async def sniff_assigned_tickets(self) -> List[Dict[str, Any]]:
        """
        Query Linear for issues assigned strictly to Hermes (viewer / hermes.horned@gmail.com).
        Ignores tickets in 'Done' (or completed type), unassigned, or assigned to Carl.
        All non-Done statuses receive attention.
        """
        bridge = self.get_linear_bridge()
        if not bridge:
            return []
        try:
            res = bridge.get_assigned_issues(non_done_only=True)
            if inspect.isawaitable(res):
                res = await res
            return list(res) if res else []
        except Exception as exc:
            logger.warning(f"Error querying assigned Linear tickets in NoseSentinel: {exc}")
            return []

    def is_ticket_actively_worked(self, ticket: Dict[str, Any], now: Optional[float] = None) -> bool:
        """Evaluate deterministic active work detection on an In Progress ticket."""
        return is_ticket_actively_worked(
            ticket,
            session_router=getattr(self, "session_router", None) or getattr(self, "router", None),
            eyes_observer=getattr(self, "eyes_observer", None) or getattr(self, "eyes", None),
            registry_path=getattr(self, "registry_path", None),
            now=now,
        )

    def compute_status_deltas(
        self,
        prev: Optional[Any],
        curr: Any,
        failure_threshold: int = 1,
    ) -> List[StatusDelta]:
        return self.delta_calculator.compute_deltas(
            prev,
            curr,
            failure_counts=self._node_consecutive_failures,
            failure_threshold=failure_threshold,
        )

    def compute_status_deltas_with_hysteresis(
        self,
        prev: Optional[Any],
        curr: Any,
        failure_threshold: int = 3,
    ) -> List[StatusDelta]:
        return self.compute_status_deltas(prev, curr, failure_threshold=failure_threshold)

    async def evaluate_anomaly(self, error_logs: List[str], deltas: List[Any]) -> Optional[AnomalyVerdict]:
        """
        Evaluate health status using NOSE_MODEL.
        Returns AnomalyVerdict if an anomaly exists, or None if quiescent.
        """
        if self.evaluator.is_quiescent(error_logs, deltas):
            return None
        return await self._call_gemini_evaluation(error_logs, deltas)

    async def _call_gemini_evaluation(self, error_logs: List[str], deltas: List[Any]) -> AnomalyVerdict:
        """Invokes evaluator, allowing test patch targeting self._call_gemini_evaluation."""
        return await self.evaluator._call_gemini_evaluation(error_logs, deltas)

    async def tick(self) -> Optional[ShoulderTapAlert]:
        """
        Execute one complete sniff-and-evaluate cycle.
        Protected by _sniff_lock against concurrent overlapping executions.
        """
        async with self._sniff_lock:
            self._last_tick_time = time.time()

            # 1. Sniff bot.err for newly appended errors
            sniff_res = self.sniff_bot_err()
            if inspect.isawaitable(sniff_res):
                sniff_res = await sniff_res

            if sniff_res is None:
                errors, raw = [], ""
            elif isinstance(sniff_res, tuple) and len(sniff_res) == 2:
                errors, raw = sniff_res
            elif hasattr(sniff_res, "error_lines"):
                errors = getattr(sniff_res, "error_lines", [])
                raw = getattr(sniff_res, "raw_text", "")
            else:
                errors = list(sniff_res) if isinstance(sniff_res, (list, tuple)) else []
                raw = ""
            self._last_error_count = len(errors)

            # 2. Sniff fleet telemetry & Tailscale
            current_snapshot = await self.sniff_fleet()

            # 3. Compute status deltas
            deltas = self.compute_status_deltas(self.last_snapshot, current_snapshot)
            self.last_snapshot = current_snapshot

            # 4. Sniff email inbox
            email_alerts: List[ShoulderTapAlert] = []
            try:
                email_alerts = await self.sniff_emails()
            except Exception as ee:
                logger.warning(f"Error during email sniffing in NoseSentinel.tick: {ee}")

            # 4b. Sniff assigned Linear tickets
            assigned_tickets: List[Dict[str, Any]] = []
            linear_alert: Optional[ShoulderTapAlert] = None
            try:
                assigned_tickets = await self.sniff_assigned_tickets()
            except Exception as le:
                logger.warning(f"Error checking Linear assigned tickets in NoseSentinel.tick: {le}")
            self._last_assigned_tickets = assigned_tickets

            # Active Work Detection: ignore tickets that are actively being worked on
            # and suppress Backlog/Todo tickets tracked by an In Progress ticket in assigned_tickets
            in_progress_texts = [
                f"{t.get('title', '')} {t.get('description', '')}"
                for t in assigned_tickets
                if (
                    "in progress" in str(t.get("state") or "").lower()
                    or str(t.get("state_type") or "").lower() == "started"
                    or (isinstance(t.get("state"), dict) and "in progress" in str(t.get("state", {}).get("name") or "").lower())
                    or (isinstance(t.get("state"), dict) and str(t.get("state", {}).get("type") or "").lower() == "started")
                )
            ]

            actionable_tickets = []
            for t in assigned_tickets:
                state_str = str(t.get("state") or "").lower()
                state_type_str = str(t.get("state_type") or "").lower()
                if isinstance(t.get("state"), dict):
                    state_str = str(t.get("state", {}).get("name") or "").lower()
                    state_type_str = str(t.get("state", {}).get("type") or "").lower()

                ident = str(t.get("identifier") or "").strip()
                if ident and (state_str in ("todo", "backlog") or state_type_str in ("unstarted", "backlog", "triage")):
                    if any(ident in ip_text for ip_text in in_progress_texts):
                        continue

                if not self.is_ticket_actively_worked(t):
                    actionable_tickets.append(t)

            if actionable_tickets:
                ticket_sig = {f"{t.get('identifier')}:{t.get('state')}" for t in actionable_tickets}
                last_sig = getattr(self, "_last_alerted_ticket_signatures", set())
                newly_actionable = [
                    t for t in actionable_tickets
                    if f"{t.get('identifier')}:{t.get('state')}" not in last_sig
                ]
                self._last_alerted_ticket_signatures = ticket_sig
                if newly_actionable:
                    ticket_summaries = [f"[{t.get('identifier')}] {t.get('title')} ({t.get('state')})" for t in newly_actionable]
                    linear_alert = ShoulderTapAlert(
                        alert_type="linear_assigned_tickets",
                        source="nose",
                        timestamp=time.time(),
                        summary=f"The Nose detected {len(newly_actionable)} assigned Linear ticket(s) awaiting attention: " + ", ".join(ticket_summaries[:3]),
                        data={
                            "tickets": newly_actionable,
                            "count": len(newly_actionable),
                        },
                    )
                    if self.bus is not None:
                        try:
                            self.bus.publish(linear_alert)
                            logger.info(f"Published linear_assigned_tickets alert: {linear_alert.summary}")
                        except Exception as e:
                            logger.warning(f"Error publishing linear assigned tickets alert: {e}")
            else:
                self._last_alerted_ticket_signatures = set()

            # 4c. Assemble & Stream Real-Time Sniff Cycle Telemetry Receipt
            nodes_data = {}
            if hasattr(current_snapshot, "nodes") and isinstance(current_snapshot.nodes, dict):
                nodes_data = current_snapshot.nodes
            elif hasattr(current_snapshot, "get"):
                nodes_data = current_snapshot.get("nodes", {}) or {}

            total_nodes = len(nodes_data) if isinstance(nodes_data, dict) else 0
            online_nodes = (
                sum(
                    1
                    for n in nodes_data.values()
                    if (isinstance(n, dict) and n.get("online")) or (isinstance(n, bool) and n)
                )
                if isinstance(nodes_data, dict)
                else 0
            )
            fleet_str = f"{online_nodes}/{total_nodes} nodes online" if total_nodes > 0 else "0 nodes online"

            disk_data = {}
            if hasattr(current_snapshot, "disk") and isinstance(current_snapshot.disk, dict):
                disk_data = current_snapshot.disk
            elif hasattr(current_snapshot, "get"):
                disk_data = current_snapshot.get("disk", {}) or {}

            free_gb = float(disk_data.get("free_gb", 0.0) if hasattr(disk_data, "get") else 0.0)
            pct_used = float(disk_data.get("percent_used", 0.0) if hasattr(disk_data, "get") else 0.0)
            disk_pressure = bool(disk_data.get("pressure", False) if hasattr(disk_data, "get") else False)
            disk_str = f"{free_gb:.1f}GB free ({pct_used:.1f}%)"

            docker_data = {}
            if hasattr(current_snapshot, "docker") and isinstance(current_snapshot.docker, dict):
                docker_data = current_snapshot.docker
            elif hasattr(current_snapshot, "get"):
                docker_data = current_snapshot.get("docker", {}) or {}

            docker_running = bool(docker_data.get("docker_running", docker_data.get("running", False)) if hasattr(docker_data, "get") else False)
            raw_c_list = docker_data.get("containers", []) if hasattr(docker_data, "get") else []
            container_count = int(
                docker_data.get(
                    "container_count",
                    len(raw_c_list) if isinstance(raw_c_list, list) else 0,
                ) if hasattr(docker_data, "get") else 0
            )
            docker_status = "running" if docker_running else "stopped"
            docker_str = f"{docker_status} ({container_count} containers)"

            log_offset = int(getattr(self, "last_log_offset", 0))
            log_delta = int(getattr(self, "_last_log_delta_bytes", 0))
            if log_delta == 0 and raw:
                log_delta = len(raw.encode("utf-8", errors="replace"))
            error_count = int(self._last_error_count)
            bot_err_str = f"offset={log_offset}b (+{log_delta}b delta, {error_count} errs)"

            assigned_count = len(assigned_tickets)
            actionable_count = len(actionable_tickets)
            actionable_ids = [
                str(t.get("identifier") or t.get("id"))
                for t in actionable_tickets
                if t.get("identifier") or t.get("id")
            ]
            if actionable_count > 0:
                linear_str = f"{assigned_count} assigned ({actionable_count} actionable: {', '.join(actionable_ids[:3])})"
            else:
                linear_str = f"{assigned_count} assigned (0 actionable)"

            receipt_str = (
                f"[SNIFF] Fleet: {fleet_str} | Disk: {disk_str} | "
                f"Docker: {docker_str} | bot.err: {bot_err_str} | Linear: {linear_str}"
            )
            receipt_data = {
                "fleet": {
                    "total": total_nodes,
                    "online": online_nodes,
                    "nodes": nodes_data if isinstance(nodes_data, dict) else {},
                },
                "disk": {
                    "free_gb": free_gb,
                    "percent_used": pct_used,
                    "pressure": disk_pressure,
                },
                "docker": {
                    "running": docker_running,
                    "container_count": container_count,
                    "containers": raw_c_list if isinstance(raw_c_list, list) else [],
                },
                "bot_err": {
                    "offset": log_offset,
                    "delta_bytes": log_delta,
                    "error_count": error_count,
                },
                "linear": {
                    "assigned_count": assigned_count,
                    "actionable_count": actionable_count,
                    "ticket_ids": actionable_ids,
                    "tickets": actionable_tickets,
                },
                "timestamp": time.time(),
            }
            self.last_sniff_receipt = receipt_data
            self.last_sniff_receipt_str = receipt_str

            # Direct WebUI Server logging & SSE broadcast
            webui = getattr(self, "webui_server", None) or getattr(self, "server", None)
            if webui is not None:
                try:
                    if hasattr(webui, "append_organ_log"):
                        webui.append_organ_log("nose", receipt_str)
                    if hasattr(webui, "broadcast"):
                        webui.broadcast("nose_telemetry", {
                            "sniff_receipt": receipt_data,
                            "receipt_str": receipt_str,
                            "timestamp": receipt_data["timestamp"],
                        })
                except Exception as we:
                    logger.debug(f"WebUI server sniff receipt emit warning: {we}")

            # Shoulder Tap Bus broadcast
            if self.bus is not None:
                try:
                    sniff_alert = ShoulderTapAlert(
                        alert_type="nose_sniff",
                        source="nose",
                        timestamp=time.time(),
                        summary=receipt_str,
                        data=receipt_data,
                    )
                    self.bus.publish(sniff_alert)
                except Exception as be:
                    logger.warning(f"Error publishing nose_sniff alert to bus: {be}")

            # 5. Quiescent Zero-Regex Gate: 0 tokens if quiet
            if self.evaluator.is_quiescent(errors, deltas):
                logger.debug("NoseSentinel tick: Quiescent state, 0 errors, 0 actionable deltas.")
                self.last_verdict = AnomalyVerdict(
                    smells_off=False,
                    severity="low",
                    category="other",
                    summary="All systems nominal; 0 errors, fleet healthy.",
                    affected_nodes=[],
                    recommended_action="",
                )
                return linear_alert or (email_alerts[0] if email_alerts else None)

            # 6. Evaluate via NOSE_MODEL
            verdict = await self.evaluate_anomaly(errors, deltas)
            self.last_verdict = verdict

            # 7. Publish ShoulderTapAlert if issue smells off
            smells_off = False
            if verdict is not None:
                if hasattr(verdict, "smells_off"):
                    smells_off = bool(verdict.smells_off)
                elif isinstance(verdict, dict):
                    smells_off = bool(verdict.get("smells_off", False))

            if verdict and smells_off:
                summary = verdict.summary if hasattr(verdict, "summary") else verdict.get("summary", "Health anomaly detected")
                actionable_deltas = [
                    (d.to_dict() if hasattr(d, "to_dict") else dict(d))
                    for d in deltas
                    if not (d.get("benign") if isinstance(d, dict) else getattr(d, "benign", False))
                ]
                alert_data = verdict.to_dict() if hasattr(verdict, "to_dict") else dict(verdict)
                alert_data["deltas"] = actionable_deltas
                alert_data["error_line_count"] = len(errors)
                alert_data["error_sample"] = errors[-10:] if errors else []

                alert = ShoulderTapAlert(
                    alert_type="health_anomaly",
                    source="nose",
                    timestamp=time.time(),
                    summary=summary or "Fleet health anomaly detected by The Nose",
                    data=alert_data,
                )
                try:
                    self.bus.publish(alert)
                    logger.info(f"Published health_anomaly alert: {alert.summary}")
                except Exception as e:
                    logger.warning(f"Error publishing health anomaly alert: {e}")

                return alert

            return linear_alert or (email_alerts[0] if email_alerts else None)

    def start(self) -> asyncio.Task[Any]:
        """
        Start the background sentinel watchdog coroutine.
        Returns the created asyncio.Task.
        Non-blocking and idempotent.
        """
        if self._task is not None and not self._task.done():
            return self._task

        self._is_running = True
        self._shutdown_event.clear()

        if not self._is_calibrated:
            self.calibrate_startup()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()

        self._task = loop.create_task(self._run_loop())
        logger.info(
            f"NoseSentinel started (interval={self.interval_minutes}m / {self.interval_seconds}s, "
            f"model='{self.nose_model}', bot_err='{self.bot_err_path}')"
        )
        return self._task

    async def _run_loop(self) -> None:
        """Internal monitoring loop running every interval_seconds until shutdown."""
        while self._is_running and not self._shutdown_event.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error during NoseSentinel tick: {e}", exc_info=True)

            try:
                # Sleep interval interrupted immediately when shutdown_event is set
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.interval_seconds,
                )
                break  # shutdown_event was set
            except asyncio.TimeoutError:
                # Interval elapsed; proceed to next tick
                pass

        self._is_running = False

    async def stop(self) -> None:
        """
        Stop the background sentinel watchdog cleanly.
        Signals shutdown_event and cancels background task immediately.
        """
        self._is_running = False
        self._shutdown_event.set()
        if self._task is not None:
            if not self._task.done():
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error awaiting NoseSentinel task cancellation: {e}")
            self._task = None
        if getattr(self, "linear_bridge", None) is not None:
            if hasattr(self.linear_bridge, "close"):
                try:
                    res = self.linear_bridge.close()
                    if inspect.isawaitable(res):
                        await res
                except Exception:
                    pass
            self.linear_bridge = None
        logger.info("NoseSentinel stopped cleanly")

    async def get_telemetry_digest(self, trigger_fresh_sniff: bool = False) -> Dict[str, Any]:
        """
        Return structured telemetry dictionary from The Nose Sentinel.
        If trigger_fresh_sniff is False, returns cached snapshot immediately (<1ms).
        If trigger_fresh_sniff is True, runs tick() and returns fresh snapshot.
        On cold boot (last_snapshot is None), provides accurate default metrics without blocking.
        """
        if trigger_fresh_sniff:
            await self.tick()

        now = time.time()
        last_ts = None
        if self._last_tick_time is not None:
            last_ts = _safe_float(self._last_tick_time, 0.0)
            if last_ts <= 0:
                last_ts = None
        if last_ts is None and self.last_snapshot is not None:
            snap_ts = getattr(self.last_snapshot, "timestamp", None)
            if snap_ts is not None:
                last_ts = _safe_float(snap_ts, 0.0)
                if last_ts <= 0:
                    last_ts = None
        if last_ts is None:
            last_ts = now
        seconds_ago = max(0.0, round(now - last_ts, 1))
        minutes_ago = max(0.0, round(seconds_ago / 60.0, 1))
        last_check_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(last_ts))

        snap = self.last_snapshot
        disk_raw = (snap.disk if snap else {}) or {}
        docker_raw = (snap.docker if snap else {}) or {}
        ts_raw = (snap.tailscale if snap else {}) or {}

        # Disk
        disk_dict = {
            "free_gb": _safe_float(disk_raw.get("free_gb"), 0.0),
            "percent_used": _safe_float(disk_raw.get("percent_used"), 0.0),
            "pressure": bool(disk_raw.get("pressure", False) or False),
        }

        # Docker
        docker_running = bool(docker_raw.get("docker_running") or docker_raw.get("running", False) or False)
        docker_containers = docker_raw.get("containers", [])
        if not isinstance(docker_containers, list):
            docker_containers = list(docker_containers) if docker_containers else []
        container_count = _safe_int(docker_raw.get("container_count"), len(docker_containers))
        docker_dict = {
            "running": docker_running,
            "container_count": container_count,
            "containers": docker_containers,
        }

        # Tailscale
        ts_online = bool(ts_raw.get("online", False) or False)
        ts_backend = str(ts_raw.get("backend_state") or "Unknown")
        ts_peers = ts_raw.get("peers")
        if not isinstance(ts_peers, dict):
            ts_peers = {}

        tailscale_dict = {
            "online": ts_online,
            "backend_state": ts_backend,
            "peers": ts_peers,
        }

        smells_off = False
        verdict_summary = "All systems nominal; 0 errors, fleet healthy."
        if self.last_verdict:
            if hasattr(self.last_verdict, "smells_off"):
                smells_off = bool(self.last_verdict.smells_off)
                verdict_summary = str(self.last_verdict.summary or verdict_summary)
            elif isinstance(self.last_verdict, dict):
                smells_off = bool(self.last_verdict.get("smells_off", False))
                verdict_summary = str(self.last_verdict.get("summary") or verdict_summary)

        recent_err_count = _safe_int(getattr(self, "_last_error_count", 0), 0)
        assigned_tickets_list = list(getattr(self, "_last_assigned_tickets", []))

        digest: Dict[str, Any] = {
            "seconds_ago": seconds_ago,
            "minutes_ago": minutes_ago,
            "last_check_timestamp": last_check_str,
            "timestamp": last_ts,
            "smells_off": smells_off,
            "verdict_summary": verdict_summary,
            "disk": disk_dict,
            "docker": docker_dict,
            "tailscale": tailscale_dict,
            "email": self.email_sniffer.get_telemetry(),
            "assigned_tickets": assigned_tickets_list,
            "linear_assigned_tickets": assigned_tickets_list,
            "recent_error_count": recent_err_count,
            "error_count": recent_err_count,
        }
        return digest

    # Aliases
    get_telemetry = get_telemetry_digest
    get_status = get_telemetry_digest
    get_telemetry_snapshot = get_telemetry_digest


# ==============================================================================
# Live Probing Layer (WebUI real-time cadence)
# ==============================================================================

# Ports that signal an in-flight OAuth callback or an exposed debugger.
FLAGGED_PORTS: Dict[int, str] = {
    53682: "OAuth callback listener",
    9229: "Node.js inspector",
    5678: "debugpy",
    9222: "Chrome DevTools",
}

# Ephemeral range listeners are churn, not signal; only the watchlist is tracked there.
EPHEMERAL_PORT_FLOOR = 49152

DEFAULT_FLEET_TARGETS: Dict[str, Tuple[str, int]] = {
    "desktop-pc": ("100.80.59.45", 22),
    "oci-machine": ("100.113.173.33", 22),
}


def parse_fleet_targets(raw: Optional[str]) -> Dict[str, Tuple[str, int]]:
    """
    Parse GOLEM_NOSE_FLEET_TARGETS, e.g. "desktop-pc=100.80.59.45:22,oci-machine=100.113.173.33:22".
    Falls back to the documented fleet defaults when unset or unparseable.
    """
    if not raw:
        return dict(DEFAULT_FLEET_TARGETS)
    targets: Dict[str, Tuple[str, int]] = {}
    for item in raw.split(","):
        name, sep, addr = item.strip().partition("=")
        host, _, port = addr.strip().rpartition(":")
        if not (sep and name.strip() and host):
            continue
        try:
            targets[name.strip()] = (host, int(port))
        except ValueError:
            continue
    return targets or dict(DEFAULT_FLEET_TARGETS)


def _parse_proc_net_tcp(text: str) -> Set[int]:
    """Extract LISTEN-state local ports from /proc/net/tcp{,6} content."""
    ports: Set[int] = set()
    for line in text.splitlines()[1:]:
        cols = line.split()
        if len(cols) < 4 or cols[3] != "0A":
            continue
        try:
            ports.add(int(cols[1].rsplit(":", 1)[1], 16))
        except (IndexError, ValueError):
            continue
    return ports


def _parse_lsof_listen(text: str) -> Set[int]:
    """Extract listening ports from `lsof -nP -iTCP -sTCP:LISTEN -Fn` output."""
    ports: Set[int] = set()
    for line in text.splitlines():
        if not line.startswith("n"):
            continue
        tail = line.rsplit(":", 1)[-1].strip()
        if tail.isdigit():
            ports.add(int(tail))
    return ports


class LivePortSniffer:
    """Cross-platform local TCP listener enumeration (procfs on Linux, lsof elsewhere)."""

    def scan(self) -> Set[int]:
        """Blocking scan; call via asyncio.to_thread. Returns an empty set on total failure."""
        found: Set[int] = set()
        proc_ok = False
        for path in ("/proc/net/tcp", "/proc/net/tcp6"):
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    found |= _parse_proc_net_tcp(fh.read())
                proc_ok = True
            except OSError:
                continue
        if proc_ok:
            return found

        lsof = shutil.which("lsof")
        if lsof:
            try:
                import subprocess
                out = subprocess.run(
                    [lsof, "-nP", "-iTCP", "-sTCP:LISTEN", "-Fn"],
                    capture_output=True, text=True, timeout=2.0, check=False,
                ).stdout
                found |= _parse_lsof_listen(out)
            except Exception as e:
                logger.debug(f"LivePortSniffer lsof failed: {e}")
        return found


def read_memory_gauge() -> Dict[str, Any]:
    """Host memory gauge: {total_gb, used_gb, percent_used}; values None when unavailable."""
    gauge: Dict[str, Any] = {"total_gb": None, "used_gb": None, "percent_used": None}
    try:
        import psutil  # type: ignore
        vm = psutil.virtual_memory()
        gauge.update(
            total_gb=round(vm.total / 1024 ** 3, 1),
            used_gb=round((vm.total - vm.available) / 1024 ** 3, 1),
            percent_used=round(float(vm.percent), 1),
        )
        return gauge
    except Exception:
        pass
    try:
        info: Dict[str, int] = {}
        with open("/proc/meminfo", "r", encoding="utf-8") as fh:
            for line in fh:
                k, _, v = line.partition(":")
                info[k] = int(v.strip().split()[0])
        total_kb, avail_kb = info["MemTotal"], info["MemAvailable"]
        gauge.update(
            total_gb=round(total_kb / 1024 ** 2, 1),
            used_gb=round((total_kb - avail_kb) / 1024 ** 2, 1),
            percent_used=round((total_kb - avail_kb) / total_kb * 100.0, 1),
        )
    except Exception:
        pass
    return gauge


class FleetReachabilityProbe:
    """Concurrent TCP reachability checks for fleet nodes with a TTL cache."""

    def __init__(
        self,
        targets: Optional[Dict[str, Tuple[str, int]]] = None,
        timeout: float = 1.0,
        ttl: float = 10.0,
    ) -> None:
        self.targets = targets if targets is not None else parse_fleet_targets(
            os.environ.get("GOLEM_NOSE_FLEET_TARGETS")
        )
        self.timeout = timeout
        self.ttl = ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_at: float = 0.0

    async def _probe_one(self, name: str, host: str, port: int) -> Tuple[str, Dict[str, Any]]:
        started = time.monotonic()
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=self.timeout)
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
            except Exception:
                pass
            latency = round((time.monotonic() - started) * 1000.0, 1)
            return name, {"online": True, "status": "Online", "latency_ms": latency, "target": f"{host}:{port}"}
        except asyncio.TimeoutError:
            status = f"Timeout after {self.timeout:g}s"
        except Exception as e:
            status = f"Unreachable ({type(e).__name__})"
        return name, {"online": False, "status": status, "latency_ms": None, "target": f"{host}:{port}"}

    async def probe(self, force: bool = False) -> Dict[str, Dict[str, Any]]:
        now = time.monotonic()
        if not force and self._cache and (now - self._cache_at) < self.ttl:
            return {k: dict(v) for k, v in self._cache.items()}
        results = await asyncio.gather(*(self._probe_one(n, h, p) for n, (h, p) in self.targets.items()))
        self._cache = {name: info for name, info in results}
        self._cache_at = time.monotonic()
        return {k: dict(v) for k, v in self._cache.items()}


class NoseTelemetryTracker:
    """
    Separates continuous gauges from log-worthy events.

    `observe()` is called every tick but returns events ONLY for:
      1. a node changing state (including its first observation),
      2. a listening port opening or closing,
      3. a newly flagged anomaly (smells_off with a not-yet-reported summary).
    `explicit_check()` records an operator-requested health check (/health, !nose).
    Identical consecutive ticks therefore yield an empty event list.
    """

    def __init__(self) -> None:
        self._nodes: Dict[str, bool] = {}
        self._ports: Optional[Set[int]] = None
        self._anomaly_sig: Optional[str] = None

    @staticmethod
    def _event(kind: str, level: str, message: str, **extra: Any) -> Dict[str, Any]:
        ev = {"kind": kind, "level": level, "message": message, "timestamp": time.time()}
        ev.update(extra)
        return ev

    @staticmethod
    def _trackable(port: int) -> bool:
        return port < EPHEMERAL_PORT_FLOOR or port in FLAGGED_PORTS

    def observe(
        self,
        nodes: Dict[str, Dict[str, Any]],
        ports: Set[int],
        smells_off: bool = False,
        verdict_summary: str = "",
    ) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []

        for name in sorted(nodes):
            info = nodes[name]
            online = bool(info.get("online"))
            was = self._nodes.get(name)
            if was is online:
                continue
            self._nodes[name] = online
            if was is None:
                detail = f"{info.get('status', '')} {info.get('target', '')}".strip()
                events.append(self._event(
                    "node_state", "info" if online else "warn",
                    f"Fleet ping: {name} {'reachable' if online else 'unreachable'} ({detail})",
                    node=name, online=online,
                ))
            else:
                events.append(self._event(
                    "node_state", "info" if online else "warn",
                    f"{name} went {'online' if online else 'offline'}: {info.get('status', '')}".rstrip(": "),
                    node=name, online=online,
                ))

        tracked = {p for p in ports if self._trackable(p)}
        if self._ports is None:
            # Baseline: only surface flagged ports that are already listening.
            for p in sorted(tracked & set(FLAGGED_PORTS)):
                events.append(self._event(
                    "port_opened", "warn", f"Port {p} listening — {FLAGGED_PORTS[p]} (flagged)",
                    port=p, flagged=True,
                ))
        else:
            for p in sorted(tracked - self._ports):
                flagged = p in FLAGGED_PORTS
                events.append(self._event(
                    "port_opened", "warn" if flagged else "info",
                    f"Port {p} now listening" + (f" — {FLAGGED_PORTS[p]} (flagged)" if flagged else ""),
                    port=p, flagged=flagged,
                ))
            for p in sorted(self._ports - tracked):
                events.append(self._event(
                    "port_closed", "info",
                    f"Port {p} closed" + (f" — {FLAGGED_PORTS[p]}" if p in FLAGGED_PORTS else ""),
                    port=p, flagged=p in FLAGGED_PORTS,
                ))
        self._ports = tracked

        if smells_off:
            sig = verdict_summary or "anomaly"
            if sig != self._anomaly_sig:
                self._anomaly_sig = sig
                events.append(self._event("anomaly", "warn", f"Anomaly: {sig}", smells_off=True))
        else:
            self._anomaly_sig = None

        return events

    def note_anomaly(self, summary: str) -> Optional[Dict[str, Any]]:
        """Record an anomaly delivered out-of-band (e.g. bus alert); returns None if already reported."""
        sig = summary or "anomaly"
        if sig == self._anomaly_sig:
            return None
        self._anomaly_sig = sig
        return self._event("anomaly", "warn", f"Anomaly: {sig}", smells_off=True)

    def explicit_check(self, summary: str, smells_off: bool = False) -> Dict[str, Any]:
        """Always returns an event; explicit checks are never deduplicated."""
        return self._event(
            "health_check", "warn" if smells_off else "info",
            f"Health check: {summary}" if summary else "Health check executed",
            smells_off=bool(smells_off),
        )


class NoseLiveProbe:
    """Per-tick live sampler: local listening ports, fleet reachability, memory."""

    def __init__(
        self,
        fleet_probe: Optional[FleetReachabilityProbe] = None,
        port_sniffer: Optional[LivePortSniffer] = None,
    ) -> None:
        self.fleet_probe = fleet_probe or FleetReachabilityProbe()
        self.port_sniffer = port_sniffer or LivePortSniffer()

    async def sample(self) -> Dict[str, Any]:
        ports, nodes, memory = await asyncio.gather(
            asyncio.to_thread(self.port_sniffer.scan),
            self.fleet_probe.probe(),
            asyncio.to_thread(read_memory_gauge),
        )
        shown = sorted(p for p in ports if p < EPHEMERAL_PORT_FLOOR or p in FLAGGED_PORTS)
        return {
            "ports": shown,
            "port_set": set(ports),
            "flagged_ports": [{"port": p, "label": FLAGGED_PORTS[p]} for p in shown if p in FLAGGED_PORTS],
            "fleet_nodes": nodes,
            "memory": memory,
        }

