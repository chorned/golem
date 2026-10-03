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
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List, Set, Union, Tuple, Literal

from golem.sensory.bus import (
    ShoulderTapAlert,
    ShoulderTapEventBus,
    get_default_event_bus,
)
from golem.config import get_nose_model as _config_get_nose_model
from golem.agent.mouth import get_gemini_api_key

logger = logging.getLogger("Golem.Sensory.Nose")

# Project root path resolution
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_BOT_ERR_PATH = str(_PROJECT_ROOT / "bot.err")

# Max slice bounding: 32KB
DEFAULT_MAX_SLICE_BYTES = 32768

# Default check interval in minutes
DEFAULT_NOSE_INTERVAL_MINUTES = 15.0

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"

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
        return ("timestamp", "nodes", "tailscale", "disk", "docker")

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
            data = json.loads(raw_json)
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
                deltas.append(StatusDelta(
                    category="node_online",
                    target=node_name,
                    description=f"Fleet node '{node_name}' is back online ({curr_info.get('status')})",
                    previous_value=prev_info.get("status"),
                    current_value=curr_info.get("status"),
                    benign=True,
                    severity="info",
                ))

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

        if self._is_mock_env():
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

        url = GEMINI_API_URL.format(model=self.model, key=key)
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload, headers={"Content-Type": "application/json"}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candidates = data.get("candidates", [])
                    if candidates:
                        parts = candidates[0].get("content", {}).get("parts", [])
                        text_chunks = [p.get("text", "") for p in parts if "text" in p]
                        raw_json = "".join(text_chunks).strip()
                        raw_json = re.sub(r"^```json\s*", "", raw_json)
                        raw_json = re.sub(r"\s*```$", "", raw_json)
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
                else:
                    logger.warning(f"NOSE_MODEL API returned HTTP {resp.status}: {await resp.text()}")

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
    ) -> None:
        self.fleet = fleet
        self.bus = event_bus or bus or get_default_event_bus()
        self.event_bus = self.bus

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
            return self.last_log_offset
        except OSError as e:
            logger.warning(f"Error calibrating log startup for {self.bot_err_path}: {e}")
            self.last_log_offset = 0
            self.last_log_inode = None
            self._is_calibrated = True
            return 0

    def sniff_bot_err(self) -> Tuple[List[str], str]:
        """
        Sniff newly appended lines in bot.err starting from last_log_offset.
        Returns 2-tuple: (error_lines, raw_slice_text).
        """
        if not os.path.exists(self.bot_err_path):
            self.last_log_offset = 0
            self.last_log_inode = None
            return [], ""

        try:
            st = os.stat(self.bot_err_path)
        except OSError as e:
            logger.warning(f"Error stating log file {self.bot_err_path}: {e}")
            return [], ""

        current_size = st.st_size
        current_inode = getattr(st, "st_ino", None)

        if not self._is_calibrated:
            self.calibrate_startup()
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
            return [], ""

        bytes_to_read = current_size - self.last_log_offset
        read_start = self.last_log_offset

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
        return await self.fleet_sniffer.capture_snapshot()

    async def sniff_fleet(self) -> FleetSnapshot:
        return await self.capture_snapshot()

    def compute_status_deltas(self, prev: Optional[Any], curr: Any) -> List[StatusDelta]:
        return self.delta_calculator.compute_deltas(prev, curr)

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

            # 4. Quiescent Zero-Regex Gate: 0 tokens if quiet
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
                return None

            # 5. Evaluate via NOSE_MODEL
            verdict = await self.evaluate_anomaly(errors, deltas)
            self.last_verdict = verdict

            # 6. Publish ShoulderTapAlert if issue smells off
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

            return None

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
            "recent_error_count": recent_err_count,
            "error_count": recent_err_count,
        }
        return digest

    # Aliases
    get_telemetry = get_telemetry_digest
    get_status = get_telemetry_digest
