#!/usr/bin/env python3
"""
first_mate.execution - Dual-Pane Execution Harness & Herdr Fleet Mesh.
Decouples conversational orchestration from terminal execution across the fleet.
"""

from typing import Dict, Any, Optional

from first_mate.execution.harness import DualPaneHarness
from first_mate.execution.runner import ExecutionHandle, RunnerExecutor
from first_mate.execution.buffer import RunnerBufferObserver
from first_mate.execution.delegation import WorkerDelegator, sanitize_worker_output
from first_mate.execution.mesh import (
    FleetMesh,
    TOPOLOGY_SPECS,
    NODE_ALIASES,
    wrap_windows_command,
)


async def herdr_pane_run(
    pane: str = "runner",
    command: str = "",
    workspace: Optional[str] = None,
    yield_timeout: float = 3.0,
) -> Any:
    """Canonical runner execution delegating to RunnerExecutor."""
    executor = RunnerExecutor()
    return await executor.run_command(pane=pane, command=command, yield_timeout=yield_timeout)


async def herdr_agent_read(
    pane: str = "runner",
    lines: int = 30,
) -> str:
    """Canonical buffer reading delegating to RunnerBufferObserver."""
    observer = RunnerBufferObserver()
    return await observer.read_buffer(pane=pane, lines=lines)


async def herdr_handoff(
    subagent: str = "opencode",
    task: str = "",
    cwd: Optional[str] = None,
    node: str = "hermes",
    timeout_ms: int = 120000,
    isolate_branch: bool = True,
) -> Dict[str, Any]:
    """Canonical worker delegation delegating to WorkerDelegator."""
    delegator = WorkerDelegator()
    return await delegator.handoff(
        subagent=subagent,
        task=task,
        cwd=cwd,
        node=node,
        timeout_ms=timeout_ms,
        isolate_branch=isolate_branch,
    )


__all__ = [
    # Dual-Pane Harness
    "DualPaneHarness",
    # Runner Execution
    "ExecutionHandle",
    "RunnerExecutor",
    # Buffer Observation & Blocker Detection
    "RunnerBufferObserver",
    # Subagent Delegation Sandbox
    "WorkerDelegator",
    "sanitize_worker_output",
    # Fleet Mesh & Concurrency
    "FleetMesh",
    "TOPOLOGY_SPECS",
    "NODE_ALIASES",
    "wrap_windows_command",
    # Canonical Execution Tools
    "herdr_pane_run",
    "herdr_agent_read",
    "herdr_handoff",
]
