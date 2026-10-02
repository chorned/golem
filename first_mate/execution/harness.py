#!/usr/bin/env python3
"""
first_mate.execution.harness - Dual-Pane Execution Harness.
Manages the lifecycle of a Dual-Pane Workspace (Pane 1 Liaison + Pane 2 Runner):
- Pane 1: Liaison pane hosting the Chief of Staff (agy) engine for reasoning and orchestration.
- Pane 2: Dedicated interactive shell pane for non-blocking command execution.
"""

import json
import logging
import time
from typing import Dict, Any, Optional, Tuple, List

from herdr_client import HerdrClient

logger = logging.getLogger("FirstMate.Execution.Harness")


class DualPaneHarness:
    """
    Manages the lifecycle of a Dual-Pane Workspace.
    Decouples conversational orchestration (Pane 1: liaison agy) from
    interactive shell execution (Pane 2: runner shell).
    """

    def __init__(
        self,
        herdr_client: Optional[HerdrClient] = None,
        default_cwd: Optional[str] = None
    ):
        self.herdr_client = herdr_client or HerdrClient()
        self.default_cwd = default_cwd or "/Users/hermes"
        self._workspaces: Dict[str, Dict[str, Any]] = {}

    @property
    def active_workspaces(self) -> Dict[str, Dict[str, Any]]:
        """Return dict of currently active dual-pane workspaces."""
        return {k: v for k, v in self._workspaces.items() if v.get("status") == "active"}

    def get_workspace(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Fetch workspace metadata by session ID or workspace ID."""
        if session_id in self._workspaces:
            return self._workspaces[session_id]
        for ws in self._workspaces.values():
            if ws.get("workspace_id") == session_id:
                return ws
        return None

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """Return list of all registered workspace metadata dictionaries."""
        return list(self._workspaces.values())

    async def provision_dual_pane(
        self,
        session_id: str,
        cwd: str
    ) -> Tuple[str, str]:
        """
        Creates and provisions a dual-pane workspace:
        - Creates Herdr workspace with label session_id and target cwd.
        - Spawns Pane 1: liaison agy agent (session_id-liaison).
        - Spawns Pane 2: runner interactive shell pane (label: runner).

        Returns:
            Tuple[str, str]: (liaison_pane, runner_pane)
        """
        effective_cwd = cwd or self.default_cwd
        liaison_name = f"{session_id}-liaison"
        runner_pane_label = "runner"

        # 1. Create Herdr workspace
        ws_cmd = [
            "workspace", "create",
            "--label", session_id,
            "--cwd", effective_cwd,
            "--no-focus"
        ]
        ws_code, ws_out, ws_err = await self.herdr_client.run_cli(ws_cmd)
        ws_id = session_id
        root_pane_id = "1"

        if ws_code == 0 and ws_out:
            try:
                data = json.loads(ws_out)
                res = data.get("result", {})
                ws_id = res.get("workspace", {}).get("workspace_id") or ws_id
                root_pane_id = res.get("root_pane", {}).get("pane_id") or root_pane_id
            except Exception as e:
                logger.debug(f"Could not parse workspace create output as JSON: {e}")

        # 2. Spawn Pane 1 (liaison): Launch agy agent in root pane
        liaison_cmd = [
            "agent", "start", liaison_name,
            "--kind", "agy",
            "--pane", "1"
        ]
        start_code, start_out, start_err = await self.herdr_client.run_cli(liaison_cmd)
        if start_code != 0:
            logger.warning(f"herdr agent start returned non-zero ({start_code}): {start_err or start_out}")

        # 3. Spawn Pane 2 (runner): Persistent shell pane
        runner_cmd = [
            "pane", "create",
            "--workspace", session_id,
            "--label", runner_pane_label
        ]
        runner_code, runner_out, runner_err = await self.herdr_client.run_cli(runner_cmd)
        if runner_code != 0:
            logger.warning(f"herdr pane create returned non-zero ({runner_code}): {runner_err or runner_out}")

        runner_pane_id = runner_pane_label
        if runner_code == 0 and runner_out:
            try:
                data = json.loads(runner_out)
                runner_pane_id = data.get("result", {}).get("pane", {}).get("pane_id") or runner_pane_label
            except Exception:
                pass

        # Record tracking state
        self._workspaces[session_id] = {
            "workspace_id": ws_id,
            "session_id": session_id,
            "session_name": session_id,
            "liaison_agent": liaison_name,
            "liaison_pane": root_pane_id,
            "runner_pane": runner_pane_label,
            "runner_pane_id": runner_pane_id,
            "cwd": effective_cwd,
            "created_at": time.time(),
            "status": "active"
        }

        return liaison_name, runner_pane_label

    async def create_workspace(
        self,
        session_name: str,
        cwd: str,
        channel: str = "",
        thread_ts: str = ""
    ) -> Dict[str, Any]:
        """
        Creates a dual-pane workspace with channel and thread tracking.
        Returns detailed workspace metadata dictionary.
        """
        liaison_pane, runner_pane = await self.provision_dual_pane(
            session_id=session_name,
            cwd=cwd
        )

        ws_info = self._workspaces.get(session_name, {})
        ws_info.update({
            "channel": channel,
            "thread_ts": thread_ts,
            "status": "active"
        })
        return {
            "workspace_id": ws_info.get("workspace_id", session_name),
            "session_name": session_name,
            "session_id": session_name,
            "liaison_agent": ws_info.get("liaison_agent", liaison_pane),
            "liaison_pane": ws_info.get("liaison_pane", "1"),
            "runner_pane": runner_pane,
            "cwd": cwd,
            "channel": channel,
            "thread_ts": thread_ts,
            "status": "active"
        }

    async def teardown(self, session_id: str) -> None:
        """
        Tears down a dual-pane workspace by session_id or workspace_id, closing all associated panes.
        Idempotent: if already closed, returns immediately without re-executing CLI.
        """
        ws_info = self.get_workspace(session_id)
        if ws_info and ws_info.get("status") == "closed":
            return

        target_ws_id = ws_info.get("workspace_id", session_id) if ws_info else session_id

        await self.herdr_client.run_cli(["workspace", "close", target_ws_id])
        if ws_info:
            ws_info["status"] = "closed"
            ws_info["closed_at"] = time.time()
        elif session_id in self._workspaces:
            self._workspaces[session_id]["status"] = "closed"
            self._workspaces[session_id]["closed_at"] = time.time()

    async def close_workspace(self, workspace_id: str) -> None:
        """
        Closes a workspace by workspace_id or session label.
        """
        await self.teardown(workspace_id)
