#!/usr/bin/env python3
"""
golem.execution.harness - Dual-Pane Execution Harness.
Manages the lifecycle of a Dual-Pane Workspace (Pane 1 Liaison + Pane 2 Runner):
- Pane 1: Liaison pane hosting the Chief of Staff (agy) engine for reasoning and orchestration.
- Pane 2: Dedicated interactive shell pane for non-blocking command execution.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, Union

from golem.outbox import OutboxManager, get_default_outbox_manager

try:
    from herdr_client import HerdrClient
except ImportError:
    HerdrClient = None  # type: ignore

logger = logging.getLogger("Golem.Execution.Harness")


class OpaquePaneCoordinate(str):
    """
    Authoritative opaque physical coordinate (e.g. 'ws-m4-1:p2', 'w2J:p2')
    with dual-compatibility equality to 'runner'.
    """

    def __new__(cls, coord: str, legacy_label: str = "runner"):
        obj = super().__new__(cls, coord)
        obj._legacy_label = legacy_label
        obj._coord = coord
        return obj

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return str(self) == other or self._legacy_label == other
        return super().__eq__(other)

    def __hash__(self) -> int:
        return hash(str(self))


class DualPaneHarness:
    """
    Manages the lifecycle of a Dual-Pane Workspace.
    Decouples conversational orchestration (Pane 1: liaison agy) from
    interactive shell execution (Pane 2: runner shell).
    """

    def __init__(
        self,
        herdr_client: Optional[Any] = None,
        default_cwd: Optional[str] = None,
        outbox: Optional[OutboxManager] = None,
        router: Optional[Any] = None,
    ):
        self.herdr_client = herdr_client or (HerdrClient() if HerdrClient is not None else None)
        self.default_cwd = default_cwd or "/Users/hermes"
        self.outbox = outbox or get_default_outbox_manager()
        self.router = router
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
        import re
        if re.match(r"^[a-z][a-z0-9_-]{0,23}$", session_id):
            liaison_name = f"{session_id}-liaison"
        else:
            clean = re.sub(r"[^a-z0-9_-]", "_", session_id.lower()).strip("_")
            liaison_name = f"g_{clean}"[:32] if clean else "golem_liaison"
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
            "--pane", root_pane_id
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
            logger.debug(f"herdr pane create returned non-zero ({runner_code}); falling back to 'pane split': {runner_err or runner_out}")
            runner_cmd = [
                "pane", "split", root_pane_id,
                "--direction", "right",
                "--no-focus"
            ]
            runner_code, runner_out, runner_err = await self.herdr_client.run_cli(runner_cmd)
            if runner_code != 0:
                logger.warning(f"herdr pane split returned non-zero ({runner_code}): {runner_err or runner_out}")
            else:
                try:
                    data = json.loads(runner_out)
                    pane_obj = (data.get("result", {}) or {}).get("pane") or data.get("pane") or {}
                    split_pid = pane_obj.get("pane_id") or data.get("pane_id")
                    if split_pid:
                        await self.herdr_client.run_cli(["pane", "rename", split_pid, runner_pane_label])
                except Exception:
                    pass

        runner_pane_id = runner_pane_label
        terminal_id = None
        if runner_code == 0 and runner_out:
            try:
                data = json.loads(runner_out)
                pane_obj = (data.get("result", {}) or {}).get("pane") or data.get("pane") or {}
                runner_pane_id = pane_obj.get("pane_id") or data.get("pane_id") or runner_pane_label
                terminal_id = pane_obj.get("terminal_id") or data.get("terminal_id")
            except Exception:
                pass

        opaque_hand_pane = OpaquePaneCoordinate(runner_pane_id, legacy_label=runner_pane_label)

        # Provision session outbox directory
        session_outbox = self.outbox.get_session_artifact_dir(session_id)

        # Record tracking state
        self._workspaces[session_id] = {
            "workspace_id": ws_id,
            "session_id": session_id,
            "session_name": session_id,
            "liaison_agent": liaison_name,
            "liaison_pane": root_pane_id,
            "runner_pane": runner_pane_label,
            "runner_pane_id": opaque_hand_pane,
            "hand_pane": opaque_hand_pane,
            "hand_pane_id": opaque_hand_pane,
            "terminal_id": terminal_id,
            "cwd": effective_cwd,
            "created_at": time.time(),
            "status": "active",
            "outbox_path": str(session_outbox),
        }

        return f"{session_id}-liaison", opaque_hand_pane

    async def allocate_hand_tab(
        self,
        session_id: str,
        label: Optional[str] = None,
        cwd: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Dynamically allocate an isolated Herdr tab per Hand to prevent buffer interleaving.
        Returns dict containing tab_id, pane_id, and terminal_id.
        """
        ws_info = self.get_workspace(session_id)
        ws_id = ws_info.get("workspace_id", session_id) if ws_info else session_id
        effective_cwd = cwd or (ws_info.get("cwd") if ws_info else self.default_cwd)
        tab_label = label or f"{session_id}-hand-{int(time.time())}"

        cmd = [
            "tab", "create",
            "--workspace", ws_id,
            "--label", tab_label,
            "--cwd", effective_cwd,
            "--no-focus"
        ]
        code, out, err = await self.herdr_client.run_cli(cmd)
        if code != 0:
            raise RuntimeError(f"Failed to allocate hand tab in workspace '{ws_id}': {err or out}")

        tab_id, pane_id, term_id = None, None, None
        try:
            data = json.loads(out)
            res = data.get("result", {})
            tab_id = (res.get("tab") or {}).get("tab_id")
            pane_obj = res.get("pane") or res.get("root_pane") or {}
            pane_id = pane_obj.get("pane_id")
            term_id = pane_obj.get("terminal_id")
        except Exception as e:
            logger.warning(f"Could not parse tab create response: {e}")

        pane_id_str = pane_id or f"{ws_id}:p2"
        opaque_coord = OpaquePaneCoordinate(pane_id_str, "runner")
        return {
            "workspace_id": ws_id,
            "tab_id": tab_id or f"{ws_id}:t2",
            "pane_id": opaque_coord,
            "terminal_id": term_id,
            "cwd": effective_cwd,
            "label": tab_label,
            "allocated_at": time.time(),
        }

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
            "runner_pane_id": ws_info.get("runner_pane_id", runner_pane),
            "hand_pane": ws_info.get("hand_pane", runner_pane),
            "hand_pane_id": ws_info.get("hand_pane_id", runner_pane),
            "terminal_id": ws_info.get("terminal_id"),
            "cwd": cwd,
            "channel": channel,
            "thread_ts": thread_ts,
            "outbox_path": ws_info.get("outbox_path", ""),
            "status": "active"
        }

    async def verify_terminal_id(self, pane: str, terminal_id: str) -> bool:
        """
        Verify that the given terminal_id is active and attached to the target pane.
        Prevents pre-write execution on stale or closed PTYs.
        """
        if not terminal_id or not self.herdr_client:
            return True
        try:
            code, out, _ = await self.herdr_client.run_cli(
                ["pane", "process-info", "--pane", str(pane)],
                timeout=1.0
            )
            if code == 0 and out:
                data = json.loads(out)
                res = data.get("result", {}) if isinstance(data, dict) else {}
                active_tid = res.get("terminal_id") or data.get("terminal_id")
                if active_tid:
                    return str(active_tid) == str(terminal_id)
            return True
        except Exception as e:
            logger.debug(f"Terminal ID verification error for pane '{pane}': {e}")
            return True

    async def execute_command(
        self,
        session_id: str,
        command: str,
        yield_timeout: float = 3.0,
        pane: Optional[str] = None,
        terminal_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a command in the workspace Hand pane with strict 3.0s non-blocking yield.
        Resolves authoritative opaque coordinate from workspace metadata.
        Validates terminal_id before dispatch if provided.
        Returns TaskHandle(status='running' | 'completed' | 'error').
        """
        ws = self.get_workspace(session_id)
        target_pane = (
            pane
            or (ws.get("hand_pane_id") if ws else None)
            or (ws.get("runner_pane_id") if ws else None)
            or (ws.get("runner_pane") if ws else None)
            or f"{session_id}:p2"
        )
        effective_term_id = terminal_id or (ws.get("terminal_id") if ws else None)
        if effective_term_id and self.herdr_client:
            is_valid = await self.verify_terminal_id(str(target_pane), effective_term_id)
            if not is_valid:
                logger.warning(
                    f"Pre-write validation: terminal_id '{effective_term_id}' not active on '{target_pane}'"
                )

        from golem.runner import RunnerExecutor
        executor = getattr(self, "_runner_executor", None)
        if executor is None or getattr(executor, "herdr_client", None) is not self.herdr_client:
            executor = RunnerExecutor(herdr_client=self.herdr_client)
            self._runner_executor = executor
        return await executor.run_command(
            pane=str(target_pane),
            command=command,
            yield_timeout=yield_timeout,
            session_id=session_id,
            opaque_coordinate=str(target_pane),
            terminal_id=effective_term_id,
            **kwargs,
        )

    async def run_command(
        self,
        session_id: str,
        command: str,
        yield_timeout: float = 3.0,
        pane: Optional[str] = None,
        terminal_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """Alias for execute_command."""
        return await self.execute_command(
            session_id=session_id,
            command=command,
            yield_timeout=yield_timeout,
            pane=pane,
            terminal_id=terminal_id,
            **kwargs,
        )

    def get_session_outbox(self, session_id: str) -> Path:
        """Return the dedicated outbox artifact directory for a session."""
        return self.outbox.get_session_artifact_dir(session_id)

    def stage_artifact(
        self,
        session_id: str,
        content_or_path: Any,
        filename: str,
        **kwargs: Any,
    ) -> Any:
        """Stage an artifact into the session's outbox directory."""
        ws = self.get_workspace(session_id)
        target_channel = kwargs.pop("channel", ws.get("channel") if ws else None)
        target_thread_ts = kwargs.pop("thread_ts", ws.get("thread_ts") if ws else None)
        meta = self.outbox.stage_artifact(
            session_id=session_id,
            content_or_path=content_or_path,
            filename=filename,
            channel=target_channel,
            thread_ts=target_thread_ts,
            **kwargs,
        )
        if hasattr(self, "router") and self.router and hasattr(self.router, "sessions"):
            sess = self.router.sessions.get(session_id)
            if sess and hasattr(sess, "record_artifact"):
                sess.record_artifact(meta)
        return meta

    def list_session_artifacts(self, session_id: str) -> List[Any]:
        """List all artifacts generated during this session."""
        return self.outbox.list_artifacts(session_id=session_id)

    async def share_artifact(
        self,
        session_id: str,
        path: Optional[Union[str, Path]] = None,
        client: Optional[Any] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        title: Optional[str] = None,
        artifact_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Share a session artifact strictly sandboxed to outbox."""
        ws = self.get_workspace(session_id)
        target_channel = channel or (ws.get("channel") if ws else None)
        target_thread = thread_ts or (ws.get("thread_ts") if ws else None)
        sess = None
        if hasattr(self, "router") and self.router and hasattr(self.router, "sessions"):
            sess = self.router.sessions.get(session_id)
        from golem.outbox import share_artifact
        return await share_artifact(
            path=path,
            client=client,
            channel=target_channel,
            thread_ts=target_thread,
            title=title,
            artifact_id=artifact_id,
            session_id=session_id,
            session=sess,
            outbox_manager=self.outbox,
        )

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

    async def dispatch_hand(
        self,
        session_id: str,
        task_description: str,
        target_node: str = "localhost",
        worker: str = "agy",
        cwd: Optional[str] = None,
        yield_timeout: float = 3.0,
        precedent_status: Optional[str] = None,
        precedent_source: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Prepend pre-flight node invariants and dispatch command to runner pane.
        """
        invariants = get_preflight_node_invariants(target_node)
        effective_task = task_description
        if invariants:
            effective_task = f"[Node Context & Invariants]\n{invariants}\n\n[Task]\n{task_description}"

        subtask_id = kwargs.get("subtask_issue_id")
        if subtask_id:
            if hasattr(self, "router") and self.router and hasattr(self.router, "sessions"):
                sess = self.router.sessions.get(session_id)
                if sess:
                    sess.linear_issue_id = subtask_id
            try:
                from golem.linear_bridge import LinearBridge
                linear_bridge = LinearBridge()
                asyncio.create_task(linear_bridge.update_issue_state(subtask_id, "in progress"))
            except Exception as e:
                logger.debug(f"Failed to update subtask {subtask_id} to in progress: {e}")

        # Run Linear creation as a detached task
        hand_issue_holder: Dict[str, Optional[str]] = {"id": None}
        create_task_ref = None
        hand_title = f"[Hand] {task_description[:60]}"
        if not subtask_id and os.getenv("LINEAR_API_KEY"):
            async def _safe_linear_create():
                try:
                    from golem.linear_bridge import LinearBridge
                    linear_bridge = LinearBridge()
                    existing_hand = await linear_bridge.find_open_hand_issue(hand_title)
                    issue = existing_hand or await linear_bridge.create_task_issue(
                        title=hand_title,
                        description=f"Node: {target_node}\nCWD: {cwd}\nCommand: `{task_description}`"
                    )
                    if isinstance(issue, dict) and issue.get("id"):
                        hand_issue_holder["id"] = issue.get("id")
                    if hasattr(self, "router") and self.router and hasattr(self.router, "sessions"):
                        sess = self.router.sessions.get(session_id)
                        if sess:
                            sess.linear_issue_id = issue.get("id")
                            sess.linear_issue_key = issue.get("identifier")
                except Exception as e:
                    logger.warning(f"Linear background issue creation skipped: {e}")

            create_task_ref = asyncio.create_task(_safe_linear_create())

        res = None
        try:
            res = await self.execute_command(
                session_id=session_id,
                command=effective_task,
                yield_timeout=yield_timeout,
                precedent_status=precedent_status,
                precedent_source=precedent_source,
                target_node=target_node,
                task_description=task_description,
                **kwargs,
            )
            return res
        finally:
            if not subtask_id and os.getenv("LINEAR_API_KEY"):
                async def _settle_hand():
                    if create_task_ref and not create_task_ref.done():
                        try:
                            await asyncio.wait_for(asyncio.shield(create_task_ref), timeout=2.0)
                        except Exception:
                            pass
                    hand_id = hand_issue_holder.get("id")
                    if hand_id:
                        try:
                            from golem.linear_bridge import LinearBridge
                            linear_bridge = LinearBridge()
                            exit_c = getattr(res, "exit_code", None)
                            if exit_c is None and isinstance(res, dict):
                                exit_c = res.get("exit_code")
                            settle_status = "SUCCESS" if exit_c == 0 else "CANCELED"
                            await linear_bridge.settle_hand_issue(hand_id, status=settle_status)
                        except Exception as e:
                            logger.debug(f"Linear hand settlement skipped in harness: {e}")

                asyncio.create_task(_settle_hand())


def get_preflight_node_invariants(target_node: str = "localhost") -> str:
    """
    Retrieve pre-flight node facts and topology from wiki:
    Checks context/rules/<target_node>.md and context/topology.md.
    """
    invariants = []
    try:
        from golem.wiki import GolemWiki
        wiki = GolemWiki()
        node_name = (target_node or "localhost").strip().lower()
        if node_name == "localhost":
            node_name = "hermes"

        node_rule_path = f"context/rules/{node_name}.md"
        if wiki.exists(node_rule_path):
            content = wiki.read(node_rule_path).strip()
            if content:
                invariants.append(f"--- Node Invariants ({node_name}) ---\n{content}")

        if wiki.exists("context/topology.md"):
            topo_content = wiki.read("context/topology.md").strip()
            if topo_content:
                invariants.append(f"--- Fleet Topology ---\n{topo_content}")
    except Exception as e:
        logger.debug(f"Error reading preflight node invariants: {e}")

    return "\n\n".join(invariants)


__all__ = [
    "OpaquePaneCoordinate",
    "DualPaneHarness",
    "get_preflight_node_invariants",
]
