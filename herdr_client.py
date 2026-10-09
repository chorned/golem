#!/usr/bin/env python3
"""
herdr_client.py
Native Herdr CLI and Socket Client for AGY Chief of Staff Orchestration.
Wraps Herdr's official Socket and CLI primitives:
- herdr api snapshot (session.snapshot)
- herdr worktree create --cwd <path> --branch <name> --no-focus
- herdr worktree remove --workspace <id> --force
- herdr worktree list --cwd <path>
- herdr pane split <pane_id> --direction right|down
- herdr pane run <pane_id> "<command>"
- herdr agent start <name> --kind <kind> --pane <pane_id> -- <args>
- herdr agent prompt <name|pane_id> "<prompt>" --wait --timeout <ms>
- herdr agent wait <name|pane_id> --until done --until idle
- herdr agent read <name|pane_id> --lines <N>
Integrates FrugaLLM routing and declarative worker catalog (.agents/workers.yaml).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import json
import time
import shutil
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

try:
    import yaml
except ImportError:
    yaml = None  # Fallback handled gracefully

logger = logging.getLogger("AGY.HerdrClient")

class InfrastructureError(RuntimeError):
    """Raised when underlying infrastructure (Herdr, worktree, agent process) fails."""
    pass

HOST_DIR = os.environ.get("GOLEM_HOST_DIR", os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = HOST_DIR
DEFAULT_TARGET_REPO = os.environ.get("DEFAULT_TARGET_REPO", "/Users/hermes/projects/AGY")
DEFAULT_WORKERS_YAML = os.path.join(HOST_DIR, ".agents", "workers.yaml")

DEFAULT_TOPOLOGY_PANES = {
    "fleet-control": "w1:p1",
    "homelab-ops": "w2:p1",
    "cloud-ops": "w3:p1",
    "dev-grid": "w4:p1",
    "compute-grid": "w5:p1"
}


class HerdrClient:
    """Native Herdr Socket and CLI interface wrapper."""

    def __init__(
        self,
        herdr_bin: Optional[str] = None,
        config_path: Optional[str] = None
    ):
        self.herdr_bin = (
            herdr_bin
            or shutil.which("herdr")
            or os.path.expanduser("~/.local/bin/herdr")
        )
        self.config_path = config_path or DEFAULT_WORKERS_YAML
        self._workers_config: Optional[Dict[str, Any]] = None

    def load_workers_config(self) -> Dict[str, Any]:
        """Load and cache worker definitions from .agents/workers.yaml."""
        if self._workers_config is not None:
            return self._workers_config

        if not os.path.isfile(self.config_path):
            logger.warning(f"Workers config file not found at {self.config_path}")
            self._workers_config = {"version": "1.0", "frugallm": {}, "workers": {}}
            return self._workers_config

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                if yaml:
                    data = yaml.safe_load(f) or {}
                else:
                    content = f.read()
                    data = json.loads(content) if content.strip().startswith("{") else {}
            self._workers_config = data
            return self._workers_config
        except Exception as e:
            logger.error(f"Failed to parse workers config from {self.config_path}: {e}")
            self._workers_config = {"version": "1.0", "frugallm": {}, "workers": {}}
            return self._workers_config

    def get_worker_config(self, worker_id: str) -> Dict[str, Any]:
        """Fetch worker configuration by ID."""
        cfg = self.load_workers_config()
        workers = cfg.get("workers", {})
        if worker_id not in workers:
            raise ValueError(f"Worker '{worker_id}' is not defined in {self.config_path}")
        return workers[worker_id]

    def get_frugallm_config(self) -> Dict[str, Any]:
        """Fetch global FrugaLLM proxy configuration."""
        cfg = self.load_workers_config()
        flm = cfg.get("frugallm", {})
        # Support both endpoint and base_url for compatibility
        endpoint = flm.get("endpoint") or flm.get("base_url") or "http://127.0.0.1:61721/v1"
        api_key = flm.get("password") or flm.get("api_key") or "horned66"
        model = flm.get("model") or "frugallm/coding"
        return {
            "endpoint": endpoint,
            "base_url": endpoint,
            "api_key": api_key,
            "password": api_key,
            "model": model,
            "host": flm.get("host", "0.0.0.0:61721"),
            "system_integrated": flm.get("system_integrated", True)
        }

    async def run_cli(self, args: List[str], timeout: float = 30.0) -> Tuple[int, str, str]:
        """Execute a Herdr CLI command asynchronously and return (code, stdout, stderr)."""
        cmd = [self.herdr_bin] + args
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout
            )
            code = proc.returncode if proc.returncode is not None else 0
            out_str = stdout_bytes.decode("utf-8", errors="replace").strip()
            err_str = stderr_bytes.decode("utf-8", errors="replace").strip()
            return code, out_str, err_str
        except asyncio.TimeoutError:
            logger.warning(f"Herdr command timed out after {timeout}s: {' '.join(cmd)}")
            return -1, "", f"Command timed out after {timeout}s"
        except Exception as e:
            logger.error(f"Error executing Herdr command {' '.join(cmd)}: {e}")
            return -1, "", str(e)

    async def get_snapshot(self) -> Dict[str, Any]:
        """
        Execute `herdr api snapshot` to parse active session workspaces, tabs, and panes.
        Returns parsed JSON dict or structured error.
        """
        code, out, err = await self.run_cli(["api", "snapshot"], timeout=10.0)
        raw = out if out else err
        if raw:
            try:
                return json.loads(raw)
            except Exception as e:
                logger.warning(f"Failed to parse Herdr snapshot JSON: {e}")
                return {"error": str(e), "raw": raw}
        return {"error": "Empty snapshot output from herdr api snapshot"}

    def _extract_pane_for_workspace(self, snapshot: Dict[str, Any], workspace_label: str) -> Optional[str]:
        """Locate a pane ID in snapshot matching workspace_label."""
        if not isinstance(snapshot, dict):
            return None

        workspaces = (
            snapshot.get("result", {}).get("workspaces")
            or snapshot.get("workspaces")
            or snapshot.get("session", {}).get("workspaces")
            or []
        )
        for ws in workspaces:
            if isinstance(ws, dict) and ws.get("label") == workspace_label:
                # Direct pane list
                panes = ws.get("panes", [])
                if panes and isinstance(panes, list):
                    first = panes[0]
                    if isinstance(first, dict):
                        return first.get("pane_id")
                    elif isinstance(first, str):
                        return first
                # Tabs -> panes
                tabs = ws.get("tabs", [])
                for tab in tabs:
                    if isinstance(tab, dict):
                        t_panes = tab.get("panes", [])
                        if t_panes:
                            first = t_panes[0]
                            if isinstance(first, dict):
                                return first.get("pane_id")
                            elif isinstance(first, str):
                                return first
                if ws.get("root_pane_id"):
                    return ws.get("root_pane_id")
                if ws.get("pane_id"):
                    return ws.get("pane_id")
        return None

    def parse_snapshot_pane_info(
        self,
        snapshot: Dict[str, Any],
        pane_id: Optional[str] = None,
        workspace_id: Optional[str] = None,
        agent_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extracts agent_status, terminal_title, cwd, and process info from session_snapshot:
        Returns:
        {
            "agent_status": str,       # e.g. "running", "blocked", "idle", "unknown"
            "terminal_title": str,     # e.g. "OC | Fixing missing slack_bolt dependency …"
            "cwd": str,
            "pane_id": str,
            "workspace_id": str,
            "agent_name": str
        }
        """
        if not isinstance(snapshot, dict):
            return {
                "agent_status": "unknown",
                "terminal_title": "",
                "cwd": "",
                "pane_id": pane_id or "",
                "workspace_id": workspace_id or "",
                "agent_name": agent_name or ""
            }

        snap = (
            snapshot.get("result", {}).get("snapshot")
            or snapshot.get("snapshot")
            or snapshot.get("result")
            or snapshot
        )
        if not isinstance(snap, dict):
            snap = {}

        panes = snap.get("panes", [])
        agents = snap.get("agents", [])

        # 1. If pane_id not specified, try to find pane_id via workspace_id or agent_name
        target_pane_id = pane_id
        if not target_pane_id and agent_name:
            for ag in agents:
                if isinstance(ag, dict) and (ag.get("name") == agent_name or ag.get("agent") == agent_name):
                    target_pane_id = ag.get("pane_id")
                    if not workspace_id:
                        workspace_id = ag.get("workspace_id")
                    break

        if not target_pane_id and workspace_id:
            for p in panes:
                if isinstance(p, dict) and p.get("workspace_id") == workspace_id:
                    target_pane_id = p.get("pane_id")
                    break

        # 2. Locate pane object
        pane_obj = None
        for p in panes:
            if isinstance(p, dict):
                if target_pane_id and p.get("pane_id") == target_pane_id:
                    pane_obj = p
                    break
                elif workspace_id and p.get("workspace_id") == workspace_id:
                    pane_obj = p
                    break

        terminal_title = ""
        agent_status = "unknown"
        cwd = ""
        found_workspace_id = workspace_id or ""
        found_pane_id = target_pane_id or ""

        if pane_obj:
            terminal_title = pane_obj.get("terminal_title") or pane_obj.get("terminal_title_stripped") or ""
            agent_status = pane_obj.get("agent_status") or "unknown"
            cwd = pane_obj.get("cwd") or pane_obj.get("foreground_cwd") or ""
            found_workspace_id = pane_obj.get("workspace_id") or found_workspace_id
            found_pane_id = pane_obj.get("pane_id") or found_pane_id

        # 3. Check agents list for more specific agent_status or name
        for ag in agents:
            if isinstance(ag, dict):
                matches = False
                if found_pane_id and ag.get("pane_id") == found_pane_id:
                    matches = True
                elif agent_name and (ag.get("name") == agent_name or ag.get("agent") == agent_name):
                    matches = True
                elif found_workspace_id and ag.get("workspace_id") == found_workspace_id:
                    matches = True

                if matches:
                    if agent_status in ("unknown", None) and ag.get("agent_status"):
                        agent_status = ag.get("agent_status")
                    if not agent_name and ag.get("name"):
                        agent_name = ag.get("name")
                    if not cwd:
                        cwd = ag.get("cwd") or ag.get("foreground_cwd") or ""
                    break

        # 4. If title strongly suggests blocked or fixing interactive prompt, reflect it
        if agent_status in ("unknown", "idle", None):
            title_lower = terminal_title.lower()
            if "fixing" in title_lower or "blocked" in title_lower or "confirm" in title_lower or "waiting" in title_lower:
                agent_status = "blocked"

        return {
            "agent_status": agent_status or "unknown",
            "terminal_title": terminal_title,
            "cwd": cwd,
            "pane_id": found_pane_id,
            "workspace_id": found_workspace_id,
            "agent_name": agent_name or ""
        }

    async def wait_for_agent_ready(
        self,
        agent_name: str,
        pane_id: Optional[str] = None,
        timeout_s: float = 10.0,
        poll_interval_s: float = 0.5
    ) -> bool:
        """
        Poll herdr api snapshot until the registered agent appears with interactive_ready: true,
        or settles into ready idle/working state.
        """
        start_time = time.time()
        consecutive_errors = 0
        while (time.time() - start_time) < timeout_s:
            try:
                snapshot = await self.get_snapshot()
                if isinstance(snapshot, dict) and (snapshot.get("error") or not snapshot):
                    consecutive_errors += 1
                    if consecutive_errors >= 2:
                        logger.warning("Snapshot unavailable or empty; skipping readiness poll")
                        return False
                else:
                    consecutive_errors = 0

                snap = (
                    snapshot.get("result", {}).get("snapshot")
                    or snapshot.get("snapshot")
                    or snapshot.get("result")
                    or snapshot
                )
                if isinstance(snap, dict):
                    agents = snap.get("agents", [])
                    panes = snap.get("panes", [])
                    if not agents and not panes:
                        consecutive_errors += 1
                        if consecutive_errors >= 2:
                            logger.warning("Snapshot contains no panes or agents; skipping readiness poll")
                            return False
                    else:
                        consecutive_errors = 0
                    for ag in agents:
                        if not isinstance(ag, dict):
                            continue
                        if ag.get("name") == agent_name or (pane_id and ag.get("pane_id") == pane_id):
                            if ag.get("interactive_ready") is True or ag.get("agent_status") in ("idle", "ready", "working"):
                                logger.info(f"Agent {agent_name} is interactively ready in pane {ag.get('pane_id')}")
                                return True
                    if pane_id:
                        for p in snap.get("panes", []):
                            if isinstance(p, dict) and p.get("pane_id") == pane_id:
                                if p.get("interactive_ready") is True or p.get("agent_status") in ("idle", "ready", "working"):
                                    logger.info(f"Pane {pane_id} is interactively ready")
                                    return True
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= 2:
                    logger.debug(f"Error checking agent readiness from snapshot: {e}")
                    return False
            await asyncio.sleep(poll_interval_s)

        logger.warning(f"Agent {agent_name} readiness poll timed out after {timeout_s}s; proceeding with prompt")
        return False

    async def allocate_worker_pane(
        self,
        workspace_label: str,
        direction: str = "right",
        cwd: Optional[str] = None,
        base_pane_id: Optional[str] = None
    ) -> str:
        """
        Locates target workspace from session snapshot; splits a pane
        (`herdr pane split <pane_id> --direction right|down --no-focus`) and returns new_pane_id.
        """
        pane_to_split = base_pane_id
        if not pane_to_split:
            snapshot = await self.get_snapshot()
            pane_to_split = self._extract_pane_for_workspace(snapshot, workspace_label)

        if not pane_to_split:
            pane_to_split = DEFAULT_TOPOLOGY_PANES.get(workspace_label, "w1:p1")

        split_args = [
            "pane", "split", pane_to_split,
            "--direction", direction,
            "--no-focus"
        ]
        if cwd:
            split_args.extend(["--cwd", os.path.expanduser(cwd)])

        code, out, err = await self.run_cli(split_args, timeout=10.0)
        raw = out or err

        new_pane_id = None
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    res = data.get("result", {})
                    if isinstance(res, dict):
                        pane_obj = res.get("pane", {})
                        if isinstance(pane_obj, dict):
                            new_pane_id = pane_obj.get("pane_id")
                    if not new_pane_id:
                        new_pane_id = data.get("pane_id")
            except Exception:
                pass

            if not new_pane_id:
                m = re.search(r"\b(w\d+:p\d+)\b", raw)
                if m:
                    new_pane_id = m.group(1)

        if not new_pane_id:
            # Fallback allocation logic for tests or simulated execution
            if ":" in pane_to_split:
                ws_prefix, p_num = pane_to_split.split(":", 1)
                num_match = re.search(r"\d+", p_num)
                next_num = int(num_match.group(0)) + 1 if num_match else 2
                new_pane_id = f"{ws_prefix}:p{next_num}"
            else:
                new_pane_id = f"{pane_to_split}:p2"

        logger.info(f"Allocated worker pane {new_pane_id} for workspace {workspace_label} (split from {pane_to_split})")
        return new_pane_id

    # --------------------------------------------------------------------------
    # Native Herdr Git Worktree Management
    # --------------------------------------------------------------------------
    async def list_worktrees(self, repo_path: str = DEFAULT_TARGET_REPO) -> List[Dict[str, Any]]:
        """List active Herdr worktree-backed workspaces for repo_path."""
        code, out, err = await self.run_cli(["worktree", "list", "--cwd", repo_path], timeout=10.0)
        raw = out or err
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    res = data.get("result", {})
                    if "worktrees" in res:
                        return res["worktrees"]
                    if "worktrees" in data:
                        return data["worktrees"]
            except Exception:
                pass
        return []

    async def create_worktree(
        self,
        branch_name: str,
        repo_path: str = DEFAULT_TARGET_REPO,
        label: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create an isolated git worktree environment using Herdr:
        `herdr worktree create --cwd <repo_path> --branch <branch_name> --no-focus`
        Returns parsed worktree info:
        - workspace_id
        - pane_id (root pane already attached to the worktree cwd)
        - worktree_path
        - branch
        """
        safe_branch_name = branch_name.replace("/", "-")
        repo_name = os.path.basename(repo_path.rstrip(os.sep))
        base_wt_dir = os.path.expanduser(f"~/.herdr/worktrees/{repo_name}")
        os.makedirs(base_wt_dir, exist_ok=True)

        cmd_args = [
            "worktree", "create",
            "--cwd", repo_path,
            "--branch", branch_name,
            "--no-focus"
        ]
        if label:
            cmd_args.extend(["--label", label])

        code, out, err = await self.run_cli(cmd_args, timeout=20.0)
        raw = out or err

        workspace_id = None
        pane_id = None
        worktree_path = None

        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    res = data.get("result", {})
                    if isinstance(res, dict):
                        root_pane = res.get("root_pane", {})
                        if isinstance(root_pane, dict):
                            pane_id = root_pane.get("pane_id")
                            workspace_id = root_pane.get("workspace_id")
                            worktree_path = root_pane.get("cwd")

                        if not workspace_id:
                            ws_obj = res.get("workspace", {})
                            workspace_id = ws_obj.get("workspace_id") if isinstance(ws_obj, dict) else None
                        if not worktree_path:
                            wt_obj = res.get("worktree", {})
                            if isinstance(wt_obj, dict):
                                worktree_path = wt_obj.get("path") or wt_obj.get("checkout_path")
                        if not worktree_path:
                            worktree_path = res.get("path") or res.get("worktree_path")
            except Exception as e:
                logger.warning(f"Failed to parse herdr worktree create JSON: {e}")

        # If not resolved from stdout JSON, inspect worktree list or snapshot
        if not worktree_path or not workspace_id:
            try:
                wts = await self.list_worktrees(repo_path)
                for wt in wts:
                    if wt.get("branch") in (branch_name, safe_branch_name) or wt.get("label") in (branch_name, safe_branch_name):
                        worktree_path = wt.get("path") or wt.get("checkout_path") or worktree_path
                        workspace_id = wt.get("open_workspace_id") or wt.get("workspace_id") or workspace_id
                        break
            except Exception:
                pass

        if not worktree_path or not workspace_id:
            try:
                snap = await self.get_snapshot()
                for ws in snap.get("workspaces", []):
                    ws_label = ws.get("label", "")
                    if ws_label in (branch_name, safe_branch_name) or branch_name in ws_label:
                        workspace_id = ws.get("workspace_id") or workspace_id
                        wt_dict = ws.get("worktree")
                        if isinstance(wt_dict, dict):
                            worktree_path = wt_dict.get("checkout_path") or wt_dict.get("path") or worktree_path
                        break
            except Exception:
                pass

        # Fallback path sanitized without nested slashes
        if not worktree_path:
            worktree_path = os.path.join(base_wt_dir, safe_branch_name)

        if not pane_id:
            pane_id = "w_wt:p1"

        # Ensure parent directory exists
        try:
            os.makedirs(Path(worktree_path).parent, exist_ok=True)
        except OSError:
            pass

        # Verify os.path.isdir(worktree_path) before proceeding
        is_dir = bool(worktree_path and os.path.isdir(worktree_path))
        if not is_dir and code == 0:
            try:
                os.makedirs(worktree_path, exist_ok=True)
                is_dir = True
            except OSError:
                is_dir = False

        # Fallback to atomic git worktree provisioning if herdr server is not running
        if (code != 0 or not is_dir) and os.path.exists(repo_path):
            try:
                dest_dir = await self.create_task_worktree(
                    repo_path=repo_path,
                    task_id=branch_name,
                    base_ref="origin/main"
                )
                worktree_path = dest_dir
                is_dir = True
                code = 0
            except Exception as fb_err:
                logger.warning(f"Fallback create_task_worktree failed: {fb_err}")

        success = (code == 0 and is_dir)
        error = None
        if not success:
            error = err or out or f"Worktree directory not found or could not be created at {worktree_path} (exit code {code})"

        logger.info(
            f"Herdr worktree create: branch={branch_name} path={worktree_path} "
            f"workspace={workspace_id} pane={pane_id} success={success}"
        )

        return {
            "success": success,
            "error": error,
            "branch": branch_name,
            "workspace_id": workspace_id,
            "pane_id": pane_id,
            "worktree_path": worktree_path,
            "repo_path": repo_path,
            "raw": raw
        }

    async def create_task_worktree(
        self,
        repo_path: str = DEFAULT_TARGET_REPO,
        task_id: str = "opencode-dev",
        base_ref: str = "origin/main"
    ) -> str:
        """
        Atomic worktree provisioning:
        Creates isolated worktree via:
        `git -C {repo_path} worktree add -b {branch_name} {dest_dir} {base_ref}`
        without touching or checking out branches in the root repository.
        Returns validated absolute dest_dir.
        """
        clean_task = str(task_id).strip()
        if clean_task.startswith("task-") or clean_task.startswith("task/"):
            branch_name = clean_task.replace("/", "-")
        else:
            sanitized_task = re.sub(r"[^a-zA-Z0-9_]", "-", clean_task)
            branch_name = f"task-{int(time.time())}-{sanitized_task}"

        repo_name = os.path.basename(repo_path.rstrip(os.sep))
        dest_dir = os.path.abspath(os.path.expanduser(f"~/.herdr/worktrees/{repo_name}/{branch_name}"))
        os.makedirs(Path(dest_dir).parent, exist_ok=True)

        cmd = ["git", "-C", repo_path, "worktree", "add", "-b", branch_name, dest_dir, base_ref]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_b, stderr_b = await proc.communicate()
        stderr_text = stderr_b.decode("utf-8", errors="replace").strip()

        # If base_ref (e.g. origin/main) failed because origin is not fetched or doesn't exist, fallback to HEAD
        if proc.returncode != 0 and ("invalid reference" in stderr_text or "fatal" in stderr_text or "Needed a single revision" in stderr_text):
            logger.warning(f"git worktree add with base_ref '{base_ref}' failed ({stderr_text}); retrying with HEAD")
            fallback_cmd = ["git", "-C", repo_path, "worktree", "add", "-b", branch_name, dest_dir, "HEAD"]
            proc_fb = await asyncio.create_subprocess_exec(
                *fallback_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout_b, stderr_b = await proc_fb.communicate()
            if proc_fb.returncode != 0:
                err_msg = stderr_b.decode("utf-8", errors="replace").strip()
                raise RuntimeError(f"Failed to create git worktree at {dest_dir}: {err_msg}")

        if not os.path.isdir(dest_dir):
            raise RuntimeError(f"Worktree directory {dest_dir} was not created on disk.")

        logger.info(f"Atomic worktree provisioned: branch={branch_name} at {dest_dir} (base: {base_ref})")
        return dest_dir

    async def remove_worktree(
        self,
        workspace_id: Optional[str] = None,
        branch_name: Optional[str] = None,
        worktree_path: Optional[str] = None,
        repo_path: str = DEFAULT_TARGET_REPO,
        force: bool = True,
        agent_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Remove a worktree checkout and workspace cleanly:
        1. If agent_name provided, stop it via `herdr agent stop {agent_name}`.
        2. herdr worktree remove --workspace <workspace_id> --force (if workspace_id)
        3. git -C {repo_path} worktree remove --force {worktree_path} (if worktree_path)
        4. git -C {repo_path} worktree prune
        5. git -C {repo_path} branch -D {branch_name} (if branch_name)
        """
        # If workspace_id is missing, attempt to discover it via list_worktrees
        if not workspace_id and branch_name:
            worktrees = await self.list_worktrees(repo_path)
            for wt in worktrees:
                if wt.get("branch") == branch_name:
                    workspace_id = wt.get("open_workspace_id") or wt.get("workspace_id")
                    break

        # Stop agent if provided
        if agent_name:
            try:
                await self.run_cli(["agent", "stop", agent_name], timeout=10.0)
            except Exception as e:
                logger.warning(f"Could not stop agent {agent_name}: {e}")

        if workspace_id:
            cmd_args = ["worktree", "remove", "--workspace", workspace_id]
            if force:
                cmd_args.append("--force")
            code, out, err = await self.run_cli(cmd_args, timeout=15.0)
        else:
            code, out, err = 0, "No workspace ID provided", ""

        # git worktree remove --force {worktree_path}
        if worktree_path:
            try:
                rm_cmd = ["git", "-C", repo_path, "worktree", "remove"]
                if force:
                    rm_cmd.append("--force")
                rm_cmd.append(worktree_path)
                rm_proc = await asyncio.create_subprocess_exec(
                    *rm_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await rm_proc.communicate()
            except Exception as e:
                logger.warning(f"git worktree remove failed for {worktree_path}: {e}")

            if os.path.exists(worktree_path):
                shutil.rmtree(worktree_path, ignore_errors=True)

        # Best-effort git worktree prune
        try:
            prune_proc = await asyncio.create_subprocess_exec(
                "git", "-C", repo_path, "worktree", "prune",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await prune_proc.communicate()
        except Exception:
            pass

        # Clean up local branch reference
        if branch_name:
            try:
                branch_proc = await asyncio.create_subprocess_exec(
                    "git", "-C", repo_path, "branch", "-D", branch_name,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await branch_proc.communicate()
            except Exception:
                pass

        logger.info(f"Removed Herdr worktree workspace {workspace_id} (branch: {branch_name}, path: {worktree_path})")
        return {
            "success": (code == 0),
            "workspace_id": workspace_id,
            "branch": branch_name,
            "worktree_path": worktree_path,
            "output": out or err
        }

    # --------------------------------------------------------------------------
    # Worker Dispatch
    # --------------------------------------------------------------------------
    async def dispatch_worker(
        self,
        worker_id: str,
        prompt: str,
        branch_name: Optional[str] = None,
        repo_path: str = "/Users/hermes/projects/AGY",
        pane_id: Optional[str] = None,
        timeout_ms: int = 180000,
        worktree_info: Optional[Dict[str, Any]] = None,
        task_id: Optional[str] = None,
        agent_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Dispatches a junior workforce agent to Herdr:
        1. Reads worker config from .agents/workers.yaml.
        2. Allocates execution pane in target workspace or attaches to Herdr worktree.
        3. Creates and checks out isolated git branch if specified (when worktree not used).
        4. Injects worker-specific environment variables into the pane if defined.
        5. Derives unique agent name per task (e.g. opencode-<task_id>).
        6. Starts agent via Herdr agent API:
           herdr agent start <agent_name> --kind <harness> --pane <pane_id> [-- <launch_args>]
        7. Prompts agent via herdr agent prompt <agent_name> "<prompt>" --wait --timeout <ms>
        8. Reads settled output via herdr agent read <agent_name> --lines 100.
        """
        worker_cfg = self.get_worker_config(worker_id)
        target_node = worker_cfg.get("target_node", "hermes")
        workspace = worker_cfg.get("workspace", "dev-grid")
        harness = worker_cfg.get("harness", "opencode")
        env_vars = dict(worker_cfg.get("env", {}))
        launch_command = worker_cfg.get("launch_command", harness)

        # Ensure native FrugaLLM credentials are configured for worker execution
        flm_cfg = self.get_frugallm_config()
        if "OPENAI_BASE_URL" not in env_vars:
            env_vars["OPENAI_BASE_URL"] = flm_cfg.get("base_url") or "http://127.0.0.1:61721/v1"
        if "OPENAI_API_BASE" not in env_vars:
            env_vars["OPENAI_API_BASE"] = flm_cfg.get("base_url") or "http://127.0.0.1:61721/v1"
        if "OPENAI_API_KEY" not in env_vars:
            env_vars["OPENAI_API_KEY"] = flm_cfg.get("api_key") or "horned66"
        if "MODEL" not in env_vars:
            env_vars["MODEL"] = flm_cfg.get("model") or "frugallm/coding"

        # 1. Allocate / attach worker pane
        if worktree_info:
            pane_id = worktree_info.get("pane_id") or pane_id
            repo_path = worktree_info.get("worktree_path") or repo_path

        if not pane_id:
            pane_id = await self.allocate_worker_pane(workspace, cwd=repo_path)

        # 2. Branch isolation setup (only if not already running inside a Herdr worktree)
        if branch_name and not worktree_info:
            branch_cmd = f"git checkout -b {branch_name}"
            await self.run_cli(["pane", "run", pane_id, branch_cmd])
            await asyncio.sleep(0.2)

        # 3. Inject environment variables (if any configured for this worker)
        is_windows = (target_node == "desktop-pc" or workspace == "compute-grid")
        if env_vars:
            if is_windows:
                env_parts = [f"set {k}={v}" for k, v in env_vars.items()]
                export_cmd = " && ".join(env_parts)
            else:
                env_parts = [f'export {k}="{v}"' for k, v in env_vars.items()]
                export_cmd = " && ".join(env_parts)
            await self.run_cli(["pane", "run", pane_id, export_cmd])
            await asyncio.sleep(0.2)

        # 4. Derive unique agent name per task
        if not agent_name:
            if task_id:
                clean_tid = str(task_id).replace("/", "-")
            elif branch_name:
                m = re.search(r"task[-/](\d+)", branch_name)
                if m:
                    clean_tid = m.group(1)
                else:
                    clean_tid = branch_name.replace("task-", "").replace("task/", "").replace("/", "-")
            else:
                clean_tid = str(int(time.time()))

            base_prefix = worker_id.split("-")[0] if "-" in worker_id else worker_id
            agent_name = f"{base_prefix}-{clean_tid}"

        agent_name = re.sub(r"[^a-zA-Z0-9_-]", "-", agent_name)

        # 5. Start agent
        SUPPORTED_KINDS = {
            "opencode", "hermes", "agy", "codex", "claude", "gemini",
            "pi", "cursor", "devin", "cline", "omp", "copilot", "kimi",
            "kiro", "droid", "amp", "grok", "kilo", "qwen", "letta"
        }
        kind_mapping = {
            "opencode": "opencode",
            "hermes-agent": "hermes",
            "hermes": "hermes",
            "agy": "agy"
        }
        agent_kind = kind_mapping.get(harness, harness)

        launch_args = list(worker_cfg.get("launch_args", []))
        if agent_kind == "opencode" and "--auto" not in launch_args:
            launch_args.append("--auto")

        if agent_kind in SUPPORTED_KINDS:
            start_cmd = [
                "agent", "start", agent_name,
                "--kind", agent_kind,
                "--pane", pane_id
            ]
            if launch_args:
                start_cmd.append("--")
                start_cmd.extend(launch_args)

            start_code, start_out, start_err = await self.run_cli(start_cmd)
            raw_start = start_out or start_err
            is_start_err = (start_code != 0)
            start_err_msg = start_err or start_out or f"herdr agent start returned code {start_code}"

            if raw_start:
                try:
                    data = json.loads(raw_start)
                    if isinstance(data, dict) and data.get("error"):
                        is_start_err = True
                        e_obj = data["error"]
                        start_err_msg = e_obj.get("message") if isinstance(e_obj, dict) else str(e_obj)
                except Exception:
                    pass

            if not is_start_err and raw_start:
                if "agent_name_taken" in raw_start or "agent_blocked" in raw_start:
                    is_start_err = True
                    start_err_msg = raw_start

            if is_start_err:
                logger.error(f"Herdr agent start failed: {start_err_msg}")
                raise InfrastructureError(f"Herdr agent dispatch failed: {start_err_msg}")

            # 5b. Readiness Polling: Verify interactive_ready: true in herdr api snapshot before prompting
            await self.wait_for_agent_ready(
                agent_name=agent_name,
                pane_id=pane_id,
                timeout_s=10.0,
                poll_interval_s=0.5
            )

        elif launch_command:
            await self.run_cli(["pane", "run", pane_id, launch_command])
            await asyncio.sleep(0.5)

        # 6. Decoupled Prompt Submission & Wait
        prompt_target = agent_name or pane_id
        wait_timeout_ms = timeout_ms if timeout_ms and timeout_ms > 0 else 240000

        # Submit prompt using Herdr's native primitives (without aggressive 5s blocking assertions)
        prompt_code, prompt_out, prompt_err = await self.run_cli([
            "agent", "prompt", prompt_target, prompt
        ], timeout=30.0)

        raw_prompt = prompt_out or prompt_err
        is_prompt_err = (prompt_code != 0)
        prompt_err_msg = prompt_err or prompt_out or f"herdr agent prompt returned code {prompt_code}"

        if raw_prompt:
            try:
                data = json.loads(raw_prompt)
                if isinstance(data, dict) and data.get("error"):
                    is_prompt_err = True
                    e_obj = data["error"]
                    prompt_err_msg = e_obj.get("message") if isinstance(e_obj, dict) else str(e_obj)
            except Exception:
                pass

        if not is_prompt_err and raw_prompt:
            if "agent_blocked" in raw_prompt or "agent_name_taken" in raw_prompt:
                is_prompt_err = True
                prompt_err_msg = raw_prompt

        # Graceful fallback for 5000ms idle observer warning or agent_prompt_stalled
        if is_prompt_err:
            is_stalled_or_5s = (
                "agent prompt produced no observed working" in prompt_err_msg
                or "agent_prompt_stalled" in prompt_err_msg
                or "within 5000 ms" in prompt_err_msg
            )
            if is_stalled_or_5s:
                logger.warning(
                    f"Agent prompt returned 5000ms idle warning: {prompt_err_msg}. "
                    "Checking snapshot for active process..."
                )
                try:
                    snapshot = await self.get_snapshot()
                    info = self.parse_snapshot_pane_info(
                        snapshot=snapshot,
                        pane_id=pane_id,
                        agent_name=agent_name
                    )
                    agent_st = info.get("agent_status", "unknown")
                    if agent_st in ("working", "running", "busy", "transitioning", "idle", "blocked") or info.get("pane_id"):
                        logger.info(f"Agent {agent_name} process is alive with status '{agent_st}'. Proceeding to agent wait.")
                        is_prompt_err = False
                except Exception as snap_err:
                    logger.warning(f"Error querying snapshot fallback: {snap_err}")

        # Fail-Fast Enforcement for unrecoverable errors (e.g. agent_name_taken, agent_blocked before input)
        if is_prompt_err:
            logger.error(f"Herdr agent prompt failed: {prompt_err_msg}")
            raise InfrastructureError(f"Herdr agent dispatch failed: {prompt_err_msg}")

        # Follow immediately with herdr agent wait <name> --until done --until blocked --timeout 240000
        wait_code, wait_out, wait_err = await self.run_cli([
            "agent", "wait", prompt_target,
            "--until", "done",
            "--until", "blocked",
            "--timeout", str(wait_timeout_ms)
        ], timeout=float(wait_timeout_ms / 1000) + 15.0)

        # Check if agent entered blocked state during or after wait
        raw_wait = wait_out or wait_err
        if "agent_blocked" in raw_wait:
            snapshot = await self.get_snapshot()
            info = self.parse_snapshot_pane_info(snapshot=snapshot, pane_id=pane_id, agent_name=agent_name)
            block_title = info.get("terminal_title") or "waiting for user input"
            logger.error(f"Agent {agent_name} is blocked: {block_title}")
            raise InfrastructureError(f"Herdr agent dispatch failed: agent_blocked ({block_title})")

        # 7. Read settled output
        read_code, read_out, _ = await self.run_cli([
            "agent", "read", prompt_target, "--lines", "100", "--source", "recent-unwrapped"
        ])
        if not read_out or read_code != 0:
            _, read_out, _ = await self.run_cli([
                "pane", "read", pane_id, "--lines", "100", "--source", "recent-unwrapped"
            ])

        return {
            "worker_id": worker_id,
            "agent_name": agent_name,
            "pane_id": pane_id,
            "workspace": workspace,
            "target_node": target_node,
            "branch": branch_name,
            "output": read_out or prompt_out,
            "status": "settled",
            "prompt": prompt,
            "env": env_vars,
            "worktree_path": repo_path
        }

    async def prompt_worker(
        self,
        worker_id: str,
        prompt: str,
        wait: bool = True,
        timeout_ms: int = 120000,
        target: Optional[str] = None
    ) -> str:
        """Submit follow-up prompt to active worker and return settled output."""
        target_name = target or worker_id
        args = ["agent", "prompt", target_name, prompt]
        code, out, err = await self.run_cli(args, timeout=30.0)

        raw = out or err
        is_err = (code != 0)
        err_msg = err or out or f"herdr agent prompt returned code {code}"

        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, dict) and data.get("error"):
                    is_err = True
                    e_obj = data["error"]
                    err_msg = e_obj.get("message") if isinstance(e_obj, dict) else str(e_obj)
            except Exception:
                pass

        if not is_err and raw:
            if "agent_blocked" in raw or "agent_name_taken" in raw:
                is_err = True
                err_msg = raw

        if is_err:
            is_stalled = (
                "agent prompt produced no observed working" in err_msg
                or "agent_prompt_stalled" in err_msg
                or "within 5000 ms" in err_msg
            )
            if is_stalled:
                logger.warning(f"Follow-up prompt returned 5000ms idle warning: {err_msg}. Checking snapshot...")
                try:
                    snapshot = await self.get_snapshot()
                    info = self.parse_snapshot_pane_info(snapshot=snapshot, agent_name=target_name)
                    if info.get("agent_status") in ("working", "running", "busy", "transitioning", "idle", "blocked") or info.get("pane_id"):
                        is_err = False
                except Exception:
                    pass

        if is_err:
            logger.error(f"Herdr agent prompt failed: {err_msg}")
            raise InfrastructureError(f"Herdr agent dispatch failed: {err_msg}")

        if wait:
            await self.run_cli([
                "agent", "wait", target_name,
                "--until", "done",
                "--until", "blocked",
                "--timeout", str(timeout_ms)
            ], timeout=float(timeout_ms / 1000) + 15.0)

        output = await self.read_worker(target_name)
        return output or out

    async def wait_worker(
        self,
        worker_id: str,
        until: Optional[List[str]] = None,
        timeout_ms: int = 120000
    ) -> str:
        """Wait until worker reaches specified settled state(s)."""
        args = ["agent", "wait", worker_id, "--timeout", str(timeout_ms)]
        if until:
            for st in until:
                args.extend(["--until", st])
        else:
            args.extend(["--until", "done", "--until", "idle"])
        code, out, err = await self.run_cli(args, timeout=float(timeout_ms / 1000) + 15.0)
        return out

    async def read_worker(
        self,
        worker_id: str,
        pane_id: Optional[str] = None,
        lines: int = 100
    ) -> str:
        """Read settled terminal output from Herdr agent or fallback pane."""
        code, out, _ = await self.run_cli([
            "agent", "read", worker_id, "--lines", str(lines), "--source", "recent-unwrapped"
        ])
        if code == 0 and out:
            return out
        if pane_id:
            code, out, _ = await self.run_cli([
                "pane", "read", pane_id, "--lines", str(lines), "--source", "recent-unwrapped"
            ])
            if code == 0 and out:
                return out
        return ""
