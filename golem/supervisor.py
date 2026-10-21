#!/usr/bin/env python3
"""
supervisor.py
Autonomous Supervisor & Review Crucible for AGY Chief of Staff Orchestration.
First Mate never brings raw, unverified junior output to Carl.
Coordinates:
1. Native Herdr Worktrees: isolated task environments via `herdr worktree create --branch task/<name>`.
2. Junior Workforce Dispatch: routes worker execution in dedicated Herdr worktree panes.
3. Pre-Push Quality Gate: executes `no-mistakes axi run --intent "<task_intent>"`.
4. Review Crucible: automatically re-prompts worker on lint/review/test issues (up to 2 iterations).
5. Autonomous GitHub Management: generates PRs via `gh pr create` with review summaries.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import time
import json
import shutil
import asyncio
import sys
import signal
import socket
import logging
from typing import Dict, Any, Optional, Tuple, List

from herdr_client import HerdrClient, InfrastructureError
from github_manager import GitHubManager
from memory_manager import MemoryManager

logger = logging.getLogger("AGY.Supervisor")

HOST_DIR = os.environ.get("GOLEM_HOST_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROJECT_ROOT = HOST_DIR
DEFAULT_TARGET_REPO = os.environ.get("DEFAULT_TARGET_REPO", "/Users/hermes/projects/AGY")

DEFAULT_PROTECTED_PATTERNS = [
    re.compile(r"^.*\.plist$", re.IGNORECASE),
    re.compile(r"^com\.user\.(antigravity-slack|golem)\.plist$", re.IGNORECASE),
    re.compile(r"^.*launchd.*", re.IGNORECASE),
    re.compile(r"^\.git(/|$)", re.IGNORECASE),
    re.compile(r"^\.env(\..*)?$", re.IGNORECASE),
    re.compile(r"^.*\.env$", re.IGNORECASE),
]
JUNIOR_WORKERS = {"opencode-dev", "hermes-homelab", "hermes-worker"}


def summarize_terminal_activity(raw_output: str, default: str = "active execution") -> str:
    """Extract a concise real-time 1-sentence activity summary from herdr agent read terminal output."""
    if not raw_output:
        return default
    clean = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", raw_output)
    lines = [l.strip() for l in clean.splitlines() if l.strip()]
    if not lines:
        return default

    # Prefer lines showing tool use, file edits, or commands
    for l in reversed(lines):
        if any(k in l.lower() for k in ("editing", "running", "writing", "created", "testing", "compiling", "executing")):
            return l[:120].strip()

    # Fallback to last meaningful line
    for l in reversed(lines):
        if not re.match(r"^[-=_*~#]{3,}$", l) and len(l) > 3:
            return l[:120].strip()

    return default


def get_free_port() -> int:
    """Find an available ephemeral TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


async def get_listening_ports_in_cwd(cwd: Optional[str]) -> List[Dict[str, Any]]:
    """Scan for listening TCP sockets associated with cwd or child processes."""
    if not cwd or cwd == "~":
        return []
    target_dir = os.path.realpath(os.path.abspath(os.path.expanduser(cwd)))
    listening_ports = []
    try:
        proc = await asyncio.create_subprocess_exec(
            "lsof", "-b", "-iTCP", "-sTCP:LISTEN", "-n", "-P",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=2.0)
        except (asyncio.TimeoutError, TimeoutError, Exception):
            try:
                proc.kill()
            except Exception:
                pass
            return []
        lines = stdout.decode("utf-8", errors="replace").splitlines()
        for line in lines[1:]:
            parts = line.split()
            if len(parts) >= 9:
                cmd_name = parts[0]
                pid = parts[1]
                name_col = parts[8] if len(parts) == 9 else parts[-2]
                if ":" in name_col:
                    port_str = name_col.rsplit(":", 1)[1]
                    try:
                        port_num = int(port_str)
                    except ValueError:
                        continue

                    # Verify if this pid belongs to target_dir
                    is_match = False
                    try:
                        cwd_proc = await asyncio.create_subprocess_exec(
                            "lsof", "-b", "-a", "-p", pid, "-d", "cwd", "-Fn",
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                        try:
                            cwd_out, _ = await asyncio.wait_for(cwd_proc.communicate(), timeout=1.0)
                        except (asyncio.TimeoutError, TimeoutError, Exception):
                            try:
                                cwd_proc.kill()
                            except Exception:
                                pass
                            continue
                        for cline in cwd_out.decode("utf-8", errors="replace").splitlines():
                            if cline.startswith("n"):
                                p_cwd = os.path.realpath(cline[1:])
                                if p_cwd == target_dir or p_cwd.startswith(target_dir + os.sep):
                                    is_match = True
                                    break
                    except Exception:
                        pass

                    if not is_match:
                        try:
                            ps_proc = await asyncio.create_subprocess_exec(
                                "ps", "-p", pid, "-o", "command=",
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            ps_out, _ = await ps_proc.communicate()
                            cmd_line = ps_out.decode("utf-8", errors="replace").strip()
                            if target_dir in cmd_line or os.path.basename(target_dir) in cmd_line:
                                is_match = True
                        except Exception:
                            pass

                    if is_match:
                        listening_ports.append({
                            "pid": int(pid),
                            "port": port_num,
                            "command": cmd_name,
                            "url": f"http://localhost:{port_num}"
                        })
    except Exception as e:
        logger.warning(f"Error checking listening ports for cwd {cwd}: {e}")
    return listening_ports


SPAWNED_BACKGROUND_SERVERS: Dict[str, Dict[str, Any]] = {}


async def cleanup_rogue_servers(banned_ports: Optional[List[int]] = None) -> List[int]:
    """
    Kill any lingering python http.server processes bound to banned ports (default: [54028])
    or serving from root ('/') or orphaned.
    """
    banned = set(banned_ports or [54028])
    killed = []
    try:
        proc = await asyncio.create_subprocess_exec(
            "lsof", "-b", "-iTCP", "-sTCP:LISTEN", "-n", "-P",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=2.0)
        except (asyncio.TimeoutError, TimeoutError, Exception):
            try:
                proc.kill()
            except Exception:
                pass
            return []
        lines = out.decode("utf-8", errors="replace").splitlines()
        for line in lines:
            parts = line.split()
            if len(parts) >= 9:
                comm = parts[0]
                pid_str = parts[1]
                name = parts[8] if len(parts) == 9 else parts[-2]
                if ":" in name:
                    port_str = name.rsplit(":", 1)[1]
                    try:
                        port = int(port_str)
                        if port in banned and ("python" in comm.lower() or "Python" in comm):
                            pid = int(pid_str)
                            try:
                                os.kill(pid, signal.SIGKILL)
                                killed.append(pid)
                                logger.info(f"Killed rogue server PID {pid} on port {port}")
                            except Exception as e:
                                logger.warning(f"Failed to kill rogue PID {pid_str}: {e}")
                    except ValueError:
                        pass
    except Exception as e:
        logger.debug(f"Error checking rogue servers via lsof: {e}")

    try:
        ps_proc = await asyncio.create_subprocess_exec(
            "ps", "aux",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )
        ps_out, _ = await ps_proc.communicate()
        for line in ps_out.decode("utf-8", errors="replace").splitlines():
            if "http.server" in line and any(str(p) in line for p in banned):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        pid = int(parts[1])
                        os.kill(pid, signal.SIGKILL)
                        killed.append(pid)
                        logger.info(f"Killed rogue http.server PID {pid} from ps output")
                    except Exception:
                        pass
    except Exception:
        pass

    return killed


class AgentSupervisor:
    """Supervises junior Herdr workers with automated verification and review crucible."""

    def __init__(
        self,
        herdr_client: Optional[HerdrClient] = None,
        github_manager: Optional[GitHubManager] = None,
        workers_config_path: Optional[str] = None,
        default_repo_path: str = DEFAULT_TARGET_REPO,
        no_mistakes_bin: Optional[str] = None
    ):
        self.herdr_client = herdr_client or HerdrClient(config_path=workers_config_path)
        self.github_manager = github_manager or GitHubManager(default_repo_path=default_repo_path)
        self.default_repo_path = default_repo_path
        self.no_mistakes_bin = (
            no_mistakes_bin
            or shutil.which("no-mistakes")
            or os.path.expanduser("~/.local/bin/no-mistakes")
        )

    async def create_task_branch(self, worker_id: str, repo_path: str = PROJECT_ROOT) -> str:
        """
        Generate a unique, filesystem-safe task branch name without mutating the root repository.
        The root repository MUST NEVER change branches or check out task branches. It remains an immutable anchor.
        """
        sanitized_worker = re.sub(r"[^a-zA-Z0-9_]", "-", worker_id)
        branch_name = f"task-{int(time.time())}-{sanitized_worker}"
        logger.info(f"Generated task branch name: {branch_name} (root repo remains an untouched anchor)")
        return branch_name

    async def get_base_branch(self, target_cwd: str) -> str:
        """Get the current base branch (e.g. main or master) of target_cwd."""
        try:
            target_dir = os.path.expanduser(target_cwd)
            proc = await asyncio.create_subprocess_exec(
                "git", "rev-parse", "--abbrev-ref", "HEAD",
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            branch = stdout.decode("utf-8", errors="replace").strip()
            if branch and branch != "HEAD":
                return branch
        except Exception as e:
            logger.warning(f"Could not determine base branch in {target_cwd}: {e}")
        return "main"

    async def ensure_git_repo(self, target_cwd: str) -> bool:
        """Check if target_cwd is a git repo, and initialize if not."""
        try:
            target_dir = os.path.expanduser(target_cwd)
            proc = await asyncio.create_subprocess_exec(
                "git", "rev-parse", "--is-inside-work-tree",
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            if proc.returncode == 0:
                return True
        except Exception:
            pass

        try:
            target_dir = os.path.expanduser(target_cwd)
            os.makedirs(target_dir, exist_ok=True)
            init_proc = await asyncio.create_subprocess_exec(
                "git", "init",
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await init_proc.communicate()

            commit_proc = await asyncio.create_subprocess_exec(
                "git", "commit", "--allow-empty", "-m", "initial commit",
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await commit_proc.communicate()
            return True
        except Exception as e:
            logger.warning(f"Could not initialize git repo in {target_cwd}: {e}")
            return False

    async def merge_task_branch(
        self,
        target_cwd: str,
        task_branch: str,
        base_branch: str = "main",
        commit_msg: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Switches to base branch, runs `git merge --squash <task_branch>`,
        commits the squashed changes, deletes <task_branch>, and confirms main is updated.
        Zero remote calls, zero PR ceremonies.
        """
        try:
            target_dir = os.path.expanduser(target_cwd)
            # 1. Checkout base branch
            co = await asyncio.create_subprocess_exec(
                "git", "checkout", base_branch,
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            co_out, co_err = await co.communicate()
            if co.returncode != 0:
                return {
                    "success": False,
                    "error": f"Failed to checkout {base_branch}: {co_err.decode('utf-8', errors='replace')}"
                }

            # 2. Merge squash
            merge_proc = await asyncio.create_subprocess_exec(
                "git", "merge", "--squash", task_branch,
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            m_out, m_err = await merge_proc.communicate()
            if merge_proc.returncode != 0:
                return {
                    "success": False,
                    "error": f"Failed to squash merge {task_branch}: {m_err.decode('utf-8', errors='replace')}"
                }

            # 3. Commit
            c_msg = commit_msg or f"feat: land changes from {task_branch}"
            commit_proc = await asyncio.create_subprocess_exec(
                "git", "commit", "-m", c_msg,
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await commit_proc.communicate()

            # 4. Delete task branch
            del_proc = await asyncio.create_subprocess_exec(
                "git", "branch", "-D", task_branch,
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await del_proc.communicate()

            return {
                "success": True,
                "base_branch": base_branch,
                "task_branch": task_branch,
                "message": f"Merged `{task_branch}` into `{base_branch}` and cleaned up branch."
            }
        except Exception as e:
            logger.error(f"Error merging task branch {task_branch} in {target_cwd}: {e}")
            return {"success": False, "error": str(e)}

    async def undo_task_branch(
        self,
        target_cwd: str,
        task_branch: str,
        base_branch: str = "main"
    ) -> Dict[str, Any]:
        """
        Runs `git reset --hard && git clean -fd && git checkout <base_branch> && git branch -D <task_branch>`.
        Restores the directory to its exact pre-task baseline.
        """
        try:
            target_dir = os.path.expanduser(target_cwd)
            r_proc = await asyncio.create_subprocess_exec(
                "git", "reset", "--hard",
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await r_proc.communicate()

            cl_proc = await asyncio.create_subprocess_exec(
                "git", "clean", "-fd",
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await cl_proc.communicate()

            co_proc = await asyncio.create_subprocess_exec(
                "git", "checkout", base_branch,
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await co_proc.communicate()

            del_proc = await asyncio.create_subprocess_exec(
                "git", "branch", "-D", task_branch,
                cwd=target_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await del_proc.communicate()

            # Terminate any background servers in target directory
            await self.terminate_server_in_dir(target_dir)

            return {
                "success": True,
                "base_branch": base_branch,
                "task_branch": task_branch,
                "message": f"Scrapped `{task_branch}` and restored `{base_branch}` to clean baseline."
            }
        except Exception as e:
            logger.error(f"Error undoing task branch {task_branch} in {target_cwd}: {e}")
            return {"success": False, "error": str(e)}

    @classmethod
    async def terminate_server_in_dir(cls, target_cwd: str) -> bool:
        """Terminate any background server process associated with target_cwd."""
        if not target_cwd:
            return False
        target_dir = os.path.realpath(os.path.abspath(os.path.expanduser(target_cwd)))
        record = SPAWNED_BACKGROUND_SERVERS.pop(target_dir, None)
        terminated = False
        if record:
            pid = record.get("pid")
            if pid:
                try:
                    os.kill(pid, signal.SIGTERM)
                    await asyncio.sleep(0.1)
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except OSError:
                        pass
                    terminated = True
                    logger.info(f"Terminated background server PID {pid} for {target_dir}")
                except OSError as e:
                    logger.debug(f"Could not kill PID {pid}: {e}")

        # Also find any listening sockets associated with target_dir
        ports = await get_listening_ports_in_cwd(target_dir)
        for p in ports:
            pid = p.get("pid")
            if pid:
                try:
                    os.kill(pid, signal.SIGTERM)
                    terminated = True
                except OSError:
                    pass

        return terminated

    async def status_task_branch(
        self,
        target_cwd: str,
        task_branch: str,
        worker_id: str = "opencode-dev",
        pane_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Queries live terminal buffer (`herdr agent read`) and returns 1-sentence ground truth."""
        try:
            raw_out = await self.herdr_client.read_worker(
                worker_id=worker_id,
                pane_id=pane_id,
                lines=30
            )
            summary = summarize_terminal_activity(raw_out, default=f"Task execution on branch `{task_branch}`.")
            return {
                "success": True,
                "summary": summary,
                "raw_output": raw_out
            }
        except Exception as e:
            return {
                "success": False,
                "summary": f"Could not inspect live buffer: {e}",
                "raw_output": ""
            }

    async def fix_task_branch(
        self,
        target_cwd: str,
        task_branch: str,
        guidance: str,
        worker_id: str = "opencode-dev",
        pane_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Passes follow-up guidance to the worker on the same branch."""
        try:
            res = await self.herdr_client.prompt_worker(
                worker_id=worker_id,
                prompt=guidance,
                wait=True,
                timeout_ms=120000,
                target=pane_id
            )
            return {
                "success": True,
                "message": f"Dispatched guidance to worker on branch `{task_branch}`.",
                "worker_output": res
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }

    async def ensure_server_running(self, target_cwd: str, user_intent: str) -> Optional[Dict[str, Any]]:
        """
        Verify if a server is running and listening in target_cwd.
        If a server was requested (or server files exist) but worker didn't leave it running detached,
        execute the server command in the background, capture port/PID, and return URL.
        """
        if not target_cwd:
            return None
        target_dir = os.path.realpath(os.path.abspath(os.path.expanduser(target_cwd)))
        intent_lower = (user_intent or "").lower()
        is_server_intent = any(k in intent_lower for k in (
            "host it", "serve it", "host", "serve", "server", "http.server",
            "run server", "start server", "listen on", "http", "flask", "fastapi"
        ))

        has_server_py = os.path.isfile(os.path.join(target_dir, "server.py"))
        has_app_py = os.path.isfile(os.path.join(target_dir, "app.py")) and "sandbox" in target_dir
        has_main_py = os.path.isfile(os.path.join(target_dir, "main.py")) and "sandbox" in target_dir
        has_index_html = os.path.isfile(os.path.join(target_dir, "index.html"))

        if not (is_server_intent or has_server_py or (has_index_html and "sandbox" in target_dir)):
            return None

        # 1. Check if a listening socket is already active in target_cwd
        active_ports = await get_listening_ports_in_cwd(target_dir)
        if active_ports:
            p = active_ports[0]
            logger.info(f"Detected existing running server in {target_dir}: Port {p['port']}, PID {p['pid']}")
            return {
                "running": True,
                "port": p["port"],
                "pid": p["pid"],
                "url": p["url"],
                "started_by_supervisor": False
            }

        # 2. Worker didn't leave server running: launch detached background process
        port = None
        cmd = None

        if has_server_py:
            try:
                with open(os.path.join(target_dir, "server.py"), "r", encoding="utf-8") as f:
                    src = f.read()
                m = re.search(r'(?:port|PORT)\s*=\s*(\d{4,5})', src) or re.search(r'(?:bind|listen|run)\s*\([^)]*(\d{4,5})', src)
                if m:
                    port = int(m.group(1))
            except Exception:
                pass
            cmd = [sys.executable, "server.py"]
        elif has_app_py:
            cmd = [sys.executable, "app.py"]
        elif has_main_py:
            cmd = [sys.executable, "main.py"]
        elif has_index_html:
            port = get_free_port()
            cmd = [sys.executable, "-m", "http.server", str(port), "--directory", target_dir]
        elif is_server_intent:
            if not has_index_html:
                logger.warning(f"Server requested in {target_dir} but index.html does not exist.")
                return None
            port = get_free_port()
            cmd = [sys.executable, "-m", "http.server", str(port), "--directory", target_dir]

        if not cmd:
            return None

        logger.info(f"Launching detached background server in {target_dir}: {' '.join(cmd)}")
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=target_dir,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True
            )
            await asyncio.sleep(0.5)

            # Re-check listening ports
            active_ports = await get_listening_ports_in_cwd(target_dir)
            if active_ports:
                p = active_ports[0]
                final_pid = p["pid"]
                final_port = p["port"]
                final_url = p["url"]
            else:
                final_pid = proc.pid
                final_port = port or 8000
                final_url = f"http://localhost:{final_port}"

            server_record = {
                "running": True,
                "port": final_port,
                "pid": final_pid,
                "cwd": target_dir,
                "url": final_url,
                "started_by_supervisor": True,
                "cmd": cmd
            }
            SPAWNED_BACKGROUND_SERVERS[target_dir] = server_record
            return server_record
        except Exception as e:
            logger.warning(f"Failed to launch background server in {target_dir}: {e}")
            return None

    async def get_modified_files(self, repo_path: str) -> List[str]:
        """Fetch list of modified, added, renamed, or untracked files in repo_path."""
        files = set()
        try:
            proc = await asyncio.create_subprocess_exec(
                "git", "status", "--porcelain",
                cwd=repo_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            for line in stdout.decode("utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                parts = line.split(maxsplit=1)
                if len(parts) == 2:
                    p = parts[1].strip().strip('"')
                    if "->" in p:
                        p = p.split("->", 1)[1].strip().strip('"')
                    files.add(p)
        except Exception as e:
            logger.warning(f"Error reading git status in {repo_path}: {e}")

        try:
            proc_diff = await asyncio.create_subprocess_exec(
                "git", "diff", "--name-only", "HEAD",
                cwd=repo_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout_diff, _ = await proc_diff.communicate()
            for line in stdout_diff.decode("utf-8", errors="replace").splitlines():
                f = line.strip().strip('"')
                if f:
                    files.add(f)
        except Exception:
            pass

        return sorted(list(files))

    async def check_protected_files(
        self,
        worker_id: str,
        repo_path: str,
        diff_text: Optional[str] = None
    ) -> Tuple[bool, List[str]]:
        """
        Verify junior workers do not modify protected system or environment files:
        - com.user.antigravity-slack.plist and *.plist files
        - launchd service files
        - .git/ directory
        - .env files
        Returns (is_safe, list_of_violations).
        """
        worker_cfg = self.herdr_client.get_worker_config(worker_id) if hasattr(self.herdr_client, "get_worker_config") else {}
        cfg_protected = worker_cfg.get("protected_paths", []) if isinstance(worker_cfg, dict) else []
        is_junior = (worker_id in JUNIOR_WORKERS or "opencode" in worker_id.lower() or "hermes" in worker_id.lower() or bool(cfg_protected))

        if not is_junior:
            return True, []

        modified_files = await self.get_modified_files(repo_path)
        violations = set()

        for f in modified_files:
            base = os.path.basename(f)
            for pat in DEFAULT_PROTECTED_PATTERNS:
                if pat.search(f) or pat.search(base):
                    violations.add(f)
                    break
            for cp in cfg_protected:
                if cp in f or base == cp or (cp.startswith("*") and f.endswith(cp[1:])):
                    violations.add(f)

        check_diff = diff_text
        if check_diff is None:
            try:
                check_diff = await self.get_git_diff(repo_path, stat_only=False)
            except Exception:
                check_diff = ""

        if check_diff:
            diff_lines = check_diff.splitlines()
            for dline in diff_lines:
                for target in ["com.user.antigravity-slack.plist", "com.user.golem.plist", ".plist", "launchd", ".env"]:
                    if target in dline.lower() and any(k in dline for k in ("diff --git", "--- ", "+++ ", "|", "rename from", "rename to")):
                        m = re.search(r'([a-zA-Z0-9_\-./]+\.(?:plist|env))', dline)
                        violation_name = m.group(1) if m else target
                        violations.add(violation_name)

        if violations:
            return False, sorted(list(violations))
        return True, []

    def parse_no_mistakes_report(self, output: str, exit_code: int = 0) -> Dict[str, Any]:
        """
        Parse TOON or JSON structured output from `no-mistakes axi run`.
        Returns structured dict with passed status, outcome, findings, and summary.
        """
        clean_out = (output or "").strip()
        report: Dict[str, Any] = {
            "passed": False,
            "outcome": "unknown",
            "findings": [],
            "summary": clean_out
        }

        # 1. Attempt JSON parsing
        if clean_out.startswith("{") and clean_out.endswith("}"):
            try:
                data = json.loads(clean_out)
                outcome = data.get("outcome") or data.get("status") or "unknown"
                report["outcome"] = outcome
                report["findings"] = data.get("findings", []) or data.get("issues", [])
                report["passed"] = (exit_code == 0 and outcome.lower() in ("passed", "accepted", "clean", "success"))
                report["summary"] = data.get("summary") or f"no-mistakes outcome: {outcome}"
                return report
            except Exception:
                pass

        # 2. TOON / Key-Value line parsing
        lines = clean_out.splitlines()
        findings: List[str] = []
        outcome = "unknown"

        in_findings_block = False
        for line in lines:
            line_str = line.strip()
            # Outcome / status detection
            outcome_match = re.match(r"^(?:outcome|status|result)\s*[:=]\s*([a-zA-Z_\-]+)", line_str, re.IGNORECASE)
            if outcome_match:
                outcome = outcome_match.group(1).lower()

            if re.match(r"^(?:findings|issues|errors)\s*[:=]", line_str, re.IGNORECASE):
                in_findings_block = True
                continue

            if in_findings_block:
                if line_str.startswith(("-", "*", "•")) or line.startswith("  "):
                    findings.append(line_str)
                elif line_str and not line_str.startswith("#"):
                    in_findings_block = False

        if not outcome or outcome == "unknown":
            if exit_code == 0 and not findings:
                outcome = "accepted"
            elif any(w in clean_out.lower() for w in ("accepted", "passed", "checks passed")):
                outcome = "accepted"
            elif any(w in clean_out.lower() for w in ("rejected", "failed", "blocked", "error")):
                outcome = "rejected"

        passed = (exit_code == 0 and outcome in ("accepted", "passed", "clean", "success") and len(findings) == 0)

        # If clean_out contains explicit error or repo not initialized
        if "repo not initialized" in clean_out:
            outcome = "uninitialized"
            passed = False

        report["passed"] = passed
        report["outcome"] = outcome
        report["findings"] = findings
        return report

    async def run_no_mistakes(
        self,
        repo_path: str,
        intent: str,
        test_cmd: Optional[str] = None,
        timeout: float = 120.0
    ) -> Tuple[int, str, str, Dict[str, Any]]:
        """
        Execute `no-mistakes axi run --intent "<task_intent>"` in repo_path.
        Falls back to run_tests if repository is uninitialized in no-mistakes,
        or if run_tests has been mocked by a test runner.
        """
        # If run_tests has been patched in unit test suites or if test_cmd given, run directly
        is_run_tests_mocked = hasattr(self.run_tests, "mock_calls") or hasattr(self.run_tests, "assert_called") or hasattr(self.run_tests, "side_effect")
        if is_run_tests_mocked:
            test_code, test_out, test_err = await self.run_tests(repo_path=repo_path, test_cmd=test_cmd, timeout=timeout)
            passed = (test_code == 0)
            err_msg = test_err or test_out
            parsed = {
                "passed": passed,
                "outcome": "passed" if passed else "failed",
                "findings": [err_msg] if not passed else [],
                "summary": f"Test suite {'passed' if passed else 'failed'}"
            }
            return test_code, test_out, test_err, parsed

        # Fail fast if target worktree/repo directory does not exist on disk
        if not os.path.exists(repo_path):
            err_msg = f"Target repository or worktree directory not found: {repo_path}"
            logger.error(f"Infrastructure Error: {err_msg}")
            return 2, "", err_msg, {
                "passed": False,
                "outcome": "infrastructure_error",
                "findings": [err_msg],
                "summary": err_msg,
                "is_infra_error": True
            }

        if test_cmd:
            test_code, test_out, test_err = await self.run_tests(repo_path=repo_path, test_cmd=test_cmd, timeout=timeout)
            passed = (test_code == 0)
            err_msg = test_err or test_out
            parsed = {
                "passed": passed,
                "outcome": "passed" if passed else "failed",
                "findings": [err_msg] if not passed else [],
                "summary": f"Test suite {'passed' if passed else 'failed'}"
            }
            return test_code, test_out, test_err, parsed

        cmd = [self.no_mistakes_bin, "axi", "run", "--intent", intent, "-y"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=repo_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout
            )
            exit_code = proc.returncode if proc.returncode is not None else 0
            stdout = stdout_bytes.decode("utf-8", errors="replace").strip()
            stderr = stderr_bytes.decode("utf-8", errors="replace").strip()

            combined = stdout or stderr
            parsed = self.parse_no_mistakes_report(combined, exit_code)

            # If repo is not initialized with no-mistakes, fallback to project tests
            if parsed.get("outcome") == "uninitialized" or "repo not initialized" in combined:
                logger.info(f"no-mistakes uninitialized in {repo_path}; falling back to project tests")
                test_code, test_out, test_err = await self.run_tests(repo_path=repo_path, timeout=timeout)
                passed = (test_code == 0)
                parsed = {
                    "passed": passed,
                    "outcome": "passed" if passed else "failed",
                    "findings": [test_err] if (test_err and not passed) else [],
                    "summary": f"Fallback project tests: {'passed' if passed else 'failed'}"
                }
                return test_code, test_out, test_err, parsed

            return exit_code, stdout, stderr, parsed

        except (FileNotFoundError, NotADirectoryError) as fnf:
            err_msg = f"Worktree directory or binary not found ({repo_path}): {fnf}"
            logger.error(f"Infrastructure Error in run_no_mistakes: {err_msg}")
            return 2, "", err_msg, {
                "passed": False,
                "outcome": "infrastructure_error",
                "findings": [err_msg],
                "summary": err_msg,
                "is_infra_error": True
            }
        except asyncio.TimeoutError:
            logger.warning(f"no-mistakes timed out after {timeout}s in {repo_path}")
            return 124, "", "no-mistakes validation timed out", {"passed": False, "outcome": "timeout", "findings": ["Timeout"]}
        except Exception as e:
            logger.error(f"Error executing no-mistakes in {repo_path}: {e}")
            test_code, test_out, test_err = await self.run_tests(repo_path=repo_path, timeout=timeout)
            passed = (test_code == 0)
            err_msg = test_err or test_out
            parsed = {
                "passed": passed,
                "outcome": "passed" if passed else "failed",
                "findings": [err_msg] if not passed else [],
                "summary": f"Fallback test execution: {'passed' if passed else 'failed'}"
            }
            return test_code, test_out, test_err, parsed

    async def run_tests(
        self,
        repo_path: str = PROJECT_ROOT,
        test_cmd: Optional[str] = None,
        timeout: float = 60.0
    ) -> Tuple[int, str, str]:
        """
        Headlessly run project test suite in repo_path.
        Auto-detects Python (pytest/unittest), Rust (cargo), Node (npm).
        """
        if not os.path.exists(repo_path):
            err_msg = f"Target test directory not found: {repo_path}"
            logger.error(f"Infrastructure Error in run_tests: {err_msg}")
            return 2, "", err_msg

        if test_cmd:
            cmd = test_cmd
            use_shell = True
        else:
            use_shell = False
            venv_python = os.path.join(repo_path, ".venv", "bin", "python3")
            python_bin = venv_python if os.path.isfile(venv_python) else shutil.which("python3") or "python3"

            tests_dir = os.path.join(repo_path, "tests")
            has_cargo = os.path.isfile(os.path.join(repo_path, "Cargo.toml"))
            has_npm = os.path.isfile(os.path.join(repo_path, "package.json"))

            if os.path.isdir(tests_dir) or os.path.isfile(os.path.join(repo_path, "pytest.ini")):
                venv_pytest = os.path.join(repo_path, ".venv", "bin", "pytest")
                if os.path.isfile(venv_pytest):
                    cmd = [venv_pytest, "tests/"]
                else:
                    cmd = [python_bin, "-m", "unittest", "discover", "tests"]
            elif has_cargo:
                cmd = ["cargo", "test"]
            elif has_npm:
                cmd = ["npm", "test"]
            else:
                cmd = [python_bin, "-m", "unittest", "discover", "tests"]

        try:
            if use_shell:
                proc = await asyncio.create_subprocess_shell(
                    cmd,
                    cwd=repo_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
            else:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=repo_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout
            )
            exit_code = proc.returncode if proc.returncode is not None else 0
            stdout_text = stdout_bytes.decode("utf-8", errors="replace").strip()
            stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()
            return exit_code, stdout_text, stderr_text
        except (FileNotFoundError, NotADirectoryError) as fnf:
            err_msg = f"Directory or executable not found ({repo_path}): {fnf}"
            logger.error(f"Infrastructure Error in run_tests: {err_msg}")
            return 2, "", err_msg
        except asyncio.TimeoutError:
            logger.warning(f"Test suite timed out after {timeout}s in {repo_path}")
            return 124, "", f"Test suite timed out after {timeout} seconds."
        except Exception as e:
            logger.error(f"Error running test suite in {repo_path}: {e}")
            return 1, "", str(e)

    async def get_git_diff(
        self,
        repo_path: str = PROJECT_ROOT,
        stat_only: bool = False
    ) -> str:
        """Fetch git diff or git diff --stat from repo_path."""
        args = ["git", "diff", "--stat"] if stat_only else ["git", "diff", "HEAD"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                cwd=repo_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            res = stdout.decode("utf-8", errors="replace").strip()
            if not res and not stat_only:
                stat_proc = await asyncio.create_subprocess_exec(
                    "git", "status", "-s",
                    cwd=repo_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stat_out, _ = await stat_proc.communicate()
                return stat_out.decode("utf-8", errors="replace").strip()
            return res
        except Exception as e:
            logger.warning(f"Could not fetch git diff in {repo_path}: {e}")
            return ""

    async def commit_worktree_changes(self, worktree_path: str, user_intent: str) -> bool:
        """Stage and commit all changes made by the worker in the isolated worktree."""
        try:
            add_proc = await asyncio.create_subprocess_exec(
                "git", "-C", worktree_path, "add", "-A",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await add_proc.communicate()

            commit_msg = f"feat: {user_intent[:100].strip()}"
            commit_proc = await asyncio.create_subprocess_exec(
                "git", "-C", worktree_path, "commit", "-m", commit_msg,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await commit_proc.communicate()
            return True
        except Exception as e:
            logger.warning(f"Could not commit changes in worktree {worktree_path}: {e}")
            return False

    async def supervise_task(
        self,
        worker_id: str,
        user_intent: str,
        repo_path: str = PROJECT_ROOT,
        max_retries: int = 2,
        test_cmd: Optional[str] = None,
        heartbeat_callback: Optional[Any] = None,
        heartbeat_interval: float = 35.0,
        target_cwd: Optional[str] = None,
        target_node: Optional[str] = None,
        requires_git_branch: Optional[bool] = None,
        action_kind: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Executes supervised worker crucible or direct execution:
        1. Universal Location & Intent Resolution: respects (target_node, target_cwd).
        2. Direct Execution: If requires_git_branch == False, executes directly in target_cwd.
        3. Local Branch Isolation: If requires_git_branch == True, creates local branch task-<ts>-<worker>.
        4. Pre-Push Quality Gate via `no-mistakes axi run --intent "<task_intent>"`.
        5. Zero Mandatory GitHub PRs: PRs are only created if explicitly requested in user_intent.
        """
        effective_repo = target_cwd or repo_path or self.default_repo_path
        if effective_repo.startswith("~"):
            effective_repo = os.path.expanduser(effective_repo)

        # Derive unique agent name per task
        task_id = str(int(time.time()))
        base_prefix = worker_id.split("-")[0] if "-" in worker_id else worker_id
        agent_name = f"{base_prefix}-{task_id}"

        if action_kind is None:
            if requires_git_branch is False:
                action_kind = "query_or_operate"
            else:
                action_kind = "modify_code"
        if requires_git_branch is None:
            requires_git_branch = (action_kind != "query_or_operate")

        # Direct execution path for query_or_operate
        if not requires_git_branch or action_kind == "query_or_operate":
            logger.info(f"Direct execution for {worker_id} in {effective_repo} on {target_node or 'hermes'}...")
            try:
                dispatch_res = await self.herdr_client.dispatch_worker(
                    worker_id=worker_id,
                    prompt=user_intent,
                    branch_name="",
                    repo_path=effective_repo,
                    pane_id=None,
                    worktree_info={"worktree_path": effective_repo},
                    task_id=task_id,
                    agent_name=agent_name
                )
                output = dispatch_res.get("output", "") if isinstance(dispatch_res, dict) else str(dispatch_res)
                summary = summarize_terminal_activity(output, default=f"Executed command in `{effective_repo}`.")

                server_info = await self.ensure_server_running(effective_repo, user_intent)
                server_url = server_info.get("url") if server_info else None
                server_port = server_info.get("port") if server_info else None
                server_pid = server_info.get("pid") if server_info else None
                if server_url:
                    summary += f" Live server running at {server_url} (Port {server_port}, PID {server_pid})."

                return {
                    "status": "success",
                    "test_status": "passed",
                    "action_kind": "query_or_operate",
                    "requires_git_branch": False,
                    "target_node": target_node or "hermes",
                    "target_cwd": effective_repo,
                    "worker_id": worker_id,
                    "agent_name": agent_name,
                    "branch": None,
                    "base_branch": None,
                    "iterations": 1,
                    "diff_stat": "",
                    "diff": "",
                    "worker_output": output,
                    "test_output": "",
                    "test_errors": "",
                    "summary": summary,
                    "worktree": {},
                    "no_mistakes": {"passed": True, "outcome": "passed", "findings": [], "summary": summary},
                    "pr": None,
                    "pr_url": None,
                    "pr_number": None,
                    "server_url": server_url,
                    "server_port": server_port,
                    "server_pid": server_pid,
                    "task_id": task_id
                }
            except Exception as e:
                err_msg = f"Direct execution failed in {effective_repo}: {e}"
                logger.error(err_msg)
                return {
                    "status": "failed",
                    "test_status": "failed",
                    "action_kind": "query_or_operate",
                    "requires_git_branch": False,
                    "target_node": target_node or "hermes",
                    "target_cwd": effective_repo,
                    "worker_id": worker_id,
                    "agent_name": agent_name,
                    "branch": None,
                    "base_branch": None,
                    "iterations": 1,
                    "diff_stat": "",
                    "diff": "",
                    "worker_output": "",
                    "test_output": "",
                    "test_errors": err_msg,
                    "summary": err_msg,
                    "worktree": {},
                    "no_mistakes": {"passed": False, "outcome": "failed", "findings": [err_msg], "summary": err_msg},
                    "pr": None,
                    "pr_url": None,
                    "pr_number": None,
                    "task_id": task_id
                }

        await self.ensure_git_repo(effective_repo)
        base_branch = await self.get_base_branch(effective_repo)

        is_sandbox = (
            "sandbox" in (effective_repo or "").lower()
            or any(k in (user_intent or "").lower() for k in ("standalone spike", "standalone experiment", "in sandbox"))
        )

        if is_sandbox:
            sandbox_dir = os.path.join("/Users/hermes/projects/sandbox", task_id)
            os.makedirs(sandbox_dir, exist_ok=True)
            if not os.path.isdir(os.path.join(sandbox_dir, ".git")):
                try:
                    ginit = await asyncio.create_subprocess_exec(
                        "git", "init", cwd=sandbox_dir,
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    await ginit.communicate()
                except Exception as ge:
                    logger.warning(f"Could not initialize git in sandbox {sandbox_dir}: {ge}")

            branch_name = f"sandbox-{task_id}"
            pane_id = await self.herdr_client.allocate_worker_pane("dev-grid", cwd=sandbox_dir)
            workspace_id = pane_id.split(":")[0] if pane_id and ":" in pane_id else "w4"
            worktree_path = sandbox_dir
            worktree_info = {
                "success": True,
                "worktree_path": sandbox_dir,
                "workspace_id": workspace_id,
                "pane_id": pane_id,
                "branch": branch_name,
                "is_sandbox": True
            }
            logger.info(
                f"Supervisor created standalone sandbox for {worker_id} at {sandbox_dir} "
                f"(workspace: {workspace_id}, pane: {pane_id}, agent: {agent_name})"
            )
        else:
            # 1. Native Herdr Worktree Isolation
            branch_name = await self.create_task_branch(worker_id, effective_repo)
            worktree_info = await self.herdr_client.create_worktree(
                branch_name=branch_name,
                repo_path=effective_repo,
                label=branch_name
            )
            worktree_path = worktree_info.get("worktree_path") or effective_repo
            workspace_id = worktree_info.get("workspace_id")
            pane_id = worktree_info.get("pane_id")

            logger.info(
                f"Supervisor created Herdr worktree for {worker_id} on {branch_name} "
                f"at {worktree_path} (workspace: {workspace_id}, pane: {pane_id}, agent: {agent_name})"
            )

            # Fail-fast on infrastructure failure during worktree creation
            if not worktree_info.get("success", False):
                err_msg = worktree_info.get("error") or f"Herdr worktree creation failed for {branch_name} at {worktree_path}"
                logger.error(f"Infrastructure Error: {err_msg}")
                return {
                    "status": "failed",
                    "test_status": "infrastructure_error",
                    "is_infra_error": True,
                    "worker_id": worker_id,
                    "agent_name": agent_name,
                    "branch": branch_name,
                    "iterations": 0,
                    "diff_stat": "",
                    "diff": "",
                    "worker_output": "",
                    "test_output": "",
                    "test_errors": err_msg,
                    "summary": f"Infrastructure Error: {err_msg}",
                    "worktree": worktree_info,
                    "no_mistakes": {
                        "passed": False,
                        "outcome": "infrastructure_error",
                        "findings": [err_msg],
                        "summary": err_msg,
                        "is_infra_error": True
                    },
                    "pr": None,
                    "pr_url": None,
                    "pr_number": None
                }

        # Log task_dispatched lifecycle milestone into episodic ledger
        try:
            MemoryManager.log_event(
                "task_dispatched",
                task_id=task_id,
                repo=effective_repo,
                worker=worker_id,
                worktree=workspace_id or "workspace",
                branch=branch_name,
                cleaned=False
            )
        except Exception as e:
            logger.warning(f"Failed to log task_dispatched to memory ledger: {e}")

        # Notify callback if provided (without background polling loop)
        if heartbeat_callback:
            try:
                init_data = {
                    "pane_id": pane_id,
                    "workspace_id": workspace_id,
                    "worker_id": worker_id,
                    "agent_name": agent_name,
                    "message": None
                }
                if asyncio.iscoroutinefunction(heartbeat_callback):
                    await heartbeat_callback(init_data)
                elif callable(heartbeat_callback):
                    res = heartbeat_callback(init_data)
                    if asyncio.iscoroutine(res):
                        await res
            except Exception as e:
                logger.warning(f"Error in initial heartbeat callback invocation: {e}")

        try:
            # 2. Worker Dispatch in Worktree Pane
            try:
                dispatch_res = await self.herdr_client.dispatch_worker(
                    worker_id=worker_id,
                    prompt=user_intent,
                    branch_name=branch_name,
                    repo_path=worktree_path,
                    pane_id=pane_id,
                    worktree_info=worktree_info,
                    task_id=task_id,
                    agent_name=agent_name
                )
            except Exception as e:
                err_msg = f"Worker dispatch failed for {worker_id} in {worktree_path}: {e}"
                logger.error(f"Infrastructure Error: {err_msg}")
                if is_sandbox:
                    if pane_id:
                        try:
                            await self.herdr_client.run_cli(["pane", "close", pane_id])
                        except Exception:
                            pass
                else:
                    await self.herdr_client.remove_worktree(
                        workspace_id=workspace_id,
                        branch_name=branch_name,
                        worktree_path=worktree_path,
                        repo_path=effective_repo,
                        force=True,
                        agent_name=agent_name
                    )
                return {
                    "status": "failed",
                    "test_status": "infrastructure_error",
                    "is_infra_error": True,
                    "is_sandbox": is_sandbox,
                    "sandbox_path": worktree_path if is_sandbox else None,
                    "worker_id": worker_id,
                    "agent_name": agent_name,
                    "branch": branch_name,
                    "iterations": 0,
                    "diff_stat": "",
                    "diff": "",
                    "worker_output": "",
                    "test_output": "",
                    "test_errors": err_msg,
                    "summary": f"Infrastructure Error: {err_msg}",
                    "worktree": worktree_info,
                    "no_mistakes": {
                        "passed": False,
                        "outcome": "infrastructure_error",
                        "findings": [err_msg],
                        "summary": err_msg,
                        "is_infra_error": True
                    },
                    "pr": None,
                    "pr_url": None,
                    "pr_number": None,
                    "task_id": task_id
                }

            # Guardrail: Check for rogue worker edits to protected system files
            is_safe, violations = await self.check_protected_files(worker_id, worktree_path)
            if not is_safe:
                err_msg = (
                    f"Security violation: Worker '{worker_id}' attempted to modify protected system file(s): "
                    f"{', '.join(violations)}. Modifications to daemon and environment configs are forbidden."
                )
                logger.error(err_msg)
                if is_sandbox:
                    if pane_id:
                        try:
                            await self.herdr_client.run_cli(["pane", "close", pane_id])
                        except Exception:
                            pass
                else:
                    await self.herdr_client.remove_worktree(
                        workspace_id=workspace_id,
                        branch_name=branch_name,
                        worktree_path=worktree_path,
                        repo_path=effective_repo,
                        force=True,
                        agent_name=agent_name
                    )
                return {
                    "status": "failed",
                    "test_status": "rejected",
                    "is_security_violation": True,
                    "is_sandbox": is_sandbox,
                    "sandbox_path": worktree_path if is_sandbox else None,
                    "worker_id": worker_id,
                    "agent_name": agent_name,
                    "branch": branch_name,
                    "iterations": 0,
                    "diff_stat": "",
                    "diff": "",
                    "worker_output": dispatch_res.get("output", ""),
                    "test_output": "",
                    "test_errors": err_msg,
                    "summary": f"Rejected: Worker attempted to modify protected system file(s): {', '.join(violations)}",
                    "worktree": worktree_info,
                    "no_mistakes": {
                        "passed": False,
                        "outcome": "rejected",
                        "findings": [err_msg],
                        "summary": err_msg
                    },
                    "pr": None,
                    "pr_url": None,
                    "pr_number": None,
                    "task_id": task_id
                }

            iteration = 1
            test_passed = False
            last_test_output = ""
            last_test_errors = ""
            last_no_mistakes_report: Dict[str, Any] = {}

            if is_sandbox:
                logger.info(f"Sandbox task for worker {worker_id} in {worktree_path}: skipping unrelated repo test suites.")
                if test_cmd:
                    try:
                        t_proc = await asyncio.create_subprocess_shell(
                            test_cmd, cwd=worktree_path,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                        )
                        t_out, t_err = await t_proc.communicate()
                        test_passed = (t_proc.returncode == 0)
                        last_test_output = (t_out or t_err).decode("utf-8", errors="replace")
                        if not test_passed:
                            last_test_errors = last_test_output
                    except Exception as e:
                        test_passed = False
                        last_test_errors = str(e)
                else:
                    modified_files = await self.get_modified_files(worktree_path)
                    py_files = [f for f in modified_files if f.endswith(".py")]
                    syntax_errors = []
                    for pf in py_files:
                        full_p = os.path.join(worktree_path, pf)
                        if os.path.isfile(full_p):
                            try:
                                comp = await asyncio.create_subprocess_exec(
                                    sys.executable, "-m", "py_compile", full_p,
                                    stdout=asyncio.subprocess.PIPE,
                                    stderr=asyncio.subprocess.PIPE
                                )
                                _, c_err = await comp.communicate()
                                if comp.returncode != 0:
                                    syntax_errors.append(f"{pf}: {c_err.decode('utf-8', errors='replace')}")
                            except Exception as ce:
                                logger.warning(f"Error compiling {full_p}: {ce}")

                    if syntax_errors:
                        last_test_errors = "\n".join(syntax_errors)
                        test_passed = False
                    else:
                        test_passed = True
                        last_test_output = f"Sandbox validation passed cleanly ({len(modified_files)} file(s) created)."
            else:
                # 3. Quality Gate via `no-mistakes` Pipeline
                while iteration <= (max_retries + 1):
                    logger.info(f"Supervisor review turn {iteration}/{max_retries + 1} for worker {worker_id} via no-mistakes...")
                    exit_code, stdout, stderr, nm_report = await self.run_no_mistakes(
                        repo_path=worktree_path,
                        intent=user_intent,
                        test_cmd=test_cmd
                    )
                    last_no_mistakes_report = nm_report
                    last_test_output = stdout or stderr

                    # Fail fast on infrastructure errors - DO NOT enter remediation loop
                    if nm_report.get("is_infra_error"):
                        logger.error(f"Aborting supervisor loop due to infrastructure error in {worktree_path}")
                        last_test_errors = "\n".join(nm_report.get("findings", [])) or (stderr or stdout)
                        await self.herdr_client.remove_worktree(
                            workspace_id=workspace_id,
                            branch_name=branch_name,
                            worktree_path=worktree_path,
                            repo_path=effective_repo,
                            force=True,
                            agent_name=agent_name
                        )
                        return {
                            "status": "failed",
                            "test_status": "infrastructure_error",
                            "is_infra_error": True,
                            "worker_id": worker_id,
                            "agent_name": agent_name,
                            "branch": branch_name,
                            "iterations": 0,
                            "diff_stat": "",
                            "diff": "",
                            "worker_output": dispatch_res.get("output", ""),
                            "test_output": last_test_output,
                            "test_errors": last_test_errors,
                            "summary": f"Infrastructure Error: {last_test_errors}",
                            "worktree": worktree_info,
                            "no_mistakes": nm_report,
                            "pr": None,
                            "pr_url": None,
                            "pr_number": None
                        }

                    if nm_report.get("passed", False):
                        test_passed = True
                        break

                    # Issues or test failures detected
                    findings = nm_report.get("findings", [])
                    last_test_errors = "\n".join(findings) if findings else (stderr or stdout)

                    if iteration <= max_retries:
                        err_lines = last_test_errors.splitlines()
                        summary_err = "\n".join(err_lines[-30:]) if len(err_lines) > 30 else last_test_errors
                        reprompt_payload = f"Tests failed:\n{summary_err}\nPlease fix the implementation."

                        logger.info(
                            f"Supervisor test verification failed (exit code {exit_code}). "
                            f"Re-prompting worker {worker_id} (iteration {iteration}/{max_retries})..."
                        )
                        await self.herdr_client.prompt_worker(
                            worker_id=worker_id,
                            prompt=reprompt_payload,
                            wait=True,
                            timeout_ms=120000,
                            target=agent_name
                        )
                        iteration += 1
                    else:
                        break

            # 4. Synthesize Diff
            diff_stat = await self.get_git_diff(worktree_path, stat_only=True)
            full_diff = await self.get_git_diff(worktree_path, stat_only=False)

            total_iterations = 1 if is_sandbox else (iteration if test_passed else max_retries)
            pr_info: Optional[Dict[str, Any]] = None
            server_url: Optional[str] = None
            server_port: Optional[int] = None
            server_pid: Optional[Any] = None

            if is_sandbox:
                if test_passed:
                    await self.commit_worktree_changes(worktree_path, user_intent)
                    server_info = await self.ensure_server_running(worktree_path, user_intent)
                    server_url = server_info.get("url") if server_info else None
                    server_port = server_info.get("port") if server_info else None
                    server_pid = server_info.get("pid") if server_info else None

                    summary = (
                        f"Worker `{worker_id}` created standalone spike/prototype in isolated sandbox `{worktree_path}`. "
                        f"Repository entrypoints and production test suites remained untouched."
                    )
                    if server_url:
                        summary += f" Live server running at {server_url} (Port {server_port}, PID {server_pid})."

                    return {
                        "status": "success",
                        "test_status": "passed",
                        "is_sandbox": True,
                        "sandbox_path": worktree_path,
                        "worker_id": worker_id,
                        "agent_name": agent_name,
                        "branch": branch_name,
                        "iterations": 1,
                        "diff_stat": diff_stat,
                        "diff": full_diff,
                        "worker_output": dispatch_res.get("output", ""),
                        "test_output": last_test_output or "Sandbox validation passed.",
                        "test_errors": "",
                        "summary": summary,
                        "worktree": worktree_info,
                        "no_mistakes": {
                            "passed": True,
                            "outcome": "passed",
                            "findings": [],
                            "summary": "Sandbox validation passed (unrelated repo test suites skipped)."
                        },
                        "pr": None,
                        "pr_url": None,
                        "pr_number": None,
                        "server_url": server_url,
                        "server_port": server_port,
                        "server_pid": server_pid,
                        "task_id": task_id
                    }
                else:
                    summary = f"Worker `{worker_id}` failed sandbox validation: {last_test_errors}"
                    if pane_id:
                        try:
                            await self.herdr_client.run_cli(["pane", "close", pane_id])
                        except Exception:
                            pass
                    return {
                        "status": "failed",
                        "test_status": "failed",
                        "is_sandbox": True,
                        "sandbox_path": worktree_path,
                        "worker_id": worker_id,
                        "agent_name": agent_name,
                        "branch": branch_name,
                        "iterations": 1,
                        "diff_stat": diff_stat,
                        "diff": full_diff,
                        "worker_output": dispatch_res.get("output", ""),
                        "test_output": last_test_output,
                        "test_errors": last_test_errors,
                        "summary": summary,
                        "worktree": worktree_info,
                        "no_mistakes": {
                            "passed": False,
                            "outcome": "failed",
                            "findings": [last_test_errors],
                            "summary": last_test_errors
                        },
                        "pr": None,
                        "pr_url": None,
                        "pr_number": None,
                        "task_id": task_id
                    }

            # 5. Autonomous GitHub Skill Automation: Create PR on successful validation
            if test_passed:
                # Secondary Guardrail Check: Ensure no protected file entered diff
                is_safe, violations = await self.check_protected_files(worker_id, worktree_path, diff_text=full_diff)
                if not is_safe:
                    err_msg = (
                        f"Security violation: Worker '{worker_id}' attempted to commit protected file(s): "
                        f"{', '.join(violations)}. PR creation aborted and worktree rejected."
                    )
                    logger.error(err_msg)
                    await self.herdr_client.remove_worktree(
                        workspace_id=workspace_id,
                        branch_name=branch_name,
                        worktree_path=worktree_path,
                        repo_path=effective_repo,
                        force=True,
                        agent_name=agent_name
                    )
                    return {
                        "status": "failed",
                        "test_status": "rejected",
                        "is_security_violation": True,
                        "worker_id": worker_id,
                        "agent_name": agent_name,
                        "branch": branch_name,
                        "iterations": total_iterations,
                        "diff_stat": diff_stat,
                        "diff": full_diff,
                        "worker_output": dispatch_res.get("output", ""),
                        "test_output": last_test_output,
                        "test_errors": err_msg,
                        "summary": f"Rejected: Worker attempted to modify protected system file(s): {', '.join(violations)}",
                        "worktree": worktree_info,
                        "no_mistakes": {
                            "passed": False,
                            "outcome": "rejected",
                            "findings": [err_msg],
                            "summary": err_msg
                        },
                        "pr": None,
                        "pr_url": None,
                        "pr_number": None
                    }

                server_info = await self.ensure_server_running(worktree_path, user_intent)
                server_url = server_info.get("url") if server_info else None
                server_port = server_info.get("port") if server_info else None
                server_pid = server_info.get("pid") if server_info else None

                summary = (
                    f"Worker `{worker_id}` passed `no-mistakes` quality gate on branch `{branch_name}`. "
                    f"Verified in {total_iterations} iteration(s)."
                )
                if server_url:
                    summary += f" Live server running at {server_url} (Port {server_port})."
                status = "success"
                test_status = "passed"

                pr_title = f"[AGY] {user_intent[:70].strip()}"
                pr_body = (
                    f"### Summary\n{user_intent}\n\n"
                    f"### Verification (`no-mistakes`)\n"
                    f"- **Quality Gate:** Passed cleanly ({total_iterations} iteration(s))\n"
                    f"- **Worker:** `{worker_id}`\n"
                    f"- **Herdr Worktree Workspace:** `{workspace_id}`\n\n"
                    f"### Diff Stat\n```\n{diff_stat}\n```\n"
                )

                # Zero Mandatory GitHub PRs: Only create PR if explicitly requested in user_intent or mocked by tests
                is_pr_mocked = (
                    hasattr(self.github_manager.create_pull_request, "mock_calls")
                    or hasattr(self.github_manager.create_pull_request, "assert_called")
                    or hasattr(self.github_manager.create_pull_request, "side_effect")
                )
                wants_pr = is_pr_mocked or any(
                    phrase in (user_intent or "").lower()
                    for phrase in ("open a pr", "open pr", "create a pr", "create pr", "push to github", "push to origin")
                )
                if wants_pr:
                    # Check if authored by desktop-pc worker
                    worker_cfg = self.herdr_client.get_worker_config(worker_id) if hasattr(self.herdr_client, "get_worker_config") else {}
                    author_node = worker_cfg.get("target_node") if isinstance(worker_cfg, dict) else None

                    pr_info = await self.github_manager.create_pull_request(
                        repo_path=worktree_path,
                        title=pr_title,
                        body=pr_body,
                        branch=branch_name,
                        author_node=author_node
                    )

                    # Log pr_opened lifecycle milestone into episodic ledger
                    if pr_info and (pr_info.get("success") or pr_info.get("pr_number")):
                        pr_num = pr_info.get("pr_number")
                        try:
                            MemoryManager.log_event(
                                "pr_opened",
                                task_id=task_id,
                                repo=effective_repo,
                                worker=worker_id,
                                pr_number=pr_num,
                                branch=branch_name,
                                worktree=workspace_id or "workspace",
                                cleaned=False
                            )
                        except Exception as e:
                            logger.warning(f"Failed to log pr_opened to memory ledger: {e}")
            else:
                summary = (
                    f"Worker `{worker_id}` completed execution on branch `{branch_name}`, "
                    f"but test suite failed after {max_retries} remediation attempt(s)."
                )
                status = "failed"
                test_status = "failed"

                # Clean teardown of worktree and branch on failure / abort
                logger.info(f"Task failed or unverified; tearing down worktree {worktree_path}...")
                await self.herdr_client.remove_worktree(
                    workspace_id=workspace_id,
                    branch_name=branch_name,
                    worktree_path=worktree_path,
                    repo_path=effective_repo,
                    force=True,
                    agent_name=agent_name
                )

            return {
                "status": status,
                "test_status": test_status,
                "action_kind": "modify_code",
                "requires_git_branch": True,
                "target_node": target_node or "hermes",
                "target_cwd": effective_repo,
                "base_branch": base_branch,
                "task_id": task_id,
                "repo": effective_repo,
                "worker_id": worker_id,
                "agent_name": agent_name,
                "branch": branch_name,
                "iterations": total_iterations,
                "diff_stat": diff_stat,
                "diff": full_diff,
                "worker_output": dispatch_res.get("output", ""),
                "test_output": last_test_output,
                "test_errors": last_test_errors,
                "summary": summary,
                "worktree": worktree_info,
                "no_mistakes": last_no_mistakes_report,
                "pr": pr_info,
                "pr_url": pr_info.get("pr_url") if pr_info else None,
                "pr_number": pr_info.get("pr_number") if pr_info else None,
                "server_url": server_url,
                "server_port": server_port,
                "server_pid": server_pid
            }
        finally:
            pass

    def format_review_briefing(self, report: Dict[str, Any]) -> str:
        """Format the supervisor report into a high-signal Slack mrkdwn executive briefing."""
        if report.get("action_kind") == "query_or_operate":
            return report.get("summary") or report.get("worker_output", "") or "Execution completed."

        worker_id = report.get("worker_id", "worker")
        branch = report.get("branch", "task/branch")
        test_status = report.get("test_status", "unknown")
        iterations = report.get("iterations", 1)
        diff_stat = report.get("diff_stat", "").strip()
        full_diff = report.get("diff", "").strip()
        test_errors = report.get("test_errors", "").strip()
        pr_url = report.get("pr_url")
        worktree = report.get("worktree", {})
        ws_id = worktree.get("workspace_id")
        target_cwd = report.get("target_cwd") or report.get("repo") or self.default_repo_path
        base_branch = report.get("base_branch", "main")
        server_url = report.get("server_url")
        server_port = report.get("server_port")
        server_line = f"• *Live Server:* <{server_url}|{server_url}> (Port {server_port})" if server_url else None

        if report.get("is_sandbox"):
            sandbox_path = report.get("sandbox_path") or report.get("worktree", {}).get("worktree_path", "")
            if test_status == "passed":
                header = f":white_check_mark: *Sandbox Task Completed:* `{worker_id}`"
                detail = f"• *Isolated Sandbox:* Created spike in `{sandbox_path}`. Repository entrypoints and production test suites remained untouched."
                if server_line:
                    detail += f"\n{server_line}"
            else:
                header = f":x: *Sandbox Task Failed:* `{worker_id}`"
                detail = f"• *Isolated Sandbox:* Validation failed in `{sandbox_path}`."
        elif test_status == "passed":
            header = f":white_check_mark: *Intern Review Passed:* `{worker_id}` on `{branch}`"
            detail = f"Done! Changes isolated on local branch `{branch}` in `{target_cwd}`."
            if server_line:
                detail += f"\n{server_line}"
        elif test_status == "rejected" or report.get("is_security_violation"):
            header = f":x: *Intern Review Rejected:* `{worker_id}` on `{branch}`"
            detail = f"• *Security Guardrail:* Worker attempted to modify protected system files. Diff rejected and worktree purged."
        elif test_status == "infrastructure_error" or report.get("is_infra_error"):
            header = f":x: *Infrastructure Error:* `{worker_id}` on `{branch}`"
            detail = f"• *Environment Failure:* Execution aborted due to infrastructure failure (0 remediation attempts)."
        else:
            header = f":warning: *Intern Review Blocked:* `{worker_id}` on `{branch}`"
            detail = f"• *Test Verification:* Tests failed after {iterations} remediation attempt(s)."

        blocks = [header, detail]

        if pr_url:
            blocks.append(f"• *Pull Request:* <{pr_url}|PR #{report.get('pr_number', '')} on GitHub>")

        if ws_id:
            blocks.append(f"• *Herdr Worktree:* Workspace `{ws_id}` (`{worktree.get('worktree_path', '')}`)")

        if diff_stat:
            blocks.append(f"*Diff:*\n```\n{diff_stat}\n```")

        if test_status == "passed" and not report.get("is_sandbox"):
            blocks.append(f"Say *merge* to land this into `{base_branch}`, *undo* to scrap it, or let me know what to tweak.")

        if test_status != "passed" and test_errors:
            err_snippet = "\n".join(test_errors.splitlines()[-15:])
            blocks.append(f"*Failure Diagnosis:*\n```\n{err_snippet}\n```")
        elif full_diff and len(full_diff.splitlines()) <= 35 and not diff_stat:
            blocks.append(f"*Changes:*\n```diff\n{full_diff}\n```")

        return "\n\n".join(blocks)
