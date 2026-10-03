#!/usr/bin/env python3
"""
herdr_tools.py
Direct Native Herdr Python Tools for Gemini Flash Chief of Staff.
Zero-MCP architecture: direct function calls into the Herdr daemon, git isolation, and FrugaLLM.
"""

import os
import re
import time
import shutil
import asyncio
import logging
from typing import Dict, Any, Optional, List

from golem.config import get_default_hand_worker
from golem.execution.delegation import WorkerDelegator

logger = logging.getLogger("FirstMate.HerdrTools")

# In-memory registry of active spawned agent panes and their metadata
SPAWNED_AGENTS: Dict[str, Dict[str, Any]] = {}

HERDR_BIN_DEFAULT = shutil.which("herdr") or os.path.expanduser("~/.local/bin/herdr")

HERDR_TOOLS_DECLARATIONS = [
    {
        "name": "herdr_spawn",
        "description": "Spawn a backstage hand worker agent in Herdr on a specific fleet node and directory to perform a task.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "node": {
                    "type": "STRING",
                    "description": "The target machine node (hermes, desktop-pc, oci-machine, personal-laptop)."
                },
                "cwd": {
                    "type": "STRING",
                    "description": "The target working directory on the node (e.g. /Users/hermes, ~/projects/<name>, or /Users/hermes/projects/sandbox/<task_id>)."
                },
                "worker": {
                    "type": "STRING",
                    "description": "The worker agent to spawn (default: 'agy'; supports 'opencode', 'hermes')."
                },
                "prompt": {
                    "type": "STRING",
                    "description": "Detailed task prompt and requirements for the worker."
                },
                "isolate_branch": {
                    "type": "BOOLEAN",
                    "description": "Whether to isolate code modifications on a new local git branch (default: true for code edits, false for system queries/read-only ops)."
                }
            },
            "required": ["node", "cwd", "prompt"]
        }
    },
    {
        "name": "herdr_read",
        "description": "Read the latest settled terminal output buffer (last 30 lines) of an active Herdr agent pane to check status or runtime results.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "pane_id": {
                    "type": "STRING",
                    "description": "The Herdr pane ID (e.g. w1:p2) or agent identifier to read."
                }
            },
            "required": ["pane_id"]
        }
    },
    {
        "name": "herdr_stop",
        "description": "Stop and terminate an active Herdr agent pane, terminating any listening server processes and optionally discarding git changes.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "pane_id": {
                    "type": "STRING",
                    "description": "The Herdr pane ID or agent identifier to stop."
                },
                "cwd": {
                    "type": "STRING",
                    "description": "The working directory of the agent (optional, used for terminating background servers and git rollback)."
                },
                "rollback": {
                    "type": "BOOLEAN",
                    "description": "If true, hard resets git changes, cleans untracked files, restores base branch, and deletes the task branch."
                }
            },
            "required": ["pane_id"]
        }
    },
    {
        "name": "run_command",
        "description": "Run a shell inspection or diagnostic command (e.g. lsof -iTCP -sTCP:LISTEN, git status, ls -la) in cwd and return output.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "cmd": {
                    "type": "STRING",
                    "description": "The shell command to execute."
                },
                "cwd": {
                    "type": "STRING",
                    "description": "Working directory to execute the command in (default: /Users/hermes)."
                }
            },
            "required": ["cmd"]
        }
    },
    {
        "name": "spawn_agent",
        "description": "Alias for herdr_spawn.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "node": {"type": "STRING"},
                "cwd": {"type": "STRING"},
                "worker": {"type": "STRING"},
                "instructions": {"type": "STRING"},
                "isolate_branch": {"type": "BOOLEAN"}
            },
            "required": ["node", "cwd", "instructions"]
        }
    },
    {
        "name": "read_agent",
        "description": "Alias for herdr_read.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "pane_id": {"type": "STRING"}
            },
            "required": ["pane_id"]
        }
    },
    {
        "name": "stop_agent",
        "description": "Alias for herdr_stop.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "pane_id": {"type": "STRING"},
                "rollback": {"type": "BOOLEAN"}
            },
            "required": ["pane_id"]
        }
    },
    {
        "name": "herdr_pane_run",
        "description": "Execute a shell command in the dedicated persistent runner pane (Pane 2) without blocking liaison reasoning.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "pane": {
                    "type": "STRING",
                    "description": "The pane identifier or label (default: 'runner')."
                },
                "command": {
                    "type": "STRING",
                    "description": "The shell command to execute in Pane 2."
                }
            },
            "required": ["command"]
        }
    },
    {
        "name": "herdr_agent_read",
        "description": "Inspect the terminal buffer of an agent or pane (e.g. runner) to observe interactive prompts, OAuth listeners, or build outputs.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "pane": {
                    "type": "STRING",
                    "description": "The pane or agent identifier (default: 'runner')."
                },
                "lines": {
                    "type": "INTEGER",
                    "description": "Number of terminal output lines to read (default: 30)."
                }
            }
        }
    },
    {
        "name": "herdr_handoff",
        "description": "Handoff heavy coding or homelab execution to a backstage hand worker (default: 'agy'; supports 'opencode', 'hermes').",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "worker": {
                    "type": "STRING",
                    "description": "The target worker agent (default: 'agy'; supports 'opencode', 'hermes')."
                },
                "subagent": {
                    "type": "STRING",
                    "description": "Legacy alias for worker."
                },
                "task": {
                    "type": "STRING",
                    "description": "Detailed task instructions for the worker."
                },
                "cwd": {
                    "type": "STRING",
                    "description": "Target working directory."
                },
                "node": {
                    "type": "STRING",
                    "description": "Target fleet machine node (default: hermes)."
                }
            },
            "required": ["task"]
        }
    }
]


async def herdr_spawn(
    node: str = "hermes",
    cwd: str = "/Users/hermes",
    worker: Optional[str] = None,
    prompt: str = "",
    instructions: str = "",
    isolate_branch: bool = True
) -> Dict[str, Any]:
    """
    Spawns a worker agent in Herdr:
    1. Ensures `cwd` exists on the target filesystem.
    2. Defaults `worker` to get_default_hand_worker() ('agy') if None or empty.
    3. If `isolate_branch` is True and `cwd` is a git repository, checks out local branch `task-<ts>-<worker>`.
    4. Spawns worker pane via `herdr agent spawn --node <node> --cwd <cwd> --agent <worker>`
       and configures FrugaLLM credentials (0.0.0.0:61721).
    5. Returns {"pane_id": "...", "node": node, "cwd": cwd, "branch": branch_name, "worker": effective_worker}.
    """
    raw_worker = worker.strip() if (worker is not None and isinstance(worker, str) and worker.strip()) else get_default_hand_worker()
    effective_worker = WorkerDelegator.validate_worker(raw_worker)
    effective_prompt = prompt or instructions
    herdr_bin = os.environ.get("HERDR_BIN") or HERDR_BIN_DEFAULT
    effective_cwd = os.path.realpath(os.path.abspath(os.path.expanduser(cwd)))
    os.makedirs(effective_cwd, exist_ok=True)

    branch_name: Optional[str] = None
    base_branch = "main"

    # Git branch isolation if requested
    if isolate_branch:
        is_git = False
        try:
            chk_proc = await asyncio.create_subprocess_exec(
                "git", "rev-parse", "--is-inside-work-tree",
                cwd=effective_cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            chk_out, _ = await chk_proc.communicate()
            is_git = (chk_proc.returncode == 0 and chk_out.decode().strip() == "true")
        except Exception as e:
            logger.debug(f"Git check failed in {effective_cwd}: {e}")
            is_git = False

        if is_git:
            try:
                base_proc = await asyncio.create_subprocess_exec(
                    "git", "rev-parse", "--abbrev-ref", "HEAD",
                    cwd=effective_cwd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                base_out, _ = await base_proc.communicate()
                if base_proc.returncode == 0:
                    base_branch = base_out.decode().strip() or "main"
            except Exception:
                base_branch = "main"

            ts = int(time.time())
            clean_worker = effective_worker.split("-")[0] if "-" in effective_worker else effective_worker
            branch_name = f"task-{ts}-{clean_worker}"

            try:
                co_proc = await asyncio.create_subprocess_exec(
                    "git", "checkout", "-b", branch_name,
                    cwd=effective_cwd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await co_proc.communicate()
                logger.info(f"Isolated code modifications in {effective_cwd} on branch {branch_name}")
            except Exception as e:
                logger.warning(f"Failed to checkout task branch {branch_name}: {e}")

    # Spawns worker pane via `herdr agent spawn --node <node> --cwd <cwd> --agent <worker>`
    spawn_cmd = [
        herdr_bin, "agent", "spawn",
        "--node", node,
        "--cwd", effective_cwd,
        "--agent", effective_worker
    ]
    pane_id: Optional[str] = None

    try:
        sp_proc = await asyncio.create_subprocess_exec(
            *spawn_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        sp_out, sp_err = await sp_proc.communicate()
        if sp_proc.returncode == 0:
            out_str = sp_out.decode("utf-8", errors="replace").strip()
            m = re.search(r'(w\d+:p\d+)', out_str)
            if m:
                pane_id = m.group(1)
            else:
                pane_id = out_str.strip().split()[-1] if out_str else None
    except Exception as e:
        logger.debug(f"Direct herdr agent spawn call returned: {e}")

    # Fallback to HerdrClient / pane allocation if spawn command not directly supported by binary
    if not pane_id:
        try:
            from herdr_client import HerdrClient
            client = HerdrClient(herdr_bin=herdr_bin)
            # Map node to default Herdr workspace
            workspace_map = {
                "hermes": "fleet-control",
                "localhost": "fleet-control",
                "desktop-pc": "compute-grid",
                "personal-laptop": "dev-grid",
                "oci-machine": "cloud-ops"
            }
            ws_label = workspace_map.get(node, "dev-grid")
            pane_id = await client.allocate_worker_pane(ws_label, cwd=effective_cwd)
        except Exception as e:
            logger.warning(f"HerdrClient pane allocation fallback: {e}")
            pane_id = f"w1:p{int(time.time()) % 100}"

    # Pass instructions via FrugaLLM (0.0.0.0:61721)
    flm_env_cmd = (
        f'export OPENAI_BASE_URL="http://0.0.0.0:61721/v1" && '
        f'export OPENAI_API_BASE="http://0.0.0.0:61721/v1" && '
        f'export OPENAI_API_KEY="horned66" && '
        f'export MODEL="frugallm/coding"'
    )
    try:
        env_proc = await asyncio.create_subprocess_exec(
            herdr_bin, "pane", "run", pane_id, flm_env_cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await env_proc.communicate()
    except Exception:
        pass

    if effective_prompt:
        try:
            prompt_proc = await asyncio.create_subprocess_exec(
                herdr_bin, "agent", "prompt", pane_id, effective_prompt,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            await prompt_proc.communicate()
        except Exception:
            pass

    # Save to active spawned agents registry
    SPAWNED_AGENTS[pane_id] = {
        "pane_id": pane_id,
        "node": node,
        "cwd": effective_cwd,
        "branch": branch_name,
        "base_branch": base_branch,
        "worker": effective_worker,
        "prompt": effective_prompt,
        "instructions": effective_prompt,
        "start_time": time.time()
    }

    return {
        "pane_id": pane_id,
        "node": node,
        "cwd": effective_cwd,
        "branch": branch_name,
        "worker": effective_worker,
    }


async def herdr_read(pane_id: str) -> str:
    """
    Calls `herdr agent read <pane_id>` to fetch the latest settled terminal output buffer (last 30 lines) and returns raw text.
    """
    herdr_bin = os.environ.get("HERDR_BIN") or HERDR_BIN_DEFAULT
    try:
        proc = await asyncio.create_subprocess_exec(
            herdr_bin, "agent", "read", pane_id, "--lines", "30",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        out, _ = await proc.communicate()
        text = out.decode("utf-8", errors="replace").strip()
        if text:
            return text

        # Fallback to pane read
        p_proc = await asyncio.create_subprocess_exec(
            herdr_bin, "pane", "read", pane_id, "--lines", "30",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        p_out, _ = await p_proc.communicate()
        p_text = p_out.decode("utf-8", errors="replace").strip()
        return p_text or f"No terminal output recorded for pane {pane_id}."
    except Exception as e:
        return f"Error reading agent in pane {pane_id}: {e}"


async def herdr_stop(pane_id: str, cwd: str = "", rollback: bool = False) -> str:
    """
    Terminates the agent pane via `herdr agent stop <pane_id>`.
    If a server was spawned, kills any listening process in cwd.
    If `rollback=True` and a git branch was created: executes
    `git reset --hard && git clean -fd && git checkout main && git branch -D <branch>` in `cwd`.
    Returns confirmation message.
    """
    herdr_bin = os.environ.get("HERDR_BIN") or HERDR_BIN_DEFAULT
    agent_info = SPAWNED_AGENTS.get(pane_id, {})
    effective_cwd = cwd or agent_info.get("cwd", "")
    branch = agent_info.get("branch")
    base_branch = agent_info.get("base_branch", "main")

    # Stop agent process
    try:
        st_proc = await asyncio.create_subprocess_exec(
            herdr_bin, "agent", "stop", pane_id,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await st_proc.communicate()
    except Exception:
        pass

    # Close pane
    try:
        cl_proc = await asyncio.create_subprocess_exec(
            herdr_bin, "pane", "close", pane_id,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await cl_proc.communicate()
    except Exception:
        pass

    # Terminate any background servers in effective_cwd
    if effective_cwd:
        try:
            from supervisor import AgentSupervisor
            await AgentSupervisor.terminate_server_in_dir(effective_cwd)
        except Exception as e:
            logger.debug(f"Error terminating background servers in {effective_cwd}: {e}")

    rollback_detail = ""
    # Git rollback if requested and branch was isolated
    if rollback and effective_cwd and branch:
        if os.path.isdir(effective_cwd):
            rb_cmd = f"git reset --hard && git clean -fd && git checkout {base_branch} && git branch -D {branch}"
            try:
                proc = await asyncio.create_subprocess_shell(
                    rb_cmd,
                    cwd=effective_cwd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await proc.communicate()
                rollback_detail = f" Rolled back working directory `{effective_cwd}` to `{base_branch}` and deleted branch `{branch}`."
                logger.info(f"Rolled back {effective_cwd} to {base_branch} and deleted {branch}")
            except Exception as e:
                rollback_detail = f" Git rollback error: {e}."

    SPAWNED_AGENTS.pop(pane_id, None)
    return f"Stopped agent pane `{pane_id}`.{rollback_detail}".strip()


async def run_command(
    cmd: str,
    cwd: str = "/Users/hermes",
    pane: str = "runner",
    yield_timeout: float = 3.0,
) -> str:
    """
    Execute a shell inspection or diagnostic command without blocking the liaison loop.
    Routes command execution through RunnerExecutor targeting Pane 2 (the runner pane)
    with a strict <= 3.0s yield timeout ceiling so the liaison is never blocked by host subprocesses.
    """
    effective_cwd = os.path.realpath(os.path.abspath(os.path.expanduser(cwd))) if cwd else "/Users/hermes"
    if not os.path.isdir(effective_cwd):
        effective_cwd = "/Users/hermes"

    sanitized_cmd = cmd
    if re.search(r"\b(?:ffmpeg|ffprobe)\b", sanitized_cmd):
        if "-nostdin" not in sanitized_cmd:
            sanitized_cmd = re.sub(r"\b(ffmpeg|ffprobe)\b", r"\1 -nostdin", sanitized_cmd, count=1)
        if "-hide_banner" not in sanitized_cmd:
            sanitized_cmd = re.sub(r"\b(ffmpeg|ffprobe)\b", r"\1 -hide_banner", sanitized_cmd, count=1)

    full_cmd = f"cd {effective_cwd} && {sanitized_cmd}" if effective_cwd != "/Users/hermes" else sanitized_cmd

    from golem.execution.runner import RunnerExecutor
    executor = RunnerExecutor()

    try:
        handle = await executor.run_command(pane=pane, command=full_cmd, yield_timeout=yield_timeout)
        if handle.status == "completed":
            return handle.stdout or "(No output)"
        elif handle.status == "running":
            try:
                from golem.gateway.session import CURRENT_SESSION
                session = CURRENT_SESSION.get()
                if session is not None:
                    session.active_execution = handle
            except Exception as e:
                logger.debug(f"Failed to record active_execution on CURRENT_SESSION: {e}")

            return (
                f"[Running in Pane 2 (pid={handle.pid})]: "
                f"{handle.message or 'Command dispatched to runner pane; executing in background without blocking liaison.'}"
            )
        else:
            combined = f"{handle.stdout}\n{handle.stderr}".strip()
            return f"Command exited with code {handle.returncode}:\n{combined or handle.message}"
    except Exception as e:
        logger.error(f"Error executing command '{cmd}' via RunnerExecutor in pane {pane}: {e}")
        return f"Error executing command '{cmd}': {e}"


async def herdr_pane_run(
    pane: str = "runner",
    command: str = "",
    workspace: Optional[str] = None,
    yield_timeout: float = 3.0,
) -> Dict[str, Any]:
    """
    Execute a shell command in the dedicated runner pane (Pane 2) without blocking liaison reasoning.
    Enforces a strict <= 3.0s yield timeout ceiling and clamps auxiliary timeouts.
    """
    if not command:
        return {"error": "Command string is required."}

    dispatched_at = time.time()
    herdr_bin = shutil.which("herdr") or HERDR_BIN_DEFAULT
    cmd_args = [herdr_bin, "pane", "run", pane, command]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            rem_initial = max(0.05, yield_timeout - (time.time() - dispatched_at))
            stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=rem_initial)
            code = proc.returncode if proc.returncode is not None else 0
            out = stdout_bytes.decode("utf-8", errors="replace").strip()
            err = stderr_bytes.decode("utf-8", errors="replace").strip()

            # If pane run failed with pane_not_found, try resolving pane_id via herdr pane list
            if code != 0 and ("pane_not_found" in err or "not found" in err.lower()):
                try:
                    rem_list = max(0.05, min(1.0, yield_timeout - (time.time() - dispatched_at)))
                    list_proc = await asyncio.create_subprocess_exec(
                        herdr_bin, "pane", "list",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    l_out, _ = await asyncio.wait_for(list_proc.communicate(), timeout=rem_list)
                    if list_proc.returncode == 0 and l_out:
                        data = json.loads(l_out.decode("utf-8", errors="replace"))
                        panes_list = data.get("result", {}).get("panes", []) if isinstance(data, dict) else []
                        for p_info in panes_list:
                            if p_info.get("label") == pane or p_info.get("name") == pane:
                                resolved_id = p_info.get("pane_id")
                                if resolved_id:
                                    rem_rerun = max(0.05, yield_timeout - (time.time() - dispatched_at))
                                    r_proc = await asyncio.create_subprocess_exec(
                                        herdr_bin, "pane", "run", resolved_id, command,
                                        stdout=asyncio.subprocess.PIPE,
                                        stderr=asyncio.subprocess.PIPE
                                    )
                                    r_out_b, r_err_b = await asyncio.wait_for(r_proc.communicate(), timeout=rem_rerun)
                                    return {
                                        "pane": resolved_id,
                                        "command": command,
                                        "status": "dispatched" if r_proc.returncode == 0 else "error",
                                        "returncode": r_proc.returncode,
                                        "stdout": r_out_b.decode("utf-8", errors="replace").strip(),
                                        "stderr": r_err_b.decode("utf-8", errors="replace").strip()
                                    }
                except Exception:
                    pass

            return {
                "pane": pane,
                "command": command,
                "status": "dispatched" if code == 0 else "error",
                "returncode": code,
                "stdout": out,
                "stderr": err
            }
        except asyncio.TimeoutError:
            try:
                from golem.gateway.session import CURRENT_SESSION
                from golem.execution.runner import ExecutionHandle
                session = CURRENT_SESSION.get()
                if session is not None:
                    session.active_execution = ExecutionHandle(
                        status="running",
                        pane=pane,
                        command=command,
                        message=f"Command dispatched to {pane} pane; executing in background without blocking liaison."
                    )
            except Exception:
                pass
            return {
                "pane": pane,
                "command": command,
                "status": "running",
                "message": f"Command dispatched to {pane} pane; executing in background without blocking liaison."
            }
    except Exception as e:
        logger.error(f"Error in herdr_pane_run: {e}")
        return {"error": str(e), "pane": pane, "command": command}


async def herdr_agent_read(
    pane: str = "runner",
    lines: int = 30
) -> str:
    """
    Read terminal output buffer from runner pane or agent via herdr agent read or herdr pane read.
    """
    herdr_bin = shutil.which("herdr") or HERDR_BIN_DEFAULT
    cmd_args = [herdr_bin, "agent", "read", pane, "--lines", str(lines)]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        out = stdout_bytes.decode("utf-8", errors="replace").strip()
        code = proc.returncode if proc.returncode is not None else 0

        # If agent read succeeded and returned clean text (not error JSON), return it
        if code == 0 and out and "error" not in out:
            return out

        # Fallback to pane read (for plain shell panes like 'runner')
        pane_cmd = [herdr_bin, "pane", "read", pane, "--lines", str(lines)]
        p_proc = await asyncio.create_subprocess_exec(
            *pane_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        p_stdout, p_stderr = await asyncio.wait_for(p_proc.communicate(), timeout=5.0)
        p_out = p_stdout.decode("utf-8", errors="replace").strip()
        p_code = p_proc.returncode if p_proc.returncode is not None else 0
        if p_code == 0 and p_out and "error" not in p_out:
            return p_out

        # If pane read by label failed, try resolving pane_id via herdr pane list
        try:
            list_proc = await asyncio.create_subprocess_exec(
                herdr_bin, "pane", "list",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            l_out, _ = await asyncio.wait_for(list_proc.communicate(), timeout=3.0)
            if list_proc.returncode == 0 and l_out:
                data = json.loads(l_out.decode("utf-8", errors="replace"))
                panes_list = data.get("result", {}).get("panes", []) if isinstance(data, dict) else []
                for p_info in panes_list:
                    if p_info.get("label") == pane or p_info.get("name") == pane:
                        resolved_id = p_info.get("pane_id")
                        if resolved_id:
                            r_proc = await asyncio.create_subprocess_exec(
                                herdr_bin, "pane", "read", resolved_id, "--lines", str(lines),
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            r_out, _ = await asyncio.wait_for(r_proc.communicate(), timeout=5.0)
                            if r_proc.returncode == 0 and r_out:
                                return r_out.decode("utf-8", errors="replace").strip()
        except Exception:
            pass

        return p_out or out or f"No terminal output recorded for pane {pane}."
    except Exception as e:
        logger.error(f"Error in herdr_agent_read: {e}")
        return f"Error reading pane {pane}: {e}"


async def herdr_handoff(
    subagent: Optional[str] = None,
    task: str = "",
    instructions: Optional[str] = None,
    cwd: Optional[str] = None,
    node: str = "hermes",
    worker: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Handoff heavy coding or homelab execution to a backstage hand worker (default: 'agy'; supports 'opencode', 'hermes').
    Worker parameter takes precedence over subagent; if neither is provided, defaults to get_default_hand_worker() ('agy').
    """
    raw_worker = worker if worker is not None else (subagent if subagent is not None else get_default_hand_worker())
    if isinstance(raw_worker, str):
        raw_worker = raw_worker.strip()
    if not raw_worker:
        raw_worker = get_default_hand_worker()
    effective_worker = WorkerDelegator.validate_worker(raw_worker)

    prompt = task or instructions or ""
    target_cwd = cwd or "/Users/hermes"
    return await herdr_spawn(
        node=node,
        cwd=target_cwd,
        worker=effective_worker,
        prompt=prompt,
        instructions=prompt,
        isolate_branch=True
    )


# Backwards compatibility aliases
spawn_agent = herdr_spawn
read_agent = herdr_read
stop_agent = herdr_stop
pane_run = herdr_pane_run
agent_read = herdr_agent_read
handoff = herdr_handoff

BANNED_ACTION_PAYLOAD_PATTERNS = [
    re.compile(r"^\s*echo\s+['\"].*?['\"]\s*$", re.IGNORECASE),
    re.compile(r"^\s*echo\s+.*?$", re.IGNORECASE),
    re.compile(r"^\s*python(?:3)?\s+-c\s+['\"].*?['\"]\s*$", re.IGNORECASE),
    re.compile(r"^\s*$", re.IGNORECASE)
]

def validate_action_payload(payload: Any, target_node: str = "hermes", worker_id: Optional[str] = None) -> str:
    clean = str(payload or "").strip()
    is_banned = not clean or any(p.match(clean) for p in BANNED_ACTION_PAYLOAD_PATTERNS)
    if not is_banned:
        return clean
    if worker_id == "opencode-dev" or "opencode" in str(worker_id).lower():
        return "Herdr worktree create -> Dispatch opencode-dev in dev-grid (FrugaLLM :61721) -> no-mistakes review"
    if target_node in ("hermes", "localhost", "local"):
        return "git status -s"
    elif target_node == "desktop-pc":
        return "nvidia-smi"
    elif target_node == "oci-machine":
        return "uptime && uname -a"
    return "git status -s"


def build_agent_tool_dispatch_table() -> Dict[str, Any]:
    """Build dispatch table mapping tool names to execution coroutines/functions."""
    return {
        "herdr_spawn": herdr_spawn,
        "spawn_agent": spawn_agent,
        "herdr_read": herdr_read,
        "read_agent": read_agent,
        "herdr_stop": herdr_stop,
        "stop_agent": stop_agent,
        "herdr_pane_run": herdr_pane_run,
        "pane_run": pane_run,
        "herdr_agent_read": herdr_agent_read,
        "agent_read": agent_read,
        "herdr_handoff": herdr_handoff,
        "handoff": handoff,
        "run_command": run_command,
    }


__all__ = [
    "SPAWNED_AGENTS",
    "HERDR_TOOLS_DECLARATIONS",
    "HERDR_BIN_DEFAULT",
    "herdr_spawn",
    "spawn_agent",
    "herdr_read",
    "read_agent",
    "herdr_stop",
    "stop_agent",
    "herdr_pane_run",
    "pane_run",
    "herdr_agent_read",
    "agent_read",
    "herdr_handoff",
    "handoff",
    "run_command",
    "validate_action_payload",
    "BANNED_ACTION_PAYLOAD_PATTERNS",
    "build_agent_tool_dispatch_table",
    "get_default_hand_worker",
]


