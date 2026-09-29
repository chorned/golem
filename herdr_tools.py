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

logger = logging.getLogger("FirstMate.HerdrTools")

# In-memory registry of active spawned agent panes and their metadata
SPAWNED_AGENTS: Dict[str, Dict[str, Any]] = {}

HERDR_BIN_DEFAULT = shutil.which("herdr") or os.path.expanduser("~/.local/bin/herdr")

HERDR_TOOLS_DECLARATIONS = [
    {
        "name": "herdr_spawn",
        "description": "Spawn a junior worker agent in Herdr on a specific fleet node and directory to perform a task.",
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
                    "description": "The worker agent to spawn (e.g. opencode-dev, hermes, opencode)."
                },
                "prompt": {
                    "type": "STRING",
                    "description": "Detailed task prompt and requirements for the junior worker."
                },
                "isolate_branch": {
                    "type": "BOOLEAN",
                    "description": "Whether to isolate code modifications on a new local git branch (default: true for code edits, false for system queries/read-only ops)."
                }
            },
            "required": ["node", "cwd", "worker", "prompt"]
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
            "required": ["node", "cwd", "worker", "instructions"]
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
    }
]


async def herdr_spawn(
    node: str = "hermes",
    cwd: str = "/Users/hermes",
    worker: str = "opencode-dev",
    prompt: str = "",
    instructions: str = "",
    isolate_branch: bool = True
) -> Dict[str, Any]:
    """
    Spawns a junior worker agent in Herdr:
    1. Ensures `cwd` exists on the target filesystem.
    2. If `isolate_branch` is True and `cwd` is a git repository, checks out local branch `task-<ts>-<worker>`.
    3. Spawns worker pane via `herdr agent spawn --node <node> --cwd <cwd> --agent <worker>`
       and configures FrugaLLM credentials (0.0.0.0:61721).
    4. Returns {"pane_id": "...", "node": node, "cwd": cwd, "branch": branch_name}.
    """
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
            clean_worker = worker.split("-")[0] if "-" in worker else worker
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
        "--agent", worker
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
        "worker": worker,
        "prompt": effective_prompt,
        "instructions": effective_prompt,
        "start_time": time.time()
    }

    return {
        "pane_id": pane_id,
        "node": node,
        "cwd": effective_cwd,
        "branch": branch_name
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


async def run_command(cmd: str, cwd: str = "/Users/hermes") -> str:
    """
    Runs a shell inspection command (e.g. lsof, git status, ls -la) in cwd and returns output.
    """
    effective_cwd = os.path.realpath(os.path.abspath(os.path.expanduser(cwd))) if cwd else "/Users/hermes"
    if not os.path.isdir(effective_cwd):
        effective_cwd = "/Users/hermes"

    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            cwd=effective_cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15.0)
        out_text = stdout.decode("utf-8", errors="replace").strip()
        err_text = stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode == 0:
            return out_text or "(No output)"
        else:
            combined = f"{out_text}\n{err_text}".strip()
            return f"Command exited with code {proc.returncode}:\n{combined}"
    except asyncio.TimeoutError:
        return f"Command '{cmd}' timed out after 15 seconds."
    except Exception as e:
        return f"Error executing command '{cmd}': {e}"


# Backwards compatibility aliases
spawn_agent = herdr_spawn
read_agent = herdr_read
stop_agent = herdr_stop

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

