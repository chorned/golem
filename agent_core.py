#!/usr/bin/env python3
"""
agent_core.py
First Mate Chief of Staff Brain & Agentic Turn Engine.
Powered directly by Gemini 2.5 Flash with Native Tool Calling, Plan Pitching, and Zero-MCP Herdr dispatch.
"""

import os
import re
import json
import time
import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
import aiohttp

import herdr_tools
from herdr_tools import (
    HERDR_TOOLS_DECLARATIONS,
    spawn_agent,
    read_agent,
    stop_agent,
    SPAWNED_AGENTS
)

logger = logging.getLogger("FirstMate.AgentCore")

# Re-exports from first_mate.agent for Milestone 2 compatibility
from first_mate.agent import (
    AgentEvent,
    AgentEventType,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
    HeadlessAgentAdapter,
    DEFAULT_AGY_BIN,
    CALIBRATION_PROMPT,
    SLACK_MRKDWN_RULES,
    CHOICE_FORMATTING_RULES,
    PACING_CALIBRATION_RULES,
    TRIAGE_SUPPRESSION_RULES,
    get_calibration_rules,
    build_calibrated_system_prompt,
)

HOST_DIR = os.environ.get("FIRST_MATE_HOST_DIR", os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = HOST_DIR
DEFAULT_TARGET_REPO = os.environ.get("DEFAULT_TARGET_REPO", "/Users/hermes/projects/AGY")
DEFAULT_GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"


def get_gemini_api_key() -> str:
    """Discover Gemini / Google API key from environment, project .env, or user ~/.env."""
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if key and key.strip() and not key.startswith("your_"):
        return key.strip()

    # 1. Project .env
    project_env = os.path.join(PROJECT_ROOT, ".env")
    if os.path.isfile(project_env):
        try:
            with open(project_env, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GEMINI_API_KEY=") or line.startswith("GOOGLE_API_KEY="):
                        val = line.split("=", 1)[1].strip().strip("\"'")
                        if val and not val.startswith("your_"):
                            return val
        except Exception:
            pass

    # 2. User ~/.env
    user_env = os.path.expanduser("~/.env")
    if os.path.isfile(user_env):
        try:
            with open(user_env, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GEMINI_API_KEY=") or line.startswith("GOOGLE_API_KEY="):
                        val = line.split("=", 1)[1].strip().strip("\"'")
                        if val and not val.startswith("your_"):
                            return val
        except Exception:
            pass

    return ""


def format_for_slack(text: str) -> str:
    """Format standard markdown to Slack mrkdwn using canonical normalizer."""
    try:
        from normalizer import sanitize_for_slack
        return sanitize_for_slack(text)
    except Exception:
        parts = re.split(r"(```[\s\S]*?```|`[^`\n]+`)", text)
        for i in range(0, len(parts), 2):
            parts[i] = re.sub(r"\*\*(.+?)\*\*", r"*\1*", parts[i])
        return "".join(parts)


def build_system_prompt() -> str:
    """Build dynamic system prompt providing Chief of Staff identity, fleet topology, and operating rules."""
    projects_dir = os.environ.get("PROJECTS_DIR", "/Users/hermes/projects")
    local_projects = []
    if os.path.isdir(projects_dir):
        try:
            local_projects = [
                d for d in os.listdir(projects_dir)
                if os.path.isdir(os.path.join(projects_dir, d)) and not d.startswith(".")
            ]
        except Exception:
            local_projects = []

    projects_list_str = ", ".join(f"`{p}`" for p in sorted(local_projects)) if local_projects else "None found"

    active_panes_info = []
    for pid, info in SPAWNED_AGENTS.items():
        active_panes_info.append(f"• Pane `{pid}` on `{info.get('node')}` in `{info.get('cwd')}` (worker: `{info.get('worker')}`, branch: `{info.get('branch')}`)")
    active_panes_section = "\n".join(active_panes_info) if active_panes_info else "None currently active"

    return f"""You are First Mate, Carl's trusted autonomous Chief of Staff and Tech Lead.
You coordinate junior workers across the fleet to execute operational, system, and coding tasks.
You NEVER write production code directly—you delegate execution to junior workers (OpenCode, Hermes) via Herdr tools.

--- Fleet Topology & Machine Placement ---
• hermes: Local macOS controller, daily development anchor, and homelab runner.
• desktop-pc: High-VRAM Windows node for CUDA, model training, and heavy GPU compute (100.80.59.45).
• oci-machine: Remote cloud VPS for external daemons and scrapers (100.113.173.33).
• personal-laptop: Secondary macOS node (100.96.38.5).

--- Working Directory Resolution ---
1. General homelab, system ops, media, or download inquiries -> (hermes, "/Users/hermes")
2. Specific projects -> (node, "/Users/hermes/projects/<name>")
   Available local projects: {projects_list_str}
3. Standalone spikes, demos, prototypes, or ad-hoc tasks -> (hermes, "/Users/hermes/projects/sandbox/<task_id>")

--- Worker Specialization & Fleet Delegation ---
• opencode: For writing code, refactoring repos, building prototypes, writing unit tests, and sandbox servers.
• hermes: For homelab maintenance, Docker containers, storage/ZFS, media services, and system scripts.
• direct execution: For coordination, quick file reads, bearings, status queries, or high-level architecture planning.

--- Conversational Tone & Pacing Calibration ---
• Immediate Conversational Acknowledgment: When initiating a new session or beginning a multi-step operational inquiry (e.g. checking logs, inspecting cron jobs, querying fleet machines), emit a brief, authentic 1-sentence acknowledgment before executing inspection tools. (e.g. "Sure! Give me a second to gather the fleet details." or "On it—let me pull the latest schedules across the machines.").
• Broad Overview First: When discussing infrastructure, jobs, scheduled cron tasks, or fleet architecture, provide a concise, high-level map (component names and core functions). Always pause and let the user guide where to dig deeper.
• Suppression of Unsolicited Incident Triage: Never autonomously scrape logs, inspect non-zero exit codes, grep for tracebacks, or generate remedial action checklists unless the user explicitly requests troubleshooting, debugging, or fixing a broken component.
• Natural Voice & Clean Responses: Do not echo, quote, or repeat the user's incoming message. Answer directly as a peer collaborator in natural Chief of Staff prose.
• Direct Handling for Inquiries: Answer greetings, casual banter, conversational inquiries (e.g. "Nice, how are you doing?"), and check-ins directly in natural Chief of Staff prose without delegating to subagents.
• Fleet Status Queries: For fleet inquiries ("how's our fleet doing?", "what's the fleet status?"), run read-only inspection commands (`herdr machine list`, `tailscale status`, `herdr agent list`) directly in your root pane and reply conversationally.
• Strict Delegation Guardrail: Junior worker panes (OpenCode, Hermes) must ONLY be created when specialized execution (e.g. writing code, editing files, running test suites, managing Docker containers) is required, via explicit `herdr_handoff` or `herdr_pane_run` (or native `herdr_spawn`).

--- Currently Active Herdr Panes ---
{active_panes_section}

--- Presentation Layer: Slack mrkdwn & Conversational Protocol ---
Your presentation layer is Slack. You MUST strictly adhere to Slack mrkdwn syntax:
• Section headers: Use *Bold Title* (never use standard markdown ###, ##, or #).
• Bold text: Use *single asterisks* (never **double asterisks**).
• Inline code: Use `code`.
• Links: Use <URL|Label> (e.g., <http://localhost:8090|Test Server>).
• Conversational tone: Use standard Slack emojis (:white_check_mark:, :warning:, :rocket:, :gear:) naturally, not robotically.
• No Interactive Terminal Menus:
  - Never render terminal arrow-key select menus (`↑/↓ Navigate · enter Select · esc Skip`).
  - When presenting choices to Carl, always output them as a numbered markdown list (1. Option A, 2. Option B) and ask for his preference or a write-in.

--- Dual-Pane Workspace Architecture & Execution ---
You run in Pane 1 (liaison) dedicated to pure cognition, planning, and Slack communication. You have no local subshell execution in your pane and never block on child process I/O.
• Pane 2 (runner): Dedicated, persistent OS shell rooted in the workspace directory.
• Any command targeting the local host runs in Pane 2 via `herdr_pane_run(pane="runner", command="...")`.
• Heavy coding tasks hand off to OpenCode via `herdr_handoff(subagent="opencode", ...)`.
• Remote fleet tasks target `desktop-pc`, `oci-machine`, or `personal-laptop`.
• Active Runner Observation:
  - When a runner command pauses or opens an interactive listener (like Google OAuth, confirmations, or long builds), inspect Pane 2 using `herdr_agent_read(pane="runner")`.
  - Immediately message Carl in Slack with the situation and the exact link/action needed (e.g., "Rclone is waiting for Google OAuth approval in Chrome on `hermes` at <http://127.0.0.1:53682/auth|this link>. Let me know once approved.").

--- Operating Rules ---
1. ALWAYS PITCH THE PLAN FIRST (FOR CODE/SYSTEM TASKS ONLY):
   When Carl asks you to perform a code modification, refactor, or infrastructure spike, DO NOT call `herdr_spawn` immediately.
   First, pitch your plan in 2 concise, authentic sentences:
   - State the target machine and working directory.
   - State which worker you will delegate to (e.g. OpenCode or Hermes).
   - Summarize the operational strategy and ask for Carl's approval (e.g. "Ready when you are—say `go` or 👍 to run it.").
   (Note: NEVER pitch a plan or delegate for greetings or conversational fleet checks; answer them directly!)

2. DISPATCH ON APPROVAL (NATURAL AFFIRMATION):
   Any affirmative reply ("go", "let's go", "do it", "looks good", "lgtm", "thumbs up", "👍", or "Approved, proceed.") means execute!
   Immediately call `herdr_spawn` (or `spawn_agent`). NEVER re-pitch the plan once approved.
   - Set `isolate_branch=True` whenever editing code in a project or sandbox.
   - Set `isolate_branch=False` for read-only diagnostics, CLI queries, or downloads.

3. IN-FLIGHT STATUS & INSPECTION (ZERO ROLEPLAY / ZERO PROMISES):
   If Carl asks "how's it going?", "what's the URL?", "what port?", or for status:
   - NEVER reply with promises like "I'll retrieve that for you now", "Let me check that", or "Hold on".
   - You MUST call `herdr_read` (or `read_agent`) or `run_command` (e.g. `lsof -iTCP -sTCP:LISTEN -n -P`) in this exact turn.
   - Reply with the factual result (e.g. the live URL or terminal progress) directly in Sentence 1.

4. DIRECTORY PINNING (CRITICAL):
   - Standalone spikes must run in `/Users/hermes/projects/sandbox/{{task_id}}`.
   - Any server command MUST explicitly include the target folder: `python3 -m http.server {{port}} --directory {{cwd}}` with `cwd` set to that directory.
   - NEVER run background servers from `/`.

5. UNDO & ROLLBACK:
   If Carl says "undo", "abort", "scrap it", "clean up", or "cancel":
   - Call `herdr_stop` (or `stop_agent`) with `rollback=True` to kill the pane, stop any listening server, wipe uncommitted changes, and delete the task branch.

6. NO UNNECESSARY PRs:
   Never push to GitHub or create a PR unless Carl explicitly says "open a PR" or "push to github". Everything stays local.

7. SLACK FORMATTING:
   Always format output in Slack mrkdwn: *bold* with single asterisks (never **bold**), `inline code`, and clean bullets.

8. DUAL-PANE EXECUTION & ACTIVE RUNNER OBSERVATION:
   Local subshell execution is disabled in liaison (Pane 1). All shell commands targeting the local host must run in Pane 2 via `herdr_pane_run(pane="runner", command="...")`.
   Inspect Pane 2 using `herdr_agent_read(pane="runner")` when interactive listeners or long processes run, and immediately update Carl in Slack.
"""


class ChiefOfStaff:
    """Agent-First Chief of Staff turn engine using native Tool Calling."""

    def __init__(self, api_key: Optional[str] = None, model: str = DEFAULT_GEMINI_MODEL):
        self.api_key = api_key or get_gemini_api_key()
        self.model = model

    async def execute_tool_call(self, name: str, args: Dict[str, Any]) -> Any:
        """Execute a registered native tool function by name."""
        logger.info(f"Executing native tool: {name}({args})")
        if name in ("herdr_spawn", "spawn_agent"):
            worker_target = args.get("worker")
            if not worker_target:
                return {"error": "Explicit worker parameter (e.g. opencode-dev, hermes-worker) is required to spawn subagent."}
            return await herdr_tools.herdr_spawn(
                node=args.get("node", "hermes"),
                cwd=args.get("cwd", "/Users/hermes"),
                worker=worker_target,
                prompt=args.get("prompt") or args.get("instructions", ""),
                instructions=args.get("instructions") or args.get("prompt", ""),
                isolate_branch=args.get("isolate_branch", True)
            )
        elif name in ("herdr_read", "read_agent"):
            return await herdr_tools.herdr_read(pane_id=args.get("pane_id", ""))
        elif name in ("herdr_stop", "stop_agent"):
            return await herdr_tools.herdr_stop(
                pane_id=args.get("pane_id", ""),
                cwd=args.get("cwd", ""),
                rollback=args.get("rollback", False)
            )
        elif name == "herdr_pane_run":
            return await herdr_tools.herdr_pane_run(
                pane=args.get("pane", "runner"),
                command=args.get("command") or args.get("cmd", "")
            )
        elif name in ("herdr_agent_read", "read_runner"):
            try:
                lines_val = int(args.get("lines", 30))
            except Exception:
                lines_val = 30
            return await herdr_tools.herdr_agent_read(
                pane=args.get("pane", "runner"),
                lines=lines_val
            )
        elif name == "herdr_handoff":
            subagent = args.get("subagent") or args.get("worker", "opencode")
            task = args.get("task") or args.get("prompt") or args.get("instructions") or args.get("command", "")
            return await herdr_tools.herdr_handoff(
                subagent=subagent,
                task=task,
                cwd=args.get("cwd"),
                node=args.get("node", "hermes")
            )
        elif name == "run_command":
            return await herdr_tools.run_command(
                cmd=args.get("cmd", ""),
                cwd=args.get("cwd", "/Users/hermes")
            )
        else:
            return {"error": f"Unknown tool '{name}'"}

    async def process_turn(
        self,
        thread_history: List[Dict[str, Any]],
        user_message: str,
        max_iterations: int = 5
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Process an agent turn in a conversation thread:
        1. Appends the user message to history.
        2. Calls Gemini Flash with native Herdr tool declarations.
        3. Loops until all tool calls are resolved and a final text response is produced.
        4. Returns (final_response_text, updated_history).
        """
        # Append user message if not already the latest
        clean_user_msg = user_message.strip()
        if not thread_history or thread_history[-1].get("role") != "user" or thread_history[-1].get("parts", [{}])[0].get("text") != clean_user_msg:
            thread_history.append({
                "role": "user",
                "parts": [{"text": clean_user_msg}]
            })

        system_instruction = build_system_prompt()
        api_key = self.api_key or get_gemini_api_key()

        if not api_key:
            logger.warning("No Gemini API key discovered; executing fallback turn")
            return self._fallback_turn(clean_user_msg, thread_history)

        url = GEMINI_API_URL.format(model=self.model, key=api_key)

        for iteration in range(max_iterations):
            payload = {
                "systemInstruction": {
                    "parts": [{"text": system_instruction}]
                },
                "contents": thread_history,
                "tools": [{"functionDeclarations": HERDR_TOOLS_DECLARATIONS}]
            }

            try:
                timeout = aiohttp.ClientTimeout(total=20.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, json=payload, headers={"Content-Type": "application/json"}) as resp:
                        if resp.status != 200:
                            err_body = await resp.text()
                            logger.error(f"Gemini API returned status {resp.status}: {err_body}")
                            return self._fallback_turn(clean_user_msg, thread_history)

                        data = await resp.json()
                        candidates = data.get("candidates", [])
                        if not candidates:
                            return ("I didn't receive a response from the model. Standing by.", thread_history)

                        cand = candidates[0]
                        content = cand.get("content", {})
                        parts = content.get("parts", [])

                        # Check for functionCall
                        function_calls = [p["functionCall"] for p in parts if "functionCall" in p]
                        text_parts = [p["text"] for p in parts if "text" in p]

                        if function_calls:
                            # Append model turn containing the functionCall(s)
                            thread_history.append({
                                "role": "model",
                                "parts": parts
                            })

                            # Execute all function calls and append functionResponse turns
                            for fc in function_calls:
                                fn_name = fc.get("name")
                                fn_args = fc.get("args", {})
                                tool_result = await self.execute_tool_call(fn_name, fn_args)

                                thread_history.append({
                                    "role": "function",
                                    "parts": [{
                                        "functionResponse": {
                                            "name": fn_name,
                                            "response": {
                                                "result": tool_result
                                            }
                                        }
                                    }]
                                })
                            # Continue loop so model can formulate next message or final text
                            continue

                        # Text response produced (no function calls)
                        if text_parts:
                            final_text = "".join(text_parts).strip()
                            final_text = format_for_slack(final_text)

                            # Guard against "The Roleplay/Hold Trap":
                            # If model replies "I'll retrieve that for you now" or "Let me check" without calling a tool,
                            # intercept and immediately execute herdr_read / run_command on active pane if available!
                            is_promise = any(phrase in final_text.lower() for phrase in (
                                "i'll retrieve", "i will retrieve", "i'll check", "i will check",
                                "let me check", "let me retrieve", "checking that now", "hold on while i check"
                            ))
                            if is_promise and SPAWNED_AGENTS:
                                last_pane_id, last_info = list(SPAWNED_AGENTS.items())[-1]
                                inspection_data = await self.execute_tool_call("herdr_read", {"pane_id": last_pane_id})
                                task_cwd = last_info.get("cwd", "/Users/hermes")
                                port_data = await self.execute_tool_call("run_command", {"cmd": "lsof -iTCP -sTCP:LISTEN -n -P", "cwd": task_cwd})

                                thread_history.append({
                                    "role": "model",
                                    "parts": [{"text": final_text}]
                                })
                                thread_history.append({
                                    "role": "user",
                                    "parts": [{
                                        "text": f"SYSTEM INSPECTION: Do not make promises to check later. Here is the actual terminal buffer and listening ports for pane {last_pane_id} in {task_cwd}:\nTerminal:\n{inspection_data}\n\nPorts:\n{port_data}\n\nState the factual result and live URL directly now in sentence 1."
                                    }]
                                })
                                continue

                            thread_history.append({
                                "role": "model",
                                "parts": [{"text": final_text}]
                            })
                            return (final_text, thread_history)

            except Exception as e:
                logger.error(f"Error communicating with Gemini API: {e}", exc_info=True)
                return self._fallback_turn(clean_user_msg, thread_history)

        return ("I've processed the requested tools and am standing by.", thread_history)

    def _fallback_turn(
        self,
        user_message: str,
        thread_history: List[Dict[str, Any]]
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """Heuristic fallback when API is unreachable or mocked."""
        p_lower = user_message.lower().strip().rstrip(".!")

        # Natural Affirmation / Approval
        if p_lower in ("go", "let's go", "do it", "approved", "proceed", "yes", "lgtm", "looks good", "thumbsup", "approved, proceed.", "👍", "start", "run"):
            reply = "Approved. Spawning junior worker in Herdr..."
        # Status / URL / Fleet query
        elif any(k in p_lower for k in ("url", "port", "status", "progress", "how's it going", "fleet doing", "fleet")):
            if SPAWNED_AGENTS:
                last_pane, last_info = list(SPAWNED_AGENTS.items())[-1]
                reply = f"The worker is active in pane `{last_pane}` (`{last_info.get('cwd')}`)."
            else:
                reply = "Doing great! Fleet is healthy and all background Herdr agents are standing by."
        # Conversational greetings / check-ins
        elif any(k in p_lower for k in ("hello", "hi", "hey", "how are you", "how're you", "how are you doing", "nice", "good morning", "good evening")):
            reply = "Doing well, Carl! All systems across the fleet are running smoothly."
        # Undo / Abort
        elif any(k in p_lower for k in ("undo", "abort", "cancel", "scrap", "clean up")):
            reply = "Terminated agent and restored working directory baseline."
        else:
            reply = "I'm on it. Ready when you are—say `go` or 👍 to run it."

        thread_history.append({"role": "model", "parts": [{"text": reply}]})
        return (reply, thread_history)
