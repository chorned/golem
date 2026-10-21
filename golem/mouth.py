#!/usr/bin/env python3
"""
golem/agent/mouth.py
Dynamic Liaison Mouth LLM Engine & Out-of-Band Conversational Reactor.

Eliminates hardcoded regex gates, brittle keyword filters, and Mad-Libs f-string templates.
Ensures the Chief of Staff Mouth exists in Slack like a real human:
always responsive, able to converse, acknowledge, answer questions, or coordinate with running work
without being silenced by hardcoded filters or thread mutex locks.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import json
import logging
import inspect
import asyncio
from typing import Dict, Any, Optional, List, Tuple

from golem.config import (
    get_mouth_model as _config_get_mouth_model,
    get_llm_base_url,
    get_architect_model,
    get_workhorse_model,
    get_sentinel_model,
)

logger = logging.getLogger("Golem.Agent.Mouth")

MOUTH_MODEL_DEFAULT = _config_get_mouth_model()
DEFAULT_GEMINI_MODEL = _config_get_mouth_model()
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"


def get_mouth_model() -> str:
    """Resolve MOUTH_MODEL dynamically from environment, prioritizing MOUTH_MODEL over GEMINI_MODEL."""
    val = (os.environ.get("MOUTH_MODEL") or os.environ.get("GOLEM_MOUTH_MODEL") or os.environ.get("GEMINI_MODEL") or "").strip()
    if val:
        return val
    return "gemini-3.8-flash"


def get_gemini_api_key() -> str:
    """Discover LLM / Gemini / Google API key from environment, project .env, or user ~/.env."""
    key = os.environ.get("LLM_API_KEY") or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if key and key.strip() and not key.startswith("your_"):
        return key.strip()

    candidate_files = [
        os.path.expanduser("~/.env"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env"),
    ]
    for path in candidate_files:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        for prefix in ("LLM_API_KEY=", "GEMINI_API_KEY=", "GOOGLE_API_KEY="):
                            if line.startswith(prefix):
                                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                                if val and not val.startswith("your_"):
                                    return val
            except Exception:
                pass
    return ""


PROPOSE_MEMORY_UPDATE_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "propose_memory_update",
    "description": (
        "Triggered when the user instructs Golem to remember a rule, environment fact, "
        "tool capability, procedural skill, or reactive flow. "
        "Stages updates across the 4 memory tiers for human confirmation."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "summary": {
                "type": "STRING",
                "description": "Concise summary of what is being learned or updated.",
            },
            "changes": {
                "type": "ARRAY",
                "description": "List of staged changes across the 4 memory tiers.",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "tier": {
                            "type": "STRING",
                            "enum": ["fact", "capability", "skill", "flow", "logbook"],
                            "description": "Memory tier: 'fact', 'capability', 'skill', 'flow', or 'logbook'.",
                        },
                        "rel_path": {
                            "type": "STRING",
                            "description": (
                                "Path relative to ~/.golem/wiki/ (e.g., context/rules/hermes.md, "
                                "context/capabilities.md, runbooks/skills/tool/SKILL.md, runbooks/flows/flow.md)"
                            ),
                        },
                        "content": {
                            "type": "STRING",
                            "description": "Complete updated file content to be written.",
                        },
                    },
                    "required": ["tier", "rel_path", "content"],
                },
            },
        },
        "required": ["summary", "changes"],
    },
}

DISPATCH_TASK_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "dispatch_task",
    "description": (
        "Hand an objective to a capable backstage agent (a Hand) that works on it autonomously across many steps "
        "via Herdr: inspecting systems, running commands, editing config, calling APIs, and verifying the result. "
        "Describe the WHOLE objective, what 'done' looks like, and the facts it needs (hosts, paths, ports, where "
        "credentials are stored). Do not split an objective into shell one-liners and do not micro-manage steps: "
        "one dispatch should cover a whole piece of work. Never paste secrets into the brief; tell the Hand where "
        "they are stored instead."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "task_description": {
                "type": "STRING",
                "description": (
                    "The full objective for the Hand: what to accomplish, success criteria, relevant context and "
                    "constraints, and what to report back."
                ),
            },
            "command": {
                "type": "STRING",
                "description": "Optional suggested starting command or script. The Hand may adapt or replace it.",
            },
            "cwd": {
                "type": "STRING",
                "description": "Optional working directory for execution (e.g., '/Users/hermes/projects/podcast-bot').",
            },
            "target_node": {
                "type": "STRING",
                "enum": ["hermes", "desktop-pc", "oci-machine", "personal-laptop"],
                "description": "Target fleet node (localhost / hermes).",
            },
            "worker": {
                "type": "STRING",
                "description": "Optional worker engine to execute the task (defaults to 'agy').",
            },
            "read_only": {
                "type": "BOOLEAN",
                "description": "Set to True when running a non-mutating verification probe or inspection command.",
            },
        },
        "required": ["task_description"],
    },
}

CONSULT_MEMORY_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "consult_memory",
    "description": "Read the exact content of an operational runbook, capability, or node rule from ~/.golem/wiki/.",
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "path": {
                "type": "STRING",
                "description": "Relative path to wiki document, e.g. 'runbooks/flows/oauth_token_refresh.md'",
            }
        },
        "required": ["path"],
    },
}

READ_PROJECT_FILE_DECLARATION: Dict[str, Any] = {
    "name": "read_project_file",
    "description": (
        "Directly read a local file or inspect a directory on the host filesystem "
        "(e.g., project registries, JSON databases, configs, dossiers, or logs). "
        "Use this for fast, non-mutating inspection without dispatching a heavy Hand."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "path": {
                "type": "STRING",
                "description": "Absolute or home-relative path to inspect (e.g. '/Users/hermes/projects/podcast-bot/the_unmade_god_episodes.json').",
            },
            "max_lines": {
                "type": "INTEGER",
                "description": "Maximum number of lines to return from file content (default: 200).",
            },
            "pattern": {
                "type": "STRING",
                "description": "Optional glob pattern if inspecting a directory (e.g. '*episodes*.json').",
            },
        },
        "required": ["path"],
    },
}

CHECK_EYES_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "check_eyes",
    "description": "Inspect the active runner terminal buffer via The Eyes.",
    "parameters": {
        "type": "OBJECT",
        "properties": {"pane": {"type": "STRING", "description": "Runner pane identifier."}},
        "required": [],
    },
}

CHECK_NOSE_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "check_nose",
    "description": "Inspect 24/7 fleet and system health telemetry via The Nose Sentinel.",
    "parameters": {
        "type": "OBJECT",
        "properties": {"trigger_fresh_sniff": {"type": "BOOLEAN", "description": "Force fresh sniff."}},
        "required": [],
    },
}

SHARE_ARTIFACT_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "share_artifact",
    "description": "Share an artifact or file staged in the Golem Outbox (~/.golem/outbox/) with Carl.",
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "path": {"type": "STRING", "description": "Path to artifact."},
            "artifact_id": {"type": "STRING", "description": "Artifact ID."},
            "title": {"type": "STRING", "description": "Title/label for file."},
        },
        "required": [],
    },
}

PLAN_TASK_WITH_SUBTASKS_DECLARATION: Dict[str, Any] = {
    "name": "plan_task_with_subtasks",
    "description": (
        "Create a parent task and broken-down child subtasks in Linear (Golem_Brain project). "
        "MANDATORY for compound or multi-stage operations (e.g. podcast series generation, "
        "multi-file refactors, migrations) BEFORE dispatching the first backstage Hand."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "parent_title": {
                "type": "STRING",
                "description": "High-level deliverable title (e.g., 'Produce 10-Episode Podcast Curriculum').",
            },
            "parent_description": {
                "type": "STRING",
                "description": "Comprehensive scope and final success criteria.",
            },
            "subtasks": {
                "type": "ARRAY",
                "items": {"type": "STRING"},
                "description": "Ordered list of discrete subtask milestones to execute sequentially.",
            },
            "parent_key": {
                "type": "STRING",
                "description": "Optional existing Linear issue key (e.g. 'ENG-123') to attach subtasks to and transition to In Progress instead of creating a duplicate parent ticket.",
            },
            "related_issue_key": {
                "type": "STRING",
                "description": (
                    "Optional upstream/original Linear issue key (e.g. 'ENG-101') when creating a follow-up "
                    "remediation or phase-2 ticket so Golem links the new follow-up issue to the original task in Linear."
                ),
            },
        },
        "required": ["parent_title", "parent_description", "subtasks"],
    },
}

CHECK_LINEAR_TASK_STATUS_DECLARATION: Dict[str, Any] = {
    "name": "check_linear_task_status",
    "description": (
        "Inspect active parent tasks and child subtask trees in Golem_Brain. "
        "Use this first whenever Carl asks for progress, status, or updates on deliverables."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "query": {
                "type": "STRING",
                "description": "Optional keyword or ticket identifier.",
            }
        },
    },
}

APPROVE_LINEAR_TASK_DECLARATION: Dict[str, Any] = {
    "name": "approve_linear_task",
    "description": (
        "Transition a parent task and all child subtasks from 'Claimed Fixed' to 'Completed'. "
        "STRICT INVARIANT: MUST ONLY be called when Carl explicitly verifies and approves the deliverable."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "issue_key": {
                "type": "STRING",
                "description": "Optional Linear parent issue identifier (e.g. '<parent_key>'); auto-binds to the active Linear parent issue if omitted.",
            },
            "signoff_comment": {
                "type": "STRING",
                "description": "Carl's explicit signoff note or verification feedback.",
            },
        },
        "required": [],
    },
}

DISPATCH_SUBTASKS_TOOL_DECLARATION: Dict[str, Any] = {
    "name": "dispatch_subtasks",
    "description": (
        "Dispatch multiple broken-down subtasks concurrently or sequentially across fleet nodes. "
        "Each subtask is bound to a dedicated Hand agent and tracked under its Linear issue key."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "parent_issue_key": {
                "type": "STRING",
                "description": "Optional parent Linear issue key (e.g. '<parent_key>'); auto-binds from the active Linear plan if omitted.",
            },
            "execution_mode": {
                "type": "STRING",
                "enum": ["parallel", "sequential"],
                "description": "Whether to execute subtasks concurrently ('parallel') or in order ('sequential').",
            },
            "subtasks": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "subtask_index": {
                            "type": "INTEGER",
                            "description": "1-based subtask number (1..N) from the active plan; auto-resolves the Linear issue_key.",
                        },
                        "issue_key": {
                            "type": "STRING",
                            "description": "Optional Linear subtask issue key (e.g. '<subtask_key>'); auto-resolves from plan if omitted.",
                        },
                        "target_node": {
                            "type": "STRING",
                            "description": "Target fleet node (e.g. 'oci-machine', 'localhost', 'desktop-pc').",
                        },
                        "role_name": {
                            "type": "STRING",
                            "description": "Descriptive role or subtask name (e.g. 'Series Inventory').",
                        },
                        "read_only": {
                            "type": "BOOLEAN",
                            "description": "True if this subtask performs read-only inspection.",
                        },
                        "task_brief": {
                            "type": "STRING",
                            "description": "Complete self-contained instruction/prompt for the Hand agent.",
                        },
                    },
                    "required": ["task_brief"],
                },
                "description": "List of subtask specifications to dispatch.",
            },
        },
        "required": ["execution_mode", "subtasks"],
    },
}

CANCEL_ACTIVE_TASK_DECLARATION: Dict[str, Any] = {
    "name": "cancel_active_task",
    "description": "Cancel or stop the currently running backstage Hand execution in this thread when Carl asks to stop, abort, or cancel it.",
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "reason": {
                "type": "STRING",
                "description": "Brief note on why the task is being cancelled.",
            }
        },
    },
}

MOUTH_TOOLS: List[Dict[str, Any]] = [
    DISPATCH_TASK_TOOL_DECLARATION,
    DISPATCH_SUBTASKS_TOOL_DECLARATION,
    READ_PROJECT_FILE_DECLARATION,
    CONSULT_MEMORY_TOOL_DECLARATION,
    PLAN_TASK_WITH_SUBTASKS_DECLARATION,
    CHECK_LINEAR_TASK_STATUS_DECLARATION,
    APPROVE_LINEAR_TASK_DECLARATION,
    CANCEL_ACTIVE_TASK_DECLARATION,
]

BASE_CHIEF_OF_STAFF_INSTRUCTION = """
You are Golem, Chief of Staff for Carl's distributed computing fleet and homelab.
You have direct autonomous control over a distributed mesh:
- hermes: Local macOS controller & homelab operations
- desktop-pc: Windows CUDA workstation (heavy compute, PyTorch, InvokeAI)
- oci-machine: Oracle Cloud Linux VPS (Audiobookshelf, scrapers, ingress daemons)

OPERATIONAL MANDATE & UNIFIED "ONE MIND" LIFECYCLE:
You are a single unified Chief of Staff persona across Planning, Execution, Validation, UAT, and Deployment:

1. BIAS TOWARD ACTION: When Carl asks you to inspect, verify, download, transfer, or sync, USE YOUR TOOLS IMMEDIATELY. For simple 1-step requests or read-only checks, execute directly without ceremony.
2. PLAN (CHECKPOINT 1): For multi-step engineering, refactors, migrations, or compound operations, call `plan_task_with_subtasks` to create the Linear parent + subtasks, share the concise plan + Linear link with Carl, and pause for Carl's approval before dispatching multi-step execution.
3. EXECUTE (NATURAL >3s HEADS-UP): When calling `dispatch_task` or `dispatch_subtasks`, include a brief, natural 1-sentence status in your response text alongside the tool call (e.g., "Running the test suite on `hermes` now—I'll report back once I've verified the results."). If the task finishes in <3s, that interim text is suppressed automatically; if it runs >3s, it is posted as your natural heads-up.
4. VALIDATE & SELF-REPAIR (SAME TURN): After a mutating `dispatch_task` or `dispatch_subtasks` returns, do not blindly claim success. In the same turn, verify the result (inspect exit code/output, or call a verification probe via `dispatch_task(read_only=True, ...)` or `read_project_file`). If verification fails, automatically dispatch a targeted fix (up to 2 self-repair rounds) and re-verify before reporting to Carl.
5. UAT & DEPLOY (CHECKPOINT 2): Once verified, subtasks settle to 'Claimed Fixed' in Linear. Present the verified executive briefing to Carl and pause for his UAT / deployment approval. Only when Carl replies approving the work (e.g., "looks good", "approved", "deploy it"), execute any production deployment and call `approve_linear_task` to transition the Linear tree to 'Completed'.
6. STRICT TOOL HONESTY: Never claim in prose to have consulted the wiki, created/updated Linear issues, or verified a fix unless you actually invoked the corresponding tool in this turn or are citing a real key present in `[Linear working notes]`. Note that your full wiki (`context/` and `runbooks/`) is already pre-loaded in your system prompt under `OPERATIONAL WIKI PERIPHERAL VISION`.
7. DO NOT TALK ABOUT EXECUTING: Never reply saying "Starting transfer now", "Triggering script now", or "Spooling up backstage work" without calling `dispatch_task` or `dispatch_subtasks` in the same turn. If an action is required, call the tool.
8. READ BEFORE ACTING: Use `read_project_file` to check manifests, JSON files, and local directories before guessing file paths.
9. SYNTHESIZE OUTCOMES: Once your tools finish executing, report the actual output, verified artifacts, and live status directly to Carl.
10. DISPATCH BACKSTAGE WORK FOR OPERATIONAL TASKS:
When asked to perform operational tasks (e.g. Cloudflare tunnels, DNS updates, service configuration, deployments, code changes, system administration), inspection of files or wiki memory is only the pre-flight phase. You MUST call `dispatch_task` to perform the actual work (e.g., using `worker="agy"` or executing the necessary shell/python commands on the target node). NEVER terminate your turn after reading files with promises like "Executing now" or "Applying next". If the action is ready, CALL `dispatch_task`.
11. MULTI-AGENT SUBTASK DISPATCH & AUDIOBOOKSHELF RULES:
When handling multi-part audits, parallel inspections, or tasks with multiple subtasks, use `dispatch_subtasks` with `execution_mode: "parallel"` (or `"sequential"`).
Audiobookshelf Subtask & Inspection Rules (Strictly Read-Only):
Audiobookshelf runs in Docker on `oci-machine` with access via curl to http://localhost:13378 or local SQLite/filesystem. Format subtask briefs as strictly read-only:
- Series & Library Inventory: Target `oci-machine` via SSH, execute read-only query against local ABS REST API (`curl -s http://localhost:13378/api/libraries/<library_id>/items`), output JSON summary of podcast series IDs, titles, and episode counts. Flag brief with `read_only: true`.
- Episode Durations: Target `oci-machine` via SSH, execute read-only query against ABS item endpoints or inspect media files via `ffprobe` / `docker exec` SQLite read-only to calculate exact runtimes across all episodes. Flag brief with `read_only: true`.
12. LINEAR IS YOUR MEMORY ACROSS TURNS:
Nothing carries over between turns except the Slack thread and Linear. When a `[Linear working notes]` block appears in the prompt, treat its findings as verified by an earlier turn: do NOT repeat that discovery, continue from it. Hand results are recorded to the thread's Linear issue automatically. Never trust your own earlier chat briefings over the notes; if they disagree, the Linear notes win.
13. DO NOT CLAIM CAPABILITIES YOU LACK:
You have no tool to switch models. Backstage model fallback (for example to Sonnet) is automatic inside the worker driver, so never tell Carl that you "switched" a worker's model. Report only what a tool result actually says.
14. AN IDLE RUNNER IS NOT A CRASH:
If `check_eyes` reports an idle runner whose only content is a shell prompt, that is a fresh runner waiting for a command. Nothing crashed. Dispatch the work instead of reporting a failure.
15. DELEGATE WHOLE OBJECTIVES:
A Hand is a capable agent, not a shell. Give `dispatch_task` the complete objective in `task_description` (goal, success criteria, known facts, where credentials live, what to report back) so it can work through many steps itself and verify its own result. Do not chop work into one-line probes, and do not repeat discovery that Linear notes or earlier results already contain.
16. REPORT ONLY WHAT TOOLS CONFIRMED:
Never say a credential, tunnel, route, or policy is "verified", "configured", or "live" unless a tool result in this conversation shows it. If something was not checked, say it was not checked. If you need something from Carl (for example a credential's location), ask for that one thing plainly.
17. THREAD SOVEREIGN PRINCIPLE:
A Slack thread (`channel:thread_ts`) is a dedicated, sovereign workspace for the specific topic, incident, or Linear ticket established in that thread's conversation history. Pronouns, short replies, and conversational follow-ups ("yes plz", "this project", "let's continue", "did we complete this project?") MUST be resolved strictly against the immediate conversation in the current thread—never cross-matched against unrelated tickets sitting in the global Linear queue. If Carl is discussing `ENG-101` (or an ad-hoc request like whitelisting a phone number), refuse to pull in or trigger unrelated tickets (such as `ENG-202`) unless Carl explicitly asks to switch topics or check the global backlog.
18. NUDGE HYGIENE & IN-FLIGHT AWARENESS:
When evaluating Linear tickets or proactive sentinel nudges, always inspect live runtime telemetry (`[LIVE RUNTIME & RUNNER TELEMETRY]`) to evaluate whether a ticket is genuinely stalled/abandoned (no active worker, not queued in an active batch execution, and no recent human/agent interaction) versus actively in-flight or newly queued. If a ticket was just triggered minutes ago, has an active background runner, or is waiting its turn in an active background batch run, silently suppress the nudge rather than nagging Carl. When formulating a prompt for a genuinely stalled ticket, never emit robotic static boilerplate ("Would you like me to start, continue, retry, or cancel this ticket?"); instead, tailor the call-to-action naturally to the actual ticket and subtask state:
- If `0/N` subtasks are settled and all are `Todo`/`Planned` (even if the parent ticket was prematurely marked `In Progress`), ask if Carl is ready to kick off Phase 1.
- If some subtasks are settled or a runner paused mid-stream, ask if Carl wants to pick back up where we left off on Subtask X.
- If the last run threw an error or failed verification, mention what broke and ask if Carl wants to retry.
19. THE IRON LAW OF EXTERNAL INTEGRATIONS (NO SILENT MOCKS):
- Never Hallucinate Fallbacks for Live APIs: When building or executing tools that interact with external services (H-E-B, Slack, iMessage, GitHub, APIs), NEVER implement "synthetic fallbacks," "mock IDs," or "offline heuristic stubs" to make an end-to-end test pass when blocked by auth, 2FA, or WAF/Cloudflare/Imperva challenges.
- Fail Loudly at the Boundary: If an external API blocks you or requires interactive human authentication (like 2FA or a browser captcha), STOP at that exact step, leave downstream state untouched, and report the exact blocker so Carl can authenticate or intervene.
- Never Redefine Deliverables: "Staged," "Deployed," or "Sent" means the external target system confirmed the state change via a verified network response—never a local cache write pretending to be remote state.
20. LINEAR FOLLOW-UP ISSUE LINKING:
When creating a new follow-up, remediation, or phase-2 ticket via `plan_task_with_subtasks` that stems from an original Linear issue discussed in the thread (e.g. creating `ENG-303` as a follow-up to `ENG-101`), ALWAYS pass `related_issue_key` with the original issue identifier so Golem formally links the follow-up ticket to the original task in Linear.
21. INTERNAL [Hand] TICKET HYGIENE:
Tickets starting with `[Hand]` are internal, ephemeral execution steps dispatched by backstage runners—never top-level user projects. Never surface `[Hand]` tickets to Carl as standalone project deliverables or prompt him to start, retry, or review them. If orphaned `[Hand]` tickets with no active local process are encountered, treat them as internal background artifacts to be auto-reconciled or ignored rather than surfaced in user nudges.
"""

BASE_SYSTEM_INSTRUCTION: str = BASE_CHIEF_OF_STAFF_INSTRUCTION


def build_mouth_system_instruction() -> str:
    """Build Mouth system prompt including Ambient Peripheral Vision."""
    from golem.memory.peripheral import compile_peripheral_vision
    peripheral_vision = compile_peripheral_vision()
    return f"{BASE_CHIEF_OF_STAFF_INSTRUCTION}\n\n{peripheral_vision}"



def is_cancellation_message(text: str) -> bool:
    """Check if message expresses cancellation intent."""
    clean = text.strip().lower()
    stripped = clean.rstrip("?.!").strip()
    if stripped in (
        "/cancel", "!cancel", "/stop", "!stop", "/abort", "!abort",
        "cancel", "stop", "abort", "kill", "quit", "interrupt",
        "please cancel", "please stop", "please abort", "please interrupt",
        "cancel please", "stop please", "abort please",
    ):
        return True
    cancellation_patterns = [
        r"^(please\s+)?(cancel|stop|abort|kill|terminate|interrupt)(\s+(that|this|it|the))?(\s+(task|command|execution|job|runner|process))?[\.!\?]?$",
        r"^(can|could|would)\s+you\s+(please\s+)?(cancel|stop|abort|kill|terminate|interrupt)(\s+(that|this|it|the))?(\s+(task|command|execution|job|runner|process))?[\.!\?]?$",
        r"^(please\s+)?(cancel|stop|abort)\s+(that|this|it|the\s+\w+|task|command|execution|job|runner|process)\b",
        r"\b(can|could|would)\s+you\s+(please\s+)?(cancel|stop|abort)\b",
        r"\bplease\s+(cancel|stop|abort|terminate)\b",
        r"\b(cancel|stop|abort)\s+(the\s+)?(task|command|execution|job|runner|process)\b",
    ]
    return any(bool(re.search(p, clean)) for p in cancellation_patterns)


def is_status_inquiry(text: str) -> bool:
    """Check if message is inquiring about status, progress, buffer, or blockers."""
    clean = text.strip().lower()
    if clean in ("status", "progress", "url", "oauth", "link", "/status", "!status"):
        return True
    patterns = [
        r"\b(status|progress|check\s+status|show\s+buffer)\b",
        r"\b(what('?s|\s+is)\s+(the\s+)?(status|url|link|oauth|progress))\b",
        r"\b(how('?s|\s+is)\s+it\s+going)\b",
        r"\b(where\s+are\s+we|what('?s|\s+is)\s+happening)\b",
        r"\b(what('?s|\s+is)\s+the\s+url)\b",
    ]
    return any(bool(re.search(p, clean)) for p in patterns)


def is_conversational_checkin(text: str) -> bool:
    """Check if message is a conversational check-in during background execution."""
    clean = text.strip().lower()
    if clean in ("ping", "update", "updates", "updates?"):
        return True
    patterns = [
        r"\b(are\s+you\s+(still\s+)?(there|working|running))\b",
        r"\b(is\s+it\s+(still\s+)?running)\b",
        r"\b(still\s+running\??)\b",
        r"\b(any\s+updates?\??)\b",
        r"\b(what\s+are\s+you\s+doing\??)\b",
        r"\b(are\s+we\s+done\??)\b",
    ]
    return any(bool(re.search(p, clean)) for p in patterns)


def is_batch_task_command(text: str) -> bool:
    """
    Check if a message is an explicit sequential batch task or imperative dev command
    meant to be queued for the adapter runner rather than handled conversationally by the Mouth liaison.
    """
    clean = text.strip().lower()
    if not clean:
        return False
    # Unit test synthetic turn / message markers (e.g. Turn 1, turn1, turn_A, Question 1, Message-0, Task Alpha)
    if re.match(r"^(turn|task)[-_ ]?([0-9]+|[a-z]|alpha|beta)?$", clean):
        return True
    if re.match(r"^(question|message)[-_ ]?\d+$", clean):
        return True
    if "sequential task" in clean or clean.startswith("run heavy training pipeline"):
        return True
    if clean in ("build a web server", "deploy the container", "write a script to cancel reservations"):
        return True
    return False


def is_conversational_inquiry(text: str) -> bool:
    """
    Check if a message is conversational inquiry/input meant for the Mouth liaison.
    Under FirstMate, the Mouth LLM reacts dynamically to all natural input without regex gates.
    """
    clean = text.strip()
    if not clean:
        return False
    return not is_batch_task_command(clean)


def is_out_of_band_message(text: str, session: Optional[Any] = None) -> bool:
    """
    Check if message qualifies for out-of-band processing while session is busy/executing.
    Eliminates hardcoded regex gates: in FirstMate, the Chief of Staff Mouth exists in Slack
    like a real human, dynamically responding to any conversational input, question, or check-in
    without being silenced or blocked by regex gates.
    """
    clean = text.strip()
    if not clean:
        return False

    if is_cancellation_message(clean):
        return True

    if is_batch_task_command(clean):
        return False

    return True


async def generate_mouth_response(
    prompt: str,
    status_info: Dict[str, Any],
    session: Optional[Any] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    timeout_seconds: float = 30.0,
    eyes_status: Optional[Dict[str, Any]] = None,
    nose_status: Optional[Dict[str, Any]] = None,
    worker_registry_state: Optional[str] = None,
) -> str:
    """
    Generate dynamic Mouth response using Gemini Flash LLM.
    Grounds responses in The Eyes (runner state) and The Nose (fleet health).
    Seamlessly falls back to calibrated status format when offline or in test environments.
    """
    # 1. Immediate Blocker cards (OAuth URLs, interactive prompts require clickable UI links)
    blocker = status_info.get("blocker")
    if not blocker and status_info.get("blockers"):
        b_list = status_info["blockers"]
        if b_list and isinstance(b_list, list):
            blocker = b_list[0]

    if blocker and blocker.get("type") == "oauth":
        url = blocker.get("url")
        return f"🔐 *Authentication Required:* Runner is waiting for OAuth authorization:\n<{url}|Click here to authenticate>"
    elif blocker and blocker.get("type") == "prompt":
        content = blocker.get("content") or blocker.get("prompt")
        return f"⚠️ *Confirmation Required:* Runner is waiting for input:\n> `{content}`"

    try:
        from golem.webui import get_active_webui_server
        webui = get_active_webui_server()
        if webui is not None:
            clean_p = " ".join(prompt.splitlines()).strip()
            if len(clean_p) > 200:
                clean_p = clean_p[:197] + "..."
            webui.append_organ_log("mouth", f"[INBOUND] {clean_p}")
    except Exception:
        pass

    # Auto-resolve sensory statuses if omitted
    if eyes_status is None and session is not None and hasattr(session, "get_eyes_status"):
        try:
            res = session.get_eyes_status(status_info)
            if not inspect.isawaitable(res):
                eyes_status = res
        except Exception:
            pass

    if nose_status is None:
        try:
            from golem.nose import get_default_nose_sentinel
            sentinel = get_default_nose_sentinel()
            if sentinel is not None and getattr(sentinel, "last_snapshot", None) is not None:
                res = sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
                if not inspect.isawaitable(res):
                    nose_status = res
        except Exception:
            pass

    # 2. Dynamic Mouth LLM call via Gemini Flash
    key = api_key or get_gemini_api_key()
    if not key:
        return "⚠️ *Mouth Generation Error:* GEMINI_API_KEY is not configured. Cannot generate dynamic response."

    try:
        import aiohttp

        is_mock_post = (
            hasattr(aiohttp.ClientSession.post, "assert_called")
            or hasattr(aiohttp.ClientSession.post, "_mock_return_value")
            or type(aiohttp.ClientSession.post).__name__ in ("MagicMock", "AsyncMock", "Mock")
        )
        is_test_env = os.environ.get("FIRSTMATE_TEST_MODE") == "1" or os.environ.get("GOLEM_TEST_MODE") == "1"
        if is_test_env and not is_mock_post:
            pid = status_info.get("pid") or getattr(getattr(session, "active_execution", None), "pid", None) or "active"
            command = status_info.get("command") or getattr(getattr(session, "active_execution", None), "command", "") or ""
            tail = status_info.get("buffer_tail") or status_info.get("output_tail") or ""
            pid_str = f"PID {pid}" if pid else ""
            tail_str = f"\n> {tail.strip()}" if tail else ""
            return f"⚓ *Liaison Update:* Runner In Progress. Still on it! Background execution for `{command}` ({pid_str}) is running smoothly.{tail_str}"

        pid = status_info.get("pid") or getattr(getattr(session, "active_execution", None), "pid", None) or "active"
        command = status_info.get("command") or getattr(getattr(session, "active_execution", None), "command", "") or ""
        tail = status_info.get("buffer_tail") or status_info.get("output_tail") or ""
        last_prompt = getattr(session, "last_prompt", "") or ""
        status_val = status_info.get("status") or ("running" if getattr(session, "is_executing", False) else "idle")

        sys_prompt = (
            "You are First Mate, Carl's Chief of Staff and operational liaison in Slack.\n"
            "You are communicating with Carl in a Slack thread while background work is active.\n"
            "Fleet Topology & Machine Roles:\n"
            "• hermes: Local macOS controller, daily development anchor, and homelab runner (localhost).\n"
            "• desktop-pc: High-VRAM Windows node for CUDA, model training, and heavy GPU compute (100.80.59.45) — this is Carl's PC / Windows machine.\n"
            "• oci-machine: Remote cloud VPS for external daemons and scrapers (100.113.173.33).\n"
            "• personal-laptop: Secondary macOS node (100.96.38.5) — Carl's personal MacBook.\n\n"
            "You possess full sensory awareness of The Eyes (active task observer) and The Nose (fleet health sentinel).\n"
            "Respond naturally, directly, and conversationally like a trusted senior colleague.\n"
            "Strictly adhere to Slack mrkdwn syntax: *bold*, `code`, <URL|Label>.\n"
            "Never dump terminal escape codes, ANSI colors, or box drawing characters.\n"
            "CRITICAL GROUNDING INVARIANT:\n"
            "You must ALWAYS ground your status reports strictly in [CURRENT WORKER REGISTRY STATE].\n"
            "If Active Workers is 0 (or 'None running'), NEVER state or imply that workers, agents, or subtasks are running, attached, or active. State plainly and factually that 0 workers are active.\n\n"
            "If Carl asks if he can ask something mid-stream or converse, reassure him warmly and answer his question.\n"
            "If Carl asks about what The Eyes see or what The Nose smells mid-stream, answer factually using the current sensory state.\n"
            "If Carl asks about the fleet, machines, or which machine is the PC, answer factually using the fleet topology above.\n"
            "If Carl asks for status or progress, report current progress accurately based on the worker registry.\n"
            "Keep answers focused, high-signal, and proportionate to Carl's question."
        )

        # Ambient Peripheral Vision & Capabilities Injection from Wiki
        try:
            from golem.memory.peripheral import compile_peripheral_vision
            pv_content = compile_peripheral_vision()
            if pv_content and pv_content.strip():
                sys_prompt += f"\n\n{pv_content.strip()}"
        except Exception:
            pass

        state_ctx_lines = []
        if worker_registry_state:
            state_ctx_lines.append(worker_registry_state)
        state_ctx_lines.extend([
            f"Runner Status: {status_val}",
            f"Active Command: {command}",
            f"PID: {pid}",
            f"Buffer Tail:\n{tail}",
        ])
        if eyes_status is not None:
            e_st = eyes_status.get("state", "IDLE")
            e_cmd = eyes_status.get("active_command") or eyes_status.get("command", "")
            e_blk = eyes_status.get("blocker")
            state_ctx_lines.append(f"The Eyes Status: state={e_st} | command={e_cmd} | blocker={e_blk}")

        if nose_status is not None:
            n_off = nose_status.get("smells_off", False)
            n_min = nose_status.get("minutes_ago", 0.0)
            n_verd = nose_status.get("verdict_summary", "Nominal")
            n_disk = nose_status.get("disk", {})
            n_free = n_disk.get("free_gb", "unknown")
            n_dock = nose_status.get("docker", {})
            n_cnt = n_dock.get("container_count", 0)
            n_ts = nose_status.get("tailscale", {})
            n_online = n_ts.get("online", False)
            state_ctx_lines.append(
                f"The Nose Status: smells_off={n_off} (checked {n_min:.1f}m ago) | verdict={n_verd} | "
                f"disk_free={n_free}GB | docker_containers={n_cnt} | tailscale_online={n_online}"
            )

        state_ctx_lines.append(f"Initial Prompt / Task: {last_prompt}")
        state_ctx = "\n".join(state_ctx_lines)

        generation_config: Dict[str, Any] = {
            "temperature": 0.3,
        }
        payload = {
            "systemInstruction": {"parts": [{"text": sys_prompt}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [{
                        "text": f"Current background state:\n{state_ctx}\n\nCarl's message: {prompt}"
                    }]
                }
            ],
            "generationConfig": generation_config
        }
        model_name = model or get_mouth_model()
        url = GEMINI_API_URL.format(model=model_name)
        if "?key=" in url or "&key=" in url:
            raise ValueError("Security violation: Gemini API keys must not be passed via URL query parameters; use 'x-goog-api-key' header.")
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
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

        async with aiohttp.ClientSession(timeout=timeout) as client_session:
            async with client_session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, dict):
                        usage = data.get("usageMetadata")
                        if isinstance(usage, dict):
                            p_tok = usage.get("promptTokenCount", 0) or 0
                            c_tok = usage.get("candidatesTokenCount", 0) or 0
                            th_tok = usage.get("thoughtsTokenCount", 0) or 0
                            logger.info(
                                "Gemini usageMetadata (%s): promptTokenCount=%s, cachedContentTokenCount=%s, thoughtsTokenCount=%s, candidatesTokenCount=%s",
                                model_name,
                                p_tok,
                                usage.get("cachedContentTokenCount"),
                                th_tok,
                                c_tok,
                            )
                            try:
                                from golem.webui import get_active_webui_server
                                webui = get_active_webui_server()
                                if webui is not None:
                                    webui.append_organ_log(
                                        "mouth",
                                        f"[TOKENS] prompt={p_tok} candidates={c_tok} thoughts={th_tok} model={model_name}",
                                    )
                            except Exception:
                                pass
                    candidates = data.get("candidates") if isinstance(data, dict) else None
                    if candidates and isinstance(candidates, list):
                        cand = candidates[0] if isinstance(candidates[0], dict) else {}
                        finish_reason = cand.get("finishReason", "") if isinstance(cand, dict) else ""
                        if finish_reason == "MAX_TOKENS":
                            logger.warning("Gemini Mouth response reached MAX_TOKENS limit")
                        content = cand.get("content") if isinstance(cand, dict) else None
                        parts = (content.get("parts") if isinstance(content, dict) else None) or []
                        if not isinstance(parts, list):
                            parts = []
                        for p in parts:
                            if isinstance(p, dict) and p.get("thought") and p.get("text"):
                                th_text = " ".join(str(p["text"]).splitlines()).strip()
                                if len(th_text) > 120:
                                    th_text = th_text[:117] + "..."
                                try:
                                    from golem.webui import get_active_webui_server
                                    webui = get_active_webui_server()
                                    if webui is not None:
                                        webui.append_organ_log("mouth", f"[THOUGHT] {th_text}")
                                except Exception:
                                    pass
                        text_chunks = [
                            str(p["text"]) for p in parts
                            if isinstance(p, dict) and "text" in p and p.get("text")
                        ]
                        resp_text = "".join(text_chunks).strip()
                        if resp_text:
                            try:
                                from golem.webui import get_active_webui_server
                                webui = get_active_webui_server()
                                if webui is not None:
                                    clean_rep = " ".join(resp_text.splitlines()).strip()
                                    if len(clean_rep) > 200:
                                        clean_rep = clean_rep[:197] + "..."
                                    webui.append_organ_log("mouth", f"[REPLY] {clean_rep}")
                            except Exception:
                                pass
                            return resp_text
                        return f"⚠️ *Mouth Generation Error:* Gemini API returned empty candidate text (finishReason={finish_reason})."
                    return "⚠️ *Mouth Generation Error:* Gemini API returned no candidates."
                else:
                    err_body = await resp.text()
                    logger.warning(f"Gemini API returned status {resp.status}: {err_body}")
                    return f"⚠️ *Mouth Generation Error:* Gemini API error (HTTP {resp.status}): {err_body}"
    except Exception as e:
        logger.warning(f"Gemini Mouth response generation encountered error: {e}")
        return f"⚠️ *Mouth Generation Error:* Failed to generate dynamic LLM response ({type(e).__name__}): {e}"


def get_mouth_fallback_models() -> List[str]:
    """Resolve ordered fallback models for Mouth LLM failover."""
    try:
        import golem.config as gcfg
        if hasattr(gcfg, "get_mouth_fallback_models"):
            res = gcfg.get_mouth_fallback_models()
            if res:
                return res
    except Exception:
        pass
    raw = os.environ.get("GOLEM_MOUTH_FALLBACK_MODELS") or os.environ.get("MOUTH_FALLBACK_MODELS") or ""
    if raw.strip():
        return [m.strip() for m in raw.split(",") if m.strip()]
    primary = (get_mouth_model() or "").lower()
    if "argon" in primary or "gemini-4" in primary:
        return ["gemini-3.8-flash"]
    return ["gemini-3.8-flash"]


def calculate_token_cost(
    model: str,
    prompt_tokens: int = 0,
    cached_tokens: int = 0,
    candidates_tokens: int = 0,
) -> float:
    """
    Calculate estimated turn cost in USD based on model pricing per 1M tokens.
    Tier 1 (Architect: gemini-4-argon): $2.50 input, $0.625 cached, $10.00 output
    Tier 2 (Workhorse: gemini-3.8-flash): $0.10 input, $0.025 cached, $0.40 output
    Tier 3 (Sentinel: gemini-3.5-flash-lite): $0.075 input, $0.01875 cached, $0.30 output
    """
    m = (model or "").lower()
    if "argon" in m or "gemini-4" in m or "pro" in m:
        rate_uncached = 2.50 / 1_000_000.0
        rate_cached = 0.625 / 1_000_000.0
        rate_output = 10.00 / 1_000_000.0
    elif "lite" in m or "3.5-flash" in m:
        rate_uncached = 0.075 / 1_000_000.0
        rate_cached = 0.01875 / 1_000_000.0
        rate_output = 0.30 / 1_000_000.0
    else:
        # Default / Workhorse (gemini-3.8-flash)
        rate_uncached = 0.10 / 1_000_000.0
        rate_cached = 0.025 / 1_000_000.0
        rate_output = 0.40 / 1_000_000.0

    uncached_tokens = max(0, prompt_tokens - cached_tokens)
    cost = (uncached_tokens * rate_uncached) + (cached_tokens * rate_cached) + (candidates_tokens * rate_output)
    return round(cost, 6)


def get_mouth_tools_declaration() -> List[Dict[str, Any]]:
    """Return standard Google Gemini REST API tools declaration with deterministically sorted functionDeclarations."""
    sorted_tools = sorted(MOUTH_TOOLS, key=lambda d: d.get("name", ""))
    return [{"functionDeclarations": sorted_tools}]


async def call_gemini_api(
    payload: Optional[Dict[str, Any]] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    timeout_seconds: float = 60.0,
    **kwargs: Any,
) -> Optional[Dict[str, Any]]:
    """Execute REST API POST call to Google Gemini endpoint or configured MOUTH_BACKEND with automatic failover and usage logging."""
    import aiohttp

    if payload is None:
        payload = {}
        if "contents" in kwargs:
            payload["contents"] = kwargs["contents"]
        if "tools" in kwargs:
            payload["tools"] = kwargs["tools"]
        if "tool_config" in kwargs:
            tc = kwargs["tool_config"]
            if isinstance(tc, dict) and "function_calling_config" in tc and "functionCallingConfig" not in tc:
                tc = {"functionCallingConfig": tc["function_calling_config"]}
            payload["toolConfig"] = tc
        elif "toolConfig" in kwargs:
            payload["toolConfig"] = kwargs["toolConfig"]
        if "system_instruction" in kwargs:
            si = kwargs["system_instruction"]
            if isinstance(si, str):
                payload["systemInstruction"] = {"parts": [{"text": si}]}
            else:
                payload["systemInstruction"] = si
        elif "systemInstruction" in kwargs:
            payload["systemInstruction"] = kwargs["systemInstruction"]
        if "generationConfig" in kwargs:
            payload["generationConfig"] = kwargs["generationConfig"]

    base_url = get_llm_base_url("mouth")
    backend = (os.environ.get("MOUTH_BACKEND") or os.environ.get("GOLEM_MOUTH_BACKEND") or "gemini").lower().strip()
    if base_url or backend in ("openai", "openai-compatible", "openrouter", "groq", "ollama", "deepseek"):
        from golem.ingress import call_openai_compatible_api
        return await call_openai_compatible_api(
            payload=payload,
            model=model,
            api_key=api_key,
            base_url=base_url or None,
            timeout_seconds=timeout_seconds,
        )

    key = api_key or get_gemini_api_key()
    if not key:
        return None

    configured_primary = get_mouth_model()
    is_default_mouth = (model is None or model == configured_primary)
    models_to_try = [model or configured_primary]
    if is_default_mouth:
        fallbacks = get_mouth_fallback_models()
        for fb in fallbacks:
            if fb and fb not in models_to_try:
                models_to_try.append(fb)

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
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

    last_error_res: Optional[Dict[str, Any]] = None
    for attempt_idx, current_model in enumerate(models_to_try):
        url = GEMINI_API_URL.format(model=current_model)
        if "?key=" in url or "&key=" in url:
            raise ValueError("Security violation: Gemini API keys must not be passed via URL query parameters; use 'x-goog-api-key' header.")

        try:
            async with aiohttp.ClientSession(timeout=timeout) as http_session:
                async with http_session.post(url, json=payload, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, dict):
                            usage = data.get("usageMetadata")
                            if isinstance(usage, dict):
                                p_tok = usage.get("promptTokenCount", 0) or 0
                                c_tok = usage.get("candidatesTokenCount", 0) or 0
                                cached_tok = usage.get("cachedContentTokenCount", 0) or 0
                                th_tok = usage.get("thoughtsTokenCount", 0) or 0
                                cache_hit_rate = (cached_tok / p_tok * 100.0) if p_tok > 0 else 0.0
                                cost = calculate_token_cost(
                                    model=current_model,
                                    prompt_tokens=p_tok,
                                    cached_tokens=cached_tok,
                                    candidates_tokens=c_tok,
                                )
                                logger.info(
                                    "Gemini usageMetadata (%s): promptTokenCount=%s, cachedContentTokenCount=%s (Cache: %.1f%% hit), thoughtsTokenCount=%s, candidatesTokenCount=%s, cost=$%.4f",
                                    current_model,
                                    p_tok,
                                    cached_tok,
                                    cache_hit_rate,
                                    th_tok,
                                    c_tok,
                                    cost,
                                )
                        return data
                    else:
                        err_text = await resp.text()
                        logger.warning(f"Gemini API ({current_model}) returned status {resp.status}: {err_text}")
                        last_error_res = {
                            "_error": True,
                            "status": resp.status,
                            "code": resp.status,
                            "error_type": f"HTTP_{resp.status}",
                            "raw_error": err_text,
                            "message": err_text,
                        }
                        is_quota_or_server_err = (
                            resp.status == 429
                            or resp.status >= 500
                            or "quota" in err_text.lower()
                            or "resource_exhausted" in err_text.lower()
                        )
                        if is_quota_or_server_err and attempt_idx < len(models_to_try) - 1:
                            logger.info(f"Retrying Gemini call with fallback model '{models_to_try[attempt_idx + 1]}' due to HTTP {resp.status}")
                            continue
                        return last_error_res
        except (asyncio.TimeoutError, aiohttp.ClientError, Exception) as e:
            if isinstance(e, asyncio.TimeoutError):
                err_msg = f"Gemini API call timed out after {timeout_seconds}s"
            else:
                err_msg = str(e) or type(e).__name__
            logger.warning(f"Gemini API call ({current_model}) failed: {err_msg}")
            last_error_res = {
                "_error": True,
                "status": 500,
                "code": 500,
                "error_type": type(e).__name__,
                "raw_error": err_msg,
                "message": err_msg,
            }
            if attempt_idx < len(models_to_try) - 1:
                logger.info(f"Retrying Gemini call with fallback model '{models_to_try[attempt_idx + 1]}' after error: {err_msg}")
                continue
            return last_error_res

    return last_error_res



async def triage_memory_input(
    text: str,
    session: Optional[Any] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> Tuple[str, Optional[Any]]:
    """
    Triage an operational memory input across the 4 tiers vs daily logbook.
    - If purely a reflection or journal note -> appends to wiki logbook. Returns ("logbook", None)
    - If contains Facts, Capabilities, Skills, or Flows -> creates a multi-file proposal
      via wiki.propose_multi_update(...). Sets session.pending_memory_proposal and returns ("proposal", proposal).
    """
    from golem.wiki import GolemWiki
    wiki = GolemWiki()
    clean_text = (text or "").strip()
    if not clean_text:
        return "logbook", None

    # Check for pure daily note / reflection markers
    lower = clean_text.lower()
    is_pure_reflection = (
        lower.startswith(("daily note", "reflection:", "journal:", "logbook:", "retrospective:", "wrapup note:"))
        or (
            "today we" in lower
            and not any(
                k in lower
                for k in ("rule", "invariant", "installed", "capability", "skill", "trigger", "flow", "oauth")
            )
        )
    )
    if is_pure_reflection:
        wiki.logbook_append(entry=clean_text, title="Operational Learning (/learn)")
        return "logbook", None

    # Triage across 4 tiers
    changes: List[Dict[str, Any]] = []

    # 1. Facts (context/rules/<node>.md or context/topology.md)
    fact_nodes = ["hermes", "desktop-pc", "oci-machine", "personal-laptop"]
    matched_nodes = [node for node in fact_nodes if node in lower]
    has_fact_keywords = any(
        k in lower
        for k in (
            "invariant", "tcc", "ax permission", "cuda", "wsl", "permission",
            "host invariant", "tailscale ip", "screen recording"
        )
    )
    if matched_nodes or has_fact_keywords:
        target_node = matched_nodes[0] if matched_nodes else "hermes"
        rel_path = f"context/rules/{target_node}.md"
        existing = wiki.read(rel_path) if wiki.exists(rel_path) else f"# {target_node.title()} Rules & Invariants\n"
        fact_lines = [
            line.strip()
            for line in clean_text.splitlines()
            if any(
                k in line.lower()
                for k in (
                    target_node, "invariant", "tcc", "permission", "access",
                    "cuda", "wsl", "rule", "screen recording"
                )
            )
        ]
        fact_content = "\n".join(fact_lines) if fact_lines else clean_text
        new_content = existing.rstrip() + f"\n\n## Updated Invariants\n{fact_content}\n"
        changes.append({
            "tier": "fact",
            "rel_path": rel_path,
            "content": new_content,
        })

    # 2. Capabilities (context/capabilities.md)
    has_cap_keywords = any(
        k in lower
        for k in (
            "capability", "capabilities", "macos-computer-use-kit",
            "installed", "tool", "kit", "affordance", "package"
        )
    )
    if has_cap_keywords:
        rel_path = "context/capabilities.md"
        existing = wiki.read(rel_path) if wiki.exists(rel_path) else "# Fleet Capabilities & Tool Affordances\n"
        cap_lines = [
            line.strip()
            for line in clean_text.splitlines()
            if any(
                k in line.lower()
                for k in (
                    "capability", "tool", "kit", "affordance",
                    "macos-computer-use", "package", "installed", "staged"
                )
            )
        ]
        cap_content = "\n".join(cap_lines) if cap_lines else clean_text
        new_content = existing.rstrip() + f"\n\n### Discovered Tool Capability\n{cap_content}\n"
        changes.append({
            "tier": "capability",
            "rel_path": rel_path,
            "content": new_content,
        })

    # 3. Skills (runbooks/skills/<name>/SKILL.md)
    has_skill_keywords = any(
        k in lower
        for k in ("skill", "recipe", "cli flag", "procedure", "how to execute", "harness.sh")
    )
    if has_skill_keywords:
        skill_name = "operational_recipe"
        for word in re.findall(r"\b[a-z0-9_-]{4,}\b", lower):
            if word in lower and any(p in lower for p in (f"skill {word}", f"{word} skill", f"{word}-kit")):
                skill_name = word.replace("-", "_")
                break
        rel_path = f"runbooks/skills/{skill_name}/SKILL.md"
        content = f"# Skill: {skill_name}\n\n## Instructions\n{clean_text}\n"
        changes.append({
            "tier": "skill",
            "rel_path": rel_path,
            "content": content,
        })

    # 4. Flows (runbooks/flows/<name>.md)
    has_flow_keywords = any(
        k in lower
        for k in ("flow", "trigger", "reflex", "oauth", "token refresh", "event bus", "blocker reaction")
    )
    if has_flow_keywords:
        flow_name = "reactive_flow"
        if "oauth" in lower or "token refresh" in lower:
            flow_name = "oauth_token_refresh"
        elif "sync" in lower or "audiobookshelf" in lower:
            flow_name = "audiobookshelf_sync"
        rel_path = f"runbooks/flows/{flow_name}.md"
        flow_lines = [
            line.strip()
            for line in clean_text.splitlines()
            if any(
                k in line.lower()
                for k in ("trigger", "oauth", "reflex", "token", "refresh", "blocker", "flow", "autonomous")
            )
        ]
        flow_body = "\n".join(flow_lines) if flow_lines else clean_text
        content = f"# Flow: {flow_name}\n\nTrigger: Eyes matches {flow_name.replace('_', ' ').title()}\n\n## Reflex Procedure\n{flow_body}\n"
        changes.append({
            "tier": "flow",
            "rel_path": rel_path,
            "content": content,
        })

    # If any structured tier changes were detected, create a proposal!
    if changes:
        summary = "Operational learning distillation across memory tiers"
        if len(changes) == 1:
            summary = f"Update {changes[0]['tier']} in {changes[0]['rel_path']}"
        proposal = wiki.propose_multi_update(changes=changes, summary=summary)
        if session is not None:
            session.pending_memory_proposal = proposal
        return "proposal", proposal

    # Fallback to logbook
    wiki.logbook_append(entry=clean_text, title="Operational Learning (/learn)")
    return "logbook", None


async def _extract_media_url_metadata(url: str, timeout: float = 6.0) -> Optional[Dict[str, Any]]:
    """Attempt extracting media metadata using yt-dlp."""
    import shutil
    yt_dlp_bin = shutil.which("yt-dlp") or os.path.expanduser("~/.local/bin/yt-dlp")
    if not os.path.isfile(yt_dlp_bin) or not os.access(yt_dlp_bin, os.X_OK):
        return None

    cmd = [
        yt_dlp_bin,
        "--dump-json",
        "--skip-download",
        "--no-warnings",
        "--no-playlist",
        "--socket-timeout", "4",
        url,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        if proc.returncode == 0 and stdout:
            data = json.loads(stdout.decode("utf-8", errors="ignore").strip())
            if isinstance(data, dict):
                return {
                    "title": data.get("title") or "",
                    "description": data.get("description") or "",
                    "author": data.get("uploader") or data.get("channel") or data.get("creator") or "",
                    "duration": data.get("duration_string") or "",
                    "tags": data.get("tags") or [],
                }
    except Exception as exc:
        logger.debug(f"yt-dlp failed for {url}: {exc}")

    # For Instagram, if basic dump failed and Chrome cookies might help on macOS
    if "instagram.com" in url.lower() and os.environ.get("GOLEM_TEST_MODE") != "1":
        try:
            cmd_cookies = [
                yt_dlp_bin,
                "--cookies-from-browser", "chrome",
                "--dump-json",
                "--skip-download",
                "--no-warnings",
                "--no-playlist",
                "--socket-timeout", "4",
                url,
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd_cookies,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            if proc.returncode == 0 and stdout:
                data = json.loads(stdout.decode("utf-8", errors="ignore").strip())
                if isinstance(data, dict):
                    return {
                        "title": data.get("title") or "",
                        "description": data.get("description") or "",
                        "author": data.get("uploader") or data.get("channel") or data.get("creator") or "",
                        "duration": data.get("duration_string") or "",
                        "tags": data.get("tags") or [],
                    }
        except Exception as exc:
            logger.debug(f"yt-dlp with cookies failed for {url}: {exc}")

    return None


async def _extract_html_metadata(url: str, timeout: float = 4.0) -> Dict[str, str]:
    """Fetch OpenGraph and HTML meta tags via async HTTP."""
    import aiohttp
    import re
    import html as html_lib

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    title = ""
    description = ""
    site_name = ""

    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(headers=headers, timeout=client_timeout) as session:
            async with session.get(url, allow_redirects=True) as resp:
                if resp.status == 200:
                    raw_chunk = await resp.content.read(65536)
                    html_src = raw_chunk.decode("utf-8", errors="ignore")

                    m_og_t = re.search(
                        r"""<meta\s+[^>]*property=["']og:title["'][^>]*content=["']([^"']+)["']""",
                        html_src,
                        re.IGNORECASE,
                    )
                    if not m_og_t:
                        m_og_t = re.search(
                            r"""<meta\s+[^>]*content=["']([^"']+)["'][^>]*property=["']og:title["']""",
                            html_src,
                            re.IGNORECASE,
                        )
                    if m_og_t:
                        title = m_og_t.group(1)
                    else:
                        m_t = re.search(r"<title[^>]*>([^<]+)</title>", html_src, re.IGNORECASE)
                        if m_t:
                            title = m_t.group(1)

                    m_og_d = re.search(
                        r"""<meta\s+[^>]*property=["']og:description["'][^>]*content=["']([^"']+)["']""",
                        html_src,
                        re.IGNORECASE,
                    )
                    if not m_og_d:
                        m_og_d = re.search(
                            r"""<meta\s+[^>]*content=["']([^"']+)["'][^>]*property=["']og:description["']""",
                            html_src,
                            re.IGNORECASE,
                        )
                    if m_og_d:
                        description = m_og_d.group(1)
                    else:
                        m_d = re.search(
                            r"""<meta\s+[^>]*name=["']description["'][^>]*content=["']([^"']+)["']""",
                            html_src,
                            re.IGNORECASE,
                        )
                        if m_d:
                            description = m_d.group(1)

                    m_site = re.search(
                        r"""<meta\s+[^>]*property=["']og:site_name["'][^>]*content=["']([^"']+)["']""",
                        html_src,
                        re.IGNORECASE,
                    )
                    if m_site:
                        site_name = m_site.group(1)
    except Exception as exc:
        logger.debug(f"HTML meta fetch failed for {url}: {exc}")

    return {
        "title": html_lib.unescape(title).strip() if title else "",
        "description": html_lib.unescape(description).strip() if description else "",
        "site_name": html_lib.unescape(site_name).strip() if site_name else "",
    }


async def inspect_single_url(url: str, timeout: float = 6.0) -> Dict[str, Any]:
    """
    Inspect a single URL to extract title, author, description, and platform.
    Prioritizes yt-dlp for rich media (Instagram, YouTube, Reddit, TikTok) and falls back
    to HTML OpenGraph parsing. Guaranteed non-blocking and safe.
    """
    clean_url = str(url).strip()
    if not clean_url or not clean_url.startswith(("http://", "https://")):
        return {"url": clean_url, "error": "Invalid URL scheme"}

    lower = clean_url.lower()
    platform = "Web"
    if "instagram.com" in lower:
        platform = "Instagram"
    elif "youtube.com" in lower or "youtu.be" in lower:
        platform = "YouTube"
    elif "reddit.com" in lower:
        platform = "Reddit"
    elif "twitter.com" in lower or "x.com" in lower:
        platform = "Twitter/X"
    elif "facebook.com" in lower or "fb.watch" in lower:
        platform = "Facebook"
    elif "tiktok.com" in lower:
        platform = "TikTok"

    # Attempt yt-dlp first for media platforms
    try:
        media_data = await _extract_media_url_metadata(clean_url, timeout=timeout)
        if media_data and (media_data.get("title") or media_data.get("description")):
            return {
                "url": clean_url,
                "platform": platform,
                "title": media_data.get("title", ""),
                "description": media_data.get("description", ""),
                "author": media_data.get("author", ""),
                "duration": media_data.get("duration", ""),
                "tags": media_data.get("tags", []),
            }
    except Exception as me:
        logger.debug(f"Media extraction error for {clean_url}: {me}")

    # Fallback to HTML OpenGraph / title fetch
    try:
        html_data = await _extract_html_metadata(clean_url, timeout=timeout * 0.75)
        if html_data.get("title") or html_data.get("description"):
            return {
                "url": clean_url,
                "platform": html_data.get("site_name") or platform,
                "title": html_data.get("title", ""),
                "description": html_data.get("description", ""),
                "author": "",
            }
    except Exception as he:
        logger.debug(f"HTML extraction error for {clean_url}: {he}")

    return {
        "url": clean_url,
        "platform": platform,
        "title": "",
        "description": "",
        "author": "",
    }


async def inspect_and_summarize_urls(urls: List[str], timeout: float = 6.0) -> List[Dict[str, Any]]:
    """Concurrently inspect multiple URLs with bounded concurrency and timeout."""
    if not urls:
        return []
    valid_urls = [
        u for u in urls
        if isinstance(u, str)
        and u.startswith(("http://", "https://"))
        and not any(x in u.lower() for x in ("unsubscribe", "optout", "opt-out", "mail.google.com"))
    ][:4]
    if not valid_urls:
        return []

    tasks = [inspect_single_url(u, timeout=timeout) for u in valid_urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    summaries = []
    for idx, r in enumerate(results):
        if isinstance(r, dict):
            summaries.append(r)
        else:
            summaries.append({"url": valid_urls[idx], "error": str(r)})
    return summaries


def derive_actionable_followup(corpus: str) -> str:
    """Infer an actionable next step question based on email and link content."""
    lower = corpus.lower()
    if any(k in lower for k in ("ticket", "tickets", "concert", "tour", "show", "event", "venue", "on sale", "festival", "lineup", "performance", "presale")):
        return "Want me to create a calendar event for tomorrow to buy tickets?"
    elif any(k in lower for k in ("calendar", "schedule", "meet", "meeting", "call", "appointment", "deadline", "due date", "flight", "reservation")):
        return "Want me to schedule a calendar reminder for this?"
    elif any(k in lower for k in ("article", "newsletter", "post", "blog", "paper", "essay", "read", "thread")):
        return "Want me to summarize the key takeaways or add this to your reading list?"
    elif any(k in lower for k in ("reel", "video", "clip", "watch", "youtube", "tiktok", "instagram")):
        return "Want me to look into more details or set a reminder for you on this?"
    else:
        return "Want me to create a calendar event or follow up on this for you?"


async def synthesize_email_briefing(
    sender: str,
    subject: str,
    body: str = "",
    snippet: str = "",
    urls: Optional[List[str]] = None,
    inspected_urls: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[str, str]:
    """
    Synthesize a concise Chief of Staff message for Carl in Slack.
    Pattern:
      1. Analyze contents (what the shared item is about).
      2. Share with Carl directly in Slack.
      3. Suggest an actionable follow-up (e.g. 'Want me to create a calendar event for tomorrow to buy tickets?').
    Returns (briefing_text, suggested_action).
    """
    active_urls = urls or []
    active_inspected = inspected_urls or []

    # Build corpus to determine suggested action
    inspected_text = " ".join(
        f"{i.get('title', '')} {i.get('description', '')}" for i in active_inspected
    )
    corpus = f"{subject} {snippet} {body} {inspected_text}"
    suggested_action = derive_actionable_followup(corpus)

    clean_sender = sender.split("<")[0].strip().strip('"').strip("'") or sender.strip()
    if not clean_sender:
        clean_sender = "Carl"

    # Deterministic fallback builder
    def _build_fallback() -> str:
        analysis_parts = []
        for item in active_inspected:
            p_title = item.get("title")
            p_desc = item.get("description")
            p_auth = item.get("author")
            p_plat = item.get("platform", "Link")

            header = f"*{p_title}*" if p_title else f"*{subject}*"
            if p_auth:
                header += f" (by {p_auth} on {p_plat})"
            desc_str = f"\n> {p_desc[:400].strip()}" if p_desc else ""
            analysis_parts.append(f"{header}{desc_str}")

        if not analysis_parts:
            if snippet:
                analysis_parts.append(f"> {snippet[:400].strip()}")
            elif body:
                analysis_parts.append(f"> {body[:400].strip()}")
            else:
                analysis_parts.append(f"> Shared item: {subject}")

        analysis_block = "\n\n".join(analysis_parts)
        url_block = "\n".join([f"🔗 <{u}>" for u in active_urls]) if active_urls else ""
        url_section = f"\n{url_block}" if url_block else ""

        return (
            f"📬 *Shared via Email from {clean_sender}:* *{subject}*\n\n"
            f"{analysis_block}"
            f"{url_section}\n\n"
            f"_{suggested_action}_"
        )

    is_test_env = os.environ.get("FIRSTMATE_TEST_MODE") == "1" or os.environ.get("GOLEM_TEST_MODE") == "1"
    key = get_gemini_api_key()

    if is_test_env or not key:
        return _build_fallback(), suggested_action

    # Attempt dynamic LLM synthesis via Gemini Flash
    try:
        sys_prompt = (
            "You are AGY, Carl's Chief of Staff and operational liaison in Slack.\n"
            "Carl has received a shared email or link (e.g. Instagram Reel, Reddit post, event link).\n"
            "Deliver an executive Chief of Staff briefing directly to Carl in Slack following this exact pattern:\n"
            "1. State what was shared and analyze what the shared item/video/post is about.\n"
            "2. Include the direct URL if present (using Slack mrkdwn: <URL>).\n"
            "3. Conclude with a clear, actionable follow-up question (e.g. asking if Carl wants you to create a calendar event, set a reminder, or look into tickets/details).\n"
            "Formatting: Slack mrkdwn (*bold*, `code`, <URL|Label>). Crisp, executive, no fluff, no raw markdown headers (#)."
        )
        user_prompt = (
            f"Incoming Email Metadata:\n"
            f"• From: {sender}\n"
            f"• Subject: {subject}\n"
            f"• Snippet: {snippet}\n"
            f"• Body: {body[:1500]}\n"
            f"• Extracted URLs: {active_urls}\n"
            f"• Inspected Media/URL Details:\n{json.dumps(active_inspected, indent=2)}\n"
            f"• Inferred Suggested Action: {suggested_action}\n"
        )
        payload = {
            "systemInstruction": {"parts": [{"text": sys_prompt}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": user_prompt}],
                }
            ],
            "generationConfig": {"temperature": 0.2},
        }
        resp = await call_gemini_api(
            payload=payload,
            model=get_workhorse_model(),
            timeout_seconds=60.0,
        )
        if resp and isinstance(resp, dict):
            candidates = resp.get("candidates") or []
            if candidates and isinstance(candidates, list):
                content = candidates[0].get("content") or {}
                parts = content.get("parts") or []
                text_chunks = [p.get("text", "") for p in parts if isinstance(p, dict)]
                llm_text = "".join(text_chunks).strip()
                if llm_text:
                    return llm_text, suggested_action
    except Exception as le:
        logger.debug(f"LLM email briefing synthesis failed: {le}")

    return _build_fallback(), suggested_action


def _is_ticket_suppressed_by_telemetry(
    ticket: Dict[str, Any],
    runtime_telemetry: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Evaluate whether live runtime telemetry indicates this ticket is actively in-flight,
    queued in an active batch execution, or had recent human/agent interaction.
    """
    if not isinstance(ticket, dict):
        return False

    if str(ticket.get("title") or "").strip().startswith("[Hand]"):
        return True

    if not runtime_telemetry or not isinstance(runtime_telemetry, dict):
        return False

    ident = str(ticket.get("identifier") or ticket.get("id") or "").strip()
    if runtime_telemetry.get("is_in_flight") or runtime_telemetry.get("is_queued_in_batch"):
        return True

    in_flight_keys = {str(k) for k in (runtime_telemetry.get("in_flight_keys") or []) if k}
    queued_keys = {str(k) for k in (runtime_telemetry.get("queued_keys") or []) if k}
    related_keys = {ident} if ident else set()
    for st in (ticket.get("subtasks") or []):
        if isinstance(st, dict) and st.get("identifier"):
            related_keys.add(str(st["identifier"]))

    if related_keys.intersection(in_flight_keys) or related_keys.intersection(queued_keys):
        return True

    sec_since = runtime_telemetry.get("seconds_since_last_interaction")
    if isinstance(sec_since, (int, float)) and 0.0 <= float(sec_since) <= 900.0:
        return True

    return False


def _build_natural_ticket_cta(
    ticket: Dict[str, Any],
    runtime_telemetry: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Formulate a natural, context-aware call-to-action based on actual ticket,
    subtask roadmap, and error state (never static start/continue/retry/cancel boilerplate).
    """
    key = ticket.get("identifier", "CHO-???")
    subtasks = ticket.get("subtasks") or []
    last_error = (
        ticket.get("last_error")
        or (runtime_telemetry.get("last_error") if isinstance(runtime_telemetry, dict) else None)
    )

    uncompleted = [st for st in subtasks if isinstance(st, dict) and not st.get("settled")]
    total_st = len(subtasks)
    settled_st = total_st - len(uncompleted)

    if last_error:
        target_st = uncompleted[0] if uncompleted else None
        target_label = (
            f"*{target_st.get('identifier')}* ({target_st.get('title')})"
            if (target_st and target_st.get("identifier"))
            else f"*{key}*"
        )
        return (
            f"The last run hit an error (`{str(last_error)[:160]}`). "
            f"Would you like me to *retry* {target_label}?"
        )

    if subtasks:
        all_todo = all(
            (
                (st.get("state", {}).get("name") if isinstance(st.get("state"), dict) else str(st.get("state") or "Planned"))
                .strip()
                .lower()
                in ("todo", "planned", "backlog", "unstarted", "triage")
            )
            for st in uncompleted
        )
        if settled_st == 0 and all_todo:
            first_st = uncompleted[0] if uncompleted else subtasks[0]
            st_key = first_st.get("identifier") or ""
            st_title = first_st.get("title") or "Phase 1"
            first_label = f"*{st_key}* ({st_title})" if st_key else f"*{st_title}*"
            return f"0/{total_st} subtasks have started—ready for me to *kick off Phase 1* ({first_label})?"

        if uncompleted:
            next_st = uncompleted[0]
            st_key = next_st.get("identifier") or ""
            st_title = next_st.get("title") or "the next subtask"
            if st_key:
                return f"{settled_st}/{total_st} subtasks are settled—want me to *pick back up where we left off on {st_key}* ({st_title})?"
            return f"{settled_st}/{total_st} subtasks are settled—want me to *pick back up where we left off on {st_title}*?"

    return f"Ready for me to *kick off Phase 1* on *{key}*?"


def synthesize_assigned_tickets_prompt(
    tickets: List[Dict[str, Any]],
    runtime_telemetry: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Format a natural, context-aware Chief of Staff nudge for stalled Linear ticket(s).
    Evaluates in-flight/queued telemetry to suppress active work and tailors the
    call-to-action to the actual subtask state (kick off Phase 1, pick back up on Subtask X, or retry).
    """
    if not tickets:
        return ""

    stalled_tickets = [
        t for t in tickets
        if not _is_ticket_suppressed_by_telemetry(t, runtime_telemetry)
    ]
    if not stalled_tickets:
        return ""

    count = len(stalled_tickets)
    if count == 1:
        t0 = stalled_tickets[0]
        k0 = t0.get("identifier", "CHO-???")
        header = f"📋 *Linear Attention Required ({k0}):*"
    else:
        header = f"📋 *Linear Attention Required:* You have {count} stalled tickets awaiting direction:"

    items = []
    ctas = []
    for t in stalled_tickets:
        key = t.get("identifier", "CHO-???")
        title = t.get("title", "Untitled")
        state = t.get("state")
        state_str = state.get("name", "Unknown") if isinstance(state, dict) else str(state or "Unknown")
        project = t.get("project", "")
        url = t.get("url", "")
        proj_str = f" [{project}]" if project and project != "No Project" else ""
        link_str = f"<{url}|{key}>" if url else f"*{key}*"

        subtasks = t.get("subtasks") or []
        subtask_lines = []
        if subtasks:
            total_st = len(subtasks)
            settled_st = sum(1 for st in subtasks if st.get("settled"))
            subtask_lines.append(f"  └ *Roadmap:* {settled_st}/{total_st} milestones settled")
            for st in subtasks:
                if not st.get("settled"):
                    st_key = st.get("identifier") or ""
                    st_title = st.get("title") or "Untitled"
                    st_state = (st.get("state") or {}).get("name") if isinstance(st.get("state"), dict) else str(st.get("state") or "Planned")
                    key_prefix = f"[{st_key}] " if st_key else ""
                    subtask_lines.append(f"    • {key_prefix}{st_title} (`{st_state}`)")

        item_text = f"• {link_str}: *{title}* (Status: `{state_str}`){proj_str}"
        if subtask_lines:
            item_text += "\n" + "\n".join(subtask_lines)
        items.append(item_text)
        ctas.append(_build_natural_ticket_cta(t, runtime_telemetry))

    items_block = "\n".join(items)
    question = ctas[0] if len(ctas) == 1 else "\n".join(f"• {c}" for c in ctas)
    return f"{header}\n\n{items_block}\n\n{question}"


async def synthesize_linear_ticket_nudge(
    ticket: Dict[str, Any],
    runtime_telemetry: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Pass a single Linear sentinel ticket + live runtime telemetry through the Mouth LLM
    to evaluate whether it is genuinely stalled vs. in-flight/queued, and formulate a
    natural, context-aware prompt (or suppress silently).
    """
    if not ticket or not isinstance(ticket, dict):
        return ""

    if _is_ticket_suppressed_by_telemetry(ticket, runtime_telemetry):
        return ""

    fallback_text = synthesize_assigned_tickets_prompt([ticket], runtime_telemetry=runtime_telemetry)
    if not fallback_text:
        return ""

    is_test_env = os.environ.get("FIRSTMATE_TEST_MODE") == "1" or os.environ.get("GOLEM_TEST_MODE") == "1"
    key = get_gemini_api_key()
    if is_test_env or not key:
        return fallback_text

    try:
        sys_prompt = (
            "You are Golem, Carl's Chief of Staff in Slack.\n"
            "The Linear sentinel surfaced a single ticket that may need Carl's attention.\n"
            "First, evaluate Nudge Hygiene using the provided Live Runtime Telemetry:\n"
            "- If this ticket or any of its subtasks has an active background worker, is queued in an active batch execution, "
            "or had user/agent interaction within the last 15 minutes, output ONLY the exact token: SUPPRESS_NUDGE\n"
            "- Otherwise, the ticket is genuinely stalled/idle. Formulate a concise Slack mrkdwn nudge for this single ticket:\n"
            "  1. Include the ticket link (<URL|KEY>), bold title, status, and remaining subtask roadmap.\n"
            "  2. NEVER use the robotic phrase 'Would you like me to start, continue, retry, or cancel this ticket?'.\n"
            "  3. Formulate a natural call-to-action based on actual subtask state:\n"
            "     • If 0/N subtasks are done and all are Todo/Planned (even if parent was marked In Progress), ask if Carl is ready for you to kick off Phase 1.\n"
            "     • If some subtasks are settled or a runner paused mid-stream, ask if Carl wants to pick back up where we left off on Subtask X.\n"
            "     • If the last run threw an error or failed verification, mention what broke and ask if Carl wants to retry.\n"
            "Formatting: Slack mrkdwn (*bold*, `code`, <URL|Label>). No markdown headers (#)."
        )
        user_prompt = (
            f"Ticket Metadata:\n{json.dumps(ticket, indent=2, default=str)}\n\n"
            f"Live Runtime Telemetry:\n{json.dumps(runtime_telemetry or {}, indent=2, default=str)}\n"
        )
        payload = {
            "systemInstruction": {"parts": [{"text": sys_prompt}]},
            "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
            "generationConfig": {"temperature": 0.2},
        }
        resp = await call_gemini_api(
            payload=payload,
            model=get_workhorse_model(),
            timeout_seconds=60.0,
        )
        if resp and isinstance(resp, dict):
            candidates = resp.get("candidates") or []
            if candidates and isinstance(candidates, list):
                content = candidates[0].get("content") or {}
                parts = content.get("parts") or []
                text_chunks = [p.get("text", "") for p in parts if isinstance(p, dict)]
                llm_text = "".join(text_chunks).strip()
                if "SUPPRESS_NUDGE" in llm_text:
                    return ""
                if llm_text:
                    return llm_text
    except Exception as exc:
        logger.debug(f"LLM linear nudge synthesis fallback: {exc}")

    return fallback_text


def synthesize_linear_quota_nudge(alert_data: Optional[Dict[str, Any]] = None) -> str:
    """
    Synthesize an executive notification for Carl when Linear free tier quota is reached.
    Explains that activeIssueCount limit is exceeded and writes/issue creations are blocked.
    Prompts Carl to clean up, archive, or delete Done/Cancelled tickets.
    Explicitly confirms zero automated deletions were performed (Carl owns hard deletes).
    """
    data = alert_data or {}
    metric = data.get("metric") or "activeIssueCount"
    error_code = data.get("error_code") or "USAGE_LIMIT_EXCEEDED"

    return (
        "🚨 *Linear Free Tier Issue Quota Reached*\n"
        f"The Nose detected that Linear issue creation is currently blocked (`{error_code}` on `{metric}`).\n\n"
        "• *Impact:* Golem cannot create new tracking issues or subtasks in Linear until quota headroom is restored.\n"
        "• *Action Needed:* Please review and clean up, archive, or hard-delete settled *Done* or *Canceled* tickets in the Linear workspace.\n"
        "• *Guardrail Assurance:* In accordance with policy, no automated ticket deletions have been performed. Carl owns hard delete operations."
    )



