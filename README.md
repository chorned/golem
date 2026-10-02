# First Mate: Fleet-Wide Agentic Orchestration Companion

> **"A first-mate virtual companion that can achieve any objective across the fleet through agentic orchestration."**

First Mate (operated by **AGY**, Carl's Chief of Staff) is an autonomous, distributed operations companion built on the **FirstMate Pattern** ([`kunchenguid/firstmate`](https://github.com/kunchenguid/firstmate)). It serves as a persistent pair-programming partner, homelab operator, and fleet orchestrator across heterogeneous machines, transforming high-level conversational intent into verified, hardened backstage execution without exposing terminal noise or raw scratchpads to the user.

---

## 1. The FirstMate Pattern: One Captain, One Liaison

The foundational rule of First Mate is strict separation between conversational liaison and backstage execution:

1. **One Captain, One Liaison:**
   - **The Captain (Carl):** Interacts exclusively with AGY via natural chat ([Slack Socket Mode](file:///Users/hermes/projects/first-mate/app.py), [BlueBubbles iMessage](file:///Users/hermes/projects/first-mate/bluebubbles_transport.py)).
   - **The Crew (Backstage Workers):** Specialized workers (`hermes`, `laptop-worker`, `pc-worker`, `oci-worker`, `opencode`) operate strictly in the background. They never talk directly to Slack, leak raw terminal box-drawing glyphs, or output prompt echoes.
   - **Executive Briefings:** Raw tool outputs, exit codes, and diffs are triaged, interpreted, and synthesized by AGY into structured, high-signal briefings (verdict first, metrics/diffs, file transfers, and clear decisions).

2. **Brain, Soul & Body Architecture:**
   - **The Soul:** Declarative operational policies defined in [`AGENTS.md`](file:///Users/hermes/projects/first-mate/AGENTS.md), [`GEMINI.md`](file:///Users/hermes/projects/first-mate/GEMINI.md), and [`.agents/rules/`](file:///Users/hermes/projects/first-mate/.agents/rules/delegation.md).
   - **The Brain:** The local `agy` CLI binary (`~/.local/bin/agy`) executed headlessly in stream-json mode.
   - **The Body:** Thin Ingress Gateway ([`app.py`](file:///Users/hermes/projects/first-mate/app.py) / [`first_mate/gateway`](file:///Users/hermes/projects/first-mate/first_mate/gateway/ingress.py)) managing socket connections, thread session handles, and reacji state lifecycles.
   - **The Machinery:** Herdr daemon managing PTY persistence, dual-pane workspaces, and cross-machine mesh connectivity.

---

## 2. Dual-Pane Architecture: "Mouth vs Hands"

A key operational lesson in building autonomous fleet agents is that **reasoning and conversational loops must never be serialized behind blocking command execution**.

First Mate enforces a strict **Dual-Pane Separation** ([`DualPaneHarness`](file:///Users/hermes/projects/first-mate/first_mate/execution/harness.py)):

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             Dual-Pane Workspace                             │
│                                                                             │
│   ┌───────────────────────────────┐     ┌───────────────────────────────┐   │
│   │    Pane 1: Liaison ("Mouth")   │     │     Pane 2: Runner ("Hands")  │   │
│   │                               │     │                               │   │
│   │ • Dedicated Chief of Staff    │     │ • Dedicated interactive shell │   │
│   │   engine (agy headless)       │     │   (zsh/bash/PowerShell)       │   │
│   │ • Handles planning, reasoning,│     │ • Executes compilers, builds, │   │
│   │   conversation, & triage      │     │   ffmpeg, docker, tests       │   │
│   │ • Dispatches tasks to runner  │──┐  │ • Bounded yield timeout (3s)  │   │
│   │ • Uninterruptible & responsive│  │  │ • Non-blocking handles:       │   │
│   │   to user queries/status      │  │  │   ExecutionHandle(status)     │   │
│   └───────────────────────────────┘  │  └───────────────────────────────┘   │
│                                      │                  ▲                   │
│                                      └──────────────────┘                   │
│                                        Async Dispatch                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

- **Pane 1: Liaison ("The Mouth"):**
  - Runs the conversational reasoning engine (`agy`).
  - Evaluates user intent, plans workflows, monitors execution handles, and posts updates.
  - Releases conversational session locks immediately upon dispatching long-running work.
  - Remains permanently responsive to conversational inquiries, status checks, and cancel commands.

- **Pane 2: Runner ("The Hands"):**
  - Runs commands via [`RunnerExecutor`](file:///Users/hermes/projects/first-mate/first_mate/execution/runner.py) inside an isolated Herdr PTY.
  - If a command completes within the yield timeout (e.g., 3.0s), it returns immediately.
  - If execution exceeds the yield timeout, it yields an `ExecutionHandle(status="running", pid=...)` and continues in the background, freeing the Liaison to confirm receipt and keep conversing.

---

## 3. Fleet Machine Topology & Node Profiles

First Mate orchestrates tasks across a 5-node distributed topology:

| Workspace / Target | Host / Target Machine | Role & Responsibilities | Execution Mode | Guardrails & Policies |
| :--- | :--- | :--- | :--- | :--- |
| **`fleet-control`** | `localhost` (`/Users/hermes`) | **Antigravity Chief of Staff & Primary Dev Anchor:** 24/7 uptime local hub. All code refactoring, Git commits, PRs, test suites (`pytest`), and planning. | Headless `agy` CLI | Primary development anchor. Zero unsolicited sleeps. |
| **`homelab-ops`** | `localhost` (`hermes`) | **Hermes Butler:** Docker containers, storage pools (ZFS, TrueNAS), backups, media stacks (CarlFlix, Plex, Jellyfin, *arr). | Headless `hermes -z` / scripts | Isolated from dev worktrees. Never edit `.env` or service plists without prompt. |
| **`compute-grid`** | `desktop-pc` (`100.80.59.45`) | **Compute Grid (Windows):** NVIDIA GPU / CUDA workloads, PyTorch, model evaluations, local LLM quantization. | Remote SSH + `cmd.exe /c` wrapping | POSIX scripts wrapped for Windows. Dedicated `CUDA_VISIBLE_DEVICES`. |
| **`dev-grid`** | `personal-laptop` (`100.96.38.5`) | **Carl's Active Workstation (Strict Ad-Hoc):** Sensitive workstation; sleeps frequently. | Remote SSH (strict ad-hoc, 3s timeout) | **Strictly Ad-Hoc:** Never run background jobs here. Explicit requests only ("on my laptop"). Graceful fallback if asleep. |
| **`cloud-ops`** | `oci-machine` (`100.113.173.33`) | **Cloud Operations (Linux aarch64):** Production Oracle Cloud VPS. Ingress, public WireGuard, DNS, SSL. | Remote SSH (strict guardrails) | Strictly reserved for live production infrastructure. No generic compute or test scripts. |

---

## 4. Reacji State Lifecycle & Executive Command Suite

Conversational turns are tracked non-verbally with zero greeting spam or robotic canned footers:

### Reacji State Lifecycle
- `:eyes:` — Message received and queued (< 200ms non-verbal ingress ack).
- `:gear:` — Task dispatched backstage to Herdr runner or fleet machine.
- `:warning:` — Crewmate blocked or approval required (posted with a natural question in thread).
- `:white_check_mark:` — Task completed successfully; synthesized executive briefing posted.
- `:x:` — Task failed; concise failure diagnosis posted.

### Executive Command Suite
- **`/bearings` (Fleet Digest):** Concurrently gathers high-level 4-node fleet state telemetry (disk, Docker, GPU, laptop awake state, cloud uptime) and delivers a crisp 4-bullet executive briefing.
- **`/ahoy` (Session Catch-Up & Decision Resolver):** Audits active panes for blocked states or pending confirmations, resolves open decisions, and summarizes background progress.
- **`/wrapup` (Session Housekeeping):** Audits git worktrees across `hermes` and awake laptop, sweeps stale task buffers/locks, analyzes recent logs for recurring quirks, and proposes reviewable rule diffs for Carl's approval.
- **`/update` (Safe Full-Stack Self-Update):** Pre-flight active task gate, `git pull origin main`, automated test verification gate (`pytest`), automatic rollback on failure, binary audit, and clean daemon reload.
- **`/learn` (Operational Memory Crystallization):** Distills thread operational traces into permanent node rules (`.agents/rules/<node>.md`) or skills (`.agents/skills/<skill>/SKILL.md`).

---

## 5. System Architecture & Modular Package Layout

```
first-mate/
├── app.py                         # Thin Ingress Gateway & Socket Mode Entrypoint
├── supervisor.py                  # Process supervisor & rogue server cleanup
├── herdr_client.py                # Asynchronous JSON-RPC / CLI wrapper for Herdr daemon
├── herdr_fleet.py                 # Cross-machine SSH & Tailscale node router
├── herdr_tools.py                 # Tool dispatcher, action payload validator & runner
├── telemetry_observer.py          # Real-time terminal tail observer & milestone reporter
│
└── first_mate/                    # Core Modular Architecture (v2.0)
    ├── agent/                     # Subprocess adapter & stream-json protocol
    │   ├── adapter.py             # HeadlessAgentAdapter with NDJSON streaming
    │   ├── calibration.py         # Subprocess environment calibration & checks
    │   ├── events.py              # Event definitions & schemas
    │   ├── prompts.py             # System prompt templates & context loaders
    │   └── protocol.py            # Strongly typed AgentEvent models
    │
    ├── execution/                 # Dual-Pane & Mesh Execution Engine
    │   ├── buffer.py              # RunnerBufferObserver (PTY scrollback monitor)
    │   ├── delegation.py          # WorkerDelegator (Isolated branch/worktree sandboxes)
    │   ├── harness.py             # DualPaneHarness (Liaison Pane 1 + Runner Pane 2)
    │   ├── mesh.py                # Fleet topology & per-pane locking integration
    │   └── runner.py              # RunnerExecutor (Non-blocking yield timeouts)
    │
    ├── gateway/                   # Ingress Gateway & Session Management
    │   ├── ingress.py             # IngressGateway (Reacji, deduplication, commands)
    │   ├── router.py              # SessionRouter (Warm session cache & reaper)
    │   └── session.py             # SessionHandle (Thread-scoped state machine & turn loop)
    │
    └── transport/                 # Wire Normalization & Communication
        ├── chunker.py             # Boundary-aware Slack mrkdwn splitter (<4000 chars)
        ├── milestone.py           # Conversational milestone updater
        ├── normalizer.py          # Terminal chrome, ANSI, and TUI debris stripping
        └── slack_client.py        # Robust async Slack SDK client wrapper
```

---

## 6. Operational Guardrails & Beta Hardening

To ensure uninterrupted uptime and absolute conversational responsiveness:

1. **Volume & I/O Protection (`/Volumes/*`, SMB/NFS):**
   - Media commands (`ffmpeg`, `ffprobe`, `mediainfo`) MUST run non-interactively (`-nostdin`, `-hide_banner`, `-y`) and with explicit extraction bounds (`-frames:v 1`, `-t 30`).
   - File sweeps over network volumes (`find`, `grep`, `du`) must specify depth boundaries (`-maxdepth 3`) or run asynchronously in Pane 2.
2. **Subprocess Execution Watchdogs:**
   - Synchronous tool calls enforce a hard wall-clock timeout.
   - Processes entering uninterruptible I/O wait (`U+`) trigger alerts and fail-fast timeouts rather than hanging the agent turn.
3. **Conversational Non-Blocking Guarantee:**
   - Ingress messages are never locked behind blocking background jobs.
   - User status check-ins ("did you figure it out?", "are we stuck?") query the active runner state out-of-band and reply immediately.
4. **NDJSON Stream Integrity:**
   - Turn demarcations are driven exclusively by protocol events (`result`, `awaiting_input`) rather than arbitrary idle timers, preventing off-by-one stdout pipe phase shifts.

---

## 7. Quickstart & Verification

### Prerequisites
- Python 3.11+
- `agy` CLI binary (`~/.local/bin/agy`)
- `herdr` daemon installed and running (`herdr daemon status`)
- Tailscale connected fleet machines

### Setup
```bash
# Clone and enter directory
cd /Users/hermes/projects/first-mate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with SLACK_APP_TOKEN, SLACK_BOT_TOKEN, etc.
```

### Running Tests
The test suite contains over 640 unit, boundary, and adversarial stress tests:
```bash
# Run entire test suite
pytest

# Run gateway and execution test suites specifically
pytest tests/test_gateway_m3.py tests/test_execution_m4.py tests/test_workspace_layout.py
```

### Launching the Daemon
```bash
# Direct run
python3 app.py

# Or via launchd service
launchctl load ~/Library/LaunchAgents/com.user.first-mate.plist
```
