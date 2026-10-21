# Golem: Autonomous Fleet Chief of Staff

[![CI](https://github.com/chorned/golem/actions/workflows/ci.yml/badge.svg)](https://github.com/chorned/golem/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Lineage](https://img.shields.io/badge/open--source-notable--projects-purple.svg)](#notable-open-source-projects)

<p align="center">
  <img src="docs/assets/golem_architecture.jpg" alt="Golem Sensory Architecture" width="100%">
</p>

---

## Built for Slack & Antigravity, easily extensible to any Stack

Golem is built for **Slack** and **Antigravity (`agy`)** out of the box, but its modular architecture makes it easily modified to work with any combination of LLM provider and chat interface:

### Requirements:
- **LLM API Key:** Google Gemini, Anthropic Claude, OpenAI, or local open-weights models (**Mistral**, **DeepSeek**, **Qwen**) via Ollama or vLLM.
- **Chat Interface:** Slack Bot Token, or Discord, WhatsApp, iMessage (via BlueBubbles), or any other chat platform.
- **Agent Harness:** Antigravity (`agy`) out of the box, with built-in support for alternative harnesses like Claude Code, Codex, or OpenCode.

---

## Why Golem? The Open-Source Alternative to Closed Assistants

Persistent digital assistants are emerging rapidly—from **Meta Muse** and **OpenAI Dot** to **xAI Grokbot**. But commercial walled gardens and proprietary corporate assistants trap your personal data, terminal workflows, and codebases inside locked silos. You cannot audit their prompts, you cannot inspect their executions, you cannot connect them to your private infrastructure, and you are beholden to their subscription whims and terms of service.

**Golem is the sovereign, open-source alternative.** Built on open foundations—the **Herdr** distributed terminal manager, **BlueBubbles** iMessage bridge, and Git-backed memory engines—Golem runs on your own machines, lives inside your daily chat tools, and coordinates an elastic fleet of worker subagents across your workstations, homelab servers, GPU machines, and cloud instances.

### Why Open Source Wins:

1. **Endlessly Customizable:**
   - Closed corporate assistants force you into rigid conversational boxes, fixed persona traits, and artificial guardrails.
   - Golem is an open-ended Chief of Staff endowed with **infinite hiring powers**. You define your own toolchains, automated reflexes, bash harnesses, execution rules, and specialized crewmates. Spin up detached Git worktrees for deep refactors, author new skills on the fly via `/learn`, or link reactive triggers directly to your infrastructure. It adapts completely to your workflow.

2. **True Model Mobility & Freedom:**
   - Proprietary assistants lock you into a single corporate LLM provider with zero portability.
   - Golem is completely model-agnostic and mobile. By default, it is optimized to harness Google's Gemini and Antigravity stack for fast responses and massive 1M–2M token context windows. But you can freely swap the brain to **any LLM**—including open-weight models like **Mistral**, **DeepSeek**, or **Qwen** running locally via Ollama or vLLM, frontier models like Anthropic Claude and OpenAI, or cost-optimizing proxies like FrugaLLM.

3. **Enhanced Privacy & Self-Hosted Sovereignty:**
   - Golem executes commands, manages files, and stores memory locally on your own private encrypted mesh (like Tailscale or WireGuard) with zero external SaaS dependencies.
   - Unlike walled-garden assistants that ingest and index your workspace on external clouds, your raw terminal sessions, full file trees, and local configurations remain strictly on your machines. Only prompt text specifically constructed for each turn transits the LLM API you configure.
   - For **total privacy and zero data leakage**, connect Golem to local self-hosted open-weights models (via Ollama or vLLM)—allowing you to run the entire Chief of Staff stack 100% offline with zero external network calls.

---

## Intuitive Architecture: The Sensory Model

Golem replaces fragile, monolithic agent loops with an intuitive four-organ sensory architecture designed to keep your conversation fluid no matter how intense the background workload:

- 👄 **Front-of-House Mouth (Liaison):**
  **The Mouth is what talks to you.** Operating in Pane 1 (`{session_id}-liaison`), it is your dedicated conversational partner in Slack or your preferred chat app. It handles natural dialogue, answers questions, reviews diffs, dispatches background tasks via `dispatch_task`, and delivers concise executive briefings. Because the Mouth is completely decoupled from heavy terminal execution, you can chat with Golem or ask questions mid-stream while long-running jobs execute backstage—without ever blocking the conversational thread.

- ✋ **Backstage Hands (Execution Grid):**
  **The Hands do the work.** Operating headlessly in Pane 2 (`runner`) and dedicated subagent sandboxes, the Hands run builds, execute tests, refactor codebases, and manage services. Powered by **Herdr's distributed terminal mesh**, the Hands have genuine cross-machine capability: Golem seamlessly coordinates work across your local workstation, homelab servers, GPU compute boxes, and cloud VPS instances over an encrypted network mesh (such as Tailscale or WireGuard). Powered by the default lightning-fast AGY CLI (`worker="agy"`, `GOLEM_DEFAULT_HAND_WORKER=agy`), with pluggable support for alternatives like Claude Code, Codex, or OpenCode. Commands running longer than 3.0 seconds yield non-blocking `ExecutionHandle(status="running")` tokens so you are never left waiting on a frozen terminal pipe.

- 👁️ **The Eyes (Task Observer):**
  **The Eyes look at ongoing work.** A non-blocking background observer inspecting active terminal buffers every 15–30 seconds. Using SHA-256 buffer delta hashing, it consumes zero LLM tokens when commands are running quietly without changes. It interrupts only when human attention is truly required (such as an interactive prompt or confirmation) or when a task finishes.

- 👃 **The Nose (Fleet Health Sentinel):**
  **The Nose smells what's going on elsewhere and alerts you.** An autonomous 24/7 background sentinel that scans log tails, container health, storage pressure, and node reachability across your machines every 15 minutes. Operating with zero regex gates, it stays completely silent when your machines are healthy (0 tokens) and emits proactive alerts across the `ShoulderTapEventBus` the moment an anomaly is detected.

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                        GOLEM TRIPARTITE SENSORY ARCHITECTURE                                │
│                                                                                             │
│   Slack / Chat Ingress                                                                      │
│              │                                                                              │
│              ▼                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                         FRONT-OF-HOUSE MOUTH (Pane 1)                               │   │
│   │                                                                                     │   │
│   │  • Conversational liaison (MOUTH_MODEL: Gemini 3.8 Flash)                           │   │
│   │  • Session isolation; formatted Slack mrkdwn output                                 │   │
│   │  • Dispatches background work via structured tool calls (dispatch_task)             │   │
│   │  • Reads sensory status on-demand (check_eyes, check_nose)                          │   │
│   │  • Non-blocking: Keeps conversation responsive while tasks execute backstage        │   │
│   └──────────────┬──────────────────────────────────────────────────────▲───────────────┘   │
│                  │                                                      │                   │
│         dispatch_task                                           ShoulderTapEventBus         │
│         (Tool Call)                                             (Async Event Alerts)        │
│                  │                                                      │                   │
│                  ▼                                                      │                   │
│   ┌──────────────────────────────────────────────────┐   ┌──────────────┴───────────────┐   │
│   │           BACKSTAGE HANDS (Pane 2..N)            │   │        SENSORY PLANE         │   │
│   │                                                  │   │                              │   │
│   │  • RunnerExecutor in isolated Herdr PTY panes    │   │  • THE EYES (Task Observer)  │   │
│   │  • Cross-machine fleet execution over mesh       │   │    Inspects active buffer    │   │
│   │  • Default Engine: Local AGY CLI (worker="agy")  │◄──┤    every 15–30s; hashes      │   │
│   │  • Pluggable Harness Support:                    │   │    deltas to avoid redundant │   │
│   │    - AGY, Claude Code, Codex, or OpenCode        │   │    calls; flags blockers     │   │
│   │    - Dynamic subagents for custom tasks & ops    │   │    and completions.          │   │
│   │  • 3.0s Yield Timeout: ExecutionHandle(running)  │   │                              │   │
│   │    ensures liaison loop is never starved         │   │  • THE NOSE (Sentinel)       │   │
│   │  • Stream Normalization: Automatic filtering     │   │    Periodic health check     │   │
│   │    of ANSI escapes and terminal chrome           │   │    auditing logs, services,  │   │
│   │                                                  │   │    and machine health.       │   │
│   └──────────────────────────────────────────────────┘   └──────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Dual-Tier Memory System: Linear Short-Term & Git Wiki Long-Term

Golem separates active operational state from permanent institutional knowledge, avoiding context bloat and memory degradation over long projects:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GOLEM DUAL-TIER MEMORY ARCHITECTURE                      │
│                                                                             │
│  ┌─────────────────────────────────┐   ┌─────────────────────────────────┐  │
│  │ SHORT-TERM MEMORY (LINEAR)      │   │ LONG-TERM MEMORY (GIT WIKI)     │  │
│  │                                 │   │                                 │  │
│  │ • Active issue & subtask state  │   │ • Tier 1: Facts & node rules    │  │
│  │ • Live blockers & dependencies  │   │ • Tier 2: Capabilities & tools  │  │
│  │ • Project milestones & triage   │   │ • Tier 3: Procedural skills     │  │
│  │ • Status lifecycle transitions  │   │ • Tier 4: Reactive reflex flows │  │
│  │ • Working task context & queue  │   │ • Logbook: Daily journal entries│  │
│  └────────────────┬────────────────┘   └────────────────▲────────────────┘  │
│                   │                                     │                   │
│                   │         Conversational Review       │                   │
│                   └────────► (/learn & Review Cards) ───┘                   │
│                              Crystallizes into Wiki                         │
│└────────────────────────────────────────────────────────────────────────────┘
```

### 1. Short-Term Memory: Linear Operational Tracking
- **Live Task & Milestone State:** Active issues, child subtasks, milestones, and blockers are tracked natively in Linear.
- **Dynamic Context Management:** When managing complex workflows, Golem surfaces active child issues under settled parents, tracks dependencies, updates statuses, and links follow-up issues automatically.
- **Zero Hallucinated Progress:** Task deliverables reflect verified system states and confirmed issue transitions rather than ephemeral chat memory.

### 2. Long-Term Memory: Git-Backed Institutional Wiki (`~/.golem/wiki/`)
Institutional knowledge is codified into a structured, version-controlled Git wiki rather than an unstructured append-only log:

```
~/.golem/wiki/
├── .wiki.lock                          # POSIX flock descriptor
├── context/                            # Tier 1 (Facts) & Tier 2 (Capabilities)
│   ├── rules/
│   │   ├── primary-node.md             # Tier 1: Host invariants, OS permissions
│   │   ├── compute-node.md             # Tier 1: Hardware specs, compute paths
│   │   └── cloud-node.md               # Tier 1: Cloud sandbox rules, open ports
│   ├── topology.md                     # Tier 1: Fleet-wide IPs, mesh endpoints
│   └── capabilities.md                 # Tier 2: Available tools, CLIs, packages
├── runbooks/                           # Tier 3 (Skills) & Tier 4 (Flows)
│   ├── skills/                         # Tier 3: Procedural execution recipes
│   │   └── computer_use/
│   │       ├── SKILL.md                # CLI flags, schemas, instructions
│   │       └── harness.sh              # Script harness for backstage Hands
│   └── flows/                          # Tier 4: Reactive reflexes & chains
│       ├── oauth_token_refresh.md      # Trigger: Eyes matches OAuth -> reflex
│       └── service_health_recovery.md  # Trigger: Nose detects service failure
└── logbook/                            # Chronological Operational Journal
    ├── legacy_ledger.md                # Ingested historical events from migration
    └── YYYY-MM-DD.md                   # Instant-commit daily journal entries
```

- **Tier 1 (Facts):** Machine-specific invariants, hardware profiles, OS permissions, and network topology.
- **Tier 2 (Capabilities):** Available CLIs, package affordances, library versions, and installed binaries.
- **Tier 3 (Skills):** Reusable procedural execution recipes, flag schemas, and script harnesses (`SKILL.md`).
- **Tier 4 (Flows):** Reactive event-driven reflexes, event bus triggers, and automated chains.
- **Logbook:** Instant-commit chronological operational notes and retrospectives.
- **Multi-File Atomic Staging (`/learn`):** Operational improvements trigger interactive review cards in chat with unified diff previews. Replying `confirm` triggers atomic multi-file replacement, a Git commit, and cache synchronization.

---

## Infinite Hiring Powers: Dynamic Crewmate Orchestration

As your Digital Chief of Staff, Golem holds **infinite hiring powers**—the ability to dynamically provision, allocate, and orchestrate specialized worker subagents across your machine mesh without human overhead:

- **The General Contractor:** You speak only to Golem. Golem assesses the job, designs the execution plan, and "hires" the exact specialist subagents needed.
- **Cross-Machine Fleet Dispatch:** Powered by Herdr, Golem dispatches worker tasks across all connected machines in your network mesh (local workstations, remote servers, GPU compute boxes, and cloud instances).
- **Pluggable Harness Support:** Built with Antigravity (`agy`) out of the box for fast builds and tests, with modular support for alternative agent harnesses such as Claude Code, Codex, or OpenCode.
- **On-Demand Specialist Workers:** Dynamically spawned workers for database migrations, Docker management, refactoring in isolated Git worktrees, or recurring maintenance.
- **Executive Synthesis:** Crewmates work in isolated background terminal panes, stream progress to the Sensory Plane, and report results to Golem. Golem synthesizes raw output into clear, actionable executive briefings.

---

## Observability WebUI

Golem includes a real-time dashboard built on `aiohttp.web` running on port `8765`:

- **CLI Flag:** `golem --webUI` (or `python3 app.py --webUI`)
- **Environment Toggle:** `GOLEM_ENABLE_WEBUI=1` (or `GOLEM_WEBUI=1`)
- **URL:** [`http://127.0.0.1:8765`](http://127.0.0.1:8765)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│               GOLEM OBSERVABILITY COMMAND CENTER (PORT 8765)                │
├───────────────────────────────┬─────────────────────────────────────────────┤
│ 👁️ EYE AGENT PANE             │ 👃 NOSE AGENT PANE                           │
│ • Ingress event monitor       │ • Background health check telemetry         │
│ • Runner buffer activity      │ • Container, disk, and network status       │
│ • Interactive prompt alerts   │ • Anomaly warning logs                      │
├───────────────────────────────┼─────────────────────────────────────────────┤
│ 👄 MOUTH AGENT PANE            │ ✋ DYNAMIC HAND GRID (PANES 0–9)             │
│ • Liaison conversation stream │ • Hand 0: AGY Runner (Pane 2 stream)        │
│ • Prompt trace logs           │ • Hand 1: Worker (Git worktree sandbox)     │
│ • Outbound message drafts     │ • Hand 2: Service Worker (Sandbox task)     │
│ • File upload activity        │ • Dynamic worker allocation display         │
└───────────────────────────────┴─────────────────────────────────────────────┘
```

- **Event Streaming:** Real-time log updates are served via Server-Sent Events (`/events`) without interrupting chat listeners.
- **Status Endpoint:** Machine health and sensory state can be queried via `GET /api/status`.
- **UI Architecture:** Self-contained HTML5/CSS interface with zero external CDN dependencies.

---

## Technical Foundations: Background Execution Made Simple

Most AI terminal assistants take a naive approach: they run commands in a standard shell, try to scrape characters off the screen, and dump raw output back into the model prompt. This often causes chats to freeze during long commands, clutters conversations with messy terminal color codes, and wastes thousands of tokens re-reading static screens.

Golem uses **Herdr** to run terminal processes cleanly in the background with clear advantages:

- **No Frozen Chats:** If a command takes longer than 3 seconds or prompts for user input (`[y/N]` confirmations, login links), Golem yields a background task handle so your conversation continues without interruption.
- **Clean, Intelligible Output:** Terminal color codes, progress spinners, and screen formatting artifacts are automatically stripped and cleaned up before reaching the LLM or chat interface.
- **Zero Wasted Tokens:** Golem's Eyes monitor background work by hashing terminal changes. When commands run quietly, zero LLM tokens are consumed.
- **Cross-Machine Fleet Mesh:** Run commands and manage services across your local laptop, homelab servers, GPU boxes, and cloud instances over a private encrypted network mesh (such as Tailscale or WireGuard) without fragile SSH scripts.

---

## Quickstart Guide

> [!TIP]
> **Setting up with an AI coding assistant?**
> If you are setting up Golem with an AI coding assistant (like Antigravity / AGY, Claude Code, or Codex), see [`SETUP.md`](SETUP.md) for a step-by-step conversational setup guide that walks you through prerequisites, fleet discovery, model configuration, and service installation.

### Prerequisites
- **Python:** Python 3.11, 3.12, or 3.13 (Python 3.11+ required).
- **Herdr:** Daemon installed and running (`herdr daemon status`).
- **Network Mesh:** Encrypted network connection between fleet machines (e.g. Tailscale or WireGuard).

### 1. Installation
Clone the repository and install Golem in editable mode:

```bash
# Clone the repository
git clone https://github.com/chorned/golem.git golem
cd golem

# Set up a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install the base package
pip install -e .

# Or install with dev & test dependencies
pip install -e ".[dev]"
```

Installed entrypoints:
- `golem`: Primary CLI binary (`golem --help`, `golem --webUI`)

### 2. Configuration
Copy the template configuration file:

```bash
cp .env.example .env
```

Configure your environment variables in `.env`:
```bash
# Chat Ingress (Slack Socket Mode)
SLACK_APP_TOKEN=xapp-your-app-token-here
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
SLACK_SIGNING_SECRET=your-signing-secret-here

# Primary LLM API (Google Gemini)
GEMINI_API_KEY=your-gemini-api-key-here

# Model Selection
MOUTH_MODEL=gemini-3.8-flash
EYES_MODEL=gemini-flash-lite-latest
NOSE_MODEL=gemini-flash-lite-latest

# Worker Execution Engine
GOLEM_ENGINE=antigravity
GOLEM_DEFAULT_HAND_WORKER=agy
ANTIGRAVITY_MODEL=gemini-3.8-flash-high

# WebUI Observability Dashboard
GOLEM_ENABLE_WEBUI=0
```

### 3. Running the Gateway
Start the service from your terminal (make sure your `.venv` is activated):

```bash
# Launch with the WebUI enabled
golem --webUI

# Or launch directly via Python
python3 app.py --webUI
```

### 4. Running as a Background Service (Optional)

Running Golem as an operating system service is completely optional—running `golem --webUI` in your terminal or a `tmux` session is often the easiest way to run it. If you want an always-on background daemon:

#### On Linux (systemd)
Create a user service file at `~/.config/systemd/user/golem.service`:

```ini
[Unit]
Description=Golem Autonomous Fleet Chief of Staff
After=network.target

[Service]
Type=simple
WorkingDirectory=%h/golem
ExecStart=%h/golem/.venv/bin/golem --webUI
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
```

Enable and start the service:
```bash
systemctl --user daemon-reload
systemctl --user enable --now golem
```

#### On macOS (launchd)

> [!NOTE]
> `com.user.golem.plist` is a template file. Make sure to edit the paths (e.g. `/Users/hermes/projects/golem`) and environment tokens (`YOUR_SLACK_APP_TOKEN`, `YOUR_GEMINI_API_KEY`) to match your system before loading.

```bash
# Copy and customize the launchd definition
cp com.user.golem.plist ~/Library/LaunchAgents/

# Load and start the service
launchctl load ~/Library/LaunchAgents/com.user.golem.plist
launchctl kickstart -k -p gui/$(id -u)/com.user.golem
```

---

## Test Suite

The test suite validates unit logic, system boundaries, and end-to-end integration across sensory organs and PTY runners:

```bash
# Run repository hygiene and CI checks
python3 -m unittest tests/test_ci_and_repo_hygiene.py

# Run Tier 1 feature verification
python3 -m unittest tests/e2e/test_e2e_tier1_features.py

# Discover and run the full test suite
python3 -m unittest discover tests
```

---

## Notable Open Source Projects

Golem stands on the shoulders of notable open-source projects:

- **[Herdr](https://github.com/kunchenguid/firstmate)** — Distributed pseudo-terminal (PTY) multiplexer and mesh coordinator that powers Golem's resilient, cross-machine execution hands.
- **[First Mate](https://github.com/kunchenguid/firstmate)** — The original personal assistant framework created by Kun Chen ([`kunchenguid/firstmate`](https://github.com/kunchenguid/firstmate)), from which Golem evolved.
- **[BlueBubbles](https://bluebubbles.app/)** — Open-source iMessage ecosystem bridge providing Apple tapback reactions, typing indicators, and message routing.

### Documentation & Resources
- **Operating Guidelines:** [`AGENTS.md`](AGENTS.md)
- **Setup Guide:** [`SETUP.md`](SETUP.md)
- **Contributing:** [`CONTRIBUTING.md`](CONTRIBUTING.md)
- **License:** [Apache License, Version 2.0](LICENSE)
