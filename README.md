# Golem: Autonomous Fleet Chief of Staff

[![CI](https://github.com/kunchenguid/firstmate/actions/workflows/ci.yml/badge.svg)](https://github.com/kunchenguid/firstmate/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Lineage](https://img.shields.io/badge/lineage-first--mate-purple.svg)](#project-lineage)

> **"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."**

**Golem** is an open-source coordinator and personal Chief of Staff designed to run across multiple machines. It pairs a conversational liaison (accessible via Slack and BlueBubbles) with a headless, distributed pseudo-terminal (PTY) execution grid that spans local machines, homelab servers, GPU workstations, and cloud VPS instances.

---

## Design Motivation: The Pitfalls of Raw Terminal Scraping

Many terminal-agent setups take a straightforward approach: spin up a subshell, scrape visible screen characters or periodically dump the terminal buffer, and feed the raw output back into an LLM prompt.

In real-world multi-node workflows, directly scraping interactive PTYs introduces several practical issues:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    NAIVE PTY SCRAPING (FAILURE MODES)                       │
│                                                                             │
│  [User Chat] ───► [Single Agent Loop] ───► [Blocking Subprocess / PTY]      │
│                            │                         │                      │
│               Context Bloat│  Visual Scraping        ▼                      │
│               from Scraped │  Desync & ANSI Debris  [Corrupted Terminal]    │
│               Buffers      ▼                         │                      │
│                [Raw Full-Screen Frames] ─────────────┘                      │
│                (24x80 cells x many turns = High Latency & Deadlocks)        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1. Visual Buffer Scraping Desynchronization
Polling a rendered character matrix at arbitrary intervals frequently misses transient output from fast-moving commands (compilers, test runners, or build scripts). The agent ends up evaluating stale snapshots rather than the actual sequence of events or specific exit transitions.

### 2. ANSI Color and VT100 Control Code Corruption
Interactive terminal applications emit raw ANSI color sequences (`\x1b[38;2;...m`), cursor repositioning instructions (`\x1b[H`, `\x1b[2J`), alternate screen buffer triggers (`smcup`/`rmcup`), and box-drawing glyphs. Feeding this uncleaned stream to an LLM adds unnecessary token overhead, degrades attention, and can interfere with strict JSON parsing during tool calls.

### 3. Artificial Screen Line Wrapping Artifacts
Fixed-width virtual terminals (typically 80 or 120 columns) force trailing line breaks on long strings. URLs, base64 strings, file paths, and compiler flags get split across lines with trailing whitespace or wrapping markers, making reliable path extraction and link resolution harder than it needs to be.

### 4. Blocking Subshell Race Conditions
When a CLI tool prompts for input (`[y/N]`, a confirmation prompt, or an OAuth flow), an agent blocked on synchronous pipe reads will stall the entire runloop. If the main thread is tied up waiting for stdout, inbound user messages (like checking status or requesting a cancel) get delayed or dropped.

### 5. Catastrophic LLM Token Starvation
Re-ingesting full visual terminal screens (around 2,000 characters per standard 24×80 frame) on every turn inflates conversational history with static headers, curses borders, and shell prompts, steadily eating into available reasoning context over long sessions.

---

## Architecture: Decoupled Sensory Model

To solve these constraints, Golem separates high-level conversational flow from backend terminal execution, using background watchers to keep track of state:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                        GOLEM TRIPARTITE SENSORY ARCHITECTURE                                │
│                                                                                             │
│   Slack / BlueBubbles Ingress                                                               │
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
│   │  • Default Engine: Local AGY CLI (worker="agy")  │◄──┤    Inspects active buffer    │   │
│   │  • Pluggable Hand Routing:                       │   │    every 15–30s; hashes      │   │
│   │    - opencode: Isolated git worktree sandbox     │   │    deltas to avoid redundant │   │
│   │    - hermes: Homelab Docker & system maintenance │   │    calls; flags blockers     │   │
│   │  • 3.0s Yield Timeout: ExecutionHandle(running)  │   │    and completions.          │   │
│   │    ensures liaison loop is never starved         │   │                              │   │
│   │  • Stream Normalization: Automatic filtering     │   │  • THE NOSE (Sentinel)       │   │
│   │    of ANSI escapes and terminal chrome           │   │    Periodic health check     │   │
│   └──────────────────────────────────────────────────┘   │    auditing logs, services,  │   │
│                                                          │    and node reachability.    │   │
│                                                          └──────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

1. **Front-of-House Mouth (Liaison):** Runs in Pane 1 (`{session_id}-liaison`). Communicates directly with the user via Slack or BlueBubbles. Because it does not run terminal processes in its own thread, questions asked while long jobs are running are answered promptly.
2. **Backstage Hands (Worker Grid):** Operates in Pane 2 (`runner`) and dedicated subagent sandboxes. Runs commands headlessly—using the local AGY CLI by default (`GOLEM_DEFAULT_HAND_WORKER=agy`)—and yields execution handles after 3 seconds so the main conversation remains interactive. Output is normalized and stripped of ANSI noise before logging or handoff.
3. **Sensory Plane (The Eyes & The Nose):**
   - **The Eyes (`EyesTaskObserver`):** Checks running terminal buffers on a 15–30 second interval. It uses SHA-256 delta hashing to skip LLM calls entirely if output is unchanged, pinging the Mouth only when user action is required (such as an auth prompt) or when a task finishes.
   - **The Nose (`NoseSentinel`):** A lightweight background check that runs every 15 minutes to review error logs, container status, and machine reachability, alerting the Mouth via the `ShoulderTapEventBus` if an anomaly is detected.

---

## Fleet Topology

Golem coordinates a multi-node fleet linked over an encrypted Tailscale network using Herdr:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              GOLEM FLEET MACHINE TOPOLOGY                                   │
│                                                                                             │
│                 ┌─────────────────────────────────────────────────────────┐                 │
│                 │               fleet-control (localhost)                 │                 │
│                 │   macOS Anchor (Always-on local daemon)                 │                 │
│                 │   • Headless AGY CLI & Golem Ingress Daemon             │                 │
│                 │   • Real-Time WebUI (http://127.0.0.1:8765)             │                 │
│                 │   • Git worktrees, tests, and build packaging           │                 │
│                 └────────────────────────────┬────────────────────────────┘                 │
│                                              │ Tailscale Mesh                               │
│                        ┌─────────────────────┼─────────────────────┐                        │
│                        ▼                     ▼                     ▼                        │
│             ┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐            │
│             │    homelab-ops     │ │    compute-grid    │ │      dev-grid      │            │
│             │     (hermes)       │ │    (desktop-pc)    │ │ (personal-laptop)  │            │
│             │  • Docker & ZFS    │ │  • Windows + CUDA  │ │  • Active Laptop   │            │
│             │  • Media streaming │ │  • PyTorch & GPUs  │ │  • Ad-hoc only     │            │
│             │  • Storage/Backups │ │  • Model workloads │ │  • Powers down     │            │
│             └────────────────────┘ └────────────────────┘ └────────────────────┘            │
│                                              │                                              │
│                                              ▼                                              │
│                                    ┌────────────────────┐                                   │
│                                    │     cloud-ops      │                                   │
│                                    │   (oci-machine)    │                                   │
│                                    │  • Oracle VPS      │                                   │
│                                    │  • Public Ingress  │                                   │
│                                    │  • WireGuard & SSL │                                   │
│                                    └────────────────────┘                                   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

| Node / Target | Host Address | Role & Workload | Execution Engine | Operating Notes |
| :--- | :--- | :--- | :--- | :--- |
| **`fleet-control`** | `localhost` | **Primary Anchor:** Local orchestration hub, repo management, test execution, and WebUI host. | Headless `agy` CLI | Primary local node; kept running continuously. |
| **`homelab-ops`** | `localhost` (`hermes`) | **Homelab Operations:** Docker containers, storage pools, media services, and system maintenance. | Headless `hermes -z` | Sandboxed service management. |
| **`compute-grid`** | `desktop-pc` (`100.80.59.45`) | **GPU Compute (Windows):** NVIDIA CUDA workloads, model training, and quantization tasks. | Remote SSH + `cmd.exe /c` | POSIX command wrapping for Windows environments. |
| **`dev-grid`** | `personal-laptop` (`100.96.38.5`) | **Workstation:** Daily development machine. | Remote SSH (ad-hoc only) | **Ad-hoc use only:** Machine frequently sleeps; no persistent background services. |
| **`cloud-ops`** | `oci-machine` (`100.113.173.33`) | **Cloud VPS (Linux aarch64):** Production server for ingress routing, WireGuard endpoints, and DNS. | Remote SSH | Reserved for production services and infrastructure tasks. |

---

## Observability WebUI

Golem includes a local dashboard built on `aiohttp.web` running on port `8765`:

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
│ • Prompt trace logs           │ • Hand 1: opencode (Git worktree sandbox)   │
│ • Outbound message drafts     │ • Hand 2: hermes (Homelab ops container)    │
│ • File upload activity        │ • Dynamic worker allocation display         │
└───────────────────────────────┴─────────────────────────────────────────────┘
```

- **Event Streaming:** Real-time log updates are served via Server-Sent Events (`/events`) without interrupting chat listeners.
- **Status Endpoint:** Machine health and sensory state can be queried via `GET /api/status`.
- **UI Fallback:** Defaults to a clean, self-contained HTML5/CSS interface with zero external CDN dependencies, while supporting custom design templates when present.

---

## Backwards Compatibility: Subpackage Facades

To support existing automation scripts, test suites, and workflows, Golem provides compatibility facades in subpackages:

```python
# Both import paths resolve to the exact same class object:
import golem.ingress
import golem.gateway.ingress

assert golem.ingress.IngressGateway is golem.gateway.ingress.IngressGateway
```

1. **Flattened Modules:** Core modules reside directly under `golem/` (`golem.ingress`, `golem.mouth`, `golem.harness`, `golem.bus`, etc.).
2. **Mock Redirection:** Tests patching paths such as `unittest.mock.patch("golem.gateway.ingress.call_gemini_api")` correctly update the underlying `golem` module.
3. **Compatibility Facades:** Facades in subpackages re-export symbols using `from golem.<module> import *`, ensuring type checkers (mypy, pyright) and editor autocompletion work without configuration changes.

---

## Quickstart Guide

### Prerequisites
- **Python:** Python 3.11, 3.12, or 3.13 (Python 3.11+ required).
- **Herdr:** Daemon installed and running (`herdr daemon status`).
- **Tailscale:** Active connection between fleet machines.

### 1. Installation
Clone the repository and install Golem in editable mode:

```bash
# Clone the repository
git clone https://github.com/kunchenguid/firstmate.git
cd golem

# Set up a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install the base package
pip install -e .

# Or install with test dependencies
pip install -e ".[dev]"
```

Installed entrypoints:
- `golem`: Primary CLI binary (`golem --help`, `golem --webUI`)

### 2. Configuration
Copy the template configuration file:

```bash
cp .env.example .env
```

Add your environment variables to `.env`:
```bash
# Slack Socket Mode
SLACK_APP_TOKEN=xapp-your-app-token-here
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
SLACK_SIGNING_SECRET=your-signing-secret-here

# Google Gemini API
GEMINI_API_KEY=your-gemini-api-key-here

# Model Selection
MOUTH_MODEL=gemini-3.8-flash
EYES_MODEL=gemini-flash-lite-latest
NOSE_MODEL=gemini-flash-lite-latest

# Worker Execution
GOLEM_ENGINE=antigravity
GOLEM_DEFAULT_HAND_WORKER=agy
ANTIGRAVITY_MODEL=gemini-3.8-flash-high
```

### 3. Running the Gateway
Start the service from your terminal:

```bash
# Launch with the WebUI enabled
golem --webUI

# Or launch directly via Python
python3 app.py --webUI
```

### 4. Running as a macOS `launchd` Service
To run Golem as a background daemon on macOS:

```bash
# Copy the launchd definition
cp com.user.golem.plist ~/Library/LaunchAgents/

# Load the service
launchctl load ~/Library/LaunchAgents/com.user.golem.plist

# Start or restart the service
launchctl kickstart -k -p gui/$(id -u)/com.user.golem
```

---

## Test Suite

The test suite covers unit logic, system boundaries, and end-to-end integration across the sensory plane, PTY runners, and compatibility shims:

```bash
# Run documentation and operating manual validation
python3 -m unittest tests/test_documentation_and_manuals.py

# Run repository hygiene and CI checks
python3 -m unittest tests/test_ci_and_repo_hygiene.py

# Run Tier 1 feature verification
python3 -m unittest tests/e2e/test_e2e_tier1_features.py

# Discover and run the full test suite
python3 -m unittest discover tests
```

---

## Project Lineage

Golem is the open-source continuation of **First Mate**, powered by Herdr's distributed PTY mesh.
First Mate was originally created by Carl ([`kunchenguid/firstmate`](https://github.com/kunchenguid/firstmate)) as a personal assistant framework. Golem refactors that foundation into a generalized coordinator designed for multi-machine task execution and fleet management.

- **Operating Guidelines:** [`AGENTS.md`](AGENTS.md)
- **Contributing:** [`CONTRIBUTING.md`](CONTRIBUTING.md)
- **License:** [Apache License, Version 2.0](LICENSE)