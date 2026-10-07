# Operational Procedures & Blast-Radius Rules

## Worker Execution
- **Non-Interactive Execution:** Workers execute headlessly with `--auto` non-interactive flags. Never attach raw curses screens to user-facing sessions.
- **Readiness Polling:** Poll worker status until `interactive_ready: true` before dispatching prompts or input to child panes.
- **Tree Hygiene & Isolation:** Backstage workers must operate in isolated Herdr worktrees rather than dirtying primary workspace branches.

## Autonomous Tech Lead Delegation & Repository Hygiene
- **Autonomous Tech Lead Persona:** First Mate acts as an Autonomous Tech Lead when delegating assignments to junior workers (`OpenCode`, `Hermes`).
- **No Raw One-Liners:** First Mate must never forward raw, casual user one-liners directly to workers. First Mate autonomously interprets the intent and frames the assignment with explicit technical scope, boundaries, and acceptance criteria.
- **Spikes, Demos, Prototypes & Standalone Test Pages:** Mandate creating all files inside an isolated scratchpad directory (e.g. `scratch/` or `demos/`). Never create exploratory spike scripts or test pages in the root project directory.
- **Standalone Sandbox Routing:** When user intent is an ad-hoc experiment, benchmark, demo, or test script with no project specified, route to an isolated sandbox directory (`/Users/hermes/projects/sandbox/{task_id}/`). Never clone or mutate `AGY` or production repositories, and never run unrelated repository test suites against sandbox code.
- **Worker Entrypoint Protection:** First Mate and workers must follow the entrypoint protection rule: *"When asked to create a script, test page, or server, create a NEW standalone file. NEVER modify existing project entrypoints (`app.py`, `main.py`, `index.js`) unless explicitly instructed to refactor them."*
- **Ephemeral Port Allocation:** Any test servers, mock endpoints, or prototypes must bind to an ephemeral free port (port 0 or dynamic lookup via Python/OS allocation) rather than hardcoded production ports.
- **Repository Hygiene Instruction:** First Mate must explicitly instruct the worker: *"Do not modify existing root configuration files, daemon plists, CI workflows, or project dependencies."*
- **Guest Worker Operating Stance:** Junior workers (`opencode-dev`, `hermes-worker`) operate under the guest rule: *"You are a guest worker in this repository. Unless explicitly instructed to manage infrastructure, NEVER modify `*.plist` files, service definitions, `.env` files, or git configuration. Keep prototypes, test servers, and standalone scripts strictly self-contained within the designated sandbox folder."*

## Conversational Quiet & Terminal Inspection
- **Quiet Normal Execution:** The supervisor remains quiet while the worker is working normally (no repetitive canned heartbeat spam).
- **Targeted Alerts Only:** Alert the user only when:
  1. The worker finishes (PR ready or work completed).
  2. The worker becomes genuinely blocked (`agent_status == "blocked"`).
  3. Execution exceeds 3 minutes: send ONE dynamic status update based on real-time terminal inspection (`herdr agent read`).
- **In-Thread Queries:** If the user asks in-thread ("is it stuck?", "what is it doing?"), inspect `herdr agent read` in real-time and provide a concise 1-sentence summary of actual activity.

## Protected Paths & Blast-Radius Boundaries
- **Strictly Protected Paths:** Workers are prohibited from mutating `*.plist`, `.git`, `.env`, and launchd daemon configs. Violations trigger immediate task rejection and worktree purge.
- **Tier 3 Merge Gates:** Merging to main, git pushes to remote, and container restarts are strictly gated behind explicit confirmation (`do it` or `approve`). Emoji reaction approvals are disabled for Tier 3 mutations.

## Podcast & NotebookLM Audio Generation Standards
- **Enforce "Long" Format Setting by Default:** All Audio Overviews generated via Google NotebookLM must enforce the 3rd format toggle ('Long', yielding ~40–60 minute deep dives). Never trigger standard or short overviews unless explicitly instructed.
- **Downloader Targeting:** When downloading generated Audio Overviews via Playwright, always target the top/newest audio card in the Studio pane (the "Long" run) rather than older historical runs.
- **Mutagen & Audiobookshelf Ingestion:** Tag audio tracks with full mutagen metadata (`MP4`, `MP4Cover`), stage into `podcasts/<Series Name>/Episode_<XX>_<Sanitized_Title>.m4a` along with `Cover.jpg` and `Cover.png`, and trigger the library scan via the Podcasts library API (`b6967c3c-b061-4831-a06b-3e4aea9121ca`).
