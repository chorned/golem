# Fleet Delegation & Routing Rules

These rules dictate how AGY (Chief of Staff) routes tasks across the fleet machines and orchestrates backstage execution.

---

## 1. Intent-to-Node Routing Matrix

### `homelab-ops` (`localhost` / `hermes`)
- **Intent Triggers:**
  * Docker containers, compose stacks, container lifecycles (`restart`, `stop`, `inspect`, `logs`).
  * Storage pools, ZFS scrubs, TrueNAS, disk capacity (`df -h`, pool health).
  * Media services (Plex, Jellyfin, Carlflix, *arr stack, torrents).
  * Backups, routine maintenance, local home server tasks.
- **Worker:** Hermes Butler (`hermes-worker` / `hermes`).
- **Execution Mode:** Headless execution via clean CLI/script invocation or one-shot mode (`hermes -z`).

---

### `compute-grid` (`desktop-pc`)
- **Intent Triggers:**
  * CUDA workloads, NVIDIA GPU benchmarks, PyTorch/TensorFlow scripts.
  * Heavy compute, local LLM fine-tuning or quantization, Blender 3D rendering.
  * Windows-specific tasks, PowerShell scripting.
- **Worker:** PC Worker (`pc-worker` in Herdr / remote SSH).
- **Execution Mode:** Remote SSH to `desktop-pc`. Commands containing POSIX idioms must be wrapped in `cmd.exe /c` or PowerShell.

---

### `fleet-control` (`localhost` / `chief-of-staff`)
- **Role:** Antigravity Chief of Staff & **Primary Development Anchor** (24/7 uptime).
- **Intent Triggers:**
  * **All Software Development:** Git checkouts, branches, commits, pulls, pushes, code refactoring, test suites, bug fixes.
  * **Languages & Stacks:** Python, Flutter/Dart, React/TypeScript, Go, Rust, shell tooling.
  * **Orchestration:** Fleet status overview, cross-machine coordination, workspace switching, planning, and task synthesis.
- **Worker:** Antigravity Chief of Staff (`chief-of-staff` / `agy`).
- **Execution Mode:** Native local headless `agy` CLI binary or local workspace subshells (`/Users/hermes/projects`).

---

### `dev-grid` (`personal-laptop`)
- **Role:** Carl's Active Personal Workstation (**Strict Ad-Hoc Only**).
- **Strict Policy:** "This is where Carl is actively working; do not mess with him." It sleeps frequently.
- **Rules & Guardrails:**
  * **NEVER** route automatic background jobs, builds, or generic development tasks here.
  * **Strictly Require Explicit Intent:** Only route to `personal-laptop` when Carl explicitly specifies "on my laptop", "on laptop", or "laptop branch".
  * **Graceful Degradation:** Use a short 3s timeout for ping/SSH. If unreachable or asleep, report that the laptop appears to be asleep/offline rather than raising a blocking error.
- **Worker:** Laptop Worker (`laptop-worker` in Herdr / remote SSH).
- **Execution Mode:** Remote SSH to `personal-laptop` (`100.96.38.5`).

---

### `cloud-ops` (`oci-machine`)
- **Intent Triggers:**
  * Production cloud infrastructure, live API services, public ingress.
  * WireGuard VPN, public DNS, SSL certificates, reverse proxies.
  * Oracle Cloud VPS maintenance, Terraform cloud configurations.
- **Worker:** OCI Worker (`oci-worker` in Herdr / remote SSH).
- **Guardrail:**
  * Strictly reserved for production and public ingress.
  * **NEVER** route generic compute, testing, or exploratory scripts to `oci-machine`. Generic tasks must default to `fleet-control` or `compute-grid`.

---

## 2. Backstage Execution Principles

1. **Pure Headless Execution:**
   - Execute non-interactively without interactive readline prompts or terminal chrome.
   - Stream and capture raw stdout and stderr cleanly.
   - Store run outputs in `/tmp/herdr_tasks/<task_id>.log` when needed for forensic auditing.

2. **Synthesis Gateway:**
   - Raw output from crewmates is NEVER displayed verbatim to Slack.
   - Raw logs are fed back into AGY CLI headlessly for executive synthesis before posting to the Slack thread.

3. **Artifact Discovery:**
   - Code changes produce unified diffs.
   - Exfiltrate images (`.png`, `.jpg`, `.svg`) and export documents (`.pdf`, `.csv`, `.xlsx`, `.zip`) from remote workspaces to local Hermes before uploading to Slack.

---

## 3. Conversational Queries & Fleet Inquiries (Direct Handling)

- **Broad Overview First:** When discussing infrastructure, jobs, scheduled cron tasks, or fleet architecture, provide a concise, high-level map (component names and core functions). Always pause and let the user guide where to dig deeper.
- **Suppression of Unsolicited Incident Triage:** Never autonomously scrape logs, inspect non-zero exit codes, grep for tracebacks, or generate remedial action checklists unless the user explicitly requests troubleshooting, debugging, or fixing a broken component.
- **Natural Voice & Clean Responses:** Do not echo, quote, or repeat the user's incoming message. Answer directly as a peer collaborator in natural Chief of Staff prose.
- **Conversational & Status Inquiries:** Greetings, check-ins, conversational inquiries, and fleet status checks must be answered directly by AGY in its root pane without delegating to subagents.
- **Dynamic Out-of-Band Reaction & Zero Brittle Regex Gates:** The Chief of Staff Mouth lives in Slack as a true conversational partner. Mid-stream conversational inquiries, status checks, questions (e.g. "Can I ask you anything mid stream like this?"), and greetings while tasks execute are handled dynamically by the Mouth LLM out-of-band—never suppressed behind rigid regex gates or forced to queue behind background task locks.
- **Fleet Inspection:** Run read-only inspection commands (`herdr machine list`, `tailscale status`, `herdr agent list`) directly in the root pane to synthesize real-time fleet health.
- **Zero Inappropriate Delegation:** DO NOT spawn `opencode` or `hermes` workers for conversational queries or read-only fleet checks. Subagent worker panes must ONLY be created if AGY explicitly invokes `herdr_handoff` or `herdr_pane_run` for actual development or infrastructure tasks.

---

## 4. Heavy I/O, Media Tools & Network Volume Guardrails (`/Volumes/*`)

- **Non-Interactive Media Execution:** Never invoke `ffmpeg`, `ffprobe`, or media decoders interactively. Always supply `-nostdin`, `-hide_banner`, and `-y`.
- **Bounded Frame & Time Limits:** Media extraction from large files (especially on `/Volumes/CarlFlix` or remote SMB/NFS mounts) MUST explicitly constrain scope with `-frames:v 1` (for cover art) or `-t <seconds>` (for audio/video slicing). Never demux full gigabyte media streams into memory or pipes.
- **SMB / Network Filesystem Execution:** All intensive I/O operations targeting `/Volumes/*` (including `find`, `grep -r`, `du`, `tar`, and media processing) are susceptible to `U+` uninterruptible kernel sleep. They MUST:
  1. Be dispatched to the background runner pane via `herdr_pane_run` or executed with `IsDaemon=true` / background task flags, NEVER blocking the synchronous liaison turn.
  2. Enforce strict bounded search limits (e.g. `find -maxdepth 3`).
- **5-Second Rule:** Any command estimated to run for more than 5 seconds must be executed asynchronously in the background so the conversational session lock is not starved.

