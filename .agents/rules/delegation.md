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
