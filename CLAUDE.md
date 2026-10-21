# CLAUDE.md — Claude Code Operational Guide

> **"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."**

Welcome, Claude Code! This repository contains **Golem**, an autonomous, distributed Chief of Staff agent powered by Herdr's PTY mesh.

---

## First-Time Setup & Onboarding Instruction

> [!IMPORTANT]
> **If the user is setting up this repository for the first time (or `.env` does not exist), follow the interactive interview in `SETUP.md` step-by-step.**
> Conduct the interview conversationally, asking questions one section at a time as outlined in [`SETUP.md`](SETUP.md).

---

## Architectural Summary

Golem decouples conversational front-of-house interactions from backstage execution and sensory monitoring:

1. **The Mouth (Front-of-House Liaison):**
   - Operates in Pane 1 (`{session_id}-liaison`).
   - The sole speaker to the operator in Slack/BlueBubbles.
   - Always responsive; dispatches backstage work via `dispatch_task`, queries sensory state via `check_eyes` and `check_nose`, and synthesizes clean executive briefings.

2. **The Hands (Backstage Runner):**
   - Operates in Pane 2 (`runner`) and dynamic subagent panes (`Pane 3..N`).
   - Executes commands, builds, refactors, and test suites across fleet nodes using Antigravity (`agy`) by default, with modular support for alternative harnesses like Claude Code, Codex, or OpenCode.
   - Long-running commands (> 3.0s) yield non-blocking `ExecutionHandle(status="running")`.

3. **The Eyes (Task Observer):**
   - Non-blocking coroutine observing runner terminal buffers every 15–30s with SHA-256 buffer delta hashing.
   - Emits blocker/completion alerts across `ShoulderTapEventBus`.

4. **The Nose (Fleet Health Sentinel):**
   - 24/7 background watchdog sniffing log tails and telemetry every 15m.
   - Zero-regex quiescent gate emitting anomaly alerts across `ShoulderTapEventBus`.

5. **Herdr Mesh Machinery:**
   - Herdr daemon managing persistent PTY sessions, multi-pane workspaces, and multi-node mesh networking.

6. **Git-Backed Memory Wiki (`~/.golem/wiki/`):**
   - Institutional memory storing runbooks (`runbooks/`), rules (`context/rules/`), and logbook history (`logbook/`) with automated git commits.

---

## Development & Test Commands

- **Run all unit tests:**
  ```bash
  python3 -m unittest discover tests
  ```

- **Run CI and repository hygiene tests:**
  ```bash
  python3 -m unittest tests/test_ci_and_repo_hygiene.py
  ```

- **Run wiki and memory tests:**
  ```bash
  python3 -m unittest tests/test_wiki_engine.py tests/test_stage6_wiki.py tests/test_memory_tiers.py
  ```

- **Run E2E test suite:**
  ```bash
  python3 -m unittest discover tests/e2e
  ```

---

## Key Invariants

- **Lineage Invariant:** Every critical document, CI workflow, and test header preserves:
  `Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.`
- **Dual-Layer Compatibility Shim:** `firstmate` module acts as a strict passthrough alias to `golem`.
- **Zero Raw Terminal Dumps:** Synthesize high-signal executive briefings; never dump raw escape codes, ANSI sequences, or curses chrome to the user.
