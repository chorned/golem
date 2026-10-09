# Golem 1.0 Universal Task Queue (Embodied Actor-Auditor Protocol)

> **Protocol:** Embodied Actor-Auditor (Mouth / Eye / Hand A / Hand B)  
> **Balance:** 50% Software Building | 50% Task Execution  
> **Single Source of Truth:** `.specs/<task_id>.md` and local workspace filesystem  
> **Lifecycle:** `[TODO]` ──► `[IN_PROGRESS: Hand A]` ──► `[IN_AUDIT: Hand B]` ──► `[VERIFIED]` / `[FAILED]`

---

## Task Board

| Task ID | Type | Risk Tier | Hand A (Primary Actor) Scope | Hand B (Adversarial Auditor) Invariant | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `M1-BUILD` | `build` | `medium` | `golem/`, `pyproject.toml`, `setup.py` | 1,326 unit tests pass, exact object identity (`assertIs`), zero broken imports | `[VERIFIED]` |
| `M1-EXEC` | `execution` | `high` | `com.user.golem.plist`, shim verification | Launchd service syntax valid, env vars (`GOLEM_*`) functional | `[VERIFIED]` |
| `M2-BUILD` | `build` | `medium` | `golem/execution/`, `herdr_tools.py` | AGY CLI default worker, pluggable `opencode`/`hermes` routing via param and env var | `[VERIFIED]` |
| `M2-EXEC` | `execution` | `medium` | Headless PTY worker subshell dispatch scripts | Zero PTY deadlocks under concurrent multi-pane subshells, exit code fidelity | `[VERIFIED]` |
| `M3-BUILD` | `build` | `medium` | `golem/webui/`, `golem/sensory/`, dashboard assets | Async WebUI server initializes, Stitch styling with semantic HTML/CSS fallback, SSE feeds | `[VERIFIED]` |
| `M3-EXEC` | `execution` | `high` | Port 8765 socket binding, daemon runtime | Port 8765 binds cleanly under `GOLEM_ENABLE_WEBUI=1`, zero event-loop starvation of Slack/BlueBubbles | `[VERIFIED]` |
| `M4-BUILD` | `build` | `low` | `.github/workflows/ci.yml`, `LICENSE`, `CONTRIBUTING.md` | Multi-version CI matrix (Python 3.11, 3.12, 3.13), open-source assets complete | `[VERIFIED]` |
| `M4-EXEC` | `execution` | `low` | CI syntax verification, automated secret scanning script | Zero credentials in repo, all linters and tests exit code 0 | `[VERIFIED]` |
| `M5-BUILD` | `build` | `low` | `README.md`, `AGENTS.md` | Architectural flex thoroughly documented, ASCII topology diagrams, lineage invariant verified | `[VERIFIED]` |
| `M6-EXEC` | `execution` | `high` | `tests/e2e/`, daemon kickstart verification | 100% of 179 E2E tests pass, daemon kickstarted cleanly with exit code 0, Hand B clean-room signoff | `[VERIFIED]` |
| `R1-BUILD` | `build` | `medium` | `golem/gateway/ingress.py`, `tests/` | Step 2 schema fix, fallback briefings removed, fail-loud errors with :boom:, thread rehydration without mutation, sensory commands routed | `[VERIFIED]` |
| `R2-BUILD` | `build` | `medium` | `golem/webui/server.py`, `golem/sensory/bus.py`, `golem/gateway/ingress.py`, `app.py` | Headless mode safe, TextDeltaEvent streamed, active hands settled, mouth briefing broadcast, event bus thread-safe | `[VERIFIED]` |
| `R3-BUILD` | `build` | `medium` | `golem/webui/templates/dashboard.html`, `golem/webui/server.py`, `golem/config.py`, `tests/` | Mock fixtures purged, dynamic hands lifecycle & standby card, nose deduplication, config & log caching, event loop latency < 5ms, 100% test pass | `[VERIFIED: Iteration 5]` |
| `R4-BUILD` | `build` | `medium` | `golem/sensory/eyes.py`, `golem/execution/runner.py`, `tests/` | Structured lifecycle log markers (`Dispatched`, `exceeded 3.0s yield window`, `settled`), dynamic EyesTaskObserver attachment on runner yield, sensory_bus & webui_server wiring, clean teardown, yield ceiling <= 3.15s preserved, 100% test pass | `[VERIFIED: Iteration 1]` |

---

## Log Streams (`.logs/<task_id>.log`)
- `M1-BUILD`: `.logs/M1-BUILD.log`
- `M1-EXEC`: `.logs/M1-EXEC.log`
- `M2-BUILD`: `.logs/M2-BUILD.log`
- `M2-EXEC`: `.logs/M2-EXEC.log`
- `M3-BUILD`: `.logs/M3-BUILD.log`
- `M3-EXEC`: `.logs/M3-EXEC.log`
- `M4-BUILD`: `.logs/M4-BUILD.log`
- `M4-EXEC`: `.logs/M4-EXEC.log`
- `M5-BUILD`: `.logs/M5-BUILD.log`
- `M6-EXEC`: `.logs/M6-EXEC.log`
- `R1-BUILD`: `.logs/R1-BUILD.log`
- `R2-BUILD`: `.logs/R2-BUILD.log`
- `R3-BUILD`: `.logs/R3-BUILD.log`
- `R4-BUILD`: `.logs/R4-BUILD.log`
