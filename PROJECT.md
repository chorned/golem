# Project: Golem Autonomous Fleet Chief of Staff

Lineage Invariant: *"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."*

## Architecture
Golem is structured into a decoupled tripartite architecture separating cognitive dialogue, backstage execution, and sensory monitoring:
1. **Front-of-House Mouth (Conversational Liaison)**: Operates in Pane 1 (`{session_id}-liaison`). Handles Slack Socket Mode and BlueBubbles ingress, converses with the user via `MOUTH_MODEL`, dispatches backstage tasks via Gemini function calling, and delivers formatted executive briefings. Never executes blocking subshell commands directly.
2. **Backstage Hands (Pluggable Worker Engine)**: Operates in Pane 2 (`runner`) and dynamic worker sandboxes (Pane 3..N). Executes commands, builds, and sandbox tasks. Standardized on the local AGY CLI as default (`worker: str = "agy"`, `GOLEM_DEFAULT_HAND_WORKER=agy`), with on-demand delegation hooks to `opencode` (code edits / tests) and `hermes` (homelab/Docker ops). Commands taking > 3.0s yield non-blocking `ExecutionHandle(status="running")`.
3. **Sensory Plane (The Eyes & The Nose)**:
   - **The Eyes** (`EyesTaskObserver`): Non-blocking coroutine observing terminal buffers every 15–30s with buffer change hashing (0 tokens when unchanged). Emits blocker/completion alerts over `ShoulderTapEventBus`.
   - **The Nose** (`NoseSentinel`): 24/7 background watchdog sniffing `bot.err` and fleet telemetry across nodes. Emits anomaly alerts over `ShoulderTapEventBus`.
4. **Real-Time Observability WebUI**: Embedded `aiohttp.web.AppRunner` on `http://127.0.0.1:8765` (`--webUI` / `GOLEM_ENABLE_WEBUI=1`). Streams live logs and sensory telemetry via Server-Sent Events (`/events`) and REST (`/api/status`). Renders a 4-pane dark-mode dashboard (Eye, Nose, Mouth, Hand 0-9 Grid) with Google Stitch MCP design and resilient vanilla HTML/CSS/JS fallback.
5. **Compatibility & Packaging Layer**: Canonical `golem/` package accompanied by a Dual-Layer Compatibility Shim in `first_mate/` (`sys.meta_path` alias finder + static mirror files) guaranteeing 100% object identity and bidirectional `unittest.mock.patch` fidelity. Bundled via PEP 621 `pyproject.toml` and `setup.py`.

## Feature Inventory
Every feature from the Survey phase and user requirements is inventoried below with its assigned milestone:
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | Canonical `golem/` Package | Migrate 34 files from `first_mate/` to `golem/` with internal imports updated to `golem.*` | M1 | R1 |
| 2 | Dual-Layer Compatibility Shim | `_FirstMateAliasFinder` on `sys.meta_path` + static mirror files in `first_mate/` ensuring `assertIs` identity and mock patch fidelity | M1 | R1 |
| 3 | PEP 621 Packaging & Entrypoints | Author `pyproject.toml` and `setup.py` bundling `golem*`, `first_mate*`, and `app.py` with console scripts `golem=golem.cli:main` and `first-mate=golem.cli:main` | M1 | R1 |
| 4 | Launchd Dual-Key Compatibility | Dual-key resolution (`GOLEM_*` / `FIRST_MATE_*`) in `config.py` and `app.py`, adding `com.user.golem.plist` while preserving `com.user.first-mate.plist` | M1 | R1 |
| 5 | Lineage Invariant Codification | Embed lineage invariant banner and docstrings across packages, manifests, and CLI banners | M1 | R1 |
| 6 | AGY Hand Default Standardization | Standardize `get_default_hand_worker()` to return `os.environ.get("GOLEM_DEFAULT_HAND_WORKER") or "agy"` in `golem/config.py` | M2 | R2 |
| 7 | Pluggable Worker Signatures | Update `herdr_spawn`, `herdr_handoff`, and `WorkerDelegator.handoff` with `worker: str = "agy"`, supporting legacy `subagent` parameter | M2 | R2 |
| 8 | On-Demand Worker Routing Hooks | Clean routing to `opencode` (isolated worktree) and `hermes` (homelab guest rules) when explicitly requested | M2 | R2 |
| 9 | Dual-Pane Isolation & 3.0s Yield | Enforce Pane 1 (Mouth) / Pane 2 (Hands) isolation and 3.0s yield timeout in `RunnerExecutor` | M2 | R2 |
| 10 | WebUI Async Server Launch | Support `--webUI` CLI flag and `GOLEM_ENABLE_WEBUI=1` env var, binding `aiohttp.web.AppRunner` on port 8765 without blocking Slack or BlueBubbles | M3 | R3 |
| 11 | Real-Time SSE & REST API | Expose `/events` (text/event-stream) and `/api/status` streaming live logs from Eyes, Nose, Mouth, and Hand panes | M3 | R3 |
| 12 | 4-Pane Command Center Layout | High-contrast dark-mode dashboard with Eye Pane, Nose Pane, Mouth Pane, and responsive Hand Grid (0 to 9 dynamic panes) | M3 | R3 |
| 13 | Stitch MCP + Resilient Fallback | Leverage Google Stitch MCP design assets if available; robust, self-contained vanilla HTML/CSS/JS fallback | M3 | R3 |
| 14 | GitHub Actions CI Matrix | Author `.github/workflows/ci.yml` matrix across Python 3.11, 3.12, 3.13 running unit tests, lint, and security checks | M4 | R4 |
| 15 | Open Source Repository Assets | Provide `LICENSE` (Apache-2.0), `CONTRIBUTING.md`, issue templates, PR template, and expanded `.gitignore` | M4 | R4 |
| 16 | Flagship README Architecture Flex | Author `README.md` positioning Golem as Autonomous Fleet Chief of Staff, detailing PTY scraping pitfalls vs decoupled tripartite architecture | M5 | R5 |
| 17 | AGENTS.md Pair-Programming Invariants | Update `AGENTS.md` with strict operational pair-programming rules, backstage invariants, and fleet topology | M5 | R5 |
| 18 | E2E Opaque-Box Test Suite | Dual-track comprehensive test suite covering Tiers 1-4 with test runner and publishing `TEST_READY.md` | E2E | Dual Track |
| 19 | 100% E2E Pass & Coverage Hardening | Implementation track final validation: pass 100% E2E tests + Tier 5 adversarial coverage hardening | M6 | Final Track |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| E2E | E2E Testing Track | Test infra (`tests/e2e/`), test cases (Tiers 1-4), publish `TEST_READY.md` | none | DONE |
| M1 | Package Namespace Migration & Lineage Invariant | Canonical `golem/` package, dual-layer shims in `first_mate/`, `pyproject.toml`, `setup.py`, launchd compatibility, lineage invariant | none | DONE |
| M2 | Pluggable Hand Execution Harness | `golem/execution/`, `herdr_tools.py`, AGY default worker (`worker: str = "agy"`, `GOLEM_DEFAULT_HAND_WORKER=agy`), on-demand delegation to `opencode`/`hermes` | M1 | DONE |
| M3 | Real-Time Observability WebUI | `golem/webui/`, `--webUI` / `GOLEM_ENABLE_WEBUI=1`, port 8765, SSE/REST, 4-pane dark-mode UI with Hand 0-9 dynamic grid, Stitch MCP + fallback | M1 | DONE |
| M4 | CI/CD Release Pipeline & Repository Hygiene | `.github/workflows/ci.yml` (Python 3.11, 3.12, 3.13), `LICENSE` (Apache-2.0), `CONTRIBUTING.md`, templates, `.gitignore` | M1 | DONE |
| M5 | Flagship Documentation & Operating Manuals | Flagship `README.md` (PTY scraping flex, topology diagrams, lineage), `AGENTS.md` pair-programming invariants | M1, M2, M3 | DONE |
| M6 | Final Milestone: 100% E2E Pass & Adversarial Hardening | Phase 1: Pass 100% E2E tests (Tiers 1-4). Phase 2: Adversarial coverage hardening (Tier 5 challenger loop). Verify clean daemon kickstart. | E2E, M1, M2, M3, M4, M5 | DONE |

## Interface Contracts
### `first_mate` ↔ `golem` (Dual-Layer Shim Contract)
- `_FirstMateAliasFinder` on `sys.meta_path`:
  - When `fullname.startswith("first_mate")`, loads corresponding `golem.<submodule>`.
  - Guarantees `sys.modules["first_mate.<submodule>"] is sys.modules["golem.<submodule>"]`.
  - Bidirectional mock patching: `patch('first_mate.gateway.ingress.call_gemini_api')` directly mutates `golem.gateway.ingress.call_gemini_api`.
- Static mirror files: `from golem.<submodule> import *` in every corresponding `first_mate/<submodule>.py` for static analyzers.

### `golem.execution` ↔ Hand Workers Contract
- Default worker resolution: `get_default_hand_worker() -> str` in `golem.config` returns `os.environ.get("GOLEM_DEFAULT_HAND_WORKER", "agy")`.
- `herdr_spawn(node="hermes", cwd="/Users/hermes", worker="agy", ...)`
- `herdr_handoff(worker="agy", task="", ...)` accepts both `worker` and legacy `subagent`.
- `WorkerDelegator.handoff(worker="agy", task="", ...)`
- Routing:
  - `"agy"` -> Local AGY CLI (`/Users/hermes/.local/bin/agy`) or Pane 2 shell.
  - `"opencode"` -> Isolated git worktree `task-<ts>-opencode`, FrugaLLM proxy.
  - `"hermes"` -> Homelab container / system ops workspace.

### `golem.webui` ↔ Ingress Gateway Contract
- Server lifecycle: `start_webui_server(port: int = 8765, host: str = "127.0.0.1") -> WebUISiteHandle` launched in `app.py:main()` if `args.webui` or `GOLEM_ENABLE_WEBUI=1`.
- Event broadcast: `webui_broadcast(event_type: str, data: Dict[str, Any])` emitting to connected SSE clients.
- Status API: `GET /api/status` returning JSON: `{ eyes: {...}, nose: {...}, mouth: {...}, hands: [...] }`.

## Code Layout
- `golem/`:
  - `gateway/`: `ingress.py`, `router.py`, `session.py`
  - `agent/`: `adapter.py`, `mouth.py`, `prompts.py`, `events.py`, `drivers/`
  - `execution/`: `harness.py`, `runner.py`, `delegation.py`, `buffer.py`, `mesh.py`
  - `sensory/`: `bus.py`, `eyes.py`, `nose.py`
  - `transport/`: `chunker.py`, `normalizer.py`, `file_transfer.py`, `slack_client.py`
  - `webui/`: `server.py`, `templates/`, `static/`
  - `config.py`, `__init__.py`
- `first_mate/`: Dual-layer compatibility shim mirroring `golem/`
- `tests/`: 71+ test files, 1,263+ unit tests + new tests for shims, webui, agy default
- `tests/e2e/`: Opaque-box E2E test suite (Tiers 1-4)
- `.github/workflows/ci.yml`: Multi-version CI workflow
- `pyproject.toml`, `setup.py`: Packaging definitions
- `com.user.golem.plist`, `com.user.first-mate.plist`: Launchd service definitions
- `README.md`, `AGENTS.md`, `LICENSE`, `CONTRIBUTING.md`: Documentation and repo hygiene
