# E2E Test Infrastructure: Golem Autonomous Fleet Chief of Staff

Lineage Invariant: *"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."*

---

## 1. Test Philosophy & Design Principles

The Golem End-to-End (E2E) Test Suite establishes an opaque-box, requirement-driven verification track derived directly from `ORIGINAL_REQUEST.md` and `PROJECT.md`.

### Core Principles
1. **Opaque-Box Requirement Derivation**:
   Tests treat the system as a sealed black box, interacting exclusively through external interfaces:
   - Public Python module imports (`golem.*` and backward-compatible `first_mate.*`)
   - CLI flags (`--webUI`, `--port`, `--input-format`, `--output-format`)
   - Environment variables (`GOLEM_*`, `FIRST_MATE_*`)
   - HTTP / SSE network endpoints (`/events`, `/api/status`)
   - External event buses and process boundaries (`ShoulderTapEventBus`, Herdr PTY, subprocess stdio)
   Zero tests rely on private or transient implementation details.

2. **Zero Internal Coupling & Contract Fidelity**:
   Tests validate observable interface contracts, protocol compliance, data formats, and behavioral guarantees. If internal algorithms are refactored while preserving external contracts, the test suite remains 100% stable.

3. **Progressive Testability Across Milestones**:
   Tests gracefully support execution across the milestone progression. For features pending delivery in planned milestones (M1–M5), tests employ dynamic feature detection and clear, informative skip messages (`unittest.skipUnless`, `unittest.skipIf`). As each milestone lands, corresponding test cases immediately execute full assertion suites, culminating in 100% pass rate in Milestone 6.

4. **Hermetic Isolation & Clean State**:
   All network daemons (Slack Socket Mode, BlueBubbles), external cloud LLMs (Gemini API), and OS-level PTYs (Herdr daemon) are hermetically mocked or sandboxed. Tests never perform unsolicited external network I/O, leave dangling background threads, or leak file handles.

---

## 2. Feature Inventory & Requirement Mapping

The test infrastructure maps all features across the 5 core requirement domains defined in `ORIGINAL_REQUEST.md`:

| Requirement | Domain | Key Features & Responsibilities | Test Suite |
| :--- | :--- | :--- | :--- |
| **R1** | **Package Namespace Migration & Lineage Invariant** | • Canonical `golem/` package namespace (`gateway`, `agent`, `execution`, `transport`, `sensory`, `webui`)<br>• Dual-layer compatibility shim (`sys.meta_path` `_FirstMateAliasFinder` + static mirrors)<br>• 100% Object identity (`first_mate.X is golem.X`) & bidirectional mock patching<br>• PEP 621 packaging (`pyproject.toml`, `setup.py`, entrypoints `golem` and `first-mate`)<br>• Dual-key launchd configuration (`com.user.golem.plist`, `com.user.first-mate.plist`, `GOLEM_*` / `FIRST_MATE_*`)<br>• Lineage invariant documentation: *"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."* | Tier 1 (F01–F05)<br>Tier 2 (B05)<br>Tier 3 (C02, C05)<br>Tier 4 (S01, S04) |
| **R2** | **Pluggable Hand Execution Harness (AGY Default)** | • Standardization on local AGY CLI as default Hand worker (`get_default_hand_worker() -> "agy"`)<br>• Configuration override via `GOLEM_DEFAULT_HAND_WORKER`<br>• Parameter signatures supporting `worker: str = "agy"` and legacy `subagent`<br>• Clean on-demand delegation hooks to `opencode` (code worktree) and `hermes` (Docker ops)<br>• Dual-pane isolation (Pane 1 Mouth vs Pane 2 Hands) and 3.0s non-blocking yield | Tier 1 (F06–F10)<br>Tier 2 (B02, B03)<br>Tier 3 (C01, C04)<br>Tier 4 (S01, S02) |
| **R3** | **Real-Time Observability WebUI** | • CLI flag `--webUI` and env toggle `GOLEM_ENABLE_WEBUI=1` / `GOLEM_WEBUI=1`<br>• Non-blocking async server on `http://127.0.0.1:8765`<br>• SSE endpoint `/events` (`text/event-stream`) broadcasting real-time logs and sensory events<br>• REST endpoint `/api/status` returning structured JSON telemetry<br>• 4-pane dark-mode command center (Eye, Nose, Mouth, Hand 0–9 dynamic grid)<br>• Resilient vanilla HTML/CSS/JS fallback if Google Stitch MCP is unavailable | Tier 1 (F11–F15)<br>Tier 2 (B01, B04)<br>Tier 3 (C01, C03)<br>Tier 4 (S01, S03) |
| **R4** | **CI/CD Release Pipeline & Repo Hygiene** | • Multi-version GitHub Actions workflow (`.github/workflows/ci.yml`) across Python 3.11, 3.12, 3.13<br>• Automated testing (`python -m unittest discover -s tests -v`), lint, and security scanning<br>• Open source repository assets: `LICENSE` (Apache-2.0), `CONTRIBUTING.md`, `.gitignore`, issue/PR templates | Tier 1 (F16–F20)<br>Tier 2 (B02)<br>Tier 3 (C05)<br>Tier 4 (S04) |
| **R5** | **Flagship Documentation & Agent Manuals** | • Flagship `README.md` positioning Golem as Autonomous Fleet Chief of Staff<br>• Architectural flex articulating PTY scraping pitfalls (VT100 corruption, token starvation, subshell deadlocks) vs decoupled tripartite architecture<br>• ASCII topology diagrams & lineage credits<br>• `AGENTS.md` operational contract with pair-programming rules and backstage invariants | Tier 1 (F21–F25)<br>Tier 2 (B02)<br>Tier 3 (C05)<br>Tier 4 (S01, S04) |

---

## 3. Test Tier Breakdown

The test suite is organized into four complementary verification tiers:

### Tier 1: Feature Inventory Verification (`test_e2e_tier1_features.py`)
- **Focus**: Functional requirement coverage across all 5 requirement domains (R1–R5).
- **Quota**: Minimum 5 dedicated test cases per requirement domain (>=25 test cases total).
- **Domains**:
  - `TestTier1R1NamespaceAndLineage`: Canonical package layout, dual-layer shim, object identity, launchd dual-key, lineage citation.
  - `TestTier1R2PluggableHarness`: Default AGY resolution, env override, parameter signatures, on-demand routing to `opencode`, on-demand routing to `hermes`.
  - `TestTier1R3ObservabilityWebUI`: Async server startup, SSE `/events` format, REST `/api/status` payload, 4-pane dark-mode assets, event broadcast bus.
  - `TestTier1R4ReleaseAndHygiene`: GitHub Actions workflow syntax/matrix, test invocation step, `LICENSE` presence, `CONTRIBUTING.md` guidelines, repository templates.
  - `TestTier1R5DocumentationAndRules`: `README.md` positioning, PTY scraping flex analysis, ASCII architecture diagrams, lineage credit, `AGENTS.md` operational rules.

### Tier 2: Boundary & Corner Cases (`test_e2e_tier2_boundaries.py`)
- **Focus**: Extreme inputs, network race conditions, resource contention, and fault injection.
- **Key Boundaries**:
  - **Port Fallback & Collisions**: Primary port (8765) occupied by external listener; verify graceful fallback to secondary port or clean collision diagnosis without crashing gateway.
  - **Missing & Extreme Environment Variables**: Empty `GOLEM_DEFAULT_HAND_WORKER=""`, malformed boolean toggles (`GOLEM_ENABLE_WEBUI="invalid"`), missing sensory model keys.
  - **Invalid Worker Names**: Validation of worker parameters (`worker="malicious/path"`, `worker=""`, numeric workers) preventing arbitrary shell injection or PTY corruption.
  - **Large Streaming Chunks & SSE Stress**: Event payloads exceeding 128KB, 1,000 rapid broadcast bursts, SSE client disconnect mid-stream.
  - **Concurrent Shim Imports**: Multi-threaded concurrent imports across 20 threads simultaneously asserting zero module cache race conditions.

### Tier 3: Cross-Feature Combinations (`test_e2e_tier3_combinations.py`)
- **Focus**: Pairwise and multi-feature interactions where subsystems collaborate.
- **Key Combinations**:
  - **Combination 1 (R2 + R3)**: WebUI real-time event streaming active while AGY executes backstage task in Pane 2 (yielding at 3s while Mouth converses).
  - **Combination 2 (R1 + R1/R2)**: Dual-layer compatibility shim with nested dynamic imports and bidirectional `unittest.mock.patch` propagation.
  - **Combination 3 (R3 + Sensory)**: WebUI telemetry stream dynamically reflecting Nose Sentinel health sniff cycles and Eyes buffer change alerts over `ShoulderTapEventBus`.
  - **Combination 4 (R1 + R2)**: Launchd dual-key environment configuration (`GOLEM_*` / `FIRST_MATE_*`) resolving cleanly into pluggable worker dispatch.
  - **Combination 5 (R1 + R3 + R5)**: Full Golem identity stack: WebUI header, gateway ingress formatting, and telemetry metadata all reflecting Golem branding and Lineage Invariant.

### Tier 4: Real-World Acceptance Scenarios (`test_e2e_tier4_scenarios.py`)
- **Focus**: End-to-end integration workflows simulating realistic operational lifecycles.
- **Scenarios**:
  - **Scenario 1 (Full Operational Session Lifecycle)**: User Slack prompt -> Ingress Gateway -> SessionRouter dual panes -> AGY backstage task dispatch -> 3.0s non-blocking yield -> WebUI SSE live update -> Mouth status response -> Completion briefing.
  - **Scenario 2 (On-Demand Worker Delegation)**: User requests code modification -> Mouth delegates to `opencode` -> Isolated git worktree provisioned -> Task executed -> Summary delivered.
  - **Scenario 3 (Observability WebUI Live Dashboard)**: WebUI server launch -> SSE connection opened -> Live sensory and worker events stream -> Snapshot queried via `/api/status` -> Clean shutdown.
  - **Scenario 4 (Legacy Invariant & Backward Compatibility)**: Legacy scripts importing `first_mate` execute end-to-end through `golem` engine with verified identity and lineage preservation.

---

## 4. Test Architecture & Runner Semantics

### Directory Layout
```
first-mate/
├── TEST_INFRA.md                   # This specification
├── TEST_READY.md                   # Readiness status & runner guide
├── tests/
│   ├── e2e/
│   │   ├── __init__.py             # Test package marker
│   │   ├── test_e2e_tier1_features.py     # Tier 1: Functional feature verification
│   │   ├── test_e2e_tier2_boundaries.py   # Tier 2: Boundary & corner cases
│   │   ├── test_e2e_tier3_combinations.py # Tier 3: Cross-feature combinations
│   │   ├── test_e2e_tier4_scenarios.py    # Tier 4: Real-world acceptance scenarios
│   │   └── ...                            # Pre-existing regression suites
```

### Invocation Semantics
The entire E2E test suite is executed using standard Python `unittest`:

```bash
# Run all E2E test suites
python3 -m unittest discover -s tests/e2e -p "test_*.py"

# Run specific tiers
python3 -m unittest tests/e2e/test_e2e_tier1_features.py
python3 -m unittest tests/e2e/test_e2e_tier2_boundaries.py
python3 -m unittest tests/e2e/test_e2e_tier3_combinations.py
python3 -m unittest tests/e2e/test_e2e_tier4_scenarios.py
```

### Pass/Fail Criteria & Thresholds
- **Exit Code**: Must be `0`. Zero unhandled exceptions, zero compilation failures.
- **Test Pass Rate**: 100% of non-skipped test cases must pass assertions (`OK`).
- **Feature Coverage Threshold**:
  - Tier 1: Minimum 5 test cases per requirement domain (R1–R5), >=25 test cases total.
  - Tier 2: Minimum 5 boundary categories covered.
  - Tier 3: Minimum 5 multi-feature interaction combinations verified.
  - Tier 4: Minimum 4 comprehensive end-to-end scenarios verified.
- **Regression Guarantee**: Zero broken tests in pre-existing test suites.
