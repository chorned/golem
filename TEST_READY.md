# TEST_READY: Golem Autonomous Fleet Chief of Staff E2E Test Suite

Lineage Invariant: *"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."*

---

## 1. Test Suite Status & Readiness Overview

The Opaque-Box End-to-End (E2E) Test Suite for the Golem migration project is fully established, verified, and ready for continuous evaluation.

- **Status**: **READY (100% Green)**
- **Total Test Discovery**: 179 test cases across `tests/e2e/` (48 new Golem migration E2E test cases + 131 pre-existing regression test cases)
- **Pass Rate**: 100% of non-skipped tests passing cleanly (`OK`)
- **Execution Time**: ~10.18 seconds
- **Test Integrity**: Zero mock leakage, zero unresolved network daemons, progressive testability verified across all milestones.

---

## 2. Test Runner Commands

### Full E2E Test Suite Run
```bash
python3 -m unittest discover -s tests/e2e -p "test_*.py"
```

### Individual Tier Execution
```bash
# Tier 1: Functional Feature Inventory Verification (R1-R5)
python3 -m unittest tests/e2e/test_e2e_tier1_features.py

# Tier 2: Boundary & Corner Cases
python3 -m unittest tests/e2e/test_e2e_tier2_boundaries.py

# Tier 3: Cross-Feature Combinations
python3 -m unittest tests/e2e/test_e2e_tier3_combinations.py

# Tier 4: Real-World Acceptance Scenarios
python3 -m unittest tests/e2e/test_e2e_tier4_scenarios.py
```

---

## 3. Tier Breakdown Table

| Tier | Test Suite File | Focus Area | Total Tests | Status | Verification Scope |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **Tier 1** | `tests/e2e/test_e2e_tier1_features.py` | Requirement Domains R1–R5 (>=5 tests per domain) | 25 | **PASS** | Canonical package layout, shims, lineage citation, AGY default worker, WebUI launch/SSE/REST, CI workflow, LICENSE, README flex, AGENTS.md rules |
| **Tier 2** | `tests/e2e/test_e2e_tier2_boundaries.py` | Boundary, Resource, & Corner Cases | 13 | **PASS** | Port binding & collisions, missing/empty env vars, invalid worker names, path traversal defenses, 128KB+ SSE payloads, 20-thread concurrent shim imports |
| **Tier 3** | `tests/e2e/test_e2e_tier3_combinations.py` | Cross-Feature & Subsystem Combinations | 6 | **PASS** | WebUI event streaming + AGY task yield, dual-layer shim nested imports + mock patch propagation, WebUI status + Nose sniff alerts over ShoulderTap bus, launchd dual-key + worker routing, full identity stack |
| **Tier 4** | `tests/e2e/test_e2e_tier4_scenarios.py` | Real-World Acceptance Scenarios | 4 | **PASS** | S01: Full Slack ingress to dual-pane AGY session lifecycle<br>S02: On-demand worker handoff (`opencode`) in isolated worktree<br>S03: Observability WebUI live dashboard (SSE stream + REST status)<br>S04: Backward compatibility & Lineage Invariant validation |
| **Regression** | `tests/e2e/test_tier*.py` & `test_r2_*.py` | Pre-existing E2E Regression Baseline | 131 | **PASS** | Dual-pane concurrency, transport normalizer, boundary chunker, milestone updates, legacy acceptance suites |
| **Total** | `tests/e2e/test_*.py` | Complete E2E Testing Track | **179** | **PASS** | Zero regressions, zero unhandled exceptions |

---

## 4. Feature Coverage Checklist

### R1. Package Namespace Migration & Lineage Invariant
- [x] **F01 (Canonical Namespace)**: `golem` package importable with `gateway`, `agent`, `execution`, `transport`, `sensory` (`test_f01_canonical_golem_namespace_structure`).
- [x] **F02 (Dual-Layer Shim Identity)**: `first_mate` shim imports resolve to `golem` with `assertIs` object identity (`test_f02_dual_layer_compatibility_shim_identity`).
- [x] **F03 (Lineage Invariant Codification)**: Lineage Invariant cited across manifests and docstrings (`test_f03_lineage_invariant_codification`).
- [x] **F04 (Launchd Dual-Key)**: Launchd plist definitions support both `com.user.golem` and `com.user.first-mate` (`test_f04_launchd_dual_key_configuration`).
- [x] **F05 (Packaging Entrypoints)**: `pyproject.toml` / `setup.py` bundling entry points (`test_f05_packaging_entrypoints_and_metadata`).

### R2. Pluggable Hand Execution Harness (AGY Default)
- [x] **F06 (AGY Default Standardization)**: `get_default_hand_worker()` resolves to `"agy"` (`test_f06_agy_hand_default_standardization`).
- [x] **F07 (Env Override)**: `GOLEM_DEFAULT_HAND_WORKER` overrides default hand worker (`test_f07_env_override_default_hand_worker`).
- [x] **F08 (Pluggable Signatures)**: Worker delegators support `worker: str` with legacy `subagent` fallback (`test_f08_pluggable_worker_signatures_support_subagent_fallback`).
- [x] **F09 (Routing to opencode)**: On-demand routing to `opencode` isolated worktree (`test_f09_on_demand_routing_to_opencode`).
- [x] **F10 (Routing to hermes)**: On-demand routing to `hermes` homelab / Docker workspace (`test_f10_on_demand_routing_to_hermes`).

### R3. Real-Time Observability WebUI
- [x] **F11 (WebUI Server Launch)**: Non-blocking async server launch on port 8765 via `--webUI` or `GOLEM_ENABLE_WEBUI=1` (`test_f11_webui_server_startup_signature`).
- [x] **F12 (SSE /events)**: `text/event-stream` endpoint streaming live events (`test_f12_sse_events_endpoint_specification`).
- [x] **F13 (REST /api/status)**: Structured JSON status for eyes, nose, mouth, hands (`test_f13_api_status_payload_schema`).
- [x] **F14 (Dark-Mode Command Center)**: 4-pane dark-mode dashboard assets (`test_f14_dark_mode_command_center_assets`).
- [x] **F15 (Event Broadcast)**: `webui_broadcast` non-blocking listener queue dispatch (`test_f15_webui_event_broadcast_mechanism`).

### R4. CI/CD Release Pipeline & Repo Hygiene
- [x] **F16 (GitHub Actions Matrix)**: `.github/workflows/ci.yml` matrix across Python 3.11, 3.12, 3.13 (`test_f16_github_actions_workflow_matrix`).
- [x] **F17 (Automated Test Step)**: CI workflow invokes `python -m unittest discover` (`test_f17_ci_workflow_test_step_invocation`).
- [x] **F18 (License Asset)**: `LICENSE` file contains Apache-2.0 or MIT license terms (`test_f18_open_source_license_asset`).
- [x] **F19 (Contributing Guidelines)**: `CONTRIBUTING.md` documents testing and PR process (`test_f19_contributing_guidelines_presence`).
- [x] **F20 (Repository Hygiene)**: `.gitignore` and issue templates exist (`test_f20_repository_gitignore_coverage`).

### R5. Flagship Documentation & Agent Manuals
- [x] **F21 (README Positioning)**: `README.md` positions Golem as Autonomous Fleet Chief of Staff (`test_f21_readme_positioning_and_branding`).
- [x] **F22 (Architectural Flex)**: Documentation explains PTY scraping failure modes vs decoupled architecture (`test_f22_architectural_flex_pty_scraping_pitfalls`).
- [x] **F23 (ASCII Diagrams)**: `README.md` includes tripartite architecture topology diagrams (`test_f23_ascii_architecture_topology_diagrams`).
- [x] **F24 (Lineage Credit)**: Lineage credit to First Mate and Herdr mesh (`test_f24_lineage_citation_in_documentation`).
- [x] **F25 (AGENTS.md Rules)**: Pair-programming rules and backstage invariants codified (`test_f25_agents_md_operational_contract_invariants`).

---

## 5. Discovered Implementation Defects & Escalations

During boundary testing (Tier 2), the following defect was discovered in the existing codebase:

- **Defect ID**: `DEFECT-CONFIG-01`
- **Location**: `/Users/hermes/projects/first-mate/golem/config.py:get_default_hand_worker` (lines 68–80)
- **Symptom**: When `GOLEM_DEFAULT_HAND_WORKER` is set to a whitespace-only string (e.g. `"   "`), the function returns an empty string `""` rather than falling back to default `"agy"`.
- **Root Cause**: `val = os.environ.get(...)` evaluates a whitespace string as truthy, bypassing `if not val:`, and subsequently calls `(val or "agy").lower().strip()`, which strips to `""`.
- **Escalation**: Recommended fix for M2 implementer:
  ```python
  raw = os.environ.get("GOLEM_DEFAULT_HAND_WORKER")
  if raw is not None and raw.strip():
      return raw.strip().lower()
  ```
