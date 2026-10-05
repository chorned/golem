# Task Spec: M4-BUILD (GitHub Actions CI & Open-Source Assets)

## Metadata
- **Task ID:** `M4-BUILD`
- **Type:** `build`
- **Risk Tier:** `low`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- CI Configuration: `.github/workflows/ci.yml`.
- Multi-version test matrix: Python 3.11, 3.12, 3.13.
- Assets: `LICENSE` (Apache-2.0 / MIT), `CONTRIBUTING.md`, `.gitignore`, `.github/ISSUE_TEMPLATE/`.

## Hand B (Adversarial Auditor) Invariants
1. CI YAML syntax conforms strictly to GitHub Actions schema.
2. Steps include dependency install, linting, mock verification, and `python -m unittest discover tests`.
3. Open-source licenses and contribution guidelines are complete and accurate.

## Blast Radius & Boundaries
- Non-functional repository configuration and docs. Zero impact on runtime daemon.

## Milestone 4 Iteration 1 Addendum: Deliverable R4 (Runner Instrumentation & Eyes Observer Integration)
- **Status:** `[VERIFIED: Iteration 1]`
- **Task ID:** `R4-BUILD`
- **Target Source Files:**
  - `golem/sensory/eyes.py`
  - `golem/execution/runner.py`
- **Target Test Files:**
  - `tests/test_runner_instrumentation_r4.py`
- **Hand B Invariants:**
  1. Verbatim structured lifecycle log markers (`Dispatched command to pane '{pane}' [PID: {pid}]`, `Command exceeded 3.0s yield window; background execution continues`, `Command settled with exit code {returncode}`) emitted to `Golem.Hands` and `Golem.Execution.Runner`.
  2. Dynamic `EyesTaskObserver` attachment upon 3.0s yield for runner panes (Pane 2), strictly excluding Pane 1 (`liaison`, `fm-sess-liaison`, `w1:p1`) and admin panes.
  3. Clean observer teardown and unregistration upon settlement (`settle_command`, `settle_pane`, and sensory bus completion alerts).
  4. `EyesTaskObserver.__init__` compatibility: accepts `sensory_bus`, `bus`, `event_bus`, `webui_server`, defaults `buffer_observer` to `RunnerBufferObserver()`, provides `stop_monitoring_sync()`, suppresses `CancelledError`.
  5. Preservation of yield ceiling <= 3.15s (TC-02).
  6. 100% pass across target and regression test suites.

