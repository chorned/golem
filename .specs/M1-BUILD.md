# Task Spec: M1-BUILD (Canonical Namespace Migration)

## Metadata
- **Task ID:** `M1-BUILD`
- **Type:** `build`
- **Risk Tier:** `medium`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Target Directories: `golem/` subpackages (`golem/gateway`, `golem/agent`, `golem/execution`, `golem/transport`, `golem/sensory`, `golem/webui`).
- Metadata Files: `pyproject.toml`, `setup.py`.
- Tests: `tests/test_golem_namespace_and_shims.py`.

## Hand B (Adversarial Auditor) Invariants
1. Direct imports of `golem.*` resolve cleanly.
2. Exact module and class identity preserved between `first_mate.*` and `golem.*` (`assertIs`).
3. 1,326 unit tests execute cleanly without regressions.
4. Lineage invariant present in docstrings and headers: *"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."*

## Blast Radius & Boundaries
- Only Python source and configuration files related to the package namespace.
- No deletion of legacy files without backward-compatibility shims.
