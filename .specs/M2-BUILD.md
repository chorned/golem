# Task Spec: M2-BUILD (Pluggable Hand Execution Harness)

## Metadata
- **Task ID:** `M2-BUILD`
- **Type:** `build`
- **Risk Tier:** `medium`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Source Files: `golem/execution/`, `herdr_tools.py`, `agent_core.py`.
- Refactor worker dispatch to make local AGY CLI the default hand worker.
- Provide pluggable delegation hooks for `opencode` (code modification / tests) and `hermes` (Docker/ops).
- Support parameter `worker: str = "agy"` and env var `GOLEM_DEFAULT_HAND_WORKER=agy`.

## Hand B (Adversarial Auditor) Invariants
1. Default dispatch routes to AGY CLI without manual flag specification.
2. Explicit `worker="opencode"` or `worker="hermes"` accurately routes to respective agents.
3. Unit test coverage authored for worker resolution, parameter override, and fallback handling.

## Blast Radius & Boundaries
- Confined to backstage worker dispatch and tool execution harnesses.
- Dual-pane isolation and Herdr PTY boundaries must remain intact.
