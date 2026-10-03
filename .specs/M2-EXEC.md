# Task Spec: M2-EXEC (PTY Concurrency & Deadlock Stress Test)

## Metadata
- **Task ID:** `M2-EXEC`
- **Type:** `execution`
- **Risk Tier:** `medium`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Scripts: Headless subshell execution tests under Herdr PTY mesh.
- Concurrency harness simulating simultaneous multi-pane worker runs.

## Hand B (Adversarial Auditor) Invariants
1. Zero deadlocks during long subshells (e.g. `sleep 3`, nested pipes, interactive escapes).
2. Exit codes accurately propagated to session state without truncation.
3. Clean process termination and buffer drain upon task completion or timeout.

## Blast Radius & Boundaries
- Uses isolated PTY test panes or mock subprocesses; zero interference with primary daemon sessions.
