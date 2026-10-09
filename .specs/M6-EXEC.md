# Task Spec: M6-EXEC (Full-Fleet E2E Victory Audit & Daemon Kickstart)

## Metadata
- **Task ID:** `M6-EXEC`
- **Type:** `execution`
- **Risk Tier:** `high`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- E2E Tests: Execute full 179 opaque-box test cases (`TEST_READY.md`).
- Daemon Kickstart: `launchctl kickstart -k -p gui/$(id -u)/com.user.golem` (or `com.user.golem`).

## Hand B (Adversarial Auditor) Invariants
1. 100% of 179 E2E tests pass under clean-room execution.
2. Background daemon PID active, healthy, and responsive to Slack/mesh pings.
3. Clean-room signoff: Hand B issues final completion certification.

## Blast Radius & Boundaries
- Service kickstart touches live local launchd daemon. Requires pre-check verification of PID and socket availability.
