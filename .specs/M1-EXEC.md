# Task Spec: M1-EXEC (Daemon & Shim Execution Verification)

## Metadata
- **Task ID:** `M1-EXEC`
- **Type:** `execution`
- **Risk Tier:** `high`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Configuration Files: `com.user.golem.plist`, `com.user.golem.plist`.
- CLI / Shims: Verify `golem` binary alias and environment variable fallback (`GOLEM_*` ──► `golem_*`).

## Hand B (Adversarial Auditor) Invariants
1. `plutil -lint` returns cleanly for both launchd plists.
2. `sys.meta_path` shim handles duplicate imports, pickle serialization, and monkeypatching without collision.
3. Dual-key environment variable resolution tested: missing `GOLEM_*` seamlessly falls back to `golem_*`.

## Blast Radius & Boundaries
- Non-destructive dry-run validation of plists. No uncoordinated service reloads without explicit pre-checks.
