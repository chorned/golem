# Task Spec: M3-EXEC (Port 8765 Binding & Loop Non-Starvation)

## Metadata
- **Task ID:** `M3-EXEC`
- **Type:** `execution`
- **Risk Tier:** `high`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Server Lifecycle: Bind `http://127.0.0.1:8765` when `--webUI` or `GOLEM_ENABLE_WEBUI=1` is set.
- Stress Harness: High-frequency log emitter pushing continuous SSE updates to multiple concurrent clients.

## Hand B (Adversarial Auditor) Invariants
1. Zero event loop starvation: Slack Socket Mode frames and BlueBubbles webhooks maintain sub-second response times under 100+ SSE msgs/sec.
2. Port 8765 is immediately released on clean shutdown (no `EADDRINUSE` leaks).
3. Eye observer validates connection count, memory stability, and zero CPU runaway during streaming.

## Blast Radius & Boundaries
- Port 8765 local interface only. No external cloud ingress exposure.
