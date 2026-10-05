# Task Spec: R2-BUILD (Live Backstage SSE Stream Wiring & Event Bridge)

## Metadata
- **Task ID:** `R2-BUILD`
- **Type:** `build`
- **Risk Tier:** `medium`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Target Source Files:
  - `golem/webui/server.py`
  - `golem/sensory/bus.py`
  - `golem/gateway/ingress.py`
  - `app.py`
- Target Test Files:
  - `tests/test_gateway_r2_sse.py`
  - `tests/test_webui_hands_sse.py`
- Protocol Artifacts:
  - `TASK_QUEUE.md`
  - `.specs/R2-BUILD.md`
  - `.logs/R2-BUILD.log`

## Hand B (Adversarial Auditor) Invariants
1. **Headless Invariant:** When `webui_server` is `None` (headless mode), execution completes with zero `AttributeError` or exceptions.
2. **Buffer Stream Invariant:** Every `TextDeltaEvent` emits both `update_hand_buffer` and `hand_buffer` SSE broadcast without dropping tokens or truncating chunks.
3. **No Hanging Hands Invariant:** Every active hand is cleanly settled (`hand_settle` with exit code and status) and unregistered (`unregister_active_hand`) on success, failure, and cancellation.
4. **Mouth Broadcast Invariant:** Executive mouth briefings (`mouth_briefing`) are broadcast with exact `{"briefing": clean_briefing, "model": model}` payload immediately upon posting to Slack.
5. **Thread Safety Invariant:** Sensory bus alert dispatch to WebUI SSE queues strictly schedules onto the server's asyncio loop via `loop.call_soon_threadsafe(self._process_bus_alert, alert)`, preventing cross-thread race conditions.
6. **Polymorphic Sync/Async Invariant:** Hand methods and broadcast return `_SyncAndAwaitableNone`, allowing them to be called synchronously or awaited with zero `RuntimeWarnings` and zero `TypeError`.
7. **Immediate State Handshake:** Connecting SSE clients immediately receive active `hands_status` if hands are currently running.
8. **Test Verification:** 100% test pass rate across all test targets (`test_mouth*.py`, `test_gateway*.py`, `test_sensory*.py`, `test_webui*.py`, `test_gateway_r2_sse.py`, `test_webui_hands_sse.py`).

## Blast Radius & Boundaries
- Scoped strictly to WebUI server, sensory bus, gateway ingress, app.py startup wiring, and test suites.
- Preserves all existing CLI, gateway, and transport interfaces without regression.
