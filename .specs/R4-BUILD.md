# Task Spec: R4-BUILD (Runner Instrumentation & Eyes Observer Integration)

## Metadata
- **Task ID:** `R4-BUILD`
- **Type:** `build`
- **Risk Tier:** `medium`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Target Source Files:
  - `golem/sensory/eyes.py`
  - `golem/execution/runner.py`
- Target Test Files:
  - `tests/test_runner_instrumentation_r4.py`
- Protocol Artifacts:
  - `TASK_QUEUE.md`
  - `.specs/R4-BUILD.md`
  - `.logs/R4-BUILD.log`
  - `.specs/M4-BUILD.md`
  - `.logs/M4-BUILD.log`

## Hand B (Adversarial Auditor) Invariants
1. **Structured Log Marker Invariant:** Verbatim emission of lifecycle markers:
   - Dispatch: `[INFO] Golem.Hands: Dispatched command to pane '{pane}' [PID: {pid}]`
   - Yield: `[INFO] Golem.Hands: Command exceeded 3.0s yield window; background execution continues`
   - Settlement: `[INFO] Golem.Hands: Command settled with exit code {returncode}`
   Logged synchronously to both `Golem.Hands` and `Golem.Execution.Runner` so handlers attached to either logger capture the exact marker.
2. **Dynamic EyesTaskObserver Attachment Invariant:**
   - Tasks yielding after the 3.0s window in runner panes (Pane 2) dynamically instantiate and register an `EyesTaskObserver` in `self._active_observers[pane]`.
   - Observer is wired with `sensory_bus` (falling back to `get_default_event_bus()`) and optional `webui_server`.
   - Pane 1 (`liaison`, `fm-sess-liaison`, `w1:p1`) and administrative panes strictly excluded from observer attachment.
3. **Clean Teardown Invariant:**
   - Observer monitoring task is cancelled/stopped and removed from `_active_observers` when commands settle.
   - Synchronous `settle_command(pane, returncode)` and `settle_pane(pane, returncode)` methods provided.
   - Automatic teardown and settlement executed upon `completion` event alerts on `sensory_bus`.
4. **EyesTaskObserver Constructor Compatibility Invariant:**
   - `EyesTaskObserver.__init__` accepts `sensory_bus: Optional[ShoulderTapEventBus] = None`, `bus`, `event_bus`, `webui_server`, and `**kwargs`.
   - Defaults `buffer_observer` to `RunnerBufferObserver()` if `None`.
   - Forwards terminal buffer updates to `webui_server.update_hand_buffer(target_pane, clean_buffer)` if `webui_server` is present.
   - Provides `stop_monitoring_sync()` and cleanly suppresses `asyncio.CancelledError` in `_watch_loop`.
5. **Yield Ceiling Invariant:**
   - Non-blocking observer attachment preserves the strict <= 3.15s yield ceiling (TC-02) under long-running execution.
6. **Test Verification Invariant:**
   - 100% pass rate (18/18) in `tests/test_runner_instrumentation_r4.py`.
   - 100% pass rate (32/32) in `tests/test_runner_yield_ceiling.py` and `tests/test_eyes_observer.py`.
   - 100% pass rate (69/69) in `tests/test_execution_m4.py` and `tests/test_sensory_tools.py`.
   - Full discovery unittest run with 0 regressions.

## Blast Radius & Boundaries
- Scoped strictly to `golem/sensory/eyes.py`, `golem/execution/runner.py`, and `tests/test_runner_instrumentation_r4.py`.
- Preserves all existing execution semantics, PTY allocations, yield windows, and sensory buses.
