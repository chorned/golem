# Task Spec: R1-BUILD (Slack Gateway Resiliency & Protocol Integrity)

## Metadata
- **Task ID:** `R1-BUILD`
- **Type:** `build`
- **Risk Tier:** `medium`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Target Source Files:
  - `golem/gateway/ingress.py`
- Target Test Files:
  - `tests/test_gateway_m3.py`
  - `tests/test_sensory_tools.py`
  - `tests/test_gateway_r1_resiliency.py`
- Protocol Artifacts:
  - `TASK_QUEUE.md`
  - `.specs/R1-BUILD.md`
  - `.logs/R1-BUILD.log`

## Hand B (Adversarial Auditor) Invariants
1. **Fallback Removal:** Hardcoded fallback briefings (`Runner is currently IDLE`, `Fleet Health: Air is clean`) completely removed from `if not briefing_text:` blocks.
2. **Step 2 Schema Invariant:** Step 2 Gemini synthesis payload strictly includes `"tools": tools` schema declaration (`get_mouth_tools_declaration()`), resolving HTTP 400 Bad Request.
3. **Fail-Loud Reporting:** Step 2 synthesis or runtime failures post raw exception, HTTP status code, and tool JSON payload directly to Slack (`⚠️ *Executive Synthesis Failed*...`) and set reaction to `:boom:` or `:x:`. Never simulate normal operational telemetry on failure.
4. **Thread Context Rehydration:** Active thread replies rehydrate thread context via `_fetch_thread_context` without mutating `clean_text`.
5. **Sensory Command Routing:** Sensory direct commands (`/status`, `!status`, `/eyes`, `!eyes`, `/health`, `!health`, `/nose`, `!nose`) route directly to `_execute_check_eyes` and `_execute_check_nose`.
6. **Mouth System Prompt:** System prompt explicitly forbids unprompted sensory tool calls (`check_eyes`, `check_nose`) during conversational chatter.
7. **Test Verification:** 100% test pass rate across `tests/test_gateway*.py`, `tests/test_sensory*.py`, and `tests/test_gateway_r1_resiliency.py`.

## Blast Radius & Boundaries
- Strictly scoped to gateway ingress, tests, and task board protocol artifacts.
- No modifications to out-of-scope files.
