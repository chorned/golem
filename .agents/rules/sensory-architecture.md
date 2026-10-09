# Sensory Architecture: The Mouth, Hands, Eyes, and Nose

This document codifies the operational contract for First Mate 2.0's decoupled sensory layer and cross-sensory self-awareness.

---

## 1. The Four Sensory Organs

First Mate decomposes cognition and execution into four specialized organs:

1. **The Mouth (Liaison — `MOUTH_MODEL`):**
   - **Role:** Sole speaker to Carl (the Captain) in Slack and iMessage. Front-of-house conversational partner and executive synthesizer.
   - **Ingress & Pacing:** Receives every inbound message with thread and session context. Answers greetings, queries, and conversational banter directly in natural Chief of Staff prose.
   - **Function Calling:** Dispatches backstage operational work via native Gemini function calling (`dispatch_task`). Actively inspects terminal and fleet state on demand via `check_eyes` and `check_nose`.
   - **Autonomous Synthesis:** Transforms structured JSON tool outputs received via `functionResponse` into concise, high-signal executive briefings in Slack mrkdwn (`*bold*`, inline `code`, clean bullets). Zero regex gates, zero canned templates.
   - **Concurrency:** Remains available to converse mid-stream while commands execute backstage, grounding out-of-band responses in cached sensory state with sub-second latency (<1s).

2. **The Hands (Worker Engine — `GOLEM_ENGINE`):**
   - **Role:** Backstage execution engine running silently in Pane 2 (the runner).
   - **Tools & Mechanics:** Executes deterministic commands, shell scripts, builds, Git operations, and test suites across fleet nodes (`hermes`, `desktop-pc`, `personal-laptop`, `oci-machine`) via Herdr mesh.
   - **Silence:** Never emits raw stdout/stderr, terminal escape codes, ANSI colors, prompt echoes, or curses widgets directly to Slack.

3. **The Eyes (Active Task Observer — `EYES_MODEL`):**
   - **Role:** Non-blocking background coroutine observing Pane 2 runner terminal buffers every 15–30s during active task execution.
   - **Efficiency:** Uses SHA-256 buffer change hashing (0 tokens consumed when buffer is unchanged).
   - **Classification:** Categorizes runner buffer tails into structured states: `RUNNING`, `BLOCKER`, `COMPLETED`, `ERROR`. Identifies interactive blockers (OAuth authorization links, `[y/N]` confirmation prompts).
   - **Alerting:** Emits typed `ShoulderTapAlert(alert_type="blocker"|"completion", source="eyes")` across the event bus to wake The Mouth without acquiring sequential thread locks.

4. **The Nose (24/7 Fleet Health Sentinel — `NOSE_MODEL`):**
   - **Role:** Evergreen background watchdog running autonomously on a recurring schedule (default every 15m).
   - **Telemetry:** Sniffs `bot.err` log tail for unhandled exception spikes, probes multi-node fleet reachability, checks Tailscale mesh status, monitors root disk space, and inspects Docker container health.
   - **Quiescent Zero-Regex Gate:** When error logs are empty and status deltas are benign (e.g. personal laptop sleeping), bypasses LLM entirely (0 tokens, 0 API calls).
   - **Alerting & Telemetry:** If an operational anomaly occurs (`smells_off=True`), emits `ShoulderTapAlert(alert_type="health_anomaly", source="nose")` to wake The Mouth. Caches telemetry snapshots for instant (<1ms) on-demand retrieval by The Mouth via `check_nose`.

---

## 2. The Shoulder Tap Event Bus

- In-process asynchronous event bus (`ShoulderTapEventBus`, `bus.py`) providing decoupled inter-organ communication.
- Sensory observers (The Eyes, The Nose) publish typed `ShoulderTapAlert` objects:
  - `alert_type="blocker"`: Interactive blocker detected in runner (OAuth URL or prompt).
  - `alert_type="completion"`: Long-running task concluded.
  - `alert_type="health_anomaly"`: Fleet or daemon health issue detected.
- The Mouth subscribes to alerts per session/thread or system-wide, waking up immediately to notify Carl or update reaction state markers (`:gear:` -> `:warning:`, `:white_check_mark:`, `:x:`).

---

## 3. Sensory Inspection Tools (`check_eyes`, `check_nose`)

The Mouth is equipped with first-class inspection tools in `get_mouth_tools_declaration()`:

### `check_eyes`
- **Purpose:** Inspects active runner execution state, command, PID, blockers, and buffer tail.
- **Contract:** Pure JSON-serializable dictionary:
  ```json
  {
    "state": "RUNNING" | "IDLE" | "COMPLETED" | "BLOCKER",
    "active_command": "...",
    "pid": 12345,
    "blocker": {
      "type": "oauth" | "prompt" | null,
      "prompt": "...",
      "url": "..."
    },
    "buffer_tail": "..."
  }
  ```

### `check_nose`
- **Purpose:** Inspects 24/7 fleet and system health telemetry.
- **Parameters:** `trigger_fresh_sniff: bool` (default `False`). When `False`, returns cached 15m snapshot instantly (<1ms). When `True`, triggers an immediate live probe across fleet nodes.
- **Contract:** Pure JSON-serializable dictionary:
  ```json
  {
    "seconds_ago": 120.0,
    "minutes_ago": 2.0,
    "last_check_timestamp": "2026-10-02T18:00:00Z",
    "smells_off": false,
    "verdict_summary": "All systems nominal; 0 errors, fleet healthy.",
    "disk": {"free_gb": 48.2, "percent_used": 75.0, "pressure": false},
    "docker": {"running": true, "container_count": 6, "containers": [...]},
    "tailscale": {"online": true, "backend_state": "Running", "peers": {...}},
    "recent_error_count": 0
  }
  ```

---

## 4. Operational Integrity Principles

1. **Zero Pre-Baked Strings:** Tool execution methods return 100% pure dictionaries. The Mouth LLM synthesizes natural briefings.
2. **Zero Regex Gates:** User queries are never intercepted by brittle keyword regexes; Gemini function calling handles semantic tool selection.
3. **Sub-Second Mid-Stream Latency:** When Carl asks a question mid-stream, The Mouth responds in <1s using cached sensory states without waiting for background commands to complete.
4. **Mutual Awareness:** All system prompts codify the collaborative relationship among Mouth, Hands, Eyes, Nose, and the Shoulder Tap Event Bus.
