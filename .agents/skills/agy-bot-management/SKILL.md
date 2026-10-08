---
name: agy-bot-management
description: Comprehensive operational runbook for monitoring, starting, stopping, restarting, debugging, and testing the AGY native macOS Slack background daemon (com.user.first-mate). Use whenever inspecting service status, viewing logs, updating configurations, or troubleshooting the AGY Slack bot.
---

# AGY Bot Management Guide

This skill provides step-by-step instructions for managing the native macOS background service that connects Slack to the local Antigravity CLI (`agy`).

## Service Architecture
- **Daemon Label:** `com.user.first-mate` (launchd domain `gui/502`)
- **Plist Location:** `~/Library/LaunchAgents/com.user.first-mate.plist`
- **Entry Point:** `/Users/hermes/projects/first-mate/app.py`
- **Project Directory:** `/Users/hermes/projects/first-mate`
- **Virtual Environment:** `/Users/hermes/projects/first-mate/.venv`
- **Log Files:** 
  - Standard Out: `/Users/hermes/projects/first-mate/bot.log`
  - Standard Error: `/Users/hermes/projects/first-mate/bot.err`
- **Retired:** `com.user.antigravity-slack` (pointed at a deleted worktree, exited `78 EX_CONFIG`). Its plist is archived in `~/Library/LaunchAgents.disabled/`. Do not reload it.

---

## 1. Checking Service Status

To verify if the daemon is loaded and running:
```bash
launchctl print gui/$(id -u)/com.user.first-mate | grep -E "state =|pid =|runs =|last exit"
```
- **Output interpretation:**
  - `state = running` with a `pid`: Running healthy.
  - `state = spawn scheduled` / non-zero `last exit code`: Crash-looping or misconfigured; inspect `bot.err`.
  - `Could not find service`: The daemon is not loaded into launchd.

---

## 2. Stopping, Starting, and Restarting

### Restarting the Service (Recommended after code/env updates)
```bash
launchctl kickstart -k gui/$(id -u)/com.user.first-mate
```
Then confirm with `tail -n 15 /Users/hermes/projects/first-mate/bot.err` and look for `Bolt app is running!`.

### Stopping the Service
```bash
launchctl bootout gui/$(id -u)/com.user.first-mate
```

### Starting / Enabling the Service
```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.user.first-mate.plist
```

---

## 3. Log Inspection & Troubleshooting

Real-time tailing of connection logs and errors:
```bash
# View recent error and debug output (includes Socket Mode connection status)
tail -f /Users/hermes/projects/first-mate/bot.err

# View standard output
tail -f /Users/hermes/projects/first-mate/bot.log
```

### Common Log Signatures:
- `⚡️ Bolt app is running!`: Service is successfully connected to Slack via Socket Mode.
- `A new session has been established`: WebSocket connection connected to Slack Link API.
- `Captured conversation_id '<id>' for session <channel>:<thread_ts>`: Successfully initialized thread session with Antigravity.

---

## 4. Running Automated Tests

Run the regression test suites covering in-memory locks, debouncer rate limits, `switch to` directory commands, post-execution git diff / artifact uploads, and the Herdr Chief of Staff persistent execution engine:
```bash
cd /Users/hermes/projects/first-mate
./.venv/bin/python3 -m unittest tests/test_app.py
./.venv/bin/python3 -m unittest tests/test_herdr_fleet.py
```

---

## 5. Configuration & Workspace Management

- **Credentials & Defaults:** Stored in `/Users/hermes/projects/first-mate/.env`.
- **Dynamic Workspace Switch:** In any Slack thread, users can send:
  ```text
  switch to /path/to/project
  ```
  The bot verifies the path, resets `conversation_id`, and repoints subsequent thread prompts to that folder.

---

## 6. Thread-Scoped Implicit Triggering (No Repeated Mentions)

- In channels, a session begins when the bot is explicitly mentioned via `app_mention`.
- That `thread_ts` is registered in `active_thread_sessions`.
- The bot listens to general channel `message` events (`message.channels`, `message.groups`).
- If a message arrives inside an already registered `thread_ts`, it is processed automatically as a follow-up prompt even if the user did not `@`-mention the bot.
- Messages containing an `@mention` in a tracked thread are deduplicated using `f"{channel}:{ts}"` to prevent double execution.
- Top-level channel messages not mentioning the bot and not tied to an active thread are automatically disregarded.

---

## 7. Reacji-Driven State Lifecycle & Executive Synthesis

- **FirstMate Architecture:** Carl speaks only to AGY (Chief of Staff). Backstage crew operates headlessly.
- **Reacji State Markers:** Real-time execution status is conveyed via emoji reactions on the user's triggering message:
  - `:eyes:` — Message received and queued (instant acknowledgment).
  - `:gear:` — Task dispatched and executing headlessly across the fleet.
  - `:warning:` — Agent blocked (requires human decision, prompt, or credentials).
  - `:white_check_mark:` — Task succeeded; executive briefing posted.
  - `:x:` — Task failed; error details and root cause posted.
- **Executive Synthesis:** Raw backstage findings are passed through the headless local AGY CLI brain (`agy -p <synthesis_prompt> --output-format text`) running in `/Users/hermes/projects/AGY` (inheriting `AGENTS.md` and `GEMINI.md`) to distill noise into a clean Slack briefing.
- **Trace Upload:** For tasks generating $>$ 10 log/trace lines, full chronological logs are uploaded as `agy_trace_<timestamp>.txt`.
- **Failures:** Subprocess exit codes and stderr lines are displayed directly in an error code block if a run fails.

---

## 8. Remote Host Targets (Tailscale SSH)

- **Configured Aliases:** Defined via `REMOTE_HOSTS=alias:ip,...` and `REMOTE_SSH_USER` in `.env`.
- **Switching Host:** In any Slack thread or DM:
  ```text
  switch to <alias>
  ```
  Repoints the target to `<alias>` (`~`), resets `conversation_id`, and executes subsequent prompts via non-interactive SSH with streaming NDJSON.
- **Switching Back:**
  ```text
  switch to /path/to/local/dir
  ```

---

## 9. Bidirectional File Transfers & Artifact Sharing

- **Slack -> Workspace (User sends files to AGY):**
  - Users can drag & drop or upload files in Slack threads or DMs.
  - Files are automatically saved to `active_workspace/slack_transfers/<filename>` (or `~/slack_transfers/<filename>` on remote hosts).
  - AGY receives context about the uploaded file in its prompt and can inspect or process it immediately.
- **Workspace -> Slack (AGY sends files to User):**
  - Direct file download command without invoking LLM:
    ```text
    send me <filename>
    ```
    Searches both active workspace root and `slack_transfers/`.
  - AGY directive in responses:
    ```text
    [SEND_FILE: path/to/file]
    ```
  - Export artifacts (`.pdf`, `.csv`, `.xlsx`, `.zip`, etc.) and image artifacts (`.png`, `.jpg`, `.svg`) are auto-detected and uploaded to the thread upon completion.

---

## 10. Operational Crystallization (`/learn`)

- **Triggering:** In any Slack thread where a workflow was completed, reply with:
  ```text
  /learn [optional notes]
  ```
  or `@AGY /learn [optional notes]`.
- **Lifecycle:**
  - `:brain:` — Trigger acknowledged, thread history compiled into transcript.
  - Headless distillation turn executed via `agy` CLI in `/Users/hermes/projects/AGY`.
  - Golden path extracted and saved to disk:
    * Node quirks & machine specifics -> `.agents/rules/<node>.md`
    * Repeatable tasks & runbooks -> `.agents/skills/<skill_name>/SKILL.md`
  - `:white_check_mark:` — Crystallization complete; confirmation posted in thread.
- **Failures:** If executed outside a thread or if distillation fails, AGY responds with `:warning:` or `:x:` and a concise diagnostic message.

---

## 11. FirstMate Executive Command Suite & Bang (`!`) Triggers

All executive commands support both slash syntax, bot mentions, and client-friendly **bang (`!`) prefixes** that bypass Slack client-side command trapping:

- **`/bearings`, `!bearings` (or `@AGY fleet`):**
  - Gathers 4-node telemetry concurrently (root disk space and Docker count on `hermes`, GPU utilization and VRAM on `desktop-pc`, sleep/branch status on `personal-laptop`, and uptime on `oci-machine` with 8.0s timeout).
  - Synthesizes findings via headless `agy` CLI into a concise executive digest.
  - Automatically degrades gracefully if `personal-laptop` is asleep (`[Asleep / Offline]`).

- **`/ahoy`, `!ahoy`:**
  - Session catch-up and pending decision resolver.
  - Reviews background task completions from `/tmp/herdr_tasks/`.
  - Flags any blocked panes awaiting input, providing concrete instructions to unblock.

- **`/wrapup`, `!wrapup`:**
  - End-of-day session housekeeping.
  - Audits git worktree status on `hermes` and `personal-laptop` (if awake).
  - Sweeps stale task logs older than 2 hours.
  - Scans error logs for recurring node quirks and presents proposed rule diffs in Slack.
  - **Tree Hygiene Guarantee:** Never modifies rule files silently.

- **`approve quirk` (or `approve`):**
  - Approves and commits the suggested quirk rule from `/wrapup` directly to `.agents/rules/<node>.md`.

- **`/update`, `!update`:**
  - Safe full-stack self-update.
  - Checks if any fleet pane or task lock is held; aborts if busy.
  - Pulls latest changes from `origin/main`.
  - Runs automated test suite (`python -m unittest discover tests`).
  - **Automatic Rollback:** If any test fails, runs `git reset --hard ORIG_HEAD` and alerts in Slack.
  - Audits binary paths (`herdr`, `agy`, `hermes`) and reloads daemon via `launchctl kickstart`.

- **`/learn`, `!learn`:**
  - Thread-scoped operational crystallization into rules and skills.

---

## 12. Chief of Staff Pre-Flight Alignment ("Grill-Me" Workflow)

Natural language instructions sent to AGY are **NEVER dispatched directly to a shell executor**. Instead, AGY runs a pre-flight alignment turn:

1. **Alignment Briefing:** AGY presents a structured alignment message stating:
   - Summary of user intent.
   - Delegated worker and target node.
   - Exact deterministic command payload in quotation (`> *"..."*`).
   - Assumptions and operational choices.
   - Explicit **Recommendation (Default)** path.
2. **Approval Loop:**
   - **Approve Default:** Carl simply types `go`, `yes`, `proceed`, `do it`, or reacts with 👍 (`+1`/`thumbsup`) on the alignment message.
   - **Tweak Plan:** Carl provides adjustments in the thread (e.g. "include ZFS as well"), and AGY updates the alignment briefing with revised commands.
3. **Execution & Synthesis:** Upon approval, the planned payload executes headlessly and the findings are synthesized by the AGY CLI into an executive briefing.

---

## 13. BlueBubbles iMessage Transport Integration

AGY operates across both Slack and native Apple iMessage via local BlueBubbles integration:

- **Listener Port:** Embedded `aiohttp` server on port `8646` (`0.0.0.0:8646/bluebubbles-webhook`).
- **BlueBubbles Server:** Connects to `127.0.0.1:1234` with password authentication.
- **Dynamic Registration:** Automatically registers webhook URL `http://127.0.0.1:8646/bluebubbles-webhook?password=...` on boot and deregisters on shutdown.
- **Security & Whitelist:** Restricts message execution to handles listed in `BLUEBUBBLES_ALLOWED_HANDLES` (`k.horned@gmail.com`). Ignores tapback reactions and server self-echoes.
- **Multi-Channel Parity:** Carl can send any command (`!bearings`, `!ahoy`, `!wrapup`, `!update`, `!learn`) or natural language request over iMessage. AGY will generate the Pre-Flight Alignment briefing, wait for `go` or 👍, execute headlessly on Herdr, chunk responses over 3,500 characters, and upload patches/files directly to the iMessage chat.
- **Thread Lifecycle (Model A - Strict Slack Parity):**
  - **Top-Level Message (Bottom Input Bar):** `threadOriginatorGuid` is empty. Every message sent in the general text field starts a brand-new conversation root (`thread_ts = ts`) with zero prior context.
  - **Inline Thread Reply (Native Apple Messages "Reply"):** When Carl long-presses a message bubble and selects "Reply", BlueBubbles passes `threadOriginatorGuid`. Golem traverses the chain to the thread root via `resolve_thread_root()`, rehydrates the preceding messages from that thread branch, and resumes the warm session. Hermes posts replies inline directly under the message bubble via `selectedMessageGuid`.
- **Testing:** Run `python -m unittest tests/test_bluebubbles_adapter.py`.
