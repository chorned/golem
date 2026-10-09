---
name: on-demand-messaging
description: Operational runbook and tool guide for Golem to message Carl on demand in Slack, iMessage, or both when attention, approval, or decision is required, with full thread continuity.
---

# Golem On-Demand Messaging Skill

Golem can proactively initiate contact with Carl on demand across Slack and iMessage whenever human attention, approval, or an executive decision is required.

---

## 1. Channels & Addressing

| Platform | Default Target | Delivery Mechanism |
| :--- | :--- | :--- |
| **Slack** | Carl's 1:1 DM (`D0C2J1HKNN7`) | Slack API (`chat_postMessage`) via `SLACK_BOT_TOKEN` |
| **iMessage** | Carl's Apple ID (`iMessage;-;k.horned@gmail.com`) | BlueBubbles API (`/api/v1/message/text`) via macOS server |

---

## 2. Thread Continuity Architecture

When Golem messages Carl on demand, Carl must be able to reply directly to the message and continue the conversation seamlessly:

1. **Slack Thread Continuation:**
   - When Golem sends a message, Slack returns a message timestamp (`ts`).
   - Golem registers the session key `D0C2J1HKNN7:<ts>` in `~/.gemini/golem/sessions_registry.json`.
   - When Carl replies to the thread in Slack, Golem's `SessionRouter` matches `channel:thread_ts`, rehydrates the warm session, fetches previous thread context via `conversations_replies`, and answers within that exact thread.

2. **iMessage Thread Continuation:**
   - When Golem sends an iMessage, BlueBubbles returns the message GUID.
   - The message is recorded in Golem's local conversation history cache (`~/.gemini/golem/imessage_history/`).
   - When Carl replies (either using Apple's inline reply or in the 1:1 chat), Golem matches the history and provides `[Previous iMessage Conversation Context:]` to the Mouth LLM, maintaining full conversational grounding.

---

## 3. Usage Methods

### Method A: Command-Line Tool (`scripts/message_carl.py`)
Run from terminal or background runner:
```bash
# Message Carl on both Slack and iMessage (Default)
python scripts/message_carl.py --target both --message "Carl, tests passed on hermes. Ready to deploy?"

# Message Slack only
python scripts/message_carl.py --target slack --message "Carl, need your approval on PR #42."

# Message iMessage only
python scripts/message_carl.py --target imessage --message "Carl, homelab backup completed."

# Continue an existing Slack thread
python scripts/message_carl.py --target slack --slack-thread-ts 1740000000.123456 --message "Update: container restarted."
```

### Method B: Python API (`golem.notify`)
Import directly in any script, service, or task:
```python
from golem.notify import send_on_demand_message, send_slack_message, send_imessage

# Async call
res = await send_on_demand_message(
    text="Carl, runner encountered an ambiguous git conflict on dev-grid.",
    target="both",  # "slack", "imessage", or "both"
)

# Synchronous convenience call
from golem.notify import send_on_demand_message_sync

res = send_on_demand_message_sync(
    text="Carl, please check the build status.",
    target="both",
)
```

---

## 4. Best Practices for On-Demand Messages

- **Direct & High-Signal:** State the situation and the bottom line first.
- **Specific Decision / Action:** Tell Carl exactly what choice or confirmation is needed.
- **Clean Formatting:** Use Slack mrkdwn (`*bold*`, inline `code`, clean bullets). Avoid raw tracebacks or ANSI escapes.
