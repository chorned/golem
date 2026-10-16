# Conversational Setup & Agent Onboarding Guide

> **"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."**

This guide is designed for an operator collaborating with an AI coding assistant (such as **Claude Code**, **OpenCode**, or **AGY**) to configure and deploy Golem from scratch.

When an AI assistant is asked to "help set up Golem", the agent should follow this document interactively, verifying prerequisites and conducting the interview one section at a time.

---

## Step 1: Verify Hard Prerequisites

Before starting the setup interview, the assistant must verify two core system prerequisites:

1. **Python 3.11+ Runtime**:
   ```bash
   python3 --version
   ```
   *Requirement*: Python version must be 3.11, 3.12, or 3.13.

2. **Herdr Daemon Mesh**:
   ```bash
   herdr daemon status
   ```
   *Requirement*: Herdr daemon must be active and responding. Herdr manages PTY persistence, dual-pane workspaces (Pane 1 Mouth, Pane 2 Hands), and cross-machine mesh connectivity. If Herdr is not installed or running, start it:
   ```bash
   herdr daemon start
   ```

3. **Virtual Environment Setup**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -e ".[dev]"
   ```

---

## Step 2: Interactive Setup Interview (Ask One Section at a Time)

The assistant should conduct the following conversational interview with the operator, asking questions section by section and never overwhelming the user with a single massive form.

### 1. Operator Identity
- **Ask the Operator:**
  > *"What do you want Golem to call you?"*
- **Action:**
  - Record response as `USER_NAME` (e.g., `Carl`, `Alice`).
  - Auto-detect operator home directory from environment: `USER_HOME=$HOME` (e.g., `/Users/hermes`).

### 2. Fleet Machines (Minimum 1 Machine in Herdr)
Golem requires at least one primary anchor machine running Herdr.
- **Ask the Operator:**
  > *"What's the name of your primary machine?"*
  *(The assistant can suggest names by running `herdr machine list` or reading `hostname`)*
  - Record response as `MACHINE_0` (e.g., `localhost` or machine name).
- **Ask for Additional Fleet Nodes:**
  > *"Do you have a second machine in your Herdr mesh? If so, what's its name? (Or say 'none' if you only have one machine)"*
  - If the user provides a machine, record as `MACHINE_1`, and ask for `MACHINE_2`, `MACHINE_3`, etc., until the user responds with `'none'` or indicates they are done.
  - *Note*: Emphasize to the operator that only `MACHINE_0` is required.

### 3. Per-Agent Model Selection & `/v1/` Endpoint
Explain the 4 organs briefly to the operator:
- **The Mouth (Liaison):** Front-of-house conversational agent in Pane 1 communicating with you.
- **The Hands (Worker):** Backstage runner executing code, commands, and tasks in Pane 2.
- **The Eyes (Observer):** Non-blocking observer monitoring Pane 2 terminal buffers.
- **The Nose (Sentinel):** 24/7 background watchdog monitoring system health and logs.

Ask the operator for their model choices and endpoint preferences:
- **API Key & Endpoint:**
  - `LLM_API_KEY`: The API key for your chosen provider.
  - `LLM_BASE_URL` *(optional)*: Any OpenAI-compatible `/v1/` endpoint (e.g., OpenRouter, OpenAI, Ollama, LiteLLM, vLLM). Leave blank to use native Google Gemini.
- **Model Assignments:**
  - `MOUTH_MODEL`: Flagship conversational model (e.g., `gemini-4-argon`, `claude-sonnet-4-5`, `gpt-4o`).
  - `EYES_MODEL` & `NOSE_MODEL`: Lightweight, cost-efficient observer models (e.g., `gemini-flash-lite-latest`, `gpt-4o-mini`).
  - `GOLEM_DEFAULT_HAND_WORKER`: Backstage execution worker (`claude_code`, `opencode`, or `agy`).
  - `HANDS_MODEL`: Execution model used by the backstage runner.

### 4. Slack / BlueBubbles Credentials
Ask the operator how they wish to communicate with Golem:
- **For Slack (Recommended):**
  - `SLACK_APP_TOKEN`: Socket Mode app-level token (`xapp-...`).
  - `SLACK_BOT_TOKEN`: Bot User OAuth token (`xoxb-...`).
  - `SLACK_SIGNING_SECRET`: App signing secret.
  - `DEFAULT_SLACK_CHANNEL`: Default channel ID for proactive briefings.
- **For BlueBubbles / iMessage (Optional):**
  - `BLUEBUBBLES_URL`, `BLUEBUBBLES_PASSWORD`, `BLUEBUBBLES_ALLOWED_HANDLES`.

---

## Step 3: Generate `.env` & `launchd` Service

Once the interview is complete, the assistant automatically writes the configuration files.

### 1. Write `.env`
Create `.env` at the project root based on the answers gathered during the interview:

```bash
# Operator Identity
USER_NAME="<OPERATOR_NAME>"
USER_HOME="<OPERATOR_HOME>"

# Fleet Topology
MACHINE_0="<PRIMARY_MACHINE>"
# MACHINE_1="<OPTIONAL_SECOND_MACHINE>"

# Sensory Plane Models & Endpoint
LLM_API_KEY="<YOUR_API_KEY>"
LLM_BASE_URL="<OPTIONAL_V1_ENDPOINT_OR_BLANK>"
MOUTH_MODEL="<CHOSEN_MOUTH_MODEL>"
EYES_MODEL="<CHOSEN_EYES_MODEL>"
NOSE_MODEL="<CHOSEN_NOSE_MODEL>"
GOLEM_DEFAULT_HAND_WORKER="<agy|claude_code|opencode>"
HANDS_MODEL="<CHOSEN_HANDS_MODEL>"

# Slack Integration
SLACK_APP_TOKEN="<YOUR_SLACK_APP_TOKEN>"
SLACK_BOT_TOKEN="<YOUR_SLACK_BOT_TOKEN>"
SLACK_SIGNING_SECRET="<YOUR_SLACK_SIGNING_SECRET>"
DEFAULT_SLACK_CHANNEL="<YOUR_DEFAULT_CHANNEL_ID>"

# Runtime Settings
GOLEM_ENABLE_WEBUI=0
GOLEM_LOG_LEVEL=INFO
```

### 2. Generate macOS `launchd` Service (`com.user.golem.plist`)
Copy `com.user.golem.plist` to `~/Library/LaunchAgents/` substituting the operator's real `$HOME`, virtualenv Python path, and environment variables:

```bash
mkdir -p ~/Library/LaunchAgents
sed -e "s|/Users/hermes|$HOME|g" \
    com.user.golem.plist > ~/Library/LaunchAgents/com.user.golem.plist

# Load and start the background daemon
launchctl unload ~/Library/LaunchAgents/com.user.golem.plist 2>/dev/null || true
launchctl load -w ~/Library/LaunchAgents/com.user.golem.plist
```

To verify the daemon is running:
```bash
launchctl list | grep golem
```

---

## Step 4: Blank-Slate Institutional Wiki (`~/.golem/wiki/`)

On first boot, Golem automatically creates a clean, git-backed institutional memory repository at `~/.golem/wiki/` (or the path set by `GOLEM_WIKI_DIR`):

- **Clean Slate Guarantee:** The wiki starts empty without hardcoded legacy machines or credentials.
- **Autonomous Learning:** Whenever you teach Golem something new in Slack with `/learn [notes]` or during conversation, Golem commits codified runbooks (`runbooks/`), node rules (`context/rules/`), and logbook entries (`logbook/`) directly to this repository.
- **Git Backed:** Every memory mutation is committed with a clear git commit hash and message, supporting rollbacks and auditing.

---

## Troubleshooting & Verification

To verify that Golem is properly configured:

```bash
# Run hygiene and configuration validation suite
python3 -m unittest tests/test_ci_and_repo_hygiene.py tests/test_documentation_and_manuals.py

# Verify sensory bus and wiki engine
python3 -m unittest tests/test_wiki_engine.py tests/test_memory_tiers.py
```
