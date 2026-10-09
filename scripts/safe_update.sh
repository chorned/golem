#!/usr/bin/env bash
# scripts/safe_update.sh
# Automated verification gate and safe daemon reload for First Mate.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_DIR}"

echo "=== First Mate Safe Update Pre-Flight Gate ==="

# 1. Automated test verification gate
echo "🧪 Running test suite verification..."
if ! env DUAL_PANE_WORKSPACE=0 ./.venv/bin/python3 -m unittest tests/test_app.py tests/test_herdr_fleet.py tests/test_agent_stream_sync.py; then
    echo "❌ TEST VERIFICATION FAILED! Aborting update to prevent regression."
    exit 1
fi

echo "✅ Core test verification passed cleanly."

# If caller only requested test verification (e.g. gateway daemon handling its own reload), exit cleanly
if [ "${1:-}" = "--verify-only" ] || [ "${1:-}" = "--no-reload" ]; then
    echo "=== Verification Complete (No Reload Requested) ==="
    exit 0
fi

# 2. Reload LaunchAgent daemon
echo "🔄 Reloading launchd daemon: com.user.golem..."
launchctl kickstart -k -p "gui/$(id -u)/com.user.golem"
sleep 1

# 3. Verify running daemon PID
DAEMON_PID=$(pgrep -f "python.*app.py" || true)
if [ -n "${DAEMON_PID}" ]; then
    echo "🚀 First Mate is running cleanly under PID ${DAEMON_PID}."
else
    echo "⚠️ Warning: Daemon process not detected immediately. Check bot.err."
fi

echo "=== Update Complete ==="
