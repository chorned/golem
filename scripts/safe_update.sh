#!/usr/bin/env bash
# scripts/safe_update.sh
# Automated verification gate and safe daemon reload for First Mate.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_DIR}"

echo "=== First Mate Safe Update Pre-Flight Gate ==="

# 1. Active task / worktree check
if ! git diff-index --quiet HEAD --; then
    echo "⚠️ Warning: Working directory has uncommitted changes. Stashing..."
    git stash push -m "safe_update_auto_stash_$(date +%s)"
fi

# 2. Automated test verification gate
echo "🧪 Running full test suite verification..."
if ! ./.venv/bin/python3 -m unittest discover tests; then
    echo "❌ TEST VERIFICATION FAILED! Aborting update to prevent regression."
    exit 1
fi

echo "✅ All tests passed cleanly."

# 3. Reload LaunchAgent daemon
echo "🔄 Reloading launchd daemon: com.user.first-mate..."
launchctl kickstart -k -p "gui/$(id -u)/com.user.first-mate"
sleep 1

# 4. Verify running daemon PID
DAEMON_PID=$(pgrep -f "python.*first-mate/app.py" || true)
if [ -n "${DAEMON_PID}" ]; then
    echo "🚀 First Mate is running cleanly under PID ${DAEMON_PID}."
else
    echo "⚠️ Warning: Daemon process not detected immediately. Check bot.err."
fi

echo "=== Update Complete ==="
