#!/usr/bin/env bash
# scripts/rollback.sh
# Instant emergency rollback to a known golden tag or commit.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_DIR}"

TARGET_REF="${1:-v0.2.0-beta}"

echo "=== First Mate Emergency Rollback ==="
echo "Target Rollback Ref: ${TARGET_REF}"

# 1. Verify target ref exists in git
if ! git rev-parse --verify "${TARGET_REF}" >/dev/null 2>&1; then
    echo "❌ Error: Target ref '${TARGET_REF}' does not exist in git."
    echo "Available tags:"
    git tag -l
    exit 1
fi

CURRENT_SHA=$(git rev-parse HEAD)
echo "${CURRENT_SHA}" > "/tmp/golem_pre_rollback_sha"
echo "Saved pre-rollback commit: ${CURRENT_SHA}"

# 2. Stash any uncommitted work safely
if ! git diff-index --quiet HEAD --; then
    echo "Stashing uncommitted working tree state..."
    git stash push -m "emergency_rollback_stash_$(date +%s)"
fi

# 3. Checkout target ref
echo "Checking out ${TARGET_REF}..."
git checkout "${TARGET_REF}"

# 4. Quick sanity check
echo "Running smoke tests..."
./.venv/bin/python3 -m unittest tests/test_app.py tests/test_router_dual_pane.py

# 5. Kickstart LaunchAgent daemon
echo "Restarting background daemon..."
launchctl kickstart -k -p "gui/$(id -u)/com.user.golem"
sleep 1

DAEMON_PID=$(pgrep -f "python.*app.py" || true)
echo "✅ Rollback successful. Golem is active on ${TARGET_REF} (PID ${DAEMON_PID:-unknown})."
echo "To revert this rollback: git checkout main && launchctl kickstart -k -p gui/\$(id -u)/com.user.golem"
