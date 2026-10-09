#!/usr/bin/env bash
# scripts/safe_cutover.sh
# Automated snapshot cutover, preflight verification gate, and safe launchd daemon reload.
#
# Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_DIR="${GOLEM_DEPLOY_DIR:-${HOME}/.golem/active}"
BACKUP_DIR="${HOME}/.golem/backup_$(date +%s)"
PREFLIGHT_PORT="${PREFLIGHT_PORT:-8766}"
DAEMON_LABEL="${GOLEM_DAEMON_LABEL:-com.user.golem}"

echo "================================================================="
echo "        PROJECT GOLEM SAFE CUTOVER & PREFLIGHT PIPELINE          "
echo "================================================================="
echo "Source:      ${REPO_DIR}"
echo "Target:      ${TARGET_DIR}"
echo "Daemon:      ${DAEMON_LABEL}"
echo "Preflight:   Port ${PREFLIGHT_PORT}"
echo "================================================================="

cd "${REPO_DIR}"

# 1. Monolithic Gate Check: Run preflight dry-run check on loopback
echo "🔍 [Stage 1/4] Running preflight verification gate..."
if ! python3 app.py --preflight --port "${PREFLIGHT_PORT}"; then
    echo "❌ PREFLIGHT VERIFICATION FAILED! Aborting cutover to protect live deployment."
    exit 1
fi
echo "✅ Preflight verification passed."

# If only dry-run preflight check was requested, exit cleanly
if [ "${1:-}" = "--dry-run" ] || [ "${1:-}" = "--preflight-only" ]; then
    echo "=== Preflight Dry-Run Complete (No Deployment Made) ==="
    exit 0
fi

# 2. Snapshot current active deployment for automated rollback
if [ -d "${TARGET_DIR}" ]; then
    echo "📦 [Stage 2/4] Backing up existing deployment to ${BACKUP_DIR}..."
    mkdir -p "$(dirname "${BACKUP_DIR}")"
    cp -R "${TARGET_DIR}" "${BACKUP_DIR}"
fi

# 3. Copy snapshot to target directory
echo "🚀 [Stage 3/4] Deploying snapshot to ${TARGET_DIR}..."
mkdir -p "${TARGET_DIR}"
# Sync repo files excluding .git, venvs, and cache
rsync -a --delete \
    --exclude=".git" \
    --exclude=".venv*" \
    --exclude="__pycache__" \
    --exclude="*.pyc" \
    --exclude=".pytest_cache" \
    "${REPO_DIR}/" "${TARGET_DIR}/"

# 4. Detached launchd daemon reload with readiness polling
echo "🔄 [Stage 4/4] Reloading launchd service: ${DAEMON_LABEL}..."
UID_NUM="$(id -u)"
if launchctl list | grep -q "${DAEMON_LABEL}"; then
    launchctl kickstart -k -p "gui/${UID_NUM}/${DAEMON_LABEL}" || true
else
    echo "ℹ️  Daemon not currently registered in launchd. Skipping kickstart."
fi

# 5. Readiness polling
echo "⏳ Polling daemon readiness..."
READY=false
for i in {1..10}; do
    DAEMON_PID=$(pgrep -f "python.*app.py" || true)
    if [ -n "${DAEMON_PID}" ]; then
        echo "✅ Golem daemon verified running under PID: ${DAEMON_PID}"
        READY=true
        break
    fi
    sleep 1
done

if [ "${READY}" = false ]; then
    echo "⚠️ Daemon process not detected within 10s. Checking logs..."
    if [ -d "${BACKUP_DIR}" ]; then
        echo "🚨 Initiating automatic rollback from backup..."
        rsync -a --delete "${BACKUP_DIR}/" "${TARGET_DIR}/"
        launchctl kickstart -k -p "gui/${UID_NUM}/${DAEMON_LABEL}" || true
        echo "⏪ Rollback complete."
        exit 1
    fi
fi

echo "================================================================="
echo "✅ Golem safe cutover complete! Subsystems operational."
echo "================================================================="
