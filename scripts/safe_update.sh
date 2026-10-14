#!/usr/bin/env bash
# scripts/safe_update.sh
# Automated verification gate, upstream sync, and Herdr recycling routine for Golem.

set -euo pipefail

HERDR_SOCKET="${HERDR_SOCKET_PATH:-$HOME/.config/herdr/herdr.sock}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$REPO_DIR/.venv/bin/python3"
VERIFY_ONLY=0
RECYCLE_HERDR_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --verify-only)
      VERIFY_ONLY=1
      shift
      ;;
    --recycle-herdr)
      RECYCLE_HERDR_ONLY=1
      shift
      ;;
    --no-reload)
      # Compatibility with existing callers
      shift
      ;;
    *)
      shift
      ;;
  esac
done

# Ensure standard user binary paths are in PATH
export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

# Resolve herdr binary
HERDR_BIN="$(command -v herdr || echo "")"
if [ -z "$HERDR_BIN" ] && [ -x "$HOME/.local/bin/herdr" ]; then
  HERDR_BIN="$HOME/.local/bin/herdr"
fi

# If only recycling herdr is requested
if [ "$RECYCLE_HERDR_ONLY" -eq 1 ]; then
  echo "🔄 Recycling Herdr PTY Multiplexer..."
  pkill -f "herdr (daemon|server)" 2>/dev/null || true
  sleep 0.5
  mkdir -p "$(dirname "$HERDR_SOCKET")"
  rm -f "$HERDR_SOCKET"

  if [ -n "$HERDR_BIN" ] && [ -x "$HERDR_BIN" ]; then
    nohup "$HERDR_BIN" server >/dev/null 2>&1 &
    SOCKET_READY=0
    for _ in {1..10}; do
      if [ -S "$HERDR_SOCKET" ]; then
        SOCKET_READY=1
        echo "✅ Herdr daemon active at $HERDR_SOCKET"
        break
      fi
      sleep 0.5
    done
    if [ "$SOCKET_READY" -eq 0 ]; then
      echo "⚠️ Warning: Herdr socket did not appear within 5 seconds."
    fi
  else
    echo "⚠️ Warning: herdr binary not found in PATH or ~/.local/bin."
  fi
  exit 0
fi

echo "🔍 [1/4] Running Pre-Flight Verification Tests..."

PYTHON_BIN="python3"
if [ -f "$VENV_PY" ]; then
  PYTHON_BIN="$VENV_PY"
fi

# Run pre-flight regression test battery via Python standard library unittest
# (Zero external dependencies; supports Python 3.8 - 3.14+ natively)
"$PYTHON_BIN" -m unittest \
  "$REPO_DIR/tests/test_app.py" \
  "$REPO_DIR/tests/test_herdr_fleet.py" \
  "$REPO_DIR/tests/test_react_loop.py" \
  "$REPO_DIR/tests/test_read_project_file.py" \
  "$REPO_DIR/tests/test_update_lifecycle.py"
echo "✅ Test battery passed."

if [ "$VERIFY_ONLY" -eq 1 ]; then
  exit 0
fi

echo "🔄 [2/4] Pulling Latest Upstream Code..."
cd "$REPO_DIR"
git pull --ff-only 2>/dev/null || {
  echo "⚠️ Warning: git pull failed or dirty working tree. Proceeding with local state."
}

echo "🔄 [3/4] Recycling Herdr PTY Multiplexer..."
# 1. Terminate orphaned daemons or stuck servers
pkill -f "herdr (daemon|server)" 2>/dev/null || true
sleep 0.5

# 2. Unlink stale socket to prevent bind collisions
mkdir -p "$(dirname "$HERDR_SOCKET")"
rm -f "$HERDR_SOCKET"

# 3. Start clean daemon
if [ -n "$HERDR_BIN" ] && [ -x "$HERDR_BIN" ]; then
  nohup "$HERDR_BIN" server >/dev/null 2>&1 &
  
  # 4. Assert socket readiness
  SOCKET_READY=0
  for _ in {1..10}; do
    if [ -S "$HERDR_SOCKET" ]; then
      SOCKET_READY=1
      echo "✅ Herdr daemon active at $HERDR_SOCKET"
      break
    fi
    sleep 0.5
  done

  if [ "$SOCKET_READY" -eq 0 ]; then
    echo "⚠️ Warning: Herdr socket did not appear within 5 seconds."
  fi
else
  echo "⚠️ Warning: herdr binary not found in PATH or ~/.local/bin."
fi

echo "🚀 [4/4] Update routines complete. Ready for daemon kickstart."
