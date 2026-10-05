#!/usr/bin/env python3
"""
golem/config.py
Declarative Configuration & Environment Variable Reader.

Enforces ZERO hardcoded model names in Python files.
Resolves models and execution engines strictly from environment variables
or the project-level .env file, supporting dual-key resolution for Golem
and legacy First Mate configurations.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import time
import functools
from pathlib import Path
from typing import Optional, Dict, Any

try:
    from dotenv import load_dotenv, dotenv_values
except ImportError:
    load_dotenv = None
    dotenv_values = None

LINEAGE = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."

# Locate project root .env
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_PATH = _PROJECT_ROOT / ".env"

# Load environment on import if dotenv is available and .env exists
if load_dotenv and _ENV_PATH.exists():
    load_dotenv(dotenv_path=_ENV_PATH, override=False)
elif load_dotenv:
    load_dotenv(override=False)


@functools.lru_cache(maxsize=1)
def _get_raw_file_env(mtime: float, env_path: Path) -> Dict[str, str]:
    if dotenv_values:
        try:
            return {
                k: str(v)
                for k, v in dotenv_values(env_path).items()
                if v is not None and str(v).strip()
            }
        except Exception:
            return {}
    return {}


# In-memory TTL cache state for .env file resolution
_ENV_CACHE_TTL: float = float(os.environ.get("GOLEM_ENV_CACHE_TTL", "2.0"))
_FILE_ENV_CACHE: Optional[Dict[str, str]] = None
_FILE_ENV_CACHE_MTIME: Optional[float] = None
_FILE_ENV_LAST_CHECK: float = 0.0


def clear_env_cache() -> None:
    """Clear cached .env values to force re-reading from disk."""
    global _FILE_ENV_CACHE, _FILE_ENV_CACHE_MTIME, _FILE_ENV_LAST_CHECK
    _FILE_ENV_CACHE = None
    _FILE_ENV_CACHE_MTIME = None
    _FILE_ENV_LAST_CHECK = 0.0
    _get_raw_file_env.cache_clear()


def _get_file_env() -> Dict[str, str]:
    """
    Read key-value pairs directly from .env file on disk without modifying os.environ.
    Provides safe fallback when os.environ is cleared (e.g. in unit test fixtures).
    Backed by an in-memory mtime-validated cache with a lightweight TTL to eliminate
    synchronous disk I/O and stat() syscalls during high-frequency REST polling.
    Zero hardcoded strings in Python code.
    """
    global _FILE_ENV_CACHE, _FILE_ENV_CACHE_MTIME, _FILE_ENV_LAST_CHECK
    now = time.monotonic()

    # Fast-path: return cached dictionary within TTL window without any stat() syscalls
    if _FILE_ENV_CACHE is not None and (now - _FILE_ENV_LAST_CHECK) < _ENV_CACHE_TTL:
        if _FILE_ENV_CACHE_MTIME is not None:
            # Invoking _get_raw_file_env with cached mtime preserves cache_info hit tracking
            return dict(_get_raw_file_env(_FILE_ENV_CACHE_MTIME, _ENV_PATH))
        return dict(_FILE_ENV_CACHE)

    _FILE_ENV_LAST_CHECK = now
    if not dotenv_values:
        _FILE_ENV_CACHE = {}
        _FILE_ENV_CACHE_MTIME = None
        return {}

    # Single-stat optimization: catch OSError directly to eliminate redundant _ENV_PATH.exists()
    try:
        mtime = _ENV_PATH.stat().st_mtime
    except (OSError, ValueError):
        _FILE_ENV_CACHE = {}
        _FILE_ENV_CACHE_MTIME = None
        return {}

    _FILE_ENV_CACHE_MTIME = mtime
    _FILE_ENV_CACHE = dict(_get_raw_file_env(mtime, _ENV_PATH))
    return dict(_FILE_ENV_CACHE)


def reload_config() -> None:
    """Re-read .env file and update os.environ."""
    clear_env_cache()
    if load_dotenv and _ENV_PATH.exists():
        load_dotenv(dotenv_path=_ENV_PATH, override=True)


def get_default_hand_worker() -> str:
    """
    Return default hand worker (standardized on 'agy').
    Resolves from:
      1. os.environ['GOLEM_DEFAULT_HAND_WORKER']
      2. os.environ['FIRSTMATE_DEFAULT_HAND_WORKER'] / os.environ['FIRST_MATE_DEFAULT_HAND_WORKER']
      3. File .env fallback for the above variables
      4. Engine fallbacks (excluding engine-only drivers like claude_code)
      5. Defaults to 'agy'
    Standardizes 'antigravity' to 'agy'.
    """
    val = (
        (os.environ.get("GOLEM_DEFAULT_HAND_WORKER") or "").strip()
        or (os.environ.get("FIRSTMATE_DEFAULT_HAND_WORKER") or "").strip()
        or (os.environ.get("FIRST_MATE_DEFAULT_HAND_WORKER") or "").strip()
    )
    if not val:
        file_env = _get_file_env()
        val = (
            (file_env.get("GOLEM_DEFAULT_HAND_WORKER") or "").strip()
            or (file_env.get("FIRSTMATE_DEFAULT_HAND_WORKER") or "").strip()
            or (file_env.get("FIRST_MATE_DEFAULT_HAND_WORKER") or "").strip()
        )
    if not val:
        eng = (os.environ.get("GOLEM_ENGINE") or "").strip() or (os.environ.get("FIRST_MATE_ENGINE") or "").strip()
        if eng and eng.lower() not in ("claude_code", "claude"):
            val = eng
    if not val:
        file_env = _get_file_env()
        eng = (file_env.get("GOLEM_ENGINE") or "").strip() or (file_env.get("FIRST_MATE_ENGINE") or "").strip()
        if eng and eng.lower() not in ("claude_code", "claude"):
            val = eng

    normalized = (val or "").strip().lower()
    if not normalized or normalized == "antigravity":
        return "agy"
    return normalized


def get_active_engine() -> str:
    """
    Return active execution engine name.
    Resolves from GOLEM_DEFAULT_HAND_WORKER -> GOLEM_ENGINE -> FIRST_MATE_ENGINE.
    Defaults to 'antigravity'.
    """
    val = (
        (os.environ.get("GOLEM_DEFAULT_HAND_WORKER") or "").strip()
        or (os.environ.get("GOLEM_ENGINE") or "").strip()
        or (os.environ.get("FIRST_MATE_ENGINE") or "").strip()
    )
    if not val:
        file_env = _get_file_env()
        val = (
            (file_env.get("GOLEM_DEFAULT_HAND_WORKER") or "").strip()
            or (file_env.get("GOLEM_ENGINE") or "").strip()
            or (file_env.get("FIRST_MATE_ENGINE") or "").strip()
        )
    return (val or "antigravity").lower().strip()


def get_engine_model(engine: Optional[str] = None) -> str:
    """
    Resolve model name for specified engine directly from environment or .env.
    Maps:
      - 'antigravity' / 'agy' -> GOLEM_ANTIGRAVITY_MODEL / ANTIGRAVITY_MODEL
      - 'claude_code' / 'claude' -> GOLEM_CLAUDE_CODE_MODEL / CLAUDE_CODE_MODEL
      - 'opencode' -> GOLEM_OPENCODE_MODEL / OPENCODE_MODEL
    Zero hardcoded model names in code.
    """
    target = (engine or get_active_engine()).lower().strip()
    var_candidates: Dict[str, list[str]] = {
        "antigravity": ["GOLEM_ANTIGRAVITY_MODEL", "ANTIGRAVITY_MODEL"],
        "agy": ["GOLEM_ANTIGRAVITY_MODEL", "ANTIGRAVITY_MODEL"],
        "claude_code": ["GOLEM_CLAUDE_CODE_MODEL", "CLAUDE_CODE_MODEL"],
        "claude": ["GOLEM_CLAUDE_CODE_MODEL", "CLAUDE_CODE_MODEL"],
        "opencode": ["GOLEM_OPENCODE_MODEL", "OPENCODE_MODEL"],
    }
    candidates = var_candidates.get(target)
    if not candidates:
        return ""

    for var in candidates:
        val = (os.environ.get(var) or "").strip()
        if val:
            return val

    file_env = _get_file_env()
    for var in candidates:
        val = (file_env.get(var) or "").strip()
        if val:
            return val

    return ""


def get_mouth_model() -> str:
    """
    Resolve Mouth model from GOLEM_MOUTH_MODEL, MOUTH_MODEL, or GEMINI_MODEL.
    Zero hardcoded model names in code.
    """
    candidates = ["GOLEM_MOUTH_MODEL", "MOUTH_MODEL", "GEMINI_MODEL"]
    for var in candidates:
        val = (os.environ.get(var) or "").strip()
        if val:
            return val

    file_env = _get_file_env()
    for var in candidates:
        val = (file_env.get(var) or "").strip()
        if val:
            return val

    return ""


def get_eyes_model() -> str:
    """
    Resolve Eyes model from GOLEM_EYES_MODEL or EYES_MODEL.
    Zero hardcoded model names in code.
    """
    candidates = ["GOLEM_EYES_MODEL", "EYES_MODEL"]
    for var in candidates:
        val = (os.environ.get(var) or "").strip()
        if val:
            return val

    file_env = _get_file_env()
    for var in candidates:
        val = (file_env.get(var) or "").strip()
        if val:
            return val

    return ""


def get_nose_model() -> str:
    """
    Resolve Nose model from GOLEM_NOSE_MODEL or NOSE_MODEL.
    Zero hardcoded model names in code.
    """
    candidates = ["GOLEM_NOSE_MODEL", "NOSE_MODEL"]
    for var in candidates:
        val = (os.environ.get(var) or "").strip()
        if val:
            return val

    file_env = _get_file_env()
    for var in candidates:
        val = (file_env.get(var) or "").strip()
        if val:
            return val

    return ""


def get_host_dir() -> str:
    """Resolve project host directory with dual-key resolution."""
    val = (os.environ.get("GOLEM_HOST_DIR") or "").strip() or (os.environ.get("FIRST_MATE_HOST_DIR") or "").strip()
    return val or str(_PROJECT_ROOT)


def get_daemon_label() -> str:
    """Resolve daemon label with dual-key resolution."""
    val = (os.environ.get("GOLEM_DAEMON_LABEL") or "").strip() or (os.environ.get("DAEMON_LABEL") or "").strip()
    return val or "com.user.golem"



def is_webui_enabled() -> bool:
    """
    Check if WebUI server is enabled via environment variables.
    Checks: GOLEM_ENABLE_WEBUI, GOLEM_WEBUI, FIRST_MATE_ENABLE_WEBUI, FIRSTMATE_ENABLE_WEBUI.
    Truthy values: '1', 'true', 'yes', 'on', 'enable' (case-insensitive, whitespace-stripped).
    """
    candidates = [
        "GOLEM_ENABLE_WEBUI",
        "GOLEM_WEBUI",
        "FIRST_MATE_ENABLE_WEBUI",
        "FIRSTMATE_ENABLE_WEBUI",
    ]
    for var in candidates:
        if var in os.environ:
            val = (os.environ.get(var) or "").strip().lower()
            return val in ("1", "true", "yes", "on", "enable")

    return False


def get_webui_host() -> str:
    """Return WebUI host address (default: '127.0.0.1')."""
    candidates = [
        "GOLEM_WEBUI_HOST",
        "WEBUI_HOST",
        "FIRST_MATE_WEBUI_HOST",
        "FIRSTMATE_WEBUI_HOST",
    ]
    for var in candidates:
        val = (os.environ.get(var) or "").strip()
        if val:
            return val
    return "127.0.0.1"


def get_webui_port() -> int:
    """Return WebUI port number (default: 8765)."""
    candidates = [
        "GOLEM_WEBUI_PORT",
        "WEBUI_PORT",
        "FIRST_MATE_WEBUI_PORT",
        "FIRSTMATE_WEBUI_PORT",
    ]
    for var in candidates:
        val = (os.environ.get(var) or "").strip()
        if val:
            try:
                return int(val)
            except ValueError:
                pass
    return 8765


def get_config_dict() -> Dict[str, str]:
    """Return dictionary snapshot of all sensory, engine, and webui settings."""
    return {
        "active_engine": get_active_engine(),
        "default_hand_worker": get_default_hand_worker(),
        "engine_model": get_engine_model(),
        "mouth_model": get_mouth_model(),
        "eyes_model": get_eyes_model(),
        "nose_model": get_nose_model(),
        "host_dir": get_host_dir(),
        "daemon_label": get_daemon_label(),
        "webui_enabled": str(is_webui_enabled()),
        "webui_host": get_webui_host(),
        "webui_port": str(get_webui_port()),
    }

