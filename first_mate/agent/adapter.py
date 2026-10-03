#!/usr/bin/env python3
"""
first_mate/agent/adapter.py
Backwards compatibility mirror shim for golem.agent.adapter.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.agent.adapter import *  # noqa: F401, F403
import golem.agent.adapter as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
