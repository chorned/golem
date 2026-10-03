#!/usr/bin/env python3
"""
first_mate/agent/__init__.py
Backwards compatibility mirror shim for golem.agent.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.agent import *  # noqa: F401, F403
import golem.agent as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
