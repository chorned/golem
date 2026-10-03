#!/usr/bin/env python3
"""
first_mate/execution/__init__.py
Backwards compatibility mirror shim for golem.execution.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.execution import *  # noqa: F401, F403
import golem.execution as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
