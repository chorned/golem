#!/usr/bin/env python3
"""
first_mate/execution/mesh.py
Backwards compatibility mirror shim for golem.execution.mesh.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.execution.mesh import *  # noqa: F401, F403
import golem.execution.mesh as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
