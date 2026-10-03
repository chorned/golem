#!/usr/bin/env python3
"""
first_mate/sensory/__init__.py
Backwards compatibility mirror shim for golem.sensory.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.sensory import *  # noqa: F401, F403
import golem.sensory as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
