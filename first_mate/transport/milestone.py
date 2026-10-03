#!/usr/bin/env python3
"""
first_mate/transport/milestone.py
Backwards compatibility mirror shim for golem.transport.milestone.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.transport.milestone import *  # noqa: F401, F403
import golem.transport.milestone as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
