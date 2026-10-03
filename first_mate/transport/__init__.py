#!/usr/bin/env python3
"""
first_mate/transport/__init__.py
Backwards compatibility mirror shim for golem.transport.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.transport import *  # noqa: F401, F403
import golem.transport as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
