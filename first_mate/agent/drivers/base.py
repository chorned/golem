#!/usr/bin/env python3
"""
first_mate/agent/drivers/base.py
Backwards compatibility mirror shim for golem.agent.drivers.base.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.agent.drivers.base import *  # noqa: F401, F403
import golem.agent.drivers.base as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
