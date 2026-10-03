#!/usr/bin/env python3
"""
first_mate/sensory/nose.py
Backwards compatibility mirror shim for golem.sensory.nose.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.sensory.nose import *  # noqa: F401, F403
import golem.sensory.nose as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
