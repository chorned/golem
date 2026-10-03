#!/usr/bin/env python3
"""
first_mate/webui/__init__.py
Backwards compatibility mirror shim for golem.webui.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.webui import *  # noqa: F401, F403
import golem.webui as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
