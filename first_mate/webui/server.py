#!/usr/bin/env python3
"""
first_mate/webui/server.py
Backwards compatibility mirror shim for golem.webui.server.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.webui.server import *  # noqa: F401, F403
import golem.webui.server as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
