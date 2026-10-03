#!/usr/bin/env python3
"""
first_mate/transport/file_transfer.py
Backwards compatibility mirror shim for golem.transport.file_transfer.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.transport.file_transfer import *  # noqa: F401, F403
import golem.transport.file_transfer as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
