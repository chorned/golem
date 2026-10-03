#!/usr/bin/env python3
"""
first_mate/gateway/ingress.py
Backwards compatibility mirror shim for golem.gateway.ingress.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from golem.gateway.ingress import *  # noqa: F401, F403
import golem.gateway.ingress as _canonical

__all__ = getattr(_canonical, "__all__", [k for k in dir(_canonical) if not k.startswith("_")])
