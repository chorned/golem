#!/usr/bin/env python3
"""
golem/gateway
Ingress gateway, thread-scoped session handles, and asynchronous session router for First Mate 2.0.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from .session import (
    SessionState,
    SessionHandle,
)
from .router import (
    SessionRouter,
)
from .ingress import (
    IngressGateway,
    add_reaction,
    remove_reaction,
    swap_reaction,
    strip_bot_mentions,
    strip_flushed_chunks,
    process_slack_message,
    IGNORABLE_REACTION_ERRORS,
)
from . import session
from . import router
from . import ingress

__all__ = [
    # Submodules
    "session",
    "router",
    "ingress",
    # Session
    "SessionState",
    "SessionHandle",
    # Router
    "SessionRouter",
    # Ingress
    "IngressGateway",
    "add_reaction",
    "remove_reaction",
    "swap_reaction",
    "strip_bot_mentions",
    "strip_flushed_chunks",
    "process_slack_message",
    "IGNORABLE_REACTION_ERRORS",
]
