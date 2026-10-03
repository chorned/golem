#!/usr/bin/env python3
"""
golem/agent/adapter.py
Subprocess supervisor and NDJSON stream-json adapter for the Headless Chief of Staff engine.
Maintains 100% backward compatibility by subclassing AntigravityDriver.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import logging
from typing import Optional, List, Dict, Any, AsyncGenerator

from .protocol import (
    AgentEvent,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
)
from .drivers.antigravity import (
    AntigravityDriver,
    DEFAULT_AGY_BIN,
)

logger = logging.getLogger("Golem.Agent.Adapter")


class HeadlessAgentAdapter(AntigravityDriver):
    """
    Subprocess manager for agy in stream-json mode.
    Backward-compatible subclass of AntigravityDriver.
    Maintains persistent stdin/stdout pipes across multiple conversational turns,
    parses raw line-delimited NDJSON events, and emits strongly typed AgentEvent objects.
    """
    pass


__all__ = [
    "HeadlessAgentAdapter",
    "AntigravityDriver",
    "DEFAULT_AGY_BIN",
    "AgentEvent",
    "TextDeltaEvent",
    "StatusUpdateEvent",
    "ToolStartEvent",
    "ToolEndEvent",
    "AwaitingInputEvent",
    "FinishEvent",
]
