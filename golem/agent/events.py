#!/usr/bin/env python3
"""
golem/agent/events.py
Re-exports AgentEvent and all typed event classes from golem.agent.protocol.
Shim to satisfy PROJECT.md interface contract.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from .protocol import (
    AgentEvent,
    AgentEventType,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
)

__all__ = [
    "AgentEvent",
    "AgentEventType",
    "TextDeltaEvent",
    "StatusUpdateEvent",
    "ToolStartEvent",
    "ToolEndEvent",
    "AwaitingInputEvent",
    "FinishEvent",
]
