#!/usr/bin/env python3
"""
first_mate/agent/events.py
Re-exports AgentEvent and all typed event classes from first_mate.agent.protocol.
Shim to satisfy PROJECT.md interface contract.
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
