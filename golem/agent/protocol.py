#!/usr/bin/env python3
"""
golem/agent/protocol.py
Strongly typed event hierarchy and NDJSON wire protocol for Headless Agent IPC.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List, Literal, Type, Union

AgentEventType = Literal[
    "base",
    "text_delta",
    "status_update",
    "tool_start",
    "tool_end",
    "awaiting_input",
    "finish",
]


@dataclass
class AgentEvent:
    """Base event emitted by the headless agent lifecycle."""
    event_type: str = "base"
    timestamp: float = field(default_factory=time.time)
    conversation_id: Optional[str] = None
    step_index: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to serializable dictionary."""
        d = asdict(self)
        return d

    def to_ndjson(self) -> str:
        """Serialize event to a single newline-terminated NDJSON string."""
        return json.dumps(self.to_dict()) + "\n"

    @classmethod
    def from_dict(cls: Type[AgentEvent], data: Any) -> AgentEvent:
        """
        Deserialize dictionary to a typed AgentEvent subclass based on event_type.
        If called directly on a subclass, instantiates that subclass.
        """
        if not isinstance(data, dict):
            raise ValueError(f"AgentEvent payload must be a dictionary, got {type(data).__name__}")

        event_type = data.get("event_type", "base")
        subclass_map: Dict[str, Type[AgentEvent]] = {
            "base": AgentEvent,
            "text_delta": TextDeltaEvent,
            "status_update": StatusUpdateEvent,
            "tool_start": ToolStartEvent,
            "tool_end": ToolEndEvent,
            "awaiting_input": AwaitingInputEvent,
            "finish": FinishEvent,
        }

        # If called on base AgentEvent, resolve target subclass
        target_cls = cls if cls is not AgentEvent else subclass_map.get(event_type, AgentEvent)

        # Extract known field names for target_cls
        known_fields = getattr(target_cls, "__dataclass_fields__", {})
        init_kwargs = {}
        for k, v in data.items():
            if k in known_fields:
                init_kwargs[k] = v

        # Special alias handling for TextDeltaEvent (text vs text_delta)
        if target_cls is TextDeltaEvent:
            if "text_delta" not in init_kwargs and "text" in data:
                init_kwargs["text_delta"] = data["text"]

        return target_cls(**init_kwargs)

    @classmethod
    def from_ndjson(cls: Type[AgentEvent], line: str) -> AgentEvent:
        """Deserialize a single NDJSON line to a typed AgentEvent."""
        if not isinstance(line, str):
            raise ValueError(f"Expected string for NDJSON line, got {type(line).__name__}")
        data = json.loads(line.strip())
        if not isinstance(data, dict):
            raise ValueError(f"Parsed NDJSON line is not a JSON object, got {type(data).__name__}")
        return cls.from_dict(data)


@dataclass
class TextDeltaEvent(AgentEvent):
    """Event emitted when incremental assistant text tokens are produced."""
    event_type: str = "text_delta"
    text_delta: str = ""

    def __init__(
        self,
        text: str = "",
        text_delta: str = "",
        event_type: str = "text_delta",
        **kwargs: Any,
    ) -> None:
        base_field_names = {f.name for f in getattr(AgentEvent, "__dataclass_fields__", {}).values()}
        base_kwargs = {k: v for k, v in kwargs.items() if k in base_field_names}
        super().__init__(event_type=event_type, **base_kwargs)
        self.text_delta = text or text_delta
        self.__post_init__()

    def __post_init__(self) -> None:
        if self.event_type != "text_delta":
            self.event_type = "text_delta"

    @property
    def text(self) -> str:
        """Alias for text_delta."""
        return self.text_delta

    @text.setter
    def text(self, value: str) -> None:
        self.text_delta = value


@dataclass
class StatusUpdateEvent(AgentEvent):
    """Event emitted when the agent transitions between lifecycle states."""
    event_type: str = "status_update"
    status: str = "working"  # "initializing" | "working" | "idle" | "awaiting_input"
    detail: Optional[str] = None

    def __post_init__(self) -> None:
        if self.event_type != "status_update":
            self.event_type = "status_update"


@dataclass
class ToolStartEvent(AgentEvent):
    """Event emitted when the agent initiates a tool call."""
    event_type: str = "tool_start"
    tool_name: str = ""
    tool_call_id: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.event_type != "tool_start":
            self.event_type = "tool_start"


@dataclass
class ToolEndEvent(AgentEvent):
    """Event emitted when a tool call completes execution."""
    event_type: str = "tool_end"
    tool_name: str = ""
    tool_call_id: Optional[str] = None
    output: Any = None
    duration_seconds: Optional[float] = None
    is_error: bool = False

    def __post_init__(self) -> None:
        if self.event_type != "tool_end":
            self.event_type = "tool_end"


@dataclass
class AwaitingInputEvent(AgentEvent):
    """Event emitted when the agent pauses awaiting user input or confirmation."""
    event_type: str = "awaiting_input"
    prompt: Optional[str] = None
    choices: Optional[List[str]] = None
    tool_name: Optional[str] = None

    def __post_init__(self) -> None:
        if self.event_type != "awaiting_input":
            self.event_type = "awaiting_input"


@dataclass
class FinishEvent(AgentEvent):
    """Event emitted when an agent turn completes with the final aggregated response."""
    event_type: str = "finish"
    response: str = ""
    status: str = "SUCCESS"  # "SUCCESS" | "ERROR"
    duration_seconds: Optional[float] = None
    usage: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if self.event_type != "finish":
            self.event_type = "finish"
