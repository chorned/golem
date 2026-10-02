#!/usr/bin/env python3
"""
first_mate/agent
Unified package for Headless Chief of Staff agent adapter, event protocol, and prompt calibrations.
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
from .adapter import (
    HeadlessAgentAdapter,
    DEFAULT_AGY_BIN,
)
from .calibration import (
    CALIBRATION_PROMPT,
    SLACK_MRKDWN_RULES,
    CHOICE_FORMATTING_RULES,
    PACING_CALIBRATION_RULES,
    TRIAGE_SUPPRESSION_RULES,
    get_calibration_rules,
    build_calibrated_system_prompt,
)
from . import events
from . import prompts
from . import protocol
from . import calibration
from . import adapter

__all__ = [
    # Submodules
    "events",
    "prompts",
    "protocol",
    "calibration",
    "adapter",
    # Event protocol
    "AgentEvent",
    "AgentEventType",
    "TextDeltaEvent",
    "StatusUpdateEvent",
    "ToolStartEvent",
    "ToolEndEvent",
    "AwaitingInputEvent",
    "FinishEvent",
    # Adapter
    "HeadlessAgentAdapter",
    "DEFAULT_AGY_BIN",
    # Calibration
    "CALIBRATION_PROMPT",
    "SLACK_MRKDWN_RULES",
    "CHOICE_FORMATTING_RULES",
    "PACING_CALIBRATION_RULES",
    "TRIAGE_SUPPRESSION_RULES",
    "get_calibration_rules",
    "build_calibrated_system_prompt",
]
