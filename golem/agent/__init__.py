#!/usr/bin/env python3
"""
golem/agent
Unified package for Headless Chief of Staff agent adapter, event protocol, and prompt calibrations.

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
from .mouth import (
    generate_mouth_response,
    get_mouth_model,
    MOUTH_MODEL_DEFAULT,
    is_cancellation_message,
    is_status_inquiry,
    is_conversational_checkin,
    is_conversational_inquiry,
    is_out_of_band_message,
)
from . import events
from . import prompts
from . import protocol
from . import calibration
from . import adapter
from . import mouth

__all__ = [
    # Submodules
    "events",
    "prompts",
    "protocol",
    "calibration",
    "adapter",
    "mouth",
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
    # Mouth LLM & Out-of-Band Reactor
    "generate_mouth_response",
    "get_mouth_model",
    "MOUTH_MODEL_DEFAULT",
    "is_cancellation_message",
    "is_status_inquiry",
    "is_conversational_checkin",
    "is_conversational_inquiry",
    "is_out_of_band_message",
]
