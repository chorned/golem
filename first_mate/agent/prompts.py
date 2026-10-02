#!/usr/bin/env python3
"""
first_mate/agent/prompts.py
Re-exports prompt calibrations and system prompt builder from first_mate.agent.calibration.
Shim to satisfy PROJECT.md interface contract.
"""

from .calibration import (
    CALIBRATION_PROMPT,
    SLACK_MRKDWN_RULES,
    CHOICE_FORMATTING_RULES,
    PACING_CALIBRATION_RULES,
    TRIAGE_SUPPRESSION_RULES,
    DUAL_PANE_EXECUTION_RULES,
    get_calibration_rules,
    build_calibrated_system_prompt,
)

__all__ = [
    "CALIBRATION_PROMPT",
    "SLACK_MRKDWN_RULES",
    "CHOICE_FORMATTING_RULES",
    "PACING_CALIBRATION_RULES",
    "TRIAGE_SUPPRESSION_RULES",
    "DUAL_PANE_EXECUTION_RULES",
    "get_calibration_rules",
    "build_calibrated_system_prompt",
]
