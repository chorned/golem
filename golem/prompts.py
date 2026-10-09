#!/usr/bin/env python3
"""
golem/agent/prompts.py
Re-exports prompt calibrations and system prompt builder from golem.agent.calibration.
Shim to satisfy PROJECT.md interface contract.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from .calibration import (
    CALIBRATION_PROMPT,
    SLACK_MRKDWN_RULES,
    CHOICE_FORMATTING_RULES,
    PACING_CALIBRATION_RULES,
    TRIAGE_SUPPRESSION_RULES,
    DUAL_PANE_EXECUTION_RULES,
    OPERATIONAL_MEMORY_RULES,
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
    "OPERATIONAL_MEMORY_RULES",
    "get_calibration_rules",
    "build_calibrated_system_prompt",
]
