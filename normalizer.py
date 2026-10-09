"""
normalizer.py
Backwards-compatibility shim for golem.transport.normalizer.

Legacy modules and tests importing directly from `normalizer` will receive
identical functions and constants re-exported from `golem.transport.normalizer`.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from golem.normalizer import *  # noqa: F401, F403
from golem.normalizer import (
    sanitize_for_slack,
    transpile_markdown_to_mrkdwn,
    transpile_markdown_to_slack,
    strip_terminal_chrome,
    strip_ansi,
    strip_tui_debris,
    strip_box_drawing,
    strip_prompt_echoes,
    protect_code_blocks,
    restore_code_blocks,
    ANSI_ESCAPE_PATTERN,
    BOX_DRAWING_PATTERN,
    BRAILLE_SPINNER_PATTERN,
    ELLIPSIS_CLOSE_PAREN_PATTERN,
    EXPAND_PAREN_PATTERN,
    STANDALONE_NUMBER_ELLIPSIS_PATTERN,
    PROMPT_MARKER_PATTERN,
    ASCII_SPINNER_PATTERN,
    TOOL_CRUMB_PREFIXES,
    NAV_BAR_PATTERN,
    TIP_PATTERN,
)

__all__ = [
    "sanitize_for_slack",
    "transpile_markdown_to_mrkdwn",
    "transpile_markdown_to_slack",
    "strip_terminal_chrome",
    "strip_ansi",
    "strip_tui_debris",
    "strip_box_drawing",
    "strip_prompt_echoes",
    "protect_code_blocks",
    "restore_code_blocks",
    "ANSI_ESCAPE_PATTERN",
    "BOX_DRAWING_PATTERN",
    "BRAILLE_SPINNER_PATTERN",
    "ELLIPSIS_CLOSE_PAREN_PATTERN",
    "EXPAND_PAREN_PATTERN",
    "STANDALONE_NUMBER_ELLIPSIS_PATTERN",
    "PROMPT_MARKER_PATTERN",
    "ASCII_SPINNER_PATTERN",
    "TOOL_CRUMB_PREFIXES",
    "NAV_BAR_PATTERN",
    "TIP_PATTERN",
]
