"""
first_mate.transport - Wire normalization, message chunking, milestone updates, and Slack transport client.
"""

from first_mate.transport.normalizer import (
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
)
from first_mate.transport.chunker import (
    split_message_boundary_aware,
    parse_code_blocks,
    CodeBlockSpan,
)
from first_mate.transport.milestone import (
    MilestoneUpdater,
)
from first_mate.transport.slack_client import (
    SlackClient,
    SlackTransport,
    SlackTransportError,
    SlackRateLimitError,
    SlackMessageNotFoundError,
    SlackAuthenticationError,
)
from first_mate.transport.file_transfer import (
    extract_and_strip_file_directives,
    resolve_file_path,
    resolve_local_file_path,
    fetch_remote_file,
    upload_directive_files,
)

__all__ = [
    # Wire Normalization
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
    # Chunker
    "split_message_boundary_aware",
    "parse_code_blocks",
    "CodeBlockSpan",
    # Milestone
    "MilestoneUpdater",
    # Slack Client & Transport
    "SlackClient",
    "SlackTransport",
    "SlackTransportError",
    "SlackRateLimitError",
    "SlackMessageNotFoundError",
    "SlackAuthenticationError",
    # File Directives & Outbound File Transfer
    "extract_and_strip_file_directives",
    "resolve_file_path",
    "resolve_local_file_path",
    "fetch_remote_file",
    "upload_directive_files",
]
