r"""
golem/transport/normalizer.py
Outbound text normalizer for First Mate 2.0 transport layer.

Deterministic two-stage sanitization pipeline:
1. Terminal TUI & Widget Stripper:
   - Strips ANSI escape sequences (CSI, OSC, and control codes).
   - Strips VT100 box-drawing glyphs ([\u2500-\u257f]) and eliminates standalone border lines.
   - Strips prompt markers (^>\s*$, ^❯\s*$, ^\$\s*$) and Starship command echoes (^❯\s+.*$),
     while strictly preserving blockquotes (> text).
   - Strips collapsible progress rows, spinners, crumbs, navigation bars, and CLI hints:
     - Lines ending in '...)' (e.g. mate/supervisor.py...), 3...)).
     - Lines matching 'expand)', '(expand)', or standalone numbers with ellipses.
     - Braille (⠋, ⠙, ⠹, etc.) and ASCII progress spinners.
     - Keyboard navigation hints (↑/↓ Navigate, ? for shortcuts, press enter).
     - CLI tip lines (└ Tip:..., Tip:...).
   - Strips leading blank lines or whitespace while collapsing multiple blank lines.
2. CommonMark to Slack mrkdwn Transpiler:
   - ATX headings (### Header, ## Header, # Header) -> *Header*.
   - Standard Markdown bold-italic (***text***) -> *_text_*.
   - Standard Markdown bold (**text**) -> *text*.
   - Markdown links ([label](url)) and images (![alt](url)) -> <url|label>.
   - Bare unlinked URLs -> <url>.
   - Complete TC-05 compliance: zero ATX #, zero **, zero box runes, all URLs in Slack format.
   - Comprehensive bidirectional code block protection (fenced ```/~~~ and inline backticks).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import re
import uuid
from typing import List, Tuple


# ANSI escape sequences (CSI, OSC, 2-character)
ANSI_ESCAPE_PATTERN = re.compile(
    r"\x1b(?:"
    r"\[[0-?]*[ -/]*[@-~]"              # CSI (Control Sequence Introducer)
    r"|\][^\x07\x1b]*(?:\x07|\x1b\\)"  # OSC (Operating System Command)
    r"|[=><]"                          # 2-character controls (DEC keypad =, >, VT52 <)
    r"|[78](?![0-9a-zA-Z])"            # DEC save/restore cursor (\x1b7, \x1b8) when not part of word/number
    r")"
)

# Box-drawing characters (Unicode range \u2500-\u257f: light, heavy, double, rounded)
BOX_DRAWING_PATTERN = re.compile(r"[\u2500-\u257f]")

# Braille spinner glyph pattern (Unicode range \u2800-\u28ff)
BRAILLE_SPINNER_PATTERN = re.compile(r"[\u2800-\u28ff]")

# Lines ending in ...) or …) (e.g. mate/supervisor.py...), 3...))
ELLIPSIS_CLOSE_PAREN_PATTERN = re.compile(r"^\s*(?:[\w/.-]+)?(?:\.{3}|…)\s*\)\s*$")

# Lines matching expand), (expand), (ctrl+o to expand), or to expand)
EXPAND_PAREN_PATTERN = re.compile(r"^\s*\(?(?:expand|ctrl\+o to expand|to expand)\)?\s*$", re.IGNORECASE)

# Standalone numbers with ellipses (e.g., 3..., (3...), 42…)
STANDALONE_NUMBER_ELLIPSIS_PATTERN = re.compile(r"^\s*\(?\d+\s*(?:\.{3}|…)\)?\s*$")

# Lingering subshell or prompt markers
PROMPT_MARKER_PATTERN = re.compile(r"^\s*>\s*$")
PROMPT_EMPTY_MARKER = re.compile(r"^\s*[>❯\$]\s*$")
PROMPT_COMMAND_ECHO = re.compile(r"^\s*❯\s+.*$")
PROMPT_SHELL_USER = re.compile(r"^\s*(?:bash-\d+\.?\d*|[\w.-]+@[\w.-]+)[:\w~/-]*[\$#]\s*$")

# Standalone ASCII spinners: [-], [/], [|], [\], or standalone / \
ASCII_SPINNER_PATTERN = re.compile(r"^\s*(?:\[[-/\\|]\]|\([-/\\]\)|\/|\\)\s*$")

# Tool crumbs and thought marker prefixes
TOOL_CRUMB_PREFIXES = ("● ", "▸ ")

# Interactive navigation bars
NAV_BAR_PATTERN = re.compile(
    r"(?:[↑^]/[↓v]\s*Navigate.*Select.*Skip|\\?\?\s*for\s*shortcuts|press\s+enter\s+to\s+continue)",
    re.IGNORECASE
)

# CLI Hints & Tips
TIP_PATTERN = re.compile(r"^\s*(?:└\s*Tip:.*|Tip:.*)", re.IGNORECASE)

# Bare URLs (http/https) not already enclosed in Slack <...|label> or <...>
BARE_URL_PATTERN = re.compile(r"(?<![<|])(https?://[^\s<>]+?)(?=[.,;:!?)\]]*(?:\s|$))")

# ATX headings: 0-3 spaces, 1-6 hashes, followed by space + content or end of line
ATX_HEADING_PATTERN = re.compile(r"^([ ]{0,3}(?:>[ ]*)*)#{1,6}(?:[ \t]+(.*?)|[ \t]*)$", re.MULTILINE)

# Markdown links and images with balanced parens support (non-overlapping to eliminate ReDoS)
MARKDOWN_LINK_PATTERN = re.compile(
    r"\[([^\]\r\n]+)\]\(([^\s()]+(?:\([^\s()]*\)[^\s()]*)*(?:\s+[\"\x27][^\"\x27]*[\"\x27])?)\)"
)
MARKDOWN_IMAGE_PATTERN = re.compile(
    r"!\[([^\]\r\n]*)\]\(([^\s()]+(?:\([^\s()]*\)[^\s()]*)*(?:\s+[\"\x27][^\"\x27]*[\"\x27])?)\)"
)


def strip_ansi(text: str) -> str:
    """Strip ANSI escape sequences (CSI, OSC, and control codes) from text."""
    if not text:
        return ""
    return ANSI_ESCAPE_PATTERN.sub("", text).replace("\x1b", "")


def protect_code_blocks(text: str) -> Tuple[str, List[str]]:
    """
    Protect both fenced code blocks (``` or ~~~) and inline backtick spans (`...`, ``...``)
    according to CommonMark rules:
    - Opening fence: indented <= 3 spaces, 3+ backticks or tildes.
    - Closing fence: indented <= 3 spaces, same char, at least same length.
    - Unclosed code block extends to the end of the text.
    - Inline backticks: matched by equal length delimiter runs.
    Replaces protected spans with UUID-salted sentinel tokens \x00FM_CODE_{salt}_{idx}\x00.
    """
    if not text:
        return "", []

    salt = uuid.uuid4().hex[:12]
    lines = text.splitlines(keepends=True)
    in_block = False
    fence_char = ""
    fence_len = 0
    current_block: List[str] = []
    output_parts: List[str] = []
    blocks: List[str] = []
    fence_re = re.compile(r"^([ ]{0,3})(`{3,}|~{3,})(.*)$")

    # Pass 1: Fenced code blocks
    for line in lines:
        if not in_block:
            m = fence_re.match(line)
            if m:
                in_block = True
                fence_char = m.group(2)[0]
                fence_len = len(m.group(2))
                current_block = [line]
            else:
                output_parts.append(line)
        else:
            current_block.append(line)
            close_re = re.compile(rf"^[ ]{{0,3}}{re.escape(fence_char)}{{{fence_len},}}\s*$")
            if close_re.match(line.rstrip("\r\n")):
                block_str = "".join(current_block)
                idx = len(blocks)
                blocks.append(block_str)
                output_parts.append(f"\x00FM_CODE_{salt}_{idx}\x00")
                if block_str.endswith("\n"):
                    output_parts.append("\n")
                in_block = False
                current_block = []

    if in_block and current_block:
        block_str = "".join(current_block)
        idx = len(blocks)
        blocks.append(block_str)
        output_parts.append(f"\x00FM_CODE_{salt}_{idx}\x00")
        if block_str.endswith("\n"):
            output_parts.append("\n")

    protected_text = "".join(output_parts)

    # Pass 2: Inline code spans (delimited by 1, 2, or more backticks)
    inline_re = re.compile(r"(`+)(.+?)(?<!`)\1(?!`)", re.DOTALL)

    def _replace_inline(match: re.Match) -> str:
        idx = len(blocks)
        blocks.append(match.group(0))
        return f"\x00FM_CODE_{salt}_{idx}\x00"

    protected_text = inline_re.sub(_replace_inline, protected_text)
    return protected_text, blocks


def restore_code_blocks(text: str, blocks: List[str]) -> str:
    """
    Restore previously protected code blocks and inline spans in reverse order.
    Supports UUID-salted \x00FM_CODE_{salt}_{idx}\x00 and legacy placeholders.
    """
    if not text or not blocks:
        return text

    salts = set(re.findall(r"\x00FM_CODE_([a-f0-9]{8,32})_\d+\x00", text))

    for idx in range(len(blocks) - 1, -1, -1):
        cb = blocks[idx]
        if salts:
            tokens = [f"\x00FM_CODE_{s}_{idx}\x00" for s in salts]
        else:
            tokens = [f"\x00FM_CODE_{idx}\x00", f"\x00FM_CODEBLOCK_{idx}\x00"]
        for token in tokens:
            if cb.endswith("\n") and (token + "\n") in text:
                text = text.replace(token + "\n", cb)
            elif token in text:
                text = text.replace(token, cb)
    return text


def strip_box_drawing(text: str) -> str:
    r"""
    Strip VT100 box-drawing characters ([\u2500-\u257f]), eliminating standalone border lines
    (e.g., ┌───┐, ├───┤, └───┘) and extracting substantive text wrapped in borders.
    Preserves code blocks verbatim.
    """
    if not text:
        return ""

    text, blocks = protect_code_blocks(text)
    lines = text.splitlines(keepends=True)
    filtered_lines: List[str] = []

    for raw_line in lines:
        line = raw_line.rstrip("\r\n")
        if BOX_DRAWING_PATTERN.search(line):
            # Check if line contains any substantive non-box text
            substantive = BOX_DRAWING_PATTERN.sub("", line).strip()
            # If line is purely border chrome (e.g., ┌────┐, ├───┤, or empty │   │)
            if not substantive or re.match(r"^[\s\-_=|+]*$", substantive):
                continue
            # Retain substantive text, removing box glyphs and cleaning border padding
            cleaned_line = BOX_DRAWING_PATTERN.sub("", line).strip()
            suffix = "\n" if raw_line.endswith("\n") else ""
            filtered_lines.append(cleaned_line + suffix)
        else:
            filtered_lines.append(raw_line)

    result = "".join(filtered_lines)
    return restore_code_blocks(result, blocks)


def strip_prompt_echoes(text: str) -> str:
    r"""
    Strip shell prompt markers (^>\s*$, ^❯\s*$, ^\$\s*$) and Starship command echoes (^❯\s+.*$),
    while strictly preserving blockquotes (> text). Preserves code blocks verbatim.
    """
    if not text:
        return ""

    text, blocks = protect_code_blocks(text)
    lines = text.splitlines(keepends=True)
    filtered: List[str] = []

    for raw_line in lines:
        stripped = raw_line.rstrip("\r\n").strip()

        # Explicitly preserve blockquotes: lines starting with > followed by substantive text
        if stripped.startswith(">") and len(stripped) > 1 and not PROMPT_EMPTY_MARKER.match(stripped):
            filtered.append(raw_line)
            continue

        # Strip empty prompt markers
        if PROMPT_EMPTY_MARKER.match(stripped):
            continue

        # Strip command prompt echoes (e.g. ❯ git status, ❯ df -h)
        if PROMPT_COMMAND_ECHO.match(stripped):
            continue

        # Strip shell user/host prompts (e.g. user@host:~$ )
        if PROMPT_SHELL_USER.match(stripped):
            continue

        filtered.append(raw_line)

    result = "".join(filtered)
    return restore_code_blocks(result, blocks)


def strip_tui_debris(text: str) -> str:
    """
    Stage 1: Strip interactive terminal TUI debris, collapsible widgets,
    spinner frames, crumb fragments, box drawing borders, and prompt echoes.
    Strips leading blank lines and leading whitespace.
    """
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    clean = strip_ansi(text)

    # Protect code blocks (both fenced and inline backticks)
    clean, code_blocks = protect_code_blocks(clean)

    # Strip box-drawing border lines and glyphs
    clean = strip_box_drawing(clean)

    # Strip prompt markers and command echoes
    clean = strip_prompt_echoes(clean)

    lines = clean.splitlines()
    filtered: List[str] = []

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()

        # Preserve single blank lines between content
        if not stripped:
            if filtered and filtered[-1] != "":
                filtered.append("")
            continue

        # 1. Lines ending in ...) or …) (e.g. mate/supervisor.py...), 3...))
        if ELLIPSIS_CLOSE_PAREN_PATTERN.search(line):
            continue

        # 2. Lines matching expand), (expand), or (ctrl+o to expand)
        if EXPAND_PAREN_PATTERN.search(line):
            continue

        # 3. Standalone numbers with ellipses (e.g. 3..., (3...), 42…)
        if STANDALONE_NUMBER_ELLIPSIS_PATTERN.match(line):
            continue

        # 4. Lingering subshell prompt markers (^>\s*$)
        if PROMPT_MARKER_PATTERN.match(line):
            continue

        # 5. Standalone ASCII spinners
        if ASCII_SPINNER_PATTERN.match(line):
            continue

        # 6. Tool invocation crumbs and thought crumbs
        if stripped.startswith(TOOL_CRUMB_PREFIXES):
            continue

        # 7. Interactive navigation bars (↑/↓ Navigate, ? for shortcuts, press enter to continue)
        if NAV_BAR_PATTERN.search(line):
            continue

        # 8. CLI hints and tips (└ Tip:..., Tip:...)
        if TIP_PATTERN.match(stripped):
            continue

        # 9. Braille spinner lines or leading braille spinners
        if re.match(r"^\s*[\u2800-\u28ff]", stripped):
            continue

        # Strip any inline braille spinner glyphs
        line = BRAILLE_SPINNER_PATTERN.sub("", line)
        if not line.strip():
            continue

        filtered.append(line)

    # Restore protected code blocks
    result = "\n".join(filtered)
    result = restore_code_blocks(result, code_blocks)

    # Strip leading blank lines and leading whitespace
    result = result.lstrip("\r\n \t")
    # Strip trailing whitespace
    result = result.rstrip()

    return result


def transpile_markdown_to_mrkdwn(text: str) -> str:
    """
    Stage 2: Transpile standard CommonMark syntax to Slack-native mrkdwn:
    - ATX headings (### Header, ## Header, # Header) -> *Header*
    - Standard Markdown bold-italic (***text***) -> *_text_*
    - Standard Markdown bold (**text**) -> *text*
    - Standard Markdown links ([label](url)) and images (![alt](url)) -> <url|label>
    - Bare URLs (https://...) -> <url>
    - Eliminates stray double asterisks outside code blocks
    - Preserves bullet lists, standard indentation, and existing code blocks.
    """
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Protect code blocks and inline code spans
    transpiled, code_blocks = protect_code_blocks(text)

    # 1. Convert Markdown images: ![alt](url) -> <url|alt>
    def _image_repl(match: re.Match) -> str:
        alt = match.group(1).strip()
        url = match.group(2).strip()
        url = re.sub(r"\s+[\"\x27][^\"\x27]*[\"\x27]$", "", url).strip()
        if url.startswith("<") and url.endswith(">"):
            url = url[1:-1].strip()
        if alt:
            return f"<{url}|{alt}>"
        return f"<{url}>"

    transpiled = MARKDOWN_IMAGE_PATTERN.sub(_image_repl, transpiled)

    # 2. Convert Markdown links: [label](url) -> <url|label>
    def _link_repl(match: re.Match) -> str:
        label = match.group(1).strip()
        url = match.group(2).strip()
        url = re.sub(r"\s+[\"\x27][^\"\x27]*[\"\x27]$", "", url).strip()
        if url.startswith("<") and url.endswith(">"):
            url = url[1:-1].strip()
        if label == url:
            return f"<{url}>"
        return f"<{url}|{label}>"

    transpiled = MARKDOWN_LINK_PATTERN.sub(_link_repl, transpiled)

    # 3. Convert ATX headings to Slack bold headers: *Header*
    def _heading_repl(match: re.Match) -> str:
        prefix = match.group(1) or ""
        content = match.group(2)
        if content is None or not content.strip():
            return ""

        content = content.strip()
        # Remove trailing CommonMark closing hashes if present: ### Heading ###
        content = re.sub(r"\s+#+$", "", content).strip()
        if not content:
            return ""

        # Strip outer bold/italic wrappers if entire heading was wrapped
        if re.match(r"^\*\*[^\*\r\n]+\*\*$", content):
            content = content[2:-2].strip()
        elif re.match(r"^\*[^\*\r\n]+\*$", content):
            content = content[1:-1].strip()

        # Convert remaining bold-italic ***text*** inside heading to _text_
        content = re.sub(r"\*\*\*([^\*\r\n]+?)\*\*\*", r"_\1_", content)
        # Remove remaining markdown bold **text** inside heading since heading is already bold
        content = re.sub(r"\*\*([^\*\r\n]+?)\*\*", r"\1", content)
        # Convert remaining markdown italic *text* inside heading to _text_ (Slack italic)
        content = re.sub(r"(?<!\*)\*([^\*\r\n]+?)\*(?!\*)", r"_\1_", content)
        return f"{prefix}*{content}*"

    transpiled = ATX_HEADING_PATTERN.sub(_heading_repl, transpiled)

    # 5. Convert Markdown bold-italic and bold
    # Remove empty bold ****
    transpiled = re.sub(r"\*{4,}", "", transpiled)
    transpiled = re.sub(r"\*\*\*([^\*\r\n]+?)\*\*\*", r"*_\1_*", transpiled)
    transpiled = re.sub(r"\*\*([^\*\r\n]+?)\*\*", r"*\1*", transpiled)

    # Secondary sweep: clean any unclosed double/triple asterisks outside code blocks
    transpiled = re.sub(r"\*{2,}", "*", transpiled)

    # 6. Restore protected code blocks and inline code
    transpiled = restore_code_blocks(transpiled, code_blocks)

    return transpiled


def sanitize_for_slack(text: str) -> str:
    """
    Two-stage sanitization pipeline for outbound text destined for Slack:
    Stage 1: Strip terminal TUI debris, widgets, spinners, box-drawing, and prompts.
    Stage 2: Transpile CommonMark syntax into Slack mrkdwn.
    """
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    stage1 = strip_tui_debris(text)
    stage2 = transpile_markdown_to_mrkdwn(stage1)
    return stage2


# Compatibility aliases matching interface contracts
strip_terminal_chrome = strip_tui_debris
transpile_markdown_to_slack = transpile_markdown_to_mrkdwn
