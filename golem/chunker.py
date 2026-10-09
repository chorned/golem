r"""
golem/transport/chunker.py
Boundary-aware, code-fence-preserving message chunker for First Mate 2.0 transport layer.

Features:
- Respects Slack message limit (max_chars = 3800).
- Priority splitting:
  1. Paragraph boundaries (\n\n) strictly OUTSIDE fenced code blocks
  2. Single newline (\n)
  3. Sentence boundaries ([.!?]\s+)
  4. Whitespace boundaries (" ")
  5. Hard character cutoff fallback
- Code Fence Invariant:
  If a message is split within an active fenced code block (```{lang} or ~~~{lang}):
  - Chunk 1 is terminated cleanly with \n{fence}\n.
  - Chunk 2 is reopened with {fence}{lang}\n.
  - Chunk 1 length never exceeds max_chars.
  - Code indentation in Chunk 2 is strictly preserved (never stripped).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class CodeBlockSpan:
    """Represents the character boundaries and metadata of a fenced code block."""
    start: int          # Character index where opening fence line begins
    content_start: int  # Character index where code body starts (after opening fence line)
    content_end: int    # Character index where code body ends (before closing fence line)
    end: int            # Character index where closing fence line ends (or len(text) if unclosed)
    fence_char: str     # '`' or '~'
    fence_len: int      # 3, 4, etc.
    lang: str           # Language identifier (e.g. "python", "bash", or "")
    is_closed: bool     # True if closing fence found; False if unclosed extends to EOF


OPEN_FENCE_REGEX = re.compile(r"^[ ]{0,3}(`{3,}|~{3,})([a-zA-Z0-9_\-\+]*).*$")


def parse_code_blocks(text: str) -> List[CodeBlockSpan]:
    """Parse all CommonMark fenced code blocks within text to locate character boundaries."""
    blocks: List[CodeBlockSpan] = []
    lines = text.splitlines(keepends=True)
    offset = 0
    in_block = False

    start_pos = 0
    content_start_pos = 0
    fence_char = ""
    fence_len = 0
    lang = ""

    for line in lines:
        line_len = len(line)
        line_stripped = line.rstrip("\r\n")

        if not in_block:
            m = OPEN_FENCE_REGEX.match(line_stripped)
            if m:
                in_block = True
                start_pos = offset
                content_start_pos = offset + line_len
                fence_char = m.group(1)[0]
                fence_len = len(m.group(1))
                lang = m.group(2).strip()
        else:
            close_regex = re.compile(rf"^[ ]{{0,3}}{re.escape(fence_char)}{{{fence_len},}}\s*$")
            if close_regex.match(line_stripped):
                blocks.append(CodeBlockSpan(
                    start=start_pos,
                    content_start=content_start_pos,
                    content_end=offset,
                    end=offset + line_len,
                    fence_char=fence_char,
                    fence_len=fence_len,
                    lang=lang,
                    is_closed=True
                ))
                in_block = False
        offset += line_len

    if in_block:
        # CommonMark rule: unclosed code block extends to EOF
        blocks.append(CodeBlockSpan(
            start=start_pos,
            content_start=content_start_pos,
            content_end=offset,
            end=offset,
            fence_char=fence_char,
            fence_len=fence_len,
            lang=lang,
            is_closed=False
        ))

    return blocks


def find_enclosing_block(blocks: List[CodeBlockSpan], pos: int) -> Optional[CodeBlockSpan]:
    """
    Returns the CodeBlockSpan if pos falls inside the code block [b.start, b.end).
    Properly classifies opening fence, code body, and closing fence as inside the block.
    """
    for b in blocks:
        if b.start <= pos < b.end:
            return b
    return None


def split_message_boundary_aware(text: str, max_chars: int = 3800) -> List[str]:
    r"""
    Split outbound message preserving code blocks and respecting boundaries.

    Priority:
    1. Paragraph boundaries (\n\n) outside code blocks
    2. Single newline (\n)
    3. Sentence boundaries ([.!?]\s+)
    4. Whitespace (" ")
    5. Hard cutoff

    If slicing inside a fenced code block, Chunk 1 closes with \n{fence}\n,
    and Chunk 2 reopens with {fence}{lang}\n.
    Guarantees strictly positive progress on every slice.
    """
    if not text or not text.strip():
        return []
    # Normalize CRLF and bare CR line endings at entry
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if len(text) <= max_chars:
        return [text]

    chunks: List[str] = []
    remaining = text.strip()

    while remaining:
        if len(remaining) <= max_chars:
            chunks.append(remaining)
            break

        # Re-parse code blocks on current remaining text
        blocks = parse_code_blocks(remaining)

        # Budget calculation: dynamically reserve space for closing fence delimiter based on actual fence length
        max_fence_len = max((b.fence_len for b in blocks), default=3)
        closing_fence_pad = max(10, max_fence_len + 2)
        safe_budget = max(1, max_chars - closing_fence_pad)
        candidate_budget = max_chars
        min_split_threshold = max(1, min(max_chars // 4, safe_budget // 2))

        split_idx = -1
        is_inside_block = False
        active_block: Optional[CodeBlockSpan] = None

        def check_split_pos(p: int, is_newline: bool = False) -> tuple[bool, int, bool, Optional[CodeBlockSpan]]:
            block = find_enclosing_block(blocks, p)
            if block is None:
                return True, p, False, None

            # Closed block on closing fence line [content_end, end)
            if block.is_closed and p >= block.content_end:
                if is_newline and p == block.end - 1:
                    return True, block.end, False, None
                return False, -1, False, None

            # On opening fence line [start, content_start]
            if block.start <= p <= block.content_start:
                if block.start > 0:
                    return True, block.start, False, None
                else:
                    return False, -1, False, None

            # Inside code content [content_start, content_end)
            fence_pad = block.fence_len + 2
            if p > (max_chars - fence_pad):
                return False, -1, False, None

            if p <= block.content_start:
                return False, -1, False, None

            return True, p, True, block

        # ----------------------------------------------------------------------
        # Priority 1: Paragraph boundaries (\n\n) OUTSIDE code blocks
        # ----------------------------------------------------------------------
        p1_matches = list(re.finditer(r"\n\n", remaining[:candidate_budget]))
        for m in reversed(p1_matches):
            p = m.start()
            if p < min_split_threshold:
                break
            # Must not be enclosed in any code block
            if not any(b.start <= p < b.end for b in blocks):
                split_idx = p
                is_inside_block = False
                break

        # ----------------------------------------------------------------------
        # Priority 2: Single newline (\n)
        # ----------------------------------------------------------------------
        if split_idx == -1:
            for p in range(candidate_budget - 1, min_split_threshold - 1, -1):
                if remaining[p] == "\n":
                    valid, s_idx, inside, blk = check_split_pos(p, is_newline=True)
                    if valid:
                        split_idx = s_idx
                        is_inside_block = inside
                        active_block = blk
                        break

        # ----------------------------------------------------------------------
        # Priority 3: Sentence boundaries ([.!?](?:\s+|$))
        # ----------------------------------------------------------------------
        if split_idx == -1:
            sentence_matches = list(re.finditer(r"[.!?](?:\s+|$)", remaining[:candidate_budget]))
            for m in reversed(sentence_matches):
                p = m.end()
                if p < min_split_threshold:
                    break
                valid, s_idx, inside, blk = check_split_pos(p, is_newline=False)
                if valid:
                    split_idx = s_idx
                    is_inside_block = inside
                    active_block = blk
                    break

        # ----------------------------------------------------------------------
        # Priority 4: Whitespace boundary (" ")
        # ----------------------------------------------------------------------
        if split_idx == -1:
            for p in range(candidate_budget - 1, min_split_threshold - 1, -1):
                if remaining[p] == " ":
                    valid, s_idx, inside, blk = check_split_pos(p, is_newline=False)
                    if valid:
                        split_idx = s_idx
                        is_inside_block = inside
                        active_block = blk
                        break

        # ----------------------------------------------------------------------
        # Priority 5: Hard character limit fallback
        # ----------------------------------------------------------------------
        if split_idx == -1:
            block = find_enclosing_block(blocks, candidate_budget) or find_enclosing_block(blocks, safe_budget)
            if block is not None:
                fence_pad = block.fence_len + 2
                target = max_chars - fence_pad
                if block.start <= target <= block.content_start and block.start > 0:
                    split_idx = block.start
                    is_inside_block = False
                    active_block = None
                elif target < block.start and block.start > 0:
                    split_idx = block.start
                    is_inside_block = False
                    active_block = None
                elif target > block.content_start:
                    split_idx = min(target, block.content_end if block.is_closed else len(remaining))
                    is_inside_block = True
                    active_block = block
                else:
                    split_idx = max(1, min(candidate_budget, len(remaining)))
                    is_inside_block = False
                    active_block = None
            else:
                split_idx = max(1, min(candidate_budget, len(remaining)))
                is_inside_block = False
                active_block = None

        # Hard invariant: split_idx must be positive
        if split_idx <= 0:
            split_idx = max(1, min(candidate_budget, len(remaining)))
            is_inside_block = False
            active_block = None

        # ----------------------------------------------------------------------
        # Form Chunk and Advance Remaining
        # ----------------------------------------------------------------------
        if is_inside_block and active_block is not None:
            lang = active_block.lang
            fence = active_block.fence_char * active_block.fence_len

            chunk_body = remaining[:split_idx].rstrip()
            chunk = f"{chunk_body}\n{fence}\n"
            chunks.append(chunk)

            # Preserve code indentation in Chunk 2: only strip leading newlines
            remainder_body = remaining[split_idx:].lstrip("\r\n")
            new_remaining = f"{fence}{lang}\n{remainder_body}".strip()

            # Monotonic shrinkage guarantee
            if len(new_remaining) >= len(remaining):
                chunks.pop()
                raw_cut = max(1, min(candidate_budget, len(remaining)))
                chunk = remaining[:raw_cut].strip()
                if chunk:
                    chunks.append(chunk)
                remaining = remaining[raw_cut:].strip()
            else:
                remaining = new_remaining
        else:
            chunk = remaining[:split_idx].strip()
            if chunk:
                chunks.append(chunk)
            remaining = remaining[split_idx:].strip()

    return chunks
