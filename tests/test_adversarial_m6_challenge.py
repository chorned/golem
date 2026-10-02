"""
tests/test_adversarial_m6_challenge.py
Adversarial Challenge Test Suite for Milestone 6 (E2E Test Suite & Boundary Stabilization).

Adversarial Stress Test Dimensions:
1. Bare \\x1b escape characters mixed with multibyte UTF-8, null bytes, backspaces, and OSC/CSI sequences.
2. CRLF and CR line endings embedded within multi-line fenced code blocks, unclosed code blocks, and markdown lists.
3. Chunker behavior with massive outputs (>20,000 chars), pathological unclosed code blocks, tildes vs backticks, and extreme boundary cuts.
"""

import signal
import unittest
from typing import List

import first_mate.transport.chunker as chunker
import first_mate.transport.normalizer as norm


class TestAdversarialBareEscapesAndControlChars(unittest.TestCase):
    """Adversarial tests for bare \\x1b, multibyte UTF-8, null bytes, backspaces, and OSC/CSI."""

    def test_bare_escape_mixed_with_multibyte_utf8(self):
        """Verify bare \\x1b does not corrupt multibyte UTF-8 characters."""
        samples = [
            ("Test \x1b🚀 Rocket \x1b🎉 Party \x1b😀 Smile", "Rocket"),
            ("CJK: \x1b你好\x1b世界 \x1bこんにちは \x1b안녕하세요", "你好世界"),
            ("Cyrillic: \x1bПривет \x1bмир", "мир"),
            ("Mixed: \x1b\U0001f916 Robot \x1b\u2728 Sparkle", "Robot"),
        ]
        for s, expected_sub in samples:
            out = norm.sanitize_for_slack(s)
            self.assertNotIn("\x1b", out, f"\\x1b leaked in {out!r}")
            self.assertIn(expected_sub, out)

    def test_bare_escape_interspersed_in_utf8_payload(self):
        """Verify \\x1b interspersed between UTF-8 code points is stripped without mangling code points."""
        text = "\x1b😀\x1b \x1b🚀\x1b \x1b你好\x1b"
        out = norm.sanitize_for_slack(text)
        self.assertNotIn("\x1b", out)
        self.assertEqual(out, "😀 🚀 你好")

    def test_null_bytes_and_backspaces_containment(self):
        """Verify handling of null bytes \\x00 and backspaces \\x08 alongside \\x1b."""
        text = "Prefix \x00\x1b\x00 and backspace \x08\x1b\x08 test"
        out = norm.sanitize_for_slack(text)
        self.assertNotIn("\x1b", out)
        self.assertIn("Prefix", out)
        self.assertIn("test", out)

    def test_exotic_and_malformed_osc_csi_sequences(self):
        """Verify standard, exotic, and malformed OSC/CSI sequences do not leak \\x1b."""
        sequences = [
            "\x1b[?25hCursorOn\x1b[?25lCursorOff",
            "\x1b[2J\x1b[HClearAndHome",
            "\x1b]8;;https://example.com\x1b\\Hyperlink\x1b]8;;\x1b\\",
            "\x1b]0;WindowTitle\x07BodyContent",
            "\x1b\x1b\x1bMultipleBareEscapes",
            "\x1b]99;MalformedOSCWithoutTerminator\nTrailing",
            "\x1b[IncompleteCSI",
        ]
        for seq in sequences:
            out = norm.sanitize_for_slack(seq)
            self.assertNotIn("\x1b", out, f"\\x1b leaked for sequence {seq!r}")

    def test_bare_escape_before_capital_letter_devour_bug(self):
        """
        Verify bare \\x1b followed by uppercase letter A-Z or digit 0-9
        is preserved and not devoured by ANSI_ESCAPE_PATTERN.
        """
        raw = "\x1bTask completed"
        out = norm.sanitize_for_slack(raw)
        self.assertEqual(out, "Task completed")

    def test_sentinel_collision_in_code_block_restoration(self):
        """
        Verify literal sentinel tokens in input text do not collide with code block restoration.
        """
        raw = "Literal \x00FM_CODE_0\x00 in text\n```python\nprint(123)\n```\n"
        out = norm.sanitize_for_slack(raw)
        self.assertIn("Literal \x00FM_CODE_0\x00 in text", out)


class TestAdversarialCrlfAndLineEndings(unittest.TestCase):
    """Adversarial tests for CRLF and CR embedded in code blocks, unclosed blocks, and lists."""

    def test_crlf_and_cr_in_fenced_code_blocks(self):
        """Verify sanitize_for_slack completely removes \\r from multi-line code blocks."""
        raw = (
            "```python\r\n"
            "def compute():\r\n"
            "    value = 42\r\n"
            "    return value\r\n"
            "```\r\n"
        )
        out = norm.sanitize_for_slack(raw)
        self.assertNotIn("\r", out)
        self.assertIn("def compute():\n    value = 42\n    return value", out)

    def test_crlf_and_cr_in_unclosed_code_blocks(self):
        """Verify unclosed code blocks extending to EOF are normalized with zero \\r."""
        raw = (
            "```python\r\n"
            "x = 10\r\n"
            "y = 20\r\n"
            "z = x + y"
        )
        out = norm.sanitize_for_slack(raw)
        self.assertNotIn("\r", out)
        self.assertTrue(out.startswith("```python\nx = 10\ny = 20\nz = x + y"))

    def test_crlf_and_cr_in_markdown_lists_and_quotes(self):
        """Verify markdown lists and blockquotes with CRLF/CR normalize cleanly."""
        raw = (
            "### Header Title\r\n\r\n"
            "* List Item 1\r\n"
            "* List Item 2 with **bold** text\r\n"
            "- Hyphen Item\r\n"
            "1. Numbered Item 1\r\n"
            "2. Numbered Item 2\r\n\r\n"
            "> Blockquote line 1\r\n"
            "> Blockquote line 2\r\n"
        )
        out = norm.sanitize_for_slack(raw)
        self.assertNotIn("\r", out)
        self.assertIn("*Header Title*", out)
        self.assertIn("* List Item 1", out)
        self.assertIn("* List Item 2 with *bold* text", out)
        self.assertIn("> Blockquote line 1", out)

    def test_chunker_crlf_paragraph_boundary_bypassed_bug(self):
        """
        Verify chunker.split_message_boundary_aware normalizes \\r\\n at entry
        and cleanly splits at paragraph boundaries.
        """
        p1 = "Paragraph 1 text\nSentence in p1\n" + "A" * 1500
        p2 = "Paragraph 2 text\nSentence in p2\n" + "B" * 1500
        raw_win = p1.replace("\n", "\r\n") + "\r\n\r\n" + p2.replace("\n", "\r\n")

        chunks = chunker.split_message_boundary_aware(raw_win, max_chars=2000)
        self.assertTrue(chunks[0].endswith("A" * 10))


class TestAdversarialChunkerMassiveAndBoundaryCuts(unittest.TestCase):
    """Adversarial tests for massive outputs, unclosed blocks, tildes vs backticks, and boundaries."""

    def test_massive_output_exceeding_20000_chars_without_newlines(self):
        """Verify 25,000-char continuous string chunks cleanly without data loss."""
        raw = "a" * 25000
        chunks = chunker.split_message_boundary_aware(raw, max_chars=3800)
        self.assertEqual(len(chunks), 7)
        self.assertTrue(all(len(c) <= 3800 for c in chunks))
        self.assertEqual("".join(chunks), raw)

    def test_massive_unclosed_code_block_30000_chars(self):
        """Verify massive 30,000-char unclosed code block preserves fences and balances intermediate chunks."""
        raw = "```python\n" + ("print('stress test line')\n" * 1200)
        self.assertGreater(len(raw), 25000)
        chunks = chunker.split_message_boundary_aware(raw, max_chars=3800)
        self.assertGreater(len(chunks), 5)
        for i, c in enumerate(chunks):
            self.assertLessEqual(len(c), 3800, f"Chunk {i} exceeded 3800 chars")
        # Intermediate chunks must be closed (even number of ```)
        for i, c in enumerate(chunks[:-1]):
            self.assertEqual(c.count("```") % 2, 0, f"Intermediate chunk {i} not closed")
        # Terminal chunk must be unclosed (1 opening fence)
        self.assertEqual(chunks[-1].count("```") % 2, 1)

    def test_tildes_vs_backticks_and_nested_fences(self):
        """Verify tilde fences (~~~), 4-backtick fences (````), and nested blocks preserve delimiters."""
        # Nested ~~~ enclosing ```
        nested_text = "~~~markdown\nHere is python:\n```python\nprint(1)\n```\n~~~\n" * 300
        chunks_nested = chunker.split_message_boundary_aware(nested_text, max_chars=3800)
        for c in chunks_nested:
            self.assertLessEqual(len(c), 3800)

        # 4-backtick outer block
        f4_text = "````python\n# outer\n```\ninner\n```\n````\n" * 300
        chunks_f4 = chunker.split_message_boundary_aware(f4_text, max_chars=3800)
        for c in chunks_f4:
            self.assertLessEqual(len(c), 3800)

    def test_boundary_cut_at_opening_fence_line(self):
        """Verify split when opening fence occurs right near the 3800 character boundary."""
        pre = "Word " * 750  # ~3750 chars
        code = "```python\n" + ("x = 1\n" * 500) + "```\n"
        text = pre + "\n\n" + code
        chunks = chunker.split_message_boundary_aware(text, max_chars=3800)
        self.assertGreaterEqual(len(chunks), 2)
        for c in chunks:
            self.assertLessEqual(len(c), 3800)

    def test_chunker_infinite_loop_on_small_budget_or_long_opening_fence_bug(self):
        """
        Verify that when a code block starts at pos 0 with a small budget,
        the chunker guarantees strictly positive progress without hanging.
        """
        text = "```python\n# This is a comment that is thirty characters long\nprint(1)\n```"

        def _timeout_handler(signum, frame):
            raise TimeoutError("Chunker entered infinite loop!")

        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(2)
        try:
            chunks = chunker.split_message_boundary_aware(text, max_chars=30)
            self.assertTrue(len(chunks) > 0)
        finally:
            signal.alarm(0)

    def test_chunker_indented_closing_fence_split_bug(self):
        """
        Verify that candidate budgets landing on indented closing fence lines
        do not sever closing fences or leave unclosed chunks.
        """
        open_fence = "```python\n"
        target_body_len = 98 - len(open_fence) - 1
        code_body = "x" * target_body_len + "\n"
        close_fence = "   ```\n"
        text = open_fence + code_body + close_fence + "after text"

        chunks = chunker.split_message_boundary_aware(text, max_chars=100)
        self.assertEqual(chunks[0].count("```") % 2, 0, "Chunk 0 should be properly closed")


if __name__ == "__main__":
    unittest.main()
