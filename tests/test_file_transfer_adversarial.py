#!/usr/bin/env python3
"""
tests/test_file_transfer_adversarial.py
Adversarial empirical test harness for Milestone 3 (file_transfer.py):
1. Malformed, weirdly spaced, and mixed-case directives.
2. Directives inside complex Markdown (tables, bullet lists, blockquotes, code blocks).
3. Text stripping for word concatenation bugs and markdown whitespace preservation.
4. Multiple directives (e.g. 5 directives in one turn).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import unittest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.transport.file_transfer import (
    extract_and_strip_file_directives,
    DIRECTIVE_PATTERN,
)


class TestAdversarialDirectiveExtraction(unittest.TestCase):
    """Stress-test directive syntax variants, weird spacing, and malformed inputs."""

    def test_both_spaces_dispatch_case(self):
        """Test exact dispatch case: '[ send_file : /path/a.pdf ]'."""
        text = "Here is the log: [ send_file : /path/a.pdf ]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/path/a.pdf"])
        self.assertNotIn("send_file", clean.lower())

    def test_space_after_opening_bracket(self):
        """Test space immediately following '[': '[ SEND_FILE: /path/a.pdf]'."""
        text = "Result: [ SEND_FILE: /path/a.pdf]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/path/a.pdf"])
        self.assertNotIn("SEND_FILE", clean)

    def test_space_before_colon(self):
        """Test space immediately preceding ':': '[SEND_FILE : /path/a.pdf]'."""
        text = "Result: [SEND_FILE : /path/a.pdf]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/path/a.pdf"])
        self.assertNotIn("SEND_FILE", clean)

    def test_tabs_inside_directive(self):
        """Test tabs inside directive brackets."""
        text = "Result: [\tSEND_FILE:\t/path/a.pdf\t]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/path/a.pdf"])
        self.assertNotIn("SEND_FILE", clean)

    def test_multiple_spaces_inside_directive(self):
        """Test multiple spaces around verb and colon."""
        text = "Result: [   UPLOAD_FILE   :   /path/data.csv   ]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/path/data.csv"])
        self.assertNotIn("UPLOAD_FILE", clean)

    def test_upload_file_no_space_quoted(self):
        """Test dispatch case: '[UPLOAD_FILE:"path with spaces/file.txt"]'."""
        text = 'File ready: [UPLOAD_FILE:"path with spaces/file.txt"]'
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["path with spaces/file.txt"])
        self.assertNotIn("UPLOAD_FILE", clean)

    def test_attach_file_angle_brackets(self):
        """Test dispatch case: '[attach_file: <http://example.com/file>]'."""
        text = "See link: [attach_file: <http://example.com/file>]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["http://example.com/file"])
        self.assertNotIn("attach_file", clean)

    def test_empty_directive_ignored_or_stripped(self):
        """Test empty directive with no path: '[SEND_FILE:]'."""
        text = "Notice: [SEND_FILE:]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, [])

    def test_whitespace_only_directive(self):
        """Test directive containing only whitespace: '[SEND_FILE:   ]'."""
        text = "Notice: [SEND_FILE:   ]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, [])


class TestAdversarialMarkdownContainment(unittest.TestCase):
    """Stress-test directives inside markdown tables, bullet lists, blockquotes, code blocks."""

    def test_directives_in_markdown_table(self):
        """Verify directives in table cells extract correctly and maintain table columns."""
        table = (
            "| Step | Artifact | Status |\n"
            "| :--- | :--- | :--- |\n"
            "| 1 | [SEND_FILE: /tmp/a.pdf] | Complete |\n"
            "| 2 | [SEND_FILE: /tmp/b.pdf] | Complete |"
        )
        clean, targets = extract_and_strip_file_directives(table)
        self.assertEqual(targets, ["/tmp/a.pdf", "/tmp/b.pdf"])
        lines = clean.splitlines()
        self.assertEqual(len(lines), 4)
        for line in lines:
            self.assertEqual(line.count("|"), 4, f"Table row lost pipe column: {line}")

    def test_bullet_list_empty_bullet_prevention(self):
        """Verify standalone bullet directives do not leave orphaned bullet markers."""
        text = (
            "Executive Briefing:\n"
            "- [SEND_FILE: /tmp/a.pdf]\n"
            "- Next step"
        )
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/tmp/a.pdf"])
        # Should not have an orphaned empty bullet item
        for line in clean.splitlines():
            self.assertNotEqual(line.strip(), "-", "Orphaned empty dash bullet found")

    def test_asterisk_bullet_bold_corruption(self):
        """Verify asterisk bullets do not leave dangling '*' that trigger Slack bold corruption."""
        text = (
            "Summary:\n"
            "* [SEND_FILE: /tmp/a.pdf]\n"
            "* Status: active\n"
            "* All systems go."
        )
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/tmp/a.pdf"])
        for line in clean.splitlines():
            self.assertNotEqual(line.strip(), "*", "Orphaned asterisk bullet found (triggers Slack bolding)")

    def test_blockquote_empty_quote_marker(self):
        """Verify blockquotes containing only directive do not leave dangling '>' lines."""
        text = (
            "> Executive Note:\n"
            "> [SEND_FILE: /tmp/quote.pdf]\n"
            "> Operational."
        )
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/tmp/quote.pdf"])
        for line in clean.splitlines():
            self.assertNotEqual(line.strip(), ">", "Orphaned blockquote marker found")

    def test_fenced_code_block_string_literal_preservation(self):
        """Verify code blocks with multi-space string literals are NOT collapsed."""
        code_text = (
            "Script output:\n"
            "```python\n"
            "val = \"hello   world\"\n"
            "x  =  1\n"
            "```\n"
            "[SEND_FILE: /tmp/script.py]"
        )
        clean, targets = extract_and_strip_file_directives(code_text)
        self.assertEqual(targets, ["/tmp/script.py"])
        self.assertIn('"hello   world"', clean, "Multi-space string literal inside code block was corrupted")

    def test_fenced_code_block_documentation_directive_not_executed(self):
        """Verify directive examples inside code blocks do not mutate code syntax or trigger unintended upload."""
        doc_code = (
            "Here is how to configure directives:\n"
            "```bash\n"
            "echo \"[SEND_FILE: /var/log/sample.log]\"\n"
            "```\n"
            "Follow the example above."
        )
        clean, targets = extract_and_strip_file_directives(doc_code)
        # If code block directives are extracted, they trigger unintended uploads and mutate code
        self.assertIn('echo "[SEND_FILE: /var/log/sample.log]"', clean, "Code block example was mutated")
        self.assertEqual(targets, [], "Directive inside fenced code block should not be extracted as outbound file")


class TestAdversarialTextStrippingAndFormatting(unittest.TestCase):
    """Stress-test word concatenation, quote/backtick eating, and whitespace fidelity."""

    def test_adjacent_word_concatenation_prefix_suffix(self):
        """Verify directive stripping does not concatenate adjacent words."""
        text = "Download[SEND_FILE: /tmp/a.pdf]now for review."
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/tmp/a.pdf"])
        self.assertNotIn("Downloadnow", clean, "Words were concatenated without space")

    def test_colon_adjacent_word_concatenation(self):
        """Verify directive stripping does not collapse 'Status:[SEND_FILE]Done'."""
        text = "Status:[SEND_FILE: /tmp/a.pdf]Done"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/tmp/a.pdf"])
        self.assertNotIn("Status:Done", clean, "Words were concatenated without space")

    def test_asymmetric_quote_eating(self):
        """Verify trailing quote is not swallowed when opening quote belongs to sentence."""
        text = 'He said "please review [SEND_FILE: /tmp/a.pdf]" today.'
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/tmp/a.pdf"])
        self.assertEqual(clean.count('"'), 2, f"Sentence double quotes unbalanced: {clean!r}")

    def test_asymmetric_backtick_eating(self):
        """Verify trailing backtick is not swallowed when opening backtick belongs to inline code."""
        text = "Execute `cat [SEND_FILE: /tmp/a.txt]` to verify."
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/tmp/a.txt"])
        self.assertEqual(clean.count("`"), 2, f"Inline code backticks unbalanced: {clean!r}")

    def test_floating_punctuation(self):
        """Verify stripping does not leave space before period or comma."""
        text = "Please find attached [SEND_FILE: /tmp/a.pdf]."
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/tmp/a.pdf"])
        self.assertNotIn("attached .", clean, "Floating punctuation space left before period")

    def test_five_directives_in_one_turn(self):
        """Test dispatch requirement: multiple directives (e.g. 5 directives in one turn)."""
        text = (
            "Executive Briefing:\n"
            "[SEND_FILE: /tmp/file1.pdf]\n"
            "[UPLOAD_FILE: /tmp/file2.json]\n"
            "[ATTACH_FILE: /tmp/file3.csv]\n"
            "[send_file: /tmp/file4.txt]\n"
            "[upload_file: /tmp/file5.png]\n"
            "All 5 artifacts delivered."
        )
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(
            targets,
            ["/tmp/file1.pdf", "/tmp/file2.json", "/tmp/file3.csv", "/tmp/file4.txt", "/tmp/file5.png"]
        )
        self.assertEqual(clean, "Executive Briefing:\n\nAll 5 artifacts delivered.")


if __name__ == "__main__":
    unittest.main()
