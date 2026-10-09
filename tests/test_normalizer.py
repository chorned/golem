#!/usr/bin/env python3
"""
tests/test_normalizer.py
Unit tests for outbound normalizer (TUI debris stripping and CommonMark to Slack mrkdwn transpiler).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import unittest
from normalizer import (
    strip_ansi,
    strip_tui_debris,
    transpile_markdown_to_mrkdwn,
    sanitize_for_slack
)


class TestNormalizer(unittest.TestCase):

    def test_strips_tui_widgets_and_spinner_crumbs(self):
        """Feed raw buffer containing mate/supervisor.py...), 3...), and expand) -> assert completely removed."""
        raw_buffer = (
            "mate/supervisor.py...)\n"
            "3...)\n"
            "expand)\n"
            "Here is the substantive operational update."
        )
        cleaned = sanitize_for_slack(raw_buffer)

        self.assertNotIn("mate/supervisor.py...)", cleaned)
        self.assertNotIn("3...)", cleaned)
        self.assertNotIn("expand)", cleaned)
        self.assertEqual(cleaned, "Here is the substantive operational update.")

    def test_converts_markdown_headers_and_bold_to_slack_mrkdwn(self):
        """Feed ### Title and **bold** -> assert output is *Title* and *bold*."""
        raw_markdown = "### Title\nThis text contains **bold** styling."
        converted = sanitize_for_slack(raw_markdown)

        self.assertIn("*Title*", converted)
        self.assertNotIn("###", converted)
        self.assertIn("*bold*", converted)
        self.assertNotIn("**bold**", converted)
        self.assertEqual(converted, "*Title*\nThis text contains *bold* styling.")

    def test_converts_markdown_links(self):
        """Feed [My Server](http://localhost:8090) -> assert <http://localhost:8090|My Server>."""
        raw_link = "Check out the endpoint at [My Server](http://localhost:8090)."
        converted = sanitize_for_slack(raw_link)

        self.assertIn("<http://localhost:8090|My Server>", converted)
        self.assertNotIn("[My Server](http://localhost:8090)", converted)

    def test_strips_braille_spinners_and_standalone_prompts(self):
        """Verify braille spinners, progress rows, and subshell prompt markers (>) are stripped."""
        raw_buffer = (
            "\n\n"
            "⠋ Thinking...\n"
            "⠙ Searching files...\n"
            "⠹\n"
            "> \n"
            "> \n"
            "### Cloud Ops (oci-machine)\n"
            "• Machine is online and healthy.\n"
            ">\n"
        )
        cleaned = sanitize_for_slack(raw_buffer)

        self.assertNotIn("⠋", cleaned)
        self.assertNotIn("⠙", cleaned)
        self.assertNotIn("⠹", cleaned)
        self.assertNotIn("Thinking...", cleaned)
        self.assertNotIn("Searching files...", cleaned)
        self.assertFalse(cleaned.startswith("\n"))
        self.assertFalse(cleaned.startswith(">"))
        self.assertIn("*Cloud Ops (oci-machine)*", cleaned)
        self.assertIn("• Machine is online and healthy.", cleaned)

    def test_strips_leading_blank_lines_and_whitespace(self):
        """Verify leading blank lines or orphaned whitespace are stripped."""
        text = "\n\n   \n\n*Important message starts here.*"
        cleaned = sanitize_for_slack(text)
        self.assertEqual(cleaned, "*Important message starts here.*")

    def test_preserves_bullet_lists_and_indentation(self):
        """Verify bullet lists (-, •, numbered) and standard indentation are preserved."""
        text = (
            "Here is the status:\n"
            "- Item 1\n"
            "  - Nested item A\n"
            "  - Nested item B\n"
            "• Bullet item\n"
            "1. Step one\n"
            "2. Step two"
        )
        cleaned = sanitize_for_slack(text)
        self.assertEqual(cleaned, text)

    def test_preserves_fenced_code_blocks(self):
        """Verify fenced code blocks are not mangled by markdown transpiler or TUI stripper."""
        text = (
            "Here is a code snippet:\n"
            "```python\n"
            "# Not a header\n"
            "### Also not a header\n"
            "value = **not_bold**\n"
            "url = [not_a_link](http://example.com)\n"
            "path = mate/supervisor.py...)\n"
            "expand)\n"
            "```\n"
            "### Real Header\n"
            "Outside code is **bold**."
        )
        cleaned = sanitize_for_slack(text)

        expected_code = (
            "```python\n"
            "# Not a header\n"
            "### Also not a header\n"
            "value = **not_bold**\n"
            "url = [not_a_link](http://example.com)\n"
            "path = mate/supervisor.py...)\n"
            "expand)\n"
            "```"
        )
        self.assertIn(expected_code, cleaned)
        self.assertIn("*Real Header*", cleaned)
        self.assertIn("*bold*", cleaned)

    def test_preserves_inline_code(self):
        """Verify inline code blocks preserve their internal syntax without transpilation."""
        text = "Run `### git status` and use `**flags**` with `[link](http://foo)`."
        cleaned = sanitize_for_slack(text)
        self.assertEqual(cleaned, "Run `### git status` and use `**flags**` with `[link](http://foo)`.")

    def test_multiple_heading_levels(self):
        """Verify # through ###### are converted to bold headers."""
        text = (
            "# Heading 1\n"
            "## Heading 2\n"
            "### Heading 3\n"
            "#### Heading 4\n"
            "##### Heading 5\n"
            "###### Heading 6 ###"
        )
        cleaned = transpile_markdown_to_mrkdwn(text)
        self.assertEqual(
            cleaned,
            "*Heading 1*\n*Heading 2*\n*Heading 3*\n*Heading 4*\n*Heading 5*\n*Heading 6*"
        )

    def test_heading_with_embedded_bold_and_italic(self):
        """Verify headings with embedded bold and italic render clean Slack mrkdwn without broken asterisks."""
        self.assertEqual(sanitize_for_slack("### 1. **Setup**"), "*1. Setup*")
        self.assertEqual(sanitize_for_slack("### **Important:** Notice here"), "*Important: Notice here*")
        self.assertEqual(sanitize_for_slack("### Section with *italic* emphasis"), "*Section with _italic_ emphasis*")
        self.assertEqual(sanitize_for_slack("### **Already bold**"), "*Already bold*")
        self.assertEqual(sanitize_for_slack("### *Already bold*"), "*Already bold*")
        self.assertEqual(sanitize_for_slack("### [My Server](http://localhost:8090)"), "*<http://localhost:8090|My Server>*")

    def test_markdown_link_with_parentheses_in_url(self):
        """Verify markdown links with balanced parentheses in the URL are not prematurely truncated."""
        link_text = "See [Daemon](https://en.wikipedia.org/wiki/Daemon_(computing)) for details."
        cleaned = sanitize_for_slack(link_text)
        self.assertEqual(cleaned, "See <https://en.wikipedia.org/wiki/Daemon_(computing)|Daemon> for details.")

        nested_link = "Visit [Nested](https://example.com/path(1)/test) now."
        self.assertEqual(sanitize_for_slack(nested_link), "Visit <https://example.com/path(1)/test|Nested> now.")

    def test_inline_code_with_multiple_backticks(self):
        """Verify inline code delimited by multiple backticks containing backticks or bold is preserved."""
        text = "Run `` `foo **bar** baz` `` carefully."
        cleaned = sanitize_for_slack(text)
        self.assertEqual(cleaned, "Run `` `foo **bar** baz` `` carefully.")

    def test_indented_comments_not_converted_to_headings(self):
        """Verify 4-space indented Python comments in code snippets are not converted to headings."""
        code_text = (
            "Here is the script:\n"
            "    # This is a comment\n"
            "    x = 1"
        )
        cleaned = sanitize_for_slack(code_text)
        self.assertIn("    # This is a comment", cleaned)
        self.assertNotIn("    *This is a comment*", cleaned)

    def test_unclosed_code_blocks_preserved(self):
        """Verify unclosed code fences extending to end of buffer preserve code and inner lines."""
        text = (
            "```python\n"
            "# Here is the code\n"
            "x = mate/supervisor.py...)\n"
            "expand)\n"
            "y = 10"
        )
        cleaned = sanitize_for_slack(text)
        self.assertIn("x = mate/supervisor.py...)", cleaned)
        self.assertIn("expand)", cleaned)
        self.assertIn("# Here is the code", cleaned)

    def test_nested_fenced_code_blocks(self):
        """Verify 4-backtick code fences containing 3-backtick code fences are preserved intact."""
        text = (
            "````markdown\n"
            "```python\n"
            "print('hello')\n"
            "```\n"
            "### Inner Header\n"
            "````\n"
            "### Real Header"
        )
        cleaned = sanitize_for_slack(text)
        self.assertIn("````markdown", cleaned)
        self.assertIn("### Inner Header", cleaned)
        self.assertIn("*Real Header*", cleaned)

    def test_markdown_image_transpilation(self):
        """Verify markdown image tags transpiled to Slack link format."""
        img_text = "See image: ![Dashboard Architecture](https://example.com/arch.png)"
        cleaned = sanitize_for_slack(img_text)
        self.assertEqual(cleaned, "See image: <https://example.com/arch.png|Dashboard Architecture>")

    def test_idempotency(self):
        """Verify running sanitize_for_slack twice produces the exact same output."""
        text = (
            "### Title\n"
            "This is **bold** with [Docs](https://docs.firstmate.ai).\n"
            "• Bullet 1\n"
            "  - Nested"
        )
        once = sanitize_for_slack(text)
        twice = sanitize_for_slack(once)
        self.assertEqual(once, twice)

    def test_cronjobs_conversational_smoke_test(self):
        """
        Smoke test scenario:
        Raw terminal buffer containing mate/supervisor.py...) debris, spinner crumbs,
        and CommonMark ### headers.
        Assert output starts cleanly without debris and headers render as bold mrkdwn.
        """
        raw_buffer = (
            "● View file (mate/supervisor.py...)\n"
            "mate/supervisor.py...)\n"
            "3...)\n"
            "expand)\n"
            "⠋\n"
            ">\n"
            "Here is the breakdown of our scheduled cronjobs across the fleet:\n\n"
            "### Cloud Ops (oci-machine)\n"
            "• Dynamic DNS updater (hourly)\n"
            "• Wireguard tunnel watchdog (every 5m)\n\n"
            "### Homelab Ops (localhost)\n"
            "• ZFS pool scrubbing & backup (nightly at 02:00 UTC)\n\n"
            "Would you like to review or edit any of these schedules?\n"
            ">\n"
        )
        cleaned = sanitize_for_slack(raw_buffer)

        # 1. Zero mate/supervisor or widget debris
        self.assertNotIn("mate/supervisor.py...)", cleaned)
        self.assertNotIn("3...)", cleaned)
        self.assertNotIn("expand)", cleaned)
        self.assertNotIn("⠋", cleaned)
        self.assertFalse(cleaned.startswith(">"))
        self.assertTrue(cleaned.startswith("Here is the breakdown"))

        # 2. Section headers render cleanly with bold titles instead of literal ### hashes
        self.assertNotIn("###", cleaned)
        self.assertIn("*Cloud Ops (oci-machine)*", cleaned)
        self.assertIn("*Homelab Ops (localhost)*", cleaned)

    def test_bluebubbles_isolation_retains_clean_urls_without_slack_markup(self):
        """Verify BlueBubbles normalizer strips TUI debris but does NOT inject Slack mrkdwn."""
        from app import normalize_for_bluebubbles

        raw = (
            "mate/supervisor.py...)\n"
            "3...)\n"
            "expand)\n"
            "Check out <http://localhost:8080|Dashboard> and <https://example.com> :white_check_mark:"
        )
        cleaned = normalize_for_bluebubbles(raw)

        # Debris stripped
        self.assertNotIn("mate/supervisor.py...)", cleaned)
        self.assertNotIn("3...)", cleaned)
        self.assertNotIn("expand)", cleaned)

        # Clean plain-text formatting (no <url|label> Slack markup)
        self.assertNotIn("<http://localhost:8080|Dashboard>", cleaned)
        self.assertIn("Dashboard (http://localhost:8080)", cleaned)
        self.assertIn("✅", cleaned)

    def test_strips_interactive_navigation_chrome(self):
        """Asserts ↑/↓ Navigate... and └ Tip:... are completely stripped."""
        raw_text = (
            "Here is the status report:\n"
            "↑/↓ Navigate · enter Select · esc Skip\n"
            "? for shortcuts\n"
            "press enter to continue\n"
            "└ Tip: In /diff, press c to add inline comments...\n"
            "Tip: Use --help for more information.\n"
            "All systems operational."
        )
        cleaned = sanitize_for_slack(raw_text)
        self.assertNotIn("Navigate", cleaned)
        self.assertNotIn("Select", cleaned)
        self.assertNotIn("Skip", cleaned)
        self.assertNotIn("? for shortcuts", cleaned)
        self.assertNotIn("press enter to continue", cleaned)
        self.assertNotIn("Tip:", cleaned)
        self.assertNotIn("inline comments", cleaned)
        self.assertEqual(cleaned, "Here is the status report:\nAll systems operational.")

    def test_converts_markdown_to_slack_syntax(self):
        """Asserts ### Header becomes *Header* and **bold** becomes *bold*."""
        raw_markdown = "### Header\nThis is **bold** text."
        converted = sanitize_for_slack(raw_markdown)
        self.assertIn("*Header*", converted)
        self.assertNotIn("### Header", converted)
        self.assertIn("*bold*", converted)
        self.assertNotIn("**bold**", converted)
        self.assertEqual(converted, "*Header*\nThis is *bold* text.")

    def test_sentences_with_ellipses_not_stripped(self):
        """Verify normal sentences ending with parentheses and ellipses are not accidentally stripped."""
        text = "Here is a list (item 1, item 2, ...)\nDone."
        cleaned = sanitize_for_slack(text)
        self.assertIn("Here is a list (item 1, item 2, ...)", cleaned)
        self.assertIn("Done.", cleaned)

    def test_heading_with_multiple_asterisk_pairs_preserved(self):
        """Verify headings containing multiple asterisk pairs do not have inner syntax mangled."""
        text = "### Setup *component A* and *component B*"
        cleaned = sanitize_for_slack(text)
        self.assertIn("*Setup _component A_ and _component B_*", cleaned)

    def test_backslash_shortcuts_navigation_chrome_stripped(self):
        """Verify \\? for shortcuts with literal backslash is completely stripped."""
        text = "Status ok.\n\\? for shortcuts\nReady."
        cleaned = sanitize_for_slack(text)
        self.assertNotIn("shortcuts", cleaned)
        self.assertEqual(cleaned, "Status ok.\nReady.")


if __name__ == "__main__":
    unittest.main()
