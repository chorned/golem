"""
tests/test_adversarial_normalizer.py
Empirical adversarial test harness for First Mate 2.0 wire normalizer.

Adversarial test categories:
1. Exotic ANSI Escape Sequences (256-color, 24-bit RGB, cursor movements, OSC window titles, OSC 8 hyperlinks).
2. Code Fences & Backticks (nested fences, escaped backticks, unclosed fences/backticks, code blocks with headings/bold/curses).
3. Starship Prompt Echoes & Box-Drawing (unicode ranges \\u2500-\\u257f, light/heavy/double/rounded/diagonal, table borders, prompt echoes, blockquote preservation).
4. Scale, Performance & Memory (100,000+ character inputs, ReDoS stress, memory stability).
5. TC-05 Invariant Fuzzing (5,000 randomized adversarial inputs verifying zero ATX #, zero **, zero curses runes, valid link formatting).
6. Compatibility Shim Verification (root normalizer.py parity).
"""

import random
import re
import string
import time
import tracemalloc
import unittest

import first_mate.transport.normalizer as canonical_normalizer
import normalizer as shim_normalizer


class TestAdversarialAnsiEscapes(unittest.TestCase):
    """Stress-test ANSI escape sequence stripping under adversarial inputs."""

    def setUp(self):
        self.normalizers = [canonical_normalizer, shim_normalizer]

    def test_256_color_escapes(self):
        """Verify 256-color foreground and background escape sequences are fully stripped."""
        for norm in self.normalizers:
            for color_code in [0, 16, 128, 196, 231, 255]:
                raw_fg = f"\x1b[38;5;{color_code}mColorText {color_code}\x1b[0m"
                raw_bg = f"\x1b[48;5;{color_code}mBgText {color_code}\x1b[0m"
                out_fg = norm.sanitize_for_slack(raw_fg)
                out_bg = norm.sanitize_for_slack(raw_bg)
                self.assertNotIn("\x1b", out_fg)
                self.assertNotIn("\x1b", out_bg)
                self.assertEqual(out_fg, f"ColorText {color_code}")
                self.assertEqual(out_bg, f"BgText {color_code}")

    def test_24_bit_truecolor_escapes(self):
        """Verify 24-bit RGB truecolor escapes (both semicolon and colon formats) are stripped."""
        for norm in self.normalizers:
            samples = [
                "\x1b[38;2;255;0;0mRed\x1b[0m",
                "\x1b[38;2;0;255;0mGreen\x1b[0m",
                "\x1b[38;2;0;0;255mBlue\x1b[0m",
                "\x1b[48;2;128;64;32mCustom Bg\x1b[0m",
                "\x1b[38:2::255:128:64mColon RGB\x1b[0m",
                "\x1b[48:2::10:20:30mColon Bg RGB\x1b[0m",
                "\x1b[58;2;255;255;0mUnderline RGB\x1b[0m",
            ]
            for s in samples:
                out = norm.sanitize_for_slack(s)
                self.assertNotIn("\x1b", out)
                self.assertFalse(re.search(r"\[\d+[:;]", out))

    def test_cursor_movement_escapes(self):
        """Verify cursor movements, clear screen, and private modes are stripped."""
        for norm in self.normalizers:
            samples = [
                ("\x1b[2JClear\x1b[HHome", "ClearHome"),
                ("\x1b[10;20HPositioned", "Positioned"),
                ("\x1b[1AUp\x1b[2BDown\x1b[3CForward\x1b[4DBack", "UpDownForwardBack"),
                ("\x1b[2KLineClear", "LineClear"),
                ("\x1b[?25hCursorOn\x1b[?25lCursorOff", "CursorOnCursorOff"),
                ("\x1b[?1049hAltScreen\x1b[?1049lMainScreen", "AltScreenMainScreen"),
                ("\x1b[sSaved\x1b[uRestored", "SavedRestored"),
                ("\x1b[>4;2mOtherKeys", "OtherKeys"),
            ]
            for s, expected in samples:
                out = norm.sanitize_for_slack(s)
                self.assertNotIn("\x1b", out)
                self.assertEqual(out, expected)

    def test_osc_window_titles_and_hyperlinks(self):
        """Verify OSC window titles (BEL and ST terminated) and OSC 8 hyperlinks are stripped."""
        for norm in self.normalizers:
            samples = [
                ("\x1b]0;Terminal Window Title\x07Visible text", "Visible text"),
                ("\x1b]2;Another Window Title\x1b\\Visible text", "Visible text"),
                ("\x1b]8;;https://firstmate.ai\x1b\\Click Here\x1b]8;;\x1b\\", "Click Here"),
                ("\x1b]1337;CustomProp=Value\x07Visible", "Visible"),
            ]
            for s, expected in samples:
                out = norm.sanitize_for_slack(s)
                self.assertNotIn("\x1b", out)
                self.assertEqual(out, expected)

    def test_mixed_ansi_in_code_blocks_and_prose(self):
        """Verify ANSI escapes are stripped even when interspersed with markdown and code blocks."""
        for norm in self.normalizers:
            text = (
                "\x1b[32m### Normalizing\x1b[0m Status\n"
                "Here is \x1b[1m**bold**\x1b[0m and \x1b[34m`code`\x1b[0m.\n"
                "```bash\n"
                "echo '\x1b[31mError\x1b[0m'\n"
                "```"
            )
            out = norm.sanitize_for_slack(text)
            self.assertNotIn("\x1b[32m", out)
            self.assertNotIn("\x1b[0m", out)
            self.assertNotIn("\x1b[1m", out)
            self.assertNotIn("\x1b[34m", out)
            self.assertIn("*Normalizing Status*", out)
            self.assertIn("*bold*", out)
            self.assertIn("`code`", out)


class TestAdversarialCodeFencesAndBackticks(unittest.TestCase):
    """Stress-test code block preservation, fence nesting, and backtick edge cases."""

    def setUp(self):
        self.normalizers = [canonical_normalizer, shim_normalizer]

    def test_nested_code_fences(self):
        """Verify 4-backtick fence cleanly encloses 3-backtick fence without premature closing."""
        for norm in self.normalizers:
            text = (
                "````markdown\n"
                "```python\n"
                "# not an ATX header\n"
                "x = 2 ** 8\n"
                "```\n"
                "````"
            )
            out = norm.sanitize_for_slack(text)
            self.assertEqual(out, text)
            self.assertIn("# not an ATX header", out)
            self.assertIn("2 ** 8", out)

    def test_tilde_fences_enclosing_backtick_fences(self):
        """Verify tilde fence (~~~) encloses backtick fence (```) without interference."""
        for norm in self.normalizers:
            text = (
                "~~~markdown\n"
                "```bash\n"
                "# comment\n"
                "echo **star**\n"
                "```\n"
                "~~~"
            )
            out = norm.sanitize_for_slack(text)
            self.assertEqual(out, text)
            self.assertIn("# comment", out)
            self.assertIn("**star**", out)

    def test_markdown_elements_inside_code_blocks(self):
        """Verify ATX headings, bold, curses runes, and prompt echoes inside code blocks are preserved verbatim."""
        for norm in self.normalizers:
            text = (
                "```python\n"
                "# Heading 1\n"
                "## Heading 2\n"
                "def calculate(base, exp):\n"
                "    \"\"\"\n"
                "    ┌─────────────┐\n"
                "    │ Math Engine │\n"
                "    └─────────────┘\n"
                "    \"\"\"\n"
                "    ❯ git branch\n"
                "    return base ** exp\n"
                "```"
            )
            out = norm.sanitize_for_slack(text)
            self.assertEqual(out, text)
            self.assertIn("# Heading 1", out)
            self.assertIn("┌─────────────┐", out)
            self.assertIn("❯ git branch", out)
            self.assertIn("base ** exp", out)

    def test_inline_code_backtick_variations(self):
        """Verify inline backtick variations (single, double, triple, with symbols) are protected."""
        for norm in self.normalizers:
            cases = [
                ("`# not a header`", "`# not a header`"),
                ("`**not bold**`", "`**not bold**`"),
                ("`│ not a box │`", "`│ not a box │`"),
                ("`` `# not header` ``", "`` `# not header` ``"),
                ("`` `x ** 2` and `*y*` ``", "`` `x ** 2` and `*y*` ``"),
            ]
            for input_str, expected in cases:
                out = norm.sanitize_for_slack(input_str)
                self.assertEqual(out, expected)

    def test_escaped_backticks(self):
        """Verify escaped backticks in markdown prose do not corrupt formatting."""
        for norm in self.normalizers:
            text = r"Here is a \`literal backtick\` and **bold text**."
            out = norm.sanitize_for_slack(text)
            self.assertIn(r"\`literal backtick\`", out)
            self.assertIn("*bold text*", out)
            self.assertNotIn("**", out)

    def test_unclosed_backtick_inline(self):
        """Verify unclosed single backtick does not swallow subsequent headings or bold text."""
        for norm in self.normalizers:
            text = (
                "Here is an unclosed `backtick in text.\n"
                "# Real Header\n"
                "Some **bold text**."
            )
            out = norm.sanitize_for_slack(text)
            self.assertIn("*Real Header*", out)
            self.assertIn("*bold text*", out)
            self.assertNotIn("**bold text**", out)

    def test_unclosed_fenced_code_block(self):
        """Verify unclosed fenced code block extends safely to the end of input without crash."""
        for norm in self.normalizers:
            text = (
                "```python\n"
                "def foo():\n"
                "    # unclosed block comment\n"
                "    return 42 ** 2"
            )
            out = norm.sanitize_for_slack(text)
            self.assertEqual(out, text)

    def test_heading_containing_inline_code(self):
        """Verify heading with inline code keeps code verbatim while converting outer heading to bold."""
        for norm in self.normalizers:
            text = "# Heading with `code # not a header` and `x ** 2`"
            out = norm.sanitize_for_slack(text)
            self.assertEqual(out, "*Heading with `code # not a header` and `x ** 2`*")


class TestAdversarialStarshipAndBoxDrawing(unittest.TestCase):
    """Stress-test Starship prompt echo stripping and unicode box-drawing glyph cleanup."""

    def setUp(self):
        self.normalizers = [canonical_normalizer, shim_normalizer]

    def test_starship_prompt_echoes(self):
        """Verify Starship prompt echoes with commands are cleanly stripped."""
        for norm in self.normalizers:
            samples = [
                ("❯ git status\nOn branch main\nnothing to commit", "On branch main\nnothing to commit"),
                ("❯ cargo test --all\nrunning 15 tests\ntest result: ok", "running 15 tests\ntest result: ok"),
                ("   ❯ npm run build\nBundling completed in 1.2s", "Bundling completed in 1.2s"),
                ("❯ python3 app.py\nServer started on 8080", "Server started on 8080"),
            ]
            for s, expected in samples:
                out = norm.sanitize_for_slack(s)
                self.assertEqual(out, expected)

    def test_empty_prompt_markers(self):
        r"""Verify empty prompt markers (^>$, ^❯$, ^\$$) are stripped while blockquotes are preserved."""
        for norm in self.normalizers:
            s = "❯\nOutput line 1\n>\nOutput line 2\n$\nOutput line 3"
            out = norm.sanitize_for_slack(s)
            self.assertEqual(out, "Output line 1\nOutput line 2\nOutput line 3")

    def test_blockquote_strict_preservation(self):
        """Verify blockquotes (> text) are strictly preserved and never mistaken for prompts."""
        for norm in self.normalizers:
            s = (
                "> Important announcement\n"
                "> Deployment complete\n"
                "❯ git status\n"
                "> ❯ Note: prompt symbol inside blockquote"
            )
            out = norm.sanitize_for_slack(s)
            self.assertIn("> Important announcement", out)
            self.assertIn("> Deployment complete", out)
            self.assertIn("> ❯ Note: prompt symbol inside blockquote", out)
            self.assertNotIn("\n❯ git status", out)

    def test_box_drawing_unicode_ranges(self):
        r"""Verify all unicode box glyphs (\u2500-\u257f) are stripped outside code blocks."""
        for norm in self.normalizers:
            # Generate sample with every block of box drawing glyphs
            box_runes = "".join(chr(cp) for cp in range(0x2500, 0x2580))
            raw = f"Prefix text {box_runes} Suffix text"
            out = norm.sanitize_for_slack(raw)
            self.assertFalse(re.search(r"[\u2500-\u257f]", out))
            self.assertIn("Prefix text", out)
            self.assertIn("Suffix text", out)

    def test_box_tables_and_substantive_text_extraction(self):
        """Verify table borders are discarded while substantive cell contents are extracted."""
        for norm in self.normalizers:
            table = (
                "┌──────────────┬──────────────┬──────────────┐\n"
                "│ Host         │ Role         │ Status       │\n"
                "├──────────────┼──────────────┼──────────────┤\n"
                "│ localhost    │ liaison      │ online       │\n"
                "│ dev-grid     │ worker       │ idle         │\n"
                "└──────────────┴──────────────┴──────────────┘"
            )
            out = norm.sanitize_for_slack(table)
            self.assertFalse(re.search(r"[\u2500-\u257f]", out))
            self.assertIn("Host", out)
            self.assertIn("Role", out)
            self.assertIn("Status", out)
            self.assertIn("localhost", out)
            self.assertIn("dev-grid", out)

    def test_braille_spinners_and_tui_crumbs(self):
        """Verify braille spinners, ASCII spinners, navigation hints, and tool crumbs are stripped."""
        for norm in self.normalizers:
            debris = (
                "⠋ Loading packages...\n"
                "● herdr_pane_run executed\n"
                "▸ tool parameter inspected\n"
                "[-]\n"
                "↑/↓ Navigate  •  Enter Select  •  Esc Skip\n"
                "? for shortcuts\n"
                "└ Tip: Use --json for output\n"
                "Valid Content Line"
            )
            out = norm.sanitize_for_slack(debris)
            self.assertFalse(re.search(r"[\u2800-\u28ff]", out))
            self.assertNotIn("●", out)
            self.assertNotIn("▸", out)
            self.assertNotIn("Navigate", out)
            self.assertNotIn("Tip:", out)
            self.assertEqual(out, "Valid Content Line")


class TestAdversarialScaleAndPerformance(unittest.TestCase):
    """Stress-test normalizer performance and memory on payloads exceeding 100,000 characters."""

    def setUp(self):
        self.normalizer = canonical_normalizer

    def test_100k_plain_text_perf(self):
        """100k+ plain text payload must execute in < 0.5s and consume < 5MB memory."""
        payload = "The quick brown fox jumps over the lazy dog. A quick test line.\n" * 2000  # ~130,000 chars
        self.assertGreater(len(payload), 100000)

        tracemalloc.start()
        t0 = time.perf_counter()
        out = self.normalizer.sanitize_for_slack(payload)
        elapsed = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.assertLess(elapsed, 0.5, f"100k plain text took {elapsed:.4f}s (exceeds 0.5s)")
        self.assertLess(peak, 5 * 1024 * 1024, f"100k plain text peak memory {peak / 1024 / 1024:.2f}MB (exceeds 5MB)")
        self.assertTrue(len(out) > 0)

    def test_100k_ansi_heavy_perf(self):
        """100k+ ANSI-heavy payload must execute in < 0.5s."""
        ansi_line = "\x1b[38;2;120;200;80mStatus: \x1b[1mOK\x1b[0m \x1b]0;Title\x07\x1b[2K\n"
        payload = ansi_line * 2000  # ~116,000 chars
        self.assertGreater(len(payload), 100000)

        t0 = time.perf_counter()
        out = self.normalizer.sanitize_for_slack(payload)
        elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 0.5, f"100k ANSI took {elapsed:.4f}s")
        self.assertNotIn("\x1b", out)

    def test_100k_box_drawing_tables_perf(self):
        """100k+ box table payload must execute in < 0.5s."""
        table = (
            "┌──────────────┬──────────────┐\n"
            "│ Column A     │ Column B     │\n"
            "├──────────────┼──────────────┤\n"
            "│ Metric 1     │ Metric 2     │\n"
            "└──────────────┴──────────────┘\n"
        )
        payload = table * 800  # ~128,000 chars
        self.assertGreater(len(payload), 100000)

        t0 = time.perf_counter()
        out = self.normalizer.sanitize_for_slack(payload)
        elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 0.5, f"100k box table took {elapsed:.4f}s")
        self.assertFalse(re.search(r"[\u2500-\u257f]", out))

    def test_100k_code_blocks_dense_perf(self):
        """100k+ code blocks (1,000+ distinct blocks) must execute in < 1.0s."""
        code_block = (
            "```python\n"
            "def fn(x):\n"
            "    # comment inside block\n"
            "    return x ** 2\n"
            "```\n"
            "Prose line.\n"
        )
        payload = code_block * 1500  # ~123,000 chars
        self.assertGreater(len(payload), 100000)

        t0 = time.perf_counter()
        out = self.normalizer.sanitize_for_slack(payload)
        elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 1.0, f"100k code blocks took {elapsed:.4f}s")
        self.assertIn("# comment inside block", out)
        self.assertIn("x ** 2", out)

    def test_100k_unclosed_fence_perf(self):
        """100k+ unclosed code fence must execute in < 0.5s."""
        payload = "```python\n" + ("line = 1\n# comment\nx = y ** 2\n" * 4000)  # ~120,000 chars
        self.assertGreater(len(payload), 100000)

        t0 = time.perf_counter()
        out = self.normalizer.sanitize_for_slack(payload)
        elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 0.5, f"100k unclosed fence took {elapsed:.4f}s")
        self.assertEqual(out, payload.rstrip())

    def test_100k_unclosed_backtick_redos_resilience(self):
        """100k+ unclosed backtick payload must not trigger polynomial ReDoS backtracking."""
        payload = "`" + ("token " * 25000)
        self.assertGreater(len(payload), 100000)

        t0 = time.perf_counter()
        out = self.normalizer.sanitize_for_slack(payload)
        elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 0.5, f"100k unclosed backticks took {elapsed:.4f}s (possible ReDoS)")

    def test_100k_mixed_adversarial_scale(self):
        """175k+ mixed adversarial payload combining all elements must execute cleanly in < 1.0s."""
        blocks = []
        for i in range(1000):
            blocks.append(f"### Subheading {i}\n")
            blocks.append(f"\x1b[38;5;120m│ Worker {i} │\x1b[0m **active**\n")
            blocks.append(f"❯ git checkout task-{i}\n")
            blocks.append(f"```json\n{{\"id\": {i}, \"hash\": \"#hash\", \"exp\": 2 ** 4}}\n```\n")
            blocks.append(f"[Status {i}](https://cluster.local/node/{i})\n\n")
        payload = "".join(blocks)
        self.assertGreater(len(payload), 170000)

        tracemalloc.start()
        t0 = time.perf_counter()
        out = self.normalizer.sanitize_for_slack(payload)
        elapsed = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.assertLess(elapsed, 1.0, f"175k mixed payload took {elapsed:.4f}s")
        self.assertLess(peak, 10 * 1024 * 1024, f"Peak memory {peak / 1024 / 1024:.2f}MB")

        # Invariant check on output: outside code blocks
        text_outside_code, _ = self.normalizer.protect_code_blocks(out)
        self.assertFalse(re.search(r"^[ ]{0,3}#{1,6}\s+", text_outside_code, re.MULTILINE))
        self.assertNotIn("**", text_outside_code)
        self.assertFalse(re.search(r"[\u2500-\u257f]", text_outside_code))


class TestTC05InvariantsFuzzing(unittest.TestCase):
    """
    Property-based fuzz testing verifying TC-05 invariants hold across 5,000 randomized inputs:
    - Invariant 1: Zero ATX '#' outside code blocks (all headings converted to *Header*).
    - Invariant 2: Zero '**' outside code blocks (all bold converted to *bold*).
    - Invariant 3: Zero curses runes [\\u2500-\\u257f] outside code blocks.
    - Invariant 4: All markdown links [label](url) transpiled to <url|label>.
    """

    def setUp(self):
        self.normalizer = canonical_normalizer

    def _generate_adversarial_snippet(self, rng: random.Random) -> str:
        """Generate a randomized snippet exercising markdown, ANSI, TUI, or code blocks."""
        choice = rng.randint(1, 10)
        word = "".join(rng.choices(string.ascii_letters, k=rng.randint(3, 8)))

        if choice == 1:
            # ATX Heading (1-6 hashes, with 0-3 spaces and optional blockquote)
            hashes = "#" * rng.randint(1, 6)
            indent = " " * rng.randint(0, 3)
            bq = "> " if rng.random() < 0.3 else ""
            return f"\n{indent}{bq}{hashes} Heading {word}\n"
        elif choice == 2:
            # Markdown bold or bold-italic
            stars = "*" * rng.choice([2, 3, 4])
            return f" {stars}bold {word}{stars} "
        elif choice == 3:
            # Box-drawing characters
            runes = "".join(chr(rng.randint(0x2500, 0x257F)) for _ in range(rng.randint(1, 5)))
            return f"\n│ {word} {runes} │\n"
        elif choice == 4:
            # Markdown link or image
            url = f"https://example.com/{word}"
            if rng.random() < 0.2:
                return f" ![{word} image]({url}) "
            return f" [{word} link]({url}) "
        elif choice == 5:
            # Fenced code block with code containing #, **, and box runes
            fence = "`" * rng.choice([3, 4])
            return (
                f"\n{fence}python\n"
                f"# comment {word}\n"
                f"x = {word} ** 2\n"
                f"box = '┌───┐'\n"
                f"{fence}\n"
            )
        elif choice == 6:
            # Inline code span
            return f" `{word} # not a header **not bold** ─` "
        elif choice == 7:
            # ANSI escape sequence
            return f" \x1b[38;5;{rng.randint(1, 255)}m{word}\x1b[0m "
        elif choice == 8:
            # Starship prompt echo
            return f"\n❯ git {word}\n"
        elif choice == 9:
            # Bare URL
            return f" https://example.com/api/{word} "
        else:
            # Ordinary text
            return f" text_{word} "

    def test_5000_fuzzed_adversarial_inputs_tc05(self):
        """Run 5,000 fuzzed iterations verifying all TC-05 invariants hold unconditionally."""
        rng = random.Random(42)  # Deterministic seed

        for batch_idx in range(500):
            # Assemble a document from 10 random snippets
            num_snippets = rng.randint(5, 15)
            raw_input = "".join(self._generate_adversarial_snippet(rng) for _ in range(num_snippets))

            sanitized = self.normalizer.sanitize_for_slack(raw_input)

            # Extract content outside protected code blocks
            text_outside_code, _ = self.normalizer.protect_code_blocks(sanitized)

            # Invariant 1: Zero ATX '#' outside code blocks
            atx_match = re.search(r"^[ ]{0,3}(?:>[ ]*)*#{1,6}(?:[ \t]+.*|[ \t]*)$", text_outside_code, re.MULTILINE)
            self.assertIsNone(
                atx_match,
                f"TC-05 Violation: Leaked ATX heading '{atx_match.group(0) if atx_match else ''}'\nInput:\n{raw_input}\nOutput:\n{sanitized}"
            )

            # Invariant 2: Zero '**' outside code blocks
            self.assertNotIn(
                "**",
                text_outside_code,
                f"TC-05 Violation: Leaked double asterisks '**'\nInput:\n{raw_input}\nOutput:\n{sanitized}"
            )

            # Invariant 3: Zero curses runes [\u2500-\u257f] outside code blocks
            box_match = re.search(r"[\u2500-\u257f]", text_outside_code)
            self.assertIsNone(
                box_match,
                f"TC-05 Violation: Leaked curses rune '{box_match.group(0) if box_match else ''}'\nInput:\n{raw_input}\nOutput:\n{sanitized}"
            )

            # Invariant 4: All markdown links [label](url) transpiled to <url|label>
            untranspiled_link = re.search(r"\[([^\]\r\n]+)\]\((https?://[^\s()]+)\)", text_outside_code)
            self.assertIsNone(
                untranspiled_link,
                f"TC-05 Violation: Leaked untranspiled markdown link '{untranspiled_link.group(0) if untranspiled_link else ''}'\nInput:\n{raw_input}\nOutput:\n{sanitized}"
            )


class TestBackwardsCompatibilityParity(unittest.TestCase):
    """Verify that root normalizer.py is a 100% transparent shim for first_mate.transport.normalizer."""

    def test_all_symbols_identical(self):
        shim_symbols = shim_normalizer.__all__
        self.assertTrue(len(shim_symbols) > 0)

        for sym in shim_symbols:
            self.assertTrue(
                hasattr(canonical_normalizer, sym),
                f"Symbol {sym} in normalizer.__all__ missing from first_mate.transport.normalizer"
            )
            canon_val = getattr(canonical_normalizer, sym)
            shim_val = getattr(shim_normalizer, sym)
            self.assertIs(canon_val, shim_val, f"Symbol {sym} does not refer to the canonical object")


if __name__ == "__main__":
    unittest.main()
