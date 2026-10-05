#!/usr/bin/env python3
"""
tests/test_transport_m1.py
Comprehensive unit tests for Milestone 1 (Transport & Wire Normalization).

Test coverage:
1. Wire Normalizer:
   - ANSI stripping
   - Box-drawing glyph and border line stripping
   - Prompt echo and command echo stripping while preserving blockquotes
   - Bidirectional code block protection (fenced and inline backticks)
   - CommonMark to Slack mrkdwn transpilation (headings, bold, links, images, bare URLs)
   - TC-05 acceptance battery (zero ATX #, zero **, zero box runes, all URLs in Slack format)
   - Interface aliases (strip_terminal_chrome, transpile_markdown_to_slack)
2. Boundary-Aware Chunker:
   - Message length budgeting (<= max_chars)
   - Priority slicing: paragraph (\n\n outside blocks) -> newline (\n) -> sentence -> whitespace -> hard cutoff
   - Code fence invariant: Chunk 1 closed with \n{fence}\n, Chunk 2 reopened with {fence}{lang}\n
   - Indentation preservation on first line of subsequent code chunks
   - Tilde fences (~~~) and language tag propagation
3. Milestone Updater:
   - Initial message creation via chat_postMessage
   - In-place updating via chat_update with token-bucket debounce
   - Error fallback: message_not_found and cant_update_message recover via chat_postMessage
   - Finalization: resolves card in-place into final response; posts overflow chunks if > max_chars
   - Resilient handling of clients lacking chat_update
4. Resilient Slack Client & Transport:
   - Idempotent reactions_add (already_reacted returns True)
   - Idempotent reactions_remove (no_reaction returns True)
   - Reaction swap
   - Exponential backoff retry on HTTP 429 respecting Retry-After
   - Rate limit exhaustion raises SlackRateLimitError
   - chat_update non-fatal error handling (returns None)
   - High-level SlackTransport façade (chunked sending, milestone factory)
5. Compatibility Shim:
   - Root normalizer.py re-exports identical symbols to golem.transport.normalizer
"""

import asyncio
import re
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from slack_sdk.errors import SlackApiError

import normalizer
from golem.transport import (
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
    split_message_boundary_aware,
    parse_code_blocks,
    CodeBlockSpan,
    MilestoneUpdater,
    SlackClient,
    SlackTransport,
    SlackTransportError,
    SlackRateLimitError,
    SlackMessageNotFoundError,
    SlackAuthenticationError,
)


class TestNormalizerWireEgress(unittest.TestCase):
    """Unit tests for wire normalization, debris stripping, and mrkdwn transpilation."""

    def test_strip_ansi_csi_osc_control(self):
        sample = "\x1b[31;1mError:\x1b[0m \x1b]0;Title\x07Something went wrong.\x1b[2K"
        cleaned = strip_ansi(sample)
        self.assertEqual(cleaned, "Error: Something went wrong.")

    def test_strip_box_drawing_glyphs_and_border_lines(self):
        sample = (
            "┌──────────────────────┐\n"
            "│ Node: fleet-control  │\n"
            "├──────────────────────┤\n"
            "│ Status: Online (OK)  │\n"
            "└──────────────────────┘"
        )
        cleaned = strip_box_drawing(sample)
        self.assertNotIn("┌", cleaned)
        self.assertNotIn("─", cleaned)
        self.assertNotIn("│", cleaned)
        self.assertNotIn("└", cleaned)
        self.assertIn("Node: fleet-control", cleaned)
        self.assertIn("Status: Online (OK)", cleaned)

    def test_strip_box_drawing_preserves_code_blocks(self):
        sample = (
            "Outer text with ─ box rune.\n"
            "```text\n"
            "┌───┐\n"
            "│Hi │\n"
            "└───┘\n"
            "```\n"
            "Run with `─` flag."
        )
        cleaned = strip_box_drawing(sample)
        # Inside fenced code block, box drawing is preserved verbatim
        self.assertIn("┌───┐\n│Hi │\n└───┘", cleaned)
        # Inside inline backticks, box rune is preserved verbatim
        self.assertIn("`─`", cleaned)
        # Outside code blocks, box rune is stripped
        self.assertNotIn("Outer text with ─", cleaned)
        self.assertIn("Outer text with  box rune.", cleaned)

    def test_strip_prompt_echoes_starship_and_empty(self):
        sample = (
            ">\n"
            "> \n"
            "❯ git checkout main\n"
            "Switched to branch 'main'\n"
            "❯ \n"
            "$\n"
            "hermes@localhost:~$ \n"
            "Done."
        )
        cleaned = strip_prompt_echoes(sample)
        self.assertNotIn("❯ git checkout main", cleaned)
        self.assertNotIn("hermes@localhost", cleaned)
        lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
        self.assertEqual(lines, ["Switched to branch 'main'", "Done."])

    def test_strip_prompt_echoes_strictly_preserves_blockquotes(self):
        sample = (
            ">\n"
            "> This is a legitimate blockquote.\n"
            "> Another line of quote.\n"
            ">\n"
            "Normal text."
        )
        cleaned = strip_prompt_echoes(sample)
        self.assertIn("> This is a legitimate blockquote.", cleaned)
        self.assertIn("> Another line of quote.", cleaned)
        self.assertIn("Normal text.", cleaned)

    def test_code_block_protection_fenced_and_inline(self):
        sample = (
            "Here is `inline code with ### and **bold** and ─`.\n"
            "```python\n"
            "# Inside fenced code\n"
            "def foo():\n"
            "    ### Not a heading\n"
            "    x = **invalid_syntax**\n"
            "    url = [not a link](http://foo)\n"
            "```\n"
            "### Real Heading\n"
            "**Real Bold**"
        )
        cleaned = sanitize_for_slack(sample)
        self.assertIn("`inline code with ### and **bold** and ─`", cleaned)
        self.assertIn("### Not a heading", cleaned)
        self.assertIn("x = **invalid_syntax**", cleaned)
        self.assertIn("[not a link](http://foo)", cleaned)
        self.assertIn("*Real Heading*", cleaned)
        self.assertIn("*Real Bold*", cleaned)
        self.assertNotIn("### Real Heading", cleaned)
        self.assertNotIn("**Real Bold**", cleaned)

    def test_transpile_markdown_headings_all_levels(self):
        sample = (
            "# H1\n"
            "## H2\n"
            "### H3 ###\n"
            "#### H4\n"
            "##### H5\n"
            "###### H6\n"
            "###   \n"
            "### 1. **Setup**\n"
            "### Step with *emphasis*"
        )
        cleaned = transpile_markdown_to_mrkdwn(sample)
        self.assertIn("*H1*", cleaned)
        self.assertIn("*H2*", cleaned)
        self.assertIn("*H3*", cleaned)
        self.assertIn("*H4*", cleaned)
        self.assertIn("*H5*", cleaned)
        self.assertIn("*H6*", cleaned)
        self.assertIn("*1. Setup*", cleaned)
        self.assertIn("*Step with _emphasis_*", cleaned)
        self.assertNotIn("###", cleaned)

    def test_transpile_markdown_bold_and_bold_italic(self):
        sample = "Here is ***bold italic*** and **plain bold** and unclosed **stray bold."
        cleaned = transpile_markdown_to_mrkdwn(sample)
        self.assertIn("*_bold italic_*", cleaned)
        self.assertIn("*plain bold*", cleaned)
        self.assertNotIn("**", cleaned)

    def test_transpile_markdown_links_and_images(self):
        sample = (
            "See [Docs](https://fleet.internal) and "
            "[Wiki](https://en.wikipedia.org/wiki/Daemon_(computing)) and "
            "![Diagram](https://example.com/arch.png) and "
            "[https://example.com](https://example.com)."
        )
        cleaned = transpile_markdown_to_mrkdwn(sample)
        self.assertIn("<https://fleet.internal|Docs>", cleaned)
        self.assertIn("<https://en.wikipedia.org/wiki/Daemon_(computing)|Wiki>", cleaned)
        self.assertIn("<https://example.com/arch.png|Diagram>", cleaned)
        self.assertIn("<https://example.com>", cleaned)

    def test_tc05_acceptance_criteria_strict_battery(self):
        """
        TC-05 Verification:
        Egress messages contain:
        - ZERO raw Markdown ATX hashes (###) outside code blocks
        - ZERO double asterisks (**) outside code blocks
        - ZERO box-drawing runes ([\u2500-\u257f]) outside code blocks
        - ALL Markdown links follow Slack format <url|label> or <url>
        """
        raw_payload = (
            "### Fleet Telemetry & Status\n"
            "The cluster is **operational** and **healthy**.\n"
            "┌─────────────────────────┐\n"
            "│ GPU Node: desktop-pc    │\n"
            "├─────────────────────────┤\n"
            "│ Utilization: 94%        │\n"
            "└─────────────────────────┘\n"
            "❯ herdr machine list\n"
            "mate/supervisor.py...)\n"
            "(expand)\n"
            "See [Docs](https://docs.firstmate.ai) or [Health](https://example.com/health) for info.\n"
            "```python\n"
            "# Code block retains everything verbatim\n"
            "header = '### not converted'\n"
            "bold = '**not bold**'\n"
            "box = '┌───┐'\n"
            "url = 'https://unconverted.internal'\n"
            "```\n"
            "Use `### test` and `**flag**` and `─` safely."
        )

        cleaned = sanitize_for_slack(raw_payload)

        # 1. Zero terminal widget debris
        self.assertNotIn("mate/supervisor.py...)", cleaned)
        self.assertNotIn("(expand)", cleaned)
        self.assertNotIn("❯ herdr machine list", cleaned)

        # Split into outside code vs inside code
        lines = cleaned.splitlines()
        in_code = False
        non_code_lines = []
        for line in lines:
            if line.startswith("```"):
                in_code = not in_code
                continue
            if not in_code:
                non_code_lines.append(line)

        # 2. Zero ATX hashes outside code blocks
        for line in non_code_lines:
            if "`" not in line:
                self.assertFalse(re.search(r"^#{1,6}\s", line), f"ATX hash leaked on line: {line}")
                self.assertNotIn("###", line, f"### leaked on line: {line}")

        # 3. Zero double asterisks outside code blocks
        for line in non_code_lines:
            if "`" not in line:
                self.assertNotIn("**", line, f"** leaked on line: {line}")

        # 4. Zero box-drawing glyphs outside code blocks
        for line in non_code_lines:
            if "`" not in line:
                self.assertFalse(re.search(r"[\u2500-\u257f]", line), f"Box glyph leaked on line: {line}")

        # 5. All URLs in Slack format
        self.assertIn("<https://docs.firstmate.ai|Docs>", cleaned)
        self.assertIn("<https://example.com/health|Health>", cleaned)
        self.assertNotIn("[Docs](https://docs.firstmate.ai)", cleaned)
        self.assertNotIn("[Health](https://example.com/health)", cleaned)

        # 6. Code block preserved verbatim
        self.assertIn("header = '### not converted'", cleaned)
        self.assertIn("bold = '**not bold**'", cleaned)
        self.assertIn("box = '┌───┐'", cleaned)

        # 7. Inline code spans preserved verbatim
        self.assertIn("`### test`", cleaned)
        self.assertIn("`**flag**`", cleaned)
        self.assertIn("`─`", cleaned)

    def test_aliases_strip_terminal_chrome_and_transpile_markdown_to_slack(self):
        text = "### Header\n**bold**\n\x1b[32mgreen\x1b[0m"
        self.assertEqual(strip_terminal_chrome(text), strip_tui_debris(text))
        self.assertEqual(transpile_markdown_to_slack(text), transpile_markdown_to_mrkdwn(text))


class TestChunkerBoundaryAware(unittest.TestCase):
    """Unit tests for boundary-aware, code-fence-preserving message chunker."""

    def test_short_message_intact(self):
        text = "Short single message"
        chunks = split_message_boundary_aware(text, max_chars=3800)
        self.assertEqual(chunks, [text])

    def test_empty_and_whitespace_input(self):
        self.assertEqual(split_message_boundary_aware(""), [])
        self.assertEqual(split_message_boundary_aware("   \n\n  "), [])

    def test_priority_1_paragraph_split_outside_code(self):
        p1 = "Paragraph 1 " * 200  # ~2400 chars
        p2 = "Paragraph 2 " * 200  # ~2400 chars
        full = f"{p1}\n\n{p2}"
        chunks = split_message_boundary_aware(full, max_chars=3800)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], p1.strip())
        self.assertEqual(chunks[1], p2.strip())

    def test_priority_1_ignores_double_newline_inside_code_blocks(self):
        lead = "Introduction lead paragraph."
        code_body = "def func1():\n    pass\n\n\ndef func2():\n    pass\n" * 50  # large code block
        trail = "Trailing conclusion paragraph."
        full = f"{lead}\n```python\n{code_body}```\n\n{trail}"

        chunks = split_message_boundary_aware(full, max_chars=1200)
        # Verify all chunks respect max_chars
        for idx, chunk in enumerate(chunks):
            self.assertLessEqual(len(chunk), 1200, f"Chunk {idx} exceeded max_chars: {len(chunk)}")
            # Verify code fences are properly balanced
            backtick_runs = [m.group(0) for m in re.finditer(r"^```\w*", chunk, re.MULTILINE)]
            self.assertTrue(len(backtick_runs) % 2 == 0, f"Unbalanced code fences in chunk {idx}:\n{chunk}")

    def test_code_fence_invariant_with_language(self):
        code_lines = [f"    line_{i} = 'some payload string value {i}'" for i in range(150)]
        code_text = "```python\n" + "\n".join(code_lines) + "\n```"
        self.assertGreater(len(code_text), 3800)

        chunks = split_message_boundary_aware(code_text, max_chars=2000)
        self.assertGreaterEqual(len(chunks), 2)

        # Chunk 1 must close with \n```\n
        self.assertTrue(chunks[0].startswith("```python\n"))
        self.assertTrue(chunks[0].rstrip().endswith("```"))

        # Chunk 2 must reopen with ```python\n
        self.assertTrue(chunks[1].startswith("```python\n"))

        # Every chunk must be <= max_chars
        for c in chunks:
            self.assertLessEqual(len(c), 2000)

    def test_code_fence_invariant_without_language(self):
        code_lines = [f"data_entry_{i} = {i * 10}" for i in range(150)]
        code_text = "```\n" + "\n".join(code_lines) + "\n```"
        chunks = split_message_boundary_aware(code_text, max_chars=1500)
        self.assertTrue(chunks[0].startswith("```\n"))
        self.assertTrue(chunks[0].rstrip().endswith("```"))
        self.assertTrue(chunks[1].startswith("```\n"))

    def test_code_fence_invariant_with_tildes(self):
        code_lines = [f"echo 'line {i}'" for i in range(150)]
        code_text = "~~~bash\n" + "\n".join(code_lines) + "\n~~~"
        chunks = split_message_boundary_aware(code_text, max_chars=1500)
        self.assertTrue(chunks[0].startswith("~~~bash\n"))
        self.assertTrue(chunks[0].rstrip().endswith("~~~"))
        self.assertTrue(chunks[1].startswith("~~~bash\n"))

    def test_preserves_indentation_in_chunk2(self):
        # When splitting inside a code block, 4-space indentation on first code line of Chunk 2 must not be stripped
        chunk1_body = "\n".join([f"    x_{i} = {i}" for i in range(50)])
        split_line = "    def target_function():\n        return True"
        code_text = f"```python\n{chunk1_body}\n{split_line}\n```"

        chunks = split_message_boundary_aware(code_text, max_chars=len(chunk1_body) + 30)
        self.assertGreaterEqual(len(chunks), 2)
        # Check that target_function in chunk 2 retains indentation
        chunk2_lines = chunks[1].splitlines()
        self.assertEqual(chunk2_lines[0], "```python")
        self.assertTrue(chunk2_lines[1].startswith("    def target_function"), f"Indentation lost: {chunk2_lines[1]}")

    def test_priority_3_sentence_and_whitespace_fallback(self):
        # Single long paragraph without newlines
        sentences = [f"This is sentence number {i} with some details." for i in range(120)]
        text = " ".join(sentences)
        self.assertGreater(len(text), 3800)
        chunks = split_message_boundary_aware(text, max_chars=1500)
        for c in chunks:
            self.assertLessEqual(len(c), 1500)
            # Should end on a sentence boundary or word boundary
            self.assertTrue(c[-1] in ".!?" or c[-1].isalnum())

    def test_multiple_code_blocks_split(self):
        b1 = "```bash\n" + "\n".join([f"curl https://host/{i}" for i in range(40)]) + "\n```"
        b2 = "```json\n" + "\n".join([f'  "key_{i}": {i},' for i in range(40)]) + "\n```"
        text = f"{b1}\n\nIntermediary text\n\n{b2}"

        chunks = split_message_boundary_aware(text, max_chars=1200)
        for idx, chunk in enumerate(chunks):
            self.assertLessEqual(len(chunk), 1200)


class TestMilestoneUpdater(unittest.IsolatedAsyncioTestCase):
    """Unit tests for debounced, in-place MilestoneUpdater with fallback."""

    async def test_initial_post_uses_chat_post_message(self):
        client = AsyncMock()
        client.chat_postMessage.return_value = {"ok": True, "ts": "100.123"}
        updater = MilestoneUpdater(client=client, min_interval=1.0)

        ts = await updater.post_or_update(channel="C1", thread_ts="100.000", text="Step 1: Starting")
        self.assertEqual(ts, "100.123")
        client.chat_postMessage.assert_called_once_with(
            channel="C1",
            thread_ts="100.000",
            text="Step 1: Starting"
        )
        self.assertEqual(client.chat_update.call_count, 0)

    async def test_debounce_throttling_trailing_update(self):
        client = AsyncMock()
        client.chat_postMessage.return_value = {"ok": True, "ts": "100.123"}
        client.chat_update.return_value = {"ok": True, "ts": "100.123"}
        updater = MilestoneUpdater(client=client, min_interval=0.05)

        # 1. Initial post
        await updater.post_or_update(channel="C1", thread_ts="100.000", text="Init")
        self.assertEqual(client.chat_postMessage.call_count, 1)

        # 2. Rapid second update immediately (should be debounced)
        await updater.post_or_update(channel="C1", thread_ts="100.000", text="Rapid 1")
        # chat_update has not been called yet because token bucket is empty
        self.assertEqual(client.chat_update.call_count, 0)

        # 3. Rapid third update before timer fires
        await updater.post_or_update(channel="C1", thread_ts="100.000", text="Rapid 2 (Final)")
        self.assertEqual(client.chat_update.call_count, 0)

        # 4. Wait for debounce interval to flush trailing update
        await asyncio.sleep(0.08)
        self.assertEqual(client.chat_update.call_count, 1)
        client.chat_update.assert_called_with(
            channel="C1",
            ts="100.123",
            text="Rapid 2 (Final)"
        )

    async def test_message_not_found_fallback_to_chat_post_message(self):
        client = AsyncMock()
        client.chat_postMessage.side_effect = [
            {"ok": True, "ts": "100.123"},
            {"ok": True, "ts": "100.456"},
        ]
        err_resp = {"ok": False, "error": "message_not_found"}
        client.chat_update.side_effect = SlackApiError("message_not_found", err_resp)

        updater = MilestoneUpdater(client=client, min_interval=0.0)
        # Initial post
        await updater.post_or_update(channel="C1", thread_ts="100.000", text="Init")

        # Update fails with message_not_found -> recovers with fresh chat_postMessage
        await updater.post_or_update(channel="C1", thread_ts="100.000", text="Update")
        self.assertEqual(client.chat_postMessage.call_count, 2)
        self.assertEqual(updater.message_ts, "100.456")

    async def test_cant_update_message_fallback_to_chat_post_message(self):
        client = AsyncMock()
        client.chat_postMessage.side_effect = [
            {"ok": True, "ts": "100.123"},
            {"ok": True, "ts": "100.789"},
        ]
        err_resp = {"ok": False, "error": "cant_update_message"}
        client.chat_update.side_effect = SlackApiError("cant_update_message", err_resp)

        updater = MilestoneUpdater(client=client, min_interval=0.0)
        await updater.post_or_update(channel="C1", thread_ts="100.000", text="Init")
        await updater.post_or_update(channel="C1", thread_ts="100.000", text="Update")

        self.assertEqual(client.chat_postMessage.call_count, 2)
        self.assertEqual(updater.message_ts, "100.789")

    async def test_finalize_in_place_clean_resolution(self):
        client = AsyncMock()
        client.chat_postMessage.return_value = {"ok": True, "ts": "100.123"}
        client.chat_update.return_value = {"ok": True, "ts": "100.123"}
        updater = MilestoneUpdater(client=client, min_interval=1.0)

        await updater.post_or_update(channel="C1", thread_ts="100.000", text="Working...")
        await updater.finalize(channel="C1", thread_ts="100.000", final_text="Task Complete!")

        # Final text resolved in-place on card
        client.chat_update.assert_called_with(
            channel="C1",
            ts="100.123",
            text="Task Complete!"
        )
        self.assertTrue(updater.is_finalized)

    async def test_finalize_with_overflow_emits_threaded_replies(self):
        client = AsyncMock()
        client.chat_postMessage.return_value = {"ok": True, "ts": "100.123"}
        client.chat_update.return_value = {"ok": True, "ts": "100.123"}
        updater = MilestoneUpdater(client=client, max_chars=1000)

        await updater.post_or_update(channel="C1", thread_ts="100.000", text="Working...")

        long_text = "Paragraph 1 " * 100 + "\n\n" + "Paragraph 2 " * 100  # ~2400 chars
        await updater.finalize(channel="C1", thread_ts="100.000", final_text=long_text)

        # Chunk 1 updated in-place
        self.assertEqual(client.chat_update.call_count, 1)
        # Overflow chunk posted as reply
        self.assertGreaterEqual(client.chat_postMessage.call_count, 2)

    async def test_finalize_without_initial_post(self):
        client = AsyncMock()
        client.chat_postMessage.return_value = {"ok": True, "ts": "100.999"}
        updater = MilestoneUpdater(client=client)

        await updater.finalize(channel="C1", thread_ts="100.000", final_text="Direct completion")
        client.chat_postMessage.assert_called_once_with(
            channel="C1",
            thread_ts="100.000",
            text="Direct completion"
        )
        self.assertEqual(client.chat_update.call_count, 0)

    async def test_client_without_chat_update_graceful(self):
        # Non-Slack mock without chat_update
        mock_client = MagicMock(spec=["chat_postMessage"])
        mock_client.chat_postMessage = AsyncMock(return_value={"ts": "100.1"})
        updater = MilestoneUpdater(client=mock_client, min_interval=0.0)

        await updater.post_or_update(channel="C1", thread_ts="100.0", text="Hello")
        await updater.post_or_update(channel="C1", thread_ts="100.0", text="Update")
        await updater.finalize(channel="C1", thread_ts="100.0", final_text="Done")
        self.assertTrue(updater.is_finalized)


class TestSlackClient(unittest.IsolatedAsyncioTestCase):
    """Unit tests for resilient SlackClient wrapper and error handling."""

    async def test_reactions_add_success(self):
        web_mock = AsyncMock()
        web_mock.reactions_add.return_value = {"ok": True}
        client = SlackClient(client=web_mock)
        res = await client.reactions_add(channel="C1", name="gear", timestamp="100.1")
        self.assertTrue(res)

    async def test_reactions_add_idempotent_already_reacted(self):
        web_mock = AsyncMock()
        err_resp = {"ok": False, "error": "already_reacted"}
        web_mock.reactions_add.side_effect = SlackApiError("already_reacted", err_resp)
        client = SlackClient(client=web_mock)
        res = await client.reactions_add(channel="C1", name="gear", timestamp="100.1")
        self.assertTrue(res)

    async def test_reactions_remove_success(self):
        web_mock = AsyncMock()
        web_mock.reactions_remove.return_value = {"ok": True}
        client = SlackClient(client=web_mock)
        res = await client.reactions_remove(channel="C1", name="gear", timestamp="100.1")
        self.assertTrue(res)

    async def test_reactions_remove_idempotent_no_reaction(self):
        web_mock = AsyncMock()
        err_resp = {"ok": False, "error": "no_reaction"}
        web_mock.reactions_remove.side_effect = SlackApiError("no_reaction", err_resp)
        client = SlackClient(client=web_mock)
        res = await client.reactions_remove(channel="C1", name="gear", timestamp="100.1")
        self.assertTrue(res)

    async def test_swap_reaction_removes_old_and_adds_new(self):
        web_mock = AsyncMock()
        client = SlackClient(client=web_mock)
        await client.swap_reaction(channel="C1", timestamp="100.1", old_name="gear", new_name="white_check_mark")
        web_mock.reactions_remove.assert_called_once_with(channel="C1", name="gear", timestamp="100.1")
        web_mock.reactions_add.assert_called_once_with(channel="C1", name="white_check_mark", timestamp="100.1")

    async def test_swap_reaction_same_name_noop_on_remove(self):
        web_mock = AsyncMock()
        client = SlackClient(client=web_mock)
        await client.swap_reaction(channel="C1", timestamp="100.1", old_name="gear", new_name="gear")
        web_mock.reactions_remove.assert_not_called()
        web_mock.reactions_add.assert_called_once_with(channel="C1", name="gear", timestamp="100.1")

    async def test_rate_limit_retry_after_success(self):
        web_mock = AsyncMock()
        err_resp = MagicMock()
        err_resp.status_code = 429
        err_resp.headers = {"Retry-After": "0.01"}
        err_resp.get.return_value = "ratelimited"

        web_mock.chat_postMessage.side_effect = [
            SlackApiError("ratelimited", err_resp),
            {"ok": True, "ts": "123.456"}
        ]
        client = SlackClient(client=web_mock, max_retries=2)
        resp = await client.chat_postMessage(channel="C1", text="Hello")
        self.assertEqual(resp.get("ts"), "123.456")
        self.assertEqual(web_mock.chat_postMessage.call_count, 2)

    async def test_rate_limit_retries_exhausted_raises_exception(self):
        web_mock = AsyncMock()
        err_resp = MagicMock()
        err_resp.status_code = 429
        err_resp.headers = {"Retry-After": "0.01"}
        err_resp.get.return_value = "ratelimited"

        web_mock.chat_postMessage.side_effect = SlackApiError("ratelimited", err_resp)
        client = SlackClient(client=web_mock, max_retries=2)

        with self.assertRaises(SlackRateLimitError) as ctx:
            await client.chat_postMessage(channel="C1", text="Hello")
        self.assertEqual(ctx.exception.retry_after, 0.01)

    async def test_chat_update_message_not_found_returns_none(self):
        web_mock = AsyncMock()
        err_resp = {"ok": False, "error": "message_not_found"}
        web_mock.chat_update.side_effect = SlackApiError("message_not_found", err_resp)
        client = SlackClient(client=web_mock)
        resp = await client.chat_update(channel="C1", ts="100.1", text="Update")
        self.assertIsNone(resp)

    async def test_chat_post_message_with_sanitization(self):
        web_mock = AsyncMock()
        web_mock.chat_postMessage.return_value = {"ok": True, "ts": "100.1"}
        client = SlackClient(client=web_mock)
        await client.chat_postMessage(channel="C1", text="### Header\n**bold**", sanitize=True)
        web_mock.chat_postMessage.assert_called_once_with(
            channel="C1",
            text="*Header*\n*bold*"
        )

    async def test_context_manager_lifecycle_close(self):
        web_mock = AsyncMock()
        mock_session = AsyncMock()
        mock_session.closed = False
        web_mock.session = mock_session

        async with SlackClient(client=web_mock) as client:
            self.assertIsNotNone(client)
        mock_session.close.assert_called_once()


class TestSlackTransport(unittest.IsolatedAsyncioTestCase):
    """Unit tests for high-level SlackTransport façade."""

    async def test_send_message_normal(self):
        web_mock = AsyncMock()
        web_mock.chat_postMessage.return_value = {"ok": True, "ts": "100.1"}
        client = SlackClient(client=web_mock)
        transport = SlackTransport(client=client)

        resps = await transport.send_message(channel="C1", text="Hello world")
        self.assertEqual(len(resps), 1)
        web_mock.chat_postMessage.assert_called_once_with(
            channel="C1",
            text="Hello world"
        )

    async def test_send_message_chunked_with_blocks_on_first(self):
        web_mock = AsyncMock()
        web_mock.chat_postMessage.return_value = {"ok": True, "ts": "100.1"}
        client = SlackClient(client=web_mock)
        transport = SlackTransport(client=client)

        long_msg = "Paragraph A " * 100 + "\n\n" + "Paragraph B " * 100
        sample_blocks = [{"type": "header", "text": {"type": "plain_text", "text": "Lead"}}]

        resps = await transport.send_message(
            channel="C1",
            text=long_msg,
            blocks=sample_blocks,
            max_chars=1000
        )
        self.assertGreaterEqual(len(resps), 2)
        # Blocks attached to first chunk only
        first_call = web_mock.chat_postMessage.call_args_list[0][1]
        second_call = web_mock.chat_postMessage.call_args_list[1][1]
        self.assertEqual(first_call["blocks"], sample_blocks)
        self.assertIsNone(second_call.get("blocks"))

    async def test_create_milestone_updater(self):
        web_mock = AsyncMock()
        client = SlackClient(client=web_mock)
        transport = SlackTransport(client=client)

        updater = transport.create_milestone_updater(channel="C1", thread_ts="100.1", debounce_interval=0.5)
        self.assertEqual(updater.channel, "C1")
        self.assertEqual(updater.thread_ts, "100.1")
        self.assertEqual(updater.min_interval, 0.5)

    async def test_reaction_helpers(self):
        web_mock = AsyncMock()
        web_mock.reactions_add.return_value = {"ok": True}
        web_mock.reactions_remove.return_value = {"ok": True}
        client = SlackClient(client=web_mock)
        transport = SlackTransport(client=client)

        self.assertTrue(await transport.add_reaction("C1", "100.1", "gear"))
        self.assertTrue(await transport.remove_reaction("C1", "100.1", "gear"))
        await transport.swap_reaction("C1", "100.1", "gear", "white_check_mark")
        self.assertEqual(web_mock.reactions_add.call_count, 2)
        self.assertEqual(web_mock.reactions_remove.call_count, 2)


class TestCompatibilityShim(unittest.TestCase):
    """Verify that root normalizer.py re-exports match golem.transport.normalizer."""

    def test_shim_exports_identical_symbols(self):
        from golem.transport import normalizer as canonical

        self.assertIs(normalizer.sanitize_for_slack, canonical.sanitize_for_slack)
        self.assertIs(normalizer.transpile_markdown_to_mrkdwn, canonical.transpile_markdown_to_mrkdwn)
        self.assertIs(normalizer.strip_terminal_chrome, canonical.strip_terminal_chrome)
        self.assertIs(normalizer.strip_ansi, canonical.strip_ansi)
        self.assertIs(normalizer.strip_tui_debris, canonical.strip_tui_debris)
        self.assertIs(normalizer.strip_box_drawing, canonical.strip_box_drawing)
        self.assertIs(normalizer.strip_prompt_echoes, canonical.strip_prompt_echoes)
        self.assertIs(normalizer.protect_code_blocks, canonical.protect_code_blocks)
        self.assertIs(normalizer.restore_code_blocks, canonical.restore_code_blocks)
        self.assertIs(normalizer.ANSI_ESCAPE_PATTERN, canonical.ANSI_ESCAPE_PATTERN)
        self.assertIs(normalizer.BOX_DRAWING_PATTERN, canonical.BOX_DRAWING_PATTERN)


if __name__ == "__main__":
    unittest.main()
