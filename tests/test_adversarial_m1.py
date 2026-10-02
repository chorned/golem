#!/usr/bin/env python3
"""
tests/test_adversarial_m1.py
Empirical adversarial stress test suite for First Mate 2.0 Milestone 1:
- first_mate.transport.chunker
- first_mate.transport.milestone
- first_mate.transport.slack_client

Author: Challenger 2 (Milestone 1)
"""

import asyncio
import time
import unittest
from unittest.mock import AsyncMock

from slack_sdk.errors import SlackApiError

from first_mate.transport.chunker import (
    split_message_boundary_aware,
    parse_code_blocks,
)
from first_mate.transport.milestone import MilestoneUpdater
from first_mate.transport.slack_client import SlackClient


class TestChunkerAdversarial(unittest.TestCase):
    """Adversarial stress tests for split_message_boundary_aware."""

    def test_extremely_large_unbroken_strings(self):
        """Verify chunker handles 5,000 to 20,000 characters of contiguous unbroken characters."""
        for size in [5000, 10000, 20000]:
            text = "X" * size
            chunks = split_message_boundary_aware(text, max_chars=3800)
            self.assertTrue(len(chunks) > 1)
            for idx, c in enumerate(chunks):
                self.assertLessEqual(len(c), 3800, f"Chunk {idx} exceeded 3800 chars (len={len(c)})")
            reconstructed = "".join(chunks)
            self.assertEqual(reconstructed, text, f"Reconstructed string length mismatch for size {size}")

    def test_code_blocks_10k_chars_various_languages(self):
        """Verify 10,000-character code blocks with python, bash, json, and empty language tags."""
        for lang in ["python", "bash", "json", ""]:
            code_lines = [f"stmt_{i}_{lang} = do_work({i})" for i in range(450)]
            code_body = "\n".join(code_lines)
            fence = "```"
            text = f"{fence}{lang}\n{code_body}\n{fence}"
            self.assertGreaterEqual(len(text), 10000)

            chunks = split_message_boundary_aware(text, max_chars=3800)
            self.assertGreater(len(chunks), 1)

            for idx, c in enumerate(chunks):
                self.assertLessEqual(len(c), 3800, f"Chunk {idx} len {len(c)} > 3800 for lang '{lang}'")
                blocks = parse_code_blocks(c)
                self.assertEqual(len(blocks), 1, f"Chunk {idx} must contain exactly 1 code block")
                self.assertTrue(blocks[0].is_closed, f"Chunk {idx} code block must be closed")
                if idx > 0:
                    expected_prefix = f"```{lang}"
                    self.assertTrue(
                        c.startswith(expected_prefix),
                        f"Continuation chunk {idx} must reopen with '{expected_prefix}', got '{c[:20]}'"
                    )

    def test_sequential_code_blocks_spanning_boundary(self):
        """Verify multiple sequential code blocks sliced across boundary 3,800."""
        b1 = "```python\n" + ("x = 1\n" * 300) + "```\n"   # ~1800 chars
        b2 = "```bash\n" + ("echo 2\n" * 300) + "```\n"     # ~2100 chars
        b3 = "```json\n" + ("{\"k\": 3}\n" * 200) + "```\n" # ~2000 chars
        full_text = f"{b1}\n\n{b2}\n\n{b3}"

        chunks = split_message_boundary_aware(full_text, max_chars=3800)
        self.assertGreater(len(chunks), 1)
        for idx, c in enumerate(chunks):
            self.assertLessEqual(len(c), 3800)
            blocks = parse_code_blocks(c)
            for b in blocks:
                self.assertTrue(b.is_closed, f"Sequential block in chunk {idx} left unclosed")

    def test_nested_code_blocks_sliced_across_boundary(self):
        """Verify 4-backtick outer block containing inner 3-backtick block sliced across boundary."""
        inner = "\n".join([f"inner_code_{i} = {i}" for i in range(200)])
        text = f"````markdown\nouter start\n```python\n{inner}\n```\nouter end\n````"
        chunks = split_message_boundary_aware(text, max_chars=3800)
        self.assertGreater(len(chunks), 1)
        for idx, c in enumerate(chunks):
            self.assertLessEqual(len(c), 3800)

    def test_boundary_opening_fence_split_vulnerability(self):
        """
        Verify that if code block opening fence line (```python\n) falls near boundary 3,800,
        chunker does not leave an unclosed fence in chunk 0 or orphaned code in chunk 1.
        """
        text = ("x" * 3780) + "\n```python\n" + ("y" * 1000) + "\n```"
        chunks = split_message_boundary_aware(text, max_chars=3800)
        self.assertEqual(len(chunks), 2)

        # Check if Chunk 0 has an unclosed code block
        blocks_0 = parse_code_blocks(chunks[0])
        unclosed_in_0 = any(not b.is_closed for b in blocks_0) or chunks[0].rstrip().endswith("```python")
        self.assertFalse(
            unclosed_in_0,
            "Chunk 0 must not leave unclosed ```python fence"
        )
        for idx, c in enumerate(chunks):
            self.assertLessEqual(len(c), 3800, f"Chunk {idx} len {len(c)} > 3800")
            blocks = parse_code_blocks(c)
            for b in blocks:
                self.assertTrue(b.is_closed, f"Chunk {idx} code block must be closed")

    def test_long_fence_pad_overflow_vulnerability(self):
        """
        Verify that dynamic fence padding ensures chunks are strictly <= 3,800
        even when code fence length >= 15.
        """
        fence = "`" * 15
        text = f"{fence}python\n" + ("x" * 5000) + f"\n{fence}"
        chunks = split_message_boundary_aware(text, max_chars=3800)
        exceeded = any(len(c) > 3800 for c in chunks)
        self.assertFalse(
            exceeded,
            "Dynamic fence padding must prevent chunk from exceeding 3800 chars"
        )
        for idx, c in enumerate(chunks):
            self.assertLessEqual(len(c), 3800, f"Chunk {idx} length {len(c)} > 3800")


class TestMilestoneUpdaterAdversarial(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress tests for MilestoneUpdater."""

    async def test_high_concurrency_burst_simulation(self):
        """Simulate 100 rapid concurrent updates across 10 threads with latency."""
        mock_client = AsyncMock()
        ts_counter = 0

        async def mock_post(channel, thread_ts, text, **kwargs):
            nonlocal ts_counter
            ts_counter += 1
            await asyncio.sleep(0.002)
            return {"ok": True, "ts": f"1700000000.{ts_counter:06d}"}

        async def mock_update(channel, ts, text, **kwargs):
            await asyncio.sleep(0.002)
            return {"ok": True, "ts": ts}

        mock_client.chat_postMessage = AsyncMock(side_effect=mock_post)
        mock_client.chat_update = AsyncMock(side_effect=mock_update)

        updater = MilestoneUpdater(client=mock_client, min_interval_seconds=0.1)

        async def worker(channel, thread_ts):
            for i in range(10):
                await updater.post_or_update(channel, thread_ts, f"update_{thread_ts}_{i}")
                await asyncio.sleep(0.005)

        # 10 threads concurrent
        tasks = [worker(f"C_{t%3}", f"thread_{t}") for t in range(10)]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.2)

        # Invariant: exactly 10 initial posts (1 per thread)
        self.assertEqual(mock_client.chat_postMessage.call_count, 10)
        # Updates must be throttled (less than 90 updates)
        self.assertLess(mock_client.chat_update.call_count, 90)

    async def test_token_bucket_quadratic_accumulation_defect(self):
        """
        Verify that _replenish_tokens advances the baseline without quadratic accumulation,
        strictly obeying the 1.0s interval under burst conditions.
        """
        mock_client = AsyncMock()
        mock_client.chat_postMessage.return_value = {"ok": True, "ts": "100.1"}
        mock_client.chat_update.return_value = {"ok": True, "ts": "100.1"}

        updater = MilestoneUpdater(client=mock_client, min_interval_seconds=1.0)
        await updater.post_or_update("C1", "T1", "init")

        t0 = time.time()
        # Fire 20 updates in ~200ms
        for i in range(20):
            await asyncio.sleep(0.01)
            await updater.post_or_update("C1", "T1", f"step_{i}")

        elapsed = time.time() - t0
        # Under a 1.0s rate limit, 0 updates should fire in ~200ms
        update_calls = mock_client.chat_update.call_count
        self.assertEqual(
            update_calls,
            0,
            f"Rate limit violated: chat_update fired {update_calls} times in {elapsed:.3f}s under 1.0s limit"
        )

    async def test_message_not_found_recovery_mock_client(self):
        """Verify message_not_found recovery when client raises Exception directly."""
        mock_client = AsyncMock()
        mock_client.chat_postMessage.side_effect = [
            {"ok": True, "ts": "100.1"},
            {"ok": True, "ts": "100.2"}
        ]
        mock_client.chat_update.side_effect = Exception("message_not_found")

        updater = MilestoneUpdater(client=mock_client, min_interval_seconds=0.0)
        ts1 = await updater.post_or_update("C1", "T1", "Initial")
        self.assertEqual(ts1, "100.1")

        ts2 = await updater.post_or_update("C1", "T1", "Update after delete")
        self.assertEqual(ts2, "100.2")
        self.assertEqual(mock_client.chat_postMessage.call_count, 2)

    async def test_message_not_found_swallowed_by_slack_client_defect(self):
        """
        Verify that when SlackClient.chat_update returns None on message_not_found,
        MilestoneUpdater catches the None response and falls back to chat_postMessage,
        ensuring the final response in finalize() is never dropped.
        """
        mock_web = AsyncMock()
        mock_web.chat_postMessage.return_value = {"ok": True, "ts": "100.1"}

        resp_mock = {"ok": False, "error": "message_not_found"}
        mock_web.chat_update.side_effect = SlackApiError("Failed", response=resp_mock)

        slack_client = SlackClient(client=mock_web)
        updater = MilestoneUpdater(client=slack_client, min_interval_seconds=0.0)

        # Step 1: Initial post succeeds
        await updater.post_or_update("C1", "T1", "Initial")
        self.assertEqual(mock_web.chat_postMessage.call_count, 1)

        # Step 2: finalize when message is deleted
        await updater.finalize("C1", "T1", "Final turn answer")

        # Invariant: chat_postMessage was called to deliver the final response
        self.assertEqual(
            mock_web.chat_postMessage.call_count,
            2,
            "finalize must fall back to chat_postMessage when chat_update returns None on message_not_found"
        )

    async def test_per_card_lock_concurrency_isolation(self):
        """Verify that concurrent threads do not block each other across network I/O."""
        client = AsyncMock()
        async def slow_post(*a, **k):
            await asyncio.sleep(0.05)
            return {"ok": True, "ts": "1.0"}
        client.chat_postMessage = slow_post
        updater = MilestoneUpdater(client=client)
        t0 = time.time()
        await asyncio.gather(
            updater.post_or_update("C1", "tA", "Msg A"),
            updater.post_or_update("C1", "tB", "Msg B")
        )
        elapsed = time.time() - t0
        self.assertLess(elapsed, 0.09, f"Threads were serialized! Elapsed: {elapsed:.3f}s")


if __name__ == "__main__":
    unittest.main()
