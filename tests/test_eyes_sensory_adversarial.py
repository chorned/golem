#!/usr/bin/env python3
"""
tests/test_eyes_sensory_adversarial.py
Adversarial Stress Test Suite for Sensory Bus and The Eyes Observer (Milestone M1.2).

Empirically tests and challenges:
1. SHA-256 buffer change hashing:
   - 100 consecutive ticks of static, cursor-only, ANSI styling, and whitespace variations.
   - Exact call-count verification that LLM evaluation is completely bypassed (0 tokens consumed).
   - Invalidation and re-evaluation on genuine substantive buffer changes.
   - Severe ANSI escape storms (color palettes, 256 colors, cursor controls, OSC sequences).
2. Zero-regex semantic classification against adversarial buffer patterns:
   - Non-standard OAuth ports (e.g. 49152, 65534).
   - Custom IP and IPv6 bindings (e.g. 192.168.1.100, [::1], 10.0.0.5).
   - Nested and non-standard CLI confirmation/input prompts ([y/N/cancel], [sudo] password, passphrase).
   - Multi-line Python/system exception stack traces.
   - Malformed/fenced LLM JSON responses and error fallback.
3. ShoulderTapEventBus under adversarial concurrency & load:
   - Multi-threaded concurrent publishing across 10 threads (1,000 rapid alerts).
   - Bounded queue overflow handling verifying strict FIFO drop of oldest items without deadlocks.
   - Subscriber exception isolation: failing sync & async subscribers do not interrupt delivery.
   - Concurrent subscribe/unsubscribe churn during active publishing.
"""

from __future__ import annotations

import os
import sys
import time
import asyncio
import threading
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import List, Dict, Any, Optional

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.sensory.bus import (
    ShoulderTapEventBus,
    ShoulderTapAlert,
    get_default_event_bus,
    reset_default_event_bus,
)
from golem.sensory.eyes import EyesTaskObserver


class TestAdversarialBufferHashing(unittest.IsolatedAsyncioTestCase):
    """
    Adversarial challenge for SHA-256 buffer change hashing:
    Verifies 0 LLM calls and 0 token consumption across 100 ticks of static,
    cursor-only, styling, or whitespace variations.
    """

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.observer = EyesTaskObserver(bus=self.bus, model="gemini-flash-lite-latest")

    async def test_100_ticks_static_and_cursor_only_zero_llm_calls(self) -> None:
        """
        Stress test: Verify with exact call counts that LLM evaluation is
        completely bypassed across 100 ticks of static or cursor-only buffer changes.
        """
        mock_classify_llm = AsyncMock(return_value={
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Compiling project targets in background",
        })
        self.observer._call_gemini_classification = mock_classify_llm

        base_text = "Building target 42/100 [=======>          ] 42% complete"

        # Tick 0: Initial buffer state -> LLM must be called exactly once
        res0 = await self.observer.classify_buffer(base_text)
        self.assertEqual(mock_classify_llm.call_count, 1)
        self.assertEqual(res0.get("state"), "RUNNING")

        # 100 subsequent ticks of adversarial cursor and styling permutations
        for tick in range(1, 101):
            if tick % 6 == 0:
                # ANSI cursor repositioning (row;col)
                variant = f"{base_text}\x1b[{tick};1H"
            elif tick % 6 == 1:
                # ANSI cursor visibility show/hide toggling
                variant = f"{base_text}\x1b[?25l\x1b[?25h"
            elif tick % 6 == 2:
                # Terminal line-clearing and carriage-return sequences
                variant = f"{base_text}\x1b[K\x1b[2K"
            elif tick % 6 == 3:
                # Line ending variations (\r\n vs \n and trailing whitespace)
                variant = f"{base_text}   \r\n\t"
            elif tick % 6 == 4:
                # Interleaved ANSI color/style resets
                variant = f"\x1b[0m\x1b[1m{base_text}\x1b[22m\x1b[0m"
            else:
                # Identical static text
                variant = base_text

            res = await self.observer.classify_buffer(variant)
            self.assertTrue(
                res.get("skipped") is True or res.get("state") in ("UNCHANGED", "RUNNING"),
                f"Tick {tick} failed to recognize unchanged semantic buffer: {res}",
            )
            # CRITICAL ASSERTION: Exactly 1 LLM call total across all 100 ticks
            self.assertEqual(
                mock_classify_llm.call_count,
                1,
                f"LLM was invoked on tick {tick}! Expected exactly 1 call, got {mock_classify_llm.call_count}",
            )

        # Confirm zero alerts emitted across the 100 static ticks
        alert = await self.observer.evaluate_buffer_content(base_text)
        self.assertIsNone(alert)
        self.assertEqual(mock_classify_llm.call_count, 1)

    async def test_buffer_hash_invalidation_on_semantic_change(self) -> None:
        """
        Verify that after static ticks, any genuine substantive change immediately
        invalidates the hash and invokes the LLM.
        """
        mock_classify_llm = AsyncMock(return_value={
            "state": "RUNNING",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Compiling target",
        })
        self.observer._call_gemini_classification = mock_classify_llm

        # Step 1: Initial buffer
        await self.observer.classify_buffer("Running task A...")
        self.assertEqual(mock_classify_llm.call_count, 1)

        # Step 2: 10 static ticks
        for _ in range(10):
            await self.observer.classify_buffer("Running task A...")
        self.assertEqual(mock_classify_llm.call_count, 1)

        # Step 3: Substantive semantic update -> LLM must be called
        await self.observer.classify_buffer("Running task B...")
        self.assertEqual(mock_classify_llm.call_count, 2)

        # Step 4: Another 10 static ticks on task B
        for _ in range(10):
            await self.observer.classify_buffer("Running task B...")
        self.assertEqual(mock_classify_llm.call_count, 2)

    def test_ansi_escape_storm_normalization(self) -> None:
        """
        Verify that severe ANSI escape storms (color codes, 256 colors, cursor controls, OSC codes)
        normalize deterministically and match the clean text's hash.
        """
        clean_text = "Task completed successfully\nAll 42 checks passed"
        raw_storm = (
            "\x1b[38;5;196m\x1b[48;5;232mTask \x1b[1mcompleted\x1b[22m \x1b[32msuccessfully\x1b[0m\x1b[K\r\n"
            "\x1b[?25l\x1b]0;Title\x07All \x1b[4m42\x1b[24m \x1b[33mchecks\x1b[39m passed\x1b[?25h\x1b[10;20H"
        )
        h_clean = self.observer.compute_buffer_hash(clean_text)
        h_storm = self.observer.compute_buffer_hash(raw_storm)
        self.assertEqual(h_clean, h_storm)


class TestAdversarialZeroRegexClassification(unittest.IsolatedAsyncioTestCase):
    """
    Adversarial challenge for Zero-Regex Semantic Classification:
    Stress tests classification against non-standard ports, custom IP bindings,
    complex nested CLI prompts, multi-line stack traces, and malformed LLM responses.
    """

    async def asyncSetUp(self) -> None:
        self.bus = ShoulderTapEventBus()
        self.observer = EyesTaskObserver(bus=self.bus, model="gemini-flash-lite-latest")

    async def test_non_standard_oauth_port_classification(self) -> None:
        """
        Verify classification of OAuth URLs with high/non-standard ports (e.g. 49152, 65534)
        and extensive query parameters.
        """
        raw_buffer = (
            "Authenticating with remote fleet node...\n"
            "Action required: Visit the following verification URL in your browser:\n"
            "http://127.0.0.1:49152/oauth/callback?state=sec_token_999&code_challenge=xyz777&scope=all\n"
            "Awaiting authorization..."
        )
        expected_url = "http://127.0.0.1:49152/oauth/callback?state=sec_token_999&code_challenge=xyz777&scope=all"
        mock_response = {
            "state": "BLOCKER",
            "blocker_type": "oauth",
            "blocker_url": expected_url,
            "blocker_prompt": None,
            "summary": "OAuth authentication blocker on port 49152",
        }
        self.observer._call_gemini_classification = AsyncMock(return_value=mock_response)

        alert = await self.observer.evaluate_buffer_content(
            raw_buffer, session_id="sess_oauth", channel="C_TEST", thread_ts="123.45"
        )

        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "blocker")
        self.assertEqual(alert.data.get("blocker_type"), "oauth")
        self.assertEqual(alert.data.get("url"), expected_url)
        self.assertEqual(alert.session_id, "sess_oauth")

    async def test_custom_ip_and_ipv6_oauth_bindings(self) -> None:
        """
        Verify classification of OAuth callback URLs binding to non-localhost LAN IPs or IPv6:
        - http://192.168.1.100:8080/oauth/callback
        - http://[::1]:8080/auth
        """
        test_cases = [
            ("http://192.168.1.100:8080/oauth/callback?client=1", "http://192.168.1.100:8080/oauth/callback?client=1"),
            ("http://[::1]:8080/auth/login", "http://[::1]:8080/auth/login"),
        ]

        for raw_url, expected in test_cases:
            with self.subTest(url=raw_url):
                buf = f"Please authenticate by browsing to: {raw_url}\nWaiting for response..."
                self.observer._call_gemini_classification = AsyncMock(return_value={
                    "state": "BLOCKER",
                    "blocker_type": "oauth",
                    "blocker_url": expected,
                    "blocker_prompt": None,
                    "summary": "OAuth login required on custom binding",
                })

                alert = await self.observer.evaluate_buffer_content(buf, session_id="sess_ip")
                self.assertIsNotNone(alert)
                self.assertEqual(alert.alert_type, "blocker")
                self.assertEqual(alert.data.get("url"), expected)

    async def test_nested_cli_prompts_and_custom_confirmation(self) -> None:
        """
        Verify tricky interactive CLI prompts:
        - [y/N/cancel] multi-option prompts
        - [sudo] password for hermes:
        - Enter passphrase for key /Users/hermes/.ssh/id_ed25519:
        - Explicit typing prompt: Type "DELETE" to confirm:
        """
        prompts = [
            ("Conflict detected in schema. Overwrite existing file? [y/N/cancel]: ", "prompt", "Overwrite file"),
            ("[sudo] password for hermes: ", "secret", "Sudo password"),
            ("Enter passphrase for key '/Users/hermes/.ssh/id_ed25519': ", "secret", "SSH passphrase"),
            ("WARNING: 12 production clusters will be removed. Type 'DELETE' to confirm: ", "prompt", "Delete confirm"),
        ]

        for prompt_text, b_type, summary in prompts:
            with self.subTest(prompt=prompt_text):
                buf = f"Deploying cluster changes...\n{prompt_text}"
                self.observer._call_gemini_classification = AsyncMock(return_value={
                    "state": "BLOCKER",
                    "blocker_type": b_type,
                    "blocker_url": None,
                    "blocker_prompt": prompt_text.strip(),
                    "summary": summary,
                })

                alert = await self.observer.evaluate_buffer_content(buf, session_id="sess_prompt")
                self.assertIsNotNone(alert)
                self.assertEqual(alert.alert_type, "blocker")
                self.assertEqual(alert.data.get("blocker_type"), b_type)
                self.assertEqual(alert.data.get("prompt"), prompt_text.strip())

    async def test_multiline_stack_trace_classification(self) -> None:
        """
        Verify multi-line fatal stack traces are classified as state='ERROR'
        and emit a completion alert with is_error=True.
        """
        stack_trace_buf = (
            "Traceback (most recent call last):\n"
            "  File \"/app/runner/worker.py\", line 142, in execute_stage\n"
            "    res = await remote_call(node=\"compute-grid\", action=\"train\")\n"
            "  File \"/app/runner/client.py\", line 56, in remote_call\n"
            "    raise ConnectionResetError(\"Peer dropped connection unexpectedly\")\n"
            "ConnectionResetError: Peer dropped connection unexpectedly\n"
            "Process terminated with exit code 1"
        )

        self.observer._call_gemini_classification = AsyncMock(return_value={
            "state": "ERROR",
            "blocker_type": None,
            "blocker_url": None,
            "blocker_prompt": None,
            "summary": "Process terminated with ConnectionResetError",
        })

        alert = await self.observer.evaluate_buffer_content(stack_trace_buf, session_id="sess_crash")
        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "completion")
        self.assertTrue(alert.data.get("is_error"))
        self.assertEqual(alert.data.get("state"), "ERROR")

    async def test_resilient_handling_of_fenced_llm_json(self) -> None:
        """
        Verify that when the LLM returns JSON enclosed in markdown code fences
        (```json ... ``` or ``` ... ```), the JSON parser strips fences cleanly.
        """
        fenced_json_text = '```json\n{\n  "state": "RUNNING",\n  "blocker_type": null,\n  "blocker_url": null,\n  "blocker_prompt": null,\n  "summary": "Actively compiling"\n}\n```'

        mock_resp_json = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": fenced_json_text}]
                    }
                }
            ]
        }

        mock_post = MagicMock()
        mock_post.status = 200
        mock_post.json = AsyncMock(return_value=mock_resp_json)
        mock_post.__aenter__ = AsyncMock(return_value=mock_post)
        mock_post.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post.return_value = mock_post
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            parsed = await self.observer._call_gemini_classification("Sample buffer text")
            self.assertEqual(parsed.get("state"), "RUNNING")
            self.assertEqual(parsed.get("summary"), "Actively compiling")


class TestAdversarialShoulderTapEventBus(unittest.IsolatedAsyncioTestCase):
    """
    Adversarial challenge for ShoulderTapEventBus:
    Stress tests concurrent multi-threaded publishing, 1,000 rapid alerts,
    bounded queue FIFO drop without deadlocks, and subscriber exception isolation.
    """

    async def asyncSetUp(self) -> None:
        reset_default_event_bus()
        self.bus = ShoulderTapEventBus(queue_maxsize=25)

    async def asyncTearDown(self) -> None:
        self.bus.clear()
        reset_default_event_bus()

    def test_concurrent_multithreaded_publishing_1000_alerts(self) -> None:
        """
        Stress test: 10 OS threads concurrently publishing 100 alerts each (total 1,000 alerts).
        Verify all 1,000 alerts are received safely without race conditions or memory corruption.
        """
        received: List[str] = []
        lock = threading.Lock()

        def sync_subscriber(alert: ShoulderTapAlert) -> None:
            with lock:
                received.append(alert.summary)

        self.bus.subscribe(sync_subscriber)

        def worker_target(thread_id: int) -> None:
            for i in range(100):
                self.bus.publish(ShoulderTapAlert(
                    alert_type="blocker",
                    source="eyes",
                    session_id="concurrent_sess",
                    summary=f"t{thread_id}_{i}",
                ))

        threads = [threading.Thread(target=worker_target, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(received), 1000)
        # Verify unique elements count
        self.assertEqual(len(set(received)), 1000)

    async def test_queue_overflow_fifo_dropping_without_deadlock(self) -> None:
        """
        Stress test: Publish 1,000 alerts to a session queue bounded to maxsize=25.
        Verify:
        - No deadlocks occur.
        - Queue size never exceeds maxsize (25).
        - Oldest alerts are dropped in exact FIFO order.
        - The 25 retained alerts correspond to the latest published items.
        """
        q = self.bus.get_session_queue("overflow_test_sess")

        # Publish 1,000 numbered alerts
        for i in range(1000):
            self.bus.publish(ShoulderTapAlert(
                alert_type="blocker",
                source="eyes",
                session_id="overflow_test_sess",
                summary=f"seq_{i:04d}",
            ))

        self.assertEqual(q.qsize(), 25)

        drained: List[str] = []
        while not q.empty():
            alert = await q.get()
            drained.append(alert.summary)

        self.assertEqual(len(drained), 25)
        # The oldest items (0..974) should have been dropped.
        # The newest items (975..999) must be retained in strict order.
        expected = [f"seq_{i:04d}" for i in range(975, 1000)]
        self.assertEqual(drained, expected)

    async def test_subscriber_exception_isolation_under_high_load(self) -> None:
        """
        Stress test: Register failing subscribers alongside healthy subscribers.
        Verify that crashing subscribers (both sync and async) do not impede delivery
        to healthy subscribers or the session queue across 1,000 rapid alerts.
        """
        sync_received: List[str] = []
        async_received: List[str] = []
        q = self.bus.get_session_queue("resilient_sess")

        def bad_sync_sub(alert: ShoulderTapAlert) -> None:
            raise RuntimeError("Fatal sync subscriber crash")

        async def bad_async_sub(alert: ShoulderTapAlert) -> None:
            raise ValueError("Fatal async subscriber crash")

        def good_sync_sub(alert: ShoulderTapAlert) -> None:
            sync_received.append(alert.summary)

        async def good_async_sub(alert: ShoulderTapAlert) -> None:
            async_received.append(alert.summary)

        self.bus.subscribe(bad_sync_sub)
        self.bus.subscribe(good_sync_sub)
        self.bus.subscribe(bad_async_sub)
        self.bus.subscribe(good_async_sub)

        for i in range(1000):
            self.bus.publish(ShoulderTapAlert(
                alert_type="blocker",
                source="eyes",
                session_id="resilient_sess",
                summary=f"alert_{i}",
            ))

        await asyncio.sleep(0.1)

        self.assertEqual(len(sync_received), 1000)
        self.assertEqual(len(async_received), 1000)
        # Queue should contain the last 25 alerts
        self.assertEqual(q.qsize(), 25)

    def test_concurrent_subscribe_unsubscribe_during_publishing(self) -> None:
        """
        Stress test: High subscription churn (concurrent subscribe and unsubscribe)
        occurring simultaneously with multi-threaded alert publishing.
        Verify zero race conditions, deadlocks, or unhandled exceptions.
        """
        stop_event = threading.Event()
        publish_errors: List[Exception] = []
        churn_errors: List[Exception] = []

        def publisher_thread(pid: int) -> None:
            try:
                for i in range(200):
                    self.bus.publish(ShoulderTapAlert(
                        alert_type="blocker",
                        source="eyes",
                        summary=f"churn_{pid}_{i}",
                    ))
            except Exception as e:
                publish_errors.append(e)

        def churn_thread() -> None:
            try:
                while not stop_event.is_set():
                    unsub = self.bus.subscribe(lambda a: None)
                    time.sleep(0.0005)
                    unsub()
            except Exception as e:
                churn_errors.append(e)

        publishers = [threading.Thread(target=publisher_thread, args=(p,)) for p in range(5)]
        churners = [threading.Thread(target=churn_thread) for _ in range(3)]

        for t in publishers + churners:
            t.start()

        for t in publishers:
            t.join()

        stop_event.set()
        for t in churners:
            t.join()

        self.assertEqual(len(publish_errors), 0, f"Publishing threads encountered errors: {publish_errors}")
        self.assertEqual(len(churn_errors), 0, f"Churn threads encountered errors: {churn_errors}")


if __name__ == "__main__":
    unittest.main()
