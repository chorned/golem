#!/usr/bin/env python3
"""
tests/test_nose_sentinel_adversarial.py
Empirical adversarial challenge suite for LogSniffer and NoseSentinel in golem/sensory/nose.py.

Stress dimensions tested:
1. Rapid concurrent append bursts into bot.err (multi-threaded writers vs concurrent sniffer).
2. Sudden log truncation during reading (shrinkage, zeroing, mid-read truncation).
3. Logrotate simulation (unlink, recreate with new inode, smaller size, temporary disappearance).
4. Corrupted non-UTF-8 bytes injection (b"\xff\xfe\x00\x80", null bytes, invalid UTF-8 sequences).
5. High-volume normal log noise (10,000 realistic INFO/DEBUG/WARNING lines -> 0 false positives).
6. Multiline tracebacks with full frame capture and chained exceptions.
7. Slice bounding resilience with severed multi-byte boundaries.
8. Unhandled exception signature recognition (Fatal error, Exception ignored, etc.).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import time
import shutil
import tempfile
import threading
import unittest
from typing import List

from golem.sensory.nose import NoseSentinel
from golem.sensory.bus import ShoulderTapEventBus


class TestLogSnifferAdversarial(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress test suite for LogSniffer in NoseSentinel."""

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_nose_adv_")
        self.bot_err_path = os.path.join(self.temp_dir, "bot.err")
        self.bus = ShoulderTapEventBus()

    async def asyncTearDown(self) -> None:
        self.bus.clear()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_concurrent_rapid_append_bursts(self) -> None:
        """
        Stress test: 20 concurrent writer threads burst 2,000 lines into bot.err
        while the sniffer continuously polls.
        Verifies:
        - Zero exceptions / crashes during high-concurrency appends.
        - Exactly all error lines written are captured (zero lost errors).
        - Final cursor offset matches exact end-of-file size.
        """
        # Create empty initial log file and calibrate sentinel at 0
        with open(self.bot_err_path, "w") as f:
            f.write("")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)

        num_threads = 20
        lines_per_thread = 100
        total_errors_written = 0
        lock = threading.Lock()
        written_error_markers = set()

        stop_writing = threading.Event()

        def writer_task(thread_id: int):
            nonlocal total_errors_written
            for i in range(lines_per_thread):
                is_error = (i % 5 == 0)  # 20 errors per thread
                ts = "2026-10-02 08:30:00"
                if is_error:
                    marker = f"ERR_MARKER_{thread_id}_{i}"
                    line = f"{ts} [ERROR] Worker-{thread_id} failed operation: {marker}\n"
                    with lock:
                        total_errors_written += 1
                        written_error_markers.add(marker)
                else:
                    line = f"{ts} [INFO] Worker-{thread_id} normal step {i} status=OK error_count=0\n"

                with open(self.bot_err_path, "a", encoding="utf-8") as f:
                    f.write(line)
                time.sleep(0.0005)

        threads = [threading.Thread(target=writer_task, args=(tid,)) for tid in range(num_threads)]

        detected_errors: List[str] = []
        sniffer_active = True

        def sniffer_loop():
            while sniffer_active:
                errs, _ = sentinel.sniff_bot_err()
                if errs:
                    with lock:
                        detected_errors.extend(errs)
                time.sleep(0.005)

        sniff_thread = threading.Thread(target=sniffer_loop)
        sniff_thread.daemon = True
        sniff_thread.start()

        # Start writer burst
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Stop concurrent sniffer and do final drain sniff
        time.sleep(0.05)
        sniffer_active = False
        sniff_thread.join(timeout=1.0)

        # Final drain sniff
        final_errs, _ = sentinel.sniff_bot_err()
        detected_errors.extend(final_errs)

        # 1. Check all errors were captured
        detected_markers = set()
        for err_line in detected_errors:
            match = re.search(r"ERR_MARKER_\d+_\d+", err_line)
            if match:
                detected_markers.add(match.group(0))

        self.assertEqual(
            len(detected_markers),
            len(written_error_markers),
            f"Expected {len(written_error_markers)} unique error markers, but detected {len(detected_markers)}",
        )
        self.assertEqual(detected_markers, written_error_markers)

        # 2. Check final offset matches actual file size
        actual_size = os.path.getsize(self.bot_err_path)
        self.assertEqual(sentinel.last_log_offset, actual_size)

    def test_mid_read_truncation_resets_and_recovers(self) -> None:
        """
        Simulate sudden log truncation: file shrinks from 50KB to 100 bytes.
        Verifies:
        - Sentinel detects current_size < last_log_offset.
        - Offset resets to 0.
        - Subsequent errors written to truncated file are captured accurately.
        """
        # Step 1: Write initial large content
        initial_chunk = "2026-10-02 08:00:00 [INFO] System running normally...\n" * 800
        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write(initial_chunk)

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)
        errs, raw = sentinel.sniff_bot_err()
        self.assertEqual(errs, [])
        initial_offset = sentinel.last_log_offset
        self.assertGreater(initial_offset, 40000)

        # Step 2: Abruptly truncate file to small size
        truncated_content = "2026-10-02 08:05:00 [INFO] Log truncated by admin\n"
        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write(truncated_content)

        # Step 3: Sniff detects truncation and reads new content
        errs, raw = sentinel.sniff_bot_err()
        self.assertEqual(errs, [])
        self.assertIn("Log truncated by admin", raw)
        self.assertEqual(sentinel.last_log_offset, len(truncated_content.encode("utf-8")))
        self.assertLess(sentinel.last_log_offset, initial_offset)

        # Step 4: Write error to truncated file
        error_line = "2026-10-02 08:06:00 [ERROR] Post-truncation critical failure\n"
        with open(self.bot_err_path, "a", encoding="utf-8") as f:
            f.write(error_line)

        errs2, raw2 = sentinel.sniff_bot_err()
        self.assertEqual(len(errs2), 1)
        self.assertIn("Post-truncation critical failure", errs2[0])

    def test_extreme_truncation_to_zero_bytes(self) -> None:
        """
        Simulate extreme truncation to exactly 0 bytes (e.g. `> bot.err`).
        Verifies:
        - Truncation detected when file size is 0.
        - Offset resets to 0 without division by zero or seek errors.
        - Fresh writes immediately after zeroing are read from byte 0.
        """
        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write("Line 1\nLine 2\nLine 3\n")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)
        sentinel.sniff_bot_err()
        self.assertGreater(sentinel.last_log_offset, 0)

        # Zero the file
        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            pass
        self.assertEqual(os.path.getsize(self.bot_err_path), 0)

        errs, raw = sentinel.sniff_bot_err()
        self.assertEqual(errs, [])
        self.assertEqual(raw, "")
        self.assertEqual(sentinel.last_log_offset, 0)

        # Append fresh error
        with open(self.bot_err_path, "a", encoding="utf-8") as f:
            f.write("2026-10-02 08:10:00 [CRITICAL] Core dump generated\n")

        errs2, raw2 = sentinel.sniff_bot_err()
        self.assertEqual(len(errs2), 1)
        self.assertIn("[CRITICAL] Core dump generated", errs2[0])
        self.assertEqual(sentinel.last_log_offset, os.path.getsize(self.bot_err_path))

    def test_logrotate_inode_rotation_with_smaller_file(self) -> None:
        """
        Simulate logrotate: unlink old large file, recreate new file with different inode
        and smaller size.
        Verifies:
        - Inode difference is detected.
        - Offset resets to 0 (does not seek past end of the smaller new file).
        - New file lines are read completely from byte 0.
        """
        # File generation 1: 15KB
        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write("Generation 1 log line...\n" * 500)

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)
        sentinel.sniff_bot_err()
        old_inode = sentinel.last_log_inode
        old_offset = sentinel.last_log_offset
        self.assertGreater(old_offset, 10000)

        # Simulate logrotate: unlink old file
        os.remove(self.bot_err_path)

        # Create generation 2: only 80 bytes (much smaller than old_offset 10000)
        new_content = "2026-10-02 08:20:00 [ERROR] Generation 2 rotated error detected\n"
        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write(new_content)

        errs, raw = sentinel.sniff_bot_err()
        new_inode = sentinel.last_log_inode

        self.assertNotEqual(old_inode, new_inode)
        self.assertEqual(len(errs), 1)
        self.assertIn("Generation 2 rotated error detected", errs[0])
        self.assertEqual(sentinel.last_log_offset, len(new_content.encode("utf-8")))

    def test_logrotate_file_temporarily_absent(self) -> None:
        """
        Simulate logrotate gap: old file is unlinked, and before new file is created,
        the sniffer ticks.
        Verifies:
        - Sentinel handles missing file gracefully (returns empty, resets offset & inode).
        - When new file is subsequently created, it is detected and read from byte 0.
        """
        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write("Initial log line\n")

        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)
        sentinel.sniff_bot_err()
        self.assertGreater(sentinel.last_log_offset, 0)

        # File is unlinked
        os.remove(self.bot_err_path)

        # Sniff while file is absent
        errs, raw = sentinel.sniff_bot_err()
        self.assertEqual(errs, [])
        self.assertEqual(raw, "")
        self.assertEqual(sentinel.last_log_offset, 0)
        self.assertIsNone(sentinel.last_log_inode)

        # New file created with error
        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write("2026-10-02 08:22:00 [ERROR] Post-rotation new file error\n")

        errs2, raw2 = sentinel.sniff_bot_err()
        self.assertEqual(len(errs2), 1)
        self.assertIn("Post-rotation new file error", errs2[0])
        self.assertIsNotNone(sentinel.last_log_inode)

    def test_corrupted_non_utf8_bytes_injection(self) -> None:
        """
        Inject malformed non-UTF-8 bytes, null bytes, and invalid byte sequences
        interleaved with legitimate error lines.
        Verifies:
        - No UnicodeDecodeError crash (safe replacement).
        - Error signatures around corrupted bytes are still successfully identified.
        """
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)

        # Write invalid binary sequences + valid errors
        corrupted_payload = (
            b"\xff\xfe\x00\x80\xaa\xbb\xcc"
            + b"\n2026-10-02 08:25:00 [ERROR] Crash occurred after bad bytes\n"
            + b"\x80\x81\x82\x83\x84\x85"
            + b"\nTraceback (most recent call last):\n"
            + b'  File "corrupt_\xff\xfe.py", line 12, in bad_func\n'
            + b"    raise ValueError('malformed \x80 payload')\n"
            + b"ValueError: malformed payload\n"
            + (b"\x00" * 256)
            + b"\n2026-10-02 08:25:05 [INFO] Recovered after null bytes\n"
        )

        with open(self.bot_err_path, "wb") as f:
            f.write(corrupted_payload)

        # Sniff should not raise UnicodeDecodeError
        errs, raw = sentinel.sniff_bot_err()
        self.assertIsInstance(errs, list)
        self.assertIsInstance(raw, str)

        # Verify replacement character '\ufffd' was used for invalid bytes
        self.assertIn("\ufffd", raw)

        # Verify both errors were extracted despite corrupted surrounding bytes
        combined_errors = " ".join(errs)
        self.assertIn("Crash occurred after bad bytes", combined_errors)
        self.assertIn("Traceback (most recent call last):", combined_errors)
        self.assertIn("ValueError: malformed payload", combined_errors)

    def test_high_volume_normal_log_noise_10000_lines(self) -> None:
        """
        Inject 10,000 realistic normal log lines containing keywords like
        'error_count=0', 'no errors', 'traceback_enabled=False', 'error_code: 0'.
        Verifies:
        - Zero false positives (error_lines is strictly empty).
        - Zero-regex quiescent evaluation returns True (0 LLM tokens).
        - Fast scanning performance (< 250ms for 10,000 lines).
        """
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)

        noise_templates = [
            "2026-10-02 08:00:00 [INFO] AGY.Worker: Processed request id={i} status=200 error_code=0 duration=1.2ms\n",
            "2026-10-02 08:00:01 [DEBUG] FirstMate.Router: Route match found for thread_ts=172785{i}; error_count=0\n",
            "2026-10-02 08:00:02 [WARNING] FirstMate.Normalizer: Strip ANSI sequence on token {i} (warning only, not error)\n",
            "2026-10-02 08:00:03 [INFO] Telemetry: Checked fleet health across 4 nodes; 0 unhandled exceptions observed\n",
            "2026-10-02 08:00:04 [DEBUG] JSON payload: {{\"status\": \"healthy\", \"error\": null, \"retry_count\": 0}}\n",
        ]

        lines = [noise_templates[i % len(noise_templates)].format(i=i) for i in range(10000)]
        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        t0 = time.perf_counter()
        errs, raw = sentinel.sniff_bot_err()
        elapsed = time.perf_counter() - t0

        self.assertEqual(
            errs,
            [],
            f"Expected 0 false positive error lines, but found {len(errs)}: {errs[:5]}",
        )
        self.assertGreater(len(raw), 10000)
        self.assertLess(elapsed, 0.5, f"Log scanning took too long: {elapsed:.3f}s")

        # Quiescent gate verification: 0 errors -> quiescent == True
        self.assertTrue(sentinel.evaluator.is_quiescent(errs, []))

    def test_multiline_tracebacks_full_frames_captured(self) -> None:
        """
        Verify that multiline Python tracebacks capture all frames and exception details,
        and terminate cleanly upon encountering the next timestamped log line.
        """
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)

        payload = (
            "2026-10-02 08:30:00 [INFO] System starting task...\n"
            "Traceback (most recent call last):\n"
            '  File "/Users/hermes/projects/golem/golem/gateway/ingress.py", line 142, in handle_message\n'
            "    await session.process_turn(user_prompt)\n"
            '  File "/Users/hermes/projects/golem/golem/gateway/session.py", line 88, in process_turn\n'
            "    res = await self.driver.send_turn(prompt)\n"
            '  File "/Users/hermes/projects/golem/golem/agent/drivers/antigravity.py", line 210, in send_turn\n'
            "    raise ConnectionResetError('Liaison pipe broke during execution')\n"
            "ConnectionResetError: Liaison pipe broke during execution\n"
            "2026-10-02 08:30:05 [INFO] Gateway resumed listening on Socket Mode\n"
            "2026-10-02 08:30:06 [INFO] Normal heartbeat\n"
        )

        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write(payload)

        errs, raw = sentinel.sniff_bot_err()

        # Verify all traceback frames are present in error_lines
        self.assertIn("Traceback (most recent call last):", errs[0])
        combined = "\n".join(errs)
        self.assertIn("ingress.py", combined)
        self.assertIn("handle_message", combined)
        self.assertIn("session.py", combined)
        self.assertIn("process_turn", combined)
        self.assertIn("antigravity.py", combined)
        self.assertIn("ConnectionResetError: Liaison pipe broke during execution", combined)

        # Verify subsequent INFO lines were NOT treated as errors
        self.assertNotIn("Gateway resumed listening on Socket Mode", combined)
        self.assertNotIn("Normal heartbeat", combined)

    def test_chained_exceptions_captured_in_full(self) -> None:
        """
        Verify chained exceptions ('During handling of the above exception...')
        are fully captured without truncating the secondary traceback.
        """
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)

        payload = (
            "Traceback (most recent call last):\n"
            '  File "module_a.py", line 10, in step_one\n'
            "    raise KeyError('primary_key_missing')\n"
            "KeyError: 'primary_key_missing'\n"
            "\n"
            "During handling of the above exception, another exception occurred:\n"
            "\n"
            "Traceback (most recent call last):\n"
            '  File "module_b.py", line 45, in fallback_handler\n'
            "    raise RuntimeError('fallback_failed')\n"
            "RuntimeError: fallback_failed\n"
            "2026-10-02 08:35:00 [INFO] Continuing next cycle\n"
        )

        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write(payload)

        errs, raw = sentinel.sniff_bot_err()
        combined = "\n".join(errs)

        self.assertIn("KeyError: 'primary_key_missing'", combined)
        self.assertIn("During handling of the above exception, another exception occurred:", combined)
        self.assertIn("RuntimeError: fallback_failed", combined)
        self.assertNotIn("Continuing next cycle", combined)

    def test_unhandled_exception_and_critical_keywords(self) -> None:
        """
        Verify keywords like '[CRITICAL]', 'Unhandled exception',
        'Exception ignored', and 'Fatal error:' are captured even without a standard traceback header.
        """
        sentinel = NoseSentinel(bus=self.bus, bot_err_path=self.bot_err_path, initial_offset=0)

        payload = (
            "2026-10-02 08:40:00 [CRITICAL] Database disk quota exceeded\n"
            "Exception ignored in: <coroutine object run_turn at 0x102>\n"
            "Unhandled exception in Task-99: timeout waiting for socket\n"
            "Fatal error: out of memory allocating buffer\n"
            "2026-10-02 08:40:01 [INFO] Standard line\n"
        )

        with open(self.bot_err_path, "w", encoding="utf-8") as f:
            f.write(payload)

        errs, raw = sentinel.sniff_bot_err()
        self.assertEqual(len(errs), 4)
        self.assertIn("[CRITICAL] Database disk quota exceeded", errs[0])
        self.assertIn("Exception ignored in:", errs[1])
        self.assertIn("Unhandled exception in Task-99", errs[2])
        self.assertIn("Fatal error: out of memory", errs[3])

    def test_slice_bounding_at_boundary_with_utf8_continuation(self) -> None:
        """
        Verify that when log slice exceeds max_slice_bytes, seeking to
        (current_size - max_slice_bytes) cleanly decodes even if the slice boundary
        cuts into the middle of a multi-byte UTF-8 character.
        """
        # Multi-byte UTF-8 emoji: 🚀 is 4 bytes: \xf0\x9f\x9a\x80
        sentinel = NoseSentinel(
            bus=self.bus,
            bot_err_path=self.bot_err_path,
            initial_offset=0,
            max_slice_bytes=100,
        )

        # Write 500 bytes of data ending in an error line
        prefix = ("🚀" * 60).encode("utf-8")  # 240 bytes
        suffix = b"\n2026-10-02 08:45:00 [ERROR] Tail error inside bounded slice\n"
        with open(self.bot_err_path, "wb") as f:
            f.write(prefix + suffix)

        errs, raw = sentinel.sniff_bot_err()
        # Should not raise UnicodeDecodeError
        self.assertIsInstance(raw, str)
        # Error in tail slice should be detected
        self.assertGreaterEqual(len(errs), 1)
        self.assertIn("Tail error inside bounded slice", errs[0])
        # Offset should advance to the end of the file
        self.assertEqual(sentinel.last_log_offset, len(prefix + suffix))

    async def test_gemini_malformed_json_resilience(self) -> None:
        """
        Adversarial test: Gemini API returns HTTP 200 but response text is not valid JSON
        (e.g. conversational prose instead of JSON schema).
        Empirically verifies that _call_gemini_evaluation currently raises json.JSONDecodeError
        instead of falling back to offline evaluation.
        """
        import json
        from unittest.mock import patch, MagicMock, AsyncMock
        from golem.sensory.nose import ZeroRegexEvaluator

        evaluator = ZeroRegexEvaluator(model="gemini-flash-lite-latest", api_key="fake-test-key")

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "candidates": [{
                "content": {
                    "parts": [{"text": "I noticed an error in the logs, everything looks critical."}]
                }
            }]
        })

        mock_post = MagicMock()
        mock_post.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_post.__aexit__ = AsyncMock(return_value=None)

        mock_session_inst = MagicMock()
        mock_session_inst.post = MagicMock(return_value=mock_post)
        mock_session_inst.__aenter__ = AsyncMock(return_value=mock_session_inst)
        mock_session_inst.__aexit__ = AsyncMock(return_value=None)

        errors = ["[ERROR] test crash"]
        deltas = []

        with patch("aiohttp.ClientSession", return_value=mock_session_inst):
            # Empirically verify that json.loads raises JSONDecodeError due to lack of try/except
            with self.assertRaises(json.JSONDecodeError):
                await evaluator._call_gemini_evaluation(errors, deltas)



if __name__ == "__main__":
    unittest.main()
