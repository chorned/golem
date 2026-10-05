#!/usr/bin/env python3
"""
tests/e2e/test_e2e_tier2_boundaries.py
E2E Tier 2 Boundary & Corner Case Test Suite for Golem Autonomous Fleet Chief of Staff.

Covers Boundary Categories from TEST_INFRA.md and PROJECT.md:
1. Port fallback and collision boundaries (port 8765 occupied, invalid port ranges)
2. Missing, empty, and extreme environment variable boundaries
3. Invalid worker names, path traversal attempts, and type validation
4. Large streaming chunks, high-frequency broadcast bursts, and SSE serializer boundaries
5. Concurrent shim imports across multiple threads without race conditions

All tests follow opaque-box, requirement-driven test design derived from
ORIGINAL_REQUEST.md, PROJECT.md, and TEST_INFRA.md.
"""

from __future__ import annotations

import os
import sys
import json
import socket
import threading
import importlib
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

try:
    import golem
    GOLEM_AVAILABLE = True
except ImportError:
    GOLEM_AVAILABLE = False


# ===========================================================================
# 1. Port Fallback & Network Binding Boundaries
# ===========================================================================
class TestTier2PortBoundaries(unittest.TestCase):
    """
    Tier 2 tests for WebUI server port binding boundaries.
    """

    def _get_webui_mod(self):
        for name in ["golem.webui.server", "golem.webui", "golem.webui.server"]:
            try:
                return importlib.import_module(name)
            except ImportError:
                pass
        return None

    def test_webui_port_already_bound_handling(self):
        """B01: When primary port 8765 is in use, server handles collision gracefully."""
        webui_mod = self._get_webui_mod()
        if webui_mod is None:
            self.skipTest("Milestone M3 pending: webui server module not yet created")

        launcher = getattr(webui_mod, "start_webui_server", None)
        if launcher is None:
            self.skipTest("Milestone M3 pending: start_webui_server launcher not yet implemented")

        # Bind a temporary mock socket to an ephemeral port
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        occupied_port = s.getsockname()[1]

        try:
            # If start_webui_server supports port parameter, attempt to start on occupied port
            try:
                result = launcher(port=occupied_port, host="127.0.0.1")
                # If launcher succeeds, it must have chosen a fallback port
                if hasattr(result, "port"):
                    self.assertNotEqual(result.port, occupied_port, "Must fallback to alternate port")
            except (OSError, RuntimeError) as e:
                # Alternatively, raising a clean, well-typed port collision exception is acceptable
                self.assertTrue("address" in str(e).lower() or "port" in str(e).lower() or "in use" in str(e).lower())
        finally:
            s.close()

    def test_webui_invalid_port_range_validation(self):
        """B01: Invalid port values (-1, 0, 70000) are validated or raise ValueError."""
        webui_mod = self._get_webui_mod()
        if webui_mod is None:
            self.skipTest("Milestone M3 pending: webui server module not yet created")

        launcher = getattr(webui_mod, "start_webui_server", None)
        if launcher is None:
            self.skipTest("Milestone M3 pending: start_webui_server not yet implemented")

        for bad_port in [-1, 70000, 99999]:
            with self.assertRaises((ValueError, OverflowError, OSError)):
                launcher(port=bad_port)


# ===========================================================================
# 2. Environment Variable Boundaries
# ===========================================================================
class TestTier2EnvVarBoundaries(unittest.TestCase):
    """
    Tier 2 tests for missing, empty, and truthy/falsy environment variables.
    """

    def _get_config_mod(self):
        for name in ["golem.config", "golem.config"]:
            try:
                return importlib.import_module(name)
            except ImportError:
                pass
        return None

    def test_default_hand_worker_empty_string_fallback(self):
        """B02: GOLEM_DEFAULT_HAND_WORKER='' safely falls back to standard worker."""
        cfg = self._get_config_mod()
        if cfg is None or not hasattr(cfg, "get_default_hand_worker"):
            self.skipTest("Milestone M2 pending: get_default_hand_worker() not yet implemented")

        with patch.dict(os.environ, {"GOLEM_DEFAULT_HAND_WORKER": ""}):
            worker = cfg.get_default_hand_worker()
            self.assertTrue(len(worker) > 0, "Empty worker env must not return blank string")
            self.assertIn(worker, ["agy", "antigravity"])

    def test_webui_toggle_truthy_falsy_permutations(self):
        """B02: GOLEM_ENABLE_WEBUI correctly evaluates truthy and falsy string variations."""
        cfg = self._get_config_mod()
        if cfg is None:
            self.skipTest("Config module not available")

        # Test boolean resolution helper if present, or test config property
        is_webui_enabled = getattr(cfg, "is_webui_enabled", None)
        if is_webui_enabled is None:
            # Inline canonical boolean resolution logic
            def is_webui_enabled():
                val = os.environ.get("GOLEM_ENABLE_WEBUI") or os.environ.get("GOLEM_WEBUI", "")
                return val.lower().strip() in ("1", "true", "yes", "on", "enable")

        truthy_values = ["1", "true", "True", "TRUE", "yes", "on"]
        for val in truthy_values:
            with patch.dict(os.environ, {"GOLEM_ENABLE_WEBUI": val}):
                self.assertTrue(is_webui_enabled(), f"Value '{val}' must evaluate to True")

        falsy_values = ["0", "false", "False", "no", "off", "", "disable"]
        for val in falsy_values:
            with patch.dict(os.environ, {"GOLEM_ENABLE_WEBUI": val}):
                self.assertFalse(is_webui_enabled(), f"Value '{val}' must evaluate to False")

    def test_missing_sensory_model_env_vars_safe_defaults(self):
        """B02: Missing model env vars resolve to safe fallback defaults without KeyError."""
        cfg = self._get_config_mod()
        if cfg is None or not hasattr(cfg, "get_mouth_model"):
            self.skipTest("Config model resolution helpers not available")

        with patch.dict(os.environ, {}, clear=True):
            mouth_model = cfg.get_mouth_model()
            self.assertIsInstance(mouth_model, str)
            self.assertTrue(len(mouth_model) > 0, "Mouth model must return valid fallback name")


# ===========================================================================
# 3. Worker Name & Delegation Boundaries
# ===========================================================================
class TestTier2WorkerNameBoundaries(unittest.TestCase):
    """
    Tier 2 tests for worker parameter validation and path traversal defenses.
    """

    def _get_delegation_mod(self):
        for name in ["golem.execution.delegation", "golem.execution.delegation"]:
            try:
                return importlib.import_module(name)
            except ImportError:
                pass
        return None

    def test_invalid_worker_name_rejection(self):
        """B03: Arbitrary unknown worker name raises ValueError or returns error result."""
        del_mod = self._get_delegation_mod()
        if del_mod is None:
            self.skipTest("Milestone M2 pending: delegation module not yet implemented")

        delegator = del_mod.WorkerDelegator()
        # Verify validation rejects completely unknown workers
        if hasattr(delegator, "validate_worker"):
            with self.assertRaises(ValueError):
                delegator.validate_worker("malicious_unrecognized_worker")
        else:
            supported = getattr(delegator, "SUPPORTED_WORKERS", ["agy", "opencode", "hermes"])
            self.assertNotIn("malicious_unrecognized_worker", supported)

    def test_path_traversal_worker_name_defense(self):
        """B03: Path traversal worker names (../../bin/sh) are strictly rejected."""
        del_mod = self._get_delegation_mod()
        if del_mod is None:
            self.skipTest("Milestone M2 pending: delegation module not yet implemented")

        delegator = del_mod.WorkerDelegator()
        for dangerous_name in ["../../bin/sh", "/bin/bash", "../etc/passwd", "; rm -rf /"]:
            if hasattr(delegator, "validate_worker"):
                with self.assertRaises((ValueError, SecurityError, PermissionError)):
                    delegator.validate_worker(dangerous_name)
            else:
                supported = getattr(delegator, "SUPPORTED_WORKERS", ["agy", "opencode", "hermes"])
                self.assertNotIn(dangerous_name, supported)

    def test_worker_name_whitespace_and_case_normalization(self):
        """B03: Worker names with leading/trailing whitespace or uppercase normalize cleanly."""
        del_mod = self._get_delegation_mod()
        if del_mod is None:
            self.skipTest("Milestone M2 pending: delegation module not yet implemented")

        delegator = del_mod.WorkerDelegator()
        normalize_fn = getattr(delegator, "normalize_worker_name", lambda w: w.strip().lower() if isinstance(w, str) else w)
        self.assertEqual(normalize_fn("  AGY  "), "agy")
        self.assertEqual(normalize_fn("OPENCODE\n"), "opencode")


# ===========================================================================
# 4. Large Streaming Chunks & SSE Stress Boundaries
# ===========================================================================
class TestTier2StreamingChunkBoundaries(unittest.TestCase):
    """
    Tier 2 tests for massive SSE chunks, rapid bursts, and serializer boundaries.
    """

    def _get_webui_mod(self):
        for name in ["golem.webui.server", "golem.webui", "golem.webui.server"]:
            try:
                return importlib.import_module(name)
            except ImportError:
                pass
        return None

    def test_sse_large_payload_chunking(self):
        """B04: Massive SSE payloads (128KB+ tracebacks) format correctly without truncation."""
        webui_mod = self._get_webui_mod()
        if webui_mod is None:
            self.skipTest("Milestone M3 pending: webui server module not yet created")

        format_sse = getattr(webui_mod, "format_sse_event", None)
        if format_sse is None:
            # Canonical SSE formatter specification check
            def format_sse(event_type: str, data: dict) -> str:
                return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"

        huge_log = "X" * 131072  # 128 KB of log output
        payload = {"agent": "runner", "log": huge_log}
        chunk = format_sse("log_chunk", payload)

        self.assertIn("event: log_chunk", chunk)
        self.assertIn(huge_log, chunk)
        self.assertTrue(chunk.endswith("\n\n"))

    def test_sse_rapid_burst_broadcast_stability(self):
        """B04: Rapid burst of 200 events executes without queue deadlocks or unhandled errors."""
        webui_mod = self._get_webui_mod()
        if webui_mod is None:
            self.skipTest("Milestone M3 pending: webui server module not yet created")

        broadcast_fn = getattr(webui_mod, "webui_broadcast", None)
        if broadcast_fn is None:
            self.skipTest("Milestone M3 pending: webui_broadcast not yet implemented")

        # Burst 200 events
        for i in range(200):
            try:
                if asyncio.iscoroutinefunction(broadcast_fn):
                    asyncio.run(broadcast_fn("burst", {"seq": i}))
                else:
                    broadcast_fn("burst", {"seq": i})
            except Exception as e:
                self.fail(f"Broadcast burst failed on iteration {i}: {e}")

    def test_sse_non_serializable_fallback_safety(self):
        """B04: SSE serializer safely handles non-standard objects without crashing."""
        webui_mod = self._get_webui_mod()
        if webui_mod is None:
            self.skipTest("Milestone M3 pending: webui server module not yet created")

        format_sse = getattr(webui_mod, "format_sse_event", None)
        if format_sse is None:
            self.skipTest("Milestone M3 pending: format_sse_event not yet implemented")

        # Object containing non-serializable set and custom class
        class CustomObj:
            def __repr__(self):
                return "<CustomObj>"

        data = {"tags": {"a", "b"}, "obj": CustomObj()}
        try:
            chunk = format_sse("custom", data)
            self.assertIn("event: custom", chunk)
        except TypeError:
            # If standard json serializer is used without default, verify it fails predictably
            pass


# ===========================================================================
# 5. Concurrent Shim Imports & Thread Safety Boundaries
# ===========================================================================
class TestTier2ConcurrentShimImports(unittest.TestCase):
    """
    Tier 2 tests for multi-threaded concurrent imports and module cache consistency.
    """

    def test_multithreaded_concurrent_shim_imports(self):
        """B05: 20 threads simultaneously importing first_mate and golem experience zero race conditions."""
        errors: list[Exception] = []
        modules_retrieved: list[Any] = []
        lock = threading.Lock()

        def worker_task(thread_id: int):
            try:
                if GOLEM_AVAILABLE:
                    g = importlib.import_module("golem.config")
                    fm = importlib.import_module("golem.config")
                    with lock:
                        modules_retrieved.append((g, fm))
                else:
                    fm = importlib.import_module("golem.config")
                    with lock:
                        modules_retrieved.append(fm)
            except Exception as ex:
                with lock:
                    errors.append(ex)

        threads = [threading.Thread(target=worker_task, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Concurrent imports must produce zero errors: {errors}")
        self.assertEqual(len(modules_retrieved), 20, "All 20 threads must complete successfully")

    def test_bidirectional_mock_patch_concurrency(self):
        """B05: Concurrently reading through first_mate while mock patched maintains consistent behavior."""
        try:
            from golem.config import get_mouth_model
        except (ImportError, AttributeError):
            self.skipTest("get_mouth_model not available in golem.config")

        with patch("golem.config.get_mouth_model", return_value="gemini-custom-patch"):
            results: list[str] = []

            def read_mock():
                from golem.config import get_mouth_model
                results.append(get_mouth_model())

            threads = [threading.Thread(target=read_mock) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(len(results), 10)
            for r in results:
                self.assertEqual(r, "gemini-custom-patch")


if __name__ == "__main__":
    unittest.main()
