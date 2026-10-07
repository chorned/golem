"""Quota-exhaustion failover for the Antigravity driver (Hands)."""
import asyncio
import unittest
from unittest import mock

from golem.agent.drivers.antigravity import AntigravityDriver, is_quota_error
from golem.agent.protocol import FinishEvent, StatusUpdateEvent, TextDeltaEvent
from golem.config import get_engine_fallback_models


def _driver(**kw):
    kw.setdefault("model", "primary-model")
    kw.setdefault("fallback_models", ["sonnet-fallback"])
    return AntigravityDriver(cwd="/tmp", **kw)


class QuotaDetectionTests(unittest.TestCase):
    def test_detects_quota_variants(self):
        for msg in (
            # Verbatim error captured from the live agy CLI on 2026-10-06
            "RESOURCE_EXHAUSTED (code 429): Individual quota reached. Please upgrade your subscription "
            "to increase your limits. Resets in 7h8m39s.",
            "You have hit your individual subscription quota (resets in 7h)",
            "RESOURCE_EXHAUSTED: capacity",
            "HTTP 429 Too Many Requests",
            "rate limit exceeded",
        ):
            self.assertTrue(is_quota_error(msg), msg)

    def test_ignores_unrelated_errors(self):
        for msg in ("", None, "Broken pipe writing to agent stdin", "tool failed: ENOENT"):
            self.assertFalse(is_quota_error(msg), msg)


class FallbackConfigTests(unittest.TestCase):
    def test_parses_ordered_list(self):
        with mock.patch.dict("os.environ", {"GOLEM_ANTIGRAVITY_FALLBACK_MODELS": " a , b,,c "}):
            self.assertEqual(get_engine_fallback_models("agy"), ["a", "b", "c"])

    def test_unknown_engine_empty(self):
        self.assertEqual(get_engine_fallback_models("opencode"), [])


class SendPromptFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def _collect(self, drv, prompt="go"):
        return [e async for e in drv.send_prompt(prompt)]

    async def test_switches_model_and_retries_on_quota(self):
        drv = _driver()
        drv.conversation_id = "conv-1"
        calls = []

        async def fake_turn(prompt):
            calls.append((drv.model, list(drv.extra_args)))
            if len(calls) == 1:
                yield FinishEvent(response="", status="ERROR", error="subscription quota hit, resets in 7h")
            else:
                yield TextDeltaEvent(text_delta="done")
                yield FinishEvent(response="done", status="SUCCESS")

        drv._stream_turn_impl = fake_turn
        drv.terminate_session = mock.AsyncMock()
        events = await self._collect(drv)

        self.assertEqual([c[0] for c in calls], ["primary-model", "sonnet-fallback"])
        self.assertEqual(calls[1][1], ["--conversation", "conv-1"])
        self.assertTrue(any(isinstance(e, StatusUpdateEvent) and e.status == "model_fallback" for e in events))
        finishes = [e for e in events if isinstance(e, FinishEvent)]
        self.assertEqual(len(finishes), 1)
        self.assertEqual(finishes[0].status, "SUCCESS")
        self.assertEqual(drv.fallback_history[0]["to"], "sonnet-fallback")
        drv.terminate_session.assert_awaited_once()

    async def test_non_quota_error_passes_through(self):
        drv = _driver()

        async def fake_turn(prompt):
            yield FinishEvent(response="", status="ERROR", error="ENOENT")

        drv._stream_turn_impl = fake_turn
        events = await self._collect(drv)
        self.assertEqual(drv.model, "primary-model")
        self.assertEqual(events[-1].error, "ENOENT")

    async def test_chain_exhausted_surfaces_error(self):
        drv = _driver()

        async def fake_turn(prompt):
            yield FinishEvent(response="", status="ERROR", error="quota exceeded")

        drv._stream_turn_impl = fake_turn
        drv.terminate_session = mock.AsyncMock()
        events = await self._collect(drv)
        finishes = [e for e in events if isinstance(e, FinishEvent)]
        self.assertEqual(len(finishes), 1)
        self.assertEqual(finishes[0].status, "ERROR")
        self.assertEqual(drv.model, "sonnet-fallback")

    async def test_no_fallback_configured_unchanged(self):
        drv = _driver(fallback_models=[])

        async def fake_turn(prompt):
            yield FinishEvent(response="", status="ERROR", error="quota exceeded")

        drv._stream_turn_impl = fake_turn
        events = await self._collect(drv)
        self.assertEqual(len(events), 1)
        self.assertEqual(drv.model, "primary-model")

    async def test_start_failure_with_quota_triggers_fallback(self):
        drv = _driver()
        n = {"i": 0}

        async def fake_turn(prompt):
            n["i"] += 1
            if n["i"] == 1:
                raise RuntimeError("agy process exited prematurely on start: quota exhausted")
            yield FinishEvent(response="ok", status="SUCCESS")

        drv._stream_turn_impl = fake_turn
        drv.terminate_session = mock.AsyncMock()
        events = await self._collect(drv)
        self.assertEqual(drv.model, "sonnet-fallback")
        self.assertEqual(events[-1].status, "SUCCESS")


if __name__ == "__main__":
    unittest.main()
