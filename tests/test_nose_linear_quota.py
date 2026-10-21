#!/usr/bin/env python3
"""
tests/test_nose_linear_quota.py
Unit and integration tests for Linear free tier quota exhaustion detection in The Nose,
ShoulderTapEventBus alerting, and Mouth liaison prompting (with zero automated deletions).
"""

import os
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from golem.bus import ShoulderTapEventBus, ShoulderTapAlert
from golem.nose import NoseSentinel
from golem.mouth import synthesize_linear_quota_nudge
from golem.ingress import IngressGateway


SAMPLE_BOT_ERR_QUOTA_LINE = (
    "2026-10-19 08:36:00 [WARNING] Golem.Gateway.Ingress: "
    "Could not record hand result to Linear for D0C2J1HKNN7:1792416594.073069: "
    "Linear GraphQL error: [{'message': 'usage limit exceeded', 'path': ['issueCreate'], "
    "'locations': [{'line': 3, 'column': 13}], 'extensions': {'type': 'usage limit exceeded', "
    "'code': 'USAGE_LIMIT_EXCEEDED', 'statusCode': 400, 'userError': True, "
    "'userPresentableMessage': \"You've exceeded the free issue limit for this workspace. Please upgrade or contact sales@linear.app for a free trial.\", "
    "'meta': {'usageMetric': 'activeIssueCount'}}}]"
)


class TestNoseLinearQuotaDetection(unittest.IsolatedAsyncioTestCase):
    """Test detection of USAGE_LIMIT_EXCEEDED in logs and Linear API calls."""

    def test_scan_error_text_captures_usage_limit_exceeded(self) -> None:
        sentinel = NoseSentinel()
        errors = sentinel._scan_error_text(SAMPLE_BOT_ERR_QUOTA_LINE)
        self.assertEqual(len(errors), 1)
        self.assertIn("USAGE_LIMIT_EXCEEDED", errors[0])
        self.assertIn("activeIssueCount", errors[0])

    def test_detect_linear_quota_exceeded_from_raw_and_errors(self) -> None:
        sentinel = NoseSentinel()
        details = sentinel.detect_linear_quota_exceeded(raw_text=SAMPLE_BOT_ERR_QUOTA_LINE)
        self.assertIsNotNone(details)
        self.assertEqual(details["metric"], "activeIssueCount")
        self.assertEqual(details["error_code"], "USAGE_LIMIT_EXCEEDED")
        self.assertTrue(len(details["sample_lines"]) > 0)

    def test_sniff_bot_err_detects_quota_lines(self) -> None:
        with tempfile.NamedTemporaryFile("w+", delete=False) as tf:
            tf.write("2026-10-19 08:30:00 [INFO] Golem.Gateway: Normal log line\n")
            tf.write(SAMPLE_BOT_ERR_QUOTA_LINE + "\n")
            tf.flush()
            tmp_path = tf.name

        try:
            sentinel = NoseSentinel(bot_err_path=tmp_path, startup_mode="zero")
            errors, raw = sentinel.sniff_bot_err()
            self.assertEqual(len(errors), 1)
            self.assertIn("USAGE_LIMIT_EXCEEDED", errors[0])
            self.assertIn("activeIssueCount", raw)

            details = sentinel.detect_linear_quota_exceeded(raw_text=raw, error_lines=errors)
            self.assertIsNotNone(details)
            self.assertEqual(details["error_code"], "USAGE_LIMIT_EXCEEDED")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    async def test_sniff_assigned_tickets_handles_quota_exception(self) -> None:
        mock_bridge = MagicMock()
        mock_bridge.get_assigned_issues = AsyncMock(
            side_effect=Exception("Linear GraphQL error: USAGE_LIMIT_EXCEEDED activeIssueCount")
        )
        sentinel = NoseSentinel(linear_bridge=mock_bridge)
        res = await sentinel.sniff_assigned_tickets()
        self.assertEqual(res, [])
        self.assertTrue(sentinel._linear_quota_exceeded)
        self.assertIsNotNone(sentinel._last_linear_quota_details)
        self.assertEqual(sentinel._last_linear_quota_details["error_code"], "USAGE_LIMIT_EXCEEDED")


class TestNoseLinearQuotaAlertEmission(unittest.IsolatedAsyncioTestCase):
    """Test ShoulderTapAlert emission over ShoulderTapEventBus for linear_quota_exceeded."""

    def test_bus_alert_type_support(self) -> None:
        alert = ShoulderTapAlert(
            alert_type="linear_quota_exceeded",
            source="nose",
            summary="Test quota exceeded",
            data={"metric": "activeIssueCount"},
        )
        self.assertTrue(alert.is_linear_quota_exceeded)
        self.assertEqual(alert.alert_type, "linear_quota_exceeded")

    async def test_check_linear_quota_emits_shoulder_tap_alert(self) -> None:
        bus = ShoulderTapEventBus()
        received_alerts: list[ShoulderTapAlert] = []
        bus.subscribe(lambda a: received_alerts.append(a), alert_types=["linear_quota_exceeded"])

        sentinel = NoseSentinel(bus=bus)
        alert = sentinel.check_linear_quota(raw_text=SAMPLE_BOT_ERR_QUOTA_LINE)

        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "linear_quota_exceeded")
        self.assertTrue(alert.is_linear_quota_exceeded)
        self.assertEqual(len(received_alerts), 1)
        self.assertIn("activeIssueCount", received_alerts[0].summary)
        self.assertEqual(received_alerts[0].data["status"], "blocked")

        # Deduplication check: calling again with same text does not duplicate alert
        second_alert = sentinel.check_linear_quota(raw_text=SAMPLE_BOT_ERR_QUOTA_LINE)
        self.assertIsNone(second_alert)
        self.assertEqual(len(received_alerts), 1)

    async def test_tick_emits_quota_alert(self) -> None:
        bus = ShoulderTapEventBus()
        received_alerts: list[ShoulderTapAlert] = []
        bus.subscribe(lambda a: received_alerts.append(a), alert_types=["linear_quota_exceeded"])

        sentinel = NoseSentinel(bus=bus)
        sentinel.sniff_bot_err = MagicMock(return_value=([SAMPLE_BOT_ERR_QUOTA_LINE], SAMPLE_BOT_ERR_QUOTA_LINE))
        sentinel.sniff_fleet = AsyncMock(return_value=None)
        sentinel.compute_status_deltas = MagicMock(return_value=[])
        sentinel.sniff_emails = AsyncMock(return_value=[])
        sentinel.sniff_assigned_tickets = AsyncMock(return_value=[])

        alert = await sentinel.tick()
        self.assertIsNotNone(alert)
        self.assertEqual(alert.alert_type, "linear_quota_exceeded")
        self.assertEqual(len(received_alerts), 1)
        self.assertEqual(received_alerts[0].alert_type, "linear_quota_exceeded")

    async def test_telemetry_digest_reflects_quota_exceeded(self) -> None:
        sentinel = NoseSentinel()
        sentinel._linear_quota_exceeded = True
        sentinel._last_linear_quota_details = {
            "metric": "activeIssueCount",
            "error_code": "USAGE_LIMIT_EXCEEDED",
        }

        digest = await sentinel.get_telemetry_digest(trigger_fresh_sniff=False)
        self.assertTrue(digest["linear_quota_exceeded"])
        self.assertTrue(digest["smells_off"])
        self.assertIn("Linear free tier issue limit reached", digest["verdict_summary"])
        self.assertEqual(digest["linear_quota_details"]["metric"], "activeIssueCount")


class TestNoAutomaticDeletionAndPrompting(unittest.IsolatedAsyncioTestCase):
    """Verify that NO automatic deletion occurs and Carl is prompted instead."""

    def test_synthesize_linear_quota_nudge_prompts_carl_and_guarantees_no_auto_delete(self) -> None:
        data = {
            "metric": "activeIssueCount",
            "error_code": "USAGE_LIMIT_EXCEEDED",
        }
        nudge = synthesize_linear_quota_nudge(data)

        # Must mention Linear quota exceeded
        self.assertIn("Linear Free Tier Issue Quota Reached", nudge)
        self.assertIn("activeIssueCount", nudge)
        self.assertIn("USAGE_LIMIT_EXCEEDED", nudge)

        # Must prompt Carl to clean up Done / Canceled tickets
        self.assertIn("Done", nudge)
        self.assertIn("Canceled", nudge)

        # Must explicitly assure zero automated deletions
        self.assertIn("no automated ticket deletions", nudge.lower())
        self.assertIn("carl owns hard delete operations", nudge.lower())

    async def test_ingress_gateway_handles_quota_alert_without_deleting_tickets(self) -> None:
        mock_bridge = MagicMock()
        mock_bridge.execute_query = AsyncMock()

        gateway = IngressGateway(linear_bridge=mock_bridge)
        mock_client = MagicMock()
        mock_client.chat_postMessage = MagicMock(return_value={"ts": "1234.5678"})

        alert = ShoulderTapAlert(
            alert_type="linear_quota_exceeded",
            source="nose",
            channel="C_TEST",
            data={
                "metric": "activeIssueCount",
                "error_code": "USAGE_LIMIT_EXCEEDED",
            },
        )

        await gateway.process_sensory_alert(alert, client=mock_client)

        # Verify notification posted to Slack
        mock_client.chat_postMessage.assert_called_once()
        call_kwargs = mock_client.chat_postMessage.call_args[1]
        self.assertEqual(call_kwargs["channel"], "C_TEST")
        self.assertIn("Linear Free Tier Issue Quota Reached", call_kwargs["text"])

        # CRITICAL INVARIANT: No Linear API mutation was called, especially issueDelete
        mock_bridge.execute_query.assert_not_called()
        self.assertEqual(len(mock_bridge.mock_calls), 0)
        from golem.linear_bridge import LinearBridge
        self.assertFalse(hasattr(LinearBridge, "issueDelete"))


if __name__ == "__main__":
    unittest.main()
