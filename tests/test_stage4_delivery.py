#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

tests/test_stage4_delivery.py
Unified Stage 4 Test Suite: Channel-Context Artifact Delivery, Sandboxing & SQLite TaskLedger.
Consolidates artifact sandboxing, resolution fallback, auto-binding, TaskLedger persistence,
and multi-channel client upload into a fast, deterministic, high-signal test suite.
"""

from __future__ import annotations

import os
import sys
import json
import time
import shutil
import tempfile
import asyncio
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from golem.outbox import (
    ArtifactMetadata,
    SandboxSecurityError,
    OutboxManager,
    assert_sandboxed_path,
    calculate_sha256,
)
from golem.tools.delivery import share_artifact
from golem.session import (
    SessionHandle,
    SessionState,
    CURRENT_SESSION,
    get_current_session,
)
from golem.router import SessionRouter
from golem.harness import DualPaneHarness
from golem.state import TaskLedger
from golem.ingress import (
    SHARE_ARTIFACT_TOOL_DECLARATION,
    get_mouth_tools_declaration,
    IngressGateway,
)


class TestDeliverySandboxSecurity(unittest.TestCase):
    """Test suite verifying strict outbox containment and path traversal defenses."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_sandbox_sec_")
        self.outbox_dir = Path(self.temp_dir) / "outbox"
        self.mgr = OutboxManager(root_dir=self.outbox_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_valid_sandboxed_file_permitted(self):
        """Verify normal file created inside ~/.golem/outbox/ passes sandbox assertion."""
        valid_file = self.outbox_dir / "valid.txt"
        valid_file.write_text("valid content")
        resolved = assert_sandboxed_path(valid_file, sandbox_root=self.outbox_dir)
        self.assertEqual(resolved, valid_file.resolve())

    def test_parent_directory_traversal_rejected(self):
        """Verify ../ directory traversal escaping the sandbox raises SandboxSecurityError."""
        escape_path = str(self.outbox_dir / ".." / "escape.txt")
        with self.assertRaises(SandboxSecurityError):
            assert_sandboxed_path(escape_path, sandbox_root=self.outbox_dir)

    def test_absolute_system_paths_rejected(self):
        """Verify absolute paths outside outbox (/etc/passwd, ~/.ssh) are rejected."""
        with self.assertRaises(SandboxSecurityError):
            assert_sandboxed_path("/etc/passwd", sandbox_root=self.outbox_dir)

        ssh_key = Path.home() / ".ssh" / "id_rsa"
        with self.assertRaises(SandboxSecurityError):
            assert_sandboxed_path(str(ssh_key), sandbox_root=self.outbox_dir)

    def test_null_byte_injection_rejected(self):
        """Verify null bytes in path raise SandboxSecurityError or ValueError."""
        null_path = str(self.outbox_dir / "report.pdf\x00.exe")
        with self.assertRaises((SandboxSecurityError, ValueError)):
            assert_sandboxed_path(null_path, sandbox_root=self.outbox_dir)

    def test_symlink_escape_rejected(self):
        """Verify symlinks pointing outside the outbox are strictly rejected."""
        outside_target = Path(self.temp_dir) / "secret.key"
        outside_target.write_text("sensitive")
        symlink_path = self.outbox_dir / "symlink_escape.key"
        symlink_path.symlink_to(outside_target)

        with self.assertRaises(SandboxSecurityError):
            assert_sandboxed_path(symlink_path, sandbox_root=self.outbox_dir)

    def test_internal_symlink_permitted(self):
        """Verify symlinks resolving inside the outbox sandbox are allowed."""
        internal_target = self.outbox_dir / "target.txt"
        internal_target.write_text("internal payload")
        internal_symlink = self.outbox_dir / "link_target.txt"
        internal_symlink.symlink_to(internal_target)

        resolved = assert_sandboxed_path(internal_symlink, sandbox_root=self.outbox_dir)
        self.assertEqual(resolved, internal_target.resolve())


class TestDeliveryAutoBindingAndResolution(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying delivery auto-binding, fallback resolution, and ContextVar support."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_delivery_res_")
        self.outbox_dir = Path(self.temp_dir) / "outbox"
        self.mgr = OutboxManager(root_dir=self.outbox_dir)
        self.session = SessionHandle(
            session_id="sess_res_001",
            channel="C_RESOLVE",
            thread_ts="1800.0001",
            target_cwd=self.temp_dir,
        )
        self._prev_token = CURRENT_SESSION.set(None)

    def tearDown(self):
        CURRENT_SESSION.reset(self._prev_token)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_explicit_path_sharing_and_metadata(self):
        """Verify share_artifact with explicit path calculates hashes, mime, and metadata."""
        meta = self.mgr.stage_artifact(
            session_id="sess_res_001",
            content_or_path=b"Explicit payload",
            filename="explicit.txt",
        )

        res = await share_artifact(
            path=meta.absolute_path,
            channel="C_RESOLVE",
            thread_ts="1800.0001",
            title="Explicit Test",
            outbox_manager=self.mgr,
            session=self.session,
        )

        self.assertTrue(res["ok"])
        self.assertEqual(res["filename"], "explicit.txt")
        self.assertEqual(res["size_bytes"], len(b"Explicit payload"))
        self.assertEqual(res["sha256"], calculate_sha256(Path(meta.absolute_path)))
        self.assertEqual(res["title"], "Explicit Test")
        self.assertEqual(res["directive"], f"[SEND_FILE: {meta.absolute_path}]")
        # Auto-bound to session
        self.assertEqual(self.session.last_artifact["filename"], "explicit.txt")

    async def test_omitted_path_resolves_session_last_artifact(self):
        """Verify omitted path automatically resolves from session.get_last_artifact()."""
        meta = self.mgr.stage_artifact(
            session_id="sess_res_001",
            content_or_path=b"Session bound content",
            filename="from_session.txt",
        )
        self.session.record_artifact(meta)

        res = await share_artifact(
            path=None,
            session=self.session,
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertEqual(res["filename"], "from_session.txt")
        self.assertEqual(res["artifact_id"], meta.artifact_id)

    async def test_empty_string_path_resolves_from_session(self):
        """Verify empty string path is treated as omitted path and resolves from session."""
        meta = self.mgr.stage_artifact(
            session_id="sess_res_001",
            content_or_path=b"Empty string test",
            filename="empty_str.txt",
        )
        self.session.record_artifact(meta)

        res = await share_artifact(
            path="",
            session=self.session,
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertEqual(res["filename"], "empty_str.txt")

    async def test_omitted_path_resolves_current_session_contextvar(self):
        """Verify omitted path and omitted session resolves via CURRENT_SESSION ContextVar."""
        meta = self.mgr.stage_artifact(
            session_id="sess_res_001",
            content_or_path=b"ContextVar payload",
            filename="contextvar.txt",
        )
        self.session.record_artifact(meta)

        token = CURRENT_SESSION.set(self.session)
        try:
            res = await share_artifact(
                path=None,
                session=None,
                outbox_manager=self.mgr,
            )
            self.assertTrue(res["ok"])
            self.assertEqual(res["filename"], "contextvar.txt")
        finally:
            CURRENT_SESSION.reset(token)

    async def test_lookup_by_artifact_id(self):
        """Verify resolving artifact by artifact_id when path is omitted."""
        meta = self.mgr.stage_artifact(
            session_id="sess_res_001",
            content_or_path=b"ID lookup payload",
            filename="id_lookup.txt",
        )

        res = await share_artifact(
            path=None,
            artifact_id=meta.artifact_id,
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertEqual(res["filename"], "id_lookup.txt")
        self.assertEqual(res["artifact_id"], meta.artifact_id)

    async def test_omitted_path_without_session_or_context_raises_value_error(self):
        """Verify omitting path with no active session or thread pointer raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            await share_artifact(
                path=None,
                session=None,
                outbox_manager=self.mgr,
            )
        self.assertIn("No artifact specified and no active last_artifact found", str(ctx.exception))

    async def test_nonexistent_artifact_raises_file_not_found(self):
        """Verify invalid artifact_id or missing file raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            await share_artifact(
                path=None,
                artifact_id="art_nonexistent_99999",
                outbox_manager=self.mgr,
            )


class TestDeliverySqliteLedgerIntegration(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying persistence in SQLite TaskLedger (~/.golem/state/tasks.db)."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_delivery_ledger_")
        self.outbox_dir = Path(self.temp_dir) / "outbox"
        self.state_db = Path(self.temp_dir) / "tasks.db"
        self.ledger = TaskLedger(db_path=self.state_db)
        self.mgr = OutboxManager(root_dir=self.outbox_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_delivered_artifact_persisted_in_sqlite_ledger(self):
        """Verify share_artifact records the artifact in SQLite TaskLedger."""
        test_file = self.outbox_dir / "data_export.csv"
        test_file.write_text("col1,col2\n1,2\n")

        with patch("golem.state.get_default_task_ledger", return_value=self.ledger):
            res = await share_artifact(
                path=str(test_file),
                channel="C_LEDGER",
                thread_ts="1900.0001",
                outbox_manager=self.mgr,
            )

            self.assertTrue(res["ok"])
            art_id = res["artifact_id"]

            row = self.ledger.get_artifact(art_id)
            self.assertIsNotNone(row)
            self.assertEqual(row["artifact_id"], art_id)
            self.assertEqual(row["channel"], "C_LEDGER")
            self.assertEqual(row["thread_ts"], "1900.0001")
            self.assertEqual(row["abs_path"], str(test_file.resolve()))
            self.assertEqual(row["size_bytes"], len("col1,col2\n1,2\n"))

    async def test_resolve_from_sqlite_ledger_by_channel_and_thread(self):
        """Verify delivery engine resolves last artifact from SQLite ledger by thread pointer."""
        test_file = self.outbox_dir / "thread_report.md"
        test_file.write_text("# Thread Report")

        with patch("golem.state.get_default_task_ledger", return_value=self.ledger):
            # Seed ledger with artifact
            self.ledger.record_artifact(
                artifact_id="art_thread_seed",
                task_id="task_seed_1",
                channel="C_THREAD_LOOKUP",
                thread_ts="2000.0001",
                rel_path="thread_report.md",
                abs_path=str(test_file.resolve()),
                sha256=calculate_sha256(test_file),
                size_bytes=len("# Thread Report"),
            )

            # Share with path=None, resolving via channel + thread_ts from ledger
            res = await share_artifact(
                path=None,
                channel="C_THREAD_LOOKUP",
                thread_ts="2000.0001",
                outbox_manager=self.mgr,
            )

            self.assertTrue(res["ok"])
            self.assertEqual(res["artifact_id"], "art_thread_seed")
            self.assertEqual(res["filename"], "thread_report.md")

    async def test_foreign_key_safety_for_standalone_artifacts(self):
        """Verify standalone artifacts insert stub parent task without foreign key integrity errors."""
        art = self.ledger.record_artifact(
            artifact_id="art_standalone_test",
            task_id="standalone_session_x",
            channel="C_STUB",
            thread_ts="3000.0001",
            rel_path="stub.txt",
            abs_path="/tmp/stub.txt",
            sha256="aabbcc",
            size_bytes=10,
        )
        self.assertEqual(art["artifact_id"], "art_standalone_test")

        # Parent task should have been stubbed
        parent = self.ledger.get_task("standalone_session_x")
        self.assertIsNotNone(parent)
        self.assertEqual(parent["task_id"], "standalone_session_x")
        self.assertEqual(parent["status"], "completed")


class TestDeliveryClientUpload(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying Slack and BlueBubbles client upload handling."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_delivery_upload_")
        self.outbox_dir = Path(self.temp_dir) / "outbox"
        self.mgr = OutboxManager(root_dir=self.outbox_dir)
        self.sample_file = self.outbox_dir / "test_chart.png"
        self.sample_file.write_bytes(b"\x89PNG\r\n\x1a\nfake_image_data")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_slack_files_upload_v2_async_client(self):
        """Verify delivery via Slack files_upload_v2 with an async client."""
        mock_client = AsyncMock()
        mock_client.files_upload_v2.return_value = {
            "ok": True,
            "file": {"id": "F999888", "name": "test_chart.png"},
        }

        res = await share_artifact(
            path=str(self.sample_file),
            client=mock_client,
            channel="C_SLACK",
            thread_ts="1500.0001",
            title="Async Slack Upload",
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertTrue(res["shared"])
        self.assertIsNotNone(res["shared_at"])
        self.assertEqual(res["upload_metadata"]["file"]["id"], "F999888")
        mock_client.files_upload_v2.assert_called_once_with(
            channel="C_SLACK",
            file=str(self.sample_file.resolve()),
            filename="test_chart.png",
            title="Async Slack Upload",
            thread_ts="1500.0001",
        )

    async def test_slack_files_upload_v2_sync_client(self):
        """Verify delivery via Slack files_upload_v2 with a synchronous client."""
        mock_client = MagicMock()
        mock_client.files_upload_v2.return_value = {
            "ok": True,
            "file": {"id": "F777666", "name": "test_chart.png"},
        }

        res = await share_artifact(
            path=str(self.sample_file),
            client=mock_client,
            channel="C_SLACK_SYNC",
            title="Sync Slack Upload",
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertTrue(res["shared"])
        self.assertEqual(res["upload_metadata"]["file"]["id"], "F777666")
        mock_client.files_upload_v2.assert_called_once()

    async def test_bluebubbles_upload_file_client(self):
        """Verify delivery via BlueBubbles or generic client with upload_file method."""
        mock_client = MagicMock(spec=["upload_file"])
        mock_client.upload_file = AsyncMock(return_value={"ok": True, "message_id": "bb_msg_123"})

        res = await share_artifact(
            path=str(self.sample_file),
            client=mock_client,
            channel="bb_guid_chat",
            thread_ts="bb_msg_001",
            title="BlueBubbles Upload",
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertTrue(res["shared"])
        mock_client.upload_file.assert_called_once()

    async def test_client_missing_upload_methods_raises_attribute_error(self):
        """Verify providing a client object without files_upload_v2 or upload_file raises AttributeError."""
        dummy_client = object()
        with self.assertRaises(AttributeError):
            await share_artifact(
                path=str(self.sample_file),
                client=dummy_client,
                channel="C_FAIL",
                outbox_manager=self.mgr,
            )


class TestSessionAndRouterArtifactTracking(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying session-level artifact tracking, ContextVars, and router rehydration."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_sess_router_")
        self.session = SessionHandle(
            session_id="sess_tracking_01",
            channel="C_TRACK",
            thread_ts="1600.0001",
            target_cwd=self.temp_dir,
        )
        self.registry_file = os.path.join(self.temp_dir, "sessions.json")
        self.router = SessionRouter(
            default_target_cwd=self.temp_dir,
            registry_path=self.registry_file,
            driver_factory=lambda cwd, extra_args=None: MagicMock(conversation_id="conv_track"),
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_session_artifact_recording_and_binding(self):
        """Verify recording an artifact dictionary updates last_artifact and artifact list."""
        art_dict = {
            "artifact_id": "art_111",
            "filename": "report.pdf",
            "absolute_path": "/tmp/outbox/report.pdf",
            "size_bytes": 1024,
        }
        res = self.session.record_artifact(art_dict)
        self.assertEqual(res["filename"], "report.pdf")
        self.assertEqual(self.session.last_artifact, res)
        self.assertEqual(self.session.last_artifact_path, "/tmp/outbox/report.pdf")
        self.assertIn(res, self.session.artifacts)

    async def test_router_persists_and_rehydrates_last_artifact(self):
        """Verify SessionRouter records last_artifact to JSON registry and rehydrates it."""
        art = {
            "artifact_id": "art_rehydrate",
            "filename": "metrics.json",
            "path": "/tmp/metrics.json",
            "absolute_path": "/tmp/metrics.json",
            "size_bytes": 512,
        }
        self.router.record_session(
            session_key="C_TRACK:1600.0001",
            conversation_id="conv_track",
            target_cwd=self.temp_dir,
            channel="C_TRACK",
            thread_ts="1600.0001",
            last_artifact=art,
        )

        # Clear in-memory cache to force registry rehydration
        self.router.sessions.clear()

        session, is_new = await self.router.get_or_create(channel="C_TRACK", thread_ts="1600.0001")
        self.assertTrue(is_new)
        self.assertEqual(session.last_artifact["artifact_id"], "art_rehydrate")
        self.assertEqual(session.last_artifact["filename"], "metrics.json")
        self.assertEqual(session.last_artifact_path, "/tmp/metrics.json")

    async def test_harness_stage_artifact_auto_binds_to_session(self):
        """Verify DualPaneHarness.stage_artifact auto-binds artifact to session."""
        outbox_mgr = OutboxManager(root_dir=Path(self.temp_dir) / "outbox")
        mock_herdr = AsyncMock()
        mock_herdr.run_cli.return_value = (0, json.dumps({"result": {"workspace_id": "ws_test"}}), "")
        harness = DualPaneHarness(
            herdr_client=mock_herdr,
            default_cwd=self.temp_dir,
            outbox=outbox_mgr,
            router=self.router,
        )

        session, _ = await self.router.get_or_create(channel="C_HARN", thread_ts="1800.0001")
        staged = harness.stage_artifact(
            session_id=session.session_id,
            content_or_path=b"Harness staged content",
            filename="harness_file.txt",
            channel="C_HARN",
            thread_ts="1800.0001",
        )

        self.assertEqual(staged.filename, "harness_file.txt")
        self.assertIsNotNone(session.last_artifact)
        self.assertEqual(session.last_artifact["filename"], "harness_file.txt")
        self.assertEqual(session.last_artifact["artifact_id"], staged.artifact_id)


class TestIngressDeliveryToolIntegration(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying IngressGateway share_artifact tool declaration and execution."""

    def test_share_artifact_tool_declaration_schema(self):
        """Verify share_artifact tool schema exposes path, artifact_id, and title."""
        decl = SHARE_ARTIFACT_TOOL_DECLARATION
        self.assertEqual(decl["name"], "share_artifact")
        self.assertIn("outbox", decl["description"].lower())
        props = decl["parameters"]["properties"]
        self.assertIn("path", props)
        self.assertIn("artifact_id", props)
        self.assertIn("title", props)

    def test_mouth_tools_includes_share_artifact(self):
        """Verify get_mouth_tools_declaration exposes share_artifact to Chief of Staff Mouth."""
        tools = get_mouth_tools_declaration()
        tool_names = [fn["name"] for g in tools for fn in g.get("functionDeclarations", [])]
        self.assertIn("share_artifact", tool_names)
        self.assertIn("dispatch_task", tool_names)

    async def test_ingress_execute_share_artifact_success(self):
        """Verify IngressGateway._execute_share_artifact executes correctly."""
        temp_dir = tempfile.mkdtemp(prefix="test_ing_deliv_")
        try:
            outbox_dir = Path(temp_dir) / "outbox"
            mgr = OutboxManager(root_dir=outbox_dir)
            meta = mgr.stage_artifact(
                session_id="sess_ing_01",
                content_or_path=b"Ingress test file",
                filename="ing_test.txt",
            )
            session = SessionHandle(
                session_id="sess_ing_01",
                channel="C_ING",
                thread_ts="1700.0001",
                target_cwd=temp_dir,
            )
            session.record_artifact(meta)

            router = SessionRouter(default_target_cwd=temp_dir)
            router.harness = MagicMock(outbox=mgr)
            gateway = IngressGateway(router=router, bot_user_id="U_ING")

            res = await gateway._execute_share_artifact(
                path=meta.absolute_path,
                session=session,
                channel="C_ING",
                thread_ts="1700.0001",
                title="Ingress Delivered",
            )

            self.assertTrue(res["ok"])
            self.assertEqual(res["filename"], "ing_test.txt")
            self.assertEqual(res["artifact_id"], meta.artifact_id)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    async def test_ingress_execute_share_artifact_raises_security_error(self):
        """Verify IngressGateway._execute_share_artifact rejects files outside outbox."""
        temp_dir = tempfile.mkdtemp(prefix="test_ing_sec_")
        try:
            router = SessionRouter(default_target_cwd=temp_dir)
            gateway = IngressGateway(router=router, bot_user_id="U_ING")
            session = SessionHandle(
                session_id="sess_sec_01",
                channel="C_SEC",
                thread_ts="1700.0002",
                target_cwd=temp_dir,
            )

            with self.assertRaises(SandboxSecurityError):
                await gateway._execute_share_artifact(
                    path="/etc/passwd",
                    session=session,
                )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    async def test_prompt_hydration_includes_active_artifact(self):
        """Verify prompt hydration prepends [Active Session Artifact: ...] when last_artifact is active."""
        temp_dir = tempfile.mkdtemp(prefix="test_prompt_hyd_")
        try:
            router = SessionRouter(default_target_cwd=temp_dir)
            gateway = IngressGateway(router=router, bot_user_id="U_BOT")
            session = SessionHandle(
                session_id="sess_hyd_01",
                channel="C_HYD",
                thread_ts="1800.0001",
                target_cwd=temp_dir,
            )
            session.record_artifact({
                "filename": "summary.pdf",
                "path": "/tmp/outbox/summary.pdf",
                "absolute_path": "/tmp/outbox/summary.pdf",
            })

            router.get_or_create = AsyncMock(return_value=(session, False))

            captured_prompt = None

            async def mock_send_user_message(prompt, **kwargs):
                nonlocal captured_prompt
                captured_prompt = prompt
                if False:
                    yield None

            session.send_user_message = mock_send_user_message

            event = {
                "type": "message",
                "text": "Send me the document",
                "channel": "C_HYD",
                "thread_ts": "1800.0001",
                "ts": "1800.0002",
                "user": "U9876",
            }
            mock_client = AsyncMock()
            mock_client.conversations_replies.return_value = {"messages": []}
            await gateway.handle_message(event=event, client=mock_client)

            self.assertIsNotNone(captured_prompt)
            self.assertIn("[Active Session Artifact: summary.pdf at /tmp/outbox/summary.pdf]", captured_prompt)
            self.assertIn("Send me the document", captured_prompt)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
