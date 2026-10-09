#!/usr/bin/env python3
"""
tests/test_stage4_artifact_sandboxing.py
Comprehensive functional integration test suite for Stage 4: Channel-Context Artifact Sandboxing & Transport Resolution.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

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
    share_artifact,
    assert_sandboxed_path,
)
from golem.session import (
    SessionHandle,
    SessionState,
    CURRENT_SESSION,
    get_current_session,
)
from golem.router import SessionRouter
from golem.harness import DualPaneHarness
from golem.ingress import (
    SHARE_ARTIFACT_TOOL_DECLARATION,
    DISPATCH_TASK_TOOL_DECLARATION,
    CHECK_EYES_TOOL_DECLARATION,
    CHECK_NOSE_TOOL_DECLARATION,
    get_mouth_tools_declaration,
    IngressGateway,
)


class TestSessionArtifactTracking(unittest.TestCase):
    """Test suite verifying SessionHandle artifact tracking and ContextVar helpers."""

    def setUp(self):
        self._prev_token = CURRENT_SESSION.set(None)
        self.session = SessionHandle(
            session_id="test_sess_001",
            channel="C12345678",
            thread_ts="1710000000.000100",
            target_cwd="/tmp",
        )

    def tearDown(self):
        CURRENT_SESSION.reset(self._prev_token)

    def test_session_artifact_initialization(self):
        """Verify session initializes with empty artifact state."""
        self.assertIsNone(self.session.last_artifact)
        self.assertEqual(self.session.artifacts, [])
        self.assertIsNone(self.session.last_artifact_path)

    def test_session_record_artifact_dict(self):
        """Verify recording a dictionary updates last_artifact and artifacts list."""
        art = {
            "filename": "summary.pdf",
            "path": "/tmp/outbox/artifacts/summary.pdf",
            "size_bytes": 1024,
            "mime_type": "application/pdf",
        }
        res = self.session.record_artifact(art)

        self.assertEqual(res["filename"], "summary.pdf")
        self.assertEqual(res["path"], "/tmp/outbox/artifacts/summary.pdf")
        self.assertEqual(self.session.last_artifact, res)
        self.assertEqual(self.session.artifacts, [res])
        self.assertEqual(self.session.last_artifact_path, "/tmp/outbox/artifacts/summary.pdf")

    def test_session_record_artifact_path_str(self):
        """Verify recording a string path normalizes to absolute path and metadata."""
        test_path = "/tmp/report.txt"
        res = self.session.record_artifact(test_path)

        self.assertEqual(res["filename"], "report.txt")
        self.assertEqual(res["path"], os.path.abspath(test_path))
        self.assertEqual(res["absolute_path"], os.path.abspath(test_path))
        self.assertEqual(res["session_id"], "test_sess_001")
        self.assertEqual(res["channel"], "C12345678")
        self.assertEqual(res["thread_ts"], "1710000000.000100")
        self.assertEqual(self.session.last_artifact_path, os.path.abspath(test_path))

    def test_session_record_artifact_metadata_object(self):
        """Verify recording an ArtifactMetadata instance converts cleanly."""
        meta = ArtifactMetadata(
            artifact_id="art_999",
            filename="data.csv",
            relative_path="artifacts/data.csv",
            absolute_path="/tmp/outbox/artifacts/data.csv",
            sha256="abcdef123456",
            size_bytes=512,
            created_at=time.time(),
            mime_type="text/csv",
            session_id="test_sess_001",
        )
        res = self.session.record_artifact(meta)

        self.assertEqual(res["artifact_id"], "art_999")
        self.assertEqual(res["filename"], "data.csv")
        self.assertEqual(res["absolute_path"], "/tmp/outbox/artifacts/data.csv")
        self.assertEqual(self.session.last_artifact, res)
        self.assertEqual(self.session.last_artifact_path, "/tmp/outbox/artifacts/data.csv")

    def test_session_bind_artifact_alias(self):
        """Verify bind_artifact is an alias to record_artifact."""
        self.assertEqual(self.session.bind_artifact, self.session.record_artifact)
        res = self.session.bind_artifact({"filename": "alias_test.png", "path": "/tmp/img.png"})
        self.assertEqual(res["filename"], "alias_test.png")
        self.assertEqual(self.session.last_artifact["filename"], "alias_test.png")

    def test_session_get_last_artifact_fallback_to_outbox(self):
        """Verify get_last_artifact queries OutboxManager if in-memory last_artifact is None."""
        self.assertIsNone(self.session.last_artifact)
        mock_meta = ArtifactMetadata(
            artifact_id="art_fallback",
            filename="fallback.txt",
            relative_path="artifacts/fallback.txt",
            absolute_path="/tmp/fallback.txt",
            sha256="123456",
            size_bytes=128,
            created_at=time.time(),
            session_id="test_sess_001",
        )

        mock_mgr = MagicMock()
        mock_mgr.list_artifacts.return_value = [mock_meta]

        with patch("golem.outbox.get_default_outbox_manager", return_value=mock_mgr):
            found = self.session.get_last_artifact()
            self.assertIsNotNone(found)
            self.assertEqual(found["artifact_id"], "art_fallback")
            self.assertEqual(found["filename"], "fallback.txt")
            mock_mgr.list_artifacts.assert_called_once_with(session_id="test_sess_001")

    def test_get_current_session_contextvar(self):
        """Verify get_current_session retrieves the SessionHandle bound in ContextVar."""
        self.assertIsNone(get_current_session())
        token = CURRENT_SESSION.set(self.session)
        try:
            self.assertEqual(get_current_session(), self.session)
        finally:
            CURRENT_SESSION.reset(token)
        self.assertIsNone(get_current_session())


class TestRouterArtifactPersistence(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying persistence and rehydration of last_artifact in SessionRouter."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_router_artifacts_")
        self.registry_file = os.path.join(self.temp_dir, "registry.json")
        self.router = SessionRouter(
            default_target_cwd=self.temp_dir,
            registry_path=self.registry_file,
            driver_factory=lambda cwd, extra_args=None: MagicMock(conversation_id="conv_123"),
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_record_session_persists_last_artifact_to_disk(self):
        """Verify record_session saves last_artifact into registry JSON file."""
        art = {
            "filename": "chart.png",
            "path": "/tmp/outbox/artifacts/chart.png",
            "size_bytes": 2048,
        }
        self.router.record_session(
            session_key="C111:1700.0001",
            conversation_id="conv_test_1",
            target_cwd=self.temp_dir,
            channel="C111",
            thread_ts="1700.0001",
            last_artifact=art,
        )

        recorded = self.router.get_recorded_session("C111:1700.0001")
        self.assertIsNotNone(recorded)
        self.assertEqual(recorded["last_artifact"], art)

        # Confirm persisted on disk
        with open(self.registry_file, "r") as f:
            disk_data = json.load(f)
        self.assertEqual(disk_data["C111:1700.0001"]["last_artifact"], art)

    async def test_get_or_create_rehydrates_last_artifact(self):
        """Verify get_or_create rehydrates saved last_artifact onto newly instantiated SessionHandle."""
        art = {
            "filename": "output.pdf",
            "path": "/tmp/outbox/artifacts/output.pdf",
            "absolute_path": "/tmp/outbox/artifacts/output.pdf",
            "size_bytes": 4096,
        }
        # Pre-seed registry
        self.router.record_session(
            session_key="C222:1700.0002",
            conversation_id="conv_test_2",
            target_cwd=self.temp_dir,
            channel="C222",
            thread_ts="1700.0002",
            last_artifact=art,
        )

        # Clear in-memory sessions if any
        self.router.sessions.clear()

        session, is_new = await self.router.get_or_create(channel="C222", thread_ts="1700.0002")
        self.assertTrue(is_new)
        self.assertEqual(session.last_artifact, art)
        self.assertIn(art, session.artifacts)
        self.assertEqual(session.last_artifact_path, "/tmp/outbox/artifacts/output.pdf")


class TestHarnessArtifactIntegration(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying DualPaneHarness artifact stage auto-binding and share_artifact helper."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_harness_artifacts_")
        self.outbox_dir = os.path.join(self.temp_dir, "outbox")
        self.outbox_mgr = OutboxManager(root_dir=self.outbox_dir)
        self.mock_herdr = AsyncMock()
        self.mock_herdr.run_cli.return_value = (0, json.dumps({"result": {"workspace_id": "ws_1"}}), "")
        self.router = SessionRouter(
            default_target_cwd=self.temp_dir,
            driver_factory=lambda cwd, extra_args=None: MagicMock(conversation_id="conv_harness"),
        )
        self.harness = DualPaneHarness(
            herdr_client=self.mock_herdr,
            default_cwd=self.temp_dir,
            outbox=self.outbox_mgr,
            router=self.router,
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_stage_artifact_auto_binds_to_session(self):
        """Verify harness.stage_artifact automatically calls session.record_artifact."""
        session, _ = await self.router.get_or_create(channel="C_HARN", thread_ts="1800.0001")
        session_id = session.session_id

        payload = b"Harness stage artifact payload"
        meta = self.harness.stage_artifact(
            session_id=session_id,
            content_or_path=payload,
            filename="result.txt",
            channel="C_HARN",
            thread_ts="1800.0001",
        )

        self.assertIsNotNone(session.last_artifact)
        self.assertEqual(session.last_artifact["filename"], "result.txt")
        self.assertEqual(session.last_artifact["artifact_id"], meta.artifact_id)
        self.assertEqual(session.last_artifact_path, meta.absolute_path)

    async def test_harness_share_artifact_helper(self):
        """Verify harness.share_artifact delegates to golem.outbox.share_artifact with session context."""
        session, _ = await self.router.get_or_create(channel="C_HARN2", thread_ts="1800.0002")
        session_id = session.session_id

        meta = self.harness.stage_artifact(
            session_id=session_id,
            content_or_path=b"Test payload",
            filename="share_me.txt",
            channel="C_HARN2",
            thread_ts="1800.0002",
        )

        res = await self.harness.share_artifact(
            session_id=session_id,
            path=None,  # Omitted path, resolves to session last_artifact
            channel="C_HARN2",
            thread_ts="1800.0002",
        )

        self.assertTrue(res["ok"])
        self.assertEqual(res["filename"], "share_me.txt")
        self.assertEqual(res["artifact_id"], meta.artifact_id)


class TestShareArtifactResolutionAndMetadata(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying share_artifact resolution, contract schema, and sidecars."""

    def setUp(self):
        self._prev_token = CURRENT_SESSION.set(None)
        self.temp_dir = tempfile.mkdtemp(prefix="test_share_art_")
        self.mgr = OutboxManager(root_dir=self.temp_dir)
        self.session = SessionHandle(
            session_id="sess_share_01",
            channel="C_SHARE",
            thread_ts="1900.0001",
            target_cwd=self.temp_dir,
        )

    def tearDown(self):
        CURRENT_SESSION.reset(self._prev_token)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_share_artifact_with_explicit_path_metadata_schema(self):
        """Verify comprehensive metadata schema contract when sharing by explicit path."""
        meta = self.mgr.stage_artifact(
            session_id="sess_share_01",
            content_or_path=b"Explicit path content",
            filename="explicit.txt",
        )

        res = await share_artifact(
            path=meta.absolute_path,
            channel="C_SHARE",
            thread_ts="1900.0001",
            title="Explicit Title",
            session=self.session,
            outbox_manager=self.mgr,
        )

        # Verify all contract keys
        required_keys = [
            "ok", "path", "absolute_path", "relative_path", "filename",
            "size_bytes", "size", "mime_type", "sha256", "artifact_id",
            "session_id", "shared", "shared_at", "channel", "thread_ts",
            "title", "upload_metadata", "slack_response", "directive", "file",
        ]
        for key in required_keys:
            self.assertIn(key, res, f"Missing required contract key: {key}")

        self.assertTrue(res["ok"])
        self.assertEqual(res["filename"], "explicit.txt")
        self.assertEqual(res["title"], "Explicit Title")
        self.assertEqual(res["directive"], f"[SEND_FILE: {meta.absolute_path}]")
        self.assertEqual(res["channel"], "C_SHARE")
        self.assertEqual(res["thread_ts"], "1900.0001")

    async def test_share_artifact_omitted_path_resolves_session_last_artifact(self):
        """Verify omitted path automatically resolves session.get_last_artifact()."""
        meta = self.mgr.stage_artifact(
            session_id="sess_share_01",
            content_or_path=b"Auto resolved content",
            filename="autoresolved.txt",
        )
        self.session.record_artifact(meta)

        res = await share_artifact(
            path=None,
            session=self.session,
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertEqual(res["filename"], "autoresolved.txt")
        self.assertEqual(res["artifact_id"], meta.artifact_id)

    async def test_share_artifact_omitted_path_resolves_contextvar_session(self):
        """Verify omitted path automatically resolves active session from CURRENT_SESSION ContextVar."""
        meta = self.mgr.stage_artifact(
            session_id="sess_share_01",
            content_or_path=b"ContextVar resolved content",
            filename="ctxvar.txt",
        )
        self.session.record_artifact(meta)

        token = CURRENT_SESSION.set(self.session)
        try:
            res = await share_artifact(
                path=None,
                session=None,  # Will resolve via CURRENT_SESSION.get()
                outbox_manager=self.mgr,
            )
            self.assertTrue(res["ok"])
            self.assertEqual(res["filename"], "ctxvar.txt")
        finally:
            CURRENT_SESSION.reset(token)

    async def test_share_artifact_with_artifact_id_lookup(self):
        """Verify looking up artifact by artifact_id when path is omitted."""
        meta = self.mgr.stage_artifact(
            session_id="sess_share_01",
            content_or_path=b"Artifact ID lookup payload",
            filename="lookup.txt",
        )

        res = await share_artifact(
            path=None,
            artifact_id=meta.artifact_id,
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertEqual(res["filename"], "lookup.txt")
        self.assertEqual(res["artifact_id"], meta.artifact_id)

    async def test_share_artifact_omitted_path_no_session_raises_value_error(self):
        """Verify omitted path without active session raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            await share_artifact(
                path=None,
                session=None,
                outbox_manager=self.mgr,
            )
        self.assertIn("No artifact specified and no active last_artifact found", str(ctx.exception))

    async def test_share_artifact_offline_mode_writes_sidecar_json(self):
        """Verify offline mode (client=None) marks shared=False and writes sidecar metadata."""
        meta = self.mgr.stage_artifact(
            session_id="sess_share_01",
            content_or_path=b"Offline sidecar payload",
            filename="sidecar_test.txt",
        )

        res = await share_artifact(
            path=meta.absolute_path,
            client=None,
            channel="C_OFFLINE",
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertFalse(res["shared"])

        # Check sidecar JSON was created in meta/
        meta_dir = Path(self.temp_dir) / "meta"
        sidecars = list(meta_dir.glob("*.json"))
        self.assertGreater(len(sidecars), 0)

    async def test_share_artifact_with_client_upload_success(self):
        """Verify sharing with mock Slack client uploads via files_upload_v2 and marks shared=True."""
        meta = self.mgr.stage_artifact(
            session_id="sess_share_01",
            content_or_path=b"Upload payload",
            filename="uploaded.png",
        )

        mock_client = AsyncMock()
        mock_client.files_upload_v2.return_value = {
            "ok": True,
            "file": {"id": "F12345", "name": "uploaded.png"},
        }

        res = await share_artifact(
            path=meta.absolute_path,
            client=mock_client,
            channel="C_UPLOAD",
            thread_ts="1900.0005",
            title="My Upload",
            outbox_manager=self.mgr,
        )

        self.assertTrue(res["ok"])
        self.assertTrue(res["shared"])
        self.assertEqual(res["upload_metadata"]["file"]["id"], "F12345")
        mock_client.files_upload_v2.assert_called_once()


class TestIngressShareArtifactToolIntegration(unittest.IsolatedAsyncioTestCase):
    """Test suite verifying IngressGateway share_artifact tool declaration, dispatch, and prompt hydration."""

    def test_share_artifact_tool_declaration_schema(self):
        """Verify SHARE_ARTIFACT_TOOL_DECLARATION schema parameters."""
        decl = SHARE_ARTIFACT_TOOL_DECLARATION
        self.assertEqual(decl["name"], "share_artifact")
        self.assertIn("outbox", decl["description"].lower())
        props = decl["parameters"]["properties"]
        self.assertIn("path", props)
        self.assertIn("artifact_id", props)
        self.assertIn("title", props)

    def test_get_mouth_tools_declaration_includes_share_artifact(self):
        """Verify get_mouth_tools_declaration includes SHARE_ARTIFACT_TOOL_DECLARATION."""
        tools = get_mouth_tools_declaration()
        all_funcs = []
        for tool_group in tools:
            for fn in tool_group.get("functionDeclarations", []):
                all_funcs.append(fn.get("name"))

        self.assertIn("share_artifact", all_funcs)
        self.assertIn("dispatch_task", all_funcs)
        self.assertIn("check_eyes", all_funcs)
        self.assertIn("check_nose", all_funcs)

    async def test_prompt_hydration_with_active_artifact(self):
        """Verify prompt hydration prepends [Active Session Artifact: ...] when last_artifact is present."""
        temp_dir = tempfile.mkdtemp(prefix="test_prompt_hydration_")
        try:
            router = SessionRouter(default_target_cwd=temp_dir)
            gateway = IngressGateway(router=router, bot_user_id="U_BOT")
            session = SessionHandle(
                session_id="sess_prompt_01",
                channel="C_PROMPT",
                thread_ts="2000.0001",
                target_cwd=temp_dir,
            )
            session.record_artifact({
                "filename": "chart.png",
                "path": "/tmp/outbox/artifacts/chart.png",
                "absolute_path": "/tmp/outbox/artifacts/chart.png",
            })

            # Mock router to return our session
            router.get_or_create = AsyncMock(return_value=(session, False))

            captured_prompt = None

            async def mock_send_user_message(prompt, **kwargs):
                nonlocal captured_prompt
                captured_prompt = prompt
                if False:
                    yield None

            session.send_user_message = mock_send_user_message

            # Invoke handle_message
            event = {
                "type": "message",
                "text": "Please share the latest chart",
                "channel": "C_PROMPT",
                "thread_ts": "2000.0001",
                "ts": "2000.0002",
                "user": "U12345",
            }
            mock_client = AsyncMock()
            mock_client.conversations_replies.return_value = {"messages": []}
            await gateway.handle_message(
                event=event,
                client=mock_client,
            )

            self.assertIsNotNone(captured_prompt)
            self.assertIn("[Active Session Artifact: chart.png at /tmp/outbox/artifacts/chart.png]", captured_prompt)
            self.assertIn("Please share the latest chart", captured_prompt)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    async def test_ingress_execute_share_artifact_success(self):
        """Verify IngressGateway._execute_share_artifact executes correctly."""
        temp_dir = tempfile.mkdtemp(prefix="test_ing_exec_")
        try:
            outbox_dir = os.path.join(temp_dir, "outbox")
            mgr = OutboxManager(root_dir=outbox_dir)
            meta = mgr.stage_artifact(
                session_id="sess_exec_01",
                content_or_path=b"Ingress exec payload",
                filename="exec_file.txt",
            )

            session = SessionHandle(
                session_id="sess_exec_01",
                channel="C_EXEC",
                thread_ts="2100.0001",
                target_cwd=temp_dir,
            )
            session.record_artifact(meta)

            router = SessionRouter(default_target_cwd=temp_dir)
            router.harness = MagicMock(outbox=mgr)
            gateway = IngressGateway(router=router, bot_user_id="U_BOT")

            res = await gateway._execute_share_artifact(
                path=meta.absolute_path,
                session=session,
                channel="C_EXEC",
                thread_ts="2100.0001",
                title="Executed Share",
            )

            self.assertTrue(res["ok"])
            self.assertEqual(res["filename"], "exec_file.txt")
            self.assertEqual(res["artifact_id"], meta.artifact_id)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    async def test_ingress_execute_share_artifact_failure(self):
        """Verify IngressGateway._execute_share_artifact raises SandboxSecurityError when accessing outside sandbox."""
        temp_dir = tempfile.mkdtemp(prefix="test_ing_fail_")
        try:
            router = SessionRouter(default_target_cwd=temp_dir)
            gateway = IngressGateway(router=router, bot_user_id="U_BOT")
            session = SessionHandle(
                session_id="sess_fail_01",
                channel="C_FAIL",
                thread_ts="2200.0001",
                target_cwd=temp_dir,
            )

            # Share non-existent file outside sandbox
            with self.assertRaises(SandboxSecurityError):
                await gateway._execute_share_artifact(
                    path="/etc/passwd",
                    session=session,
                )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
