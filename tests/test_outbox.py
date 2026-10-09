#!/usr/bin/env python3
"""
tests/test_outbox.py
Unit tests for the Golem Outbox Contract, Remote Fleet Staging & Sandboxed Artifact Sharing.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import sys
import json
import time
import shutil
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from golem.outbox import (
    ArtifactMetadata,
    SandboxSecurityError,
    FleetTransferError,
    FleetNodeUnreachableError,
    calculate_sha256,
    assert_sandboxed_path,
    OutboxManager,
    share_artifact,
)


class TestOutboxContract(unittest.TestCase):
    """Test suite verifying outbox structure, metadata, and traversal defenses."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_golem_outbox_")
        self.mgr = OutboxManager(root_dir=self.temp_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_directory_hierarchy_created(self):
        """Verify outbox directory tree is fully initialized on creation."""
        self.assertTrue((Path(self.temp_dir) / "artifacts").is_dir())
        self.assertTrue((Path(self.temp_dir) / "staging").is_dir())
        self.assertTrue((Path(self.temp_dir) / "downloads").is_dir())
        self.assertTrue((Path(self.temp_dir) / "meta").is_dir())

    def test_stage_artifact_bytes(self):
        """Verify staging binary payload generates artifact and sidecar metadata."""
        content = b"Hello, Carl! Outbox test payload."
        meta = self.mgr.stage_artifact(
            session_id="sess_100",
            content_or_path=content,
            filename="output.txt",
            tags=["test", "bytes"],
        )

        self.assertEqual(meta.filename, "output.txt")
        self.assertEqual(meta.size_bytes, len(content))
        self.assertTrue(Path(meta.absolute_path).is_file())
        self.assertIn("sess_100", meta.relative_path)
        self.assertEqual(meta.origin_host, "local")

        # Verify sidecar JSON
        meta_file = Path(self.temp_dir) / "meta" / f"{meta.artifact_id}.json"
        self.assertTrue(meta_file.is_file())
        with open(meta_file, "r") as f:
            data = json.load(f)
        self.assertEqual(data["artifact_id"], meta.artifact_id)
        self.assertEqual(data["sha256"], meta.sha256)

    def test_stage_artifact_file(self):
        """Verify staging an existing file copies it into session outbox."""
        src_file = Path(self.temp_dir) / "source_doc.md"
        src_file.write_text("# Test Document\nLine 2")

        meta = self.mgr.stage_artifact(
            session_id="sess_200",
            content_or_path=src_file,
            filename="doc.md",
        )

        self.assertEqual(meta.filename, "doc.md")
        self.assertTrue(Path(meta.absolute_path).is_file())
        self.assertEqual(Path(meta.absolute_path).read_text(), "# Test Document\nLine 2")

    def test_assert_sandboxed_path_valid(self):
        """Verify valid path inside outbox resolves successfully."""
        valid_file = Path(self.temp_dir) / "artifacts" / "test.txt"
        valid_file.write_text("ok")

        resolved = assert_sandboxed_path(valid_file, sandbox_root=self.temp_dir)
        self.assertEqual(resolved, valid_file.resolve())

    def test_assert_sandboxed_path_relative_escape(self):
        """Verify relative traversal path (..) raises PermissionError."""
        escape_path = Path(self.temp_dir) / "artifacts" / ".." / ".." / "etc" / "passwd"
        with self.assertRaises(PermissionError):
            assert_sandboxed_path(escape_path, sandbox_root=self.temp_dir)

    def test_assert_sandboxed_path_absolute_escape(self):
        """Verify absolute path outside sandbox raises PermissionError."""
        with self.assertRaises(PermissionError):
            assert_sandboxed_path("/tmp/outside_file.txt", sandbox_root=self.temp_dir)

    def test_assert_sandboxed_path_symlink_escape(self):
        """Verify symlink pointing outside sandbox raises PermissionError."""
        outside_dir = tempfile.mkdtemp(prefix="outside_")
        outside_file = Path(outside_dir) / "secret.txt"
        outside_file.write_text("secret_token")

        symlink_path = Path(self.temp_dir) / "artifacts" / "symlink_test.txt"
        try:
            os.symlink(outside_file, symlink_path)
            with self.assertRaises(PermissionError):
                assert_sandboxed_path(symlink_path, sandbox_root=self.temp_dir)
        finally:
            shutil.rmtree(outside_dir, ignore_errors=True)

    def test_assert_sandboxed_path_sensitive_file(self):
        """Verify sensitive filenames (.env, id_rsa, credentials.json) raise PermissionError."""
        sensitive_file = Path(self.temp_dir) / "artifacts" / ".env"
        sensitive_file.write_text("API_KEY=foo")

        with self.assertRaises(PermissionError):
            assert_sandboxed_path(sensitive_file, sandbox_root=self.temp_dir)


class TestRemoteFleetStaging(unittest.IsolatedAsyncioTestCase):
    """Test suite for SSH/SCP cross-machine file staging."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_golem_staging_")
        self.mgr = OutboxManager(root_dir=self.temp_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_stage_from_remote_success(self):
        """Verify successful remote SCP staging creates inbound file and metadata."""
        async def mock_subprocess_exec(*args, **kwargs):
            # args[-1] is the destination file (.tmp)
            dest_tmp = args[-1]
            Path(dest_tmp).write_text("remote GPU benchmark results")
            proc = AsyncMock()
            proc.returncode = 0
            proc.communicate = AsyncMock(return_value=(b"", b""))
            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess_exec):
            meta = await self.mgr.stage_from_remote(
                machine="desktop-pc",
                remote_path="C:\\logs\\benchmark.txt",
                session_id="gpu_sess_1",
            )

            self.assertTrue(Path(meta.absolute_path).is_file())
            self.assertEqual(meta.origin_host, "desktop-pc")
            self.assertIn("staging/desktop-pc/inbound", meta.relative_path)
            self.assertEqual(meta.size_bytes, len("remote GPU benchmark results"))

    async def test_stage_from_remote_laptop_timeout(self):
        """Verify personal laptop timeout raises FleetNodeUnreachableError."""
        async def mock_subprocess_exec(*args, **kwargs):
            proc = AsyncMock()
            proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError())
            proc.kill = MagicMock()
            proc.wait = AsyncMock()
            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess_exec):
            with self.assertRaises(FleetNodeUnreachableError) as ctx:
                await self.mgr.stage_from_remote(
                    machine="personal-laptop",
                    remote_path="/Users/carl/notes.md",
                    timeout_seconds=0.1,
                )
            self.assertIn("asleep", str(ctx.exception).lower())

    async def test_stage_from_remote_empty_file_cleans_up(self):
        """Verify 0-byte fetched file cleans up .tmp and raises error."""
        async def mock_subprocess_exec(*args, **kwargs):
            dest_tmp = args[-1]
            Path(dest_tmp).touch()  # 0 bytes
            proc = AsyncMock()
            proc.returncode = 0
            proc.communicate = AsyncMock(return_value=(b"", b""))
            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess_exec):
            with self.assertRaises(FleetTransferError):
                await self.mgr.stage_from_remote(
                    machine="oci-machine",
                    remote_path="/var/log/empty.log",
                )


class TestShareArtifact(unittest.IsolatedAsyncioTestCase):
    """Test suite for sandboxed share_artifact."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_golem_share_")
        self.mgr = OutboxManager(root_dir=self.temp_dir)
        self.mock_client = AsyncMock()
        self.mock_client.files_upload_v2.return_value = {"ok": True, "file": {"id": "F123"}}

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_share_artifact_inside_outbox(self):
        """Verify artifact inside outbox uploads to Slack and updates metadata."""
        meta = self.mgr.stage_artifact(
            session_id="sess_share",
            content_or_path=b"Chart data",
            filename="chart.png",
        )

        resp = await share_artifact(
            path=meta.absolute_path,
            client=self.mock_client,
            channel="C_OUTBOX",
            thread_ts="1234.5678",
            outbox_manager=self.mgr,
        )

        self.assertEqual(resp["ok"], True)
        self.mock_client.files_upload_v2.assert_called_once()

        # Verify metadata was updated
        updated_meta = self.mgr.get_artifact(meta.artifact_id)
        self.assertIsNotNone(updated_meta)
        self.assertTrue(updated_meta.shared)
        self.assertEqual(updated_meta.channel, "C_OUTBOX")

    async def test_share_artifact_outside_outbox_raises_permission_error(self):
        """Verify sharing file outside outbox strictly raises PermissionError."""
        outside_file = tempfile.NamedTemporaryFile(delete=False)
        outside_file.write(b"outside")
        outside_file.close()

        try:
            with self.assertRaises(PermissionError):
                await share_artifact(
                    path=outside_file.name,
                    client=self.mock_client,
                    channel="C_TEST",
                    outbox_manager=self.mgr,
                )
            self.mock_client.files_upload_v2.assert_not_called()
        finally:
            os.remove(outside_file.name)


if __name__ == "__main__":
    unittest.main()
