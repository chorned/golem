#!/usr/bin/env python3
"""
tests/test_adversarial_stage4_channel_containment.py
Adversarial stress-testing suite for Stage 4: Channel-Context Artifact Sandboxing & Transport Containment.

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
from golem.session import SessionHandle
from golem.file_transfer import resolve_local_file_path


class TestAdversarialOutboxSandboxing(unittest.TestCase):
    """Adversarial stress testing of assert_sandboxed_path."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="adv_stage4_outbox_")
        self.mgr = OutboxManager(root_dir=self.temp_dir)
        self.sandbox_root = Path(self.temp_dir).resolve()

        # Create a legitimate artifact file inside the sandbox
        self.legit_dir = self.sandbox_root / "artifacts"
        self.legit_dir.mkdir(parents=True, exist_ok=True)
        self.legit_file = self.legit_dir / "valid.txt"
        self.legit_file.write_text("Legitimate payload")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_null_byte_injection_vectors(self):
        """Verify null byte injection vectors are strictly rejected with SandboxSecurityError."""
        attack_vectors = [
            "artifacts/valid.txt\0.jpg",
            "\0/etc/passwd",
            "artifacts/output.pdf\0",
            f"{self.legit_file}\0/something",
            "\x00/var/log/syslog",
        ]
        for vector in attack_vectors:
            with self.subTest(vector=vector):
                with self.assertRaises(SandboxSecurityError):
                    assert_sandboxed_path(vector, sandbox_root=self.sandbox_root)

    def test_file_uri_stripping_and_containment(self):
        """Verify file:// URI prefix is stripped and strictly sandbox-enforced."""
        # 1. Valid file:// pointing inside sandbox
        valid_uri = f"file://{self.legit_file}"
        resolved = assert_sandboxed_path(valid_uri, sandbox_root=self.sandbox_root)
        self.assertEqual(resolved, self.legit_file)

        # 2. Malicious file:// pointing outside sandbox
        malicious_uris = [
            "file:///etc/passwd",
            "file:///etc/shadow",
            "file:///var/log/syslog",
            "file://../../etc/hosts",
            f"file://{self.sandbox_root}/../../etc/passwd",
        ]
        for uri in malicious_uris:
            with self.subTest(uri=uri):
                with self.assertRaises(SandboxSecurityError):
                    assert_sandboxed_path(uri, sandbox_root=self.sandbox_root)

    def test_tilde_expansion_and_escape_vectors(self):
        """Verify tilde paths inside sandbox resolve while traversal escapes are blocked."""
        # Tilde escape vectors
        escapes = [
            "~/../../etc/passwd",
            "~/../../../etc/shadow",
            "~root/secret.pem",
            "~/../../var/log/system.log",
        ]
        for vector in escapes:
            with self.subTest(vector=vector):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(vector, sandbox_root=self.sandbox_root)

    def test_windows_drive_letter_vectors(self):
        """Verify Windows drive letter patterns strictly raise SandboxSecurityError."""
        win_vectors = [
            "C:\\Windows\\System32\\cmd.exe",
            "D:/secrets/key.pem",
            "C:/payload.exe",
            "c:\\test\\file.txt",
            "X:/data/sensitive.db",
            "c:/users/admin/credentials",
        ]
        for vector in win_vectors:
            with self.subTest(vector=vector):
                with self.assertRaises(SandboxSecurityError):
                    assert_sandboxed_path(vector, sandbox_root=self.sandbox_root)

    def test_backslash_normalization_and_traversals(self):
        """Verify backslash traversal vectors strictly raise SandboxSecurityError."""
        traversals = [
            "..\\..\\etc\\passwd",
            "artifacts\\..\\..\\..\\etc\\shadow",
            "..\\..\\..\\var\\log",
            ".\\..\\..\\etc\\hosts",
        ]
        for vector in traversals:
            with self.subTest(vector=vector):
                with self.assertRaises(SandboxSecurityError):
                    assert_sandboxed_path(vector, sandbox_root=self.sandbox_root)

    def test_symlink_escape_vectors(self):
        """Verify symlinks pointing outside the sandbox root are strictly rejected."""
        outside_file = Path(self.temp_dir).parent / "outside_target.txt"
        outside_file.write_text("Secret outside contents")

        symlink_path = self.legit_dir / "evil_symlink.txt"
        try:
            symlink_path.symlink_to(outside_file)
            with self.assertRaises((SandboxSecurityError, PermissionError)):
                assert_sandboxed_path(symlink_path, sandbox_root=self.sandbox_root)
        finally:
            if symlink_path.is_symlink() or symlink_path.exists():
                symlink_path.unlink(missing_ok=True)
            outside_file.unlink(missing_ok=True)

    def test_symlink_internal_is_permitted(self):
        """Verify internal symlinks pointing within the sandbox are permitted."""
        internal_symlink = self.legit_dir / "internal_symlink.txt"
        try:
            internal_symlink.symlink_to(self.legit_file)
            resolved = assert_sandboxed_path(internal_symlink, sandbox_root=self.sandbox_root)
            self.assertEqual(resolved, self.legit_file.resolve())
        finally:
            internal_symlink.unlink(missing_ok=True)


class TestAdversarialShareArtifactContainment(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress-testing of share_artifact path validation and session context."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="adv_share_art_")
        self.mgr = OutboxManager(root_dir=self.temp_dir)
        self.session = SessionHandle(
            session_id="adv_sess_01",
            channel="C_ADV",
            thread_ts="3000.0001",
            target_cwd=self.temp_dir,
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_direct_share_artifact_adversarial_paths(self):
        """Verify malicious paths in share_artifact raise SandboxSecurityError."""
        malicious_inputs = [
            "../../etc/passwd",
            "/etc/hosts",
            "file:///etc/shadow",
            "C:\\Windows\\win.ini",
            "data.csv\0/etc/passwd",
            "artifacts/../../../../var/log/syslog",
        ]
        for path in malicious_inputs:
            with self.subTest(path=path):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    await share_artifact(
                        path=path,
                        session=self.session,
                        outbox_manager=self.mgr,
                    )

    async def test_session_last_artifact_malicious_path_contained(self):
        """Verify share_artifact rejects malicious path even if injected into session last_artifact."""
        self.session.record_artifact({
            "filename": "poisoned.txt",
            "path": "/etc/passwd",
            "absolute_path": "/etc/passwd",
        })

        with self.assertRaises(SandboxSecurityError):
            await share_artifact(
                path=None,  # Resolves session.last_artifact pointing to /etc/passwd
                session=self.session,
                outbox_manager=self.mgr,
            )

    async def test_nonexistent_file_in_sandbox_raises_file_not_found(self):
        """Verify non-existent file inside sandbox raises FileNotFoundError, not security error."""
        nonexistent = Path(self.temp_dir) / "artifacts" / "does_not_exist.txt"
        with self.assertRaises(FileNotFoundError):
            await share_artifact(
                path=str(nonexistent),
                session=self.session,
                outbox_manager=self.mgr,
            )


class TestAdversarialFileTransferContainment(unittest.TestCase):
    """Adversarial testing of candidate path containment in resolve_local_file_path."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="adv_ft_")
        self.active_cwd = os.path.join(self.temp_dir, "workspace")
        os.makedirs(self.active_cwd, exist_ok=True)

        self.valid_file = os.path.join(self.active_cwd, "test.py")
        with open(self.valid_file, "w") as f:
            f.write("print('hello')")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_arbitrary_system_paths_rejected(self):
        """Verify arbitrary system paths return None."""
        forbidden_candidates = [
            "/etc/passwd",
            "/etc/shadow",
            "/var/log/system.log",
            "~/.ssh/id_rsa",
            "~/.bash_history",
        ]
        for candidate in forbidden_candidates:
            with self.subTest(candidate=candidate):
                resolved = resolve_local_file_path(candidate, active_cwd=self.active_cwd)
                self.assertIsNone(resolved)

    def test_relative_directory_traversals_rejected(self):
        """Verify relative traversal paths return None."""
        traversals = [
            "../../../../etc/passwd",
            "../outside.txt",
            "../../..",
            "sub/../../../../etc/hosts",
        ]
        for candidate in traversals:
            with self.subTest(candidate=candidate):
                resolved = resolve_local_file_path(candidate, active_cwd=self.active_cwd)
                self.assertIsNone(resolved)

    def test_valid_file_in_active_cwd_resolved(self):
        """Verify valid candidate path within active_cwd resolves properly."""
        resolved = resolve_local_file_path("test.py", active_cwd=self.active_cwd)
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved, str(Path(self.valid_file).resolve()))

    def test_tmp_host_file_exfiltration_strictly_rejected(self):
        """Verify arbitrary files in /tmp cannot be exfiltrated via resolve_local_file_path."""
        secret_file = "/tmp/test_containment_secret.txt"
        with open(secret_file, "w") as f:
            f.write("SECRET_EXFILTRATION_DATA")
        try:
            # Rejection with active_cwd set to workspace
            resolved = resolve_local_file_path(secret_file, active_cwd=self.active_cwd)
            self.assertIsNone(resolved)
            # Rejection with active_cwd = None
            resolved_none = resolve_local_file_path(secret_file, active_cwd=None)
            self.assertIsNone(resolved_none)
        finally:
            if os.path.exists(secret_file):
                os.remove(secret_file)

    def test_known_hosts_outbox_and_share_artifact_rejected(self):
        """Verify known_hosts and its variants strictly raise SandboxSecurityError in outbox."""
        kh_variants = [
            "known_hosts",
            "known_hosts.old",
            "known_hosts.bak",
            "known_hosts.tmp",
        ]
        sandbox_dir = Path(self.temp_dir)
        for name in kh_variants:
            target = sandbox_dir / "artifacts" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5...")
            with self.subTest(name=name):
                with self.assertRaises(SandboxSecurityError):
                    assert_sandboxed_path(f"artifacts/{name}", sandbox_root=sandbox_dir)


class TestAdversarialCrossChannelParity(unittest.IsolatedAsyncioTestCase):
    """Verify cross-channel parity between SlackClient and duck-typed BlueBubblesClient."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="adv_parity_")
        self.mgr = OutboxManager(root_dir=self.temp_dir)
        self.meta = self.mgr.stage_artifact(
            session_id="parity_sess_01",
            content_or_path=b"Cross channel payload",
            filename="parity.txt",
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_slack_and_bluebubbles_client_parity(self):
        """Verify both Slack client (files_upload_v2) and BlueBubbles client (upload_file) share artifacts cleanly."""
        # 1. Slack client mock
        slack_client = AsyncMock()
        slack_client.files_upload_v2.return_value = {
            "ok": True,
            "file": {"id": "F_SLACK", "name": "parity.txt"},
        }

        slack_res = await share_artifact(
            path=self.meta.absolute_path,
            client=slack_client,
            channel="C_SLACK",
            thread_ts="4000.0001",
            outbox_manager=self.mgr,
        )

        # 2. BlueBubbles client mock (duck-typed with upload_file)
        bb_client = AsyncMock()
        bb_client.upload_file.return_value = {
            "ok": True,
            "file": {"id": "F_BB", "name": "parity.txt"},
        }
        # Ensure no files_upload_v2
        del bb_client.files_upload_v2

        bb_res = await share_artifact(
            path=self.meta.absolute_path,
            client=bb_client,
            channel="chat_guid_123",
            outbox_manager=self.mgr,
        )

        # Verify parity of metadata schemas
        self.assertTrue(slack_res["ok"])
        self.assertTrue(bb_res["ok"])
        self.assertTrue(slack_res["shared"])
        self.assertTrue(bb_res["shared"])
        self.assertEqual(set(slack_res.keys()), set(bb_res.keys()))
        self.assertEqual(slack_res["filename"], bb_res["filename"])
        self.assertEqual(slack_res["sha256"], bb_res["sha256"])

    async def test_client_upload_failure_resilience(self):
        """Verify client upload exceptions raise FleetTransferError / RuntimeError without corrupting outbox."""
        failing_client = AsyncMock()
        failing_client.files_upload_v2.side_effect = RuntimeError("Slack API network connection dropped")

        with self.assertRaises(RuntimeError):
            await share_artifact(
                path=self.meta.absolute_path,
                client=failing_client,
                channel="C_FAIL",
                thread_ts="4000.0002",
                outbox_manager=self.mgr,
            )

        # Verify local artifact remains intact in outbox
        self.assertTrue(Path(self.meta.absolute_path).is_file())


if __name__ == "__main__":
    unittest.main()
