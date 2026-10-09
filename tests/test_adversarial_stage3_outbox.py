#!/usr/bin/env python3
"""
tests/test_adversarial_stage3_outbox.py
Adversarial and Empirical Challenge Test Suite for Stage 3 (Outbox Contract,
Path Traversal Sandboxing, and Remote Fleet Staging).

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
from golem.runner import (
    TaskHandle,
    ExecutionHandle,
    RunnerExecutor,
    TerminalValidationError,
    TerminalMismatchError,
    PaneNotFoundError,
)
from golem.harness import OpaquePaneCoordinate


class TestAdversarialPathTraversals(unittest.TestCase):
    """Adversarial stress-testing of path traversal vectors against assert_sandboxed_path."""

    def setUp(self):
        self.sandbox_dir = tempfile.mkdtemp(prefix="adv_sandbox_")
        self.artifacts_dir = Path(self.sandbox_dir) / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.sandbox_dir, ignore_errors=True)

    def test_parent_directory_traversals(self):
        """Verify various relative parent directory traversals strictly raise SandboxSecurityError."""
        attack_vectors = [
            "../../etc/passwd",
            "../outbox/../../etc/hosts",
            "artifacts/../../../../etc/passwd",
            "..//..//etc//passwd",
            "./../../etc/passwd",
            "artifacts/sess_1/../../../../etc/shadow",
            "artifacts/../..",
            "..",
            "../",
            "artifacts/../../",
        ]
        for vector in attack_vectors:
            with self.subTest(vector=vector):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(vector, sandbox_root=self.sandbox_dir)

    def test_root_and_empty_paths(self):
        """Verify root, empty, and whitespace paths strictly raise SandboxSecurityError."""
        for empty in ["", "   ", "\t\n", None]:
            with self.subTest(empty=empty):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(empty, sandbox_root=self.sandbox_dir)

        # Root directory itself escapes sandbox containment
        with self.assertRaises((SandboxSecurityError, PermissionError)):
            assert_sandboxed_path("/", sandbox_root=self.sandbox_dir)

    def test_sibling_directory_prefix_collision(self):
        """
        Verify that a sibling directory sharing the sandbox path as a prefix
        (classic string prefix matching bug) is strictly rejected.
        """
        sibling_dir = self.sandbox_dir + "_fake_sibling"
        os.makedirs(sibling_dir, exist_ok=True)
        try:
            evil_file = Path(sibling_dir) / "evil.txt"
            evil_file.write_text("evil payload")
            with self.assertRaises((SandboxSecurityError, PermissionError)):
                assert_sandboxed_path(evil_file, sandbox_root=self.sandbox_dir)
        finally:
            shutil.rmtree(sibling_dir, ignore_errors=True)

    def test_backslash_and_outside_paths_traversal(self):
        """Verify backslash paths pointing outside sandbox are strictly rejected."""
        vectors = [
            r"/tmp/evil\..\..\secret",
            r"/var/tmp\..\..\secret.log",
        ]
        for vector in vectors:
            with self.subTest(vector=vector):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(vector, sandbox_root=self.sandbox_dir)

    def test_quoted_paths_traversal(self):
        """Verify paths wrapped in quotes are stripped and still rejected."""
        vectors = [
            "'../../etc/passwd'",
            '"../../etc/passwd"',
            "`../../etc/passwd`",
            "<../../etc/passwd>",
        ]
        for vector in vectors:
            with self.subTest(vector=vector):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(vector, sandbox_root=self.sandbox_dir)


class TestAdversarialAbsolutePaths(unittest.TestCase):
    """Adversarial stress-testing of absolute paths outside sandbox."""

    def setUp(self):
        self.sandbox_dir = tempfile.mkdtemp(prefix="adv_sandbox_abs_")

    def tearDown(self):
        shutil.rmtree(self.sandbox_dir, ignore_errors=True)

    def test_system_paths_outside_sandbox(self):
        """Verify system files outside sandbox strictly raise SandboxSecurityError."""
        system_targets = [
            "/etc/passwd",
            "/etc/hosts",
            "/tmp/evil.sh",
            "/var/log/system.log",
            "/bin/sh",
            "/usr/bin/python3",
        ]
        for target in system_targets:
            with self.subTest(target=target):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(target, sandbox_root=self.sandbox_dir)

    def test_ssh_and_user_keys(self):
        """Verify user SSH directories and keys outside sandbox are rejected."""
        ssh_targets = [
            os.path.expanduser("~/.ssh/id_rsa"),
            os.path.expanduser("~/.ssh/id_ed25519"),
            os.path.expanduser("~/.ssh/config"),
        ]
        for target in ssh_targets:
            with self.subTest(target=target):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(target, sandbox_root=self.sandbox_dir)

    def test_nonexistent_outside_paths(self):
        """Verify non-existent paths outside sandbox are also strictly rejected."""
        nonexistent_targets = [
            "/nonexistent/directory/outside/sandbox/file.txt",
            "/tmp/fake_dir_12345/sub/file.bin",
        ]
        for target in nonexistent_targets:
            with self.subTest(target=target):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(target, sandbox_root=self.sandbox_dir)

    def test_windows_drive_paths_blocked(self):
        """Verify Windows absolute drive paths (e.g. C:\\... or c:/...) strictly raise SandboxSecurityError."""
        windows_targets = [
            r"C:\Windows\System32\cmd.exe",
            r"C:\Users\carl\secret.txt",
            "C:/Windows/System32/cmd.exe",
            "c:/users/carl/desktop/data.csv",
            r"D:\Data\output.csv",
            "D:/Data/output.csv",
            r"Z:\remote\payload.bin",
        ]
        for target in windows_targets:
            with self.subTest(target=target):
                with self.assertRaises((SandboxSecurityError, PermissionError)) as ctx:
                    assert_sandboxed_path(target, sandbox_root=self.sandbox_dir)
                self.assertIn("Windows absolute drive paths are not permitted in sandboxed outbox", str(ctx.exception))


class TestAdversarialSymlinkEscapes(unittest.TestCase):
    """Adversarial stress-testing of symlink escapes and bypass attempts."""

    def setUp(self):
        self.sandbox_dir = tempfile.mkdtemp(prefix="adv_sandbox_sym_")
        self.artifacts_dir = Path(self.sandbox_dir) / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.outside_dir = tempfile.mkdtemp(prefix="adv_outside_")

    def tearDown(self):
        shutil.rmtree(self.sandbox_dir, ignore_errors=True)
        shutil.rmtree(self.outside_dir, ignore_errors=True)

    def test_symlink_to_system_file(self):
        """Verify symlink pointing to /etc/hosts is rejected."""
        sym = self.artifacts_dir / "sym_hosts.txt"
        os.symlink("/etc/hosts", sym)
        with self.assertRaises((SandboxSecurityError, PermissionError)):
            assert_sandboxed_path(sym, sandbox_root=self.sandbox_dir)

    def test_symlink_to_outside_file(self):
        """Verify symlink pointing to an arbitrary file outside sandbox is rejected."""
        outside_file = Path(self.outside_dir) / "secret.txt"
        outside_file.write_text("classified")
        sym = self.artifacts_dir / "sym_secret.txt"
        os.symlink(outside_file, sym)

        with self.assertRaises((SandboxSecurityError, PermissionError)):
            assert_sandboxed_path(sym, sandbox_root=self.sandbox_dir)

    def test_directory_symlink_escape(self):
        """Verify directory symlink pointing outside sandbox is rejected."""
        sym_dir = self.artifacts_dir / "sym_dir"
        os.symlink(self.outside_dir, sym_dir)

        payload_file = Path(self.outside_dir) / "payload.txt"
        payload_file.write_text("payload inside outside dir")

        target = sym_dir / "payload.txt"
        with self.assertRaises((SandboxSecurityError, PermissionError)):
            assert_sandboxed_path(target, sandbox_root=self.sandbox_dir)

    def test_multi_hop_symlink_escape(self):
        """Verify chained symlinks pointing outside are rejected."""
        outside_file = Path(self.outside_dir) / "deep_secret.txt"
        outside_file.write_text("deep")

        hop1 = self.artifacts_dir / "hop1.txt"
        hop2 = self.artifacts_dir / "hop2.txt"
        os.symlink(outside_file, hop1)
        os.symlink(hop1, hop2)

        with self.assertRaises((SandboxSecurityError, PermissionError)):
            assert_sandboxed_path(hop2, sandbox_root=self.sandbox_dir)

    def test_dangling_symlink_outside(self):
        """Verify broken / dangling symlink pointing outside is rejected."""
        dangling_sym = self.artifacts_dir / "broken_link.txt"
        os.symlink(Path(self.outside_dir) / "nonexistent.txt", dangling_sym)

        with self.assertRaises((SandboxSecurityError, PermissionError)):
            assert_sandboxed_path(dangling_sym, sandbox_root=self.sandbox_dir)

    def test_innocuous_symlink_to_sensitive_file_inside(self):
        """
        Verify that a symlink named 'innocent.txt' pointing to a sensitive file
        like '.env' inside the sandbox is rejected by post-resolution checks.
        """
        env_file = self.artifacts_dir / ".env"
        env_file.write_text("API_SECRET=supersecret")

        innocent_link = self.artifacts_dir / "innocent.txt"
        os.symlink(env_file, innocent_link)

        with self.assertRaises((SandboxSecurityError, PermissionError)):
            assert_sandboxed_path(innocent_link, sandbox_root=self.sandbox_dir)


class TestAdversarialSensitiveFiles(unittest.TestCase):
    """Adversarial testing of sensitive filenames protection."""

    def setUp(self):
        self.sandbox_dir = tempfile.mkdtemp(prefix="adv_sandbox_sens_")
        self.artifacts_dir = Path(self.sandbox_dir) / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.sandbox_dir, ignore_errors=True)

    def test_sensitive_filenames_inside_sandbox(self):
        """Verify sensitive filenames are rejected even when placed directly in sandbox."""
        sensitive_names = [
            ".env",
            ".env.local",
            ".env.production",
            ".env.staging",
            "id_rsa",
            "id_ed25519",
            "id_ecdsa",
            "id_dsa",
            "credentials.json",
        ]
        for name in sensitive_names:
            target = self.artifacts_dir / name
            target.write_text("sensitive content")
            with self.subTest(name=name):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(target, sandbox_root=self.sandbox_dir)

    def test_case_insensitive_sensitive_files(self):
        """Verify case-insensitivity on sensitive filename protection."""
        variations = [
            ".ENV",
            ".Env.Local",
            "ID_RSA",
            "ID_ED25519",
            "CREDENTIALS.JSON",
        ]
        for name in variations:
            target = self.artifacts_dir / name
            target.write_text("sensitive content")
            with self.subTest(name=name):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    assert_sandboxed_path(target, sandbox_root=self.sandbox_dir)


class TestAdversarialShareArtifact(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress-testing of share_artifact function."""

    def setUp(self):
        self.sandbox_dir = tempfile.mkdtemp(prefix="adv_share_box_")
        self.mgr = OutboxManager(root_dir=self.sandbox_dir)
        self.mock_client = AsyncMock()
        self.mock_client.files_upload_v2.return_value = {"ok": True, "file": {"id": "F_TEST"}}

    def tearDown(self):
        shutil.rmtree(self.sandbox_dir, ignore_errors=True)

    async def test_share_artifact_traversal_rejection(self):
        """Verify share_artifact rejects directory traversal vectors and never calls client."""
        traversal_paths = [
            "../../etc/passwd",
            "/etc/passwd",
            "/tmp/evil.sh",
            "../outside/data.txt",
        ]
        for path in traversal_paths:
            with self.subTest(path=path):
                with self.assertRaises((SandboxSecurityError, PermissionError)):
                    await share_artifact(
                        path=path,
                        client=self.mock_client,
                        channel="C_TEST",
                        outbox_manager=self.mgr,
                    )
        self.mock_client.files_upload_v2.assert_not_called()

    async def test_share_artifact_symlink_rejection(self):
        """Verify share_artifact rejects symlinks pointing outside sandbox."""
        outside_dir = tempfile.mkdtemp(prefix="adv_share_outside_")
        try:
            outside_file = Path(outside_dir) / "stolen.txt"
            outside_file.write_text("stolen")
            sym = Path(self.sandbox_dir) / "artifacts" / "sym_stolen.txt"
            os.symlink(outside_file, sym)

            with self.assertRaises((SandboxSecurityError, PermissionError)):
                await share_artifact(
                    path=sym,
                    client=self.mock_client,
                    channel="C_TEST",
                    outbox_manager=self.mgr,
                )
            self.mock_client.files_upload_v2.assert_not_called()
        finally:
            shutil.rmtree(outside_dir, ignore_errors=True)

    async def test_share_artifact_sensitive_rejection(self):
        """Verify share_artifact rejects sensitive files even inside outbox."""
        sens = Path(self.sandbox_dir) / "artifacts" / ".env"
        sens.write_text("SECRET=1")

        with self.assertRaises((SandboxSecurityError, PermissionError)):
            await share_artifact(
                path=sens,
                client=self.mock_client,
                channel="C_TEST",
                outbox_manager=self.mgr,
            )
        self.mock_client.files_upload_v2.assert_not_called()

    async def test_share_artifact_directory_rejection(self):
        """Verify share_artifact raises FileNotFoundError if path is a directory."""
        dir_path = Path(self.sandbox_dir) / "artifacts"
        with self.assertRaises(FileNotFoundError):
            await share_artifact(
                path=dir_path,
                client=self.mock_client,
                channel="C_TEST",
                outbox_manager=self.mgr,
            )
        self.mock_client.files_upload_v2.assert_not_called()

    async def test_share_artifact_nonexistent_rejection(self):
        """Verify share_artifact raises FileNotFoundError if file does not exist inside outbox."""
        ghost = Path(self.sandbox_dir) / "artifacts" / "ghost_file.txt"
        with self.assertRaises(FileNotFoundError):
            await share_artifact(
                path=ghost,
                client=self.mock_client,
                channel="C_TEST",
                outbox_manager=self.mgr,
            )
        self.mock_client.files_upload_v2.assert_not_called()

    async def test_share_artifact_valid_updates_metadata(self):
        """Verify share_artifact successfully shares valid file and updates metadata."""
        valid_f = Path(self.sandbox_dir) / "artifacts" / "valid_chart.png"
        valid_f.write_bytes(b"\x89PNG\r\n\x1a\n")

        meta = self.mgr.stage_artifact(
            session_id="sess_valid",
            content_or_path=valid_f,
            filename="valid_chart.png",
        )

        resp = await share_artifact(
            path=meta.absolute_path,
            client=self.mock_client,
            channel="C_OUTBOX",
            thread_ts="999.888",
            outbox_manager=self.mgr,
        )

        self.assertEqual(resp["ok"], True)
        self.mock_client.files_upload_v2.assert_called_once()

        updated_meta = self.mgr.get_artifact(meta.artifact_id)
        self.assertIsNotNone(updated_meta)
        self.assertTrue(updated_meta.shared)
        self.assertEqual(updated_meta.channel, "C_OUTBOX")
        self.assertEqual(updated_meta.thread_ts, "999.888")


class TestAdversarialRemoteFleetStaging(unittest.IsolatedAsyncioTestCase):
    """Adversarial testing of remote fleet staging timeouts and .tmp cleanup."""

    def setUp(self):
        self.sandbox_dir = tempfile.mkdtemp(prefix="adv_remote_box_")
        self.mgr = OutboxManager(root_dir=self.sandbox_dir)

    def tearDown(self):
        shutil.rmtree(self.sandbox_dir, ignore_errors=True)

    async def test_laptop_timeout_enforcement_and_connect_timeout_flag(self):
        """
        Verify personal-laptop connect timeout is strictly <= 3.0s,
        passes -o ConnectTimeout=3 to scp, and raises FleetNodeUnreachableError on timeout.
        """
        captured_cmd = None

        async def mock_subprocess_exec(*args, **kwargs):
            nonlocal captured_cmd
            captured_cmd = args
            proc = AsyncMock()
            proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError())
            proc.kill = MagicMock()
            proc.wait = AsyncMock()
            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess_exec):
            with self.assertRaises(FleetNodeUnreachableError) as ctx:
                await self.mgr.stage_from_remote(
                    machine="personal-laptop",
                    remote_path="/Users/carl/benchmark.log",
                )

            # Assert error identifies sleeping workstation
            self.assertIn("asleep", str(ctx.exception).lower())

            # Assert connect timeout flag is strictly <= 3.0s
            self.assertIsNotNone(captured_cmd)
            self.assertIn("-o", captured_cmd)
            ct_flag = [arg for arg in captured_cmd if arg.startswith("ConnectTimeout=")]
            self.assertTrue(len(ct_flag) > 0)
            timeout_val = int(ct_flag[0].split("=")[1])
            self.assertLessEqual(timeout_val, 3)

    async def test_dev_grid_alias_timeout_enforcement(self):
        """Verify dev-grid machine alias also enforces <= 3.0s timeout."""
        captured_cmd = None

        async def mock_subprocess_exec(*args, **kwargs):
            nonlocal captured_cmd
            captured_cmd = args
            proc = AsyncMock()
            proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError())
            proc.kill = MagicMock()
            proc.wait = AsyncMock()
            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess_exec):
            with self.assertRaises(FleetNodeUnreachableError):
                await self.mgr.stage_from_remote(
                    machine="dev-grid",
                    remote_path="/Users/carl/test.log",
                )
            ct_flag = [arg for arg in captured_cmd if arg.startswith("ConnectTimeout=")]
            self.assertTrue(len(ct_flag) > 0)
            self.assertLessEqual(int(ct_flag[0].split("=")[1]), 3)

    async def test_temporary_tmp_file_cleaned_up_on_timeout(self):
        """Verify .tmp file is completely deleted when remote staging times out."""
        async def mock_subprocess_exec(*args, **kwargs):
            dest_tmp = args[-1]
            Path(dest_tmp).write_text("partial timeout buffer")
            proc = AsyncMock()
            proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError())
            proc.kill = MagicMock()
            proc.wait = AsyncMock()
            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess_exec):
            with self.assertRaises(FleetNodeUnreachableError):
                await self.mgr.stage_from_remote(
                    machine="personal-laptop",
                    remote_path="/Users/carl/test.log",
                )
            inbound_dir = self.mgr.get_machine_staging_dir("personal-laptop", "inbound")
            tmp_files = list(inbound_dir.glob("*.tmp"))
            self.assertEqual(len(tmp_files), 0)

    async def test_temporary_tmp_file_cleaned_up_on_nonzero_exit(self):
        """Verify .tmp file is completely deleted when SCP exits with non-zero error."""
        async def mock_subprocess_exec(*args, **kwargs):
            dest_tmp = args[-1]
            Path(dest_tmp).write_text("partial corrupted buffer")
            proc = AsyncMock()
            proc.returncode = 255
            proc.communicate = AsyncMock(return_value=(b"", b"Connection refused"))
            return proc

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess_exec):
            with self.assertRaises(FleetNodeUnreachableError):
                await self.mgr.stage_from_remote(
                    machine="desktop-pc",
                    remote_path="C:\\logs\\error.log",
                )
            inbound_dir = self.mgr.get_machine_staging_dir("desktop-pc", "inbound")
            tmp_files = list(inbound_dir.glob("*.tmp"))
            self.assertEqual(len(tmp_files), 0)

    async def test_temporary_tmp_file_cleaned_up_on_zero_bytes(self):
        """Verify .tmp file is completely deleted when fetched remote file is 0 bytes."""
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
            inbound_dir = self.mgr.get_machine_staging_dir("oci-machine", "inbound")
            tmp_files = list(inbound_dir.glob("*.tmp"))
            self.assertEqual(len(tmp_files), 0)

    async def test_remote_sensitive_file_rejected_before_scp(self):
        """Verify remote sensitive files (.env, id_rsa) are rejected before invoking scp."""
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            with self.assertRaises((SandboxSecurityError, PermissionError)):
                await self.mgr.stage_from_remote(
                    machine="desktop-pc",
                    remote_path="C:\\Users\\carl\\.env",
                )
            mock_exec.assert_not_called()


class TestStage3RunnerAndHarnessCoordination(unittest.IsolatedAsyncioTestCase):
    """Adversarial testing of Stage 3 Runner coordinates, TaskHandle, and pre-write checks."""

    def test_opaque_coordinate_hand_detection(self):
        """Verify RunnerExecutor is_runner_pane correctly discriminates Hand vs Liaison panes."""
        executor = RunnerExecutor()
        # Hand panes
        self.assertTrue(executor.is_runner_pane("w12:p2"))
        self.assertTrue(executor.is_runner_pane("ws-m4-1:p2"))
        self.assertTrue(executor.is_runner_pane("w2J:p3"))
        self.assertTrue(executor.is_runner_pane("runner"))
        self.assertTrue(executor.is_runner_pane("pane2"))
        self.assertTrue(executor.is_runner_pane("w99:p10"))
        self.assertTrue(executor.is_runner_pane("w1:p11"))
        self.assertTrue(executor.is_runner_pane("w1:p19"))
        self.assertTrue(executor.is_runner_pane("w2:p100"))
        self.assertTrue(executor.is_runner_pane("w_test:p2"))
        self.assertTrue(executor.is_runner_pane("w-test:p3"))

        # Liaison panes (Pane 1)
        self.assertFalse(executor.is_runner_pane("w12:p1"))
        self.assertFalse(executor.is_runner_pane("ws-m4-1:p1"))
        self.assertFalse(executor.is_runner_pane("sess1-liaison"))
        self.assertFalse(executor.is_runner_pane("p1"))
        self.assertFalse(executor.is_runner_pane("pane1"))
        self.assertFalse(executor.is_runner_pane("w1:p1"))
        self.assertFalse(executor.is_runner_pane("w2J:p1"))
        self.assertFalse(executor.is_runner_pane("w99:p1"))
        self.assertFalse(executor.is_runner_pane("control"))
        self.assertFalse(executor.is_runner_pane("system"))
        self.assertFalse(executor.is_runner_pane("daemon"))
        self.assertFalse(executor.is_runner_pane(""))
        self.assertFalse(executor.is_runner_pane(None))

    def test_is_runner_pane_multi_digit_indices(self):
        """Verify is_runner_pane matches arbitrary multi-digit pane indices >= 2 and rejects liaison."""
        executor = RunnerExecutor()
        multi_digit_panes = [
            "w99:p10",
            "w1:p11",
            "w1:p19",
            "w2:p100",
            "w10:p25",
            "w1:p2",
            "w2J:p2",
            "w_custom:p10",
            "w-worker:p99",
            "w999:p1000",
        ]
        for pane in multi_digit_panes:
            with self.subTest(pane=pane):
                self.assertTrue(executor.is_runner_pane(pane), f"Failed for {pane}")

        liaison_panes = [
            "w1:p1",
            "w2J:p1",
            "w99:p1",
            "p1",
            "pane1",
            "pane_1",
            "session-liaison",
            "w1:p0",
        ]
        for pane in liaison_panes:
            with self.subTest(pane=pane):
                self.assertFalse(executor.is_runner_pane(pane), f"Failed for {pane}")

    def test_opaque_pane_coordinate_dual_equality(self):
        """Verify OpaquePaneCoordinate matches both opaque coordinate and 'runner'."""
        coord = OpaquePaneCoordinate("ws-test:p2", legacy_label="runner")
        self.assertEqual(coord, "ws-test:p2")
        self.assertEqual(coord, "runner")
        self.assertEqual(str(coord), "ws-test:p2")

    def test_task_handle_field_and_property_sync(self):
        """Verify TaskHandle Stage 3 fields, legacy properties, and dict compatibility."""
        handle = TaskHandle(
            task_id="task_123",
            terminal_id="term_abc",
            opaque_coordinate="w1:p2",
            command="echo hi",
            status="running",
            pid=42,
        )

        self.assertEqual(handle.task_id, "task_123")
        self.assertEqual(handle.terminal_id, "term_abc")
        self.assertEqual(handle.opaque_coordinate, "w1:p2")
        self.assertEqual(handle.pane, "w1:p2")
        self.assertTrue(handle.is_running)
        self.assertFalse(handle.is_completed)

        # Mutate pane and check opaque_coordinate sync
        handle.pane = "w1:p3"
        self.assertEqual(handle.opaque_coordinate, "w1:p3")

        # Mutate exit_code and check returncode sync
        handle.exit_code = 0
        self.assertEqual(handle.returncode, 0)
        handle.status = "completed"
        self.assertTrue(handle.is_completed)

        # Mutate output_tail and check stdout sync
        handle.output_tail = "hello world\n"
        self.assertEqual(handle.stdout, "hello world\n")

        # Dict compatibility
        self.assertEqual(handle["task_id"], "task_123")
        self.assertEqual(handle.get("terminal_id"), "term_abc")
        self.assertIn("command", handle)


if __name__ == "__main__":
    unittest.main()
