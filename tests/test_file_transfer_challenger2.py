#!/usr/bin/env python3
"""
tests/test_file_transfer_challenger2.py
Empirical challenge test suite for Milestone 3 (Fleet Path Resolution & Upload Robustness Challenge):
1. Windows drive paths (C:\\..., C:/..., d:\\...) vs fleet <node>:<path> references.
2. Remote SCP failure simulation (timeout, exit code 1, 255, 0-byte file, spawn error)
   and strict assertion that temporary files are deleted in tempfile.gettempdir().
3. Sensitive credential file exfiltration blocking (.env, id_rsa, id_ed25519, credentials.json),
   including challenge tests for fleet prefixes (hermes:.env, desktop-pc:.env) and symlinks.
4. Simulated Slack API upload errors (HTTP 429, 500, network error) verifying session continuity,
   clean response text delivery, reaction swapping, and zero crashed sessions.
"""

from __future__ import annotations

import os
import sys
import tempfile
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.transport.file_transfer import (
    extract_and_strip_file_directives,
    resolve_local_file_path,
    resolve_file_path,
    fetch_remote_file,
    upload_directive_files,
    WINDOWS_DRIVE_PATTERN,
    SENSITIVE_FILENAME_PATTERNS,
)
from golem.gateway.ingress import IngressGateway
from golem.gateway.router import SessionRouter


class MockSubprocess:
    """Mock subprocess for asyncio.create_subprocess_exec simulating SCP."""

    def __init__(self, returncode: int = 0, stdout: bytes = b"", stderr: bytes = b"", delay: float = 0.0):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr
        self._delay = delay
        self.killed = False

    async def communicate(self):
        if self._delay > 0:
            await asyncio.sleep(self._delay)
        return self._stdout, self._stderr

    def kill(self):
        self.killed = True

    async def wait(self):
        return self.returncode


class SimulatedSlackApiError(Exception):
    """Simulated Slack WebClient error."""

    def __init__(self, status_code: int, error_code: str):
        self.status_code = status_code
        self.response = {"ok": False, "error": error_code}
        super().__init__(f"Slack API error: status {status_code}, error {error_code}")


class TestChallengerWindowsVsFleetPaths(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: Windows drive paths vs fleet <node>:<path> references."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.active_cwd = os.path.join(self.temp_dir, "workspace")
        self.transfers_dir = os.path.join(self.active_cwd, "slack_transfers")
        os.makedirs(self.transfers_dir, exist_ok=True)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_windows_drive_paths_disambiguation(self):
        """Assert that various Windows drive notations are NOT treated as fleet node references."""
        windows_paths = [
            r"C:\Users\Carl\data.csv",
            r"C:/Users/Carl/data.csv",
            r"c:\windows\system32\drivers\etc\hosts",
            r"D:\Projects\benchmark\chart.png",
            r"d:/data/run.log",
            r"Z:\deep\archive.tar.gz",
            r"C:\Program Files\App\output.txt",
        ]
        for win_path in windows_paths:
            self.assertTrue(
                bool(WINDOWS_DRIVE_PATTERN.match(win_path)),
                f"WINDOWS_DRIVE_PATTERN failed to match {win_path}",
            )
            # When resolved locally on macOS/Linux, it must not treat 'C' or 'D' as a fleet node
            resolved, node, is_temp = await resolve_file_path(win_path, self.active_cwd)
            # Since the file does not exist on this machine, it returns None, None, False
            self.assertIsNone(resolved, f"Unexpected resolution for non-existent {win_path}")
            self.assertIsNone(node, f"Unexpected node for Windows drive path {win_path}")
            self.assertFalse(is_temp)

    async def test_fleet_node_with_remote_windows_path(self):
        r"""Assert that <remote_node>:C:\... splits into node and remote Windows path."""
        mock_mesh = MagicMock()
        mock_mesh.resolve_node.return_value = {
            "label": "compute-grid",
            "target_machine": "desktop-pc",
            "ssh_target": "100.80.59.45",
        }

        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_temp = os.path.join(self.temp_dir, "weights.pth")
            with open(mock_temp, "w") as f:
                f.write("weights")
            mock_fetch.return_value = mock_temp

            target_str = r"desktop-pc:C:\models\weights.pth"
            resolved, node, is_temp = await resolve_file_path(target_str, self.active_cwd, fleet_mesh=mock_mesh)

            self.assertEqual(resolved, mock_temp)
            self.assertEqual(node, "compute-grid")
            self.assertTrue(is_temp)
            mock_fetch.assert_called_once_with(
                target="100.80.59.45",
                remote_path=r"C:\models\weights.pth",
                ssh_user=None,
            )

    async def test_malformed_and_boundary_targets(self):
        """Empirically test boundary targets like ':', '::', 'desktop-pc:', ':file.txt'."""
        mock_mesh = MagicMock()
        mock_mesh.resolve_node.side_effect = KeyError("Node not found")

        malformed_targets = [
            ":",
            "::",
            ":/tmp/test.txt",
            "desktop-pc:",
            "   ",
            "",
            "unknown-node:/var/log/syslog",
            "file:without:node:lookup",
        ]
        for target in malformed_targets:
            resolved, node, is_temp = await resolve_file_path(target, self.active_cwd, fleet_mesh=mock_mesh)
            self.assertIsNone(resolved, f"Target {target!r} should not resolve to a file")
            self.assertFalse(is_temp)


class TestChallengerScpFailureAndTempCleanup(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: Remote SCP failure simulation and strict temp file cleanup in /tmp."""

    async def test_scp_exit_code_1_deletes_temp_file(self):
        """Assert temp file is strictly deleted when SCP exits with code 1 (failure)."""
        created_temp = []

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            async def fake_proc(*args, **kwargs):
                dest_file = args[-1]
                created_temp.append(dest_file)
                with open(dest_file, "wb") as f:
                    f.write(b"partial failure data")
                return MockSubprocess(returncode=1, stderr=b"SCP failed: Connection refused")

            mock_exec.side_effect = fake_proc

            res = await fetch_remote_file(target="oci-machine", remote_path="/var/log/app.log")
            self.assertIsNone(res)
            self.assertEqual(len(created_temp), 1)
            self.assertFalse(
                os.path.exists(created_temp[0]),
                f"LEAKED TEMP FILE: {created_temp[0]} exists after exit code 1!",
            )

    async def test_scp_exit_code_255_deletes_temp_file(self):
        """Assert temp file is strictly deleted when SCP exits with code 255 (host unreachable)."""
        created_temp = []

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            async def fake_proc(*args, **kwargs):
                dest_file = args[-1]
                created_temp.append(dest_file)
                with open(dest_file, "wb") as f:
                    f.write(b"garbage")
                return MockSubprocess(returncode=255, stderr=b"ssh: connect to host port 22: Host is down")

            mock_exec.side_effect = fake_proc

            res = await fetch_remote_file(target="personal-laptop", remote_path="benchmark.txt")
            self.assertIsNone(res)
            self.assertEqual(len(created_temp), 1)
            self.assertFalse(
                os.path.exists(created_temp[0]),
                f"LEAKED TEMP FILE: {created_temp[0]} exists after exit code 255!",
            )

    async def test_scp_timeout_deletes_temp_file(self):
        """Assert temp file is strictly deleted when SCP times out."""
        created_temp = []

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            async def fake_proc(*args, **kwargs):
                dest_file = args[-1]
                created_temp.append(dest_file)
                with open(dest_file, "wb") as f:
                    f.write(b"hanging stream data")
                return MockSubprocess(returncode=0, delay=10.0)

            mock_exec.side_effect = fake_proc

            res = await fetch_remote_file(target="dev-grid", remote_path="large_dump.sql", timeout_seconds=0.05)
            self.assertIsNone(res)
            self.assertEqual(len(created_temp), 1)
            self.assertFalse(
                os.path.exists(created_temp[0]),
                f"LEAKED TEMP FILE: {created_temp[0]} exists after timeout!",
            )

    async def test_scp_zero_byte_file_deletes_temp_file(self):
        """Assert temp file is strictly deleted when SCP completes with 0 bytes."""
        created_temp = []

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            async def fake_proc(*args, **kwargs):
                dest_file = args[-1]
                created_temp.append(dest_file)
                # Leaves file at 0 bytes
                return MockSubprocess(returncode=0)

            mock_exec.side_effect = fake_proc

            res = await fetch_remote_file(target="desktop-pc", remote_path="empty_log.txt")
            self.assertIsNone(res)
            self.assertEqual(len(created_temp), 1)
            self.assertFalse(
                os.path.exists(created_temp[0]),
                f"LEAKED TEMP FILE: {created_temp[0]} exists after 0-byte result!",
            )

    async def test_scp_spawn_error_deletes_temp_file(self):
        """Assert temp file is strictly deleted when subprocess creation raises OSError."""
        orig_ntf = tempfile.NamedTemporaryFile
        captured = []

        def wrapped_ntf(*args, **kwargs):
            f = orig_ntf(*args, **kwargs)
            captured.append(f.name)
            return f

        with patch("tempfile.NamedTemporaryFile", side_effect=wrapped_ntf):
            with patch("asyncio.create_subprocess_exec", side_effect=OSError("Exec format error")):
                res = await fetch_remote_file(target="desktop-pc", remote_path="file.txt")
                self.assertIsNone(res)
                self.assertEqual(len(captured), 1)
                self.assertFalse(
                    os.path.exists(captured[0]),
                    f"LEAKED TEMP FILE: {captured[0]} exists after spawn error!",
                )

    async def test_upload_directive_files_strictly_cleans_temp_on_success_and_failure(self):
        """Assert upload_directive_files always deletes temp SCP files in both success and failure cases."""
        mock_mesh = MagicMock()
        mock_mesh.resolve_node.return_value = {
            "label": "compute-grid",
            "target_machine": "desktop-pc",
        }

        # Case 1: Success cleans temp
        temp_success = os.path.join(tempfile.gettempdir(), "challenger_clean_success.png")
        with open(temp_success, "wb") as f:
            f.write(b"data")
        client_ok = MagicMock()
        client_ok.files_upload_v2 = AsyncMock(return_value={"ok": True})
        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = temp_success
            await upload_directive_files(
                client=client_ok,
                channel="C_TEST",
                thread_ts="123",
                targets=["desktop-pc:chart.png"],
                active_cwd="/tmp",
                fleet_mesh=mock_mesh,
            )
            self.assertFalse(os.path.exists(temp_success), "Temp file leaked on upload success!")

        # Case 2: Slack API Exception cleans temp
        temp_fail = os.path.join(tempfile.gettempdir(), "challenger_clean_fail.png")
        with open(temp_fail, "wb") as f:
            f.write(b"data")
        client_fail = MagicMock()
        client_fail.files_upload_v2 = AsyncMock(side_effect=Exception("Slack 500 error"))
        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = temp_fail
            await upload_directive_files(
                client=client_fail,
                channel="C_TEST",
                thread_ts="123",
                targets=["desktop-pc:chart.png"],
                active_cwd="/tmp",
                fleet_mesh=mock_mesh,
            )
            self.assertFalse(os.path.exists(temp_fail), "Temp file leaked on upload failure!")


class TestChallengerSensitiveFileExfiltration(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: Sensitive credential file exfiltration blocking."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.active_cwd = os.path.join(self.temp_dir, "workspace")
        os.makedirs(self.active_cwd, exist_ok=True)
        self.mock_client = MagicMock()
        self.mock_client.files_upload_v2 = AsyncMock(return_value={"ok": True})

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_direct_sensitive_files_blocked(self):
        """Verify baseline protection for direct filenames."""
        direct_targets = [
            ".env",
            ".env.production",
            ".env.local",
            "id_rsa",
            "id_ed25519",
            "credentials.json",
        ]
        for target in direct_targets:
            file_path = os.path.join(self.active_cwd, target)
            with open(file_path, "w") as f:
                f.write("SECRET")

            results = await upload_directive_files(
                client=self.mock_client,
                channel="C_SEC",
                thread_ts="123",
                targets=[target],
                active_cwd=self.active_cwd,
            )
            self.assertEqual(results, [], f"Direct sensitive target {target} should be blocked")
            self.assertEqual(self.mock_client.files_upload_v2.call_count, 0)

    async def test_fleet_prefix_exfiltration_vulnerability(self):
        """
        Assert fleet-prefixed targets like 'hermes:.env' or 'desktop-pc:.env'
        are strictly blocked by SENSITIVE_FILENAME_PATTERNS and pre-resolution checks.
        """
        env_file = os.path.join(self.active_cwd, ".env")
        with open(env_file, "w") as f:
            f.write("SLACK_BOT_TOKEN=xoxb-secret-12345")

        # Test uploading 'hermes:.env'
        results = await upload_directive_files(
            client=self.mock_client,
            channel="C_SEC",
            thread_ts="123",
            targets=["hermes:.env"],
            active_cwd=self.active_cwd,
        )

        # Assert exfiltration is strictly blocked
        self.assertEqual(results, [])
        self.assertEqual(
            self.mock_client.files_upload_v2.call_count,
            0,
            "Security failure: hermes:.env was uploaded!",
        )

    async def test_symlink_exfiltration_vulnerability(self):
        """
        Assert symlink 'harmless_log.txt' -> '.env' is strictly blocked by
        post-resolution defense-in-depth security checks.
        """
        env_file = os.path.join(self.active_cwd, ".env")
        with open(env_file, "w") as f:
            f.write("AWS_SECRET_ACCESS_KEY=secret")

        symlink_file = os.path.join(self.active_cwd, "harmless_log.txt")
        os.symlink(env_file, symlink_file)

        results = await upload_directive_files(
            client=self.mock_client,
            channel="C_SEC",
            thread_ts="123",
            targets=["harmless_log.txt"],
            active_cwd=self.active_cwd,
        )

        # Assert symlink exfiltration is strictly blocked
        self.assertEqual(results, [])
        self.assertEqual(
            self.mock_client.files_upload_v2.call_count,
            0,
            "Security failure: symlink to .env was uploaded!",
        )


class TestChallengerSlackUploadErrorResilience(unittest.IsolatedAsyncioTestCase):
    """Empirical challenge: Simulated Slack API upload errors without session crash."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.active_cwd = os.path.join(self.temp_dir, "workspace")
        os.makedirs(self.active_cwd, exist_ok=True)

        self.test_file = os.path.join(self.active_cwd, "performance_chart.png")
        with open(self.test_file, "wb") as f:
            f.write(b"PNG_DATA")

        self.mock_router = MagicMock(spec=SessionRouter)
        self.mock_router.default_target_cwd = self.active_cwd
        self.mock_router.harness = MagicMock()
        self.mock_router.harness.mesh = MagicMock()

        self.gateway = IngressGateway(router=self.mock_router, bot_user_id="U_BOT")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def _run_gateway_turn(self, client: Any, finish_text: str):
        mock_session = MagicMock()
        mock_session.session_id = "test-session"
        mock_session.target_cwd = self.active_cwd
        mock_session.is_busy = False
        mock_session.is_executing = False
        mock_session.runner_pane = None

        async def fake_stream(_):
            yield {
                "event_type": "finish",
                "response": finish_text,
                "status": "SUCCESS",
            }

        mock_session.send_user_message = fake_stream
        self.mock_router.get_or_create = AsyncMock(return_value=(mock_session, False))

        event = {
            "type": "message",
            "text": "run benchmarks",
            "channel": "C_TEST",
            "ts": "1234.0001",
        }

        # Must execute cleanly without raising an exception
        await self.gateway.handle_message(event=event, client=client)

    async def test_slack_429_rate_limit_handled_gracefully(self):
        """Assert HTTP 429 rate limit on files_upload_v2 delivers message and does not crash turn."""
        client = MagicMock()
        client.chat_postMessage = MagicMock(return_value={"ok": True})
        client.reactions_add = AsyncMock(return_value={"ok": True})
        client.reactions_remove = AsyncMock(return_value={"ok": True})
        client.files_upload_v2 = AsyncMock(side_effect=SimulatedSlackApiError(429, "ratelimited"))

        finish_text = f"Benchmarks complete. [SEND_FILE: {self.test_file}] Summary attached."
        await self._run_gateway_turn(client, finish_text)

        # Message text was delivered
        self.assertEqual(client.chat_postMessage.call_count, 1)
        posted = client.chat_postMessage.call_args.kwargs["text"]
        self.assertNotIn("[SEND_FILE:", posted)
        self.assertIn("Benchmarks complete.", posted)
        self.assertIn("Summary attached.", posted)

        # files_upload_v2 was attempted
        client.files_upload_v2.assert_called_once()

        # Reaction swapped to white_check_mark
        added_reactions = [c.kwargs["name"] for c in client.reactions_add.call_args_list]
        self.assertIn("white_check_mark", added_reactions)

    async def test_slack_500_server_error_handled_gracefully(self):
        """Assert HTTP 500 internal server error on files_upload_v2 delivers message and does not crash turn."""
        client = MagicMock()
        client.chat_postMessage = MagicMock(return_value={"ok": True})
        client.reactions_add = AsyncMock(return_value={"ok": True})
        client.reactions_remove = AsyncMock(return_value={"ok": True})
        client.files_upload_v2 = AsyncMock(side_effect=SimulatedSlackApiError(500, "internal_error"))

        finish_text = f"Benchmarks complete. [UPLOAD_FILE: {self.test_file}]"
        await self._run_gateway_turn(client, finish_text)

        self.assertEqual(client.chat_postMessage.call_count, 1)
        self.assertNotIn("[UPLOAD_FILE:", client.chat_postMessage.call_args.kwargs["text"])
        client.files_upload_v2.assert_called_once()
        added_reactions = [c.kwargs["name"] for c in client.reactions_add.call_args_list]
        self.assertIn("white_check_mark", added_reactions)

    async def test_slack_network_error_handled_gracefully(self):
        """Assert network timeout or connection reset on files_upload_v2 delivers message and does not crash turn."""
        client = MagicMock()
        client.chat_postMessage = MagicMock(return_value={"ok": True})
        client.reactions_add = AsyncMock(return_value={"ok": True})
        client.reactions_remove = AsyncMock(return_value={"ok": True})
        client.files_upload_v2 = AsyncMock(side_effect=ConnectionResetError("Connection reset by peer"))

        finish_text = f"Here is the chart: [ATTACH_FILE: {self.test_file}]"
        await self._run_gateway_turn(client, finish_text)

        self.assertEqual(client.chat_postMessage.call_count, 1)
        self.assertNotIn("[ATTACH_FILE:", client.chat_postMessage.call_args.kwargs["text"])
        client.files_upload_v2.assert_called_once()
        added_reactions = [c.kwargs["name"] for c in client.reactions_add.call_args_list]
        self.assertIn("white_check_mark", added_reactions)


if __name__ == "__main__":
    unittest.main()
