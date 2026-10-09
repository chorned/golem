#!/usr/bin/env python3
"""
tests/test_file_transfer.py
Comprehensive unit and integration tests for Milestone 3 (Slack Outbound File Uploads via Directives):
1. Directive parsing and regex extraction ([SEND_FILE], [UPLOAD_FILE], [ATTACH_FILE]).
2. Egress sanitization and directive stripping with clean newline / whitespace normalization.
3. Target path sanitization (quotes, backticks, Slack angle brackets).
4. Local candidate path search (absolute, workspace cwd, slack_transfers/, os.getcwd()).
5. Windows drive letter disambiguation vs fleet node prefixes.
6. Remote fleet path parsing and resolution (<node>:<path>).
7. Remote SCP fetch, suffix preservation, and temporary staging.
8. Lifecycle cleanup of temporary files (success, error, timeout).
9. Sensitive file exfiltration protection (.env, id_rsa, credentials.json).
10. client.files_upload_v2 parameter passing and error resilience.
11. Full IngressGateway integration with directive stripping and threaded Slack upload.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
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
    DIRECTIVE_PATTERN,
    WINDOWS_DRIVE_PATTERN,
    SENSITIVE_FILENAME_PATTERNS,
)
from golem.transport import (
    extract_and_strip_file_directives as exported_extract,
    resolve_file_path as exported_resolve,
    fetch_remote_file as exported_fetch,
    upload_directive_files as exported_upload,
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


class TestExportedSymbols(unittest.TestCase):
    """Verify golem.transport exports all required file transfer functions."""

    def test_transport_exports(self):
        self.assertIs(exported_extract, extract_and_strip_file_directives)
        self.assertIs(exported_resolve, resolve_file_path)
        self.assertIs(exported_fetch, fetch_remote_file)
        self.assertIs(exported_upload, upload_directive_files)


class TestDirectiveExtractionAndStripping(unittest.TestCase):
    """Test suite for directive regex matching, path sanitization, and text stripping."""

    def test_extract_basic_send_file(self):
        text = "Here is the report:\n[SEND_FILE: /tmp/report.pdf]\nLet me know what you think."
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/tmp/report.pdf"])
        self.assertEqual(clean, "Here is the report:\n\nLet me know what you think.")

    def test_extract_upload_and_attach_file(self):
        text1 = "Generated chart: [UPLOAD_FILE: output/chart.png]"
        clean1, targets1 = extract_and_strip_file_directives(text1)
        self.assertEqual(targets1, ["output/chart.png"])
        self.assertEqual(clean1, "Generated chart:")

        text2 = "Check the logs [ATTACH_FILE: run.log] immediately."
        clean2, targets2 = extract_and_strip_file_directives(text2)
        self.assertEqual(targets2, ["run.log"])
        self.assertEqual(clean2, "Check the logs immediately.")

    def test_case_insensitivity(self):
        variants = [
            "[send_file: test.txt]",
            "[Send_File: test.txt]",
            "[SEND_FILE: test.txt]",
            "[upload_file: test.txt]",
            "[Upload_File: test.txt]",
            "[attach_file: test.txt]",
            "[Attach_File: test.txt]",
        ]
        for variant in variants:
            clean, targets = extract_and_strip_file_directives(f"Status:\n{variant}\nDone.")
            self.assertEqual(targets, ["test.txt"], f"Failed on variant {variant}")
            self.assertEqual(clean, "Status:\n\nDone.")

    def test_quoted_and_backticked_directives(self):
        # Surrounding double quotes
        text1 = 'Here is the file: "[SEND_FILE: /tmp/test.png]"'
        clean1, targets1 = extract_and_strip_file_directives(text1)
        self.assertEqual(targets1, ["/tmp/test.png"])
        self.assertEqual(clean1, "Here is the file:")

        # Surrounding backticks
        text2 = "Review this: `[ATTACH_FILE: results.json]`"
        clean2, targets2 = extract_and_strip_file_directives(text2)
        self.assertEqual(targets2, ["results.json"])
        self.assertEqual(clean2, "Review this:")

        # Nested backticks inside path
        text3 = "```[SEND_FILE: `nested.py`]```"
        clean3, targets3 = extract_and_strip_file_directives(text3)
        self.assertEqual(targets3, ["nested.py"])
        self.assertEqual(clean3, "")

        # Internal quotes inside brackets
        text4 = '[SEND_FILE: "data/report.pdf"]'
        clean4, targets4 = extract_and_strip_file_directives(text4)
        self.assertEqual(targets4, ["data/report.pdf"])
        self.assertEqual(clean4, "")

    def test_bold_and_italic_directive_stripping(self):
        # Markdown bold wrapped directive must not leave ****
        text1 = "Please see the report:\n**[SEND_FILE: report.pdf]**\nDone."
        clean1, targets1 = extract_and_strip_file_directives(text1)
        self.assertEqual(targets1, ["report.pdf"])
        self.assertEqual(clean1, "Please see the report:\n\nDone.")
        self.assertNotIn("****", clean1)
        self.assertNotIn("**", clean1)

        # Markdown italic wrapped directive
        text2 = "Status: *[SEND_FILE: status.log]* Active"
        clean2, targets2 = extract_and_strip_file_directives(text2)
        self.assertEqual(targets2, ["status.log"])
        self.assertEqual(clean2, "Status: Active")
        self.assertNotIn("*", clean2)

    def test_slack_angle_bracket_wrapping(self):
        # Slack often wraps paths or URLs in <...>
        text = "Download: [UPLOAD_FILE: </Users/hermes/test.png>]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/Users/hermes/test.png"])
        self.assertEqual(clean, "Download:")

    def test_multiple_directives(self):
        text = "First file: [SEND_FILE: a.txt] and second file: [SEND_FILE: b.txt]"
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["a.txt", "b.txt"])
        self.assertEqual(clean, "First file: and second file:")

    def test_directive_stripping_collapses_newlines(self):
        text = "Summary:\n\n\n[SEND_FILE: /var/log/audit.log]\n\n\nAll tasks completed."
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["/var/log/audit.log"])
        self.assertEqual(clean, "Summary:\n\nAll tasks completed.")

    def test_inline_directive_space_collapsing(self):
        text = "Please find attached [SEND_FILE: summary.pdf] for your records."
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["summary.pdf"])
        self.assertEqual(clean, "Please find attached for your records.")

    def test_code_block_indentation_preserved(self):
        code_text = (
            "Here is the script:\n"
            "```python\n"
            "def calculate():\n"
            "    total = 0\n"
            "    for i in range(10):\n"
            "        total += i\n"
            "    return total\n"
            "```\n"
            "[SEND_FILE: script.py]\n"
            "Ready for execution."
        )
        clean, targets = extract_and_strip_file_directives(code_text)
        self.assertEqual(targets, ["script.py"])
        self.assertIn("    total = 0\n", clean)
        self.assertIn("        total += i\n", clean)
        self.assertTrue(clean.endswith("Ready for execution."))

    def test_empty_or_none_input(self):
        self.assertEqual(extract_and_strip_file_directives(""), ("", []))
        self.assertEqual(extract_and_strip_file_directives(None), ("", []))

    def test_no_directives_in_text(self):
        text = "Normal message without any directives."
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, [])
        self.assertEqual(clean, text)


class TestPathResolution(unittest.IsolatedAsyncioTestCase):
    """Test suite for local candidate resolution, Windows drive letter disambiguation, and remote fleet parsing."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.active_cwd = os.path.join(self.temp_dir, "workspace")
        self.transfers_dir = os.path.join(self.active_cwd, "slack_transfers")
        os.makedirs(self.transfers_dir, exist_ok=True)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_resolve_local_absolute_path(self):
        file_path = os.path.join(self.temp_dir, "absolute_test.txt")
        with open(file_path, "w") as f:
            f.write("absolute content")

        resolved, node, is_temp = await resolve_file_path(file_path, self.active_cwd)
        self.assertEqual(resolved, os.path.realpath(file_path))
        self.assertEqual(node, "local")
        self.assertFalse(is_temp)

    async def test_resolve_local_workspace_relative_path(self):
        rel_file = os.path.join(self.active_cwd, "report.pdf")
        with open(rel_file, "w") as f:
            f.write("pdf data")

        resolved, node, is_temp = await resolve_file_path("report.pdf", self.active_cwd)
        self.assertEqual(resolved, os.path.realpath(rel_file))
        self.assertEqual(node, "local")
        self.assertFalse(is_temp)

    async def test_resolve_local_slack_transfers_path(self):
        transfers_file = os.path.join(self.transfers_dir, "inbound_upload.csv")
        with open(transfers_file, "w") as f:
            f.write("a,b,c\n1,2,3")

        # Querying with just the filename should find it in slack_transfers/
        resolved, node, is_temp = await resolve_file_path("inbound_upload.csv", self.active_cwd)
        self.assertEqual(resolved, os.path.realpath(transfers_file))
        self.assertEqual(node, "local")
        self.assertFalse(is_temp)

    async def test_resolve_local_file_not_found(self):
        resolved, node, is_temp = await resolve_file_path("non_existent_file.xyz", self.active_cwd)
        self.assertIsNone(resolved)
        self.assertIsNone(node)
        self.assertFalse(is_temp)

    async def test_windows_drive_disambiguation(self):
        # Paths starting with C:\ or C:/ must NOT be split on ':' as fleet nodes
        win_path_backslash = r"C:\Users\Carl\Documents\data.xlsx"
        win_path_forward = "C:/Users/Carl/Documents/data.xlsx"

        self.assertTrue(WINDOWS_DRIVE_PATTERN.match(win_path_backslash))
        self.assertTrue(WINDOWS_DRIVE_PATTERN.match(win_path_forward))

        # Because it's a Windows drive path not on this Mac, it evaluates locally and fails to find file
        resolved, node, is_temp = await resolve_file_path(win_path_backslash, self.active_cwd)
        self.assertIsNone(resolved)
        self.assertIsNone(node)
        self.assertFalse(is_temp)

    async def test_remote_fleet_node_resolution(self):
        # Mock FleetMesh
        mock_mesh = MagicMock()
        mock_mesh.resolve_node.return_value = {
            "label": "compute-grid",
            "target_machine": "desktop-pc",
            "ssh_target": "desktop-pc",
        }

        # Mock fetch_remote_file
        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_temp = os.path.join(self.temp_dir, "fetched_remote.png")
            with open(mock_temp, "w") as f:
                f.write("remote png")
            mock_fetch.return_value = mock_temp

            target_str = "desktop-pc:/tmp/benchmark.png"
            resolved, node, is_temp = await resolve_file_path(target_str, self.active_cwd, fleet_mesh=mock_mesh)

            self.assertEqual(resolved, mock_temp)
            self.assertEqual(node, "compute-grid")
            self.assertTrue(is_temp)
            mock_fetch.assert_called_once_with(
                target="desktop-pc",
                remote_path="/tmp/benchmark.png",
                ssh_user=None,
            )

    async def test_remote_fleet_windows_path(self):
        # Remote node with Windows path, e.g. desktop-pc:/C:/output/model.onnx
        mock_mesh = MagicMock()
        mock_mesh.resolve_node.return_value = {
            "label": "compute-grid",
            "target_machine": "desktop-pc",
            "ssh_target": "100.80.59.45",
            "ssh_user": "carl",
        }

        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_temp = os.path.join(self.temp_dir, "model.onnx")
            with open(mock_temp, "w") as f:
                f.write("onnx weights")
            mock_fetch.return_value = mock_temp

            target_str = "desktop-pc:C:/output/model.onnx"
            resolved, node, is_temp = await resolve_file_path(target_str, self.active_cwd, fleet_mesh=mock_mesh)

            self.assertEqual(resolved, mock_temp)
            self.assertEqual(node, "compute-grid")
            self.assertTrue(is_temp)
            mock_fetch.assert_called_once_with(
                target="100.80.59.45",
                remote_path="C:/output/model.onnx",
                ssh_user="carl",
            )

    async def test_remote_fleet_node_local_machine(self):
        # Node resolving to target_machine == "local" (e.g. hermes alias)
        mock_mesh = MagicMock()
        mock_mesh.resolve_node.return_value = {
            "label": "homelab-ops",
            "target_machine": "local",
        }

        local_file = os.path.join(self.active_cwd, "backup.tar.gz")
        with open(local_file, "w") as f:
            f.write("backup data")

        target_str = "hermes:backup.tar.gz"
        resolved, node, is_temp = await resolve_file_path(target_str, self.active_cwd, fleet_mesh=mock_mesh)

        self.assertEqual(resolved, os.path.realpath(local_file))
        self.assertEqual(node, "homelab-ops")
        self.assertFalse(is_temp)

    async def test_unknown_fleet_node_fallback(self):
        # Unknown node prefix raises KeyError in resolve_node
        mock_mesh = MagicMock()
        mock_mesh.resolve_node.side_effect = KeyError("Node unknown not found")

        resolved, node, is_temp = await resolve_file_path("unknown:/tmp/nope.txt", self.active_cwd, fleet_mesh=mock_mesh)
        self.assertIsNone(resolved)
        self.assertIsNone(node)
        self.assertFalse(is_temp)


class TestFetchRemoteFile(unittest.IsolatedAsyncioTestCase):
    """Test suite for non-blocking SCP file fetching and temporary file lifecycle."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_fetch_remote_file_success(self):
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            async def fake_proc(*args, **kwargs):
                tmp_target = args[-1]
                # Simulate SCP writing 100 bytes into destination temp file
                with open(tmp_target, "wb") as f:
                    f.write(b"X" * 100)
                return MockSubprocess(returncode=0)

            mock_exec.side_effect = fake_proc

            result_path = await fetch_remote_file(
                target="desktop-pc",
                remote_path="/home/carl/weights.bin",
                ssh_user="hermes",
            )
            self.assertIsNotNone(result_path)
            self.assertTrue(os.path.exists(result_path))
            self.assertTrue(result_path.endswith(".bin"))
            self.assertEqual(os.path.getsize(result_path), 100)

            # Cleanup
            os.remove(result_path)

    async def test_fetch_remote_file_nonzero_exit(self):
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            async def fake_proc(*args, **kwargs):
                tmp_target = args[-1]
                # Even if file was partially created, failure must delete it
                with open(tmp_target, "wb") as f:
                    f.write(b"partial")
                return MockSubprocess(returncode=1, stderr=b"Permission denied")

            mock_exec.side_effect = fake_proc

            result_path = await fetch_remote_file(
                target="oci-machine",
                remote_path="/var/log/private.log",
            )
            self.assertIsNone(result_path)

    async def test_fetch_remote_file_zero_bytes(self):
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            async def fake_proc(*args, **kwargs):
                return MockSubprocess(returncode=0)  # File not written or 0 bytes

            mock_exec.side_effect = fake_proc

            result_path = await fetch_remote_file(
                target="personal-laptop",
                remote_path="empty.txt",
            )
            self.assertIsNone(result_path)

    async def test_fetch_remote_file_timeout(self):
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            async def fake_proc(*args, **kwargs):
                return MockSubprocess(returncode=0, delay=5.0)

            mock_exec.side_effect = fake_proc

            # Use tiny timeout to trigger TimeoutError
            result_path = await fetch_remote_file(
                target="dev-grid",
                remote_path="slow.tar",
                timeout_seconds=0.05,
            )
            self.assertIsNone(result_path)

    async def test_fetch_remote_file_spawn_exception(self):
        with patch("asyncio.create_subprocess_exec", side_effect=OSError("Command not found")):
            result_path = await fetch_remote_file(
                target="desktop-pc",
                remote_path="err.txt",
            )
            self.assertIsNone(result_path)

    async def test_fetch_remote_file_empty_inputs(self):
        self.assertIsNone(await fetch_remote_file("", "/path/test"))
        self.assertIsNone(await fetch_remote_file("target", ""))


class TestUploadDirectiveFiles(unittest.IsolatedAsyncioTestCase):
    """Test suite for upload_directive_files execution and safety guardrails."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.active_cwd = os.path.join(self.temp_dir, "workspace")
        os.makedirs(self.active_cwd, exist_ok=True)

        self.mock_client = MagicMock()
        self.mock_client.files_upload_v2 = AsyncMock(return_value={"ok": True, "file": {"id": "F123"}})

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_upload_single_local_file_success(self):
        local_file = os.path.join(self.active_cwd, "summary.pdf")
        with open(local_file, "w") as f:
            f.write("pdf contents")

        results = await upload_directive_files(
            client=self.mock_client,
            channel="C12345",
            thread_ts="1234.5678",
            targets=["summary.pdf"],
            active_cwd=self.active_cwd,
        )

        self.assertEqual(len(results), 1)
        self.mock_client.files_upload_v2.assert_called_once_with(
            channel="C12345",
            thread_ts="1234.5678",
            file=os.path.realpath(local_file),
            filename="summary.pdf",
            title="File: summary.pdf",
        )

    async def test_upload_remote_file_and_cleanup(self):
        # Setup a temporary file simulating remote fetch
        temp_remote = os.path.join(self.temp_dir, "remote_chart.png")
        with open(temp_remote, "w") as f:
            f.write("chart data")

        mock_mesh = MagicMock()
        mock_mesh.resolve_node.return_value = {
            "label": "compute-grid",
            "target_machine": "desktop-pc",
        }

        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = temp_remote

            results = await upload_directive_files(
                client=self.mock_client,
                channel="C_REMOTE",
                thread_ts="9999.1111",
                targets=["desktop-pc:/tmp/remote_chart.png"],
                active_cwd=self.active_cwd,
                fleet_mesh=mock_mesh,
            )

            self.assertEqual(len(results), 1)
            self.mock_client.files_upload_v2.assert_called_once_with(
                channel="C_REMOTE",
                thread_ts="9999.1111",
                file=temp_remote,
                filename="remote_chart.png",
                title="File (compute-grid): remote_chart.png",
            )
            # Verify temporary file was deleted in finally:
            self.assertFalse(os.path.exists(temp_remote))

    async def test_upload_cleanup_on_upload_error(self):
        # If files_upload_v2 raises an exception, the temporary file must still be deleted
        temp_remote = os.path.join(self.temp_dir, "failing_upload.png")
        with open(temp_remote, "w") as f:
            f.write("data")

        mock_mesh = MagicMock()
        mock_mesh.resolve_node.return_value = {
            "label": "cloud-ops",
            "target_machine": "oci-machine",
        }

        self.mock_client.files_upload_v2.side_effect = Exception("Slack API rate limit")

        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = temp_remote

            results = await upload_directive_files(
                client=self.mock_client,
                channel="C_ERR",
                thread_ts=None,
                targets=["oci-machine:failing_upload.png"],
                active_cwd=self.active_cwd,
                fleet_mesh=mock_mesh,
            )

            # Error was caught gracefully; results empty
            self.assertEqual(results, [])
            # Temp file was deleted despite exception
            self.assertFalse(os.path.exists(temp_remote))

    async def test_sensitive_file_skipped(self):
        # Files like .env or id_rsa must never be uploaded
        sensitive_targets = [
            ".env",
            ".env.production",
            "/Users/hermes/.ssh/id_rsa",
            "id_ed25519",
            "credentials.json",
        ]
        for s_target in sensitive_targets:
            s_file = os.path.join(self.active_cwd, os.path.basename(s_target))
            with open(s_file, "w") as f:
                f.write("SECRET_KEY=12345")

            results = await upload_directive_files(
                client=self.mock_client,
                channel="C_SEC",
                thread_ts="111.222",
                targets=[s_target],
                active_cwd=self.active_cwd,
            )
            self.assertEqual(results, [])
            self.mock_client.files_upload_v2.assert_not_called()

    async def test_sensitive_fleet_prefix_skipped(self):
        # Sensitive targets with fleet node prefixes must be strictly blocked
        fleet_sensitive_targets = [
            "hermes:.env",
            "desktop-pc:.env",
            "local:.env",
            "personal-laptop:~/.ssh/id_rsa",
            "dev-grid:id_ed25519",
            "hermes:credentials.json",
        ]
        # Create dummy local files in workspace
        for name in [".env", "id_rsa", "id_ed25519", "credentials.json"]:
            with open(os.path.join(self.active_cwd, name), "w") as f:
                f.write("SECRET")

        for f_target in fleet_sensitive_targets:
            results = await upload_directive_files(
                client=self.mock_client,
                channel="C_SEC",
                thread_ts="111.222",
                targets=[f_target],
                active_cwd=self.active_cwd,
            )
            self.assertEqual(results, [], f"Fleet sensitive target {f_target} should be blocked")
            self.mock_client.files_upload_v2.assert_not_called()

    async def test_sensitive_symlink_skipped(self):
        # Symlink pointing to .env must be blocked by post-resolution checks
        env_file = os.path.join(self.active_cwd, ".env")
        with open(env_file, "w") as f:
            f.write("SLACK_TOKEN=secret")

        symlink_file = os.path.join(self.active_cwd, "harmless_report.txt")
        os.symlink(env_file, symlink_file)

        results = await upload_directive_files(
            client=self.mock_client,
            channel="C_SEC",
            thread_ts="111.222",
            targets=["harmless_report.txt"],
            active_cwd=self.active_cwd,
        )
        self.assertEqual(results, [])
        self.mock_client.files_upload_v2.assert_not_called()

    async def test_remote_directive_filename_normalized(self):
        # Remote directive desktop-pc:chart.png must pass filename="chart.png" to Slack
        temp_remote = os.path.join(self.temp_dir, "chart.png")
        with open(temp_remote, "w") as f:
            f.write("chart data")

        mock_mesh = MagicMock()
        mock_mesh.resolve_node.return_value = {
            "label": "compute-grid",
            "target_machine": "desktop-pc",
        }

        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = temp_remote
            results = await upload_directive_files(
                client=self.mock_client,
                channel="C_REMOTE",
                thread_ts="123",
                targets=["desktop-pc:chart.png"],
                active_cwd=self.active_cwd,
                fleet_mesh=mock_mesh,
            )
            self.assertEqual(len(results), 1)
            self.mock_client.files_upload_v2.assert_called_once()
            call_kwargs = self.mock_client.files_upload_v2.call_args.kwargs
            self.assertEqual(call_kwargs["filename"], "chart.png")
            self.assertEqual(call_kwargs["title"], "File (compute-grid): chart.png")

    async def test_duplicate_target_deduplication(self):
        local_file = os.path.join(self.active_cwd, "dup.txt")
        with open(local_file, "w") as f:
            f.write("duplicate content")

        results = await upload_directive_files(
            client=self.mock_client,
            channel="C1",
            thread_ts="123",
            targets=["dup.txt", "dup.txt", "dup.txt"],
            active_cwd=self.active_cwd,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(self.mock_client.files_upload_v2.call_count, 1)

    async def test_missing_file_skipped_without_crash(self):
        results = await upload_directive_files(
            client=self.mock_client,
            channel="C1",
            thread_ts="123",
            targets=["missing_file_12345.dat"],
            active_cwd=self.active_cwd,
        )
        self.assertEqual(results, [])
        self.mock_client.files_upload_v2.assert_not_called()

    async def test_sync_client_compatibility(self):
        # Sync mock client (not returning a coroutine)
        sync_client = MagicMock()
        sync_client.files_upload_v2.return_value = {"ok": True, "file": {"id": "SYNC123"}}

        local_file = os.path.join(self.active_cwd, "sync_test.txt")
        with open(local_file, "w") as f:
            f.write("sync client data")

        results = await upload_directive_files(
            client=sync_client,
            channel="C_SYNC",
            thread_ts="555.666",
            targets=["sync_test.txt"],
            active_cwd=self.active_cwd,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["file"]["id"], "SYNC123")

    async def test_empty_or_none_inputs(self):
        self.assertEqual(await upload_directive_files(None, "C", "T", ["a"], "/tmp"), [])
        self.assertEqual(await upload_directive_files(self.mock_client, "", "T", ["a"], "/tmp"), [])
        self.assertEqual(await upload_directive_files(self.mock_client, "C", "T", [], "/tmp"), [])


class TestIngressGatewayDirectiveIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests verifying directive stripping and upload wiring in IngressGateway."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.mock_router = MagicMock(spec=SessionRouter)
        self.mock_router.default_target_cwd = self.temp_dir
        self.mock_router.harness = MagicMock()
        self.mock_router.harness.mesh = MagicMock()

        self.gateway = IngressGateway(router=self.mock_router, bot_user_id="U_BOT")

        self.mock_client = MagicMock()
        self.mock_client.chat_postMessage = MagicMock(return_value={"ok": True})
        self.mock_client.reactions_add = AsyncMock(return_value={"ok": True})
        self.mock_client.reactions_remove = AsyncMock(return_value={"ok": True})
        self.mock_client.files_upload_v2 = AsyncMock(return_value={"ok": True, "file": {"id": "F999"}})

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_handle_message_strips_directive_and_uploads_file(self):
        # Create an artifact file in the workspace
        test_file = os.path.join(self.temp_dir, "analysis_report.pdf")
        with open(test_file, "w") as f:
            f.write("Report bytes")

        # Mock session producing a finish event with directive markup
        mock_session = MagicMock()
        mock_session.session_id = "test-session"
        mock_session.target_cwd = self.temp_dir
        mock_session.is_busy = False
        mock_session.is_executing = False
        mock_session.runner_pane = None

        finish_text = (
            "Executive briefing: All benchmarks completed.\n"
            f"[SEND_FILE: {test_file}]\n"
            "Summary attached."
        )

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
            "text": "run analysis",
            "channel": "C_TEST",
            "ts": "1000.0001",
        }

        await self.gateway.handle_message(event=event, client=self.mock_client)

        # 1. Verify clean text posted to Slack WITHOUT directive markup
        self.mock_client.chat_postMessage.assert_called_once()
        call_kwargs = self.mock_client.chat_postMessage.call_args.kwargs
        posted_text = call_kwargs["text"]
        self.assertNotIn("[SEND_FILE:", posted_text)
        self.assertIn("Executive briefing: All benchmarks completed.", posted_text)
        self.assertIn("Summary attached.", posted_text)

        # 2. Verify files_upload_v2 was called for analysis_report.pdf
        self.mock_client.files_upload_v2.assert_called_once_with(
            channel="C_TEST",
            thread_ts="1000.0001",
            file=os.path.realpath(test_file),
            filename="analysis_report.pdf",
            title="File: analysis_report.pdf",
        )

    async def test_handle_message_uploads_files_for_imessage(self):
        test_file = os.path.join(self.temp_dir, "chart.png")
        with open(test_file, "w") as f:
            f.write("chart data")

        mock_session = MagicMock()
        mock_session.session_id = "test-imessage"
        mock_session.target_cwd = self.temp_dir
        mock_session.is_busy = False
        mock_session.is_executing = False
        mock_session.runner_pane = None

        async def fake_stream(_):
            yield {
                "event_type": "finish",
                "response": f"Here is the chart [SEND_FILE: {test_file}]",
                "status": "SUCCESS",
            }

        mock_session.send_user_message = fake_stream
        self.mock_router.get_or_create = AsyncMock(return_value=(mock_session, False))

        event = {
            "type": "message",
            "text": "give me chart",
            "channel": "imessage_guid",
            "ts": "2000.0001",
            "is_imessage": True,
        }

        await self.gateway.handle_message(event=event, client=self.mock_client)

        # Clean text delivered
        self.mock_client.chat_postMessage.assert_called_once()
        self.assertNotIn("[SEND_FILE:", self.mock_client.chat_postMessage.call_args.kwargs["text"])
        # files_upload_v2 SHOULD be called for iMessage
        self.mock_client.files_upload_v2.assert_called_once_with(
            channel="imessage_guid",
            thread_ts="2000.0001",
            file=os.path.realpath(test_file),
            filename="chart.png",
            title="File: chart.png",
        )

    async def test_handle_message_directive_only_message(self):
        # When model outputs ONLY [SEND_FILE: file.txt] with zero commentary
        test_file = os.path.join(self.temp_dir, "standalone.txt")
        with open(test_file, "w") as f:
            f.write("standalone data")

        mock_session = MagicMock()
        mock_session.session_id = "test-directive-only"
        mock_session.target_cwd = self.temp_dir
        mock_session.is_busy = False
        mock_session.is_executing = False
        mock_session.runner_pane = None

        async def fake_stream(_):
            yield {
                "event_type": "finish",
                "response": f"[SEND_FILE: {test_file}]",
                "status": "SUCCESS",
            }

        mock_session.send_user_message = fake_stream
        self.mock_router.get_or_create = AsyncMock(return_value=(mock_session, False))

        event = {
            "type": "message",
            "text": "send file",
            "channel": "C_ONLY",
            "ts": "3000.0001",
        }

        await self.gateway.handle_message(event=event, client=self.mock_client)

        # No empty chat message posted
        self.mock_client.chat_postMessage.assert_not_called()

    async def test_handle_message_interim_text_strips_directives(self):
        # Verify interim debounce flushes do not leak directive markup
        test_file = os.path.join(self.temp_dir, "interim_file.txt")
        with open(test_file, "w") as f:
            f.write("interim file content")

        mock_session = MagicMock()
        mock_session.session_id = "test-interim"
        mock_session.target_cwd = self.temp_dir
        mock_session.is_busy = False
        mock_session.is_executing = False
        mock_session.runner_pane = None

        async def fake_stream(_):
            yield {
                "event_type": "text_delta",
                "text_delta": f"Working on it... [SEND_FILE: {test_file}]",
            }
            # Allow debounce timeout in test if needed, or emit tool_start to trigger flush
            yield {
                "event_type": "tool_start",
                "tool": "test_tool",
            }
            yield {
                "event_type": "finish",
                "response": f"Working on it... [SEND_FILE: {test_file}] All finished.",
                "status": "SUCCESS",
            }

        mock_session.send_user_message = fake_stream
        self.mock_router.get_or_create = AsyncMock(return_value=(mock_session, False))

        event = {
            "type": "message",
            "text": "run tool",
            "channel": "C_INTERIM",
            "ts": "4000.0001",
        }

        await self.gateway.handle_message(event=event, client=self.mock_client)

        # Check all chat_postMessage calls: NONE should contain "[SEND_FILE:"
        for call in self.mock_client.chat_postMessage.call_args_list:
            text = call.kwargs.get("text", "")
            self.assertNotIn("[SEND_FILE:", text)

        # Verify file was uploaded
        self.mock_client.files_upload_v2.assert_called_once_with(
            channel="C_INTERIM",
            thread_ts="4000.0001",
            file=os.path.realpath(test_file),
            filename="interim_file.txt",
            title="File: interim_file.txt",
        )

    async def test_handle_message_multiple_directives_uploaded(self):
        file1 = os.path.join(self.temp_dir, "doc1.pdf")
        file2 = os.path.join(self.temp_dir, "doc2.pdf")
        with open(file1, "w") as f:
            f.write("doc1")
        with open(file2, "w") as f:
            f.write("doc2")

        mock_session = MagicMock()
        mock_session.session_id = "test-multi"
        mock_session.target_cwd = self.temp_dir
        mock_session.is_busy = False
        mock_session.is_executing = False
        mock_session.runner_pane = None

        finish_text = f"Here are your docs:\n[SEND_FILE: {file1}]\n[SEND_FILE: {file2}]\nEnjoy!"

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
            "text": "give me docs",
            "channel": "C_MULTI",
            "ts": "5000.0001",
        }

        await self.gateway.handle_message(event=event, client=self.mock_client)

        # Verify chat text is clean
        call_kwargs = self.mock_client.chat_postMessage.call_args.kwargs
        self.assertNotIn("[SEND_FILE:", call_kwargs["text"])
        self.assertIn("Here are your docs:", call_kwargs["text"])
        self.assertIn("Enjoy!", call_kwargs["text"])

        # Verify both files uploaded
        self.assertEqual(self.mock_client.files_upload_v2.call_count, 2)


class TestAdditionalEdgeCases(unittest.IsolatedAsyncioTestCase):
    """Additional edge cases for path resolution and error tolerance."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.active_cwd = os.path.join(self.temp_dir, "workspace")
        os.makedirs(self.active_cwd, exist_ok=True)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_resolve_path_with_default_mesh(self):
        # fleet_mesh=None should use default FleetMesh() instance
        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_temp = os.path.join(self.temp_dir, "default_mesh.txt")
            with open(mock_temp, "w") as f:
                f.write("data")
            mock_fetch.return_value = mock_temp

            resolved, node, is_temp = await resolve_file_path(
                target_str="desktop-pc:/tmp/test.txt",
                active_cwd=self.active_cwd,
                fleet_mesh=None,
            )
            self.assertEqual(resolved, mock_temp)
            self.assertEqual(node, "compute-grid")
            self.assertTrue(is_temp)

    def test_spaces_in_directive_path(self):
        text = 'Here is the spreadsheet: [SEND_FILE: "Spreadsheets/Q3 Financial Report 2026.xlsx"]'
        clean, targets = extract_and_strip_file_directives(text)
        self.assertEqual(targets, ["Spreadsheets/Q3 Financial Report 2026.xlsx"])
        self.assertEqual(clean, "Here is the spreadsheet:")

    async def test_upload_directive_files_os_remove_oserror_handled(self):
        # When os.remove raises OSError during temp file cleanup, it must be ignored gracefully
        mock_client = MagicMock()
        mock_client.files_upload_v2 = AsyncMock(return_value={"ok": True})

        mock_mesh = MagicMock()
        mock_mesh.resolve_node.return_value = {
            "label": "compute-grid",
            "target_machine": "desktop-pc",
        }

        temp_file = os.path.join(self.temp_dir, "os_err.dat")
        with open(temp_file, "w") as f:
            f.write("data")

        with patch("golem.transport.file_transfer.fetch_remote_file", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = temp_file
            with patch("os.remove", side_effect=OSError("Disk error")):
                results = await upload_directive_files(
                    client=mock_client,
                    channel="C_ERR",
                    thread_ts="123.456",
                    targets=["desktop-pc:os_err.dat"],
                    active_cwd=self.active_cwd,
                    fleet_mesh=mock_mesh,
                )
                self.assertEqual(len(results), 1)


if __name__ == "__main__":
    unittest.main()
