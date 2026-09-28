import os
import sys
import json
import asyncio
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Add AGY directory to sys.path
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import app


class TestAGYService(unittest.IsolatedAsyncioTestCase):

    def test_parse_remote_hosts(self):
        """Test parsing of REMOTE_HOSTS string into alias -> ip mapping."""
        raw = "box1:100.64.0.1, server2:100.64.0.2 , invalid_entry, BOX3:100.64.0.3"
        hosts = app.parse_remote_hosts(raw)
        self.assertEqual(hosts.get("box1"), "100.64.0.1")
        self.assertEqual(hosts.get("server2"), "100.64.0.2")
        self.assertEqual(hosts.get("box3"), "100.64.0.3")
        self.assertNotIn("invalid_entry", hosts)
        self.assertEqual(app.parse_remote_hosts(""), {})

    def test_workspace_locks(self):
        """Test in-memory asyncio.Lock per target host and workspace tuple (host, workspace)."""
        dir1 = "/tmp/workspace1"
        dir2 = "/tmp/workspace2"
        dir1_relative = "/tmp/workspace1/../workspace1"

        lock_local1 = app.get_workspace_lock("local", dir1)
        lock_local2 = app.get_workspace_lock("local", dir2)
        lock_local1_alias = app.get_workspace_lock("local", dir1_relative)
        lock_remote_box1 = app.get_workspace_lock("box1", "~")
        lock_remote_box2 = app.get_workspace_lock("box2", "~")
        lock_remote_box1_again = app.get_workspace_lock("box1", "~")

        # Local canonical equivalence
        self.assertIs(lock_local1, lock_local1_alias)
        self.assertIsNot(lock_local1, lock_local2)

        # Remote lock isolation (remote does not block local, different hosts do not block each other)
        self.assertIsNot(lock_local1, lock_remote_box1)
        self.assertIsNot(lock_remote_box1, lock_remote_box2)
        self.assertIs(lock_remote_box1, lock_remote_box1_again)

    async def test_switch_to_command_remote_alias(self):
        """Test switching to a configured remote host alias over Tailscale."""
        client = AsyncMock()
        session_key = "C_REMOTE:T100"
        app.sessions[session_key] = {
            "host": "local",
            "ip": None,
            "workspace": "/initial/path",
            "conversation_id": "test-conv-123"
        }

        with patch.object(app, "get_remote_hosts", return_value={"box1": "100.64.0.1"}):
            event = {
                "channel": "C_REMOTE",
                "ts": "T100",
                "text": "switch to box1"
            }
            await app.process_user_prompt(event, client)

            session = app.sessions[session_key]
            self.assertEqual(session["host"], "box1")
            self.assertEqual(session["ip"], "100.64.0.1")
            self.assertEqual(session["workspace"], "~")
            self.assertIsNone(session["conversation_id"])

            client.chat_postMessage.assert_called_once()
            reply_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Switched target to remote host *box1* (100.64.0.1:~).", reply_text)

    async def test_switch_to_command_local_valid_and_invalid(self):
        """Test the 'switch to <path>' command behavior for local filesystem paths."""
        client = AsyncMock()
        with tempfile.TemporaryDirectory() as valid_dir:
            session_key = "C123:T456"
            app.sessions[session_key] = {
                "host": "box1",
                "ip": "100.64.0.1",
                "workspace": "~",
                "conversation_id": "test-conv-id-123"
            }

            # Test switching back to local directory
            event = {
                "channel": "C123",
                "ts": "T456",
                "text": f"switch to {valid_dir}"
            }
            await app.process_user_prompt(event, client)

            session = app.sessions[session_key]
            self.assertEqual(session["host"], "local")
            self.assertIsNone(session["ip"])
            self.assertEqual(session["workspace"], os.path.realpath(valid_dir))
            self.assertIsNone(session["conversation_id"])
            client.chat_postMessage.assert_called()
            last_call = client.chat_postMessage.call_args[1]
            self.assertIn("Switched workspace to", last_call["text"])

            # Test invalid target (neither alias nor directory)
            invalid_dir = os.path.join(valid_dir, "nonexistent_subfolder")
            event_invalid = {
                "channel": "C123",
                "ts": "T456",
                "text": f"switch to {invalid_dir}"
            }
            await app.process_user_prompt(event_invalid, client)

            # Workspace should remain unchanged
            self.assertEqual(app.sessions[session_key]["workspace"], os.path.realpath(valid_dir))
            last_call = client.chat_postMessage.call_args[1]
            self.assertIn("Unrecognized target", last_call["text"])

    async def test_remote_ssh_invocation(self):
        """Verify SSH command invocation over Tailscale to remote user's profile root (~) without PTY."""
        client = AsyncMock()
        channel = "C_SSH"
        thread_ts = "100.200"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {
            "host": "box1",
            "ip": "100.64.0.1",
            "workspace": "~",
            "conversation_id": "remote-conv-999"
        }

        client.chat_postMessage.return_value = {"ts": "hud-ts"}

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.stdout.readline = AsyncMock(side_effect=[
            b'{"type":"message","content":"Remote execution completed on box1."}\n',
            b''
        ])
        mock_proc.wait = AsyncMock(return_value=0)

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec, \
             patch.object(app, "get_remote_ssh_user", return_value="hermes"), \
             patch.object(app, "get_git_diff", new_callable=AsyncMock, return_value=None):

            await app.execute_agy_task(
                prompt="Check disk space",
                host="box1",
                ip="100.64.0.1",
                active_workspace="~",
                conversation_id="remote-conv-999",
                channel=channel,
                thread_ts=thread_ts,
                session_key=session_key,
                client=client
            )

            mock_exec.assert_called_once()
            args, kwargs = mock_exec.call_args
            # Verify SSH parameters
            self.assertEqual(args[0], "ssh")
            self.assertIn("-o", args)
            self.assertIn("BatchMode=yes", args)
            self.assertIn("ConnectTimeout=5", args)
            self.assertIn("-T", args)  # No pseudo-terminal
            self.assertEqual(args[args.index("-T") + 1], "hermes@100.64.0.1")

            # Verify remote command string includes agy, -p, stream-json, --work-dir ~, and conversation id
            remote_cmd = args[-1]
            self.assertIn("agy", remote_cmd)
            self.assertIn("--work-dir", remote_cmd)
            self.assertIn("~", remote_cmd)
            self.assertIn("--conversation remote-conv-999", remote_cmd)
            self.assertIn("stream-json", remote_cmd)

            # Local cwd must be None for remote runs
            self.assertIsNone(kwargs.get("cwd"))

            # Final response displayed
            client.chat_postMessage.assert_called()
            last_call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertEqual(last_call_text, "Remote execution completed on box1.")

    async def test_remote_ssh_connection_failure(self):
        """Verify SSH connection failure (exit code 255) reports clear network/auth failure message."""
        client = AsyncMock()
        channel = "C_SSH_FAIL"
        thread_ts = "100.300"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {
            "host": "box1",
            "ip": "100.64.0.1",
            "workspace": "~",
            "conversation_id": None
        }

        client.chat_postMessage.return_value = {"ts": "hud-ts"}

        mock_proc = AsyncMock()
        mock_proc.returncode = 255
        mock_proc.stdout.readline = AsyncMock(side_effect=[b''])
        mock_proc.stderr.read = AsyncMock(return_value=b"ssh: connect to host 100.64.0.1 port 22: Operation timed out")
        mock_proc.wait = AsyncMock(return_value=255)

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            await app.execute_agy_task(
                prompt="Run remote task",
                host="box1",
                ip="100.64.0.1",
                active_workspace="~",
                conversation_id=None,
                channel=channel,
                thread_ts=thread_ts,
                session_key=session_key,
                client=client
            )

            client.chat_postMessage.assert_called()
            last_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Status: SSH Connection Failed (Exit Code: 255)", last_text)
            self.assertIn("Operation timed out", last_text)

    async def test_remote_git_diff_non_git_repo_skipped_silently(self):
        """Verify git diff HEAD for remote target skips silently when ~ is not a Git repo."""
        mock_proc = AsyncMock()
        # Simulated failure from `cd ~ && git rev-parse --is-inside-work-tree ...`
        mock_proc.returncode = 128
        mock_proc.communicate = AsyncMock(return_value=(b"", b"fatal: not a git repository"))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            diff = await app.get_git_diff(host="box1", ip="100.64.0.1", active_workspace="~")
            self.assertIsNone(diff, "Diff should be None when remote profile root is not a git repo")

    async def test_remote_skips_local_image_uploads(self):
        """Verify remote executions do not attempt local image artifact uploads."""
        client = AsyncMock()
        channel = "C_IMG_REMOTE"
        thread_ts = "100.400"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "box1", "ip": "100.64.0.1", "workspace": "~", "conversation_id": None}

        client.chat_postMessage.return_value = {"ts": "hud-ts"}

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.stdout.readline = AsyncMock(side_effect=[
            b'{"type":"message","content":"Remote finished."}\n',
            b''
        ])
        mock_proc.wait = AsyncMock(return_value=0)
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
             patch.object(app, "upload_new_images", new_callable=AsyncMock) as mock_img_upload:

            await app.execute_agy_task(
                prompt="Create chart",
                host="box1",
                ip="100.64.0.1",
                active_workspace="~",
                conversation_id=None,
                channel=channel,
                thread_ts=thread_ts,
                session_key=session_key,
                client=client
            )

            mock_img_upload.assert_not_called()

    async def test_reacji_state_lifecycle_helpers(self):
        """Verify add_reacji, remove_reacji, and swap_reacji correctly invoke reactions API."""
        client = AsyncMock()
        channel = "C123"
        ts = "12345.67890"

        await app.add_reacji(client, channel, ts, "eyes")
        client.reactions_add.assert_called_with(channel=channel, timestamp=ts, name="eyes")

        await app.remove_reacji(client, channel, ts, "eyes")
        client.reactions_remove.assert_called_with(channel=channel, timestamp=ts, name="eyes")

        await app.swap_reacji(client, channel, ts, "eyes", "gear")
        client.reactions_remove.assert_called_with(channel=channel, timestamp=ts, name="eyes")
        client.reactions_add.assert_called_with(channel=channel, timestamp=ts, name="gear")

    async def test_empty_output_bug_fixed_with_message_event(self):
        """Verify when stream contains type=='message' with content, conversational text is accumulated."""
        client = AsyncMock()
        channel = "C_MSG_TEST"
        thread_ts = "111.222"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/tmp", "conversation_id": None}

        client.chat_postMessage.return_value = {"ts": "hud-ts"}

        ndjson_lines = [
            b'{"type":"message","content":"Here is the direct answer you asked for."}\n',
            b'{"type":"result","data":{"status":"SUCCESS"}}\n',
            b''
        ]

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.stdout.readline = AsyncMock(side_effect=ndjson_lines)
        mock_proc.wait = AsyncMock(return_value=0)
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            await app.execute_agy_task(
                prompt="Hello",
                host="local",
                ip=None,
                active_workspace="/tmp",
                conversation_id=None,
                channel=channel,
                thread_ts=thread_ts,
                session_key=session_key,
                client=client
            )

            client.chat_postMessage.assert_called()
            last_call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertEqual(last_call_text, "Here is the direct answer you asked for.")

    async def test_result_event_output_capture(self):
        """Verify type=='result' with data.output is captured as final answer."""
        client = AsyncMock()
        channel = "C_RES_TEST"
        thread_ts = "333.444"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/tmp", "conversation_id": None}

        client.chat_postMessage.return_value = {"ts": "hud-ts"}

        ndjson_lines = [
            b'{"type":"result","data":{"output":"Calculated 42 successfully"}}\n',
            b''
        ]

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.stdout.readline = AsyncMock(side_effect=ndjson_lines)
        mock_proc.wait = AsyncMock(return_value=0)
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            await app.execute_agy_task(
                prompt="Calculate",
                host="local",
                ip=None,
                active_workspace="/tmp",
                conversation_id=None,
                channel=channel,
                thread_ts=thread_ts,
                session_key=session_key,
                client=client
            )

            client.chat_postMessage.assert_called()
            last_call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertEqual(last_call_text, "Calculated 42 successfully")

    async def test_subprocess_failure_error_block(self):
        """Verify local subprocess failures capture stderr and exit code, displaying error block."""
        client = AsyncMock()
        channel = "C_ERR_TEST"
        thread_ts = "555.666"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/tmp", "conversation_id": None}

        client.chat_postMessage.return_value = {"ts": "hud-ts"}

        mock_proc = AsyncMock()
        mock_proc.returncode = 127
        mock_proc.stdout.readline = AsyncMock(side_effect=[b''])
        mock_proc.stderr.read = AsyncMock(return_value=b"agy: command not found\npermission denied")
        mock_proc.wait = AsyncMock(return_value=127)

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            await app.execute_agy_task(
                prompt="Do work",
                host="local",
                ip=None,
                active_workspace="/tmp",
                conversation_id=None,
                channel=channel,
                thread_ts=thread_ts,
                session_key=session_key,
                client=client
            )

            client.chat_postMessage.assert_called()
            last_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Status: Failed (Exit Code: 127)", last_text)
            self.assertIn("command not found", last_text)

    async def test_full_trace_transcript_upload(self):
        """Verify if execution generated > 10 total trace lines, full trace is uploaded as agy_trace_<ts>.txt."""
        client = AsyncMock()
        channel = "C_TRACE_TEST"
        thread_ts = "777.888"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/tmp", "conversation_id": None}

        client.chat_postMessage.return_value = {"ts": "hud-ts"}

        ndjson_lines = [
            b'{"event":"init","conversation_id":"c-123"}\n',
            b'{"data":{"thought":"Analyzing problem..."}}\n',
            b'{"data":{"command":"git status"}}\n',
            b'{"data":{"thought":"Reading files..."}}\n',
            b'{"step_update":{"step_type":"tool","tool_name":"list_dir","state":"ACTIVE","tool_info":{"parameters":{"DirectoryPath":"/tmp"}}}}\n',
            b'{"step_update":{"step_type":"tool","tool_name":"list_dir","state":"DONE","tool_info":{"output":"file1 file2"}}}\n',
            b'{"data":{"thought":"Editing code..."}}\n',
            b'{"step_update":{"step_type":"tool","tool_name":"write_to_file","state":"ACTIVE","tool_info":{"parameters":{"TargetFile":"/tmp/a.py"}}}}\n',
            b'{"step_update":{"step_type":"tool","tool_name":"write_to_file","state":"DONE","tool_info":{"output":"ok"}}}\n',
            b'{"data":{"command":"pytest"}}\n',
            b'{"type":"message","content":"Finished all tasks successfully."}\n',
            b''
        ]

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.stdout.readline = AsyncMock(side_effect=ndjson_lines)
        mock_proc.wait = AsyncMock(return_value=0)
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            await app.execute_agy_task(
                prompt="Do big task",
                host="local",
                ip=None,
                active_workspace="/tmp",
                conversation_id=None,
                channel=channel,
                thread_ts=thread_ts,
                session_key=session_key,
                client=client
            )

            client.files_upload_v2.assert_called()
            upload_calls = client.files_upload_v2.call_args_list
            trace_call = next((c[1] for c in upload_calls if "agy_trace_" in c[1].get("filename", "")), None)
            self.assertIsNotNone(trace_call, "Full trace file agy_trace_<timestamp>.txt must be uploaded")
            self.assertEqual(trace_call["title"], "Full Execution Trace")
            self.assertIn("[INIT]", trace_call["content"])
            self.assertIn("[BASH]", trace_call["content"])

    async def test_git_diff_small_appended(self):
        """Diff under 35 lines should be appended inside a markdown diff code block."""
        client = AsyncMock()
        channel = "C_DIFF_SMALL"
        thread_ts = "123.456"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/tmp", "conversation_id": None}

        client.chat_postMessage.return_value = {"ts": "hud-ts"}

        short_diff = "diff --git a/test.txt b/test.txt\n+hello world\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.stdout.readline = AsyncMock(side_effect=[
                b'{"type":"message","content":"Done editing."}\n',
                b''
            ])
            mock_proc.wait = AsyncMock(return_value=0)

            with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
                 patch.object(app, "get_git_diff", new_callable=AsyncMock, return_value=short_diff):
                await app.execute_agy_task(
                    prompt="Edit test.txt",
                    host="local",
                    ip=None,
                    active_workspace=tmpdir,
                    conversation_id=None,
                    channel=channel,
                    thread_ts=thread_ts,
                    session_key=session_key,
                    client=client
                )

                client.chat_postMessage.assert_called()
                last_text = client.chat_postMessage.call_args[1]["text"]
                self.assertIn("Done editing.", last_text)
                self.assertIn("```diff", last_text)
                self.assertIn("+hello world", last_text)

    async def test_git_diff_large_uploaded(self):
        """Diff over 35 lines should be uploaded as changes.patch via files.uploadV2."""
        client = AsyncMock()
        channel = "C_DIFF_LARGE"
        thread_ts = "123.456"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/tmp", "conversation_id": None}

        client.chat_postMessage.return_value = {"ts": "hud-ts"}

        long_diff = "\n".join([f"+line {i}" for i in range(40)])

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.stdout.readline = AsyncMock(side_effect=[
                b'{"type":"message","content":"Done large editing."}\n',
                b''
            ])
            mock_proc.wait = AsyncMock(return_value=0)

            with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
                 patch.object(app, "get_git_diff", new_callable=AsyncMock, return_value=long_diff):
                await app.execute_agy_task(
                    prompt="Edit many lines",
                    host="local",
                    ip=None,
                    active_workspace=tmpdir,
                    conversation_id=None,
                    channel=channel,
                    thread_ts=thread_ts,
                    session_key=session_key,
                    client=client
                )

                client.files_upload_v2.assert_called()
                upload_call = next((c[1] for c in client.files_upload_v2.call_args_list if c[1].get("filename") == "changes.patch"), None)
                self.assertIsNotNone(upload_call, "changes.patch must be uploaded for diff > 35 lines")
                self.assertEqual(upload_call["content"], long_diff)

    async def test_thread_scoped_implicit_triggering(self):
        """Verify Thread-Scoped Implicit Triggering (No Repeated Mentions)."""
        client = AsyncMock()
        channel = "C_TEST_THREAD"
        root_ts = "1700000000.000100"
        follow_ts = "1700000005.000200"
        untracked_root_ts = "1700000010.000300"

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_process:
            # 1. In channels, a session begins when the bot is explicitly mentioned via app_mention
            mention_event = {
                "channel": channel,
                "ts": root_ts,
                "text": "<@U0C2N9ENPAA> start working on podcast-bot"
            }
            await app.handle_app_mention(mention_event, client)

            session_key = f"{channel}:{root_ts}"
            self.assertIn(session_key, app.active_thread_sessions)
            mock_process.assert_called_once_with(mention_event, client)
            mock_process.reset_mock()

            # 2. Follow-up message arrives inside the registered thread WITHOUT an @-mention
            unmentioned_reply = {
                "channel": channel,
                "ts": follow_ts,
                "thread_ts": root_ts,
                "text": "now fix the imports"
            }
            await app.handle_direct_or_thread_message(unmentioned_reply, client)
            mock_process.assert_called_once_with(unmentioned_reply, client)
            mock_process.reset_mock()

            # 3. Duplicate event (both app_mention and message)
            duplicate_mention_reply = {
                "channel": channel,
                "ts": "1700000020.000400",
                "thread_ts": root_ts,
                "text": "<@U0C2N9ENPAA> also run pytest"
            }
            await app.handle_app_mention(duplicate_mention_reply, client)
            await app.handle_direct_or_thread_message(duplicate_mention_reply, client)
            self.assertEqual(mock_process.call_count, 1)
            mock_process.reset_mock()

            # 4. Disregard unmentioned top-level message
            untracked_top_level = {
                "channel": channel,
                "ts": untracked_root_ts,
                "text": "random unmentioned channel chatter"
            }
            await app.handle_direct_or_thread_message(untracked_top_level, client)
            mock_process.assert_not_called()

    def test_get_remote_ssh_user_resolution(self):
        """Verify get_remote_ssh_user resolves explicitly configured user or SSH config user."""
        with patch.dict(os.environ, {"REMOTE_SSH_USER": "custom_user"}):
            self.assertEqual(app.get_remote_ssh_user("100.113.173.33"), "custom_user")

        with patch.dict(os.environ, {"REMOTE_SSH_USER": ""}):
            # Test subprocess resolution via ssh -G
            mock_proc = MagicMock()
            mock_proc.returncode = 0
            mock_proc.stdout = "user opc\nhostname 100.113.173.33\n"
            with patch("subprocess.run", return_value=mock_proc):
                self.assertEqual(app.get_remote_ssh_user("100.113.173.33"), "opc")

    async def test_switch_to_oci_remote_host(self):
        """Verify switching to OCI alias repoints target to 100.113.173.33:~."""
        client = AsyncMock()
        channel = "C_OCI"
        thread_ts = "999.001"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/local/path", "conversation_id": "c-1"}

        with patch.object(app, "get_remote_hosts", return_value={"oci": "100.113.173.33"}):
            event = {
                "channel": channel,
                "ts": thread_ts,
                "text": "switch to OCI"
            }
            await app.process_user_prompt(event, client)

            session = app.sessions[session_key]
            self.assertEqual(session["host"], "oci")
            self.assertEqual(session["ip"], "100.113.173.33")
            self.assertEqual(session["workspace"], "~")
            self.assertIsNone(session["conversation_id"])

            client.chat_postMessage.assert_called_once()
            call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Switched target to remote host *oci* (100.113.173.33:~).", call_text)

    async def test_switch_to_pc_remote_host(self):
        """Verify switching to PC alias repoints target to 100.80.59.45:~."""
        client = AsyncMock()
        channel = "C_PC"
        thread_ts = "999.002"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/local/path", "conversation_id": "c-2"}

        with patch.object(app, "get_remote_hosts", return_value={"pc": "100.80.59.45"}):
            event = {
                "channel": channel,
                "ts": thread_ts,
                "text": "switch to PC"
            }
            await app.process_user_prompt(event, client)

            session = app.sessions[session_key]
            self.assertEqual(session["host"], "pc")
            self.assertEqual(session["ip"], "100.80.59.45")
            self.assertEqual(session["workspace"], "~")
            self.assertIsNone(session["conversation_id"])

            client.chat_postMessage.assert_called_once()
            call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Switched target to remote host *pc* (100.80.59.45:~).", call_text)

    async def test_switch_to_remote_host_by_ip(self):
        """Verify switching directly by configured Tailscale IP resolves and repoints target."""
        client = AsyncMock()
        channel = "C_IP"
        thread_ts = "999.003"
        session_key = f"{channel}:{thread_ts}"
        app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/local/path", "conversation_id": "c-3"}

        with patch.object(app, "get_remote_hosts", return_value={"oci": "100.113.173.33", "pc": "100.80.59.45"}):
            # Test switching by OCI IP
            event_oci = {
                "channel": channel,
                "ts": thread_ts,
                "text": "switch to 100.113.173.33"
            }
            await app.process_user_prompt(event_oci, client)

            session = app.sessions[session_key]
            self.assertEqual(session["host"], "oci")
            self.assertEqual(session["ip"], "100.113.173.33")
            self.assertEqual(session["workspace"], "~")

            client.chat_postMessage.assert_called_once()
            call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Switched target to remote host *oci* (100.113.173.33:~).", call_text)

            # Test switching by PC IP
            client.reset_mock()
            event_pc = {
                "channel": channel,
                "ts": thread_ts,
                "text": "switch workspace to 100.80.59.45"
            }
            await app.process_user_prompt(event_pc, client)

            session = app.sessions[session_key]
            self.assertEqual(session["host"], "pc")
            self.assertEqual(session["ip"], "100.80.59.45")
            self.assertEqual(session["workspace"], "~")

            client.chat_postMessage.assert_called_once()
            call_text = client.chat_postMessage.call_args[1]["text"]
            self.assertIn("Switched target to remote host *pc* (100.80.59.45:~).", call_text)

    def test_format_for_slack(self):
        """Verify markdown **bold** is converted to Slack mrkdwn *bold* outside code blocks."""
        input_text = "Here is **bold** text and `**inline code**` and:\n```\n**code block**\n```\nAlso **another bold**."
        formatted = app.format_for_slack(input_text)
        self.assertIn("*bold*", formatted)
        self.assertIn("*another bold*", formatted)
        self.assertIn("`**inline code**`", formatted)
        self.assertIn("```\n**code block**\n```", formatted)

    def test_build_slack_context_prompt(self):
        """Verify Slack context wrapper injects user, channel, and target environment awareness."""
        # Test DM context
        dm_event = {
            "channel": "D12345",
            "channel_type": "im",
            "user": "U12345",
            "ts": "1700000000.000100"
        }
        prompt_dm = app.build_slack_context_prompt(
            clean_text="What channel am I in?",
            event=dm_event,
            host="pc",
            ip="100.80.59.45",
            active_workspace="~",
            thread_ts="1700000000.000100"
        )
        self.assertIn("[Slack Interface Context]", prompt_dm)
        self.assertIn("<@U12345>", prompt_dm)
        self.assertIn("Direct Message (1:1 with @agy)", prompt_dm)
        self.assertIn("remote host 'pc' (100.80.59.45:~)", prompt_dm)
        self.assertIn("What channel am I in?", prompt_dm)

        # Test Public channel context
        chan_event = {
            "channel": "C98765",
            "channel_type": "channel",
            "user": "U999",
            "ts": "1700000000.000200"
        }
        prompt_chan = app.build_slack_context_prompt(
            clean_text="Run tests",
            event=chan_event,
            host="local",
            ip=None,
            active_workspace="/Users/hermes/projects/AGY",
            thread_ts="1700000000.000200"
        )
        self.assertIn("channel <#C98765>", prompt_chan)
        self.assertIn("local workspace (/Users/hermes/projects/AGY)", prompt_chan)

    async def test_switch_to_flexible_phrasing(self):
        """Verify conversational variants of workspace/target switching work as expected."""
        client = AsyncMock()
        phrasings = [
            "can you switch to pc?",
            "switch workspace to pc",
            "switch target to pc",
            "switch over to pc",
            "change workspace to pc",
            "change target to pc",
            "could you please switch to pc"
        ]

        for phrase in phrasings:
            client.reset_mock()
            session_key = f"C_FLEX:{phrase}"
            app.sessions[session_key] = {"host": "local", "ip": None, "workspace": "/local/path", "conversation_id": "c-flex"}

            with patch.object(app, "get_remote_hosts", return_value={"pc": "100.80.59.45"}):
                event = {
                    "channel": "C_FLEX",
                    "ts": phrase,
                    "text": phrase
                }
                await app.process_user_prompt(event, client)

                session = app.sessions[session_key]
                self.assertEqual(session["host"], "pc", f"Failed for phrase: {phrase}")
                self.assertEqual(session["ip"], "100.80.59.45")
                self.assertEqual(session["workspace"], "~")
                client.chat_postMessage.assert_called_once()
                call_text = client.chat_postMessage.call_args[1]["text"]
                self.assertIn("Switched target to remote host *pc* (100.80.59.45:~).", call_text)

    async def test_save_uploaded_file_local(self):
        """Verify uploaded file bytes are saved cleanly to the slack_transfers subfolder."""
        with tempfile.TemporaryDirectory() as tmp_ws:
            file_bytes = b"print('hello from uploaded slack file')"
            saved = await app.save_uploaded_file(
                file_bytes=file_bytes,
                filename="myscript.py",
                host="local",
                ip=None,
                active_workspace=tmp_ws
            )
            self.assertIsNotNone(saved)
            self.assertTrue(os.path.isfile(saved))
            expected_path = os.path.join(tmp_ws, app.TRANSFERS_DIR_NAME, "myscript.py")
            self.assertEqual(saved, expected_path)
            with open(saved, "rb") as f:
                self.assertEqual(f.read(), file_bytes)

    async def test_direct_send_me_command(self):
        """Verify 'send me <filename>' immediately uploads an existing workspace or slack_transfers file."""
        client = AsyncMock()
        with tempfile.TemporaryDirectory() as tmp_ws:
            transfers_dir = os.path.join(tmp_ws, app.TRANSFERS_DIR_NAME)
            os.makedirs(transfers_dir, exist_ok=True)
            test_file = os.path.join(transfers_dir, "results.csv")
            with open(test_file, "w") as f:
                f.write("col1,col2\nval1,val2\n")

            channel = "C_FILES"
            thread_ts = "1700000000.000300"
            session_key = f"{channel}:{thread_ts}"
            app.sessions[session_key] = {
                "host": "local",
                "ip": None,
                "workspace": tmp_ws,
                "conversation_id": "c-file"
            }

            event = {
                "channel": channel,
                "ts": thread_ts,
                "text": "can you please send me results.csv?"
            }
            await app.process_user_prompt(event, client)

            client.files_upload_v2.assert_called_once()
            upload_kwargs = client.files_upload_v2.call_args[1]
            self.assertEqual(upload_kwargs["file"], os.path.realpath(test_file))
            self.assertEqual(upload_kwargs["filename"], "results.csv")

            client.chat_postMessage.assert_called_once()
            self.assertIn("Here is `results.csv`", client.chat_postMessage.call_args[1]["text"])

    async def test_send_file_directive_in_execute_agy_task(self):
        """Verify [SEND_FILE: path] in agy output uploads file and strips directive from message."""
        client = AsyncMock()
        with tempfile.TemporaryDirectory() as tmp_ws:
            report_path = os.path.join(tmp_ws, "report.pdf")
            with open(report_path, "wb") as f:
                f.write(b"%PDF-1.4 dummy pdf content")

            channel = "C_DIRECTIVE"
            thread_ts = "1700000000.000400"
            session_key = f"{channel}:{thread_ts}"

            ndjson_lines = [
                json.dumps({"type": "message", "content": "I generated the report. [SEND_FILE: report.pdf]"}).encode("utf-8") + b"\n",
                json.dumps({"type": "result", "output": "I generated the report. [SEND_FILE: report.pdf]"}).encode("utf-8") + b"\n",
                b""
            ]

            mock_proc = AsyncMock()
            mock_proc.returncode = 0
            mock_proc.stdout.readline = AsyncMock(side_effect=ndjson_lines)
            mock_proc.stderr.read = AsyncMock(return_value=b"")
            mock_proc.wait = AsyncMock(return_value=0)
            mock_proc.communicate = AsyncMock(return_value=(b"", b""))

            with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
                 patch.object(app, "get_git_diff", new_callable=AsyncMock, return_value=None):

                await app.execute_agy_task(
                    prompt="Generate report",
                    host="local",
                    ip=None,
                    active_workspace=tmp_ws,
                    conversation_id=None,
                    channel=channel,
                    thread_ts=thread_ts,
                    session_key=session_key,
                    client=client
                )

                # files_upload_v2 should have been called for report.pdf
                upload_calls = client.files_upload_v2.call_args_list
                uploaded_files = [c[1].get("filename") for c in upload_calls if "filename" in c[1]]
                self.assertIn("report.pdf", uploaded_files)

    async def test_file_share_subtype_handled(self):
        """Verify messages with subtype='file_share' are not discarded."""
        client = AsyncMock()
        channel = "C_SHARE"
        thread_ts = "1700000000.000500"
        session_key = f"{channel}:{thread_ts}"
        app.active_thread_sessions.add(session_key)

        file_event = {
            "channel": channel,
            "ts": thread_ts,
            "thread_ts": thread_ts,
            "subtype": "file_share",
            "text": "here is my code",
            "files": [{"name": "script.py", "url_private_download": "https://fake.url"}]
        }

        with patch.object(app, "process_user_prompt", new_callable=AsyncMock) as mock_process:
            await app.handle_direct_or_thread_message(file_event, client)
            mock_process.assert_called_once_with(file_event, client)


if __name__ == "__main__":
    unittest.main()



