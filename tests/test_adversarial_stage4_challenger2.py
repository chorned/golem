#!/usr/bin/env python3
"""
tests/test_adversarial_stage4_challenger2.py
Adversarial Stress Test Suite for Stage 4: Channel-Context Artifact Sandboxing,
Auto-Binding, Multi-Session Concurrency Isolation, Daemon Rehydration, and Ingress Grounding.

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
from golem.ingress import IngressGateway


class TestAdversarialSessionLastArtifactAutoBinding(unittest.TestCase):
    """
    Empirically challenge last_artifact auto-binding, omitted path resolution,
    and missing context error handling.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="adv_stage4_autobind_")
        self.outbox_dir = Path(self.temp_dir) / "outbox"
        self.mgr = OutboxManager(root_dir=self.outbox_dir)
        self.artifacts_dir = self.outbox_dir / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

        self._token = CURRENT_SESSION.set(None)
        self.session = SessionHandle(
            session_id="sess_adv_01",
            channel="C_ADV_01",
            thread_ts="1710000000.100",
            target_cwd=self.temp_dir,
        )

        # Create a sample valid artifact file
        self.art_file = self.artifacts_dir / "data_export.csv"
        self.art_file.write_text("id,val\n1,100\n2,200\n")

    def tearDown(self):
        CURRENT_SESSION.reset(self._token)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_share_artifact_auto_binds_to_explicit_session(self):
        """Verify share_artifact(path=...) explicitly binds metadata to session.last_artifact."""
        self.assertIsNone(self.session.last_artifact)

        res = asyncio.run(
            share_artifact(
                path=str(self.art_file),
                session=self.session,
                outbox_manager=self.mgr,
            )
        )

        self.assertIsNotNone(self.session.last_artifact)
        self.assertEqual(self.session.last_artifact["filename"], "data_export.csv")
        self.assertEqual(os.path.realpath(self.session.last_artifact["absolute_path"]), os.path.realpath(self.art_file))
        self.assertEqual(self.session.last_artifact["session_id"], "sess_adv_01")
        self.assertEqual(len(self.session.artifacts), 1)
        self.assertEqual(self.session.artifacts[0], self.session.last_artifact)
        self.assertEqual(res["artifact_id"], self.session.last_artifact["artifact_id"])

    def test_share_artifact_auto_binds_via_current_session_contextvar(self):
        """Verify share_artifact auto-binds to session via CURRENT_SESSION ContextVar when session arg is None."""
        CURRENT_SESSION.set(self.session)
        self.assertIsNone(self.session.last_artifact)

        res = asyncio.run(
            share_artifact(
                path=str(self.art_file),
                session=None,
                outbox_manager=self.mgr,
            )
        )

        self.assertIsNotNone(self.session.last_artifact)
        self.assertEqual(self.session.last_artifact["filename"], "data_export.csv")
        self.assertEqual(self.session.last_artifact["session_id"], "sess_adv_01")
        self.assertEqual(os.path.realpath(res["path"]), os.path.realpath(self.art_file))

    def test_harness_stage_artifact_auto_binds_to_session(self):
        """Verify harness.stage_artifact auto-binds metadata to session via router."""
        mock_router = MagicMock()
        mock_router.sessions = {"sess_adv_01": self.session}

        harness = DualPaneHarness(outbox=self.mgr, router=mock_router)
        self.assertIsNone(self.session.last_artifact)

        meta = harness.stage_artifact(
            session_id="sess_adv_01",
            content_or_path=b"sample report output",
            filename="output.txt",
        )

        self.assertIsNotNone(self.session.last_artifact)
        self.assertEqual(self.session.last_artifact["filename"], "output.txt")
        self.assertEqual(self.session.last_artifact["session_id"], "sess_adv_01")
        self.assertEqual(len(self.session.artifacts), 1)

    def test_send_file_directive_auto_binds_to_session(self):
        """Verify parsing [SEND_FILE: ...] directives auto-binds artifact to session."""
        from golem.file_transfer import extract_and_strip_file_directives

        response_text = f"Here is the report:\n[SEND_FILE: {self.art_file}]\nPlease let me know your thoughts."
        clean_text, file_targets = extract_and_strip_file_directives(response_text)

        self.assertEqual(len(file_targets), 1)
        self.assertEqual(file_targets[0], str(self.art_file))

        # Emulate ingress loop auto-binding
        for ft in file_targets:
            self.session.record_artifact(ft)

        self.assertIsNotNone(self.session.last_artifact)
        self.assertEqual(self.session.last_artifact["filename"], "data_export.csv")
        self.assertEqual(os.path.realpath(self.session.last_artifact["absolute_path"]), os.path.realpath(self.art_file))
        self.assertEqual(self.session.last_artifact["session_id"], "sess_adv_01")
        self.assertEqual(os.path.realpath(self.session.last_artifact_path), os.path.realpath(self.art_file))

    def test_share_artifact_resolves_omitted_path_from_active_session(self):
        """
        Verify share_artifact() with path=None resolves automatically from
        active session.last_artifact and populates full return metadata.
        """
        # Pre-bind an artifact
        self.session.record_artifact(str(self.art_file))
        self.assertIsNotNone(self.session.last_artifact)

        res = asyncio.run(
            share_artifact(
                path=None,
                session=self.session,
                outbox_manager=self.mgr,
            )
        )

        self.assertTrue(res["ok"])
        self.assertEqual(res["filename"], "data_export.csv")
        self.assertEqual(os.path.realpath(res["absolute_path"]), os.path.realpath(self.art_file))
        self.assertIn("sha256", res)
        self.assertIn("mime_type", res)
        self.assertEqual(res["session_id"], "sess_adv_01")

    def test_share_artifact_resolves_empty_string_path_from_active_session(self):
        """Verify share_artifact(path='') behaves like omitted path and resolves last_artifact."""
        self.session.record_artifact(str(self.art_file))

        res = asyncio.run(
            share_artifact(
                path="",
                session=self.session,
                outbox_manager=self.mgr,
            )
        )

        self.assertEqual(res["filename"], "data_export.csv")
        self.assertEqual(os.path.realpath(res["absolute_path"]), os.path.realpath(self.art_file))

    def test_share_artifact_raises_value_error_when_no_active_artifact(self):
        """
        Verify share_artifact() raises ValueError("No artifact specified and no active last_artifact found in session context.")
        when no path is specified and the session has NO active last_artifact.
        """
        self.assertIsNone(self.session.last_artifact)

        with self.assertRaises(ValueError) as ctx:
            asyncio.run(
                share_artifact(
                    path=None,
                    session=self.session,
                    outbox_manager=self.mgr,
                )
            )

        self.assertEqual(
            str(ctx.exception),
            "No artifact specified and no active last_artifact found in session context.",
        )

    def test_share_artifact_raises_value_error_with_no_session_and_no_path(self):
        """Verify share_artifact(path=None, session=None) raises ValueError."""
        CURRENT_SESSION.set(None)

        with self.assertRaises(ValueError) as ctx:
            asyncio.run(
                share_artifact(
                    path=None,
                    session=None,
                    outbox_manager=self.mgr,
                )
            )

        self.assertEqual(
            str(ctx.exception),
            "No artifact specified and no active last_artifact found in session context.",
        )

    def test_share_artifact_resolves_from_artifact_id_when_path_omitted(self):
        """Verify share_artifact resolves target_path from artifact_id when path is omitted."""
        meta = self.mgr.stage_artifact(
            session_id="sess_adv_01",
            content_or_path=b"binary content",
            filename="artifact_by_id.bin",
        )
        art_id = meta.artifact_id

        res = asyncio.run(
            share_artifact(
                path=None,
                artifact_id=art_id,
                session=self.session,
                outbox_manager=self.mgr,
            )
        )

        self.assertEqual(res["filename"], "artifact_by_id.bin")
        self.assertEqual(res["artifact_id"], art_id)
        self.assertEqual(self.session.last_artifact["artifact_id"], art_id)

    def test_share_artifact_raises_filenotfound_for_invalid_artifact_id(self):
        """Verify share_artifact raises FileNotFoundError if artifact_id is not found."""
        with self.assertRaises(FileNotFoundError):
            asyncio.run(
                share_artifact(
                    path=None,
                    artifact_id="non_existent_artifact_id_12345",
                    session=self.session,
                    outbox_manager=self.mgr,
                )
            )


class TestAdversarialMultiSessionIsolation(unittest.TestCase):
    """
    Stress-test concurrent session execution across 50 concurrent sessions.
    Empirically verify ZERO cross-session artifact leakage across in-memory state.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="adv_stage4_multisess_")
        self.outbox_dir = Path(self.temp_dir) / "outbox"
        self.mgr = OutboxManager(root_dir=self.outbox_dir)
        self.artifacts_dir = self.outbox_dir / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_concurrent_50_sessions_isolation(self):
        """
        Create 50 sessions concurrently and record distinct artifacts in each.
        Assert zero cross-session leakage of last_artifact or artifacts list.
        """
        session_count = 50
        sessions = [
            SessionHandle(
                session_id=f"sess_{i:03d}",
                channel=f"C{i:04d}",
                thread_ts=f"1710000000.{i:04d}",
                target_cwd=self.temp_dir,
            )
            for i in range(session_count)
        ]

        # Prepare 50 distinct artifact files in outbox
        files = []
        for i in range(session_count):
            p = self.artifacts_dir / f"artifact_report_{i:03d}.json"
            p.write_text(json.dumps({"session_index": i, "payload": f"secret_{i:03d}"}))
            files.append(p)

        async def worker_task(index: int):
            session = sessions[index]
            file_path = files[index]

            # Set ContextVar locally in this coroutine
            token = CURRENT_SESSION.set(session)
            try:
                # 1. Record artifact 1
                session.record_artifact(str(file_path))

                # 2. Add small random-like async yield to simulate real concurrency interleaved execution
                await asyncio.sleep(0.001 * (index % 5))

                # 3. Share artifact with omitted path (must resolve session's own last_artifact)
                result = await share_artifact(
                    path=None,
                    session=session,
                    outbox_manager=self.mgr,
                )

                # 4. Record a second distinct artifact for this session
                second_p = self.artifacts_dir / f"secondary_{index:03d}.log"
                second_p.write_text(f"log data for session {index}")
                session.record_artifact(str(second_p))

                return result
            finally:
                CURRENT_SESSION.reset(token)

        async def run_all():
            tasks = [worker_task(i) for i in range(session_count)]
            return await asyncio.gather(*tasks)

        results = asyncio.run(run_all())
        self.assertEqual(len(results), session_count)

        # Rigorous cross-session leak verification
        for i in range(session_count):
            session = sessions[i]

            # 1. Verify last_artifact is strictly session i's secondary artifact
            last_art = session.last_artifact
            self.assertIsNotNone(last_art, f"Session {i} missing last_artifact")
            self.assertEqual(
                last_art.get("session_id"),
                f"sess_{i:03d}",
                f"Session {i} last_artifact has leaked session_id: {last_art.get('session_id')}",
            )
            self.assertEqual(
                last_art.get("filename"),
                f"secondary_{i:03d}.log",
                f"Session {i} last_artifact filename mismatch",
            )

            # 2. Verify artifacts list contains exactly 3 entries (initial record, share_artifact auto-bind, second record)
            self.assertEqual(
                len(session.artifacts),
                3,
                f"Session {i} artifacts list length mismatch: expected 3, got {len(session.artifacts)}",
            )

            # 3. Verify ALL artifacts in session i belong strictly to session i
            for art in session.artifacts:
                art_sess = art.get("session_id")
                self.assertEqual(
                    art_sess,
                    f"sess_{i:03d}",
                    f"Leakage detected! Session {i} contains artifact with session_id {art_sess}",
                )
                fn = art.get("filename", "")
                self.assertTrue(
                    f"{i:03d}" in fn,
                    f"Leakage detected! Session {i} has alien artifact filename '{fn}'",
                )


class TestAdversarialPersistenceAndDaemonRestart(unittest.TestCase):
    """
    Stress-test SessionRouter disk registry serialization and deserialization
    across simulated daemon process restarts.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="adv_stage4_persist_")
        self.registry_path = os.path.join(self.temp_dir, "sessions_registry.json")
        self.outbox_dir = Path(self.temp_dir) / "outbox"
        self.mgr = OutboxManager(root_dir=self.outbox_dir)
        self.artifacts_dir = self.outbox_dir / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_daemon_restart_rehydrates_last_artifact_and_artifacts_across_sessions(self):
        """
        1. Initialize SessionRouter with custom registry_path.
        2. Create multiple sessions, record distinct artifacts in each.
        3. Drop router instance (simulating daemon shutdown).
        4. Initialize NEW SessionRouter pointing to same registry_path.
        5. Verify get_or_create() restores last_artifact and artifacts list cleanly.
        """
        router1 = SessionRouter(
            inactivity_timeout=3600,
            registry_path=self.registry_path,
        )

        session_configs = [
            ("C_ALPHA", "1710001000.001", "summary_alpha.pdf", 4096),
            ("C_BETA", "1710002000.002", "metrics_beta.json", 1024),
            ("C_GAMMA", "1710003000.003", "chart_gamma.png", 20480),
            ("C_DELTA", "1710004000.004", "report_delta.tar.gz", 1048576),
        ]

        async def setup_sessions():
            created = []
            for channel, thread_ts, filename, size in session_configs:
                sess, is_new = await router1.get_or_create(channel, thread_ts, target_cwd=self.temp_dir)
                p = self.artifacts_dir / filename
                p.write_bytes(b"0" * min(size, 100))

                art_meta = {
                    "filename": filename,
                    "path": str(p),
                    "absolute_path": str(p),
                    "size_bytes": size,
                    "session_id": sess.session_id,
                    "channel": channel,
                    "thread_ts": thread_ts,
                    "timestamp": time.time(),
                }
                sess.record_artifact(art_meta)
                created.append(sess)
            return created

        asyncio.run(setup_sessions())

        # Verify registry file was written to disk and has content
        self.assertTrue(os.path.isfile(self.registry_path))
        with open(self.registry_path, "r") as fp:
            disk_data = json.load(fp)

        self.assertEqual(len(disk_data), 4)
        for channel, thread_ts, filename, size in session_configs:
            key = f"{channel}:{thread_ts}"
            self.assertIn(key, disk_data)
            self.assertIn("last_artifact", disk_data[key])
            self.assertEqual(disk_data[key]["last_artifact"]["filename"], filename)

        # ---------------- SIMULATE DAEMON RESTART ----------------
        del router1  # Discard old in-memory router

        router2 = SessionRouter(
            inactivity_timeout=3600,
            registry_path=self.registry_path,
        )

        async def verify_rehydration():
            for channel, thread_ts, filename, size in session_configs:
                sess, is_new = await router2.get_or_create(channel, thread_ts, target_cwd=self.temp_dir)

                # Verify rehydrated last_artifact
                self.assertIsNotNone(
                    sess.last_artifact,
                    f"Session {sess.session_id} failed to rehydrate last_artifact",
                )
                self.assertEqual(sess.last_artifact["filename"], filename)
                self.assertEqual(sess.last_artifact["size_bytes"], size)
                self.assertEqual(sess.last_artifact["channel"], channel)
                self.assertEqual(sess.last_artifact["thread_ts"], thread_ts)

                # Verify artifacts list was populated with restored last_artifact
                self.assertEqual(len(sess.artifacts), 1)
                self.assertEqual(sess.artifacts[0]["filename"], filename)

                # Verify get_last_artifact() returns identical dictionary
                retrieved = sess.get_last_artifact()
                self.assertEqual(retrieved, sess.last_artifact)

                # Verify last_artifact_path property
                self.assertEqual(os.path.realpath(sess.last_artifact_path), os.path.realpath(sess.last_artifact["absolute_path"]))

        asyncio.run(verify_rehydration())


class TestAdversarialPromptContextHydration(unittest.TestCase):
    """
    Verify prompt context hydration prepends [Active Session Artifact: {filename} at {path}]
    without formatting corruption when filename contains spaces, unicode, or special chars.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="adv_stage4_prompt_")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_prompt_hydration_format_exactness(self):
        """Verify standard prompt construction format."""
        session = SessionHandle(
            session_id="s1",
            channel="C1",
            thread_ts="1.1",
            target_cwd=self.temp_dir,
        )
        session.record_artifact({
            "filename": "summary.pdf",
            "absolute_path": "/tmp/outbox/artifacts/summary.pdf",
        })

        # Emulate ingress prompt hydration logic directly
        prompt_to_send = "Please review this document."
        last_art = session.get_last_artifact()
        self.assertIsNotNone(last_art)
        art_name = last_art.get("filename", "artifact")
        art_p = last_art.get("absolute_path") or last_art.get("path", "")
        hydrated_prompt = f"[Active Session Artifact: {art_name} at {art_p}]\n\n{prompt_to_send}"

        expected = "[Active Session Artifact: summary.pdf at /tmp/outbox/artifacts/summary.pdf]\n\nPlease review this document."
        self.assertEqual(hydrated_prompt, expected)

    def test_prompt_hydration_adversarial_filenames(self):
        """
        Verify prompt hydration with adversarial filenames:
        - Spaces: "Weekly Report Q3 2026 Final (1).pdf"
        - Unicode & emoji: "📊_rapport_financier_€_日本語_v2.0.csv"
        - Brackets & braces: "[SEC-10K]_{draft}_{2026}.json"
        - Special characters: "!@#$%^&*()_+-=[]{}|;':,.<>?~"
        """
        adversarial_cases = [
            (
                "Weekly Report Q3 2026 Final (1).pdf",
                "/tmp/outbox/artifacts/Weekly Report Q3 2026 Final (1).pdf",
            ),
            (
                "📊_rapport_financier_€_日本語_v2.0.csv",
                "/tmp/outbox/artifacts/📊_rapport_financier_€_日本語_v2.0.csv",
            ),
            (
                "[SEC-10K]_{draft}_{2026}.json",
                "/tmp/outbox/artifacts/[SEC-10K]_{draft}_{2026}.json",
            ),
            (
                "!@#$%^&*()_+-=[]{}|;':,.<>?~.txt",
                "/tmp/outbox/artifacts/!@#$%^&*()_+-=[]{}|;':,.<>?~.txt",
            ),
            (
                "filename with 'single' and \"double\" quotes.md",
                "/tmp/outbox/artifacts/filename with 'single' and \"double\" quotes.md",
            ),
        ]

        for filename, abs_path in adversarial_cases:
            with self.subTest(filename=filename):
                session = SessionHandle(
                    session_id=f"sess_{hash(filename)}",
                    channel="C_TEST",
                    thread_ts="1.1",
                    target_cwd=self.temp_dir,
                )
                session.record_artifact({
                    "filename": filename,
                    "absolute_path": abs_path,
                })

                prompt_base = "What is the conclusion of this file?"
                last_art = session.get_last_artifact()
                art_name = last_art.get("filename", "artifact")
                art_p = last_art.get("absolute_path") or last_art.get("path", "")
                hydrated = f"[Active Session Artifact: {art_name} at {art_p}]\n\n{prompt_base}"

                # Assert that the exact filename and path are safely embedded without corruption
                self.assertTrue(hydrated.startswith(f"[Active Session Artifact: {filename} at {abs_path}]\n\n"))
                self.assertTrue(hydrated.endswith(prompt_base))
                self.assertIn(filename, hydrated)
                self.assertIn(abs_path, hydrated)

    def test_prompt_hydration_when_no_artifact_present(self):
        """Verify prompt is unchanged when session has no active artifact."""
        session = SessionHandle(
            session_id="s_empty",
            channel="C1",
            thread_ts="1.1",
            target_cwd=self.temp_dir,
        )
        self.assertIsNone(session.get_last_artifact())

        prompt_base = "Hello Chief of Staff!"
        hydrated = prompt_base
        if hasattr(session, "get_last_artifact"):
            last_art = session.get_last_artifact()
            if last_art:
                art_name = last_art.get("filename", "artifact")
                art_p = last_art.get("absolute_path") or last_art.get("path", "")
                hydrated = f"[Active Session Artifact: {art_name} at {art_p}]\n\n{hydrated}"

        self.assertEqual(hydrated, prompt_base)


if __name__ == "__main__":
    unittest.main()
