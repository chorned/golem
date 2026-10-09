#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

golem/tools/delivery.py
Channel-Context Artifact Delivery Engine:
  - Sandboxed artifact delivery to Slack and BlueBubbles.
  - Strict path traversal defense enforcing containment in ~/.golem/outbox/.
  - Session auto-binding and fallback resolution (path, artifact_id, session context, or SQLite ledger).
  - Synchronous and asynchronous registration with SQLite TaskLedger.
"""

from __future__ import annotations

import os
import sys
import time
import uuid
import inspect
import logging
import mimetypes
from pathlib import Path
from typing import Dict, Any, Optional, Union

logger = logging.getLogger("Golem.Delivery")

from golem.outbox import (
    assert_sandboxed_path,
    calculate_sha256,
    get_default_outbox_manager,
    SandboxSecurityError,
    ArtifactMetadata,
)


async def share_artifact(
    path: Optional[Union[str, Path]] = None,
    client: Optional[Any] = None,
    channel: Optional[str] = None,
    thread_ts: Optional[str] = None,
    title: Optional[str] = None,
    outbox_manager: Optional[Any] = None,
    artifact_id: Optional[str] = None,
    session_id: Optional[str] = None,
    session: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Share an artifact strictly sandboxed to ~/.golem/outbox/ with Slack or BlueBubbles.
    Enforces path containment, resolves omitted paths from session or SQLite TaskLedger,
    uploads via client, and records the artifact in the persistent task ledger.
    """
    mgr = outbox_manager or get_default_outbox_manager()

    # 1. Resolve active session from argument or contextvar
    active_session = session
    if active_session is None:
        try:
            from golem.session import get_current_session
            active_session = get_current_session()
        except Exception:
            pass

    # 2. Resolve target artifact path
    target_path = path

    # If path was omitted or empty string, resolve from available contexts
    if not target_path:
        # A. Look up by artifact_id in SQLite TaskLedger first, then OutboxManager
        if artifact_id:
            try:
                from golem.state import get_default_task_ledger
                ledger = get_default_task_ledger()
                art_row = ledger.get_artifact(artifact_id)
                if art_row:
                    target_path = art_row.get("abs_path")
                    if not session_id and art_row.get("channel"):
                        channel = channel or art_row.get("channel")
                        thread_ts = thread_ts or art_row.get("thread_ts")
            except Exception as e:
                logger.debug(f"TaskLedger artifact lookup: {e}")

            if not target_path:
                meta_rec = mgr.get_artifact(artifact_id)
                if meta_rec:
                    target_path = meta_rec.absolute_path
                    if not session_id and meta_rec.session_id:
                        session_id = meta_rec.session_id
                else:
                    raise FileNotFoundError(f"Artifact ID '{artifact_id}' not found.")

        # B. Resolve from active session's last_artifact
        elif active_session is not None and hasattr(active_session, "get_last_artifact"):
            last_art = active_session.get_last_artifact()
            if last_art:
                if isinstance(last_art, dict):
                    target_path = last_art.get("absolute_path") or last_art.get("path")
                    if not artifact_id:
                        artifact_id = last_art.get("artifact_id")
                    if not session_id:
                        session_id = last_art.get("session_id")
                elif hasattr(last_art, "absolute_path"):
                    target_path = last_art.absolute_path
                    if not artifact_id:
                        artifact_id = getattr(last_art, "artifact_id", None)
                    if not session_id:
                        session_id = getattr(last_art, "session_id", None)
                elif isinstance(last_art, str):
                    target_path = last_art

        # C. Resolve from SQLite TaskLedger by channel + thread_ts
        elif channel and thread_ts:
            try:
                from golem.state import get_default_task_ledger
                ledger = get_default_task_ledger()
                art_row = ledger.get_last_artifact_for_thread(channel, thread_ts)
                if art_row:
                    target_path = art_row.get("abs_path")
                    if not artifact_id:
                        artifact_id = art_row.get("artifact_id")
            except Exception as e:
                logger.debug(f"TaskLedger thread lookup: {e}")

    if not target_path:
        raise ValueError("No artifact specified and no active last_artifact found in session context.")

    # 3. Enforce strict sandbox containment
    resolved_path = assert_sandboxed_path(target_path, sandbox_root=mgr.root_dir)

    if not resolved_path.is_file():
        raise FileNotFoundError(f"Artifact does not exist at {resolved_path}")

    filename = resolved_path.name
    file_size = resolved_path.stat().st_size
    sha256 = calculate_sha256(resolved_path)
    mime_type_val, _ = mimetypes.guess_type(str(resolved_path))
    mime_type = mime_type_val or "application/octet-stream"
    rel_path = str(resolved_path.relative_to(mgr.root_dir))
    upload_title = title or f"Artifact: {filename}"

    # 4. Perform client upload if client and channel provided
    is_shared = False
    shared_at = None
    upload_resp_data = None

    if client is not None and channel:
        upload_kwargs: Dict[str, Any] = {
            "channel": channel,
            "file": str(resolved_path),
            "filename": filename,
            "title": upload_title,
        }
        if thread_ts:
            upload_kwargs["thread_ts"] = thread_ts

        if hasattr(client, "files_upload_v2"):
            res = client.files_upload_v2(**upload_kwargs)
            resp = await res if inspect.isawaitable(res) else res
        elif hasattr(client, "upload_file"):
            res = client.upload_file(**upload_kwargs)
            resp = await res if inspect.isawaitable(res) else res
        else:
            raise AttributeError("Client does not provide files_upload_v2 or upload_file method.")

        upload_resp_data = (
            resp if isinstance(resp, dict) else (
                resp.data if hasattr(resp, "data") else {"ok": True, "file": filename}
            )
        )
        is_shared = True
        shared_at = time.time()

    file_info = (
        (upload_resp_data.get("file") if isinstance(upload_resp_data, dict) else filename)
        if upload_resp_data
        else filename
    )

    # 5. Resolve existing metadata or artifact identifier
    matched_meta = None
    if artifact_id and mgr and hasattr(mgr, "get_artifact"):
        matched_meta = mgr.get_artifact(artifact_id)
    if matched_meta is None and mgr and hasattr(mgr, "list_artifacts"):
        for meta in mgr.list_artifacts():
            if meta.absolute_path == str(resolved_path):
                matched_meta = meta
                break

    if matched_meta:
        artifact_id = matched_meta.artifact_id
        if not session_id and matched_meta.session_id:
            session_id = matched_meta.session_id

    # If still not found, check active_session
    if not artifact_id and active_session and hasattr(active_session, "last_artifact"):
        last_art = getattr(active_session, "last_artifact", None)
        if isinstance(last_art, dict) and (last_art.get("absolute_path") == str(resolved_path) or last_art.get("path") == str(resolved_path)):
            artifact_id = last_art.get("artifact_id")

    # If still no artifact_id, generate a new one
    if not artifact_id:
        artifact_id = f"art_{int(time.time())}_{uuid.uuid4().hex[:8]}"

    resolved_session_id = session_id or (getattr(active_session, "session_id", None) if active_session else "standalone")

    # 6. Update sidecar metadata in outbox manager if available
    if mgr and hasattr(mgr, "meta_dir") and mgr.meta_dir:
        try:
            if matched_meta:
                if is_shared:
                    matched_meta.shared = True
                    matched_meta.shared_at = shared_at
                    if channel:
                        matched_meta.channel = channel
                    if thread_ts:
                        matched_meta.thread_ts = thread_ts
                if resolved_session_id and not matched_meta.session_id:
                    matched_meta.session_id = resolved_session_id
                meta_file = mgr.meta_dir / f"{matched_meta.artifact_id}.json"
                matched_meta.save_meta(meta_file)
            else:
                new_meta = ArtifactMetadata(
                    artifact_id=artifact_id,
                    filename=filename,
                    relative_path=rel_path,
                    absolute_path=str(resolved_path),
                    sha256=sha256,
                    size_bytes=file_size,
                    created_at=time.time(),
                    mime_type=mime_type,
                    session_id=resolved_session_id,
                    channel=channel,
                    thread_ts=thread_ts,
                    shared=is_shared,
                    shared_at=shared_at,
                    staged_by="share_artifact",
                    tags=["shared"] if is_shared else [],
                )
                meta_file = mgr.meta_dir / f"{artifact_id}.json"
                new_meta.save_meta(meta_file)
        except Exception as se:
            logger.debug(f"Outbox sidecar sync notice: {se}")

    # 7. Record in persistent SQLite TaskLedger
    try:
        from golem.state import get_default_task_ledger
        ledger = get_default_task_ledger()
        ledger.record_artifact(
            artifact_id=artifact_id,
            task_id=resolved_session_id,
            channel=channel or "default",
            thread_ts=thread_ts or "default",
            rel_path=rel_path,
            abs_path=str(resolved_path),
            sha256=sha256,
            size_bytes=file_size,
        )
    except Exception as le:
        logger.debug(f"TaskLedger record_artifact notice: {le}")

    # 8. Construct metadata dictionary
    meta_dict: Dict[str, Any] = {
        "ok": True,
        "artifact_id": artifact_id,
        "filename": filename,
        "path": str(resolved_path),
        "absolute_path": str(resolved_path),
        "relative_path": rel_path,
        "sha256": sha256,
        "size_bytes": file_size,
        "size": file_size,
        "mime_type": mime_type,
        "created_at": time.time(),
        "shared": is_shared,
        "shared_at": shared_at,
        "session_id": resolved_session_id,
        "channel": channel,
        "thread_ts": thread_ts,
        "title": upload_title,
        "upload_metadata": upload_resp_data,
        "slack_response": upload_resp_data,
        "file": file_info,
        "directive": f"[SEND_FILE: {resolved_path}]",
    }

    # 9. Auto-bind to active session
    if active_session is not None and hasattr(active_session, "record_artifact"):
        try:
            active_session.record_artifact(meta_dict)
        except Exception as e:
            logger.debug(f"Error binding artifact to session: {e}")

    return meta_dict

