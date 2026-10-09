#!/usr/bin/env python3
"""
golem/outbox.py
Outbox Contract, File Staging & Sandboxed Artifact Sharing.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import time
import json
import uuid
import shutil
import hashlib
import asyncio
import logging
import inspect
import mimetypes
import unicodedata
import urllib.parse
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List, Union

try:
    from golem.config import get_outbox_dir
except ImportError:
    def get_outbox_dir() -> Path:
        val = (os.environ.get("GOLEM_OUTBOX_DIR") or "").strip()
        if val:
            return Path(val).resolve()
        return Path.home() / ".golem" / "outbox"

from golem.file_transfer import (
    is_sensitive_target,
    sanitize_filename,
    WINDOWS_DRIVE_PATTERN,
)


UNICODE_DOT_TRANSLATION = str.maketrans({
    "\uff0e": ".",  # Fullwidth full stop
    "\u2024": ".",  # One dot leader
    "\u2025": "..", # Two dot leader
    "\u2026": "..", # Horizontal ellipsis
    "\u3002": ".",  # Ideographic full stop
    "\ufe52": ".",  # Small full stop
    "\uff61": ".",  # Halfwidth ideographic full stop
})

logger = logging.getLogger("Golem.Outbox")


class SandboxSecurityError(PermissionError):
    """Raised when an operation attempts to access or share a file outside ~/.golem/outbox/."""
    pass


class FleetTransferError(RuntimeError):
    """Raised when an SSH/SCP file transfer to or from a remote fleet machine fails."""
    pass


class FleetNodeUnreachableError(FleetTransferError):
    """Raised when a remote fleet node is unreachable or asleep within the connect timeout."""
    pass


@dataclass
class ArtifactMetadata:
    """
    Metadata record for a file staged in or shared through the Golem Outbox.
    """
    artifact_id: str
    filename: str
    relative_path: str
    absolute_path: str
    sha256: str
    size_bytes: int
    created_at: float
    mime_type: Optional[str] = None
    origin_host: str = "local"
    origin_pane: Optional[str] = None
    origin_path: Optional[str] = None
    session_id: Optional[str] = None
    channel: Optional[str] = None
    thread_ts: Optional[str] = None
    staged_by: Optional[str] = None
    shared: bool = False
    shared_at: Optional[float] = None
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata record to a JSON-serializable dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ArtifactMetadata:
        """Construct metadata instance from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def save_meta(self, meta_path: Path) -> None:
        """Save metadata sidecar file as JSON atomically."""
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_meta = meta_path.with_suffix(".tmp")
        with open(tmp_meta, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        os.replace(tmp_meta, meta_path)

    @classmethod
    def load_meta(cls, meta_path: Path) -> Optional[ArtifactMetadata]:
        """Load metadata record from JSON sidecar file."""
        if not meta_path.is_file():
            return None
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return cls.from_dict(data)
        except Exception as e:
            logger.warning(f"Failed to load metadata from {meta_path}: {e}")
            return None


def calculate_sha256(file_path: Union[str, Path]) -> str:
    """Calculate SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def assert_sandboxed_path(
    target_path: Union[str, Path],
    sandbox_root: Optional[Union[str, Path]] = None,
) -> Path:
    """
    Verify that target_path resolves strictly within sandbox_root.
    Resolves all symlinks, directory traversal markers (..), and relative segments.

    Raises:
        SandboxSecurityError: If target_path resolves outside sandbox_root, attempts
                              directory traversal, or points to a protected sensitive file.
        FileNotFoundError: If sandbox_root cannot be accessed.
    """
    if not target_path:
        raise SandboxSecurityError("Empty target path cannot be sandboxed.")

    clean_str = str(target_path).strip().strip("\"'`<>")
    if not clean_str:
        raise SandboxSecurityError("Blank target path cannot be sandboxed.")

    if "\0" in clean_str:
        raise SandboxSecurityError("Null bytes are not permitted in sandboxed paths.")

    if clean_str.startswith("file://"):
        clean_str = clean_str[7:]

    clean_str = clean_str.replace("\\", "/")

    # Defense-in-depth: Unicode normalization and dot homoglyph traversal detection
    norm_str = unicodedata.normalize("NFKC", clean_str).translate(UNICODE_DOT_TRANSLATION)
    if norm_str != clean_str:
        if (
            ".." in norm_str
            or norm_str.startswith("/")
            or WINDOWS_DRIVE_PATTERN.match(norm_str)
            or is_sensitive_target(norm_str)
        ):
            raise SandboxSecurityError(
                f"Unicode dot homoglyph path traversal attempt is not permitted: {clean_str}"
            )

    if WINDOWS_DRIVE_PATTERN.match(clean_str):
        raise SandboxSecurityError("Windows absolute drive paths are not permitted in sandboxed outbox: " + clean_str)

    # Defense-in-depth: early sensitive filename check
    if is_sensitive_target(clean_str):
        raise SandboxSecurityError(f"Access denied: sensitive target '{clean_str}' cannot be accessed or shared.")

    root = Path(sandbox_root or get_outbox_dir()).resolve()

    # Check URL-encoded traversal patterns
    curr_unquoted = clean_str
    while True:
        next_unquoted = urllib.parse.unquote(curr_unquoted)
        if next_unquoted == curr_unquoted:
            break
        curr_unquoted = next_unquoted.replace("\\", "/")
        norm_unquoted = unicodedata.normalize("NFKC", curr_unquoted).translate(UNICODE_DOT_TRANSLATION)
        if "\0" in curr_unquoted:
            raise SandboxSecurityError("Null bytes are not permitted in sandboxed paths.")
        if curr_unquoted.startswith("file://"):
            curr_unquoted = curr_unquoted[7:]
        if (
            ".." in curr_unquoted
            or ".." in norm_unquoted
            or curr_unquoted.startswith("/")
            or norm_unquoted.startswith("/")
            or WINDOWS_DRIVE_PATTERN.match(curr_unquoted)
            or WINDOWS_DRIVE_PATTERN.match(norm_unquoted)
            or is_sensitive_target(curr_unquoted)
            or is_sensitive_target(norm_unquoted)
        ):
            raise SandboxSecurityError(f"URL-encoded path traversal attempt is not permitted: {clean_str}")
        try:
            curr_obj = Path(os.path.expanduser(norm_unquoted))
            res_unquoted = (root / curr_obj).resolve() if not curr_obj.is_absolute() else curr_obj.resolve()
            res_unquoted.relative_to(root)
        except Exception:
            raise SandboxSecurityError(f"URL-encoded path traversal attempt is not permitted: {clean_str}")

    expanded_str = os.path.expanduser(norm_str)
    target_obj = Path(expanded_str)

    if not target_obj.is_absolute():
        resolved_target = (root / target_obj).resolve()
    else:
        resolved_target = target_obj.resolve()

    # Verify strict path containment
    try:
        resolved_target.relative_to(root)
    except ValueError:
        raise SandboxSecurityError(
            f"Access denied: path '{clean_str}' resolves to '{resolved_target}', "
            f"which escapes the outbox sandbox root '{root}'."
        )

    # Defense-in-depth: post-resolution sensitive file check against symlinks
    if is_sensitive_target(str(resolved_target)):
        raise SandboxSecurityError(
            f"Access denied: resolved path '{resolved_target}' references a protected sensitive file."
        )

    return resolved_target


class OutboxManager:
    """
    Manages the Golem Outbox hierarchy rooted at ~/.golem/outbox/ (or GOLEM_OUTBOX_DIR).
    Provides structured file staging, metadata generation, cross-machine fleet transfer,
    and sandboxed sharing.
    """

    def __init__(
        self,
        root_dir: Optional[Union[str, Path]] = None,
        fleet_mesh: Optional[Any] = None,
    ):
        self.root_dir = Path(root_dir or get_outbox_dir()).resolve()
        self.artifacts_dir = self.root_dir / "artifacts"
        self.staging_dir = self.root_dir / "staging"
        self.downloads_dir = self.root_dir / "downloads"
        self.meta_dir = self.root_dir / "meta"
        self.fleet_mesh = fleet_mesh
        self.ensure_dirs()

    def ensure_dirs(self) -> None:
        """Ensure all primary outbox directory trees exist on disk."""
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)

    def get_session_artifact_dir(self, session_id: str) -> Path:
        """Return dedicated artifact directory for a conversational session."""
        safe_sess = sanitize_filename(session_id.replace(":", "_").replace("/", "_"))
        sess_dir = self.artifacts_dir / safe_sess
        sess_dir.mkdir(parents=True, exist_ok=True)
        return sess_dir

    def get_machine_staging_dir(self, machine: str, direction: str = "inbound") -> Path:
        """Return staging directory for a specific fleet machine."""
        safe_node = sanitize_filename(machine)
        safe_dir = sanitize_filename(direction)
        m_dir = self.staging_dir / safe_node / safe_dir
        m_dir.mkdir(parents=True, exist_ok=True)
        return m_dir

    def stage_artifact(
        self,
        session_id: str,
        content_or_path: Union[bytes, str, Path],
        filename: str,
        origin_host: str = "local",
        origin_pane: Optional[str] = None,
        origin_path: Optional[str] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        staged_by: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> ArtifactMetadata:
        """
        Stage a local file or binary buffer into the session's outbox directory.
        Calculates SHA-256, generates metadata, and writes the sidecar JSON.
        """
        sess_dir = self.get_session_artifact_dir(session_id)
        safe_name = sanitize_filename(filename)
        dest_path = sess_dir / safe_name

        # Avoid accidental overwrites with collision-resistant naming
        if dest_path.exists():
            nonce = uuid.uuid4().hex[:6]
            stem = dest_path.stem
            suffix = dest_path.suffix
            dest_path = sess_dir / f"{stem}_{nonce}{suffix}"

        # Atomic file write
        tmp_dest = dest_path.with_suffix(dest_path.suffix + ".tmp")
        if isinstance(content_or_path, bytes):
            with open(tmp_dest, "wb") as f:
                f.write(content_or_path)
        else:
            src = Path(content_or_path).resolve()
            if not src.is_file():
                raise FileNotFoundError(f"Source file does not exist: {src}")
            shutil.copy2(src, tmp_dest)

        os.replace(tmp_dest, dest_path)

        file_size = dest_path.stat().st_size
        sha256 = calculate_sha256(dest_path)
        mime_type, _ = mimetypes.guess_type(str(dest_path))
        artifact_id = f"art_{int(time.time())}_{uuid.uuid4().hex[:8]}"

        rel_path = str(dest_path.relative_to(self.root_dir))

        meta = ArtifactMetadata(
            artifact_id=artifact_id,
            filename=dest_path.name,
            relative_path=rel_path,
            absolute_path=str(dest_path),
            sha256=sha256,
            size_bytes=file_size,
            created_at=time.time(),
            mime_type=mime_type or "application/octet-stream",
            origin_host=origin_host,
            origin_pane=origin_pane,
            origin_path=origin_path or str(content_or_path if not isinstance(content_or_path, bytes) else dest_path),
            session_id=session_id,
            channel=channel,
            thread_ts=thread_ts,
            staged_by=staged_by or "agy",
            tags=tags or [],
        )

        meta_file = self.meta_dir / f"{artifact_id}.json"
        meta.save_meta(meta_file)
        logger.info(f"Staged artifact '{meta.filename}' ({artifact_id}) to {dest_path}")

        try:
            from golem.state import get_default_task_ledger
            ledger = get_default_task_ledger()
            ledger.record_artifact(
                artifact_id=artifact_id,
                task_id=session_id or "standalone",
                channel=channel or "default",
                thread_ts=thread_ts or "default",
                rel_path=rel_path,
                abs_path=str(dest_path),
                sha256=sha256,
                size_bytes=file_size,
            )
        except Exception as le:
            logger.debug(f"TaskLedger record_artifact notice: {le}")

        return meta

    async def stage_from_remote(
        self,
        machine: str,
        remote_path: str,
        session_id: Optional[str] = None,
        ssh_user: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
    ) -> ArtifactMetadata:
        """
        Fetch a file from a remote fleet node via SCP into ~/.golem/outbox/staging/<machine>/inbound/.
        Preserves original extension, calculates SHA-256, and records persistent metadata.
        """
        clean_remote = remote_path.strip().strip("\"'`<>")
        if not clean_remote:
            raise ValueError("remote_path must be specified.")

        if is_sensitive_target(clean_remote):
            raise SandboxSecurityError(f"Remote file '{clean_remote}' is a protected sensitive file.")

        # Resolve node configuration via FleetMesh
        node_info = None
        mesh = self.fleet_mesh
        if mesh is None:
            try:
                from golem.mesh import FleetMesh
                mesh = FleetMesh()
            except Exception:
                mesh = None

        if mesh and hasattr(mesh, "resolve_node"):
            try:
                node_info = mesh.resolve_node(machine)
            except Exception as e:
                logger.warning(f"Could not resolve fleet node '{machine}': {e}")

        target_host = machine
        if node_info:
            target_host = node_info.get("ssh_target") or node_info.get("target_machine") or machine
            ssh_user = ssh_user or node_info.get("ssh_user")

        # Laptop has strict ad-hoc timeout
        if timeout_seconds is None:
            timeout_seconds = 3.0 if machine in ("personal-laptop", "dev-grid", "laptop") else 10.0

        inbound_dir = self.get_machine_staging_dir(machine, "inbound")
        # Handle Windows backslashes
        normalized_remote = clean_remote.replace("\\", "/")
        base_name = sanitize_filename(os.path.basename(normalized_remote))
        timestamp_prefix = int(time.time())
        dest_filename = f"{timestamp_prefix}_{base_name}"
        dest_path = inbound_dir / dest_filename
        tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

        dest_scp = f"{ssh_user}@{target_host}:{clean_remote}" if ssh_user else f"{target_host}:{clean_remote}"
        cmd = [
            "scp",
            "-o", "BatchMode=yes",
            "-o", f"ConnectTimeout={int(timeout_seconds)}",
            dest_scp,
            str(tmp_path),
        ]

        logger.info(f"Executing fleet SCP fetch: {' '.join(cmd)}")
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds + 5.0)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
                if machine in ("personal-laptop", "dev-grid", "laptop"):
                    raise FleetNodeUnreachableError(
                        f"Personal laptop '{machine}' timed out ({timeout_seconds}s); workstation is likely asleep."
                    )
                raise FleetNodeUnreachableError(
                    f"SCP fetch from '{dest_scp}' timed out after {timeout_seconds}s."
                )

            if proc.returncode != 0:
                err_text = stderr.decode(errors="replace") if stderr else ""
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
                if "No route to host" in err_text or "Connection refused" in err_text or "Operation timed out" in err_text:
                    raise FleetNodeUnreachableError(f"Fleet machine '{machine}' is unreachable: {err_text.strip()}")
                raise FleetTransferError(f"SCP fetch failed (code {proc.returncode}): {err_text.strip()}")

            if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
                raise FleetTransferError(f"Fetched remote file '{clean_remote}' is empty (0 bytes) or was not created.")

            os.replace(tmp_path, dest_path)

        except (FleetTransferError, FleetNodeUnreachableError):
            raise
        except Exception as e:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise FleetTransferError(f"Unexpected error staging remote file from {machine}: {e}") from e

        file_size = dest_path.stat().st_size
        sha256 = calculate_sha256(dest_path)
        mime_type, _ = mimetypes.guess_type(str(dest_path))
        artifact_id = f"art_rem_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        rel_path = str(dest_path.relative_to(self.root_dir))

        meta = ArtifactMetadata(
            artifact_id=artifact_id,
            filename=dest_path.name,
            relative_path=rel_path,
            absolute_path=str(dest_path),
            sha256=sha256,
            size_bytes=file_size,
            created_at=time.time(),
            mime_type=mime_type or "application/octet-stream",
            origin_host=machine,
            origin_path=clean_remote,
            session_id=session_id,
            channel=channel,
            thread_ts=thread_ts,
            staged_by="fleet-scp",
            tags=["remote", machine],
        )

        meta_file = self.meta_dir / f"{artifact_id}.json"
        meta.save_meta(meta_file)
        logger.info(f"Staged remote file from {machine}:{clean_remote} to {dest_path} ({artifact_id})")
        return meta

    async def stage_to_remote(
        self,
        machine: str,
        source_path: Union[str, Path],
        remote_dest: str,
        ssh_user: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> ArtifactMetadata:
        """
        Stage a local file and transfer it to a remote fleet machine via SCP.
        """
        src = Path(source_path).resolve()
        if not src.is_file():
            raise FileNotFoundError(f"Source file does not exist: {src}")

        if is_sensitive_target(str(src)):
            raise SandboxSecurityError(f"Source file '{src}' is a protected sensitive file.")

        # Stage in local outbound queue
        outbound_dir = self.get_machine_staging_dir(machine, "outbound")
        staged_src = outbound_dir / src.name
        shutil.copy2(src, staged_src)

        node_info = None
        mesh = self.fleet_mesh
        if mesh and hasattr(mesh, "resolve_node"):
            try:
                node_info = mesh.resolve_node(machine)
            except Exception:
                pass

        target_host = machine
        if node_info:
            target_host = node_info.get("ssh_target") or node_info.get("target_machine") or machine
            ssh_user = ssh_user or node_info.get("ssh_user")

        if timeout_seconds is None:
            timeout_seconds = 3.0 if machine in ("personal-laptop", "dev-grid", "laptop") else 10.0

        dest_scp = f"{ssh_user}@{target_host}:{remote_dest}" if ssh_user else f"{target_host}:{remote_dest}"
        cmd = [
            "scp",
            "-o", "BatchMode=yes",
            "-o", f"ConnectTimeout={int(timeout_seconds)}",
            str(staged_src),
            dest_scp,
        ]

        logger.info(f"Executing fleet SCP push: {' '.join(cmd)}")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds + 5.0)
        except asyncio.TimeoutError:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            raise FleetNodeUnreachableError(f"SCP push to '{dest_scp}' timed out after {timeout_seconds}s.")

        if proc.returncode != 0:
            err_text = stderr.decode(errors="replace") if stderr else ""
            raise FleetTransferError(f"SCP push to {dest_scp} failed (code {proc.returncode}): {err_text.strip()}")

        artifact_id = f"art_push_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        meta = ArtifactMetadata(
            artifact_id=artifact_id,
            filename=staged_src.name,
            relative_path=str(staged_src.relative_to(self.root_dir)),
            absolute_path=str(staged_src),
            sha256=calculate_sha256(staged_src),
            size_bytes=staged_src.stat().st_size,
            created_at=time.time(),
            origin_host="local",
            origin_path=str(src),
            tags=["push", machine],
        )
        meta.save_meta(self.meta_dir / f"{artifact_id}.json")
        return meta

    def list_artifacts(self, session_id: Optional[str] = None) -> List[ArtifactMetadata]:
        """List all tracked artifacts, optionally filtered by session_id."""
        results: List[ArtifactMetadata] = []
        if not self.meta_dir.is_dir():
            return results

        for meta_file in sorted(self.meta_dir.glob("*.json"), reverse=True):
            meta = ArtifactMetadata.load_meta(meta_file)
            if meta:
                if session_id is None or meta.session_id == session_id:
                    results.append(meta)
        return results

    def get_artifact(self, artifact_id: str) -> Optional[ArtifactMetadata]:
        """Retrieve metadata for a specific artifact ID."""
        meta_file = self.meta_dir / f"{artifact_id}.json"
        return ArtifactMetadata.load_meta(meta_file)

    async def share_artifact(
        self,
        path: Optional[Union[str, Path]] = None,
        client: Optional[Any] = None,
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        title: Optional[str] = None,
        artifact_id: Optional[str] = None,
        session_id: Optional[str] = None,
        session: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Convenience method delegating to share_artifact with this manager."""
        return await share_artifact(
            path=path,
            client=client,
            channel=channel,
            thread_ts=thread_ts,
            title=title,
            outbox_manager=self,
            artifact_id=artifact_id,
            session_id=session_id,
            session=session,
        )


# Global default outbox manager singleton
_DEFAULT_OUTBOX_MANAGER: Optional[OutboxManager] = None


def get_default_outbox_manager() -> OutboxManager:
    """Return or initialize global default OutboxManager instance."""
    global _DEFAULT_OUTBOX_MANAGER
    if _DEFAULT_OUTBOX_MANAGER is None:
        _DEFAULT_OUTBOX_MANAGER = OutboxManager()
    return _DEFAULT_OUTBOX_MANAGER


async def share_artifact(
    path: Optional[Union[str, Path]] = None,
    client: Optional[Any] = None,
    channel: Optional[str] = None,
    thread_ts: Optional[str] = None,
    title: Optional[str] = None,
    outbox_manager: Optional[OutboxManager] = None,
    artifact_id: Optional[str] = None,
    session_id: Optional[str] = None,
    session: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Share an artifact strictly sandboxed to ~/.golem/outbox/ with Slack or BlueBubbles.
    Enforces path traversal defenses: raises SandboxSecurityError (PermissionError)
    if path resolves outside the outbox directory.

    If path is omitted, automatically resolves artifact_id or session's last_artifact.
    Updates metadata to mark shared=True upon successful upload.
    Auto-binds last_artifact context to active session.
    Satisfies Stage 4 Channel-Context Artifact Sandboxing invariant.
    Delegates to golem.tools.delivery.share_artifact.
    """
    from golem.tools.delivery import share_artifact as _delivery_share
    return await _delivery_share(
        path=path,
        client=client,
        channel=channel,
        thread_ts=thread_ts,
        title=title,
        outbox_manager=outbox_manager,
        artifact_id=artifact_id,
        session_id=session_id,
        session=session,
    )


__all__ = [
    "ArtifactMetadata",
    "SandboxSecurityError",
    "FleetTransferError",
    "FleetNodeUnreachableError",
    "calculate_sha256",
    "assert_sandboxed_path",
    "OutboxManager",
    "get_default_outbox_manager",
    "share_artifact",
]
