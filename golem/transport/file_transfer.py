#!/usr/bin/env python3
"""
golem/transport/file_transfer.py
Outbound Slack file directive handling, target path resolution (local candidates,
slack_transfers/, and remote fleet SCP), and files_upload_v2 execution.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import tempfile
import asyncio
import logging
import inspect
from typing import Tuple, List, Optional, Dict, Any, Set

logger = logging.getLogger("Golem.Transport.FileTransfer")

# Directive extraction regex supporting [SEND_FILE: <path>], [UPLOAD_FILE: <path>], [ATTACH_FILE: <path>]
# and optional surrounding quotes, backticks, asterisks (bold/italic), and flexible spacing.
DIRECTIVE_PATTERN = re.compile(
    r"(?P<lead_space>[ \t]*)"
    r"(?P<wrap>\*{1,3}|`{1,3}|[\"'])?"
    r"\[[ \t]*(?:SEND_FILE|UPLOAD_FILE|ATTACH_FILE)[ \t]*:[ \t]*(?P<path>[^\]]*?)\]"
    r"(?(wrap)(?P=wrap)|)"
    r"(?P<trail_space>[ \t]*)",
    re.IGNORECASE,
)

# Markdown fenced code block pattern (```...``` or ~~~...~~~), requiring newline after opening fence
FENCED_CODE_BLOCK_PATTERN = re.compile(
    r"(?ms)^[ \t]*(?P<fence>`{3,}|~{3,})[^\n]*\n.*?(?:^[ \t]*(?P=fence)[ \t]*(?=\n|\Z))"
)

# Windows drive letter pattern (e.g. C:\... or c:/...)
WINDOWS_DRIVE_PATTERN = re.compile(r"^[a-zA-Z]:[\\/]")

# Sensitive files that must never be exfiltrated to Slack channels
SENSITIVE_FILENAME_PATTERNS = [
    re.compile(r"(?:^|[/\\]|:)(?:\.env(?:\..*)?|id_rsa|id_ed25519|id_ecdsa|id_dsa|credentials\.json)$", re.IGNORECASE),
]

TRANSFERS_DIR_NAME = "slack_transfers"


def is_sensitive_target(target_str: str) -> bool:
    """
    Check whether a target path or resolved file path points to a sensitive file
    (.env, id_rsa, id_ed25519, credentials.json, etc.).
    Handles fleet prefixes, Windows drive paths, and symlink targets.
    """
    if not target_str:
        return False
    clean = target_str.strip().strip("\"'`<>")
    if not clean:
        return False

    if any(p.search(clean) for p in SENSITIVE_FILENAME_PATTERNS):
        return True

    is_win = bool(WINDOWS_DRIVE_PATTERN.match(clean))
    path_part = clean.split(":", 1)[1] if (not is_win and ":" in clean) else clean

    if any(p.search(path_part) for p in SENSITIVE_FILENAME_PATTERNS):
        return True

    base_name = os.path.basename(path_part.replace("\\", "/"))
    if any(p.search(base_name) for p in SENSITIVE_FILENAME_PATTERNS):
        return True

    if (
        base_name.lower() in {".env", "id_rsa", "id_ed25519", "id_ecdsa", "id_dsa", "credentials.json"}
        or base_name.lower().startswith(".env.")
    ):
        return True

    return False


def extract_and_strip_file_directives(text: str) -> Tuple[str, List[str]]:
    """
    Extract file paths from [SEND_FILE: <path>], [UPLOAD_FILE: <path>], or [ATTACH_FILE: <path>]
    directives and return (clean_text, target_paths).

    Strips directive markup from text, collapsing excessive newlines and whitespace
    while preserving indentation, code blocks, and markdown tables intact.
    """
    if not text:
        return "", []

    # 1. Mask fenced code blocks to prevent false-positive extraction or code mutation
    code_blocks: List[str] = []
    def _mask_code_block(m: re.Match) -> str:
        idx = len(code_blocks)
        code_blocks.append(m.group(0))
        return f"__FM_CODE_BLOCK_{idx}__"

    masked_text = FENCED_CODE_BLOCK_PATTERN.sub(_mask_code_block, text)

    # 2. Extract directive target paths from non-code text
    target_paths: List[str] = []
    for match in DIRECTIVE_PATTERN.finditer(masked_text):
        raw_path = match.group("path") if "path" in match.groupdict() else match.group(1)
        clean_path = raw_path.strip().strip("\"'`<>")
        if clean_path:
            target_paths.append(clean_path)

    # 3. Strip directive markup cleanly without corrupting adjacent punctuation or words
    def _repl(m: re.Match) -> str:
        start, end = m.span()
        lead = m.group("lead_space")
        trail = m.group("trail_space")

        prev_char = masked_text[start - 1] if start > 0 else ""
        next_char = masked_text[end] if end < len(masked_text) else ""

        # Avoid word concatenation when directive had no spaces:
        if not lead and not trail:
            if (prev_char.isalnum() or prev_char == ":") and (next_char.isalnum() or next_char == ":"):
                return " "

        # If preceding had space and next is punctuation/closing delimiter/newline, drop space
        if lead and next_char in ".,;!?\"'`\n\r":
            return ""

        # Normalize adjacent spaces to single space
        if lead and trail:
            return " "
        if lead or trail:
            return " "

        return ""

    clean_text = DIRECTIVE_PATTERN.sub(_repl, masked_text)

    # 4. Clean up orphaned bullet markers and blockquotes
    clean_text = re.sub(r"^[ \t]*(?:[-*•>]|\d+\.)[ \t]*$\n?", "", clean_text, flags=re.MULTILINE)

    # 5. Clean up empty markdown bold markers like ****
    clean_text = re.sub(r"\*{2,}[ \t]*\*{2,}", "", clean_text)
    clean_text = re.sub(r"^[ \t]*\*{4,}[ \t]*$\n?", "", clean_text, flags=re.MULTILINE)

    # 6. Blank lines with only whitespace become truly empty lines
    clean_text = re.sub(r"^[ \t]+$", "", clean_text, flags=re.MULTILINE)

    # 7. Collapse 3 or more consecutive newlines to 2
    clean_text = re.sub(r"\n{3,}", "\n\n", clean_text)

    clean_text = clean_text.strip()

    # 8. Restore fenced code blocks
    for idx, block in enumerate(code_blocks):
        clean_text = clean_text.replace(f"__FM_CODE_BLOCK_{idx}__", block)

    return clean_text, target_paths


async def fetch_remote_file(
    target: str,
    remote_path: str,
    ssh_user: Optional[str] = None,
    timeout_seconds: float = 30.0,
) -> Optional[str]:
    """
    Fetch a remote file over SCP to a local temporary file for uploading to Slack.
    Preserves original file extension suffix.
    Deletes temporary file on failure, timeout, or empty file.
    """
    if not target or not remote_path:
        return None

    clean_remote = remote_path.strip().strip("\"'`<>")
    suffix = os.path.splitext(clean_remote)[1]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp_path = tmp.name
    tmp.close()

    dest = f"{ssh_user}@{target}:{clean_remote}" if ssh_user else f"{target}:{clean_remote}"
    cmd = [
        "scp",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        dest,
        tmp_path,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            logger.error(f"Timeout ({timeout_seconds}s) fetching remote file via scp from {dest}")
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            return None

        if proc.returncode == 0 and os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
            return tmp_path
        else:
            err_msg = stderr.decode(errors="replace") if stderr else ""
            logger.error(f"scp failed with returncode {proc.returncode} from {dest}: {err_msg}")
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            return None

    except Exception as e:
        logger.error(f"Error executing scp from {dest}: {e}")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        return None


def resolve_local_file_path(target_str: str, active_cwd: str) -> Optional[str]:
    """
    Search candidate locations for a local file:
    1) target_str if absolute else os.path.join(active_cwd, target_str)
    2) os.path.join(active_cwd, TRANSFERS_DIR_NAME, target_str)
    3) os.path.join(os.getcwd(), target_str)
    Returns normalized absolute path if found and is a regular file, else None.
    """
    if not target_str:
        return None

    clean_target = target_str.strip().strip("\"'`<>")
    expanded = os.path.expanduser(clean_target)

    candidates: List[str] = []
    if os.path.isabs(expanded):
        candidates.append(expanded)
        base_name = os.path.basename(expanded)
        if active_cwd:
            candidates.append(os.path.join(active_cwd, base_name))
            candidates.append(os.path.join(active_cwd, TRANSFERS_DIR_NAME, base_name))
    else:
        if active_cwd:
            candidates.append(os.path.join(active_cwd, expanded))
            candidates.append(os.path.join(active_cwd, TRANSFERS_DIR_NAME, expanded))
        candidates.append(os.path.join(os.getcwd(), expanded))

    for cand in candidates:
        norm = os.path.realpath(os.path.abspath(cand))
        if os.path.isfile(norm):
            return norm

    return None


async def resolve_file_path(
    target_str: str,
    active_cwd: str,
    fleet_mesh: Optional[Any] = None,
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Resolve local path or remote fleet path.
    Disambiguates Windows drive letter (^[a-zA-Z]:[\\/]) vs fleet node (<node>:<path>).
    Returns (resolved_path, node_label, is_temporary).
    """
    if not target_str:
        return None, None, False

    clean_target = target_str.strip().strip("\"'`<>")
    if not clean_target:
        return None, None, False

    # Check for Windows drive letter (e.g. C:\... or C:/...)
    is_windows_drive = bool(WINDOWS_DRIVE_PATTERN.match(clean_target))

    if not is_windows_drive and ":" in clean_target:
        node_prefix, remote_path = clean_target.split(":", 1)
        node_prefix = node_prefix.strip()
        remote_path = remote_path.strip().strip("\"'`<>")

        # Resolve node via fleet mesh
        node_info = None
        mesh = fleet_mesh
        if mesh is None:
            try:
                from golem.execution.mesh import FleetMesh
                mesh = FleetMesh()
            except Exception:
                mesh = None

        if mesh and hasattr(mesh, "resolve_node"):
            try:
                node_info = mesh.resolve_node(node_prefix)
            except (KeyError, Exception) as e:
                logger.warning(f"Could not resolve fleet node '{node_prefix}': {e}")
                node_info = None

        if node_info:
            target_machine = node_info.get("target_machine", "")
            node_label = node_info.get("label") or node_prefix

            if target_machine == "local":
                # Evaluates on local disk
                local_path = resolve_local_file_path(remote_path, active_cwd)
                if local_path:
                    return local_path, node_label, False
                return None, node_label, False
            else:
                # Remote machine: fetch via SCP
                ssh_target = node_info.get("ssh_target") or target_machine
                ssh_user = node_info.get("ssh_user")
                tmp_path = await fetch_remote_file(
                    target=ssh_target,
                    remote_path=remote_path,
                    ssh_user=ssh_user,
                )
                if tmp_path and os.path.isfile(tmp_path):
                    return tmp_path, node_label, True
                return None, node_label, False

        # If node lookup failed, fallback to local candidate check on clean_target
        local_path = resolve_local_file_path(clean_target, active_cwd)
        if local_path:
            return local_path, "local", False
        return None, None, False

    # Local path resolution
    local_path = resolve_local_file_path(clean_target, active_cwd)
    if local_path:
        return local_path, "local", False

    return None, None, False


async def upload_directive_files(
    client: Any,
    channel: str,
    thread_ts: Optional[str],
    targets: List[str],
    active_cwd: str,
    fleet_mesh: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """
    Upload resolved directive files via client.files_upload_v2() into active channel and thread.
    Cleans up any temporary files created from remote fetches in a finally: block.
    Catches errors without crashing the turn.
    """
    results: List[Dict[str, Any]] = []
    if not client or not channel or not targets:
        return results

    seen_paths: Set[str] = set()

    for raw_target in targets:
        if not raw_target:
            continue
        target_str = raw_target.strip().strip("\"'`<>")
        if not target_str:
            continue

        # Security check: guard against exfiltrating sensitive credentials or keys (pre-resolution)
        if is_sensitive_target(target_str):
            logger.warning(f"File directive '{target_str}' references a protected sensitive path; upload skipped.")
            continue

        resolved_path: Optional[str] = None
        node_label: Optional[str] = None
        is_temp: bool = False

        try:
            resolved_path, node_label, is_temp = await resolve_file_path(
                target_str=target_str,
                active_cwd=active_cwd,
                fleet_mesh=fleet_mesh,
            )

            if not resolved_path or not os.path.exists(resolved_path):
                logger.warning(f"File directive '{target_str}' could not be resolved or file does not exist; skipping.")
                continue

            # Post-resolution security check: defense-in-depth against symlink and alias bypasses
            real_path = os.path.realpath(resolved_path)
            if is_sensitive_target(resolved_path) or is_sensitive_target(real_path):
                logger.warning(
                    f"Resolved path '{resolved_path}' (real path: '{real_path}') for directive '{target_str}' "
                    f"references a protected sensitive path; upload skipped."
                )
                if is_temp and os.path.exists(resolved_path):
                    try:
                        os.remove(resolved_path)
                    except OSError:
                        pass
                continue

            # Prevent duplicate uploads in the same turn
            if resolved_path in seen_paths:
                logger.debug(f"File '{resolved_path}' already uploaded in this turn; skipping duplicate.")
                continue
            seen_paths.add(resolved_path)

            is_windows_drive = bool(WINDOWS_DRIVE_PATTERN.match(target_str))
            path_for_name = target_str.split(":", 1)[1] if (not is_windows_drive and ":" in target_str) else target_str
            filename = os.path.basename(path_for_name.replace("\\", "/")) or os.path.basename(resolved_path)
            if node_label and node_label != "local":
                title = f"File ({node_label}): {filename}"
            else:
                title = f"File: {filename}"

            upload_kwargs: Dict[str, Any] = {
                "channel": channel,
                "file": resolved_path,
                "filename": filename,
                "title": title,
            }
            if thread_ts:
                upload_kwargs["thread_ts"] = thread_ts

            if hasattr(client, "files_upload_v2"):
                res = client.files_upload_v2(**upload_kwargs)
                if inspect.isawaitable(res):
                    resp = await res
                else:
                    resp = res
            elif hasattr(client, "upload_file"):
                res = client.upload_file(**upload_kwargs)
                if inspect.isawaitable(res):
                    resp = await res
                else:
                    resp = res
            else:
                logger.error("Client does not provide files_upload_v2 or upload_file method.")
                resp = None

            if resp is not None:
                res_dict = resp if isinstance(resp, dict) else (resp.data if hasattr(resp, "data") else {"ok": True, "file": filename})
                results.append(res_dict)
                logger.info(f"Successfully uploaded directive file '{target_str}' ({resolved_path}) to {channel}")

        except Exception as up_err:
            logger.error(f"Failed to upload file from directive '{target_str}': {up_err}")
        finally:
            if is_temp and resolved_path and os.path.exists(resolved_path):
                try:
                    os.remove(resolved_path)
                except OSError:
                    pass

    return results
