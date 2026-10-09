#!/usr/bin/env python3
"""
golem.execution.buffer - Runner terminal buffer observer and blocker extraction.
Monitors runner pane terminal output, reads settled buffers, and identifies interactive blockers:
- OAuth callback listeners (e.g. http://127.0.0.1:53682/auth)
- Interactive prompts ([y/N], (y/n), password/token prompts)
Satisfies Acceptance Criterion TC-03.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import re
import json
import shutil
import asyncio
import logging
from typing import Dict, Any, Optional

try:
    from herdr_client import HerdrClient
except ImportError:
    HerdrClient = None  # type: ignore

logger = logging.getLogger("Golem.Execution.Buffer")

HERDR_BIN_DEFAULT = shutil.which("herdr") or os.path.expanduser("~/.local/bin/herdr")

# Regex pattern for OAuth callback URLs on localhost/127.0.0.1/0.0.0.0
OAUTH_CALLBACK_PATTERN = re.compile(
    r"(https?://(?:localhost|127\.0\.0\.1|0\.0\.0\.0)(?::\d+)?/[^\s\)\"\'<>\]]+)",
    re.IGNORECASE
)

# Regex pattern for interactive prompts requiring user confirmation or secret input
INTERACTIVE_PROMPT_PATTERNS = [
    re.compile(r"[\(\[](?:y/n|yes/no)[\)\]]", re.IGNORECASE),
    re.compile(r"\?\s+[A-Za-z0-9_\-\s]+\[.*\]"),
    re.compile(r"\?\s*$", re.MULTILINE),
    re.compile(r"(?:\[sudo\]\s+)?(?:password|passphrase|token|pin|secret)(?:\s+for\s+[^:]+)?:|\b(?:password|passphrase|token|pin|secret):\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"(?:proceed|continue)\s*\?\s*\[.*\]", re.IGNORECASE),
]


class RunnerBufferObserver:
    """
    Observes terminal buffer of the runner pane or active subagent panes.
    Detects interactive blockers (OAuth logins, confirmation prompts) to allow
    the Chief of Staff to surface action cards directly to the user.
    """

    def __init__(
        self,
        herdr_bin: Optional[str] = None,
        herdr_client: Optional[Any] = None
    ):
        self.herdr_bin = herdr_bin or shutil.which("herdr") or HERDR_BIN_DEFAULT
        self.herdr_client = herdr_client or (HerdrClient(herdr_bin=self.herdr_bin) if HerdrClient is not None else None)

    async def read_buffer(
        self,
        pane: str = "runner",
        lines: int = 30,
        source: str = "recent-unwrapped"
    ) -> str:
        """
        Read the latest settled terminal buffer from the runner pane.
        First tries 'herdr agent read', falls back to 'herdr pane read'.
        """
        # 1. Try herdr agent read
        cmd_args = [self.herdr_bin, "agent", "read", pane, "--lines", str(lines)]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout_bytes, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
            out = stdout_bytes.decode("utf-8", errors="replace").strip()
            code = proc.returncode if proc.returncode is not None else 0

            if code == 0 and out and "error" not in out:
                return out

            # 2. Fallback to herdr pane read
            pane_cmd = [self.herdr_bin, "pane", "read", pane, "--lines", str(lines)]
            if source:
                pane_cmd.extend(["--source", source])
            p_proc = await asyncio.create_subprocess_exec(
                *pane_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            p_stdout, _ = await asyncio.wait_for(p_proc.communicate(), timeout=5.0)
            p_out = p_stdout.decode("utf-8", errors="replace").strip()
            p_code = p_proc.returncode if p_proc.returncode is not None else 0

            if p_code == 0 and p_out and "error" not in p_out:
                return p_out

            # 3. Fallback: Resolve pane ID if pane was identified by label
            try:
                list_proc = await asyncio.create_subprocess_exec(
                    self.herdr_bin, "pane", "list",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                l_out, _ = await asyncio.wait_for(list_proc.communicate(), timeout=3.0)
                if list_proc.returncode == 0 and l_out:
                    data = json.loads(l_out.decode("utf-8", errors="replace"))
                    panes_list = (data.get("result", {}) or {}).get("panes") or data.get("panes", [])
                    for p_info in panes_list:
                        if p_info.get("label") == pane or p_info.get("name") == pane:
                            resolved_id = p_info.get("pane_id")
                            if resolved_id:
                                r_proc = await asyncio.create_subprocess_exec(
                                    self.herdr_bin, "pane", "read", resolved_id, "--lines", str(lines),
                                    stdout=asyncio.subprocess.PIPE,
                                    stderr=asyncio.subprocess.PIPE
                                )
                                r_out, _ = await asyncio.wait_for(r_proc.communicate(), timeout=5.0)
                                if r_proc.returncode == 0 and r_out:
                                    return r_out.decode("utf-8", errors="replace").strip()
            except Exception:
                pass

            return p_out or out or f"No terminal output recorded for pane {pane}."
        except Exception as e:
            logger.error(f"Error in read_buffer for pane {pane}: {e}")
            return f"Error reading pane {pane}: {e}"

    def extract_interactive_blockers(self, buffer_text: str) -> Optional[Dict[str, Any]]:
        """
        Inspect buffer text for interactive blockers:
        - OAuth callback URL -> {"type": "oauth", "content": ..., "url": ...}
        - Confirmation or password prompt -> {"type": "prompt", "content": ..., "url": None}

        Returns None if no blocker detected.
        """
        if not buffer_text or not buffer_text.strip():
            return None

        from golem.normalizer import strip_ansi
        clean_text = strip_ansi(buffer_text)
        if not clean_text or not clean_text.strip():
            return None

        lines = clean_text.strip().splitlines()

        # 1. Check for OAuth callback URL
        oauth_match = OAUTH_CALLBACK_PATTERN.search(clean_text)
        if oauth_match:
            matched_url = oauth_match.group(1).rstrip(".,;)>]\"'")
            content_line = ""
            for line in lines:
                if matched_url in line:
                    content_line = line.strip()
                    break
            if not content_line:
                content_line = f"Authentication required: {matched_url}"

            return {
                "type": "oauth",
                "content": content_line,
                "url": matched_url
            }

        # 2. Check for interactive prompts
        for line in reversed(lines):
            stripped_line = line.strip()
            if not stripped_line:
                continue
            for pat in INTERACTIVE_PROMPT_PATTERNS:
                if pat.search(stripped_line):
                    return {
                        "type": "prompt",
                        "content": stripped_line,
                        "url": None
                    }

        return None

    # Alias for single-blocker signature
    extract_interactive_blocker = extract_interactive_blockers

    def extract_all_blockers(self, buffer_text: str) -> list[Dict[str, Any]]:
        """
        Inspect buffer text and return all interactive blockers found
        (OAuth callback URLs and/or interactive prompts).
        """
        if not buffer_text or not buffer_text.strip():
            return []

        from golem.normalizer import strip_ansi
        clean_text = strip_ansi(buffer_text)
        if not clean_text or not clean_text.strip():
            return []

        results: list[Dict[str, Any]] = []
        lines = clean_text.strip().splitlines()

        # 1. Check for OAuth callback URL
        oauth_match = OAUTH_CALLBACK_PATTERN.search(clean_text)
        if oauth_match:
            matched_url = oauth_match.group(1).rstrip(".,;)>]\"'")
            content_line = ""
            for line in lines:
                if matched_url in line:
                    content_line = line.strip()
                    break
            if not content_line:
                content_line = f"Authentication required: {matched_url}"

            results.append({
                "type": "oauth",
                "content": content_line,
                "url": matched_url
            })

        # 2. Check for interactive prompts
        for line in reversed(lines):
            stripped_line = line.strip()
            if not stripped_line:
                continue
            for pat in INTERACTIVE_PROMPT_PATTERNS:
                if pat.search(stripped_line):
                    results.append({
                        "type": "prompt",
                        "content": stripped_line,
                        "url": None
                    })
                    break
            if results and results[-1].get("type") == "prompt":
                break

        return results
