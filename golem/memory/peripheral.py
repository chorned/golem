#!/usr/bin/env python3
"""
golem.memory.peripheral - Ambient Peripheral Vision Generator.
Compiles a compact, high-density table-of-contents from ~/.golem/wiki/
for injection into The Mouth's system prompt.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import yaml
from golem.wiki import DEFAULT_WIKI_DIR

_CACHED_PERIPHERAL_VISION: str = ""
_LAST_WIKI_MTIME: float = 0.0
_LAST_WIKI_ROOT: Optional[Path] = None
_LAST_WIKI_COUNT: int = 0


def get_wiki_latest_mtime(wiki_root: Path) -> float:
    """Scans all markdown files in wiki for the latest modification timestamp."""
    root = Path(wiki_root)
    if not root.exists():
        return 0.0
    mtimes = []
    for p in root.glob("**/*.md"):
        if "logbook" in p.parts or p.name.startswith(".") or ".git" in p.parts:
            continue
        try:
            mtimes.append(p.stat().st_mtime)
        except OSError:
            pass
    return max(mtimes, default=0.0)


def clear_peripheral_vision_cache() -> None:
    """Clear cached peripheral vision string."""
    global _CACHED_PERIPHERAL_VISION, _LAST_WIKI_MTIME, _LAST_WIKI_ROOT, _LAST_WIKI_COUNT
    _CACHED_PERIPHERAL_VISION = ""
    _LAST_WIKI_MTIME = 0.0
    _LAST_WIKI_ROOT = None
    _LAST_WIKI_COUNT = 0


def compile_peripheral_vision(wiki_root: Optional[Path] = None) -> str:
    """
    Crawls ~/.golem/wiki/ (context/ and runbooks/) and produces a high-density
    table-of-contents along with full verbatim wiki context in memory for the
    Mouth's system prompt.
    """
    global _CACHED_PERIPHERAL_VISION, _LAST_WIKI_MTIME, _LAST_WIKI_ROOT, _LAST_WIKI_COUNT
    if wiki_root is not None:
        root = Path(wiki_root)
    else:
        root = Path(os.environ.get("GOLEM_WIKI_DIR") or DEFAULT_WIKI_DIR)
    
    current_mtime = get_wiki_latest_mtime(root)
    current_count = 0
    if root.exists():
        current_count = len([
            p for p in root.glob("**/*.md")
            if "logbook" not in p.parts and not p.name.startswith(".") and ".git" not in p.parts
        ])

    if (
        _CACHED_PERIPHERAL_VISION
        and _LAST_WIKI_ROOT == root
        and current_mtime <= _LAST_WIKI_MTIME
        and current_count == _LAST_WIKI_COUNT
    ):
        return _CACHED_PERIPHERAL_VISION

    lines = [
        "### OPERATIONAL WIKI PERIPHERAL VISION (KNOWN PRECEDENTS & PLAYBOOKS)",
        "Your institutional memory contains verified precedents for the following tasks and nodes.",
        "When executing backstage tasks matching these domains, you MUST cite the corresponding path in `dispatch_task`:",
    ]

    index_entries = []
    wiki_pages = []

    if root.exists():
        all_md_files = []
        for path in root.glob("**/*.md"):
            if "logbook" in path.parts or path.name.startswith(".") or ".git" in path.parts:
                continue
            try:
                rel_path = str(path.relative_to(root))
            except ValueError:
                continue
            if rel_path == "README.md":
                continue
            all_md_files.append((rel_path, path))

        all_md_files.sort(key=lambda item: item[0])

        for rel_path, path in all_md_files:
            title = path.stem.replace("_", " ").title()
            summary = ""
            raw_text = ""

            try:
                raw_text = path.read_text(encoding="utf-8")
                if raw_text.startswith("---"):
                    parts = raw_text.split("---", 2)
                    if len(parts) >= 3:
                        meta = yaml.safe_load(parts[1]) or {}
                        if isinstance(meta, dict):
                            title = meta.get("title") or meta.get("name") or title
                            summary = meta.get("summary") or meta.get("description") or ""
                        if not summary:
                            for line in parts[2].splitlines():
                                line_s = line.strip().lstrip("#").strip()
                                if line_s:
                                    summary = line_s[:90]
                                    break
                else:
                    for line in raw_text.splitlines():
                        line_s = line.strip().lstrip("#").strip()
                        if line_s:
                            summary = line_s[:90]
                            break
            except Exception:
                continue

            category = "flow" if "flows" in rel_path else ("skill" if "skills" in rel_path else "rule")
            desc = f": {summary}" if summary else ""
            index_entries.append(f"- [{category}] **{title}** (`{rel_path}`){desc}")
            wiki_pages.append(f'<wiki_page path="{rel_path}">\n{raw_text.strip()}\n</wiki_page>')

    if index_entries:
        lines.extend(index_entries)
    else:
        lines.append("- *(Wiki is empty or initializing: proceed with cautious exploration)*")

    lines.append("")
    lines.append("### FULL WIKI CONTEXT & MANDATORY RUNBOOKS (LOADED IN MEMORY)")
    if wiki_pages:
        lines.extend(wiki_pages)
    else:
        lines.append("*(No full context pages loaded)*")

    lines.append("")
    lines.append(
        "OPERATIONAL MEMORY NOTE: All wiki context pages and runbooks/skills above are already loaded in full in your system prompt. "
        "The `consult_memory(path)` tool remains available for `logbook/` entries or re-reading specific files."
    )

    _CACHED_PERIPHERAL_VISION = "\n".join(lines)
    _LAST_WIKI_MTIME = current_mtime
    _LAST_WIKI_ROOT = root
    _LAST_WIKI_COUNT = current_count
    return _CACHED_PERIPHERAL_VISION


compile_full_wiki_context = compile_peripheral_vision
