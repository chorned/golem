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
    return max(
        (p.stat().st_mtime for p in root.glob("**/*.md") if "logbook" not in p.parts and not p.name.startswith(".")),
        default=0.0,
    )


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
    table-of-contents for the Mouth's system prompt.
    """
    global _CACHED_PERIPHERAL_VISION, _LAST_WIKI_MTIME, _LAST_WIKI_ROOT, _LAST_WIKI_COUNT
    root = Path(wiki_root) if wiki_root is not None else Path(DEFAULT_WIKI_DIR)
    
    current_mtime = get_wiki_latest_mtime(root)
    current_count = 0
    if root.exists():
        current_count = len([p for p in root.glob("**/*.md") if "logbook" not in p.parts and not p.name.startswith(".")])

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

    entries = []
    if root.exists():
        for path in sorted(root.glob("**/*.md")):
            # Ignore logbook and hidden git files
            if "logbook" in path.parts or path.name.startswith(".") or ".git" in path.parts:
                continue
            rel_path = str(path.relative_to(root))
            title = path.stem.replace("_", " ").title()
            summary = ""

            try:
                raw_text = path.read_text(encoding="utf-8")
                if raw_text.startswith("---"):
                    parts = raw_text.split("---", 2)
                    if len(parts) >= 3:
                        meta = yaml.safe_load(parts[1]) or {}
                        if isinstance(meta, dict):
                            title = meta.get("title", title)
                            summary = meta.get("summary", "")
                        if not summary:
                            # Fallback: grab first non-empty line of body
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
            entries.append(f"- [{category}] **{title}** (`{rel_path}`){desc}")

    if entries:
        lines.extend(entries)
    else:
        lines.append("- *(Wiki is empty or initializing: proceed with cautious exploration)*")

    lines.append("")
    lines.append(
        "PRECEDENT INVARIANT: Before calling `dispatch_task`, consult the cited wiki runbook "
        "via `consult_memory(path)` to verify arguments and environmental prerequisites."
    )

    _CACHED_PERIPHERAL_VISION = "\n".join(lines)
    _LAST_WIKI_MTIME = current_mtime
    _LAST_WIKI_ROOT = root
    _LAST_WIKI_COUNT = current_count
    return _CACHED_PERIPHERAL_VISION
