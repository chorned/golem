#!/usr/bin/env python3
"""
golem.memory.reflection - Asynchronous Post-Flight Settlement Worker.
Runs non-blocking reflection upon Hand completion, committing audit
entries to daily logbooks and scaffolding novel runbook proposals.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import os
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union, Dict, Any

from golem.wiki import logbook_append, propose_update, DEFAULT_WIKI_DIR

logger = logging.getLogger("Golem.Memory.Reflection")


async def record_task_settlement_async(
    task_id: str,
    task_description: str,
    target_node: str,
    exit_code: int,
    output_tail: str,
    precedent_source: str,
    precedent_status: str
) -> None:
    """
    Runs in background via asyncio.create_task upon Hand settlement.
    Silently records audit entries and flags novel runbook proposals.
    """
    try:
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        status_label = "SUCCESS" if exit_code == 0 else f"FAILED (exit {exit_code})"
        
        log_entry = (
            f"- **[{timestamp}] Task `{task_id}`** on `{target_node}` -> `{status_label}`\n"
            f"  - **Intent:** {task_description}\n"
            f"  - **Precedent:** `{precedent_status}` ({precedent_source})\n"
        )
        if exit_code != 0 and output_tail:
            clean_tail = "\n".join(output_tail.splitlines()[-10:])
            log_entry += f"  - **Tail Output:**\n```\n{clean_tail}\n```\n"

        # 1. Non-blocking audit record into today's logbook
        try:
            logbook_append(entry=log_entry, title=f"Task Settlement: {task_id}")
        except Exception as e:
            logger.debug(f"logbook_append error during reflection: {e}")

        # 2. If NOVEL_EXPLORATION succeeded, scaffold a runbook draft for review
        if precedent_status == "NOVEL_EXPLORATION" and exit_code == 0:
            slug = "".join(c if c.isalnum() else "_" for c in task_description[:30]).strip("_").lower()
            if not slug:
                slug = f"novel_task_{task_id}"
            rel_target = f"runbooks/flows/{slug}.md"
            wiki_root = Path(os.environ.get("GOLEM_WIKI_DIR") or DEFAULT_WIKI_DIR)
            target_path = wiki_root / rel_target
            if not target_path.exists():
                draft_content = (
                    f"---\ntitle: {task_description[:50]}\nsummary: Automatically captured flow from task {task_id}\n---\n\n"
                    f"# {task_description[:50]}\n\n"
                    f"**Node:** `{target_node}`\n"
                    f"**Dispatched Instructions:**\n{task_description}\n\n"
                    f"**Verified Execution Tail:**\n```\n{output_tail[-800:]}\n```\n"
                )
                try:
                    propose_update(rel_target, draft_content, f"Auto-captured novel flow from task {task_id}")
                except Exception as e:
                    logger.debug(f"propose_update error during reflection: {e}")
    except Exception as e:
        logger.debug(f"Error in record_task_settlement_async: {e}")


def record_task_settlement(
    task_id: str,
    task_description: str,
    target_node: str,
    exit_code: int,
    output_tail: str,
    precedent_source: str,
    precedent_status: str,
) -> None:
    """Synchronous wrapper for record_task_settlement_async."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    coro = record_task_settlement_async(
        task_id=task_id,
        task_description=task_description,
        target_node=target_node,
        exit_code=exit_code,
        output_tail=output_tail,
        precedent_source=precedent_source,
        precedent_status=precedent_status,
    )
    if loop and loop.is_running():
        asyncio.create_task(coro)
    else:
        asyncio.run(coro)
