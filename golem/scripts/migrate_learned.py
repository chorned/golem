#!/usr/bin/env python3
"""
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.

golem/scripts/migrate_learned.py
CLI & utility script to migrate legacy operational rules, skills, and ledger entries
into the Git-backed Golem operational memory wiki (~/.golem/wiki).
"""

from __future__ import annotations

import os
import sys
import json
import argparse
import logging
from typing import Dict, Any, List

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.wiki import GolemWiki

logger = logging.getLogger("Golem.MigrateLearned")


def migrate_legacy_knowledge(
    source_root: str,
    wiki: GolemWiki,
) -> Dict[str, int]:
    """
    Ingest legacy rules, skills, and ledger from source_root into the wiki.
    Returns counts of migrated files by category.
    """
    counts = {"rules": 0, "skills": 0, "ledger": 0}

    # 1. Ingest rules into context/rules/
    rules_dir = os.path.join(source_root, ".agents", "rules")
    if os.path.isdir(rules_dir):
        for fname in sorted(os.listdir(rules_dir)):
            if fname.endswith(".md"):
                fpath = os.path.join(rules_dir, fname)
                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                        content = f.read()
                    dest_rel = f"context/rules/{fname}"
                    wiki.write(
                        rel_path=dest_rel,
                        content=content,
                        commit_msg=f"chore(migration): ingest rule {fname}",
                    )
                    counts["rules"] += 1
                except Exception as e:
                    logger.warning(f"Failed to migrate rule {fname}: {e}")

    # 2. Ingest skills into runbooks/skills/
    skills_dir = os.path.join(source_root, ".agents", "skills")
    if os.path.isdir(skills_dir):
        for skill_name in sorted(os.listdir(skills_dir)):
            skill_path = os.path.join(skills_dir, skill_name)
            if os.path.isdir(skill_path):
                skill_md = os.path.join(skill_path, "SKILL.md")
                if os.path.isfile(skill_md):
                    try:
                        with open(skill_md, "r", encoding="utf-8", errors="replace") as f:
                            content = f.read()
                        dest_rel = f"runbooks/skills/{skill_name}/SKILL.md"
                        wiki.write(
                            rel_path=dest_rel,
                            content=content,
                            commit_msg=f"chore(migration): ingest skill {skill_name}",
                        )
                        counts["skills"] += 1
                    except Exception as e:
                        logger.warning(f"Failed to migrate skill {skill_name}: {e}")

    # 3. Ingest ledger into logbook/legacy_ledger.md
    ledger_path = os.path.join(source_root, "memory", "ledger.jsonl")
    if os.path.isfile(ledger_path):
        try:
            entries: List[str] = []
            with open(ledger_path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            item = json.loads(line)
                            ts = item.get("ts", "Unknown Time")
                            etype = item.get("type", "event")
                            notes = item.get("notes") or item.get("summary") or json.dumps(item)
                            entries.append(f"- **{ts}** `[{etype}]`: {notes}")
                        except Exception:
                            entries.append(f"- {line}")

            if entries:
                ledger_md = (
                    "# Legacy Operational Ledger\n\n"
                    "Historical events ingested from memory/ledger.jsonl:\n\n"
                    + "\n".join(entries)
                    + "\n"
                )
                wiki.write(
                    rel_path="logbook/legacy_ledger.md",
                    content=ledger_md,
                    commit_msg="chore(migration): ingest legacy ledger.jsonl",
                )
                counts["ledger"] += len(entries)
        except Exception as e:
            logger.warning(f"Failed to migrate legacy ledger: {e}")

    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate legacy learned knowledge into Golem Wiki.")
    parser.add_argument("--wiki-dir", type=str, default=None, help="Path to wiki directory (defaults to ~/.golem/wiki)")
    parser.add_argument("--source-root", type=str, default=PROJECT_DIR, help="Path to source project root")
    args = parser.parse_args()

    wiki = GolemWiki(wiki_dir=args.wiki_dir)
    print(f"Migrating legacy knowledge into wiki at {wiki.wiki_dir}...")
    counts = migrate_legacy_knowledge(args.source_root, wiki)
    print(f"Migration complete: {counts['rules']} rules, {counts['skills']} skills, {counts['ledger']} ledger events.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
