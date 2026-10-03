#!/usr/bin/env python3
"""
golem/cli.py
Synchronous CLI Entrypoint Wrapper for Golem & First Mate.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from __future__ import annotations

import asyncio
import sys
from typing import Optional, Sequence

LINEAGE = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."


def main(argv: Optional[Sequence[str]] = None) -> None:
    """
    Synchronous CLI entrypoint for Golem / First Mate console scripts.
    Executes app.main(args) inside asyncio.run() and terminates cleanly.
    """
    try:
        from app import main as app_main, parse_args
    except ImportError:
        import app
        app_main = app.main
        parse_args = getattr(app, "parse_args", None)

    args = parse_args(list(argv)) if parse_args and argv is not None else (parse_args() if parse_args else None)
    try:
        res = asyncio.run(app_main(args) if args is not None else app_main())
        sys.exit(res if isinstance(res, int) else 0)
    except KeyboardInterrupt:
        sys.exit(0)
    except SystemExit as exc:
        sys.exit(exc.code)


if __name__ == "__main__":
    main()
