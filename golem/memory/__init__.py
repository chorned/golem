#!/usr/bin/env python3
"""
golem.memory - Contract-Level Memory Gating & Ambient Peripheral Vision.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from golem.memory.peripheral import (
    compile_peripheral_vision,
    get_wiki_latest_mtime,
    clear_peripheral_vision_cache,
)
from golem.memory.reflection import (
    record_task_settlement_async,
)

__all__ = [
    "compile_peripheral_vision",
    "get_wiki_latest_mtime",
    "clear_peripheral_vision_cache",
    "record_task_settlement_async",
]
