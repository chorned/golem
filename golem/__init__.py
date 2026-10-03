"""
golem: Autonomous Fleet Chief of Staff.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from __future__ import annotations

__version__ = "2.1.0"
LINEAGE = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."

# Install the meta_path compatibility finder so first_mate.* imports resolve directly to golem.*
from ._alias import register_first_mate_alias_finder
register_first_mate_alias_finder()

from . import config
from . import cli
from . import gateway
from . import agent
from . import execution
from . import sensory
from . import transport
from . import webui

__all__ = [
    "__version__",
    "LINEAGE",
    "config",
    "cli",
    "gateway",
    "agent",
    "execution",
    "sensory",
    "transport",
    "webui",
]


