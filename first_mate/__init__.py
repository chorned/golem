"""
first_mate: Backwards compatibility shim package.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from __future__ import annotations

import sys
import importlib
import importlib.abc
import importlib.util
from typing import Any, Optional, Sequence

__version__ = "2.1.0"
LINEAGE = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."

# Register _FirstMateAliasFinder on sys.meta_path
from golem._alias import (
    _FirstMateAliasFinder,
    _FirstMateAliasLoader,
    register_first_mate_alias_finder,
)
register_first_mate_alias_finder()

# Import canonical golem package and re-export top-level attributes
import golem
for attr in dir(golem):
    if not attr.startswith("__"):
        globals()[attr] = getattr(golem, attr)


def __getattr__(name: str) -> Any:
    """Dynamically resolve submodules and attributes from golem."""
    if hasattr(golem, name):
        return getattr(golem, name)
    try:
        return importlib.import_module(f"golem.{name}")
    except ImportError:
        raise AttributeError(f"module 'first_mate' has no attribute '{name}'")

