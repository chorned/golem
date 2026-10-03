"""
golem._alias
Dual-Layer Compatibility Shim importlib hook.
Maps first_mate.<submodule> to golem.<submodule> dynamically via sys.meta_path.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from __future__ import annotations

import sys
import importlib
import importlib.abc
import importlib.util
from typing import Any, Optional, Sequence

LINEAGE = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."


class _FirstMateAliasLoader(importlib.abc.Loader):
    """Loader that delegates module resolution directly to the loaded golem module."""

    def __init__(self, target_mod: Any) -> None:
        self.target_mod = target_mod

    def create_module(self, spec: Any) -> Any:
        return self.target_mod

    def exec_module(self, module: Any) -> None:
        pass


class _FirstMateAliasFinder(importlib.abc.MetaPathFinder):
    """
    MetaPathFinder that intercepts any 'first_mate.<submodule>' import
    and binds it directly to 'golem.<submodule>' in sys.modules.
    Guarantees exact assertIs object identity and bidirectional unittest.mock.patch fidelity.
    """

    def find_spec(
        self,
        fullname: str,
        path: Optional[Sequence[str]] = None,
        target: Optional[Any] = None,
    ) -> Optional[Any]:
        if fullname.startswith("first_mate."):
            golem_name = "golem." + fullname[len("first_mate."):]
            try:
                target_mod = importlib.import_module(golem_name)
            except ImportError:
                return None
            loader = _FirstMateAliasLoader(target_mod)
            is_pkg = hasattr(target_mod, "__path__")
            spec = importlib.util.spec_from_loader(
                fullname,
                loader,
                origin=getattr(target_mod, "__file__", None),
                is_package=is_pkg,
            )
            if is_pkg and spec is not None:
                spec.submodule_search_locations = list(target_mod.__path__)
            return spec
        return None


def register_first_mate_alias_finder() -> None:
    """Register _FirstMateAliasFinder on sys.meta_path if not already registered."""
    if not any(isinstance(f, _FirstMateAliasFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _FirstMateAliasFinder())
