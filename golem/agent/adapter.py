"""
Compatibility facade redirecting to golem.adapter.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.adapter
from golem.adapter import *  # noqa: F401, F403

sys.modules[__name__] = golem.adapter