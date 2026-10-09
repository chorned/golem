"""
Compatibility facade redirecting to golem.chunker.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.chunker
from golem.chunker import *  # noqa: F401, F403

sys.modules[__name__] = golem.chunker