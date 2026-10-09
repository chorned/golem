"""
Compatibility facade redirecting to golem.buffer.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.buffer
from golem.buffer import *  # noqa: F401, F403

sys.modules[__name__] = golem.buffer