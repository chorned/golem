"""
Compatibility facade redirecting to golem.bus.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.bus
from golem.bus import *  # noqa: F401, F403

sys.modules[__name__] = golem.bus