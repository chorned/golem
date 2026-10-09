"""
Compatibility facade redirecting to golem.protocol.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.protocol
from golem.protocol import *  # noqa: F401, F403

sys.modules[__name__] = golem.protocol