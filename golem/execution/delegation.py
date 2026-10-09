"""
Compatibility facade redirecting to golem.delegation.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.delegation
from golem.delegation import *  # noqa: F401, F403

sys.modules[__name__] = golem.delegation