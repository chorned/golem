"""
Compatibility facade redirecting to golem.eyes.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.eyes
from golem.eyes import *  # noqa: F401, F403

sys.modules[__name__] = golem.eyes