"""
Compatibility facade redirecting to golem.milestone.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.milestone
from golem.milestone import *  # noqa: F401, F403

sys.modules[__name__] = golem.milestone