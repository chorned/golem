"""
Compatibility facade redirecting to golem.harness.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.harness
from golem.harness import *  # noqa: F401, F403

sys.modules[__name__] = golem.harness