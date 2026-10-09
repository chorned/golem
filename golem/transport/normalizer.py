"""
Compatibility facade redirecting to golem.normalizer.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.normalizer
from golem.normalizer import *  # noqa: F401, F403

sys.modules[__name__] = golem.normalizer