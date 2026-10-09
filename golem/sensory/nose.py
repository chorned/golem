"""
Compatibility facade redirecting to golem.nose.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.nose
from golem.nose import *  # noqa: F401, F403

sys.modules[__name__] = golem.nose