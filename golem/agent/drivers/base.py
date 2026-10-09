"""
Compatibility facade redirecting to golem.drivers.base.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.drivers.base
from golem.drivers.base import *  # noqa: F401, F403

sys.modules[__name__] = golem.drivers.base