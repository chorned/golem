"""
Compatibility facade redirecting to golem.drivers.antigravity.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.drivers.antigravity
from golem.drivers.antigravity import *  # noqa: F401, F403

sys.modules[__name__] = golem.drivers.antigravity