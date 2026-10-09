"""
Compatibility facade redirecting to golem.drivers.opencode.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.drivers.opencode
from golem.drivers.opencode import *  # noqa: F401, F403

sys.modules[__name__] = golem.drivers.opencode