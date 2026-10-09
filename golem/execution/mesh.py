"""
Compatibility facade redirecting to golem.mesh.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.mesh
from golem.mesh import *  # noqa: F401, F403

sys.modules[__name__] = golem.mesh