"""
Compatibility facade redirecting to golem.session.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.session
from golem.session import *  # noqa: F401, F403

sys.modules[__name__] = golem.session