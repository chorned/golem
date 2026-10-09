"""
Compatibility facade redirecting to golem.runner.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.runner
from golem.runner import *  # noqa: F401, F403

sys.modules[__name__] = golem.runner