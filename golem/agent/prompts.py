"""
Compatibility facade redirecting to golem.prompts.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.prompts
from golem.prompts import *  # noqa: F401, F403

sys.modules[__name__] = golem.prompts