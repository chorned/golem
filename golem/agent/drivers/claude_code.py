"""
Compatibility facade redirecting to golem.drivers.claude_code.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.drivers.claude_code
from golem.drivers.claude_code import *  # noqa: F401, F403

sys.modules[__name__] = golem.drivers.claude_code