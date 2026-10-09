"""
Compatibility facade redirecting to golem.file_transfer.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.file_transfer
from golem.file_transfer import *  # noqa: F401, F403

sys.modules[__name__] = golem.file_transfer