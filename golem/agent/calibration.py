"""
Compatibility facade redirecting to golem.calibration.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.calibration
from golem.calibration import *  # noqa: F401, F403

sys.modules[__name__] = golem.calibration