"""
Compatibility facade redirecting to golem.ingress.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.ingress
from golem.ingress import *  # noqa: F401, F403

sys.modules[__name__] = golem.ingress