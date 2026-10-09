"""
Compatibility facade redirecting to golem.slack_client.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.slack_client
from golem.slack_client import *  # noqa: F401, F403

sys.modules[__name__] = golem.slack_client