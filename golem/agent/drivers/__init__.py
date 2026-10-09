"""
Compatibility facade redirecting to golem.drivers.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import sys
import golem.drivers
import golem.drivers.base
import golem.drivers.antigravity
import golem.drivers.claude_code
import golem.drivers.opencode
from golem.drivers import *  # noqa: F401, F403

sys.modules["golem.agent.drivers"] = golem.drivers
sys.modules["golem.agent.drivers.base"] = golem.drivers.base
sys.modules["golem.agent.drivers.antigravity"] = golem.drivers.antigravity
sys.modules["golem.agent.drivers.claude_code"] = golem.drivers.claude_code
sys.modules["golem.agent.drivers.opencode"] = golem.drivers.opencode