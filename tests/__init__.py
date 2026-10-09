"""
tests package initialization.
Ensures test environment flags are set when run via unittest discover.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import os

os.environ.setdefault("FIRSTMATE_TEST_MODE", "1")
os.environ.setdefault("GOLEM_TEST_MODE", "1")
os.environ.pop("GOLEM_DEFAULT_HAND_WORKER", None)
os.environ.pop("GOLEM_ENGINE", None)
os.environ.pop("FIRSTMATE_DEFAULT_HAND_WORKER", None)
