"""
tests/conftest.py
Global pytest configuration and fixtures for Golem test suite.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import os
import pytest

# Ensure test mode is active during test discovery and execution
os.environ.setdefault("FIRSTMATE_TEST_MODE", "1")
os.environ.setdefault("GOLEM_TEST_MODE", "1")
os.environ.pop("GOLEM_DEFAULT_HAND_WORKER", None)
os.environ.pop("GOLEM_ENGINE", None)
os.environ.pop("FIRSTMATE_DEFAULT_HAND_WORKER", None)


@pytest.fixture(autouse=True)
def clean_engine_env():
    """Ensure engine environment variables are clean before and after each test."""
    for key in ("GOLEM_DEFAULT_HAND_WORKER", "GOLEM_ENGINE", "FIRSTMATE_DEFAULT_HAND_WORKER"):
        os.environ.pop(key, None)
    yield
    for key in ("GOLEM_DEFAULT_HAND_WORKER", "GOLEM_ENGINE", "FIRSTMATE_DEFAULT_HAND_WORKER"):
        os.environ.pop(key, None)
