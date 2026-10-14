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


@pytest.fixture(autouse=True)
def clean_exhausted_models():
    """The AGY quota-exhaustion registry is process-wide; keep it from leaking between tests."""
    from golem.drivers.antigravity import clear_exhausted_models
    clear_exhausted_models()
    yield
    clear_exhausted_models()


@pytest.fixture(autouse=True)
def _hermetic_wiki_dir(tmp_path, monkeypatch):
    """
    Hermetic wiki fixture redirecting all GolemWiki writes and peripheral vision crawls
    to an isolated temporary git repository so tests never touch ~/.golem/wiki.
    """
    import shutil
    from pathlib import Path
    hermetic_dir = tmp_path / "hermetic_wiki"
    hermetic_dir.mkdir(parents=True, exist_ok=True)
    real_wiki = Path(os.path.expanduser("~/.golem/wiki"))
    if real_wiki.exists():
        for folder in ("context", "runbooks"):
            src = real_wiki / folder
            dst = hermetic_dir / folder
            if src.exists():
                shutil.copytree(src, dst, symlinks=True, ignore=shutil.ignore_patterns(".git*", "*.tmp", ".wiki.lock"))
    from golem.wiki import GolemWiki
    wiki = GolemWiki(wiki_dir=hermetic_dir)
    monkeypatch.setenv("GOLEM_WIKI_DIR", str(hermetic_dir))
    monkeypatch.setattr("golem.wiki.DEFAULT_WIKI_DIR", hermetic_dir)
    from golem.memory.peripheral import clear_peripheral_vision_cache
    clear_peripheral_vision_cache()
    yield hermetic_dir
    clear_peripheral_vision_cache()
