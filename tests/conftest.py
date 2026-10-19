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


@pytest.fixture(scope="session")
def _hermetic_wiki_template(tmp_path_factory):
    """
    Session-scoped template git repository for GolemWiki.
    Initializes git repository, standard directories, and default templates ONCE per session
    so individual tests can copy the pre-initialized git repository in <2ms with ZERO subprocess calls.
    """
    import shutil
    from pathlib import Path
    from golem.wiki import GolemWiki

    template_dir = tmp_path_factory.mktemp("hermetic_wiki_template")

    # Initialize git repo and base files ONCE for the session
    GolemWiki(wiki_dir=template_dir)
    GolemWiki._session_template_wiki_dir = str(template_dir)
    return template_dir


@pytest.fixture(autouse=True)
def _hermetic_wiki_dir(tmp_path, monkeypatch, _hermetic_wiki_template):
    """
    Hermetic wiki fixture redirecting all GolemWiki writes and peripheral vision crawls
    to an isolated temporary git repository so tests never touch ~/.golem/wiki.
    Fast copy of the session template avoids running git subprocesses on every test.
    """
    import shutil
    hermetic_dir = tmp_path / "hermetic_wiki"
    shutil.copytree(_hermetic_wiki_template, hermetic_dir, symlinks=True)

    monkeypatch.setenv("GOLEM_WIKI_DIR", str(hermetic_dir))
    monkeypatch.setattr("golem.wiki.DEFAULT_WIKI_DIR", hermetic_dir)
    from golem.memory.peripheral import clear_peripheral_vision_cache
    clear_peripheral_vision_cache()
    yield hermetic_dir
    clear_peripheral_vision_cache()
