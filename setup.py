#!/usr/bin/env python3
"""
setup.py
Golem: Autonomous Fleet Chief of Staff.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
try:
    from setuptools import setup, find_packages
except ImportError:
    def setup(*args, **kwargs):
        pass
    def find_packages(*args, **kwargs):
        return ["golem"]

setup(
    name="golem",
    version="2.1.0",
    description="Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.",
    long_description="Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.",
    packages=find_packages(include=["golem*"]),
    py_modules=["app"],
    entry_points={
        "console_scripts": [
            "golem=golem.cli:main",
        ],
    },
)
