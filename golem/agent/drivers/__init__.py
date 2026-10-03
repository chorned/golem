#!/usr/bin/env python3
"""
golem/agent/drivers
Driver registry and factory for First Mate CLI agent execution engines (The Hands).

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

from typing import Dict, Type, Optional, List, Any

from golem.agent.drivers.base import BaseAgentDriver
from golem.agent.drivers.antigravity import AntigravityDriver, DEFAULT_AGY_BIN
from golem.agent.drivers.claude_code import ClaudeCodeDriver
from golem.agent.drivers.opencode import OpenCodeDriver
from golem.config import get_active_engine, get_engine_model

DRIVER_REGISTRY: Dict[str, Type[BaseAgentDriver]] = {
    "antigravity": AntigravityDriver,
    "agy": AntigravityDriver,
    "claude_code": ClaudeCodeDriver,
    "claude": ClaudeCodeDriver,
    "opencode": OpenCodeDriver,
}


def register_driver(name: str, driver_cls: Type[BaseAgentDriver]) -> None:
    """Register a custom execution driver class."""
    if not issubclass(driver_cls, BaseAgentDriver):
        raise TypeError(f"Driver class {driver_cls} must subclass BaseAgentDriver")
    DRIVER_REGISTRY[name.lower().strip()] = driver_cls


def get_driver_class(engine: Optional[str] = None) -> Type[BaseAgentDriver]:
    """
    Resolve driver class by engine name.
    Falls back to FIRST_MATE_ENGINE from config if engine is None.
    Raises ValueError if engine is not recognized.
    """
    target = (engine or get_active_engine()).lower().strip()
    if target not in DRIVER_REGISTRY:
        available = ", ".join(sorted(DRIVER_REGISTRY.keys()))
        raise ValueError(
            f"Unknown execution engine '{target}'. Available engines: {available}"
        )
    return DRIVER_REGISTRY[target]


def create_driver(
    engine: Optional[str] = None,
    cwd: str = "/Users/hermes/projects/AGY",
    model: Optional[str] = None,
    extra_args: Optional[List[str]] = None,
    env: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> BaseAgentDriver:
    """
    Factory function to instantiate and return an active agent driver.
    Dynamically resolves engine via FIRST_MATE_ENGINE and model via get_engine_model().
    """
    resolved_engine = (engine or get_active_engine()).lower().strip()
    driver_cls = get_driver_class(resolved_engine)
    resolved_model = model or get_engine_model(resolved_engine) or None

    return driver_cls(
        cwd=cwd,
        model=resolved_model,
        extra_args=extra_args,
        env=env,
        **kwargs,
    )


__all__ = [
    "BaseAgentDriver",
    "AntigravityDriver",
    "ClaudeCodeDriver",
    "OpenCodeDriver",
    "DEFAULT_AGY_BIN",
    "DRIVER_REGISTRY",
    "register_driver",
    "get_driver_class",
    "create_driver",
]
