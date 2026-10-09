#!/usr/bin/env python3
"""
golem/agent/drivers/base.py
Abstract Base Class for CLI agent execution drivers (The Hands).

Defines lifecycle and streaming event contracts for headless engines
(Antigravity, Claude Code, OpenCode). Yields strongly typed AgentEvent instances.
Provides 100% backward compatibility shims for legacy HeadlessAgentAdapter callers.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import AsyncGenerator, Dict, List, Optional, Any

from golem.protocol import AgentEvent

logger = logging.getLogger("Golem.Agent.Drivers.Base")


class BaseAgentDriver(ABC):
    """
    Abstract Base Class for CLI agent execution drivers (The Hands).

    Defines lifecycle and streaming event contracts for headless engines.
    Yields strongly typed AgentEvent instances (TextDeltaEvent, StatusUpdateEvent, FinishEvent).
    Provides backward-compatible shims for legacy HeadlessAgentAdapter callers.
    """

    def __init__(
        self,
        cwd: str,
        model: Optional[str] = None,
        extra_args: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        self.cwd: str = cwd
        self.model: Optional[str] = model
        self.extra_args: List[str] = list(extra_args or [])
        self.env: Optional[Dict[str, str]] = dict(env) if env else None
        self.conversation_id: Optional[str] = None
        self.available_tools: List[str] = []

    @property
    @abstractmethod
    def engine_name(self) -> str:
        """Name of the execution engine (e.g. 'antigravity', 'claude_code', 'opencode')."""
        pass

    @property
    @abstractmethod
    def is_alive(self) -> bool:
        """Return True if the underlying process or session is currently active."""
        pass

    @abstractmethod
    async def start_session(self) -> None:
        """
        Spawn and initialize the CLI agent subprocess/session.
        Must capture session metadata (e.g. conversation_id, available_tools).
        """
        pass

    @abstractmethod
    async def send_prompt(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        """
        Submit user prompt to the engine and yield strongly typed AgentEvent objects.
        Must yield events (e.g. TextDeltaEvent, StatusUpdateEvent, ToolStartEvent,
        ToolEndEvent, AwaitingInputEvent, FinishEvent).
        """
        pass

    @abstractmethod
    async def terminate_session(self) -> None:
        """
        Gracefully terminate the agent subprocess/session and release resources.
        """
        pass

    # -------------------------------------------------------------------------
    # Backward-Compatibility Aliases (HeadlessAgentAdapter shims)
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Backward-compatibility shim for start_session."""
        await self.start_session()

    def send_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        """Backward-compatibility shim for send_prompt."""
        return self.send_prompt(prompt)

    def stream_turn(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        """Backward-compatibility shim for send_prompt (stream_turn alias)."""
        return self.send_prompt(prompt)

    async def stop(self) -> None:
        """Backward-compatibility shim for terminate_session."""
        await self.terminate_session()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} engine={self.engine_name} model={self.model} alive={self.is_alive}>"
