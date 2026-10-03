#!/usr/bin/env python3
"""
golem/agent/drivers/claude_code.py
Pluggable driver stub for Claude Code CLI engine.

Yields strongly typed AgentEvent objects: StatusUpdateEvent, TextDeltaEvent, FinishEvent.
Dynamically resolves CLAUDE_CODE_MODEL with ZERO hardcoded model strings.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import uuid
import shutil
import logging
from typing import AsyncGenerator, Dict, List, Optional, Any

from golem.agent.drivers.base import BaseAgentDriver
from golem.agent.protocol import (
    AgentEvent,
    StatusUpdateEvent,
    TextDeltaEvent,
    FinishEvent,
)
from golem.config import get_engine_model

logger = logging.getLogger("Golem.Agent.Drivers.ClaudeCode")


class ClaudeCodeDriver(BaseAgentDriver):
    """
    Pluggable driver stub for Claude Code CLI engine.
    Supports dynamic model resolution, headless session lifecycle, and typed event streaming.
    """

    def __init__(
        self,
        cwd: str,
        model: Optional[str] = None,
        extra_args: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        claude_bin: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        resolved_model = model or get_engine_model("claude_code") or None
        super().__init__(cwd=cwd, model=resolved_model, extra_args=extra_args, env=env, **kwargs)
        self.claude_bin: str = (
            claude_bin
            or os.environ.get("CLAUDE_CODE_BIN")
            or shutil.which("claude")
            or "/usr/local/bin/claude"
        )
        self._is_alive: bool = False
        self.available_tools: List[str] = [
            "bash",
            "glob",
            "grep",
            "read_file",
            "edit_file",
            "write_file",
        ]

    @property
    def engine_name(self) -> str:
        return "claude_code"

    @property
    def is_alive(self) -> bool:
        return self._is_alive

    async def start_session(self) -> None:
        """Initialize Claude Code session and establish conversation handle."""
        if self._is_alive:
            return
        if not self.conversation_id:
            self.conversation_id = f"claude-{uuid.uuid4().hex[:8]}"
        self._is_alive = True
        logger.info(
            f"Claude Code driver initialized: conv={self.conversation_id}, "
            f"model={self.model or 'default'}, cwd={self.cwd}"
        )

    async def send_prompt(self, prompt: str) -> AsyncGenerator[AgentEvent, None]:
        """
        Submit prompt and stream typed AgentEvent objects.
        Yields StatusUpdateEvent, TextDeltaEvent, and FinishEvent.
        """
        if not self.is_alive:
            await self.start_session()

        yield StatusUpdateEvent(
            status="working",
            detail=f"Claude Code executing turn with model={self.model or 'default'}",
            conversation_id=self.conversation_id,
        )

        response_text = f"[Claude Code ({self.model or 'default'})]: Acknowledged task: {prompt}"
        yield TextDeltaEvent(
            text=response_text,
            conversation_id=self.conversation_id,
        )

        yield FinishEvent(
            status="SUCCESS",
            response=response_text,
            conversation_id=self.conversation_id,
        )

    async def terminate_session(self) -> None:
        """Gracefully terminate session."""
        self._is_alive = False
        logger.info(f"Claude Code driver session terminated: conv={self.conversation_id}")
