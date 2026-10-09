#!/usr/bin/env python3
"""
golem/agent/drivers/opencode.py
Pluggable driver stub for OpenCode CLI engine.

Yields strongly typed AgentEvent objects: StatusUpdateEvent, TextDeltaEvent, FinishEvent.
Dynamically resolves OPENCODE_MODEL with ZERO hardcoded model strings.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import uuid
import shutil
import logging
from typing import AsyncGenerator, Dict, List, Optional, Any

from golem.drivers.base import BaseAgentDriver
from golem.protocol import (
    AgentEvent,
    StatusUpdateEvent,
    TextDeltaEvent,
    FinishEvent,
)
from golem.config import get_engine_model

logger = logging.getLogger("Golem.Agent.Drivers.OpenCode")


class OpenCodeDriver(BaseAgentDriver):
    """
    Pluggable driver stub for OpenCode CLI engine.
    Supports dynamic model resolution, headless session lifecycle, and typed event streaming.
    """

    def __init__(
        self,
        cwd: str,
        model: Optional[str] = None,
        extra_args: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        opencode_bin: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        resolved_model = model or get_engine_model("opencode") or None
        super().__init__(cwd=cwd, model=resolved_model, extra_args=extra_args, env=env, **kwargs)
        self.opencode_bin: str = (
            opencode_bin
            or os.environ.get("OPENCODE_BIN")
            or shutil.which("opencode")
            or os.path.expanduser("~/.local/bin/opencode")
        )
        self._is_alive: bool = False
        self.available_tools: List[str] = [
            "bash",
            "file_editor",
            "code_search",
        ]

    @property
    def engine_name(self) -> str:
        return "opencode"

    @property
    def is_alive(self) -> bool:
        return self._is_alive

    async def start_session(self) -> None:
        """Initialize OpenCode session and establish conversation handle."""
        if self._is_alive:
            return
        if not self.conversation_id:
            self.conversation_id = f"opencode-{uuid.uuid4().hex[:8]}"
        self._is_alive = True
        logger.info(
            f"OpenCode driver initialized: conv={self.conversation_id}, "
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
            detail=f"OpenCode executing turn with model={self.model or 'default'}",
            conversation_id=self.conversation_id,
        )

        response_text = f"[OpenCode ({self.model or 'default'})]: Acknowledged task: {prompt}"
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
        logger.info(f"OpenCode driver session terminated: conv={self.conversation_id}")
