"""
golem: Autonomous Fleet Chief of Staff.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from __future__ import annotations

__version__ = "2.1.0"
LINEAGE = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."

# Primary flattened modules
from . import config
from . import cli
from . import notify

from . import ingress
from . import router
from . import session
from . import mouth
from . import adapter
from . import protocol
from . import events
from . import prompts
from . import calibration
from . import drivers

from . import harness
from . import runner
from . import delegation
from . import buffer
from . import mesh

from . import bus
from . import eyes
from . import nose

from . import chunker
from . import file_transfer
from . import milestone
from . import normalizer
from . import slack_client

from . import webui

# Subpackage compatibility facades
from . import gateway
from . import agent
from . import execution
from . import sensory
from . import transport

# Key top-level exports
from .ingress import IngressGateway
from .router import SessionRouter
from .session import SessionHandle, SessionState
from .mouth import generate_mouth_response, get_mouth_model
from .harness import DualPaneHarness
from .runner import RunnerExecutor, ExecutionHandle
from .bus import ShoulderTapEventBus, ShoulderTapAlert, get_default_event_bus
from .eyes import EyesTaskObserver
from .nose import NoseSentinel
from .normalizer import sanitize_for_slack, transpile_markdown_to_mrkdwn
from .chunker import split_message_boundary_aware
from .delegation import WorkerDelegator
from .drivers import BaseAgentDriver, AntigravityDriver
from .protocol import (
    AgentEvent,
    AgentEventType,
    TextDeltaEvent,
    StatusUpdateEvent,
    ToolStartEvent,
    ToolEndEvent,
    AwaitingInputEvent,
    FinishEvent,
)
from .adapter import HeadlessAgentAdapter, DEFAULT_AGY_BIN
from .calibration import (
    CALIBRATION_PROMPT,
    SLACK_MRKDWN_RULES,
    CHOICE_FORMATTING_RULES,
    PACING_CALIBRATION_RULES,
    TRIAGE_SUPPRESSION_RULES,
    get_calibration_rules,
    build_calibrated_system_prompt,
)

__all__ = [
    "__version__",
    "LINEAGE",
    "config",
    "cli",
    "notify",
    "ingress",
    "router",
    "session",
    "mouth",
    "adapter",
    "protocol",
    "events",
    "prompts",
    "calibration",
    "drivers",
    "harness",
    "runner",
    "delegation",
    "buffer",
    "mesh",
    "bus",
    "eyes",
    "nose",
    "chunker",
    "file_transfer",
    "milestone",
    "normalizer",
    "slack_client",
    "webui",
    "gateway",
    "agent",
    "execution",
    "sensory",
    "transport",
    "IngressGateway",
    "SessionRouter",
    "SessionHandle",
    "SessionState",
    "DualPaneHarness",
    "RunnerExecutor",
    "ExecutionHandle",
    "ShoulderTapEventBus",
    "ShoulderTapAlert",
    "get_default_event_bus",
    "EyesTaskObserver",
    "NoseSentinel",
    "sanitize_for_slack",
    "transpile_markdown_to_mrkdwn",
    "split_message_boundary_aware",
    "WorkerDelegator",
    "BaseAgentDriver",
    "AntigravityDriver",
]
