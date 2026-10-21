"""
golem: Autonomous Fleet Chief of Staff.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from __future__ import annotations

import importlib
import importlib.abc
import importlib.util
import sys

__version__ = "2.1.0"
LINEAGE = "Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."

_LEGACY_ROOT_MODULES = frozenset({
    "agent_core",
    "bluebubbles_transport",
    "github_manager",
    "herdr_client",
    "herdr_fleet",
    "herdr_tools",
    "memory_manager",
    "normalizer",
    "supervisor",
    "telemetry_observer",
})


class _LegacyRootAliasLoader(importlib.abc.Loader):
    def __init__(self, target_name: str) -> None:
        self.target_name = target_name

    def create_module(self, spec):  # type: ignore[no-untyped-def]
        return importlib.import_module(self.target_name)

    def exec_module(self, module) -> None:  # type: ignore[no-untyped-def]
        short_name = self.target_name.split(".")[-1]
        module.__name__ = self.target_name
        sys.modules[short_name] = module
        sys.modules[self.target_name] = module


class _LegacyRootAliasFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path=None, target=None):  # type: ignore[no-untyped-def]
        if fullname in _LEGACY_ROOT_MODULES:
            target_mod = f"golem.{fullname}"
            return importlib.util.spec_from_loader(fullname, _LegacyRootAliasLoader(target_mod))
        return None


if not any(isinstance(f, _LegacyRootAliasFinder) for f in sys.meta_path):
    sys.meta_path.insert(0, _LegacyRootAliasFinder())

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

# Consolidated root modules (exposed both as golem.<mod> and top-level <mod>)
from . import herdr_client
from . import herdr_fleet
from . import herdr_tools
from . import github_manager
from . import memory_manager
from . import supervisor
from . import telemetry_observer
from . import bluebubbles_transport
from . import agent_core

for _legacy_name, _legacy_mod in (
    ("agent_core", agent_core),
    ("bluebubbles_transport", bluebubbles_transport),
    ("github_manager", github_manager),
    ("herdr_client", herdr_client),
    ("herdr_fleet", herdr_fleet),
    ("herdr_tools", herdr_tools),
    ("memory_manager", memory_manager),
    ("normalizer", normalizer),
    ("supervisor", supervisor),
    ("telemetry_observer", telemetry_observer),
):
    sys.modules[_legacy_name] = _legacy_mod

