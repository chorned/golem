"""
golem.webui - Real-Time Observability WebUI.

Provides the embedded Command Center WebUI and SSE broadcasting engine.
Streams live ingress events, health probes, conversational liaison turns,
and dynamic Hand worker PTY panes.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
from __future__ import annotations

from golem.webui.server import (
    LINEAGE,
    VERSION,
    DASHBOARD_HTML,
    INDEX_HTML,
    WebUIServer,
    start_webui_server,
    stop_webui_server,
    get_active_webui_server,
    webui_broadcast,
    webui_broadcast_async,
    format_sse_event,
    get_system_status,
    create_app,
)

__all__ = [
    "LINEAGE",
    "VERSION",
    "DASHBOARD_HTML",
    "INDEX_HTML",
    "WebUIServer",
    "start_webui_server",
    "stop_webui_server",
    "get_active_webui_server",
    "webui_broadcast",
    "webui_broadcast_async",
    "format_sse_event",
    "get_system_status",
    "create_app",
]
