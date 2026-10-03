"""
golem.sensory - Sensory layer for First Mate 2.0.

Provides asynchronous sensory organs and internal event communications:
- The Shoulder Tap Event Bus: ShoulderTapEventBus, ShoulderTapAlert, get_default_event_bus, get_event_bus
- The Eyes Task Observer: EyesTaskObserver (buffer watcher, zero-regex classifier, SHA-256 change hashing)
- The Nose Sentinel: NoseSentinel (24/7 fleet health watchdog, log sniffer, zero-regex evaluator)

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from golem.sensory.bus import (
    ShoulderTapAlert,
    ShoulderTapEventBus,
    get_default_event_bus,
    get_event_bus,
    peek_default_event_bus,
    reset_default_event_bus,
)
from golem.sensory.eyes import EyesTaskObserver, get_eyes_model
from golem.sensory.nose import (
    NoseSentinel,
    get_nose_model,
    get_default_nose_sentinel,
    set_default_nose_sentinel,
    FleetSnapshot,
    StatusDelta,
    AnomalyVerdict,
)

__all__ = [
    "ShoulderTapAlert",
    "ShoulderTapEventBus",
    "get_default_event_bus",
    "get_event_bus",
    "peek_default_event_bus",
    "reset_default_event_bus",
    "EyesTaskObserver",
    "get_eyes_model",
    "NoseSentinel",
    "get_nose_model",
    "get_default_nose_sentinel",
    "set_default_nose_sentinel",
    "FleetSnapshot",
    "StatusDelta",
    "AnomalyVerdict",
]
