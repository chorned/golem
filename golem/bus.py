#!/usr/bin/env python3
"""
golem/sensory/bus.py
Shoulder Tap Event Bus for internal asynchronous sensory alert dispatch.

Connects sensory observers (The Eyes, The Nose) to The Mouth liaison via typed
ShoulderTapAlert events ("blocker", "completion", "health_anomaly").
Provides non-blocking async publish, type-filtered subscriptions, and session-scoped queues.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import time
import inspect
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Set, Callable, Awaitable, Union, Literal, Tuple

logger = logging.getLogger("Golem.Sensory.Bus")

AlertSource = Union[Literal["eyes", "nose", "runner", "system", "mouth"], str]
AlertType = Union[
    Literal[
        "blocker",
        "completion",
        "health_anomaly",
        "eye_log",
        "eye_attached",
        "eye_inspection",
        "eye_blocker",
        "eye_telemetry",
        "nose_alert",
        "mouth_briefing",
    ],
    str,
]


@dataclass
class ShoulderTapAlert:
    """
    Typed sensory notification dispatched over the Shoulder Tap Event Bus.
    Used by The Eyes (task blockers, completions) and The Nose (health anomalies)
    to alert The Mouth liaison.
    """
    alert_type: AlertType = "health_anomaly"
    source: AlertSource = "system"
    session_id: Optional[str] = None
    channel: Optional[str] = None
    thread_ts: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    summary: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    details: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.details is not None and not self.data:
            self.data = self.details
        elif self.data and self.details is None:
            self.details = self.data


    @property
    def is_blocker(self) -> bool:
        """True if this alert signals an interactive blocker."""
        return self.alert_type == "blocker"

    @property
    def is_completion(self) -> bool:
        """True if this alert signals background task completion."""
        return self.alert_type == "completion"

    @property
    def is_health_anomaly(self) -> bool:
        """True if this alert signals an operational fleet/bot health anomaly."""
        return self.alert_type == "health_anomaly"

    @property
    def is_eye_log(self) -> bool:
        """True if this alert signals an eye log frame."""
        return self.alert_type == "eye_log"

    @property
    def is_eye_telemetry(self) -> bool:
        """True if this alert signals eye telemetry."""
        return self.alert_type == "eye_telemetry"

    @property
    def is_eye_observation(self) -> bool:
        """True for structured Eye observation events (attached / inspection / blocker)."""
        return self.alert_type in ("eye_attached", "eye_inspection", "eye_blocker")

    @property
    def is_nose_alert(self) -> bool:
        """True if this alert signals a nose alert or health anomaly."""
        return self.alert_type in ("nose_alert", "health_anomaly")

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to serializable dictionary."""
        return {
            "alert_type": self.alert_type,
            "source": self.source,
            "session_id": self.session_id,
            "channel": self.channel,
            "thread_ts": self.thread_ts,
            "timestamp": self.timestamp,
            "summary": self.summary,
            "data": dict(self.data),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ShoulderTapAlert:
        """Construct alert from dictionary representation."""
        return cls(
            alert_type=data.get("alert_type", "blocker"),
            source=data.get("source", "system"),
            session_id=data.get("session_id"),
            channel=data.get("channel"),
            thread_ts=data.get("thread_ts"),
            timestamp=data.get("timestamp", time.time()),
            summary=data.get("summary", ""),
            data=data.get("data", {}),
        )


SubscriberCallback = Callable[[ShoulderTapAlert], Union[Awaitable[None], None]]


class ShoulderTapEventBus:
    """
    Asynchronous in-memory event bus routing ShoulderTapAlerts across First Mate components.

    Features:
    - Non-blocking async publish() with automatic task scheduling.
    - Type-filtered global subscriptions (e.g. subscribe only to 'blocker').
    - Session-scoped queues via get_session_queue(session_id) for thread-level waking of The Mouth.
    - Overflow protection and error isolation across all subscribers.
    """

    def __init__(self, queue_maxsize: int = 100) -> None:
        self.queue_maxsize = queue_maxsize
        self._subscribers: List[Tuple[SubscriberCallback, Optional[Set[str]]]] = []
        self._session_queues: Dict[str, asyncio.Queue[ShoulderTapAlert]] = {}
        self._active_tasks: Set[asyncio.Task[Any]] = set()

    async def emit(self, alert: ShoulderTapAlert) -> None:
        """
        Asynchronously emit an alert to all subscribers and await task completion.
        """
        self.publish(alert)
        if self._active_tasks:
            tasks = list(self._active_tasks)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    def publish(self, alert: ShoulderTapAlert) -> None:
        """
        Publish an alert non-blockingly to all registered subscribers and matching session queues.
        Safe to call from async coroutines or synchronous callbacks.
        """
        # 1. Enqueue to session queue if alert has session_id and queue exists
        if alert.session_id and alert.session_id in self._session_queues:
            queue = self._session_queues[alert.session_id]

            def _enqueue_alert() -> None:
                try:
                    queue.put_nowait(alert)
                except asyncio.QueueFull:
                    logger.warning(
                        f"Session queue '{alert.session_id}' full (maxsize={self.queue_maxsize}); dropping oldest alert"
                    )
                    try:
                        queue.get_nowait()
                        queue.task_done()
                    except Exception:
                        pass
                    try:
                        queue.put_nowait(alert)
                    except Exception as e:
                        logger.error(f"Failed to enqueue alert for session '{alert.session_id}': {e}")

            q_loop = getattr(queue, "_loop", None)
            try:
                curr_loop = asyncio.get_running_loop()
            except RuntimeError:
                curr_loop = None

            if q_loop is not None and curr_loop is not None and q_loop != curr_loop and q_loop.is_running():
                q_loop.call_soon_threadsafe(_enqueue_alert)
            else:
                _enqueue_alert()

        # 2. Dispatch to global subscribers
        subscribers_snapshot = list(self._subscribers)
        for callback, filter_types in subscribers_snapshot:
            if filter_types is not None and alert.alert_type not in filter_types:
                continue

            if inspect.iscoroutinefunction(callback):
                try:
                    loop = asyncio.get_running_loop()
                    task = loop.create_task(self._safe_invoke_async(callback, alert))
                    self._active_tasks.add(task)
                    task.add_done_callback(self._active_tasks.discard)
                except RuntimeError:
                    # No running event loop in current thread
                    try:
                        asyncio.run(self._safe_invoke_async(callback, alert))
                    except Exception as e:
                        logger.error(f"Error invoking async subscriber without running loop: {e}")
            else:
                try:
                    res = callback(alert)
                    if inspect.isawaitable(res):
                        try:
                            loop = asyncio.get_running_loop()
                            task = loop.create_task(self._safe_await_coro(res))
                            self._active_tasks.add(task)
                            task.add_done_callback(self._active_tasks.discard)
                        except RuntimeError:
                            asyncio.run(self._safe_await_coro(res))
                except Exception as e:
                    logger.error(f"Error in synchronous subscriber callback {callback}: {e}", exc_info=True)

    async def _safe_invoke_async(self, callback: SubscriberCallback, alert: ShoulderTapAlert) -> None:
        """Invoke an async subscriber with exception isolation."""
        try:
            res = callback(alert)
            if inspect.isawaitable(res):
                await res
        except Exception as e:
            logger.error(f"Error in async subscriber callback {callback}: {e}", exc_info=True)

    async def _safe_await_coro(self, coro: Awaitable[None]) -> None:
        """Await a coroutine object safely with exception isolation."""
        try:
            await coro
        except Exception as e:
            logger.error(f"Error awaiting subscriber coroutine: {e}", exc_info=True)

    def subscribe(
        self,
        callback: SubscriberCallback,
        alert_types: Optional[Union[List[str], Set[str]]] = None,
    ) -> Callable[[], None]:
        """
        Subscribe a callback function to alerts.
        Optionally filter by alert_types (e.g. ['blocker', 'completion']).
        Returns an unsubscribe callable.
        """
        filter_set = set(alert_types) if alert_types else None
        entry = (callback, filter_set)
        self._subscribers.append(entry)

        def unsubscribe() -> None:
            try:
                self._subscribers.remove(entry)
            except ValueError:
                pass

        return unsubscribe

    def unsubscribe(self, callback: SubscriberCallback) -> None:
        """Remove a subscriber callback directly."""
        self._subscribers = [
            (cb, f) for cb, f in self._subscribers if cb != callback
        ]

    def get_session_queue(self, session_id: str) -> asyncio.Queue[ShoulderTapAlert]:
        """
        Get or create a dedicated asyncio.Queue for a session.
        Incoming alerts matching session_id will be automatically deposited into this queue.
        """
        if session_id not in self._session_queues:
            self._session_queues[session_id] = asyncio.Queue(maxsize=self.queue_maxsize)
        return self._session_queues[session_id]

    def remove_session_queue(self, session_id: str) -> Optional[asyncio.Queue[ShoulderTapAlert]]:
        """Remove and clean up a session's queue upon session teardown."""
        return self._session_queues.pop(session_id, None)

    def has_session_queue(self, session_id: str) -> bool:
        """Check if an active session queue exists for session_id."""
        return session_id in self._session_queues

    def clear(self) -> None:
        """Reset all subscribers, queues, and cancel active background dispatch tasks."""
        self._subscribers.clear()
        self._session_queues.clear()
        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()
        self._active_tasks.clear()


# Global bus singleton and accessor helpers
_GLOBAL_BUS: Optional[ShoulderTapEventBus] = None


def peek_default_event_bus() -> Optional[ShoulderTapEventBus]:
    """Retrieve global default bus if already instantiated, without creating a new one."""
    return _GLOBAL_BUS


def get_default_event_bus() -> ShoulderTapEventBus:
    """Retrieve or initialize the global default ShoulderTapEventBus."""
    global _GLOBAL_BUS
    if _GLOBAL_BUS is None:
        _GLOBAL_BUS = ShoulderTapEventBus()
    return _GLOBAL_BUS


def get_event_bus() -> ShoulderTapEventBus:
    """Alias for get_default_event_bus."""
    return get_default_event_bus()


def reset_default_event_bus() -> None:
    """Reset the global default bus (primarily for test teardown and isolation)."""
    global _GLOBAL_BUS
    if _GLOBAL_BUS is not None:
        _GLOBAL_BUS.clear()
    _GLOBAL_BUS = None
