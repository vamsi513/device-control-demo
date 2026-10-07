"""An in-process stand-in for an MQTT broker, for tests that should not need Docker.

It covers only what this project uses: topic filters with ``+`` and ``#``,
retained messages, and last wills. QoS is accepted and ignored, and delivery is
synchronous on the publishing thread, so a test that publishes has already seen
every handler run by the time ``publish`` returns. Delayed acks still arrive on
their own thread, because the device schedules those on a timer.
"""

from __future__ import annotations

import threading

from device_control.transport import MessageHandler


def topic_matches(topic_filter: str, topic: str) -> bool:
    """MQTT filter matching for the ``+`` and ``#`` wildcards."""
    filter_parts = topic_filter.split("/")
    topic_parts = topic.split("/")
    for index, part in enumerate(filter_parts):
        if part == "#":
            return True
        if index >= len(topic_parts):
            return False
        if part not in ("+", topic_parts[index]):
            return False
    return len(filter_parts) == len(topic_parts)


class FakeBroker:
    """Routes messages between :class:`FakeTransport` clients."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._clients: list[FakeTransport] = []
        self._retained: dict[str, bytes] = {}
        self.published: list[tuple[str, bytes, bool]] = []

    def register(self, client: FakeTransport) -> None:
        with self._lock:
            if client not in self._clients:
                self._clients.append(client)

    def unregister(self, client: FakeTransport) -> None:
        with self._lock:
            if client in self._clients:
                self._clients.remove(client)

    def retained_matching(self, topic_filter: str) -> list[tuple[str, bytes]]:
        with self._lock:
            return [
                (topic, payload)
                for topic, payload in self._retained.items()
                if topic_matches(topic_filter, topic)
            ]

    def publish(self, topic: str, payload: bytes, *, retain: bool = False) -> None:
        with self._lock:
            self.published.append((topic, payload, retain))
            if retain:
                if payload:
                    self._retained[topic] = payload
                else:
                    self._retained.pop(topic, None)
            targets = [c for c in self._clients if c.wants(topic)]
        for client in targets:
            client.deliver(topic, payload)


class FakeTransport:
    """A :class:`~device_control.transport.Transport` backed by :class:`FakeBroker`."""

    def __init__(self, broker: FakeBroker, client_id: str = "client") -> None:
        self.client_id = client_id
        self.connected = False
        self.filters: list[str] = []
        self._broker = broker
        self._handler: MessageHandler | None = None
        self._will: tuple[str, bytes, bool] | None = None

    # Transport -------------------------------------------------------------

    def set_will(self, topic: str, payload: bytes, *, qos: int = 1, retain: bool = True) -> None:
        self._will = (topic, payload, retain)

    def on_message(self, handler: MessageHandler) -> None:
        self._handler = handler

    def connect(self) -> None:
        self.connected = True
        self._broker.register(self)

    def subscribe(self, topic_filter: str, *, qos: int = 1) -> None:
        self.filters.append(topic_filter)
        for topic, payload in self._broker.retained_matching(topic_filter):
            self.deliver(topic, payload)

    def publish(self, topic: str, payload: bytes, *, qos: int = 1, retain: bool = False) -> None:
        if not self.connected:
            raise RuntimeError(f"{self.client_id} is not connected")
        self._broker.publish(topic, payload, retain=retain)

    def disconnect(self) -> None:
        self.connected = False
        self._broker.unregister(self)

    def drop(self) -> None:
        self.connected = False
        self._broker.unregister(self)
        if self._will is not None:
            topic, payload, retain = self._will
            self._broker.publish(topic, payload, retain=retain)

    # routing ---------------------------------------------------------------

    def wants(self, topic: str) -> bool:
        return self.connected and any(topic_matches(f, topic) for f in self.filters)

    def deliver(self, topic: str, payload: bytes) -> None:
        if self.connected and self._handler is not None:
            self._handler(topic, payload)
