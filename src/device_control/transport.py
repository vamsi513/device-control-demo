"""The small MQTT surface this project needs, and a paho-backed implementation.

The controller and the device talk to a ``Transport`` rather than to paho
directly, so the unit tests can drive both halves through an in-process fake
broker and the integration tests can drive the same code over real MQTT.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Protocol, runtime_checkable

import paho.mqtt.client as mqtt

MessageHandler = Callable[[str, bytes], None]

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 1883
DEFAULT_KEEPALIVE = 15


@runtime_checkable
class Transport(Protocol):
    """A connected MQTT session, reduced to what this project uses."""

    def set_will(self, topic: str, payload: bytes, *, qos: int = 1, retain: bool = True) -> None:
        """Register a last will. Must be called before :meth:`connect`."""

    def on_message(self, handler: MessageHandler) -> None:
        """Install the callback for every message on a subscribed filter."""

    def connect(self) -> None: ...

    def subscribe(self, topic_filter: str, *, qos: int = 1) -> None: ...

    def publish(
        self, topic: str, payload: bytes, *, qos: int = 1, retain: bool = False
    ) -> None: ...

    def disconnect(self) -> None:
        """Disconnect cleanly. The broker discards the last will."""

    def drop(self) -> None:
        """Vanish without a DISCONNECT, so the broker publishes the last will."""


class ConnectError(RuntimeError):
    """The broker could not be reached, or refused the connection."""


class PahoTransport:
    """A :class:`Transport` over paho-mqtt, running paho's own network thread."""

    def __init__(
        self,
        client_id: str,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        keepalive: int = DEFAULT_KEEPALIVE,
        connect_timeout: float = 5.0,
    ) -> None:
        self.client_id = client_id
        self._host = host
        self._port = port
        self._keepalive = keepalive
        self._connect_timeout = connect_timeout
        self._handler: MessageHandler | None = None
        self._connected = threading.Event()
        self._connect_rc: int | None = None
        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=True,
        )
        self._client.on_connect = self._handle_connect
        self._client.on_message = self._handle_message

    # Transport -------------------------------------------------------------

    def set_will(self, topic: str, payload: bytes, *, qos: int = 1, retain: bool = True) -> None:
        self._client.will_set(topic, payload, qos=qos, retain=retain)

    def on_message(self, handler: MessageHandler) -> None:
        self._handler = handler

    def connect(self) -> None:
        try:
            self._client.connect(self._host, self._port, self._keepalive)
        except OSError as exc:
            raise ConnectError(f"cannot reach {self._host}:{self._port}: {exc}") from exc
        self._client.loop_start()
        if not self._connected.wait(self._connect_timeout):
            self._client.loop_stop()
            raise ConnectError(f"no CONNACK from {self._host}:{self._port} in time")
        if self._connect_rc:
            self._client.loop_stop()
            raise ConnectError(f"broker refused the connection, reason code {self._connect_rc}")

    def subscribe(self, topic_filter: str, *, qos: int = 1) -> None:
        self._client.subscribe(topic_filter, qos=qos)

    def publish(self, topic: str, payload: bytes, *, qos: int = 1, retain: bool = False) -> None:
        self._client.publish(topic, payload, qos=qos, retain=retain)

    def disconnect(self) -> None:
        self._client.disconnect()
        self._client.loop_stop()
        self._connected.clear()

    def drop(self) -> None:
        # Closing the socket without sending DISCONNECT is what makes the broker
        # treat this as a lost device and publish the last will. loop_stop first
        # so paho's network thread is not reading the socket as it closes.
        self._client.loop_stop()
        sock = self._client.socket()
        if sock is not None:
            sock.close()
        self._connected.clear()

    # paho callbacks --------------------------------------------------------

    def _handle_connect(self, _client, _userdata, _flags, reason_code, _properties=None) -> None:
        self._connect_rc = int(getattr(reason_code, "value", reason_code) or 0)
        self._connected.set()

    def _handle_message(self, _client, _userdata, message) -> None:
        if self._handler is not None:
            self._handler(message.topic, message.payload)
