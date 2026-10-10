"""The small MQTT surface this project needs, and a paho-backed implementation.

The controller and the device talk to a ``Transport`` rather than to paho
directly, so the unit tests can drive both halves through an in-process fake
broker and the integration tests can drive the same code over real MQTT.
"""

from __future__ import annotations

import os
import ssl
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
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


@dataclass(frozen=True)
class TlsConfig:
    """TLS settings for a broker connection.

    Off by default everywhere. Certificate verification is on unless `insecure` is
    set, and that flag exists only for a self signed broker on a developer machine:
    it disables hostname checking, so it must never be used against anything real.
    """

    ca_certs: str | None = None
    certfile: str | None = None
    keyfile: str | None = None
    insecure: bool = False

    def apply(self, client: mqtt.Client) -> None:
        client.tls_set(
            ca_certs=self.ca_certs,
            certfile=self.certfile,
            keyfile=self.keyfile,
            cert_reqs=ssl.CERT_NONE if self.insecure else ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        if self.insecure:
            client.tls_insecure_set(True)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> TlsConfig | None:
        """Build a config from MQTT_TLS_* variables, or None when TLS is off.

        Reading this from the environment rather than arguments keeps credentials
        out of command lines, where they would show up in shell history and in the
        process list.
        """
        source = os.environ if env is None else env
        if source.get("MQTT_TLS", "").lower() not in ("1", "true", "yes", "on"):
            return None
        return cls(
            ca_certs=source.get("MQTT_TLS_CA") or None,
            certfile=source.get("MQTT_TLS_CERT") or None,
            keyfile=source.get("MQTT_TLS_KEY") or None,
            insecure=source.get("MQTT_TLS_INSECURE", "").lower() in ("1", "true", "yes", "on"),
        )


def credentials_from_env(
    env: Mapping[str, str] | None = None,
) -> tuple[str | None, str | None]:
    """Username and password from MQTT_USERNAME and MQTT_PASSWORD, or (None, None).

    Returns the username as None when unset, which is what switches authentication
    off in :class:`PahoTransport`.
    """
    source = os.environ if env is None else env
    username = source.get("MQTT_USERNAME") or None
    password = source.get("MQTT_PASSWORD") or None
    return username, password


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
        username: str | None = None,
        password: str | None = None,
        tls: TlsConfig | None = None,
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
        # Both are off unless asked for. The bundled broker is anonymous and
        # plaintext on purpose, so the default path needs no configuration, and a
        # deployment that needs more turns these on explicitly.
        if username is not None:
            self._client.username_pw_set(username, password)
        if tls is not None:
            tls.apply(self._client)
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
