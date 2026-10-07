"""Topics and wire formats shared by the controller and the simulated devices.

Three topics per device, all under ``devices/<device_id>/``:

``cmd``
    Controller to device. One JSON command per message.
``ack``
    Device to controller. One JSON acknowledgement per command, carrying the
    command id it answers and the device state after the command was applied.
``status``
    Device presence. Published retained on connect and also registered as the
    device's MQTT last will, so the broker marks the device offline when the
    connection drops without a clean disconnect.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, replace
from typing import Any

CMD_TOPIC = "devices/{device_id}/cmd"
ACK_TOPIC = "devices/{device_id}/ack"
STATUS_TOPIC = "devices/{device_id}/status"

ACK_FILTER = "devices/+/ack"
STATUS_FILTER = "devices/+/status"

ACTION_ARM = "arm"
ACTION_DISARM = "disarm"
ACTION_SET_MODE = "set_mode"
ACTIONS = (ACTION_ARM, ACTION_DISARM, ACTION_SET_MODE)

MODES = ("home", "away", "night")

RESULT_APPLIED = "applied"
RESULT_REJECTED = "rejected"


class ProtocolError(ValueError):
    """A message could not be read as the message type it arrived on."""


def new_command_id() -> str:
    """A command id unique across controllers, used to match acks and spot retries."""
    return uuid.uuid4().hex


def device_id_from_topic(topic: str) -> str:
    """Pull the device id out of a ``devices/<device_id>/<leaf>`` topic."""
    parts = topic.split("/")
    if len(parts) != 3 or parts[0] != "devices":
        raise ProtocolError(f"not a device topic: {topic!r}")
    return parts[1]


def _loads(raw: bytes | str) -> dict[str, Any]:
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"payload is not JSON: {exc}") from exc
    if not isinstance(decoded, dict):
        raise ProtocolError(f"payload is not a JSON object: {type(decoded).__name__}")
    return decoded


def _require_str(body: dict[str, Any], key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"missing or empty {key!r}")
    return value


def _dumps(body: dict[str, Any]) -> bytes:
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


@dataclass(frozen=True)
class Command:
    """One command addressed to one device."""

    command_id: str
    action: str
    params: dict[str, Any] = field(default_factory=dict)

    def encode(self) -> bytes:
        return _dumps({"command_id": self.command_id, "action": self.action, "params": self.params})

    @classmethod
    def decode(cls, raw: bytes | str) -> Command:
        body = _loads(raw)
        params = body.get("params", {})
        if not isinstance(params, dict):
            raise ProtocolError("params must be a JSON object")
        return cls(
            command_id=_require_str(body, "command_id"),
            action=_require_str(body, "action"),
            params=params,
        )


@dataclass(frozen=True)
class Ack:
    """A device's answer to one command."""

    command_id: str
    device_id: str
    result: str
    state: dict[str, Any] = field(default_factory=dict)
    duplicate: bool = False
    detail: str = ""

    @property
    def applied(self) -> bool:
        return self.result == RESULT_APPLIED

    def as_duplicate(self) -> Ack:
        """The same ack, flagged as a replay of an earlier one."""
        return replace(self, duplicate=True)

    def encode(self) -> bytes:
        return _dumps(
            {
                "command_id": self.command_id,
                "device_id": self.device_id,
                "result": self.result,
                "state": self.state,
                "duplicate": self.duplicate,
                "detail": self.detail,
            }
        )

    @classmethod
    def decode(cls, raw: bytes | str) -> Ack:
        body = _loads(raw)
        state = body.get("state", {})
        if not isinstance(state, dict):
            raise ProtocolError("state must be a JSON object")
        return cls(
            command_id=_require_str(body, "command_id"),
            device_id=_require_str(body, "device_id"),
            result=_require_str(body, "result"),
            state=state,
            duplicate=bool(body.get("duplicate", False)),
            detail=str(body.get("detail", "")),
        )


@dataclass(frozen=True)
class Status:
    """A device's presence, as held retained by the broker."""

    device_id: str
    online: bool

    def encode(self) -> bytes:
        return _dumps({"device_id": self.device_id, "online": self.online})

    @classmethod
    def decode(cls, raw: bytes | str) -> Status:
        body = _loads(raw)
        online = body.get("online")
        if not isinstance(online, bool):
            raise ProtocolError("online must be a JSON boolean")
        return cls(device_id=_require_str(body, "device_id"), online=online)
