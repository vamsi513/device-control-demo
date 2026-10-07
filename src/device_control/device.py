"""A simulated device: applies commands, acknowledges them, and can misbehave on purpose.

:class:`SimulatedDevice` is the command handling on its own, with no MQTT in it.
:class:`DeviceRunner` wires one to a :class:`~device_control.transport.Transport`.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from .protocol import (
    ACK_TOPIC,
    ACTION_ARM,
    ACTION_DISARM,
    ACTION_SET_MODE,
    CMD_TOPIC,
    MODES,
    RESULT_APPLIED,
    RESULT_REJECTED,
    STATUS_TOPIC,
    Ack,
    Command,
    ProtocolError,
    Status,
)

DEFAULT_HISTORY_SIZE = 256


@dataclass
class DeviceState:
    """Everything a command can change."""

    armed: bool = False
    mode: str = "home"

    def as_dict(self) -> dict[str, Any]:
        return {"armed": self.armed, "mode": self.mode}


@dataclass
class FaultConfig:
    """Deliberate misbehaviour, so the controller's failure paths can be tested."""

    drop_acks: int = 0
    """Suppress this many acks before publishing any. The command is still applied."""

    ack_delay: float = 0.0
    """Seconds to hold an ack back, which can push it past the controller's timeout."""

    offline_after: float | None = None
    """Seconds after start to drop the connection, so the broker fires the last will."""


class SimulatedDevice:
    """Applies commands to :class:`DeviceState` and answers each one exactly once.

    A command id already seen is never applied again: the stored ack is replayed
    with ``duplicate`` set, so a controller retry cannot arm a device twice.
    """

    def __init__(
        self,
        device_id: str,
        *,
        state: DeviceState | None = None,
        faults: FaultConfig | None = None,
        history_size: int = DEFAULT_HISTORY_SIZE,
    ) -> None:
        self.device_id = device_id
        self.state = state or DeviceState()
        self.faults = faults or FaultConfig()
        self.received_count = 0
        self.applied_count = 0
        self.dropped_ack_count = 0
        self._history: OrderedDict[str, Ack] = OrderedDict()
        self._history_size = history_size
        self._lock = threading.Lock()

    def handle(self, raw: bytes | str) -> Ack | None:
        """Take one command payload. Return the ack to publish, or ``None`` to stay quiet.

        ``None`` means either an unreadable payload or an ack this device was
        configured to drop.
        """
        with self._lock:
            self.received_count += 1
            try:
                command = Command.decode(raw)
            except ProtocolError:
                return None

            prior = self._history.get(command.command_id)
            if prior is not None:
                ack = prior.as_duplicate()
            else:
                ack = self._apply(command)
                self._remember(command.command_id, ack)

            if self.dropped_ack_count < self.faults.drop_acks:
                self.dropped_ack_count += 1
                return None
            return ack

    def _apply(self, command: Command) -> Ack:
        if command.action == ACTION_ARM:
            self.state.armed = True
        elif command.action == ACTION_DISARM:
            self.state.armed = False
        elif command.action == ACTION_SET_MODE:
            mode = command.params.get("mode")
            if mode not in MODES:
                return self._ack(command, RESULT_REJECTED, f"unknown mode: {mode!r}")
            self.state.mode = mode
        else:
            return self._ack(command, RESULT_REJECTED, f"unknown action: {command.action!r}")

        self.applied_count += 1
        return self._ack(command, RESULT_APPLIED)

    def _ack(self, command: Command, result: str, detail: str = "") -> Ack:
        return Ack(
            command_id=command.command_id,
            device_id=self.device_id,
            result=result,
            state=self.state.as_dict(),
            detail=detail,
        )

    def _remember(self, command_id: str, ack: Ack) -> None:
        self._history[command_id] = ack
        while len(self._history) > self._history_size:
            self._history.popitem(last=False)


class DeviceRunner:
    """Runs one :class:`SimulatedDevice` on a transport.

    On start it registers an offline last will, connects, publishes a retained
    online status, and subscribes to its own command topic.
    """

    def __init__(self, device: SimulatedDevice, transport, *, qos: int = 1) -> None:
        self.device = device
        self._transport = transport
        self._qos = qos
        self._status_topic = STATUS_TOPIC.format(device_id=device.device_id)
        self._ack_topic = ACK_TOPIC.format(device_id=device.device_id)
        self._cmd_topic = CMD_TOPIC.format(device_id=device.device_id)
        self._timers: list[threading.Timer] = []
        self._timer_lock = threading.Lock()
        self._started = False

    @property
    def command_topic(self) -> str:
        return self._cmd_topic

    def start(self) -> None:
        offline = Status(self.device.device_id, online=False).encode()
        self._transport.set_will(self._status_topic, offline, qos=self._qos, retain=True)
        self._transport.on_message(self._on_message)
        self._transport.connect()
        online = Status(self.device.device_id, online=True).encode()
        self._transport.publish(self._status_topic, online, qos=self._qos, retain=True)
        self._transport.subscribe(self._cmd_topic, qos=self._qos)
        self._started = True
        if self.device.faults.offline_after is not None:
            self._schedule(self.device.faults.offline_after, self.go_offline)

    def stop(self) -> None:
        """Disconnect cleanly, leaving a retained offline status behind."""
        self._cancel_timers()
        if not self._started:
            return
        self._started = False
        offline = Status(self.device.device_id, online=False).encode()
        self._transport.publish(self._status_topic, offline, qos=self._qos, retain=True)
        self._transport.disconnect()

    def go_offline(self) -> None:
        """Vanish without warning, leaving the broker to publish the last will."""
        self._cancel_timers()
        if not self._started:
            return
        self._started = False
        self._transport.drop()

    def _on_message(self, topic: str, payload: bytes) -> None:
        if topic != self._cmd_topic:
            return
        ack = self.device.handle(payload)
        if ack is None:
            return
        delay = self.device.faults.ack_delay
        if delay > 0:
            self._schedule(delay, self._publish_ack, ack)
        else:
            self._publish_ack(ack)

    def _publish_ack(self, ack: Ack) -> None:
        self._transport.publish(self._ack_topic, ack.encode(), qos=self._qos)

    def _schedule(self, delay: float, fn, *args) -> None:
        timer = threading.Timer(delay, fn, args=args)
        timer.daemon = True
        with self._timer_lock:
            self._timers = [t for t in self._timers if t.is_alive()]
            self._timers.append(timer)
        timer.start()

    def _cancel_timers(self) -> None:
        with self._timer_lock:
            timers, self._timers = self._timers, []
        for timer in timers:
            timer.cancel()
