"""Sends commands and reports what actually happened to them.

Each command carries a fresh command id. The controller publishes it, waits for
the matching ack, and on silence republishes the *same* command id after an
exponential backoff. Because the device is idempotent on command id, a retry
cannot apply the command a second time.

Every send ends in one of three statuses:

``acknowledged``
    The device answered. The ack says whether it applied or rejected the command.
``timed_out``
    The device was online but no ack arrived within the attempt budget.
``device_offline``
    The broker's retained status says the device is not connected, or it dropped
    off while the command was in flight. Nothing further is attempted.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .protocol import (
    ACK_FILTER,
    CMD_TOPIC,
    STATUS_FILTER,
    Ack,
    Command,
    ProtocolError,
    Status,
    new_command_id,
)

ACKNOWLEDGED = "acknowledged"
TIMED_OUT = "timed_out"
DEVICE_OFFLINE = "device_offline"

MAX_LATE_ACKS = 256


@dataclass(frozen=True)
class RetryPolicy:
    """How long to wait for an ack, and how hard to keep trying."""

    ack_timeout: float = 2.0
    max_attempts: int = 3
    backoff_base: float = 0.5
    backoff_factor: float = 2.0
    backoff_max: float = 8.0

    def backoff(self, attempt: int) -> float:
        """Seconds to wait after a failed 1-based ``attempt`` before the next one."""
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        return min(self.backoff_max, self.backoff_base * self.backoff_factor ** (attempt - 1))


@dataclass(frozen=True)
class CommandResult:
    """The outcome of one :meth:`Controller.send`."""

    status: str
    command_id: str
    device_id: str
    attempts: int
    elapsed: float
    ack: Ack | None = None
    detail: str = ""

    @property
    def ok(self) -> bool:
        """True only when the device answered *and* said it applied the command."""
        return self.status == ACKNOWLEDGED and self.ack is not None and self.ack.applied


@dataclass
class _Pending:
    device_id: str
    answered: threading.Event = field(default_factory=threading.Event)
    ack: Ack | None = None
    went_offline: bool = False


class Controller:
    """Sends commands to devices over one transport and tracks their presence."""

    def __init__(
        self,
        transport,
        *,
        policy: RetryPolicy | None = None,
        presence_timeout: float = 2.0,
        qos: int = 1,
    ) -> None:
        self._transport = transport
        self.policy = policy or RetryPolicy()
        self.presence_timeout = presence_timeout
        self._qos = qos
        self._cv = threading.Condition()
        self._pending: dict[str, _Pending] = {}
        self._presence: dict[str, bool] = {}
        self._late_acks: list[Ack] = []
        self._started = False
        transport.on_message(self._on_message)

    # lifecycle -------------------------------------------------------------

    def start(self) -> None:
        self._transport.connect()
        self._transport.subscribe(ACK_FILTER, qos=self._qos)
        self._transport.subscribe(STATUS_FILTER, qos=self._qos)
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        self._transport.disconnect()

    def __enter__(self) -> Controller:
        self.start()
        return self

    def __exit__(self, *_exc) -> None:
        self.stop()

    # observation -----------------------------------------------------------

    @property
    def late_acks(self) -> list[Ack]:
        """Acks that arrived for commands already given up on. Kept, never acted on."""
        with self._cv:
            return list(self._late_acks)

    def presence(self, device_id: str) -> bool | None:
        """``True``/``False`` once a status has been seen, ``None`` while unknown."""
        with self._cv:
            return self._presence.get(device_id)

    def wait_for_presence(self, device_id: str, timeout: float | None = None) -> bool:
        """Block until the device's retained status is known. ``False`` if offline or unknown.

        A device that has ever connected leaves a retained status on the broker,
        so a filter subscription delivers it immediately. Still unknown after the
        timeout therefore means the device has never connected, and is reported
        the same way as offline.
        """
        limit = self.presence_timeout if timeout is None else timeout
        deadline = time.monotonic() + limit
        with self._cv:
            while True:
                known = self._presence.get(device_id)
                if known is not None:
                    return known
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cv.wait(remaining)

    # sending ---------------------------------------------------------------

    def send(
        self,
        device_id: str,
        action: str,
        params: dict[str, Any] | None = None,
        *,
        command_id: str | None = None,
    ) -> CommandResult:
        """Deliver one command and report its final status."""
        command = Command(command_id or new_command_id(), action, params or {})
        topic = CMD_TOPIC.format(device_id=device_id)
        payload = command.encode()
        started = time.monotonic()

        def result(
            status: str,
            attempts: int,
            ack: Ack | None = None,
            detail: str = "",
        ) -> CommandResult:
            return CommandResult(
                status=status,
                command_id=command.command_id,
                device_id=device_id,
                attempts=attempts,
                elapsed=time.monotonic() - started,
                ack=ack,
                detail=detail,
            )

        if not self.wait_for_presence(device_id):
            known = self.presence(device_id)
            detail = "retained status says offline" if known is False else "no status seen"
            return result(DEVICE_OFFLINE, 0, detail=detail)

        pending = _Pending(device_id=device_id)
        with self._cv:
            self._pending[command.command_id] = pending
        try:
            for attempt in range(1, self.policy.max_attempts + 1):
                self._transport.publish(topic, payload, qos=self._qos)
                if pending.answered.wait(self.policy.ack_timeout):
                    if pending.went_offline:
                        return result(DEVICE_OFFLINE, attempt, detail="dropped off mid-command")
                    return result(ACKNOWLEDGED, attempt, ack=pending.ack)
                if attempt == self.policy.max_attempts:
                    break
                time.sleep(self.policy.backoff(attempt))
                if self.presence(device_id) is False:
                    return result(DEVICE_OFFLINE, attempt, detail="dropped off mid-command")
            return result(
                TIMED_OUT,
                self.policy.max_attempts,
                detail=f"no ack after {self.policy.max_attempts} attempts",
            )
        finally:
            with self._cv:
                self._pending.pop(command.command_id, None)

    # incoming --------------------------------------------------------------

    def _on_message(self, topic: str, payload: bytes) -> None:
        if topic.endswith("/ack"):
            self._on_ack(payload)
        elif topic.endswith("/status"):
            self._on_status(payload)

    def _on_ack(self, payload: bytes) -> None:
        try:
            ack = Ack.decode(payload)
        except ProtocolError:
            return
        with self._cv:
            pending = self._pending.get(ack.command_id)
            if pending is None or pending.device_id != ack.device_id:
                self._late_acks.append(ack)
                del self._late_acks[:-MAX_LATE_ACKS]
                return
            pending.ack = ack
        pending.answered.set()

    def _on_status(self, payload: bytes) -> None:
        try:
            status = Status.decode(payload)
        except ProtocolError:
            return
        stranded: list[_Pending] = []
        with self._cv:
            self._presence[status.device_id] = status.online
            if not status.online:
                stranded = [p for p in self._pending.values() if p.device_id == status.device_id]
            self._cv.notify_all()
        for pending in stranded:
            if pending.ack is None:
                pending.went_offline = True
                pending.answered.set()
