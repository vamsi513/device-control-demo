"""Fixtures for the fake-broker unit tests and the real-broker integration tests."""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from device_control.controller import Controller, RetryPolicy
from device_control.device import DeviceRunner, FaultConfig, SimulatedDevice
from device_control.protocol import STATUS_TOPIC
from device_control.transport import PahoTransport
from fake_broker import FakeBroker, FakeTransport

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))

# CI sets this so a missing broker fails the run instead of quietly skipping.
REQUIRE_MQTT = os.environ.get("REQUIRE_MQTT", "") not in ("", "0", "false")


@pytest.fixture
def broker() -> FakeBroker:
    return FakeBroker()


@pytest.fixture
def fast_policy() -> RetryPolicy:
    """Short timeouts, so the retry paths run in milliseconds."""
    return RetryPolicy(ack_timeout=0.1, max_attempts=3, backoff_base=0.02, backoff_factor=2.0)


@pytest.fixture
def make_device(broker):
    """Start simulated devices on the fake broker, stopping them at teardown."""
    runners: list[DeviceRunner] = []

    def factory(device_id: str = "dev-1", **faults) -> DeviceRunner:
        device = SimulatedDevice(device_id, faults=FaultConfig(**faults))
        runner = DeviceRunner(device, FakeTransport(broker, f"device-{device_id}"))
        runner.start()
        runners.append(runner)
        return runner

    yield factory
    for runner in reversed(runners):
        runner.stop()


@pytest.fixture
def make_controller(broker, fast_policy):
    """Start controllers on the fake broker, stopping them at teardown."""
    controllers: list[Controller] = []

    def factory(policy: RetryPolicy | None = None, presence_timeout: float = 0.2) -> Controller:
        controller = Controller(
            FakeTransport(broker, f"controller-{len(controllers)}"),
            policy=policy or fast_policy,
            presence_timeout=presence_timeout,
        )
        controller.start()
        controllers.append(controller)
        return controller

    yield factory
    for controller in reversed(controllers):
        controller.stop()


@pytest.fixture(scope="session")
def mqtt_broker() -> tuple[str, int]:
    """The real broker's address, skipping the test when nothing is listening."""
    try:
        with socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=2):
            pass
    except OSError as exc:
        message = f"no MQTT broker at {MQTT_HOST}:{MQTT_PORT} ({exc})"
        if REQUIRE_MQTT:
            pytest.fail(f"REQUIRE_MQTT is set but {message}")
        pytest.skip(message)
    return MQTT_HOST, MQTT_PORT


@pytest.fixture
def real_devices(mqtt_broker):
    """Start simulated devices on the real broker under one-off device ids."""
    host, port = mqtt_broker
    runners: list[DeviceRunner] = []

    def factory(**faults) -> DeviceRunner:
        device_id = f"it-{uuid.uuid4().hex[:10]}"
        device = SimulatedDevice(device_id, faults=FaultConfig(**faults))
        transport = PahoTransport(f"device-{device_id}", host=host, port=port)
        runner = DeviceRunner(device, transport)
        runner.start()
        runners.append(runner)
        return runner

    yield factory

    for runner in reversed(runners):
        runner.stop()
    _clear_retained_status(host, port, [r.device.device_id for r in runners])


@pytest.fixture
def real_controller(mqtt_broker, fast_policy):
    """A controller on the real broker, with timeouts loose enough for real network hops."""
    host, port = mqtt_broker
    policy = RetryPolicy(ack_timeout=1.0, max_attempts=3, backoff_base=0.05, backoff_factor=2.0)
    transport = PahoTransport(f"controller-{uuid.uuid4().hex[:8]}", host=host, port=port)
    controller = Controller(transport, policy=policy, presence_timeout=2.0)
    controller.start()
    yield controller
    controller.stop()


def _clear_retained_status(host: str, port: int, device_ids: list[str]) -> None:
    """Drop the retained status messages this test left on the broker."""
    if not device_ids:
        return
    transport = PahoTransport(f"cleanup-{uuid.uuid4().hex[:8]}", host=host, port=port)
    transport.connect()
    try:
        for device_id in device_ids:
            transport.publish(STATUS_TOPIC.format(device_id=device_id), b"", retain=True)
    finally:
        transport.disconnect()
