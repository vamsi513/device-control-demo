"""The same paths over a real MQTT broker, including the retained status and last will.

These need a broker on ``MQTT_HOST``/``MQTT_PORT`` (default ``localhost:1883``):
``docker compose up -d`` locally, a service container in CI. They skip when none
is reachable, unless ``REQUIRE_MQTT`` is set, which CI does.
"""

from __future__ import annotations

import time

import pytest

from device_control.controller import ACKNOWLEDGED, DEVICE_OFFLINE, TIMED_OUT

pytestmark = pytest.mark.integration


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_ack_received_over_a_real_broker(real_devices, real_controller):
    runner = real_devices()

    result = real_controller.send(runner.device.device_id, "arm")

    assert result.status == ACKNOWLEDGED
    assert result.ok is True
    assert result.attempts == 1
    assert result.ack.state == {"armed": True, "mode": "home"}


def test_retained_status_tells_the_controller_a_device_is_online(real_devices, real_controller):
    runner = real_devices()
    device_id = runner.device.device_id

    assert wait_until(lambda: real_controller.presence(device_id) is True)


def test_last_will_marks_a_dropped_device_offline(real_devices, real_controller):
    runner = real_devices()
    device_id = runner.device.device_id
    assert wait_until(lambda: real_controller.presence(device_id) is True)

    runner.go_offline()

    assert wait_until(lambda: real_controller.presence(device_id) is False), (
        "the broker did not publish the last will after the connection dropped"
    )
    result = real_controller.send(device_id, "arm")
    assert result.status == DEVICE_OFFLINE
    assert result.attempts == 0


def test_dropped_ack_then_retry_succeeds_over_a_real_broker(real_devices, real_controller):
    runner = real_devices(drop_acks=1)

    result = real_controller.send(runner.device.device_id, "set_mode", {"mode": "away"})

    assert result.status == ACKNOWLEDGED
    assert result.attempts == 2
    assert result.ack.duplicate is True
    assert runner.device.applied_count == 1
    assert runner.device.state.mode == "away"


def test_retries_exhausted_over_a_real_broker(real_devices, real_controller):
    runner = real_devices(drop_acks=99)

    result = real_controller.send(runner.device.device_id, "arm")

    assert result.status == TIMED_OUT
    assert result.attempts == real_controller.policy.max_attempts
    assert wait_until(lambda: runner.device.received_count == 3)
    assert runner.device.applied_count == 1


def test_a_second_controller_sending_the_same_command_id_applies_it_once(
    real_devices, real_controller
):
    runner = real_devices()
    device_id = runner.device.device_id

    first = real_controller.send(device_id, "arm", command_id="shared-command-id")
    second = real_controller.send(device_id, "arm", command_id="shared-command-id")

    assert first.status == ACKNOWLEDGED
    assert second.status == ACKNOWLEDGED
    assert first.ack.duplicate is False
    assert second.ack.duplicate is True
    assert runner.device.applied_count == 1
