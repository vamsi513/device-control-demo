"""Controller behaviour against the fake broker: acks, retries, presence, late acks."""

from __future__ import annotations

import threading
import time

import pytest

from device_control.controller import (
    ACKNOWLEDGED,
    DEVICE_OFFLINE,
    TIMED_OUT,
    RetryPolicy,
)


def test_backoff_grows_and_is_capped():
    policy = RetryPolicy(backoff_base=0.5, backoff_factor=2.0, backoff_max=2.0)
    assert [policy.backoff(n) for n in range(1, 6)] == [0.5, 1.0, 2.0, 2.0, 2.0]


def test_backoff_rejects_a_zeroth_attempt():
    with pytest.raises(ValueError, match="1-based"):
        RetryPolicy().backoff(0)


def test_ack_received_on_the_first_attempt(make_device, make_controller):
    runner = make_device("dev-1")
    controller = make_controller()

    result = controller.send("dev-1", "arm")

    assert result.status == ACKNOWLEDGED
    assert result.ok is True
    assert result.attempts == 1
    assert result.ack.duplicate is False
    assert result.ack.state == {"armed": True, "mode": "home"}
    assert runner.device.applied_count == 1


def test_a_rejected_command_is_still_an_acknowledgement(make_device, make_controller):
    make_device("dev-1")
    controller = make_controller()

    result = controller.send("dev-1", "set_mode", {"mode": "vacation"})

    assert result.status == ACKNOWLEDGED
    assert result.ok is False
    assert result.ack.result == "rejected"


def test_ack_dropped_then_the_retry_succeeds(make_device, make_controller):
    runner = make_device("dev-1", drop_acks=1)
    controller = make_controller()

    result = controller.send("dev-1", "arm")

    assert result.status == ACKNOWLEDGED
    assert result.attempts == 2
    # The retry reused the command id, so the device replayed its stored ack.
    assert result.ack.duplicate is True
    assert runner.device.received_count == 2
    assert runner.device.applied_count == 1


def test_retries_exhausted_reports_timed_out(make_device, make_controller):
    runner = make_device("dev-1", drop_acks=99)
    controller = make_controller()

    result = controller.send("dev-1", "arm")

    assert result.status == TIMED_OUT
    assert result.attempts == 3
    assert "3 attempts" in result.detail
    assert result.ack is None
    assert runner.device.received_count == 3
    # Every attempt reached the device, and the command was still applied once.
    assert runner.device.applied_count == 1


def test_duplicate_command_is_applied_once_across_every_retry(make_device, make_controller):
    runner = make_device("dev-1", drop_acks=2)
    controller = make_controller()

    result = controller.send("dev-1", "set_mode", {"mode": "away"})

    assert result.status == ACKNOWLEDGED
    assert result.attempts == 3
    assert runner.device.received_count == 3
    assert runner.device.applied_count == 1
    assert runner.device.state.mode == "away"


def test_device_that_has_never_connected_is_reported_offline(make_controller):
    controller = make_controller()

    result = controller.send("ghost", "arm")

    assert result.status == DEVICE_OFFLINE
    assert result.attempts == 0
    assert result.detail == "no status seen"


def test_device_offline_after_it_drops_the_connection(make_device, make_controller):
    runner = make_device("dev-1")
    controller = make_controller()
    assert controller.presence("dev-1") is True

    runner.go_offline()

    assert controller.presence("dev-1") is False
    result = controller.send("dev-1", "arm")
    assert result.status == DEVICE_OFFLINE
    assert result.attempts == 0
    assert result.detail == "retained status says offline"
    assert runner.device.received_count == 0


def test_device_offline_after_a_clean_stop(make_device, make_controller):
    runner = make_device("dev-1")
    controller = make_controller()

    runner.stop()

    result = controller.send("dev-1", "arm")
    assert result.status == DEVICE_OFFLINE


def test_device_dropping_off_mid_command_stops_the_retries(make_device, make_controller):
    policy = RetryPolicy(ack_timeout=0.08, max_attempts=6, backoff_base=0.1, backoff_factor=1.0)
    runner = make_device("dev-1", drop_acks=99)
    controller = make_controller(policy=policy)

    outcome: list = []
    sender = threading.Thread(target=lambda: outcome.append(controller.send("dev-1", "arm")))
    sender.start()
    time.sleep(0.15)
    runner.go_offline()
    sender.join(timeout=5)

    assert not sender.is_alive()
    result = outcome[0]
    assert result.status == DEVICE_OFFLINE
    assert result.detail == "dropped off mid-command"
    assert result.attempts < policy.max_attempts


def test_late_ack_after_the_timeout_is_recorded_but_does_not_change_the_result(
    make_device, make_controller
):
    policy = RetryPolicy(ack_timeout=0.05, max_attempts=2, backoff_base=0.01)
    runner = make_device("dev-1", ack_delay=0.4)
    controller = make_controller(policy=policy)

    result = controller.send("dev-1", "arm")

    assert result.status == TIMED_OUT
    assert controller.late_acks == []

    deadline = time.monotonic() + 3.0
    while not controller.late_acks and time.monotonic() < deadline:
        time.sleep(0.01)

    late = controller.late_acks
    assert late, "the delayed ack never arrived"
    assert all(ack.command_id == result.command_id for ack in late)
    assert late[0].result == "applied"
    # The late ack did not retroactively turn the timeout into a success.
    assert result.status == TIMED_OUT
    assert runner.device.applied_count == 1


def test_presence_is_unknown_before_any_status_is_seen(make_controller):
    controller = make_controller()
    assert controller.presence("dev-1") is None
    assert controller.wait_for_presence("dev-1", timeout=0.05) is False


def test_two_devices_do_not_cross_wires(make_device, make_controller):
    first = make_device("dev-1")
    second = make_device("dev-2")
    controller = make_controller()

    assert controller.send("dev-1", "arm").ok
    assert controller.send("dev-2", "set_mode", {"mode": "night"}).ok

    assert first.device.state.armed is True
    assert first.device.state.mode == "home"
    assert second.device.state.armed is False
    assert second.device.state.mode == "night"
