"""The simulated device's command handling, with no transport involved."""

from __future__ import annotations

from device_control.device import DeviceState, FaultConfig, SimulatedDevice
from device_control.protocol import Ack, Command


def ack_for(device: SimulatedDevice, command: Command) -> Ack | None:
    return device.handle(command.encode())


def test_arm_sets_state_and_acknowledges():
    device = SimulatedDevice("dev-1")
    ack = ack_for(device, Command("c1", "arm"))
    assert ack is not None
    assert ack.result == "applied"
    assert ack.state == {"armed": True, "mode": "home"}
    assert ack.duplicate is False
    assert device.applied_count == 1


def test_disarm_and_set_mode():
    device = SimulatedDevice("dev-1", state=DeviceState(armed=True))
    assert ack_for(device, Command("c1", "disarm")).state["armed"] is False
    assert ack_for(device, Command("c2", "set_mode", {"mode": "night"})).state["mode"] == "night"
    assert device.applied_count == 2


def test_unknown_action_is_rejected_not_applied():
    device = SimulatedDevice("dev-1")
    ack = ack_for(device, Command("c1", "self_destruct"))
    assert ack.result == "rejected"
    assert "self_destruct" in ack.detail
    assert device.applied_count == 0


def test_unknown_mode_is_rejected_and_leaves_mode_alone():
    device = SimulatedDevice("dev-1")
    ack = ack_for(device, Command("c1", "set_mode", {"mode": "vacation"}))
    assert ack.result == "rejected"
    assert ack.state["mode"] == "home"
    assert device.applied_count == 0


def test_duplicate_command_id_is_applied_once_and_replays_the_ack():
    device = SimulatedDevice("dev-1")
    command = Command("c1", "arm")
    first = ack_for(device, command)
    second = ack_for(device, command)

    assert device.applied_count == 1
    assert device.received_count == 2
    assert second.duplicate is True
    assert second.command_id == first.command_id
    assert second.state == first.state


def test_duplicate_of_a_rejection_replays_the_rejection():
    device = SimulatedDevice("dev-1")
    command = Command("c1", "set_mode", {"mode": "vacation"})
    ack_for(device, command)
    second = ack_for(device, command)
    assert second.result == "rejected"
    assert second.duplicate is True
    assert device.applied_count == 0


def test_dropped_ack_still_applies_the_command():
    device = SimulatedDevice("dev-1", faults=FaultConfig(drop_acks=1))
    assert ack_for(device, Command("c1", "arm")) is None
    assert device.applied_count == 1
    assert device.state.armed is True
    assert device.dropped_ack_count == 1


def test_only_the_configured_number_of_acks_is_dropped():
    device = SimulatedDevice("dev-1", faults=FaultConfig(drop_acks=2))
    assert ack_for(device, Command("c1", "arm")) is None
    assert ack_for(device, Command("c2", "disarm")) is None
    assert ack_for(device, Command("c3", "arm")) is not None


def test_malformed_payload_is_ignored_without_an_ack():
    device = SimulatedDevice("dev-1")
    assert device.handle(b"{nope") is None
    assert device.received_count == 1
    assert device.applied_count == 0


def test_history_is_bounded_so_a_long_lived_device_does_not_grow_forever():
    device = SimulatedDevice("dev-1", history_size=4)
    for index in range(10):
        ack_for(device, Command(f"c{index}", "arm"))
    # The oldest id has been forgotten, so it is treated as new and applied again.
    replay = ack_for(device, Command("c0", "arm"))
    assert replay.duplicate is False
    # The newest ids are still remembered.
    assert ack_for(device, Command("c9", "arm")).duplicate is True
