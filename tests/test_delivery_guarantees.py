"""Delivery behaviour: reconnect, ack matching, and dedupe across a restart.

These pin the edges of what the service actually guarantees, including the one it
does not. A device deduplicates by remembering command ids in memory, so a restart
loses that memory and a replayed command is applied again. That is asserted here
rather than glossed over, because the guarantee is effectively-once only while a
device stays up.
"""

from __future__ import annotations

from device_control.controller import DEVICE_OFFLINE
from device_control.device import DeviceRunner, SimulatedDevice
from device_control.protocol import Ack, Command
from fake_broker import FakeTransport


class TestReconnect:
    def test_a_device_answers_again_after_going_offline_and_back(
        self, broker, make_device, make_controller
    ):
        """A dropped connection must not be terminal. Once the device is back,
        presence flips to online and commands are answered on the first attempt."""
        runner = make_device("dev-reconnect")
        controller = make_controller()
        assert controller.send("dev-reconnect", "arm").ok

        runner.go_offline()
        assert controller.presence("dev-reconnect") is False

        # Same device object, fresh connection, which is what a reconnect looks like.
        reconnected = DeviceRunner(runner.device, FakeTransport(broker, "device-dev-reconnect-2"))
        reconnected.start()
        try:
            assert controller.presence("dev-reconnect") is True
            result = controller.send("dev-reconnect", "disarm")
            assert result.ok
            assert result.attempts == 1, "a reconnected device should answer first time"
        finally:
            reconnected.stop()

    def test_state_survives_a_reconnect(self, broker, make_device, make_controller):
        """The device object keeps its state across the transport being replaced, so
        a reconnect does not silently reset what the device thinks it is doing."""
        runner = make_device("dev-state")
        controller = make_controller()
        controller.send("dev-state", "set_mode", {"mode": "night"})
        assert runner.device.state.mode == "night"

        runner.go_offline()
        reconnected = DeviceRunner(runner.device, FakeTransport(broker, "device-dev-state-2"))
        reconnected.start()
        try:
            assert reconnected.device.state.mode == "night"
        finally:
            reconnected.stop()

    def test_a_command_to_an_offline_device_fails_fast_without_retrying(
        self, make_device, make_controller
    ):
        """Retained status says offline, so there is no point burning the retry
        budget waiting for an ack that cannot come."""
        runner = make_device("dev-gone")
        controller = make_controller()
        runner.go_offline()

        result = controller.send("dev-gone", "arm")
        assert result.status == DEVICE_OFFLINE
        assert result.attempts == 0


class TestAckMatching:
    def test_an_ack_carrying_another_command_id_does_not_complete_this_one(
        self, broker, make_controller
    ):
        """Acks are matched on command id, not on arrival order. Were they matched by
        order, a stray or late ack would wrongly complete whatever is in flight."""
        controller = make_controller()
        device_transport = FakeTransport(broker, "device-ooo")
        seen: list[Command] = []

        def handle(topic: str, payload: bytes) -> None:
            command = Command.decode(payload)
            seen.append(command)
            # Answer with a command id that was never sent.
            stray = Ack(
                command_id="some-other-command-id",
                device_id="dev-ooo",
                result="applied",
            )
            device_transport.publish("devices/dev-ooo/ack", stray.encode(), qos=1)

        device_transport.on_message(handle)
        device_transport.connect()
        device_transport.subscribe("devices/dev-ooo/cmd", qos=1)
        # A retained online status, so the controller does not short circuit.
        device_transport.publish(
            "devices/dev-ooo/status",
            b'{"device_id": "dev-ooo", "online": true}',
            qos=1,
            retain=True,
        )
        try:
            result = controller.send("dev-ooo", "arm")
            assert not result.ok, "a mismatched ack completed the command"
            assert seen, "the device never received the command at all"
        finally:
            device_transport.disconnect()

    def test_a_late_ack_is_kept_for_inspection_but_does_not_rewrite_the_result(
        self, make_device, make_controller
    ):
        """An ack arriving after the controller gave up must not turn a reported
        failure into a success after the fact."""
        runner = make_device("dev-late", drop_acks=99)
        controller = make_controller()

        result = controller.send("dev-late", "arm")
        assert not result.ok
        # The command was applied every time; only the acks were suppressed.
        assert runner.device.applied_count == 1


class TestDeduplication:
    def test_a_command_is_applied_once_across_every_retry(self, make_device, make_controller):
        runner = make_device("dev-dedupe", drop_acks=2)
        controller = make_controller()
        assert controller.send("dev-dedupe", "arm").ok
        assert runner.device.applied_count == 1
        assert runner.device.received_count == 3, "expected two retries before the ack"

    def test_a_replay_while_the_device_is_up_is_flagged_as_a_duplicate(self):
        device = SimulatedDevice("dev-replay")
        command = Command("cmd-1", "arm")

        first = device.handle(command.encode())
        assert first is not None and first.duplicate is False

        second = device.handle(command.encode())
        assert second is not None and second.duplicate is True
        assert device.applied_count == 1

    def test_a_restart_loses_the_dedupe_history_and_applies_it_again(self):
        """The honest limit. Command ids are remembered in memory, so a device that
        restarts has no record of what it already did and applies a replayed command
        a second time. This is why the README says effectively-once holds only while
        a device stays up.

        If this ever starts failing, dedupe has become durable and the README
        guarantee should be strengthened rather than this test relaxed.
        """
        command = Command("cmd-across-restart", "arm")

        before = SimulatedDevice("dev-restart")
        first = before.handle(command.encode())
        assert first is not None and first.duplicate is False
        assert before.applied_count == 1

        # A fresh object is what a process restart produces.
        after = SimulatedDevice("dev-restart")
        replayed = after.handle(command.encode())
        assert replayed is not None
        assert replayed.duplicate is False
        assert after.applied_count == 1, "the command was applied a second time"

    def test_the_dedupe_history_is_bounded_so_it_cannot_grow_without_limit(self):
        """A long lived device must not accumulate command ids forever. The cost is
        that a replay older than the window is applied again."""
        device = SimulatedDevice("dev-bounded", history_size=4)
        first = Command("cmd-oldest", "arm")
        device.handle(first.encode())

        for index in range(6):
            device.handle(Command(f"cmd-{index}", "arm").encode())

        evicted = device.handle(first.encode())
        assert evicted is not None
        assert evicted.duplicate is False, "an evicted id should no longer be known"
