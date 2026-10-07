"""Wire format round trips and the errors a malformed payload must raise."""

from __future__ import annotations

import pytest

from device_control.protocol import (
    ACK_FILTER,
    STATUS_FILTER,
    Ack,
    Command,
    ProtocolError,
    Status,
    device_id_from_topic,
    new_command_id,
)
from fake_broker import topic_matches


def test_command_round_trip():
    command = Command("abc123", "set_mode", {"mode": "away"})
    assert Command.decode(command.encode()) == command


def test_ack_round_trip():
    ack = Ack("abc123", "dev-1", "applied", {"armed": True, "mode": "away"}, detail="")
    assert Ack.decode(ack.encode()) == ack


def test_ack_duplicate_flag_is_the_only_difference():
    ack = Ack("abc123", "dev-1", "applied", {"armed": True})
    duplicate = ack.as_duplicate()
    assert duplicate.duplicate is True
    assert duplicate.command_id == ack.command_id
    assert duplicate.state == ack.state


def test_status_round_trip():
    status = Status("dev-1", online=True)
    assert Status.decode(status.encode()) == status


def test_command_ids_are_unique():
    assert len({new_command_id() for _ in range(1000)}) == 1000


@pytest.mark.parametrize(
    "raw",
    [b"", b"not json", b"[]", b'"a string"', b"{}", b'{"action": "arm"}', b'{"command_id": ""}'],
)
def test_command_rejects_bad_payloads(raw):
    with pytest.raises(ProtocolError):
        Command.decode(raw)


def test_command_rejects_non_object_params():
    with pytest.raises(ProtocolError):
        Command.decode(b'{"command_id": "a", "action": "arm", "params": 7}')


def test_status_rejects_non_boolean_online():
    with pytest.raises(ProtocolError):
        Status.decode(b'{"device_id": "dev-1", "online": "yes"}')


def test_device_id_from_topic():
    assert device_id_from_topic("devices/dev-1/ack") == "dev-1"


@pytest.mark.parametrize("topic", ["dev-1/ack", "devices/dev-1", "devices/dev-1/ack/extra"])
def test_device_id_from_topic_rejects_other_shapes(topic):
    with pytest.raises(ProtocolError):
        device_id_from_topic(topic)


def test_filters_match_their_own_topics():
    assert topic_matches(ACK_FILTER, "devices/dev-1/ack")
    assert topic_matches(STATUS_FILTER, "devices/dev-1/status")
    assert not topic_matches(ACK_FILTER, "devices/dev-1/cmd")
    assert not topic_matches(ACK_FILTER, "devices/dev-1/sub/ack")
