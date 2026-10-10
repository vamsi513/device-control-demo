"""Broker authentication and TLS configuration.

Both are off by default. These assert the switch works in both directions: that
nothing is configured unless asked for, and that what is asked for reaches paho.

No broker is involved. The point is that the configuration is honoured, which is
checkable without a connection, and a TLS handshake against a real broker would be
testing OpenSSL rather than this code.
"""

from __future__ import annotations

import ssl
from typing import Any

import pytest

from device_control.transport import PahoTransport, TlsConfig, credentials_from_env


class _RecordingClient:
    """Captures what the transport asks paho to do."""

    def __init__(self) -> None:
        self.username: str | None = None
        self.password: str | None = None
        self.tls_kwargs: dict[str, Any] | None = None
        self.insecure: bool | None = None

    def username_pw_set(self, username: str, password: str | None = None) -> None:
        self.username, self.password = username, password

    def tls_set(self, **kwargs: Any) -> None:
        self.tls_kwargs = kwargs

    def tls_insecure_set(self, value: bool) -> None:
        self.insecure = value


@pytest.fixture
def recording(monkeypatch) -> _RecordingClient:
    client = _RecordingClient()
    monkeypatch.setattr(
        "device_control.transport.mqtt.Client",
        lambda *a, **k: client,  # noqa: ARG005
    )
    return client


class TestDefaults:
    def test_no_credentials_are_sent_unless_asked_for(self, recording):
        PahoTransport("c1")
        assert recording.username is None

    def test_tls_is_not_configured_unless_asked_for(self, recording):
        PahoTransport("c1")
        assert recording.tls_kwargs is None
        assert recording.insecure is None


class TestAuthentication:
    def test_a_username_and_password_reach_the_client(self, recording):
        PahoTransport("c1", username="device", password="s3cret")
        assert recording.username == "device"
        assert recording.password == "s3cret"

    def test_a_username_with_no_password_is_allowed(self, recording):
        PahoTransport("c1", username="device")
        assert recording.username == "device"
        assert recording.password is None

    def test_credentials_come_from_the_environment_not_the_command_line(self):
        """Reading them from env keeps them out of shell history and `ps` output."""
        assert credentials_from_env({}) == (None, None)
        assert credentials_from_env({"MQTT_USERNAME": "device"}) == ("device", None)
        assert credentials_from_env({"MQTT_USERNAME": "device", "MQTT_PASSWORD": "pw"}) == (
            "device",
            "pw",
        )

    def test_an_empty_username_is_treated_as_unset(self):
        """An exported-but-blank variable must not switch auth on with a blank user."""
        assert credentials_from_env({"MQTT_USERNAME": "", "MQTT_PASSWORD": "pw"}) == (None, "pw")


class TestTls:
    def test_verification_is_required_by_default(self, recording):
        PahoTransport("c1", tls=TlsConfig())
        assert recording.tls_kwargs is not None
        assert recording.tls_kwargs["cert_reqs"] == ssl.CERT_REQUIRED
        assert recording.insecure is None, "verification must not be disabled implicitly"

    def test_paths_are_passed_through(self, recording):
        PahoTransport(
            "c1",
            tls=TlsConfig(ca_certs="/ca.pem", certfile="/client.pem", keyfile="/client.key"),
        )
        assert recording.tls_kwargs["ca_certs"] == "/ca.pem"
        assert recording.tls_kwargs["certfile"] == "/client.pem"
        assert recording.tls_kwargs["keyfile"] == "/client.key"

    def test_insecure_disables_verification_only_when_asked(self, recording):
        PahoTransport("c1", tls=TlsConfig(insecure=True))
        assert recording.tls_kwargs["cert_reqs"] == ssl.CERT_NONE
        assert recording.insecure is True

    def test_from_env_returns_none_when_tls_is_off(self):
        assert TlsConfig.from_env({}) is None
        assert TlsConfig.from_env({"MQTT_TLS": "0"}) is None
        assert TlsConfig.from_env({"MQTT_TLS": "false"}) is None

    @pytest.mark.parametrize("flag", ["1", "true", "TRUE", "yes", "on"])
    def test_from_env_accepts_the_usual_truthy_spellings(self, flag):
        assert TlsConfig.from_env({"MQTT_TLS": flag}) is not None

    def test_from_env_reads_the_paths_and_the_insecure_flag(self):
        config = TlsConfig.from_env(
            {
                "MQTT_TLS": "1",
                "MQTT_TLS_CA": "/ca.pem",
                "MQTT_TLS_CERT": "/c.pem",
                "MQTT_TLS_KEY": "/c.key",
                "MQTT_TLS_INSECURE": "yes",
            }
        )
        assert config == TlsConfig(
            ca_certs="/ca.pem", certfile="/c.pem", keyfile="/c.key", insecure=True
        )

    def test_from_env_does_not_turn_insecure_on_by_itself(self):
        config = TlsConfig.from_env({"MQTT_TLS": "1"})
        assert config is not None and config.insecure is False
