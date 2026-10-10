"""The whole story in one run: command, ack, retry, offline presence, recovery.

    docker compose up -d
    python scripts/demo.py

Each step prints what happened and why, so the output is the explanation. Runs
against a real broker; set MQTT_HOST and MQTT_PORT to point somewhere else.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from device_control.controller import Controller, RetryPolicy
from device_control.device import DeviceRunner, FaultConfig, SimulatedDevice
from device_control.transport import PahoTransport, TlsConfig, credentials_from_env


def banner(step: int, title: str) -> None:
    print(f"\n{step}. {title}\n{'-' * (len(title) + 3)}")


def make_transport(client_id: str, host: str, port: int) -> PahoTransport:
    username, password = credentials_from_env()
    return PahoTransport(
        client_id,
        host=host,
        port=port,
        username=username,
        password=password,
        tls=TlsConfig.from_env(),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("MQTT_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    args = parser.parse_args()

    device_id = f"demo-{int(time.time()) % 100000}"
    policy = RetryPolicy(ack_timeout=1.0, max_attempts=3, backoff_base=0.3, backoff_factor=2.0)

    try:
        controller = Controller(
            make_transport("demo-controller", args.host, args.port), policy=policy
        )
        controller.start()
    except Exception as exc:
        print(f"cannot reach the broker at {args.host}:{args.port}: {exc}")
        print("start one with:  docker compose up -d")
        return 1

    # Suppress the first two acks, so the retry path runs for real rather than
    # being described.
    device = SimulatedDevice(device_id, faults=FaultConfig(drop_acks=2))
    runner = DeviceRunner(device, make_transport(f"demo-device-{device_id}", args.host, args.port))
    runner.start()
    time.sleep(0.3)

    try:
        banner(1, "the device announces itself with a retained status")
        print(f"   device    {device_id}")
        print(f"   presence  {controller.presence(device_id)}  (read from the retained topic)")

        banner(2, "a command, acknowledged after two suppressed acks")
        print("   this device is configured to swallow its first two acks, so the")
        print("   controller has to retry before it hears anything back")
        result = controller.send(device_id, "arm")
        print(f"   status    {result.status}")
        print(f"   attempts  {result.attempts}   (1 would mean no retry was needed)")
        print(f"   applied   {device.applied_count} time(s) on the device")
        print("   the command was applied once even though it arrived three times,")
        print("   because the device remembers command ids it has already seen")

        banner(3, "a second command, answered first time")
        result = controller.send(device_id, "set_mode", {"mode": "night"})
        print(f"   status    {result.status}   attempts {result.attempts}")
        print(f"   state     {device.state.as_dict()}")

        banner(4, "the device drops off, and the broker says so")
        runner.go_offline()
        time.sleep(0.4)
        print(f"   presence  {controller.presence(device_id)}  (the last will fired)")
        result = controller.send(device_id, "disarm")
        print(f"   status    {result.status}")
        print(f"   attempts  {result.attempts}   (0: no point retrying a device known to be down)")

        banner(5, "the device comes back")
        reconnected = DeviceRunner(
            device, make_transport(f"demo-device-{device_id}-2", args.host, args.port)
        )
        reconnected.start()
        time.sleep(0.4)
        try:
            print(f"   presence  {controller.presence(device_id)}")
            result = controller.send(device_id, "disarm")
            print(f"   status    {result.status}   attempts {result.attempts}")
            print(f"   state     {device.state.as_dict()}   (kept across the reconnect)")
        finally:
            reconnected.stop()

        banner(6, "what this does and does not guarantee")
        print("   at least once delivery from the controller: it retries until it")
        print("   hears an ack or runs out of attempts")
        print("   effectively once application on the device: a repeated command id")
        print("   is answered from history instead of being applied again")
        print("   but only while the device stays up. Its history is in memory, so a")
        print("   restart loses it and a replay would be applied a second time")
        return 0
    finally:
        runner.stop()
        controller.stop()


if __name__ == "__main__":
    sys.exit(main())
