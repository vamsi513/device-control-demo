"""Measure command round trip latency with many simulated devices on one broker.

Starts ``--devices`` simulated devices, each with its own MQTT connection, then
sends commands one at a time and records how long each one took from publish to
ack. One command is in flight at a time, so the numbers are round trip latency
under a connected fleet, not peak throughput.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time

from device_control.controller import ACKNOWLEDGED, Controller, RetryPolicy
from device_control.device import DeviceRunner, SimulatedDevice
from device_control.protocol import ACTION_ARM, ACTION_DISARM, STATUS_TOPIC
from device_control.transport import DEFAULT_HOST, DEFAULT_PORT, ConnectError, PahoTransport


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile: the smallest sample at or above ``pct`` of the data."""
    if not values:
        raise ValueError("no samples")
    ordered = sorted(values)
    rank = math.ceil(pct / 100 * len(ordered)) - 1
    return ordered[min(max(rank, 0), len(ordered) - 1)]


def start_devices(count: int, host: str, port: int) -> list[DeviceRunner]:
    runners: list[DeviceRunner] = []
    for index in range(count):
        device_id = f"load-{index:04d}"
        transport = PahoTransport(f"device-{device_id}", host=host, port=port)
        runner = DeviceRunner(SimulatedDevice(device_id), transport)
        runner.start()
        runners.append(runner)
    return runners


def stop_devices(runners: list[DeviceRunner], host: str, port: int) -> None:
    """Stop every device, then clear the retained status it left on the broker."""
    for runner in reversed(runners):
        runner.stop()
    if not runners:
        return
    cleanup = PahoTransport("load-cleanup", host=host, port=port)
    try:
        cleanup.connect()
    except ConnectError:
        return
    try:
        for runner in runners:
            topic = STATUS_TOPIC.format(device_id=runner.device.device_id)
            cleanup.publish(topic, b"", retain=True)
    finally:
        cleanup.disconnect()


def await_fleet(controller: Controller, runners: list[DeviceRunner], timeout: float) -> int:
    deadline = time.monotonic() + timeout
    device_ids = [r.device.device_id for r in runners]
    while time.monotonic() < deadline:
        online = sum(1 for d in device_ids if controller.presence(d) is True)
        if online == len(device_ids):
            return online
        time.sleep(0.05)
    return sum(1 for d in device_ids if controller.presence(d) is True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="load_check", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--devices", type=int, default=100)
    parser.add_argument("--commands-per-device", type=int, default=10)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--ack-timeout", type=float, default=5.0)
    parser.add_argument("--json", action="store_true", help="print the summary as JSON")
    args = parser.parse_args(argv)

    policy = RetryPolicy(ack_timeout=args.ack_timeout, max_attempts=3, backoff_base=0.1)
    controller = Controller(
        PahoTransport("load-controller", host=args.host, port=args.port),
        policy=policy,
        presence_timeout=10.0,
    )
    try:
        controller.start()
    except ConnectError as exc:
        print(f"load_check: {exc}", file=sys.stderr)
        return 1

    runners: list[DeviceRunner] = []
    try:
        connect_started = time.monotonic()
        runners = start_devices(args.devices, args.host, args.port)
        connect_seconds = time.monotonic() - connect_started

        online = await_fleet(controller, runners, timeout=30.0)
        if online != args.devices:
            print(
                f"load_check: only {online}/{args.devices} devices reported online",
                file=sys.stderr,
            )
            return 1

        latencies: list[float] = []
        statuses: dict[str, int] = {}
        retried = 0
        run_started = time.monotonic()
        for round_index in range(args.commands_per_device):
            action = ACTION_ARM if round_index % 2 == 0 else ACTION_DISARM
            for runner in runners:
                result = controller.send(runner.device.device_id, action)
                statuses[result.status] = statuses.get(result.status, 0) + 1
                if result.attempts > 1:
                    retried += 1
                if result.status == ACKNOWLEDGED:
                    latencies.append(result.elapsed * 1000)
        wall_seconds = time.monotonic() - run_started
    finally:
        stop_devices(runners, args.host, args.port)
        controller.stop()

    if not latencies:
        print("load_check: no command was acknowledged", file=sys.stderr)
        return 1

    summary = {
        "devices": args.devices,
        "commands_sent": sum(statuses.values()),
        "commands_acknowledged": len(latencies),
        "statuses": statuses,
        "commands_needing_a_retry": retried,
        "device_connect_seconds": round(connect_seconds, 2),
        "wall_seconds": round(wall_seconds, 2),
        "commands_per_second": round(sum(statuses.values()) / wall_seconds, 1),
        "latency_ms": {
            "min": round(min(latencies), 2),
            "p50": round(percentile(latencies, 50), 2),
            "p95": round(percentile(latencies, 95), 2),
            "p99": round(percentile(latencies, 99), 2),
            "max": round(max(latencies), 2),
        },
    }

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0

    latency = summary["latency_ms"]
    print(f"devices connected             {summary['devices']}")
    print(f"device connect time           {summary['device_connect_seconds']} s")
    print(f"commands sent                 {summary['commands_sent']} (one in flight at a time)")
    print(f"commands acknowledged         {summary['commands_acknowledged']}")
    print(f"commands needing a retry      {summary['commands_needing_a_retry']}")
    print(f"wall time                     {summary['wall_seconds']} s")
    print(f"commands per second           {summary['commands_per_second']}")
    print("round trip latency, publish to ack")
    for key in ("min", "p50", "p95", "p99", "max"):
        print(f"  {key:<4}                        {latency[key]} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
