"""Command line entry points: run a simulated device, or send it a command."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading

from .controller import Controller, RetryPolicy
from .device import DeviceRunner, FaultConfig, SimulatedDevice
from .protocol import ACTIONS, MODES, new_command_id
from .transport import DEFAULT_HOST, DEFAULT_PORT, ConnectError, PahoTransport


def _broker_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", default=DEFAULT_HOST, help="broker host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="broker port")


def device_main(argv: list[str] | None = None) -> int:
    """Run one simulated device until interrupted."""
    parser = argparse.ArgumentParser(
        prog="device-sim", description="Run a simulated device against an MQTT broker."
    )
    parser.add_argument("device_id")
    _broker_args(parser)
    parser.add_argument(
        "--drop-acks",
        type=int,
        default=0,
        metavar="N",
        help="suppress the first N acks; the commands are still applied",
    )
    parser.add_argument(
        "--ack-delay",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="hold every ack back this long",
    )
    parser.add_argument(
        "--offline-after",
        type=float,
        default=None,
        metavar="SECONDS",
        help="drop the connection after this long, so the broker fires the last will",
    )
    args = parser.parse_args(argv)

    device = SimulatedDevice(
        args.device_id,
        faults=FaultConfig(
            drop_acks=args.drop_acks,
            ack_delay=args.ack_delay,
            offline_after=args.offline_after,
        ),
    )
    transport = PahoTransport(f"device-{args.device_id}", host=args.host, port=args.port)
    runner = DeviceRunner(device, transport)
    try:
        runner.start()
    except ConnectError as exc:
        print(f"device {args.device_id}: {exc}", file=sys.stderr)
        return 1

    print(f"device {args.device_id} online, listening on {runner.command_topic}")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    stop.wait()
    runner.stop()
    print(f"device {args.device_id} offline; applied {device.applied_count} command(s)")
    return 0


def send_main(argv: list[str] | None = None) -> int:
    """Send one command and print the final status."""
    parser = argparse.ArgumentParser(
        prog="device-send", description="Send one command to a device and wait for its ack."
    )
    parser.add_argument("device_id")
    parser.add_argument("action", choices=ACTIONS)
    parser.add_argument("--mode", choices=MODES, help="target mode for set_mode")
    _broker_args(parser)
    parser.add_argument("--ack-timeout", type=float, default=2.0, metavar="SECONDS")
    parser.add_argument("--max-attempts", type=int, default=3, metavar="N")
    parser.add_argument("--backoff-base", type=float, default=0.5, metavar="SECONDS")
    args = parser.parse_args(argv)

    params = {"mode": args.mode} if args.mode else {}
    policy = RetryPolicy(
        ack_timeout=args.ack_timeout,
        max_attempts=args.max_attempts,
        backoff_base=args.backoff_base,
    )
    transport = PahoTransport(f"controller-{new_command_id()[:8]}", host=args.host, port=args.port)
    controller = Controller(transport, policy=policy)
    try:
        controller.start()
    except ConnectError as exc:
        print(f"controller: {exc}", file=sys.stderr)
        return 1

    try:
        result = controller.send(args.device_id, args.action, params)
    finally:
        controller.stop()

    report = {
        "status": result.status,
        "device_id": result.device_id,
        "command_id": result.command_id,
        "attempts": result.attempts,
        "elapsed_ms": round(result.elapsed * 1000, 1),
        "detail": result.detail,
    }
    if result.ack is not None:
        report["result"] = result.ack.result
        report["state"] = result.ack.state
        report["duplicate_ack"] = result.ack.duplicate
        report["ack_detail"] = result.ack.detail
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if result.ok else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(send_main())
