#!/usr/bin/env python3
"""Generates a distributed benchmark config for AWS private IPs."""

from __future__ import annotations

import argparse
import datetime
import ipaddress
import pathlib
import re
import sys


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = ROOT / "components/spider-storage-api-bench/config/aws.toml"


def main() -> int:
    args = parse_args()
    server_ip = parse_ip(args.server_private_ip)
    scheduler_ip = parse_ip(args.scheduler_ip)
    submitter_ip = parse_ip(args.submitter_ip)
    worker_ips = read_agent_ips(args.worker_ips)
    if not worker_ips:
        print("worker IP file must contain at least one private IP", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        render_config(args, server_ip, scheduler_ip, submitter_ip, worker_ips),
        encoding="utf-8",
    )
    log(f"config_written path={args.output}")
    log(
        f"config_summary submitter={submitter_ip} workers={len(worker_ips)} "
        f"jobs={args.job_count}"
    )
    return 0


def log(message: str) -> None:
    timestamp = (
        datetime.datetime.now(datetime.timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds")
    )
    print(f"[aws_make_config] {timestamp} {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-private-ip", required=True)
    parser.add_argument(
        "--scheduler-ip",
        required=True,
        help="Private IP for the dedicated scheduler agent.",
    )
    parser.add_argument(
        "--submitter-ip",
        required=True,
        help="Private IP for the dedicated submitter agent.",
    )
    parser.add_argument(
        "--worker-ips",
        type=pathlib.Path,
        required=True,
        help="Text file with one worker-agent private IP per line.",
    )
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--job-count", type=int, default=1280)
    parser.add_argument("--tasks-per-job", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--task-sleep-ms", type=int, default=3)
    parser.add_argument("--submitter-count", type=int, default=8)
    parser.add_argument("--worker-count", type=int, default=16)
    parser.add_argument("--worker-poll-wait-ms", type=int, default=10)
    parser.add_argument("--job-poll-wait-ms", type=int, default=10)
    parser.add_argument("--scheduler-poll-batch", type=int, default=1024)
    parser.add_argument("--scheduler-refill-interval-ms", type=int, default=10)
    parser.add_argument("--scheduler-poll-wait-ms", type=int, default=20)
    parser.add_argument("--scheduler-worker-poll-concurrency", type=int, default=512)
    parser.add_argument("--flat-percent", type=int, default=50)
    parser.add_argument("--agent-timeout-sec", type=int, default=7200)
    parser.add_argument("--poll-interval-ms", type=int, default=1000)
    parser.add_argument("--rest-port", type=int, default=8091)
    parser.add_argument("--grpc-port", type=int, default=50051)
    parser.add_argument("--agent-port", type=int, default=19091)
    parser.add_argument("--database-port", type=int, default=3306)
    parser.add_argument("--database-host", default="127.0.0.1")
    parser.add_argument("--database-name", default="spider-db")
    parser.add_argument("--database-username", default="spider-user")
    parser.add_argument("--database-password", default="spider-password")
    parser.add_argument("--database-max-connections", type=int, default=256)
    parser.add_argument(
        "--database-ssl-mode",
        choices=["disabled", "preferred", "required", "verify_ca", "verify_identity"],
        default="preferred",
    )
    return parser.parse_args()


def read_agent_ips(path: pathlib.Path) -> list[str]:
    ips = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        ips.append(parse_ip(value))
    return sorted(ips, key=lambda value: tuple(int(part) for part in value.split(".")))


def parse_ip(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as error:
        msg = f"invalid IP address `{value}`"
        raise argparse.ArgumentTypeError(msg) from error
    if address.version != 4:
        msg = f"only IPv4 addresses are supported: `{value}`"
        raise argparse.ArgumentTypeError(msg)
    return str(address)


def render_config(
    args: argparse.Namespace,
    server_ip: str,
    scheduler_ip: str,
    submitter_ip: str,
    worker_ips: list[str],
) -> str:
    lines = [
        "[server]",
        f'rest_bind = "0.0.0.0:{args.rest_port}"',
        f'grpc_bind = "0.0.0.0:{args.grpc_port}"',
        f'rest_target = "http://{server_ip}:{args.rest_port}"',
        f'grpc_target = "http://{server_ip}:{args.grpc_port}"',
        "",
        "[database]",
        f'host = "{args.database_host}"',
        f"port = {args.database_port}",
        f'name = "{args.database_name}"',
        f'username = "{args.database_username}"',
        f'password = "{args.database_password}"',
        f"max_connections = {args.database_max_connections}",
        f'ssl_mode = "{args.database_ssl_mode}"',
        "",
        "[benchmark]",
        f"task_count = {args.tasks_per_job}",
        f"job_count = {args.job_count}",
        f"payload_bytes = {args.payload_bytes}",
        f"task_sleep_ms = {args.task_sleep_ms}",
        f"client_count = {args.submitter_count}",
        f"worker_count = {args.worker_count}",
        f"worker_poll_wait_ms = {args.worker_poll_wait_ms}",
        f"job_poll_wait_ms = {args.job_poll_wait_ms}",
        f"scheduler_poll_batch = {args.scheduler_poll_batch}",
        f"scheduler_refill_interval_ms = {args.scheduler_refill_interval_ms}",
        f"scheduler_poll_wait_ms = {args.scheduler_poll_wait_ms}",
        f"scheduler_worker_poll_concurrency = {args.scheduler_worker_poll_concurrency}",
        "warmup_sec = 5",
        "duration_sec = 30",
        f"flat_percent = {args.flat_percent}",
        'output_dir = "data/"',
        "",
        "[distributed]",
        f"agent_timeout_sec = {args.agent_timeout_sec}",
        f"poll_interval_ms = {args.poll_interval_ms}",
        "",
    ]
    lines.extend(
        [
            "[distributed.scheduler]",
            f'id = "{scheduler_id(scheduler_ip)}"',
            f'url = "http://{scheduler_ip}:{args.agent_port}"',
            "",
        ]
    )
    lines.extend(
        [
            "[distributed.submitter]",
            f'id = "{submitter_id(submitter_ip)}"',
            f'url = "http://{submitter_ip}:{args.agent_port}"',
            "",
        ]
    )
    for ip in worker_ips:
        lines.extend(
            [
                "[[distributed.workers]]",
                f'id = "{worker_id(ip)}"',
                f'url = "http://{ip}:{args.agent_port}"',
                "",
            ]
        )
    return "\n".join(lines)


def submitter_id(ip: str) -> str:
    return "submitter-" + re.sub(r"[^0-9A-Za-z]+", "-", ip).strip("-")


def scheduler_id(ip: str) -> str:
    return "scheduler-" + re.sub(r"[^0-9A-Za-z]+", "-", ip).strip("-")


def worker_id(ip: str) -> str:
    return "worker-" + re.sub(r"[^0-9A-Za-z]+", "-", ip).strip("-")


if __name__ == "__main__":
    sys.exit(main())
