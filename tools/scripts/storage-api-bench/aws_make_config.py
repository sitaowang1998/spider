#!/usr/bin/env python3
"""Generates a distributed benchmark config for AWS private IPs."""

from __future__ import annotations

import argparse
import ipaddress
import pathlib
import re
import sys


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = ROOT / "components/spider-storage-api-bench/config/aws.toml"


def main() -> int:
    args = parse_args()
    server_ip = parse_ip(args.server_private_ip)
    agent_ips = read_agent_ips(args.agent_ips)
    if not agent_ips:
        print("agent IP file must contain at least one private IP", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_config(args, server_ip, agent_ips), encoding="utf-8")
    print(args.output)
    print(f"agents={len(agent_ips)} jobs={len(agent_ips) * args.jobs_per_agent}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-private-ip", required=True)
    parser.add_argument(
        "--agent-ips",
        type=pathlib.Path,
        required=True,
        help="Text file with one client-agent private IP per line.",
    )
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--jobs-per-agent", type=int, default=10)
    parser.add_argument("--tasks-per-job", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--client-count", type=int, default=8)
    parser.add_argument("--worker-count", type=int, default=16)
    parser.add_argument("--poll-batch", type=int, default=64)
    parser.add_argument("--poll-wait-ms", type=int, default=10)
    parser.add_argument("--flat-percent", type=int, default=50)
    parser.add_argument("--agent-timeout-sec", type=int, default=7200)
    parser.add_argument("--poll-interval-ms", type=int, default=1000)
    parser.add_argument("--rest-port", type=int, default=8091)
    parser.add_argument("--grpc-port", type=int, default=50051)
    parser.add_argument("--agent-port", type=int, default=19091)
    parser.add_argument("--database-port", type=int, default=3306)
    parser.add_argument("--database-name", default="spider-db")
    parser.add_argument("--database-username", default="spider-user")
    parser.add_argument("--database-password", default="spider-password")
    parser.add_argument("--database-max-connections", type=int, default=256)
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


def render_config(args: argparse.Namespace, server_ip: str, agent_ips: list[str]) -> str:
    job_count = len(agent_ips) * args.jobs_per_agent
    lines = [
        "[server]",
        f'rest_bind = "0.0.0.0:{args.rest_port}"',
        f'grpc_bind = "0.0.0.0:{args.grpc_port}"',
        f'rest_target = "http://{server_ip}:{args.rest_port}"',
        f'grpc_target = "http://{server_ip}:{args.grpc_port}"',
        "",
        "[database]",
        'host = "127.0.0.1"',
        f"port = {args.database_port}",
        f'name = "{args.database_name}"',
        f'username = "{args.database_username}"',
        f'password = "{args.database_password}"',
        f"max_connections = {args.database_max_connections}",
        "",
        "[benchmark]",
        f"task_count = {args.tasks_per_job}",
        f"job_count = {job_count}",
        f"payload_bytes = {args.payload_bytes}",
        f"client_count = {args.client_count}",
        f"worker_count = {args.worker_count}",
        f"poll_batch = {args.poll_batch}",
        f"poll_wait_ms = {args.poll_wait_ms}",
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
    for ip in agent_ips:
        lines.extend(
            [
                "[[distributed.agents]]",
                f'id = "{agent_id(ip)}"',
                f'url = "http://{ip}:{args.agent_port}"',
                "",
            ]
        )
    return "\n".join(lines)


def agent_id(ip: str) -> str:
    return "client-" + re.sub(r"[^0-9A-Za-z]+", "-", ip).strip("-")


if __name__ == "__main__":
    sys.exit(main())
