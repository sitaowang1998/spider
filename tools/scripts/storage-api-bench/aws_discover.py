#!/usr/bin/env python3
"""Discovers tagged AWS benchmark instances and writes hidden runtime files."""

from __future__ import annotations

import argparse
import pathlib
import sys

import aws_common


def main() -> int:
    args = parse_args()
    workspace = args.workspace or aws_common.default_workspace(args.run_id, args.node_count)
    server, clients = discover(args.run_id, args.node_count)
    workspace.mkdir(parents=True, exist_ok=True)
    aws_common.write_lines(workspace / "server_ip.txt", [server["private_ip"]])
    aws_common.write_lines(
        workspace / "client_ips.txt",
        [client["private_ip"] for client in clients],
    )
    aws_common.write_lines(
        workspace / "client_instance_ids.txt",
        [client["instance_id"] for client in clients],
    )
    aws_common.write_lines(workspace / "server_instance_id.txt", [server["instance_id"]])
    print(f"workspace={workspace}")
    print(f"server={server['private_ip']} {server['instance_id']}")
    print(f"clients={len(clients)}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--node-count", type=int, required=True)
    parser.add_argument("--workspace", type=pathlib.Path)
    return parser.parse_args()


def discover(run_id: str, node_count: int) -> tuple[dict[str, str], list[dict[str, str]]]:
    servers = aws_common.discover_instances(run_id, "storage-server")
    if len(servers) != 1:
        raise SystemExit(f"expected one running storage-server for {run_id}, found {len(servers)}")
    clients = aws_common.discover_instances(run_id, "benchmark-client")
    if len(clients) < node_count:
        raise SystemExit(
            f"expected at least {node_count} running benchmark-client instances, found {len(clients)}"
        )
    return servers[0], clients[:node_count]


if __name__ == "__main__":
    sys.exit(main())
