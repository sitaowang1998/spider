#!/usr/bin/env python3
"""Deploy benchmark artifacts to provisioned AWS instances via SSM."""

from __future__ import annotations

import argparse
import pathlib
import shlex
import sys

import aws_cli
import config as config_module
import env as env_module
import progress as progress_module
import state as state_module


ROOT = pathlib.Path(__file__).resolve().parents[4]


def main() -> int:
    args = parse_args()
    progress("loading config and credentials")
    config = config_module.load_config(args.config)
    secret = env_module.load_secret(args.secret)
    aws_env = env_module.build_aws_env(
        secret,
        region=config.aws.region,
        endpoint_url=config.aws.endpoint_url,
    )
    client = aws_cli.AwsCli(
        endpoint_url=config.aws.endpoint_url,
        env=aws_env,
        dry_run=args.dry_run,
    )
    state = state_module.load_state(args.state)
    deploy(config, client, state)
    progress("deploy check command submitted")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--secret", type=pathlib.Path, default=ROOT / ".secret")
    parser.add_argument("--state", type=pathlib.Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def deploy(
    config: config_module.AwsBenchConfig,
    client: aws_cli.AwsCli,
    _state: dict[str, object],
) -> None:
    progress("discovering running benchmark instances")
    instance_ids = discover_all_instance_ids(client, config.aws.run_id)
    if not instance_ids:
        if client.dry_run:
            instance_ids = ["i-dryrun"]
        else:
            msg = "no benchmark instances found for deployment"
            raise RuntimeError(msg)
    progress(f"sending deploy validation command to {len(instance_ids)} instance(s)")
    commands = deployment_commands(config)
    command_ids = client.send_shell_command(
        instance_ids,
        commands,
        comment="deploy spider benchmark artifact",
    )
    progress(f"deploy validation command submitted in {len(command_ids)} batch(es)")


def progress(message: str) -> None:
    progress_module.log("deploy", message)


def deployment_commands(config: config_module.AwsBenchConfig) -> list[str]:
    return [
        f"cd {shlex.quote(config.instances.remote_root)}",
        "test -x target/release/spider-storage-api-bench",
        "test -x tools/scripts/storage-api-bench/run_agent.py",
        "test -x tools/scripts/storage-api-bench/run_server.py",
        "command -v mariadb >/dev/null || command -v mysql >/dev/null || true",
    ]


def discover_all_instance_ids(client: aws_cli.AwsCli, run_id: str) -> list[str]:
    data = client.run_json(
        [
            "ec2",
            "describe-instances",
            "--filters",
            f"Name=tag:RunId,Values={run_id}",
            "Name=instance-state-name,Values=running",
        ]
    )
    instance_ids = []
    for reservation in data.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            instance_ids.append(instance["InstanceId"])
    return instance_ids


if __name__ == "__main__":
    sys.exit(main())
