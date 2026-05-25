#!/usr/bin/env python3
"""Deploy benchmark artifacts to provisioned AWS instances via SSM."""

from __future__ import annotations

import argparse
import json
import pathlib
import shlex
import sys

import aws_cli
import config as config_module
import env as env_module
import state as state_module


ROOT = pathlib.Path(__file__).resolve().parents[4]


def main() -> int:
    args = parse_args()
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
    instance_ids = discover_all_instance_ids(client, config.aws.run_id)
    if not instance_ids:
        if client.dry_run:
            instance_ids = ["i-dryrun"]
        else:
            msg = "no benchmark instances found for deployment"
            raise RuntimeError(msg)
    commands = deployment_commands(config)
    client.run(
        [
            "ssm",
            "send-command",
            "--document-name",
            "AWS-RunShellScript",
            "--comment",
            "deploy spider benchmark artifact",
            "--instance-ids",
            *instance_ids,
            "--parameters",
            json.dumps({"commands": commands}),
        ]
    )


def deployment_commands(config: config_module.AwsBenchConfig) -> list[str]:
    commands = [
        f"cd {shlex.quote(config.instances.remote_root)}",
        "test -x target/release/spider-storage-api-bench || cargo build --release --package spider-storage-api-bench",
        "test -x tools/scripts/storage-api-bench/run_agent.py",
        "test -x tools/scripts/storage-api-bench/run_server.py",
        "command -v mariadb >/dev/null || command -v mysql >/dev/null || true",
    ]
    if config.artifact.s3_uri is not None:
        commands.insert(0, f"aws s3 cp {shlex.quote(config.artifact.s3_uri)} /tmp/spider-bench-artifact")
    return commands


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
