#!/usr/bin/env python3
"""Tear down AWS resources created for a storage benchmark run."""

from __future__ import annotations

import argparse
import pathlib
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
    teardown(config, client, state)
    progress("teardown complete")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--secret", type=pathlib.Path, default=ROOT / ".secret")
    parser.add_argument("--state", type=pathlib.Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def teardown(
    config: config_module.AwsBenchConfig,
    client: aws_cli.AwsCli,
    state: dict[str, object],
) -> None:
    progress("discovering benchmark instances")
    instance_ids = discover_all_instance_ids(client, config.aws.run_id)
    if instance_ids:
        progress(f"terminating {len(instance_ids)} instance(s)")
        client.run(["ec2", "terminate-instances", "--instance-ids", *instance_ids])
    else:
        progress("no benchmark instances found")
    progress("deleting RDS instance")
    client.run(
        [
            "rds",
            "delete-db-instance",
            "--db-instance-identifier",
            f"{config.aws.run_id}-rds",
            "--skip-final-snapshot",
            "--delete-automated-backups",
        ]
    )
    progress("deleting RDS subnet group")
    client.run(
        [
            "rds",
            "delete-db-subnet-group",
            "--db-subnet-group-name",
            f"{config.aws.run_id}-rds-subnets",
        ]
    )
    progress("deleting placement group")
    client.run(["ec2", "delete-placement-group", "--group-name", config.network.placement_group])
    progress("deleting results bucket data")
    delete_results_bucket(client, state)


def progress(message: str) -> None:
    progress_module.log("teardown", message)


def delete_results_bucket(client: aws_cli.AwsCli, state: dict[str, object]) -> None:
    resources = state.get("resources", {})
    if not isinstance(resources, dict):
        return
    bucket = resources.get("result_bucket")
    results_s3_uri = resources.get("results_s3_uri")
    if isinstance(results_s3_uri, str) and results_s3_uri:
        progress(f"removing result objects under {results_s3_uri}")
        client.run(["s3", "rm", results_s3_uri, "--recursive"])
    if isinstance(bucket, str) and bucket:
        progress(f"deleting result bucket {bucket}")
        client.run(["s3api", "delete-bucket", "--bucket", bucket])


def discover_all_instance_ids(client: aws_cli.AwsCli, run_id: str) -> list[str]:
    data = client.run_json(
        [
            "ec2",
            "describe-instances",
            "--filters",
            f"Name=tag:RunId,Values={run_id}",
            "Name=instance-state-name,Values=running,pending,stopped",
        ]
    )
    instance_ids = []
    for reservation in data.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            instance_ids.append(instance["InstanceId"])
    return instance_ids


if __name__ == "__main__":
    sys.exit(main())
