#!/usr/bin/env python3
"""Deregisters a benchmark AMI and deletes snapshots recorded by AWS."""

from __future__ import annotations

import argparse
import pathlib
import sys

import ami_state
import aws_cli
import config as config_module
import env as env_module


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
    metadata = ami_state.load_ami_state(args.ami_state)
    cleanup_ami(client, metadata)
    metadata["cleaned_at"] = ami_state.timestamp()
    ami_state.save_ami_state(args.ami_state, metadata)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--secret", type=pathlib.Path, default=ROOT / ".secret")
    parser.add_argument("--ami-state", type=pathlib.Path, default=ami_state.default_ami_state_path())
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def cleanup_ami(client: aws_cli.AwsCli, metadata: dict[str, object]) -> None:
    ami_id = metadata.get("ami_id")
    if not isinstance(ami_id, str) or not ami_id:
        msg = "AMI state does not contain ami_id"
        raise ValueError(msg)
    snapshot_ids = describe_image_snapshots(client, ami_id)
    client.run(["ec2", "deregister-image", "--image-id", ami_id])
    for snapshot_id in snapshot_ids:
        client.run(["ec2", "delete-snapshot", "--snapshot-id", snapshot_id])
    builder_instance_id = metadata.get("builder_instance_id")
    if isinstance(builder_instance_id, str) and builder_instance_id:
        client.run(["ec2", "terminate-instances", "--instance-ids", builder_instance_id])


def describe_image_snapshots(client: aws_cli.AwsCli, ami_id: str) -> list[str]:
    data = client.run_json(["ec2", "describe-images", "--image-ids", ami_id])
    if not isinstance(data, dict):
        return []
    snapshots = []
    for image in data.get("Images", []):
        for mapping in image.get("BlockDeviceMappings", []):
            snapshot_id = mapping.get("Ebs", {}).get("SnapshotId")
            if snapshot_id:
                snapshots.append(snapshot_id)
    return snapshots


if __name__ == "__main__":
    sys.exit(main())
