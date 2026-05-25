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
    progress("deleting created network resources")
    delete_network_resources(client, state)


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


def delete_network_resources(client: aws_cli.AwsCli, state: dict[str, object]) -> None:
    resources = state.get("resources", {})
    if not isinstance(resources, dict):
        return
    for association_id in resources.get("route_table_association_ids", []):
        progress(f"disassociating route table association {association_id}")
        client.run(["ec2", "disassociate-route-table", "--association-id", association_id])
    route_table_id = resources.get("route_table_id")
    if isinstance(route_table_id, str) and route_table_id:
        progress(f"deleting route table {route_table_id}")
        client.run(["ec2", "delete-route-table", "--route-table-id", route_table_id])
    internet_gateway_id = resources.get("internet_gateway_id")
    vpc_id = resources.get("vpc_id")
    if isinstance(internet_gateway_id, str) and internet_gateway_id:
        if isinstance(vpc_id, str) and vpc_id:
            progress(f"detaching internet gateway {internet_gateway_id}")
            client.run(["ec2", "detach-internet-gateway", "--internet-gateway-id", internet_gateway_id, "--vpc-id", vpc_id])
        progress(f"deleting internet gateway {internet_gateway_id}")
        client.run(["ec2", "delete-internet-gateway", "--internet-gateway-id", internet_gateway_id])
    for subnet_id in resources.get("rds_subnet_ids", []):
        if subnet_id != resources.get("subnet_id"):
            progress(f"deleting subnet {subnet_id}")
            client.run(["ec2", "delete-subnet", "--subnet-id", subnet_id])
    subnet_id = resources.get("subnet_id")
    if isinstance(subnet_id, str) and subnet_id:
        progress(f"deleting subnet {subnet_id}")
        client.run(["ec2", "delete-subnet", "--subnet-id", subnet_id])
    security_group_id = resources.get("security_group_id")
    if isinstance(security_group_id, str) and security_group_id:
        progress(f"deleting EC2 security group {security_group_id}")
        client.run(["ec2", "delete-security-group", "--group-id", security_group_id])
    rds_security_group_id = resources.get("rds_security_group_id")
    if isinstance(rds_security_group_id, str) and rds_security_group_id:
        progress(f"deleting RDS security group {rds_security_group_id}")
        client.run(["ec2", "delete-security-group", "--group-id", rds_security_group_id])
    if isinstance(vpc_id, str) and vpc_id:
        progress(f"deleting VPC {vpc_id}")
        client.run(["ec2", "delete-vpc", "--vpc-id", vpc_id])


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
