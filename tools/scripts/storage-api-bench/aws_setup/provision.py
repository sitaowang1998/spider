#!/usr/bin/env python3
"""Provision AWS resources for storage API benchmark runs."""

from __future__ import annotations

import argparse
import pathlib
import sys

import ami_state
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
    config.instances.ami_id = resolve_runtime_ami_id(config, args.ami_state)
    provision(config, client, state)
    state_module.save_state(args.state, state)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--secret", type=pathlib.Path, default=ROOT / ".secret")
    parser.add_argument("--state", type=pathlib.Path, required=True)
    parser.add_argument("--ami-state", type=pathlib.Path, default=ami_state.default_ami_state_path())
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def resolve_runtime_ami_id(
    config: config_module.AwsBenchConfig,
    ami_state_path: pathlib.Path,
) -> str:
    if config.instances.ami_id and config.instances.ami_id != "ami-xxxxxxxx":
        return config.instances.ami_id
    metadata = ami_state.load_ami_state(ami_state_path)
    ami_id = metadata.get("ami_id")
    if isinstance(ami_id, str) and ami_id:
        return ami_id
    msg = f"instances.ami_id is not set and {ami_state_path} does not contain ami_id"
    raise ValueError(msg)


def provision(
    config: config_module.AwsBenchConfig,
    client: aws_cli.AwsCli,
    state: dict[str, object],
) -> None:
    resources = state.setdefault("resources", {})
    if not isinstance(resources, dict):
        msg = "state resources must be an object"
        raise ValueError(msg)

    ensure_network(client, config, resources)
    ensure_ssm_instance_profile(client, config)
    ensure_placement_group(client, config)
    ensure_rds_subnet_group(client, config)
    launch_instances(client, config)
    create_rds(client, config)
    ensure_results_bucket(client, config, resources)

    resources["run_id"] = config.aws.run_id
    resources["rds_instance_id"] = rds_instance_id(config)
    wait_for_rds(client, config)
    resources["rds_endpoint"] = discover_rds_endpoint(client, config)
    wait_for_ec2(client, config)


def ensure_network(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    resources: dict[str, object],
) -> None:
    if not config.network.vpc_id:
        config.network.vpc_id = create_vpc(client, config)
        resources["vpc_id"] = config.network.vpc_id
    if not config.network.subnet_id:
        config.network.subnet_id = create_subnet(
            client,
            config,
            config.network.subnet_cidr,
            config.aws.availability_zone,
        )
        resources["subnet_id"] = config.network.subnet_id
    if not config.network.rds_subnet_ids:
        availability_zones = config.network.rds_subnet_availability_zones or [
            config.aws.availability_zone,
            second_az(config.aws.availability_zone),
        ]
        config.network.rds_subnet_ids = [
            create_subnet(client, config, cidr, az)
            for cidr, az in zip(config.network.rds_subnet_cidrs, availability_zones, strict=False)
        ]
        resources["rds_subnet_ids"] = config.network.rds_subnet_ids
    if not config.network.security_group_id:
        config.network.security_group_id = create_security_group(
            client,
            config,
            f"{config.aws.run_id}-ec2",
            "Spider storage API benchmark EC2",
        )
        resources["security_group_id"] = config.network.security_group_id
        authorize_self_ingress(client, config.network.security_group_id, [8091, 50051, 19091])
    if not config.network.rds_security_group_id:
        config.network.rds_security_group_id = create_security_group(
            client,
            config,
            f"{config.aws.run_id}-rds",
            "Spider storage API benchmark RDS",
        )
        resources["rds_security_group_id"] = config.network.rds_security_group_id
        authorize_group_ingress(
            client,
            config.network.rds_security_group_id,
            config.network.security_group_id,
            config.database.port,
        )


def create_vpc(client: aws_cli.AwsCli, config: config_module.AwsBenchConfig) -> str:
    data = client.run_json(["ec2", "create-vpc", "--cidr-block", config.network.vpc_cidr])
    return data.get("Vpc", {}).get("VpcId", f"vpc-{config.aws.run_id}")


def create_subnet(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    cidr: str,
    availability_zone: str,
) -> str:
    data = client.run_json(
        [
            "ec2",
            "create-subnet",
            "--vpc-id",
            config.network.vpc_id,
            "--cidr-block",
            cidr,
            "--availability-zone",
            availability_zone,
        ]
    )
    return data.get("Subnet", {}).get("SubnetId", f"subnet-{cidr.replace('.', '-').replace('/', '-')}")


def create_security_group(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    name: str,
    description: str,
) -> str:
    data = client.run_json(
        [
            "ec2",
            "create-security-group",
            "--group-name",
            name,
            "--description",
            description,
            "--vpc-id",
            config.network.vpc_id,
        ]
    )
    return data.get("GroupId", f"sg-{name}")


def authorize_self_ingress(client: aws_cli.AwsCli, group_id: str, ports: list[int]) -> None:
    for port in ports:
        authorize_group_ingress(client, group_id, group_id, port)


def authorize_group_ingress(
    client: aws_cli.AwsCli,
    group_id: str,
    source_group_id: str,
    port: int,
) -> None:
    client.run(
        [
            "ec2",
            "authorize-security-group-ingress",
            "--group-id",
            group_id,
            "--protocol",
            "tcp",
            "--port",
            str(port),
            "--source-group",
            source_group_id,
        ]
    )


def ensure_ssm_instance_profile(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> None:
    assume_role_policy = (
        '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":'
        '{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    )
    role_name = config.instances.iam_instance_profile
    client.run(
        [
            "iam",
            "create-role",
            "--role-name",
            role_name,
            "--assume-role-policy-document",
            assume_role_policy,
        ]
    )
    client.run(
        [
            "iam",
            "attach-role-policy",
            "--role-name",
            role_name,
            "--policy-arn",
            "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
        ]
    )
    client.run(["iam", "create-instance-profile", "--instance-profile-name", role_name])
    client.run(
        [
            "iam",
            "add-role-to-instance-profile",
            "--instance-profile-name",
            role_name,
            "--role-name",
            role_name,
        ]
    )


def ensure_placement_group(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> None:
    client.run(
        [
            "ec2",
            "create-placement-group",
            "--group-name",
            config.network.placement_group,
            "--strategy",
            "cluster",
        ]
    )


def ensure_rds_subnet_group(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> None:
    if len(config.network.rds_subnet_ids) < 2:
        msg = "network.rds_subnet_ids must contain at least two subnets"
        raise ValueError(msg)
    client.run(
        [
            "rds",
            "create-db-subnet-group",
            "--db-subnet-group-name",
            db_subnet_group_name(config),
            "--db-subnet-group-description",
            "Spider storage API benchmark",
            "--subnet-ids",
            *config.network.rds_subnet_ids,
        ]
    )


def launch_instances(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> None:
    launch_role(client, config, "storage-server", config.instances.server_type, 1)
    launch_role(client, config, "controller", config.instances.controller_type, 1)
    launch_role(
        client,
        config,
        "benchmark-client",
        config.instances.client_type,
        config.instances.client_count,
    )


def launch_role(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    role: str,
    instance_type: str,
    count: int,
) -> None:
    command = [
        "ec2",
        "run-instances",
        "--image-id",
        config.instances.ami_id,
        "--instance-type",
        instance_type,
        "--count",
        str(count),
        "--security-group-ids",
        config.network.security_group_id,
        "--subnet-id",
        config.network.subnet_id,
        "--placement",
        f"AvailabilityZone={config.aws.availability_zone},GroupName={config.network.placement_group}",
        "--tag-specifications",
        f"ResourceType=instance,Tags=[{{Key=RunId,Value={config.aws.run_id}}},{{Key=Role,Value={role}}}]",
        "--iam-instance-profile",
        f"Name={config.instances.iam_instance_profile}",
    ]
    if config.instances.key_name is not None:
        command.extend(["--key-name", config.instances.key_name])
    client.run(command)


def create_rds(client: aws_cli.AwsCli, config: config_module.AwsBenchConfig) -> None:
    client.run(
        [
            "rds",
            "create-db-instance",
            "--db-instance-identifier",
            rds_instance_id(config),
            "--engine",
            "mariadb",
            "--db-instance-class",
            config.database.instance_class,
            "--allocated-storage",
            str(config.database.allocated_storage),
            "--storage-type",
            config.database.storage_type,
            "--db-name",
            config.database.name,
            "--master-username",
            config.database.username,
            "--master-user-password",
            config.database.password,
            "--vpc-security-group-ids",
            config.network.rds_security_group_id,
            "--db-subnet-group-name",
            db_subnet_group_name(config),
            "--availability-zone",
            config.aws.availability_zone,
            "--no-publicly-accessible",
        ]
    )


def ensure_results_bucket(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    resources: dict[str, object],
) -> None:
    if config.results.s3_uri is not None:
        resources["results_s3_uri"] = config.results.s3_uri
        return
    bucket = result_bucket_name(config)
    client.run(create_bucket_command(bucket, config.aws.region))
    resources["result_bucket"] = bucket
    resources["results_s3_uri"] = f"s3://{bucket}/{config.aws.run_id}"


def create_bucket_command(bucket: str, region: str) -> list[str]:
    command = ["s3api", "create-bucket", "--bucket", bucket]
    if region != "us-east-1":
        command.extend(
            [
                "--create-bucket-configuration",
                f"LocationConstraint={region}",
            ]
        )
    return command


def wait_for_ec2(client: aws_cli.AwsCli, config: config_module.AwsBenchConfig) -> None:
    client.run(
        [
            "ec2",
            "wait",
            "instance-running",
            "--filters",
            f"Name=tag:RunId,Values={config.aws.run_id}",
        ]
    )


def wait_for_rds(client: aws_cli.AwsCli, config: config_module.AwsBenchConfig) -> None:
    client.run(
        [
            "rds",
            "wait",
            "db-instance-available",
            "--db-instance-identifier",
            rds_instance_id(config),
        ]
    )


def discover_rds_endpoint(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> str:
    data = client.run_json(
        [
            "rds",
            "describe-db-instances",
            "--db-instance-identifier",
            rds_instance_id(config),
        ]
    )
    try:
        return data["DBInstances"][0]["Endpoint"]["Address"]
    except (KeyError, IndexError, TypeError):
        return f"{rds_instance_id(config)}.dry-run.local"


def rds_instance_id(config: config_module.AwsBenchConfig) -> str:
    return f"{config.aws.run_id}-rds"


def db_subnet_group_name(config: config_module.AwsBenchConfig) -> str:
    return f"{config.aws.run_id}-rds-subnets"


def result_bucket_name(config: config_module.AwsBenchConfig) -> str:
    sanitized = "".join(
        char.lower() if char.isalnum() else "-" for char in config.aws.run_id
    ).strip("-")
    return f"spider-bench-results-{sanitized}-{config.aws.region}"


def second_az(availability_zone: str) -> str:
    if not availability_zone:
        return availability_zone
    suffix = availability_zone[-1]
    if suffix == "a":
        return f"{availability_zone[:-1]}b"
    return f"{availability_zone[:-1]}a"


if __name__ == "__main__":
    sys.exit(main())
