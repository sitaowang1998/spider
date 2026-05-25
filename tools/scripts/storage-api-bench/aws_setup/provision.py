#!/usr/bin/env python3
"""Provision AWS resources for storage API benchmark runs."""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

import ami_state
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
    config.instances.ami_id = resolve_runtime_ami_id(config, args.ami_state)
    progress(f"using benchmark AMI {config.instances.ami_id}")
    provision(config, client, state, args.state)
    progress(f"saving state to {args.state}")
    state_module.save_state(args.state, state)
    progress("provision complete")
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
    state_path: pathlib.Path | None = None,
) -> None:
    resources = state.setdefault("resources", {})
    if not isinstance(resources, dict):
        msg = "state resources must be an object"
        raise ValueError(msg)

    progress("ensuring network resources")
    ensure_network(client, config, resources)
    resources["run_id"] = config.aws.run_id
    save_progress_state(state_path, state)
    progress("ensuring SSM instance profile")
    ensure_ssm_instance_profile(client, config)
    progress("ensuring placement group")
    ensure_placement_group(client, config)
    progress("ensuring RDS subnet group")
    ensure_rds_subnet_group(client, config, resources)
    save_progress_state(state_path, state)
    progress("launching benchmark instances")
    launch_instances(client, config)
    progress("creating RDS instance")
    create_rds(client, config)
    progress("ensuring results bucket")
    ensure_results_bucket(client, config, resources)

    resources["rds_instance_id"] = rds_instance_id(config)
    save_progress_state(state_path, state)
    progress("waiting for RDS instance to become available")
    wait_for_rds(client, config)
    progress("discovering RDS endpoint")
    resources["rds_endpoint"] = discover_rds_endpoint(client, config)
    save_progress_state(state_path, state)
    progress(f"RDS endpoint: {resources['rds_endpoint']}")
    progress("waiting for EC2 instances to enter running state")
    wait_for_ec2(client, config)
    progress("EC2 instances are running")


def progress(message: str) -> None:
    progress_module.log("provision", message)


def save_progress_state(state_path: pathlib.Path | None, state: dict[str, object]) -> None:
    if state_path is None:
        return
    progress(f"saving partial state to {state_path}")
    state_module.save_state(state_path, state)


def ensure_network(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    resources: dict[str, object],
) -> None:
    hydrate_network_config_from_state(config, resources)
    normalize_network_config(config)
    if not config.network.vpc_id:
        progress("creating VPC")
        config.network.vpc_id = create_vpc(client, config)
        resources["vpc_id"] = config.network.vpc_id
        progress(f"created VPC {config.network.vpc_id}")
    if not config.network.subnet_id:
        progress("creating primary subnet")
        validate_primary_rds_subnet_config(config)
        config.network.subnet_id = create_subnet(
            client,
            config,
            config.network.subnet_cidr,
            config.aws.availability_zone,
        )
        resources["subnet_id"] = config.network.subnet_id
        progress(f"created primary subnet {config.network.subnet_id}")
    if not config.network.rds_subnet_ids:
        progress("creating secondary RDS subnet")
        secondary_cidr, secondary_az = secondary_rds_subnet_config(config)
        secondary_subnet_id = create_subnet(client, config, secondary_cidr, secondary_az)
        config.network.rds_subnet_ids = [config.network.subnet_id, secondary_subnet_id]
        resources["rds_subnet_ids"] = config.network.rds_subnet_ids
        progress(f"created RDS subnets: {', '.join(config.network.rds_subnet_ids)}")
    if resources.get("vpc_id") and not resources.get("route_table_id"):
        progress("creating public routing for created benchmark subnets")
        setup_public_routing(client, config, resources)
    if not config.network.security_group_id:
        progress("creating EC2 security group")
        config.network.security_group_id = create_security_group(
            client,
            config,
            f"{config.aws.run_id}-ec2",
            "Spider storage API benchmark EC2",
        )
        resources["security_group_id"] = config.network.security_group_id
        authorize_self_ingress(client, config.network.security_group_id, [8091, 50051, 19091])
        progress(f"created EC2 security group {config.network.security_group_id}")
    if not config.network.rds_security_group_id:
        progress("creating RDS security group")
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
        progress(f"created RDS security group {config.network.rds_security_group_id}")


def hydrate_network_config_from_state(
    config: config_module.AwsBenchConfig,
    resources: dict[str, object],
) -> None:
    if not config.network.vpc_id and isinstance(resources.get("vpc_id"), str):
        config.network.vpc_id = resources["vpc_id"]
    if not config.network.subnet_id and isinstance(resources.get("subnet_id"), str):
        config.network.subnet_id = resources["subnet_id"]
    if not config.network.rds_subnet_ids and isinstance(resources.get("rds_subnet_ids"), list):
        config.network.rds_subnet_ids = [
            subnet_id for subnet_id in resources["rds_subnet_ids"] if isinstance(subnet_id, str)
        ]
    if not config.network.security_group_id and isinstance(resources.get("security_group_id"), str):
        config.network.security_group_id = resources["security_group_id"]
    if not config.network.rds_security_group_id and isinstance(resources.get("rds_security_group_id"), str):
        config.network.rds_security_group_id = resources["rds_security_group_id"]


def normalize_network_config(config: config_module.AwsBenchConfig) -> None:
    config.network.rds_subnet_ids = non_empty_strings(config.network.rds_subnet_ids)
    config.network.rds_subnet_cidrs = non_empty_strings(config.network.rds_subnet_cidrs)
    config.network.rds_subnet_availability_zones = non_empty_strings(config.network.rds_subnet_availability_zones)


def non_empty_strings(values: list[str]) -> list[str]:
    return [value for value in values if value]


def validate_primary_rds_subnet_config(config: config_module.AwsBenchConfig) -> None:
    if not config.network.rds_subnet_availability_zones:
        msg = "network.rds_subnet_availability_zones must include primary and secondary RDS AZs"
        raise ValueError(msg)
    if config.network.rds_subnet_availability_zones[0] != config.aws.availability_zone:
        msg = "first network.rds_subnet_availability_zones entry must match aws.availability_zone"
        raise ValueError(msg)
    if not config.network.rds_subnet_cidrs:
        msg = "network.rds_subnet_cidrs must include primary and secondary RDS CIDRs"
        raise ValueError(msg)
    if config.network.rds_subnet_cidrs[0] != config.network.subnet_cidr:
        msg = "first network.rds_subnet_cidrs entry must match network.subnet_cidr"
        raise ValueError(msg)


def secondary_rds_subnet_config(config: config_module.AwsBenchConfig) -> tuple[str, str]:
    if len(config.network.rds_subnet_cidrs) < 2:
        msg = "network.rds_subnet_cidrs must contain a secondary RDS subnet CIDR"
        raise ValueError(msg)
    if len(config.network.rds_subnet_availability_zones) < 2:
        msg = "network.rds_subnet_availability_zones must contain a secondary RDS AZ"
        raise ValueError(msg)
    secondary_az = config.network.rds_subnet_availability_zones[1]
    if secondary_az == config.aws.availability_zone:
        msg = "secondary RDS AZ must differ from aws.availability_zone"
        raise ValueError(msg)
    return config.network.rds_subnet_cidrs[1], secondary_az


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


def setup_public_routing(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    resources: dict[str, object],
) -> None:
    internet_gateway_id = create_internet_gateway(client, config)
    resources["internet_gateway_id"] = internet_gateway_id
    attach_internet_gateway(client, config.network.vpc_id, internet_gateway_id)
    route_table_id = create_route_table(client, config)
    resources["route_table_id"] = route_table_id
    create_default_route(client, route_table_id, internet_gateway_id)
    subnet_ids = non_empty_strings([config.network.subnet_id])
    subnet_ids.extend(
        subnet_id
        for subnet_id in non_empty_strings(config.network.rds_subnet_ids)
        if subnet_id not in subnet_ids
    )
    route_table_association_ids = []
    for subnet_id in subnet_ids:
        enable_public_ip_mapping(client, subnet_id)
        association_id = associate_route_table(client, route_table_id, subnet_id)
        route_table_association_ids.append(association_id)
    resources["route_table_association_ids"] = route_table_association_ids


def create_internet_gateway(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> str:
    data = client.run_json(
        [
            "ec2",
            "create-internet-gateway",
            "--tag-specifications",
            f"ResourceType=internet-gateway,Tags=[{{Key=RunId,Value={config.aws.run_id}}}]",
        ]
    )
    return data.get("InternetGateway", {}).get("InternetGatewayId", f"igw-{config.aws.run_id}")


def attach_internet_gateway(client: aws_cli.AwsCli, vpc_id: str, internet_gateway_id: str) -> None:
    client.run(["ec2", "attach-internet-gateway", "--vpc-id", vpc_id, "--internet-gateway-id", internet_gateway_id])


def create_route_table(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> str:
    data = client.run_json(
        [
            "ec2",
            "create-route-table",
            "--vpc-id",
            config.network.vpc_id,
            "--tag-specifications",
            f"ResourceType=route-table,Tags=[{{Key=RunId,Value={config.aws.run_id}}}]",
        ]
    )
    return data.get("RouteTable", {}).get("RouteTableId", f"rtb-{config.aws.run_id}")


def create_default_route(client: aws_cli.AwsCli, route_table_id: str, internet_gateway_id: str) -> None:
    client.run(
        [
            "ec2",
            "create-route",
            "--route-table-id",
            route_table_id,
            "--destination-cidr-block",
            "0.0.0.0/0",
            "--gateway-id",
            internet_gateway_id,
        ]
    )


def enable_public_ip_mapping(client: aws_cli.AwsCli, subnet_id: str) -> None:
    client.run(["ec2", "modify-subnet-attribute", "--subnet-id", subnet_id, "--map-public-ip-on-launch"])


def associate_route_table(client: aws_cli.AwsCli, route_table_id: str, subnet_id: str) -> str:
    data = client.run_json(["ec2", "associate-route-table", "--route-table-id", route_table_id, "--subnet-id", subnet_id])
    return data.get("AssociationId", f"rtbassoc-{subnet_id}")


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
    client.run_allow_failure(
        [
            "iam",
            "create-role",
            "--role-name",
            role_name,
            "--assume-role-policy-document",
            assume_role_policy,
        ]
    )
    client.run_allow_failure(
        [
            "iam",
            "attach-role-policy",
            "--role-name",
            role_name,
            "--policy-arn",
            "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
        ]
    )
    client.run_allow_failure(["iam", "create-instance-profile", "--instance-profile-name", role_name])
    client.run_allow_failure(
        [
            "iam",
            "add-role-to-instance-profile",
            "--instance-profile-name",
            role_name,
            "--role-name",
            role_name,
        ]
    )
    wait_for_instance_profile_role(client, role_name)


def wait_for_instance_profile_role(
    client: aws_cli.AwsCli,
    instance_profile_name: str,
) -> None:
    if client.dry_run:
        return
    for _ in range(24):
        data = client.run_json(
            [
                "iam",
                "get-instance-profile",
                "--instance-profile-name",
                instance_profile_name,
            ]
        )
        roles = data.get("InstanceProfile", {}).get("Roles", []) if isinstance(data, dict) else []
        if roles:
            return
        time.sleep(5)
    msg = f"instance profile {instance_profile_name} has no role after waiting"
    raise RuntimeError(msg)


def ensure_placement_group(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> None:
    if not config.network.placement_group:
        progress("placement group disabled")
        return
    returncode, _ = client.try_run_json(
        [
            "ec2",
            "describe-placement-groups",
            "--group-names",
            config.network.placement_group,
        ]
    )
    if returncode == 0:
        progress(f"reusing placement group {config.network.placement_group}")
        return
    client.run(
        [
            "ec2",
            "create-placement-group",
            "--group-name",
            config.network.placement_group,
            "--strategy",
            config.network.placement_strategy,
        ]
    )


def ensure_rds_subnet_group(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    resources: dict[str, object],
) -> None:
    if len(config.network.rds_subnet_ids) < 2:
        msg = "network.rds_subnet_ids must contain at least two subnets"
        raise ValueError(msg)
    group_name = db_subnet_group_name(config)
    resources["rds_subnet_group_name"] = group_name
    returncode, data = client.try_run_json(
        [
            "rds",
            "describe-db-subnet-groups",
            "--db-subnet-group-name",
            group_name,
        ]
    )
    if returncode == 0:
        existing_vpc_id = existing_db_subnet_group_vpc_id(data)
        if existing_vpc_id and existing_vpc_id != config.network.vpc_id:
            msg = (
                f"RDS subnet group {group_name} already exists in VPC {existing_vpc_id}, "
                f"but this run is using VPC {config.network.vpc_id}. This usually means local "
                "state was deleted while AWS resources from the same run_id still exist. Run "
                "teardown for the old run, delete the stale RDS subnet group/RDS instance, or use "
                "a new aws.run_id."
            )
            raise RuntimeError(msg)
        progress(f"reusing RDS subnet group {group_name}")
        client.run(
            [
                "rds",
                "modify-db-subnet-group",
                "--db-subnet-group-name",
                group_name,
                "--subnet-ids",
                *config.network.rds_subnet_ids,
            ]
        )
        return
    client.run(
        [
            "rds",
            "create-db-subnet-group",
            "--db-subnet-group-name",
            group_name,
            "--db-subnet-group-description",
            "Spider storage API benchmark",
            "--subnet-ids",
            *config.network.rds_subnet_ids,
        ]
    )


def existing_db_subnet_group_vpc_id(data: object) -> str | None:
    if not isinstance(data, dict):
        return None
    groups = data.get("DBSubnetGroups")
    if not isinstance(groups, list) or not groups:
        return None
    group = groups[0]
    if not isinstance(group, dict):
        return None
    vpc_id = group.get("VpcId")
    return vpc_id if isinstance(vpc_id, str) and vpc_id else None


def launch_instances(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> None:
    progress("launching storage server instance")
    launch_role(client, config, "storage-server", config.instances.server_type, 1)
    progress("launching controller instance")
    launch_role(client, config, "controller", config.instances.controller_type, 1)
    progress("launching dedicated benchmark submitter instance")
    launch_role(
        client,
        config,
        "benchmark-submitter",
        config.instances.submitter_type,
        1,
    )
    progress(f"launching {config.instances.worker_count} benchmark worker instances")
    launch_role(
        client,
        config,
        "benchmark-worker",
        config.instances.worker_type,
        config.instances.worker_count,
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
        "--tag-specifications",
        f"ResourceType=instance,Tags=[{{Key=RunId,Value={config.aws.run_id}}},{{Key=Role,Value={role}}}]",
        "--iam-instance-profile",
        f"Name={config.instances.iam_instance_profile}",
    ]
    placement = f"AvailabilityZone={config.aws.availability_zone}"
    if config.network.placement_group:
        placement += f",GroupName={config.network.placement_group}"
    command.extend(["--placement", placement])
    if config.instances.key_name is not None:
        command.extend(["--key-name", config.instances.key_name])
    client.run(command)


def create_rds(client: aws_cli.AwsCli, config: config_module.AwsBenchConfig) -> None:
    instance_id = rds_instance_id(config)
    returncode, _ = client.try_run_json(
        [
            "rds",
            "describe-db-instances",
            "--db-instance-identifier",
            instance_id,
        ]
    )
    if returncode == 0:
        progress(f"reusing RDS instance {instance_id}")
        return
    client.run(
        [
            "rds",
            "create-db-instance",
            "--db-instance-identifier",
            instance_id,
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
