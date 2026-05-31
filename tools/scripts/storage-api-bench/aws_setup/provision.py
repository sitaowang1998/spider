#!/usr/bin/env python3
"""Provision AWS resources for storage API benchmark runs."""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
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
    progress("creating RDS instance")
    create_rds(client, config)
    resources["rds_instance_id"] = rds_instance_id(config)
    save_progress_state(state_path, state)
    progress("launching benchmark instances")
    resources["instance_ids"] = launch_instances(client, config)
    save_progress_state(state_path, state)
    progress("ensuring results bucket")
    ensure_results_bucket(client, config, resources)
    ensure_results_upload_policy(client, config, resources)
    save_progress_state(state_path, state)
    progress("waiting for EC2 instances to enter running state")
    wait_for_ec2(client, resources["instance_ids"])
    progress("EC2 instances are running")
    progress("waiting for RDS instance to become available")
    wait_for_rds(client, config)
    progress("discovering RDS endpoint")
    resources["rds_endpoint"] = discover_rds_endpoint(client, config)
    save_progress_state(state_path, state)
    progress(f"RDS endpoint: {resources['rds_endpoint']}")


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
    worker_subnet_ids_configured = bool(non_empty_strings(config.network.worker_subnet_ids))
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
    ensure_worker_subnets(client, config, resources, worker_subnet_ids_configured)
    if resources.get("vpc_id") and not resources.get("route_table_id"):
        progress("creating public routing for created benchmark subnets")
        setup_public_routing(client, config, resources)
    elif resources.get("route_table_id"):
        ensure_worker_subnet_routing(client, config, resources)
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
    if not config.network.worker_subnet_ids and isinstance(resources.get("worker_subnet_ids"), list):
        config.network.worker_subnet_ids = [
            subnet_id for subnet_id in resources["worker_subnet_ids"] if isinstance(subnet_id, str)
        ]
    if not config.network.security_group_id and isinstance(resources.get("security_group_id"), str):
        config.network.security_group_id = resources["security_group_id"]
    if not config.network.rds_security_group_id and isinstance(resources.get("rds_security_group_id"), str):
        config.network.rds_security_group_id = resources["rds_security_group_id"]


def normalize_network_config(config: config_module.AwsBenchConfig) -> None:
    config.network.rds_subnet_ids = non_empty_strings(config.network.rds_subnet_ids)
    config.network.worker_subnet_ids = non_empty_strings(config.network.worker_subnet_ids)
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


def ensure_worker_subnets(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    resources: dict[str, object],
    worker_subnet_ids_configured: bool,
) -> None:
    if worker_subnet_ids_configured:
        resources["worker_subnet_ids"] = config.network.worker_subnet_ids
        return

    availability_zones = worker_subnet_availability_zones(config)
    cidrs = worker_subnet_cidrs(config, len(availability_zones))
    worker_subnet_ids = list(config.network.worker_subnet_ids)
    if not worker_subnet_ids:
        worker_subnet_ids = list(config.network.rds_subnet_ids)

    for index in range(len(worker_subnet_ids), len(availability_zones)):
        progress(
            f"creating worker subnet in {availability_zones[index]} with CIDR {cidrs[index]}"
        )
        worker_subnet_ids.append(create_subnet(client, config, cidrs[index], availability_zones[index]))

    config.network.worker_subnet_ids = worker_subnet_ids
    resources["worker_subnet_ids"] = worker_subnet_ids
    progress(f"worker subnets: {', '.join(worker_subnet_ids)}")


def worker_subnet_availability_zones(config: config_module.AwsBenchConfig) -> list[str]:
    availability_zones = non_empty_strings(config.network.rds_subnet_availability_zones)
    if availability_zones:
        return availability_zones
    return [config.aws.availability_zone]


def worker_subnet_cidrs(config: config_module.AwsBenchConfig, count: int) -> list[str]:
    cidrs = list(non_empty_strings(config.network.rds_subnet_cidrs))
    while len(cidrs) < count:
        cidrs.append(derived_worker_subnet_cidr(config, len(cidrs)))
    return cidrs


def derived_worker_subnet_cidr(config: config_module.AwsBenchConfig, index: int) -> str:
    network_prefix = config.network.vpc_cidr.split(".", maxsplit=2)[:2]
    if len(network_prefix) != 2:
        msg = f"cannot derive worker subnet CIDR from VPC CIDR {config.network.vpc_cidr}"
        raise ValueError(msg)
    return f"{network_prefix[0]}.{network_prefix[1]}.{index + 1}.0/24"


def ensure_worker_subnet_routing(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    resources: dict[str, object],
) -> None:
    route_table_id = resources.get("route_table_id")
    if not isinstance(route_table_id, str) or not route_table_id:
        return

    routed_subnet_id_list = routed_subnet_ids_from_resources(resources)
    routed_subnet_ids = set(routed_subnet_id_list)
    association_ids = route_table_associations_from_resources(resources)
    associated_subnet_ids = route_table_associated_subnet_ids_from_resources(resources)
    if not associated_subnet_ids:
        associated_subnet_ids = routed_subnet_id_list
    for subnet_id in non_empty_strings(config.network.worker_subnet_ids):
        if subnet_id in routed_subnet_ids:
            continue
        progress(f"associating worker subnet {subnet_id} with benchmark route table")
        enable_public_ip_mapping(client, subnet_id)
        association_id = associate_route_table(client, route_table_id, subnet_id)
        association_ids.append(association_id)
        associated_subnet_ids.append(subnet_id)
        routed_subnet_ids.add(subnet_id)
    resources["route_table_association_ids"] = association_ids
    resources["route_table_associated_subnet_ids"] = associated_subnet_ids


def routed_subnet_ids_from_resources(resources: dict[str, object]) -> list[str]:
    associated_subnet_ids = route_table_associated_subnet_ids_from_resources(resources)
    if associated_subnet_ids:
        return associated_subnet_ids

    subnet_ids = []
    subnet_id = resources.get("subnet_id")
    if isinstance(subnet_id, str) and subnet_id:
        subnet_ids.append(subnet_id)
    value = resources.get("rds_subnet_ids")
    if isinstance(value, list):
        subnet_ids.extend(
            item for item in value if isinstance(item, str) and item and item not in subnet_ids
        )
    return subnet_ids


def route_table_associations_from_resources(resources: dict[str, object]) -> list[str]:
    associations = resources.get("route_table_association_ids")
    if not isinstance(associations, list):
        return []
    return [association for association in associations if isinstance(association, str)]


def route_table_associated_subnet_ids_from_resources(resources: dict[str, object]) -> list[str]:
    subnet_ids = resources.get("route_table_associated_subnet_ids")
    if not isinstance(subnet_ids, list):
        return []
    return [subnet_id for subnet_id in subnet_ids if isinstance(subnet_id, str) and subnet_id]


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
    subnet_ids.extend(
        subnet_id
        for subnet_id in non_empty_strings(config.network.worker_subnet_ids)
        if subnet_id not in subnet_ids
    )
    route_table_association_ids = []
    route_table_associated_subnet_ids = []
    for subnet_id in subnet_ids:
        enable_public_ip_mapping(client, subnet_id)
        association_id = associate_route_table(client, route_table_id, subnet_id)
        route_table_association_ids.append(association_id)
        route_table_associated_subnet_ids.append(subnet_id)
    resources["route_table_association_ids"] = route_table_association_ids
    resources["route_table_associated_subnet_ids"] = route_table_associated_subnet_ids


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
) -> list[str]:
    instance_ids: list[str] = []
    progress("launching storage server instance")
    instance_ids.extend(
        launch_role(
            client,
            config,
            "storage-server",
            config.instances.server_type,
            1,
            config.network.subnet_id,
            config.aws.availability_zone,
        )
    )
    progress("launching controller instance")
    instance_ids.extend(
        launch_role(
            client,
            config,
            "controller",
            config.instances.controller_type,
            1,
            config.network.subnet_id,
            config.aws.availability_zone,
        )
    )
    progress("launching dedicated benchmark submitter instance")
    instance_ids.extend(
        launch_role(
            client,
            config,
            "benchmark-submitter",
            config.instances.submitter_type,
            1,
            config.network.subnet_id,
            config.aws.availability_zone,
        )
    )
    progress("launching dedicated benchmark scheduler instance")
    instance_ids.extend(
        launch_role(
            client,
            config,
            "benchmark-scheduler",
            config.instances.scheduler_type,
            1,
            config.network.subnet_id,
            config.aws.availability_zone,
        )
    )
    progress(f"launching {config.instances.worker_count} benchmark worker instances")
    instance_ids.extend(launch_worker_roles(client, config))
    progress(f"launched {len(instance_ids)} benchmark instance(s)")
    return instance_ids


def launch_role(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    role: str,
    instance_type: str,
    count: int,
    subnet_id: str,
    availability_zone: str | None,
) -> list[str]:
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
        subnet_id,
        "--tag-specifications",
        f"ResourceType=instance,Tags=[{{Key=RunId,Value={config.aws.run_id}}},{{Key=Role,Value={role}}}]",
        "--iam-instance-profile",
        f"Name={config.instances.iam_instance_profile}",
    ]
    placement = f"AvailabilityZone={availability_zone}" if availability_zone else ""
    if config.network.placement_group:
        placement += f"{',' if placement else ''}GroupName={config.network.placement_group}"
    if placement:
        command.extend(["--placement", placement])
    if config.instances.key_name is not None:
        command.extend(["--key-name", config.instances.key_name])
    data = client.run_json(command)
    if client.dry_run:
        subnet_suffix = subnet_id.replace("_", "-")
        return [f"i-dryrun-{role}-{subnet_suffix}-{index}" for index in range(count)]
    return instance_ids_from_run_instances(data)


def instance_ids_from_run_instances(data: object) -> list[str]:
    if not isinstance(data, dict):
        return []
    instances = data.get("Instances")
    if not isinstance(instances, list):
        return []
    instance_ids = []
    for instance in instances:
        if not isinstance(instance, dict):
            continue
        instance_id = instance.get("InstanceId")
        if isinstance(instance_id, str) and instance_id:
            instance_ids.append(instance_id)
    return instance_ids


def launch_worker_roles(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> list[str]:
    instance_ids: list[str] = []
    worker_subnet_ids = worker_subnet_ids_for_launch(config)
    if config.network.placement_group and len(worker_subnet_ids) > 1:
        msg = "network.placement_group cannot be used with workers spread across multiple subnets"
        raise ValueError(msg)
    for index, subnet_id in enumerate(worker_subnet_ids):
        count = remaining_worker_count(config.instances.worker_count, len(worker_subnet_ids), index)
        if count == 0:
            continue
        progress(f"launching {count} benchmark worker instance(s) in subnet {subnet_id}")
        instance_ids.extend(
            launch_role(
                client,
                config,
                "benchmark-worker",
                config.instances.worker_type,
                count,
                subnet_id,
                None,
            )
        )
    return instance_ids


def worker_subnet_ids_for_launch(config: config_module.AwsBenchConfig) -> list[str]:
    subnet_ids = non_empty_strings(config.network.worker_subnet_ids)
    if not subnet_ids:
        subnet_ids = non_empty_strings(config.network.rds_subnet_ids)
    if not subnet_ids:
        subnet_ids = non_empty_strings([config.network.subnet_id])
    return subnet_ids


def remaining_worker_count(total: int, subnet_count: int, index: int) -> int:
    base = total // subnet_count
    remainder = total % subnet_count
    return base + int(index < remainder)


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
    resources["results_s3_uri"] = f"s3://{bucket}/{result_s3_folder_name(config)}"


def ensure_results_upload_policy(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    resources: dict[str, object],
) -> None:
    results_s3_uri = resources.get("results_s3_uri")
    if not isinstance(results_s3_uri, str) or not results_s3_uri.startswith("s3://"):
        return
    bucket, prefix = split_s3_uri(results_s3_uri)
    object_arn = f"arn:aws:s3:::{bucket}/{prefix.rstrip('/')}/*" if prefix else f"arn:aws:s3:::{bucket}/*"
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                    "s3:PutObject",
                ],
                "Resource": object_arn,
            }
        ],
    }
    client.run(
        [
            "iam",
            "put-role-policy",
            "--role-name",
            config.instances.iam_instance_profile,
            "--policy-name",
            f"{config.aws.run_id}-results-upload",
            "--policy-document",
            json.dumps(policy),
        ]
    )


def split_s3_uri(s3_uri: str) -> tuple[str, str]:
    rest = s3_uri.removeprefix("s3://")
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix


def result_s3_folder_name(config: config_module.AwsBenchConfig) -> str:
    folder_name = config.results.s3_folder_name or config.aws.run_id
    folder_name = folder_name.strip("/")
    if not folder_name:
        msg = "results.s3_folder_name must not be empty after trimming slashes"
        raise ValueError(msg)
    return folder_name


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


def wait_for_ec2(client: aws_cli.AwsCli, instance_ids: object) -> None:
    if not isinstance(instance_ids, list):
        msg = "state resources.instance_ids must be a list"
        raise ValueError(msg)
    ids = [instance_id for instance_id in instance_ids if isinstance(instance_id, str)]
    if not ids:
        msg = "no launched EC2 instance IDs recorded for waiter"
        raise ValueError(msg)
    try:
        client.run(["ec2", "wait", "instance-running", "--instance-ids", *ids])
    except subprocess.CalledProcessError:
        describe_ec2_wait_failure(client, ids)
        raise


def describe_ec2_wait_failure(client: aws_cli.AwsCli, instance_ids: list[str]) -> None:
    try:
        data = client.run_json(["ec2", "describe-instances", "--instance-ids", *instance_ids])
    except subprocess.CalledProcessError:
        return
    for row in instance_state_rows(data):
        progress(
            "instance "
            f"{row['instance_id']} role={row['role']} type={row['instance_type']} "
            f"subnet={row['subnet_id']} state={row['state']} reason={row['reason']}"
        )


def instance_state_rows(data: object) -> list[dict[str, str]]:
    if not isinstance(data, dict):
        return []
    rows = []
    reservations = data.get("Reservations")
    if not isinstance(reservations, list):
        return rows
    for reservation in reservations:
        if not isinstance(reservation, dict):
            continue
        instances = reservation.get("Instances")
        if not isinstance(instances, list):
            continue
        for instance in instances:
            if isinstance(instance, dict):
                rows.append(instance_state_row(instance))
    return rows


def instance_state_row(instance: dict[str, object]) -> dict[str, str]:
    state = instance.get("State") if isinstance(instance.get("State"), dict) else {}
    reason = instance.get("StateReason") if isinstance(instance.get("StateReason"), dict) else {}
    return {
        "instance_id": string_value(instance.get("InstanceId")),
        "instance_type": string_value(instance.get("InstanceType")),
        "subnet_id": string_value(instance.get("SubnetId")),
        "state": string_value(state.get("Name") if isinstance(state, dict) else None),
        "reason": string_value(reason.get("Message") if isinstance(reason, dict) else None),
        "role": tag_value(instance, "Role"),
    }


def string_value(value: object) -> str:
    return value if isinstance(value, str) and value else "-"


def tag_value(instance: dict[str, object], key: str) -> str:
    tags = instance.get("Tags")
    if not isinstance(tags, list):
        return "-"
    for tag in tags:
        if not isinstance(tag, dict):
            continue
        if tag.get("Key") == key and isinstance(tag.get("Value"), str):
            return tag["Value"]
    return "-"


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
