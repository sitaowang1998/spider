#!/usr/bin/env python3
"""Configuration loading for AWS storage benchmark automation."""

from __future__ import annotations

import dataclasses
import pathlib
import re
import typing


@dataclasses.dataclass
class AwsConfig:
    region: str = "us-east-1"
    availability_zone: str = "us-east-1a"
    run_id: str = "spider-bench"
    endpoint_url: str | None = None


@dataclasses.dataclass
class BenchmarkConfig:
    node_counts: list[int] = dataclasses.field(
        default_factory=lambda: [1, 2, 4, 8, 16, 64, 128]
    )
    protocols: list[str] = dataclasses.field(default_factory=lambda: ["grpc", "rest"])
    workloads: list[str] = dataclasses.field(default_factory=lambda: ["flat", "deep", "mixed"])
    jobs_per_worker: int = 10
    tasks_per_job: int = 1000
    payload_bytes: int = 128
    task_sleep_ms: int = 3
    submitter_count: int = 8
    worker_count: int = 16
    poll_batch: int = 64
    poll_wait_ms: int = 10
    flat_percent: int = 50


@dataclasses.dataclass
class InstanceConfig:
    worker_count: int = 128
    worker_type: str = "c7i.large"
    submitter_type: str = "c7i.xlarge"
    server_type: str = "c7i.16xlarge"
    controller_type: str = "c7i.large"
    ami_id: str = "ami-xxxxxxxx"
    key_name: str | None = None
    iam_instance_profile: str = "spider-bench-ssm"
    remote_root: str = "/opt/spider"
    remote_workspace_root: str = "/var/lib/spider-bench"


@dataclasses.dataclass
class DatabaseConfig:
    name: str = "spider_db"
    username: str = "spider_user"
    password: str = "spider_password"
    port: int = 3306
    instance_class: str = "db.r7i.2xlarge"
    allocated_storage: int = 200
    storage_type: str = "gp3"
    max_connections: int = 256
    ssl_mode: str = "preferred"


@dataclasses.dataclass
class NetworkConfig:
    vpc_id: str = ""
    vpc_cidr: str = "10.42.0.0/16"
    subnet_id: str = ""
    subnet_cidr: str = "10.42.1.0/24"
    rds_subnet_cidrs: list[str] = dataclasses.field(
        default_factory=lambda: ["10.42.1.0/24", "10.42.2.0/24"]
    )
    rds_subnet_availability_zones: list[str] = dataclasses.field(default_factory=list)
    rds_subnet_ids: list[str] = dataclasses.field(default_factory=list)
    worker_subnet_ids: list[str] = dataclasses.field(default_factory=list)
    security_group_id: str = ""
    rds_security_group_id: str = ""
    placement_group: str = ""
    placement_strategy: str = "cluster"


@dataclasses.dataclass
class ArtifactConfig:
    mode: str = "existing-repo"
    s3_uri: str | None = None
    base_ami_id: str = ""
    builder_instance_type: str = "c7i.large"
    builder_iam_instance_profile: str = "spider-bench-ami-builder"
    image_name_prefix: str = "spider-storage-api-bench"


@dataclasses.dataclass
class ResultsConfig:
    s3_uri: str | None = None
    s3_folder_name: str | None = None
    remote_data_dir: str = "/var/lib/spider-bench/data"


@dataclasses.dataclass
class AwsBenchConfig:
    aws: AwsConfig = dataclasses.field(default_factory=AwsConfig)
    benchmark: BenchmarkConfig = dataclasses.field(default_factory=BenchmarkConfig)
    instances: InstanceConfig = dataclasses.field(default_factory=InstanceConfig)
    database: DatabaseConfig = dataclasses.field(default_factory=DatabaseConfig)
    network: NetworkConfig = dataclasses.field(default_factory=NetworkConfig)
    artifact: ArtifactConfig = dataclasses.field(default_factory=ArtifactConfig)
    results: ResultsConfig = dataclasses.field(default_factory=ResultsConfig)


def load_config(path: pathlib.Path) -> AwsBenchConfig:
    data = parse_toml_subset(path.read_text(encoding="utf-8"))
    config = AwsBenchConfig()
    apply_section(config.aws, data.get("aws", {}))
    apply_section(config.benchmark, data.get("benchmark", {}))
    apply_section(config.instances, data.get("instances", {}))
    apply_section(config.database, data.get("database", {}))
    apply_section(config.network, data.get("network", {}))
    apply_section(config.artifact, data.get("artifact", {}))
    apply_section(config.results, data.get("results", {}))
    validate_config(config)
    return config


def validate_config(config: AwsBenchConfig) -> None:
    if max(config.benchmark.node_counts) > config.instances.worker_count:
        msg = "node_counts cannot exceed instances.worker_count"
        raise ValueError(msg)
    if sorted(config.benchmark.node_counts) != config.benchmark.node_counts:
        msg = "node_counts must be sorted in ascending order"
        raise ValueError(msg)
    if set(config.benchmark.protocols) - {"grpc", "rest"}:
        msg = "protocols must contain only grpc and rest"
        raise ValueError(msg)
    if set(config.benchmark.workloads) - {"flat", "deep", "mixed"}:
        msg = "workloads must contain only flat, deep, and mixed"
        raise ValueError(msg)
    if config.benchmark.task_sleep_ms < 0:
        msg = "task_sleep_ms must be greater than or equal to 0"
        raise ValueError(msg)
    if config.benchmark.poll_batch <= 0:
        msg = "poll_batch must be greater than 0"
        raise ValueError(msg)
    if config.benchmark.poll_wait_ms < 0:
        msg = "poll_wait_ms must be greater than or equal to 0"
        raise ValueError(msg)
    if not config.instances.remote_root.startswith("/"):
        msg = "instances.remote_root must be an absolute path, for example /opt/spider"
        raise ValueError(msg)
    if not config.instances.remote_workspace_root.startswith("/"):
        msg = "instances.remote_workspace_root must be an absolute path, for example /var/lib/spider-bench"
        raise ValueError(msg)
    if not config.results.remote_data_dir.startswith("/"):
        msg = "results.remote_data_dir must be an absolute path, for example /var/lib/spider-bench/data"
        raise ValueError(msg)
    if config.database.ssl_mode not in {
        "disabled",
        "preferred",
        "required",
        "verify_ca",
        "verify_identity",
    }:
        msg = "database.ssl_mode must be disabled, preferred, required, verify_ca, or verify_identity"
        raise ValueError(msg)
    if not config.network.placement_strategy:
        config.network.placement_strategy = "cluster"
    if config.network.placement_strategy not in {"cluster", "partition", "spread"}:
        msg = "network.placement_strategy must be cluster, partition, or spread"
        raise ValueError(msg)


def apply_section(target: object, values: dict[str, object]) -> None:
    fields = {field.name for field in dataclasses.fields(target)}
    for key, value in values.items():
        if key in fields:
            if value == "":
                value = None
            elif isinstance(value, list):
                value = [item for item in value if item != ""]
            setattr(target, key, value)


def parse_toml_subset(text: str) -> dict[str, dict[str, object]]:
    data: dict[str, dict[str, object]] = {}
    section: dict[str, object] | None = None
    for raw_line in text.splitlines():
        line = strip_comment(raw_line).strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            section_name = line[1:-1].strip()
            section = data.setdefault(section_name, {})
            continue
        if section is None or "=" not in line:
            continue
        key, value = line.split("=", 1)
        section[key.strip()] = parse_value(value.strip())
    return data


def strip_comment(line: str) -> str:
    in_string = False
    for index, char in enumerate(line):
        if char == '"':
            in_string = not in_string
        if char == "#" and not in_string:
            return line[:index]
    return line


def parse_value(value: str) -> typing.Any:
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [parse_value(part.strip()) for part in split_array(inner)]
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if value in {"true", "false"}:
        return value == "true"
    return value


def split_array(value: str) -> list[str]:
    parts = []
    current = []
    in_string = False
    for char in value:
        if char == '"':
            in_string = not in_string
        if char == "," and not in_string:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts
