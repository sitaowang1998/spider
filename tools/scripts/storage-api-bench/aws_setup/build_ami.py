#!/usr/bin/env python3
"""Builds a reusable AWS AMI for storage API benchmark nodes."""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import tarfile
import time

import ami_state
import aws_cli
import config as config_module
import env as env_module


ROOT = pathlib.Path(__file__).resolve().parents[4]
DEFAULT_ARTIFACT_DIR = ROOT / ".aws-bench/artifacts"
SOURCE_ARCHIVE_NAME = "spider-source.tar.gz"


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
    metadata = build_ami(
        config,
        client,
        source_root=args.source_root,
        artifact_dir=args.artifact_dir,
        ami_state_path=args.ami_state,
        localstack_smoke=args.localstack_smoke,
    )
    print(metadata.get("ami_id", ""))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--secret", type=pathlib.Path, default=ROOT / ".secret")
    parser.add_argument("--ami-state", type=pathlib.Path, default=ami_state.default_ami_state_path())
    parser.add_argument("--source-root", type=pathlib.Path, default=ROOT)
    parser.add_argument("--artifact-dir", type=pathlib.Path, default=DEFAULT_ARTIFACT_DIR)
    parser.add_argument(
        "--localstack-smoke",
        action="store_true",
        help="Use a short SSM command list suitable for LocalStack wiring tests.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def build_ami(
    config: config_module.AwsBenchConfig,
    client: aws_cli.AwsCli,
    *,
    source_root: pathlib.Path,
    artifact_dir: pathlib.Path,
    ami_state_path: pathlib.Path,
    localstack_smoke: bool,
) -> dict[str, object]:
    archive = create_source_archive(source_root, artifact_dir / SOURCE_ARCHIVE_NAME)
    source_s3_uri = resolve_source_s3_uri(config)
    ensure_artifact_bucket(client, config.aws.region, source_s3_uri)
    client.run(["s3", "cp", str(archive), source_s3_uri])

    ensure_builder_instance_profile(client, config)
    builder_instance_id = launch_builder_instance(client, config)
    try:
        wait_for_builder_instance(client, builder_instance_id)
        run_builder_commands(client, config, builder_instance_id, source_s3_uri, localstack_smoke)
        ami_id = create_image(client, config, builder_instance_id)
        wait_for_image(client, ami_id)
        metadata = build_metadata(config, source_root, source_s3_uri, builder_instance_id, ami_id)
        ami_state.save_ami_state(ami_state_path, metadata)
    finally:
        terminate_builder_instance(client, builder_instance_id)
    return metadata


def create_source_archive(source_root: pathlib.Path, archive_path: pathlib.Path) -> pathlib.Path:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(
            source_root,
            arcname="spider",
            filter=source_archive_filter,
        )
    return archive_path


def source_archive_filter(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
    parts = pathlib.PurePosixPath(info.name).parts
    excluded = {".git", ".aws-bench", "target"}
    if any(part in excluded for part in parts):
        return None
    if len(parts) > 1 and parts[1] == "data":
        return None
    return info


def resolve_source_s3_uri(config: config_module.AwsBenchConfig) -> str:
    if config.artifact.s3_uri is not None:
        return config.artifact.s3_uri
    bucket = f"spider-bench-artifacts-{sanitize_name(config.aws.run_id)}-{config.aws.region}"
    return f"s3://{bucket}/{config.aws.run_id}/{SOURCE_ARCHIVE_NAME}"


def sanitize_name(value: str) -> str:
    return "".join(char if char.isalnum() or char == "-" else "-" for char in value.lower())


def ensure_artifact_bucket(client: aws_cli.AwsCli, region: str, s3_uri: str) -> None:
    bucket = bucket_from_s3_uri(s3_uri)
    client.run_allow_failure(create_bucket_command(bucket, region))


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


def bucket_from_s3_uri(s3_uri: str) -> str:
    if not s3_uri.startswith("s3://"):
        msg = f"expected s3 URI, got {s3_uri}"
        raise ValueError(msg)
    return s3_uri.removeprefix("s3://").split("/", 1)[0]


def ensure_builder_instance_profile(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
) -> None:
    role_name = config.artifact.builder_iam_instance_profile
    assume_role_policy = (
        '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":'
        '{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    )
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
    for policy in (
        "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
        "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    ):
        client.run_allow_failure(["iam", "attach-role-policy", "--role-name", role_name, "--policy-arn", policy])
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


def launch_builder_instance(client: aws_cli.AwsCli, config: config_module.AwsBenchConfig) -> str:
    if not config.artifact.base_ami_id:
        msg = "artifact.base_ami_id is required to build an AMI"
        raise ValueError(msg)
    command = [
        "ec2",
        "run-instances",
        "--image-id",
        config.artifact.base_ami_id,
        "--instance-type",
        config.artifact.builder_instance_type,
        "--count",
        "1",
        "--tag-specifications",
        (
            "ResourceType=instance,Tags=["
            f"{{Key=RunId,Value={config.aws.run_id}}},"
            "{Key=Role,Value=ami-builder}]"
        ),
        "--iam-instance-profile",
        f"Name={config.artifact.builder_iam_instance_profile}",
    ]
    if config.network.security_group_id:
        command.extend(["--security-group-ids", config.network.security_group_id])
    if config.network.subnet_id:
        command.extend(["--subnet-id", config.network.subnet_id])
    if config.instances.key_name is not None:
        command.extend(["--key-name", config.instances.key_name])
    data = client.run_json(command)
    instances = data.get("Instances", []) if isinstance(data, dict) else []
    if instances:
        return instances[0].get("InstanceId", f"i-{config.aws.run_id}-ami-builder")
    return f"i-{config.aws.run_id}-ami-builder"


def wait_for_builder_instance(client: aws_cli.AwsCli, instance_id: str) -> None:
    client.run(["ec2", "wait", "instance-running", "--instance-ids", instance_id])


def run_builder_commands(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    instance_id: str,
    source_s3_uri: str,
    localstack_smoke: bool,
) -> None:
    commands = localstack_builder_commands(source_s3_uri) if localstack_smoke else builder_commands(config, source_s3_uri)
    data = client.run_json(
        [
            "ssm",
            "send-command",
            "--document-name",
            "AWS-RunShellScript",
            "--instance-ids",
            instance_id,
            "--comment",
            "build spider benchmark AMI",
            "--parameters",
            command_parameters(commands),
        ]
    )
    command_id = data.get("Command", {}).get("CommandId") if isinstance(data, dict) else None
    if command_id:
        client.run(["ssm", "wait", "command-executed", "--command-id", command_id, "--instance-id", instance_id])


def builder_commands(config: config_module.AwsBenchConfig, source_s3_uri: str) -> list[str]:
    remote_root = config.instances.remote_root.replace("~", "/root", 1)
    return [
        "set -eux",
        "sudo dnf install -y awscli git gcc gcc-c++ make pkgconfig openssl-devel mariadb105 || sudo yum install -y awscli git gcc gcc-c++ make pkgconfig openssl-devel mariadb",
        "test -x /root/.cargo/bin/cargo || curl https://sh.rustup.rs -sSf | sh -s -- -y",
        f"rm -rf {remote_root}",
        f"mkdir -p {remote_root}",
        f"aws s3 cp {source_s3_uri} /tmp/{SOURCE_ARCHIVE_NAME}",
        f"tar -xzf /tmp/{SOURCE_ARCHIVE_NAME} -C {remote_root} --strip-components=1",
        f"cd {remote_root} && /root/.cargo/bin/cargo build --release --package spider-storage-api-bench",
        f"test -x {remote_root}/target/release/spider-storage-api-bench",
        "python3 --version",
        "aws --version",
        "command -v mariadb || command -v mysql",
    ]


def localstack_builder_commands(source_s3_uri: str) -> list[str]:
    return [
        "python3 --version",
        f"echo {source_s3_uri}",
    ]


def command_parameters(commands: list[str]) -> str:
    return json.dumps({"commands": commands})


def create_image(
    client: aws_cli.AwsCli,
    config: config_module.AwsBenchConfig,
    instance_id: str,
) -> str:
    image_name = f"{config.artifact.image_name_prefix}-{sanitize_name(config.aws.run_id)}-{int(time.time())}"
    data = client.run_json(
        [
            "ec2",
            "create-image",
            "--instance-id",
            instance_id,
            "--name",
            image_name,
            "--description",
            f"Spider storage API benchmark image for {config.aws.run_id}",
            "--no-reboot",
            "--tag-specifications",
            (
                "ResourceType=image,Tags=["
                f"{{Key=RunId,Value={config.aws.run_id}}},"
                "{Key=Role,Value=benchmark-node}]"
            ),
        ]
    )
    if isinstance(data, dict) and data.get("ImageId"):
        return str(data["ImageId"])
    return f"ami-{sanitize_name(config.aws.run_id)}"


def wait_for_image(client: aws_cli.AwsCli, ami_id: str) -> None:
    client.run(["ec2", "wait", "image-available", "--image-ids", ami_id])


def terminate_builder_instance(client: aws_cli.AwsCli, instance_id: str) -> None:
    client.run(["ec2", "terminate-instances", "--instance-ids", instance_id])


def build_metadata(
    config: config_module.AwsBenchConfig,
    source_root: pathlib.Path,
    source_s3_uri: str,
    builder_instance_id: str,
    ami_id: str,
) -> dict[str, object]:
    return {
        "ami_id": ami_id,
        "base_ami_id": config.artifact.base_ami_id,
        "builder_instance_id": builder_instance_id,
        "created_at": ami_state.timestamp(),
        "dirty": git_dirty(source_root),
        "git_commit": git_commit(source_root),
        "image_name_prefix": config.artifact.image_name_prefix,
        "region": config.aws.region,
        "run_id": config.aws.run_id,
        "source_archive_s3_uri": source_s3_uri,
    }


def git_commit(source_root: pathlib.Path) -> str | None:
    return run_git(source_root, ["rev-parse", "HEAD"])


def git_dirty(source_root: pathlib.Path) -> bool:
    return bool(run_git(source_root, ["status", "--porcelain"]))


def run_git(source_root: pathlib.Path, args: list[str]) -> str | None:
    result = subprocess.run(
        ["git", *args],
        cwd=source_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()


if __name__ == "__main__":
    sys.exit(main())
