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
import progress as progress_module


ROOT = pathlib.Path(__file__).resolve().parents[4]
DEFAULT_ARTIFACT_DIR = ROOT / ".aws-bench/artifacts"
RUNTIME_ARCHIVE_NAME = "spider-runtime.tar.gz"
BENCH_BINARY = pathlib.Path("target/release/spider-storage-api-bench")
RUNTIME_FILES = (
    pathlib.Path("components/spider-storage-api-bench/config/default.toml"),
    pathlib.Path("tools/scripts/storage-api-bench/aws_common.py"),
    pathlib.Path("tools/scripts/storage-api-bench/aws_discover.py"),
    pathlib.Path("tools/scripts/storage-api-bench/aws_run_matrix.py"),
    pathlib.Path("tools/scripts/storage-api-bench/aws_run_protocol.py"),
    pathlib.Path("tools/scripts/storage-api-bench/reset_database.py"),
    pathlib.Path("tools/scripts/storage-api-bench/run_agent.py"),
    pathlib.Path("tools/scripts/storage-api-bench/run_client.py"),
    pathlib.Path("tools/scripts/storage-api-bench/run_distributed.py"),
    pathlib.Path("tools/scripts/storage-api-bench/run_distributed_protocol.py"),
    pathlib.Path("tools/scripts/storage-api-bench/run_server.py"),
    pathlib.Path("tools/scripts/storage-api-bench/aws_setup/config.py"),
    pathlib.Path("tools/scripts/storage-api-bench/aws_setup/progress.py"),
    pathlib.Path("tools/scripts/storage-api-bench/aws_setup/run.py"),
    pathlib.Path("tools/scripts/storage-api-bench/aws_setup/state.py"),
)


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
    progress("building local benchmark binary in release mode")
    build_local_binary(source_root)
    progress(f"creating runtime archive from {source_root}")
    archive = create_runtime_archive(source_root, artifact_dir / RUNTIME_ARCHIVE_NAME)
    progress(f"runtime archive ready: {archive} ({archive.stat().st_size:,} bytes)")
    runtime_s3_uri = resolve_runtime_s3_uri(config)
    progress(f"ensuring artifact bucket for {runtime_s3_uri}")
    ensure_artifact_bucket(client, config.aws.region, runtime_s3_uri)
    progress(f"uploading runtime archive to {runtime_s3_uri}")
    client.run(["s3", "cp", str(archive), runtime_s3_uri])

    progress(f"ensuring builder instance profile {config.artifact.builder_iam_instance_profile}")
    ensure_builder_instance_profile(client, config)
    progress(
        f"launching AMI builder from {config.artifact.base_ami_id} "
        f"as {config.artifact.builder_instance_type}"
    )
    builder_instance_id = launch_builder_instance(client, config)
    progress(f"builder instance launched: {builder_instance_id}")
    try:
        progress(f"waiting for builder instance {builder_instance_id} to enter running state")
        wait_for_builder_instance(client, builder_instance_id)
        progress(f"sending build commands to builder instance {builder_instance_id}")
        run_builder_commands(client, config, builder_instance_id, runtime_s3_uri, localstack_smoke)
        progress(f"creating benchmark AMI from builder instance {builder_instance_id}")
        ami_id = create_image(client, config, builder_instance_id)
        progress(f"AMI creation started: {ami_id}; waiting until image is available")
        wait_for_image(client, ami_id)
        progress(f"AMI is available: {ami_id}")
        metadata = build_metadata(config, source_root, runtime_s3_uri, builder_instance_id, ami_id)
        progress(f"writing AMI metadata to {ami_state_path}")
        ami_state.save_ami_state(ami_state_path, metadata)
    finally:
        progress(f"terminating builder instance {builder_instance_id}")
        terminate_builder_instance(client, builder_instance_id)
        progress(f"terminate request sent for builder instance {builder_instance_id}")
    return metadata


def progress(message: str) -> None:
    progress_module.log("build_ami", message)


def build_local_binary(source_root: pathlib.Path) -> None:
    subprocess.run(
        ["cargo", "build", "--release", "--package", "spider-storage-api-bench"],
        cwd=source_root,
        check=True,
    )


def create_runtime_archive(source_root: pathlib.Path, archive_path: pathlib.Path) -> pathlib.Path:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz", compresslevel=1) as archive:
        add_runtime_file(archive, source_root, BENCH_BINARY)
        for relative_path in RUNTIME_FILES:
            add_runtime_file(archive, source_root, relative_path)
    return archive_path


def add_runtime_file(
    archive: tarfile.TarFile,
    source_root: pathlib.Path,
    relative_path: pathlib.Path,
) -> None:
    source_path = source_root / relative_path
    if not source_path.exists():
        msg = f"runtime artifact file does not exist: {source_path}"
        raise FileNotFoundError(msg)
    stat = source_path.stat()
    info = tarfile.TarInfo(str(pathlib.PurePosixPath("spider", *relative_path.parts)))
    info.mode = stat.st_mode
    info.mtime = int(stat.st_mtime)
    info.size = stat.st_size
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    with source_path.open("rb") as file:
        archive.addfile(info, file)


def resolve_runtime_s3_uri(config: config_module.AwsBenchConfig) -> str:
    if config.artifact.s3_uri is not None:
        return config.artifact.s3_uri
    bucket = f"spider-bench-artifacts-{sanitize_name(config.aws.run_id)}-{config.aws.region}"
    return f"s3://{bucket}/{config.aws.run_id}/{RUNTIME_ARCHIVE_NAME}"


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
    runtime_s3_uri: str,
    localstack_smoke: bool,
) -> None:
    commands = localstack_builder_commands(runtime_s3_uri) if localstack_smoke else builder_commands(config, runtime_s3_uri)
    progress(f"builder command count: {len(commands)}")
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
        progress(f"SSM command submitted: {command_id}; waiting for completion")
        client.run(["ssm", "wait", "command-executed", "--command-id", command_id, "--instance-id", instance_id])
        progress(f"SSM command completed: {command_id}")
    else:
        progress("SSM command submitted; no command id returned, skipping SSM wait")


def builder_commands(config: config_module.AwsBenchConfig, runtime_s3_uri: str) -> list[str]:
    remote_root = config.instances.remote_root.replace("~", "/root", 1)
    return [
        "set -eux",
        "sudo dnf install -y awscli mariadb105 openssl-libs python3 || sudo yum install -y awscli mariadb openssl-libs python3",
        f"rm -rf {remote_root}",
        f"mkdir -p {remote_root}",
        f"aws s3 cp {runtime_s3_uri} /tmp/{RUNTIME_ARCHIVE_NAME}",
        f"tar -xzf /tmp/{RUNTIME_ARCHIVE_NAME} -C {remote_root} --strip-components=1",
        f"chmod +x {remote_root}/target/release/spider-storage-api-bench",
        f"chmod +x {remote_root}/tools/scripts/storage-api-bench/*.py",
        f"chmod +x {remote_root}/tools/scripts/storage-api-bench/aws_setup/*.py",
        f"test -x {remote_root}/target/release/spider-storage-api-bench",
        f"{remote_root}/target/release/spider-storage-api-bench --help >/dev/null",
        f"python3 {remote_root}/tools/scripts/storage-api-bench/run_agent.py --help >/dev/null",
        f"python3 {remote_root}/tools/scripts/storage-api-bench/aws_setup/run.py --help >/dev/null",
        "python3 --version",
        "aws --version",
        "command -v mariadb || command -v mysql",
    ]


def localstack_builder_commands(runtime_s3_uri: str) -> list[str]:
    return [
        "python3 --version",
        f"echo {runtime_s3_uri}",
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
    runtime_s3_uri: str,
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
        "runtime_archive_s3_uri": runtime_s3_uri,
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
