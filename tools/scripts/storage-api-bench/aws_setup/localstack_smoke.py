#!/usr/bin/env python3
"""Smoke checks the AWS automation wiring against LocalStack or dry-run mode."""

from __future__ import annotations

import argparse
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.error
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parents[4]
SCRIPT_DIR = ROOT / "tools/scripts/storage-api-bench/aws_setup"
DEFAULT_LOCALSTACK_IMAGE = "localstack/localstack:4.8.1"


def main() -> int:
    args = parse_args()
    started_localstack = False
    try:
        if args.with_localstack:
            start_localstack(args.container_name, args.localstack_image)
            started_localstack = True
            wait_for_localstack(args.endpoint_url, args.start_timeout_sec)
            args.check_endpoint = True
        if args.check_endpoint:
            result = check_localstack_endpoint(args.endpoint_url)
            if result.returncode != 0:
                return result.returncode
        return run_dry_run_smoke(args.endpoint_url)
    finally:
        if started_localstack:
            stop_localstack(args.container_name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint-url", default="http://localhost:4566")
    parser.add_argument("--container-name", default="spider-bench-localstack")
    parser.add_argument(
        "--localstack-image",
        default=DEFAULT_LOCALSTACK_IMAGE,
        help=(
            "Docker image used by --with-localstack. The default pins an older "
            "community image instead of latest, which now requires a LocalStack auth token."
        ),
    )
    parser.add_argument("--start-timeout-sec", type=int, default=120)
    parser.add_argument(
        "--with-localstack",
        action="store_true",
        help="Start a LocalStack Docker container before the smoke test and remove it afterward.",
    )
    parser.add_argument(
        "--check-endpoint",
        action="store_true",
        help="Check that the LocalStack AWS endpoint is reachable before dry-run checks.",
    )
    return parser.parse_args()


def run_dry_run_smoke(endpoint_url: str) -> int:
    with tempfile.TemporaryDirectory() as directory:
        temp_dir = pathlib.Path(directory)
        config = temp_dir / "localstack.toml"
        secret = temp_dir / ".secret"
        state = temp_dir / "state.json"
        data_dir = temp_dir / "data"
        write_localstack_config(config, endpoint_url)
        write_localstack_secret(secret)
        commands = dry_run_commands(config, secret, state, data_dir)
        for command in commands:
            result = subprocess.run(command, cwd=ROOT, check=False)
            if result.returncode != 0:
                return result.returncode
    return 0


def dry_run_commands(
    config: pathlib.Path,
    secret: pathlib.Path,
    state: pathlib.Path,
    data_dir: pathlib.Path,
) -> list[list[str]]:
    return [
        [
            sys.executable,
            str(SCRIPT_DIR / "provision.py"),
            "--config",
            str(config),
            "--secret",
            str(secret),
            "--state",
            str(state),
            "--dry-run",
        ],
        [
            sys.executable,
            str(SCRIPT_DIR / "deploy.py"),
            "--config",
            str(config),
            "--secret",
            str(secret),
            "--state",
            str(state),
            "--dry-run",
        ],
        [
            sys.executable,
            str(SCRIPT_DIR / "bootstrap_controller.py"),
            "--config",
            str(config),
            "--secret",
            str(secret),
            "--state",
            str(state),
            "--dry-run",
        ],
        [
            sys.executable,
            str(SCRIPT_DIR / "run_controller.py"),
            "--config",
            str(config),
            "--secret",
            str(secret),
            "--state",
            str(state),
            "--dry-run",
        ],
        [
            sys.executable,
            str(SCRIPT_DIR / "fetch_results.py"),
            "--config",
            str(config),
            "--secret",
            str(secret),
            "--state",
            str(state),
            "--data-dir",
            str(data_dir),
            "--dry-run",
        ],
        [
            sys.executable,
            str(SCRIPT_DIR / "teardown.py"),
            "--config",
            str(config),
            "--secret",
            str(secret),
            "--state",
            str(state),
            "--dry-run",
        ],
    ]


def start_localstack(container_name: str, image: str) -> None:
    subprocess.run(build_localstack_stop_command(container_name), cwd=ROOT, check=False)
    subprocess.run(build_localstack_start_command(container_name, image), cwd=ROOT, check=True)


def stop_localstack(container_name: str) -> None:
    subprocess.run(build_localstack_stop_command(container_name), cwd=ROOT, check=False)


def build_localstack_start_command(container_name: str, image: str) -> list[str]:
    return [
        "docker",
        "run",
        "-d",
        "--name",
        container_name,
        "-p",
        "4566:4566",
        "-e",
        "SERVICES=ec2,iam,s3,ssm,sts",
        image,
    ]


def build_localstack_stop_command(container_name: str) -> list[str]:
    return ["docker", "rm", "-f", container_name]


def wait_for_localstack(endpoint_url: str, timeout_sec: int) -> None:
    deadline = time.monotonic() + timeout_sec
    health_url = endpoint_url.rstrip("/") + "/_localstack/health"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(2)
    raise TimeoutError(f"timed out waiting for LocalStack at {health_url}")


def check_localstack_endpoint(endpoint_url: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "aws",
            "--endpoint-url",
            endpoint_url,
            "sts",
            "get-caller-identity",
        ],
        env=localstack_env(),
        cwd=ROOT,
        check=False,
    )


def write_localstack_config(path: pathlib.Path, endpoint_url: str) -> None:
    path.write_text(
        textwrap.dedent(
            f"""
            [aws]
            region = "us-east-1"
            availability_zone = "us-east-1a"
            run_id = "localstack-smoke"
            endpoint_url = "{endpoint_url}"

            [benchmark]
            node_counts = [1, 2]
            protocols = ["grpc", "rest"]
            workloads = ["flat", "deep", "mixed"]
            jobs_per_worker = 1
            tasks_per_job = 10

            [instances]
            worker_count = 2
            ami_id = "ami-localstack"

            [database]
            name = "spider_db"
            username = "spider_user"
            password = "spider_password"

            [results]
            remote_data_dir = "data/localstack-smoke"

            [network]
            vpc_id = "vpc-localstack"
            subnet_id = "subnet-localstack-a"
            rds_subnet_ids = ["subnet-localstack-a", "subnet-localstack-b"]
            security_group_id = "sg-localstack"
            rds_security_group_id = "sg-localstack-rds"
            """
        ),
        encoding="utf-8",
    )


def write_localstack_secret(path: pathlib.Path) -> None:
    path.write_text(
        "AWS_ACCESS_KEY_ID=test\nAWS_SECRET_ACCESS_KEY=test\n",
        encoding="utf-8",
    )


def localstack_env() -> dict[str, str]:
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = "test"
    env["AWS_SECRET_ACCESS_KEY"] = "test"
    env["AWS_DEFAULT_REGION"] = "us-east-1"
    env["AWS_REGION"] = "us-east-1"
    return env


if __name__ == "__main__":
    sys.exit(main())
