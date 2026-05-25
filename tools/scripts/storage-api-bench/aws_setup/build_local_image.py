#!/usr/bin/env python3
"""Builds a local Docker equivalent of the benchmark AMI contents."""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
import tempfile
import textwrap

import build_ami


ROOT = pathlib.Path(__file__).resolve().parents[4]
DEFAULT_TAG = "spider-storage-api-bench-node:local"


def main() -> int:
    args = parse_args()
    with tempfile.TemporaryDirectory() as directory:
        context = pathlib.Path(directory)
        source_archive = build_ami.create_source_archive(
            args.source_root,
            context / build_ami.SOURCE_ARCHIVE_NAME,
        )
        dockerfile = write_dockerfile(context / "Dockerfile", source_archive.name)
        result = subprocess.run(build_command(args.tag, context, dockerfile), cwd=ROOT, check=False)
        if result.returncode != 0:
            return result.returncode
    if args.localstack_ami_id is not None:
        subprocess.run(tag_localstack_ami_command(args.tag, args.localstack_ami_id), cwd=ROOT, check=True)
    if args.smoke:
        return subprocess.run(smoke_command(args.tag), cwd=ROOT, check=False).returncode
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=pathlib.Path, default=ROOT)
    parser.add_argument("--tag", default=DEFAULT_TAG)
    parser.add_argument(
        "--localstack-ami-id",
        help="Also tag the Docker image as localstack-ec2/spider-bench-node:<ami-id>.",
    )
    parser.add_argument("--smoke", action="store_true")
    return parser.parse_args()


def write_dockerfile(path: pathlib.Path, source_archive_name: str) -> pathlib.Path:
    path.write_text(dockerfile_text(source_archive_name), encoding="utf-8")
    return path


def dockerfile_text(source_archive_name: str) -> str:
    return textwrap.dedent(
        f"""
        FROM ubuntu:22.04
        ENV DEBIAN_FRONTEND=noninteractive
        RUN apt-get update \\
            && apt-get install -y --no-install-recommends \\
                awscli \\
                ca-certificates \\
                curl \\
                g++ \\
                gcc \\
                make \\
                mariadb-client \\
                pkg-config \\
                python3 \\
                libssl-dev \\
            && rm -rf /var/lib/apt/lists/*
        ADD {source_archive_name} /root/
        WORKDIR /root/spider
        RUN curl https://sh.rustup.rs -sSf | sh -s -- -y \\
            && /root/.cargo/bin/cargo build --release --package spider-storage-api-bench \\
            && test -x target/release/spider-storage-api-bench \\
            && python3 --version \\
            && aws --version \\
            && mariadb --version
        CMD ["target/release/spider-storage-api-bench", "--help"]
        """
    ).strip() + "\n"


def build_command(tag: str, context: pathlib.Path, dockerfile: pathlib.Path) -> list[str]:
    return ["docker", "build", "-t", tag, "-f", str(dockerfile), str(context)]


def tag_localstack_ami_command(tag: str, ami_id: str) -> list[str]:
    return ["docker", "tag", tag, f"localstack-ec2/spider-bench-node:{ami_id}"]


def smoke_command(tag: str) -> list[str]:
    return [
        "docker",
        "run",
        "--rm",
        tag,
        "target/release/spider-storage-api-bench",
        "--help",
    ]


if __name__ == "__main__":
    sys.exit(main())
