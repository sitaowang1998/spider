#!/usr/bin/env python3
"""Fetches controller benchmark results to a local directory."""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys

import aws_cli
import bootstrap_controller
import config as config_module
import controller_common
import env as env_module
import state as state_module


ROOT = pathlib.Path(__file__).resolve().parents[4]


def main() -> int:
    args = parse_args()
    config = config_module.load_config(args.config)
    state = state_module.load_state(args.state)
    s3_uri = config.results.s3_uri or lookup_results_s3_uri(state)
    if s3_uri is None:
        if args.dry_run:
            print("results.s3_uri is empty; skipping result fetch in dry-run")
            return 0
        msg = "results.s3_uri must be configured to fetch controller results"
        raise SystemExit(msg)
    secret_values = env_module.load_secret(args.secret)
    aws_env = env_module.build_aws_env(
        secret_values,
        region=config.aws.region,
        endpoint_url=config.aws.endpoint_url,
    )
    client = aws_cli.AwsCli(
        endpoint_url=config.aws.endpoint_url,
        env=aws_env,
        dry_run=args.dry_run,
    )
    controller_id = controller_common.discover_controller_instance_id(client, config.aws.run_id)
    upload_commands = build_result_upload_commands(
        remote_root=config.instances.remote_root,
        remote_workspace=bootstrap_controller.controller_workspace(config),
        remote_data_dir=config.results.remote_data_dir,
        s3_uri=s3_uri,
    )
    controller_common.send_controller_command(
        client,
        controller_instance_id=controller_id,
        commands=upload_commands,
        comment="upload spider benchmark results",
    )
    command = ["aws", "s3", "sync", s3_uri, str(args.data_dir)]
    if config.aws.endpoint_url is not None:
        command[1:1] = ["--endpoint-url", config.aws.endpoint_url]
    if args.dry_run:
        print(" ".join(command))
        return 0
    args.data_dir.mkdir(parents=True, exist_ok=True)
    return subprocess.run(command, cwd=ROOT, env=aws_env, check=False).returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--secret", type=pathlib.Path, default=ROOT / ".secret")
    parser.add_argument("--state", type=pathlib.Path, required=True)
    parser.add_argument("--data-dir", type=pathlib.Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def build_result_upload_commands(
    *,
    remote_root: str,
    remote_workspace: str,
    remote_data_dir: str,
    s3_uri: str,
) -> list[str]:
    workspace = controller_common.quote_path(remote_workspace)
    return [
        f"cd {controller_common.quote_path(remote_root)}",
        "set -a",
        f". {workspace}/.secret",
        "set +a",
        f"aws s3 sync {controller_common.quote_path(remote_data_dir)} {controller_common.quote_path(s3_uri)}",
    ]


def lookup_results_s3_uri(state: dict[str, object]) -> str | None:
    resources = state.get("resources", {})
    if isinstance(resources, dict):
        value = resources.get("results_s3_uri")
        if isinstance(value, str) and value:
            return value
    return None


if __name__ == "__main__":
    sys.exit(main())
