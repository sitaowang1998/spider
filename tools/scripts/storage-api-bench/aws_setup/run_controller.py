#!/usr/bin/env python3
"""Runs the benchmark matrix on the AWS controller instance through SSM."""

from __future__ import annotations

import argparse
import pathlib
import sys

import aws_cli
import bootstrap_controller
import config as config_module
import controller_common
import env as env_module
import progress as progress_module


ROOT = pathlib.Path(__file__).resolve().parents[4]


def main() -> int:
    args = parse_args()
    progress("loading config and credentials")
    config = config_module.load_config(args.config)
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
    progress("discovering controller instance")
    controller_id = controller_common.discover_controller_instance_id(client, config.aws.run_id)
    progress(f"controller instance: {controller_id}")
    commands = build_controller_run_commands(
        remote_root=config.instances.remote_root,
        remote_workspace=bootstrap_controller.controller_workspace(config),
        remote_data_dir=config.results.remote_data_dir,
    )
    progress_log_path = f"{bootstrap_controller.controller_workspace(config)}/controller-run.log"
    command_id = controller_common.send_controller_command(
        client,
        controller_instance_id=controller_id,
        commands=commands,
        comment="run spider benchmark matrix on controller",
    )
    progress(f"benchmark controller command submitted: {command_id}")
    progress("waiting for benchmark controller command to finish")
    controller_common.wait_for_controller_command(
        client,
        command_id=command_id,
        controller_instance_id=controller_id,
        progress_log_path=progress_log_path,
    )
    progress("benchmark controller command complete")
    return 0


def progress(message: str) -> None:
    progress_module.log("run_controller", message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--secret", type=pathlib.Path, default=ROOT / ".secret")
    parser.add_argument("--state", type=pathlib.Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def build_controller_run_commands(
    *,
    remote_root: str,
    remote_workspace: str,
    remote_data_dir: str,
) -> list[str]:
    workspace = controller_common.quote_path(remote_workspace)
    progress_log = controller_common.quote_path(f"{remote_workspace}/controller-run.log")
    run_command = (
        "tools/scripts/storage-api-bench/aws_setup/run.py "
        f"--config {workspace}/config.toml "
        f"--state {workspace}/state.json "
        f"--data-dir {controller_common.quote_path(remote_data_dir)}"
    )
    return [
        f"cd {controller_common.quote_path(remote_root)}",
        "set -a",
        f". {workspace}/.secret",
        "set +a",
        f"rm -f {progress_log}",
        f"bash -lc {controller_common.quote_path(f'{run_command} 2>&1 | tee {progress_log}; exit ${{PIPESTATUS[0]}}')}",
    ]


if __name__ == "__main__":
    sys.exit(main())
