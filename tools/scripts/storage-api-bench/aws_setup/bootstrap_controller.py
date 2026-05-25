#!/usr/bin/env python3
"""Copies benchmark config, state, and AWS secret to the controller instance."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

import aws_cli
import config as config_module
import controller_common
import env as env_module
import progress as progress_module
import state as state_module


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
    commands = build_bootstrap_commands(
        remote_root=config.instances.remote_root,
        remote_workspace=controller_workspace(config),
        config_text=args.config.read_text(encoding="utf-8"),
        state_text=args.state.read_text(encoding="utf-8")
        if args.state.exists()
        else state_text_for(config),
        secret_text=args.secret.read_text(encoding="utf-8"),
    )
    command_id = controller_common.send_controller_command(
        client,
        controller_instance_id=controller_id,
        commands=commands,
        comment="bootstrap spider benchmark controller",
    )
    progress(f"bootstrap command submitted: {command_id}")
    progress("waiting for bootstrap command to finish")
    controller_common.wait_for_controller_command(
        client,
        command_id=command_id,
        controller_instance_id=controller_id,
    )
    progress("bootstrap command complete")
    return 0


def progress(message: str) -> None:
    progress_module.log("bootstrap_controller", message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--secret", type=pathlib.Path, default=ROOT / ".secret")
    parser.add_argument("--state", type=pathlib.Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def build_bootstrap_commands(
    *,
    remote_root: str,
    remote_workspace: str,
    config_text: str,
    state_text: str,
    secret_text: str,
) -> list[str]:
    config_marker = "SPIDER_BENCH_CONFIG"
    state_marker = "SPIDER_BENCH_STATE"
    secret_marker = "SPIDER_BENCH_SECRET"
    quoted_workspace = controller_common.quote_path(remote_workspace)
    return [
        f"cd {controller_common.quote_path(remote_root)}",
        f"mkdir -p {quoted_workspace}",
        (
            f"cat > {quoted_workspace}/config.toml <<'{config_marker}'\n"
            f"{controller_common.shell_heredoc(config_text, config_marker)}\n"
            f"{config_marker}"
        ),
        (
            f"cat > {quoted_workspace}/state.json <<'{state_marker}'\n"
            f"{controller_common.shell_heredoc(state_text, state_marker)}\n"
            f"{state_marker}"
        ),
        (
            f"cat > {quoted_workspace}/.secret <<'{secret_marker}'\n"
            f"{controller_common.shell_heredoc(secret_text, secret_marker)}\n"
            f"{secret_marker}"
        ),
        f"chmod 600 {quoted_workspace}/.secret",
        "test -x tools/scripts/storage-api-bench/aws_setup/run.py",
    ]


def controller_workspace(config: config_module.AwsBenchConfig) -> str:
    return f"{config.instances.remote_workspace_root}/{config.aws.run_id}/controller"


def state_text_for(config: config_module.AwsBenchConfig) -> str:
    state = state_module.default_state(config.aws.run_id)
    return json.dumps(state, indent=2, sort_keys=True)


if __name__ == "__main__":
    sys.exit(main())
