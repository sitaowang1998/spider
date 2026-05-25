#!/usr/bin/env python3
"""One-command AWS benchmark provision/deploy/run/teardown wrapper."""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[4]
SCRIPT_DIR = ROOT / "tools/scripts/storage-api-bench/aws_setup"


def main() -> int:
    args = parse_args()
    state = args.state or ROOT / ".aws-bench" / args.run_id / "state.json"
    for step in full_run_steps(teardown=args.teardown):
        command = step_command(step, args.config, args.secret, state, args.data_dir, args.dry_run)
        result = subprocess.run(command, cwd=ROOT, check=False)
        if result.returncode != 0:
            return result.returncode
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--secret", type=pathlib.Path, default=ROOT / ".secret")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state", type=pathlib.Path)
    parser.add_argument("--data-dir", type=pathlib.Path, required=True)
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def full_run_steps(*, teardown: bool) -> list[str]:
    steps = ["provision", "deploy", "bootstrap-controller", "run-controller", "fetch-results"]
    if teardown:
        steps.append("teardown")
    return steps


def step_command(
    step: str,
    config: pathlib.Path,
    secret: pathlib.Path,
    state: pathlib.Path,
    data_dir: pathlib.Path,
    dry_run: bool,
) -> list[str]:
    script = {
        "provision": "provision.py",
        "deploy": "deploy.py",
        "bootstrap-controller": "bootstrap_controller.py",
        "run-controller": "run_controller.py",
        "fetch-results": "fetch_results.py",
        "teardown": "teardown.py",
    }[step]
    command = [
        sys.executable,
        str(SCRIPT_DIR / script),
        "--config",
        str(config),
        "--state",
        str(state),
    ]
    if step in {"provision", "deploy", "bootstrap-controller", "run-controller", "fetch-results", "teardown"}:
        command.extend(["--secret", str(secret)])
    if step == "fetch-results":
        command.extend(["--data-dir", str(data_dir)])
    if dry_run:
        command.append("--dry-run")
    return command


if __name__ == "__main__":
    sys.exit(main())
