#!/usr/bin/env python3
"""Runs a storage API benchmark client agent."""

import argparse
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = ROOT / "components/spider-storage-api-bench/config/default.toml"


def main() -> int:
    args = parse_args()
    cmd = [
        "cargo",
        "run",
        "--release",
        "--package",
        "spider-storage-api-bench",
        "--",
        "agent",
        "--bind",
        args.bind,
        "--config",
        str(args.config),
        "--agent-id",
        args.agent_id,
    ]
    return subprocess.run(cmd, cwd=ROOT, check=False).returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="0.0.0.0:19091")
    parser.add_argument("--config", type=pathlib.Path, default=DEFAULT_CONFIG)
    parser.add_argument("--agent-id", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
