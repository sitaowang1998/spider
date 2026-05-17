#!/usr/bin/env python3
"""Runs one storage API benchmark client profile."""

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
        "--package",
        "spider-storage-api-bench",
        "--",
        "client",
        "--protocol",
        args.protocol,
        "--workload",
        args.workload,
        "--config",
        str(args.config),
    ]
    if args.target is not None:
        cmd.extend(["--target", args.target])
    if args.flat_percent is not None:
        cmd.extend(["--flat-percent", str(args.flat_percent)])
    if args.output is not None:
        cmd.extend(["--output", str(args.output)])
    return subprocess.run(cmd, cwd=ROOT, check=False).returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", choices=["rest", "grpc"], required=True)
    parser.add_argument("--workload", choices=["flat", "deep", "mixed"], required=True)
    parser.add_argument("--config", type=pathlib.Path, default=DEFAULT_CONFIG)
    parser.add_argument("--target")
    parser.add_argument("--flat-percent", type=int)
    parser.add_argument("--output", type=pathlib.Path)
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
