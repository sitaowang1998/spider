#!/usr/bin/env python3
"""Runs all distributed storage API benchmark workloads for one protocol."""

import argparse
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[3]
SCRIPT_DIR = ROOT / "tools/scripts/storage-api-bench"
DEFAULT_CONFIG = ROOT / "components/spider-storage-api-bench/config/default.toml"
WORKLOADS = ("flat", "deep", "mixed")


def main() -> int:
    args = parse_args()
    args.data_dir.mkdir(parents=True, exist_ok=True)
    for workload in WORKLOADS:
        cmd = [
            str(SCRIPT_DIR / "run_distributed.py"),
            "--protocol",
            args.protocol,
            "--workload",
            workload,
            "--config",
            str(args.config),
            "--data-dir",
            str(args.data_dir),
        ]
        if args.flat_percent is not None:
            cmd.extend(["--flat-percent", str(args.flat_percent)])
        result = subprocess.run(cmd, cwd=ROOT, check=False)
        if result.returncode != 0:
            return result.returncode
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", choices=["rest", "grpc"], required=True)
    parser.add_argument("--config", type=pathlib.Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--data-dir",
        type=pathlib.Path,
        default=ROOT / "data/distributed",
    )
    parser.add_argument("--flat-percent", type=int)
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
