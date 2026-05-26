#!/usr/bin/env python3
"""Runs the distributed storage API benchmark controller."""

import argparse
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = ROOT / "components/spider-storage-api-bench/config/default.toml"
DEFAULT_BINARY = ROOT / "target/release/spider-storage-api-bench"


def main() -> int:
    args = parse_args()
    print(
        f"=== distributed benchmark start: protocol={args.protocol} workload={args.workload} ===",
        flush=True,
    )
    cmd = [
        str(args.binary),
        "controller",
        "--protocol",
        args.protocol,
        "--workload",
        args.workload,
        "--config",
        str(args.config),
        "--data-dir",
        str(args.data_dir),
    ]
    if args.flat_percent is not None:
        cmd.extend(["--flat-percent", str(args.flat_percent)])
    result = subprocess.run(cmd, cwd=ROOT, check=False).returncode
    if result == 0:
        print(
            f"=== distributed benchmark complete: protocol={args.protocol} workload={args.workload} ===",
            flush=True,
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", choices=["rest", "grpc"], required=True)
    parser.add_argument("--workload", choices=["flat", "deep", "mixed"], required=True)
    parser.add_argument("--config", type=pathlib.Path, default=DEFAULT_CONFIG)
    parser.add_argument("--binary", type=pathlib.Path, default=DEFAULT_BINARY)
    parser.add_argument("--data-dir", type=pathlib.Path, default=ROOT / "data/distributed")
    parser.add_argument("--flat-percent", type=int)
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
