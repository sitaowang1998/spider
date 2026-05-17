#!/usr/bin/env python3
"""Runs matched REST and gRPC benchmark client profiles."""

import argparse
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = ROOT / "components/spider-storage-api-bench/config/default.toml"


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    for protocol in ("rest", "grpc"):
        cmd = [
            str(ROOT / "tools/scripts/storage-api-bench/run_client.py"),
            "--protocol",
            protocol,
            "--workload",
            args.workload,
            "--config",
            str(args.config),
            "--output",
            str(output_dir / f"{protocol}-{args.workload}.json"),
        ]
        if args.flat_percent is not None:
            cmd.extend(["--flat-percent", str(args.flat_percent)])
        result = subprocess.run(cmd, cwd=ROOT, check=False)
        if result.returncode != 0:
            return result.returncode
    print(f"REST and gRPC result JSON files written to {output_dir}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", choices=["flat", "deep", "mixed"], required=True)
    parser.add_argument("--config", type=pathlib.Path, default=DEFAULT_CONFIG)
    parser.add_argument("--flat-percent", type=int)
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=ROOT / "build/storage-api-bench/compare",
    )
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
