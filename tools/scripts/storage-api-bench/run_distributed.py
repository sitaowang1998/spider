#!/usr/bin/env python3
"""Runs the distributed storage API benchmark controller."""

import argparse
import datetime
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = ROOT / "components/spider-storage-api-bench/config/default.toml"
DEFAULT_BINARY = ROOT / "target/release/spider-storage-api-bench"


def main() -> int:
    args = parse_args()
    log(f"benchmark_start protocol={args.protocol} workload={args.workload}")
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
    if args.scheduler_trace_dir is not None:
        cmd.extend(["--scheduler-trace-dir", str(args.scheduler_trace_dir)])
    if args.scheduler_trace_s3_prefix is not None:
        cmd.extend(["--scheduler-trace-s3-prefix", args.scheduler_trace_s3_prefix])
    result = subprocess.run(cmd, cwd=ROOT, check=False).returncode
    if result == 0:
        log(f"benchmark_complete protocol={args.protocol} workload={args.workload}")
    return result


def log(message: str) -> None:
    timestamp = (
        datetime.datetime.now(datetime.timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds")
    )
    print(f"[run_distributed] {timestamp} {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", choices=["rest", "grpc"], required=True)
    parser.add_argument("--workload", choices=["flat", "deep", "mixed"], required=True)
    parser.add_argument("--config", type=pathlib.Path, default=DEFAULT_CONFIG)
    parser.add_argument("--binary", type=pathlib.Path, default=DEFAULT_BINARY)
    parser.add_argument("--data-dir", type=pathlib.Path, default=ROOT / "data/distributed")
    parser.add_argument("--flat-percent", type=int)
    parser.add_argument("--scheduler-trace-dir", type=pathlib.Path)
    parser.add_argument("--scheduler-trace-s3-prefix")
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
