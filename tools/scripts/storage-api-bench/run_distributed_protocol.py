#!/usr/bin/env python3
"""Runs all distributed storage API benchmark workloads for one protocol."""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[3]
SCRIPT_DIR = ROOT / "tools/scripts/storage-api-bench"
DEFAULT_CONFIG = ROOT / "components/spider-storage-api-bench/config/default.toml"
DEFAULT_WORKLOADS = ("flat", "deep", "mixed")


def main() -> int:
    args = parse_args()
    args.data_dir.mkdir(parents=True, exist_ok=True)
    for workload in args.workloads:
        if args.reset_database:
            result = subprocess.run(
                build_reset_database_command(args.config, args.database_reset_client_bin),
                cwd=ROOT,
                check=False,
            )
            if result.returncode != 0:
                return result.returncode
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
    parser.add_argument("--workloads", type=parse_workloads, default=list(DEFAULT_WORKLOADS))
    parser.add_argument(
        "--reset-database",
        action="store_true",
        help="Reset benchmark database tables before each workload.",
    )
    parser.add_argument(
        "--database-reset-client-bin",
        help="MariaDB/MySQL client binary forwarded to reset_database.py.",
    )
    return parser.parse_args()


def parse_workloads(value: str) -> list[str]:
    workloads = [item for item in value.split(",") if item]
    invalid = sorted(set(workloads) - set(DEFAULT_WORKLOADS))
    if invalid:
        raise argparse.ArgumentTypeError(f"invalid workloads: {', '.join(invalid)}")
    return workloads


def build_reset_database_command(config: pathlib.Path, client_bin: str | None) -> list[str]:
    command = [
        str(SCRIPT_DIR / "reset_database.py"),
        "--config",
        str(config),
        "--yes",
    ]
    if client_bin is not None:
        command.extend(["--client-bin", client_bin])
    return command


if __name__ == "__main__":
    sys.exit(main())
