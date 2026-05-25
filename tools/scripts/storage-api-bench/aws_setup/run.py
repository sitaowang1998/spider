#!/usr/bin/env python3
"""Runs the provisioned AWS benchmark matrix."""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys

import config as config_module
import progress as progress_module
import state as state_module


ROOT = pathlib.Path(__file__).resolve().parents[4]
SCRIPT_DIR = ROOT / "tools/scripts/storage-api-bench"


def main() -> int:
    args = parse_args()
    progress("loading config and state")
    config = config_module.load_config(args.config)
    state = state_module.load_state(args.state)
    database_endpoint = args.database_endpoint or lookup_database_endpoint(state)
    progress(f"using database endpoint {database_endpoint}")
    command = build_matrix_command(
        config,
        database_endpoint=database_endpoint,
        data_dir=args.data_dir,
        workspace_root=args.workspace_root,
    )
    if args.dry_run:
        print(" ".join(command))
        return 0
    progress("starting benchmark matrix")
    result = subprocess.run(command, cwd=ROOT, check=False).returncode
    if result == 0:
        progress("benchmark matrix complete")
    else:
        progress(f"benchmark matrix failed with exit code {result}")
    return result


def progress(message: str) -> None:
    progress_module.log("run", message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--state", type=pathlib.Path, required=True)
    parser.add_argument("--data-dir", type=pathlib.Path, required=True)
    parser.add_argument("--workspace-root", type=pathlib.Path, default=ROOT / ".aws-bench")
    parser.add_argument("--database-endpoint")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def build_matrix_command(
    config: config_module.AwsBenchConfig,
    *,
    database_endpoint: str,
    data_dir: pathlib.Path,
    workspace_root: pathlib.Path,
) -> list[str]:
    command = [
        sys.executable,
        str(SCRIPT_DIR / "aws_run_matrix.py"),
        "--run-id",
        config.aws.run_id,
        "--node-counts",
        ",".join(str(value) for value in config.benchmark.node_counts),
        "--protocols",
        *config.benchmark.protocols,
        "--workloads",
        ",".join(config.benchmark.workloads),
        "--workspace-root",
        str(workspace_root),
        "--data-dir",
        str(data_dir),
        "--remote-root",
        config.instances.remote_root,
        "--remote-workspace-root",
        config.instances.remote_workspace_root,
        "--jobs-per-worker",
        str(config.benchmark.jobs_per_worker),
        "--tasks-per-job",
        str(config.benchmark.tasks_per_job),
        "--payload-bytes",
        str(config.benchmark.payload_bytes),
        "--submitter-count",
        str(config.benchmark.submitter_count),
        "--worker-count",
        str(config.benchmark.worker_count),
        "--flat-percent",
        str(config.benchmark.flat_percent),
        "--database-host",
        database_endpoint,
        "--database-port",
        str(config.database.port),
        "--database-name",
        config.database.name,
        "--database-username",
        config.database.username,
        "--database-password",
        config.database.password,
        "--database-max-connections",
        str(config.database.max_connections),
    ]
    return command


def lookup_database_endpoint(state: dict[str, object]) -> str:
    resources = state.get("resources", {})
    if isinstance(resources, dict):
        endpoint = resources.get("rds_endpoint")
        if isinstance(endpoint, str) and endpoint:
            return endpoint
    msg = "database endpoint missing from state; pass --database-endpoint"
    raise ValueError(msg)


if __name__ == "__main__":
    sys.exit(main())
