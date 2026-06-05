#!/usr/bin/env python3
"""Runs AWS distributed benchmark protocols and node counts sequentially."""

from __future__ import annotations

import argparse
import datetime
import pathlib
import subprocess
import sys

import aws_common


def main() -> int:
    args = parse_args()
    for node_count in args.node_counts:
        for protocol in args.protocols:
            data_dir = args.data_dir / f"aws-{node_count}" / protocol
            workspace = args.workspace_root / args.run_id / str(node_count)
            command = [
                sys.executable,
                str(aws_common.SCRIPT_DIR / "aws_run_protocol.py"),
                "--run-id",
                args.run_id,
                "--node-count",
                str(node_count),
                "--protocol",
                protocol,
                "--workspace",
                str(workspace),
                "--data-dir",
                str(data_dir),
                "--remote-root",
                args.remote_root,
                "--remote-workspace",
                f"{args.remote_workspace_root}/{args.run_id}/{node_count}",
                "--jobs-per-worker",
                str(args.jobs_per_worker),
                "--tasks-per-job",
                str(args.tasks_per_job),
                "--payload-bytes",
                str(args.payload_bytes),
                "--task-sleep-ms",
                str(args.task_sleep_ms),
                "--submitter-count",
                str(args.submitter_count),
                "--worker-count",
                str(args.worker_count),
                "--worker-poll-wait-ms",
                str(args.worker_poll_wait_ms),
                "--worker-empty-poll-sleep-min-ms",
                str(args.worker_empty_poll_sleep_min_ms),
                "--worker-empty-poll-sleep-max-ms",
                str(args.worker_empty_poll_sleep_max_ms),
                "--job-poll-wait-ms",
                str(args.job_poll_wait_ms),
                "--scheduler-active-job-pool-capacity",
                str(args.scheduler_active_job_pool_capacity),
                "--scheduler-commit-ready-task-capacity",
                str(args.scheduler_commit_ready_task_capacity),
                "--scheduler-cleanup-ready-task-capacity",
                str(args.scheduler_cleanup_ready_task_capacity),
                "--scheduler-max-serving-requests",
                str(args.scheduler_max_serving_requests),
                "--scheduler-tick-interval-ms",
                str(args.scheduler_tick_interval_ms),
                "--scheduler-storage-poll-wait-ms",
                str(args.scheduler_storage_poll_wait_ms),
                "--flat-percent",
                str(args.flat_percent),
                "--workloads",
                ",".join(args.workloads),
                "--rest-port",
                str(args.rest_port),
                "--grpc-port",
                str(args.grpc_port),
                "--agent-port",
                str(args.agent_port),
                "--database-host",
                args.database_host,
                "--database-port",
                str(args.database_port),
                "--database-name",
                args.database_name,
                "--database-username",
                args.database_username,
                "--database-password",
                args.database_password,
                "--database-max-connections",
                str(args.database_max_connections),
                "--database-ssl-mode",
                args.database_ssl_mode,
            ]
            if not args.reset_database:
                command.append("--no-reset-database")
            if args.database_reset_client_bin is not None:
                command.extend(["--database-reset-client-bin", args.database_reset_client_bin])
            log(f"benchmark_start nodes={node_count} protocol={protocol}")
            result = subprocess.run(command, cwd=aws_common.ROOT, check=False)
            if result.returncode != 0:
                return result.returncode
    return 0


def log(message: str) -> None:
    timestamp = (
        datetime.datetime.now(datetime.timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds")
    )
    print(f"[aws_run_matrix] {timestamp} {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--node-counts", type=parse_csv_ints, default=[1, 2, 4, 8, 16, 32, 64, 128])
    parser.add_argument("--protocols", nargs="+", choices=["grpc", "rest"], default=["grpc", "rest"])
    parser.add_argument("--workspace-root", type=pathlib.Path, default=aws_common.ROOT / ".aws-bench")
    parser.add_argument("--data-dir", type=pathlib.Path, default=aws_common.ROOT / "data")
    parser.add_argument("--remote-root", default="~/spider")
    parser.add_argument("--remote-workspace-root", default=".aws-bench")
    parser.add_argument("--jobs-per-worker", type=int, default=10)
    parser.add_argument("--tasks-per-job", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--task-sleep-ms", type=int, default=3)
    parser.add_argument("--submitter-count", type=int, default=8)
    parser.add_argument("--worker-count", type=int, default=16)
    parser.add_argument("--worker-poll-wait-ms", type=int, default=10)
    parser.add_argument("--worker-empty-poll-sleep-min-ms", type=int, default=1)
    parser.add_argument("--worker-empty-poll-sleep-max-ms", type=int, default=100)
    parser.add_argument("--job-poll-wait-ms", type=int, default=10)
    parser.add_argument("--scheduler-active-job-pool-capacity", type=int, default=1024)
    parser.add_argument("--scheduler-commit-ready-task-capacity", type=int, default=1024)
    parser.add_argument("--scheduler-cleanup-ready-task-capacity", type=int, default=1024)
    parser.add_argument("--scheduler-max-serving-requests", type=int, default=1024)
    parser.add_argument("--scheduler-tick-interval-ms", type=int, default=10)
    parser.add_argument("--scheduler-storage-poll-wait-ms", type=int, default=20)
    parser.add_argument("--flat-percent", type=int, default=50)
    parser.add_argument("--workloads", type=parse_csv_strings, default=["flat", "deep", "mixed"])
    parser.add_argument("--rest-port", type=int, default=8091)
    parser.add_argument("--grpc-port", type=int, default=50051)
    parser.add_argument("--agent-port", type=int, default=19091)
    parser.add_argument("--database-host", required=True)
    parser.add_argument("--database-port", type=int, default=3306)
    parser.add_argument("--database-name", default="spider-db")
    parser.add_argument("--database-username", default="spider-user")
    parser.add_argument("--database-password", default="spider-password")
    parser.add_argument("--database-max-connections", type=int, default=256)
    parser.add_argument(
        "--database-ssl-mode",
        choices=["disabled", "preferred", "required", "verify_ca", "verify_identity"],
        default="preferred",
    )
    parser.add_argument(
        "--no-reset-database",
        dest="reset_database",
        action="store_false",
        help="Do not reset database tables before each workload.",
    )
    parser.add_argument(
        "--database-reset-client-bin",
        help="MariaDB/MySQL client binary on the controller.",
    )
    parser.set_defaults(reset_database=True)
    return parser.parse_args()


def parse_csv_ints(value: str) -> list[int]:
    return [int(item) for item in value.split(",") if item]


def parse_csv_strings(value: str) -> list[str]:
    return [item for item in value.split(",") if item]


if __name__ == "__main__":
    sys.exit(main())
