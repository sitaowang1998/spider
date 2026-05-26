#!/usr/bin/env python3
"""Runs one distributed AWS benchmark protocol using tagged EC2 instances and SSM."""

from __future__ import annotations

import argparse
import pathlib
import shlex
import subprocess
import sys

import aws_common
import aws_discover


def main() -> int:
    args = parse_args()
    workspace = args.workspace or aws_common.default_workspace(args.run_id, args.node_count)
    workspace.mkdir(parents=True, exist_ok=True)
    server, submitter, workers = aws_discover.discover(args.run_id, args.node_count)
    submitter_ip = submitter["private_ip"]
    submitter_instance_id = submitter["instance_id"]
    worker_ips = [worker["private_ip"] for worker in workers]
    worker_instance_ids = [worker["instance_id"] for worker in workers]
    server_instance_id = server["instance_id"]
    server_ip = server["private_ip"]

    write_discovery_files(workspace, server, submitter, workers)
    config = workspace / "config.toml"
    make_config(args, server_ip, submitter_ip, workspace / "worker_ips.txt", config)

    remote_config = pathlib.PurePosixPath(args.remote_workspace) / "config.toml"
    remote_log_dir = pathlib.PurePosixPath(args.remote_workspace) / "logs"
    config_text = config.read_text(encoding="utf-8")

    sync_config(
        [server_instance_id, submitter_instance_id, *worker_instance_ids],
        args.remote_root,
        remote_config,
        config_text,
    )
    start_agents(
        [submitter_instance_id],
        args.remote_root,
        remote_config,
        remote_log_dir,
        args.agent_port,
        "submitter",
        args.agent_start_timeout,
    )
    start_agents(
        worker_instance_ids,
        args.remote_root,
        remote_config,
        remote_log_dir,
        args.agent_port,
        "worker",
        args.agent_start_timeout,
    )
    aws_common.wait_for_agent_health(
        [submitter_ip, *worker_ips],
        args.agent_port,
        args.agent_start_timeout,
    )

    for workload in parse_workloads(args.workloads):
        print(
            (
                f"=== AWS benchmark workload start: nodes={args.node_count} "
                f"protocol={args.protocol} workload={workload} ==="
            ),
            flush=True,
        )
        if args.reset_database:
            result = subprocess.run(
                build_reset_database_command(config, args.database_reset_client_bin),
                cwd=aws_common.ROOT,
                check=False,
            )
            if result.returncode != 0:
                return result.returncode

        port = args.grpc_port if args.protocol == "grpc" else args.rest_port
        start_server(
            server_instance_id,
            args.protocol,
            args.remote_root,
            remote_config,
            remote_log_dir,
            port,
        )
        try:
            aws_common.wait_for_tcp(server_ip, port, args.server_start_timeout)
            result = subprocess.run(
                build_controller_command(
                    args.protocol,
                    config,
                    args.data_dir,
                    workload,
                ),
                cwd=aws_common.ROOT,
                check=False,
            )
        finally:
            stop_server(server_instance_id)
        if result.returncode != 0:
            return result.returncode
        print(
            (
                f"=== AWS benchmark workload complete: nodes={args.node_count} "
                f"protocol={args.protocol} workload={workload} ==="
            ),
            flush=True,
        )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--node-count", type=int, required=True)
    parser.add_argument("--protocol", choices=["grpc", "rest"], required=True)
    parser.add_argument("--workspace", type=pathlib.Path)
    parser.add_argument("--data-dir", type=pathlib.Path, required=True)
    parser.add_argument("--remote-root", default="/opt/spider")
    parser.add_argument("--remote-workspace")
    parser.add_argument("--jobs-per-worker", type=int, default=10)
    parser.add_argument("--tasks-per-job", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--submitter-count", type=int, default=8)
    parser.add_argument("--worker-count", type=int, default=16)
    parser.add_argument("--flat-percent", type=int, default=50)
    parser.add_argument("--workloads", default="flat,deep,mixed")
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
    parser.add_argument("--agent-start-timeout", type=int, default=300)
    parser.add_argument("--server-start-timeout", type=int, default=600)
    parser.set_defaults(reset_database=True)
    args = parser.parse_args()
    if args.remote_workspace is None:
        args.remote_workspace = f".aws-bench/{args.run_id}/{args.node_count}"
    return args


def build_controller_command(
    protocol: str,
    config: pathlib.Path,
    data_dir: pathlib.Path,
    workload: str,
) -> list[str]:
    return [
        sys.executable,
        str(aws_common.SCRIPT_DIR / "run_distributed_protocol.py"),
        "--protocol",
        protocol,
        "--config",
        str(config),
        "--data-dir",
        str(data_dir),
        "--workloads",
        workload,
    ]


def build_reset_database_command(
    config: pathlib.Path,
    database_reset_client_bin: str | None,
) -> list[str]:
    command = [
        sys.executable,
        str(aws_common.SCRIPT_DIR / "reset_database.py"),
        "--config",
        str(config),
        "--yes",
    ]
    if database_reset_client_bin is not None:
        command.extend(["--client-bin", database_reset_client_bin])
    return command


def parse_workloads(value: str) -> list[str]:
    workloads = [workload for workload in value.split(",") if workload]
    invalid = sorted(set(workloads) - {"flat", "deep", "mixed"})
    if invalid:
        msg = f"invalid workloads: {', '.join(invalid)}"
        raise ValueError(msg)
    return workloads


def write_discovery_files(
    workspace: pathlib.Path,
    server: dict[str, str],
    submitter: dict[str, str],
    workers: list[dict[str, str]],
) -> None:
    aws_common.write_lines(workspace / "server_ip.txt", [server["private_ip"]])
    aws_common.write_lines(workspace / "server_instance_id.txt", [server["instance_id"]])
    aws_common.write_lines(workspace / "submitter_ip.txt", [submitter["private_ip"]])
    aws_common.write_lines(workspace / "submitter_instance_id.txt", [submitter["instance_id"]])
    aws_common.write_lines(workspace / "worker_ips.txt", [worker["private_ip"] for worker in workers])
    aws_common.write_lines(
        workspace / "worker_instance_ids.txt",
        [worker["instance_id"] for worker in workers],
    )


def make_config(
    args: argparse.Namespace,
    server_ip: str,
    submitter_ip: str,
    worker_ips_path: pathlib.Path,
    output: pathlib.Path,
) -> None:
    subprocess.run(
        [
            sys.executable,
            str(aws_common.SCRIPT_DIR / "aws_make_config.py"),
            "--server-private-ip",
            server_ip,
            "--submitter-ip",
            submitter_ip,
            "--worker-ips",
            str(worker_ips_path),
            "--output",
            str(output),
            "--jobs-per-worker",
            str(args.jobs_per_worker),
            "--tasks-per-job",
            str(args.tasks_per_job),
            "--payload-bytes",
            str(args.payload_bytes),
            "--submitter-count",
            str(args.submitter_count),
            "--worker-count",
            str(args.worker_count),
            "--flat-percent",
            str(args.flat_percent),
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
        ],
        cwd=aws_common.ROOT,
        check=True,
    )


def sync_config(
    instance_ids: list[str],
    remote_root: str,
    remote_config: pathlib.PurePosixPath,
    config_text: str,
) -> None:
    quoted_config = shell_heredoc(config_text)
    commands = [
        f"cd {shlex.quote(remote_root)}",
        f"mkdir -p {shlex.quote(str(remote_config.parent))}",
        f"cat > {shlex.quote(str(remote_config))} <<'SPIDER_BENCH_CONFIG'\n{quoted_config}\nSPIDER_BENCH_CONFIG",
    ]
    aws_common.send_shell_command(
        instance_ids,
        commands,
        comment="sync spider benchmark config",
        wait=True,
    )


def start_agents(
    instance_ids: list[str],
    remote_root: str,
    remote_config: pathlib.PurePosixPath,
    remote_log_dir: pathlib.PurePosixPath,
    agent_port: int,
    role: str,
    timeout_s: int,
) -> None:
    commands = [
        f"cd {shlex.quote(remote_root)}",
        f"mkdir -p {shlex.quote(str(remote_log_dir))}",
        "PRIVATE_IP=$(hostname -I | awk '{print $1}')",
        f'AGENT_ID="{role}-${{PRIVATE_IP//./-}}"',
        f'LOG_PATH="{remote_log_dir}/agent-${{AGENT_ID}}.log"',
        'pkill -f "spider-storage-api-bench agent .*--agent-id ${AGENT_ID}" || true',
        'pkill -f "run_agent.py .*--agent-id ${AGENT_ID}" || true',
        'for _ in $(seq 1 30); do pgrep -f "spider-storage-api-bench agent .*--agent-id ${AGENT_ID}" >/dev/null || break; sleep 1; done',
        (
            f"nohup tools/scripts/storage-api-bench/run_agent.py "
            f"--config {shlex.quote(str(remote_config))} "
            '"--agent-id" "${AGENT_ID}" '
            f"--role {role} "
            f"--bind 0.0.0.0:{agent_port} "
            '> "${LOG_PATH}" 2>&1 &'
        ),
        (
            'for _ in $(seq 1 30); do '
            'if grep -q "^Error:" "${LOG_PATH}"; then '
            'cat "${LOG_PATH}"; exit 1; '
            "fi; "
            'pgrep -f "spider-storage-api-bench agent .*--agent-id ${AGENT_ID}" >/dev/null && exit 0; '
            "sleep 1; "
            "done; "
            'cat "${LOG_PATH}"; exit 1'
        ),
    ]
    aws_common.send_shell_command(
        instance_ids,
        commands,
        comment=f"start spider benchmark agents timeout={timeout_s}",
        wait=True,
    )


def start_server(
    instance_id: str,
    protocol: str,
    remote_root: str,
    remote_config: pathlib.PurePosixPath,
    remote_log_dir: pathlib.PurePosixPath,
    port: int,
) -> None:
    commands = [
        f"cd {shlex.quote(remote_root)}",
        f"mkdir -p {shlex.quote(str(remote_log_dir))}",
        'pkill -f "run_server.py .*--protocol" || true',
        (
            f"nohup tools/scripts/storage-api-bench/run_server.py "
            f"--protocol {protocol} "
            f"--config {shlex.quote(str(remote_config))} "
            f"--bind 0.0.0.0:{port} "
            "--external-database "
            f"> {shlex.quote(str(remote_log_dir / f'server-{protocol}.log'))} 2>&1 &"
        ),
    ]
    aws_common.send_shell_command(
        [instance_id],
        commands,
        comment=f"start spider benchmark {protocol} server",
        wait=True,
    )


def stop_server(instance_id: str) -> None:
    aws_common.send_shell_command(
        [instance_id],
        ['pkill -f "run_server.py .*--protocol" || true'],
        comment="stop spider benchmark server",
        wait=True,
    )


def shell_heredoc(value: str) -> str:
    return value.replace("SPIDER_BENCH_CONFIG", "SPIDER_BENCH_CONFIG_REPLACED")


if __name__ == "__main__":
    sys.exit(main())
