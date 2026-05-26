#!/usr/bin/env python3
"""Shared helpers for AWS storage API benchmark scripts."""

from __future__ import annotations

import json
import pathlib
import socket
import subprocess
import time
import urllib.error
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parents[3]
SCRIPT_DIR = ROOT / "tools/scripts/storage-api-bench"


def default_workspace(run_id: str, node_count: int) -> pathlib.Path:
    return ROOT / ".aws-bench" / run_id / str(node_count)


def run_aws(args: list[str]) -> object:
    command = ["aws", *args, "--output", "json"]
    result = subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def run_aws_text(args: list[str]) -> str:
    command = ["aws", *args, "--output", "text"]
    result = subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def try_run_aws_text(args: list[str]) -> tuple[int, str, str]:
    command = ["aws", *args, "--output", "text"]
    result = subprocess.run(command, cwd=ROOT, check=False, capture_output=True, text=True)
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def discover_instances(run_id: str, role: str) -> list[dict[str, str]]:
    data = run_aws(
        [
            "ec2",
            "describe-instances",
            "--filters",
            f"Name=tag:RunId,Values={run_id}",
            f"Name=tag:Role,Values={role}",
            "Name=instance-state-name,Values=running",
        ]
    )
    instances = []
    for reservation in data["Reservations"]:
        for instance in reservation["Instances"]:
            instances.append(
                {
                    "instance_id": instance["InstanceId"],
                    "private_ip": instance["PrivateIpAddress"],
                }
            )
    return sorted(instances, key=lambda item: ip_sort_key(item["private_ip"]))


def ip_sort_key(ip: str) -> tuple[int, int, int, int]:
    return tuple(int(part) for part in ip.split("."))


def write_lines(path: pathlib.Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def send_shell_command(
    instance_ids: list[str],
    commands: list[str],
    *,
    comment: str,
    wait: bool,
) -> list[str]:
    command_ids = []
    for chunk in chunks(instance_ids, 50):
        command_id = run_aws_text(
            [
                "ssm",
                "send-command",
                "--document-name",
                "AWS-RunShellScript",
                "--comment",
                comment,
                "--instance-ids",
                *chunk,
                "--parameters",
                json.dumps({"commands": commands}),
                "--query",
                "Command.CommandId",
            ]
        )
        command_ids.append(command_id)
        if wait:
            wait_for_ssm_command(command_id, chunk)
    return command_ids


def wait_for_ssm_command(command_id: str, instance_ids: list[str]) -> None:
    pending = set(instance_ids)
    failed_polls: dict[str, int] = {instance_id: 0 for instance_id in instance_ids}
    while pending:
        time.sleep(2)
        for instance_id in list(pending):
            returncode, status, stderr = try_run_aws_text(
                [
                    "ssm",
                    "get-command-invocation",
                    "--command-id",
                    command_id,
                    "--instance-id",
                    instance_id,
                    "--query",
                    "Status",
                ]
            )
            if returncode != 0:
                failed_polls[instance_id] += 1
                if failed_polls[instance_id] < 30 and is_retryable_ssm_poll_error(stderr):
                    continue
                raise RuntimeError(
                    f"SSM command {command_id} status poll failed on {instance_id}: {stderr}"
                )
            failed_polls[instance_id] = 0
            if status in {"Success"}:
                pending.remove(instance_id)
            elif status in {"Cancelled", "Failed", "TimedOut", "Cancelling"}:
                stdout, stderr = get_ssm_command_output(command_id, instance_id)
                details = []
                if stdout:
                    details.append(f"stdout:\n{stdout}")
                if stderr:
                    details.append(f"stderr:\n{stderr}")
                suffix = "\n" + "\n".join(details) if details else ""
                raise RuntimeError(
                    f"SSM command {command_id} failed on {instance_id} with status {status}{suffix}"
                )


def is_retryable_ssm_poll_error(stderr: str) -> bool:
    retryable_markers = (
        "InvocationDoesNotExist",
        "ThrottlingException",
        "TooManyUpdates",
        "InternalServerError",
        "ServiceUnavailable",
        "RequestLimitExceeded",
        "Rate exceeded",
    )
    return any(marker in stderr for marker in retryable_markers)


def get_ssm_command_output(command_id: str, instance_id: str) -> tuple[str, str]:
    returncode, stdout, _ = try_run_aws_text(
        [
            "ssm",
            "get-command-invocation",
            "--command-id",
            command_id,
            "--instance-id",
            instance_id,
            "--query",
            "StandardOutputContent",
        ]
    )
    command_stdout = stdout if returncode == 0 else ""
    returncode, stderr, _ = try_run_aws_text(
        [
            "ssm",
            "get-command-invocation",
            "--command-id",
            command_id,
            "--instance-id",
            instance_id,
            "--query",
            "StandardErrorContent",
        ]
    )
    command_stderr = stderr if returncode == 0 else ""
    return command_stdout, command_stderr


def wait_for_tcp(host: str, port: int, timeout_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            time.sleep(2)
    raise TimeoutError(f"timed out waiting for {host}:{port}")


def wait_for_agent_health(agent_ips: list[str], port: int, timeout_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    pending = set(agent_ips)
    while pending and time.monotonic() < deadline:
        for ip in list(pending):
            try:
                with urllib.request.urlopen(f"http://{ip}:{port}/health", timeout=2) as response:
                    if response.status == 200:
                        pending.remove(ip)
            except (OSError, urllib.error.URLError):
                pass
        if pending:
            time.sleep(2)
    if pending:
        raise TimeoutError(f"timed out waiting for agents: {', '.join(sorted(pending))}")


def chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]
