#!/usr/bin/env python3
"""Shared helpers for controller-owned AWS benchmark orchestration."""

from __future__ import annotations

import json
import shlex
import time

import aws_cli


def discover_controller_instance_id(client: aws_cli.AwsCli, run_id: str) -> str:
    data = client.run_json(
        [
            "ec2",
            "describe-instances",
            "--filters",
            f"Name=tag:RunId,Values={run_id}",
            "Name=tag:Role,Values=controller",
            "Name=instance-state-name,Values=running",
        ]
    )
    controllers = []
    for reservation in data.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            controllers.append(instance["InstanceId"])
    if not controllers:
        if client.dry_run:
            return "i-controller-dryrun"
        msg = f"no running controller instance found for run id {run_id}"
        raise RuntimeError(msg)
    if len(controllers) > 1:
        msg = f"expected one running controller for run id {run_id}, found {len(controllers)}"
        raise RuntimeError(msg)
    return controllers[0]


def send_controller_command(
    client: aws_cli.AwsCli,
    *,
    controller_instance_id: str,
    commands: list[str],
    comment: str,
) -> str:
    command_id = client.run_text(
        [
            "ssm",
            "send-command",
            "--document-name",
            "AWS-RunShellScript",
            "--comment",
            comment,
            "--instance-ids",
            controller_instance_id,
            "--parameters",
            json.dumps({"commands": commands}),
            "--query",
            "Command.CommandId",
        ]
    )
    if not command_id and client.dry_run:
        return "dry-run-command"
    return command_id


def wait_for_controller_command(
    client: aws_cli.AwsCli,
    *,
    command_id: str,
    controller_instance_id: str,
    poll_interval_sec: int = 2,
) -> None:
    if client.dry_run:
        return
    while True:
        data = client.run_json(
            [
                "ssm",
                "get-command-invocation",
                "--command-id",
                command_id,
                "--instance-id",
                controller_instance_id,
            ]
        )
        status = data.get("Status") if isinstance(data, dict) else None
        if status == "Success":
            return
        if status in {"Cancelled", "Failed", "TimedOut", "Cancelling"}:
            stdout = data.get("StandardOutputContent", "") if isinstance(data, dict) else ""
            stderr = data.get("StandardErrorContent", "") if isinstance(data, dict) else ""
            details = []
            if stdout:
                details.append(f"stdout:\n{stdout}")
            if stderr:
                details.append(f"stderr:\n{stderr}")
            suffix = "\n" + "\n".join(details) if details else ""
            msg = f"SSM command {command_id} failed on controller with status {status}{suffix}"
            raise RuntimeError(msg)
        time.sleep(poll_interval_sec)


def shell_heredoc(value: str, marker: str) -> str:
    return value.replace(marker, f"{marker}_REPLACED")


def quote_path(path: str) -> str:
    return shlex.quote(path)
