#!/usr/bin/env python3
"""Small AWS CLI wrapper with dry-run and LocalStack endpoint support."""

from __future__ import annotations

import json
import subprocess

SSM_SEND_COMMAND_INSTANCE_LIMIT = 50


class AwsCli:
    def __init__(
        self,
        *,
        endpoint_url: str | None,
        env: dict[str, str],
        dry_run: bool = False,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.env = env
        self.dry_run = dry_run
        self.commands: list[list[str]] = []

    def build_command(self, args: list[str]) -> list[str]:
        command = ["aws"]
        if self.endpoint_url is not None:
            command.extend(["--endpoint-url", self.endpoint_url])
        command.extend(args)
        return command

    def run_json(self, args: list[str]) -> object:
        command = self.build_command([*args, "--output", "json"])
        self.commands.append(command)
        if self.dry_run:
            return {}
        result = self.run_captured(command)
        return json.loads(result.stdout or "{}")

    def try_run_json(self, args: list[str]) -> tuple[int, object]:
        command = self.build_command([*args, "--output", "json"])
        self.commands.append(command)
        if self.dry_run:
            return 0, {}
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=self.env,
        )
        if result.returncode != 0:
            return result.returncode, {}
        return result.returncode, json.loads(result.stdout or "{}")

    def run_text(self, args: list[str]) -> str:
        command = self.build_command([*args, "--output", "text"])
        self.commands.append(command)
        if self.dry_run:
            return ""
        result = self.run_captured(command)
        return result.stdout.strip()

    def run(self, args: list[str]) -> None:
        command = self.build_command(args)
        self.commands.append(command)
        if self.dry_run:
            return
        self.run_captured(command)

    def run_allow_failure(self, args: list[str]) -> int:
        command = self.build_command(args)
        self.commands.append(command)
        if self.dry_run:
            return 0
        return subprocess.run(command, check=False, env=self.env).returncode

    def send_shell_command(
        self,
        instance_ids: list[str],
        commands: list[str],
        *,
        comment: str,
    ) -> list[str]:
        command_ids = []
        for chunk in chunks(instance_ids, SSM_SEND_COMMAND_INSTANCE_LIMIT):
            command_id = self.run_text(
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
            if not command_id and self.dry_run:
                command_id = "dry-run-command"
            command_ids.append(command_id)
        return command_ids

    def run_captured(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=self.env,
        )
        if result.returncode != 0:
            print(f"AWS CLI command failed with exit code {result.returncode}", flush=True)
            if result.stdout:
                print("AWS CLI stdout:", flush=True)
                print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", flush=True)
            if result.stderr:
                print("AWS CLI stderr:", flush=True)
                print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", flush=True)
            result.check_returncode()
        return result


def chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]
