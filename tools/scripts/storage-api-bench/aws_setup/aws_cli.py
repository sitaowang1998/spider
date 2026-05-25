#!/usr/bin/env python3
"""Small AWS CLI wrapper with dry-run and LocalStack endpoint support."""

from __future__ import annotations

import json
import subprocess


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
        subprocess.run(command, check=True, env=self.env)

    def run_allow_failure(self, args: list[str]) -> int:
        command = self.build_command(args)
        self.commands.append(command)
        if self.dry_run:
            return 0
        return subprocess.run(command, check=False, env=self.env).returncode

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
