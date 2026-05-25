#!/usr/bin/env python3

import importlib.util
import io
import json
import pathlib
import subprocess
import sys
import unittest
from contextlib import redirect_stdout


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[2] / "aws_setup"


def load_module(name: str):
    path = SCRIPT_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class AwsCliTest(unittest.TestCase):
    def test_build_command_includes_localstack_endpoint_before_service_args(self):
        aws_cli = load_module("aws_cli")
        client = aws_cli.AwsCli(endpoint_url="http://localhost:4566", env={})

        command = client.build_command(["ec2", "describe-instances"])

        self.assertEqual(
            ["aws", "--endpoint-url", "http://localhost:4566", "ec2", "describe-instances"],
            command,
        )

    def test_dry_run_records_command_without_executing(self):
        aws_cli = load_module("aws_cli")
        client = aws_cli.AwsCli(endpoint_url=None, env={}, dry_run=True)

        result = client.run_text(["ec2", "describe-vpcs"])

        self.assertEqual("", result)
        self.assertEqual([["aws", "ec2", "describe-vpcs", "--output", "text"]], client.commands)

    def test_dry_run_allow_failure_records_command(self):
        aws_cli = load_module("aws_cli")
        client = aws_cli.AwsCli(endpoint_url=None, env={}, dry_run=True)

        result = client.run_allow_failure(["s3api", "create-bucket", "--bucket", "bucket"])

        self.assertEqual(0, result)
        self.assertEqual([["aws", "s3api", "create-bucket", "--bucket", "bucket"]], client.commands)

    def test_captured_failure_prints_stdout_and_stderr(self):
        aws_cli = load_module("aws_cli")
        client = aws_cli.AwsCli(endpoint_url=None, env={})
        output = io.StringIO()

        with redirect_stdout(output), self.assertRaises(subprocess.CalledProcessError):
            client.run_captured(["sh", "-c", "printf out; printf err >&2; exit 254"])

        text = output.getvalue()
        self.assertIn("exit code 254", text)
        self.assertIn("AWS CLI stdout:", text)
        self.assertIn("out", text)
        self.assertIn("AWS CLI stderr:", text)
        self.assertIn("err", text)

    def test_send_shell_command_batches_instance_ids(self):
        aws_cli = load_module("aws_cli")
        client = aws_cli.AwsCli(endpoint_url=None, env={}, dry_run=True)
        instance_ids = [f"i-{index:017d}" for index in range(128)]

        command_ids = client.send_shell_command(
            instance_ids,
            ["echo ok"],
            comment="test command",
        )

        self.assertEqual(["dry-run-command", "dry-run-command", "dry-run-command"], command_ids)
        batch_sizes = []
        for command in client.commands:
            start = command.index("--instance-ids") + 1
            end = command.index("--parameters")
            batch_sizes.append(end - start)
            self.assertEqual(
                {"commands": ["echo ok"]},
                json.loads(command[end + 1]),
            )
        self.assertEqual([50, 50, 28], batch_sizes)


if __name__ == "__main__":
    unittest.main()
