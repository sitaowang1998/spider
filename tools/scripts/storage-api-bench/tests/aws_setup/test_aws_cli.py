#!/usr/bin/env python3

import importlib.util
import pathlib
import sys
import unittest


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


if __name__ == "__main__":
    unittest.main()
