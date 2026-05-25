#!/usr/bin/env python3

import importlib.util
import os
import pathlib
import sys
import tempfile
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[2] / "aws_setup"


def load_module(name: str):
    path = SCRIPT_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class AwsSetupEnvTest(unittest.TestCase):
    def test_load_secret_file_parses_aws_credentials(self):
        env_module = load_module("env")
        with tempfile.TemporaryDirectory() as directory:
            secret = pathlib.Path(directory) / ".secret"
            secret.write_text(
                "AWS_ACCESS_KEY_ID=key\nAWS_SECRET_ACCESS_KEY=secret\nAWS_SESSION_TOKEN=token\n",
                encoding="utf-8",
            )

            values = env_module.load_secret(secret)

        self.assertEqual("key", values["AWS_ACCESS_KEY_ID"])
        self.assertEqual("secret", values["AWS_SECRET_ACCESS_KEY"])
        self.assertEqual("token", values["AWS_SESSION_TOKEN"])

    def test_build_aws_env_adds_region_and_localstack_endpoint(self):
        env_module = load_module("env")

        aws_env = env_module.build_aws_env(
            {"AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"},
            region="us-east-1",
            endpoint_url="http://localhost:4566",
            base_env={},
        )

        self.assertEqual("key", aws_env["AWS_ACCESS_KEY_ID"])
        self.assertEqual("us-east-1", aws_env["AWS_DEFAULT_REGION"])
        self.assertEqual("", aws_env["AWS_PAGER"])
        self.assertEqual("http://localhost:4566", aws_env["AWS_ENDPOINT_URL"])
        self.assertNotIn("AWS_SESSION_TOKEN", aws_env)


if __name__ == "__main__":
    unittest.main()
