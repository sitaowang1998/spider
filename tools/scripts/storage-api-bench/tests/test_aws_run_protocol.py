#!/usr/bin/env python3

import importlib.util
import pathlib
import sys
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = SCRIPT_DIR / "aws_run_protocol.py"


def load_module():
    sys.path.insert(0, str(SCRIPT_DIR))
    spec = importlib.util.spec_from_file_location("aws_run_protocol", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["aws_run_protocol"] = module
    spec.loader.exec_module(module)
    return module


class AwsRunProtocolTest(unittest.TestCase):
    def test_controller_command_resets_database_by_default(self):
        module = load_module()

        command = module.build_controller_command(
            protocol="grpc",
            config=pathlib.Path("/tmp/config.toml"),
            data_dir=pathlib.Path("/tmp/data"),
            reset_database=True,
            database_reset_client_bin=None,
        )

        self.assertIn("--reset-database", command)

    def test_controller_command_can_disable_database_reset(self):
        module = load_module()

        command = module.build_controller_command(
            protocol="rest",
            config=pathlib.Path("/tmp/config.toml"),
            data_dir=pathlib.Path("/tmp/data"),
            reset_database=False,
            database_reset_client_bin="/usr/bin/mysql",
        )

        self.assertNotIn("--reset-database", command)
        self.assertNotIn("--database-reset-client-bin", command)


if __name__ == "__main__":
    unittest.main()
