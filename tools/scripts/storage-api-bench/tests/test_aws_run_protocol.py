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
    def test_controller_command_runs_one_workload_without_database_reset(self):
        module = load_module()

        command = module.build_controller_command(
            protocol="grpc",
            config=pathlib.Path("/tmp/config.toml"),
            data_dir=pathlib.Path("/tmp/data"),
            workload="flat",
        )

        self.assertIn("--workloads", command)
        self.assertIn("flat", command)
        self.assertNotIn("--reset-database", command)

    def test_reset_database_command_forwards_client_binary(self):
        module = load_module()

        command = module.build_reset_database_command(
            config=pathlib.Path("/tmp/config.toml"),
            database_reset_client_bin="/usr/bin/mysql",
        )

        self.assertIn(str(SCRIPT_DIR / "reset_database.py"), command)
        self.assertIn("--yes", command)
        self.assertIn("--client-bin", command)
        self.assertIn("/usr/bin/mysql", command)

    def test_parse_workloads_rejects_unknown_workload(self):
        module = load_module()

        with self.assertRaisesRegex(ValueError, "invalid workloads"):
            module.parse_workloads("flat,wide")


if __name__ == "__main__":
    unittest.main()
