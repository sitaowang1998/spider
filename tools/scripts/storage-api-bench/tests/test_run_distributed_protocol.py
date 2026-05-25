#!/usr/bin/env python3

import importlib.util
import pathlib
import sys
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = SCRIPT_DIR / "run_distributed_protocol.py"


def load_module():
    spec = importlib.util.spec_from_file_location("run_distributed_protocol", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_distributed_protocol"] = module
    spec.loader.exec_module(module)
    return module


class RunDistributedProtocolTest(unittest.TestCase):
    def test_build_reset_database_command_confirms_destructive_cleanup(self):
        module = load_module()
        config = pathlib.Path("/tmp/spider/config.toml")

        command = module.build_reset_database_command(config, None)

        self.assertEqual(str(SCRIPT_DIR / "reset_database.py"), command[0])
        self.assertIn("--config", command)
        self.assertIn(str(config), command)
        self.assertIn("--yes", command)

    def test_build_reset_database_command_forwards_client_binary(self):
        module = load_module()

        command = module.build_reset_database_command(
            pathlib.Path("/tmp/spider/config.toml"),
            "/opt/homebrew/bin/mariadb",
        )

        self.assertIn("--client-bin", command)
        self.assertIn("/opt/homebrew/bin/mariadb", command)


if __name__ == "__main__":
    unittest.main()
