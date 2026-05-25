#!/usr/bin/env python3

import importlib.util
import pathlib
import sys
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[2] / "aws_setup"


def load_module(name: str):
    sys.path.insert(0, str(SCRIPT_DIR))
    path = SCRIPT_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class LocalStackSmokeTest(unittest.TestCase):
    def test_docker_run_command_exposes_localstack_edge_port(self):
        smoke = load_module("localstack_smoke")

        command = smoke.build_localstack_start_command(
            "spider-test-localstack",
            "localstack/localstack:4.8.1",
        )

        self.assertIn("docker", command)
        self.assertIn("run", command)
        self.assertIn("-p", command)
        self.assertIn("4566:4566", command)
        self.assertIn("localstack/localstack:4.8.1", command)

    def test_docker_stop_command_removes_container(self):
        smoke = load_module("localstack_smoke")

        command = smoke.build_localstack_stop_command("spider-test-localstack")

        self.assertEqual(["docker", "rm", "-f", "spider-test-localstack"], command)


if __name__ == "__main__":
    unittest.main()
