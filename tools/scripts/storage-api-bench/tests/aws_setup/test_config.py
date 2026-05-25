#!/usr/bin/env python3

import importlib.util
import pathlib
import sys
import tempfile
import textwrap
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[2] / "aws_setup"


def load_module(name: str):
    path = SCRIPT_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class AwsSetupConfigTest(unittest.TestCase):
    def test_config_loads_benchmark_workload_knobs(self):
        config_module = load_module("config")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "config.toml"
            path.write_text(
                textwrap.dedent(
                    """
                    [aws]
                    region = "us-east-2"
                    availability_zone = "us-east-2a"
                    run_id = "bench-128"

                    [benchmark]
                    node_counts = [1, 2, 4, 8, 16, 64, 128]
                    protocols = ["grpc", "rest"]
                    workloads = ["flat", "deep", "mixed"]
                    jobs_per_agent = 20
                    tasks_per_job = 2000
                    payload_bytes = 256
                    client_count = 12
                    worker_count = 24
                    flat_percent = 60

                    [instances]
                    client_count = 128

                    [database]
                    name = "spider_db"
                    username = "spider_user"
                    password = "spider_password"
                    """
                ),
                encoding="utf-8",
            )

            config = config_module.load_config(path)

        self.assertEqual("us-east-2", config.aws.region)
        self.assertEqual([1, 2, 4, 8, 16, 64, 128], config.benchmark.node_counts)
        self.assertEqual(["grpc", "rest"], config.benchmark.protocols)
        self.assertEqual(["flat", "deep", "mixed"], config.benchmark.workloads)
        self.assertEqual(20, config.benchmark.jobs_per_agent)
        self.assertEqual(2000, config.benchmark.tasks_per_job)
        self.assertEqual(128, config.instances.client_count)

    def test_config_rejects_node_count_larger_than_client_fleet(self):
        config_module = load_module("config")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "config.toml"
            path.write_text(
                textwrap.dedent(
                    """
                    [benchmark]
                    node_counts = [1, 4]

                    [instances]
                    client_count = 2
                    """
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "node_counts"):
                config_module.load_config(path)


if __name__ == "__main__":
    unittest.main()
