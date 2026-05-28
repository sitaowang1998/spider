#!/usr/bin/env python3

import importlib.util
import pathlib
import sys
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[2] / "aws_setup"


def load_module(name: str):
    path = SCRIPT_DIR / f"{name}.py"
    sys.path.insert(0, str(SCRIPT_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class AwsRunPlanTest(unittest.TestCase):
    def test_matrix_command_forwards_workload_knobs(self):
        config_module = load_module("config")
        run_module = load_module("run")
        config = config_module.AwsBenchConfig()
        config.aws.run_id = "aws-128"
        config.benchmark.node_counts = [1, 2, 4, 8, 16, 64, 128]
        config.benchmark.protocols = ["grpc", "rest"]
        config.benchmark.jobs_per_worker = 20
        config.benchmark.tasks_per_job = 2000
        config.benchmark.payload_bytes = 256
        config.benchmark.task_sleep_ms = 7
        config.benchmark.submitter_count = 12
        config.benchmark.worker_count = 24
        config.benchmark.worker_poll_batch = 32
        config.benchmark.worker_poll_wait_ms = 25
        config.benchmark.job_poll_wait_ms = 50
        config.benchmark.flat_percent = 60
        config.database.name = "spider_db"
        config.database.username = "spider_user"
        config.database.password = "spider_password"

        command = run_module.build_matrix_command(
            config,
            database_endpoint="bench-db.example.com",
            data_dir=pathlib.Path("/tmp/results"),
            workspace_root=pathlib.Path("/tmp/workspace"),
        )

        self.assertIn("--node-counts", command)
        self.assertIn("1,2,4,8,16,64,128", command)
        self.assertIn("--protocols", command)
        self.assertIn("grpc", command)
        self.assertIn("rest", command)
        self.assertIn("--workloads", command)
        self.assertIn("flat,deep,mixed", command)
        self.assertIn("--jobs-per-worker", command)
        self.assertIn("20", command)
        self.assertIn("--tasks-per-job", command)
        self.assertIn("2000", command)
        self.assertIn("--task-sleep-ms", command)
        self.assertIn("7", command)
        self.assertIn("--worker-poll-batch", command)
        self.assertIn("32", command)
        self.assertIn("--worker-poll-wait-ms", command)
        self.assertIn("25", command)
        self.assertIn("--job-poll-wait-ms", command)
        self.assertIn("50", command)
        self.assertIn("--database-host", command)
        self.assertIn("bench-db.example.com", command)

    def test_full_run_steps_keep_teardown_last(self):
        full_run = load_module("full_run")

        steps = full_run.full_run_steps(teardown=True)

        self.assertEqual(
            ["provision", "deploy", "bootstrap-controller", "run-controller", "fetch-results", "teardown"],
            steps,
        )


if __name__ == "__main__":
    unittest.main()
