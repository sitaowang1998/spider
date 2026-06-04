#!/usr/bin/env python3

import argparse
import importlib.util
import pathlib
import sys
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = SCRIPT_DIR / "aws_make_config.py"


def load_module():
    sys.path.insert(0, str(SCRIPT_DIR))
    spec = importlib.util.spec_from_file_location("aws_make_config", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["aws_make_config"] = module
    spec.loader.exec_module(module)
    return module


class AwsMakeConfigTest(unittest.TestCase):
    def test_render_config_uses_scheduler_core_config_names(self):
        module = load_module()
        args = argparse.Namespace(
            rest_port=8091,
            grpc_port=50051,
            database_host="db.local",
            database_port=3306,
            database_name="spider-db",
            database_username="spider-user",
            database_password="spider-password",
            database_max_connections=256,
            database_ssl_mode="preferred",
            tasks_per_job=1000,
            jobs_per_worker=10,
            payload_bytes=128,
            task_sleep_ms=3,
            submitter_count=8,
            worker_count=16,
            worker_poll_wait_ms=10,
            job_poll_wait_ms=10,
            scheduler_active_job_pool_capacity=64,
            scheduler_commit_ready_task_capacity=16,
            scheduler_cleanup_ready_task_capacity=32,
            scheduler_tick_interval_ms=11,
            scheduler_storage_poll_wait_ms=22,
            flat_percent=50,
            agent_timeout_sec=7200,
            poll_interval_ms=1000,
            agent_port=19091,
        )

        config = module.render_config(
            args,
            server_ip="10.1.0.3",
            scheduler_ip="10.1.0.6",
            submitter_ip="10.1.0.7",
            worker_ips=["10.1.0.8", "10.1.0.9"],
        )

        self.assertIn("scheduler_active_job_pool_capacity = 64", config)
        self.assertIn("scheduler_dispatch_queue_capacity = 64", config)
        self.assertIn("scheduler_ready_task_capacity = 21000", config)
        self.assertIn("scheduler_commit_ready_task_capacity = 16", config)
        self.assertIn("scheduler_cleanup_ready_task_capacity = 32", config)
        self.assertIn("scheduler_tick_interval_ms = 11", config)
        self.assertIn("scheduler_storage_poll_wait_ms = 22", config)
        self.assertNotIn("scheduler_poll_batch", config)
        self.assertNotIn("scheduler_refill_interval_ms", config)
        self.assertNotIn("scheduler_poll_wait_ms", config)

    def test_render_config_sizes_ready_capacity_from_jobs_and_tasks_per_job(self):
        module = load_module()
        args = argparse.Namespace(
            rest_port=8091,
            grpc_port=50051,
            database_host="db.local",
            database_port=3306,
            database_name="spider-db",
            database_username="spider-user",
            database_password="spider-password",
            database_max_connections=256,
            database_ssl_mode="preferred",
            tasks_per_job=1000,
            jobs_per_worker=20,
            payload_bytes=128,
            task_sleep_ms=3,
            submitter_count=8,
            worker_count=2,
            worker_poll_wait_ms=10,
            job_poll_wait_ms=10,
            scheduler_active_job_pool_capacity=64,
            scheduler_commit_ready_task_capacity=16,
            scheduler_cleanup_ready_task_capacity=32,
            scheduler_tick_interval_ms=11,
            scheduler_storage_poll_wait_ms=22,
            flat_percent=50,
            agent_timeout_sec=7200,
            poll_interval_ms=1000,
            agent_port=19091,
        )

        config = module.render_config(
            args,
            server_ip="10.1.0.3",
            scheduler_ip="10.1.0.6",
            submitter_ip="10.1.0.7",
            worker_ips=["10.1.0.8", "10.1.0.9"],
        )

        self.assertIn("scheduler_dispatch_queue_capacity = 8", config)
        self.assertIn("scheduler_ready_task_capacity = 41000", config)


if __name__ == "__main__":
    unittest.main()
