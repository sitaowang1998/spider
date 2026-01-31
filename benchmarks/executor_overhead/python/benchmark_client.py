#!/usr/bin/env python3
"""Python benchmark client for executor overhead measurement."""

import argparse
import logging
import os
import sys
import time

# Add the sleep_tasks module to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sleep_tasks import py_sleep_10ms  # noqa: E402

import spider_py  # noqa: E402

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

NUM_TASKS = 160


def main() -> int:
    """Run the Python executor overhead benchmark."""
    parser = argparse.ArgumentParser(description="Python executor overhead benchmark")
    parser.add_argument("storage_url", help="Storage backend URL (JDBC format)")
    args = parser.parse_args()

    storage_url = args.storage_url
    if not storage_url:
        logger.error("storage-backend-url cannot be empty.")
        return 1

    logger.info("Starting Python executor overhead benchmark")
    logger.info("  Storage URL: %s", storage_url)
    logger.info("  Number of tasks: %d", NUM_TASKS)

    # Create a driver that connects to the Spider cluster
    driver = spider_py.Driver(storage_url)

    # Create task graphs for each task
    task_graphs = []
    task_args = []
    for i in range(NUM_TASKS):
        # Create a task graph with a single py_sleep_10ms task
        task_graph = spider_py.group([py_sleep_10ms])
        task_graphs.append(task_graph)
        task_args.append([spider_py.Int32(i)])  # task_id argument (wrapped in Int32)

    start_time = time.monotonic()

    # Submit all jobs in a batch
    jobs = driver.submit_jobs(task_graphs, task_args)

    submit_time = time.monotonic()
    submit_duration_ms = (submit_time - start_time) * 1000
    logger.info("All %d tasks submitted in %.2f ms", NUM_TASKS, submit_duration_ms)

    # Wait for all jobs to complete (poll for status)
    succeeded = 0
    failed = 0
    for job in jobs:
        while True:
            status = job.get_status()
            if status != spider_py.JobStatus.Running:
                break
            time.sleep(0.01)  # Poll every 10ms
        if status == spider_py.JobStatus.Succeeded:
            succeeded += 1
        else:
            failed += 1

    end_time = time.monotonic()
    total_duration_ms = (end_time - start_time) * 1000
    execution_duration_ms = (end_time - submit_time) * 1000

    logger.info("")
    logger.info("=== Benchmark Results ===")
    logger.info("Task type: python")
    logger.info("Total tasks: %d", NUM_TASKS)
    logger.info("Succeeded: %d", succeeded)
    logger.info("Failed: %d", failed)
    logger.info("Submit time: %.2f ms", submit_duration_ms)
    logger.info("Execution time: %.2f ms", execution_duration_ms)
    logger.info("Total time: %.2f ms", total_duration_ms)
    logger.info("Average per-task time: %.2f ms", total_duration_ms / NUM_TASKS)

    if failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
