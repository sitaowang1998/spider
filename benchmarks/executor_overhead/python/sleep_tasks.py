"""Sleep tasks for benchmarking executor overhead."""

import logging
import time

from spider_py import Int32, TaskContext

# Set up logger
logger = logging.getLogger(__name__)


def py_sleep_10ms(_: TaskContext, task_id: Int32) -> Int32:
    """
    A simple sleep task for benchmarking executor overhead.
    Sleeps for 10ms and logs timing information.

    :param _: The task context (unused).
    :param task_id: An identifier for this task instance.
    :return: The task_id that was passed in.
    """
    func_entry_ms = time.monotonic_ns() // 1_000_000
    logger.info("[TIMING] task_id=%d func=py_sleep_10ms func_entry=%d", task_id, func_entry_ms)

    # Sleep for 10ms
    time.sleep(0.010)

    func_exit_ms = time.monotonic_ns() // 1_000_000
    logger.info(
        "[TIMING] task_id=%d func=py_sleep_10ms func_entry=%d func_exit=%d func_duration_ms=%d",
        task_id,
        func_entry_ms,
        func_exit_ms,
        func_exit_ms - func_entry_ms,
    )

    return task_id
