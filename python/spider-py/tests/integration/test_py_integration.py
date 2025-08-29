"""Integration tests for the python client python task executor pipeline"""

import os
import subprocess
import time
from collections.abc import Generator
from pathlib import Path

import pytest

import spider_py
from spider_py import chain, group, Int8, Int64

from .samples import add, count_name_length, data_size, Name, swap, User
from .utils import g_scheduler_port

g_storage_url = "jdbc:mariadb://127.0.0.1:3306/spider-storage?user=spider&password=password"


def start_scheduler_worker(
    storage_url: str, scheduler_port: int
) -> tuple[subprocess.Popen[bytes], subprocess.Popen[bytes]]:
    """
    Starts a scheduler and a worker process.
    :param storage_url: The JDBC URL of the storage
    :param scheduler_port: The port for the scheduler to listen on.
    :return: A tuple of the started processes:
      - The scheduler process.
      - The worker process.
    """
    # Start the scheduler
    spider_dir_str = os.getenv("SPIDER_DIR")
    if spider_dir_str is None:
        msg = "SPIDER_DIR environment variable is not set"
        raise OSError(msg)
    spider_dir = Path(spider_dir_str).resolve()
    scheduler_cmds = [
        str(spider_dir / "spider_scheduler"),
        "--host",
        "127.0.0.1",
        "--port",
        str(scheduler_port),
        "--storage_url",
        storage_url,
    ]
    scheduler_process = subprocess.Popen(scheduler_cmds)
    # Start a worker
    worker_cmds = [
        str(spider_dir / "spider_worker"),
        "--host",
        "127.0.0.1",
        "--storage_url",
        storage_url,
        "--py-libs",
        "tests.integration.samples",
    ]
    worker_process = subprocess.Popen(worker_cmds)
    return scheduler_process, worker_process


@pytest.fixture(scope="class")
def scheduler_worker() -> Generator[None, None, None]:
    """
    Fixture to start qa scheduler process and a worker processes.
    Yields control to the test class after the scheduler and workers spawned and ensures the
    processes are killed after the tests session is complete.
    :param storage: The storage connection.
    :return: A generator that yields control to the test class.
    """
    scheduler_process, worker_process = start_scheduler_worker(
        storage_url=g_storage_url, scheduler_port=g_scheduler_port
    )
    # Wait for 5 second to make sure the scheduler and worker are started
    time.sleep(5)
    yield
    scheduler_process.kill()
    worker_process.kill()


@pytest.fixture(scope="session")
def driver() -> spider_py.Driver:
    """
    Fixture to create a spider driver.
    :return: A spider driver.
    """
    return spider_py.Driver(g_storage_url)


g_sleep_time = 5


@pytest.mark.integration
@pytest.mark.storage
@pytest.mark.usefixtures("scheduler_worker")
class TestPyIntegration:
    """Integration tests for the python client python task executor pipeline"""

    def test_simple_job(self, driver: spider_py.Driver) -> None:
        """Tests simple task graph execution."""
        (job,) = driver.submit_jobs([group([add])], [(Int8(1), Int8(2))])
        time.sleep(g_sleep_time)
        assert job.get_status() == spider_py.JobStatus.Succeeded
        assert job.get_results() == Int8(3)

    def test_multiple_jobs(self, driver: spider_py.Driver) -> None:
        """Tests multiple job submission and execution."""
        jobs = driver.submit_jobs(
            [group([add]), group([add]), group([add])],
            [(Int8(1), Int8(2)), (Int8(3), Int8(4)), (Int8(5), Int8(6))],
        )
        time.sleep(g_sleep_time)
        for i, job in enumerate(jobs):
            assert job.get_status() == spider_py.JobStatus.Succeeded
            assert job.get_results() == Int8(3 + i * 4)

    def test_complex_job(self, driver: spider_py.Driver) -> None:
        """Tests a more complex task graph execution."""
        (job,) = driver.submit_jobs(
            [chain(group([add, add]), swap)], [(Int8(1), Int8(2), Int8(3), Int8(4))]
        )
        time.sleep(g_sleep_time)
        assert job.get_status() == spider_py.JobStatus.Succeeded
        assert job.get_results() == (Int8(7), Int8(3))

    def test_data_job(self, driver: spider_py.Driver) -> None:
        """Tests a job with data input and output."""
        data = spider_py.Data(b"test_data")
        driver.create_data(data)
        (job,) = driver.submit_jobs([group([data_size])], [(data,)])
        time.sleep(g_sleep_time)
        assert job.get_status() == spider_py.JobStatus.Succeeded
        assert job.get_results() == Int64(len("test_data"))

    def test_dataclass_job(self, driver: spider_py.Driver) -> None:
        """Tests a job with dataclass input and output."""
        user = User(Name("John", "Doe"), 30)
        (job,) = driver.submit_jobs([group([count_name_length])], [(user,)])
        time.sleep(g_sleep_time)
        assert job.get_status() == spider_py.JobStatus.Succeeded
        assert job.get_results() == Int64(len(user.name.first) + len(user.name.last) + 1)
