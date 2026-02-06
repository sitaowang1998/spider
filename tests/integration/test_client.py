"""Integration test for the client_test C++ program."""

import subprocess
import time
from collections.abc import Generator
from pathlib import Path

import pytest

from integration.client import get_storage_url, SQLConnection, storage  # noqa: F401
from integration.utils import g_scheduler_port


def start_scheduler_workers(
    storage_url: str, scheduler_port: int
) -> tuple[subprocess.Popen[bytes], list[subprocess.Popen[bytes]]]:
    """
    Starts the scheduler and worker processes.
    :param storage_url: The JDBC URL of the storage.
    :param scheduler_port: The port for the scheduler to listen on.
    :return: A tuple of the started processes:
      - The scheduler process.
      - A list of worker processes.
    """
    # Start the scheduler
    dir_path = Path(__file__).resolve().parent
    dir_path = dir_path / ".." / ".." / "src" / "spider"
    scheduler_cmds = [
        str(dir_path / "spider_scheduler"),
        "--host",
        "127.0.0.1",
        "--port",
        str(scheduler_port),
        "--storage_url",
        storage_url,
    ]
    scheduler_process = subprocess.Popen(scheduler_cmds)
    worker_cmds = [
        str(dir_path / "spider_worker"),
        "--host",
        "127.0.0.1",
        "--storage_url",
        storage_url,
        "--libs",
        "tests/libworker_test.so",
    ]
    workers = [subprocess.Popen(worker_cmds) for _ in range(4)]
    return scheduler_process, workers


@pytest.fixture(scope="class")
def scheduler_worker(
    storage: SQLConnection,  # noqa: F811
) -> Generator[None, None, None]:
    """
    Fixture to start a scheduler process and worker processes.
    Yields control to the test class after the scheduler and workers spawned and ensures the
    processes are killed after the tests session is complete.
    :param storage: The storage connection.
    :return: A generator that yields control to the test class.
    """
    _ = storage  # Avoid ARG001
    scheduler_process, workers = start_scheduler_workers(
        storage_url=get_storage_url(), scheduler_port=g_scheduler_port
    )
    # Wait for 5 second to make sure the scheduler and worker are started
    time.sleep(5)
    yield
    scheduler_process.kill()
    for worker in workers:
        worker.kill()


class TestClient:
    """Wrapper class for running `client_test`."""

    @pytest.mark.usefixtures("scheduler_worker")
    def test_client(self) -> None:
        """Executes the `client_test` program and checks for successful execution."""
        dir_path = Path(__file__).resolve().parent
        dir_path = dir_path / ".."
        client_cmds = [
            str(dir_path / "client_test"),
            "--storage_url",
            get_storage_url(),
        ]
        p = subprocess.run(client_cmds, check=True, timeout=30)
        assert p.returncode == 0
