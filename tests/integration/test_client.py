import subprocess
import time
from pathlib import Path
from typing import Tuple

import pytest

from .client import (
    g_storage_url,
    storage,
)
from .utils import g_scheduler_port


def start_scheduler_workers(
    storage_url: str, scheduler_port: int
) -> Tuple[subprocess.Popen, subprocess.Popen, subprocess.Popen]:
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
    worker_process_0 = subprocess.Popen(worker_cmds)
    worker_process_1 = subprocess.Popen(worker_cmds)
    return scheduler_process, worker_process_0, worker_process_1


@pytest.fixture(scope="class")
def scheduler_worker(storage):
    scheduler_process, worker_process_0, worker_process_1 = start_scheduler_workers(
        storage_url=g_storage_url, scheduler_port=g_scheduler_port
    )
    # Wait for 5 second to make sure the scheduler and worker are started
    time.sleep(5)
    yield
    scheduler_process.kill()
    worker_process_0.kill()
    worker_process_1.kill()


class TestClient:
    def test_client(self, scheduler_worker):
        dir_path = Path(__file__).resolve().parent
        dir_path = dir_path / ".."
        client_cmds = [
            str(dir_path / "client_test"),
            "--storage_url",
            g_storage_url,
        ]
        p = subprocess.run(client_cmds, timeout=20)
        assert p.returncode == 0
