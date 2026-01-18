"""Integration tests for channel-based inter-task communication."""

import os
import subprocess
import time
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import cast

import msgpack
import pytest

from integration.client import (
    get_storage_url,
    get_task_outputs,
    get_task_state,
    remove_job,
    SQLConnection,
    storage,  # noqa: F401
    submit_job,
    Task,
    TaskGraph,
    TaskInput,
    TaskOutput,
)
from integration.utils import g_scheduler_port


def add_storage_env_vars(storage_url: str) -> dict[str, str]:
    """
    Adds the storage URL and PYTHONPATH to the environment variables.

    :param storage_url: The URL of the storage.
    :return: The environment variables with the storage URL and PYTHONPATH added.
    """
    env = os.environ.copy()
    env |= {"SPIDER_STORAGE_URL": storage_url}

    # Add spider_py source and tests directory to PYTHONPATH for Python task execution
    base_dir = Path(__file__).resolve().parent / ".." / ".."
    spider_py_src = (base_dir / "python" / "spider-py" / "src").resolve()
    tests_dir = (base_dir / "tests").resolve()
    pythonpath_additions = f"{spider_py_src}:{tests_dir}"

    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        env["PYTHONPATH"] = f"{pythonpath_additions}:{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = pythonpath_additions

    return env


def start_scheduler_workers(
    storage_url: str, scheduler_port: int, num_workers: int = 1
) -> tuple[subprocess.Popen[bytes], list[subprocess.Popen[bytes]]]:
    """
    Starts a scheduler and worker processes.

    :param storage_url: The JDBC URL of the storage
    :param scheduler_port: The port for the scheduler to listen on.
    :param num_workers: The number of worker processes to start.
    :return: A tuple of the scheduler process and list of worker processes.
    """
    dir_path = Path(__file__).resolve().parent
    dir_path = dir_path / ".." / ".." / "src" / "spider"
    env = add_storage_env_vars(storage_url)
    scheduler_cmds = [
        str(dir_path / "spider_scheduler"),
        "--host",
        "127.0.0.1",
        "--port",
        str(scheduler_port),
    ]
    scheduler_process = subprocess.Popen(scheduler_cmds, env=env)
    worker_cmds = [
        str(dir_path / "spider_worker"),
        "--host",
        "127.0.0.1",
        "--libs",
        "tests/libworker_test.so",
    ]
    worker_processes = [subprocess.Popen(worker_cmds, env=env) for _ in range(num_workers)]
    return scheduler_process, worker_processes


@pytest.fixture(scope="class")
def scheduler_worker(
    storage: SQLConnection,  # noqa: F811
) -> Generator[None, None, None]:
    """
    Fixture to start a scheduler process and multiple worker processes.

    Yields control to the test class after the scheduler and workers spawned and ensures the
    processes are killed after the tests session is complete.
    :param storage: The storage connection.
    :return: A generator that yields control to the test class.
    """
    _ = storage  # Avoid ARG001
    scheduler_process, worker_processes = start_scheduler_workers(
        storage_url=get_storage_url(), scheduler_port=g_scheduler_port, num_workers=3
    )
    # Wait for 5 seconds to make sure the scheduler and workers are started
    time.sleep(5)
    yield
    # Graceful shutdown: terminate first, then kill if needed
    for process in [scheduler_process, *worker_processes]:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


@pytest.fixture
def cpp_channel_with_dependency_job(
    storage: SQLConnection,  # noqa: F811
) -> Generator[tuple[TaskGraph, Task, Task], None, None]:
    """
    Fixture: Producer -> Consumer WITH task dependency.

    Consumer is a child of producer, so it waits for producer to complete before starting.
    Channel items are committed when producer finishes, then consumer reads them.

    :param storage: The storage connection.
    :return: A tuple of the task graph, producer task, and consumer task.
    """
    channel_id = uuid.uuid4()

    producer = Task(
        id=uuid.uuid4(),
        function_name="channel_producer_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id),
            TaskInput(type="int", value=msgpack.packb(5)),
        ],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    consumer = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id)],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    graph = TaskGraph(
        tasks={producer.id: producer, consumer.id: consumer},
        # Consumer depends on producer - waits for producer to complete
        dependencies=[(producer.id, consumer.id)],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    yield graph, producer, consumer
    remove_job(storage, graph.id)


@pytest.fixture
def cpp_channel_no_dependency_job(
    storage: SQLConnection,  # noqa: F811
) -> Generator[tuple[TaskGraph, Task, Task], None, None]:
    """
    Fixture: Producer and Consumer WITHOUT task dependency (run concurrently).

    Both tasks are head tasks (no dependencies), so they start simultaneously.
    Consumer polls the channel for items while producer is still sending.

    :param storage: The storage connection.
    :return: A tuple of the task graph, producer task, and consumer task.
    """
    channel_id = uuid.uuid4()

    producer = Task(
        id=uuid.uuid4(),
        function_name="channel_producer_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id),
            TaskInput(type="int", value=msgpack.packb(5)),
        ],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    consumer = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id)],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    graph = TaskGraph(
        tasks={producer.id: producer, consumer.id: consumer},
        # No dependencies - both tasks are head tasks and run concurrently
        dependencies=[],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    yield graph, producer, consumer
    remove_job(storage, graph.id)


@pytest.fixture
def cpp_multi_producer_with_dependency_job(
    storage: SQLConnection,  # noqa: F811
) -> Generator[tuple[TaskGraph, Task, Task, Task], None, None]:
    """
    Fixture: Two producers -> Consumer WITH dependencies.

    Consumer depends on both producers (waits for both to complete).

    :param storage: The storage connection.
    :return: A tuple of the task graph and tasks.
    """
    channel_id = uuid.uuid4()

    producer1 = Task(
        id=uuid.uuid4(),
        function_name="channel_producer_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id),
            TaskInput(type="int", value=msgpack.packb(3)),
        ],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    producer2 = Task(
        id=uuid.uuid4(),
        function_name="channel_producer_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id),
            TaskInput(type="int", value=msgpack.packb(3)),
        ],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    consumer = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id)],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    graph = TaskGraph(
        tasks={producer1.id: producer1, producer2.id: producer2, consumer.id: consumer},
        # Consumer depends on both producers
        dependencies=[(producer1.id, consumer.id), (producer2.id, consumer.id)],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    yield graph, producer1, producer2, consumer
    remove_job(storage, graph.id)


@pytest.fixture
def cpp_multi_consumer_job(
    storage: SQLConnection,  # noqa: F811
) -> Generator[tuple[TaskGraph, Task, Task, Task], None, None]:
    """
    Fixture: Producer -> Two Consumers (both read from same channel).

    Multiple consumers read from the same channel. Each item is consumed by only one consumer.
    Both consumers depend on producer.

    :param storage: The storage connection.
    :return: A tuple of the task graph and tasks.
    """
    channel_id = uuid.uuid4()

    producer = Task(
        id=uuid.uuid4(),
        function_name="channel_producer_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id),
            TaskInput(type="int", value=msgpack.packb(6)),
        ],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    consumer1 = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id)],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    consumer2 = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id)],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    graph = TaskGraph(
        tasks={producer.id: producer, consumer1.id: consumer1, consumer2.id: consumer2},
        # Both consumers depend on producer
        dependencies=[(producer.id, consumer1.id), (producer.id, consumer2.id)],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    yield graph, producer, consumer1, consumer2
    remove_job(storage, graph.id)


class TestChannelCommunication:
    """Tests for channel-based inter-task communication."""

    @pytest.mark.usefixtures("scheduler_worker")
    def test_channel_with_dependency(
        self,
        storage: SQLConnection,  # noqa: F811
        cpp_channel_with_dependency_job: tuple[TaskGraph, Task, Task],
    ) -> None:
        """
        Test channel communication WITH task dependency.

        Consumer is a child of producer (dependency: producer -> consumer).
        Consumer waits for producer to complete before starting.
        Producer sends 0,1,2,3,4 (count=5) to channel.
        Consumer receives all items and returns sum (0+1+2+3+4=10).
        """
        _, producer, consumer = cpp_channel_with_dependency_job

        # Wait for execution
        time.sleep(10)

        assert get_task_state(storage, producer.id) == "success"
        assert get_task_state(storage, consumer.id) == "success"

        # Producer returns count
        producer_outputs = get_task_outputs(storage, producer.id)
        assert len(producer_outputs) >= 1
        assert producer_outputs[0].value == msgpack.packb(5)

        # Consumer returns sum of received items (0+1+2+3+4=10)
        consumer_outputs = get_task_outputs(storage, consumer.id)
        assert len(consumer_outputs) >= 1
        assert consumer_outputs[0].value == msgpack.packb(10)

    @pytest.mark.usefixtures("scheduler_worker")
    def test_channel_no_dependency(
        self,
        storage: SQLConnection,  # noqa: F811
        cpp_channel_no_dependency_job: tuple[TaskGraph, Task, Task],
    ) -> None:
        """
        Test channel communication WITHOUT task dependency (concurrent execution).

        Producer and consumer are both head tasks (no dependencies).
        Both start simultaneously - consumer polls channel while producer sends.
        Producer sends 0,1,2,3,4 (count=5) to channel.
        Consumer receives items as they arrive and returns sum (0+1+2+3+4=10).
        """
        _, producer, consumer = cpp_channel_no_dependency_job

        # Wait for execution (may need more time for concurrent execution)
        time.sleep(15)

        assert get_task_state(storage, producer.id) == "success"
        assert get_task_state(storage, consumer.id) == "success"

        # Producer returns count
        producer_outputs = get_task_outputs(storage, producer.id)
        assert len(producer_outputs) >= 1
        assert producer_outputs[0].value == msgpack.packb(5)

        # Consumer returns sum of received items (0+1+2+3+4=10)
        consumer_outputs = get_task_outputs(storage, consumer.id)
        assert len(consumer_outputs) >= 1
        assert consumer_outputs[0].value == msgpack.packb(10)

    @pytest.mark.usefixtures("scheduler_worker")
    def test_multiple_producers_with_dependency(
        self,
        storage: SQLConnection,  # noqa: F811
        cpp_multi_producer_with_dependency_job: tuple[TaskGraph, Task, Task, Task],
    ) -> None:
        """
        Test multiple producers with consumer dependency.

        Consumer depends on both producers (waits for both to complete).
        Two producers each send 0,1,2 (count=3) to the same channel.
        Consumer receives all 6 items and returns sum (2*(0+1+2)=6).
        """
        _, producer1, producer2, consumer = cpp_multi_producer_with_dependency_job

        # Wait for execution
        time.sleep(10)

        assert get_task_state(storage, producer1.id) == "success"
        assert get_task_state(storage, producer2.id) == "success"
        assert get_task_state(storage, consumer.id) == "success"

        # Consumer receives items from both producers
        # Each producer sends 0+1+2=3, total sum = 6
        consumer_outputs = get_task_outputs(storage, consumer.id)
        assert len(consumer_outputs) >= 1
        assert consumer_outputs[0].value == msgpack.packb(6)

    @pytest.mark.usefixtures("scheduler_worker")
    def test_multiple_consumers(
        self,
        storage: SQLConnection,  # noqa: F811
        cpp_multi_consumer_job: tuple[TaskGraph, Task, Task, Task],
    ) -> None:
        """
        Test multiple consumers reading from the same channel.

        Producer sends 0,1,2,3,4,5 (count=6) to channel.
        Two consumers read from the same channel. Each item is consumed by only one consumer.
        The sum of both consumers' results should equal total sum (0+1+2+3+4+5=15).
        """
        _, producer, consumer1, consumer2 = cpp_multi_consumer_job

        # Wait for execution
        time.sleep(10)

        assert get_task_state(storage, producer.id) == "success"
        assert get_task_state(storage, consumer1.id) == "success"
        assert get_task_state(storage, consumer2.id) == "success"

        # Producer returns count
        producer_outputs = get_task_outputs(storage, producer.id)
        assert len(producer_outputs) >= 1
        assert producer_outputs[0].value == msgpack.packb(6)

        # Both consumers together should have received all items
        # Each item is consumed by only one consumer, so sum of both = total sum
        consumer1_outputs = get_task_outputs(storage, consumer1.id)
        consumer2_outputs = get_task_outputs(storage, consumer2.id)
        assert len(consumer1_outputs) >= 1
        assert len(consumer2_outputs) >= 1

        sum1 = msgpack.unpackb(cast("bytes", consumer1_outputs[0].value))
        sum2 = msgpack.unpackb(cast("bytes", consumer2_outputs[0].value))
        # Total sum of 0+1+2+3+4+5 = 15
        assert sum1 + sum2 == 15  # noqa: PLR2004
