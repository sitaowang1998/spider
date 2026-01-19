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


@pytest.fixture
def cpp_multi_sender_job(
    storage: SQLConnection,  # noqa: F811
) -> Generator[tuple[TaskGraph, Task, Task, Task], None, None]:
    """
    Fixture: Multi-sender producer -> Two Consumers (each reads from separate channel).

    One task (multi_channel_producer_test) writes to TWO channels and returns a tuple.
    Two consumers each read from their respective channel.

    :param storage: The storage connection.
    :return: A tuple of the task graph and tasks.
    """
    channel_id1 = uuid.uuid4()
    channel_id2 = uuid.uuid4()

    # Producer with two senders that returns int (count)
    producer = Task(
        id=uuid.uuid4(),
        function_name="multi_channel_producer_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id1),  # sender1
            TaskInput(type="channel", channel_id=channel_id2),  # sender2
            TaskInput(type="int", value=msgpack.packb(3)),  # count
        ],
        outputs=[TaskOutput(type="int")],  # returns count
        language="cpp",
    )

    consumer1 = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id1)],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    consumer2 = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id2)],
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


@pytest.fixture
def cpp_passthrough_job(
    storage: SQLConnection,  # noqa: F811
) -> Generator[tuple[TaskGraph, Task, Task, Task], None, None]:
    """
    Fixture: Producer -> Passthrough -> Consumer (channel chain).

    Producer sends items to channel1.
    Passthrough receives from channel1, doubles values, sends to channel2.
    Consumer receives from channel2.

    :param storage: The storage connection.
    :return: A tuple of the task graph and tasks.
    """
    channel_id1 = uuid.uuid4()
    channel_id2 = uuid.uuid4()

    producer = Task(
        id=uuid.uuid4(),
        function_name="channel_producer_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id1),
            TaskInput(type="int", value=msgpack.packb(3)),  # count=3 -> sends 0,1,2
        ],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    # Passthrough: receives from channel1, doubles, sends to channel2
    passthrough = Task(
        id=uuid.uuid4(),
        function_name="channel_passthrough_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id1),  # receiver
            TaskInput(type="channel", channel_id=channel_id2),  # sender
        ],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    consumer = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id2)],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    graph = TaskGraph(
        tasks={producer.id: producer, passthrough.id: passthrough, consumer.id: consumer},
        # passthrough depends on producer, consumer depends on passthrough
        dependencies=[(producer.id, passthrough.id), (passthrough.id, consumer.id)],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    yield graph, producer, passthrough, consumer
    remove_job(storage, graph.id)


class TestMultiChannelCommunication:
    """Tests for tasks with multiple channels (senders/receivers)."""

    @pytest.mark.usefixtures("scheduler_worker")
    def test_multi_sender_task(
        self,
        storage: SQLConnection,  # noqa: F811
        cpp_multi_sender_job: tuple[TaskGraph, Task, Task, Task],
    ) -> None:
        """
        Test task with multiple Senders (writes to two channels).

        multi_channel_producer_test(sender1, sender2, count=3):
        - Sends 0,1,2 to sender1
        - Sends 0,2,4 to sender2
        - Returns tuple<int, int>(3, 3)

        consumer1 receives from channel1: 0+1+2=3
        consumer2 receives from channel2: 0+2+4=6
        """
        _, producer, consumer1, consumer2 = cpp_multi_sender_job

        # Wait for execution
        time.sleep(15)

        assert get_task_state(storage, producer.id) == "success"
        assert get_task_state(storage, consumer1.id) == "success"
        assert get_task_state(storage, consumer2.id) == "success"

        # Producer returns count=3
        producer_outputs = get_task_outputs(storage, producer.id)
        assert len(producer_outputs) >= 1
        assert producer_outputs[0].value == msgpack.packb(3)

        # consumer1 receives 0+1+2=3
        consumer1_outputs = get_task_outputs(storage, consumer1.id)
        assert len(consumer1_outputs) >= 1
        assert consumer1_outputs[0].value == msgpack.packb(3)

        # consumer2 receives 0+2+4=6
        consumer2_outputs = get_task_outputs(storage, consumer2.id)
        assert len(consumer2_outputs) >= 1
        assert consumer2_outputs[0].value == msgpack.packb(6)

    @pytest.mark.usefixtures("scheduler_worker")
    def test_passthrough_chain(
        self,
        storage: SQLConnection,  # noqa: F811
        cpp_passthrough_job: tuple[TaskGraph, Task, Task, Task],
    ) -> None:
        """
        Test channel chain: Producer -> Passthrough -> Consumer.

        producer sends 0,1,2 to channel1
        passthrough receives from channel1, doubles (0,2,4), sends to channel2
        consumer receives from channel2: 0+2+4=6
        """
        _, producer, passthrough, consumer = cpp_passthrough_job

        # Wait for execution
        time.sleep(15)

        assert get_task_state(storage, producer.id) == "success"
        assert get_task_state(storage, passthrough.id) == "success"
        assert get_task_state(storage, consumer.id) == "success"

        # Producer returns count=3
        producer_outputs = get_task_outputs(storage, producer.id)
        assert len(producer_outputs) >= 1
        assert producer_outputs[0].value == msgpack.packb(3)

        # Passthrough returns count=3 (forwarded 3 items)
        passthrough_outputs = get_task_outputs(storage, passthrough.id)
        assert len(passthrough_outputs) >= 1
        assert passthrough_outputs[0].value == msgpack.packb(3)

        # Consumer receives doubled values: 0+2+4=6
        consumer_outputs = get_task_outputs(storage, consumer.id)
        assert len(consumer_outputs) >= 1
        assert consumer_outputs[0].value == msgpack.packb(6)


@pytest.fixture
def cpp_mixed_output_tuple_with_sender_job(
    storage: SQLConnection,  # noqa: F811
) -> Generator[tuple[TaskGraph, Task, Task], None, None]:
    """
    Fixture: Mixed output task (tuple return with Sender argument) -> Consumer.

    Producer takes a Sender argument and returns tuple<int, int>.
    Sends items through the Sender while returning two regular values.
    Output order: [int, int, channel_items...]

    :param storage: The storage connection.
    :return: A tuple of the task graph and tasks.
    """
    channel_id = uuid.uuid4()

    # Producer takes Sender and returns tuple<int, int>
    # Outputs: [count, sum, channel_items...]
    producer = Task(
        id=uuid.uuid4(),
        function_name="mixed_output_tuple_with_sender_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id),  # Sender argument
            TaskInput(type="int", value=msgpack.packb(4)),  # count=4
        ],
        outputs=[
            TaskOutput(type="int"),  # return value: count
            TaskOutput(type="int"),  # return value: sum
        ],
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
        dependencies=[(producer.id, consumer.id)],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    yield graph, producer, consumer
    remove_job(storage, graph.id)


@pytest.fixture
def cpp_mixed_output_multi_sender_job(
    storage: SQLConnection,  # noqa: F811
) -> Generator[tuple[TaskGraph, Task, Task, Task], None, None]:
    """
    Fixture: Mixed output task (int return with two Sender arguments) -> Two Consumers.

    Producer takes two Sender arguments and returns int.
    Sends items to both Senders while returning a regular value.
    Output order: [int, channel_items_1..., channel_items_2...]

    :param storage: The storage connection.
    :return: A tuple of the task graph and tasks.
    """
    channel_id1 = uuid.uuid4()
    channel_id2 = uuid.uuid4()

    # Producer takes two Senders and returns int
    producer = Task(
        id=uuid.uuid4(),
        function_name="mixed_output_multi_sender_test",
        inputs=[
            TaskInput(type="channel", channel_id=channel_id1),  # sender1
            TaskInput(type="channel", channel_id=channel_id2),  # sender2
            TaskInput(type="int", value=msgpack.packb(3)),  # count=3
        ],
        outputs=[TaskOutput(type="int")],  # return value: count
        language="cpp",
    )

    consumer1 = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id1)],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    consumer2 = Task(
        id=uuid.uuid4(),
        function_name="channel_consumer_test",
        inputs=[TaskInput(type="channel", channel_id=channel_id2)],
        outputs=[TaskOutput(type="int")],
        language="cpp",
    )

    graph = TaskGraph(
        tasks={producer.id: producer, consumer1.id: consumer1, consumer2.id: consumer2},
        dependencies=[(producer.id, consumer1.id), (producer.id, consumer2.id)],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    yield graph, producer, consumer1, consumer2
    remove_job(storage, graph.id)


class TestMixedOutputTasks:
    """Tests for tasks that return both regular values and channel items from Sender arguments."""

    @pytest.mark.usefixtures("scheduler_worker")
    def test_mixed_output_tuple_with_sender(
        self,
        storage: SQLConnection,  # noqa: F811
        cpp_mixed_output_tuple_with_sender_job: tuple[TaskGraph, Task, Task],
    ) -> None:
        """
        Test task with Sender argument returning tuple<int, int>.

        mixed_output_tuple_with_sender_test(sender, count=4):
        - Sends 0,1,2,3 through sender
        - Returns tuple(4, 6) where 6 = 0+1+2+3
        - Outputs: [4, 6, channel_items...]

        Consumer receives 0+1+2+3=6 from channel.
        Tests that regular return values come before channel items from Sender arguments.
        """
        _, producer, consumer = cpp_mixed_output_tuple_with_sender_job

        # Wait for execution
        time.sleep(10)

        assert get_task_state(storage, producer.id) == "success"
        assert get_task_state(storage, consumer.id) == "success"

        # Producer outputs: first is count=4, second is sum=6
        producer_outputs = get_task_outputs(storage, producer.id)
        regular_outputs = [o for o in producer_outputs if o.channel_id is None]
        expected_num_regular_outputs = 2
        assert len(regular_outputs) >= expected_num_regular_outputs
        assert regular_outputs[0].value == msgpack.packb(4)
        assert regular_outputs[1].value == msgpack.packb(6)

        # Consumer receives 0+1+2+3=6
        consumer_outputs = get_task_outputs(storage, consumer.id)
        assert len(consumer_outputs) >= 1
        assert consumer_outputs[0].value == msgpack.packb(6)

    @pytest.mark.usefixtures("scheduler_worker")
    def test_mixed_output_multi_sender(
        self,
        storage: SQLConnection,  # noqa: F811
        cpp_mixed_output_multi_sender_job: tuple[TaskGraph, Task, Task, Task],
    ) -> None:
        """
        Test task with two Sender arguments returning int.

        mixed_output_multi_sender_test(sender1, sender2, count=3):
        - Sends 0,1,2 to sender1
        - Sends 0,2,4 to sender2
        - Returns 3 (count)
        - Outputs: [3, channel_items_1..., channel_items_2...]

        consumer1 receives 0+1+2=3 from channel1.
        consumer2 receives 0+2+4=6 from channel2.
        Tests multiple Senders with a return value.
        """
        _, producer, consumer1, consumer2 = cpp_mixed_output_multi_sender_job

        # Wait for execution
        time.sleep(15)

        assert get_task_state(storage, producer.id) == "success"
        assert get_task_state(storage, consumer1.id) == "success"
        assert get_task_state(storage, consumer2.id) == "success"

        # Producer outputs: first regular value is count=3
        producer_outputs = get_task_outputs(storage, producer.id)
        regular_outputs = [o for o in producer_outputs if o.channel_id is None]
        assert len(regular_outputs) >= 1
        assert regular_outputs[0].value == msgpack.packb(3)

        # consumer1 receives 0+1+2=3
        consumer1_outputs = get_task_outputs(storage, consumer1.id)
        assert len(consumer1_outputs) >= 1
        assert consumer1_outputs[0].value == msgpack.packb(3)

        # consumer2 receives 0+2+4=6
        consumer2_outputs = get_task_outputs(storage, consumer2.id)
        assert len(consumer2_outputs) >= 1
        assert consumer2_outputs[0].value == msgpack.packb(6)
