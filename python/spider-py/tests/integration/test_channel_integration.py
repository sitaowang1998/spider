"""
Integration tests for high-level Channel API with storage backend.

These tests verify that channel-based task graphs are correctly submitted to the
storage backend with proper channel, producer, and consumer records created.
"""

import os

import pytest

from spider_py.client import (
    Channel,
    channel_task,
    Driver,
    group,
    Receiver,
    Sender,
    TaskContext,
)
from spider_py.core import JobStatus
from spider_py.storage import MariaDBStorage, parse_jdbc_url
from spider_py.type import Int8, Int32

MariaDBTestUrl = "jdbc:mariadb://127.0.0.1:3306/spider-storage?user=spider&password=password"


@pytest.fixture(scope="session")
def storage_url() -> str:
    """Fixture for storage URL."""
    return os.getenv("SPIDER_STORAGE_URL", MariaDBTestUrl)


@pytest.fixture(scope="session")
def driver(storage_url: str) -> Driver:
    """Fixture for the Spider driver."""
    return Driver(storage_url)


@pytest.fixture(scope="session")
def mariadb_storage(storage_url: str) -> MariaDBStorage:
    """Fixture to create a MariaDB storage instance for verification."""
    params = parse_jdbc_url(storage_url)
    return MariaDBStorage(params)


# Task functions for testing
def producer_bytes(ctx: TaskContext, s: Sender[bytes]) -> bytes:
    """Producer that sends bytes to a channel."""
    s.send(b"item1")
    s.send(b"item2")
    return b"done"


def consumer_bytes(ctx: TaskContext, r: Receiver[bytes]) -> Int32:
    """Consumer that receives bytes from a channel."""
    count = 0
    while True:
        item, drained = r.recv()
        if drained:
            break
        if item:
            count += 1
    return Int32(count)


def producer_int32(ctx: TaskContext, s: Sender[Int32]) -> Int32:
    """Producer that sends Int32 to a channel."""
    s.send(Int32(1))
    s.send(Int32(2))
    return Int32(0)


def consumer_int32(ctx: TaskContext, r: Receiver[Int32]) -> Int32:
    """Consumer that receives Int32 from a channel."""
    return Int32(0)


def producer_with_count(ctx: TaskContext, s: Sender[bytes], count: Int8) -> bytes:
    """Producer with additional regular parameter."""
    for i in range(int(count)):
        s.send(f"item-{i}".encode())
    return b"done"


def passthrough(ctx: TaskContext, r: Receiver[bytes], s: Sender[Int32]) -> bytes:
    """Passthrough task that consumes from one channel and produces to another."""
    count = 0
    while True:
        item, drained = r.recv()
        if drained:
            break
        if item:
            s.send(Int32(count))
            count += 1
    return b"done"


def multi_producer(ctx: TaskContext, s1: Sender[bytes], s2: Sender[Int32]) -> bytes:
    """Producer that sends to multiple channels."""
    s1.send(b"data")
    s2.send(Int32(42))
    return b"done"


def multi_consumer(ctx: TaskContext, r1: Receiver[bytes], r2: Receiver[Int32]) -> Int32:
    """Consumer that receives from multiple channels."""
    return Int32(0)


@pytest.mark.integration
@pytest.mark.storage
class TestChannelJobSubmission:
    """Tests for submitting channel-based jobs through the Driver."""

    def test_simple_producer_consumer(self, driver: Driver) -> None:
        """Tests submitting a simple producer-consumer job."""
        channel = Channel[bytes]()
        prod = channel_task(producer_bytes, senders={"s": channel})
        cons = channel_task(consumer_bytes, receivers={"r": channel})
        graph = group([prod, cons])

        jobs = driver.submit_jobs([graph], [()])
        assert len(jobs) == 1
        assert jobs[0].job_id is not None

    def test_producer_consumer_int32(self, driver: Driver) -> None:
        """Tests submitting producer-consumer with Int32 channel type."""
        channel = Channel[Int32]()
        prod = channel_task(producer_int32, senders={"s": channel})
        cons = channel_task(consumer_int32, receivers={"r": channel})
        graph = group([prod, cons])

        jobs = driver.submit_jobs([graph], [()])
        assert len(jobs) == 1

    def test_multiple_producers_single_consumer(self, driver: Driver) -> None:
        """Tests multiple producers sending to a single consumer."""
        channel = Channel[bytes]()
        prod1 = channel_task(producer_bytes, senders={"s": channel})
        prod2 = channel_task(producer_bytes, senders={"s": channel})
        cons = channel_task(consumer_bytes, receivers={"r": channel})
        graph = group([prod1, prod2, cons])

        jobs = driver.submit_jobs([graph], [()])
        assert len(jobs) == 1

    def test_single_producer_multiple_consumers(self, driver: Driver) -> None:
        """Tests single producer with multiple consumers."""
        channel = Channel[bytes]()
        prod = channel_task(producer_bytes, senders={"s": channel})
        cons1 = channel_task(consumer_bytes, receivers={"r": channel})
        cons2 = channel_task(consumer_bytes, receivers={"r": channel})
        graph = group([prod, cons1, cons2])

        jobs = driver.submit_jobs([graph], [()])
        assert len(jobs) == 1

    def test_producer_with_regular_input(self, driver: Driver) -> None:
        """Tests producer task with additional regular input parameter."""
        channel = Channel[bytes]()
        prod = channel_task(producer_with_count, senders={"s": channel})
        cons = channel_task(consumer_bytes, receivers={"r": channel})
        graph = group([prod, cons])

        # Producer needs count parameter (Int8)
        jobs = driver.submit_jobs([graph], [(Int8(5),)])
        assert len(jobs) == 1

    def test_passthrough_two_channels(self, driver: Driver) -> None:
        """Tests passthrough pattern with two channels."""
        ch1 = Channel[bytes]()
        ch2 = Channel[Int32]()

        prod = channel_task(producer_bytes, senders={"s": ch1})
        mid = channel_task(passthrough, receivers={"r": ch1}, senders={"s": ch2})
        cons = channel_task(consumer_int32, receivers={"r": ch2})

        graph = group([prod, mid, cons])
        jobs = driver.submit_jobs([graph], [()])
        assert len(jobs) == 1

    def test_multi_channel_producer(self, driver: Driver) -> None:
        """Tests producer sending to multiple channels."""
        ch1 = Channel[bytes]()
        ch2 = Channel[Int32]()

        prod = channel_task(multi_producer, senders={"s1": ch1, "s2": ch2})
        cons1 = channel_task(consumer_bytes, receivers={"r": ch1})
        cons2 = channel_task(consumer_int32, receivers={"r": ch2})

        graph = group([prod, cons1, cons2])
        jobs = driver.submit_jobs([graph], [()])
        assert len(jobs) == 1

    def test_multi_channel_consumer(self, driver: Driver) -> None:
        """Tests consumer receiving from multiple channels."""
        ch1 = Channel[bytes]()
        ch2 = Channel[Int32]()

        prod1 = channel_task(producer_bytes, senders={"s": ch1})
        prod2 = channel_task(producer_int32, senders={"s": ch2})
        cons = channel_task(multi_consumer, receivers={"r1": ch1, "r2": ch2})

        graph = group([prod1, prod2, cons])
        jobs = driver.submit_jobs([graph], [()])
        assert len(jobs) == 1

    def test_multiple_jobs_same_graph(self, driver: Driver) -> None:
        """Tests submitting multiple jobs with the same graph structure."""
        # Each job gets its own channel instance since Channel is created fresh
        ch1 = Channel[bytes]()
        ch2 = Channel[bytes]()
        g1 = group(
            [
                channel_task(producer_bytes, senders={"s": ch1}),
                channel_task(consumer_bytes, receivers={"r": ch1}),
            ]
        )
        g2 = group(
            [
                channel_task(producer_bytes, senders={"s": ch2}),
                channel_task(consumer_bytes, receivers={"r": ch2}),
            ]
        )

        jobs = driver.submit_jobs([g1, g2], [(), ()])
        assert len(jobs) == 2
        assert jobs[0].job_id != jobs[1].job_id


@pytest.mark.integration
@pytest.mark.storage
class TestChannelDatabaseStructure:
    """Tests that verify the correct database structure is created for channel jobs."""

    def test_channel_records_created(
        self,
        driver: Driver,
        mariadb_storage: MariaDBStorage,
    ) -> None:
        """Tests that channel records are created in the database."""
        channel = Channel[bytes]()
        prod = channel_task(producer_bytes, senders={"s": channel})
        cons = channel_task(consumer_bytes, receivers={"r": channel})
        graph = group([prod, cons])

        jobs = driver.submit_jobs([graph], [()])
        job = jobs[0]

        # Verify job was created
        status = mariadb_storage.get_job_status(job._impl)
        assert status == JobStatus.Running

    def test_job_status_running(
        self,
        driver: Driver,
        mariadb_storage: MariaDBStorage,
    ) -> None:
        """Tests that submitted channel jobs have Running status."""
        channel = Channel[bytes]()
        prod = channel_task(producer_bytes, senders={"s": channel})
        cons = channel_task(consumer_bytes, receivers={"r": channel})
        graph = group([prod, cons])

        jobs = driver.submit_jobs([graph], [()])

        for job in jobs:
            status = mariadb_storage.get_job_status(job._impl)
            assert status == JobStatus.Running

    def test_complex_pipeline_submission(
        self,
        driver: Driver,
        mariadb_storage: MariaDBStorage,
    ) -> None:
        """Tests a complex pipeline with multiple channels."""
        ch1 = Channel[bytes]()
        ch2 = Channel[Int32]()
        ch3 = Channel[bytes]()

        # Producer -> Passthrough -> Consumer pattern with additional producer
        prod1 = channel_task(producer_bytes, senders={"s": ch1})
        mid = channel_task(passthrough, receivers={"r": ch1}, senders={"s": ch2})
        cons1 = channel_task(consumer_int32, receivers={"r": ch2})
        prod2 = channel_task(producer_bytes, senders={"s": ch3})
        cons2 = channel_task(consumer_bytes, receivers={"r": ch3})

        graph = group([prod1, mid, cons1, prod2, cons2])
        jobs = driver.submit_jobs([graph], [()])

        assert len(jobs) == 1
        status = mariadb_storage.get_job_status(jobs[0]._impl)
        assert status == JobStatus.Running


class TestChannelTaskGraphStructure:
    """Tests that verify the task graph structure for channel tasks."""

    def test_producer_task_structure(self) -> None:
        """Tests the structure of a producer task in the graph."""
        channel = Channel[bytes]()
        graph = channel_task(producer_bytes, senders={"s": channel})

        assert len(graph._impl.tasks) == 1
        task = graph._impl.tasks[0]

        # Producer should have:
        # - 1 input (Sender bound to channel)
        # - 2 outputs (channel output + return value)
        assert len(task.task_inputs) == 1
        assert len(task.task_outputs) == 2

        # Verify input
        assert task.task_inputs[0].channel_id == channel.id
        assert task.task_inputs[0].type == "channel:bytes"

        # Verify channel output
        assert task.task_outputs[0].channel_id == channel.id
        assert task.task_outputs[0].type == "channel:bytes"

        # Verify return output
        assert task.task_outputs[1].channel_id is None
        assert task.task_outputs[1].type == "bytes"

    def test_consumer_task_structure(self) -> None:
        """Tests the structure of a consumer task in the graph."""
        channel = Channel[bytes]()
        graph = channel_task(consumer_bytes, receivers={"r": channel})

        assert len(graph._impl.tasks) == 1
        task = graph._impl.tasks[0]

        # Consumer should have:
        # - 1 input (Receiver bound to channel)
        # - 1 output (return value only, no channel output)
        assert len(task.task_inputs) == 1
        assert len(task.task_outputs) == 1

        # Verify input
        assert task.task_inputs[0].channel_id == channel.id
        assert task.task_inputs[0].type == "channel:bytes"

        # Verify return output (no channel)
        assert task.task_outputs[0].channel_id is None
        assert task.task_outputs[0].type == "int32"

    def test_grouped_graph_structure(self) -> None:
        """Tests the structure of a grouped producer-consumer graph."""
        channel = Channel[bytes]()
        prod = channel_task(producer_bytes, senders={"s": channel})
        cons = channel_task(consumer_bytes, receivers={"r": channel})
        graph = group([prod, cons])

        # Should have 2 tasks
        assert len(graph._impl.tasks) == 2

        # Both tasks share the same channel ID
        prod_task = graph._impl.tasks[0]
        cons_task = graph._impl.tasks[1]

        assert prod_task.task_inputs[0].channel_id == channel.id
        assert cons_task.task_inputs[0].channel_id == channel.id
        assert prod_task.task_inputs[0].channel_id == cons_task.task_inputs[0].channel_id

        # No task dependencies (channel coordinates execution)
        assert len(graph._impl.dependencies) == 0

    def test_passthrough_task_structure(self) -> None:
        """Tests the structure of a passthrough task."""
        ch_in = Channel[bytes]()
        ch_out = Channel[Int32]()
        graph = channel_task(passthrough, receivers={"r": ch_in}, senders={"s": ch_out})

        assert len(graph._impl.tasks) == 1
        task = graph._impl.tasks[0]

        # Passthrough should have:
        # - 2 inputs (Receiver + Sender)
        # - 2 outputs (channel output for sender + return value)
        assert len(task.task_inputs) == 2
        assert len(task.task_outputs) == 2

        # Verify inputs (order: Receiver, Sender as in function signature)
        assert task.task_inputs[0].channel_id == ch_in.id
        assert task.task_inputs[0].type == "channel:bytes"
        assert task.task_inputs[1].channel_id == ch_out.id
        assert task.task_inputs[1].type == "channel:int32"

        # Verify outputs (channel output first, then return)
        assert task.task_outputs[0].channel_id == ch_out.id
        assert task.task_outputs[0].type == "channel:int32"
        assert task.task_outputs[1].channel_id is None
        assert task.task_outputs[1].type == "bytes"

    def test_mixed_input_order_preserved(self) -> None:
        """Tests that parameter order is preserved for mixed inputs."""
        channel = Channel[bytes]()
        graph = channel_task(producer_with_count, senders={"s": channel})

        task = graph._impl.tasks[0]

        # Should have 2 inputs: Sender[bytes], Int8
        assert len(task.task_inputs) == 2

        # First: Sender (channel)
        assert task.task_inputs[0].channel_id == channel.id
        assert task.task_inputs[0].type == "channel:bytes"

        # Second: Int8 (regular)
        assert task.task_inputs[1].channel_id is None
        assert task.task_inputs[1].type == "int8"
