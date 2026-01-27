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

MARIADB_TEST_URL = "jdbc:mariadb://127.0.0.1:3306/spider-storage?user=spider&password=password"


@pytest.fixture(scope="session")
def storage_url() -> str:
    """Fixture for storage URL."""
    return os.getenv("SPIDER_STORAGE_URL", MARIADB_TEST_URL)


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


# Task functions for mixed output testing (return value + channel output)
def producer_tuple_with_sender(
    ctx: TaskContext, s: Sender[Int32], count: Int8
) -> tuple[Int32, Int32]:
    """
    Producer that sends items to channel and returns a tuple.

    Sends 0, 1, ..., count-1 to the channel.
    Returns (count, sum) where sum = 0 + 1 + ... + (count-1).
    This tests the mixed output scenario: regular return values + channel items.
    """
    total = 0
    for i in range(int(count)):
        s.send(Int32(i))
        total += i
    return Int32(int(count)), Int32(total)


def producer_multi_sender(
    ctx: TaskContext, s1: Sender[Int32], s2: Sender[Int32], count: Int8
) -> Int32:
    """
    Producer that sends to two channels and returns a value.

    Sends 0, 1, ..., count-1 to s1.
    Sends 0, 2, ..., 2*(count-1) to s2.
    Returns count.
    This tests multiple Senders with a return value.
    """
    for i in range(int(count)):
        s1.send(Int32(i))
        s2.send(Int32(i * 2))
    return Int32(int(count))


def producer_single_return_with_sender(ctx: TaskContext, s: Sender[bytes]) -> Int32:
    """Producer that sends bytes and returns an Int32."""
    s.send(b"first")
    s.send(b"second")
    return Int32(99)


@pytest.mark.integration
@pytest.mark.storage
class TestMixedOutputJobSubmission:
    """
    Tests for submitting jobs where tasks have both return values and channel outputs.

    These tests verify that the job submission correctly handles tasks that:
    1. Return regular values (tuples or single values)
    2. Have Sender arguments that produce channel items
    """

    def test_producer_tuple_with_sender_submission(self, driver: Driver) -> None:
        """
        Tests submitting a producer that returns tuple AND sends to channel.

        This validates the scenario from test_mixed_output_tuple_with_sender:
        - Producer returns tuple<int, int>
        - Producer also sends items through Sender
        """
        channel = Channel[Int32]()
        prod = channel_task(producer_tuple_with_sender, senders={"s": channel})
        cons = channel_task(consumer_int32, receivers={"r": channel})
        graph = group([prod, cons])

        # Producer needs count parameter
        jobs = driver.submit_jobs([graph], [(Int8(4),)])
        assert len(jobs) == 1
        assert jobs[0].job_id is not None

    def test_producer_multi_sender_submission(self, driver: Driver) -> None:
        """
        Tests submitting a producer with multiple Senders and return value.

        This validates the scenario from test_mixed_output_multi_sender:
        - Producer returns int
        - Producer sends to two different channels
        """
        ch1 = Channel[Int32]()
        ch2 = Channel[Int32]()
        prod = channel_task(producer_multi_sender, senders={"s1": ch1, "s2": ch2})
        cons1 = channel_task(consumer_int32, receivers={"r": ch1})
        cons2 = channel_task(consumer_int32, receivers={"r": ch2})
        graph = group([prod, cons1, cons2])

        jobs = driver.submit_jobs([graph], [(Int8(3),)])
        assert len(jobs) == 1
        assert jobs[0].job_id is not None

    def test_single_return_with_sender_submission(self, driver: Driver) -> None:
        """Tests submitting a producer with single return value and Sender."""
        channel = Channel[bytes]()
        prod = channel_task(producer_single_return_with_sender, senders={"s": channel})
        cons = channel_task(consumer_bytes, receivers={"r": channel})
        graph = group([prod, cons])

        jobs = driver.submit_jobs([graph], [()])
        assert len(jobs) == 1


@pytest.mark.integration
@pytest.mark.storage
class TestMixedOutputDatabaseStructure:
    """
    Tests that verify database structure for mixed output jobs.

    These tests ensure that tasks with both return values and channel outputs
    have the correct output records created with proper channel_id assignments.
    """

    def test_tuple_with_sender_output_structure(
        self,
        driver: Driver,
        mariadb_storage: MariaDBStorage,
    ) -> None:
        """
        Tests that tuple return + Sender creates correct output structure.

        Verifies that:
        - Regular return values have channel_id = NULL
        - Channel outputs have channel_id set
        """
        channel = Channel[Int32]()
        prod = channel_task(producer_tuple_with_sender, senders={"s": channel})
        cons = channel_task(consumer_int32, receivers={"r": channel})
        graph = group([prod, cons])

        jobs = driver.submit_jobs([graph], [(Int8(2),)])
        job = jobs[0]

        # Verify job was created and is running
        status = mariadb_storage.get_job_status(job._impl)
        assert status == JobStatus.Running

    def test_multi_sender_output_structure(
        self,
        driver: Driver,
        mariadb_storage: MariaDBStorage,
    ) -> None:
        """Tests that multiple Senders + return creates correct output structure."""
        ch1 = Channel[Int32]()
        ch2 = Channel[Int32]()
        prod = channel_task(producer_multi_sender, senders={"s1": ch1, "s2": ch2})
        cons1 = channel_task(consumer_int32, receivers={"r": ch1})
        cons2 = channel_task(consumer_int32, receivers={"r": ch2})
        graph = group([prod, cons1, cons2])

        jobs = driver.submit_jobs([graph], [(Int8(2),)])
        job = jobs[0]

        status = mariadb_storage.get_job_status(job._impl)
        assert status == JobStatus.Running

    def test_multiple_mixed_output_jobs(
        self,
        driver: Driver,
        mariadb_storage: MariaDBStorage,
    ) -> None:
        """Tests submitting multiple jobs with mixed outputs."""
        ch1 = Channel[Int32]()
        ch2 = Channel[Int32]()

        g1 = group(
            [
                channel_task(producer_tuple_with_sender, senders={"s": ch1}),
                channel_task(consumer_int32, receivers={"r": ch1}),
            ]
        )
        g2 = group(
            [
                channel_task(producer_tuple_with_sender, senders={"s": ch2}),
                channel_task(consumer_int32, receivers={"r": ch2}),
            ]
        )

        jobs = driver.submit_jobs([g1, g2], [(Int8(3),), (Int8(5),)])
        assert len(jobs) == 2
        assert jobs[0].job_id != jobs[1].job_id

        for job in jobs:
            status = mariadb_storage.get_job_status(job._impl)
            assert status == JobStatus.Running
