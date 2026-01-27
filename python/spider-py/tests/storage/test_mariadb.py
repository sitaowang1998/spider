"""Tests for the MariaDB storage backend."""

import os
from uuid import uuid4

import msgpack
import pytest

from spider_py import chain, group, Int8, TaskContext
from spider_py.core import (
    Data,
    DriverId,
    Job,
    JobStatus,
    Task,
    TaskGraph,
    TaskInput,
    TaskInputValue,
    TaskOutput,
)
from spider_py.storage import MariaDBStorage, parse_jdbc_url, StorageError

MariaDBTestUrl = "jdbc:mariadb://127.0.0.1:3306/spider-storage?user=spider&password=password"


@pytest.fixture(scope="session")
def mariadb_storage() -> MariaDBStorage:
    """Fixture to create a MariaDB storage instance."""
    url = os.getenv("SPIDER_STORAGE_URL", MariaDBTestUrl)
    params = parse_jdbc_url(url)
    return MariaDBStorage(params)


def double(_: TaskContext, x: Int8) -> Int8:
    """Double a number."""
    return Int8(x * 2)


def swap(_: TaskContext, x: Int8, y: Int8) -> tuple[Int8, Int8]:
    """Swaps two numbers."""
    return y, x


@pytest.fixture
def submit_job(mariadb_storage: MariaDBStorage) -> Job:
    """
    Fixture to submit a simple job to the MariaDB storage backend.
    The job composes of two parent tasks of `double` and a child task of `swap`.
    :param mariadb_storage:
    :return: The submitted job.
    """
    graph = chain(group([double, double]), group([swap]))._impl
    # Fill input data
    for i, task_index in enumerate(graph.input_task_indices):
        task = graph.tasks[task_index]
        task.task_inputs[0].value = TaskInputValue(msgpack.packb(i))

    driver_id = uuid4()
    jobs = mariadb_storage.submit_jobs(driver_id, [graph])
    return jobs[0]


@pytest.fixture
def driver(mariadb_storage: MariaDBStorage) -> DriverId:
    """Fixture to create a driver."""
    driver_id = uuid4()
    mariadb_storage.create_driver(driver_id)
    return driver_id


class TestMariaDBStorage:
    """Test class for the MariaDB storage backend."""

    @pytest.mark.storage
    def test_job_submission(self, mariadb_storage: MariaDBStorage) -> None:
        """Tests job submission to the MariaDB storage backend."""
        graph = chain(group([double, double, double, double]), group([swap, swap]))._impl
        # Fill input data
        for i, task_index in enumerate(graph.input_task_indices):
            task = graph.tasks[task_index]
            task.task_inputs[0].value = TaskInputValue(msgpack.packb(i))

        driver_id = uuid4()
        jobs = mariadb_storage.submit_jobs(driver_id, [graph])
        assert len(jobs) == 1

    @pytest.mark.storage
    def test_channel_submission(self, mariadb_storage: MariaDBStorage) -> None:
        """Tests channel rows are created during job submission."""
        channel_id = uuid4()
        graph = TaskGraph()
        producer = Task(function_name="producer")
        producer.task_outputs.append(TaskOutput(type="int", channel_id=channel_id))
        producer.task_outputs.append(TaskOutput(type="int", channel_id=channel_id))
        consumer = Task(function_name="consumer")
        consumer.task_inputs.append(TaskInput(type="int", value=None, channel_id=channel_id))
        graph.add_task(producer)
        graph.add_task(consumer)
        graph.input_task_indices = [0, 1]
        graph.output_task_indices = [0, 1]

        driver_id = uuid4()
        jobs = mariadb_storage.submit_jobs(driver_id, [graph])
        assert len(jobs) == 1

    @pytest.mark.storage
    def test_running_job_status(self, mariadb_storage: MariaDBStorage, submit_job: Job) -> None:
        """Tests getting status of a running job."""
        status = mariadb_storage.get_job_status(submit_job)
        assert status == JobStatus.Running

    @pytest.mark.storage
    def test_running_job_result(self, mariadb_storage: MariaDBStorage, submit_job: Job) -> None:
        """Tests getting results of a running job."""
        results = mariadb_storage.get_job_results(submit_job)
        assert results is None

    @pytest.mark.storage
    def test_data(self, mariadb_storage: MariaDBStorage, driver: DriverId) -> None:
        """Tests data storage and retrieval."""
        value = b"test data"
        data = Data(id=uuid4(), value=value, localities=["localhost"])
        mariadb_storage.create_data_with_driver_ref(driver, data)
        retrieved_data = mariadb_storage.get_data(data.id)
        assert retrieved_data is not None
        assert retrieved_data.id == data.id
        assert retrieved_data.value == value
        assert retrieved_data.hard_locality == data.hard_locality
        assert retrieved_data.localities == data.localities

    @pytest.mark.storage
    def test_create_data_fail(self, mariadb_storage: MariaDBStorage) -> None:
        """Tests creating data without a driver fails."""
        value = b"test data"
        data = Data(id=uuid4(), value=value, localities=["localhost"])
        with pytest.raises(StorageError):
            mariadb_storage.create_data_with_driver_ref(uuid4(), data)


def _get_output_task_id(mariadb_storage: MariaDBStorage, job: Job) -> bytes:
    """Helper to get the first output task ID from the database."""
    with mariadb_storage._conn.cursor() as cursor:  # noqa: SLF001
        cursor.execute(
            "SELECT `task_id` FROM `output_tasks` WHERE `job_id` = ? ORDER BY `position`",
            (job.job_id.bytes,),
        )
        row = cursor.fetchone()
        if row is None:
            msg = "No output task found"
            raise ValueError(msg)
        task_id: bytes = row[0]
        return task_id


class TestMixedChannelOutputs:
    """
    Tests for jobs with mixed channel outputs and regular return values.

    These tests verify that get_job_results() correctly filters out channel outputs
    and returns only regular return values (Bug 2 fix).
    """

    @pytest.mark.storage
    def test_get_job_results_filters_channel_outputs(self, mariadb_storage: MariaDBStorage) -> None:
        """
        Tests that get_job_results() filters out channel outputs.

        Creates a job where the output task has both regular outputs and channel
        outputs, verifies that only regular outputs are returned.
        """
        # Create a job with a task that has 3 regular outputs initially
        graph = TaskGraph()
        task = Task(function_name="mixed_output_producer")
        task.task_outputs.append(TaskOutput(type="int"))  # pos 0
        task.task_outputs.append(TaskOutput(type="int"))  # pos 1
        task.task_outputs.append(TaskOutput(type="int"))  # pos 2
        graph.add_task(task)
        graph.input_task_indices = [0]
        graph.output_task_indices = [0]

        driver_id = uuid4()
        jobs = mariadb_storage.submit_jobs(driver_id, [graph])
        job = jobs[0]

        # Get the actual task ID from the database (it's regenerated during submission)
        task_id = _get_output_task_id(mariadb_storage, job)

        # Manually update to simulate mixed outputs:
        # Position 0: regular output (value=10)
        # Position 1: channel output (should be filtered)
        # Position 2: regular output (value=20)
        with mariadb_storage._conn.cursor() as cursor:  # noqa: SLF001
            channel_id = uuid4()

            # Create the channel record first (required for foreign key)
            cursor.execute(
                "INSERT INTO `channels` (`id`, `job_id`, `type`) VALUES (?, ?, ?)",
                (channel_id.bytes, job.job_id.bytes, "int"),
            )

            # Update regular outputs with values
            cursor.execute(
                "UPDATE `task_outputs` SET `value` = ? WHERE `task_id` = ? AND `position` = ?",
                (msgpack.packb(10), task_id, 0),
            )
            cursor.execute(
                "UPDATE `task_outputs` SET `value` = ? WHERE `task_id` = ? AND `position` = ?",
                (msgpack.packb(20), task_id, 2),
            )
            # Set middle output as channel output (no value, just channel_id)
            cursor.execute(
                "UPDATE `task_outputs` SET `channel_id` = ? WHERE `task_id` = ? AND `position` = ?",
                (channel_id.bytes, task_id, 1),
            )
            # Update task state to success
            cursor.execute(
                "UPDATE `tasks` SET `state` = 'success' WHERE `id` = ?",
                (task_id,),
            )
            # Update job state to success
            cursor.execute(
                "UPDATE `jobs` SET `state` = 'success' WHERE `id` = ?",
                (job.job_id.bytes,),
            )
            mariadb_storage._conn.commit()  # noqa: SLF001

        # Get job results - should only return regular outputs (positions 0 and 2)
        results = mariadb_storage.get_job_results(job)

        assert results is not None
        assert len(results) == 2  # Only 2 regular outputs, channel output filtered
        assert isinstance(results[0].value, bytes)
        assert isinstance(results[1].value, bytes)
        assert msgpack.unpackb(results[0].value) == 10
        assert msgpack.unpackb(results[1].value) == 20

    @pytest.mark.storage
    def test_get_job_results_with_multiple_channel_outputs(
        self, mariadb_storage: MariaDBStorage
    ) -> None:
        """
        Tests get_job_results with multiple channel outputs.

        Creates a job where the output task has one regular output followed by
        multiple channel outputs (like a producer with multiple Senders).
        """
        # Create a job with 4 outputs initially
        graph = TaskGraph()
        task = Task(function_name="multi_sender_producer")
        for _ in range(4):
            task.task_outputs.append(TaskOutput(type="int"))
        graph.add_task(task)
        graph.input_task_indices = [0]
        graph.output_task_indices = [0]

        driver_id = uuid4()
        jobs = mariadb_storage.submit_jobs(driver_id, [graph])
        job = jobs[0]

        # Get the actual task ID from the database
        task_id = _get_output_task_id(mariadb_storage, job)

        # Manually update: [regular, channel1, channel1, channel2]
        with mariadb_storage._conn.cursor() as cursor:  # noqa: SLF001
            channel_id_1 = uuid4()
            channel_id_2 = uuid4()

            # Create the channel records
            cursor.execute(
                "INSERT INTO `channels` (`id`, `job_id`, `type`) VALUES (?, ?, ?)",
                (channel_id_1.bytes, job.job_id.bytes, "int"),
            )
            cursor.execute(
                "INSERT INTO `channels` (`id`, `job_id`, `type`) VALUES (?, ?, ?)",
                (channel_id_2.bytes, job.job_id.bytes, "int"),
            )

            # Update regular output with value
            cursor.execute(
                "UPDATE `task_outputs` SET `value` = ? WHERE `task_id` = ? AND `position` = ?",
                (msgpack.packb(42), task_id, 0),
            )
            # Set positions 1, 2, 3 as channel outputs
            cursor.execute(
                "UPDATE `task_outputs` SET `channel_id` = ? WHERE `task_id` = ? AND `position` = ?",
                (channel_id_1.bytes, task_id, 1),
            )
            cursor.execute(
                "UPDATE `task_outputs` SET `channel_id` = ? WHERE `task_id` = ? AND `position` = ?",
                (channel_id_1.bytes, task_id, 2),
            )
            cursor.execute(
                "UPDATE `task_outputs` SET `channel_id` = ? WHERE `task_id` = ? AND `position` = ?",
                (channel_id_2.bytes, task_id, 3),
            )
            # Update task and job state
            cursor.execute(
                "UPDATE `tasks` SET `state` = 'success' WHERE `id` = ?",
                (task_id,),
            )
            cursor.execute(
                "UPDATE `jobs` SET `state` = 'success' WHERE `id` = ?",
                (job.job_id.bytes,),
            )
            mariadb_storage._conn.commit()  # noqa: SLF001

        # Get job results - should only return the single regular output
        results = mariadb_storage.get_job_results(job)

        assert results is not None
        assert len(results) == 1
        assert isinstance(results[0].value, bytes)
        assert msgpack.unpackb(results[0].value) == 42

    @pytest.mark.storage
    def test_get_job_results_tuple_with_channel(self, mariadb_storage: MariaDBStorage) -> None:
        """
        Tests get_job_results for task returning tuple AND sending to channel.

        Simulates the mixed_output_tuple_with_sender_test scenario:
        - Task returns tuple<int, int> AND has Sender argument
        - Outputs: [int, int, channel_items...]
        - get_job_results should return only the two tuple elements
        """
        # Create a job with 6 outputs initially
        graph = TaskGraph()
        task = Task(function_name="tuple_with_sender")
        for _ in range(6):
            task.task_outputs.append(TaskOutput(type="int"))
        graph.add_task(task)
        graph.input_task_indices = [0]
        graph.output_task_indices = [0]

        driver_id = uuid4()
        jobs = mariadb_storage.submit_jobs(driver_id, [graph])
        job = jobs[0]

        # Get the actual task ID from the database
        task_id = _get_output_task_id(mariadb_storage, job)

        # Manually update: positions 0-1 are tuple return values, 2-5 are channel items
        with mariadb_storage._conn.cursor() as cursor:  # noqa: SLF001
            channel_id = uuid4()

            # Create the channel record
            cursor.execute(
                "INSERT INTO `channels` (`id`, `job_id`, `type`) VALUES (?, ?, ?)",
                (channel_id.bytes, job.job_id.bytes, "int"),
            )

            # Update regular outputs: count=4, sum=6
            cursor.execute(
                "UPDATE `task_outputs` SET `value` = ? WHERE `task_id` = ? AND `position` = ?",
                (msgpack.packb(4), task_id, 0),
            )
            cursor.execute(
                "UPDATE `task_outputs` SET `value` = ? WHERE `task_id` = ? AND `position` = ?",
                (msgpack.packb(6), task_id, 1),
            )
            # Set positions 2-5 as channel outputs
            for pos in range(2, 6):
                cursor.execute(
                    "UPDATE `task_outputs` SET `channel_id` = ? "
                    "WHERE `task_id` = ? AND `position` = ?",
                    (channel_id.bytes, task_id, pos),
                )
            # Update task and job state
            cursor.execute(
                "UPDATE `tasks` SET `state` = 'success' WHERE `id` = ?",
                (task_id,),
            )
            cursor.execute(
                "UPDATE `jobs` SET `state` = 'success' WHERE `id` = ?",
                (job.job_id.bytes,),
            )
            mariadb_storage._conn.commit()  # noqa: SLF001

        # Get job results - should return tuple(4, 6)
        results = mariadb_storage.get_job_results(job)

        assert results is not None
        assert len(results) == 2
        assert isinstance(results[0].value, bytes)
        assert isinstance(results[1].value, bytes)
        assert msgpack.unpackb(results[0].value) == 4  # count
        assert msgpack.unpackb(results[1].value) == 6  # sum

    @pytest.mark.storage
    def test_get_job_results_no_channel_outputs(self, mariadb_storage: MariaDBStorage) -> None:
        """
        Tests get_job_results works correctly when there are no channel outputs.

        Verifies the fix doesn't break normal jobs without channels.
        """
        graph = TaskGraph()
        task = Task(function_name="regular_task")
        task.task_outputs.append(TaskOutput(type="int"))
        task.task_outputs.append(TaskOutput(type="int"))
        graph.add_task(task)
        graph.input_task_indices = [0]
        graph.output_task_indices = [0]

        driver_id = uuid4()
        jobs = mariadb_storage.submit_jobs(driver_id, [graph])
        job = jobs[0]

        # Get the actual task ID from the database
        task_id = _get_output_task_id(mariadb_storage, job)

        # Manually update to simulate completion
        with mariadb_storage._conn.cursor() as cursor:  # noqa: SLF001
            cursor.execute(
                "UPDATE `task_outputs` SET `value` = ? WHERE `task_id` = ? AND `position` = ?",
                (msgpack.packb(100), task_id, 0),
            )
            cursor.execute(
                "UPDATE `task_outputs` SET `value` = ? WHERE `task_id` = ? AND `position` = ?",
                (msgpack.packb(200), task_id, 1),
            )
            cursor.execute(
                "UPDATE `tasks` SET `state` = 'success' WHERE `id` = ?",
                (task_id,),
            )
            cursor.execute(
                "UPDATE `jobs` SET `state` = 'success' WHERE `id` = ?",
                (job.job_id.bytes,),
            )
            mariadb_storage._conn.commit()  # noqa: SLF001

        results = mariadb_storage.get_job_results(job)

        assert results is not None
        assert len(results) == 2
        assert isinstance(results[0].value, bytes)
        assert isinstance(results[1].value, bytes)
        assert msgpack.unpackb(results[0].value) == 100
        assert msgpack.unpackb(results[1].value) == 200
