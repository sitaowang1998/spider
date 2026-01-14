"""MariaDB Storage module."""

from collections.abc import Sequence
from typing import Literal, TypedDict
from uuid import UUID, uuid4

import mariadb
from typing_extensions import override

from spider_py import core
from spider_py.storage.jdbc_url import JdbcParameters
from spider_py.storage.storage import Storage, StorageError

InsertJob = """
INSERT INTO
  `jobs` (`id`, `client_id`)
VALUES
  (?, ?)"""

InsertTask = """
INSERT INTO
  `tasks` (`id`, `job_id`, `func_name`, `language`, `state`, `timeout`, `max_retry`)
VALUES
  (?, ?, ?, ?, ?, ?, ?)"""

InsertTaskInputOutput = """
INSERT INTO
  `task_inputs` (`task_id`, `position`, `type`, `output_task_id`, `output_task_position`,
  `channel_id`)
VALUES
  (?, ?, ?, ?, ?, ?)"""

InsertTaskInputData = """
INSERT INTO
  `task_inputs` (`task_id`, `position`, `type`, `data_id`, `channel_id`)
VALUES
  (?, ?, ?, ?, ?)"""

InsertTaskInputValue = """
INSERT INTO
  `task_inputs` (`task_id`, `position`, `type`, `value`, `channel_id`)
VALUES
  (?, ?, ?, ?, ?)"""

InsertTaskOutput = """
INSERT INTO
  `task_outputs` (`task_id`, `position`, `type`, `channel_id`)
VALUES
  (?, ?, ?, ?)"""

InsertTaskInputChannel = """
INSERT INTO
  `task_inputs` (`task_id`, `position`, `type`, `channel_id`)
VALUES
  (?, ?, ?, ?)"""

InsertTaskDependency = """
INSERT INTO
  `task_dependencies` (parent, child)
VALUES
  (?, ?)"""

InsertInputTask = """
INSERT INTO
  `input_tasks` (`job_id`, `task_id`, `position`)
VALUES
  (?, ?, ?)"""

InsertOutputTask = """
INSERT INTO
  `output_tasks` (`job_id`, `task_id`, `position`)
VALUES
  (?, ?, ?)"""

InsertChannel = """
INSERT INTO
  `channels` (`id`, `job_id`, `type`)
VALUES
  (?, ?, ?)"""

InsertChannelProducer = """
INSERT INTO
  `channel_producers` (`channel_id`, `task_id`)
VALUES
  (?, ?)"""

InsertChannelConsumer = """
INSERT INTO
  `channel_consumers` (`channel_id`, `task_id`)
VALUES
  (?, ?)"""


class _ChannelInfo(TypedDict):
    type: str
    producers: set[bytes]
    consumers: set[bytes]


GetJobStatus = """
SELECT
  `state`
FROM
  `jobs`
WHERE
  `id` = ?"""

GetOutputTasks = """
SELECT
  `task_id`
FROM
  `output_tasks`
WHERE
  `job_id` = ?
ORDER BY
  `position`"""

GetTaskOutputs = """
SELECT
  `type`,
  `value`,
  `data_id`
FROM
  `task_outputs`
WHERE
  `task_id` = ?
ORDER BY
  `position`"""

InsertData = """
INSERT INTO
  `data` (`id`, `value`, `hard_locality`)
VALUES
  (?, ?, ?)"""

InsertDataLocality = """
INSERT INTO
  `data_locality` (`id`, `address`)
VALUES
  (?, ?)"""

InsertDataRefDriver = """
INSERT INTO
  `data_ref_driver` (`id`, `driver_id`)
VALUES
  (?, ?)"""

InsertDataRefTask = """
INSERT INTO
  `data_ref_task` (`id`, `task_id`)
VALUES
  (?, ?)"""

GetData = """
SELECT
  `value`,
  `hard_locality`
FROM
  `data`
WHERE
  `id` = ?"""

GetDataLocality = """
SELECT
  `address`
FROM
  `data_locality`
WHERE
  `id` = ?"""

InsertDriver = """
INSERT INTO
  `drivers` (`id`)
VALUES
  (?)"""

SelectChannelItemForUpdate = """
SELECT
  `producer_task_id`,
  `item_index`,
  `value`,
  `data_id`
FROM
  `channel_items`
WHERE
  `channel_id` = ? AND `delivered_to_task_id` IS NULL
ORDER BY
  `item_index`
LIMIT 1 FOR UPDATE"""

UpdateChannelItemDelivery = """
UPDATE
  `channel_items`
SET
  `delivered_to_task_id` = ?
WHERE
  `channel_id` = ? AND `producer_task_id` = ? AND `item_index` = ?
  AND `delivered_to_task_id` IS NULL"""

_StrToJobStatusMap = {
    "running": core.JobStatus.Running,
    "success": core.JobStatus.Succeeded,
    "fail": core.JobStatus.Failed,
    "cancel": core.JobStatus.Cancelled,
}


class MariaDBStorage(Storage):
    """MariaDB Storage class."""

    def __del__(self) -> None:
        """
        Closes the connection to the MariaDB database.
        :raises StorageError: If closing the connection fails.
        """
        self._conn.close()

    def __init__(self, params: JdbcParameters) -> None:
        """
        Connects to the MariaDB database.
        :param params: The JDBC parameters for connecting to the database.
        :raises StorageError: If the connection to the database fails.
        """
        try:
            self._conn = mariadb.connect(
                host=params.host,
                port=params.port,
                user=params.user,
                password=params.password,
                database=params.database,
            )
        except mariadb.Error as e:
            raise StorageError(str(e)) from e

    @override
    def submit_jobs(
        self, driver_id: core.DriverId, task_graphs: Sequence[core.TaskGraph]
    ) -> Sequence[core.Job]:
        if not task_graphs:
            return []
        try:
            # Create job UUIDs and task UUIDs.
            jobs = []
            task_ids = []
            for task_graph in task_graphs:
                jobs.append(core.Job(uuid4()))
                task_ids.append([uuid4() for _ in task_graph.tasks])
            channel_params, channel_producer_params, channel_consumer_params = (
                self._gen_channel_insertion_params(jobs, task_ids, task_graphs)
            )

            with self._conn.cursor() as cursor:
                # Insert jobs table
                cursor.executemany(InsertJob, [(job.job_id.bytes, driver_id.bytes) for job in jobs])
                # Insert tasks table
                cursor.executemany(
                    InsertTask,
                    self._gen_task_insertion_params(jobs, task_ids, task_graphs),
                )

                # Insert channels tables
                self._execute_optional_batch(cursor, InsertChannel, channel_params)
                self._execute_optional_batch(cursor, InsertChannelProducer, channel_producer_params)
                self._execute_optional_batch(cursor, InsertChannelConsumer, channel_consumer_params)

                # Insert task dependencies table
                dep_params = self._gen_task_dependency_insertion_params(task_ids, task_graphs)
                self._execute_optional_batch(cursor, InsertTaskDependency, dep_params)

                # Insert input tasks table
                cursor.executemany(
                    InsertInputTask,
                    self._gen_input_task_insertion_params(jobs, task_ids, task_graphs),
                )

                # Insert output tasks table
                cursor.executemany(
                    InsertOutputTask,
                    self._gen_output_task_insertion_params(jobs, task_ids, task_graphs),
                )

                # Insert task outputs table
                cursor.executemany(
                    InsertTaskOutput,
                    self._gen_task_output_insertion_params(task_ids, task_graphs),
                )

                # Insert task input data table
                input_data_params = self._gen_task_input_data_insertion_params(
                    task_ids, task_graphs
                )
                self._execute_optional_batch(cursor, InsertTaskInputData, input_data_params)

                # Insert task input values table
                input_value_params = self._gen_task_input_value_insertion_params(
                    task_ids, task_graphs
                )
                self._execute_optional_batch(cursor, InsertTaskInputValue, input_value_params)

                # Insert task input channels table
                input_channel_params = self._gen_task_input_channel_insertion_params(
                    task_ids, task_graphs
                )
                self._execute_optional_batch(cursor, InsertTaskInputChannel, input_channel_params)

                # Insert task input outputs table
                input_output_params = self._gen_task_input_output_ref_insertion_params(
                    task_ids, task_graphs
                )
                self._execute_optional_batch(cursor, InsertTaskInputOutput, input_output_params)

                self._conn.commit()
                return jobs
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e
        except StorageError:
            self._conn.rollback()
            raise

    @override
    def get_job_status(self, job: core.Job) -> core.JobStatus:
        try:
            with self._conn.cursor() as cursor:
                status = self._get_job_status(cursor, job)
                self._conn.commit()
                return status
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e
        except StorageError:
            self._conn.rollback()
            raise

    @override
    def get_job_results(self, job: core.Job) -> list[core.TaskOutput] | None:
        try:
            with self._conn.cursor() as cursor:
                status = self._get_job_status(cursor, job)
                if status != core.JobStatus.Succeeded:
                    self._conn.commit()
                    return None

                cursor.execute(GetOutputTasks, (job.job_id.bytes,))
                task_ids = [task_id for (task_id,) in cursor.fetchall()]

                results = []
                for task_id in task_ids:
                    cursor.execute(GetTaskOutputs, (task_id,))
                    for output_type, value, data_id in cursor.fetchall():
                        if value is not None:
                            results.append(
                                core.TaskOutput(
                                    type=output_type,
                                    value=core.TaskOutputValue(value),
                                )
                            )
                        elif data_id is not None:
                            data = self._get_data(cursor, core.DataId(bytes=data_id))
                            results.append(
                                core.TaskOutput(
                                    type=output_type,
                                    value=data,
                                )
                            )
                        else:
                            msg = "Invalid task output"
                            _raise_storage_error(msg)
                self._conn.commit()
                return results
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e
        except StorageError:
            self._conn.rollback()
            raise

    @override
    def create_data_with_driver_ref(self, driver_id: core.DriverId, data: core.Data) -> None:
        self._create_data_with_ref(data, InsertDataRefDriver, driver_id)

    @override
    def create_data_with_task_ref(self, task_id: core.TaskId, data: core.Data) -> None:
        self._create_data_with_ref(data, InsertDataRefTask, task_id)

    @override
    def get_data(self, data_id: core.DataId) -> core.Data:
        try:
            with self._conn.cursor() as cursor:
                data = self._get_data(cursor, data_id)
                self._conn.commit()
                return data
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e

    @override
    def create_driver(self, driver_id: core.DriverId) -> None:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(InsertDriver, (driver_id.bytes,))
                self._conn.commit()
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e

    @override
    def dequeue_channel_item(
        self,
        channel_id: core.ChannelId,
        consumer_task_id: core.TaskId,
    ) -> tuple[core.ChannelItem | None, bool]:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(SelectChannelItemForUpdate, (channel_id.bytes,))
                row = cursor.fetchone()
                if row is None:
                    cursor.execute(
                        "SELECT `sender_closed` FROM `channels` WHERE `id` = ?",
                        (channel_id.bytes,),
                    )
                    closed_row = cursor.fetchone()
                    if closed_row is None:
                        msg = f"Channel {channel_id} not found."
                        raise StorageError(msg)
                    drained = bool(closed_row[0])
                    self._conn.commit()
                    return None, drained
                producer_task_id_bytes, item_index, value, data_id = row
                cursor.execute(
                    UpdateChannelItemDelivery,
                    (
                        consumer_task_id.bytes,
                        channel_id.bytes,
                        producer_task_id_bytes,
                        item_index,
                    ),
                )
                if cursor.rowcount == 0:
                    self._conn.commit()
                    return None, False
                item = core.ChannelItem(
                    channel_id=channel_id,
                    producer_task_id=UUID(bytes=producer_task_id_bytes),
                    item_index=item_index,
                    value=value,
                    data_id=UUID(bytes=data_id) if data_id is not None else None,
                    delivered_to_task_id=consumer_task_id,
                )
                self._conn.commit()
                return item, False
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e

    def _create_data_with_ref(
        self, data: core.Data, insert_stmt: str, ref_id: core.DriverId | core.TaskId
    ) -> None:
        """
        Creates a data object in the storage with the given `ref_id` references to the data.
        :param data: The data object to create.
        :param insert_stmt: The SQL statement to insert the reference.
        :param ref_id: The reference ID.
        :raises StorageError: If the storage operations fail.
        """
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    InsertData,
                    (data.id.bytes, data.value, data.hard_locality),
                )
                if data.localities:
                    cursor.executemany(
                        InsertDataLocality,
                        [(data.id.bytes, addr) for addr in data.localities],
                    )
                cursor.execute(
                    insert_stmt,
                    (data.id.bytes, ref_id.bytes),
                )
                self._conn.commit()
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e

    @staticmethod
    def _execute_optional_batch(
        cursor: mariadb.Cursor,
        statement: str,
        params: Sequence[tuple[object, ...]],
    ) -> None:
        if params:
            cursor.executemany(statement, params)

    @staticmethod
    def _ensure_jobs_task_graphs_match(
        jobs: Sequence[core.Job],
        task_graphs: Sequence[core.TaskGraph],
    ) -> None:
        if len(jobs) != len(task_graphs):
            msg = "The lengths of `jobs` and `task_graphs` must match."
            raise ValueError(msg)

    @staticmethod
    def _update_channel_info(
        channel_info: dict[UUID, _ChannelInfo],
        channel_id: UUID,
        channel_type: str,
        task_id_bytes: bytes,
        member_key: Literal["producers", "consumers"],
    ) -> None:
        entry = channel_info.setdefault(
            channel_id,
            {"type": channel_type, "producers": set(), "consumers": set()},
        )
        if entry["type"] != channel_type:
            msg = (
                f"Channel {channel_id} has conflicting types: "
                f"existing type '{entry['type']}', new type '{channel_type}'."
            )
            raise StorageError(msg)
        entry[member_key].add(task_id_bytes)

    @staticmethod
    def _add_task_outputs(
        channel_info: dict[UUID, _ChannelInfo],
        task: core.Task,
        task_id_bytes: bytes,
    ) -> None:
        for task_output in task.task_outputs:
            if task_output.channel_id is None:
                continue
            MariaDBStorage._update_channel_info(
                channel_info,
                task_output.channel_id,
                task_output.type,
                task_id_bytes,
                "producers",
            )

    @staticmethod
    def _add_task_inputs(
        channel_info: dict[UUID, _ChannelInfo],
        task: core.Task,
        task_id_bytes: bytes,
    ) -> None:
        for task_input in task.task_inputs:
            if task_input.channel_id is None:
                continue
            if task_input.value is not None:
                continue
            MariaDBStorage._update_channel_info(
                channel_info,
                task_input.channel_id,
                task_input.type,
                task_id_bytes,
                "consumers",
            )

    @staticmethod
    def _collect_channel_info(
        task_ids: Sequence[UUID],
        task_graph: core.TaskGraph,
    ) -> dict[UUID, _ChannelInfo]:
        channel_info: dict[UUID, _ChannelInfo] = {}
        for task_index, task in enumerate(task_graph.tasks):
            task_id_bytes = task_ids[task_index].bytes
            MariaDBStorage._add_task_outputs(channel_info, task, task_id_bytes)
            MariaDBStorage._add_task_inputs(channel_info, task, task_id_bytes)
        return channel_info

    @staticmethod
    def _append_channel_params(
        channel_params: list[tuple[bytes, bytes, str]],
        channel_producer_params: list[tuple[bytes, bytes]],
        channel_consumer_params: list[tuple[bytes, bytes]],
        channel_info: dict[UUID, _ChannelInfo],
        job_id_bytes: bytes,
    ) -> None:
        for channel_id, info in channel_info.items():
            channel_params.append((channel_id.bytes, job_id_bytes, info["type"]))
            channel_producer_params.extend(
                (channel_id.bytes, task_id_bytes) for task_id_bytes in info["producers"]
            )
            channel_consumer_params.extend(
                (channel_id.bytes, task_id_bytes) for task_id_bytes in info["consumers"]
            )

    @staticmethod
    def _gen_task_insertion_params(
        jobs: Sequence[core.Job],
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, bytes, str, str, str, float, int]]:
        """
        Generates parameters for inserting tasks into the database.
        :param jobs: The jobs.
        :param task_ids: The task IDs. Must be the same length as `jobs`.
        :param task_graphs: The task graphs. Must be the same length as `jobs`.
        :return: A list of tuples containing the parameters for each task. Each tuple contains:
            - Task ID.
            - Job ID.
            - Task function name.
            - Task language (always "python").
            - Task state.
            - Task timeout.
            - Task max retry.
        :raises ValueError: If the lengths of `jobs` and `task_graphs` do not match.
        """
        task_insert_params = []
        if len(jobs) != len(task_graphs):
            msg = "The lengths of `jobs` and `task_graphs` must match."
            raise ValueError(msg)
        for graph_index, (job, task_graph) in enumerate(zip(jobs, task_graphs, strict=True)):
            for task_index, task in enumerate(task_graph.tasks):
                task_insert_params.append(
                    (
                        task_ids[graph_index][task_index].bytes,
                        job.job_id.bytes,
                        task.function_name,
                        "python",
                        str(task.state),
                        task.timeout,
                        task.max_retries,
                    )
                )
        return task_insert_params

    @staticmethod
    def _gen_task_dependency_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, bytes]]:
        """
        Generates parameters for inserting task dependencies into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task dependency. Each tuple
            contains:
            - Parent task ID.
            - Child task ID.
        """
        dep_params = []
        for graph_index, task_graph in enumerate(task_graphs):
            for parent, child in task_graph.dependencies:
                dep_params.append(
                    (task_ids[graph_index][parent].bytes, task_ids[graph_index][child].bytes)
                )
        return dep_params

    @staticmethod
    def _gen_input_task_insertion_params(
        jobs: Sequence[core.Job],
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, bytes, int]]:
        """
        Generates parameters for inserting input tasks into the database.
        :param jobs: The jobs.
        :param task_ids: The task IDs. Must be the same length as `jobs`.
        :param task_graphs: The task graphs. Must be the same length as `jobs`.
        :return: A list of tuples containing the parameters for each input task. Each tuple
            contains:
            - Job ID.
            - Task ID.
            - The positional index of the input task.
        :raises ValueError: If the lengths of `jobs` and `task_graphs` do not match.
        """
        input_task_params = []
        if len(jobs) != len(task_graphs):
            msg = "The lengths of `jobs` and `task_graphs` must match."
            raise ValueError(msg)
        for graph_index, (job, task_graph) in enumerate(zip(jobs, task_graphs, strict=True)):
            for position, task_index in enumerate(task_graph.input_task_indices):
                input_task_params.append(
                    (job.job_id.bytes, task_ids[graph_index][task_index].bytes, position)
                )
        return input_task_params

    @staticmethod
    def _gen_output_task_insertion_params(
        jobs: Sequence[core.Job],
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, bytes, int]]:
        """
        Generates parameters for inserting output tasks into the database.
        :param jobs: The jobs.
        :param task_ids: The task IDs. Must be the same length as `jobs`.
        :param task_graphs: The task graphs. Must be the same length as `jobs`.
        :return: A list of tuples containing the parameters for each output task. Each tuple
            contains:
            - Job ID.
            - Task ID.
            - The positional index of the output task.
        :raises ValueError: If the lengths of `jobs` and `task_graphs` do not match.
        """
        output_task_params = []
        if len(jobs) != len(task_graphs):
            msg = "The lengths of `jobs` and `task_graphs` must match."
            raise ValueError(msg)
        for graph_index, (job, task_graph) in enumerate(zip(jobs, task_graphs, strict=True)):
            for position, task_index in enumerate(task_graph.output_task_indices):
                output_task_params.append(
                    (job.job_id.bytes, task_ids[graph_index][task_index].bytes, position)
                )
        return output_task_params

    @staticmethod
    def _gen_channel_insertion_params(
        jobs: Sequence[core.Job],
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> tuple[
        list[tuple[bytes, bytes, str]],
        list[tuple[bytes, bytes]],
        list[tuple[bytes, bytes]],
    ]:
        """
        Generates parameters for inserting channels and channel memberships.
        :param jobs: The jobs.
        :param task_ids: The task IDs. Must be the same length as `jobs`.
        :param task_graphs: The task graphs. Must be the same length as `jobs`.
        :return: A tuple containing channel params, producer params, and consumer params.
        """
        channel_params: list[tuple[bytes, bytes, str]] = []
        channel_producer_params: list[tuple[bytes, bytes]] = []
        channel_consumer_params: list[tuple[bytes, bytes]] = []
        MariaDBStorage._ensure_jobs_task_graphs_match(jobs, task_graphs)
        for graph_index, (job, task_graph) in enumerate(zip(jobs, task_graphs, strict=True)):
            channel_info = MariaDBStorage._collect_channel_info(task_ids[graph_index], task_graph)
            MariaDBStorage._append_channel_params(
                channel_params,
                channel_producer_params,
                channel_consumer_params,
                channel_info,
                job.job_id.bytes,
            )
        return channel_params, channel_producer_params, channel_consumer_params

    @staticmethod
    def _gen_task_output_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, int, str, bytes | None]]:
        """
        Generates parameters for inserting task outputs into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task output. Each tuple
            contains:
            - Task ID.
            - Positional index of the output.
            - Type of the output.
        """
        output_params = []
        for graph_index, task_graph in enumerate(task_graphs):
            for task_index, task in enumerate(task_graph.tasks):
                for position, task_output in enumerate(task.task_outputs):
                    channel_id = (
                        task_output.channel_id.bytes if task_output.channel_id is not None else None
                    )
                    output_params.append(
                        (
                            task_ids[graph_index][task_index].bytes,
                            position,
                            task_output.type,
                            channel_id,
                        )
                    )
        return output_params

    @staticmethod
    def _gen_task_input_data_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, int, str, bytes, bytes | None]]:
        """
        Generates parameters for inserting task input data into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task input data. Each tuple
            contains:
            - Task ID.
            - Positional index of the input.
            - Type of the input.
            - Input data.
        """
        input_data_params: list[tuple[bytes, int, str, bytes, bytes | None]] = []
        for graph_index, task_graph in enumerate(task_graphs):
            for task_index, task in enumerate(task_graph.tasks):
                for position, task_input in enumerate(task.task_inputs):
                    if not isinstance(task_input.value, core.TaskInputData):
                        continue
                    value = task_input.value
                    data = value.id.bytes if isinstance(value, core.Data) else value.bytes
                    input_data_params.append(
                        (
                            task_ids[graph_index][task_index].bytes,
                            position,
                            task_input.type,
                            data,
                            None,
                        )
                    )
        return input_data_params

    @staticmethod
    def _gen_task_input_value_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, int, str, bytes, bytes | None]]:
        """
        Generates parameters for inserting task input values into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task input value. Each tuple
            contains:
            - Task ID.
            - Positional index of the input.
            - Type of the input.
            - Input value.
        """
        input_value_params: list[tuple[bytes, int, str, bytes, bytes | None]] = []
        for graph_index, task_graph in enumerate(task_graphs):
            for task_index, task in enumerate(task_graph.tasks):
                for position, task_input in enumerate(task.task_inputs):
                    if isinstance(task_input.value, core.TaskInputValue):
                        input_value_params.append(
                            (
                                task_ids[graph_index][task_index].bytes,
                                position,
                                task_input.type,
                                task_input.value,
                                None,
                            )
                        )
        return input_value_params

    @staticmethod
    def _gen_task_input_channel_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, int, str, bytes]]:
        """
        Generates parameters for inserting task input channels into the database.
        :param task_ids: The task IDs. Must be the same length as `task_graphs`.
        :param task_graphs: The task graphs.
        :return: A list of tuples containing the parameters for each task input channel. Each tuple
            contains:
            - Task ID.
            - Positional index of the input.
            - Type of the input.
            - Channel ID.
        """
        input_channel_params = []
        for graph_index, task_graph in enumerate(task_graphs):
            for task_index, task in enumerate(task_graph.tasks):
                for position, task_input in enumerate(task.task_inputs):
                    if task_input.channel_id is None:
                        continue
                    if task_input.value is not None:
                        continue
                    input_channel_params.append(
                        (
                            task_ids[graph_index][task_index].bytes,
                            position,
                            task_input.type,
                            task_input.channel_id.bytes,
                        )
                    )
        return input_channel_params

    @staticmethod
    def _gen_task_input_output_ref_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, int, str, bytes, int, bytes | None]]:
        """
        Generates parameters for inserting task input output refs into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task input output ref. Each
            tuple contains:
            - Input task ID.
            - Positional index of the input.
            - Type of the input.
            - Output task ID.
            - Positional index of the output.
        """
        input_output_params: list[tuple[bytes, int, str, bytes, int, bytes | None]] = []
        for graph_index, task_graph in enumerate(task_graphs):
            for input_output_ref in task_graph.task_input_output_refs:
                task_input = task_graph.tasks[input_output_ref.input_task_index].task_inputs[
                    input_output_ref.input_position
                ]
                input_output_params.append(
                    (
                        task_ids[graph_index][input_output_ref.input_task_index].bytes,
                        input_output_ref.input_position,
                        task_input.type,
                        task_ids[graph_index][input_output_ref.output_task_index].bytes,
                        input_output_ref.output_position,
                        None,
                    )
                )
        return input_output_params

    @staticmethod
    def _get_job_status(cursor: mariadb.Cursor, job: core.Job) -> core.JobStatus:
        """
        Gets the status of `job` from the database using the `cursor`.
        This method does not commit or rollback the transaction.
        :param cursor:
        :param job:
        :return: The job status.
        :raises StorageError: If the job is not found or if the job status is unknown.
        """
        cursor.execute(GetJobStatus, (job.job_id.bytes,))
        row = cursor.fetchone()
        if row is None:
            msg = f"No job found with id {job.job_id}."
            raise StorageError(msg)
        status_str = row[0]
        if status_str not in _StrToJobStatusMap:
            msg = f"Unknown job status: {status_str}."
            raise StorageError(msg)
        # Use fetchall after a fetchone to drain the result set even if it is already empty.
        cursor.fetchall()
        return _StrToJobStatusMap[status_str]

    @staticmethod
    def _get_data(cursor: mariadb.Cursor, data_id: core.DataId) -> core.Data:
        """
        Gets the data with `data_id` from the database using the `cursor`.
        This method does not commit or rollback the transaction.
        :param cursor:
        :param data_id:
        :return: The data.
        :raises StorageError: If the data is not found.
        """
        cursor.execute(GetData, (data_id.bytes,))
        row = cursor.fetchone()
        if row is None:
            msg = f"No data found with id {data_id}."
            raise StorageError(msg)
        value, hard_locality = row
        data = core.Data(id=data_id, value=value, hard_locality=hard_locality)
        cursor.execute(GetDataLocality, (data_id.bytes,))
        for (address,) in cursor.fetchall():
            data.localities.append(core.DataAddr(address))
        return data


def _raise_storage_error(message: str) -> None:
    """
    Raises a StorageError with the `message`.
    Workaround for ruff TRY301. See https://docs.astral.sh/ruff/rules/raise-within-try/.
    :param message:
    :raises StorageError: Always.
    """
    raise StorageError(message)
