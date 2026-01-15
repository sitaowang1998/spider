"""Spider client task context module."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from spider_py.client.receiver import Receiver
from spider_py.client.sender import Sender

if TYPE_CHECKING:
    from uuid import UUID

    from spider_py import core
    from spider_py.client.data import Data
    from spider_py.storage import Storage


class TaskContext:
    """
    Represents the task context, providing:
    - Access to the task ID.
    - Task-referenced Data creation.
    - Channel Sender/Receiver creation.
    """

    def __init__(self, task_id: core.TaskId, storage: Storage) -> None:
        """
        Initializes the task context.
        :param task_id:
        :param storage:
        """
        self._task_id = task_id
        self._storage = storage

    @property
    def task_id(self) -> core.TaskId:
        """:return: The task id."""
        return self._task_id

    @property
    def storage(self) -> Storage:
        """:return: The storage backend."""
        return self._storage

    def create_data(self, data: Data) -> None:
        """
        Creates a task-ID referenced data object in the storage.
        :param data:
        """
        self._storage.create_data_with_task_ref(self._task_id, data._impl)

    def get_receiver(self, channel_id: UUID, item_type: type) -> Receiver[Any]:
        """
        Creates a Receiver for the given channel.

        :param channel_id: The ID of the channel to receive items from.
        :param item_type: The type of items to receive.
        :return: A Receiver instance for the channel.
        """
        return Receiver(
            channel_id=channel_id,
            item_type=item_type,
            task_id=self._task_id,
            storage=self._storage,
        )

    def get_sender(self, channel_id: UUID, item_type: type) -> Sender[Any]:
        """
        Creates a Sender for the given channel.

        :param channel_id: The ID of the channel to send items to.
        :param item_type: The type of items to send.
        :return: A Sender instance for the channel.
        """
        return Sender(
            channel_id=channel_id,
            item_type=item_type,
        )
