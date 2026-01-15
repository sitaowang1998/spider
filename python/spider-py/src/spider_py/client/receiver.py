"""Spider client Receiver module."""

from __future__ import annotations

import contextlib
import time
from typing import Generic, TYPE_CHECKING, TypeVar

import msgpack

if TYPE_CHECKING:
    from uuid import UUID

    from spider_py.storage import Storage

T = TypeVar("T")


class Receiver(Generic[T]):
    """
    A receiver handle for reading items from a channel.

    Items are retrieved from the channel with polling and timeout support.
    """

    def __init__(
        self,
        channel_id: UUID,
        item_type: type,
        task_id: UUID,
        storage: Storage,
    ) -> None:
        """
        Creates a receiver for the given channel.

        :param channel_id: The ID of the channel to receive items from.
        :param item_type: The type of items to receive.
        :param task_id: The ID of the consumer task.
        :param storage: The storage backend for channel operations.
        """
        self._channel_id = channel_id
        self._item_type = item_type
        self._task_id = task_id
        self._storage = storage

    def recv(
        self,
        timeout_ms: int = 30000,
        poll_interval_ms: int = 100,
    ) -> tuple[T | None, bool]:
        """
        Receives an item from the channel with polling and timeout.

        :param timeout_ms: Maximum time to wait for an item in milliseconds.
        :param poll_interval_ms: Time to sleep between polling attempts in milliseconds.
        :return: A tuple (item, drained):
            - (item, False): An item was received
            - (None, True): The channel is drained (sender closed and empty)
            - (None, False): Timeout reached without receiving an item
        :raises StorageError: If storage operations fail.
        """
        start_time = time.monotonic()
        timeout_sec = timeout_ms / 1000.0
        poll_interval_sec = poll_interval_ms / 1000.0

        while True:
            # Try to dequeue an item
            item, drained = self._storage.dequeue_channel_item(
                self._channel_id,
                self._task_id,
            )

            # If we got an item, deserialize and return it
            if item is not None:
                if item.value is None:
                    msg = "Channel item missing value."
                    raise RuntimeError(msg)
                value = msgpack.unpackb(item.value)
                # Convert to the expected type if needed
                if self._item_type is not None and callable(self._item_type):
                    with contextlib.suppress(TypeError, ValueError):
                        value = self._item_type(value)
                return (value, False)

            # If channel is drained, return that
            if drained:
                return (None, True)

            # Check if we've exceeded the timeout
            elapsed = time.monotonic() - start_time
            if elapsed >= timeout_sec:
                return (None, False)

            # Sleep before next poll
            remaining = timeout_sec - elapsed
            sleep_time = min(poll_interval_sec, remaining)
            time.sleep(sleep_time)

    @property
    def channel_id(self) -> UUID:
        """Returns the channel ID this receiver is bound to."""
        return self._channel_id

    @property
    def item_type(self) -> type:
        """Returns the type of items this receiver accepts."""
        return self._item_type
