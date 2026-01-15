"""Spider client Sender module."""

from __future__ import annotations

from typing import Generic, TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from uuid import UUID

T = TypeVar("T")


class Sender(Generic[T]):
    """
    A sender handle for writing items to a channel.

    Items are buffered in memory and committed atomically when the task succeeds.
    If the task fails, buffered items are discarded.
    """

    def __init__(self, channel_id: UUID, item_type: type) -> None:
        """
        Creates a sender for the given channel.

        :param channel_id: The ID of the channel to send items to.
        :param item_type: The type of items to send.
        """
        self._channel_id = channel_id
        self._item_type = item_type
        self._buffer: list[T] = []

    def send(self, item: T) -> None:
        """
        Buffers an item to be sent to the channel.

        The item will be committed when the task successfully completes.

        :param item: The item to send.
        """
        self._buffer.append(item)

    @property
    def channel_id(self) -> UUID:
        """Returns the channel ID this sender is bound to."""
        return self._channel_id

    @property
    def item_type(self) -> type:
        """Returns the type of items this sender accepts."""
        return self._item_type

    def get_buffered_items(self) -> list[T]:
        """Returns a copy of the buffered items."""
        return list(self._buffer)

    def clear(self) -> None:
        """Clears all buffered items."""
        self._buffer.clear()

    def __len__(self) -> int:
        """Returns the number of buffered items."""
        return len(self._buffer)
