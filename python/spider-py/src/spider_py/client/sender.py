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

    Note: Sender instances are created internally by the task executor.
    Users should not instantiate this class directly.
    """

    # Private attributes set by _channel_impl.create_sender()
    _channel_id: UUID
    _item_type: type
    _buffer: list[T]

    def send(self, item: T) -> None:
        """
        Buffers an item to be sent to the channel.

        The item will be committed when the task successfully completes.

        :param item: The item to send.
        """
        self._buffer.append(item)

    def __len__(self) -> int:
        """Returns the number of buffered items."""
        return len(self._buffer)
