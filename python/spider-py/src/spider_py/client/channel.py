"""Spider client Channel module."""

from __future__ import annotations

from typing import Generic, TYPE_CHECKING, TypeVar
from uuid import uuid4

if TYPE_CHECKING:
    from uuid import UUID

T = TypeVar("T")


class Channel(Generic[T]):
    """
    A typed channel handle for inter-task communication.

    Channels mediate communication between producer and consumer tasks.
    Each channel has a unique ID and a type parameter that specifies
    the type of items it carries.

    Usage:
        channel = Channel[bytes]()  # Creates a channel for bytes

    Note: Channels are created by users and passed to channel_task() to
    bind producer/consumer tasks. The actual channel runtime is managed
    by the Spider scheduler.
    """

    # Class attribute to store item type for typed subclasses
    _default_item_type: type | None = None

    def __init__(self, item_type: type | None = None) -> None:
        """
        Initializes a new channel.

        :param item_type: The type of items this channel carries. Usually set
            automatically when using Channel[T]() syntax.
        """
        self._id = uuid4()
        # Use class-level default if no explicit item_type provided
        self._item_type = item_type if item_type is not None else self._default_item_type

    def __class_getitem__(cls, item_type: type) -> type[Channel[T]]:
        """
        Enable Channel[T] syntax that captures the type at runtime.

        :param item_type: The type parameter T.
        :return: A subclass of Channel with the item type stored.
        """
        # Use type() to create a subclass dynamically
        # This avoids mypy issues with using cls as a base class
        typed_channel_cls: type[Channel[T]] = type(
            f"Channel[{getattr(item_type, '__name__', str(item_type))}]",
            (cls,),
            {"_default_item_type": item_type},
        )
        return typed_channel_cls

    @property
    def id(self) -> UUID:
        """Returns the channel's unique identifier."""
        return self._id

    @property
    def item_type(self) -> type | None:
        """Returns the type of items this channel carries."""
        return self._item_type
