"""
Internal implementation for channel Sender/Receiver creation.

This module is internal and should not be used by end users.
It provides factory functions for creating Sender/Receiver instances
and accessing their internal state.
"""
# ruff: noqa: SLF001  # This module intentionally accesses private members

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from spider_py.client.receiver import Receiver
from spider_py.client.sender import Sender

if TYPE_CHECKING:
    from uuid import UUID

    from spider_py.storage import Storage


def create_receiver(
    channel_id: UUID,
    item_type: type,
    task_id: UUID,
    storage: Storage,
) -> Receiver[Any]:
    """
    Internal factory for creating Receiver instances.

    :param channel_id: The ID of the channel to receive items from.
    :param item_type: The type of items to receive.
    :param task_id: The ID of the consumer task.
    :param storage: The storage backend for channel operations.
    :return: A Receiver instance for the channel.
    """
    receiver: Receiver[Any] = object.__new__(Receiver)
    receiver._channel_id = channel_id
    receiver._item_type = item_type
    receiver._task_id = task_id
    receiver._storage = storage
    return receiver


def create_sender(channel_id: UUID, item_type: type) -> Sender[Any]:
    """
    Internal factory for creating Sender instances.

    :param channel_id: The ID of the channel to send items to.
    :param item_type: The type of items to send.
    :return: A Sender instance for the channel.
    """
    sender: Sender[Any] = object.__new__(Sender)
    sender._channel_id = channel_id
    sender._item_type = item_type
    sender._buffer = []
    return sender


def get_sender_channel_id(sender: Sender[Any]) -> UUID:
    """Get the channel ID from a Sender instance."""
    return sender._channel_id


def get_sender_item_type(sender: Sender[Any]) -> type:
    """Get the item type from a Sender instance."""
    return sender._item_type


def get_sender_buffered_items(sender: Sender[Any]) -> list[Any]:
    """Get a copy of the buffered items from a Sender instance."""
    return list(sender._buffer)


def get_receiver_channel_id(receiver: Receiver[Any]) -> UUID:
    """Get the channel ID from a Receiver instance."""
    return receiver._channel_id


def get_receiver_item_type(receiver: Receiver[Any]) -> type:
    """Get the item type from a Receiver instance."""
    return receiver._item_type
