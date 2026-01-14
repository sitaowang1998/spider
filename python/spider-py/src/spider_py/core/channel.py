"""Channel module for Spider."""

from __future__ import annotations

from dataclasses import dataclass

from spider_py.core.data import DataId
from spider_py.core.ids import ChannelId, TaskId


@dataclass(frozen=True)
class ChannelItemPayload:
    """Represents a channel item payload."""

    item_index: int
    value: bytes | None = None
    data_id: DataId | None = None


@dataclass(frozen=True)
class ChannelItem:
    """Represents a channel item with metadata."""

    channel_id: ChannelId
    producer_task_id: TaskId
    item_index: int
    value: bytes | None = None
    data_id: DataId | None = None
    delivered_to_task_id: TaskId | None = None
