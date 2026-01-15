"""Channel module for Spider."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spider_py.core.ids import ChannelId, TaskId


@dataclass(frozen=True)
class ChannelItem:
    """Represents a channel item with metadata."""

    channel_id: ChannelId
    producer_task_id: TaskId
    value: bytes | None = None
    delivered_to_task_id: TaskId | None = None
