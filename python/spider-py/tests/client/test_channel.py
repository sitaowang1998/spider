"""Tests for channel Sender and Receiver classes."""

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from spider_py import Receiver, Sender
from spider_py.core import ChannelItem


class TestSender:
    """Test class for Sender."""

    def test_send_buffers_items(self) -> None:
        """Tests that send() buffers items."""
        channel_id = uuid4()
        sender: Sender[str] = Sender(channel_id, str)

        sender.send("item1")
        sender.send("item2")
        sender.send("item3")

        assert len(sender) == 3
        assert sender.get_buffered_items() == ["item1", "item2", "item3"]

    def test_clear(self) -> None:
        """Tests that clear() removes all buffered items."""
        channel_id = uuid4()
        sender: Sender[int] = Sender(channel_id, int)

        sender.send(1)
        sender.send(2)
        assert len(sender) == 2

        sender.clear()
        assert len(sender) == 0
        assert sender.get_buffered_items() == []

    def test_channel_id(self) -> None:
        """Tests channel_id property."""
        channel_id = uuid4()
        sender: Sender[str] = Sender(channel_id, str)
        assert sender.channel_id == channel_id

    def test_item_type(self) -> None:
        """Tests item_type property."""
        channel_id = uuid4()
        sender: Sender[int] = Sender(channel_id, int)
        assert sender.item_type is int


class TestReceiver:
    """Test class for Receiver."""

    def test_recv_returns_item(self) -> None:
        """Tests that recv() returns an item when available."""
        channel_id = uuid4()
        task_id = uuid4()

        # Mock the storage
        mock_storage = MagicMock()
        item = ChannelItem(
            channel_id=channel_id,
            producer_task_id=uuid4(),
            value=b"\xa5item1",  # msgpack packed "item1"
        )
        mock_storage.dequeue_channel_item.return_value = (item, False)

        receiver: Receiver[str] = Receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=task_id,
            storage=mock_storage,
        )

        result, drained = receiver.recv(timeout_ms=100)

        assert result == "item1"
        assert not drained
        mock_storage.dequeue_channel_item.assert_called_once_with(channel_id, task_id)

    def test_recv_returns_drained(self) -> None:
        """Tests that recv() returns (None, True) when channel is drained."""
        channel_id = uuid4()
        task_id = uuid4()

        mock_storage = MagicMock()
        mock_storage.dequeue_channel_item.return_value = (None, True)

        receiver: Receiver[str] = Receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=task_id,
            storage=mock_storage,
        )

        result, drained = receiver.recv(timeout_ms=100)

        assert result is None
        assert drained

    def test_recv_timeout(self) -> None:
        """Tests that recv() returns (None, False) on timeout."""
        channel_id = uuid4()
        task_id = uuid4()

        mock_storage = MagicMock()
        # Always return empty (no item, not drained)
        mock_storage.dequeue_channel_item.return_value = (None, False)

        receiver: Receiver[str] = Receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=task_id,
            storage=mock_storage,
        )

        # Use short timeout for test speed
        result, drained = receiver.recv(timeout_ms=100, poll_interval_ms=50)

        assert result is None
        assert not drained
        # Should have polled multiple times
        assert mock_storage.dequeue_channel_item.call_count >= 2

    def test_channel_id(self) -> None:
        """Tests channel_id property."""
        channel_id = uuid4()
        receiver: Receiver[str] = Receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=uuid4(),
            storage=MagicMock(),
        )
        assert receiver.channel_id == channel_id

    def test_item_type(self) -> None:
        """Tests item_type property."""
        channel_id = uuid4()
        receiver: Receiver[int] = Receiver(
            channel_id=channel_id,
            item_type=int,
            task_id=uuid4(),
            storage=MagicMock(),
        )
        assert receiver.item_type is int
