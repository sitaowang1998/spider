"""Tests for channel Sender and Receiver classes."""

from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from spider_py.core import ChannelItem
from spider_py.core._channel_impl import (
    create_receiver,
    create_sender,
    get_receiver_channel_id,
    get_receiver_item_type,
    get_sender_buffered_items,
    get_sender_channel_id,
    get_sender_item_type,
)
from spider_py.storage.storage import StorageError

if TYPE_CHECKING:
    from spider_py import Receiver, Sender


class TestSender:
    """Test class for Sender."""

    def test_send_buffers_items(self) -> None:
        """Tests that send() buffers items."""
        channel_id = uuid4()
        sender: Sender[str] = create_sender(channel_id, str)

        sender.send("item1")
        sender.send("item2")
        sender.send("item3")

        assert len(sender) == 3
        assert get_sender_buffered_items(sender) == ["item1", "item2", "item3"]

    def test_channel_id(self) -> None:
        """Tests channel_id via impl function."""
        channel_id = uuid4()
        sender: Sender[str] = create_sender(channel_id, str)
        assert get_sender_channel_id(sender) == channel_id

    def test_item_type(self) -> None:
        """Tests item_type via impl function."""
        channel_id = uuid4()
        sender: Sender[int] = create_sender(channel_id, int)
        assert get_sender_item_type(sender) is int


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

        receiver: Receiver[str] = create_receiver(
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

        receiver: Receiver[str] = create_receiver(
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

        receiver: Receiver[str] = create_receiver(
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
        """Tests channel_id via impl function."""
        channel_id = uuid4()
        receiver: Receiver[str] = create_receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=uuid4(),
            storage=MagicMock(),
        )
        assert get_receiver_channel_id(receiver) == channel_id

    def test_item_type(self) -> None:
        """Tests item_type via impl function."""
        channel_id = uuid4()
        receiver: Receiver[int] = create_receiver(
            channel_id=channel_id,
            item_type=int,
            task_id=uuid4(),
            storage=MagicMock(),
        )
        assert get_receiver_item_type(receiver) is int


class TestReceiverStorageErrorRetry:
    """Tests for Receiver.recv() retry on transient StorageError (Bug 4 fix)."""

    def test_recv_retries_on_storage_error_then_succeeds(self) -> None:
        """Tests that recv() retries after a StorageError and returns an item."""
        channel_id = uuid4()
        task_id = uuid4()

        mock_storage = MagicMock()
        item = ChannelItem(
            channel_id=channel_id,
            producer_task_id=uuid4(),
            value=b"\xa5item1",
        )
        # First call raises StorageError, second call succeeds
        mock_storage.dequeue_channel_item.side_effect = [
            StorageError("deadlock"),
            (item, False),
        ]

        receiver: Receiver[str] = create_receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=task_id,
            storage=mock_storage,
        )

        result, drained = receiver.recv(timeout_ms=5000, poll_interval_ms=50)

        assert result == "item1"
        assert not drained
        assert mock_storage.dequeue_channel_item.call_count == 2

    def test_recv_retries_on_storage_error_then_drained(self) -> None:
        """Tests that recv() retries after a StorageError and detects drain."""
        channel_id = uuid4()
        task_id = uuid4()

        mock_storage = MagicMock()
        mock_storage.dequeue_channel_item.side_effect = [
            StorageError("lock timeout"),
            (None, True),
        ]

        receiver: Receiver[str] = create_receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=task_id,
            storage=mock_storage,
        )

        result, drained = receiver.recv(timeout_ms=5000, poll_interval_ms=50)

        assert result is None
        assert drained
        assert mock_storage.dequeue_channel_item.call_count == 2

    def test_recv_raises_storage_error_after_timeout(self) -> None:
        """Tests that recv() re-raises StorageError when timeout is exceeded."""
        channel_id = uuid4()
        task_id = uuid4()

        mock_storage = MagicMock()
        # Always raise StorageError
        mock_storage.dequeue_channel_item.side_effect = StorageError("persistent error")

        receiver: Receiver[str] = create_receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=task_id,
            storage=mock_storage,
        )

        with pytest.raises(StorageError, match="persistent error"):
            receiver.recv(timeout_ms=150, poll_interval_ms=50)

        # Should have retried at least once before timing out
        assert mock_storage.dequeue_channel_item.call_count >= 2

    def test_recv_retries_multiple_storage_errors(self) -> None:
        """Tests that recv() retries through multiple consecutive StorageErrors."""
        channel_id = uuid4()
        task_id = uuid4()

        mock_storage = MagicMock()
        item = ChannelItem(
            channel_id=channel_id,
            producer_task_id=uuid4(),
            value=b"\xa3abc",
        )
        mock_storage.dequeue_channel_item.side_effect = [
            StorageError("error 1"),
            StorageError("error 2"),
            StorageError("error 3"),
            (item, False),
        ]

        receiver: Receiver[str] = create_receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=task_id,
            storage=mock_storage,
        )

        result, drained = receiver.recv(timeout_ms=5000, poll_interval_ms=50)

        assert result == "abc"
        assert not drained
        assert mock_storage.dequeue_channel_item.call_count == 4

    def test_recv_does_not_catch_non_storage_errors(self) -> None:
        """Tests that recv() does not catch non-StorageError exceptions."""
        channel_id = uuid4()
        task_id = uuid4()

        mock_storage = MagicMock()
        mock_storage.dequeue_channel_item.side_effect = RuntimeError("unexpected")

        receiver: Receiver[str] = create_receiver(
            channel_id=channel_id,
            item_type=str,
            task_id=task_id,
            storage=mock_storage,
        )

        with pytest.raises(RuntimeError, match="unexpected"):
            receiver.recv(timeout_ms=5000, poll_interval_ms=50)

        # Should fail immediately, no retry
        assert mock_storage.dequeue_channel_item.call_count == 1
