"""Unit tests for high-level Channel API."""

import pytest

from spider_py.client import (
    Channel,
    channel_task,
    group,
    Receiver,
    Sender,
    TaskContext,
    TaskGraph,
)
from spider_py.type import Int8, Int32


class TestChannel:
    """Tests for Channel class."""

    def test_channel_creation_with_type(self) -> None:
        """Tests that Channel[T]() creates a channel with correct item type."""
        channel = Channel[bytes]()
        assert channel.item_type is bytes

    def test_channel_creation_int32(self) -> None:
        """Tests Channel creation with Int32 type."""
        channel = Channel[Int32]()
        assert channel.item_type is Int32

    def test_channel_unique_ids(self) -> None:
        """Tests that each channel has a unique ID."""
        ch1 = Channel[bytes]()
        ch2 = Channel[bytes]()
        ch3 = Channel[Int32]()
        assert ch1.id != ch2.id
        assert ch1.id != ch3.id
        assert ch2.id != ch3.id

    def test_channel_without_type(self) -> None:
        """Tests Channel creation without explicit type parameter."""
        channel: Channel[object] = Channel()
        assert channel.item_type is None
        assert channel.id is not None


class TestChannelTaskProducer:
    """Tests for channel_task() with producer (Sender) tasks."""

    def test_producer_single_sender(self) -> None:
        """Tests channel_task with a single Sender parameter."""

        def producer(ctx: TaskContext, s: Sender[bytes]) -> bytes:
            return b"done"

        channel = Channel[bytes]()
        graph = channel_task(producer, senders={"s": channel})

        assert isinstance(graph, TaskGraph)
        assert len(graph._impl.tasks) == 1

        task = graph._impl.tasks[0]
        assert len(task.task_inputs) == 1
        assert len(task.task_outputs) == 2  # channel output + return value

        # Verify channel input
        assert task.task_inputs[0].channel_id == channel.id
        assert task.task_inputs[0].type == "channel:bytes"
        assert task.task_inputs[0].value is None

        # Verify channel output (producer registration)
        assert task.task_outputs[0].channel_id == channel.id
        assert task.task_outputs[0].type == "channel:bytes"

        # Verify regular return output
        assert task.task_outputs[1].channel_id is None
        assert task.task_outputs[1].type == "bytes"

    def test_producer_with_int32_channel(self) -> None:
        """Tests channel_task with Int32 channel type."""

        def producer(ctx: TaskContext, s: Sender[Int32]) -> Int32:
            return Int32(0)

        channel = Channel[Int32]()
        graph = channel_task(producer, senders={"s": channel})

        task = graph._impl.tasks[0]
        assert task.task_inputs[0].type == "channel:int32"
        assert task.task_outputs[0].type == "channel:int32"

    def test_producer_multiple_senders(self) -> None:
        """Tests channel_task with multiple Sender parameters."""

        def multi_producer(
            ctx: TaskContext,
            s1: Sender[bytes],
            s2: Sender[Int32],
        ) -> bytes:
            return b"done"

        ch1 = Channel[bytes]()
        ch2 = Channel[Int32]()
        graph = channel_task(multi_producer, senders={"s1": ch1, "s2": ch2})

        task = graph._impl.tasks[0]
        assert len(task.task_inputs) == 2
        assert len(task.task_outputs) == 3  # 2 channel outputs + 1 return

        # Verify inputs
        assert task.task_inputs[0].channel_id == ch1.id
        assert task.task_inputs[0].type == "channel:bytes"
        assert task.task_inputs[1].channel_id == ch2.id
        assert task.task_inputs[1].type == "channel:int32"

        # Verify outputs (channel outputs come first)
        assert task.task_outputs[0].channel_id == ch1.id
        assert task.task_outputs[1].channel_id == ch2.id
        assert task.task_outputs[2].channel_id is None  # return value


class TestChannelTaskConsumer:
    """Tests for channel_task() with consumer (Receiver) tasks."""

    def test_consumer_single_receiver(self) -> None:
        """Tests channel_task with a single Receiver parameter."""

        def consumer(ctx: TaskContext, r: Receiver[bytes]) -> Int32:
            return Int32(0)

        channel = Channel[bytes]()
        graph = channel_task(consumer, receivers={"r": channel})

        assert len(graph._impl.tasks) == 1

        task = graph._impl.tasks[0]
        assert len(task.task_inputs) == 1
        assert len(task.task_outputs) == 1  # only return value, no channel output

        # Verify channel input
        assert task.task_inputs[0].channel_id == channel.id
        assert task.task_inputs[0].type == "channel:bytes"

        # Verify return output (no channel output for consumer)
        assert task.task_outputs[0].channel_id is None
        assert task.task_outputs[0].type == "int32"

    def test_consumer_multiple_receivers(self) -> None:
        """Tests channel_task with multiple Receiver parameters."""

        def multi_consumer(
            ctx: TaskContext,
            r1: Receiver[bytes],
            r2: Receiver[Int32],
        ) -> bytes:
            return b"result"

        ch1 = Channel[bytes]()
        ch2 = Channel[Int32]()
        graph = channel_task(multi_consumer, receivers={"r1": ch1, "r2": ch2})

        task = graph._impl.tasks[0]
        assert len(task.task_inputs) == 2
        assert len(task.task_outputs) == 1  # only return value

        assert task.task_inputs[0].channel_id == ch1.id
        assert task.task_inputs[1].channel_id == ch2.id


class TestChannelTaskMixed:
    """Tests for channel_task() with mixed parameters."""

    def test_sender_with_regular_params(self) -> None:
        """Tests channel_task with Sender and regular parameters."""

        def producer(ctx: TaskContext, s: Sender[bytes], count: Int32) -> bytes:
            return b"done"

        channel = Channel[bytes]()
        graph = channel_task(producer, senders={"s": channel})

        task = graph._impl.tasks[0]
        assert len(task.task_inputs) == 2

        # First input: Sender
        assert task.task_inputs[0].channel_id == channel.id
        assert task.task_inputs[0].type == "channel:bytes"

        # Second input: regular Int32
        assert task.task_inputs[1].channel_id is None
        assert task.task_inputs[1].type == "int32"

    def test_receiver_with_regular_params(self) -> None:
        """Tests channel_task with Receiver and regular parameters."""

        def consumer(ctx: TaskContext, r: Receiver[bytes], limit: Int8) -> Int32:
            return Int32(0)

        channel = Channel[bytes]()
        graph = channel_task(consumer, receivers={"r": channel})

        task = graph._impl.tasks[0]
        assert len(task.task_inputs) == 2

        # First input: Receiver
        assert task.task_inputs[0].channel_id == channel.id
        assert task.task_inputs[0].type == "channel:bytes"

        # Second input: regular Int8
        assert task.task_inputs[1].channel_id is None
        assert task.task_inputs[1].type == "int8"

    def test_regular_param_between_channel_params(self) -> None:
        """Tests channel_task with regular param between channel params."""

        def mixed(
            ctx: TaskContext,
            s: Sender[bytes],
            count: Int32,
            r: Receiver[Int32],
        ) -> bytes:
            return b"done"

        ch1 = Channel[bytes]()
        ch2 = Channel[Int32]()
        graph = channel_task(mixed, senders={"s": ch1}, receivers={"r": ch2})

        task = graph._impl.tasks[0]
        assert len(task.task_inputs) == 3

        # Inputs preserve parameter order
        assert task.task_inputs[0].channel_id == ch1.id  # Sender
        assert task.task_inputs[1].channel_id is None  # Int32
        assert task.task_inputs[2].channel_id == ch2.id  # Receiver


class TestChannelTaskValidation:
    """Tests for channel_task() validation errors."""

    def test_missing_sender_binding(self) -> None:
        """Tests that missing Sender binding raises TypeError."""

        def producer(ctx: TaskContext, s: Sender[bytes]) -> bytes:
            return b"done"

        with pytest.raises(TypeError, match="Sender parameter 's' must have channel binding"):
            channel_task(producer)

    def test_missing_receiver_binding(self) -> None:
        """Tests that missing Receiver binding raises TypeError."""

        def consumer(ctx: TaskContext, r: Receiver[bytes]) -> Int32:
            return Int32(0)

        with pytest.raises(TypeError, match="Receiver parameter 'r' must have channel binding"):
            channel_task(consumer)

    def test_wrong_binding_name(self) -> None:
        """Tests that wrong binding name raises TypeError."""

        def producer(ctx: TaskContext, s: Sender[bytes]) -> bytes:
            return b"done"

        channel = Channel[bytes]()
        with pytest.raises(TypeError, match="Sender parameter 's' must have channel binding"):
            channel_task(producer, senders={"wrong_name": channel})

    def test_no_task_context(self) -> None:
        """Tests that missing TaskContext raises TypeError."""

        def invalid(s: Sender[bytes]) -> bytes:
            return b"done"

        channel = Channel[bytes]()
        with pytest.raises(TypeError, match="First argument must be TaskContext"):
            channel_task(invalid, senders={"s": channel})

    def test_not_a_function(self) -> None:
        """Tests that non-function raises TypeError."""
        channel = Channel[bytes]()
        with pytest.raises(TypeError, match="not a function"):
            channel_task("not a function", senders={"s": channel})  # type: ignore[arg-type]

    def test_variadic_params_not_supported(self) -> None:
        """Tests that variadic parameters raise TypeError."""

        def variadic(ctx: TaskContext, *args: bytes) -> bytes:
            return b"done"

        with pytest.raises(TypeError, match="Variadic parameters are not supported"):
            channel_task(variadic)

    def test_missing_return_annotation(self) -> None:
        """Tests that missing return annotation raises TypeError."""

        def no_return(ctx: TaskContext, s: Sender[bytes]):  # type: ignore[no-untyped-def]
            pass

        channel = Channel[bytes]()
        with pytest.raises(TypeError, match="Return type must have type annotation"):
            channel_task(no_return, senders={"s": channel})


class TestChannelTaskComposition:
    """Tests for composing channel tasks with group()."""

    def test_group_producer_consumer(self) -> None:
        """Tests grouping producer and consumer tasks."""

        def producer(ctx: TaskContext, s: Sender[bytes]) -> bytes:
            return b"done"

        def consumer(ctx: TaskContext, r: Receiver[bytes]) -> Int32:
            return Int32(0)

        channel = Channel[bytes]()
        prod_graph = channel_task(producer, senders={"s": channel})
        cons_graph = channel_task(consumer, receivers={"r": channel})

        combined = group([prod_graph, cons_graph])

        assert len(combined._impl.tasks) == 2
        assert len(combined._impl.dependencies) == 0  # No dependencies, channel coordinates
        assert len(combined._impl.input_task_indices) == 2
        assert len(combined._impl.output_task_indices) == 2

    def test_multiple_producers_single_consumer(self) -> None:
        """Tests multiple producers with single consumer."""

        def producer(ctx: TaskContext, s: Sender[bytes]) -> bytes:
            return b"done"

        def consumer(ctx: TaskContext, r: Receiver[bytes]) -> Int32:
            return Int32(0)

        channel = Channel[bytes]()
        prod1 = channel_task(producer, senders={"s": channel})
        prod2 = channel_task(producer, senders={"s": channel})
        cons = channel_task(consumer, receivers={"r": channel})

        combined = group([prod1, prod2, cons])

        assert len(combined._impl.tasks) == 3

        # All producers share the same channel ID
        assert combined._impl.tasks[0].task_inputs[0].channel_id == channel.id
        assert combined._impl.tasks[1].task_inputs[0].channel_id == channel.id
        assert combined._impl.tasks[2].task_inputs[0].channel_id == channel.id

    def test_single_producer_multiple_consumers(self) -> None:
        """Tests single producer with multiple consumers."""

        def producer(ctx: TaskContext, s: Sender[bytes]) -> bytes:
            return b"done"

        def consumer(ctx: TaskContext, r: Receiver[bytes]) -> Int32:
            return Int32(0)

        channel = Channel[bytes]()
        prod = channel_task(producer, senders={"s": channel})
        cons1 = channel_task(consumer, receivers={"r": channel})
        cons2 = channel_task(consumer, receivers={"r": channel})

        combined = group([prod, cons1, cons2])

        assert len(combined._impl.tasks) == 3

    def test_passthrough_pattern(self) -> None:
        """Tests passthrough task (producer and consumer of different channels)."""

        def passthrough(
            ctx: TaskContext,
            r: Receiver[bytes],
            s: Sender[Int32],
        ) -> bytes:
            return b"done"

        ch_in = Channel[bytes]()
        ch_out = Channel[Int32]()
        graph = channel_task(passthrough, receivers={"r": ch_in}, senders={"s": ch_out})

        task = graph._impl.tasks[0]
        assert len(task.task_inputs) == 2

        # Receiver input
        assert task.task_inputs[0].channel_id == ch_in.id
        assert task.task_inputs[0].type == "channel:bytes"

        # Sender input
        assert task.task_inputs[1].channel_id == ch_out.id
        assert task.task_inputs[1].type == "channel:int32"

        # Outputs: channel output for sender + return value
        assert len(task.task_outputs) == 2
        assert task.task_outputs[0].channel_id == ch_out.id

    def test_pipeline_pattern(self) -> None:
        """Tests pipeline: producer -> passthrough -> consumer."""

        def producer(ctx: TaskContext, s: Sender[bytes]) -> bytes:
            return b"done"

        def passthrough(ctx: TaskContext, r: Receiver[bytes], s: Sender[Int32]) -> bytes:
            return b"done"

        def consumer(ctx: TaskContext, r: Receiver[Int32]) -> Int32:
            return Int32(0)

        ch1 = Channel[bytes]()
        ch2 = Channel[Int32]()

        prod = channel_task(producer, senders={"s": ch1})
        mid = channel_task(passthrough, receivers={"r": ch1}, senders={"s": ch2})
        cons = channel_task(consumer, receivers={"r": ch2})

        pipeline = group([prod, mid, cons])

        assert len(pipeline._impl.tasks) == 3
        # All connected via channels (no explicit dependencies in graph)
        assert len(pipeline._impl.dependencies) == 0
