"""Spider client task module."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from types import FunctionType, GenericAlias
from typing import Any, get_args, get_origin, TYPE_CHECKING

from spider_py import core
from spider_py.client.data import Data
from spider_py.client.receiver import Receiver
from spider_py.client.sender import Sender
from spider_py.client.task_context import TaskContext
from spider_py.core import DataId, TaskInput, TaskOutput, TaskOutputValue
from spider_py.type import to_tdl_type_str

if TYPE_CHECKING:
    from spider_py.client.channel import Channel
    from spider_py.client.task_graph import TaskGraph

# NOTE: This type alias is for clarification purposes only. It does not enforce static type checks.
# Instead, we rely on the runtime check to ensure the first argument is `TaskContext`. To statically
# enforce the first argument to be `TaskContext`, `Protocol` is required, which is not compatible
# with `Callable` without explicit type casting.
TaskFunction = Callable[..., object]


def _is_tuple(t: type | GenericAlias) -> bool:
    """
    :param t:
    :return: Whether t is a tuple.
    """
    return get_origin(t) is tuple


def _is_sender(annotation: type | GenericAlias) -> bool:
    """
    Check if annotation is Sender[T].
    :param annotation: The type annotation to check.
    :return: True if annotation is Sender[T], False otherwise.
    """
    origin = get_origin(annotation)
    return origin is Sender


def _is_receiver(annotation: type | GenericAlias) -> bool:
    """
    Check if annotation is Receiver[T].
    :param annotation: The type annotation to check.
    :return: True if annotation is Receiver[T], False otherwise.
    """
    origin = get_origin(annotation)
    return origin is Receiver


def _get_channel_item_type(annotation: type | GenericAlias) -> type:
    """
    Extract T from Sender[T] or Receiver[T].
    :param annotation: The Sender[T] or Receiver[T] annotation.
    :return: The item type T.
    :raises TypeError: If the annotation has no type argument.
    """
    args = get_args(annotation)
    if not args:
        msg = "Sender/Receiver must have a type argument."
        raise TypeError(msg)
    item_type: type = args[0]
    return item_type


def _validate_and_convert_params(signature: inspect.Signature) -> list[TaskInput]:
    """
    Validates the task parameters and converts them into a list of `core.TaskInput`.
    :param signature:
    :return: The converted task parameters.
    :raises TypeError: If the parameters are invalid.
    """
    params = list(signature.parameters.values())
    inputs = []
    if not params or params[0].annotation is not TaskContext:
        msg = "First argument is not a TaskContext."
        raise TypeError(msg)
    for param in params[1:]:
        if param.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            msg = "Variadic parameters are not supported."
            raise TypeError(msg)
        if param.annotation is inspect.Parameter.empty:
            msg = "Parameters must have type annotation."
            raise TypeError(msg)
        tdl_type_str = to_tdl_type_str(param.annotation)
        inputs.append(TaskInput(tdl_type_str, None))
    return inputs


def _validate_and_convert_return(signature: inspect.Signature) -> list[TaskOutput]:
    """
    Validates the task returns and converts them into a list of `core.TaskOutput`.
    :param signature:
    :return: The converted task returns.
    :raises TypeError: If the return type is invalid.
    """
    returns = signature.return_annotation
    outputs = []
    if returns is inspect.Parameter.empty:
        msg = "Return type must have type annotation."
        raise TypeError(msg)

    if not _is_tuple(returns):
        tdl_type_str = to_tdl_type_str(returns)
        if returns is Data:
            outputs.append(TaskOutput(tdl_type_str, DataId(int=0)))
        else:
            outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))
        return outputs

    args = get_args(returns)
    if Ellipsis in args:
        msg = "Variable-length tuple return types are not supported."
        raise TypeError(msg)
    for arg in args:
        tdl_type_str = to_tdl_type_str(arg)
        if arg is Data:
            outputs.append(TaskOutput(tdl_type_str, DataId(int=0)))
        else:
            outputs.append(TaskOutput(tdl_type_str, TaskOutputValue()))
    return outputs


def create_task(func: TaskFunction) -> core.Task:
    """
    Creates a core Task object from the task function.
    :param func:
    :return: The created core Task object.
    :raise TypeError: If the function signature contains unsupported types.
    """
    if not isinstance(func, FunctionType):
        msg = "`func` is not a function."
        raise TypeError(msg)
    signature = inspect.signature(func)
    return core.Task(
        function_name=f"{func.__module__}.{func.__qualname__}",
        task_inputs=_validate_and_convert_params(signature),
        task_outputs=_validate_and_convert_return(signature),
    )


def _create_channel_input(channel: Channel[Any], annotation: type | GenericAlias) -> TaskInput:
    """Creates a TaskInput for a channel parameter."""
    item_type = _get_channel_item_type(annotation)
    tdl_type_str = to_tdl_type_str(item_type)
    return TaskInput(
        type=f"channel:{tdl_type_str}",
        value=None,
        channel_id=channel.id,
    )


def _create_channel_output(channel: Channel[Any], annotation: type | GenericAlias) -> TaskOutput:
    """Creates a TaskOutput for producer registration."""
    item_type = _get_channel_item_type(annotation)
    tdl_type_str = to_tdl_type_str(item_type)
    return TaskOutput(
        type=f"channel:{tdl_type_str}",
        value=None,
        channel_id=channel.id,
    )


def _validate_channel_bindings(
    provided: dict[str, Channel[Any]],
    used: set[str],
    binding_type: str,
) -> None:
    """Validates that all provided channel bindings were used."""
    unused = set(provided.keys()) - used
    if unused:
        msg = f"Unused {binding_type} bindings: {unused}"
        raise TypeError(msg)


def _process_channel_params(
    params: list[inspect.Parameter],
    senders: dict[str, Channel[Any]],
    receivers: dict[str, Channel[Any]],
) -> tuple[list[TaskInput], list[TaskOutput]]:
    """
    Processes function parameters and creates TaskInputs and channel TaskOutputs.

    :return: Tuple of (task_inputs, channel_outputs)
    """
    task_inputs: list[TaskInput] = []
    channel_outputs: list[TaskOutput] = []
    used_senders: set[str] = set()
    used_receivers: set[str] = set()

    for param in params:
        if param.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            msg = "Variadic parameters are not supported."
            raise TypeError(msg)
        if param.annotation is inspect.Parameter.empty:
            msg = "Parameters must have type annotation."
            raise TypeError(msg)

        annotation = param.annotation
        name = param.name

        if _is_sender(annotation):
            if name not in senders:
                msg = f"Sender parameter '{name}' must have channel binding."
                raise TypeError(msg)
            used_senders.add(name)
            channel = senders[name]
            task_inputs.append(_create_channel_input(channel, annotation))
            channel_outputs.append(_create_channel_output(channel, annotation))
        elif _is_receiver(annotation):
            if name not in receivers:
                msg = f"Receiver parameter '{name}' must have channel binding."
                raise TypeError(msg)
            used_receivers.add(name)
            channel = receivers[name]
            task_inputs.append(_create_channel_input(channel, annotation))
        else:
            tdl_type_str = to_tdl_type_str(annotation)
            task_inputs.append(TaskInput(tdl_type_str, None))

    _validate_channel_bindings(senders, used_senders, "sender")
    _validate_channel_bindings(receivers, used_receivers, "receiver")

    return task_inputs, channel_outputs


def channel_task(
    func: TaskFunction,
    senders: dict[str, Channel[Any]] | None = None,
    receivers: dict[str, Channel[Any]] | None = None,
) -> TaskGraph:
    """
    Creates a TaskGraph with a task bound to channels.

    This function creates a task that can send items to channels (via Sender parameters)
    and/or receive items from channels (via Receiver parameters). The task can also have
    regular typed parameters mixed with channel parameters.

    :param func: The task function. Must have TaskContext as first parameter.
        Sender[T] parameters must be bound via the senders dict.
        Receiver[T] parameters must be bound via the receivers dict.
    :param senders: Dict mapping parameter names to Channel objects for Sender parameters.
    :param receivers: Dict mapping parameter names to Channel objects for Receiver parameters.
    :return: A TaskGraph containing the channel-bound task.
    :raises TypeError: If func is not a function, bindings don't match parameter types/names,
        or parameter validation fails.
    """
    # Import here to avoid circular import
    from spider_py.client.task_graph import TaskGraph

    if not isinstance(func, FunctionType):
        msg = "`func` is not a function."
        raise TypeError(msg)

    senders = senders or {}
    receivers = receivers or {}
    signature = inspect.signature(func)
    params = list(signature.parameters.values())

    # Validate first param is TaskContext
    if not params or params[0].annotation is not TaskContext:
        msg = "First argument is not a TaskContext."
        raise TypeError(msg)

    task_inputs, channel_outputs = _process_channel_params(params[1:], senders, receivers)

    # Process return type and prepend channel outputs
    task_outputs = channel_outputs + _validate_and_convert_return(signature)

    task = core.Task(
        function_name=f"{func.__module__}.{func.__qualname__}",
        task_inputs=task_inputs,
        task_outputs=task_outputs,
    )

    graph = TaskGraph()
    graph._impl.add_task(task)
    return graph
