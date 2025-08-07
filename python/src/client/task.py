"""Client task function module."""

from typing import Any, Protocol


class TaskContext:
    """
    Provides access to Spider operations inside a task.
    Must be the first argument of a Spider task function.
    """


class TaskFunction(Protocol):
    """Represents a task function."""

    # Suppress ANN401 for returning Any
    # Suppress D102 for no docstring
    def __call__(self, context: TaskContext, *args: Any, **kwargs: Any) -> Any: ...  # noqa: ANN401, D102
