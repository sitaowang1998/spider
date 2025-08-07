"""Client TaskGraph module."""

import core.taskgraph


class TaskGraph:
    """Wraps around the underlying core TaskGraph."""

    def __init__(self) -> None:
        """Creates an empty TaskGraph."""
        self._graph = core.taskgraph.TaskGraph()
