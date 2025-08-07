"""Client Driver module."""

import uuid
from typing import TYPE_CHECKING

from client.task import TaskFunction
from client.taskgraph import TaskGraph

if TYPE_CHECKING:
    from core.driver import DriverId


class Driver:
    """
    Represents a client driver through which clients compose task graphs, submit jobs and track job
    status and result.
    """

    def __init__(self) -> None:
        """Creates a new client driver."""
        self.id: DriverId = uuid.uuid4()

    def group(self, inputs: list[TaskFunction | TaskGraph]) -> TaskGraph:
        """
        "
        Groups task functions and task graphs into a new task graph.
        The task functions and task graphs are added into the new task graph in order.
        For a task function, it will become both and input task and an output task.
        For a task graph, all the input tasks are added as input tasks, and all the output tasks.
        are added as output tasks. All the dependencies are reserved.
        :param inputs: Task functions and task graphs to group.
        :return: The new task graph.
        """
        return TaskGraph()

    def chain(self, inputs: list[TaskFunction | TaskGraph]) -> TaskGraph:
        """
        Chains tasks and task graphs into a new task graph.
        The tasks and task graphs are added into the new task graph in order.
        If the first element in inputs is a task function, it will become the only input task.
        If the first element in inputs is a task graph, all its input tasks will become the new
        input tasks.
        If the last element in inputs is a task function, it will become the only output task.
        If the last element in inputs is a task graph, all its output tasks will become the new
        output tasks.
        Dependencies inside all task graphs are reserved. New dependencies are formed between
        consecutive inputs.
        :param inputs: Task functions and task graphs to chain. Must have at least two elements.
        :return: The new task graph.
        :raise ValueError: If the inputs have less than two elements.
        :raise ValueError: If the number of outputs of a parent does not match the number of inputs
        of a child, or the type does not match.
        """
        return TaskGraph()
