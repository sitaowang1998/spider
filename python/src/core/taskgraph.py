"""TaskGraph module for Spider."""

from copy import deepcopy
from uuid import uuid4

from core.task import Task, TaskId, TaskInputOutput


class TaskGraph:
    """Represents a task graph in Spider."""

    def __init__(self) -> None:
        """Initializes an empty task graph."""
        self.tasks: dict[TaskId, Task] = {}
        # Dependency list consists of a list of tuples of
        #   - parent task id
        #   - child task id
        self.dependencies: list[tuple[TaskId, TaskId]] = []
        self.input_tasks: set[TaskId] = set()
        self.output_tasks: set[TaskId] = set()

    def add_task(
        self, task: Task, parents: list[TaskId] | None = None, children: list[TaskId] | None = None
    ) -> None:
        """
        Adds a task to the graph.
        :param task: The task to add.
        :param parents: The parent ids of the task. Must be already in the task graph.
        :param children: The children ids of the task. Must be already in the task graph.
        """
        self.tasks[task.task_id] = deepcopy(task)
        if parents:
            for parent in parents:
                self.dependencies.append((parent, task.task_id))
                self.output_tasks.discard(parent)
        else:
            self.input_tasks.add(task.task_id)
        if children:
            for child in children:
                self.dependencies.append((task.task_id, child))
                self.input_tasks.discard(child)
        else:
            self.output_tasks.add(task.task_id)

    def merge_task_graph(self, task_graph: "TaskGraph") -> None:
        """
        Merges a task graph into this task graph.
        :param task_graph: The task graph to be merged into this task graph.
        """
        self.tasks.update(task_graph.tasks)
        self.dependencies.extend(task_graph.dependencies)
        self.input_tasks.update(task_graph.input_tasks)
        self.output_tasks.update(task_graph.output_tasks)

    def get_parents(self, task_id: TaskId) -> list[Task]:
        """
        Gets parent tasks of task.
        :param task_id: ID of the task.
        :return: List of parent tasks.
        """
        return [self.tasks[parent] for (parent, child) in self.dependencies if child == task_id]

    def get_children(self, task_id: TaskId) -> list[Task]:
        """
        Gets child tasks of task.
        :param task_id: ID of the task.
        :return: List of children tasks.
        """
        return [self.tasks[child] for (parent, child) in self.dependencies if parent == task_id]

    def reset_task_ids(self) -> None:
        """Resets task ids for all tasks."""
        task_id_map: dict[TaskId, TaskId] = {}
        for task_id in self.tasks:
            task_id_map[task_id] = uuid4()

        new_tasks: dict[TaskId, Task] = {}
        for task_id in self.tasks:
            new_task = self.tasks[task_id]
            new_task.task_id = task_id_map[new_task.task_id]
            for task_input in new_task.task_input:
                if isinstance(task_input, TaskInputOutput):
                    task_input.task_id = task_id_map[task_input.task_id]
            new_tasks[new_task.task_id] = new_task
        self.tasks = new_tasks

        new_dependencies: list[tuple[TaskId, TaskId]] = []
        for parent, child in self.dependencies:
            new_dependencies.append((task_id_map[parent], task_id_map[child]))
        self.dependencies = new_dependencies

        new_input_tasks: set[TaskId] = set()
        for task_id in self.input_tasks:
            new_input_tasks.add(task_id_map[task_id])
        self.input_tasks = new_input_tasks

        new_output_tasks: set[TaskId] = set()
        for task_id in self.output_tasks:
            new_output_tasks.add(task_id_map[task_id])
        self.output_tasks = new_output_tasks
