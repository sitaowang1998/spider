"""Classes and task functions for integration tests."""

from dataclasses import dataclass

import spider_py


@dataclass
class Name:
    """Simple dataclass representing a name."""

    first: str
    last: str


@dataclass
class User:
    """Simple dataclass representing a user."""

    name: Name
    age: int


def add(_: spider_py.TaskContext, x: spider_py.Int8, y: spider_py.Int8) -> spider_py.Int8:
    """Adds two numbers."""
    return spider_py.Int8(x + y)


def swap(
    _: spider_py.TaskContext, x: spider_py.Int8, y: spider_py.Int8
) -> tuple[spider_py.Int8, spider_py.Int8]:
    """Swaps two numbers."""
    return spider_py.Int8(y), spider_py.Int8(x)


def count_name_length(_: spider_py.TaskContext, user: User) -> spider_py.Int64:
    """Returns the number of characters in a user's full name, including the space in between."""
    return spider_py.Int64(len(user.name.first) + len(user.name.last) + 1)


def data_size(_: spider_py.TaskContext, data: spider_py.Data) -> spider_py.Int64:
    """Returns the number of characters in a data."""
    return spider_py.Int64(len(data.value.decode()))
