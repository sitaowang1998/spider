"""Serialization and deserialization using msgpack."""

from dataclasses import fields, is_dataclass
from types import GenericAlias
from typing import cast, get_args, get_origin

import pydantic


def msgpack_encoder(obj: object) -> list[object] | object:
    """
    Encodes an object into a list of its field values.
    This function recursively encodes nested dataclasses, lists, and dictionaries.
    :param obj: Dataclass instance to serialize.
    :return: List of field values.
    """
    if is_dataclass(obj):
        return {f.name: getattr(obj, f.name) for f in fields(obj)}
    if isinstance(obj, pydantic.BaseModel):
        return obj.model_dump()
    if isinstance(obj, list):
        return [msgpack_encoder(item) for item in obj]
    if isinstance(obj, dict):
        return {msgpack_encoder(k): msgpack_encoder(v) for k, v in obj.items()}
    return obj


def _decode_pydantic(cls: type, data: object) -> object:
    """
    Decodes data into an instance of a `cls`.
    This function only works for pydantic models.
    :param cls: Pydantic model to decode.
    :param data: Data to decode.
    :return: Instance of `cls`.
    :raises TypeError: If `data` is not an instance of `cls`.
    """
    msg = f"Cannot create instance of {cls} with {data!r}"
    if not isinstance(data, dict):
        raise TypeError(msg)
    return cls(**data)


def _decode_dataclass(cls: type, data: object) -> object:
    """
    Decodes data into an instance of a `cls`.
    This function only works for dataclasses.
    :param cls: Class to deserialize into.
    :param data: Data to decode.
    :return: Instance of `cls`.
    :raises TypeError: if `data` is not compatible with `cls`.
    """
    msg = f"Cannot create instance of {cls} with {data!r}"
    if not isinstance(data, dict):
        raise TypeError(msg)
    args = {}
    parameters = {f.name: f for f in fields(cls)}
    for name, value in data.items():
        if name not in parameters:
            raise TypeError(msg)
        arg_cls = parameters[name].type
        if not isinstance(arg_cls, type) and not isinstance(arg_cls, GenericAlias):
            raise TypeError(msg)
        args[name] = msgpack_decoder(arg_cls, value)
    return cls(**args)


def msgpack_decoder(cls: type | GenericAlias, data: object) -> object:
    """
    Decodes data into an instance of `cls`.
    This function recursively decodes nested dataclasses, lists, and dictionaries.
    :param cls: Class to deserialize into.
    :param data: Data to decode.
    :return: Instance of `cls`.
    :raise: TypeError if `data` is not compatible with `cls`.
    """
    msg = f"Cannot create instance of {cls} with {data!r}"

    origin = get_origin(cls)
    if origin is None:
        # If `cls` does not have an origin, it is a concrete type.
        cls_type = cast("type", cls)
        if is_dataclass(cls_type):
            return _decode_dataclass(cls_type, data)
        if issubclass(cls_type, pydantic.BaseModel):
            return _decode_pydantic(cls_type, data)
        # Fall back to initialize the type directly with data.
        return cls_type(data)  # type: ignore[call-arg]

    if origin is list:
        (key_type,) = get_args(cls)
        if not isinstance(data, list):
            raise TypeError(msg)
        return [msgpack_decoder(key_type, item) for item in data]

    if origin is dict:
        key_type, value_type = get_args(cls)
        if not isinstance(data, dict):
            raise TypeError(msg)
        return {
            msgpack_decoder(key_type, k): msgpack_decoder(value_type, v) for k, v in data.items()
        }

    raise TypeError(msg)
