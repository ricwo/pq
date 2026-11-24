"""Serialization utilities for task arguments.

Supports JSON-serializable types via Pydantic, with dill fallback for
non-serializable types like custom objects, local functions, and lambdas.
"""

from __future__ import annotations

import base64
import json
from typing import Any

import dill
from pydantic import BaseModel


def _is_json_storable(value: Any) -> bool:
    """Check if a value can be stored in a JSONB column as it is.

    Stricter than "``json.dumps`` accepts it": Postgres rejects ``NaN`` and
    ``Infinity`` (which ``json.dumps`` writes by default) and the NUL
    character in a string.
    """
    try:
        encoded = json.dumps(value, allow_nan=False)
    except (TypeError, ValueError):
        return False
    return "\\u0000" not in encoded


def _prepare_value(value: Any) -> tuple[Any, bool]:
    """Prepare a single value for storage.

    Returns:
        Tuple of (prepared_value, is_pickled).
    """
    # Pydantic models are stored as their JSON form: ``mode="json"`` turns
    # datetimes, UUIDs, Decimals and enums into JSON types.
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")

    if _is_json_storable(value):
        return value, False

    # Fall back to dill (handles local objects, lambdas, etc.)
    pickled = dill.dumps(value)
    encoded = base64.b64encode(pickled).decode("ascii")
    return encoded, True


def _restore_value(value: Any, is_pickled: bool) -> Any:
    """Restore a value from storage."""
    if not is_pickled:
        return value

    decoded = base64.b64decode(value.encode("ascii"))
    return dill.loads(decoded)


def serialize(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Serialize args and kwargs for database storage.

    For each value:
    1. If Pydantic BaseModel, use model_dump(mode="json")
    2. If JSONB can hold it, store directly
    3. Otherwise, pickle and base64-encode

    Args:
        args: Positional arguments to serialize.
        kwargs: Keyword arguments to serialize.

    Returns:
        Dictionary suitable for JSONB storage.
    """
    serialized_args: list[Any] = []
    pickled_arg_indices: list[int] = []

    for i, arg in enumerate(args):
        prepared, is_pickled = _prepare_value(arg)
        serialized_args.append(prepared)
        if is_pickled:
            pickled_arg_indices.append(i)

    serialized_kwargs: dict[str, Any] = {}
    pickled_kwarg_keys: list[str] = []

    for key, value in kwargs.items():
        prepared, is_pickled = _prepare_value(value)
        serialized_kwargs[key] = prepared
        if is_pickled:
            pickled_kwarg_keys.append(key)

    return {
        "args": serialized_args,
        "kwargs": serialized_kwargs,
        "pickled_args": pickled_arg_indices,
        "pickled_kwargs": pickled_kwarg_keys,
    }


def deserialize(data: dict[str, Any]) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Deserialize args and kwargs from database storage.

    Args:
        data: Serialized payload from database.

    Returns:
        Tuple of (args, kwargs) ready to call handler.
    """
    serialized_args = data.get("args", [])
    serialized_kwargs = data.get("kwargs", {})
    pickled_arg_indices = set(data.get("pickled_args", []))
    pickled_kwarg_keys = set(data.get("pickled_kwargs", []))

    args = tuple(
        _restore_value(arg, i in pickled_arg_indices)
        for i, arg in enumerate(serialized_args)
    )

    kwargs = {
        key: _restore_value(value, key in pickled_kwarg_keys)
        for key, value in serialized_kwargs.items()
    }

    return args, kwargs
