"""Function path resolution for task handlers."""

import importlib
import inspect
import sys
from collections.abc import Callable
from typing import Any


def get_function_path(func: Callable[..., Any]) -> str:
    """Get the import path for a function as 'module:name'.

    Args:
        func: A function the worker can import: defined at module level, or a
            static or class method of a module-level class
            ('module:Class.method').

    Returns:
        Import path in 'module:name' format.

    Raises:
        ValueError: If the worker could not import the function (lambda,
            closure, nested function, method bound to an instance, or an
            object without a name such as ``functools.partial``).
    """
    module = getattr(func, "__module__", None)
    name = getattr(func, "__name__", None)
    if module is None or name is None:
        raise ValueError(f"Cannot determine import path for {func!r}")

    if name == "<lambda>":
        raise ValueError(
            "Cannot enqueue lambda functions - use a named function instead"
        )

    # Check for closure (has free variables)
    code = getattr(func, "__code__", None)
    if code is not None and code.co_freevars:
        raise ValueError(
            f"Cannot enqueue closure '{name}' - closures capture local variables "
            "and cannot be imported by the worker"
        )

    if inspect.ismethod(func) and not inspect.isclass(func.__self__):
        raise ValueError(
            f"Cannot enqueue '{name}' bound to an instance - the worker imports"
            " the function and has no instance to call it on"
        )

    qualname = getattr(func, "__qualname__", name)
    if qualname == name:
        return f"{module}:{name}"

    # Not a plain module-level function. The path has to lead back to this
    # very function: the dotted path for a method of a module-level class,
    # or the bare name if the module exports the function under it. Another
    # object with the same name would run in its place.
    owner = sys.modules.get(module)
    if owner is None:
        return f"{module}:{name}"
    for candidate in (qualname, name):
        try:
            found = _walk(owner, candidate)
        except AttributeError:
            continue
        if _is_same_function(found, func):
            return f"{module}:{candidate}"
    raise ValueError(
        f"Cannot enqueue nested function or method '{qualname}' - the worker"
        f" cannot import it from '{module}'"
    )


def _is_same_function(found: object, func: object) -> bool:
    """Whether running ``found`` runs ``func``.

    Equal, not only identical: each access to a class method makes a new,
    equal, object (and one bound to another class, such as a subclass that
    inherits the method, is not equal). Two plain functions made by one
    factory are the same if they share their code and their defaults.
    """
    if found is func or found == func:
        return True
    if inspect.ismethod(found) or inspect.ismethod(func):
        return False
    code = getattr(func, "__code__", None)
    return (
        code is not None
        and getattr(found, "__code__", None) is code
        and getattr(found, "__defaults__", None) == getattr(func, "__defaults__", None)
        and getattr(found, "__kwdefaults__", None)
        == getattr(func, "__kwdefaults__", None)
    )


def _walk(module: object, qualname: str) -> Any:
    """Follow a dotted name ('Class.method') from a module."""
    target = module
    for part in qualname.split("."):
        target = getattr(target, part)
    return target


def resolve_function_path(path: str) -> Callable[..., Any]:
    """Import and return a function from its 'module:name' path.

    Args:
        path: Import path in 'module:name' format.

    Returns:
        The imported callable function.

    Raises:
        ValueError: If the path is invalid or the function cannot be imported.
    """
    if ":" not in path:
        raise ValueError(
            f"Invalid function path: {path} (expected 'module:name' format)"
        )

    module_path, func_name = path.rsplit(":", 1)

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise ValueError(f"Cannot import module '{module_path}': {e}") from e
    try:
        func = _walk(module, func_name)
    except AttributeError as e:
        raise ValueError(f"Module '{module_path}' has no function '{func_name}'") from e
    if not callable(func):
        raise ValueError(f"{path} is not callable")
    return func
