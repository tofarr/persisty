from logging import Logger
from time import time
from typing import Callable, Optional, TypeVar, FrozenSet

from persisty.util import get_logger

T = TypeVar("T")


def logify(
    obj: T, methods: Optional[FrozenSet[str]] = None, logger: Optional[Logger] = None
) -> T:
    cls = obj.__class__
    if logger is None:
        logger = get_logger(cls.__name__)
    attrs = {**cls.__dict__}
    for name, attr in attrs.items():
        if methods:
            if name not in methods:
                continue
        elif name.startswith("__"):
            continue
        if callable(attr):
            attrs[name] = logify_callable(name, attr, logger)
    return type(cls.__name__, tuple(), attrs)()


def logify_callable(name, fn: Callable, logger: Optional[Logger] = None):
    def wrapper(*args, **kwargs):
        start = int(time())
        logger.info(f"{name}:start:{args[1:]}:{kwargs}")
        return_value = fn(*args, **kwargs)
        end = int(time())
        time_taken = end - start
        logger.info(f"{name}:end:{time_taken}")
        return return_value

    return wrapper
