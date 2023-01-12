from logging import Logger
from time import time
from typing import Callable, Optional, TypeVar, FrozenSet, Any

from persisty.util import get_logger, filter_none

T = TypeVar("T")


def logify(
    obj: T,
    log_methods: Optional[FrozenSet[str]] = None,
    logger: Optional[Logger] = None,
) -> T:
    return LoggingWrapper(obj, log_methods, logger)


def logify_callable(name, fn: Callable, logger: Optional[Logger] = None):
    def wrapper(*args, **kwargs):
        start = time()
        error = None
        try:
            return_value = fn(*args, **kwargs)
            return return_value
        except Exception as e:
            error = str(e)
            raise e
        finally:
            end = time()
            time_taken = round(end - start)
            msg = filter_none(
                dict(
                    name=name,
                    time=time_taken,
                    error=error,
                    args=args if args else None,
                    kwargs=kwargs if kwargs else None,
                )
            )
            logger.info(msg)

    return wrapper


class LoggingWrapper:
    def __init__(
        self,
        wrapped: Any,
        log_methods: Optional[FrozenSet[str]] = None,
        logger: Optional[Logger] = None,
    ):
        self.wrapped = wrapped
        if logger is None:
            logger = get_logger(wrapped.__class__.__name__)
        self.logger = logger
        self.log_methods = log_methods

    def __getattr__(self, name: str):
        ret = getattr(self.wrapped, name)
        if callable(ret):
            if self.log_methods:
                wrap = name in self.log_methods
            else:
                wrap = not name.startswith("_")
            if wrap:
                ret = logify_callable(name, ret, self.logger)
                setattr(self, name, ret)
        return ret
