from logging import Logger
from typing import Optional, FrozenSet

from persisty.storage.storage_abc import StorageABC
from persisty.util.logify import logify

METHODS: FrozenSet[str] = frozenset(
    name for name in StorageABC.__dict__ if name != "get_storage_meta"
)


def logging_storage(storage: StorageABC, logger: Optional[Logger] = None) -> StorageABC:
    return logify(storage, METHODS, logger)
