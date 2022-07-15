from logging import Logger
from typing import Optional, FrozenSet

from persisty.storage.storage_abc import StorageABC, T, F, S
from persisty.util.logify import logify

METHODS: FrozenSet[str] = frozenset(name for name in StorageABC.__dict__ if name != 'storage_meta')


def logging_storage(storage: StorageABC[T, F, S], logger: Optional[Logger] = None) -> StorageABC[T, F, S]:
    return logify(storage, METHODS, logger)
