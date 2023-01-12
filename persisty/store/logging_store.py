from logging import Logger
from typing import Optional, FrozenSet

from persisty.store.store_abc import StoreABC
from persisty.util.logify import logify

METHODS: FrozenSet[str] = frozenset(
    name for name in StoreABC.__dict__ if name != "get_meta"
)


def logging_store(store: StoreABC, logger: Optional[Logger] = None) -> StoreABC:
    return logify(store, METHODS, logger)
