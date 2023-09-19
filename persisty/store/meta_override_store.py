from dataclasses import dataclass

from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.store_meta import StoreMeta


@dataclass
class MetaOverrideStore(WrapperStoreABC):
    store: StoreABC
    store_meta: StoreMeta

    def get_store(self) -> StoreABC:
        return self.store

    def get_meta(self) -> StoreMeta:
        return self.store_meta
