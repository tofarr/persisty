from dataclasses import dataclass
from typing import Optional

from persisty.errors import PersistyError
from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta
from persisty_data.content_meta import ContentMeta


class S3ContentMetaStoreFactory(StoreFactoryABC[ContentMeta]):
    pass


@dataclass
class S3ContentMetaStore(StoreABC[ContentMeta]):
    meta: StoreMeta

    def get_meta(self) -> StoreMeta:
        return self.meta

    def create(self, item: T) -> Optional[T]:
        raise PersistyError('unavailable_operation')

    def read(self, key: str) -> Optional[T]:
        pass

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        raise PersistyError('unavailable_operation')

    def _delete(self, key: str, item: T) -> bool:
        pass

    def count(self, search_filter: SearchFilterABC[T] = INCLUDE_ALL) -> int:
        pass

    def search(
        self,
        search_filter: SearchFilterABC[T] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[T]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[T]:
        pass
