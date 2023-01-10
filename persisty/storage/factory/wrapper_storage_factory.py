from dataclasses import dataclass
from typing import FrozenSet, Optional, Tuple

from servey.security.authorization import Authorization

from persisty.access_control.factory.access_control_factory_abc import AccessControlFactoryABC
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta


@dataclass
class WrapperStorageFactory(StorageFactoryABC):
    """
    Storage factory which wraps another and provides some filtering or aggregation
    """
    storage_factory: StorageFactoryABC
    exclude_field_names: FrozenSet[str] = frozenset()
    search_filter: SearchFilterABC = INCLUDE_ALL
    access_control_factories = Optional[Tuple[AccessControlFactoryABC, ...]]
    externally_accessible: bool = True

    def get_storage_meta(self) -> StorageMeta:
        pass

    def is_externally_accessible(self) -> bool:
        return self.externally_accessible

    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        storage = self.storage_factory.create(authorization)
        if self.exclude_field_names:
            storage = ExcludeFieldsStorage
        pass
