from typing import Union

from persisty.storage.storage_abc import StorageABC, T
from persisty.persisty_context_abc import StorageContextABC
from persisty.storage.storage_meta import StorageMeta


class StorageContextWrapper(StorageContextABC):

    def get_storage(self, name: Union[str, T]) -> StorageABC[T]:
        pass

    def get_meta_storage(self) -> StorageABC[StorageMeta]:
        pass