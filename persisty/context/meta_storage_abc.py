from abc import abstractmethod, ABC
from typing import Optional

from marshy import get_default_context

from persisty.field.field_type import FieldType
from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.obj_storage.stored import stored, get_storage_meta
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta

StoredStorageMeta = stored(
    StorageMeta, key_config=FieldKeyConfig("name", FieldType.STR)
)
STORED_STORAGE_META = get_storage_meta(StoredStorageMeta)
STORAGE_META_MARSHALLER = get_default_context().get_marshaller(StorageMeta)


class MetaStorageABC(StorageABC, ABC):
    def get_storage_meta(self) -> StorageMeta:
        return STORED_STORAGE_META

    @abstractmethod
    def get_item_storage(self, name: str) -> Optional[StorageABC]:
        """Get the storage with the name given"""
