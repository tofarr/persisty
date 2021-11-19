from dataclasses import dataclass, field

from marshy import get_default_context
from marshy.marshaller_context import MarshallerContext

from persisty.errors import PersistyError
from persisty.storage.dynamic_storage_abc import DynamicStorageABC
from persisty.storage.in_mem.in_mem_storage import InMemStorage
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_meta import StorageMeta, storage_meta_from_dataclass
from persisty.storage.wrappers.access_filtered_storage import with_access_filtered
from persisty.storage.wrappers.timestamped_storage import with_timestamps
from persisty.storage.wrappers.wrapper_storage_abc import WrapperStorageABC


def _wrapped_storage_factory():
    storage_meta = storage_meta_from_dataclass(StorageMeta)
    marshaller = get_default_context().get_marshaller(StorageMeta)
    return InMemStorage(storage_meta, marshaller)


@dataclass(frozen=True)
class InMemDynamicStorage(DynamicStorageABC, WrapperStorageABC[StorageMeta]):
    wrapped_storage: StorageABC[StorageMeta] = field(default_factory=_wrapped_storage_factory)
    marshaller_context: MarshallerContext = field(default_factory=get_default_context)

    @property
    def storage(self) -> StorageABC[T]:
        return self.wrapped_storage

    def get_storage(self, name: str):
        meta = self.read(name)
        if meta is None:
            raise PersistyError(f'missing_value:{name}')
        item_type = meta.to_dataclass()
        marshaller = self.marshaller_context.get_marshaller(item_type)
        storage = InMemStorage(meta, marshaller)
        storage = with_access_filtered(storage)
        storage = with_timestamps(storage)
        return storage
