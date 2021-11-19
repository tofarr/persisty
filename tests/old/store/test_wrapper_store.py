from dataclasses import dataclass

from old.persisty.persisty_context import get_default_persisty_context
from old.persisty.storage.in_mem_storage import in_mem_storage
from old.persisty.storage.storage_abc import StorageABC
from old.persisty.storage import WrapperStorageABC, T
from tests.old.fixtures.data import setup_bands
from tests.old.fixtures.items import Band
from tests import TestInMemStorage


@dataclass(frozen=True)
class WrapperStorage(WrapperStorageABC[T]):
    wrapped_storage: StorageABC[T]

    @property
    def storage(self) -> StorageABC[T]:
        return self.wrapped_storage


class TestTTLCacheStorage(TestInMemStorage):
    """ Mostly here for coverage """

    def setUp(self):
        persisty_context = get_default_persisty_context()
        storage = WrapperStorage[T](in_mem_storage(Band))
        setup_bands(storage)
        persisty_context.register_storage(storage)

    def test_name(self):
        storage = get_default_persisty_context().get_storage(Band)
        assert storage.name == getattr(storage, 'wrapped_storage').name
