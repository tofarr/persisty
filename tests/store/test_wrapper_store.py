from dataclasses import dataclass

from persisty.persisty_context import get_default_persisty_context
from persisty.store.in_mem_store import in_mem_store
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T
from tests.fixtures.data import setup_bands
from tests.fixtures.items import Band
from tests.store.test_in_mem_store import TestInMemStore


@dataclass(frozen=True)
class WrapperStore(WrapperStoreABC[T]):
    wrapped_store: StoreABC[T]

    @property
    def store(self) -> StoreABC[T]:
        return self.wrapped_store


class TestTTLCacheStore(TestInMemStore):
    """ Mostly here for coverage """

    def setUp(self):
        persisty_context = get_default_persisty_context()
        store = WrapperStore[T](in_mem_store(Band))
        setup_bands(store)
        persisty_context.register_store(store)

    def test_name(self):
        store = get_default_persisty_context().get_store(Band)
        assert store.name == getattr(store, 'wrapped_store').name
