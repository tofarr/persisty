from typing import Optional
from unittest import TestCase
from uuid import UUID

from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty.impl.mem.mem_store import MemStore
from persisty.store_meta import get_meta
from persisty.stored import stored


@stored
class Contact:
    id: UUID
    name: str
    email: Optional[str]
    phone: Optional[str]


class TestDefaultStoreFactory(TestCase):
    def test_getters(self):
        meta = get_meta(Contact)
        store = MemStore(meta)
        factory = DefaultStoreFactory(store)
        self.assertEqual(meta, factory.get_meta())
        self.assertEqual(store, factory.create(None))
