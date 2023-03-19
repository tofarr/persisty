from uuid import UUID

from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty.impl.mem.mem_store import MemStore
from persisty.store_meta import get_meta
from persisty.stored import stored


@stored
class Message:
    id: UUID
    owner: str
    text: str


store = MemStore(get_meta(Message))
factory = DefaultStoreFactory(store)
