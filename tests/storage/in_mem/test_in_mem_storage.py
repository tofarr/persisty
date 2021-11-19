from persisty.storage.in_mem.in_mem_storage import in_mem_storage
from persisty.storage.storage_context import StorageContext
from persisty.storage.wrappers.timestamped_storage import with_timestamps
from tests.fixtures.item_types import Band, Member, Tag, Node, NodeTag
from tests.storage.tst_storage_abc import TstStorageABC


class TestInMemStorage(TstStorageABC):

    def create_storage_context(self):
        storage_context = StorageContext()
        storage_context.register_storage(in_mem_storage(Band))
        storage_context.register_storage(in_mem_storage(Member))
        storage_context.register_storage(with_timestamps(in_mem_storage(Tag)))
        storage_context.register_storage(with_timestamps(in_mem_storage(Node)))
        storage_context.register_storage(with_timestamps(in_mem_storage(NodeTag)))
        return storage_context
