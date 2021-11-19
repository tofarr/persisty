from persisty.storage.in_mem.in_mem_storage import in_mem_storage
from persisty.storage.storage_context import StorageContext
from persisty.storage.wrappers.timestamped_storage import with_timestamps
from persisty.storage.wrappers.ttl_cache_storage import TTLCacheStorage
from tests.fixtures.item_types import Band, Member, Tag, Node, NodeTag
from tests.storage.tst_storage_abc import TstStorageABC


class TestTTLCacheStorage(TstStorageABC):

    def create_storage_context(self):
        storage_context = StorageContext()
        storage_context.register_storage(TTLCacheStorage(in_mem_storage(Band)))
        storage_context.register_storage(TTLCacheStorage(in_mem_storage(Member)))
        storage_context.register_storage(TTLCacheStorage(with_timestamps(in_mem_storage(Tag))))
        storage_context.register_storage(TTLCacheStorage(with_timestamps(in_mem_storage(Node))))
        storage_context.register_storage(TTLCacheStorage(with_timestamps(in_mem_storage(NodeTag))))
        return storage_context

    def test_name(self):
        storage = self.storage_context.get_storage(Band)
        assert storage.meta.name == getattr(storage, 'wrapped_storage').meta.name

    def test_cache_is_disconnected_from_object_graph(self):
        """ Caching live objects without calls to 'update' could result in all sorts of chaos """
        storage = self.storage_context.get_storage(Band)
        band = storage.read('rolling_stones')
        band.title = 'The Blues Boys'
        read = storage.read('rolling_stones')
        assert read != band

    def test_cache_is_present(self):
        """ Make sure things that go around the cache are not visible. """
        storage = self.storage_context.get_storage(Band)
        assert isinstance(storage, TTLCacheStorage)
        band = storage.read('rolling_stones')
        band.title = 'The Blues Boys'
        storage.storage.update(band)  # Going around the cache
        band = storage.read('rolling_stones')
        assert band.title == 'The Rolling Stones'

    def test_clear_read(self):
        """ Make sure things that go around the cache are not visible. """
        storage = self.storage_context.get_storage(Band)
        assert isinstance(storage, TTLCacheStorage)
        band = storage.read('rolling_stones')
        band.title = 'The Blues Boys'
        storage.storage.update(band)  # Going around the cache
        storage.clear()
        band = storage.read('rolling_stones')
        assert band.title == 'The Blues Boys'

    def test_clear_read_all(self):
        """ Make sure things that go around the cache are not visible. """
        storage = self.storage_context.get_storage(Band)
        assert isinstance(storage, TTLCacheStorage)
        band = storage.read('rolling_stones')
        band.title = 'The Blues Boys'
        storage.storage.update(band)  # Going around the cache
        storage.clear()
        band = next(storage.read_all(('rolling_stones',)))
        assert band.title == 'The Blues Boys'
