from old.persisty.persisty_context import get_default_persisty_context
from old.persisty.storage.in_mem_storage import in_mem_storage
from old.persisty.storage.ttl_cache_storage import TTLCacheStorage
from tests.fixtures.data import setup_bands
from tests.fixtures.items import Band
from tests import TestInMemStorage


class TestTTLCacheStorage(TestInMemStorage):

    def setUp(self):
        persisty_context = get_default_persisty_context()
        storage = TTLCacheStorage(in_mem_storage(Band))
        setup_bands(storage)
        persisty_context.register_storage(storage)

    def test_name(self):
        storage = get_default_persisty_context().get_storage(Band)
        assert storage.name == getattr(storage, 'wrapped_storage').name

    def test_cache_is_disconnected_from_object_graph(self):
        """ Caching live objects without calls to 'update' could result in all sorts of chaos """
        storage = get_default_persisty_context().get_storage(Band)
        band = storage.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        read = storage.read('rolling_stones')
        assert read != band

    def test_cache_is_present(self):
        """ Make sure things that go around the cache are not visible. """
        storage = get_default_persisty_context().get_storage(Band)
        assert isinstance(storage, TTLCacheStorage)
        band = storage.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        storage.storage.update(band)  # Going around the cache
        band = storage.read('rolling_stones')
        assert band.band_name == 'The Rolling Stones'

    def test_clear_read(self):
        """ Make sure things that go around the cache are not visible. """
        storage = get_default_persisty_context().get_storage(Band)
        assert isinstance(storage, TTLCacheStorage)
        band = storage.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        storage.storage.update(band)  # Going around the cache
        storage.clear()
        band = storage.read('rolling_stones')
        assert band.band_name == 'The Blues Boys'

    def test_clear_read_all(self):
        """ Make sure things that go around the cache are not visible. """
        storage = get_default_persisty_context().get_storage(Band)
        assert isinstance(storage, TTLCacheStorage)
        band = storage.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        storage.storage.update(band)  # Going around the cache
        storage.clear()
        band = next(storage.read_all(('rolling_stones',)))
        assert band.band_name == 'The Blues Boys'
