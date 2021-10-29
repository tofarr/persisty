from persisty import get_persisty_context
from persisty.store.in_mem_store import in_mem_store
from persisty.store.ttl_cache_store import TTLCacheStore
from tests.fixtures.data import setup_bands
from tests.fixtures.items import Band, BandFilter
from tests.store.test_in_mem_store import TestInMemStore


class TestTTLCacheStore(TestInMemStore):

    def setUp(self):
        persisty_context = get_persisty_context()
        store = TTLCacheStore(in_mem_store(Band))
        setup_bands(store)
        persisty_context.register_store(store)

    def test_name(self):
        store = get_persisty_context().get_store(Band)
        assert store.name == store.wrapped_store.name

    def test_cache_is_disconnected_from_object_graph(self):
        """ Caching live objects without calls to 'update' could result in all sorts of chaos """
        store = get_persisty_context().get_store(Band)
        band = store.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        read = store.read('rolling_stones')
        assert read != band

    def test_cache_is_present(self):
        """ Make sure things that go around the cache are not visible. """
        store = get_persisty_context().get_store(Band)
        assert isinstance(store, TTLCacheStore)
        band = store.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        store.store.update(band)  # Going around the cache
        band = store.read('rolling_stones')
        assert band.band_name == 'The Rolling Stones'

    def test_clear_read(self):
        """ Make sure things that go around the cache are not visible. """
        store = get_persisty_context().get_store(Band)
        assert isinstance(store, TTLCacheStore)
        band = store.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        store.store.update(band)  # Going around the cache
        store.clear()
        band = store.read('rolling_stones')
        assert band.band_name == 'The Blues Boys'

    def test_clear_read_all(self):
        """ Make sure things that go around the cache are not visible. """
        store = get_persisty_context().get_store(Band)
        assert isinstance(store, TTLCacheStore)
        band = store.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        store.store.update(band)  # Going around the cache
        store.clear()
        band = next(store.read_all(('rolling_stones',)))
        assert band.band_name == 'The Blues Boys'
