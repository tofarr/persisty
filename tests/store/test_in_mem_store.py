import dataclasses
from unittest import TestCase

from marshy import get_default_context

from persisty import get_persisty_context
from persisty.capabilities import Capabilities
from persisty.edit import Edit
from persisty.errors import PersistyError
from persisty.search_filter import search_filter_from_dataclass
from persisty.store.in_mem_store import in_mem_store, InMemStore
from persisty.store.store_abc import StoreABC
from tests.fixtures.data import setup_bands, BANDS
from tests.fixtures.items import Band, BandFilter


class TestInMemStore(TestCase):

    def setUp(self):
        persisty_context = get_persisty_context()
        store = in_mem_store(Band)
        setup_bands(store)
        persisty_context.register_store(store)

    @classmethod
    def get_band_store(cls) -> StoreABC[Band]:
        persisty_context = get_persisty_context()
        store = persisty_context.get_store(Band)
        return store

    def test_init(self):
        store = InMemStore(get_default_context().get_marshaller(Band))
        assert store.name == Band.__name__

    def test_get_item_type(self):
        store = self.get_band_store()
        assert store.item_type == Band

    def test_get_capabilities(self):
        store = self.get_band_store()
        capabilities = store.capabilities
        assert isinstance(capabilities, Capabilities)
        assert capabilities.read

    def test_create_with_key(self):
        store = self.get_band_store()
        band = Band('queen', 'Queen', 1970)
        key = store.create(band)
        read = store.read(key)
        assert read == band

    def test_create_duplicate(self):
        store = self.get_band_store()
        with self.assertRaises(PersistyError):
            key = store.create(BANDS[0])

    def test_create_without_id(self):
        # Create may be missing an id
        store = self.get_band_store()
        band = Band(band_name='Nirvana', year_formed=1987)
        key = store.create(band)
        read = store.read(key)
        assert key is not None
        # id may have been reset
        band.id = read.id
        assert read == band

    def test_read_missing(self):
        store = self.get_band_store()
        band = store.read('weird_al')
        assert band is None

    def test_update(self):
        store = self.get_band_store()
        band = dataclasses.replace(next(b for b in BANDS if b.id == 'rolling_stones'))
        band.band_name = 'The Blues Boys'
        store.update(band)
        read = store.read(store.get_key(band))
        assert read == band

    def test_update_not_existing(self):
        store = self.get_band_store()
        band = Band('weird_al', 'Weird Al', 1959)
        with self.assertRaises(PersistyError):
            store.update(band)
        read = store.read('weird_al')
        assert read is None

    def test_destroy(self):
        store = self.get_band_store()
        band = BANDS[0]
        key = store.get_key(band)
        assert store.destroy(key) is True
        assert store.read(key) is None
        assert store.destroy(key) is False

    def test_search_no_filter(self):
        store = self.get_band_store()
        expected_bands = list(sorted(BANDS, key=lambda b: b.id))
        bands = list(sorted(store.search(), key=lambda b: b.id))
        assert expected_bands == bands

    def test_count_no_filter(self):
        store = self.get_band_store()
        assert len(BANDS) == store.count()

    def test_paged_search(self):
        store = self.get_band_store()
        search_filter = search_filter_from_dataclass(BandFilter(sort='band_name'), Band)
        expected_bands = list(sorted(BANDS, key=lambda b: b.band_name))
        page_1 = store.paged_search(search_filter, limit=2)
        assert page_1.items == expected_bands[0:2]
        page_2 = store.paged_search(search_filter, page_key=page_1.next_page_key, limit=2)
        assert page_2.items == expected_bands[2:4]

    def test_gt_filter(self):
        store = self.get_band_store()
        filter_ = search_filter_from_dataclass(BandFilter(year_formed__gt=1962, sort='id'), Band)
        expected_bands = (b for b in BANDS if b.year_formed > 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(store.search(filter_))
        assert expected_bands == bands
        assert store.count(filter_) == len(expected_bands)

    def test_gte_filter(self):
        store = self.get_band_store()
        filter_ = search_filter_from_dataclass(BandFilter(year_formed__gte=1962, sort='id'), Band)
        expected_bands = (b for b in BANDS if b.year_formed >= 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(store.search(filter_))
        assert expected_bands == bands
        assert store.count(filter_) == len(expected_bands)

    def test_lt_filter(self):
        store = self.get_band_store()
        filter_ = search_filter_from_dataclass(BandFilter(year_formed__lt=1962, sort='id'), Band)
        expected_bands = (b for b in BANDS if b.year_formed < 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(store.search(filter_))
        assert expected_bands == bands
        assert store.count(filter_) == len(expected_bands)

    def test_lte_filter(self):
        store = self.get_band_store()
        filter_ = search_filter_from_dataclass(BandFilter(year_formed__lte=1962, sort='id'), Band)
        expected_bands = (b for b in BANDS if b.year_formed <= 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(store.search(filter_))
        assert expected_bands == bands
        assert store.count(filter_) == len(expected_bands)

    def test_eq_filter(self):
        store = self.get_band_store()
        filter_ = search_filter_from_dataclass(BandFilter(year_formed__eq=1962, sort='id'), Band)
        expected_bands = (b for b in BANDS if b.year_formed == 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(store.search(filter_))
        assert expected_bands == bands
        assert store.count(filter_) == len(expected_bands)

    def test_ne_filter(self):
        store = self.get_band_store()
        filter_ = search_filter_from_dataclass(BandFilter(year_formed__ne=1962, sort='id'), Band)
        expected_bands = (b for b in BANDS if b.year_formed != 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(store.search(filter_))
        assert expected_bands == bands
        assert store.count(filter_) == len(expected_bands)

    def test_startswith_filter(self):
        store = self.get_band_store()
        filter_ = search_filter_from_dataclass(BandFilter(band_name__startswith='The', sort='band_name'), Band)
        expected_bands = (b for b in BANDS if b.band_name.startswith('The'))
        expected_bands = list(sorted(expected_bands, key=lambda b: b.band_name))
        bands = list(store.search(filter_))
        assert expected_bands == bands
        assert store.count(filter_) == len(expected_bands)

    def test_contains_filter(self):
        store = self.get_band_store()
        filter_ = search_filter_from_dataclass(BandFilter(band_name__contains='he', sort='band_name'), Band)
        expected_bands = (b for b in BANDS if 'he' in b.band_name)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.band_name))
        bands = list(store.search(filter_))
        assert expected_bands == bands
        assert store.count(filter_) == len(expected_bands)

    def test_multi_filter(self):
        store = self.get_band_store()
        filter_ = BandFilter(year_formed__gt=BANDS[0].year_formed - 10,
                             year_formed__lt=BANDS[0].year_formed + 10)
        filter_ = search_filter_from_dataclass(filter_, Band)
        expected_bands = {b for b in BANDS
                          if b.year_formed - 10 < BANDS[0].year_formed < b.year_formed + 10}
        bands = set(store.search(filter_))
        assert expected_bands == bands

    def test_read_all(self):
        band_ids = tuple(('beatles', 'rolling_stones'))
        expected_bands = [next((b for b in BANDS if b.id == band_id), None) for band_id in band_ids]
        store = self.get_band_store()
        bands = list(store.read_all(band_ids))
        assert bands == expected_bands

    def test_read_all_reverse_order(self):
        band_ids = tuple(('rolling_stones', 'beatles'))
        expected_bands = [next((b for b in BANDS if b.id == band_id), None) for band_id in band_ids]
        store = self.get_band_store()
        bands = list(store.read_all(band_ids))
        assert bands == expected_bands

    def test_read_all_missing_error(self):
        band_ids = tuple(('beatles', 'jefferson_airplane', 'rolling_stones'))
        with self.assertRaises(PersistyError):
            list(self.get_band_store().read_all(band_ids))

    def test_read_all_missing_none(self):
        band_ids = tuple(('beatles', 'jefferson_airplane', 'rolling_stones'))
        expected_bands = [next((b for b in BANDS if b.id == band_id), None) for band_id in band_ids]
        store = self.get_band_store()
        bands = list(store.read_all(band_ids, False))
        assert bands == expected_bands

    def test_edit_all(self):
        store = self.get_band_store()
        created = Band('jefferson', 'Jefferson Airplane', 1965)
        updated = Band(**{**BANDS[0].__dict__, 'band_name': BANDS[0].band_name + ': Reloaded'})
        expected_bands = [created, updated]
        expected_bands.extend(BANDS[2:])
        expected_bands.sort(key=lambda b: b.band_name)
        edits = [
            Edit.create(created),
            Edit.update(updated),
            Edit.destroy(store.get_key(BANDS[1]))
        ]
        store.edit_all(edits)
        filter_ = search_filter_from_dataclass(BandFilter(sort='band_name'), Band)
        bands = list(sorted(store.search(filter_), key=lambda b: b.band_name))
        assert bands == expected_bands
