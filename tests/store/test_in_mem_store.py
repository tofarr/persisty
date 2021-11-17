import dataclasses
import logging
from unittest import TestCase

from marshy import get_default_context

from old.persisty.persisty_context import get_default_persisty_context
from old.persisty import Capabilities
from persisty.edit import Edit
from old.persisty import PersistyError
from old.persisty2.storage_filter import storage_filter_from_dataclass
from old.persisty.storage.in_mem_storage import in_mem_storage, InMemStorage
from old.persisty.storage import LoggingStorage
from old.persisty.storage.storage_abc import StorageABC
from old.persisty.storage_schemas import NO_SCHEMAS
from tests.fixtures.data import setup_bands, BANDS
from tests.fixtures.items import Band, BandFilter


class TestInMemStorage(TestCase):

    def setUp(self):
        logging.basicConfig(level='INFO', format='%(message)s')
        persisty_context = get_default_persisty_context()
        storage = LoggingStorage(in_mem_storage(Band))
        setup_bands(storage)
        persisty_context.register_storage(storage)

    @classmethod
    def get_band_storage(cls) -> StorageABC[Band]:
        persisty_context = get_default_persisty_context()
        storage = persisty_context.get_storage(Band)
        return storage

    def test_init(self):
        storage = InMemStorage(get_default_context().get_marshaller(Band))
        assert storage.name == Band.__name__

    def test_get_item_type(self):
        storage = self.get_band_storage()
        assert storage.item_type == Band

    def test_get_capabilities(self):
        storage = self.get_band_storage()
        capabilities = storage.capabilities
        assert isinstance(capabilities, Capabilities)
        assert capabilities.read

    def test_get_schemas(self):
        storage = self.get_band_storage()
        assert storage.schemas is NO_SCHEMAS

    def test_create_with_key(self):
        storage = self.get_band_storage()
        band = Band('queen', 'Queen', 1970)
        key = storage.create(band)
        read = storage.read(key)
        assert read == band

    def test_create_duplicate(self):
        storage = self.get_band_storage()
        with self.assertRaises(PersistyError):
            storage.create(BANDS[0])

    def test_create_without_id(self):
        # Create may be missing an id
        storage = self.get_band_storage()
        band = Band(band_name='Nirvana', year_formed=1987)
        key = storage.create(band)
        read = storage.read(key)
        assert key is not None
        # id may have been reset
        band.id = read.id
        assert read == band

    def test_read_missing(self):
        storage = self.get_band_storage()
        band = storage.read('weird_al')
        assert band is None

    def test_update(self):
        storage = self.get_band_storage()
        band = dataclasses.replace(next(b for b in BANDS if b.id == 'rolling_stones'))
        band.band_name = 'The Blues Boys'
        storage.update(band)
        read = storage.read(storage.get_key(band))
        assert read == band

    def test_update_not_existing(self):
        storage = self.get_band_storage()
        band = Band('weird_al', 'Weird Al', 1959)
        with self.assertRaises(PersistyError):
            storage.update(band)
        read = storage.read('weird_al')
        assert read is None

    def test_destroy(self):
        storage = self.get_band_storage()
        band = BANDS[0]
        key = storage.get_key(band)
        assert storage.destroy(key) is True
        assert storage.read(key) is None
        assert storage.destroy(key) is False

    def test_search_no_filter(self):
        storage = self.get_band_storage()
        expected_bands = list(sorted(BANDS, key=lambda b: b.id))
        bands = list(sorted(storage.search(), key=lambda b: b.id))
        assert expected_bands == bands

    def test_count_no_filter(self):
        storage = self.get_band_storage()
        assert len(BANDS) == storage.count()

    def test_paged_search(self):
        storage = self.get_band_storage()
        storage_filter = storage_filter_from_dataclass(BandFilter(sort=['band_name']), Band)
        expected_bands = list(sorted(BANDS, key=lambda b: b.band_name))
        page_1 = storage.paged_search(storage_filter, limit=2)
        assert page_1.items == expected_bands[0:2]
        page_2 = storage.paged_search(storage_filter, page_key=page_1.next_page_key, limit=2)
        assert page_2.items == expected_bands[2:4]

    def test_gt_filter(self):
        storage = self.get_band_storage()
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__gt=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed > 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_gte_filter(self):
        storage = self.get_band_storage()
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__gte=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed >= 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_lt_filter(self):
        storage = self.get_band_storage()
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__lt=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed < 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_lte_filter(self):
        storage = self.get_band_storage()
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__lte=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed <= 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_eq_filter(self):
        storage = self.get_band_storage()
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__eq=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed == 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_ne_filter(self):
        storage = self.get_band_storage()
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__ne=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed != 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_startswith_filter(self):
        storage = self.get_band_storage()
        filter_ = storage_filter_from_dataclass(BandFilter(band_name__startswith='The', sort=['band_name']), Band)
        expected_bands = (b for b in BANDS if b.band_name.startswith('The'))
        expected_bands = list(sorted(expected_bands, key=lambda b: b.band_name))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_contains_filter(self):
        storage = self.get_band_storage()
        filter_ = storage_filter_from_dataclass(BandFilter(band_name__contains='he', sort=['band_name']), Band)
        expected_bands = (b for b in BANDS if 'he' in b.band_name)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.band_name))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_multi_filter(self):
        storage = self.get_band_storage()
        filter_ = BandFilter(year_formed__gt=BANDS[0].year_formed - 10,
                             year_formed__lt=BANDS[0].year_formed + 10)
        filter_ = storage_filter_from_dataclass(filter_, Band)
        expected_bands = {b for b in BANDS
                          if b.year_formed - 10 < BANDS[0].year_formed < b.year_formed + 10}
        bands = set(storage.search(filter_))
        assert expected_bands == bands

    def test_read_all(self):
        band_ids = tuple(('beatles', 'rolling_stones'))
        expected_bands = [next((b for b in BANDS if b.id == band_id), None) for band_id in band_ids]
        storage = self.get_band_storage()
        bands = list(storage.read_all(band_ids))
        assert bands == expected_bands

    def test_read_all_reverse_order(self):
        band_ids = tuple(('rolling_stones', 'beatles'))
        expected_bands = [next((b for b in BANDS if b.id == band_id), None) for band_id in band_ids]
        storage = self.get_band_storage()
        bands = list(storage.read_all(band_ids))
        assert bands == expected_bands

    def test_read_all_missing_error(self):
        band_ids = tuple(('beatles', 'jefferson_airplane', 'rolling_stones'))
        with self.assertRaises(PersistyError):
            list(self.get_band_storage().read_all(band_ids))

    def test_read_all_missing_none(self):
        band_ids = tuple(('beatles', 'jefferson_airplane', 'rolling_stones'))
        expected_bands = [next((b for b in BANDS if b.id == band_id), None) for band_id in band_ids]
        storage = self.get_band_storage()
        bands = list(storage.read_all(band_ids, False))
        assert bands == expected_bands

    def test_edit_all(self):
        storage = self.get_band_storage()
        created = Band('jefferson', 'Jefferson Airplane', 1965)
        updated = Band(**{**BANDS[0].__dict__, 'band_name': BANDS[0].band_name + ': Reloaded'})
        expected_bands = [created, updated]
        expected_bands.extend(BANDS[2:])
        expected_bands.sort(key=lambda b: b.band_name)
        edits = [
            Edit.create(created),
            Edit.update(updated),
            Edit.destroy(storage.get_key(BANDS[1]))
        ]
        storage.edit_all(edits)
        filter_ = storage_filter_from_dataclass(BandFilter(sort=['band_name']), Band)
        bands = list(sorted(storage.search(filter_), key=lambda b: b.band_name))
        assert bands == expected_bands
