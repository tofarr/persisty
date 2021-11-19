import dataclasses
from abc import ABC, abstractmethod
from unittest import TestCase

from persisty.edit import Edit
from persisty.errors import PersistyError
from persisty.storage.storage_context import StorageContext
from persisty.storage.storage_filter import storage_filter_from_dataclass
from tests.fixtures.item_types import Band, BandFilter
from tests.fixtures.storage_data import BANDS, populate_data


class TstStorageABC(TestCase, ABC):

    @abstractmethod
    def create_storage_context(self) -> StorageContext:
        """ """

    def setUp(self):
        self.storage_context = self.create_storage_context()
        populate_data(self.storage_context)

    def test_create_with_id(self):
        storage = self.storage_context.get_storage(Band)
        band = Band('who', 'The Who', 1964)
        key = storage.create(band)
        assert key == 'who'
        read = storage.read('who')
        assert read == band

    def test_create_without_id(self):
        storage = self.storage_context.get_storage(Band)
        band = Band(None, 'The Who', 1964)
        key = storage.create(band)
        assert key is not None
        assert band.id == key
        read = storage.read(key)
        assert read == band

    def test_create_duplicate(self):
        storage = self.storage_context.get_storage(Band)
        band = Band('beatles', 'Best of the Beatles', None)
        with self.assertRaises(PersistyError):
            storage.create(band)

    def test_read_missing(self):
        storage = self.storage_context.get_storage(Band)
        band = storage.read('weird_al')
        assert band is None

    def test_update(self):
        storage = self.storage_context.get_storage(Band)
        band = dataclasses.replace(next(b for b in BANDS if b.id == 'rolling_stones'))
        band.band_name = 'The Blues Boys'
        storage.update(band)
        read = storage.read(storage.meta.key_config.get_key(band))
        assert read == band

    def test_update_not_existing(self):
        storage = self.storage_context.get_storage(Band)
        band = Band('weird_al', 'Weird Al', 1959)
        with self.assertRaises(PersistyError):
            storage.update(band)
        read = storage.read('weird_al')
        assert read is None

    def test_destroy(self):
        storage = self.storage_context.get_storage(Band)
        band = BANDS[0]
        key = storage.meta.key_config.get_key(band)
        assert storage.destroy(key) is True
        assert storage.read(key) is None
        assert storage.destroy(key) is False

    def test_search_no_filter(self):
        storage = self.storage_context.get_storage(Band)
        expected_bands = list(sorted(BANDS, key=lambda b: b.id))
        bands = list(sorted(storage.search(), key=lambda b: b.id))
        assert expected_bands == bands

    def test_count_no_filter(self):
        storage = self.storage_context.get_storage(Band)
        assert len(BANDS) == storage.count()

    def test_paged_search(self):
        storage = self.storage_context.get_storage(Band)
        storage_filter = storage_filter_from_dataclass(BandFilter(sort=['title']), Band)
        expected_bands = list(sorted(BANDS, key=lambda b: b.title))
        page_1 = storage.paged_search(storage_filter, limit=2)
        assert page_1.items == expected_bands[0:2]
        page_2 = storage.paged_search(storage_filter, page_key=page_1.next_page_key, limit=2)
        assert page_2.items == expected_bands[2:4]

    def test_gt_filter(self):
        storage = self.storage_context.get_storage(Band)
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__gt=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed > 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_gte_filter(self):
        storage = self.storage_context.get_storage(Band)
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__gte=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed >= 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_lt_filter(self):
        storage = self.storage_context.get_storage(Band)
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__lt=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed < 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_lte_filter(self):
        storage = self.storage_context.get_storage(Band)
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__lte=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed <= 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_eq_filter(self):
        storage = self.storage_context.get_storage(Band)
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__eq=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed == 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_ne_filter(self):
        storage = self.storage_context.get_storage(Band)
        filter_ = storage_filter_from_dataclass(BandFilter(year_formed__ne=1962, sort=['id']), Band)
        expected_bands = (b for b in BANDS if b.year_formed != 1962)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.id))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_startswith_filter(self):
        storage = self.storage_context.get_storage(Band)
        filter_ = storage_filter_from_dataclass(BandFilter(title__startswith='The', sort=['title']), Band)
        expected_bands = (b for b in BANDS if b.title.startswith('The'))
        expected_bands = list(sorted(expected_bands, key=lambda b: b.title))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_contains_filter(self):
        storage = self.storage_context.get_storage(Band)
        filter_ = storage_filter_from_dataclass(BandFilter(title__contains='he', sort=['title']), Band)
        expected_bands = (b for b in BANDS if 'he' in b.title)
        expected_bands = list(sorted(expected_bands, key=lambda b: b.title))
        bands = list(storage.search(filter_))
        assert expected_bands == bands
        assert storage.count(filter_.item_filter) == len(expected_bands)

    def test_multi_filter(self):
        storage = self.storage_context.get_storage(Band)
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
        storage = self.storage_context.get_storage(Band)
        bands = list(storage.read_all(band_ids))
        assert bands == expected_bands

    def test_read_all_reverse_order(self):
        band_ids = tuple(('rolling_stones', 'beatles'))
        expected_bands = [next((b for b in BANDS if b.id == band_id), None) for band_id in band_ids]
        storage = self.storage_context.get_storage(Band)
        bands = list(storage.read_all(band_ids))
        assert bands == expected_bands

    def test_read_all_missing_error(self):
        band_ids = tuple(('beatles', 'jefferson_airplane', 'rolling_stones'))
        storage = self.storage_context.get_storage(Band)
        with self.assertRaises(PersistyError):
            list(storage.read_all(band_ids))

    def test_read_all_missing_none(self):
        band_ids = tuple(('beatles', 'jefferson_airplane', 'rolling_stones'))
        expected_bands = [next((b for b in BANDS if b.id == band_id), None) for band_id in band_ids]
        storage = self.storage_context.get_storage(Band)
        bands = list(storage.read_all(band_ids, False))
        assert bands == expected_bands

    def test_edit_all(self):
        storage = self.storage_context.get_storage(Band)
        created = Band('jefferson', 'Jefferson Airplane', 1965)
        updated = Band(**{**BANDS[0].__dict__, 'title': BANDS[0].title + ': Reloaded'})
        expected_bands = [created, updated]
        expected_bands.extend(BANDS[2:])
        expected_bands.sort(key=lambda b: b.title)
        edits = [
            Edit.create(created),
            Edit.update(updated),
            Edit.destroy(storage.meta.key_config.get_key(BANDS[1]))
        ]
        storage.edit_all(edits)
        filter_ = storage_filter_from_dataclass(BandFilter(sort=['title']), Band)
        bands = list(sorted(storage.search(filter_), key=lambda b: b.title))
        assert bands == expected_bands
