from unittest import TestCase

from persisty.access_control.access_control import AccessControl, NO_ACCESS, ALL_ACCESS
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError

from persisty.storage.in_mem.in_mem_storage import in_mem_storage
from persisty.storage.storage_filter import storage_filter_from_dataclass
from persisty.storage.wrappers.access_filtered_storage import AccessFilteredStorage, with_access_filtered
from tests.fixtures.item_types import Band, BandFilter
from tests.fixtures.storage_data import BANDS


class TestAccessFilteredStorage(TestCase):

    @staticmethod
    def get_band_storage(access_control: AccessControlABC) -> AccessFilteredStorage[Band]:
        storage = in_mem_storage(Band)
        for band in BANDS:
            storage.create(band)
        storage = with_access_filtered(storage, access_control)
        return storage

    def test_allow_read(self):
        storage = self.get_band_storage(AccessControl(is_readable=True))
        assert storage.read('beatles').title == 'The Beatles'

    def test_block_read(self):
        storage = self.get_band_storage(NO_ACCESS)
        with(self.assertRaises(PersistyError)):
            storage.read('beatles')

    def test_allow_read_all(self):
        storage = self.get_band_storage(AccessControl(is_readable=True))
        expected_band_names = ['The Beatles', 'The Rolling Stones']
        band_names = [b.title for b in storage.read_all(['beatles', 'rolling_stones'])]
        assert band_names == expected_band_names

    def test_block_read_all(self):
        storage = self.get_band_storage(NO_ACCESS)
        with(self.assertRaises(PersistyError)):
            list(storage.read_all(['beatles', 'rolling_stones']))

    def test_allow_create(self):
        storage = self.get_band_storage(AccessControl(is_creatable=True))
        band = Band('jefferson_airplane', 'Jefferson Airplane')
        key = storage.create(band)
        assert storage.storage.read(key) == band

    def test_block_create(self):
        storage = self.get_band_storage(NO_ACCESS)
        band = Band('jefferson_airplane', 'Jefferson Airplane')
        with(self.assertRaises(PersistyError)):
            storage.create(band)
        assert storage.storage.read('jefferson_airplane') is None

    def test_allow_update(self):
        storage = self.get_band_storage(AccessControl(is_updatable=True))
        band = storage.storage.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        storage.update(band)
        assert storage.storage.read('rolling_stones').title == 'The Blues Boys'

    def test_block_update(self):
        storage = self.get_band_storage(NO_ACCESS)
        band = storage.storage.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        with self.assertRaises(PersistyError):
            storage.update(band)
        assert storage.storage.read('rolling_stones').title == 'The Rolling Stones'

    def test_allow_destroy(self):
        storage = self.get_band_storage(AccessControl(is_destroyable=True))
        assert storage.destroy('rolling_stones') is True
        assert storage.storage.read('rolling_stones') is None

    def test_block_destroy(self):
        storage = self.get_band_storage(NO_ACCESS)
        with self.assertRaises(PersistyError):
            storage.destroy('rolling_stones')
        assert storage.storage.read('rolling_stones').title == 'The Rolling Stones'

    def test_allow_search(self):
        storage = self.get_band_storage(AccessControl(is_searchable=True))
        bands = list(storage.search(storage_filter_from_dataclass(BandFilter(query='The', sort=['band_name']), Band)))
        expected = [b for b in BANDS if b.id in ('beatles', 'rolling_stones')]
        expected.sort(key=lambda b: b.band_name)
        assert expected == bands

    def test_block_search(self):
        storage = self.get_band_storage(NO_ACCESS)
        with(self.assertRaises(PersistyError)):
            list(storage.search())

    def test_allow_paged_search(self):
        storage = self.get_band_storage(AccessControl(is_searchable=True))
        page_1 = storage.paged_search(limit=3)
        assert page_1.items == BANDS[0:3]
        assert page_1.next_page_key is not None
        page_2 = storage.paged_search(page_key=page_1.next_page_key)
        assert page_2.items == BANDS[3:]
        assert page_2.next_page_key is None

    def test_block_paged_search(self):
        storage = self.get_band_storage(NO_ACCESS)
        with self.assertRaises(PersistyError):
            storage.paged_search(limit=3)

    def test_allow_count(self):
        storage = self.get_band_storage(AccessControl(is_searchable=True))
        assert storage.count() == len(BANDS)

    def test_block_count(self):
        storage = self.get_band_storage(NO_ACCESS)
        with self.assertRaises(PersistyError):
            storage.count()

    def test_edit_all_allow(self):
        self._edit_all(ALL_ACCESS)

    def test_edit_all_disallow_create(self):
        self._edit_all(ALL_ACCESS - AccessControl(is_creatable=True))

    def test_edit_all_disallow_update(self):
        self._edit_all(ALL_ACCESS - AccessControl(is_updatable=True))

    def test_edit_all_disallow_destroy(self):
        self._edit_all(ALL_ACCESS - AccessControl(is_destroyable=True))

    def _edit_all(self, access_control: AccessControlABC):
        storage = self.get_band_storage(access_control)
        beatles, rolling_stones, led_zeppelin = storage.read_all(('beatles', 'rolling_stones', 'led_zeppelin'))
        updated_stones = Band(**{**rolling_stones.__dict__, 'band_name': 'The Blues Boys'})
        jefferson = Band('jefferson_airplane', 'Jefferson Airplane')
        edits = [
            Edit(EditType.CREATE, item=jefferson),
            Edit(EditType.UPDATE, item=updated_stones),
            Edit(EditType.DESTROY, 'led_zeppelin')
        ]
        if access_control == ALL_ACCESS:
            storage.edit_all(edits)
        else:
            with self.assertRaises(PersistyError):
                storage.edit_all(edits)

        state = set(storage.search())
        if not access_control.is_creatable:
            assert state == {beatles, rolling_stones, led_zeppelin}
        elif not access_control.is_updatable:
            assert state == {beatles, jefferson, rolling_stones, led_zeppelin}
        elif not access_control.is_destroyable:
            assert state == {beatles, jefferson, updated_stones, led_zeppelin}
        else:
            assert state == {beatles, jefferson, updated_stones}
