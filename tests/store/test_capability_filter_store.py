from unittest import TestCase

from persisty.capabilities import Capabilities, NO_CAPABILITIES, ALL_CAPABILITIES, READ_ONLY
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.schema.any_of_schema import optional_schema
from persisty.schema.number_schema import NumberSchema
from persisty.schema.object_schema import ObjectSchema
from persisty.schema.property_schema import PropertySchema
from persisty.schema.string_schema import StringSchema
from persisty.search_filter import search_filter_from_dataclass
from persisty.store.capability_filter_store import CapabilityFilterStore
from persisty.store.in_mem_store import in_mem_store
from persisty.store.schema_store import schema_store
from persisty.store_schemas import StoreSchemas
from tests.fixtures.data import setup_bands, BANDS
from tests.fixtures.items import Band, BandFilter, Issue


class TestCapabilityFilterStore(TestCase):

    @staticmethod
    def get_band_store(capabilities: Capabilities) -> CapabilityFilterStore[Band]:
        store = in_mem_store(Band)
        setup_bands(store)
        store = CapabilityFilterStore(store, capabilities)
        return store

    def test_allow_read(self):
        store = self.get_band_store(Capabilities(read=True))
        assert store.read('beatles').band_name == 'The Beatles'

    def test_block_read(self):
        store = self.get_band_store(NO_CAPABILITIES)
        with(self.assertRaises(PersistyError)):
            store.read('beatles')

    def test_allow_read_all(self):
        store = self.get_band_store(Capabilities(read=True))
        expected_band_names = ['The Beatles', 'The Rolling Stones']
        band_names = [b.band_name for b in store.read_all(['beatles', 'rolling_stones'])]
        assert band_names == expected_band_names

    def test_block_read_all(self):
        store = self.get_band_store(NO_CAPABILITIES)
        with(self.assertRaises(PersistyError)):
            list(store.read_all(['beatles', 'rolling_stones']))

    def test_allow_create(self):
        store = self.get_band_store(Capabilities(create=True))
        band = Band('jefferson_airplane', 'Jefferson Airplane')
        key = store.create(band)
        assert store.store.read(key) == band

    def test_block_create(self):
        store = self.get_band_store(NO_CAPABILITIES)
        band = Band('jefferson_airplane', 'Jefferson Airplane')
        with(self.assertRaises(PersistyError)):
            store.create(band)
        assert store.store.read('jefferson_airplane') is None

    def test_allow_update(self):
        store = self.get_band_store(Capabilities(update=True))
        band = store.store.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        store.update(band)
        assert store.store.read('rolling_stones').band_name == 'The Blues Boys'

    def test_block_update(self):
        store = self.get_band_store(NO_CAPABILITIES)
        band = store.store.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        with self.assertRaises(PersistyError):
            store.update(band)
        assert store.store.read('rolling_stones').band_name == 'The Rolling Stones'

    def test_allow_destroy(self):
        store = self.get_band_store(Capabilities(destroy=True))
        assert store.destroy('rolling_stones') is True
        assert store.store.read('rolling_stones') is None

    def test_block_destroy(self):
        store = self.get_band_store(NO_CAPABILITIES)
        with self.assertRaises(PersistyError):
            store.destroy('rolling_stones')
        assert store.store.read('rolling_stones').band_name == 'The Rolling Stones'

    def test_allow_search(self):
        store = self.get_band_store(Capabilities(search=True))
        bands = list(store.search(search_filter_from_dataclass(BandFilter(query='The', sort='band_name'), Band)))
        expected = [b for b in BANDS if b.id in ('beatles', 'rolling_stones')]
        expected.sort(key=lambda b: b.band_name)
        assert expected == bands

    def test_block_search(self):
        store = self.get_band_store(NO_CAPABILITIES)
        with(self.assertRaises(PersistyError)):
            list(store.search())

    def test_allow_paged_search(self):
        store = self.get_band_store(Capabilities(search=True))
        page_1 = store.paged_search(limit=3)
        assert page_1.items == BANDS[0:3]
        assert page_1.next_page_key is not None
        page_2 = store.paged_search(page_key=page_1.next_page_key)
        assert page_2.items == BANDS[3:]
        assert page_2.next_page_key is None

    def test_block_paged_search(self):
        store = self.get_band_store(NO_CAPABILITIES)
        with self.assertRaises(PersistyError):
            store.paged_search(limit=3)

    def test_allow_count(self):
        store = self.get_band_store(Capabilities(search=True))
        assert store.count() == len(BANDS)

    def test_block_count(self):
        store = self.get_band_store(NO_CAPABILITIES)
        with self.assertRaises(PersistyError):
            store.count()

    def test_edit_all_allow(self):
        self._edit_all(ALL_CAPABILITIES)

    def test_edit_all_disallow_create(self):
        self._edit_all(ALL_CAPABILITIES - Capabilities(create=True))

    def test_edit_all_disallow_update(self):
        self._edit_all(ALL_CAPABILITIES - Capabilities(update=True))

    def test_edit_all_disallow_destroy(self):
        self._edit_all(ALL_CAPABILITIES - Capabilities(destroy=True))

    def _edit_all(self, capabilities: Capabilities):
        store = self.get_band_store(capabilities)
        beatles, rolling_stones, led_zeppelin = store.read_all(('beatles', 'rolling_stones', 'led_zeppelin'))
        updated_stones = Band(**{**rolling_stones.__dict__, 'band_name': 'The Blues Boys'})
        jefferson = Band('jefferson_airplane', 'Jefferson Airplane')
        edits = [
            Edit(EditType.CREATE, item=jefferson),
            Edit(EditType.UPDATE, item=updated_stones),
            Edit(EditType.DESTROY, 'led_zeppelin')
        ]
        if capabilities == ALL_CAPABILITIES:
            store.edit_all(edits)
        else:
            with self.assertRaises(PersistyError):
                store.edit_all(edits)

        state = set(store.search())
        if not capabilities.create:
            assert state == {beatles, rolling_stones, led_zeppelin}
        elif not capabilities.update:
            assert state == {beatles, jefferson, rolling_stones, led_zeppelin}
        elif not capabilities.destroy:
            assert state == {beatles, jefferson, updated_stones, led_zeppelin}
        else:
            assert state == {beatles, jefferson, updated_stones}

    def test_schema(self):
        store = (CapabilityFilterStore(schema_store(in_mem_store(Band)), READ_ONLY))
        assert store.name == 'Band'
        read_schema = ObjectSchema[Issue](tuple((
            PropertySchema('id', StringSchema(min_length=1)),
            PropertySchema('band_name', optional_schema(StringSchema())),
            PropertySchema('year_formed', optional_schema(NumberSchema(int))),
        )))
        expected = StoreSchemas(None, None, read_schema)
        assert store.schemas == expected
        with self.assertRaises(PersistyError):
            store.create(Issue('Issue 4', 'issue_4'))
