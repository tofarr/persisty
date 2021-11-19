from unittest import TestCase

from schemey.ref_schema import RefSchema
from schemey.with_defs_schema import WithDefsSchema

from old.persisty import Capabilities, NO_CAPABILITIES, ALL_CAPABILITIES, READ_ONLY
from persisty.edit import Edit
from old.persisty import EditType
from persisty.errors import PersistyError
from schemey.any_of_schema import optional_schema
from schemey.number_schema import NumberSchema
from schemey.object_schema import ObjectSchema
from schemey.property_schema import PropertySchema
from schemey.string_schema import StringSchema
from old.persisty2.storage_filter import storage_filter_from_dataclass
from old.persisty.storage.capability_filter_storage import CapabilityFilterStorage
from old.persisty.storage.in_mem_storage import in_mem_storage
from old.persisty.storage.schema_storage import schema_storage
from old.persisty.storage_schemas import StorageSchemas
from tests.old.fixtures.data import setup_bands, BANDS
from tests.old.fixtures.items import Band, BandFilter, Issue


class TestCapabilityFilterStorage(TestCase):

    @staticmethod
    def get_band_storage(capabilities: Capabilities) -> CapabilityFilterStorage[Band]:
        storage = in_mem_storage(Band)
        setup_bands(storage)
        storage = CapabilityFilterStorage(storage, capabilities)
        return storage

    def test_allow_read(self):
        storage = self.get_band_storage(Capabilities(read=True))
        assert storage.read('beatles').band_name == 'The Beatles'

    def test_block_read(self):
        storage = self.get_band_storage(NO_CAPABILITIES)
        with(self.assertRaises(PersistyError)):
            storage.read('beatles')

    def test_allow_read_all(self):
        storage = self.get_band_storage(Capabilities(read=True))
        expected_band_names = ['The Beatles', 'The Rolling Stones']
        band_names = [b.band_name for b in storage.read_all(['beatles', 'rolling_stones'])]
        assert band_names == expected_band_names

    def test_block_read_all(self):
        storage = self.get_band_storage(NO_CAPABILITIES)
        with(self.assertRaises(PersistyError)):
            list(storage.read_all(['beatles', 'rolling_stones']))

    def test_allow_create(self):
        storage = self.get_band_storage(Capabilities(create=True))
        band = Band('jefferson_airplane', 'Jefferson Airplane')
        key = storage.create(band)
        assert storage.storage.read(key) == band

    def test_block_create(self):
        storage = self.get_band_storage(NO_CAPABILITIES)
        band = Band('jefferson_airplane', 'Jefferson Airplane')
        with(self.assertRaises(PersistyError)):
            storage.create(band)
        assert storage.storage.read('jefferson_airplane') is None

    def test_allow_update(self):
        storage = self.get_band_storage(Capabilities(update=True))
        band = storage.storage.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        storage.update(band)
        assert storage.storage.read('rolling_stones').band_name == 'The Blues Boys'

    def test_block_update(self):
        storage = self.get_band_storage(NO_CAPABILITIES)
        band = storage.storage.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        with self.assertRaises(PersistyError):
            storage.update(band)
        assert storage.storage.read('rolling_stones').band_name == 'The Rolling Stones'

    def test_allow_destroy(self):
        storage = self.get_band_storage(Capabilities(destroy=True))
        assert storage.destroy('rolling_stones') is True
        assert storage.storage.read('rolling_stones') is None

    def test_block_destroy(self):
        storage = self.get_band_storage(NO_CAPABILITIES)
        with self.assertRaises(PersistyError):
            storage.destroy('rolling_stones')
        assert storage.storage.read('rolling_stones').band_name == 'The Rolling Stones'

    def test_allow_search(self):
        storage = self.get_band_storage(Capabilities(search=True))
        bands = list(storage.search(storage_filter_from_dataclass(BandFilter(query='The', sort=['band_name']), Band)))
        expected = [b for b in BANDS if b.id in ('beatles', 'rolling_stones')]
        expected.sort(key=lambda b: b.band_name)
        assert expected == bands

    def test_block_search(self):
        storage = self.get_band_storage(NO_CAPABILITIES)
        with(self.assertRaises(PersistyError)):
            list(storage.search())

    def test_allow_paged_search(self):
        storage = self.get_band_storage(Capabilities(search=True))
        page_1 = storage.paged_search(limit=3)
        assert page_1.items == BANDS[0:3]
        assert page_1.next_page_key is not None
        page_2 = storage.paged_search(page_key=page_1.next_page_key)
        assert page_2.items == BANDS[3:]
        assert page_2.next_page_key is None

    def test_block_paged_search(self):
        storage = self.get_band_storage(NO_CAPABILITIES)
        with self.assertRaises(PersistyError):
            storage.paged_search(limit=3)

    def test_allow_count(self):
        storage = self.get_band_storage(Capabilities(search=True))
        assert storage.count() == len(BANDS)

    def test_block_count(self):
        storage = self.get_band_storage(NO_CAPABILITIES)
        with self.assertRaises(PersistyError):
            storage.count()

    def test_edit_all_allow(self):
        self._edit_all(ALL_CAPABILITIES)

    def test_edit_all_disallow_create(self):
        self._edit_all(ALL_CAPABILITIES - Capabilities(create=True))

    def test_edit_all_disallow_update(self):
        self._edit_all(ALL_CAPABILITIES - Capabilities(update=True))

    def test_edit_all_disallow_destroy(self):
        self._edit_all(ALL_CAPABILITIES - Capabilities(destroy=True))

    def _edit_all(self, capabilities: Capabilities):
        storage = self.get_band_storage(capabilities)
        beatles, rolling_stones, led_zeppelin = storage.read_all(('beatles', 'rolling_stones', 'led_zeppelin'))
        updated_stones = Band(**{**rolling_stones.__dict__, 'band_name': 'The Blues Boys'})
        jefferson = Band('jefferson_airplane', 'Jefferson Airplane')
        edits = [
            Edit(EditType.CREATE, item=jefferson),
            Edit(EditType.UPDATE, item=updated_stones),
            Edit(EditType.DESTROY, 'led_zeppelin')
        ]
        if capabilities == ALL_CAPABILITIES:
            storage.edit_all(edits)
        else:
            with self.assertRaises(PersistyError):
                storage.edit_all(edits)

        state = set(storage.search())
        if not capabilities.create:
            assert state == {beatles, rolling_stones, led_zeppelin}
        elif not capabilities.update:
            assert state == {beatles, jefferson, rolling_stones, led_zeppelin}
        elif not capabilities.destroy:
            assert state == {beatles, jefferson, updated_stones, led_zeppelin}
        else:
            assert state == {beatles, jefferson, updated_stones}

    def test_schema(self):
        storage = CapabilityFilterStorage(schema_storage(in_mem_storage(Band)), READ_ONLY)
        assert storage.name == 'Band'
        read_schema = WithDefsSchema({
            'Band': ObjectSchema[Issue](tuple((
                PropertySchema('id', StringSchema(min_length=1), True),
                PropertySchema('band_name', optional_schema(StringSchema())),
                PropertySchema('year_formed', optional_schema(NumberSchema(int))),
            )))
        }, RefSchema('Band'))
        expected = StorageSchemas(None, None, read_schema, read_schema)
        assert storage.schemas == expected
        with self.assertRaises(PersistyError):
            storage.create(Issue('Issue 4', 'issue_4'))
