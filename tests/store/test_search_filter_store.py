from old.persisty.persisty_context import get_default_persisty_context
from persisty.edit import Edit
from old.persisty import EditType
from old.persisty import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from old.persisty.storage.in_mem_storage import in_mem_storage
from old.persisty.storage.storage_filter_storage import StorageFilterStorage
from tests.fixtures.data import setup_bands
from tests.fixtures.items import Band
from tests import TestInMemStorage


class TestStorageFilterStorage(TestInMemStorage):

    def setUp(self):
        persisty_context = get_default_persisty_context()
        wrapped_storage = in_mem_storage(Band)
        wrapped_storage.create(Band('mozart', 'Mozart', 1756))  # mostly filtered out
        storage = StorageFilterStorage(wrapped_storage, AttrFilter('year_formed', AttrFilterOp.gte, 1900))
        setup_bands(storage)
        self.wrapped_storage = wrapped_storage
        persisty_context.register_storage(storage)

    def test_read_blocked_key(self):
        assert self.get_band_storage().read('mozart') is None

    def test_read_all_blocked_key_allow_missing(self):
        bands = list(self.get_band_storage().read_all(('beatles', 'mozart'), False))
        assert bands[0].band_name == 'The Beatles'
        assert bands[1] is None

    def test_read_all_blocked_key_error_on_missing(self):
        with self.assertRaises(PersistyError):
            list(self.get_band_storage().read_all(('beatles', 'mozart')))

    def test_create_blocked(self):
        with self.assertRaises(PersistyError):
            self.get_band_storage().create(Band('mozart', 'Wolfgang Amadeus Mozart', 1756))
        assert self.wrapped_storage.read('mozart').band_name == 'Mozart'

    def test_update_blocked(self):
        with self.assertRaises(PersistyError):
            self.get_band_storage().update(Band('mozart', 'Wolfgang Amadeus Mozart', 1756))
        assert self.wrapped_storage.read('mozart').band_name == 'Mozart'

    def test_destroy_blocked(self):
        assert self.get_band_storage().destroy('mozart') is False
        assert self.wrapped_storage.read('mozart').band_name == 'Mozart'

    def test_edit_all_create_blocked(self):
        with self.assertRaises(PersistyError):
            edits = [Edit(EditType.CREATE, None, Band('mozart', 'Wolfgang Amadeus Mozart', 1756))]
            self.get_band_storage().edit_all(edits)
        assert self.wrapped_storage.read('mozart').band_name == 'Mozart'

    def test_edit_all_update_blocked(self):
        with self.assertRaises(PersistyError):
            edits = [Edit(EditType.UPDATE, None, Band('mozart', 'Wolfgang Amadeus Mozart', 1756))]
            self.get_band_storage().edit_all(edits)
        assert self.wrapped_storage.read('mozart').band_name == 'Mozart'

    def test_edit_all_destroy_blocked(self):
        edits = [Edit(EditType.DESTROY, 'mozart')]
        self.get_band_storage().edit_all(edits)
        assert self.wrapped_storage.read('mozart').band_name == 'Mozart'

    def test_edit_all_destroy_blocked_with_prev_edit(self):
        jefferson = Band('jefferson', 'Jefferson Airplane', 1965)
        edits = [Edit(EditType.CREATE, item=jefferson),
                 Edit(EditType.DESTROY, 'mozart')]
        self.get_band_storage().edit_all(edits)
        assert self.wrapped_storage.read('mozart').band_name == 'Mozart'
        assert self.get_band_storage().read('jefferson') == jefferson
