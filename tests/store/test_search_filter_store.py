from persisty import get_persisty_context
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.store.in_mem_store import mem_store
from persisty.store.search_filter_store import SearchFilterStore
from tests.fixtures.data import setup_bands
from tests.fixtures.items import Band
from tests.store.test_in_mem_store import TestInMemStore


class TestSearchFilterStore(TestInMemStore):

    def setUp(self):
        persisty_context = get_persisty_context()
        wrapped_store = mem_store(Band)
        wrapped_store.create(Band('mozart', 'Mozart', 1756))  # mostly filtered out
        store = SearchFilterStore(wrapped_store, AttrFilter('year_formed', AttrFilterOp.gte, 1900))
        setup_bands(store)
        self.wrapped_store = wrapped_store
        persisty_context.register_store(store)

    def test_read_blocked_key(self):
        assert self.get_band_store().read('mozart') is None

    def test_read_all_blocked_key_allow_missing(self):
        bands = list(self.get_band_store().read_all(('beatles', 'mozart'), False))
        assert bands[0].band_name == 'The Beatles'
        assert bands[1] is None

    def test_read_all_blocked_key_error_on_missing(self):
        with self.assertRaises(PersistyError):
            list(self.get_band_store().read_all(('beatles', 'mozart')))

    def test_create_blocked(self):
        with self.assertRaises(PersistyError):
            self.get_band_store().create(Band('mozart', 'Wolfgang Amadeus Mozart', 1756))
        assert self.wrapped_store.read('mozart').band_name == 'Mozart'

    def test_update_blocked(self):
        with self.assertRaises(PersistyError):
            self.get_band_store().update(Band('mozart', 'Wolfgang Amadeus Mozart', 1756))
        assert self.wrapped_store.read('mozart').band_name == 'Mozart'

    def test_destroy_blocked(self):
        assert self.get_band_store().destroy('mozart') is False
        assert self.wrapped_store.read('mozart').band_name == 'Mozart'

    def test_edit_all_create_blocked(self):
        with self.assertRaises(PersistyError):
            edits = [Edit(EditType.CREATE, None, Band('mozart', 'Wolfgang Amadeus Mozart', 1756))]
            self.get_band_store().edit_all(edits)
        assert self.wrapped_store.read('mozart').band_name == 'Mozart'

    def test_edit_all_update_blocked(self):
        with self.assertRaises(PersistyError):
            edits = [Edit(EditType.UPDATE, None, Band('mozart', 'Wolfgang Amadeus Mozart', 1756))]
            self.get_band_store().edit_all(edits)
        assert self.wrapped_store.read('mozart').band_name == 'Mozart'

    def test_edit_all_destroy_blocked(self):
        edits = [Edit(EditType.DESTROY, 'mozart')]
        self.get_band_store().edit_all(edits)
        assert self.wrapped_store.read('mozart').band_name == 'Mozart'

    def test_edit_all_destroy_blocked_with_prev_edit(self):
        jefferson = Band('jefferson', 'Jefferson Airplane', 1965)
        edits = [Edit(EditType.CREATE, item=jefferson),
                 Edit(EditType.DESTROY, 'mozart')]
        self.get_band_store().edit_all(edits)
        assert self.wrapped_store.read('mozart').band_name == 'Mozart'
        assert self.get_band_store().read('jefferson') == jefferson
