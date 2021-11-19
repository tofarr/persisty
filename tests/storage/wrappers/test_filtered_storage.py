from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.storage.in_mem.in_mem_storage import in_mem_storage
from persisty.storage.storage_context import StorageContext
from persisty.storage.wrappers.filtered_storage import FilteredStorage
from persisty.storage.wrappers.timestamped_storage import with_timestamps

from tests.fixtures.item_types import Member, Tag, Node, NodeTag, Band
from tests.fixtures.storage_data import populate_data
from tests.storage.tst_storage_abc import TstStorageABC


class TestFilteredStorage(TstStorageABC):

    def create_storage_context(self):
        storage_context = StorageContext()
        filter_ = AttrFilter('year_formed', AttrFilterOp.gte, 1900)
        storage_context.register_storage(FilteredStorage(in_mem_storage(Band), filter_))
        storage_context.register_storage(in_mem_storage(Member))
        storage_context.register_storage(with_timestamps(in_mem_storage(Tag)))
        storage_context.register_storage(with_timestamps(in_mem_storage(Node)))
        storage_context.register_storage(with_timestamps(in_mem_storage(NodeTag)))
        return storage_context
    
    def setUp(self):
        self.storage_context = self.create_storage_context()
        populate_data(self.storage_context)
        self.wrapped_storage.create(Band('mozart', 'Mozart', 1756))  # mostly filtered out

    @property
    def band_storage(self):
        return self.storage_context.get_storage(Band)

    @property
    def wrapped_storage(self):
        # noinspection PyUnresolvedReferences
        return self.band_storage.storage

    def test_read_blocked_key(self):
        assert self.band_storage.read('mozart') is None

    def test_read_all_blocked_key_allow_missing(self):
        bands = list(self.band_storage.read_all(('beatles', 'mozart'), False))
        assert bands[0].title == 'The Beatles'
        assert bands[1] is None

    def test_read_all_blocked_key_error_on_missing(self):
        with self.assertRaises(PersistyError):
            list(self.band_storage.read_all(('beatles', 'mozart')))

    def test_create_blocked(self):
        with self.assertRaises(PersistyError):
            self.band_storage.create(Band('mozart', 'Wolfgang Amadeus Mozart', 1756))
        assert self.wrapped_storage.read('mozart').title == 'Mozart'

    def test_update_blocked(self):
        with self.assertRaises(PersistyError):
            self.band_storage.update(Band('mozart', 'Wolfgang Amadeus Mozart', 1756))
        assert self.wrapped_storage.read('mozart').title == 'Mozart'

    def test_destroy_blocked(self):
        assert self.band_storage.destroy('mozart') is False
        assert self.wrapped_storage.read('mozart').title == 'Mozart'

    def test_edit_all_create_blocked(self):
        with self.assertRaises(PersistyError):
            edits = [Edit(EditType.CREATE, None, Band('mozart', 'Wolfgang Amadeus Mozart', 1756))]
            self.band_storage.edit_all(edits)
        assert self.wrapped_storage.read('mozart').title == 'Mozart'

    def test_edit_all_update_blocked(self):
        with self.assertRaises(PersistyError):
            edits = [Edit(EditType.UPDATE, None, Band('mozart', 'Wolfgang Amadeus Mozart', 1756))]
            self.band_storage.edit_all(edits)
        assert self.wrapped_storage.read('mozart').title == 'Mozart'

    def test_edit_all_destroy_blocked(self):
        edits = [Edit(EditType.DESTROY, 'mozart')]
        self.band_storage.edit_all(edits)
        assert self.wrapped_storage.read('mozart').title == 'Mozart'

    def test_edit_all_destroy_blocked_with_prev_edit(self):
        jefferson = Band('jefferson', 'Jefferson Airplane', 1965)
        edits = [Edit(EditType.CREATE, item=jefferson),
                 Edit(EditType.DESTROY, 'mozart')]
        self.band_storage.edit_all(edits)
        assert self.wrapped_storage.read('mozart').title == 'Mozart'
        assert self.band_storage.read('jefferson') == jefferson
