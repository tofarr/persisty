from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.store.restrict_access_store import RestrictAccessStore
from persisty.store.store_abc import StoreABC
from persisty.store_access import StoreAccess
from persisty_data.chunk import Chunk
from persisty_data.content_meta import ContentMeta


class ContentMetaStore(RestrictAccessStore[ContentMeta]):

    def __init__(self, content_meta_store: StoreABC[ContentMeta], chunk_store: StoreABC[Chunk]):
        super().__init__(content_meta_store, StoreAccess(creatable=False, updatable=False))
        self.chunk_store = chunk_store

    def get_store(self) -> StoreABC:
        return self.store

    def _delete(self, key: str, item: ContentMeta) -> bool:
        result = self.get_store()._delete(key, item)
        if result:
            chunk_key_config = self.chunk_store.get_meta().key_config
            chunks = self.chunk_store.search_all(AttrFilter('content_key', AttrFilterOp.eq, key))
            edits = (BatchEdit(delete_key=chunk_key_config.to_key_str(c)) for c in chunks)
            self.chunk_store.edit_all(edits)
        return result
