import mimetypes
from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta
from persisty_data_2.chunk import Chunk
from persisty_data_2.chunk_data_item import ChunkDataItem
from persisty_data_2.content_meta import ContentMeta
from persisty_data_2.data_item_abc import DataItemABC
from persisty_data_2.web_data_interface_abc import WebDataInterfaceABC


class ChunkDataStore(StoreABC[DataItemABC]):
    content_meta_store: StoreABC[ContentMeta]
    chunk_store: StoreABC[Chunk]
    web_data_interface: Optional[WebDataInterfaceABC]
    chunk_size: int = 1024 * 256

    def get_meta(self) -> StoreMeta:
        return self.content_meta_store.get_meta()

    def create(self, item: DataItemABC) -> Optional[DataItemABC]:
        content_meta = ContentMeta(
            key=item.key,
            stream_id=None,
            content_type=item.content_type or mimetypes.guess_type(item.key)[0],
            etag=None,
            size=0,
            updated_at=datetime.now()
        )
        chunk_data_item = ChunkDataItem(
            content_meta_store=self.content_meta_store,
            chunk_store=self.chunk_store,
            chunk_size=self.chunk_size,
            content_meta=content_meta,
            web_data_interface=self.web_data_interface,
            _exists=False
        )
        chunk_data_item.copy_data_from(item)
        return chunk_data_item

    def read(self, key: str) -> Optional[ChunkDataItem]:
        content_meta = self.content_meta_store.read(key)
        if content_meta:
            return self._chunk_data_item(content_meta)

    def _chunk_data_item(self, content_meta: ContentMeta):
        return ChunkDataItem(
            content_meta_store=self.content_meta_store,
            chunk_store=self.chunk_store,
            chunk_size=self.chunk_size,
            content_meta=content_meta,
            web_data_interface=self.web_data_interface
        )

    def _update(self, key: str, item: ChunkDataItem, updates: DataItemABC) -> Optional[ChunkDataItem]:
        old_stream_id = item.content_meta.stream_id
        item.copy_data_from(updates)
        self._delete_chunks(old_stream_id)
        return item

    def _delete(self, key: str, item: ChunkDataItem) -> bool:
        self.content_meta_store._delete(key, item.content_meta)
        self._delete_chunks(item.content_meta.stream_id)

    def count(self, search_filter: SearchFilterABC[T] = INCLUDE_ALL) -> int:
        return self.content_meta_store.count(search_filter)

    def search(
        self,
        search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataItemABC]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[ChunkDataItem]:
        result_set = self.content_meta_store.search(search_filter, search_order, page_key, limit)
        result_set.results = [self._chunk_data_item(c) for c in result_set.results]

    def _delete_chunks(self, stream_id: UUID):
        if not stream_id:
            return
        search_filter = AttrFilter('stream_id', AttrFilterOp.eq, stream_id)
        chunks = self.chunk_store.search_all(search_filter=search_filter)
        chunk_key_config = self.chunk_store.get_meta().key_config
        edits = (BatchEdit(delete_key=chunk_key_config.to_key_str(c)) for c in chunks)
        self.chunk_store.edit_all(edits)
