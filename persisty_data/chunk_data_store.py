import dataclasses
import hashlib
import io
from dataclasses import dataclass, field
from typing import Optional, Any, Union
from uuid import UUID, uuid4

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.errors import PersistyError
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.util import UNDEFINED
from persisty_data.chunk import Chunk
from persisty_data.chunk_data_item import ChunkDataItem
from persisty_data.content_meta import ContentMeta
from persisty_data.data_item_abc import DataItemABC, DATA_ITEM_META
from persisty_data.data_store_abc import DataStoreABC, copy_data


@dataclass
class ChunkDataStore(DataStoreABC):
    name: str
    content_meta_store: StoreABC[ContentMeta]
    chunk_store: StoreABC[Chunk]
    chunk_size: int = 1024 * 256
    max_item_size: int = 1024 * 1024 * 50

    def get_meta(self) -> StoreMeta:
        meta = getattr(self, '_meta', None)
        if meta is None:
            # noinspection PyAttributeOutsideInit
            meta = self._meta = dataclasses.replace(DATA_ITEM_META, name=self.name)
        return meta

    def create(self, item: DataItemABC) -> Optional[DataItemABC]:
        with item.get_data_reader() as reader:
            with self.get_data_writer(item.key, item.content_type) as writer:
                copy_data(reader, writer, self.chunk_size)
        chunk_data_item = self._chunk_data_item(writer.content_meta)
        return chunk_data_item

    def read(self, key: str) -> Optional[ChunkDataItem]:
        content_meta = self.content_meta_store.read(key)
        if content_meta:
            return self._chunk_data_item(content_meta)

    def _chunk_data_item(self, content_meta: ContentMeta):
        return ChunkDataItem(
            content_meta_store=self.content_meta_store,
            chunk_store=self.chunk_store,
            content_meta=content_meta
        )

    def _update(self, key: str, item: ChunkDataItem, updates: DataItemABC) -> Optional[ChunkDataItem]:
        old_stream_id = item.content_meta.stream_id
        with updates.get_data_reader() as reader:
            with self.get_data_writer(item.key, item.content_type) as writer:
                copy_data(reader, writer, self.chunk_size)
        item = self._chunk_data_item(writer.content_meta)
        _delete_chunks(self.chunk_store, old_stream_id)
        return item

    def _delete(self, key: str, item: ChunkDataItem) -> bool:
        self.content_meta_store._delete(key, item.content_meta)
        _delete_chunks(self.chunk_store, item.content_meta.stream_id)

    def count(self, search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL) -> int:
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

    def get_data_writer(self, key: str, content_type: Optional[str] = None):
        create = not self.content_meta_store.read(key)
        return _ChunkWriter(
            content_meta_store=self.content_meta_store,
            chunk_store=self.chunk_store,
            key=key,
            create=create,
            chunk_size=self.chunk_size,
            content_type=content_type,
            max_item_size=self.max_item_size
        )


def _delete_chunks(chunk_store: StoreABC[Chunk], stream_id: UUID):
    if not stream_id:
        return
    search_filter = AttrFilter('stream_id', AttrFilterOp.eq, stream_id)
    chunks = chunk_store.search_all(search_filter=search_filter)
    chunk_key_config = chunk_store.get_meta().key_config
    edits = (BatchEdit(delete_key=chunk_key_config.to_key_str(c)) for c in chunks)
    chunk_store.edit_all(edits)


@dataclass
class _ChunkWriter(io.RawIOBase):
    content_meta_store: StoreABC[ContentMeta]
    chunk_store: StoreABC[Chunk]
    key: str
    create: bool
    chunk_size: int
    max_item_size: int
    content_type: Optional[str] = None
    stream_id: UUID = field(default_factory=uuid4)
    part_number: int = 1
    md5: Any = field(default_factory=hashlib.md5)
    size: int = 0
    current_chunk: Optional[Chunk] = UNDEFINED
    content_meta: Optional[ContentMeta] = None

    def _new_chunk(self):
        self.current_chunk = Chunk(
            item_key=self.key,
            stream_id=self.stream_id,
            part_number=self.part_number,
            data=bytearray()
        )

    def __enter__(self):
        self._new_chunk()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            _delete_chunks(self.chunk_store, self.stream_id)
        if self.current_chunk.data:
            self.md5.update(self.current_chunk.data)
            self.size += len(self.current_chunk.data)
            self.current_chunk.data = bytes(self.current_chunk.data)
            self.chunk_store.create(self.current_chunk)
            self.chunk_store = None  # Prevent accidental shenanigans!
        content_meta = ContentMeta(
            key=self.key,
            stream_id=self.stream_id,
            content_type=self.content_type,
            etag=self.md5.hexdigest(),
            size=self.size
        )
        if self.create:
            self.content_meta = self.content_meta_store.create(content_meta)
        else:
            self.content_meta = self.content_meta_store.update(content_meta)
        self.close()

    def close(self):
        super().close()

    def write(self, input_: Union[bytes, bytearray]) -> Optional[int]:
        offset = 0
        chunk_size = self.chunk_size
        while offset < len(input_):
            length = min(len(input_) - offset, chunk_size - len(self.current_chunk.data))
            self.current_chunk.data[len(self.current_chunk.data):] = input_[offset:(offset+length)]
            if len(self.current_chunk.data) == chunk_size:
                self.md5.update(self.current_chunk.data)
                self.size += chunk_size
                if self.size >= self.max_item_size:
                    raise PersistyError('max_item_size_exceeded')
                self.current_chunk.data = bytes(self.current_chunk.data)
                self.chunk_store.create(self.current_chunk)
                self._new_chunk()
            offset += length
        return offset

    def writable(self):
        return True

    def seekable(self):
        return False
