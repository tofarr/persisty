import io
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Iterator

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store.store_abc import StoreABC
from persisty.util import UNDEFINED
from persisty_data.chunk import Chunk
from persisty_data.content_meta import ContentMeta
from persisty_data.data_item_abc import DataItemABC


@dataclass
class ChunkDataItem(DataItemABC):
    content_meta: ContentMeta
    content_meta_store: StoreABC[ContentMeta]
    chunk_store: StoreABC[Chunk]

    @property
    def key(self) -> str:
        return self.content_meta.key

    @property
    def updated_at(self) -> Optional[datetime]:
        return self.content_meta.updated_at

    @property
    def etag(self) -> Optional[str]:
        return self.content_meta.etag

    @property
    def content_type(self) -> Optional[str]:
        return self.content_meta.content_type

    @property
    def size(self) -> Optional[int]:
        return self.content_meta.size

    def get_data_reader(self) -> io.IOBase:
        chunks = self._load_chunks()
        return _ChunkReader(chunks)

    def _load_chunks(self) -> Iterator[Chunk]:
        return self.chunk_store.search_all(
            search_filter=(
                AttrFilter('item_key', AttrFilterOp.eq, self.key)
                & AttrFilter('stream_id', AttrFilterOp.eq, self.content_meta.stream_id)
            ),
            search_order=SearchOrder((SearchOrderAttr('part_number'),))
        )


@dataclass
class _ChunkReader(io.RawIOBase):
    chunks: Iterator[Chunk]
    current_chunk: Optional[Chunk] = UNDEFINED
    offset_: int = 0

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        super().close()

    def readinto(self, output):
        if self.current_chunk is UNDEFINED:
            self.current_chunk = next(self.chunks, None)
        read_remaining = len(output)
        result = 0
        while True:
            if self.current_chunk is None:
                return result
            data = self.current_chunk.data
            length = min(read_remaining, len(data) - self.offset_)
            output[:length] = data[self.offset_:(self.offset_+length)]
            self.offset_ += length
            read_remaining -= length
            result += length
            if not read_remaining:
                return result
            self.current_chunk = next(self.chunks, None)
            self.offset_ = 0

    def readable(self):
        return True

    def seekable(self):
        return False
