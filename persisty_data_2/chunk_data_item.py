import hashlib
import io
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Iterator, Any, Union
from uuid import uuid4, UUID

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store.store_abc import StoreABC
from persisty.util import UNDEFINED
from persisty_data_2.chunk import Chunk
from persisty_data_2.content_meta import ContentMeta
from persisty_data_2.data_item_abc import DataItemABC
from persisty_data_2.file_data_item import FileDataItem
from persisty_data_2.web_data_interface_abc import WebDataInterfaceABC


@dataclass
class ChunkDataItem(DataItemABC):
    content_meta: ContentMeta
    web_data_interface: Optional[WebDataInterfaceABC]
    content_meta_store: StoreABC[ContentMeta]
    chunk_store: StoreABC[Chunk]
    max_size: int = 1024 * 1024 * 100
    chunk_size: int = 1024 * 256
    _exists: Optional[bool] = None

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

    def exists(self):
        exists = self._exists
        if exists is None:
            exists = self._exists = bool(self.content_meta_store.read(self.content_meta.key))
        return exists

    def get_data_reader(self) -> io.IOBase:
        chunks = self._load_chunks()
        return _ChunkReader(chunks)

    def copy_data_to(self, destination):
        if isinstance(destination, str) or isinstance(destination, Path):
            Path(destination).parent.mkdir(parents=True, exist_ok=True)
            with open(destination, 'wb') as writer:
                for chunk in self._load_chunks():
                    writer.write(chunk.data)
        elif isinstance(destination, bytearray):
            for chunk in self._load_chunks():
                destination.extend(chunk.data)
        elif isinstance(destination, DataItemABC):
            chunks = self._load_chunks()
            reader = _ChunkReader(chunks)
            destination.copy_data_from(reader)
        else:
            for chunk in self._load_chunks():
                destination.write(chunk.data)

    def _load_chunks(self) -> Iterator[Chunk]:
        return self.chunk_store.search_all(
            search_filter=(
                AttrFilter('item_key', AttrFilterOp.eq, self.key)
                & AttrFilter('stream_id', AttrFilterOp.eq, self.content_meta.stream_id)
            ),
            search_order=SearchOrder((SearchOrderAttr('part_number'),))
        )

    def copy_data_from(self, source):
        if isinstance(source, str) or isinstance(source, Path):
            with open(source, 'rb') as reader:
                self._copy_data_from_reader(reader)
        elif isinstance(source, FileDataItem):
            with open(source.path, 'rb') as reader:
                self._copy_data_from_reader(reader)
        elif isinstance(source, bytes) or isinstance(source, bytearray):
            self._copy_data_from_reader(io.BytesIO(source))
        elif isinstance(source, DataItemABC):
            with _ChunkWriter(chunk_data_item=self) as writer:
                source.copy_data_to(writer)
        else:
            self._copy_data_from_reader(source)

    def _copy_data_from_reader(self, reader):
        part_number = 1
        stream_id = uuid4()
        while True:
            buffer = reader.read(self.chunk_size)
            if not buffer:
                self.content_meta.stream_id = stream_id
                if self.exists():
                    self.content_meta_store.update(self.content_meta)
                else:
                    self.content_meta_store.create(self.content_meta)
                return
            chunk = Chunk(
                item_key=self.content_meta.key,
                stream_id=stream_id,
                part_number=part_number,
                data=buffer
            )
            part_number += 1
            self.chunk_store.create(chunk)


@dataclass
class _ChunkWriter(io.RawIOBase):
    chunk_data_item: ChunkDataItem
    stream_id: UUID = field(default_factory=uuid4)
    part_number: int = 1
    md5: Any = field(default_factory=hashlib.md5)
    size: int = 0
    current_chunk: Optional[Chunk] = UNDEFINED

    def _new_chunk(self):
        _current_chunk = Chunk(
            item_key=self.chunk_data_item.key,
            stream_id=self.stream_id,
            part_number=self.part_number,
            data=bytearray()
        )

    def __enter__(self):
        self._new_chunk()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.current_chunk.data:
            self.md5.update(self.current_chunk.data)
            self.size += len(self.current_chunk.data)
            self.chunk_store.create(self.current_chunk)
            self.chunk_store = None  # Prevent accidental shenanigans!
        content_meta = self.chunk_data_item.content_meta
        content_meta.etag = self.md5.hexdigest()
        content_meta.size = self.size
        if self.chunk_data_item.exists():
            self.chunk_data_item.chunk_store.update(content_meta)
        else:
            self.chunk_data_item.chunk_store.create(content_meta)
        self.close()

    def close(self):
        super().close()

    def write(self, input_: Union[bytes, bytearray]) -> Optional[int]:
        offset = 0
        chunk_size = self.chunk_data_item.chunk_size
        while offset < len(input_):
            length = min(len(input_) - offset, chunk_size - len(self.current_chunk.data))
            self.current_chunk.data[len(self.current_chunk.data):] = input_[offset:(offset+length)]
            if len(self.current_chunk.data) == chunk_size:
                self.md5.update(self.current_chunk.data)
                self.size += chunk_size
                self.chunk_store.create(self.current_chunk)
                self._new_chunk()
            offset += length
        return offset

    def writable(self):
        return True

    def seekable(self):
        return False


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
